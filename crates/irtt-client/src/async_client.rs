use std::{
    future::{poll_fn, Future},
    io,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use tokio::time;

use crate::{
    client::{echo_sent_event, validate_datagram_length},
    config::ClientConfig,
    error::ClientError,
    event::{ClientEvent, OpenOutcome},
    receive::{drain_tx_timestamps, try_enable_tx_timestamping, try_recv_tokio_datagram},
    session::machine::{
        recv_buffer_size, OpenDatagramDisposition, PreparedOpenAcceptance, PreparedOpenRequest,
        PreparedProbe, SessionMachine, TimeoutBatch, MAX_OPEN_PACKET_SIZE,
    },
    socket::{connect_tokio_udp_socket, resolve_remote_tokio, validate_open_timeouts},
    socket_options::{apply_traffic_class_to_tokio_socket, clear_dscp_on_tokio_socket},
    timing::ClientTimestamp,
};

/// Maximum amount of immediately available opening work performed by one poll.
///
/// Opening may consume ignored UDP datagrams, roll over an expired attempt, or
/// encounter readiness false positives without ever awaiting.  Bound that work
/// so one managed target cannot retain the current-thread runtime.
const OPEN_POLL_WORK_BUDGET: usize = 64;

/// Maximum immediately available receive work performed by one
/// [`AsyncClient::poll_recv`] poll.
///
/// A `WouldBlock` readiness false positive is naturally bounded: `try_io`
/// clears the reactor's readiness on `WouldBlock`, so the next
/// `poll_recv_ready` genuinely awaits a fresh edge. An `Interrupted`
/// (`EINTR`) nonblocking `recvmsg` does not clear that readiness — the
/// syscall was interrupted, not told there was nothing to read — so without
/// an explicit bound a sustained signal delivery rate could otherwise retry
/// in a tight loop on this executor thread. Bounding both cases the same way
/// this crate already bounds open-attempt work keeps that impossible.
const RECV_POLL_WORK_BUDGET: usize = 64;

#[derive(Debug)]
struct PreparedAsyncOpen {
    machine: PreparedOpenAcceptance,
    recv_buffer_len: Option<usize>,
    negotiated_traffic_class: Option<u8>,
}

#[derive(Debug)]
struct PreparedAsyncOpenFailure {
    primary: ClientError,
    machine: PreparedOpenAcceptance,
}

#[derive(Debug)]
enum AsyncOpenCleanup {
    Acceptance {
        primary: ClientError,
        packet: Option<Box<[u8]>>,
    },
    Adapter(Box<PreparedAsyncOpenFailure>),
}

impl AsyncOpenCleanup {
    fn packet(&self) -> Option<&[u8]> {
        match self {
            Self::Acceptance { packet, .. } => packet.as_deref(),
            Self::Adapter(failure) => failure.machine.cleanup_close_packet(),
        }
    }

    fn into_primary(self) -> ClientError {
        match self {
            Self::Acceptance { primary, .. } => primary,
            Self::Adapter(failure) => failure.primary,
        }
    }
}

/// Owned state for one asynchronous open transaction.
///
/// This state deliberately contains no reference to [`AsyncClient`], allowing
/// a future managed target to store the client and opening state side by side.
#[derive(Debug)]
pub(crate) struct AsyncOpenState {
    attempt: usize,
    deadline: Option<Instant>,
    deadline_timer: Option<Pin<Box<time::Sleep>>>,
    request_submitted: bool,
    stop_after_current_attempt: bool,
    stopped_after_current_attempt: bool,
    buffer: [u8; MAX_OPEN_PACKET_SIZE],
    cleanup: Option<AsyncOpenCleanup>,
}

impl AsyncOpenState {
    pub(crate) fn new() -> Self {
        Self {
            attempt: 0,
            deadline: None,
            deadline_timer: None,
            request_submitted: false,
            stop_after_current_attempt: false,
            stopped_after_current_attempt: false,
            buffer: [0_u8; MAX_OPEN_PACKET_SIZE],
            cleanup: None,
        }
    }

    fn start_attempt(&mut self, timeout: Duration) -> Result<(), ClientError> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(ClientError::DurationOverflow)?;
        self.deadline = Some(deadline);
        self.deadline_timer = Some(Box::pin(time::sleep_until(deadline.into())));
        self.request_submitted = false;
        Ok(())
    }

    fn finish_attempt(&mut self) {
        self.attempt += 1;
        self.deadline = None;
        self.deadline_timer = None;
        self.request_submitted = false;
        self.stopped_after_current_attempt = self.stop_after_current_attempt;
    }

    pub(crate) fn has_in_flight_work(&self) -> bool {
        self.request_submitted || self.cleanup.is_some()
    }

    pub(crate) fn request_stop_after_current_attempt(&mut self) {
        self.stop_after_current_attempt = true;
    }

    pub(crate) fn stopped_after_current_attempt(&self) -> bool {
        self.stopped_after_current_attempt
    }

    fn deadline(&self) -> Instant {
        self.deadline
            .expect("an open attempt has an active deadline")
    }

    fn deadline_timer(&mut self) -> &mut Pin<Box<time::Sleep>> {
        self.deadline_timer
            .as_mut()
            .expect("an open attempt has an active deadline timer")
    }
}

/// Low-level Tokio IRTT client for one connected UDP target.
///
/// `AsyncClient` does not construct, own, or store a Tokio runtime. Its async
/// methods are polled by the caller and require a current Tokio runtime with I/O
/// and time enabled.
///
/// # Example
///
/// Use this low-level adapter when the application already owns Tokio and wants
/// to drive the session lifecycle itself. This example type-checks without
/// contacting a server.
///
/// ```no_run
/// use irtt_client::{AsyncClient, ClientConfig};
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let mut client = AsyncClient::connect(ClientConfig::default()).await?;
/// let outcome = client.open().await?;
/// println!("opened: {outcome:?}");
/// let sent = client.send_probe().await?;
/// println!("sent: {sent:?}");
/// let received = client.recv().await?;
/// println!("received: {received:?}");
/// let timed_out = client.poll_timeouts()?;
/// println!("timeouts: {timed_out:?}");
/// let closed = client.close().await?;
/// println!("closed: {closed:?}");
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct AsyncClient {
    socket: tokio::net::UdpSocket,
    machine: SessionMachine,
    remote: SocketAddr,
    recv_buffer: Vec<u8>,
    applied_traffic_class: Option<u8>,
    prepared_open: Option<PreparedOpenRequest>,
    prepared_probe: Option<PreparedProbe>,
    /// See `Client::tx_timestamping_enabled` in `client.rs` for the
    /// invariant this tracks; it is the same adapter-owned capability flag.
    tx_timestamping_enabled: bool,
}

impl AsyncClient {
    /// Resolve the configured server and construct one connected Tokio UDP
    /// socket.
    ///
    /// Polling this future without a current Tokio runtime returns
    /// [`ClientError::NoTokioRuntime`]. A runtime without enabled I/O or time
    /// drivers is outside this type's runtime contract.
    pub async fn connect(config: ClientConfig) -> Result<Self, ClientError> {
        tokio::runtime::Handle::try_current().map_err(|_| ClientError::NoTokioRuntime)?;
        validate_open_timeouts(&config.open_timeouts)?;
        let remote = resolve_remote_tokio(&config).await?;
        let machine = SessionMachine::new(config.clone(), remote)?;
        let prepared_open = machine.prepare_open_request()?;
        let socket = connect_tokio_udp_socket(&config.socket_config, remote)?;

        Ok(Self {
            socket,
            machine,
            remote,
            recv_buffer: vec![0_u8; recv_buffer_size(false, None)?],
            applied_traffic_class: None,
            prepared_open: Some(prepared_open),
            prepared_probe: None,
            tx_timestamping_enabled: false,
        })
    }

    /// Perform the IRTT open exchange.
    ///
    /// Each configured attempt uses one absolute deadline for one successful
    /// request submission and all replies inspected during that attempt.
    /// Malformed, unrelated, wrong-direction, and unauthenticated datagrams are
    /// ignored without resending the request.
    ///
    /// Dropping this future before trusted acceptance leaves the local machine
    /// connected with no negotiated adapter state. If it is dropped during
    /// best-effort post-token cleanup, cleanup may remain incomplete because
    /// this low-level client never detaches cleanup work.
    pub async fn open(&mut self) -> Result<OpenOutcome, ClientError> {
        let mut state = AsyncOpenState::new();
        poll_fn(|cx| self.poll_open(&mut state, cx)).await
    }

    /// Send one echo probe now, committing protocol state only after acceptance.
    /// The caller owns pacing and run duration.
    ///
    /// A prepared packet is retained across readiness false positives,
    /// cancellation, and socket errors so a later call retries the same logical
    /// probe without advancing state before kernel acceptance.
    pub async fn send_probe(&mut self) -> Result<Vec<ClientEvent>, ClientError> {
        poll_fn(|cx| self.poll_send_probe(cx)).await
    }

    /// Await and classify one complete UDP datagram.
    ///
    /// State is validated before readiness is awaited. Readiness false positives
    /// retry without changing protocol state. Authenticated peer-close events
    /// remain authoritative even if best-effort DSCP cleanup fails.
    pub async fn recv(&mut self) -> Result<Vec<ClientEvent>, ClientError> {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    /// Poll protocol timeouts using a newly captured monotonic timestamp.
    pub fn poll_timeouts(&mut self) -> Result<Vec<ClientEvent>, ClientError> {
        self.poll_timeouts_at(Instant::now())
    }

    /// Poll protocol timeouts using the caller's monotonic timestamp.
    pub fn poll_timeouts_at(&mut self, now: Instant) -> Result<Vec<ClientEvent>, ClientError> {
        self.drain_tx_timestamps()?;
        self.machine.poll_timeouts_at(now)
    }

    pub(crate) fn poll_timeouts_bounded_at(
        &mut self,
        now: Instant,
        limit: usize,
    ) -> Result<TimeoutBatch, ClientError> {
        self.drain_tx_timestamps()?;
        self.machine.poll_timeouts_bounded_at(now, limit)
    }

    /// Send the retained close packet and commit local close exactly once.
    ///
    /// DSCP is cleared only after write readiness and immediately before the
    /// nonblocking send attempt. A `WouldBlock` result restores it before the
    /// next await, so dropping this future at a suspension point cannot leave
    /// an otherwise-open session with cleared DSCP.
    pub async fn close(&mut self) -> Result<Vec<ClientEvent>, ClientError> {
        poll_fn(|cx| self.poll_close(cx)).await
    }

    pub(crate) fn poll_open(
        &mut self,
        state: &mut AsyncOpenState,
        cx: &mut Context<'_>,
    ) -> Poll<Result<OpenOutcome, ClientError>> {
        if self.prepared_open.is_none() {
            if let Err(error) = self.machine.prepare_open_request() {
                return Poll::Ready(Err(error));
            }
            unreachable!("connected async clients retain an open request");
        }

        for _ in 0..OPEN_POLL_WORK_BUDGET {
            if state.cleanup.is_some() {
                return self.poll_open_cleanup(state, cx);
            }

            let attempt_count = self.machine.config().open_timeouts.len();
            if state.attempt >= attempt_count {
                return Poll::Ready(Err(ClientError::OpenTimeout));
            }
            if state.stop_after_current_attempt && state.deadline.is_none() {
                state.stopped_after_current_attempt = true;
                return Poll::Ready(Err(ClientError::OpenTimeout));
            }
            if state.deadline.is_none() {
                let timeout = self.machine.config().open_timeouts[state.attempt];
                if let Err(error) = state.start_attempt(timeout) {
                    return Poll::Ready(Err(error));
                }
            }

            if !state.request_submitted {
                match poll_writable_until(&self.socket, state.deadline_timer(), cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Ready(Ok(false)) => {
                        state.finish_attempt();
                        continue;
                    }
                    Poll::Ready(Ok(true)) => {}
                }

                let request = self
                    .prepared_open
                    .as_ref()
                    .expect("connected clients retain their prepared open request");
                let send_result = self.socket.try_send(&request.bytes);
                match send_result {
                    Ok(bytes) => {
                        state.request_submitted = true;
                        if let Err(error) = validate_datagram_length(request.bytes.len(), bytes) {
                            return Poll::Ready(Err(error));
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                    Err(error) => return Poll::Ready(Err(ClientError::Socket(error))),
                }
            }

            match poll_readable_until(&self.socket, state.deadline_timer(), cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(false)) => {
                    state.finish_attempt();
                    if state.stopped_after_current_attempt() {
                        return Poll::Ready(Err(ClientError::OpenTimeout));
                    }
                    continue;
                }
                Poll::Ready(Ok(true)) => {}
            }

            let datagram = match try_recv_tokio_datagram(&self.socket, &mut state.buffer) {
                Ok(datagram) => datagram,
                // WouldBlock is a readiness false positive; Interrupted
                // (EINTR) is a transient nonblocking-recvmsg interruption.
                // Both retry through the surrounding `OPEN_POLL_WORK_BUDGET`-
                // bounded loop above, which re-checks readiness before the
                // next attempt and yields back to the executor once that
                // budget is exhausted.
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                    ) =>
                {
                    continue
                }
                Err(error) => return Poll::Ready(Err(ClientError::Socket(error))),
            };
            if datagram.received_at.mono > state.deadline() {
                state.finish_attempt();
                if state.stopped_after_current_attempt() {
                    return Poll::Ready(Err(ClientError::OpenTimeout));
                }
                continue;
            }

            let reply = match self
                .machine
                .inspect_open_datagram(&state.buffer[..datagram.len])
            {
                Ok(OpenDatagramDisposition::Ignore) => continue,
                Ok(OpenDatagramDisposition::Trusted(reply)) => reply,
                Err(error) => return Poll::Ready(Err(error)),
            };
            let machine = match self
                .machine
                .prepare_open_acceptance(reply, datagram.received_at)
            {
                Ok(machine) => machine,
                Err(failure) => {
                    state.cleanup = Some(AsyncOpenCleanup::Acceptance {
                        primary: failure.primary,
                        packet: failure.cleanup_close,
                    });
                    continue;
                }
            };
            let prepared = match self.prepare_async_open(machine) {
                Ok(prepared) => prepared,
                Err(failure) => {
                    state.cleanup = Some(AsyncOpenCleanup::Adapter(failure));
                    continue;
                }
            };
            return Poll::Ready(Ok(self.commit_async_open(prepared)));
        }

        // We deliberately truncated known immediate opening work.  Arrange for
        // another poll even if no new socket-readiness edge occurs.
        cx.waker().wake_by_ref();
        Poll::Pending
    }

    pub(crate) fn poll_send_probe(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Vec<ClientEvent>, ClientError>> {
        if let Err(error) = self.machine.ensure_open() {
            return Poll::Ready(Err(error));
        }
        if self.prepared_probe.is_none() {
            match self.machine.prepare_probe() {
                Ok(prepared) => self.prepared_probe = Some(prepared),
                Err(error) => return Poll::Ready(Err(error)),
            }
        }
        loop {
            match self.socket.poll_send_ready(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(error)) => {
                    return Poll::Ready(Err(ClientError::Socket(error)));
                }
            }

            let prepared = self
                .prepared_probe
                .as_ref()
                .expect("prepared probe was retained across readiness");
            let machine_preflight = match self.machine.preflight_probe_commit(prepared) {
                Ok(preflight) => preflight,
                Err(error) => return Poll::Ready(Err(error)),
            };
            let mut events = Vec::new();
            if let Err(source) = events.try_reserve(1) {
                return Poll::Ready(Err(ClientError::AllocationFailed {
                    operation: "probe event result",
                    source,
                }));
            }
            let expected_bytes = prepared.bytes.len();

            // Private pre-send anchor: finalizes all fallible commit work
            // (timeout deadline arithmetic, kernel TX lower bound) before
            // the send attempt. A WouldBlock retry samples a fresh one on
            // the next loop iteration; only the successful attempt's anchor
            // is retained (via `machine_commit`/`commit_probe_sent` below).
            let send_anchor = ClientTimestamp::now();
            let machine_commit = match self
                .machine
                .finalize_probe_commit(machine_preflight, send_anchor)
            {
                Ok(commit) => commit,
                Err(error) => return Poll::Ready(Err(error)),
            };
            let send_call_start = Instant::now();
            let send_result = self.socket.try_send(&prepared.bytes);
            let bytes = match send_result {
                Ok(bytes) => bytes,
                // No measurement or machine commit: retry the transaction
                // with a fresh send_anchor on the next loop iteration.
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    self.machine.invalidate_kernel_tx_correlation();
                    continue;
                }
                Err(error) => {
                    self.machine.invalidate_kernel_tx_correlation();
                    return Poll::Ready(Err(ClientError::Socket(error)));
                }
            };
            let send_finished_at = Instant::now();

            // Public post-send measurement, captured only after the
            // successful send above. The subsequent machine commit
            // is infallible.
            let sent_at = ClientTimestamp::now();
            let sent = self
                .machine
                .commit_probe_sent(machine_commit, sent_at, bytes);
            self.prepared_probe = None;
            let send_call = send_finished_at.saturating_duration_since(send_call_start);

            if let Err(error) = validate_datagram_length(expected_bytes, bytes) {
                return Poll::Ready(Err(error));
            }
            if let Err(error) = self.drain_tx_timestamps() {
                return Poll::Ready(Err(error));
            }
            events.push(echo_sent_event(self.remote, sent, send_call));
            return Poll::Ready(Ok(events));
        }
    }

    pub(crate) fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Vec<ClientEvent>, ClientError>> {
        if let Err(error) = self.machine.ensure_open() {
            return Poll::Ready(Err(error));
        }

        for _ in 0..RECV_POLL_WORK_BUDGET {
            match self.socket.poll_recv_ready(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(error)) => {
                    return Poll::Ready(Err(ClientError::Socket(error)));
                }
            }

            let datagram = match try_recv_tokio_datagram(&self.socket, &mut self.recv_buffer) {
                Ok(datagram) => datagram,
                // See `RECV_POLL_WORK_BUDGET`: both a readiness false
                // positive and an interrupted nonblocking recvmsg retry
                // within this bounded loop rather than failing the receive.
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                    ) =>
                {
                    continue
                }
                Err(error) => return Poll::Ready(Err(ClientError::Socket(error))),
            };

            // One final bounded drain before processing the reply, so a TX
            // timestamp that raced the reply is still associated with its
            // probe before that probe is looked up and possibly removed
            // below.
            if let Err(error) = self.drain_tx_timestamps() {
                return Poll::Ready(Err(error));
            }

            let events = match self.machine.process_received_echo_packet(
                &self.recv_buffer[..datagram.len],
                datagram.received_at,
                datagram.meta,
            ) {
                Ok(events) => events,
                Err(error) => return Poll::Ready(Err(error)),
            };
            if self.machine.is_peer_closed() {
                self.prepared_probe = None;
                if self.clear_peer_close_dscp().is_ok() {
                    self.applied_traffic_class = None;
                }
            }
            return Poll::Ready(Ok(events));
        }

        // Truncated known immediate receive work, exactly as `poll_open`
        // does: arrange for another poll even if no new socket-readiness
        // edge occurs, rather than spinning this executor thread further.
        cx.waker().wake_by_ref();
        Poll::Pending
    }

    pub(crate) fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Vec<ClientEvent>, ClientError>> {
        loop {
            let prepared = match self.machine.prepare_close() {
                Ok(prepared) => prepared,
                Err(error) => return Poll::Ready(Err(error)),
            };
            let mut events = Vec::new();
            if let Err(source) = events.try_reserve(1) {
                return Poll::Ready(Err(ClientError::AllocationFailed {
                    operation: "close event result",
                    source,
                }));
            }
            let previous_traffic_class = self.applied_traffic_class;
            let expected_bytes = prepared.bytes.len();

            match self.socket.poll_send_ready(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(error)) => {
                    return Poll::Ready(Err(ClientError::Socket(error)));
                }
            }
            if let Err(error) = self.clear_close_dscp() {
                return Poll::Ready(Err(error));
            }
            let mut rollback =
                DscpRollback::armed(&self.socket, self.remote, previous_traffic_class);
            let send_result = self.socket.try_send(prepared.bytes);
            let bytes = match send_result {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    if let Err(error) = rollback.restore() {
                        return Poll::Ready(Err(error));
                    }

                    continue;
                }
                Err(error) => return Poll::Ready(Err(ClientError::Socket(error))),
            };

            let close_sent_at = ClientTimestamp::now();

            let event = self
                .machine
                .commit_local_close(prepared.commit, close_sent_at);
            rollback.disarm();
            self.prepared_probe = None;
            self.applied_traffic_class = None;

            if let Err(error) = validate_datagram_length(expected_bytes, bytes) {
                return Poll::Ready(Err(error));
            }
            events.push(event);
            return Poll::Ready(Ok(events));
        }
    }

    /// Return the configured local probe timeout.
    pub fn probe_timeout(&self) -> Duration {
        self.machine.probe_timeout()
    }

    /// Negotiated parameters of the currently open session.
    pub fn negotiated_params(&self) -> Option<&crate::NegotiatedParams> {
        self.machine.negotiated_params()
    }

    /// Whether any sent probes still await a reply or timeout.
    pub fn has_pending_probes(&self) -> bool {
        !self.machine.pending_is_empty()
    }

    /// Return whether an authenticated peer close ended the session.
    pub fn is_peer_closed(&self) -> bool {
        self.machine.is_peer_closed()
    }

    pub(crate) fn remote_addr(&self) -> SocketAddr {
        self.remote
    }

    pub(crate) fn packets_sent(&self) -> u64 {
        self.machine.packets_sent()
    }

    /// Earliest timeout deadline among probes still awaiting a reply.
    pub fn next_probe_timeout_deadline(&self) -> Option<Instant> {
        self.machine.next_probe_timeout_deadline()
    }

    pub(crate) fn latest_probe_timeout_deadline(&self) -> Option<Instant> {
        self.machine.latest_probe_timeout_deadline()
    }

    pub(crate) fn discard_prepared_probe(&mut self) {
        self.prepared_probe = None;
    }

    fn prepare_async_open(
        &mut self,
        machine: PreparedOpenAcceptance,
    ) -> Result<PreparedAsyncOpen, Box<PreparedAsyncOpenFailure>> {
        let Some(negotiated) = machine.normal_negotiated() else {
            return Ok(PreparedAsyncOpen {
                machine,
                recv_buffer_len: None,
                negotiated_traffic_class: None,
            });
        };
        let recv_buffer_len = match recv_buffer_size(self.machine.has_hmac(), Some(negotiated)) {
            Ok(size) => size,
            Err(primary) => {
                return Err(Box::new(PreparedAsyncOpenFailure { primary, machine }));
            }
        };
        let negotiated_traffic_class = match u8::try_from(negotiated.params.dscp) {
            Ok(traffic_class) => traffic_class,
            Err(_) => {
                return Err(Box::new(PreparedAsyncOpenFailure {
                    primary: ClientError::InvalidConfig {
                        reason: "negotiated dscp must be in range 0..=255".to_owned(),
                    },
                    machine,
                }));
            }
        };
        let additional = recv_buffer_len.saturating_sub(self.recv_buffer.len());
        if let Err(source) = self.recv_buffer.try_reserve(additional) {
            return Err(Box::new(PreparedAsyncOpenFailure {
                primary: ClientError::AllocationFailed {
                    operation: "negotiated receive buffer",
                    source,
                },
                machine,
            }));
        }

        let dscp_result = apply_traffic_class_to_tokio_socket(
            &self.socket,
            self.remote,
            negotiated_traffic_class,
        );
        if let Err(primary) = dscp_result {
            self.restore_dscp_best_effort(self.applied_traffic_class);
            return Err(Box::new(PreparedAsyncOpenFailure { primary, machine }));
        }

        Ok(PreparedAsyncOpen {
            machine,
            recv_buffer_len: Some(recv_buffer_len),
            negotiated_traffic_class: Some(negotiated_traffic_class),
        })
    }

    fn commit_async_open(&mut self, prepared: PreparedAsyncOpen) -> OpenOutcome {
        if let Some(recv_buffer_len) = prepared.recv_buffer_len {
            self.recv_buffer.resize(recv_buffer_len, 0);
        }
        let outcome = self.machine.commit_open(prepared.machine);

        self.applied_traffic_class = prepared.negotiated_traffic_class;
        self.prepared_open = None;
        self.tx_timestamping_enabled = try_enable_tx_timestamping(&self.socket);
        outcome
    }

    /// See `Client::drain_tx_timestamps` in `client.rs`: same bounded,
    /// nonblocking, best-effort drain, adapted to the Tokio socket.
    fn drain_tx_timestamps(&mut self) -> Result<(), ClientError> {
        if !self.tx_timestamping_enabled {
            return Ok(());
        }
        let machine = &mut self.machine;
        drain_tx_timestamps(&self.socket, |id, timestamp| {
            machine.record_kernel_tx_timestamp(id, timestamp);
        })
        .map_err(ClientError::Socket)
    }

    fn poll_open_cleanup(
        &self,
        state: &mut AsyncOpenState,
        cx: &mut Context<'_>,
    ) -> Poll<Result<OpenOutcome, ClientError>> {
        let Some(packet) = state
            .cleanup
            .as_ref()
            .expect("cleanup polling requires a retained primary error")
            .packet()
        else {
            return Poll::Ready(Err(state.cleanup.take().unwrap().into_primary()));
        };
        let send_result = self.socket.try_send(packet);
        match send_result {
            Ok(_) => return Poll::Ready(Err(state.cleanup.take().unwrap().into_primary())),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(_) => return Poll::Ready(Err(state.cleanup.take().unwrap().into_primary())),
        }

        match poll_writable_until(&self.socket, state.deadline_timer(), cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(true)) => {
                // Retain cleanup state and resume the send in a fresh poll.  A
                // writable readiness false positive must not spin this future.
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Ready(Ok(false) | Err(_)) => {
                Poll::Ready(Err(state.cleanup.take().unwrap().into_primary()))
            }
        }
    }

    fn clear_peer_close_dscp(&self) -> Result<(), ClientError> {
        clear_dscp_on_tokio_socket(&self.socket, self.remote)
    }

    fn clear_close_dscp(&self) -> Result<(), ClientError> {
        clear_dscp_on_tokio_socket(&self.socket, self.remote)
    }

    fn restore_dscp_best_effort(&self, previous_traffic_class: Option<u8>) {
        let _ = match previous_traffic_class {
            Some(traffic_class) => {
                apply_traffic_class_to_tokio_socket(&self.socket, self.remote, traffic_class)
            }
            None => clear_dscp_on_tokio_socket(&self.socket, self.remote),
        };
    }
}

struct DscpRollback<'a> {
    socket: &'a tokio::net::UdpSocket,
    remote: SocketAddr,
    previous_traffic_class: Option<u8>,
    armed: bool,
}

impl<'a> DscpRollback<'a> {
    fn armed(
        socket: &'a tokio::net::UdpSocket,
        remote: SocketAddr,
        previous_traffic_class: Option<u8>,
    ) -> Self {
        Self {
            socket,
            remote,
            previous_traffic_class,
            armed: true,
        }
    }

    fn restore(&mut self) -> Result<(), ClientError> {
        if !self.armed {
            return Ok(());
        }

        match self.previous_traffic_class {
            Some(traffic_class) => {
                apply_traffic_class_to_tokio_socket(self.socket, self.remote, traffic_class)?
            }
            None => clear_dscp_on_tokio_socket(self.socket, self.remote)?,
        }
        self.armed = false;
        Ok(())
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for DscpRollback<'_> {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.restore();
        }
    }
}

fn poll_writable_until(
    socket: &tokio::net::UdpSocket,
    deadline: &mut Pin<Box<time::Sleep>>,
    cx: &mut Context<'_>,
) -> Poll<Result<bool, ClientError>> {
    match socket.poll_send_ready(cx) {
        Poll::Ready(Ok(())) => Poll::Ready(Ok(true)),
        Poll::Ready(Err(error)) => Poll::Ready(Err(ClientError::Socket(error))),
        Poll::Pending => match deadline.as_mut().poll(cx) {
            Poll::Ready(()) => Poll::Ready(Ok(false)),
            Poll::Pending => Poll::Pending,
        },
    }
}

fn poll_readable_until(
    socket: &tokio::net::UdpSocket,
    deadline: &mut Pin<Box<time::Sleep>>,
    cx: &mut Context<'_>,
) -> Poll<Result<bool, ClientError>> {
    match socket.poll_recv_ready(cx) {
        Poll::Ready(Ok(())) => Poll::Ready(Ok(true)),
        Poll::Ready(Err(error)) => Poll::Ready(Err(ClientError::Socket(error))),
        Poll::Pending => match deadline.as_mut().poll(cx) {
            Poll::Ready(()) => Poll::Ready(Ok(false)),
            Poll::Pending => Poll::Pending,
        },
    }
}

#[cfg(test)]
mod tests;
