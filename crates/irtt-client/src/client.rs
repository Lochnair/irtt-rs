use std::{
    io,
    net::{SocketAddr, UdpSocket},
    time::{Duration, Instant},
};

use crate::{
    config::{ClientConfig, RecvBudget},
    error::ClientError,
    event::{ClientEvent, OpenOutcome},
    receive::{drain_tx_timestamps, recv_datagram, try_enable_tx_timestamping, ReceivedDatagram},
    session::machine::{
        recv_buffer_size, OpenDatagramDisposition, PreparedOpenAcceptance, ProbeSent,
        SessionMachine, MAX_OPEN_PACKET_SIZE,
    },
    socket::{connect_udp_socket, resolve_remote},
    socket_options::{apply_traffic_class_to_socket, clear_dscp_on_socket},
    timing::ClientTimestamp,
};

#[derive(Debug)]
struct PreparedClientOpen {
    machine: PreparedOpenAcceptance,
    recv_buffer_len: Option<usize>,
    negotiated_traffic_class: Option<u8>,
    previous_traffic_class: Option<u8>,
    post_open_recv_timeout: Option<Duration>,
}

#[derive(Debug)]
struct PreparedClientOpenFailure {
    primary: ClientError,
    machine: PreparedOpenAcceptance,
}

/// Low-level synchronous IRTT client.
///
/// `Client` exposes the protocol steps directly: connect a UDP socket, open a
/// session, send probes, receive replies, poll timeouts, and close. Callers
/// that do not need to own this loop can use the unified managed API in
/// [`crate::managed`] when the `tokio` feature is enabled.
///
/// # Example
///
/// `Client` is the runtime-free adapter. The caller drives receives and timeout
/// polling between sends; see the standalone example for a complete loop.
/// This example type-checks without contacting a server:
///
/// ```no_run
/// use irtt_client::{Client, ClientConfig};
///
/// # fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let mut client = Client::connect("127.0.0.1:2112", ClientConfig::default())?;
/// let outcome = client.open()?;
/// println!("opened: {outcome:?}");
/// let sent = client.send_probe()?;
/// println!("sent: {sent:?}");
/// let received = client.recv_once()?;
/// println!("received: {received:?}");
/// let timed_out = client.poll_timeouts()?;
/// println!("timeouts: {timed_out:?}");
/// let closed = client.close()?;
/// println!("closed: {closed:?}");
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Client {
    runtime: SessionMachine,
    socket: UdpSocket,
    remote: SocketAddr,
    recv_buffer: Vec<u8>,
    recv_timeout: Option<Duration>,
    applied_traffic_class: Option<u8>,
    /// Whether the socket's `SO_TIMESTAMPING` flags carry `TX_SOFTWARE` +
    /// `OPT_ID` + `OPT_TSONLY`. Always `false` off Linux, without the
    /// `ancillary` feature, or before the best-effort post-Open upgrade;
    /// tracked here (adapter-owned capability, not protocol state) so
    /// unnecessary error-queue drains are skipped once it is known there is
    /// nothing to drain.
    tx_timestamping_enabled: bool,
}

impl Client {
    /// Resolve the endpoint and create a connected UDP socket.
    ///
    /// This validates local configuration and prepares the open request, but it
    /// does not contact the server. Call [`open`](Self::open) to perform the
    /// IRTT open exchange.
    ///
    /// The endpoint accepts a name or address; an omitted port defaults to 2112.
    /// IPv6 literals may be bracketed or unbracketed with the default port.
    /// The reusable config contains no endpoint identity.
    pub fn connect(endpoint: impl AsRef<str>, config: ClientConfig) -> Result<Self, ClientError> {
        config.open.validate()?;
        let remote = resolve_remote(endpoint.as_ref(), config.address_family)?;
        let runtime = SessionMachine::new(config.clone(), remote)?;
        let socket = connect_udp_socket(&config.socket, remote, config.address_family)?;

        Ok(Self {
            runtime,
            socket,
            remote,
            recv_buffer: vec![0_u8; recv_buffer_size(false, None)?],
            recv_timeout: None,
            applied_traffic_class: None,
            tx_timestamping_enabled: false,
        })
    }

    /// Set the blocking receive timeout used outside open attempts.
    ///
    /// `None` (the default) lets `recv_once()` block indefinitely. A nonzero
    /// timeout bounds one logical receive, including retries after EINTR.
    /// `recv_available()` retains its bounded drain behavior. Open attempts
    /// use only [`OpenPolicy::timeouts`](crate::OpenPolicy::timeouts) and
    /// restore this setting on success or failure.
    ///
    /// A zero duration is rejected by the socket; the previous setting is
    /// retained when applying the new timeout fails.
    pub fn set_recv_timeout(&mut self, timeout: Option<Duration>) -> Result<(), ClientError> {
        self.socket.set_read_timeout(timeout)?;
        self.recv_timeout = timeout;
        Ok(())
    }

    /// Perform the IRTT open exchange.
    ///
    /// On success, returns the negotiated open outcome and transitions the
    /// client into either an open probe session or completed no-test state.
    /// Open attempts use [`OpenPolicy::timeouts`](crate::OpenPolicy::timeouts). Malformed or
    /// unrelated datagrams are ignored until the current attempt's absolute
    /// deadline, so one attempt may consume several datagrams without
    /// retransmitting. Silence or ignored traffic eventually produces
    /// [`ClientError::OpenTimeout`]. A structurally recognizable malformed or
    /// incompatible Open reply from the connected peer is terminal; when HMAC
    /// is configured, that recognition additionally requires authentication.
    ///
    /// When a trusted reply allocates a token but later negotiation or socket
    /// preparation fails, the client sends a best-effort cleanup close and
    /// preserves the original failure. Failed opening never leaves the session
    /// machine open.
    pub fn open(&mut self) -> Result<OpenOutcome, ClientError> {
        let result = self.open_transaction();
        if result.is_err() {
            let _ = self.socket.set_read_timeout(self.recv_timeout);
        }
        result
    }

    /// Send a close request and emit a [`ClientEvent::SessionClosed`] event.
    ///
    /// The close event means the client has sent its close packet and stopped
    /// tracking the session locally; it is not a server acknowledgement. If the
    /// send fails, the negotiated DSCP is restored best-effort and the session
    /// remains open.
    pub fn close(&mut self) -> Result<Vec<ClientEvent>, ClientError> {
        let prepared = self.runtime.prepare_close()?;
        let mut events = Vec::new();

        events
            .try_reserve(1)
            .map_err(|source| ClientError::AllocationFailed {
                operation: "close event result",
                source,
            })?;
        let previous_traffic_class = self.applied_traffic_class;
        let expected_bytes = prepared.bytes.len();

        self.clear_close_dscp()?;
        let bytes = match self.socket.send(prepared.bytes) {
            Ok(bytes) => bytes,
            Err(err) => {
                self.restore_dscp_best_effort(previous_traffic_class);
                return Err(ClientError::Socket(err));
            }
        };
        let sent_at = ClientTimestamp::now();

        let event = self.runtime.commit_local_close(prepared.commit, sent_at);

        self.applied_traffic_class = None;
        validate_datagram_length(expected_bytes, bytes)?;
        events.push(event);
        Ok(events)
    }

    /// Return the local timeout used to classify pending probes as lost.
    pub fn probe_timeout(&self) -> Duration {
        self.runtime.probe_timeout()
    }

    /// Send one echo probe now. The caller owns pacing and run duration.
    ///
    /// The negotiated interval and duration do not gate low-level sends.
    pub fn send_probe(&mut self) -> Result<Vec<ClientEvent>, ClientError> {
        let remote = self.remote;
        let (runtime, socket) = (&mut self.runtime, &self.socket);
        let prepared = runtime.prepare_probe()?;
        let machine_preflight = runtime.preflight_probe_commit(&prepared)?;
        let mut events = Vec::new();
        events
            .try_reserve(1)
            .map_err(|source| ClientError::AllocationFailed {
                operation: "probe event result",
                source,
            })?;
        let expected_bytes = prepared.bytes.len();

        // All fallible probe-commit preflight (timeout deadline arithmetic,
        // capacity/collision checks already done above) is finalized from
        // this PRIVATE pre-send anchor, before the socket send. It is not
        // the public measurement `sent_at` captured further below.
        let send_anchor = ClientTimestamp::now();
        let machine_commit = runtime.finalize_probe_commit(machine_preflight, send_anchor)?;
        let send_call_start = Instant::now();

        let bytes = match socket.send(&prepared.bytes) {
            Ok(bytes) => bytes,
            Err(error) => {
                runtime.invalidate_kernel_tx_correlation();
                return Err(ClientError::Socket(error));
            }
        };
        let send_finished_at = Instant::now();

        // The public measurement timestamp: paired wall/monotonic sample
        // captured immediately after the successful socket send completed.
        // This, not `send_anchor` above, is the RTT/OWD-fallback/timer_error
        // endpoint. The infallible machine commit below never fails.
        let sent_at = ClientTimestamp::now();
        let sent = runtime.commit_probe_sent(machine_commit, sent_at, bytes);
        let send_call = send_finished_at.saturating_duration_since(send_call_start);
        validate_datagram_length(expected_bytes, bytes)?;
        self.drain_tx_timestamps()?;

        events.push(echo_sent_event(remote, sent, send_call));
        Ok(events)
    }

    /// Receive and classify at most one datagram from the socket.
    ///
    /// Returns an empty event list when the socket read would block or times
    /// out. Malformed or unrelated datagrams are reported as warning events.
    /// Requires an open session; lifecycle errors are returned before socket I/O.
    pub fn recv_once(&mut self) -> Result<Vec<ClientEvent>, ClientError> {
        self.runtime.ensure_open()?;
        self.socket.set_read_timeout(self.recv_timeout)?;
        self.recv_once_inner()
    }

    fn recv_once_inner(&mut self) -> Result<Vec<ClientEvent>, ClientError> {
        let Some(datagram) = self.recv_datagram_retrying_interrupted()? else {
            return Ok(vec![]);
        };

        // One final bounded drain before processing the reply, so a TX
        // timestamp that raced the reply is still associated with its probe
        // before that probe is looked up and possibly removed below.
        self.drain_tx_timestamps()?;

        let events = self.runtime.process_received_echo_packet(
            &self.recv_buffer[..datagram.len],
            datagram.received_at,
            datagram.meta,
        )?;
        if self.runtime.is_peer_closed() && self.clear_close_dscp().is_ok() {
            self.applied_traffic_class = None;
        }
        Ok(events)
    }

    /// Receive one datagram, transparently retrying an interrupted syscall
    /// (`io::ErrorKind::Interrupted`, e.g. `EINTR`) without treating it as a
    /// genuine socket error, timeout, or empty receive.
    ///
    /// One logical receive keeps one timeout budget: when a receive timeout
    /// is configured, an absolute deadline is computed once here and each
    /// retry after an interruption restores the socket's remaining time
    /// against that same deadline, so repeated interruptions cannot extend
    /// this receive past its configured timeout. An unconfigured timeout
    /// (`None`) keeps blocking indefinitely after an interruption, matching
    /// its no-timeout contract. The socket's read timeout is restored to the
    /// caller-configured value before returning, so a later call in the same
    /// `recv_available` budget is unaffected by an earlier interruption here.
    fn recv_datagram_retrying_interrupted(
        &mut self,
    ) -> Result<Option<ReceivedDatagram>, ClientError> {
        let configured_timeout = self.recv_timeout;
        let deadline = match configured_timeout {
            Some(timeout) => Some(
                Instant::now()
                    .checked_add(timeout)
                    .ok_or(ClientError::DurationOverflow)?,
            ),
            None => None,
        };
        let mut timeout_narrowed = false;

        let result = loop {
            match recv_datagram(&self.socket, &mut self.recv_buffer) {
                Ok(datagram) => break Ok(Some(datagram)),
                Err(err)
                    if matches!(
                        err.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                    ) =>
                {
                    break Ok(None);
                }
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {
                    if let Some(deadline) = deadline {
                        let Some(remaining) = deadline.checked_duration_since(Instant::now())
                        else {
                            break Ok(None);
                        };
                        if remaining.is_zero() {
                            break Ok(None);
                        }
                        if let Err(err) = self.socket.set_read_timeout(Some(remaining)) {
                            break Err(ClientError::Socket(err));
                        }
                        timeout_narrowed = true;
                    }
                }
                Err(err) => break Err(ClientError::Socket(err)),
            }
        };

        if timeout_narrowed {
            if let Err(err) = self.socket.set_read_timeout(configured_timeout) {
                return Err(ClientError::Socket(err));
            }
        }
        result
    }

    /// Receive and classify datagrams until a receive produces no events or the
    /// receive budget is exhausted.
    ///
    /// Requires an open session; lifecycle errors are returned before socket I/O.
    pub fn recv_available(&mut self, budget: RecvBudget) -> Result<Vec<ClientEvent>, ClientError> {
        self.runtime.ensure_open()?;
        self.socket.set_read_timeout(self.recv_timeout)?;
        let mut all_events = Vec::new();
        for _ in 0..budget.max_packets {
            let events = self.recv_once_inner()?;
            if events.is_empty() {
                break;
            }
            all_events.extend(events);
            if self.is_peer_closed() {
                break;
            }
        }
        Ok(all_events)
    }

    /// Polls for probes that have timed out as of the current monotonic time.
    pub fn poll_timeouts(&mut self) -> Result<Vec<ClientEvent>, ClientError> {
        self.poll_timeouts_at(Instant::now())
    }

    /// Polls for probes that have timed out as of `now`.
    ///
    /// This is useful for callers that drive `Client` from their own event loop and
    /// want timeout decisions to use the same sampled `Instant` as their scheduling
    /// logic.
    ///
    /// `now` is monotonic time only; wall-clock time is not used for timeout expiry.
    pub fn poll_timeouts_at(&mut self, now: Instant) -> Result<Vec<ClientEvent>, ClientError> {
        self.drain_tx_timestamps()?;
        self.runtime.poll_timeouts_at(now)
    }

    /// Accepted semantics and exact peer parameters of the currently live session.
    ///
    /// Returns `None` before Open, after Close, and after no-test completion.
    pub fn negotiation(&self) -> Option<&crate::NegotiationResult> {
        self.runtime.negotiation()
    }

    /// Earliest timeout deadline among probes still awaiting a reply.
    pub fn next_probe_timeout_deadline(&self) -> Option<Instant> {
        self.runtime.next_probe_timeout_deadline()
    }

    /// Whether any sent probes still await a reply or timeout.
    pub fn has_pending_probes(&self) -> bool {
        !self.runtime.pending_is_empty()
    }

    /// Return whether the session was closed by a peer close-flagged reply.
    ///
    /// Direct operations on a closed client still return
    /// [`ClientError::AlreadyClosed`]. This method lets higher-level run loops
    /// avoid treating a successfully observed peer close as a local cleanup
    /// failure.
    pub fn is_peer_closed(&self) -> bool {
        self.runtime.is_peer_closed()
    }

    fn open_transaction(&mut self) -> Result<OpenOutcome, ClientError> {
        let request = self.runtime.prepare_open_request()?;
        let mut buf = [0_u8; MAX_OPEN_PACKET_SIZE];
        let attempt_count = self.runtime.config().open.timeouts.len();

        for attempt in 0..attempt_count {
            let timeout = self.runtime.config().open.timeouts[attempt];
            let deadline = Instant::now()
                .checked_add(timeout)
                .ok_or(ClientError::DurationOverflow)?;
            self.socket.set_read_timeout(Some(timeout))?;
            self.socket.send(&request.bytes)?;

            loop {
                let now = Instant::now();
                let Some(remaining) = deadline.checked_duration_since(now) else {
                    break;
                };
                if remaining.is_zero() {
                    break;
                }
                self.socket.set_read_timeout(Some(remaining))?;

                let datagram = match recv_datagram(&self.socket, &mut buf) {
                    Ok(datagram) => datagram,
                    // WouldBlock/TimedOut mean the attempt's own deadline
                    // elapsed; Interrupted (EINTR) means the syscall was
                    // merely interrupted and carries no timing information of
                    // its own. Both retry through the same loop, which
                    // recomputes `remaining` and re-applies it as the read
                    // timeout on every iteration — so an interruption resumes
                    // within this same absolute deadline rather than
                    // resetting or extending it.
                    Err(err)
                        if matches!(
                            err.kind(),
                            io::ErrorKind::WouldBlock
                                | io::ErrorKind::TimedOut
                                | io::ErrorKind::Interrupted
                        ) =>
                    {
                        if Instant::now() >= deadline {
                            break;
                        }
                        continue;
                    }
                    Err(err) => return Err(ClientError::Socket(err)),
                };
                if datagram.received_at.mono > deadline {
                    break;
                }

                let reply = match self.runtime.inspect_open_datagram(&buf[..datagram.len])? {
                    OpenDatagramDisposition::Ignore => continue,
                    OpenDatagramDisposition::Trusted(reply) => reply,
                };
                let machine = match self
                    .runtime
                    .prepare_open_acceptance(reply, datagram.received_at)
                {
                    Ok(machine) => machine,
                    Err(failure) => {
                        self.send_cleanup_close_best_effort(failure.cleanup_close.as_deref());
                        return Err(failure.primary);
                    }
                };
                let prepared = match self.prepare_client_open(machine) {
                    Ok(prepared) => prepared,
                    Err(failure) => {
                        self.send_cleanup_close_best_effort(failure.machine.cleanup_close_packet());
                        return Err(failure.primary);
                    }
                };
                return self.apply_prepared_open(prepared);
            }
        }

        Err(ClientError::OpenTimeout)
    }

    fn prepare_client_open(
        &self,
        machine: PreparedOpenAcceptance,
    ) -> Result<PreparedClientOpen, Box<PreparedClientOpenFailure>> {
        let Some(negotiated) = machine.normal_negotiated() else {
            return Ok(PreparedClientOpen {
                machine,
                recv_buffer_len: None,
                negotiated_traffic_class: None,
                previous_traffic_class: self.applied_traffic_class,
                post_open_recv_timeout: self.recv_timeout,
            });
        };
        let recv_buffer_len = match recv_buffer_size(self.runtime.has_hmac(), Some(negotiated)) {
            Ok(size) => size,
            Err(primary) => return Err(Box::new(PreparedClientOpenFailure { primary, machine })),
        };

        let negotiated_traffic_class = negotiated.accepted.dscp << 2;
        Ok(PreparedClientOpen {
            machine,
            recv_buffer_len: Some(recv_buffer_len),
            negotiated_traffic_class: Some(negotiated_traffic_class),
            previous_traffic_class: self.applied_traffic_class,
            post_open_recv_timeout: self.recv_timeout,
        })
    }

    fn apply_prepared_open(
        &mut self,
        prepared: PreparedClientOpen,
    ) -> Result<OpenOutcome, ClientError> {
        let PreparedClientOpen {
            machine,
            recv_buffer_len,
            negotiated_traffic_class,
            previous_traffic_class,
            post_open_recv_timeout,
        } = prepared;

        if let (Some(recv_buffer_len), Some(negotiated_traffic_class)) =
            (recv_buffer_len, negotiated_traffic_class)
        {
            let previous_len = self.recv_buffer.len();
            let additional = recv_buffer_len.saturating_sub(previous_len);
            if let Err(source) = self.recv_buffer.try_reserve(additional) {
                self.send_cleanup_close_best_effort(machine.cleanup_close_packet());
                return Err(ClientError::AllocationFailed {
                    operation: "negotiated receive buffer",
                    source,
                });
            }
            self.recv_buffer.resize(recv_buffer_len, 0);

            if let Err(primary) =
                apply_traffic_class_to_socket(&self.socket, self.remote, negotiated_traffic_class)
            {
                self.recv_buffer.truncate(previous_len);
                self.restore_dscp_best_effort(previous_traffic_class);
                self.send_cleanup_close_best_effort(machine.cleanup_close_packet());
                return Err(primary);
            }
            if let Err(source) = self.socket.set_read_timeout(post_open_recv_timeout) {
                let primary = ClientError::ReadTimeoutRestore { source };
                self.recv_buffer.truncate(previous_len);
                self.restore_dscp_best_effort(previous_traffic_class);
                self.send_cleanup_close_best_effort(machine.cleanup_close_packet());
                return Err(primary);
            }

            let outcome = self.runtime.commit_open(machine);

            self.applied_traffic_class = Some(negotiated_traffic_class);
            self.tx_timestamping_enabled = try_enable_tx_timestamping(&self.socket);
            Ok(outcome)
        } else {
            if let Err(source) = self.socket.set_read_timeout(post_open_recv_timeout) {
                return Err(ClientError::ReadTimeoutRestore { source });
            }
            let outcome = self.runtime.commit_open(machine);

            self.applied_traffic_class = None;
            self.tx_timestamping_enabled = try_enable_tx_timestamping(&self.socket);
            Ok(outcome)
        }
    }

    /// Best-effort, bounded, nonblocking drain of the socket's
    /// `MSG_ERRQUEUE`. A no-op unless [`Self::apply_prepared_open`]
    /// successfully upgraded the socket to TX timestamping. Any TX
    /// timestamp found is handed to the session machine to attach to its
    /// matching probe; a genuine socket/network error found on the queue is
    /// surfaced through the normal socket-error path.
    fn drain_tx_timestamps(&mut self) -> Result<(), ClientError> {
        if !self.tx_timestamping_enabled {
            return Ok(());
        }
        let runtime = &mut self.runtime;
        drain_tx_timestamps(&self.socket, |id, timestamp| {
            runtime.record_kernel_tx_timestamp(id, timestamp);
        })
        .map_err(ClientError::Socket)
    }

    fn restore_dscp_best_effort(&self, previous_traffic_class: Option<u8>) {
        let _ = match previous_traffic_class {
            Some(traffic_class) => {
                apply_traffic_class_to_socket(&self.socket, self.remote, traffic_class)
            }
            None => clear_dscp_on_socket(&self.socket, self.remote),
        };
    }

    fn clear_close_dscp(&self) -> Result<(), ClientError> {
        clear_dscp_on_socket(&self.socket, self.remote)
    }

    fn send_cleanup_close_best_effort(&self, packet: Option<&[u8]>) {
        if let Some(packet) = packet {
            let _ = self.socket.send(packet);
        }
    }
}

pub(crate) fn echo_sent_event(
    remote: SocketAddr,
    sent: ProbeSent,
    send_call: Duration,
) -> ClientEvent {
    ClientEvent::EchoSent {
        seq: sent.seq,
        remote,
        scheduled_at: None,
        sent_at: sent.sent_at,
        bytes: sent.bytes,
        send_call,
        timer_error: None,
    }
}

pub(crate) fn validate_datagram_length(expected: usize, actual: usize) -> Result<(), ClientError> {
    if actual == expected {
        Ok(())
    } else {
        Err(ClientError::DatagramLengthMismatch { expected, actual })
    }
}
