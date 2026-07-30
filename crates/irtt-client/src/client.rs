use std::{
    io,
    net::{SocketAddr, UdpSocket},
    time::{Duration, Instant},
};

#[cfg(test)]
use std::cell::Cell;

#[cfg(test)]
use irtt_proto::{flags, Params, TimestampFields, PROTOCOL_VERSION};

use crate::{
    config::{ClientConfig, RecvBudget},
    error::ClientError,
    event::{ClientEvent, OpenOutcome},
    receive::recv_datagram,
    session::machine::{
        recv_buffer_size, OpenDatagramDisposition, PreparedOpenAcceptance, ProbeSent,
        SessionMachine, MAX_OPEN_PACKET_SIZE,
    },
    socket::{connect_udp_socket, resolve_remote, validate_open_timeouts},
    socket_options::{apply_dscp_to_socket, clear_dscp_on_socket},
    timing::ClientTimestamp,
};

pub(crate) mod schedule;

use schedule::ProbeSchedule;

#[derive(Debug, Clone, Copy)]
enum ProbeScheduleMode {
    CallerPaced,
    #[cfg(any(test, not(feature = "tokio")))]
    Managed {
        scheduled_at: Instant,
    },
}

#[derive(Debug)]
struct PreparedClientOpen {
    machine: PreparedOpenAcceptance,
    schedule: Option<ProbeSchedule>,
    recv_buffer_len: Option<usize>,
    negotiated_dscp: Option<u8>,
    previous_dscp: Option<u8>,
    post_open_recv_timeout: Option<Duration>,
}

#[derive(Debug)]
struct PreparedClientOpenFailure {
    primary: ClientError,
    machine: PreparedOpenAcceptance,
}

#[cfg(test)]
#[derive(Debug, Default)]
struct ClientTestHooks {
    fail_open_dscp: Cell<bool>,
    fail_open_timeout_restore: Cell<bool>,
    fail_close_send: Cell<bool>,
    close_reported_len: Cell<Option<usize>>,
    fail_cleanup_send: Cell<bool>,
    recv_buffer_len_override: Cell<Option<usize>>,
    fail_dscp_restore: Cell<bool>,
    last_restored_read_timeout: Cell<Option<Duration>>,
    prepared_active_session_before_dscp: Cell<bool>,
    probe_reported_len: Cell<Option<usize>>,
}

#[cfg(test)]
use crate::{
    session::machine::{
        compute_one_way, compute_rtt, params_from_config, sequence_is_after, sequence_is_before,
        unix_epoch_ns_i64, update_highest_received,
    },
    session::negotiate_params,
    NegotiatedParams, RunMode, SignedDuration, WarningKind, MAX_UDP_PAYLOAD_LENGTH,
};

/// Low-level synchronous IRTT client.
///
/// `Client` exposes the protocol steps directly: connect a UDP socket, open a
/// session, send probes, receive replies, poll timeouts, and close. Callers
/// that do not need to own this loop can use [`ManagedClient`](crate::ManagedClient)
/// instead.
#[derive(Debug)]
pub struct Client {
    runtime: SessionMachine,
    schedule: Option<ProbeSchedule>,
    socket: UdpSocket,
    remote: SocketAddr,
    recv_buffer: Vec<u8>,
    applied_dscp: Option<u8>,
    #[cfg(test)]
    test_hooks: ClientTestHooks,
}

impl Client {
    /// Resolve the configured server and create a connected UDP socket.
    ///
    /// This validates local configuration and prepares the open request, but it
    /// does not contact the server. Call [`open`](Self::open) to perform the
    /// IRTT open exchange.
    pub fn connect(config: ClientConfig) -> Result<Self, ClientError> {
        validate_open_timeouts(&config.open_timeouts)?;
        let remote = resolve_remote(&config)?;
        let runtime = SessionMachine::new(config.clone(), remote)?;
        let socket = connect_udp_socket(&config.socket_config, remote)?;

        Ok(Self {
            runtime,
            schedule: None,
            socket,
            remote,
            recv_buffer: vec![0_u8; recv_buffer_size(false, None)?],
            applied_dscp: None,
            #[cfg(test)]
            test_hooks: ClientTestHooks::default(),
        })
    }

    /// Perform the IRTT open exchange.
    ///
    /// On success, returns the negotiated open outcome and transitions the
    /// client into either an open probe session or completed no-test state.
    /// Open attempts use [`ClientConfig::open_timeouts`]. Malformed, unrelated,
    /// or unauthenticated datagrams are ignored until the current attempt's
    /// absolute deadline, so one attempt may consume several datagrams without
    /// retransmitting. Silence or ignored traffic eventually produces
    /// [`ClientError::OpenTimeout`], while authenticated incompatibility remains
    /// terminal.
    ///
    /// When a trusted reply allocates a token but later negotiation or socket
    /// preparation fails, the client sends a best-effort cleanup close and
    /// preserves the original failure. Failed opening never leaves the session
    /// machine open.
    pub fn open(&mut self) -> Result<OpenOutcome, ClientError> {
        let result = self.open_transaction();
        if result.is_err() {
            let _ =
                self.restore_open_read_timeout(self.runtime.config().socket_config.recv_timeout);
        }
        result
    }

    /// Send a close request and emit a [`ClientEvent::SessionClosed`] event.
    ///
    /// The close event means the client has sent its close packet and stopped
    /// tracking the session locally; it is not a server acknowledgement. If the
    /// send fails, the negotiated DSCP is restored best-effort and the session
    /// and probe schedule remain open.
    pub fn close(&mut self) -> Result<Vec<ClientEvent>, ClientError> {
        let prepared = self.runtime.prepare_close()?;
        let mut events = Vec::new();
        events
            .try_reserve(1)
            .map_err(|source| ClientError::AllocationFailed {
                operation: "close event result",
                source,
            })?;
        let previous_dscp = self.applied_dscp;
        let expected_bytes = prepared.bytes.len();

        clear_dscp_on_socket(&self.socket, self.remote)?;
        let bytes = match self.send_close_datagram(prepared.bytes) {
            Ok(bytes) => bytes,
            Err(err) => {
                self.restore_dscp_best_effort(previous_dscp);
                return Err(ClientError::Socket(err));
            }
        };
        let event = self.runtime.commit_local_close(prepared.commit);
        self.schedule = None;
        self.applied_dscp = None;

        validate_datagram_length(expected_bytes, bytes)?;
        events.push(event);
        Ok(events)
    }

    /// Return the monotonic deadline for the next probe send, if another probe
    /// is scheduled.
    pub fn next_send_deadline(&self) -> Option<Instant> {
        if !self.runtime.is_open() {
            return None;
        }
        self.schedule.as_ref()?.next_send_deadline()
    }

    /// Return the local timeout used to classify pending probes as lost.
    pub fn probe_timeout(&self) -> Duration {
        self.runtime.probe_timeout()
    }

    /// Send one echo probe if the negotiated run is still active.
    ///
    /// Returns an `EchoSent` event when a probe is sent. Returns an empty event
    /// list when the run duration has elapsed and no further probe should be
    /// sent.
    pub fn send_probe(&mut self) -> Result<Vec<ClientEvent>, ClientError> {
        self.send_probe_inner(None)
    }

    /// Receive and classify at most one datagram from the socket.
    ///
    /// Returns an empty event list when the socket read would block or times
    /// out. Malformed or unrelated datagrams are reported as warning events.
    pub fn recv_once(&mut self) -> Result<Vec<ClientEvent>, ClientError> {
        self.socket
            .set_read_timeout(self.runtime.config().socket_config.recv_timeout)?;
        self.recv_once_inner()
    }

    #[cfg(not(feature = "tokio"))]
    pub(crate) fn recv_once_until(
        &mut self,
        deadline: Option<Instant>,
        max_wait: Duration,
    ) -> Result<Vec<ClientEvent>, ClientError> {
        let Some(timeout) = bounded_receive_timeout(
            deadline,
            self.runtime.config().socket_config.recv_timeout,
            max_wait,
            Instant::now(),
        ) else {
            return Ok(Vec::new());
        };
        self.socket.set_read_timeout(Some(timeout))?;
        self.recv_once_inner()
    }

    fn recv_once_inner(&mut self) -> Result<Vec<ClientEvent>, ClientError> {
        let datagram = match recv_datagram(&self.socket, &mut self.recv_buffer) {
            Ok(datagram) => datagram,
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                return Ok(vec![]);
            }
            Err(err) => return Err(ClientError::Socket(err)),
        };

        let events = self.runtime.process_received_echo_packet(
            &self.recv_buffer[..datagram.len],
            datagram.received_at,
            datagram.meta,
        )?;
        if self.runtime.is_peer_closed() {
            self.schedule = None;
            if clear_dscp_on_socket(&self.socket, self.remote).is_ok() {
                self.applied_dscp = None;
            }
        }
        Ok(events)
    }

    /// Receive and classify datagrams until a receive produces no events or the
    /// receive budget is exhausted.
    pub fn recv_available(&mut self, budget: RecvBudget) -> Result<Vec<ClientEvent>, ClientError> {
        self.socket
            .set_read_timeout(self.runtime.config().socket_config.recv_timeout)?;
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
        self.runtime.poll_timeouts_at(now)
    }

    /// Return whether the current run has completed.
    ///
    /// A normal run is complete once no more probes will be sent and all
    /// pending probes have either replied or timed out. No-test and closed
    /// sessions are also considered complete.
    pub fn is_run_complete(&self) -> bool {
        self.runtime.is_terminal()
            || self
                .schedule
                .as_ref()
                .is_some_and(|schedule| schedule.is_finished() && self.runtime.pending_is_empty())
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

    #[cfg(not(feature = "tokio"))]
    pub(crate) fn has_timed_out_metadata(&self) -> bool {
        self.runtime.has_timed_out_metadata()
    }

    #[cfg(not(feature = "tokio"))]
    pub(crate) fn packets_sent(&self) -> u64 {
        self.runtime.packets_sent()
    }

    #[cfg(not(feature = "tokio"))]
    pub(crate) fn send_managed_probe(
        &mut self,
        scheduled_at: Instant,
    ) -> Result<Vec<ClientEvent>, ClientError> {
        self.send_managed_probe_inner(scheduled_at, None)
    }

    fn send_probe_inner(
        &mut self,
        override_ts: Option<ClientTimestamp>,
    ) -> Result<Vec<ClientEvent>, ClientError> {
        self.send_probe_transaction(override_ts, ProbeScheduleMode::CallerPaced)
    }

    #[cfg(any(test, not(feature = "tokio")))]
    fn send_managed_probe_inner(
        &mut self,
        scheduled_at: Instant,
        override_ts: Option<ClientTimestamp>,
    ) -> Result<Vec<ClientEvent>, ClientError> {
        self.send_probe_transaction(override_ts, ProbeScheduleMode::Managed { scheduled_at })
    }

    fn send_probe_transaction(
        &mut self,
        override_ts: Option<ClientTimestamp>,
        mode: ProbeScheduleMode,
    ) -> Result<Vec<ClientEvent>, ClientError> {
        self.runtime.ensure_open()?;
        let sent_at = override_ts.unwrap_or_else(ClientTimestamp::now);
        let remote = self.remote;
        let (runtime, schedule, socket) = (&mut self.runtime, &mut self.schedule, &self.socket);
        let schedule = schedule
            .as_mut()
            .expect("open sessions always have a probe schedule");
        if !schedule.permit_probe_at(sent_at.mono) {
            return Ok(Vec::new());
        }

        let Some(prepared) = runtime.prepare_probe()? else {
            return Ok(Vec::new());
        };
        let machine_commit = runtime.preflight_probe_commit(&prepared, sent_at)?;
        let schedule_commit = match mode {
            ProbeScheduleMode::CallerPaced => {
                schedule.preflight_caller_commit(sent_at.mono, machine_commit.next_packets_sent)?
            }
            #[cfg(any(test, not(feature = "tokio")))]
            ProbeScheduleMode::Managed { scheduled_at } => {
                schedule.preflight_managed_commit(scheduled_at, sent_at.mono)?
            }
        };
        let mut events = Vec::new();
        events
            .try_reserve(1)
            .map_err(|source| ClientError::AllocationFailed {
                operation: "probe event result",
                source,
            })?;

        let expected_bytes = prepared.bytes.len();
        let scheduled_at = schedule_commit.scheduled_at;
        let timer_error = schedule_commit.timer_error;
        #[cfg(test)]
        let reported_bytes = self.test_hooks.probe_reported_len.take();
        let send_call_start = Instant::now();
        let bytes = socket.send(&prepared.bytes)?;
        let sent = runtime.commit_probe_sent(machine_commit, bytes);
        schedule.commit(schedule_commit);

        let send_call = send_call_start.elapsed();
        #[cfg(test)]
        let bytes = reported_bytes.unwrap_or(bytes);
        validate_datagram_length(expected_bytes, bytes)?;

        events.push(echo_sent_event(
            remote,
            sent,
            send_call,
            scheduled_at,
            timer_error,
        ));
        Ok(events)
    }

    fn open_transaction(&mut self) -> Result<OpenOutcome, ClientError> {
        let request = self.runtime.prepare_open_request()?;
        let mut buf = [0_u8; MAX_OPEN_PACKET_SIZE];
        let attempt_count = self.runtime.config().open_timeouts.len();

        for attempt in 0..attempt_count {
            let timeout = self.runtime.config().open_timeouts[attempt];
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
                    Err(err)
                        if matches!(
                            err.kind(),
                            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
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
                let prepared = match self.prepare_client_open(machine, datagram.received_at) {
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
        opened_at: ClientTimestamp,
    ) -> Result<PreparedClientOpen, Box<PreparedClientOpenFailure>> {
        let Some(negotiated) = machine.normal_negotiated() else {
            return Ok(PreparedClientOpen {
                machine,
                schedule: None,
                recv_buffer_len: None,
                negotiated_dscp: None,
                previous_dscp: self.applied_dscp,
                post_open_recv_timeout: self.runtime.config().socket_config.recv_timeout,
            });
        };
        let schedule = match ProbeSchedule::new(opened_at.mono, negotiated) {
            Ok(schedule) => schedule,
            Err(primary) => return Err(Box::new(PreparedClientOpenFailure { primary, machine })),
        };
        let recv_buffer_len = match recv_buffer_size(self.runtime.has_hmac(), Some(negotiated)) {
            Ok(size) => size,
            Err(primary) => return Err(Box::new(PreparedClientOpenFailure { primary, machine })),
        };
        #[cfg(test)]
        let recv_buffer_len = self
            .test_hooks
            .recv_buffer_len_override
            .get()
            .unwrap_or(recv_buffer_len);
        let negotiated_dscp = match u8::try_from(negotiated.params.dscp) {
            Ok(dscp) => dscp,
            Err(_) => {
                return Err(Box::new(PreparedClientOpenFailure {
                    primary: ClientError::InvalidConfig {
                        reason: "negotiated dscp must be in range 0..=63".to_owned(),
                    },
                    machine,
                }));
            }
        };

        Ok(PreparedClientOpen {
            machine,
            schedule: Some(schedule),
            recv_buffer_len: Some(recv_buffer_len),
            negotiated_dscp: Some(negotiated_dscp),
            previous_dscp: self.applied_dscp,
            post_open_recv_timeout: self.runtime.config().socket_config.recv_timeout,
        })
    }

    fn apply_prepared_open(
        &mut self,
        prepared: PreparedClientOpen,
    ) -> Result<OpenOutcome, ClientError> {
        let PreparedClientOpen {
            machine,
            schedule,
            recv_buffer_len,
            negotiated_dscp,
            previous_dscp,
            post_open_recv_timeout,
        } = prepared;

        if let (Some(recv_buffer_len), Some(negotiated_dscp)) = (recv_buffer_len, negotiated_dscp) {
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

            #[cfg(test)]
            self.test_hooks
                .prepared_active_session_before_dscp
                .set(machine.has_prepared_active_session());
            if let Err(primary) = self.apply_open_dscp(negotiated_dscp) {
                self.recv_buffer.truncate(previous_len);
                self.restore_dscp_best_effort(previous_dscp);
                self.send_cleanup_close_best_effort(machine.cleanup_close_packet());
                return Err(primary);
            }
            if let Err(source) = self.restore_open_read_timeout(post_open_recv_timeout) {
                let primary = ClientError::ReadTimeoutRestore { source };
                self.recv_buffer.truncate(previous_len);
                self.restore_dscp_best_effort(previous_dscp);
                self.send_cleanup_close_best_effort(machine.cleanup_close_packet());
                return Err(primary);
            }

            let outcome = self.runtime.commit_open(machine);
            self.schedule = schedule;
            self.applied_dscp = Some(negotiated_dscp);
            Ok(outcome)
        } else {
            debug_assert!(schedule.is_none());
            if let Err(source) = self.restore_open_read_timeout(post_open_recv_timeout) {
                return Err(ClientError::ReadTimeoutRestore { source });
            }
            let outcome = self.runtime.commit_open(machine);
            self.schedule = None;
            self.applied_dscp = None;
            Ok(outcome)
        }
    }

    fn restore_dscp_best_effort(&self, previous_dscp: Option<u8>) {
        #[cfg(test)]
        if self.test_hooks.fail_dscp_restore.replace(false) {
            return;
        }
        let _ = match previous_dscp {
            Some(dscp) => apply_dscp_to_socket(&self.socket, self.remote, dscp),
            None => clear_dscp_on_socket(&self.socket, self.remote),
        };
    }

    fn send_cleanup_close_best_effort(&self, packet: Option<&[u8]>) {
        if let Some(packet) = packet {
            #[cfg(test)]
            if self.test_hooks.fail_cleanup_send.replace(false) {
                return;
            }
            let _ = self.socket.send(packet);
        }
    }

    fn apply_open_dscp(&self, dscp: u8) -> Result<(), ClientError> {
        #[cfg(test)]
        if self.test_hooks.fail_open_dscp.replace(false) {
            return Err(ClientError::SocketOption {
                operation: "set negotiated DSCP",
                remote: self.remote,
                source: io::Error::other("injected negotiated DSCP failure"),
            });
        }
        apply_dscp_to_socket(&self.socket, self.remote, dscp)
    }

    fn restore_open_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        #[cfg(test)]
        self.test_hooks.last_restored_read_timeout.set(timeout);
        #[cfg(test)]
        if self.test_hooks.fail_open_timeout_restore.replace(false) {
            return Err(io::Error::other(
                "injected configured read timeout restoration failure",
            ));
        }
        self.socket.set_read_timeout(timeout)
    }

    fn send_close_datagram(&self, packet: &[u8]) -> io::Result<usize> {
        #[cfg(test)]
        if self.test_hooks.fail_close_send.replace(false) {
            return Err(io::Error::other("injected close send failure"));
        }
        let bytes = self.socket.send(packet)?;
        #[cfg(test)]
        if let Some(reported) = self.test_hooks.close_reported_len.take() {
            return Ok(reported);
        }
        Ok(bytes)
    }
}

pub(crate) fn echo_sent_event(
    remote: SocketAddr,
    sent: ProbeSent,
    send_call: Duration,
    scheduled_at: Instant,
    timer_error: Duration,
) -> ClientEvent {
    ClientEvent::EchoSent {
        seq: sent.seq,
        remote,
        scheduled_at,
        sent_at: sent.sent_at,
        bytes: sent.bytes,
        send_call,
        timer_error,
    }
}

pub(crate) fn validate_datagram_length(expected: usize, actual: usize) -> Result<(), ClientError> {
    if actual == expected {
        Ok(())
    } else {
        Err(ClientError::DatagramLengthMismatch { expected, actual })
    }
}

#[cfg(test)]
impl Client {
    fn send_probe_at(&mut self, ts: ClientTimestamp) -> Result<Vec<ClientEvent>, ClientError> {
        self.send_probe_inner(Some(ts))
    }

    fn send_managed_probe_at(
        &mut self,
        scheduled_at: Instant,
        ts: ClientTimestamp,
    ) -> Result<Vec<ClientEvent>, ClientError> {
        self.send_managed_probe_inner(scheduled_at, Some(ts))
    }
}

#[cfg(any(test, not(feature = "tokio")))]
fn bounded_receive_timeout(
    deadline: Option<Instant>,
    configured_timeout: Option<Duration>,
    max_wait: Duration,
    now: Instant,
) -> Option<Duration> {
    if max_wait.is_zero() {
        return None;
    }
    let mut timeout = configured_timeout.unwrap_or(max_wait).min(max_wait);
    if let Some(deadline) = deadline {
        let remaining = deadline.checked_duration_since(now)?;
        if remaining.is_zero() {
            return None;
        }
        timeout = timeout.min(remaining);
    }
    (!timeout.is_zero()).then_some(timeout)
}

#[cfg(test)]
mod tests;
