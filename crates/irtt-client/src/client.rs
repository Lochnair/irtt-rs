use std::{
    io,
    net::{SocketAddr, UdpSocket},
    time::{Duration, Instant},
};

#[cfg(test)]
use irtt_proto::{flags, Params, TimestampFields, PROTOCOL_VERSION};

use crate::{
    config::{ClientConfig, RecvBudget},
    error::ClientError,
    event::{ClientEvent, OpenOutcome},
    receive::recv_datagram,
    session::machine::{
        recv_buffer_size, ProbeSent, SendMetadata, SessionMachine, MAX_OPEN_PACKET_SIZE,
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
        })
    }

    /// Perform the IRTT open exchange.
    ///
    /// On success, returns the negotiated open outcome and transitions the
    /// client into either an open probe session or completed no-test state.
    /// Open attempts use [`ClientConfig::open_timeouts`].
    pub fn open(&mut self) -> Result<OpenOutcome, ClientError> {
        let outcome = (|| -> Result<OpenOutcome, ClientError> {
            let packet = self.runtime.open_packet()?;
            let mut buf = [0_u8; MAX_OPEN_PACKET_SIZE];

            for timeout in &self.runtime.config().open_timeouts {
                self.socket.set_read_timeout(Some(*timeout))?;
                self.socket.send(&packet)?;

                match self.socket.recv(&mut buf) {
                    Ok(size) => {
                        let reply = self.runtime.decode_open_reply(&buf[..size])?;
                        let opened_at = ClientTimestamp::now();
                        let remote = self.remote;
                        let has_hmac = self.runtime.has_hmac();
                        let socket = &self.socket;
                        let recv_buffer = &mut self.recv_buffer;
                        let mut schedule = None;

                        let outcome =
                            self.runtime
                                .accept_open_reply(reply, opened_at, |negotiated| {
                                    let size = recv_buffer_size(has_hmac, Some(negotiated))?;
                                    let negotiated_dscp = u8::try_from(negotiated.params.dscp)
                                        .map_err(|_| ClientError::InvalidConfig {
                                            reason: "negotiated dscp must be in range 0..=63"
                                                .to_owned(),
                                        })?;
                                    apply_dscp_to_socket(socket, remote, negotiated_dscp)?;
                                    recv_buffer.resize(size, 0);
                                    schedule =
                                        Some(ProbeSchedule::new(opened_at.mono, negotiated)?);
                                    Ok(())
                                })?;
                        self.schedule = schedule;
                        return Ok(outcome);
                    }
                    Err(err)
                        if matches!(
                            err.kind(),
                            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                        ) => {}
                    Err(err) => return Err(ClientError::Socket(err)),
                }
            }

            Err(ClientError::OpenTimeout)
        })();

        let restore = self
            .socket
            .set_read_timeout(self.runtime.config().socket_config.recv_timeout);

        match (outcome, restore) {
            (Ok(outcome), Ok(())) => Ok(outcome),
            (Ok(_), Err(source)) => Err(ClientError::ReadTimeoutRestore { source }),
            (Err(err), Ok(())) => Err(err),
            (Err(err), Err(_)) => Err(err),
        }
    }

    /// Send a close request and emit a [`ClientEvent::SessionClosed`] event.
    ///
    /// The close event means the client has sent its close packet and stopped
    /// tracking the session locally; it is not a server acknowledgement.
    pub fn close(&mut self) -> Result<Vec<ClientEvent>, ClientError> {
        let socket = &self.socket;
        let remote = self.remote;
        let events = self.runtime.close_with(|packet| {
            clear_dscp_on_socket(socket, remote)?;
            socket.send(packet)?;
            Ok(())
        })?;
        self.schedule = None;
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

        self.runtime.process_received_echo_packet(
            &self.recv_buffer[..datagram.len],
            datagram.received_at,
            datagram.meta,
        )
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
        let permission_now = override_ts.map_or_else(Instant::now, |timestamp| timestamp.mono);
        let remote = self.remote;
        let (runtime, schedule, socket) = (&mut self.runtime, &mut self.schedule, &self.socket);
        let schedule = schedule
            .as_mut()
            .expect("open sessions always have a probe schedule");
        if !schedule.permit_probe_at(permission_now) {
            return Ok(Vec::new());
        }

        let Some(prepared) = runtime.prepare_probe()? else {
            return Ok(Vec::new());
        };
        let sent_at = override_ts.unwrap_or_else(ClientTimestamp::now);
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
        let send_call_start = Instant::now();
        let bytes = socket.send(&prepared.bytes)?;
        let send_call = send_call_start.elapsed();
        let sent = runtime.commit_probe_sent(machine_commit, SendMetadata { bytes, send_call });
        schedule.commit(schedule_commit);

        if bytes != expected_bytes {
            return Err(ClientError::DatagramLengthMismatch {
                expected: expected_bytes,
                actual: bytes,
            });
        }

        events.push(echo_sent_event(remote, sent, scheduled_at, timer_error));
        Ok(events)
    }
}

pub(crate) fn echo_sent_event(
    remote: SocketAddr,
    sent: ProbeSent,
    scheduled_at: Instant,
    timer_error: Duration,
) -> ClientEvent {
    ClientEvent::EchoSent {
        seq: sent.seq,
        remote,
        scheduled_at,
        sent_at: sent.sent_at,
        bytes: sent.bytes,
        send_call: sent.send_call,
        timer_error,
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
