use std::{
    io,
    net::SocketAddr,
    time::{Duration, Instant},
};

#[cfg(test)]
use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    future,
};

use tokio::time;

use crate::{
    client::{
        echo_sent_event,
        schedule::{instant_abs_diff, ProbeSchedule},
        validate_datagram_length,
    },
    config::ClientConfig,
    error::ClientError,
    event::{ClientEvent, OpenOutcome},
    receive::try_recv_tokio_datagram,
    session::machine::{
        recv_buffer_size, OpenDatagramDisposition, PreparedOpenAcceptance, PreparedOpenRequest,
        PreparedProbe, SessionMachine, MAX_OPEN_PACKET_SIZE,
    },
    socket::{connect_tokio_udp_socket, resolve_remote_tokio, validate_open_timeouts},
    socket_options::{apply_dscp_to_tokio_socket, clear_dscp_on_tokio_socket},
    timing::ClientTimestamp,
};

#[cfg(test)]
use crate::client::ProbeSendTimestamps;

#[derive(Debug)]
struct PreparedAsyncOpen {
    machine: PreparedOpenAcceptance,
    schedule: Option<ProbeSchedule>,
    recv_buffer_len: Option<usize>,
    negotiated_dscp: Option<u8>,
}

#[derive(Debug)]
struct PreparedAsyncOpenFailure {
    primary: ClientError,
    machine: PreparedOpenAcceptance,
}

#[cfg(test)]
#[derive(Debug)]
enum InjectedSend {
    WouldBlock,
    Error,
    ReportedLength(usize),
}

#[cfg(test)]
#[derive(Debug, Default)]
struct AsyncClientTestHooks {
    sends: RefCell<VecDeque<InjectedSend>>,
    send_attempts: Cell<usize>,
    receive_would_block: Cell<usize>,
    fail_open_dscp: Cell<bool>,
    fail_cleanup_send: Cell<bool>,
    fail_peer_close_dscp: Cell<bool>,
    fail_close_dscp_clear: Cell<bool>,
    fail_dscp_restore: Cell<bool>,
    pause_open_before_writable: Cell<bool>,
    pause_open_before_readable: Cell<bool>,
    pause_probe_before_writable: Cell<bool>,
    pause_close_before_writable: Cell<bool>,
    pause_close_after_would_block: Cell<bool>,
    probe_timestamps: RefCell<VecDeque<ProbeSendTimestamps>>,
    close_sent_at: Cell<Option<ClientTimestamp>>,
}

#[cfg(test)]
impl AsyncClientTestHooks {
    fn try_send(&self, socket: &tokio::net::UdpSocket, packet: &[u8]) -> io::Result<usize> {
        self.send_attempts.set(self.send_attempts.get() + 1);
        if let Some(injected) = self.sends.borrow_mut().pop_front() {
            return match injected {
                InjectedSend::WouldBlock => Err(io::Error::from(io::ErrorKind::WouldBlock)),
                InjectedSend::Error => Err(io::Error::other("injected send failure")),
                InjectedSend::ReportedLength(bytes) => Ok(bytes),
            };
        }
        socket.try_send(packet)
    }

    fn take_probe_timestamps(&self) -> Option<ProbeSendTimestamps> {
        self.probe_timestamps.borrow_mut().pop_front()
    }
}

/// Low-level Tokio IRTT client for one connected UDP target.
///
/// `AsyncClient` does not construct, own, or store a Tokio runtime. Its async
/// methods are polled by the caller and require a current Tokio runtime with I/O
/// and time enabled.
#[derive(Debug)]
pub struct AsyncClient {
    socket: tokio::net::UdpSocket,
    machine: SessionMachine,
    schedule: Option<ProbeSchedule>,
    remote: SocketAddr,
    recv_buffer: Vec<u8>,
    applied_dscp: Option<u8>,
    prepared_open: Option<PreparedOpenRequest>,
    prepared_probe: Option<PreparedProbe>,
    #[cfg(test)]
    test_hooks: AsyncClientTestHooks,
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
            schedule: None,
            remote,
            recv_buffer: vec![0_u8; recv_buffer_size(false, None)?],
            applied_dscp: None,
            prepared_open: Some(prepared_open),
            prepared_probe: None,
            #[cfg(test)]
            test_hooks: AsyncClientTestHooks::default(),
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
    /// connected with no committed schedule or negotiated adapter state. If it
    /// is dropped during best-effort post-token cleanup, cleanup may remain
    /// incomplete because this low-level client never detaches cleanup work.
    pub async fn open(&mut self) -> Result<OpenOutcome, ClientError> {
        if self.prepared_open.is_none() {
            let _ = self.machine.prepare_open_request()?;
            unreachable!("connected async clients retain an open request");
        }

        let attempt_count = self.machine.config().open_timeouts.len();
        let mut open_buffer = [0_u8; MAX_OPEN_PACKET_SIZE];

        for attempt in 0..attempt_count {
            let timeout = self.machine.config().open_timeouts[attempt];
            let deadline = Instant::now()
                .checked_add(timeout)
                .ok_or(ClientError::DurationOverflow)?;
            if !self.send_open_request(deadline).await? {
                continue;
            }

            loop {
                let Some(datagram) = self.recv_open_datagram(&mut open_buffer, deadline).await?
                else {
                    break;
                };
                if datagram.received_at.mono > deadline {
                    break;
                }

                let reply = match self
                    .machine
                    .inspect_open_datagram(&open_buffer[..datagram.len])?
                {
                    OpenDatagramDisposition::Ignore => continue,
                    OpenDatagramDisposition::Trusted(reply) => reply,
                };
                let machine = match self
                    .machine
                    .prepare_open_acceptance(reply, datagram.received_at)
                {
                    Ok(machine) => machine,
                    Err(failure) => {
                        self.send_cleanup_close_best_effort(
                            failure.cleanup_close.as_deref(),
                            deadline,
                        )
                        .await;
                        return Err(failure.primary);
                    }
                };
                let prepared = match self.prepare_async_open(machine, datagram.received_at) {
                    Ok(prepared) => prepared,
                    Err(failure) => {
                        self.send_cleanup_close_best_effort(
                            failure.machine.cleanup_close_packet(),
                            deadline,
                        )
                        .await;
                        return Err(failure.primary);
                    }
                };
                return Ok(self.commit_async_open(prepared));
            }
        }

        Err(ClientError::OpenTimeout)
    }

    /// Send one caller-paced probe with transactional protocol and schedule
    /// commits.
    ///
    /// A prepared packet is retained across readiness false positives,
    /// cancellation, and socket errors so a later call retries the same logical
    /// probe without advancing state before kernel acceptance.
    pub async fn send_probe(&mut self) -> Result<Vec<ClientEvent>, ClientError> {
        self.machine.ensure_open()?;
        if self.prepared_probe.is_none() {
            self.prepared_probe = self.machine.prepare_probe()?;
        }
        if self.prepared_probe.is_none() {
            return Ok(Vec::new());
        }

        let mut events = Vec::new();
        loop {
            #[cfg(test)]
            if self.test_hooks.pause_probe_before_writable.replace(false) {
                future::pending::<()>().await;
            }
            self.socket.writable().await?;

            #[cfg(test)]
            let test_timestamps = self.test_hooks.take_probe_timestamps();
            #[cfg(not(test))]
            let permission_at = Instant::now();
            #[cfg(test)]
            let permission_at = test_timestamps
                .map(|timestamps| timestamps.permission_at)
                .unwrap_or_else(Instant::now);

            let schedule = self
                .schedule
                .as_mut()
                .expect("open sessions always have a probe schedule");
            if !schedule.permit_probe_at(permission_at) {
                self.prepared_probe = None;
                return Ok(events);
            }
            let prepared = self
                .prepared_probe
                .as_ref()
                .expect("prepared probe was retained across readiness");
            let machine_preflight = self.machine.preflight_probe_commit(prepared)?;
            let schedule_commit =
                schedule.preflight_caller_commit(machine_preflight.next_packets_sent)?;
            events
                .try_reserve(1)
                .map_err(|source| ClientError::AllocationFailed {
                    operation: "probe event result",
                    source,
                })?;

            let expected_bytes = prepared.bytes.len();
            let scheduled_at = schedule_commit.scheduled_at;
            #[cfg(not(test))]
            let sent_at = ClientTimestamp::now();
            #[cfg(test)]
            let sent_at = test_timestamps
                .map(|timestamps| timestamps.sent_at)
                .unwrap_or_else(ClientTimestamp::now);
            let machine_commit = self
                .machine
                .finalize_probe_commit(machine_preflight, sent_at)?;
            #[cfg(not(test))]
            let send_call_start = Instant::now();
            #[cfg(test)]
            let send_call_start = test_timestamps
                .map(|timestamps| timestamps.send_call_start)
                .unwrap_or_else(Instant::now);
            #[cfg(test)]
            let send_result = self.test_hooks.try_send(&self.socket, &prepared.bytes);
            #[cfg(not(test))]
            let send_result = self.socket.try_send(&prepared.bytes);
            let bytes = match send_result {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                Err(error) => return Err(ClientError::Socket(error)),
            };

            #[cfg(not(test))]
            let send_finished_at = Instant::now();
            #[cfg(test)]
            let send_finished_at = test_timestamps
                .map(|timestamps| timestamps.send_finished_at)
                .unwrap_or_else(Instant::now);
            let sent = self.machine.commit_probe_sent(machine_commit, bytes);
            schedule.commit(schedule_commit);
            self.prepared_probe = None;

            let send_call = send_finished_at.saturating_duration_since(send_call_start);
            let timer_error = instant_abs_diff(sent_at.mono, scheduled_at);
            validate_datagram_length(expected_bytes, bytes)?;
            events.push(echo_sent_event(
                self.remote,
                sent,
                send_call,
                scheduled_at,
                timer_error,
            ));
            return Ok(events);
        }
    }

    /// Await and classify one complete UDP datagram.
    ///
    /// State is validated before readiness is awaited. Readiness false positives
    /// retry without changing protocol state. Authenticated peer-close events
    /// remain authoritative even if best-effort DSCP cleanup fails.
    pub async fn recv(&mut self) -> Result<Vec<ClientEvent>, ClientError> {
        self.machine.ensure_open()?;

        let datagram = loop {
            self.socket.readable().await?;
            #[cfg(test)]
            if self.test_hooks.receive_would_block.get() > 0 {
                self.test_hooks
                    .receive_would_block
                    .set(self.test_hooks.receive_would_block.get() - 1);
                continue;
            }
            match try_recv_tokio_datagram(&self.socket, &mut self.recv_buffer) {
                Ok(datagram) => break datagram,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                Err(error) => return Err(ClientError::Socket(error)),
            }
        };

        let events = self.machine.process_received_echo_packet(
            &self.recv_buffer[..datagram.len],
            datagram.received_at,
            datagram.meta,
        )?;
        if self.machine.is_peer_closed() {
            self.schedule = None;
            self.prepared_probe = None;
            if self.clear_peer_close_dscp().is_ok() {
                self.applied_dscp = None;
            }
        }
        Ok(events)
    }

    /// Poll protocol timeouts using a newly captured monotonic timestamp.
    pub fn poll_timeouts(&mut self) -> Result<Vec<ClientEvent>, ClientError> {
        self.poll_timeouts_at(Instant::now())
    }

    /// Poll protocol timeouts using the caller's monotonic timestamp.
    pub fn poll_timeouts_at(&mut self, now: Instant) -> Result<Vec<ClientEvent>, ClientError> {
        self.machine.poll_timeouts_at(now)
    }

    /// Send the retained close packet and commit local close exactly once.
    ///
    /// DSCP is cleared only after write readiness and immediately before the
    /// nonblocking send attempt. A `WouldBlock` result restores it before the
    /// next await, so dropping this future at a suspension point cannot leave
    /// an otherwise-open session with cleared DSCP.
    pub async fn close(&mut self) -> Result<Vec<ClientEvent>, ClientError> {
        let prepared = self.machine.prepare_close()?;
        let mut events = Vec::new();
        events
            .try_reserve(1)
            .map_err(|source| ClientError::AllocationFailed {
                operation: "close event result",
                source,
            })?;
        let previous_dscp = self.applied_dscp;
        let expected_bytes = prepared.bytes.len();

        loop {
            #[cfg(test)]
            if self.test_hooks.pause_close_before_writable.replace(false) {
                future::pending::<()>().await;
            }
            self.socket.writable().await?;
            self.clear_close_dscp()?;
            let mut rollback = DscpRollback::armed(
                &self.socket,
                self.remote,
                previous_dscp,
                #[cfg(test)]
                &self.test_hooks.fail_dscp_restore,
            );
            #[cfg(test)]
            let send_result = self.test_hooks.try_send(&self.socket, prepared.bytes);
            #[cfg(not(test))]
            let send_result = self.socket.try_send(prepared.bytes);
            let bytes = match send_result {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    rollback.restore()?;
                    #[cfg(test)]
                    if self.test_hooks.pause_close_after_would_block.replace(false) {
                        future::pending::<()>().await;
                    }
                    continue;
                }
                Err(error) => return Err(ClientError::Socket(error)),
            };

            #[cfg(not(test))]
            let close_sent_at = ClientTimestamp::now();
            #[cfg(test)]
            let close_sent_at = self
                .test_hooks
                .close_sent_at
                .take()
                .unwrap_or_else(ClientTimestamp::now);
            let event = self
                .machine
                .commit_local_close(prepared.commit, close_sent_at);
            rollback.disarm();
            self.schedule = None;
            self.prepared_probe = None;
            self.applied_dscp = None;

            validate_datagram_length(expected_bytes, bytes)?;
            events.push(event);
            return Ok(events);
        }
    }

    /// Return the next scheduled probe deadline.
    pub fn next_send_deadline(&self) -> Option<Instant> {
        if !self.machine.is_open() {
            return None;
        }
        self.schedule.as_ref()?.next_send_deadline()
    }

    /// Return the configured local probe timeout.
    pub fn probe_timeout(&self) -> Duration {
        self.machine.probe_timeout()
    }

    /// Return whether the current run has completed.
    pub fn is_run_complete(&self) -> bool {
        self.machine.is_terminal()
            || self
                .schedule
                .as_ref()
                .is_some_and(|schedule| schedule.is_finished() && self.machine.pending_is_empty())
    }

    /// Return whether an authenticated peer close ended the session.
    pub fn is_peer_closed(&self) -> bool {
        self.machine.is_peer_closed()
    }

    async fn send_open_request(&self, deadline: Instant) -> Result<bool, ClientError> {
        let request = self
            .prepared_open
            .as_ref()
            .expect("connected clients retain their prepared open request");
        loop {
            #[cfg(test)]
            if self.test_hooks.pause_open_before_writable.replace(false) {
                future::pending::<()>().await;
            }
            if !wait_for_writable(&self.socket, deadline).await? {
                return Ok(false);
            }
            #[cfg(test)]
            let send_result = self.test_hooks.try_send(&self.socket, &request.bytes);
            #[cfg(not(test))]
            let send_result = self.socket.try_send(&request.bytes);
            match send_result {
                Ok(bytes) => {
                    validate_datagram_length(request.bytes.len(), bytes)?;
                    return Ok(true);
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(ClientError::Socket(error)),
            }
        }
    }

    async fn recv_open_datagram(
        &self,
        buffer: &mut [u8],
        deadline: Instant,
    ) -> Result<Option<crate::receive::ReceivedDatagram>, ClientError> {
        loop {
            #[cfg(test)]
            if self.test_hooks.pause_open_before_readable.replace(false) {
                future::pending::<()>().await;
            }
            if !wait_for_readable(&self.socket, deadline).await? {
                return Ok(None);
            }
            match try_recv_tokio_datagram(&self.socket, buffer) {
                Ok(datagram) => return Ok(Some(datagram)),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(ClientError::Socket(error)),
            }
        }
    }

    fn prepare_async_open(
        &mut self,
        machine: PreparedOpenAcceptance,
        opened_at: ClientTimestamp,
    ) -> Result<PreparedAsyncOpen, Box<PreparedAsyncOpenFailure>> {
        let Some(negotiated) = machine.normal_negotiated() else {
            return Ok(PreparedAsyncOpen {
                machine,
                schedule: None,
                recv_buffer_len: None,
                negotiated_dscp: None,
            });
        };
        let schedule = match ProbeSchedule::new(opened_at.mono, negotiated) {
            Ok(schedule) => schedule,
            Err(primary) => {
                return Err(Box::new(PreparedAsyncOpenFailure { primary, machine }));
            }
        };
        let recv_buffer_len = match recv_buffer_size(self.machine.has_hmac(), Some(negotiated)) {
            Ok(size) => size,
            Err(primary) => {
                return Err(Box::new(PreparedAsyncOpenFailure { primary, machine }));
            }
        };
        let negotiated_dscp = match u8::try_from(negotiated.params.dscp) {
            Ok(dscp) => dscp,
            Err(_) => {
                return Err(Box::new(PreparedAsyncOpenFailure {
                    primary: ClientError::InvalidConfig {
                        reason: "negotiated dscp must be in range 0..=63".to_owned(),
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
        #[cfg(test)]
        let dscp_result = if self.test_hooks.fail_open_dscp.replace(false) {
            Err(ClientError::SocketOption {
                operation: "set negotiated DSCP",
                remote: self.remote,
                source: io::Error::other("injected negotiated DSCP failure"),
            })
        } else {
            apply_dscp_to_tokio_socket(&self.socket, self.remote, negotiated_dscp)
        };
        #[cfg(not(test))]
        let dscp_result = apply_dscp_to_tokio_socket(&self.socket, self.remote, negotiated_dscp);
        if let Err(primary) = dscp_result {
            self.restore_dscp_best_effort(self.applied_dscp);
            return Err(Box::new(PreparedAsyncOpenFailure { primary, machine }));
        }

        Ok(PreparedAsyncOpen {
            machine,
            schedule: Some(schedule),
            recv_buffer_len: Some(recv_buffer_len),
            negotiated_dscp: Some(negotiated_dscp),
        })
    }

    fn commit_async_open(&mut self, prepared: PreparedAsyncOpen) -> OpenOutcome {
        if let Some(recv_buffer_len) = prepared.recv_buffer_len {
            self.recv_buffer.resize(recv_buffer_len, 0);
        }
        let outcome = self.machine.commit_open(prepared.machine);
        self.schedule = prepared.schedule;
        self.applied_dscp = prepared.negotiated_dscp;
        self.prepared_open = None;
        outcome
    }

    async fn send_cleanup_close_best_effort(&self, packet: Option<&[u8]>, deadline: Instant) {
        let Some(packet) = packet else {
            return;
        };
        loop {
            #[cfg(test)]
            if self.test_hooks.fail_cleanup_send.replace(false) {
                return;
            }
            #[cfg(test)]
            let send_result = self.test_hooks.try_send(&self.socket, packet);
            #[cfg(not(test))]
            let send_result = self.socket.try_send(packet);
            match send_result {
                Ok(_) => return,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(_) => return,
            }
            match wait_for_writable(&self.socket, deadline).await {
                Ok(true) => {}
                Ok(false) | Err(_) => return,
            }
        }
    }

    fn clear_peer_close_dscp(&self) -> Result<(), ClientError> {
        #[cfg(test)]
        if self.test_hooks.fail_peer_close_dscp.replace(false) {
            return Err(ClientError::SocketOption {
                operation: "clear DSCP before close",
                remote: self.remote,
                source: io::Error::other("injected peer-close DSCP cleanup failure"),
            });
        }
        clear_dscp_on_tokio_socket(&self.socket, self.remote)
    }

    fn clear_close_dscp(&self) -> Result<(), ClientError> {
        #[cfg(test)]
        if self.test_hooks.fail_close_dscp_clear.replace(false) {
            return Err(ClientError::SocketOption {
                operation: "clear DSCP before close",
                remote: self.remote,
                source: io::Error::other("injected close DSCP clear failure"),
            });
        }
        clear_dscp_on_tokio_socket(&self.socket, self.remote)
    }

    fn restore_dscp_best_effort(&self, previous_dscp: Option<u8>) {
        let _ = match previous_dscp {
            Some(dscp) => apply_dscp_to_tokio_socket(&self.socket, self.remote, dscp),
            None => clear_dscp_on_tokio_socket(&self.socket, self.remote),
        };
    }
}

struct DscpRollback<'a> {
    socket: &'a tokio::net::UdpSocket,
    remote: SocketAddr,
    previous_dscp: Option<u8>,
    armed: bool,
    #[cfg(test)]
    fail_restore: &'a Cell<bool>,
}

impl<'a> DscpRollback<'a> {
    fn armed(
        socket: &'a tokio::net::UdpSocket,
        remote: SocketAddr,
        previous_dscp: Option<u8>,
        #[cfg(test)] fail_restore: &'a Cell<bool>,
    ) -> Self {
        Self {
            socket,
            remote,
            previous_dscp,
            armed: true,
            #[cfg(test)]
            fail_restore,
        }
    }

    fn restore(&mut self) -> Result<(), ClientError> {
        if !self.armed {
            return Ok(());
        }
        #[cfg(test)]
        if self.fail_restore.replace(false) {
            return Err(ClientError::SocketOption {
                operation: "restore negotiated DSCP",
                remote: self.remote,
                source: io::Error::other("injected DSCP restore failure"),
            });
        }
        match self.previous_dscp {
            Some(dscp) => apply_dscp_to_tokio_socket(self.socket, self.remote, dscp)?,
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

async fn wait_for_writable(
    socket: &tokio::net::UdpSocket,
    deadline: Instant,
) -> Result<bool, ClientError> {
    match time::timeout_at(deadline.into(), socket.writable()).await {
        Ok(result) => {
            result?;
            Ok(true)
        }
        Err(_) => Ok(false),
    }
}

async fn wait_for_readable(
    socket: &tokio::net::UdpSocket,
    deadline: Instant,
) -> Result<bool, ClientError> {
    match time::timeout_at(deadline.into(), socket.readable()).await {
        Ok(result) => {
            result?;
            Ok(true)
        }
        Err(_) => Ok(false),
    }
}

#[cfg(test)]
mod tests;
