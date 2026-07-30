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
    client::{echo_sent_event, schedule::ProbeSchedule, validate_datagram_length},
    config::ClientConfig,
    error::ClientError,
    event::{ClientEvent, OpenOutcome},
    receive::try_recv_tokio_datagram,
    session::machine::{
        recv_buffer_size, OpenDatagramDisposition, PreparedOpenAcceptance, PreparedOpenRequest,
        PreparedProbe, SendMetadata, SessionMachine, MAX_OPEN_PACKET_SIZE,
    },
    socket::{connect_tokio_udp_socket, resolve_remote, validate_open_timeouts},
    socket_options::{apply_dscp_to_tokio_socket, clear_dscp_on_tokio_socket},
    timing::ClientTimestamp,
};

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
    receive_would_block: Cell<usize>,
    fail_open_dscp: Cell<bool>,
    pause_probe_before_writable: Cell<bool>,
    pause_close_before_writable: Cell<bool>,
}

#[cfg(test)]
impl AsyncClientTestHooks {
    fn try_send(&self, socket: &tokio::net::UdpSocket, packet: &[u8]) -> io::Result<usize> {
        if let Some(injected) = self.sends.borrow_mut().pop_front() {
            return match injected {
                InjectedSend::WouldBlock => Err(io::Error::from(io::ErrorKind::WouldBlock)),
                InjectedSend::Error => Err(io::Error::other("injected send failure")),
                InjectedSend::ReportedLength(bytes) => Ok(bytes),
            };
        }
        socket.try_send(packet)
    }
}

/// Low-level Tokio IRTT client for one connected UDP target.
///
/// `AsyncClient` does not construct or own a Tokio runtime. Its async methods
/// must be polled inside a Tokio runtime with I/O and time enabled.
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
        let remote = resolve_remote(&config)?;
        let machine = SessionMachine::new(config.clone(), remote)?;
        let prepared_open = machine.prepare_open_request()?;
        let socket = connect_tokio_udp_socket(&config.socket_config, remote).await?;

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

    /// Perform the incremental IRTT open exchange.
    ///
    /// Each configured attempt uses one absolute deadline for sending and for
    /// all replies received during that attempt. Malformed, unrelated, and
    /// unauthenticated datagrams are ignored until that deadline.
    ///
    /// Dropping this future before trusted acceptance leaves the local machine
    /// connected with no committed schedule or negotiated adapter state. It
    /// does not spawn detached server cleanup; a higher-level managed driver
    /// may add bounded cancellation cleanup later.
    pub async fn open(&mut self) -> Result<OpenOutcome, ClientError> {
        self.machine.ensure_connected()?;
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

    /// Send one probe with Tokio readiness and transactional protocol commits.
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
            let sent_at = ClientTimestamp::now();

            let schedule = self
                .schedule
                .as_mut()
                .expect("open sessions always have a probe schedule");
            if !schedule.permit_probe_at(sent_at.mono) {
                self.prepared_probe = None;
                return Ok(events);
            }
            let prepared = self
                .prepared_probe
                .as_ref()
                .expect("prepared probe was retained across readiness");
            let machine_commit = self.machine.preflight_probe_commit(prepared, sent_at)?;
            let schedule_commit =
                schedule.preflight_caller_commit(sent_at.mono, machine_commit.next_packets_sent)?;
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
            #[cfg(test)]
            let send_result = self.test_hooks.try_send(&self.socket, &prepared.bytes);
            #[cfg(not(test))]
            let send_result = self.socket.try_send(&prepared.bytes);
            let bytes = match send_result {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(error) => return Err(ClientError::Socket(error)),
            };
            let send_call = send_call_start.elapsed();
            let sent = self
                .machine
                .commit_probe_sent(machine_commit, SendMetadata { bytes, send_call });
            schedule.commit(schedule_commit);
            self.prepared_probe = None;

            validate_datagram_length(expected_bytes, bytes)?;
            events.push(echo_sent_event(
                self.remote,
                sent,
                scheduled_at,
                timer_error,
            ));
            return Ok(events);
        }
    }

    /// Await and classify one complete UDP datagram.
    ///
    /// Readiness false positives retry without changing protocol state.
    /// Authenticated peer-close transitions are committed before adapter
    /// schedule and DSCP cleanup.
    pub async fn recv(&mut self) -> Result<Vec<ClientEvent>, ClientError> {
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
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    continue;
                }
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
            let clear_result = clear_dscp_on_tokio_socket(&self.socket, self.remote);
            self.applied_dscp = None;
            clear_result?;
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
    /// an open session with cleared DSCP.
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
            let mut rollback = DscpRollback::clear(&self.socket, self.remote, previous_dscp)?;
            #[cfg(test)]
            let send_result = self.test_hooks.try_send(&self.socket, prepared.bytes);
            #[cfg(not(test))]
            let send_result = self.socket.try_send(prepared.bytes);
            let bytes = match send_result {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    rollback.restore()?;
                    continue;
                }
                Err(error) => return Err(ClientError::Socket(error)),
            };
            rollback.disarm();
            let event = self.machine.commit_local_close(prepared.commit);
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
}

impl<'a> DscpRollback<'a> {
    fn clear(
        socket: &'a tokio::net::UdpSocket,
        remote: SocketAddr,
        previous_dscp: Option<u8>,
    ) -> Result<Self, ClientError> {
        clear_dscp_on_tokio_socket(socket, remote)?;
        Ok(Self {
            socket,
            remote,
            previous_dscp,
            armed: true,
        })
    }

    fn restore(&mut self) -> Result<(), ClientError> {
        if !self.armed {
            return Ok(());
        }
        match self.previous_dscp {
            Some(dscp) => {
                apply_dscp_to_tokio_socket(self.socket, self.remote, dscp)?;
            }
            None => {
                clear_dscp_on_tokio_socket(self.socket, self.remote)?;
            }
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
mod tests {
    use std::{
        future::Future,
        net::UdpSocket,
        pin::Pin,
        sync::mpsc,
        task::{Context, Poll, Waker},
        thread::{self, JoinHandle},
    };

    use irtt_proto::{
        decode_close_request, decode_echo_request, decode_open_request, encode_echo_reply,
        encode_open_reply, flags, EchoReply, OpenReply, Params, ReceivedStats, StampAt,
        TimestampFields,
    };
    use tokio::runtime::{Builder, Runtime};

    use super::*;
    use crate::{socket_options::tokio_socket_traffic_class, Client, SocketConfig, WarningKind};

    const TOKEN: u64 = 0x1234_5678_90ab_cdef;

    struct TestServer {
        addr: SocketAddr,
        packets: mpsc::Receiver<Vec<u8>>,
        done: JoinHandle<()>,
    }

    impl TestServer {
        fn join(self) {
            self.done.join().unwrap();
        }
    }

    fn runtime() -> Runtime {
        Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .unwrap()
    }

    fn config(addr: SocketAddr, key: Option<Vec<u8>>, dscp: u8) -> ClientConfig {
        ClientConfig {
            server_addr: addr.to_string(),
            received_stats: ReceivedStats::None,
            stamp_at: StampAt::None,
            dscp,
            hmac_key: key,
            open_timeouts: vec![Duration::from_millis(200)],
            socket_config: SocketConfig {
                recv_timeout: Some(Duration::from_millis(200)),
                ..SocketConfig::default()
            },
            ..ClientConfig::default()
        }
    }

    fn start_server<F>(handler: F) -> TestServer
    where
        F: FnOnce(UdpSocket, mpsc::Sender<Vec<u8>>) + Send + 'static,
    {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let addr = socket.local_addr().unwrap();
        let (tx, packets) = mpsc::channel();
        let done = thread::spawn(move || handler(socket, tx));
        TestServer {
            addr,
            packets,
            done,
        }
    }

    fn recv_packet(socket: &UdpSocket, tx: &mpsc::Sender<Vec<u8>>) -> (Vec<u8>, SocketAddr) {
        let mut buffer = [0_u8; 8192];
        let (len, peer) = socket.recv_from(&mut buffer).unwrap();
        let packet = buffer[..len].to_vec();
        tx.send(packet.clone()).unwrap();
        (packet, peer)
    }

    fn send_open_reply(
        socket: &UdpSocket,
        peer: SocketAddr,
        params: Params,
        key: Option<&[u8]>,
        reply_flags: u8,
        token: u64,
    ) {
        let reply = encode_open_reply(
            &OpenReply {
                flags: reply_flags,
                token,
                params,
            },
            key,
        )
        .unwrap();
        socket.send_to(&reply, peer).unwrap();
    }

    fn echo_reply(
        params: &Params,
        sequence: u32,
        token: u64,
        reply_flags: u8,
        key: Option<&[u8]>,
    ) -> Vec<u8> {
        encode_echo_reply(
            &EchoReply {
                flags: reply_flags,
                token,
                sequence,
                recv_count: None,
                recv_window: None,
                timestamps: TimestampFields::default(),
                payload: Vec::new(),
            },
            params,
            key,
        )
        .unwrap()
    }

    fn start_echo_close_server(key: Option<Vec<u8>>) -> TestServer {
        start_server(move |socket, tx| {
            let (open_packet, peer) = recv_packet(&socket, &tx);
            let request = decode_open_request(&open_packet, key.as_deref()).unwrap();
            send_open_reply(
                &socket,
                peer,
                request.params.clone(),
                key.as_deref(),
                flags::FLAG_OPEN | flags::FLAG_REPLY,
                TOKEN,
            );

            let (probe_packet, _) = recv_packet(&socket, &tx);
            let probe =
                decode_echo_request(&probe_packet, &request.params, key.as_deref()).unwrap();
            socket
                .send_to(
                    &echo_reply(
                        &request.params,
                        probe.sequence,
                        TOKEN,
                        flags::FLAG_REPLY,
                        key.as_deref(),
                    ),
                    peer,
                )
                .unwrap();

            let (close_packet, _) = recv_packet(&socket, &tx);
            assert_eq!(
                decode_close_request(&close_packet, key.as_deref())
                    .unwrap()
                    .token,
                TOKEN
            );
        })
    }

    #[test]
    fn connect_requires_current_runtime_when_polled() {
        let mut future = Box::pin(AsyncClient::connect(ClientConfig::default()));
        let mut context = Context::from_waker(Waker::noop());

        assert!(matches!(
            Future::poll(Pin::as_mut(&mut future), &mut context),
            Poll::Ready(Err(ClientError::NoTokioRuntime))
        ));
    }

    #[test]
    fn current_thread_hmac_lifecycle_matches_blocking_client() {
        let key = b"async-transaction-key".to_vec();
        let blocking_server = start_echo_close_server(Some(key.clone()));
        let blocking_config = config(blocking_server.addr, Some(key.clone()), 0);
        let mut blocking = Client::connect(blocking_config).unwrap();
        let blocking_open = blocking.open().unwrap();
        let blocking_sent = blocking.send_probe().unwrap();
        let blocking_reply = blocking.recv_once().unwrap();
        let blocking_close = blocking.close().unwrap();
        blocking_server.join();

        let async_server = start_echo_close_server(Some(key.clone()));
        let async_config = config(async_server.addr, Some(key), 0);
        let (async_open, async_sent, async_reply, async_close) = runtime().block_on(async {
            let mut client = AsyncClient::connect(async_config).await.unwrap();
            let opened = client.open().await.unwrap();
            client.test_hooks.receive_would_block.set(1);
            let sent = client.send_probe().await.unwrap();
            let reply = client.recv().await.unwrap();
            let closed = client.close().await.unwrap();
            (opened, sent, reply, closed)
        });
        async_server.join();

        assert_eq!(
            open_negotiated(&blocking_open),
            open_negotiated(&async_open)
        );
        assert_matching_event_shape(&blocking_sent[0], &async_sent[0]);
        assert_matching_event_shape(&blocking_reply[0], &async_reply[0]);
        assert_matching_event_shape(&blocking_close[0], &async_close[0]);
    }

    #[test]
    fn open_ignores_malformed_and_bad_hmac_before_valid_reply() {
        let key = b"open-filter-key".to_vec();
        let server_key = key.clone();
        let server = start_server(move |socket, tx| {
            let (open_packet, peer) = recv_packet(&socket, &tx);
            let request = decode_open_request(&open_packet, Some(&server_key)).unwrap();
            socket.send_to(&[0_u8], peer).unwrap();
            let mut bad_hmac = encode_open_reply(
                &OpenReply {
                    flags: flags::FLAG_OPEN | flags::FLAG_REPLY,
                    token: TOKEN,
                    params: request.params.clone(),
                },
                Some(&server_key),
            )
            .unwrap();
            bad_hmac[4] ^= 0xff;
            socket.send_to(&bad_hmac, peer).unwrap();
            send_open_reply(
                &socket,
                peer,
                request.params,
                Some(&server_key),
                flags::FLAG_OPEN | flags::FLAG_REPLY,
                TOKEN,
            );
        });

        runtime().block_on(async {
            let mut client = AsyncClient::connect(config(server.addr, Some(key), 0))
                .await
                .unwrap();
            assert!(matches!(
                client.open().await.unwrap(),
                OpenOutcome::Started { .. }
            ));
        });
        assert_eq!(server.packets.iter().take(1).count(), 1);
        assert!(server.packets.try_recv().is_err());
        server.join();
    }

    #[test]
    fn ignored_open_traffic_times_out_once_per_attempt() {
        let server = start_server(move |socket, tx| {
            for _ in 0..2 {
                let (_, peer) = recv_packet(&socket, &tx);
                socket.send_to(&[0_u8], peer).unwrap();
            }
        });
        let mut client_config = config(server.addr, None, 0);
        client_config.open_timeouts = vec![Duration::from_millis(200), Duration::from_millis(200)];

        runtime().block_on(async {
            let mut client = AsyncClient::connect(client_config).await.unwrap();
            assert!(matches!(client.open().await, Err(ClientError::OpenTimeout)));
            assert!(client.machine.prepare_open_request().is_ok());
            assert!(client.schedule.is_none());
        });
        assert_eq!(server.packets.iter().take(2).count(), 2);
        assert!(server.packets.try_recv().is_err());
        server.join();
    }

    #[test]
    fn authenticated_rejection_is_terminal_without_retry() {
        let server = start_server(move |socket, tx| {
            let (open_packet, peer) = recv_packet(&socket, &tx);
            let request = decode_open_request(&open_packet, None).unwrap();
            send_open_reply(
                &socket,
                peer,
                request.params,
                None,
                flags::FLAG_OPEN | flags::FLAG_REPLY | flags::FLAG_CLOSE,
                0,
            );
        });
        let mut client_config = config(server.addr, None, 0);
        client_config.open_timeouts = vec![Duration::from_millis(200), Duration::from_millis(200)];

        runtime().block_on(async {
            let mut client = AsyncClient::connect(client_config).await.unwrap();
            assert!(matches!(
                client.open().await,
                Err(ClientError::ServerRejected)
            ));
            assert!(client.machine.prepare_open_request().is_ok());
        });
        assert_eq!(server.packets.iter().take(1).count(), 1);
        assert!(server.packets.try_recv().is_err());
        server.join();
    }

    #[test]
    fn dropping_unpolled_open_sends_nothing() {
        let server = start_server(move |socket, tx| {
            socket
                .set_read_timeout(Some(Duration::from_millis(50)))
                .unwrap();
            let mut buffer = [0_u8; 512];
            if let Ok((len, _)) = socket.recv_from(&mut buffer) {
                tx.send(buffer[..len].to_vec()).unwrap();
            }
        });

        runtime().block_on(async {
            let mut client = AsyncClient::connect(config(server.addr, None, 0))
                .await
                .unwrap();
            let open = client.open();
            drop(open);
            assert!(client.machine.prepare_open_request().is_ok());
            assert!(client.schedule.is_none());
            assert_eq!(client.applied_dscp, None);
        });
        thread::sleep(Duration::from_millis(75));
        assert!(server.packets.try_recv().is_err());
        server.join();
    }

    #[test]
    fn open_cancellation_after_send_is_locally_transactional() {
        let server = start_server(move |socket, tx| {
            let (first, _) = recv_packet(&socket, &tx);
            let first = decode_open_request(&first, None).unwrap();
            let (_, peer) = recv_packet(&socket, &tx);
            send_open_reply(
                &socket,
                peer,
                first.params,
                None,
                flags::FLAG_OPEN | flags::FLAG_REPLY,
                TOKEN,
            );
        });
        let mut config = config(server.addr, None, 0);
        config.open_timeouts = vec![Duration::from_millis(500)];

        runtime().block_on(async {
            let mut client = AsyncClient::connect(config).await.unwrap();
            assert!(time::timeout(Duration::from_millis(20), client.open(),)
                .await
                .is_err());
            assert!(client.machine.prepare_open_request().is_ok());
            assert!(client.schedule.is_none());
            assert_eq!(client.applied_dscp, None);

            assert!(matches!(
                client.open().await.unwrap(),
                OpenOutcome::Started { .. }
            ));
        });
        assert_eq!(server.packets.iter().take(2).count(), 2);
        server.join();
    }

    #[test]
    fn adapter_failure_sends_cleanup_without_committing_open() {
        let server = start_server(move |socket, tx| {
            let (open_packet, peer) = recv_packet(&socket, &tx);
            let request = decode_open_request(&open_packet, None).unwrap();
            send_open_reply(
                &socket,
                peer,
                request.params,
                None,
                flags::FLAG_OPEN | flags::FLAG_REPLY,
                TOKEN,
            );
            let (close, _) = recv_packet(&socket, &tx);
            assert_eq!(decode_close_request(&close, None).unwrap().token, TOKEN);
        });

        runtime().block_on(async {
            let mut client = AsyncClient::connect(config(server.addr, None, 46))
                .await
                .unwrap();
            client.test_hooks.fail_open_dscp.set(true);
            assert!(matches!(
                client.open().await,
                Err(ClientError::SocketOption { .. })
            ));
            assert!(client.machine.prepare_open_request().is_ok());
            assert!(client.schedule.is_none());
            assert_eq!(client.applied_dscp, None);
        });
        assert_eq!(server.packets.iter().take(2).count(), 2);
        server.join();
    }

    #[test]
    fn probe_would_block_error_and_cancellation_preserve_transaction() {
        let server = start_server(move |socket, tx| {
            let (open_packet, peer) = recv_packet(&socket, &tx);
            let request = decode_open_request(&open_packet, None).unwrap();
            send_open_reply(
                &socket,
                peer,
                request.params.clone(),
                None,
                flags::FLAG_OPEN | flags::FLAG_REPLY,
                TOKEN,
            );
            for expected_sequence in 0..2 {
                let (probe, _) = recv_packet(&socket, &tx);
                assert_eq!(
                    decode_echo_request(&probe, &request.params, None,)
                        .unwrap()
                        .sequence,
                    expected_sequence
                );
            }
        });

        runtime().block_on(async {
            let mut client = AsyncClient::connect(config(server.addr, None, 0))
                .await
                .unwrap();
            client.open().await.unwrap();
            client
                .test_hooks
                .sends
                .borrow_mut()
                .extend([InjectedSend::WouldBlock, InjectedSend::WouldBlock]);
            let before_readiness = Instant::now();
            let first = client.send_probe().await.unwrap();
            assert!(matches!(
                first.as_slice(),
                [ClientEvent::EchoSent { seq: 0, .. }]
            ));
            assert!(matches!(
                first.as_slice(),
                [ClientEvent::EchoSent { sent_at, .. }]
                    if sent_at.mono >= before_readiness
            ));
            assert_eq!(client.machine.packets_sent(), 1);

            client.test_hooks.pause_probe_before_writable.set(true);
            assert!(
                time::timeout(Duration::from_millis(10), client.send_probe(),)
                    .await
                    .is_err()
            );
            let retained = client.prepared_probe.as_ref().unwrap().bytes.clone();
            let deadline = client.next_send_deadline();
            client
                .test_hooks
                .sends
                .borrow_mut()
                .push_back(InjectedSend::Error);
            assert!(matches!(
                client.send_probe().await,
                Err(ClientError::Socket(_))
            ));
            assert_eq!(client.prepared_probe.as_ref().unwrap().bytes, retained);
            assert_eq!(client.machine.packets_sent(), 1);
            assert_eq!(client.next_send_deadline(), deadline);

            let second = client.send_probe().await.unwrap();
            assert!(matches!(
                second.as_slice(),
                [ClientEvent::EchoSent { seq: 1, .. }]
            ));
            assert_eq!(client.machine.packets_sent(), 2);
        });
        assert_eq!(server.packets.iter().take(3).count(), 3);
        server.join();
    }

    #[test]
    fn expired_schedule_and_pending_limit_prevent_syscalls() {
        let expiry_server = start_server(move |socket, tx| {
            let (open_packet, peer) = recv_packet(&socket, &tx);
            let mut request = decode_open_request(&open_packet, None).unwrap();
            request.params.duration_ns = 1_000_000;
            send_open_reply(
                &socket,
                peer,
                request.params,
                None,
                flags::FLAG_OPEN | flags::FLAG_REPLY,
                TOKEN,
            );
            socket
                .set_read_timeout(Some(Duration::from_millis(50)))
                .unwrap();
            let mut buffer = [0_u8; 512];
            if let Ok((len, _)) = socket.recv_from(&mut buffer) {
                tx.send(buffer[..len].to_vec()).unwrap();
            }
        });
        runtime().block_on(async {
            let mut client_config = config(expiry_server.addr, None, 0);
            client_config.negotiation_policy = crate::NegotiationPolicy::Loose;
            let mut client = AsyncClient::connect(client_config).await.unwrap();
            client.open().await.unwrap();
            time::sleep(Duration::from_millis(5)).await;
            assert!(client.send_probe().await.unwrap().is_empty());
            assert_eq!(client.machine.packets_sent(), 0);
            assert!(client.prepared_probe.is_none());
        });
        thread::sleep(Duration::from_millis(75));
        assert_eq!(expiry_server.packets.iter().take(1).count(), 1);
        assert!(expiry_server.packets.try_recv().is_err());
        expiry_server.join();

        let pending_server = start_server(move |socket, tx| {
            let (open_packet, peer) = recv_packet(&socket, &tx);
            let request = decode_open_request(&open_packet, None).unwrap();
            send_open_reply(
                &socket,
                peer,
                request.params,
                None,
                flags::FLAG_OPEN | flags::FLAG_REPLY,
                TOKEN,
            );
            let _ = recv_packet(&socket, &tx);
            socket
                .set_read_timeout(Some(Duration::from_millis(50)))
                .unwrap();
            let mut buffer = [0_u8; 512];
            if let Ok((len, _)) = socket.recv_from(&mut buffer) {
                tx.send(buffer[..len].to_vec()).unwrap();
            }
        });
        runtime().block_on(async {
            let mut client_config = config(pending_server.addr, None, 0);
            client_config.max_pending_probes = 1;
            let mut client = AsyncClient::connect(client_config).await.unwrap();
            client.open().await.unwrap();
            client.send_probe().await.unwrap();
            assert!(matches!(
                client.send_probe().await,
                Err(ClientError::PendingLimitExceeded { limit: 1 })
            ));
            assert_eq!(client.machine.packets_sent(), 1);
        });
        thread::sleep(Duration::from_millis(75));
        assert_eq!(pending_server.packets.iter().take(2).count(), 2);
        assert!(pending_server.packets.try_recv().is_err());
        pending_server.join();
    }

    #[test]
    fn timed_out_probe_reply_is_classified_late() {
        let server = start_server(move |socket, tx| {
            let (open_packet, peer) = recv_packet(&socket, &tx);
            let request = decode_open_request(&open_packet, None).unwrap();
            send_open_reply(
                &socket,
                peer,
                request.params.clone(),
                None,
                flags::FLAG_OPEN | flags::FLAG_REPLY,
                TOKEN,
            );
            let (probe_packet, _) = recv_packet(&socket, &tx);
            let probe = decode_echo_request(&probe_packet, &request.params, None).unwrap();
            thread::sleep(Duration::from_millis(30));
            socket
                .send_to(
                    &echo_reply(
                        &request.params,
                        probe.sequence,
                        TOKEN,
                        flags::FLAG_REPLY,
                        None,
                    ),
                    peer,
                )
                .unwrap();
        });

        runtime().block_on(async {
            let mut client_config = config(server.addr, None, 0);
            client_config.probe_timeout = Duration::from_millis(5);
            let mut client = AsyncClient::connect(client_config).await.unwrap();
            client.open().await.unwrap();
            client.send_probe().await.unwrap();
            time::sleep(Duration::from_millis(10)).await;
            assert!(matches!(
                client.poll_timeouts().unwrap().as_slice(),
                [ClientEvent::EchoLoss { seq: 0, .. }]
            ));
            assert!(matches!(
                client.recv().await.unwrap().as_slice(),
                [ClientEvent::LateReply { seq: 0, .. }]
            ));
        });
        server.join();
    }

    #[test]
    fn short_probe_and_close_commit_before_length_error() {
        let server = start_server(move |socket, tx| {
            let (open_packet, peer) = recv_packet(&socket, &tx);
            let request = decode_open_request(&open_packet, None).unwrap();
            send_open_reply(
                &socket,
                peer,
                request.params,
                None,
                flags::FLAG_OPEN | flags::FLAG_REPLY,
                TOKEN,
            );
        });

        runtime().block_on(async {
            let mut client = AsyncClient::connect(config(server.addr, None, 0))
                .await
                .unwrap();
            client.open().await.unwrap();
            client.test_hooks.pause_probe_before_writable.set(true);
            assert!(
                time::timeout(Duration::from_millis(10), client.send_probe(),)
                    .await
                    .is_err()
            );
            let probe_len = client.prepared_probe.as_ref().unwrap().bytes.len();
            client
                .test_hooks
                .sends
                .borrow_mut()
                .push_back(InjectedSend::ReportedLength(probe_len - 1));
            assert!(matches!(
                client.send_probe().await,
                Err(ClientError::DatagramLengthMismatch { .. })
            ));
            assert_eq!(client.machine.packets_sent(), 1);
            assert!(client.prepared_probe.is_none());

            let close_len = client.machine.prepare_close().unwrap().bytes.len();
            client
                .test_hooks
                .sends
                .borrow_mut()
                .push_back(InjectedSend::ReportedLength(close_len - 1));
            assert!(matches!(
                client.close().await,
                Err(ClientError::DatagramLengthMismatch { .. })
            ));
            assert!(!client.machine.is_open());
            assert!(client.schedule.is_none());
            assert!(matches!(
                client.close().await,
                Err(ClientError::AlreadyClosed)
            ));
        });
        server.join();
    }

    #[test]
    fn close_cancellation_would_block_and_error_restore_dscp() {
        let server = start_server(move |socket, tx| {
            let (open_packet, peer) = recv_packet(&socket, &tx);
            let request = decode_open_request(&open_packet, None).unwrap();
            send_open_reply(
                &socket,
                peer,
                request.params,
                None,
                flags::FLAG_OPEN | flags::FLAG_REPLY,
                TOKEN,
            );
            let (close, _) = recv_packet(&socket, &tx);
            assert_eq!(decode_close_request(&close, None).unwrap().token, TOKEN);
        });

        runtime().block_on(async {
            let mut client = AsyncClient::connect(config(server.addr, None, 46))
                .await
                .unwrap();
            client.open().await.unwrap();
            assert_eq!(
                tokio_socket_traffic_class(&client.socket, client.remote,).unwrap() & 0xfc,
                46 << 2
            );
            let deadline_before_recv = client.next_send_deadline();
            assert!(time::timeout(Duration::from_millis(10), client.recv(),)
                .await
                .is_err());
            assert!(client.machine.is_open());
            assert_eq!(client.next_send_deadline(), deadline_before_recv);
            assert_eq!(client.applied_dscp, Some(46));

            client.test_hooks.pause_close_before_writable.set(true);
            assert!(time::timeout(Duration::from_millis(10), client.close(),)
                .await
                .is_err());
            assert!(client.machine.is_open());
            assert_eq!(client.applied_dscp, Some(46));

            client
                .test_hooks
                .sends
                .borrow_mut()
                .push_back(InjectedSend::Error);
            assert!(matches!(client.close().await, Err(ClientError::Socket(_))));
            assert!(client.machine.is_open());
            assert!(client.schedule.is_some());
            assert_eq!(
                tokio_socket_traffic_class(&client.socket, client.remote,).unwrap() & 0xfc,
                46 << 2
            );

            client
                .test_hooks
                .sends
                .borrow_mut()
                .push_back(InjectedSend::WouldBlock);
            assert!(matches!(
                client.close().await.unwrap().as_slice(),
                [ClientEvent::SessionClosed { .. }]
            ));
            assert!(client.schedule.is_none());
            assert_eq!(client.applied_dscp, None);
        });
        assert_eq!(server.packets.iter().take(2).count(), 2);
        server.join();
    }

    #[test]
    fn receive_classification_and_peer_close_cleanup_are_shared() {
        let server = start_server(move |socket, tx| {
            let (open_packet, peer) = recv_packet(&socket, &tx);
            let request = decode_open_request(&open_packet, None).unwrap();
            send_open_reply(
                &socket,
                peer,
                request.params.clone(),
                None,
                flags::FLAG_OPEN | flags::FLAG_REPLY,
                TOKEN,
            );
            let (probe_packet, _) = recv_packet(&socket, &tx);
            let probe = decode_echo_request(&probe_packet, &request.params, None).unwrap();
            socket.send_to(&[0_u8], peer).unwrap();
            socket
                .send_to(
                    &echo_reply(
                        &request.params,
                        probe.sequence,
                        TOKEN + 1,
                        flags::FLAG_REPLY,
                        None,
                    ),
                    peer,
                )
                .unwrap();
            let normal = echo_reply(
                &request.params,
                probe.sequence,
                TOKEN,
                flags::FLAG_REPLY,
                None,
            );
            socket.send_to(&normal, peer).unwrap();
            socket.send_to(&normal, peer).unwrap();
            socket
                .send_to(
                    &echo_reply(
                        &request.params,
                        probe.sequence,
                        TOKEN,
                        flags::FLAG_REPLY | flags::FLAG_CLOSE,
                        None,
                    ),
                    peer,
                )
                .unwrap();
        });

        runtime().block_on(async {
            let mut client = AsyncClient::connect(config(server.addr, None, 46))
                .await
                .unwrap();
            client.open().await.unwrap();
            client.send_probe().await.unwrap();
            client.test_hooks.receive_would_block.set(2);

            assert!(matches!(
                client.recv().await.unwrap().as_slice(),
                [ClientEvent::Warning {
                    kind: WarningKind::MalformedOrUnrelatedPacket,
                    ..
                }]
            ));
            assert!(matches!(
                client.recv().await.unwrap().as_slice(),
                [ClientEvent::Warning {
                    kind: WarningKind::WrongToken,
                    ..
                }]
            ));
            assert!(matches!(
                client.recv().await.unwrap().as_slice(),
                [ClientEvent::EchoReply { .. }]
            ));
            assert!(matches!(
                client.recv().await.unwrap().as_slice(),
                [ClientEvent::DuplicateReply { .. }]
            ));
            assert!(matches!(
                client.recv().await.unwrap().as_slice(),
                [
                    ClientEvent::DuplicateReply { .. },
                    ClientEvent::SessionClosed { .. }
                ]
            ));
            assert!(client.is_peer_closed());
            assert!(client.schedule.is_none());
            assert_eq!(client.applied_dscp, None);
            assert_eq!(
                tokio_socket_traffic_class(&client.socket, client.remote,).unwrap() & 0xfc,
                0
            );
        });
        server.join();
    }

    #[test]
    fn no_test_and_timeout_paths_match_blocking_semantics() {
        let no_test_server = start_server(move |socket, tx| {
            let (open_packet, peer) = recv_packet(&socket, &tx);
            let request = decode_open_request(&open_packet, None).unwrap();
            assert!(request.close);
            send_open_reply(
                &socket,
                peer,
                request.params,
                None,
                flags::FLAG_OPEN | flags::FLAG_REPLY | flags::FLAG_CLOSE,
                0,
            );
        });
        runtime().block_on(async {
            let mut config = config(no_test_server.addr, None, 0);
            config.run_mode = crate::RunMode::NoTest;
            let mut client = AsyncClient::connect(config).await.unwrap();
            assert!(matches!(
                client.open().await.unwrap(),
                OpenOutcome::NoTestCompleted { .. }
            ));
            assert!(client.is_run_complete());
            assert!(matches!(
                client.close().await,
                Err(ClientError::AlreadyCompleted)
            ));
        });
        no_test_server.join();

        let timeout_server = start_server(move |socket, tx| {
            let (open_packet, peer) = recv_packet(&socket, &tx);
            let request = decode_open_request(&open_packet, None).unwrap();
            send_open_reply(
                &socket,
                peer,
                request.params,
                None,
                flags::FLAG_OPEN | flags::FLAG_REPLY,
                TOKEN,
            );
            let _ = recv_packet(&socket, &tx);
        });
        runtime().block_on(async {
            let mut config = config(timeout_server.addr, None, 0);
            config.probe_timeout = Duration::from_millis(5);
            let mut client = AsyncClient::connect(config).await.unwrap();
            client.open().await.unwrap();
            client.send_probe().await.unwrap();
            time::sleep(Duration::from_millis(10)).await;
            assert!(matches!(
                client.poll_timeouts().unwrap().as_slice(),
                [ClientEvent::EchoLoss { seq: 0, .. }]
            ));
        });
        timeout_server.join();
    }

    fn open_negotiated(outcome: &OpenOutcome) -> &crate::NegotiatedParams {
        match outcome {
            OpenOutcome::Started { negotiated, .. }
            | OpenOutcome::NoTestCompleted { negotiated, .. } => negotiated,
        }
    }

    fn assert_matching_event_shape(blocking: &ClientEvent, asynchronous: &ClientEvent) {
        match (blocking, asynchronous) {
            (
                ClientEvent::EchoSent {
                    seq: left_seq,
                    bytes: left_bytes,
                    ..
                },
                ClientEvent::EchoSent {
                    seq: right_seq,
                    bytes: right_bytes,
                    ..
                },
            ) => {
                assert_eq!(left_seq, right_seq);
                assert_eq!(left_bytes, right_bytes);
            }
            (
                ClientEvent::EchoReply {
                    seq: left_seq,
                    bytes: left_bytes,
                    ..
                },
                ClientEvent::EchoReply {
                    seq: right_seq,
                    bytes: right_bytes,
                    ..
                },
            ) => {
                assert_eq!(left_seq, right_seq);
                assert_eq!(left_bytes, right_bytes);
            }
            (
                ClientEvent::SessionClosed {
                    token: left_token, ..
                },
                ClientEvent::SessionClosed {
                    token: right_token, ..
                },
            ) => assert_eq!(left_token, right_token),
            pair => panic!("event shapes differ: {pair:?}"),
        }
    }
}
