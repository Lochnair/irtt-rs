#![forbid(unsafe_code)]

use std::{
    io,
    net::{SocketAddr, ToSocketAddrs, UdpSocket},
    time::{Duration, Instant, SystemTime},
};

use irtt_proto::{
    close::CloseRequest, decode_open_reply, encode_close_request, encode_open_request, flags,
    Clock, OpenRequest, Params, ReceivedStats, ServerFill, StampAt, PROTOCOL_VERSION,
};
use socket2::{Domain, Protocol, Socket, Type};
use thiserror::Error;

const DEFAULT_PORT: u16 = 2112;
const DEFAULT_DURATION: Duration = Duration::from_secs(3);
const DEFAULT_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_OPEN_TIMEOUTS: [Duration; 4] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(8),
];
const MIN_OPEN_TIMEOUT: Duration = Duration::from_millis(200);
const MAX_OPEN_PACKET_SIZE: usize = 512;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfig {
    pub server_addr: String,
    pub duration: Option<Duration>,
    pub interval: Duration,
    pub length: u32,
    pub received_stats: ReceivedStats,
    pub stamp_at: StampAt,
    pub clock: Clock,
    pub dscp: u8,
    pub hmac_key: Option<Vec<u8>>,
    pub server_fill: Option<String>,
    pub open_timeouts: Vec<Duration>,
    pub run_mode: RunMode,
    pub negotiation_policy: NegotiationPolicy,
    pub socket_config: SocketConfig,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            server_addr: format!("127.0.0.1:{DEFAULT_PORT}"),
            duration: Some(DEFAULT_DURATION),
            interval: DEFAULT_INTERVAL,
            length: 0,
            received_stats: ReceivedStats::Both,
            stamp_at: StampAt::Both,
            clock: Clock::Both,
            dscp: 0,
            hmac_key: None,
            server_fill: None,
            open_timeouts: DEFAULT_OPEN_TIMEOUTS.to_vec(),
            run_mode: RunMode::Normal,
            negotiation_policy: NegotiationPolicy::Strict,
            socket_config: SocketConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SocketConfig {
    pub bind_addr: Option<SocketAddr>,
    pub ttl: Option<u32>,
    pub ipv4_only: bool,
    pub ipv6_only: bool,
    pub recv_timeout: Option<Duration>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NegotiationPolicy {
    Strict,
    Loose,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunMode {
    Normal,
    NoTest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegotiatedParams {
    pub params: Params,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenOutcome {
    Started {
        remote: SocketAddr,
        token: u64,
        negotiated: NegotiatedParams,
        event: ClientEvent,
    },
    NoTestCompleted {
        remote: SocketAddr,
        event: ClientEvent,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientEvent {
    SessionStarted {
        remote: SocketAddr,
        token: u64,
        negotiated: NegotiatedParams,
        at: ClientTimestamp,
    },
    NoTestCompleted {
        remote: SocketAddr,
        at: ClientTimestamp,
    },
    SessionClosed {
        remote: SocketAddr,
        token: u64,
        at: ClientTimestamp,
    },
    Warning {
        message: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientTimestamp {
    pub wall: SystemTime,
    pub mono: Instant,
}

impl ClientTimestamp {
    pub fn now() -> Self {
        Self {
            wall: SystemTime::now(),
            mono: Instant::now(),
        }
    }
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("failed to resolve server address {addr:?}")]
    Resolve { addr: String },
    #[error("socket error: {0}")]
    Socket(#[from] io::Error),
    #[error("protocol error: {0}")]
    Protocol(#[from] irtt_proto::ProtoError),
    #[error("all open requests timed out")]
    OpenTimeout,
    #[error("open timeout {timeout:?} is below the minimum {minimum:?}")]
    OpenTimeoutTooSmall {
        timeout: Duration,
        minimum: Duration,
    },
    #[error("server rejected the open request")]
    ServerRejected,
    #[error("protocol version mismatch: requested {requested}, received {received}")]
    ProtocolVersionMismatch { requested: i64, received: i64 },
    #[error("server returned a zero connection token")]
    ZeroToken,
    #[error("strict negotiation rejected changed params: {reason}")]
    NegotiationRejected { reason: String },
    #[error("client is not open")]
    NotOpen,
    #[error("client session is already closed")]
    AlreadyClosed,
    #[error("operation is not implemented for this milestone")]
    NotImplementedForMilestone,
    #[error("duration is too large to encode as nanoseconds")]
    DurationOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClientState {
    Connected,
    Open { token: u64 },
    NoTestCompleted,
    Closed,
}

#[derive(Debug)]
pub struct Client {
    config: ClientConfig,
    socket: UdpSocket,
    remote: SocketAddr,
    requested: Params,
    negotiated: Option<NegotiatedParams>,
    state: ClientState,
}

impl Client {
    pub fn connect(config: ClientConfig) -> Result<Self, ClientError> {
        validate_open_timeouts(&config.open_timeouts)?;
        let remote = resolve_remote(&config)?;
        let socket = connect_udp_socket(&config.socket_config, remote)?;
        let requested = params_from_config(&config)?;

        Ok(Self {
            config,
            socket,
            remote,
            requested,
            negotiated: None,
            state: ClientState::Connected,
        })
    }

    pub fn open(&mut self, now: ClientTimestamp) -> Result<OpenOutcome, ClientError> {
        validate_open_timeouts(&self.config.open_timeouts)?;
        let request = OpenRequest {
            params: self.requested.clone(),
            close: self.config.run_mode == RunMode::NoTest,
        };
        let packet = encode_open_request(&request, self.config.hmac_key.as_deref())?;
        let mut buf = [0_u8; MAX_OPEN_PACKET_SIZE];

        for timeout in &self.config.open_timeouts {
            self.socket.set_read_timeout(Some(*timeout))?;
            self.socket.send(&packet)?;

            match self.socket.recv(&mut buf) {
                Ok(size) => {
                    let reply = decode_open_reply(&buf[..size], self.config.hmac_key.as_deref())?;
                    return self.accept_open_reply(reply, now);
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
    }

    pub fn close(&mut self, now: ClientTimestamp) -> Result<Vec<ClientEvent>, ClientError> {
        let token = match self.state {
            ClientState::Open { token } => token,
            ClientState::Closed => return Err(ClientError::AlreadyClosed),
            ClientState::Connected | ClientState::NoTestCompleted => {
                return Err(ClientError::NotOpen)
            }
        };

        let packet =
            encode_close_request(&CloseRequest { token }, self.config.hmac_key.as_deref())?;
        self.socket.send(&packet)?;
        self.state = ClientState::Closed;

        Ok(vec![ClientEvent::SessionClosed {
            remote: self.remote,
            token,
            at: now,
        }])
    }

    fn accept_open_reply(
        &mut self,
        reply: irtt_proto::OpenReply,
        now: ClientTimestamp,
    ) -> Result<OpenOutcome, ClientError> {
        if reply.params.protocol_version != PROTOCOL_VERSION {
            return Err(ClientError::ProtocolVersionMismatch {
                requested: PROTOCOL_VERSION,
                received: reply.params.protocol_version,
            });
        }

        let reply_is_close = flags::has(reply.flags, flags::FLAG_CLOSE);
        match (self.config.run_mode, reply_is_close) {
            (RunMode::Normal, true) => return Err(ClientError::ServerRejected),
            (RunMode::Normal, false) if reply.token == 0 => return Err(ClientError::ZeroToken),
            (RunMode::NoTest, true) => {
                self.state = ClientState::NoTestCompleted;
                let event = ClientEvent::NoTestCompleted {
                    remote: self.remote,
                    at: now,
                };
                return Ok(OpenOutcome::NoTestCompleted {
                    remote: self.remote,
                    event,
                });
            }
            (RunMode::NoTest, false) if reply.token == 0 => return Err(ClientError::ZeroToken),
            (RunMode::NoTest, false) => {}
            (RunMode::Normal, false) => {}
        }

        validate_negotiated_params(
            &self.requested,
            &reply.params,
            self.config.negotiation_policy,
        )?;
        let negotiated = NegotiatedParams {
            params: reply.params.clone(),
        };
        self.negotiated = Some(negotiated.clone());
        self.state = ClientState::Open { token: reply.token };
        let event = ClientEvent::SessionStarted {
            remote: self.remote,
            token: reply.token,
            negotiated: negotiated.clone(),
            at: now,
        };

        Ok(OpenOutcome::Started {
            remote: self.remote,
            token: reply.token,
            negotiated,
            event,
        })
    }
}

fn validate_open_timeouts(timeouts: &[Duration]) -> Result<(), ClientError> {
    for timeout in timeouts {
        if *timeout < MIN_OPEN_TIMEOUT {
            return Err(ClientError::OpenTimeoutTooSmall {
                timeout: *timeout,
                minimum: MIN_OPEN_TIMEOUT,
            });
        }
    }
    Ok(())
}

fn resolve_remote(config: &ClientConfig) -> Result<SocketAddr, ClientError> {
    let addr = with_default_port(&config.server_addr);
    let mut addrs = addr
        .to_socket_addrs()
        .map_err(|_| ClientError::Resolve { addr: addr.clone() })?;
    addrs
        .find(|addr| {
            (!config.socket_config.ipv4_only || addr.is_ipv4())
                && (!config.socket_config.ipv6_only || addr.is_ipv6())
        })
        .ok_or(ClientError::Resolve { addr })
}

fn with_default_port(addr: &str) -> String {
    if addr.parse::<SocketAddr>().is_ok() {
        return addr.to_owned();
    }
    if addr
        .rsplit_once(':')
        .is_some_and(|(_, port)| port.parse::<u16>().is_ok())
    {
        return addr.to_owned();
    }
    format!("{addr}:{DEFAULT_PORT}")
}

fn connect_udp_socket(config: &SocketConfig, remote: SocketAddr) -> Result<UdpSocket, ClientError> {
    let domain = if remote.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;

    if config.ipv6_only && remote.is_ipv6() {
        socket.set_only_v6(true)?;
    }
    if let Some(ttl) = config.ttl {
        socket.set_ttl(ttl)?;
    }

    let bind_addr = config.bind_addr.unwrap_or_else(|| {
        if remote.is_ipv4() {
            SocketAddr::from(([0, 0, 0, 0], 0))
        } else {
            SocketAddr::from(([0_u16; 8], 0))
        }
    });
    socket.bind(&bind_addr.into())?;
    socket.connect(&remote.into())?;

    let socket: UdpSocket = socket.into();
    socket.set_read_timeout(config.recv_timeout)?;
    Ok(socket)
}

fn params_from_config(config: &ClientConfig) -> Result<Params, ClientError> {
    Ok(Params {
        protocol_version: PROTOCOL_VERSION,
        duration_ns: duration_to_ns(config.duration.unwrap_or_default())?,
        interval_ns: duration_to_ns(config.interval)?,
        length: i64::from(config.length),
        received_stats: config.received_stats,
        stamp_at: config.stamp_at,
        clock: config.clock,
        dscp: i64::from(config.dscp),
        server_fill: config.server_fill.clone().map(|value| ServerFill { value }),
    })
}

fn duration_to_ns(duration: Duration) -> Result<i64, ClientError> {
    i64::try_from(duration.as_nanos()).map_err(|_| ClientError::DurationOverflow)
}

fn validate_negotiated_params(
    requested: &Params,
    returned: &Params,
    policy: NegotiationPolicy,
) -> Result<(), ClientError> {
    if returned.protocol_version != PROTOCOL_VERSION {
        return Err(ClientError::ProtocolVersionMismatch {
            requested: PROTOCOL_VERSION,
            received: returned.protocol_version,
        });
    }
    if returned.duration_ns > requested.duration_ns {
        return Err(ClientError::NegotiationRejected {
            reason: "duration increased".to_owned(),
        });
    }
    if returned.length > requested.length {
        return Err(ClientError::NegotiationRejected {
            reason: "length increased".to_owned(),
        });
    }
    if returned.interval_ns <= 0 {
        return Err(ClientError::NegotiationRejected {
            reason: "interval must be positive".to_owned(),
        });
    }

    if policy == NegotiationPolicy::Strict && returned != requested {
        return Err(ClientError::NegotiationRejected {
            reason: "returned params differ from requested params".to_owned(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use irtt_proto::{
        compute_hmac_in_place, flags::FLAG_HMAC, flags::FLAG_OPEN, flags::FLAG_REPLY, verify_hmac,
        HMAC_SIZE, MAGIC,
    };
    use std::{
        net::UdpSocket,
        sync::mpsc,
        thread::{self, JoinHandle},
    };

    const TOKEN: u64 = 0x1234_5678_90ab_cdef;
    const HMAC_OFFSET: usize = 4;

    struct FakeServer {
        addr: SocketAddr,
        rx: mpsc::Receiver<Vec<u8>>,
        done: JoinHandle<()>,
    }

    impl FakeServer {
        fn join(self) {
            self.done.join().unwrap();
        }
    }

    fn default_test_config(addr: SocketAddr) -> ClientConfig {
        ClientConfig {
            server_addr: addr.to_string(),
            open_timeouts: vec![Duration::from_millis(200), Duration::from_millis(200)],
            ..ClientConfig::default()
        }
    }

    fn start_fake_server<F>(handler: F) -> FakeServer
    where
        F: FnOnce(UdpSocket, mpsc::Sender<Vec<u8>>) + Send + 'static,
    {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = socket.local_addr().unwrap();
        let (tx, rx) = mpsc::channel();
        let done = thread::spawn(move || handler(socket, tx));
        FakeServer { addr, rx, done }
    }

    fn recv_request(socket: &UdpSocket, tx: &mpsc::Sender<Vec<u8>>) -> (Vec<u8>, SocketAddr) {
        let mut buf = [0_u8; 512];
        let (size, peer) = socket.recv_from(&mut buf).unwrap();
        let packet = buf[..size].to_vec();
        tx.send(packet.clone()).unwrap();
        (packet, peer)
    }

    fn open_reply(flags: u8, token: u64, params: &Params, hmac_key: Option<&[u8]>) -> Vec<u8> {
        let mut packet = Vec::new();
        packet.extend_from_slice(&MAGIC);
        packet.push(flags | hmac_key.map_or(0, |_| FLAG_HMAC));
        if hmac_key.is_some() {
            packet.extend_from_slice(&[0_u8; HMAC_SIZE]);
        }
        packet.extend_from_slice(&token.to_le_bytes());
        packet.extend_from_slice(&params.encode());
        if let Some(key) = hmac_key {
            compute_hmac_in_place(key, &mut packet, HMAC_OFFSET).unwrap();
        }
        packet
    }

    fn open_success_server(params: Params) -> FakeServer {
        start_fake_server(move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
            socket.send_to(&reply, peer).unwrap();
        })
    }

    fn assert_open_started(outcome: OpenOutcome) -> NegotiatedParams {
        match outcome {
            OpenOutcome::Started {
                token, negotiated, ..
            } => {
                assert_eq!(token, TOKEN);
                negotiated
            }
            OpenOutcome::NoTestCompleted { .. } => panic!("unexpected no-test outcome"),
        }
    }

    #[test]
    fn client_config_default() {
        let config = ClientConfig::default();
        assert_eq!(config.duration, Some(Duration::from_secs(3)));
        assert_eq!(config.interval, Duration::from_secs(1));
        assert_eq!(config.length, 0);
        assert_eq!(config.received_stats, ReceivedStats::Both);
        assert_eq!(config.stamp_at, StampAt::Both);
        assert_eq!(config.clock, Clock::Both);
        assert_eq!(config.dscp, 0);
        assert_eq!(config.hmac_key, None);
        assert_eq!(config.server_fill, None);
        assert_eq!(config.open_timeouts, DEFAULT_OPEN_TIMEOUTS);
        assert_eq!(config.run_mode, RunMode::Normal);
        assert_eq!(config.negotiation_policy, NegotiationPolicy::Strict);
    }

    #[test]
    fn address_resolution_connects_to_local_fake_server() {
        let server = start_fake_server(|socket, tx| {
            let _ = recv_request(&socket, &tx);
        });
        let client = Client::connect(default_test_config(server.addr)).unwrap();
        client.socket.send(b"ping").unwrap();
        assert_eq!(server.rx.recv().unwrap(), b"ping");
        server.join();
    }

    #[test]
    fn successful_open_handshake() {
        let config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
        let params = params_from_config(&config).unwrap();
        let server = open_success_server(params.clone());
        let mut client = Client::connect(default_test_config(server.addr)).unwrap();

        let negotiated = assert_open_started(client.open(ClientTimestamp::now()).unwrap());
        assert_eq!(negotiated.params, params);
        assert!(matches!(client.state, ClientState::Open { token: TOKEN }));
        server.join();
    }

    #[test]
    fn open_retries_after_first_timeout() {
        let server = start_fake_server(|socket, tx| {
            let (first, _) = recv_request(&socket, &tx);
            let (_, peer) = recv_request(&socket, &tx);
            let params = Params::decode(&first[4..]).unwrap();
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
            assert_eq!(first[3] & FLAG_OPEN, FLAG_OPEN);
            socket.send_to(&reply, peer).unwrap();
        });
        let mut config = default_test_config(server.addr);
        config.open_timeouts = vec![Duration::from_millis(200), Duration::from_millis(500)];
        let params = params_from_config(&config).unwrap();
        let reply_params = params.clone();
        drop(reply_params);
        let mut client = Client::connect(config).unwrap();
        let outcome = client.open(ClientTimestamp::now()).unwrap();
        assert_open_started(outcome);
        assert_eq!(server.rx.iter().take(2).count(), 2);
        server.join();
    }

    #[test]
    fn open_timeout_after_all_timeouts() {
        let server = start_fake_server(|socket, tx| {
            socket
                .set_read_timeout(Some(Duration::from_millis(700)))
                .unwrap();
            while recv_request_timeout(&socket, &tx).is_some() {}
        });
        let mut config = default_test_config(server.addr);
        config.open_timeouts = vec![Duration::from_millis(200), Duration::from_millis(200)];
        let mut client = Client::connect(config).unwrap();
        assert!(matches!(
            client.open(ClientTimestamp::now()),
            Err(ClientError::OpenTimeout)
        ));
        assert_eq!(server.rx.iter().take(2).count(), 2);
        server.join();
    }

    fn recv_request_timeout(
        socket: &UdpSocket,
        tx: &mpsc::Sender<Vec<u8>>,
    ) -> Option<(Vec<u8>, SocketAddr)> {
        let mut buf = [0_u8; 512];
        match socket.recv_from(&mut buf) {
            Ok((size, peer)) => {
                let packet = buf[..size].to_vec();
                tx.send(packet.clone()).unwrap();
                Some((packet, peer))
            }
            Err(_) => None,
        }
    }

    #[test]
    fn strict_negotiation_accepts_identical_params() {
        let config = ClientConfig::default();
        let params = params_from_config(&config).unwrap();
        assert!(validate_negotiated_params(&params, &params, NegotiationPolicy::Strict).is_ok());
    }

    #[test]
    fn strict_negotiation_rejects_changed_params() {
        let config = ClientConfig::default();
        let requested = params_from_config(&config).unwrap();
        let mut returned = requested.clone();
        returned.dscp = 1;
        assert!(matches!(
            validate_negotiated_params(&requested, &returned, NegotiationPolicy::Strict),
            Err(ClientError::NegotiationRejected { .. })
        ));
    }

    #[test]
    fn loose_negotiation_accepts_server_restricted_params() {
        let config = ClientConfig::default();
        let requested = params_from_config(&config).unwrap();
        let mut returned = requested.clone();
        returned.duration_ns /= 2;
        returned.length = 0;
        assert!(
            validate_negotiated_params(&requested, &returned, NegotiationPolicy::Loose).is_ok()
        );
    }

    #[test]
    fn protocol_version_mismatch_fails() {
        let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
        config.negotiation_policy = NegotiationPolicy::Loose;
        let mut params = params_from_config(&config).unwrap();
        params.protocol_version = 2;
        let server = open_success_server(params);
        let mut client = Client::connect(default_test_config(server.addr)).unwrap();
        assert!(matches!(
            client.open(ClientTimestamp::now()),
            Err(ClientError::ProtocolVersionMismatch { received: 2, .. })
        ));
        server.join();
    }

    #[test]
    fn server_rejection_fails_in_normal_mode() {
        let config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
        let params = params_from_config(&config).unwrap();
        let server = start_fake_server(move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY | flags::FLAG_CLOSE, 0, &params, None);
            socket.send_to(&reply, peer).unwrap();
        });
        let mut client = Client::connect(default_test_config(server.addr)).unwrap();
        assert!(matches!(
            client.open(ClientTimestamp::now()),
            Err(ClientError::ServerRejected)
        ));
        server.join();
    }

    #[test]
    fn no_test_open_close_succeeds_on_open_reply_close() {
        let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
        config.run_mode = RunMode::NoTest;
        let params = params_from_config(&config).unwrap();
        let server = start_fake_server(move |socket, tx| {
            let (request, peer) = recv_request(&socket, &tx);
            assert_eq!(request[3] & flags::FLAG_CLOSE, flags::FLAG_CLOSE);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY | flags::FLAG_CLOSE, 0, &params, None);
            socket.send_to(&reply, peer).unwrap();
        });
        config.server_addr = server.addr.to_string();
        let mut client = Client::connect(config).unwrap();
        assert!(matches!(
            client.open(ClientTimestamp::now()).unwrap(),
            OpenOutcome::NoTestCompleted { .. }
        ));
        assert!(matches!(
            client.close(ClientTimestamp::now()),
            Err(ClientError::NotOpen)
        ));
        server.join();
    }

    #[test]
    fn close_sends_one_close_packet_with_negotiated_token() {
        let config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
        let params = params_from_config(&config).unwrap();
        let server = start_fake_server(move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
            socket.send_to(&reply, peer).unwrap();
            let _ = recv_request(&socket, &tx);
        });
        let mut client = Client::connect(default_test_config(server.addr)).unwrap();
        assert_open_started(client.open(ClientTimestamp::now()).unwrap());
        let events = client.close(ClientTimestamp::now()).unwrap();
        assert_eq!(events.len(), 1);
        let packets: Vec<_> = server.rx.iter().take(2).collect();
        let close = &packets[1];
        assert_eq!(close[3], flags::FLAG_CLOSE);
        assert_eq!(u64::from_le_bytes(close[4..12].try_into().unwrap()), TOKEN);
        server.join();
    }

    #[test]
    fn hmac_open_success() {
        let key = b"secret".to_vec();
        let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
        config.hmac_key = Some(key.clone());
        let params = params_from_config(&config).unwrap();
        let server = start_fake_server(move |socket, tx| {
            let (request, peer) = recv_request(&socket, &tx);
            verify_hmac(&key, &request, HMAC_OFFSET).unwrap();
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, Some(&key));
            socket.send_to(&reply, peer).unwrap();
        });
        config.server_addr = server.addr.to_string();
        let mut client = Client::connect(config).unwrap();
        assert_open_started(client.open(ClientTimestamp::now()).unwrap());
        server.join();
    }

    #[test]
    fn hmac_open_rejects_missing_hmac() {
        let key = b"secret".to_vec();
        let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
        config.hmac_key = Some(key);
        let params = params_from_config(&config).unwrap();
        let server = start_fake_server(move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
            socket.send_to(&reply, peer).unwrap();
        });
        config.server_addr = server.addr.to_string();
        let mut client = Client::connect(config).unwrap();
        assert!(matches!(
            client.open(ClientTimestamp::now()),
            Err(ClientError::Protocol(
                irtt_proto::ProtoError::HmacPresenceMismatch
            ))
        ));
        server.join();
    }

    #[test]
    fn hmac_open_rejects_bad_hmac() {
        let key = b"secret".to_vec();
        let wrong_key = b"wrong".to_vec();
        let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
        config.hmac_key = Some(key);
        let params = params_from_config(&config).unwrap();
        let server = start_fake_server(move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, Some(&wrong_key));
            socket.send_to(&reply, peer).unwrap();
        });
        config.server_addr = server.addr.to_string();
        let mut client = Client::connect(config).unwrap();
        assert!(matches!(
            client.open(ClientTimestamp::now()),
            Err(ClientError::Protocol(irtt_proto::ProtoError::BadHmac))
        ));
        server.join();
    }

    #[test]
    fn hmac_close_packet_includes_valid_hmac() {
        let key = b"secret".to_vec();
        let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
        config.hmac_key = Some(key.clone());
        let params = params_from_config(&config).unwrap();
        let server_key = key.clone();
        let server = start_fake_server(move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, Some(&server_key));
            socket.send_to(&reply, peer).unwrap();
            let _ = recv_request(&socket, &tx);
        });
        config.server_addr = server.addr.to_string();
        let mut client = Client::connect(config).unwrap();
        assert_open_started(client.open(ClientTimestamp::now()).unwrap());
        client.close(ClientTimestamp::now()).unwrap();
        let packets: Vec<_> = server.rx.iter().take(2).collect();
        let close = &packets[1];
        assert_eq!(close[3], flags::FLAG_CLOSE | FLAG_HMAC);
        verify_hmac(&key, close, HMAC_OFFSET).unwrap();
        assert_eq!(
            u64::from_le_bytes(close[4 + HMAC_SIZE..12 + HMAC_SIZE].try_into().unwrap()),
            TOKEN
        );
        server.join();
    }

    #[test]
    fn minimum_open_timeout_under_200ms_is_rejected() {
        let config = ClientConfig {
            open_timeouts: vec![Duration::from_millis(199)],
            ..ClientConfig::default()
        };
        assert!(matches!(
            Client::connect(config),
            Err(ClientError::OpenTimeoutTooSmall { .. })
        ));
    }
}
