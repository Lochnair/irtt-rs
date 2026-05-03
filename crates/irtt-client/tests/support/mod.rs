use std::{
    net::{SocketAddr, UdpSocket},
    sync::mpsc,
    thread::{self, JoinHandle},
    time::{Duration, SystemTime},
};

use irtt_client::{
    Client, ClientConfig, ClientEvent, ClientTimestamp, NegotiatedParams, OpenOutcome, SocketConfig,
};
use irtt_proto::{
    compute_hmac_in_place, echo_packet_len, flags, verify_hmac, Clock, Params, ReceivedStats,
    ServerFill, StampAt, TimestampFields, HMAC_SIZE, MAGIC, PROTOCOL_VERSION,
};

const HMAC_OFFSET: usize = 4;
pub const TOKEN: u64 = 0x1234_5678_90ab_cdef;
pub const RECV_COUNT: u32 = 42;
pub const RECV_WINDOW: u64 = 0x07;

pub struct FakeServer {
    pub addr: SocketAddr,
    rx: mpsc::Receiver<ServerObservation>,
    done: JoinHandle<()>,
}

impl FakeServer {
    pub fn observations(&self, count: usize) -> Vec<ServerObservation> {
        self.rx.iter().take(count).collect()
    }

    pub fn join(self) {
        self.done.join().unwrap();
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerObservation {
    Open {
        params: Params,
        hmac: bool,
    },
    Echo {
        len: usize,
        hmac: bool,
        token: u64,
        sequence: u32,
    },
}

pub struct OneProbeRun {
    pub negotiated: NegotiatedParams,
    pub sent: ClientEvent,
    pub reply: ClientEvent,
    pub observations: Vec<ServerObservation>,
}

pub fn default_params() -> Params {
    Params {
        protocol_version: PROTOCOL_VERSION,
        duration_ns: 3_000_000_000,
        interval_ns: 1_000_000_000,
        received_stats: ReceivedStats::Both,
        stamp_at: StampAt::Both,
        clock: Clock::Both,
        ..Params::default()
    }
}

pub fn params_for_modes(received_stats: ReceivedStats, stamp_at: StampAt, clock: Clock) -> Params {
    Params {
        received_stats,
        stamp_at,
        clock,
        ..default_params()
    }
}

pub fn config_for_params(addr: SocketAddr, params: &Params) -> ClientConfig {
    ClientConfig {
        server_addr: addr.to_string(),
        duration: if params.duration_ns == 0 {
            None
        } else {
            Some(Duration::from_nanos(params.duration_ns as u64))
        },
        interval: Duration::from_nanos(params.interval_ns as u64),
        length: u32::try_from(params.length).unwrap(),
        received_stats: params.received_stats,
        stamp_at: params.stamp_at,
        clock: params.clock,
        dscp: u8::try_from(params.dscp).unwrap(),
        server_fill: params.server_fill.as_ref().map(|fill| fill.value.clone()),
        open_timeouts: vec![Duration::from_millis(200)],
        socket_config: SocketConfig {
            recv_timeout: Some(Duration::from_millis(500)),
            ..Default::default()
        },
        ..ClientConfig::default()
    }
}

pub fn standard_timestamps() -> TimestampFields {
    TimestampFields {
        recv_wall: Some(0),
        recv_mono: Some(5_000_000),
        midpoint_wall: Some(0),
        midpoint_mono: Some(5_000_000),
        send_wall: Some(1),
        send_mono: Some(5_000_001),
    }
}

pub fn run_one_probe(params: Params, timestamps: TimestampFields) -> OneProbeRun {
    let config_params = params.clone();
    run_one_probe_with_config(params, timestamps, None, |addr| {
        config_for_params(addr, &config_params)
    })
}

pub fn run_one_probe_with_config<F>(
    server_params: Params,
    timestamps: TimestampFields,
    hmac_key: Option<Vec<u8>>,
    build_config: F,
) -> OneProbeRun
where
    F: FnOnce(SocketAddr) -> ClientConfig,
{
    let server = start_one_probe_server(server_params, timestamps, hmac_key);
    let mut client = Client::connect(build_config(server.addr)).unwrap();
    let negotiated = assert_started(client.open(ClientTimestamp::now()).unwrap());

    let sent_events = client.send_probe().unwrap();
    assert_eq!(sent_events.len(), 1);

    let reply_events = client.recv_once().unwrap();
    assert_eq!(reply_events.len(), 1);

    let observations = server.observations(2);
    server.join();

    OneProbeRun {
        negotiated,
        sent: sent_events.into_iter().next().unwrap(),
        reply: reply_events.into_iter().next().unwrap(),
        observations,
    }
}

pub fn start_open_server(params: Params, hmac_key: Option<Vec<u8>>) -> FakeServer {
    start_fake_server(move |socket, tx| {
        let (request, peer) = recv_request(&socket);
        let (open_params, hmac) = decode_open_request_params(&request, hmac_key.as_deref());
        tx.send(ServerObservation::Open {
            params: open_params,
            hmac,
        })
        .unwrap();

        let reply = open_reply(
            flags::FLAG_OPEN | flags::FLAG_REPLY,
            TOKEN,
            &params,
            hmac_key.as_deref(),
        );
        socket.send_to(&reply, peer).unwrap();
    })
}

pub fn server_fill(value: &str) -> Option<ServerFill> {
    Some(ServerFill {
        value: value.to_owned(),
    })
}

fn start_one_probe_server(
    params: Params,
    timestamps: TimestampFields,
    hmac_key: Option<Vec<u8>>,
) -> FakeServer {
    start_fake_server(move |socket, tx| {
        let (request, peer) = recv_request(&socket);
        let (open_params, hmac) = decode_open_request_params(&request, hmac_key.as_deref());
        tx.send(ServerObservation::Open {
            params: open_params,
            hmac,
        })
        .unwrap();

        let reply = open_reply(
            flags::FLAG_OPEN | flags::FLAG_REPLY,
            TOKEN,
            &params,
            hmac_key.as_deref(),
        );
        socket.send_to(&reply, peer).unwrap();

        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let (request, peer) = recv_request(&socket);
        let echo = observe_echo_request(&request, hmac_key.as_deref());
        let sequence = match echo {
            ServerObservation::Echo { sequence, .. } => sequence,
            ServerObservation::Open { .. } => unreachable!(),
        };
        tx.send(echo).unwrap();

        let timestamps = materialize_wall_timestamps(timestamps);
        let reply_packet =
            echo_reply_packet(TOKEN, sequence, &params, &timestamps, hmac_key.as_deref());
        socket.send_to(&reply_packet, peer).unwrap();
    })
}

fn start_fake_server<F>(handler: F) -> FakeServer
where
    F: FnOnce(UdpSocket, mpsc::Sender<ServerObservation>) + Send + 'static,
{
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let addr = socket.local_addr().unwrap();
    let (tx, rx) = mpsc::channel();
    let done = thread::spawn(move || handler(socket, tx));
    FakeServer { addr, rx, done }
}

fn recv_request(socket: &UdpSocket) -> (Vec<u8>, SocketAddr) {
    let mut buf = [0_u8; 2048];
    let (size, peer) = socket.recv_from(&mut buf).unwrap();
    (buf[..size].to_vec(), peer)
}

fn assert_started(outcome: OpenOutcome) -> NegotiatedParams {
    match outcome {
        OpenOutcome::Started { negotiated, .. } => negotiated,
        OpenOutcome::NoTestCompleted { .. } => panic!("expected started outcome"),
    }
}

fn decode_open_request_params(packet: &[u8], hmac_key: Option<&[u8]>) -> (Params, bool) {
    assert_eq!(&packet[..3], &MAGIC);
    assert!(packet[3] & flags::FLAG_OPEN != 0);
    let hmac = packet[3] & flags::FLAG_HMAC != 0;
    assert_eq!(hmac, hmac_key.is_some());
    if let Some(key) = hmac_key {
        verify_hmac(key, packet, HMAC_OFFSET).unwrap();
    }
    let params_offset = 4 + if hmac { HMAC_SIZE } else { 0 };
    (Params::decode(&packet[params_offset..]).unwrap(), hmac)
}

fn observe_echo_request(packet: &[u8], hmac_key: Option<&[u8]>) -> ServerObservation {
    assert_eq!(&packet[..3], &MAGIC);
    assert_eq!(packet[3] & flags::FLAG_OPEN, 0);
    assert_eq!(packet[3] & flags::FLAG_REPLY, 0);
    assert_eq!(packet[3] & flags::FLAG_CLOSE, 0);
    let hmac = packet[3] & flags::FLAG_HMAC != 0;
    assert_eq!(hmac, hmac_key.is_some());
    if let Some(key) = hmac_key {
        verify_hmac(key, packet, HMAC_OFFSET).unwrap();
    }

    let mut pos = 4 + if hmac { HMAC_SIZE } else { 0 };
    let token = u64::from_le_bytes(packet[pos..pos + 8].try_into().unwrap());
    pos += 8;
    let sequence = u32::from_le_bytes(packet[pos..pos + 4].try_into().unwrap());

    ServerObservation::Echo {
        len: packet.len(),
        hmac,
        token,
        sequence,
    }
}

fn open_reply(flags: u8, token: u64, params: &Params, hmac_key: Option<&[u8]>) -> Vec<u8> {
    let mut packet = Vec::new();
    packet.extend_from_slice(&MAGIC);
    packet.push(flags | hmac_key.map_or(0, |_| flags::FLAG_HMAC));
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

fn echo_reply_packet(
    token: u64,
    seq: u32,
    params: &Params,
    timestamps: &TimestampFields,
    hmac_key: Option<&[u8]>,
) -> Vec<u8> {
    let has_hmac = hmac_key.is_some();
    let layout = irtt_proto::PacketLayout::echo(has_hmac, params);
    let packet_len = echo_packet_len(has_hmac, params);
    let mut packet = Vec::with_capacity(packet_len);

    packet.extend_from_slice(&MAGIC);
    packet.push(flags::FLAG_REPLY | hmac_key.map_or(0, |_| flags::FLAG_HMAC));
    if has_hmac {
        packet.extend_from_slice(&[0_u8; HMAC_SIZE]);
    }
    packet.extend_from_slice(&token.to_le_bytes());
    packet.extend_from_slice(&seq.to_le_bytes());

    if layout.recv_count {
        packet.extend_from_slice(&RECV_COUNT.to_le_bytes());
    }
    if layout.recv_window {
        packet.extend_from_slice(&RECV_WINDOW.to_le_bytes());
    }
    if layout.recv_wall {
        packet.extend_from_slice(&timestamps.recv_wall.unwrap_or(0).to_le_bytes());
    }
    if layout.recv_mono {
        packet.extend_from_slice(&timestamps.recv_mono.unwrap_or(0).to_le_bytes());
    }
    if layout.midpoint_wall {
        packet.extend_from_slice(&timestamps.midpoint_wall.unwrap_or(0).to_le_bytes());
    }
    if layout.midpoint_mono {
        packet.extend_from_slice(&timestamps.midpoint_mono.unwrap_or(0).to_le_bytes());
    }
    if layout.send_wall {
        packet.extend_from_slice(&timestamps.send_wall.unwrap_or(0).to_le_bytes());
    }
    if layout.send_mono {
        packet.extend_from_slice(&timestamps.send_mono.unwrap_or(0).to_le_bytes());
    }

    packet.resize(packet_len, 0);
    if let Some(key) = hmac_key {
        compute_hmac_in_place(key, &mut packet, HMAC_OFFSET).unwrap();
    }
    packet
}

fn materialize_wall_timestamps(mut timestamps: TimestampFields) -> TimestampFields {
    let now_ns = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    if let Some(offset) = timestamps.recv_wall {
        timestamps.recv_wall = Some(now_ns + offset);
    }
    if let Some(offset) = timestamps.midpoint_wall {
        timestamps.midpoint_wall = Some(now_ns + offset);
    }
    if let Some(offset) = timestamps.send_wall {
        timestamps.send_wall = Some(now_ns + offset);
    }
    timestamps
}
