use super::*;
use irtt_proto::{
    echo_packet_len as checked_echo_packet_len, encode_echo_reply, encode_open_reply,
    flags::FLAG_OPEN, flags::FLAG_REPLY, layout::PacketLayout, Clock, EchoReply, OpenReply,
    ReceivedStats, StampAt,
};
use std::{
    net::UdpSocket,
    sync::mpsc,
    thread::{self, JoinHandle},
};

pub(super) const TOKEN: u64 = 0x1234_5678_90ab_cdef;
pub(super) const HMAC_OFFSET: usize = 4;

pub(super) struct FakeServer {
    pub(super) addr: SocketAddr,
    pub(super) rx: mpsc::Receiver<Vec<u8>>,
    done: JoinHandle<()>,
}

impl FakeServer {
    pub(super) fn join(self) {
        self.done.join().unwrap();
    }
}

pub(super) fn default_test_config(addr: SocketAddr) -> ClientConfig {
    ClientConfig {
        server_addr: addr.to_string(),
        open_timeouts: vec![Duration::from_millis(200), Duration::from_millis(200)],
        ..ClientConfig::default()
    }
}

pub(super) fn start_fake_server<F>(handler: F) -> FakeServer
where
    F: FnOnce(UdpSocket, mpsc::Sender<Vec<u8>>) + Send + 'static,
{
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let addr = socket.local_addr().unwrap();
    let (tx, rx) = mpsc::channel();
    let done = thread::spawn(move || handler(socket, tx));
    FakeServer { addr, rx, done }
}

pub(super) fn recv_request(
    socket: &UdpSocket,
    tx: &mpsc::Sender<Vec<u8>>,
) -> (Vec<u8>, SocketAddr) {
    let mut buf = [0_u8; 512];
    let (size, peer) = socket.recv_from(&mut buf).unwrap();
    let packet = buf[..size].to_vec();
    tx.send(packet.clone()).unwrap();
    (packet, peer)
}

pub(super) fn open_reply(
    flags: u8,
    token: u64,
    params: &Params,
    hmac_key: Option<&[u8]>,
) -> Vec<u8> {
    encode_open_reply(
        &OpenReply {
            flags,
            token,
            params: params.clone(),
        },
        hmac_key,
    )
    .unwrap()
}

pub(crate) fn echo_packet_len(hmac: bool, params: &Params) -> usize {
    checked_echo_packet_len(hmac, params)
        .expect("test params must have a non-negative packet length")
}

pub(super) fn echo_reply_packet(
    token: u64,
    seq: u32,
    params: &Params,
    timestamps: &TimestampFields,
    hmac_key: Option<&[u8]>,
) -> Vec<u8> {
    echo_reply_packet_with_flags(token, seq, params, timestamps, hmac_key, FLAG_REPLY)
}

pub(super) fn echo_reply_packet_with_flags(
    token: u64,
    seq: u32,
    params: &Params,
    timestamps: &TimestampFields,
    hmac_key: Option<&[u8]>,
    flags: u8,
) -> Vec<u8> {
    let layout = PacketLayout::echo(hmac_key.is_some(), params);
    let reply = EchoReply {
        flags,
        token,
        sequence: seq,
        recv_count: layout.recv_count.then_some(42),
        recv_window: layout.recv_window.then_some(0x07),
        timestamps: TimestampFields {
            recv_wall: layout.recv_wall.then(|| timestamps.recv_wall.unwrap_or(0)),
            recv_mono: layout.recv_mono.then(|| timestamps.recv_mono.unwrap_or(0)),
            midpoint_wall: layout
                .midpoint_wall
                .then(|| timestamps.midpoint_wall.unwrap_or(0)),
            midpoint_mono: layout
                .midpoint_mono
                .then(|| timestamps.midpoint_mono.unwrap_or(0)),
            send_wall: layout.send_wall.then(|| timestamps.send_wall.unwrap_or(0)),
            send_mono: layout.send_mono.then(|| timestamps.send_mono.unwrap_or(0)),
        },
        payload: Vec::new(),
    };
    encode_echo_reply(&reply, params, hmac_key).unwrap()
}

pub(super) fn open_success_server(params: Params) -> FakeServer {
    start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
        socket.send_to(&reply, peer).unwrap();
    })
}

pub(super) fn no_test_server(params: Params, token: u64) -> FakeServer {
    start_fake_server(move |socket, tx| {
        let (request, peer) = recv_request(&socket, &tx);
        assert_eq!(request[3] & flags::FLAG_CLOSE, flags::FLAG_CLOSE);
        let reply = open_reply(
            FLAG_OPEN | FLAG_REPLY | flags::FLAG_CLOSE,
            token,
            &params,
            None,
        );
        socket.send_to(&reply, peer).unwrap();
    })
}

pub(super) fn timeout_server(wait: Duration) -> FakeServer {
    start_fake_server(move |socket, tx| {
        socket.set_read_timeout(Some(wait)).unwrap();
        while recv_request_timeout(&socket, &tx).is_some() {}
    })
}

pub(super) fn recv_request_timeout(
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

pub(super) fn silent_open_server(params: Params) -> FakeServer {
    start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
        socket.send_to(&reply, peer).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        loop {
            let mut buf = [0_u8; 2048];
            match socket.recv_from(&mut buf) {
                Ok((size, _)) => {
                    tx.send(buf[..size].to_vec()).unwrap();
                }
                Err(_) => break,
            }
        }
    })
}

pub(super) fn echo_server(params: Params) -> FakeServer {
    start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
        socket.send_to(&reply, peer).unwrap();

        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        loop {
            let mut buf = [0_u8; 2048];
            let size = match socket.recv_from(&mut buf) {
                Ok((size, _)) => size,
                Err(_) => break,
            };
            let packet = buf[..size].to_vec();
            tx.send(packet.clone()).unwrap();

            if buf[3] & flags::FLAG_CLOSE != 0 {
                break;
            }

            let seq = u32::from_le_bytes(buf[12..16].try_into().unwrap());
            let ts = TimestampFields {
                recv_wall: Some(1_000_000_000),
                recv_mono: Some(100_000),
                send_wall: Some(1_000_100_000),
                send_mono: Some(200_000),
                ..Default::default()
            };
            let reply_packet = echo_reply_packet(TOKEN, seq, &params, &ts, None);
            socket.send_to(&reply_packet, peer).unwrap();
        }
    })
}

pub(super) fn assert_open_started(outcome: OpenOutcome) -> NegotiatedParams {
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

pub(super) fn assert_no_test_completed(outcome: OpenOutcome) -> NegotiatedParams {
    match outcome {
        OpenOutcome::NoTestCompleted {
            negotiated, event, ..
        } => {
            assert!(matches!(
                event,
                ClientEvent::NoTestCompleted {
                    negotiated: ref event_params,
                    ..
                } if *event_params == negotiated
            ));
            negotiated
        }
        OpenOutcome::Started { .. } => panic!("unexpected started outcome"),
    }
}

pub(super) fn open_client_with_echo_server(params: &Params) -> (Client, FakeServer) {
    let server = echo_server(params.clone());
    let config = ClientConfig {
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(200)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());
    (client, server)
}

pub(super) fn default_params() -> Params {
    Params {
        protocol_version: 1,
        duration_ns: 3_000_000_000,
        interval_ns: 1_000_000_000,
        received_stats: ReceivedStats::Both,
        stamp_at: StampAt::Both,
        clock: Clock::Both,
        ..Params::default()
    }
}
