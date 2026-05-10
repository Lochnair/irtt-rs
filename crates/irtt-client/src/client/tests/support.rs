use super::*;
use irtt_proto::{
    compute_hmac_in_place, echo_packet_len, flags::FLAG_HMAC, flags::FLAG_OPEN, flags::FLAG_REPLY,
    layout::PacketLayout, Clock, ReceivedStats, StampAt, HMAC_SIZE, MAGIC,
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
    let has_hmac = hmac_key.is_some();
    let layout = PacketLayout::echo(has_hmac, params);
    let packet_len = echo_packet_len(has_hmac, params);
    let mut packet = Vec::with_capacity(packet_len);

    let mut flags = flags;
    if has_hmac {
        flags |= FLAG_HMAC;
    }
    packet.extend_from_slice(&MAGIC);
    packet.push(flags);
    if has_hmac {
        packet.extend_from_slice(&[0_u8; HMAC_SIZE]);
    }
    packet.extend_from_slice(&token.to_le_bytes());
    packet.extend_from_slice(&seq.to_le_bytes());

    if layout.recv_count {
        packet.extend_from_slice(&42_u32.to_le_bytes());
    }
    if layout.recv_window {
        packet.extend_from_slice(&0x07_u64.to_le_bytes());
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
