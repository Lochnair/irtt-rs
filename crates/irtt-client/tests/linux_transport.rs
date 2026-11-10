//! Real Linux socket observations through the public client adapters.
#![cfg(all(target_os = "linux", feature = "ancillary"))]

use std::{
    io::IoSliceMut,
    net::{SocketAddr, UdpSocket},
    sync::mpsc,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use irtt_client::{
    Client, ClientConfig, ClientEvent, NegotiationPolicy, OpenPolicy, SessionRequest, SocketConfig,
    TimestampMode,
};
use irtt_proto::{decode_request, Clock, DecodedRequestKind, ReceivedStats, StampAt};
use irtt_server::{ServerConfig, ServerCore};
use nix::sys::socket::{recvmsg, ControlMessageOwned, MsgFlags};

const TEST_TTL: u32 = 37;

// A compliant packet peer uses the real server core. Its receive-side IP_TOS
// observation is independent of the client/server parameter agreement.
fn peer(
    dscp_allowed: bool,
    probes: usize,
    retry_open: bool,
) -> (SocketAddr, thread::JoinHandle<Vec<u8>>) {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    socket2::SockRef::from(&socket)
        .set_recv_tos_v4(true)
        .unwrap();
    nix::sys::socket::setsockopt(&socket, nix::sys::socket::sockopt::Ipv4RecvTtl, &true).unwrap();
    let addr = socket.local_addr().unwrap();
    let task = thread::spawn(move || {
        let mut core = ServerCore::new(
            ServerConfig::default()
                .with_min_send_interval(Duration::ZERO)
                .with_dscp_allowed(dscp_allowed),
        );
        let mut markings = Vec::new();
        let mut replies = Vec::new();
        // OPEN (possibly retried), every ECHO, then CLOSE.
        for request in 0..probes + 2 + usize::from(retry_open) {
            let mut bytes = [0; 2048];
            let mut control = nix::cmsg_space!(u8, i32);
            let mut iov = [IoSliceMut::new(&mut bytes)];
            let msg = recvmsg::<nix::sys::socket::SockaddrIn>(
                std::os::fd::AsRawFd::as_raw_fd(&socket),
                &mut iov,
                Some(&mut control),
                MsgFlags::empty(),
            )
            .unwrap();
            let size = msg.bytes;
            let source = msg.address.unwrap();
            let marking = msg
                .cmsgs()
                .unwrap()
                .find_map(|message| match message {
                    ControlMessageOwned::Ipv4Tos(tos) => Some(tos),
                    _ => None,
                })
                .expect("Linux must report IP_TOS");
            let ttl = msg
                .cmsgs()
                .unwrap()
                .find_map(|message| match message {
                    ControlMessageOwned::Ipv4Ttl(ttl) => Some(ttl),
                    _ => None,
                })
                .expect("Linux must report IP_TTL");
            assert_eq!(
                ttl, TEST_TTL as i32,
                "configured TTL must survive OPEN, ECHO and CLOSE"
            );
            if retry_open && request == 0 {
                continue;
            }
            markings.push(marking);
            let source = SocketAddr::from((source.ip(), source.port()));
            if let Some(reply) = core.handle_datagram(source, &bytes[..size]).unwrap() {
                if markings.len() == 1 {
                    socket.send_to(reply.bytes(), source).unwrap();
                } else {
                    replies.push(reply);
                    if replies.len() == probes {
                        for reply in replies.drain(..).rev() {
                            socket.send_to(reply.bytes(), source).unwrap();
                        }
                    }
                }
            }
        }
        assert_eq!(core.session_count(), 0);
        markings
    });
    (addr, task)
}

fn config() -> ClientConfig {
    ClientConfig {
        open: OpenPolicy {
            negotiation: NegotiationPolicy::Loose,
            timeouts: vec![Duration::from_millis(200), Duration::from_secs(1)],
        },
        request: SessionRequest {
            dscp: 46,
            duration: None,
            interval: Duration::from_millis(10),
            clock: Clock::Both,
            stamp_at: StampAt::Both,
            received_stats: ReceivedStats::Both,
            length: 128,
            ..Default::default()
        },
        socket: SocketConfig {
            ttl: Some(TEST_TTL),
            ..SocketConfig::default()
        },
        ..ClientConfig::default()
    }
}

const MODES: [TimestampMode; 3] = [TimestampMode::Auto, TimestampMode::On, TimestampMode::Off];
const PROBES: usize = 3;

// TX timestamps have no public field. Recover the selected OWD endpoint and
// check that it corresponds to this send, even when replies arrive in reverse.
fn assert_endpoints(
    event: &ClientEvent,
    anchors: &[SystemTime],
    rx: TimestampMode,
    tx: TimestampMode,
) {
    let ClientEvent::EchoReply {
        seq,
        sent_at,
        received_at,
        rtt,
        server_timing: Some(timing),
        one_way: Some(one_way),
        packet_meta,
        ..
    } = event
    else {
        panic!("expected measured reply: {event:?}");
    };
    let ns = |wall: SystemTime| {
        i128::try_from(wall.duration_since(UNIX_EPOCH).unwrap().as_nanos()).unwrap()
    };
    let selected =
        i128::from(timing.receive_wall_ns.unwrap()) - one_way.client_to_server.unwrap().as_nanos();
    if tx == TimestampMode::Off {
        assert_eq!(selected, ns(sent_at.wall));
    } else {
        assert_ne!(selected, ns(sent_at.wall), "kernel TX timestamp required");
        assert!(selected >= ns(anchors[*seq as usize]));
        assert!(
            selected
                <= ns(anchors
                    .get(*seq as usize + 1)
                    .copied()
                    .unwrap_or(received_at.wall))
        );
    }
    let selected_receive =
        i128::from(timing.send_wall_ns.unwrap()) + one_way.server_to_client.unwrap().as_nanos();
    if rx == TimestampMode::Off {
        assert_eq!(packet_meta.kernel_rx_timestamp, None);
        assert_eq!(selected_receive, ns(received_at.wall));
    } else {
        assert_eq!(
            selected_receive,
            ns(packet_meta
                .kernel_rx_timestamp
                .expect("kernel RX timestamp required"))
        );
    }
    assert!(
        packet_meta.traffic_class.is_some(),
        "timestamp policy must preserve traffic class"
    );
    assert_eq!(rtt.raw, received_at.mono.duration_since(sent_at.mono));
}

fn mode_cases() -> impl Iterator<Item = (TimestampMode, TimestampMode, bool, bool, ClientConfig)> {
    MODES.into_iter().flat_map(|rx| {
        MODES.into_iter().map(move |tx| {
            let mut config = config();
            config.socket.kernel_rx_timestamp = rx;
            config.socket.kernel_tx_timestamp = tx;
            let allowed = rx != TimestampMode::Auto;
            let retry = rx == TimestampMode::On && tx == TimestampMode::On;
            (rx, tx, allowed, retry, config)
        })
    })
}

fn assert_markings(markings: &[u8], allowed: bool) {
    assert_eq!(markings[0], 0, "OPEN is unmarked");
    assert_eq!(
        &markings[1..=PROBES],
        &[if allowed { 184 } else { 0 }; PROBES]
    );
    assert_eq!(
        markings[PROBES + 1],
        0,
        "local CLOSE clears the negotiated marking"
    );
}

#[test]
fn blocking_timestamp_modes_with_reordered_replies() {
    for (rx, tx, allowed, retry, config) in mode_cases() {
        let (addr, peer) = peer(allowed, PROBES, retry);
        let mut client = Client::connect(addr.to_string(), config).unwrap();
        client
            .set_recv_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        client.open().unwrap();
        let mut anchors = Vec::new();
        for _ in 0..PROBES {
            anchors.push(SystemTime::now());
            client.send_probe().unwrap();
        }
        for seq in (0..PROBES as u32).rev() {
            let events = client.recv_once().unwrap();
            assert!(
                matches!(events.as_slice(), [ClientEvent::EchoReply { seq: actual, .. }] if *actual == seq)
            );
            assert_endpoints(&events[0], &anchors, rx, tx);
        }
        client.close().unwrap();
        assert_markings(&peer.join().unwrap(), allowed);
    }
}

#[cfg(feature = "tokio")]
#[test]
fn async_timestamp_modes_with_reordered_replies() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for (rx, tx, allowed, retry, config) in mode_cases() {
        let (addr, peer) = peer(allowed, PROBES, retry);
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                let mut client = irtt_client::AsyncClient::connect(addr.to_string(), config)
                    .await
                    .unwrap();
                client.open().await.unwrap();
                let mut anchors = Vec::new();
                for _ in 0..PROBES {
                    anchors.push(SystemTime::now());
                    client.send_probe().await.unwrap();
                }
                for seq in (0..PROBES as u32).rev() {
                    let events = client.recv().await.unwrap();
                    assert!(matches!(events.as_slice(), [ClientEvent::EchoReply { seq: actual, .. }] if *actual == seq));
                    assert_endpoints(&events[0], &anchors, rx, tx);
                }
                client.close().await.unwrap();
            })
            .await
            .expect("async transport observations timed out");
        });
        assert_markings(&peer.join().unwrap(), allowed);
    }
}

// Real ICMP refusal remains a socket error, never optional timestamp metadata.
// The bound peer closes only after the client has accepted a valid OPEN.
#[test]
fn a_closed_udp_peer_surfaces_a_socket_error() {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let addr = socket.local_addr().unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let (close_tx, close_rx) = mpsc::channel();
    let task = thread::spawn(move || {
        let mut core = ServerCore::new(ServerConfig::default());
        let mut bytes = [0; 2048];
        let (size, source) = socket.recv_from(&mut bytes).unwrap();
        assert!(matches!(
            decode_request(&bytes[..size]).unwrap().kind,
            DecodedRequestKind::Open { .. }
        ));
        let reply = core
            .handle_datagram(source, &bytes[..size])
            .unwrap()
            .unwrap();
        socket.send_to(reply.bytes(), source).unwrap();
        close_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        drop(socket);
    });
    let mut client = Client::connect(addr.to_string(), config()).unwrap();
    client
        .set_recv_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    client.open().unwrap();
    close_tx.send(()).unwrap();
    task.join().unwrap();
    // Refusal can arrive during send's TX drain or the following receive.
    // Either way this first Echo was accepted and must stay pending.
    let (receipt, error) = match client.send_probe() {
        Ok(receipt) => (receipt, client.recv_once().unwrap_err()),
        Err(irtt_client::SendProbeError::AfterCommit { receipt, source }) => (receipt, *source),
        Err(error) => panic!("first Echo should be accepted: {error:?}"),
    };
    assert_eq!(receipt.seq, 0);
    assert_eq!(receipt.remote, addr);
    assert!(client.has_pending_probes());
    assert!(
        matches!(error, irtt_client::ClientError::Socket(ref error)
        if error.kind() == std::io::ErrorKind::ConnectionRefused),
        "{error:?}"
    );
    assert!(matches!(
        client.poll_timeouts_at(receipt.sent_at.mono + client.probe_timeout())
            .unwrap().as_slice(),
        [ClientEvent::EchoLoss { seq: 0, sent_at, .. }] if *sent_at == receipt.sent_at
    ));
}
