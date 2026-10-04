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
};
use irtt_proto::{decode_request, Clock, DecodedRequestKind, ReceivedStats, StampAt};
use irtt_server::{ServerConfig, ServerCore};
use nix::sys::socket::{recvmsg, ControlMessageOwned, MsgFlags};

const TEST_TTL: u32 = 37;

// A compliant packet peer uses the real server core. Its receive-side IP_TOS
// observation is independent of the client/server parameter agreement.
fn peer(dscp_allowed: bool, probes: usize) -> (SocketAddr, thread::JoinHandle<Vec<u8>>) {
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
        // OPEN, every ECHO, then CLOSE.
        for _ in 0..probes + 2 {
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
            markings.push(marking);
            let source = SocketAddr::from((source.ip(), source.port()));
            if let Some(reply) = core.handle_datagram(source, &bytes[..size]).unwrap() {
                socket.send_to(reply.bytes(), source).unwrap();
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
            timeouts: vec![Duration::from_secs(1)],
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

// TX timestamps have no separate public field: the upstream OWD exposes the
// selected send endpoint. Reconstruct it and require a real kernel endpoint
// rather than accepting the userspace fallback. RTT must still use sent_at.
fn assert_kernel_send_endpoint(event: &ClientEvent, before_send: SystemTime) {
    let ClientEvent::EchoReply {
        sent_at,
        received_at,
        rtt,
        server_timing: Some(timing),
        one_way: Some(one_way),
        packet_meta,
        ..
    } = event
    else {
        panic!("expected measured reply: {event:?}")
    };
    let ns = |wall: SystemTime| {
        i128::try_from(wall.duration_since(UNIX_EPOCH).unwrap().as_nanos()).unwrap()
    };
    let selected =
        i128::from(timing.receive_wall_ns.unwrap()) - one_way.client_to_server.unwrap().as_nanos();
    assert!(selected >= ns(before_send));
    // TX_SOFTWARE may arrive before or after send returns; only equality
    // identifies the userspace fallback. Receipt remains the upper bound.
    assert_ne!(
        selected,
        ns(sent_at.wall),
        "TX timestamp was not observed; userspace fallback is insufficient"
    );
    assert!(selected <= ns(received_at.wall));
    let selected_receive =
        i128::from(timing.send_wall_ns.unwrap()) + one_way.server_to_client.unwrap().as_nanos();
    assert_eq!(
        selected_receive,
        ns(packet_meta
            .kernel_rx_timestamp
            .expect("Linux RX timestamp required")),
        "downstream OWD must use the kernel receive endpoint"
    );
    assert_eq!(rtt.raw, received_at.mono.duration_since(sent_at.mono));
}

#[test]
fn blocking_probes_use_kernel_tx_timing_and_negotiated_packet_marking() {
    for allowed in [true, false] {
        const PROBES: usize = 3;
        let (addr, peer) = peer(allowed, PROBES);
        let mut client = Client::connect(addr.to_string(), config()).unwrap();
        client
            .set_recv_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        client.open().unwrap();
        assert_eq!(
            client.negotiated_params().unwrap().params.dscp,
            if allowed { 184 } else { 0 }
        );
        for _ in 0..PROBES {
            let before = SystemTime::now();
            client.send_probe().unwrap();
            let events = client.recv_once().unwrap();
            assert_eq!(events.len(), 1);
            assert_kernel_send_endpoint(&events[0], before);
        }
        client.close().unwrap();
        let markings = peer.join().unwrap();
        assert_eq!(markings[0], 0, "OPEN is unmarked");
        assert_eq!(
            &markings[1..=PROBES],
            vec![if allowed { 184 } else { 0 }; PROBES]
        );
        assert_eq!(
            markings[PROBES + 1],
            0,
            "local CLOSE clears the negotiated marking"
        );
    }
}

#[cfg(feature = "tokio")]
#[test]
fn async_probes_use_kernel_tx_timing_and_negotiated_packet_marking() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for allowed in [true, false] {
        const PROBES: usize = 3;
        let (addr, peer) = peer(allowed, PROBES);
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                let mut client = irtt_client::AsyncClient::connect(addr.to_string(), config())
                    .await
                    .unwrap();
                client.open().await.unwrap();
                assert_eq!(
                    client.negotiated_params().unwrap().params.dscp,
                    if allowed { 184 } else { 0 }
                );
                for _ in 0..PROBES {
                    let before = SystemTime::now();
                    client.send_probe().await.unwrap();
                    let events = client.recv().await.unwrap();
                    assert_eq!(events.len(), 1);
                    assert_kernel_send_endpoint(&events[0], before);
                }
                client.close().await.unwrap();
            })
            .await
            .expect("async transport observations timed out");
        });
        let markings = peer.join().unwrap();
        assert_eq!(markings[0], 0);
        assert_eq!(
            &markings[1..=PROBES],
            vec![if allowed { 184 } else { 0 }; PROBES]
        );
        assert_eq!(markings[PROBES + 1], 0);
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
    let result = client.send_probe().and_then(|_| client.recv_once());
    assert!(
        matches!(result, Err(irtt_client::ClientError::Socket(ref error))
        if error.kind() == std::io::ErrorKind::ConnectionRefused),
        "{result:?}"
    );
}
