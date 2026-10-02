//! Durable server contracts observed through encoded requests and public replies.
use std::{
    net::SocketAddr,
    thread,
    time::{Duration, Instant},
};

use irtt_proto::{
    decode_echo_reply, decode_open_reply, encode_request, Clock, EchoReply, OpenReply, Params,
    ReceivedStats, RequestToEncode, StampAt, FLAG_CLOSE,
};
use irtt_server::{ServerConfig, ServerCore, TimestampAllowance};

fn peer() -> SocketAddr {
    "127.0.0.1:40000".parse().unwrap()
}
fn open_packet(params: &Params, no_test: bool, key: Option<&[u8]>) -> Vec<u8> {
    encode_request(RequestToEncode::Open { params, no_test }, key).unwrap()
}
fn open(server: &mut ServerCore, params: &Params) -> OpenReply {
    let reply = server
        .handle_datagram(peer(), &open_packet(params, false, None))
        .unwrap()
        .unwrap();
    decode_open_reply(reply.bytes(), None).unwrap()
}
fn echo(server: &mut ServerCore, opened: &OpenReply, sequence: u32) -> Option<EchoReply> {
    let request = encode_request(
        RequestToEncode::Echo {
            token: opened.token,
            sequence,
            params: &opened.params,
            payload: &[],
        },
        None,
    )
    .unwrap();
    server
        .handle_datagram(peer(), &request)
        .unwrap()
        .map(|reply| decode_echo_reply(reply.bytes(), &opened.params, None).unwrap())
}
fn unthrottled() -> ServerConfig {
    ServerConfig::default().with_min_send_interval(Duration::ZERO)
}

#[test]
fn no_test_authenticates_negotiates_and_uses_no_session_capacity() {
    let key = b"no-test-contract";
    let mut server = ServerCore::new(
        ServerConfig::default()
            .with_hmac_key(key)
            .with_max_sessions(0)
            .with_max_packet_length(96)
            .with_min_send_interval(Duration::from_millis(50))
            .with_timestamp_allowance(TimestampAllowance::Single)
            .with_dscp_allowed(false),
    );
    let params = Params {
        protocol_version: 99,
        length: 128,
        dscp: 185,
        stamp_at: StampAt::Both,
        clock: Clock::Both,
        ..Params::default()
    };
    let request = open_packet(&params, true, Some(key));
    assert!(server
        .handle_datagram(peer(), &open_packet(&params, true, None))
        .unwrap()
        .is_none());
    let mut bad_mac = request.clone();
    bad_mac[4] ^= 1;
    assert!(server.handle_datagram(peer(), &bad_mac).unwrap().is_none());
    let reply = server.handle_datagram(peer(), &request).unwrap().unwrap();
    assert_eq!(reply.traffic_class(), 0);
    let negotiated = decode_open_reply(reply.bytes(), Some(key)).unwrap();
    assert_eq!(negotiated.token, 0);
    assert_eq!(negotiated.params.protocol_version, 1);
    assert_ne!(negotiated.flags & FLAG_CLOSE, 0);
    assert_eq!(negotiated.params.length, 96);
    assert_eq!(negotiated.params.interval_ns, 50_000_000);
    assert_eq!(negotiated.params.stamp_at, StampAt::Midpoint);
    assert_eq!(negotiated.params.clock, Clock::Both);
    assert_eq!(negotiated.params.dscp, 0);
    assert_eq!(server.session_count(), 0);
    assert!(server
        .handle_datagram(peer(), &open_packet(&params, false, Some(key)))
        .unwrap()
        .is_none());
}

#[test]
fn no_test_refuses_unexecutable_layout_but_validates_after_restriction() {
    let params = Params {
        stamp_at: StampAt::Both,
        ..Params::with_protocol_defaults()
    };
    let mut server = ServerCore::new(unthrottled());
    assert!(server
        .handle_datagram(peer(), &open_packet(&params, true, None))
        .unwrap()
        .is_none());
    let mut restricted =
        ServerCore::new(unthrottled().with_timestamp_allowance(TimestampAllowance::None));
    let reply = restricted
        .handle_datagram(peer(), &open_packet(&params, true, None))
        .unwrap()
        .unwrap();
    assert_eq!(
        decode_open_reply(reply.bytes(), None)
            .unwrap()
            .params
            .stamp_at,
        StampAt::None
    );
    let rich = Params {
        received_stats: ReceivedStats::Both,
        stamp_at: StampAt::Both,
        clock: Clock::Both,
        ..Params::with_protocol_defaults()
    };
    let mut too_small = ServerCore::new(unthrottled().with_max_packet_length(32));
    assert!(too_small
        .handle_datagram(peer(), &open_packet(&rich, true, None))
        .unwrap()
        .is_none());
    assert_eq!(too_small.session_count(), 0);
    let invalid = Params {
        interval_ns: -1,
        ..Params::with_protocol_defaults()
    };
    assert!(server
        .handle_datagram(peer(), &open_packet(&invalid, true, None))
        .unwrap()
        .is_none());
    assert_eq!(server.session_count(), 0);
}

#[test]
fn receive_statistics_follow_verified_gap_duplicate_reordering_and_wrap_vectors() {
    // SERVER_BEHAVIORAL_VECTORS.md Sections 1.2, 1.5, 1.6, 1.8 and 1.10.
    let vectors: &[&[(u32, u64)]] = &[
        &[(0, 1), (1, 3), (3, 13), (4, 27)],
        &[(0, 1), (1, 3), (1, 3), (2, 7)],
        &[(0, 1), (1, 3), (2, 7), (1, 1), (3, 5)],
        &[(0, 1), (63, 0x8000_0000_0000_0001)],
        &[(0, 1), (64, 1)],
        &[
            (u32::MAX - 2, 1),
            (u32::MAX - 1, 3),
            (u32::MAX, 7),
            (0, 15),
            (1, 31),
        ],
    ];
    for vector in vectors {
        let mut server = ServerCore::new(unthrottled());
        let opened = open(
            &mut server,
            &Params {
                received_stats: ReceivedStats::Both,
                ..Params::with_protocol_defaults()
            },
        );
        for (index, &(sequence, window)) in vector.iter().enumerate() {
            let reply = echo(&mut server, &opened, sequence).unwrap();
            assert_eq!(reply.recv_count, Some(index as u32 + 1));
            assert_eq!(
                reply.recv_window,
                Some(window),
                "sequence {sequence} in {vector:?}"
            );
        }
    }
}

#[test]
fn a_spent_nonzero_burst_refills_without_counting_dropped_echoes() {
    let refill = Duration::from_millis(100);
    let mut server = ServerCore::new(
        ServerConfig::default()
            .with_min_send_interval(refill)
            .with_burst_allowance(1)
            .with_idle_timeout(Duration::MAX),
    );
    let opened = open(
        &mut server,
        &Params {
            received_stats: ReceivedStats::Both,
            ..Params::with_protocol_defaults()
        },
    );
    let mut count = echo(&mut server, &opened, 0).unwrap().recv_count.unwrap();
    // Observe actual refusal rather than assuming two calls fit a timing window.
    let deadline = Instant::now() + Duration::from_secs(2);
    while let Some(reply) = echo(&mut server, &opened, 0) {
        count = reply.recv_count.unwrap();
        assert!(
            Instant::now() < deadline,
            "could not observe a spent burst within two seconds"
        );
    }
    thread::sleep(refill); // Oversleep only replenishes more; idle expiry is disabled in practice.
    let reply = echo(&mut server, &opened, 1).unwrap();
    assert_eq!(reply.recv_count, Some(count + 1));
    assert_eq!(reply.recv_window, Some(3));
}

#[test]
fn a_nonzero_idle_deadline_reclaims_an_unused_session_before_another_open() {
    let idle = Duration::from_millis(10);
    let mut server = ServerCore::new(unthrottled().with_max_sessions(1).with_idle_timeout(idle));
    let params = Params::with_protocol_defaults();
    let old = open(&mut server, &params);
    thread::sleep(idle); // Any oversleep still requires silent expiry and capacity recovery.
    assert!(echo(&mut server, &old, 0).is_none());
    assert_eq!(server.session_count(), 0);
    open(&mut server, &params);
    assert_eq!(server.session_count(), 1);
}

#[test]
fn maximum_duration_starts_at_the_first_served_echo_and_closes_with_its_reply() {
    let maximum = Duration::from_millis(1);
    let mut server = ServerCore::new(
        unthrottled()
            .with_idle_timeout(Duration::MAX)
            .with_max_test_duration(maximum),
    );
    let opened = open(
        &mut server,
        &Params {
            received_stats: ReceivedStats::Both,
            stamp_at: StampAt::Both,
            clock: Clock::Both,
            dscp: 185,
            ..Params::with_protocol_defaults()
        },
    );
    assert_eq!(opened.params.duration_ns, 1_000_000);
    let elapsed_deadline = maximum + Duration::from_secs(2);
    thread::sleep(elapsed_deadline);
    let first = echo(&mut server, &opened, 0).unwrap();
    assert_eq!(
        first.flags & FLAG_CLOSE,
        0,
        "OPEN must not start the maximum-duration clock"
    );
    thread::sleep(elapsed_deadline); // Oversleep still requires the next served echo to close.
    let request = encode_request(
        RequestToEncode::Echo {
            token: opened.token,
            sequence: 1,
            params: &opened.params,
            payload: &[],
        },
        None,
    )
    .unwrap();
    let reply = server.handle_datagram(peer(), &request).unwrap().unwrap();
    assert_eq!(reply.traffic_class(), 185);
    let closed = decode_echo_reply(reply.bytes(), &opened.params, None).unwrap();
    assert_ne!(closed.flags & FLAG_CLOSE, 0);
    assert_eq!(closed.sequence, 1);
    assert_eq!(closed.recv_count, Some(2));
    assert_eq!(closed.recv_window, Some(3));
    assert!(closed.timestamps.recv_wall.is_some());
    assert!(closed.timestamps.send_mono.is_some());
    assert_eq!(server.session_count(), 0);
    assert!(echo(&mut server, &opened, 2).is_none());
}

#[test]
fn negotiated_timestamp_modes_emit_only_the_selected_fields_in_order() {
    for clock in [Clock::Wall, Clock::Monotonic, Clock::Both] {
        for placement in [
            StampAt::None,
            StampAt::Receive,
            StampAt::Send,
            StampAt::Both,
            StampAt::Midpoint,
        ] {
            let mut server = ServerCore::new(unthrottled());
            let opened = open(
                &mut server,
                &Params {
                    stamp_at: placement,
                    clock,
                    ..Params::with_protocol_defaults()
                },
            );
            let t = echo(&mut server, &opened, 0).unwrap().timestamps;
            let wall = clock != Clock::Monotonic;
            let mono = clock != Clock::Wall;
            let recv = matches!(placement, StampAt::Receive | StampAt::Both);
            let send = matches!(placement, StampAt::Send | StampAt::Both);
            let midpoint = placement == StampAt::Midpoint;
            assert_eq!(t.recv_wall.is_some(), wall && recv);
            assert_eq!(t.recv_mono.is_some(), mono && recv);
            assert_eq!(t.send_wall.is_some(), wall && send);
            assert_eq!(t.send_mono.is_some(), mono && send);
            assert_eq!(t.midpoint_wall.is_some(), wall && midpoint);
            assert_eq!(t.midpoint_mono.is_some(), mono && midpoint);
            if let (Some(recv), Some(send)) = (t.recv_wall, t.send_wall) {
                assert!(recv <= send);
            }
            if let (Some(recv), Some(send)) = (t.recv_mono, t.send_mono) {
                assert!(0 <= recv && recv <= send);
                let next = echo(&mut server, &opened, 1).unwrap().timestamps;
                assert!(
                    next.recv_mono.unwrap() >= send,
                    "monotonic origin must remain stable"
                );
            }
        }
    }
}

#[test]
fn duration_close_waits_for_rate_allowance_after_the_deadline() {
    let refill = Duration::from_secs(4);
    let maximum = Duration::from_millis(1);
    for _ in 0..3 {
        let mut server = ServerCore::new(
            ServerConfig::default()
                .with_min_send_interval(refill)
                .with_burst_allowance(1)
                .with_idle_timeout(Duration::MAX)
                .with_max_test_duration(maximum),
        );
        let started = Instant::now();
        let opened = open(
            &mut server,
            &Params {
                received_stats: ReceivedStats::Both,
                ..Params::with_protocol_defaults()
            },
        );
        let first = echo(&mut server, &opened, 0).unwrap();
        assert_eq!(first.flags & FLAG_CLOSE, 0);
        thread::sleep(maximum + Duration::from_secs(2));
        let refused = echo(&mut server, &opened, 63);
        // Include open/setup and the request itself in the elapsed bound. If the
        // scheduler carried us past refill, the reply may legitimately close.
        if started.elapsed() >= refill {
            continue;
        }
        assert!(
            refused.is_none(),
            "exhausted allowance must suppress the deadline-crossing reply"
        );
        assert_eq!(
            server.session_count(),
            1,
            "rate refusal must not release the session"
        );
        thread::sleep(refill);
        let closed = echo(&mut server, &opened, 1).unwrap();
        assert_ne!(closed.flags & FLAG_CLOSE, 0);
        assert_eq!(closed.sequence, 1);
        assert_eq!(closed.recv_count, Some(2));
        assert_eq!(closed.recv_window, Some(3));
        assert_eq!(server.session_count(), 0);
        return;
    }
    panic!("scheduler crossed the refill boundary in all three fresh sessions");
}

#[test]
fn default_server_resource_limits_remain_bounded() {
    let config = ServerConfig::default();
    // Upper resource budgets, not an exact default or a comparison against
    // the same constants used by the implementation. Smaller bounds are fine.
    assert!((1..=1024).contains(&config.max_sessions()));
    assert!((1..=65_507).contains(&config.max_packet_length()));
}
