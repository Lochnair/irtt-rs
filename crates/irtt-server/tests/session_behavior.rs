use std::{
    net::{SocketAddr, SocketAddrV6},
    time::Duration,
};

use irtt_proto::{decode_echo_reply, decode_open_reply, encode_request, Params, RequestToEncode};
use irtt_server::{ServerConfig, ServerCore};

// Admission, authentication, endpoint ownership and capacity are checked through
// the public packet interface, with actual OS-generated session tokens.
#[test]
fn authenticated_sessions_are_bounded_and_owned_by_the_opening_endpoint() {
    let key = b"session-behavior";
    let peer: SocketAddr = "127.0.0.1:40000".parse().unwrap();
    let other: SocketAddr = "127.0.0.1:40001".parse().unwrap();
    let mut server = ServerCore::new(
        ServerConfig::default()
            .with_hmac_key(key)
            .with_max_sessions(1)
            .with_min_send_interval(Duration::ZERO),
    );
    let params = Params {
        received_stats: irtt_proto::ReceivedStats::Both,
        ..Params::with_protocol_defaults()
    };
    let open = encode_request(
        RequestToEncode::Open {
            params: &params,
            no_test: false,
        },
        Some(key),
    )
    .unwrap();
    let mut unkeyed = ServerCore::new(ServerConfig::default());
    assert!(unkeyed.handle_datagram(peer, &open).unwrap().is_none());
    assert_eq!(unkeyed.session_count(), 0);
    let unauthenticated = encode_request(
        RequestToEncode::Open {
            params: &params,
            no_test: false,
        },
        None,
    )
    .unwrap();
    for packet in [
        &[][..],
        &[0; 16][..],
        &open[..open.len() - 1],
        &unauthenticated,
    ] {
        assert!(server.handle_datagram(peer, packet).unwrap().is_none());
        assert_eq!(server.session_count(), 0);
    }
    let reply = server.handle_datagram(peer, &open).unwrap().unwrap();
    let opened = decode_open_reply(reply.bytes(), Some(key)).unwrap();
    assert_ne!(opened.token, 0);
    assert!(server.handle_datagram(other, &open).unwrap().is_none());
    assert_eq!(server.session_count(), 1);
    let echo = encode_request(
        RequestToEncode::Echo {
            token: opened.token,
            sequence: 0,
            params: &opened.params,
            payload: &[],
        },
        Some(key),
    )
    .unwrap();
    let close = encode_request(
        RequestToEncode::Close {
            token: opened.token,
        },
        Some(key),
    )
    .unwrap();
    assert!(server.handle_datagram(other, &echo).unwrap().is_none());
    assert!(server.handle_datagram(other, &close).unwrap().is_none());
    let mut corrupted = echo.clone();
    corrupted[4] ^= 1;
    assert!(server.handle_datagram(peer, &corrupted).unwrap().is_none());
    let reply = server.handle_datagram(peer, &echo).unwrap().unwrap();
    let echoed = decode_echo_reply(reply.bytes(), &opened.params, Some(key)).unwrap();
    assert_eq!(echoed.sequence, 0);
    assert_eq!(echoed.recv_count, Some(1));
    assert!(server.handle_datagram(peer, &close).unwrap().is_none());
    assert_eq!(server.session_count(), 0);
    assert!(server.handle_datagram(peer, &echo).unwrap().is_none());
    assert!(server.handle_datagram(other, &open).unwrap().is_some());
}

// Zero allowance and zero idle lifetime expose the boundary behavior directly,
// without scripting clock reads or inspecting session/rate representations.
#[test]
fn zero_burst_and_zero_idle_timeout_silently_refuse_echoes() {
    let peer: SocketAddr = "127.0.0.1:40000".parse().unwrap();
    for config in [
        ServerConfig::default().with_burst_allowance(0),
        ServerConfig::default().with_idle_timeout(Duration::ZERO),
    ] {
        let mut server = ServerCore::new(config);
        let params = Params::with_protocol_defaults();
        let open = encode_request(
            RequestToEncode::Open {
                params: &params,
                no_test: false,
            },
            None,
        )
        .unwrap();
        let reply = server.handle_datagram(peer, &open).unwrap().unwrap();
        let opened = decode_open_reply(reply.bytes(), None).unwrap();
        let echo = encode_request(
            RequestToEncode::Echo {
                token: opened.token,
                sequence: 0,
                params: &opened.params,
                payload: &[],
            },
            None,
        )
        .unwrap();
        assert!(server.handle_datagram(peer, &echo).unwrap().is_none());
    }
}

#[test]
fn ipv6_flow_labels_share_a_session_but_zones_do_not() {
    let endpoint = |flow, scope| {
        SocketAddr::V6(SocketAddrV6::new(
            "fe80::1234".parse().unwrap(),
            40000,
            flow,
            scope,
        ))
    };
    let mut server =
        ServerCore::new(ServerConfig::default().with_min_send_interval(Duration::ZERO));
    let params = Params {
        received_stats: irtt_proto::ReceivedStats::Both,
        ..Params::with_protocol_defaults()
    };
    let packet = encode_request(
        RequestToEncode::Open {
            params: &params,
            no_test: false,
        },
        None,
    )
    .unwrap();
    let reply = server
        .handle_datagram(endpoint(1, 2), &packet)
        .unwrap()
        .unwrap();
    let opened = decode_open_reply(reply.bytes(), None).unwrap();
    let echo = encode_request(
        RequestToEncode::Echo {
            token: opened.token,
            sequence: 0,
            params: &opened.params,
            payload: &[],
        },
        None,
    )
    .unwrap();
    let close = encode_request(
        RequestToEncode::Close {
            token: opened.token,
        },
        None,
    )
    .unwrap();
    assert!(server
        .handle_datagram(endpoint(1, 3), &echo)
        .unwrap()
        .is_none());
    assert!(server
        .handle_datagram(endpoint(1, 3), &close)
        .unwrap()
        .is_none());
    assert_eq!(server.session_count(), 1);
    let reply = server
        .handle_datagram(endpoint(99, 2), &echo)
        .unwrap()
        .unwrap();
    assert_eq!(
        decode_echo_reply(reply.bytes(), &opened.params, None)
            .unwrap()
            .recv_count,
        Some(1)
    );
    assert!(server
        .handle_datagram(endpoint(99, 2), &close)
        .unwrap()
        .is_none());
    assert_eq!(server.session_count(), 0);
}

#[test]
fn oversized_authenticated_echo_does_not_advance_session_state() {
    let key = b"oversized-echo-test";
    let peer = "127.0.0.1:40000".parse().unwrap();
    let mut server = ServerCore::new(
        ServerConfig::default()
            .with_hmac_key(key)
            .with_max_packet_length(96)
            .with_burst_allowance(1)
            .with_min_send_interval(Duration::from_secs(60))
            .with_max_test_duration(Duration::from_millis(1))
            .with_idle_timeout(Duration::MAX),
    );
    let params = Params {
        length: 96,
        received_stats: irtt_proto::ReceivedStats::Both,
        ..Params::with_protocol_defaults()
    };
    let open = encode_request(
        RequestToEncode::Open {
            params: &params,
            no_test: false,
        },
        Some(key),
    )
    .unwrap();
    let reply = server.handle_datagram(peer, &open).unwrap().unwrap();
    let opened = decode_open_reply(reply.bytes(), Some(key)).unwrap();
    let oversized_params = Params {
        length: 128,
        ..opened.params.clone()
    };
    let oversized = encode_request(
        RequestToEncode::Echo {
            token: opened.token,
            sequence: 63,
            params: &oversized_params,
            payload: &[],
        },
        Some(key),
    )
    .unwrap();
    assert!(oversized.len() > 96);
    assert!(server.handle_datagram(peer, &oversized).unwrap().is_none());
    // If the oversized request started maximum duration, the first valid
    // echo would now close. Oversleep preserves this distinction.
    std::thread::sleep(Duration::from_millis(2001));
    let valid = encode_request(
        RequestToEncode::Echo {
            token: opened.token,
            sequence: 0,
            params: &opened.params,
            payload: &[],
        },
        Some(key),
    )
    .unwrap();
    let reply = server
        .handle_datagram(peer, &valid)
        .unwrap()
        .expect("oversized request must not consume allowance");
    let reply = decode_echo_reply(reply.bytes(), &opened.params, Some(key)).unwrap();
    assert_eq!(reply.flags & irtt_proto::FLAG_CLOSE, 0);
    assert_eq!(reply.recv_count, Some(1));
    assert_eq!(reply.recv_window, Some(1));
    assert_eq!(server.session_count(), 1);
}
