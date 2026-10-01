use super::*;

#[test]
fn open_fails_when_already_open() {
    let config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
    let params = params_from_config(&config).unwrap();
    let server = open_success_server(params);
    let mut client = Client::connect(default_test_config(server.addr)).unwrap();
    assert_open_started(client.open().unwrap());
    assert!(matches!(client.open(), Err(ClientError::AlreadyOpen)));
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
    let config = ClientConfig {
        open_timeouts: vec![Duration::from_millis(200), Duration::from_millis(500)],
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    let outcome = client.open().unwrap();
    assert_open_started(outcome);
    assert_eq!(server.rx.iter().take(2).count(), 2);
    server.join();
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
        client.open(),
        Err(ClientError::ProtocolVersionMismatch { received: 2, .. })
    ));
    assert_eq!(server.rx.iter().take(1).count(), 1);
    assert!(client.runtime.prepare_open_request().is_ok());
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
    assert!(matches!(client.open(), Err(ClientError::ServerRejected)));
    assert_eq!(server.rx.iter().take(1).count(), 1);
    assert!(client.runtime.prepare_open_request().is_ok());
    server.join();
}

#[test]
fn ignored_datagrams_do_not_restart_the_attempt_deadline() {
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        thread::sleep(Duration::from_millis(150));
        socket.send_to(&[0_u8], peer).unwrap();
        thread::sleep(Duration::from_millis(150));
        socket.send_to(&[0_u8], peer).unwrap();
        let _ = recv_request(&socket, &tx);
    });
    let config = ClientConfig {
        open_timeouts: vec![Duration::from_millis(250), Duration::from_millis(250)],
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    let started = Instant::now();

    assert!(matches!(client.open(), Err(ClientError::OpenTimeout)));
    let requests: Vec<_> = server.rx.iter().take(2).collect();
    assert_eq!(requests.len(), 2);
    assert!(
        started.elapsed() < Duration::from_millis(700),
        "ignored datagrams restarted an attempt deadline"
    );
    server.join();
}

#[test]
fn opening_deadline_overflow_occurs_before_send() {
    let server = timeout_server(Duration::from_millis(250));
    let config = ClientConfig {
        open_timeouts: vec![Duration::MAX],
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();

    assert!(matches!(client.open(), Err(ClientError::DurationOverflow)));
    assert!(server.rx.try_recv().is_err());
    server.join();
}

#[test]
fn post_token_negotiation_failure_sends_cleanup_close() {
    let mut returned = default_params();
    returned.interval_ns += 1;
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        socket
            .send_to(
                &open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &returned, None),
                peer,
            )
            .unwrap();
        let _ = recv_request(&socket, &tx);
    });
    let mut client = Client::connect(default_test_config(server.addr)).unwrap();

    assert!(matches!(
        client.open(),
        Err(ClientError::NegotiationRejected { .. })
    ));
    let packets: Vec<_> = server.rx.iter().take(2).collect();
    assert_eq!(packets[1][3], flags::FLAG_CLOSE);
    assert_eq!(
        u64::from_le_bytes(packets[1][4..12].try_into().unwrap()),
        TOKEN
    );
    assert!(client.runtime.prepare_open_request().is_ok());
    server.join();
}

#[test]
fn successful_open_handshake() {
    let config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
    let params = params_from_config(&config).unwrap();
    let server = open_success_server(params.clone());
    let mut client = Client::connect(default_test_config(server.addr)).unwrap();

    let negotiated = assert_open_started(client.open().unwrap());
    assert_eq!(negotiated.params, params);

    assert_eq!(client.applied_traffic_class, Some(0));
    server.join();
}

#[test]
fn successful_open_restores_configured_receive_timeout() {
    let config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
    let params = params_from_config(&config).unwrap();
    let server = open_success_server(params);
    let mut config = default_test_config(server.addr);
    config.socket_config.recv_timeout = Some(Duration::from_millis(75));
    let mut client = Client::connect(config).unwrap();

    assert_open_started(client.open().unwrap());

    assert_eq!(
        client.socket.read_timeout().unwrap(),
        Some(Duration::from_millis(75))
    );
    server.join();
}

#[test]
fn open_timeout_after_all_timeouts() {
    let server = timeout_server(Duration::from_millis(700));
    let config = ClientConfig {
        open_timeouts: vec![Duration::from_millis(200), Duration::from_millis(200)],
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(50)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert!(matches!(client.open(), Err(ClientError::OpenTimeout)));
    assert_eq!(server.rx.iter().take(2).count(), 2);
    assert_eq!(
        client.socket.read_timeout().unwrap(),
        Some(Duration::from_millis(50))
    );
    server.join();
}

#[test]
fn multiple_untrusted_datagrams_before_valid_reply_use_one_attempt() {
    let params = default_params();
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        let valid = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
        let mut wrong_type = valid.clone();
        wrong_type[3] = FLAG_REPLY;
        let mut reserved_flags = valid.clone();
        reserved_flags[3] |= 0x10;
        let mut bad_magic = valid.clone();
        bad_magic[0] ^= 0xff;
        let unexpected_hmac =
            open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, Some(b"unexpected"));

        for packet in [
            vec![0_u8],
            bad_magic,
            wrong_type,
            reserved_flags,
            unexpected_hmac,
            valid,
        ] {
            socket.send_to(&packet, peer).unwrap();
        }
    });
    let mut client = Client::connect(default_test_config(server.addr)).unwrap();

    assert_open_started(client.open().unwrap());
    assert_eq!(server.rx.iter().take(1).count(), 1);
    assert!(server.rx.try_recv().is_err());

    server.join();
}

#[test]
fn only_ignored_open_datagrams_eventually_time_out() {
    let server = start_fake_server(move |socket, tx| {
        for _ in 0..2 {
            let (_, peer) = recv_request(&socket, &tx);
            socket.send_to(&[0_u8], peer).unwrap();
            socket.send_to(&MAGIC, peer).unwrap();
        }
        thread::sleep(Duration::from_millis(300));
    });
    let config = ClientConfig {
        open_timeouts: vec![Duration::from_millis(200), Duration::from_millis(200)],
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(50)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();

    assert!(matches!(client.open(), Err(ClientError::OpenTimeout)));
    assert_eq!(server.rx.iter().take(2).count(), 2);
    assert!(server.rx.try_recv().is_err());
    assert_eq!(
        client.socket.read_timeout().unwrap(),
        Some(Duration::from_millis(50))
    );
    server.join();
}

#[test]
fn trusted_zero_token_normal_reply_is_terminal() {
    let params = default_params();
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        // encode_open_reply rejects a zero token without FLAG_CLOSE, so this
        // deliberately non-compliant reply is built by encoding a normal
        // reply with a placeholder token and then zeroing the token field.
        let mut reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
        reply[HMAC_OFFSET..HMAC_OFFSET + 8].copy_from_slice(&0_u64.to_le_bytes());
        socket.send_to(&reply, peer).unwrap();
    });
    let mut client = Client::connect(default_test_config(server.addr)).unwrap();

    assert!(matches!(
        client.open(),
        Err(ClientError::Protocol(irtt_proto::ProtoError::ZeroToken))
    ));
    assert_eq!(server.rx.iter().take(1).count(), 1);
    assert!(client.runtime.prepare_open_request().is_ok());

    server.join();
}
