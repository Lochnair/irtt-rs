use super::*;

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
    assert_eq!(config.probe_timeout, Duration::from_secs(4));
    assert_eq!(config.max_pending_probes, 4096);
}

#[test]
fn params_from_config_maps_compatibility_fields() {
    let config = ClientConfig {
        duration: Some(Duration::from_secs(5)),
        interval: Duration::from_millis(250),
        length: 1472,
        received_stats: ReceivedStats::Window,
        stamp_at: StampAt::Midpoint,
        clock: Clock::Wall,
        dscp: 46,
        hmac_key: Some(b"secret".to_vec()),
        server_fill: Some("rand".to_owned()),
        ..ClientConfig::default()
    };

    let params = params_from_config(&config).unwrap();
    assert_eq!(params.protocol_version, PROTOCOL_VERSION);
    assert_eq!(params.duration_ns, 5_000_000_000);
    assert_eq!(params.interval_ns, 250_000_000);
    assert_eq!(params.length, 1472);
    assert_eq!(params.received_stats, ReceivedStats::Window);
    assert_eq!(params.stamp_at, StampAt::Midpoint);
    assert_eq!(params.clock, Clock::Wall);
    assert_eq!(params.dscp, 46, "config DSCP codepoint must not be shifted");
    assert_eq!(
        params.server_fill.as_ref().map(|fill| fill.value.as_str()),
        Some("rand")
    );
    assert_eq!(config.hmac_key.as_deref(), Some(b"secret".as_slice()));
}

#[test]
fn params_from_config_encodes_continuous_duration_as_zero() {
    let config = ClientConfig {
        duration: None,
        ..ClientConfig::default()
    };
    assert_eq!(params_from_config(&config).unwrap().duration_ns, 0);
}

#[test]
fn params_from_config_accepts_max_dscp_codepoint() {
    let config = ClientConfig {
        dscp: 63,
        ..ClientConfig::default()
    };
    assert_eq!(params_from_config(&config).unwrap().dscp, 63);
}

#[test]
fn params_from_config_rejects_invalid_dscp_codepoint() {
    let config = ClientConfig {
        dscp: 64,
        ..ClientConfig::default()
    };
    assert!(matches!(
        params_from_config(&config),
        Err(ClientError::InvalidConfig { .. })
    ));
}

#[test]
fn params_from_config_rejects_oversized_server_fill() {
    let config = ClientConfig {
        server_fill: Some("0123456789abcdef0123456789abcdefx".to_owned()),
        ..ClientConfig::default()
    };
    assert!(matches!(
        params_from_config(&config),
        Err(ClientError::InvalidConfig { .. })
    ));
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
    assert!(matches!(client.phase, ClientPhase::Open { token: TOKEN }));
    server.join();
}

#[test]
fn open_fails_when_already_open() {
    let config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
    let params = params_from_config(&config).unwrap();
    let server = open_success_server(params);
    let mut client = Client::connect(default_test_config(server.addr)).unwrap();
    assert_open_started(client.open(ClientTimestamp::now()).unwrap());
    assert!(matches!(
        client.open(ClientTimestamp::now()),
        Err(ClientError::AlreadyOpen)
    ));
    server.join();
}

#[test]
fn open_fails_after_close() {
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
    client.close(ClientTimestamp::now()).unwrap();
    assert!(matches!(
        client.open(ClientTimestamp::now()),
        Err(ClientError::AlreadyClosed)
    ));
    server.join();
}

#[test]
fn open_fails_after_no_test_completed() {
    let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
    config.run_mode = RunMode::NoTest;
    let params = params_from_config(&config).unwrap();
    let server = no_test_server(params, 0);
    config.server_addr = server.addr.to_string();
    let mut client = Client::connect(config).unwrap();
    assert_no_test_completed(client.open(ClientTimestamp::now()).unwrap());
    assert!(matches!(
        client.open(ClientTimestamp::now()),
        Err(ClientError::AlreadyCompleted)
    ));
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
    let outcome = client.open(ClientTimestamp::now()).unwrap();
    assert_open_started(outcome);
    assert_eq!(server.rx.iter().take(2).count(), 2);
    server.join();
}

#[test]
fn open_timeout_after_all_timeouts() {
    let server = timeout_server(Duration::from_millis(700));
    let config = ClientConfig {
        open_timeouts: vec![Duration::from_millis(200), Duration::from_millis(200)],
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert!(matches!(
        client.open(ClientTimestamp::now()),
        Err(ClientError::OpenTimeout)
    ));
    assert_eq!(server.rx.iter().take(2).count(), 2);
    server.join();
}

#[test]
fn open_restores_configured_read_timeout_after_timeout() {
    let server = timeout_server(Duration::from_millis(700));
    let config = ClientConfig {
        open_timeouts: vec![Duration::from_millis(200)],
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(450)),
            ..crate::SocketConfig::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert!(matches!(
        client.open(ClientTimestamp::now()),
        Err(ClientError::OpenTimeout)
    ));

    let start = std::time::Instant::now();
    let mut buf = [0_u8; 1];
    assert!(client.socket.recv(&mut buf).is_err());
    assert!(start.elapsed() >= Duration::from_millis(350));
    server.join();
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
    assert!(validate_negotiated_params(&requested, &returned, NegotiationPolicy::Loose).is_ok());
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
    let server = no_test_server(params.clone(), 0);
    config.server_addr = server.addr.to_string();
    let mut client = Client::connect(config).unwrap();
    let negotiated = assert_no_test_completed(client.open(ClientTimestamp::now()).unwrap());
    assert_eq!(negotiated.params, params);
    assert_eq!(client.negotiated.as_ref(), Some(&negotiated));
    assert!(matches!(
        client.close(ClientTimestamp::now()),
        Err(ClientError::NotOpen)
    ));
    server.join();
}

#[test]
fn no_test_success_validates_params() {
    let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
    config.run_mode = RunMode::NoTest;
    let params = params_from_config(&config).unwrap();
    let server = no_test_server(params.clone(), 0);
    config.server_addr = server.addr.to_string();
    let mut client = Client::connect(config).unwrap();
    let negotiated = assert_no_test_completed(client.open(ClientTimestamp::now()).unwrap());
    assert_eq!(negotiated.params, params);
    server.join();
}

#[test]
fn no_test_rejects_non_close_open_reply() {
    let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
    config.run_mode = RunMode::NoTest;
    let params = params_from_config(&config).unwrap();
    let server = open_success_server(params);
    config.server_addr = server.addr.to_string();
    let mut client = Client::connect(config).unwrap();
    assert!(matches!(
        client.open(ClientTimestamp::now()),
        Err(ClientError::UnexpectedNoTestReply)
    ));
    server.join();
}

#[test]
fn no_test_rejects_non_zero_token_with_close_reply() {
    let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
    config.run_mode = RunMode::NoTest;
    let params = params_from_config(&config).unwrap();
    let server = no_test_server(params, TOKEN);
    config.server_addr = server.addr.to_string();
    let mut client = Client::connect(config).unwrap();
    assert!(matches!(
        client.open(ClientTimestamp::now()),
        Err(ClientError::NonZeroNoTestToken { token: TOKEN })
    ));
    server.join();
}

#[test]
fn no_test_strict_negotiation_rejects_changed_params() {
    let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
    config.run_mode = RunMode::NoTest;
    let mut params = params_from_config(&config).unwrap();
    params.dscp = 1;
    let server = no_test_server(params, 0);
    config.server_addr = server.addr.to_string();
    let mut client = Client::connect(config).unwrap();
    assert!(matches!(
        client.open(ClientTimestamp::now()),
        Err(ClientError::NegotiationRejected { .. })
    ));
    server.join();
}

#[test]
fn no_test_loose_negotiation_accepts_restricted_params() {
    let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
    config.run_mode = RunMode::NoTest;
    config.negotiation_policy = NegotiationPolicy::Loose;
    let mut params = params_from_config(&config).unwrap();
    params.duration_ns /= 2;
    let server = no_test_server(params.clone(), 0);
    config.server_addr = server.addr.to_string();
    let mut client = Client::connect(config).unwrap();
    let negotiated = assert_no_test_completed(client.open(ClientTimestamp::now()).unwrap());
    assert_eq!(negotiated.params, params);
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

#[test]
fn empty_open_timeouts_is_rejected() {
    let config = ClientConfig {
        open_timeouts: vec![],
        ..ClientConfig::default()
    };
    assert!(matches!(
        Client::connect(config),
        Err(ClientError::NoOpenTimeouts)
    ));
}
