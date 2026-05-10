use super::*;

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

    let negotiated = assert_open_started(client.open().unwrap());
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
fn open_timeout_after_all_timeouts() {
    let server = timeout_server(Duration::from_millis(700));
    let config = ClientConfig {
        open_timeouts: vec![Duration::from_millis(200), Duration::from_millis(200)],
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert!(matches!(client.open(), Err(ClientError::OpenTimeout)));
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
    assert!(matches!(client.open(), Err(ClientError::OpenTimeout)));

    let start = std::time::Instant::now();
    let mut buf = [0_u8; 1];
    assert!(client.socket.recv(&mut buf).is_err());
    assert!(start.elapsed() >= Duration::from_millis(350));
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
    server.join();
}
