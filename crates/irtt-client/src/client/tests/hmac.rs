use super::*;

#[test]
fn hmac_open_success() {
    let key = b"secret".to_vec();
    let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
    config.hmac_key = Some(key.clone());
    let params = params_from_config(&config).unwrap();
    let server = start_fake_server(move |socket, tx| {
        let (request, peer) = recv_request(&socket, &tx);
        verify_packet_hmac(&key, &request).unwrap();
        let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, Some(&key));
        socket.send_to(&reply, peer).unwrap();
    });
    config.server_addr = server.addr.to_string();
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());
    server.join();
}

#[test]
fn hmac_open_ignores_missing_hmac_before_valid_reply() {
    let key = b"secret".to_vec();
    let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
    config.hmac_key = Some(key.clone());
    let params = params_from_config(&config).unwrap();
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        let missing = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
        let valid = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, Some(&key));
        socket.send_to(&missing, peer).unwrap();
        socket.send_to(&valid, peer).unwrap();
    });
    config.server_addr = server.addr.to_string();
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());
    assert_eq!(server.rx.iter().take(1).count(), 1);
    assert!(server.rx.try_recv().is_err());
    server.join();
}

#[test]
fn hmac_open_ignores_bad_hmac_before_valid_reply() {
    let key = b"secret".to_vec();
    let wrong_key = b"wrong".to_vec();
    let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
    config.hmac_key = Some(key.clone());
    let params = params_from_config(&config).unwrap();
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        let bad = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, Some(&wrong_key));
        let valid = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, Some(&key));
        socket.send_to(&bad, peer).unwrap();
        socket.send_to(&valid, peer).unwrap();
    });
    config.server_addr = server.addr.to_string();
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());
    assert_eq!(server.rx.iter().take(1).count(), 1);
    assert!(server.rx.try_recv().is_err());
    server.join();
}

#[test]
fn post_token_hmac_negotiation_failure_sends_authenticated_cleanup_close() {
    let key = b"secret".to_vec();
    let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
    config.hmac_key = Some(key.clone());
    let mut returned = params_from_config(&config).unwrap();
    returned.interval_ns += 1;
    let server_key = key.clone();
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &returned, Some(&server_key));
        socket.send_to(&reply, peer).unwrap();
        let _ = recv_request(&socket, &tx);
    });
    config.server_addr = server.addr.to_string();
    let mut client = Client::connect(config).unwrap();

    assert!(matches!(
        client.open(),
        Err(ClientError::NegotiationRejected { .. })
    ));
    let packets: Vec<_> = server.rx.iter().take(2).collect();
    let cleanup = &packets[1];
    assert_eq!(cleanup[3], flags::FLAG_CLOSE | FLAG_HMAC);
    verify_packet_hmac(&key, cleanup).unwrap();
    assert_eq!(
        u64::from_le_bytes(cleanup[4 + HMAC_SIZE..12 + HMAC_SIZE].try_into().unwrap()),
        TOKEN
    );
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
    assert_open_started(client.open().unwrap());
    client.close().unwrap();
    let packets: Vec<_> = server.rx.iter().take(2).collect();
    let close = &packets[1];
    assert_eq!(close[3], flags::FLAG_CLOSE | FLAG_HMAC);
    verify_packet_hmac(&key, close).unwrap();
    assert_eq!(
        u64::from_le_bytes(close[4 + HMAC_SIZE..12 + HMAC_SIZE].try_into().unwrap()),
        TOKEN
    );
    server.join();
}

#[test]
fn bad_hmac_reply_is_dropped() {
    let key = b"secret".to_vec();
    let wrong_key = b"wrong".to_vec();
    let params = default_params();
    let server_key = key.clone();
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, Some(&server_key));
        socket.send_to(&reply, peer).unwrap();

        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut buf = [0_u8; 2048];
        if let Ok((size, _)) = socket.recv_from(&mut buf) {
            tx.send(buf[..size].to_vec()).unwrap();
            let seq = u32::from_le_bytes(
                buf[4 + HMAC_SIZE + 8..4 + HMAC_SIZE + 12]
                    .try_into()
                    .unwrap(),
            );
            let ts = TimestampFields::default();
            let reply_packet = echo_reply_packet_with_flags(
                TOKEN,
                seq,
                &params,
                &ts,
                Some(&wrong_key),
                FLAG_REPLY | flags::FLAG_CLOSE,
            );
            socket.send_to(&reply_packet, peer).unwrap();
        }
    });
    let config = ClientConfig {
        hmac_key: Some(key),
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(200)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());
    client.send_probe().unwrap();
    thread::sleep(Duration::from_millis(30));
    let events = client.recv_once().unwrap();
    assert_eq!(events.len(), 1);
    assert!(matches!(&events[0], ClientEvent::Warning { .. }));
    assert!(!client.is_peer_closed());
    client.close().unwrap();
    server.join();
}

#[test]
fn hmac_echo_request_reply_works() {
    let key = b"testkey".to_vec();
    let params = default_params();
    let server_key = key.clone();
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, Some(&server_key));
        socket.send_to(&reply, peer).unwrap();

        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut buf = [0_u8; 2048];
        if let Ok((size, _)) = socket.recv_from(&mut buf) {
            tx.send(buf[..size].to_vec()).unwrap();
            verify_packet_hmac(&server_key, &buf[..size]).unwrap();
            let seq = u32::from_le_bytes(
                buf[4 + HMAC_SIZE + 8..4 + HMAC_SIZE + 12]
                    .try_into()
                    .unwrap(),
            );
            let ts = TimestampFields {
                recv_mono: Some(100),
                send_mono: Some(200),
                ..Default::default()
            };
            let reply_packet = echo_reply_packet(TOKEN, seq, &params, &ts, Some(&server_key));
            socket.send_to(&reply_packet, peer).unwrap();
        }
    });
    let config = ClientConfig {
        hmac_key: Some(key),
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(200)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());
    client.send_probe().unwrap();
    thread::sleep(Duration::from_millis(50));
    let events = client.recv_once().unwrap();
    assert_eq!(events.len(), 1);
    assert!(matches!(&events[0], ClientEvent::EchoReply { .. }));
    server.join();
}
