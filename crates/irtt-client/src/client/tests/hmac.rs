use super::*;

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
            let reply_packet = echo_reply_packet(TOKEN, seq, &params, &ts, Some(&wrong_key));
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
    assert_open_started(client.open(ClientTimestamp::now()).unwrap());
    client.send_probe().unwrap();
    thread::sleep(Duration::from_millis(30));
    let events = client.recv_once().unwrap();
    assert_eq!(events.len(), 1);
    assert!(matches!(&events[0], ClientEvent::Warning { .. }));
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
            verify_hmac(&server_key, &buf[..size], HMAC_OFFSET).unwrap();
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
    assert_open_started(client.open(ClientTimestamp::now()).unwrap());
    client.send_probe().unwrap();
    thread::sleep(Duration::from_millis(50));
    let events = client.recv_once().unwrap();
    assert_eq!(events.len(), 1);
    assert!(matches!(&events[0], ClientEvent::EchoReply { .. }));
    server.join();
}
