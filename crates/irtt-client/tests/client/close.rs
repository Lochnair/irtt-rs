use super::*;

#[test]
fn operations_fail_after_local_close_without_datagrams() {
    let params = default_params();
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
        socket.send_to(&reply, peer).unwrap();
        let _ = recv_request(&socket, &tx);
    });
    let mut config = default_test_config(server.addr);
    config.socket_config.recv_timeout = Some(Duration::from_millis(50));
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());
    client.close().unwrap();
    assert!(matches!(client.open(), Err(ClientError::AlreadyClosed)));
    let results = [
        client.recv_once(),
        client.recv_available(RecvBudget { max_packets: 1 }),
    ];
    assert!(
        matches!(
            results,
            [
                Err(ClientError::AlreadyClosed),
                Err(ClientError::AlreadyClosed)
            ]
        ),
        "unexpected receive results: {results:?}"
    );
    server.join();
}

#[test]
fn send_probe_fails_after_close() {
    let params = default_params();
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
        socket.send_to(&reply, peer).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        loop {
            let mut buf = [0_u8; 512];
            match socket.recv_from(&mut buf) {
                Ok((size, _)) => {
                    tx.send(buf[..size].to_vec()).unwrap();
                }
                Err(_) => break,
            }
        }
    });
    let mut client = Client::connect(default_test_config(server.addr)).unwrap();
    assert_open_started(client.open().unwrap());
    client.close().unwrap();
    assert!(matches!(
        client.send_probe(),
        Err(ClientError::AlreadyClosed)
    ));
    server.join();
}

#[test]
fn recv_available_stops_after_peer_close() {
    let params = default_params();
    let server = start_fake_server({
        let params = params.clone();
        move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            socket
                .send_to(
                    &open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None),
                    peer,
                )
                .unwrap();
            let (request, _) = recv_request(&socket, &tx);
            let seq = u32::from_le_bytes(request[12..16].try_into().unwrap());
            let close = echo_reply_packet_with_flags(
                TOKEN,
                seq,
                &params,
                &TimestampFields::default(),
                None,
                FLAG_REPLY | flags::FLAG_CLOSE,
            );
            socket.send_to(&close, peer).unwrap();
            socket.send_to(&close, peer).unwrap();
        }
    });
    let mut client = Client::connect(default_test_config(server.addr)).unwrap();
    assert_open_started(client.open().unwrap());
    client.send_probe().unwrap();

    let events = client
        .recv_available(RecvBudget { max_packets: 8 })
        .unwrap();

    assert!(matches!(
        events.as_slice(),
        [
            ClientEvent::EchoReply { .. },
            ClientEvent::SessionClosed { token: TOKEN, .. }
        ]
    ));
    assert!(client.is_peer_closed());
    server.join();
}

#[test]
fn close_flagged_duplicate_emits_duplicate_then_closes() {
    let params = default_params();
    let server = start_fake_server({
        let params = params.clone();
        move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            socket
                .send_to(
                    &open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None),
                    peer,
                )
                .unwrap();

            let (request, _) = recv_request(&socket, &tx);
            let seq = u32::from_le_bytes(request[12..16].try_into().unwrap());
            let normal = echo_reply_packet(TOKEN, seq, &params, &TimestampFields::default(), None);
            let close = echo_reply_packet_with_flags(
                TOKEN,
                seq,
                &params,
                &TimestampFields::default(),
                None,
                FLAG_REPLY | flags::FLAG_CLOSE,
            );
            socket.send_to(&normal, peer).unwrap();
            socket.send_to(&close, peer).unwrap();
        }
    });
    let mut client = Client::connect(ClientConfig {
        socket_config: irtt_client::SocketConfig {
            recv_timeout: Some(Duration::from_millis(200)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    })
    .unwrap();
    assert_open_started(client.open().unwrap());
    client.send_probe().unwrap();

    assert!(matches!(
        client.recv_once().unwrap().as_slice(),
        [ClientEvent::EchoReply { seq: 0, .. }]
    ));
    assert!(matches!(
        client.recv_once().unwrap().as_slice(),
        [
            ClientEvent::DuplicateReply { seq: 0, .. },
            ClientEvent::SessionClosed { token: TOKEN, .. }
        ]
    ));
    assert!(client.is_peer_closed());
    server.join();
}

#[test]
fn close_flagged_retained_timeout_emits_late_then_closes() {
    let params = default_params();
    let server = start_fake_server({
        let params = params.clone();
        move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            socket
                .send_to(
                    &open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None),
                    peer,
                )
                .unwrap();

            let (request, _) = recv_request(&socket, &tx);
            let seq = u32::from_le_bytes(request[12..16].try_into().unwrap());
            socket
                .send_to(
                    &echo_reply_packet_with_flags(
                        TOKEN,
                        seq,
                        &params,
                        &TimestampFields::default(),
                        None,
                        FLAG_REPLY | flags::FLAG_CLOSE,
                    ),
                    peer,
                )
                .unwrap();
        }
    });
    let mut client = Client::connect(ClientConfig {
        probe_timeout: Duration::from_millis(50),
        socket_config: irtt_client::SocketConfig {
            recv_timeout: Some(Duration::from_millis(200)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    })
    .unwrap();
    assert_open_started(client.open().unwrap());
    let sent = client.send_probe().unwrap();
    let ClientEvent::EchoSent { sent_at, .. } = &sent[0] else {
        panic!("expected EchoSent");
    };
    assert!(matches!(
        client
            .poll_timeouts_at(sent_at.mono + client.probe_timeout())
            .unwrap()
            .as_slice(),
        [ClientEvent::EchoLoss { seq: 0, .. }]
    ));

    assert!(matches!(
        client.recv_once().unwrap().as_slice(),
        [
            ClientEvent::LateReply {
                seq: 0,
                sent_at: Some(_),
                ..
            },
            ClientEvent::SessionClosed { token: TOKEN, .. }
        ]
    ));
    assert!(client.is_peer_closed());
    server.join();
}

#[test]
fn close_flagged_evicted_sequence_emits_late_then_closes() {
    let params = default_params();
    let server = start_fake_server({
        let params = params.clone();
        move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            socket
                .send_to(
                    &open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None),
                    peer,
                )
                .unwrap();

            let (first, _) = recv_request(&socket, &tx);
            let first_seq = u32::from_le_bytes(first[12..16].try_into().unwrap());
            socket
                .send_to(
                    &echo_reply_packet(
                        TOKEN,
                        first_seq,
                        &params,
                        &TimestampFields::default(),
                        None,
                    ),
                    peer,
                )
                .unwrap();

            let (second, _) = recv_request(&socket, &tx);
            let second_seq = u32::from_le_bytes(second[12..16].try_into().unwrap());
            socket
                .send_to(
                    &echo_reply_packet(
                        TOKEN,
                        second_seq,
                        &params,
                        &TimestampFields::default(),
                        None,
                    ),
                    peer,
                )
                .unwrap();
            socket
                .send_to(
                    &echo_reply_packet_with_flags(
                        TOKEN,
                        first_seq,
                        &params,
                        &TimestampFields::default(),
                        None,
                        FLAG_REPLY | flags::FLAG_CLOSE,
                    ),
                    peer,
                )
                .unwrap();
        }
    });
    let mut client = Client::connect(ClientConfig {
        max_pending_probes: 1,
        socket_config: irtt_client::SocketConfig {
            recv_timeout: Some(Duration::from_millis(200)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    })
    .unwrap();
    assert_open_started(client.open().unwrap());

    client.send_probe().unwrap();
    assert!(matches!(
        client.recv_once().unwrap().as_slice(),
        [ClientEvent::EchoReply { seq: 0, .. }]
    ));
    client.send_probe().unwrap();
    assert!(matches!(
        client.recv_once().unwrap().as_slice(),
        [ClientEvent::EchoReply { seq: 1, .. }]
    ));
    assert!(matches!(
        client.recv_once().unwrap().as_slice(),
        [
            ClientEvent::LateReply {
                seq: 0,
                sent_at: None,
                rtt: None,
                ..
            },
            ClientEvent::SessionClosed { token: TOKEN, .. }
        ]
    ));
    assert!(client.is_peer_closed());
    server.join();
}

#[test]
fn close_flagged_untracked_sequence_emits_warning_then_closes() {
    let params = default_params();
    let server = start_fake_server({
        let params = params.clone();
        move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            socket
                .send_to(
                    &open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None),
                    peer,
                )
                .unwrap();
            socket
                .send_to(
                    &echo_reply_packet_with_flags(
                        TOKEN,
                        42,
                        &params,
                        &TimestampFields::default(),
                        None,
                        FLAG_REPLY | flags::FLAG_CLOSE,
                    ),
                    peer,
                )
                .unwrap();
        }
    });
    let mut client = Client::connect(ClientConfig {
        socket_config: irtt_client::SocketConfig {
            recv_timeout: Some(Duration::from_millis(200)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    })
    .unwrap();
    assert_open_started(client.open().unwrap());

    assert!(matches!(
        client.recv_once().unwrap().as_slice(),
        [
            ClientEvent::Warning {
                kind: WarningKind::UntrackedReply,
                ..
            },
            ClientEvent::SessionClosed { token: TOKEN, .. }
        ]
    ));
    assert!(client.is_peer_closed());
    server.join();
}

#[test]
fn wrong_token_close_flag_does_not_close_session() {
    let params = default_params();
    let server = start_fake_server({
        let params = params.clone();
        move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            socket
                .send_to(
                    &open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None),
                    peer,
                )
                .unwrap();

            let (request, _) = recv_request(&socket, &tx);
            let seq = u32::from_le_bytes(request[12..16].try_into().unwrap());
            socket
                .send_to(
                    &echo_reply_packet_with_flags(
                        TOKEN.wrapping_add(1),
                        seq,
                        &params,
                        &TimestampFields::default(),
                        None,
                        FLAG_REPLY | flags::FLAG_CLOSE,
                    ),
                    peer,
                )
                .unwrap();

            let _ = recv_request(&socket, &tx);
            let _ = recv_request(&socket, &tx);
        }
    });
    let mut client = Client::connect(ClientConfig {
        socket_config: irtt_client::SocketConfig {
            recv_timeout: Some(Duration::from_millis(200)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    })
    .unwrap();
    assert_open_started(client.open().unwrap());
    client.send_probe().unwrap();

    assert!(matches!(
        client.recv_once().unwrap().as_slice(),
        [ClientEvent::Warning {
            kind: WarningKind::WrongToken,
            ..
        }]
    ));
    assert!(!client.is_peer_closed());
    assert!(matches!(
        client.send_probe().unwrap().as_slice(),
        [ClientEvent::EchoSent { seq: 1, .. }]
    ));
    client.close().unwrap();
    server.join();
}

#[test]
fn close_flagged_echo_reply_emits_reply_then_closes_without_sending_close() {
    let params = default_params();
    let server = start_fake_server({
        let params = params.clone();
        move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
            socket.send_to(&reply, peer).unwrap();

            let (request, _) = recv_request(&socket, &tx);
            let seq = u32::from_le_bytes(request[12..16].try_into().unwrap());
            let reply = echo_reply_packet_with_flags(
                TOKEN,
                seq,
                &params,
                &TimestampFields::default(),
                None,
                FLAG_REPLY | flags::FLAG_CLOSE,
            );
            socket.send_to(&reply, peer).unwrap();

            socket
                .set_read_timeout(Some(Duration::from_millis(250)))
                .unwrap();
            while recv_request_timeout(&socket, &tx).is_some() {}
        }
    });
    let config = ClientConfig {
        socket_config: irtt_client::SocketConfig {
            recv_timeout: Some(Duration::from_millis(200)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());
    client.send_probe().unwrap();

    let events = client.recv_once().unwrap();
    assert!(matches!(
        events.first(),
        Some(ClientEvent::EchoReply { .. })
    ));
    assert!(matches!(
        events.get(1),
        Some(ClientEvent::SessionClosed { token: TOKEN, .. })
    ));
    assert_eq!(events.len(), 2);

    let results = [
        client.recv_once(),
        client.recv_available(RecvBudget { max_packets: 1 }),
    ];
    assert!(
        matches!(
            results,
            [
                Err(ClientError::AlreadyClosed),
                Err(ClientError::AlreadyClosed)
            ]
        ),
        "unexpected receive results: {results:?}"
    );

    assert!(matches!(
        client.send_probe(),
        Err(ClientError::AlreadyClosed)
    ));

    let first = server.rx.recv_timeout(Duration::from_millis(100)).unwrap();
    let second = server.rx.recv_timeout(Duration::from_millis(100)).unwrap();
    assert_eq!(first[3] & FLAG_OPEN, FLAG_OPEN);
    assert_eq!(second[3] & flags::FLAG_CLOSE, 0);
    assert!(server.rx.recv_timeout(Duration::from_millis(400)).is_err());
    server.join();
}
