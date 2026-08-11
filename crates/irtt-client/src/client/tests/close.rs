use super::*;

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
    assert_open_started(client.open().unwrap());
    client.close().unwrap();
    assert!(matches!(client.open(), Err(ClientError::AlreadyClosed)));
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
    assert_open_started(client.open().unwrap());
    let sent_at = ClientTimestamp {
        wall: SystemTime::UNIX_EPOCH + Duration::from_secs(1_234),
        mono: Instant::now() + Duration::from_secs(5),
    };
    client.test_hooks.close_sent_at.set(Some(sent_at));
    let events = client.close().unwrap();
    assert!(matches!(
        events.as_slice(),
        [ClientEvent::SessionClosed {
            token: TOKEN,
            at,
            ..
        }] if *at == sent_at
    ));
    assert!(!client.runtime.is_open());
    assert!(client.schedule.is_none());
    assert_eq!(client.applied_traffic_class, None);
    let packets: Vec<_> = server.rx.iter().take(2).collect();
    let close = &packets[1];
    assert_eq!(close[3], flags::FLAG_CLOSE);
    assert_eq!(u64::from_le_bytes(close[4..12].try_into().unwrap()), TOKEN);
    server.join();
}

#[test]
fn close_event_reservation_failure_precedes_dscp_clear_and_send() {
    let params = default_params();
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        socket
            .send_to(
                &open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None),
                peer,
            )
            .unwrap();
        let _ = recv_request(&socket, &tx);
    });
    let mut client = Client::connect(default_test_config(server.addr)).unwrap();
    assert_open_started(client.open().unwrap());
    client.test_hooks.fail_close_event_reserve.set(true);
    client.test_hooks.fail_close_dscp_clear.set(true);

    assert!(matches!(
        client.close(),
        Err(ClientError::AllocationFailed {
            operation: "close event result",
            ..
        })
    ));
    assert_eq!(client.test_hooks.close_send_attempts.get(), 0);
    assert!(client.test_hooks.fail_close_dscp_clear.get());
    assert!(client.runtime.is_open());
    assert!(client.schedule.is_some());
    assert_eq!(client.applied_traffic_class, Some(0));

    client.test_hooks.fail_close_dscp_clear.set(false);
    client.close().unwrap();
    assert_eq!(client.test_hooks.close_send_attempts.get(), 1);
    server.join();
}

#[test]
fn close_dscp_clear_failure_precedes_send_and_preserves_open_state() {
    let params = default_params();
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        socket
            .send_to(
                &open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None),
                peer,
            )
            .unwrap();
        let _ = recv_request(&socket, &tx);
    });
    let mut client = Client::connect(default_test_config(server.addr)).unwrap();
    assert_open_started(client.open().unwrap());
    client.test_hooks.fail_close_dscp_clear.set(true);

    assert!(matches!(
        client.close(),
        Err(ClientError::SocketOption { .. })
    ));
    assert_eq!(client.test_hooks.close_send_attempts.get(), 0);
    assert!(client.runtime.is_open());
    assert!(client.schedule.is_some());
    assert_eq!(client.applied_traffic_class, Some(0));

    client.close().unwrap();
    assert_eq!(client.test_hooks.close_send_attempts.get(), 1);
    server.join();
}

#[test]
fn close_send_failure_leaves_machine_and_schedule_open_for_retry() {
    let params = default_params();
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        socket
            .send_to(
                &open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None),
                peer,
            )
            .unwrap();
        let _ = recv_request(&socket, &tx);
    });
    let mut client = Client::connect(default_test_config(server.addr)).unwrap();
    assert_open_started(client.open().unwrap());
    let sent_at = ClientTimestamp {
        wall: SystemTime::UNIX_EPOCH + Duration::from_secs(2_345),
        mono: Instant::now() + Duration::from_secs(6),
    };
    client.test_hooks.close_sent_at.set(Some(sent_at));
    client.test_hooks.fail_close_send.set(true);

    assert!(matches!(client.close(), Err(ClientError::Socket(_))));
    assert_eq!(client.test_hooks.close_sent_at.get(), Some(sent_at));
    assert!(client.runtime.is_open());
    assert!(client.schedule.is_some());
    assert_eq!(client.applied_traffic_class, Some(0));
    assert_eq!(client.test_hooks.close_send_attempts.get(), 1);

    assert!(matches!(
        client.close().unwrap().as_slice(),
        [ClientEvent::SessionClosed {
            token: TOKEN,
            at,
            ..
        }] if *at == sent_at
    ));
    assert_eq!(client.test_hooks.close_sent_at.get(), None);
    assert_eq!(client.test_hooks.close_send_attempts.get(), 2);
    assert!(matches!(client.close(), Err(ClientError::AlreadyClosed)));
    server.join();
}

#[test]
fn short_successful_close_commits_before_length_mismatch() {
    let params = default_params();
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        socket
            .send_to(
                &open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None),
                peer,
            )
            .unwrap();
        let _ = recv_request(&socket, &tx);
    });
    let mut client = Client::connect(default_test_config(server.addr)).unwrap();
    assert_open_started(client.open().unwrap());
    let expected = client.runtime.prepare_close().unwrap().bytes.len();
    let sent_at = ClientTimestamp {
        wall: SystemTime::UNIX_EPOCH + Duration::from_secs(3_456),
        mono: Instant::now() + Duration::from_secs(7),
    };
    client.test_hooks.close_sent_at.set(Some(sent_at));
    client.test_hooks.close_reported_len.set(Some(expected - 1));

    assert!(matches!(
        client.close(),
        Err(ClientError::DatagramLengthMismatch {
            expected: error_expected,
            actual,
        }) if error_expected == expected && actual + 1 == expected
    ));
    assert!(!client.runtime.is_open());
    assert!(client.schedule.is_none());
    assert_eq!(client.applied_traffic_class, None);
    assert_eq!(client.test_hooks.close_sent_at.get(), None);
    assert!(matches!(client.close(), Err(ClientError::AlreadyClosed)));
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
        socket_config: crate::SocketConfig {
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
    assert!(client.next_send_deadline().is_none());
    assert!(client.schedule.is_none());
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

#[test]
fn peer_close_dscp_cleanup_failure_preserves_events_and_closed_state() {
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
            socket
                .set_read_timeout(Some(Duration::from_millis(250)))
                .unwrap();
            while recv_request_timeout(&socket, &tx).is_some() {}
        }
    });
    let mut client = Client::connect(ClientConfig {
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(200)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    })
    .unwrap();
    assert_open_started(client.open().unwrap());
    client.send_probe().unwrap();
    client.test_hooks.fail_close_dscp_clear.set(true);

    assert!(matches!(
        client.recv_once().unwrap().as_slice(),
        [
            ClientEvent::EchoReply { .. },
            ClientEvent::SessionClosed { token: TOKEN, .. }
        ]
    ));
    assert!(client.is_peer_closed());
    assert!(client.schedule.is_none());
    assert_eq!(client.applied_traffic_class, Some(0));
    assert_eq!(client.test_hooks.close_send_attempts.get(), 0);
    let open = server.rx.recv_timeout(Duration::from_millis(100)).unwrap();
    let echo = server.rx.recv_timeout(Duration::from_millis(100)).unwrap();
    assert_ne!(open[3] & FLAG_OPEN, 0);
    assert_eq!(echo[3] & flags::FLAG_CLOSE, 0);
    assert!(server.rx.recv_timeout(Duration::from_millis(400)).is_err());
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
fn normal_echo_reply_does_not_close_session() {
    let params = default_params();
    let (mut client, server) = open_client_with_echo_server(&params);
    client.send_probe().unwrap();

    let events = client.recv_once().unwrap();
    assert!(matches!(events.as_slice(), [ClientEvent::EchoReply { .. }]));
    assert!(client.next_send_deadline().is_some());
    assert!(client.send_probe().is_ok());

    client.close().unwrap();
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
        socket_config: crate::SocketConfig {
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
        socket_config: crate::SocketConfig {
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
        socket_config: crate::SocketConfig {
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
        socket_config: crate::SocketConfig {
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
        socket_config: crate::SocketConfig {
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
