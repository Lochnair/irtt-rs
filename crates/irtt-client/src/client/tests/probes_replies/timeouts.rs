use super::*;

#[test]
fn poll_timeouts_emits_echo_loss() {
    let params = default_params();
    let server = silent_open_server(params);
    let config = ClientConfig {
        probe_timeout: Duration::from_millis(100),
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(50)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());

    client.send_probe().unwrap();
    client.send_probe().unwrap();

    let no_loss = client.poll_timeouts().unwrap();
    assert!(no_loss.is_empty());

    thread::sleep(Duration::from_millis(150));
    let events = client.poll_timeouts().unwrap();
    assert_eq!(events.len(), 2);
    for event in &events {
        assert!(matches!(event, ClientEvent::EchoLoss { .. }));
    }
    server.join();
}

#[test]
fn late_reply_after_timeout_preserves_measurement_metadata() {
    let params = default_params();
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
        socket.send_to(&reply, peer).unwrap();

        let mut buf = [0_u8; 2048];
        let (size, _) = socket.recv_from(&mut buf).unwrap();
        tx.send(buf[..size].to_vec()).unwrap();
        let seq = u32::from_le_bytes(buf[12..16].try_into().unwrap());

        thread::sleep(Duration::from_millis(90));
        let ts = TimestampFields {
            recv_wall: Some(1_000_000_000),
            recv_mono: Some(100_000),
            send_wall: Some(1_000_100_000),
            send_mono: Some(200_000),
            ..Default::default()
        };
        let reply_packet = echo_reply_packet(TOKEN, seq, &params, &ts, None);
        socket.send_to(&reply_packet, peer).unwrap();
        socket.send_to(&reply_packet, peer).unwrap();
    });
    let config = ClientConfig {
        probe_timeout: Duration::from_millis(40),
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(200)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());

    client.send_probe().unwrap();
    thread::sleep(Duration::from_millis(60));
    let losses = client.poll_timeouts().unwrap();
    assert!(matches!(&losses[0], ClientEvent::EchoLoss { seq: 0, .. }));

    let late = client.recv_once().unwrap();
    match &late[0] {
        ClientEvent::LateReply {
            seq,
            sent_at,
            rtt,
            server_timing,
            one_way,
            bytes,
            packet_meta,
            ..
        } => {
            assert_eq!(*seq, 0);
            assert!(sent_at.is_some());
            assert!(rtt.is_some());
            assert!(server_timing.is_some());
            assert!(one_way.is_some());
            assert_eq!(*bytes, echo_packet_len(false, &default_params()));
            let _ = packet_meta;
            #[cfg(not(all(target_os = "linux", feature = "ancillary")))]
            assert_packet_meta_unavailable(packet_meta);
        }
        other => panic!("expected stats-eligible LateReply, got {other:?}"),
    }
    let duplicate = client.recv_once().unwrap();
    assert!(matches!(
        &duplicate[0],
        ClientEvent::DuplicateReply {
            seq: 0,
            bytes,
            ..
        } if *bytes == echo_packet_len(false, &default_params())
    ));

    client.close().unwrap();
    server.join();
}

#[test]
fn pending_full_does_not_send_packet() {
    let params = Params {
        duration_ns: 60_000_000_000,
        ..default_params()
    };
    let server = silent_open_server(params);
    let config = ClientConfig {
        duration: Some(Duration::from_secs(60)),
        max_pending_probes: 2,
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(50)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());

    client.send_probe().unwrap();
    client.send_probe().unwrap();

    thread::sleep(Duration::from_millis(30));
    let before_count: Vec<_> = server.rx.try_iter().collect();
    let echo_before: Vec<_> = before_count
        .iter()
        .filter(|p| p.len() >= 4 && p[3] & FLAG_OPEN == 0 && p[3] & flags::FLAG_CLOSE == 0)
        .collect();
    assert_eq!(echo_before.len(), 2);

    assert!(matches!(
        client.send_probe(),
        Err(ClientError::PendingLimitExceeded { limit: 2 })
    ));

    thread::sleep(Duration::from_millis(30));
    let after: Vec<_> = server.rx.try_iter().collect();
    let echo_after: Vec<_> = after
        .iter()
        .filter(|p| p.len() >= 4 && p[3] & FLAG_OPEN == 0 && p[3] & flags::FLAG_CLOSE == 0)
        .collect();
    assert_eq!(
        echo_after.len(),
        0,
        "no packet should be sent when pending is full"
    );

    client.close().unwrap();
    server.join();
}
