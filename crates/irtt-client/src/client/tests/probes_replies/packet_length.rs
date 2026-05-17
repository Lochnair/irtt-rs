use super::*;

#[test]
fn minimal_negotiated_layout_works() {
    let params = Params {
        protocol_version: 1,
        duration_ns: 3_000_000_000,
        interval_ns: 1_000_000_000,
        received_stats: ReceivedStats::None,
        stamp_at: StampAt::None,
        clock: Clock::Both,
        ..Params::default()
    };
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
        socket.send_to(&reply, peer).unwrap();

        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut buf = [0_u8; 2048];
        if let Ok((size, _)) = socket.recv_from(&mut buf) {
            tx.send(buf[..size].to_vec()).unwrap();
            let seq = u32::from_le_bytes(buf[12..16].try_into().unwrap());
            let ts = TimestampFields::default();
            let reply_packet = echo_reply_packet(TOKEN, seq, &params, &ts, None);
            socket.send_to(&reply_packet, peer).unwrap();
        }
    });
    let config = ClientConfig {
        received_stats: ReceivedStats::None,
        stamp_at: StampAt::None,
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
    if let ClientEvent::EchoReply {
        received_stats,
        server_timing,
        ..
    } = &events[0]
    {
        assert!(received_stats.is_none());
        assert!(server_timing.is_none());
    } else {
        panic!("expected EchoReply");
    }
    server.join();
}

// ---------- Regression tests ----------

#[test]
fn short_echo_reply_does_not_emit_echo_reply() {
    let params = Params {
        length: 64,
        ..default_params()
    };
    let server = start_fake_server({
        let params = params.clone();
        move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
            socket.send_to(&reply, peer).unwrap();
            let (request, _) = recv_request(&socket, &tx);
            let seq = u32::from_le_bytes(request[12..16].try_into().unwrap());
            let mut reply =
                echo_reply_packet(TOKEN, seq, &params, &TimestampFields::default(), None);
            reply.truncate(echo_packet_len(false, &params) - 1);
            socket.send_to(&reply, peer).unwrap();
        }
    });
    let config = ClientConfig {
        length: 64,
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
        events.as_slice(),
        [ClientEvent::Warning {
            kind: WarningKind::MalformedOrUnrelatedPacket,
            ..
        }]
    ));
    client.close().unwrap();
    server.join();
}

#[test]
fn overlong_datagram_detection_uses_extra_receive_byte() {
    let params = Params {
        length: 4096,
        ..default_params()
    };
    let server = start_fake_server({
        let params = params.clone();
        move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
            socket.send_to(&reply, peer).unwrap();
            let (request, _) = recv_request(&socket, &tx);
            let seq = u32::from_le_bytes(request[12..16].try_into().unwrap());
            let mut reply =
                echo_reply_packet(TOKEN, seq, &params, &TimestampFields::default(), None);
            reply.push(0);
            socket.send_to(&reply, peer).unwrap();
            let _ = recv_request_timeout(&socket, &tx);
        }
    });
    let config = ClientConfig {
        length: 4096,
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
        events.as_slice(),
        [ClientEvent::Warning {
            kind: WarningKind::MalformedOrUnrelatedPacket,
            ..
        }]
    ));
    client.close().unwrap();
    server.join();
}

#[test]
fn exact_length_echo_reply_still_emits_echo_reply() {
    let params = Params {
        length: 4096,
        ..default_params()
    };
    let server = start_fake_server({
        let params = params.clone();
        move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
            socket.send_to(&reply, peer).unwrap();
            let (request, _) = recv_request(&socket, &tx);
            let seq = u32::from_le_bytes(request[12..16].try_into().unwrap());
            let reply = echo_reply_packet(TOKEN, seq, &params, &TimestampFields::default(), None);
            socket.send_to(&reply, peer).unwrap();
            let _ = recv_request_timeout(&socket, &tx);
        }
    });
    let config = ClientConfig {
        length: 4096,
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
        events.as_slice(),
        [ClientEvent::EchoReply { bytes, .. }] if *bytes == echo_packet_len(false, &params)
    ));
    client.close().unwrap();
    server.join();
}
