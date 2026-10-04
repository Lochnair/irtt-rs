use super::*;

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
        request: SessionRequest {
            length: 64,
            ..default_test_config().request
        },
        ..default_test_config()
    };
    let mut client = Client::connect(server.addr.to_string(), config).unwrap();
    client
        .set_recv_timeout(Some(Duration::from_millis(200)))
        .unwrap();
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
        request: SessionRequest {
            length: 4096,
            ..default_test_config().request
        },
        ..default_test_config()
    };
    let mut client = Client::connect(server.addr.to_string(), config).unwrap();
    client
        .set_recv_timeout(Some(Duration::from_millis(200)))
        .unwrap();
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
        request: SessionRequest {
            length: 4096,
            ..default_test_config().request
        },
        ..default_test_config()
    };
    let mut client = Client::connect(server.addr.to_string(), config).unwrap();
    client
        .set_recv_timeout(Some(Duration::from_millis(200)))
        .unwrap();
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
