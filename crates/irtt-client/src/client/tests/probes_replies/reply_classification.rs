use super::*;

#[test]
fn wrong_token_reply_is_dropped() {
    let params = default_params();
    let wrong_token: u64 = 0xDEAD_BEEF_CAFE_BABE;
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
            let reply_packet = echo_reply_packet(wrong_token, seq, &params, &ts, None);
            socket.send_to(&reply_packet, peer).unwrap();
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
    thread::sleep(Duration::from_millis(30));
    let events = client.recv_once().unwrap();
    assert_eq!(events.len(), 1);
    assert!(matches!(&events[0], ClientEvent::Warning { .. }));
    server.join();
}

#[test]
fn duplicate_reply_emits_duplicate_event() {
    let params = default_params();
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
            thread::sleep(Duration::from_millis(10));
            socket.send_to(&reply_packet, peer).unwrap();
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
    thread::sleep(Duration::from_millis(50));

    let events1 = client.recv_once().unwrap();
    assert_eq!(events1.len(), 1);
    assert!(matches!(&events1[0], ClientEvent::EchoReply { .. }));

    thread::sleep(Duration::from_millis(30));
    let events2 = client.recv_once().unwrap();
    assert_eq!(events2.len(), 1);
    assert!(matches!(
        &events2[0],
        ClientEvent::DuplicateReply { seq: 0, .. }
    ));
    server.join();
}

#[test]
fn out_of_order_reply_emits_late_event() {
    let params = default_params();
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
        socket.send_to(&reply, peer).unwrap();

        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();

        let mut seqs = Vec::new();
        for _ in 0..2 {
            let mut buf = [0_u8; 2048];
            if let Ok((size, _)) = socket.recv_from(&mut buf) {
                tx.send(buf[..size].to_vec()).unwrap();
                let seq = u32::from_le_bytes(buf[12..16].try_into().unwrap());
                seqs.push(seq);
            }
        }
        let ts = TimestampFields::default();
        let reply1 = echo_reply_packet(TOKEN, seqs[1], &params, &ts, None);
        socket.send_to(&reply1, peer).unwrap();
        thread::sleep(Duration::from_millis(10));
        let reply0 = echo_reply_packet(TOKEN, seqs[0], &params, &ts, None);
        socket.send_to(&reply0, peer).unwrap();
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
    client.send_probe().unwrap();
    thread::sleep(Duration::from_millis(50));

    let ev1 = client.recv_once().unwrap();
    assert_eq!(ev1.len(), 1);
    assert!(matches!(&ev1[0], ClientEvent::EchoReply { seq: 1, .. }));

    thread::sleep(Duration::from_millis(30));
    let ev2 = client.recv_once().unwrap();
    assert_eq!(ev2.len(), 1);
    match &ev2[0] {
        ClientEvent::LateReply {
            seq,
            highest_seen,
            rtt,
            ..
        } => {
            assert_eq!(*seq, 0);
            assert_eq!(*highest_seen, 1);
            assert!(rtt.is_some());
        }
        other => panic!("expected LateReply, got {other:?}"),
    }
    server.join();
}

#[test]
fn unmatched_future_reply_emits_warning_not_late() {
    let params = default_params();
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
            thread::sleep(Duration::from_millis(10));
            let future_reply = echo_reply_packet(TOKEN, 999, &params, &ts, None);
            socket.send_to(&future_reply, peer).unwrap();
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
    thread::sleep(Duration::from_millis(50));

    let ev1 = client.recv_once().unwrap();
    assert!(matches!(&ev1[0], ClientEvent::EchoReply { seq: 0, .. }));

    thread::sleep(Duration::from_millis(30));
    let ev2 = client.recv_once().unwrap();
    assert_eq!(ev2.len(), 1);
    assert!(
        matches!(&ev2[0], ClientEvent::Warning { .. }),
        "unmatched future reply should emit Warning, got {:?}",
        ev2[0]
    );
    server.join();
}

#[test]
fn unmatched_future_reply_does_not_update_highest_received_seq() {
    let params = default_params();
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
        socket.send_to(&reply, peer).unwrap();

        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();

        let mut seqs = Vec::new();
        for _ in 0..2 {
            let mut buf = [0_u8; 2048];
            if let Ok((size, _)) = socket.recv_from(&mut buf) {
                tx.send(buf[..size].to_vec()).unwrap();
                let seq = u32::from_le_bytes(buf[12..16].try_into().unwrap());
                seqs.push(seq);
            }
        }

        let ts = TimestampFields::default();
        let reply0 = echo_reply_packet(TOKEN, seqs[0], &params, &ts, None);
        socket.send_to(&reply0, peer).unwrap();
        thread::sleep(Duration::from_millis(10));
        let future_reply = echo_reply_packet(TOKEN, 999, &params, &ts, None);
        socket.send_to(&future_reply, peer).unwrap();
        thread::sleep(Duration::from_millis(10));
        let reply1 = echo_reply_packet(TOKEN, seqs[1], &params, &ts, None);
        socket.send_to(&reply1, peer).unwrap();
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
    client.send_probe().unwrap();
    thread::sleep(Duration::from_millis(50));

    let ev0 = client.recv_once().unwrap();
    assert!(matches!(&ev0[0], ClientEvent::EchoReply { seq: 0, .. }));

    thread::sleep(Duration::from_millis(30));
    let ev_future = client.recv_once().unwrap();
    assert!(matches!(&ev_future[0], ClientEvent::Warning { .. }));

    thread::sleep(Duration::from_millis(30));
    let ev1 = client.recv_once().unwrap();
    assert!(
        matches!(&ev1[0], ClientEvent::EchoReply { seq: 1, .. }),
        "valid pending reply seq=1 should not be poisoned, got {:?}",
        ev1[0]
    );
    server.join();
}
