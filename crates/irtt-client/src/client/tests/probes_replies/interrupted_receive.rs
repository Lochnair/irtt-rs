use super::*;

/// An interrupted receive (`io::ErrorKind::Interrupted`, e.g. `EINTR`) must be
/// invisible to the caller: retried transparently, with the eventual reply
/// classified exactly as if no interruption had occurred.
#[test]
fn interrupted_receive_is_retried_and_the_reply_is_still_classified() {
    let params = default_params();
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
        socket.send_to(&reply, peer).unwrap();

        let mut buf = [0_u8; 2048];
        let (size, _) = socket.recv_from(&mut buf).unwrap();
        let seq = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        let _ = size;
        let ts = TimestampFields::default();
        let reply_packet = echo_reply_packet(TOKEN, seq, &params, &ts, None);
        socket.send_to(&reply_packet, peer).unwrap();
    });
    let config = ClientConfig {
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_secs(2)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());

    client.send_probe().unwrap();
    thread::sleep(Duration::from_millis(50));

    client.test_hooks.inject_recv_interrupted.set(10);
    let events = client.recv_once().unwrap();

    assert_eq!(events.len(), 1);
    assert!(
        matches!(&events[0], ClientEvent::EchoReply { seq: 0, .. }),
        "expected a normal EchoReply despite interrupted receive attempts, got {:?}",
        events[0]
    );

    client.close().unwrap();
    server.join();
}

/// One logical receive keeps one timeout budget: repeated interruptions must
/// not extend a `recv_once` call past its configured receive timeout.
#[test]
fn interrupted_receive_does_not_extend_the_configured_timeout() {
    let params = default_params();
    let server = silent_open_server(params);
    let config = ClientConfig {
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(100)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());

    // No reply is ever coming; only interrupted attempts occur before the
    // real socket takes over and legitimately times out.
    client.test_hooks.inject_recv_interrupted.set(20);
    let started = Instant::now();
    let events = client.recv_once().unwrap();
    let elapsed = started.elapsed();

    assert!(events.is_empty());
    assert!(
        elapsed < Duration::from_millis(500),
        "interrupted receives must not extend the configured receive timeout, took {elapsed:?}"
    );

    client.close().unwrap();
    server.join();
}
