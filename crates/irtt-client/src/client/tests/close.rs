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
    assert_open_started(client.open(ClientTimestamp::now()).unwrap());
    client.close(ClientTimestamp::now()).unwrap();
    assert!(matches!(
        client.open(ClientTimestamp::now()),
        Err(ClientError::AlreadyClosed)
    ));
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
    assert_open_started(client.open(ClientTimestamp::now()).unwrap());
    let events = client.close(ClientTimestamp::now()).unwrap();
    assert_eq!(events.len(), 1);
    let packets: Vec<_> = server.rx.iter().take(2).collect();
    let close = &packets[1];
    assert_eq!(close[3], flags::FLAG_CLOSE);
    assert_eq!(u64::from_le_bytes(close[4..12].try_into().unwrap()), TOKEN);
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
    assert_open_started(client.open(ClientTimestamp::now()).unwrap());
    client.close(ClientTimestamp::now()).unwrap();
    assert!(matches!(
        client.send_probe(),
        Err(ClientError::AlreadyClosed)
    ));
    server.join();
}

#[test]
fn close_clears_timed_out_metadata() {
    let params = default_params();
    let server = silent_open_server(params);
    let config = ClientConfig {
        probe_timeout: Duration::from_millis(40),
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(50)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open(ClientTimestamp::now()).unwrap());

    client.send_probe().unwrap();
    thread::sleep(Duration::from_millis(60));
    client.poll_timeouts(ClientTimestamp::now()).unwrap();
    assert_eq!(client.session.as_ref().unwrap().timed_out.len(), 1);

    client.close(ClientTimestamp::now()).unwrap();
    assert!(client.session.is_none());
    server.join();
}
