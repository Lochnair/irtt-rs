use super::*;
use irtt_client::{Authentication, HmacKey};

#[test]
fn hmac_open_ignores_missing_hmac_before_valid_reply() {
    let key = b"secret".to_vec();
    let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
    config.auth = Authentication::Hmac(HmacKey::new(key.clone()));
    let params = default_params();
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
    config.auth = Authentication::Hmac(HmacKey::new(key.clone()));
    let params = default_params();
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
    config.auth = Authentication::Hmac(HmacKey::new(key.clone()));
    let mut returned = default_params();
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
