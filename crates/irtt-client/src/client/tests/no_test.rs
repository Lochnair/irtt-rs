use super::*;

#[test]
fn no_test_success_validates_params() {
    let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
    config.run_mode = RunMode::NoTest;
    let params = params_from_config(&config).unwrap();
    let server = no_test_server(params.clone(), 0);
    config.server_addr = server.addr.to_string();
    let mut client = Client::connect(config).unwrap();
    assert!(client.next_send_deadline().is_none());
    let negotiated = assert_no_test_completed(client.open().unwrap());
    assert_eq!(negotiated.params, params);
    assert!(client.next_send_deadline().is_none());
    assert!(client.schedule.is_none());
    assert_eq!(client.applied_traffic_class, None);
    server.join();
}

#[test]
fn no_test_rejects_non_close_open_reply() {
    let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
    config.run_mode = RunMode::NoTest;
    let params = params_from_config(&config).unwrap();
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
    config.server_addr = server.addr.to_string();
    let mut client = Client::connect(config).unwrap();
    assert!(matches!(
        client.open(),
        Err(ClientError::UnexpectedNoTestReply)
    ));
    let packets: Vec<_> = server.rx.iter().take(2).collect();
    assert_eq!(packets[1][3], flags::FLAG_CLOSE);
    assert_eq!(
        u64::from_le_bytes(packets[1][4..12].try_into().unwrap()),
        TOKEN
    );
    assert!(client.runtime.prepare_open_request().is_ok());
    server.join();
}

#[test]
fn no_test_cleanup_send_failure_preserves_unexpected_reply() {
    let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
    config.run_mode = RunMode::NoTest;
    let params = params_from_config(&config).unwrap();
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        socket
            .send_to(
                &open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None),
                peer,
            )
            .unwrap();
    });
    config.server_addr = server.addr.to_string();
    let mut client = Client::connect(config).unwrap();
    client.test_hooks.fail_cleanup_send.set(true);

    assert!(matches!(
        client.open(),
        Err(ClientError::UnexpectedNoTestReply)
    ));
    assert!(client.runtime.prepare_open_request().is_ok());
    server.join();
}

#[test]
fn no_test_rejects_non_zero_token_with_close_reply() {
    let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
    config.run_mode = RunMode::NoTest;
    let params = params_from_config(&config).unwrap();
    let server = no_test_server(params, TOKEN);
    config.server_addr = server.addr.to_string();
    let mut client = Client::connect(config).unwrap();
    assert!(matches!(
        client.open(),
        Err(ClientError::NonZeroNoTestToken { token: TOKEN })
    ));
    server.join();
}

#[test]
fn no_test_strict_negotiation_rejects_changed_params() {
    let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
    config.run_mode = RunMode::NoTest;
    let mut params = params_from_config(&config).unwrap();
    params.dscp = 1;
    let server = no_test_server(params, 0);
    config.server_addr = server.addr.to_string();
    let mut client = Client::connect(config).unwrap();
    assert!(matches!(
        client.open(),
        Err(ClientError::NegotiationRejected { .. })
    ));
    server.join();
}

#[test]
fn no_test_loose_negotiation_accepts_restricted_params() {
    let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
    config.run_mode = RunMode::NoTest;
    config.negotiation_policy = NegotiationPolicy::Loose;
    let requested = params_from_config(&config).unwrap();
    let mut params = requested.clone();
    params.duration_ns /= 2;
    let server = no_test_server(params.clone(), 0);
    config.server_addr = server.addr.to_string();
    let mut client = Client::connect(config).unwrap();
    let negotiated = assert_no_test_completed(client.open().unwrap());
    assert_eq!(negotiated.params, params);
    assert_eq!(
        negotiated.restrictions,
        vec![crate::NegotiationRestriction::DurationReduced {
            requested_ns: requested.duration_ns,
            negotiated_ns: params.duration_ns,
        }]
    );
    server.join();
}

#[test]
fn send_probe_fails_after_no_test_completed() {
    let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
    config.run_mode = RunMode::NoTest;
    let params = params_from_config(&config).unwrap();
    let server = no_test_server(params, 0);
    config.server_addr = server.addr.to_string();
    let mut client = Client::connect(config).unwrap();
    assert_no_test_completed(client.open().unwrap());
    assert!(matches!(
        client.send_probe(),
        Err(ClientError::AlreadyCompleted)
    ));
    server.join();
}

#[test]
fn open_fails_after_no_test_completed() {
    let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
    config.run_mode = RunMode::NoTest;
    let params = params_from_config(&config).unwrap();
    let server = no_test_server(params, 0);
    config.server_addr = server.addr.to_string();
    let mut client = Client::connect(config).unwrap();
    assert_no_test_completed(client.open().unwrap());
    assert!(matches!(client.open(), Err(ClientError::AlreadyCompleted)));
    server.join();
}
