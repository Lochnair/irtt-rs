use super::*;

#[test]
fn no_test_open_close_succeeds_on_open_reply_close() {
    let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
    config.run_mode = RunMode::NoTest;
    let params = params_from_config(&config).unwrap();
    let server = no_test_server(params.clone(), 0);
    config.server_addr = server.addr.to_string();
    let mut client = Client::connect(config).unwrap();
    let negotiated = assert_no_test_completed(client.open().unwrap());
    assert_eq!(negotiated.params, params);
    assert_eq!(client.negotiated.as_ref(), Some(&negotiated));
    assert!(matches!(
        client.close(ClientTimestamp::now()),
        Err(ClientError::NotOpen)
    ));
    server.join();
}

#[test]
fn no_test_success_validates_params() {
    let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
    config.run_mode = RunMode::NoTest;
    let params = params_from_config(&config).unwrap();
    let server = no_test_server(params.clone(), 0);
    config.server_addr = server.addr.to_string();
    let mut client = Client::connect(config).unwrap();
    let negotiated = assert_no_test_completed(client.open().unwrap());
    assert_eq!(negotiated.params, params);
    server.join();
}

#[test]
fn no_test_rejects_non_close_open_reply() {
    let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
    config.run_mode = RunMode::NoTest;
    let params = params_from_config(&config).unwrap();
    let server = open_success_server(params);
    config.server_addr = server.addr.to_string();
    let mut client = Client::connect(config).unwrap();
    assert!(matches!(
        client.open(),
        Err(ClientError::UnexpectedNoTestReply)
    ));
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
    let mut params = params_from_config(&config).unwrap();
    params.duration_ns /= 2;
    let server = no_test_server(params.clone(), 0);
    config.server_addr = server.addr.to_string();
    let mut client = Client::connect(config).unwrap();
    let negotiated = assert_no_test_completed(client.open().unwrap());
    assert_eq!(negotiated.params, params);
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
    assert!(matches!(
        client.open(),
        Err(ClientError::AlreadyCompleted)
    ));
    server.join();
}
