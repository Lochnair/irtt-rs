use super::*;

#[test]
fn no_test_rejects_non_close_open_reply() {
    let mut config = default_test_config();
    config.request.run_mode = RunMode::NoTest;
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
    let mut client = Client::connect(server.addr.to_string(), config).unwrap();
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
    server.join();
}

#[test]
fn no_test_rejects_non_zero_token_with_close_reply() {
    let mut config = default_test_config();
    config.request.run_mode = RunMode::NoTest;
    let params = default_params();
    let server = no_test_server(params, TOKEN);
    let mut client = Client::connect(server.addr.to_string(), config).unwrap();
    assert!(matches!(
        client.open(),
        Err(ClientError::NonZeroNoTestToken { token: TOKEN })
    ));
    server.join();
}

#[test]
fn no_test_strict_negotiation_rejects_changed_params() {
    let mut config = default_test_config();
    config.request.run_mode = RunMode::NoTest;
    let mut params = default_params();
    params.dscp = 1;
    let server = no_test_server(params, 0);
    let mut client = Client::connect(server.addr.to_string(), config).unwrap();
    assert!(matches!(
        client.open(),
        Err(ClientError::NegotiationRejected { .. })
    ));
    server.join();
}

#[test]
fn no_test_loose_negotiation_accepts_restricted_params() {
    for requested_duration in [Some(Duration::from_secs(3)), None] {
        let mut config = default_test_config();
        config.request.run_mode = RunMode::NoTest;
        config.request.duration = requested_duration;
        config.open.negotiation = NegotiationPolicy::Loose;
        let mut params = default_params();
        params.duration_ns = 1_500_000_000;
        let server = no_test_server(params.clone(), 0);
        let mut client = Client::connect(server.addr.to_string(), config).unwrap();
        let negotiated = assert_no_test_completed(client.open().unwrap());
        assert_eq!(negotiated.peer_params, params);
        assert_eq!(
            negotiated.accepted.duration,
            Some(Duration::from_millis(1500))
        );
        assert_eq!(
            negotiated.changes,
            vec![irtt_client::NegotiationChange::DurationReduced {
                requested: requested_duration,
                negotiated: Duration::from_millis(1500),
            }]
        );
        assert_eq!(
            client.negotiation(),
            None,
            "no-test creates no live session"
        );
        server.join();
    }
}

#[test]
fn send_probe_fails_after_no_test_completed() {
    let mut config = default_test_config();
    config.request.run_mode = RunMode::NoTest;
    let params = default_params();
    let server = no_test_server(params, 0);
    let mut client = Client::connect(server.addr.to_string(), config).unwrap();
    assert_no_test_completed(client.open().unwrap());
    assert!(matches!(
        client.send_probe(),
        Err(irtt_client::SendProbeError::NotCommitted(
            ClientError::AlreadyCompleted
        ))
    ));
    server.join();
}

#[test]
fn operations_fail_after_no_test_completed_without_datagrams() {
    let mut config = default_test_config();
    config.request.run_mode = RunMode::NoTest;

    let params = default_params();
    let server = no_test_server(params, 0);
    let mut client = Client::connect(server.addr.to_string(), config).unwrap();
    client
        .set_recv_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    assert_no_test_completed(client.open().unwrap());
    assert!(matches!(client.open(), Err(ClientError::AlreadyCompleted)));
    let results = [
        client.recv_once(),
        client.recv_available(RecvBudget { max_packets: 1 }),
    ];
    assert!(
        matches!(
            results,
            [
                Err(ClientError::AlreadyCompleted),
                Err(ClientError::AlreadyCompleted)
            ]
        ),
        "unexpected receive results: {results:?}"
    );
    server.join();
}
