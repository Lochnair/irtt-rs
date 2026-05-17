use super::*;

#[test]
fn send_probe_fails_before_open() {
    let server = start_fake_server(|_socket, _tx| {});
    let mut client = Client::connect(default_test_config(server.addr)).unwrap();
    assert!(matches!(client.send_probe(), Err(ClientError::NotOpen)));
    server.join();
}

#[test]
fn send_probe_sends_valid_echo_request() {
    let params = default_params();
    let server = silent_open_server(params.clone());
    let config = ClientConfig {
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(200)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());
    let events = client.send_probe().unwrap();
    assert_eq!(events.len(), 1);
    match &events[0] {
        ClientEvent::EchoSent {
            seq, remote, bytes, ..
        } => {
            assert_eq!(*seq, 0);
            assert_eq!(*remote, server.addr);
            assert_eq!(*bytes, echo_packet_len(false, &params));
        }
        other => panic!("expected EchoSent, got {other:?}"),
    }
    thread::sleep(Duration::from_millis(30));
    let packets: Vec<_> = server.rx.try_iter().collect();
    let echo_reqs: Vec<_> = packets
        .iter()
        .filter(|p| p.len() >= 4 && p[3] & FLAG_OPEN == 0)
        .collect();
    let echo_req = echo_reqs.first().unwrap();
    assert_eq!(&echo_req[..3], &MAGIC);
    assert_eq!(echo_req[3], 0x00);
    let req_token = u64::from_le_bytes(echo_req[4..12].try_into().unwrap());
    assert_eq!(req_token, TOKEN);
    let seq = u32::from_le_bytes(echo_req[12..16].try_into().unwrap());
    assert_eq!(seq, 0);
    client.close().unwrap();
    server.join();
}

#[test]
fn send_probe_respects_finite_duration_exclusive_end() {
    let params = Params {
        protocol_version: 1,
        duration_ns: 1_000_000_000,
        interval_ns: 500_000_000,
        received_stats: ReceivedStats::Both,
        stamp_at: StampAt::Both,
        clock: Clock::Both,
        ..Params::default()
    };
    let server = silent_open_server(params.clone());
    let config = ClientConfig {
        duration: Some(Duration::from_secs(1)),
        interval: Duration::from_millis(500),
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(200)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());

    let start = client.next_send_deadline().unwrap();
    let interval = Duration::from_millis(500);

    let now0 = ClientTimestamp {
        mono: start,
        wall: SystemTime::now(),
    };
    assert!(client.send_probe_at(now0).is_ok());

    let now1 = ClientTimestamp {
        mono: start + interval,
        wall: SystemTime::now(),
    };
    assert!(client.send_probe_at(now1).is_ok());

    let now2 = ClientTimestamp {
        mono: start + Duration::from_secs(1),
        wall: SystemTime::now(),
    };
    let events = client.send_probe_at(now2).unwrap();
    assert!(events.is_empty());
    assert!(client.next_send_deadline().is_none());

    client.close().unwrap();
    server.join();
}

#[test]
fn connect_rejects_invalid_probe_limits() {
    for config in [
        ClientConfig {
            max_pending_probes: 0,
            ..ClientConfig::default()
        },
        ClientConfig {
            probe_timeout: Duration::ZERO,
            ..ClientConfig::default()
        },
    ] {
        assert!(matches!(
            Client::connect(config),
            Err(ClientError::InvalidConfig { .. })
        ));
    }
}
