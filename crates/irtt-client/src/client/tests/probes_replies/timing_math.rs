use super::*;

#[test]
fn echo_reply_rtt_uses_client_monotonic() {
    let params = default_params();
    let (mut client, server) = open_client_with_echo_server(&params);

    client.send_probe().unwrap();
    thread::sleep(Duration::from_millis(20));

    let events = client.recv_once().unwrap();
    if let ClientEvent::EchoReply { rtt, .. } = &events[0] {
        assert!(rtt.raw >= Duration::from_millis(15));
    } else {
        panic!("expected EchoReply");
    }
    client.close().unwrap();
    server.join();
}

#[test]
fn server_processing_subtracted_when_valid() {
    let params = default_params();
    let (mut client, server) = open_client_with_echo_server(&params);
    client.send_probe().unwrap();
    thread::sleep(Duration::from_millis(20));
    let events = client.recv_once().unwrap();
    if let ClientEvent::EchoReply {
        rtt, server_timing, ..
    } = &events[0]
    {
        let st = server_timing.as_ref().unwrap();
        let processing = st.processing.unwrap();
        assert!(processing > Duration::ZERO);
        if let Some(adj) = rtt.adjusted {
            assert!(adj < rtt.raw);
            assert_eq!(rtt.effective, adj);
        }
    } else {
        panic!("expected EchoReply");
    }
    client.close().unwrap();
    server.join();
}

#[test]
fn server_processing_greater_than_raw_does_not_underflow() {
    let base = Instant::now();
    let rtt = compute_rtt(
        &ClientTimestamp {
            mono: base,
            wall: SystemTime::now(),
        },
        &ClientTimestamp {
            mono: base + Duration::from_nanos(1),
            wall: SystemTime::now(),
        },
        &TimestampFields {
            recv_mono: Some(0),
            send_mono: Some(1_000_000_000),
            ..Default::default()
        },
    );
    assert!(rtt.adjusted.is_none());
    assert_eq!(rtt.effective, rtt.raw);
    assert_eq!(
        rtt.adjusted_signed,
        Some(SignedDuration { ns: -999_999_999 })
    );
    assert_eq!(rtt.effective_signed, SignedDuration { ns: -999_999_999 });
}

#[test]
fn compute_one_way_returns_none_when_both_directions_fail() {
    let ts = TimestampFields::default();
    let now = ClientTimestamp::now();
    let result = compute_one_way(&now, &now, &ts);
    assert!(result.is_none());
}

#[test]
fn unix_epoch_ns_i64_rejects_out_of_range_wall_time() {
    let i64_max_ns = u64::try_from(i64::MAX).unwrap();
    let max = SystemTime::UNIX_EPOCH + Duration::from_nanos(i64_max_ns);
    let overflow = max + Duration::from_nanos(1);

    assert_eq!(unix_epoch_ns_i64(max), Some(i64::MAX));
    assert_eq!(unix_epoch_ns_i64(overflow), None);
}

#[test]
fn compute_one_way_omits_samples_when_client_wall_time_overflows_i64() {
    let i64_max_ns = u64::try_from(i64::MAX).unwrap();
    let overflow_wall =
        SystemTime::UNIX_EPOCH + Duration::from_nanos(i64_max_ns) + Duration::from_nanos(1);
    let sent_at = ClientTimestamp {
        mono: Instant::now(),
        wall: overflow_wall,
    };
    let received_at = ClientTimestamp {
        mono: sent_at.mono + Duration::from_millis(40),
        wall: overflow_wall + Duration::from_millis(40),
    };
    let ts = TimestampFields {
        recv_wall: Some(i64::MAX),
        send_wall: Some(i64::MAX),
        ..Default::default()
    };

    assert_eq!(compute_one_way(&sent_at, &received_at, &ts), None);
}

#[test]
fn matched_reply_with_reversed_monotonic_time_still_emits_event() {
    let params = default_params();
    let server = silent_open_server(params.clone());
    let config = ClientConfig {
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(50)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());

    let base = Instant::now() + Duration::from_secs(1);
    let send_ts = ClientTimestamp {
        mono: base,
        wall: SystemTime::now(),
    };
    client.send_probe_at(send_ts).unwrap();

    let recv_ts = ClientTimestamp {
        mono: send_ts.mono - Duration::from_millis(500),
        wall: send_ts.wall + Duration::from_millis(10),
    };
    let reply = echo_reply_packet(TOKEN, 0, &params, &TimestampFields::default(), None);
    let events = client
        .process_received_packet(&reply, recv_ts, ReceiveMeta::default())
        .unwrap();

    assert_eq!(events.len(), 1);
    match &events[0] {
        ClientEvent::EchoReply { rtt, .. } => {
            assert_eq!(rtt.raw, Duration::ZERO);
            assert_eq!(rtt.effective, Duration::ZERO);
        }
        other => panic!("expected EchoReply, got {other:?}"),
    }

    client.close().unwrap();
    server.join();
}

#[test]
fn compute_one_way_returns_available_direction_samples() {
    let sent_at = ClientTimestamp {
        mono: Instant::now(),
        wall: SystemTime::UNIX_EPOCH + Duration::from_secs(10),
    };
    let received_at = ClientTimestamp {
        mono: sent_at.mono + Duration::from_millis(40),
        wall: SystemTime::UNIX_EPOCH + Duration::from_secs(10) + Duration::from_millis(40),
    };
    let ts = TimestampFields {
        recv_wall: Some(10_000_000_000 + 15_000_000),
        send_wall: Some(10_000_000_000 + 25_000_000),
        ..Default::default()
    };

    let sample = compute_one_way(&sent_at, &received_at, &ts).unwrap();
    assert_eq!(sample.client_to_server, Some(Duration::from_millis(15)));
    assert_eq!(sample.server_to_client, Some(Duration::from_millis(15)));
}
