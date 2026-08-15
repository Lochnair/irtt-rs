use super::*;

fn probe_timestamps(permission_at: Instant, sent_at: ClientTimestamp) -> ProbeSendTimestamps {
    ProbeSendTimestamps {
        permission_at,
        send_anchor: sent_at,
        sent_at,
        send_call_start: sent_at.mono,
        send_finished_at: sent_at.mono,
    }
}

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
    assert!(client.next_send_deadline().is_none());
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
fn blocking_short_probe_commits_before_presentation_length_error() {
    let params = default_params();
    let server = silent_open_server(params.clone());
    let mut client = Client::connect(default_test_config(server.addr)).unwrap();
    assert_open_started(client.open().unwrap());

    let expected = echo_packet_len(false, &params);
    let initial_deadline = client.next_send_deadline().unwrap();
    client.probe_reported_len = Some(expected - 1);

    assert!(matches!(
        client.send_probe(),
        Err(ClientError::DatagramLengthMismatch {
            expected: error_expected,
            actual,
        }) if error_expected == expected && actual + 1 == expected
    ));
    assert_eq!(client.runtime.packets_sent(), 1);
    assert_eq!(
        client.next_send_deadline(),
        Some(initial_deadline + client.runtime.config().interval)
    );

    client.close().unwrap();
    server.join();
}

#[test]
fn blocking_failed_probe_send_preserves_machine_and_schedule() {
    let params = default_params();
    let server = silent_open_server(params);
    let mut client = Client::connect(default_test_config(server.addr)).unwrap();
    assert_open_started(client.open().unwrap());

    let initial_deadline = client.next_send_deadline().unwrap();
    client.probe_send_error = true;

    assert!(matches!(client.send_probe(), Err(ClientError::Socket(_))));
    assert_eq!(client.runtime.packets_sent(), 0);
    assert_eq!(client.next_send_deadline(), Some(initial_deadline));

    assert!(matches!(
        client.send_probe().unwrap().as_slice(),
        [ClientEvent::EchoSent { seq: 0, .. }]
    ));

    client.close().unwrap();
    server.join();
}

#[test]
fn blocking_timeout_finalization_overflow_does_not_send_or_commit() {
    let params = default_params();
    let server = silent_open_server(params);
    let config = ClientConfig {
        probe_timeout: Duration::MAX,
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());

    let initial_deadline = client.next_send_deadline().unwrap();
    let sent_at = ClientTimestamp {
        mono: Instant::now(),
        wall: SystemTime::now(),
    };

    assert!(matches!(
        client.send_probe_at(probe_timestamps(initial_deadline, sent_at)),
        Err(ClientError::DurationOverflow)
    ));
    assert_eq!(client.runtime.packets_sent(), 0);
    assert_eq!(client.next_send_deadline(), Some(initial_deadline));
    thread::sleep(Duration::from_millis(30));
    assert!(server
        .rx
        .try_iter()
        .all(|packet| packet.len() >= 4 && packet[3] & FLAG_OPEN != 0));

    client.close().unwrap();
    server.join();
}

#[test]
fn blocking_probe_separates_permission_committed_send_and_send_call_timing() {
    let duration = Duration::from_secs(1);
    let interval = Duration::from_millis(100);
    let params = Params {
        duration_ns: 1_000_000_000,
        interval_ns: 100_000_000,
        ..default_params()
    };
    let server = silent_open_server(params.clone());
    let config = ClientConfig {
        duration: Some(duration),
        interval,
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());

    let opened_mono = Instant::now()
        .checked_sub(Duration::from_secs(2))
        .expect("test host monotonic clock has at least two seconds of history");
    client.schedule = Some(
        ProbeSchedule::new(
            opened_mono,
            &NegotiatedParams {
                params,
                restrictions: Vec::new(),
            },
        )
        .unwrap(),
    );
    let permission_at = opened_mono + Duration::from_millis(500);
    // Distinct pre-send anchor and post-send `sent_at`: the anchor drives
    // the operational timeout deadline, while `sent_at` (captured after the
    // send call) is the public measurement/timer_error endpoint. They are
    // deliberately decoupled by this PR.
    let send_anchor = ClientTimestamp {
        wall: SystemTime::now(),
        mono: opened_mono + Duration::from_millis(525),
    };
    let sent_at = ClientTimestamp {
        wall: SystemTime::now(),
        mono: opened_mono + Duration::from_millis(534),
    };
    let timestamps = ProbeSendTimestamps {
        permission_at,
        send_anchor,
        sent_at,
        send_call_start: opened_mono + Duration::from_millis(526),
        send_finished_at: opened_mono + Duration::from_millis(533),
    };

    let events = client.send_probe_at(timestamps).unwrap();

    assert!(matches!(
        events.as_slice(),
        [ClientEvent::EchoSent {
            scheduled_at,
            sent_at: event_sent_at,
            send_call,
            timer_error,
            ..
        }] if *scheduled_at == opened_mono
            && *event_sent_at == sent_at
            && *send_call == Duration::from_millis(7)
            && *timer_error == Duration::from_millis(534)
    ));
    assert_eq!(client.next_send_deadline(), Some(opened_mono + interval));
    assert_eq!(client.runtime.packets_sent(), 1);
    // The timeout deadline is anchored to the pre-send `send_anchor`, not
    // to the post-send `sent_at` captured 9ms later.
    let timeout_at = send_anchor.mono + client.probe_timeout();
    assert_ne!(timeout_at, sent_at.mono + client.probe_timeout());
    assert!(client
        .poll_timeouts_at(timeout_at - Duration::from_nanos(1))
        .unwrap()
        .is_empty());
    assert!(matches!(
        client.poll_timeouts_at(timeout_at).unwrap().as_slice(),
        [ClientEvent::EchoLoss {
            sent_at: loss_sent_at,
            ..
        }] if *loss_sent_at == sent_at
    ));

    client.close().unwrap();
    server.join();
}

#[test]
fn blocking_send_call_excludes_wrapped_history_cleanup() {
    let params = default_params();
    let server = silent_open_server(params);
    let mut client = Client::connect(default_test_config(server.addr)).unwrap();
    assert_open_started(client.open().unwrap());

    let scheduled_at = client.next_send_deadline().unwrap();
    let sent_at = ClientTimestamp {
        wall: SystemTime::now(),
        mono: scheduled_at + Duration::from_millis(5),
    };
    client.runtime.seed_wrapped_probe_history_for_test(sent_at);
    let timestamps = ProbeSendTimestamps {
        permission_at: scheduled_at,
        send_anchor: sent_at,
        sent_at,
        send_call_start: scheduled_at + Duration::from_millis(6),
        send_finished_at: scheduled_at + Duration::from_millis(13),
    };

    let events = client.send_probe_at(timestamps).unwrap();

    assert!(matches!(
        events.as_slice(),
        [ClientEvent::EchoSent {
            seq: 0,
            send_call,
            ..
        }] if *send_call == Duration::from_millis(7)
    ));
    assert!(!client.runtime.has_timed_out_metadata());

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
    assert!(client
        .send_probe_at(probe_timestamps(now0.mono, now0))
        .is_ok());

    let now1 = ClientTimestamp {
        mono: start + interval,
        wall: SystemTime::now(),
    };
    assert!(client
        .send_probe_at(probe_timestamps(now1.mono, now1))
        .is_ok());

    let now2 = ClientTimestamp {
        mono: start + Duration::from_secs(1),
        wall: SystemTime::now(),
    };
    let events = client
        .send_probe_at(probe_timestamps(now2.mono, now2))
        .unwrap();
    assert!(events.is_empty());
    assert!(client.next_send_deadline().is_none());

    client.close().unwrap();
    server.join();
}

#[test]
fn managed_probe_skips_missed_schedule_slots() {
    let interval = Duration::from_millis(10);
    let params = Params {
        protocol_version: 1,
        duration_ns: 0,
        interval_ns: 10_000_000,
        received_stats: ReceivedStats::Both,
        stamp_at: StampAt::Both,
        clock: Clock::Both,
        ..Params::default()
    };
    let server = echo_server(params);
    let config = ClientConfig {
        duration: None,
        interval,
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());

    let first_deadline = client.next_send_deadline().unwrap();
    let permission_at = first_deadline + Duration::from_millis(45);
    let delayed_send = ClientTimestamp {
        mono: first_deadline + Duration::from_millis(48),
        wall: SystemTime::now(),
    };
    let timestamps = ProbeSendTimestamps {
        permission_at,
        send_anchor: delayed_send,
        sent_at: delayed_send,
        send_call_start: first_deadline + Duration::from_millis(49),
        send_finished_at: first_deadline + Duration::from_millis(55),
    };
    let events = client
        .send_managed_probe_at(first_deadline, timestamps)
        .unwrap();

    assert_eq!(events.len(), 1);
    assert!(matches!(
        events[0],
        ClientEvent::EchoSent {
            seq: 0,
            scheduled_at,
            sent_at,
            send_call,
            timer_error,
            ..
        } if scheduled_at == first_deadline + Duration::from_millis(40)
            && sent_at == delayed_send
            && send_call == Duration::from_millis(6)
            && timer_error == Duration::from_millis(8)
    ));
    assert_eq!(
        client.next_send_deadline(),
        Some(first_deadline + Duration::from_millis(50))
    );

    client.close().unwrap();
    server.join();
}

#[test]
fn managed_probe_skip_preserves_finite_run_end() {
    let interval = Duration::from_millis(10);
    let duration = Duration::from_millis(50);
    let params = Params {
        protocol_version: 1,
        duration_ns: 50_000_000,
        interval_ns: 10_000_000,
        received_stats: ReceivedStats::Both,
        stamp_at: StampAt::Both,
        clock: Clock::Both,
        ..Params::default()
    };
    let server = echo_server(params);
    let config = ClientConfig {
        duration: Some(duration),
        interval,
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());

    let first_deadline = client.next_send_deadline().unwrap();
    let delayed_send = ClientTimestamp {
        mono: first_deadline + Duration::from_millis(45),
        wall: SystemTime::now(),
    };
    let first = client
        .send_managed_probe_at(
            first_deadline,
            probe_timestamps(delayed_send.mono, delayed_send),
        )
        .unwrap();
    let second_sent_at = ClientTimestamp {
        mono: first_deadline + Duration::from_millis(46),
        wall: SystemTime::now(),
    };
    let second = client
        .send_managed_probe_at(
            first_deadline + duration,
            probe_timestamps(second_sent_at.mono, second_sent_at),
        )
        .unwrap();

    assert_eq!(first.len(), 1);
    assert!(second.is_empty());
    assert!(client.next_send_deadline().is_none());

    client.close().unwrap();
    server.join();
}

#[test]
fn receive_wait_is_bounded_by_next_ten_millisecond_deadline() {
    let now = Instant::now();
    let deadline = now + Duration::from_millis(10);

    assert_eq!(
        bounded_receive_timeout(
            Some(deadline),
            Some(Duration::from_millis(20)),
            Duration::from_millis(20),
            now,
        ),
        Some(Duration::from_millis(10))
    );
    assert_eq!(
        bounded_receive_timeout(
            Some(now),
            Some(Duration::from_millis(20)),
            Duration::from_millis(20),
            now,
        ),
        None
    );
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
