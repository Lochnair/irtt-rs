use super::*;

#[cfg(not(all(target_os = "linux", feature = "ancillary")))]
fn assert_packet_meta_unavailable(packet_meta: &crate::event::PacketMeta) {
    assert_eq!(packet_meta.traffic_class, None);
    assert_eq!(packet_meta.dscp, None);
    assert_eq!(packet_meta.ecn, None);
    assert_eq!(packet_meta.kernel_rx_timestamp, None);
}

#[cfg(all(target_os = "linux", feature = "ancillary"))]
fn metadata_unavailable_skip(test_name: &str) {
    eprintln!("{test_name}: skipping metadata assertion because kernel did not provide traffic class metadata");
}

#[cfg(all(target_os = "linux", feature = "ancillary"))]
fn kernel_rx_timestamp_unavailable_skip(test_name: &str) {
    eprintln!("{test_name}: skipping kernel timestamp assertion because kernel did not provide SCM_TIMESTAMPNS");
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
    assert_open_started(client.open().unwrap());
    let events = client.send_probe().unwrap();
    assert_eq!(events.len(), 1);
    match &events[0] {
        ClientEvent::EchoSent {
            seq,
            logical_seq,
            remote,
            bytes,
            ..
        } => {
            assert_eq!(*seq, 0);
            assert_eq!(*logical_seq, 0);
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
fn echo_sent_reports_schedule_and_timer_error() {
    let params = default_params();
    let server = silent_open_server(params);
    let config = ClientConfig {
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(200)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    let start = ClientTimestamp {
        mono: Instant::now(),
        wall: SystemTime::now(),
    };
    assert_open_started(client.open().unwrap());
    let session_start = client.session.as_ref().unwrap().start_mono;
    assert!(
        session_start >= start.mono,
        "probe schedule must start after open begins"
    );
    let first_probe_at = ClientTimestamp {
        mono: session_start,
        wall: SystemTime::now(),
    };

    let events = client.send_probe_at(first_probe_at).unwrap();
    match &events[0] {
        ClientEvent::EchoSent {
            scheduled_at,
            sent_at,
            timer_error,
            ..
        } => {
            assert_eq!(*scheduled_at, session_start);
            assert_eq!(*sent_at, first_probe_at);
            assert_eq!(*timer_error, Duration::ZERO);
        }
        other => panic!("expected EchoSent, got {other:?}"),
    }

    client.close().unwrap();
    server.join();
}

#[test]
fn send_probe_starts_seq_at_zero_and_increments() {
    let params = default_params();
    let server = silent_open_server(params);
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
    client.send_probe().unwrap();
    thread::sleep(Duration::from_millis(30));
    let packets: Vec<_> = server.rx.try_iter().collect();
    let echo_reqs: Vec<_> = packets
        .iter()
        .filter(|p| p.len() >= 4 && p[3] & FLAG_OPEN == 0 && p[3] & flags::FLAG_CLOSE == 0)
        .collect();
    assert_eq!(echo_reqs.len(), 3);
    for (i, pkt) in echo_reqs.iter().enumerate() {
        let seq = u32::from_le_bytes(pkt[12..16].try_into().unwrap());
        assert_eq!(seq, i as u32);
    }
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

    let session = client.session.as_ref().unwrap();
    let start = session.start_mono;
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
    assert!(client.session.as_ref().unwrap().sending_done);
    assert!(client.next_send_deadline().is_none());

    client.close().unwrap();
    server.join();
}

#[test]
fn continuous_duration_keeps_generating_send_deadlines() {
    let params = Params {
        protocol_version: 1,
        duration_ns: 0,
        interval_ns: 500_000_000,
        received_stats: ReceivedStats::Both,
        stamp_at: StampAt::Both,
        clock: Clock::Both,
        ..Params::default()
    };
    let server = silent_open_server(params);
    let config = ClientConfig {
        duration: None,
        interval: Duration::from_millis(500),
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(200)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());

    let start = client.session.as_ref().unwrap().start_mono;
    let interval = Duration::from_millis(500);
    for seq in 0..4 {
        let now = ClientTimestamp {
            mono: start + interval * seq,
            wall: SystemTime::now(),
        };
        let events = client.send_probe_at(now).unwrap();
        assert_eq!(events.len(), 1);
        assert!(client.next_send_deadline().is_some());
        assert!(!client.session.as_ref().unwrap().sending_done);
    }

    client.close().unwrap();
    server.join();
}

#[test]
fn next_send_deadline_advances_by_interval() {
    let params = default_params();
    let (mut client, server) = open_client_with_echo_server(&params);

    let session = client.session.as_ref().unwrap();
    let start = session.start_mono;
    let deadline0 = client.next_send_deadline().unwrap();
    assert_eq!(deadline0, start);

    client.send_probe().unwrap();
    let deadline1 = client.next_send_deadline().unwrap();
    assert_eq!(deadline1, start + Duration::from_secs(1));

    client.send_probe().unwrap();
    let deadline2 = client.next_send_deadline().unwrap();
    assert_eq!(deadline2, start + Duration::from_secs(2));

    client.close().unwrap();
    server.join();
}

#[test]
fn next_probe_deadline_reports_overflow() {
    let start = Instant::now();
    assert_eq!(
        next_probe_deadline(start, 1_000_000_000, 2).unwrap(),
        start + Duration::from_secs(2)
    );
    assert!(matches!(
        next_probe_deadline(start, u64::MAX, 2),
        Err(ClientError::DurationOverflow)
    ));
}

#[test]
fn send_probe_reports_schedule_overflow() {
    let params = Params {
        protocol_version: 1,
        duration_ns: 0,
        interval_ns: 1_000_000_000,
        received_stats: ReceivedStats::Both,
        stamp_at: StampAt::Both,
        clock: Clock::Both,
        ..Params::default()
    };
    let server = silent_open_server(params);
    let config = ClientConfig {
        duration: None,
        interval: Duration::from_secs(1),
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(200)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());
    client.session.as_mut().unwrap().packets_sent = u64::MAX - 1;

    assert!(matches!(
        client.send_probe(),
        Err(ClientError::DurationOverflow)
    ));
    server.join();
}

#[test]
fn send_probe_reports_logical_sequence_counter_overflow() {
    let params = default_params();
    let server = silent_open_server(params);
    let config = ClientConfig {
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(200)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());
    client.session.as_mut().unwrap().next_logical_seq = u64::MAX;

    assert!(matches!(
        client.send_probe(),
        Err(ClientError::CounterOverflow {
            counter: "next_logical_seq"
        })
    ));
    server.join();
}

#[test]
fn send_probe_reports_packets_sent_counter_overflow() {
    let params = default_params();
    let server = silent_open_server(params);
    let config = ClientConfig {
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(200)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());
    client.session.as_mut().unwrap().packets_sent = u64::MAX;

    assert!(matches!(
        client.send_probe(),
        Err(ClientError::CounterOverflow {
            counter: "packets_sent"
        })
    ));
    server.join();
}

#[test]
fn recv_once_returns_empty_on_timeout() {
    let params = default_params();
    let server = open_success_server(params);
    let config = ClientConfig {
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(50)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());
    let events = client.recv_once().unwrap();
    assert!(events.is_empty());
    server.join();
}

#[test]
fn recv_once_decodes_echo_reply_and_emits_event() {
    let params = default_params();
    let (mut client, server) = open_client_with_echo_server(&params);

    client.send_probe().unwrap();
    thread::sleep(Duration::from_millis(50));

    let events = client.recv_once().unwrap();
    assert_eq!(events.len(), 1);
    match &events[0] {
        ClientEvent::EchoReply {
            seq,
            logical_seq,
            rtt,
            received_stats,
            server_timing,
            bytes,
            packet_meta,
            ..
        } => {
            assert_eq!(*seq, 0);
            assert_eq!(*logical_seq, 0);
            assert_eq!(*bytes, echo_packet_len(false, &params));
            assert!(rtt.raw > Duration::ZERO);
            assert_eq!(rtt.effective, rtt.adjusted.unwrap_or(rtt.raw));
            assert!(received_stats.is_some());
            let stats = received_stats.as_ref().unwrap();
            assert_eq!(stats.count, Some(42));
            assert_eq!(stats.window, Some(0x07));
            assert!(server_timing.is_some());
            let st = server_timing.as_ref().unwrap();
            assert!(st.processing.is_some());
            #[cfg(not(all(target_os = "linux", feature = "ancillary")))]
            {
                assert_eq!(packet_meta.traffic_class, None);
                assert_eq!(packet_meta.dscp, None);
                assert_eq!(packet_meta.ecn, None);
                assert_eq!(packet_meta.kernel_rx_timestamp, None);
            }
            #[cfg(all(target_os = "linux", feature = "ancillary"))]
            {
                assert_eq!(
                    packet_meta.dscp,
                    packet_meta.traffic_class.map(|tc| tc >> 2)
                );
                assert_eq!(
                    packet_meta.ecn,
                    packet_meta.traffic_class.map(|tc| tc & 0b11)
                );
            }
        }
        other => panic!("expected EchoReply, got {other:?}"),
    }
    client.close().unwrap();
    server.join();
}

#[test]
fn recv_once_decodes_only_received_bytes_after_longer_datagram() {
    let params = default_params();
    let server_params = params.clone();
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &server_params, None);
        socket.send_to(&reply, peer).unwrap();

        let mut seqs = Vec::new();
        for _ in 0..2 {
            let (packet, _) = recv_request(&socket, &tx);
            seqs.push(u32::from_le_bytes(packet[12..16].try_into().unwrap()));
        }

        let mut longer = echo_reply_packet(
            TOKEN,
            seqs[0],
            &server_params,
            &TimestampFields::default(),
            None,
        );
        longer.extend_from_slice(b"trailing bytes");
        socket.send_to(&longer, peer).unwrap();

        let shorter = echo_reply_packet(
            TOKEN,
            seqs[1],
            &server_params,
            &TimestampFields::default(),
            None,
        );
        socket.send_to(&shorter, peer).unwrap();
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

    let _ = client.recv_once().unwrap();
    let events = client.recv_once().unwrap();
    assert_eq!(events.len(), 1);
    match &events[0] {
        ClientEvent::EchoReply {
            seq,
            logical_seq,
            bytes,
            ..
        } => {
            assert_eq!(*seq, 1);
            assert_eq!(*logical_seq, 1);
            assert_eq!(*bytes, echo_packet_len(false, &params));
        }
        other => panic!("expected EchoReply after shorter datagram, got {other:?}"),
    }
    server.join();
}

#[test]
#[cfg(not(all(target_os = "linux", feature = "ancillary")))]
fn echo_reply_metadata_is_unavailable_without_ancillary() {
    let params = default_params();
    let (mut client, server) = open_client_with_echo_server(&params);

    client.send_probe().unwrap();
    thread::sleep(Duration::from_millis(50));

    let events = client.recv_once().unwrap();
    assert_eq!(events.len(), 1);
    match &events[0] {
        ClientEvent::EchoReply { packet_meta, .. } => {
            assert_packet_meta_unavailable(packet_meta);
        }
        other => panic!("expected EchoReply, got {other:?}"),
    }

    client.close().unwrap();
    server.join();
}

#[test]
#[cfg(all(target_os = "linux", feature = "ancillary"))]
fn echo_reply_metadata_propagates_observed_dscp_with_ancillary() {
    let params = default_params();
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
        socket.send_to(&reply, peer).unwrap();

        let mut buf = [0_u8; 2048];
        let (size, _) = socket.recv_from(&mut buf).unwrap();
        tx.send(buf[..size].to_vec()).unwrap();
        let seq = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        crate::socket_options::apply_dscp_to_socket(&socket, peer, 46).unwrap();
        let reply_packet =
            echo_reply_packet(TOKEN, seq, &params, &TimestampFields::default(), None);
        socket.send_to(&reply_packet, peer).unwrap();
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

    let events = client.recv_once().unwrap();
    assert_eq!(events.len(), 1);
    match &events[0] {
        ClientEvent::EchoReply { packet_meta, .. } => {
            let Some(traffic_class) = packet_meta.traffic_class else {
                metadata_unavailable_skip(
                    "echo_reply_metadata_propagates_observed_dscp_with_ancillary",
                );
                client.close().unwrap();
                server.join();
                return;
            };
            assert_eq!(traffic_class, 184);
            assert_eq!(packet_meta.dscp, Some(46));
            assert_eq!(packet_meta.ecn, Some(0));
        }
        other => panic!("expected EchoReply, got {other:?}"),
    }

    client.close().unwrap();
    server.join();
}

#[test]
#[cfg(all(target_os = "linux", feature = "ancillary"))]
fn echo_reply_metadata_preserves_observed_zero_with_ancillary() {
    let params = default_params();
    let (mut client, server) = open_client_with_echo_server(&params);

    client.send_probe().unwrap();
    thread::sleep(Duration::from_millis(50));

    let events = client.recv_once().unwrap();
    assert_eq!(events.len(), 1);
    match &events[0] {
        ClientEvent::EchoReply { packet_meta, .. } => {
            let Some(traffic_class) = packet_meta.traffic_class else {
                metadata_unavailable_skip(
                    "echo_reply_metadata_preserves_observed_zero_with_ancillary",
                );
                client.close().unwrap();
                server.join();
                return;
            };
            assert_eq!(traffic_class, 0);
            assert_eq!(packet_meta.dscp, Some(0));
            assert_eq!(packet_meta.ecn, Some(0));
        }
        other => panic!("expected EchoReply, got {other:?}"),
    }

    client.close().unwrap();
    server.join();
}

#[test]
#[cfg(all(target_os = "linux", feature = "ancillary"))]
fn echo_reply_metadata_propagates_kernel_rx_timestamp_with_ancillary() {
    let params = default_params();
    let (mut client, server) = open_client_with_echo_server(&params);

    client.send_probe().unwrap();
    thread::sleep(Duration::from_millis(50));

    let events = client.recv_once().unwrap();
    assert_eq!(events.len(), 1);
    match &events[0] {
        ClientEvent::EchoReply { packet_meta, .. } => {
            let Some(timestamp) = packet_meta.kernel_rx_timestamp else {
                kernel_rx_timestamp_unavailable_skip(
                    "echo_reply_metadata_propagates_kernel_rx_timestamp_with_ancillary",
                );
                client.close().unwrap();
                server.join();
                return;
            };
            assert!(timestamp.duration_since(SystemTime::UNIX_EPOCH).is_ok());
        }
        other => panic!("expected EchoReply, got {other:?}"),
    }

    client.close().unwrap();
    server.join();
}

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
fn received_stats_parsed_into_sample() {
    let params = default_params();
    let (mut client, server) = open_client_with_echo_server(&params);
    client.send_probe().unwrap();
    thread::sleep(Duration::from_millis(30));
    let events = client.recv_once().unwrap();
    if let ClientEvent::EchoReply { received_stats, .. } = &events[0] {
        let rs = received_stats.as_ref().unwrap();
        assert_eq!(rs.count, Some(42));
        assert_eq!(rs.window, Some(0x07));
    } else {
        panic!("expected EchoReply");
    }
    client.close().unwrap();
    server.join();
}

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
fn poll_timeouts_emits_echo_loss() {
    let params = default_params();
    let server = silent_open_server(params);
    let config = ClientConfig {
        probe_timeout: Duration::from_millis(100),
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(50)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());

    client.send_probe().unwrap();
    client.send_probe().unwrap();

    let no_loss = client.poll_timeouts(ClientTimestamp::now()).unwrap();
    assert!(no_loss.is_empty());

    thread::sleep(Duration::from_millis(150));
    let events = client.poll_timeouts(ClientTimestamp::now()).unwrap();
    assert_eq!(events.len(), 2);
    for event in &events {
        assert!(matches!(event, ClientEvent::EchoLoss { .. }));
    }
    server.join();
}

#[test]
fn poll_timeouts_removes_expired_pending() {
    let params = default_params();
    let server = silent_open_server(params);
    let config = ClientConfig {
        probe_timeout: Duration::from_millis(100),
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(50)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());

    client.send_probe().unwrap();
    thread::sleep(Duration::from_millis(150));
    client.poll_timeouts(ClientTimestamp::now()).unwrap();

    let session = client.session.as_ref().unwrap();
    assert_eq!(session.pending.len(), 0);
    assert_eq!(session.timed_out.len(), 1);
    server.join();
}

#[test]
fn late_reply_after_timeout_preserves_measurement_metadata() {
    let params = default_params();
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
        socket.send_to(&reply, peer).unwrap();

        let mut buf = [0_u8; 2048];
        let (size, _) = socket.recv_from(&mut buf).unwrap();
        tx.send(buf[..size].to_vec()).unwrap();
        let seq = u32::from_le_bytes(buf[12..16].try_into().unwrap());

        thread::sleep(Duration::from_millis(90));
        let ts = TimestampFields {
            recv_wall: Some(1_000_000_000),
            recv_mono: Some(100_000),
            send_wall: Some(1_000_100_000),
            send_mono: Some(200_000),
            ..Default::default()
        };
        let reply_packet = echo_reply_packet(TOKEN, seq, &params, &ts, None);
        socket.send_to(&reply_packet, peer).unwrap();
        socket.send_to(&reply_packet, peer).unwrap();
    });
    let config = ClientConfig {
        probe_timeout: Duration::from_millis(40),
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(200)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());

    client.send_probe().unwrap();
    thread::sleep(Duration::from_millis(60));
    let losses = client.poll_timeouts(ClientTimestamp::now()).unwrap();
    assert!(matches!(&losses[0], ClientEvent::EchoLoss { seq: 0, .. }));
    assert_eq!(client.session.as_ref().unwrap().pending.len(), 0);
    assert_eq!(client.session.as_ref().unwrap().timed_out.len(), 1);

    let late = client.recv_once().unwrap();
    match &late[0] {
        ClientEvent::LateReply {
            seq,
            logical_seq,
            sent_at,
            rtt,
            server_timing,
            one_way,
            bytes,
            packet_meta,
            ..
        } => {
            assert_eq!(*seq, 0);
            assert_eq!(*logical_seq, Some(0));
            assert!(sent_at.is_some());
            assert!(rtt.is_some());
            assert!(server_timing.is_some());
            assert!(one_way.is_some());
            assert_eq!(*bytes, echo_packet_len(false, &default_params()));
            let _ = packet_meta;
            #[cfg(not(all(target_os = "linux", feature = "ancillary")))]
            assert_packet_meta_unavailable(packet_meta);
        }
        other => panic!("expected stats-eligible LateReply, got {other:?}"),
    }
    assert_eq!(client.session.as_ref().unwrap().timed_out.len(), 0);

    let duplicate = client.recv_once().unwrap();
    assert!(matches!(
        &duplicate[0],
        ClientEvent::DuplicateReply {
            seq: 0,
            bytes,
            ..
        } if *bytes == echo_packet_len(false, &default_params())
    ));

    client.close().unwrap();
    server.join();
}

#[test]
#[cfg(not(all(target_os = "linux", feature = "ancillary")))]
fn late_reply_metadata_is_unavailable_without_ancillary() {
    let params = default_params();
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
        socket.send_to(&reply, peer).unwrap();

        let mut buf = [0_u8; 2048];
        let (size, _) = socket.recv_from(&mut buf).unwrap();
        tx.send(buf[..size].to_vec()).unwrap();
        let seq = u32::from_le_bytes(buf[12..16].try_into().unwrap());

        thread::sleep(Duration::from_millis(90));
        let reply_packet =
            echo_reply_packet(TOKEN, seq, &params, &TimestampFields::default(), None);
        socket.send_to(&reply_packet, peer).unwrap();
    });
    let config = ClientConfig {
        probe_timeout: Duration::from_millis(40),
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(200)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());

    client.send_probe().unwrap();
    thread::sleep(Duration::from_millis(60));
    let losses = client.poll_timeouts(ClientTimestamp::now()).unwrap();
    assert!(matches!(&losses[0], ClientEvent::EchoLoss { seq: 0, .. }));

    let late = client.recv_once().unwrap();
    assert_eq!(late.len(), 1);
    match &late[0] {
        ClientEvent::LateReply { packet_meta, .. } => {
            assert_packet_meta_unavailable(packet_meta);
        }
        other => panic!("expected LateReply, got {other:?}"),
    }

    client.close().unwrap();
    server.join();
}

#[test]
#[cfg(all(target_os = "linux", feature = "ancillary"))]
fn late_reply_metadata_propagates_observed_dscp_with_ancillary() {
    let params = default_params();
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
        socket.send_to(&reply, peer).unwrap();

        let mut buf = [0_u8; 2048];
        let (size, _) = socket.recv_from(&mut buf).unwrap();
        tx.send(buf[..size].to_vec()).unwrap();
        let seq = u32::from_le_bytes(buf[12..16].try_into().unwrap());

        thread::sleep(Duration::from_millis(90));
        crate::socket_options::apply_dscp_to_socket(&socket, peer, 46).unwrap();
        let reply_packet =
            echo_reply_packet(TOKEN, seq, &params, &TimestampFields::default(), None);
        socket.send_to(&reply_packet, peer).unwrap();
    });
    let config = ClientConfig {
        probe_timeout: Duration::from_millis(40),
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(200)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());

    client.send_probe().unwrap();
    thread::sleep(Duration::from_millis(60));
    let losses = client.poll_timeouts(ClientTimestamp::now()).unwrap();
    assert!(matches!(&losses[0], ClientEvent::EchoLoss { seq: 0, .. }));

    let events = client.recv_once().unwrap();
    assert_eq!(events.len(), 1);
    match &events[0] {
        ClientEvent::LateReply { packet_meta, .. } => {
            let Some(traffic_class) = packet_meta.traffic_class else {
                metadata_unavailable_skip(
                    "late_reply_metadata_propagates_observed_dscp_with_ancillary",
                );
                client.close().unwrap();
                server.join();
                return;
            };
            assert_eq!(traffic_class, 184);
            assert_eq!(packet_meta.dscp, Some(46));
            assert_eq!(packet_meta.ecn, Some(0));
            if let Some(timestamp) = packet_meta.kernel_rx_timestamp {
                assert!(timestamp.duration_since(SystemTime::UNIX_EPOCH).is_ok());
            } else {
                kernel_rx_timestamp_unavailable_skip(
                    "late_reply_metadata_propagates_observed_dscp_with_ancillary",
                );
            }
        }
        other => panic!("expected LateReply, got {other:?}"),
    }

    client.close().unwrap();
    server.join();
}

#[test]
fn pending_map_bounded() {
    let params = Params {
        duration_ns: 60_000_000_000,
        ..default_params()
    };
    let server = silent_open_server(params);
    let config = ClientConfig {
        duration: Some(Duration::from_secs(60)),
        max_pending_probes: 3,
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(50)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());
    client.send_probe().unwrap();
    client.send_probe().unwrap();
    client.send_probe().unwrap();
    assert!(matches!(
        client.send_probe(),
        Err(ClientError::PendingLimitExceeded { limit: 3 })
    ));
    client.close().unwrap();
    server.join();
}

#[test]
fn minimal_negotiated_layout_works() {
    let params = Params {
        protocol_version: 1,
        duration_ns: 3_000_000_000,
        interval_ns: 1_000_000_000,
        received_stats: ReceivedStats::None,
        stamp_at: StampAt::None,
        clock: Clock::Both,
        ..Params::default()
    };
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
        }
    });
    let config = ClientConfig {
        received_stats: ReceivedStats::None,
        stamp_at: StampAt::None,
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
    if let ClientEvent::EchoReply {
        received_stats,
        server_timing,
        ..
    } = &events[0]
    {
        assert!(received_stats.is_none());
        assert!(server_timing.is_none());
    } else {
        panic!("expected EchoReply");
    }
    server.join();
}

#[test]
fn late_reply_with_pending_preserves_rtt() {
    let params = default_params();
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
        socket.send_to(&reply, peer).unwrap();

        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut seqs = Vec::new();
        for _ in 0..3 {
            let mut buf = [0_u8; 2048];
            if let Ok((size, _)) = socket.recv_from(&mut buf) {
                tx.send(buf[..size].to_vec()).unwrap();
                let seq = u32::from_le_bytes(buf[12..16].try_into().unwrap());
                seqs.push(seq);
            }
        }
        let ts = TimestampFields::default();
        let reply2 = echo_reply_packet(TOKEN, seqs[2], &params, &ts, None);
        socket.send_to(&reply2, peer).unwrap();
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
    client.send_probe().unwrap();
    thread::sleep(Duration::from_millis(50));

    let ev1 = client.recv_once().unwrap();
    assert!(matches!(&ev1[0], ClientEvent::EchoReply { seq: 2, .. }));

    thread::sleep(Duration::from_millis(30));
    let ev2 = client.recv_once().unwrap();
    match &ev2[0] {
        ClientEvent::LateReply {
            seq, rtt, sent_at, ..
        } => {
            assert_eq!(*seq, 0);
            assert!(rtt.is_some());
            assert!(sent_at.is_some());
        }
        other => panic!("expected LateReply, got {other:?}"),
    }
    server.join();
}

// ---------- Regression tests ----------

#[test]
fn pending_full_does_not_send_packet() {
    let params = Params {
        duration_ns: 60_000_000_000,
        ..default_params()
    };
    let server = silent_open_server(params);
    let config = ClientConfig {
        duration: Some(Duration::from_secs(60)),
        max_pending_probes: 2,
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(50)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());

    client.send_probe().unwrap();
    client.send_probe().unwrap();

    thread::sleep(Duration::from_millis(30));
    let before_count: Vec<_> = server.rx.try_iter().collect();
    let echo_before: Vec<_> = before_count
        .iter()
        .filter(|p| p.len() >= 4 && p[3] & FLAG_OPEN == 0 && p[3] & flags::FLAG_CLOSE == 0)
        .collect();
    assert_eq!(echo_before.len(), 2);

    assert!(matches!(
        client.send_probe(),
        Err(ClientError::PendingLimitExceeded { limit: 2 })
    ));

    thread::sleep(Duration::from_millis(30));
    let after: Vec<_> = server.rx.try_iter().collect();
    let echo_after: Vec<_> = after
        .iter()
        .filter(|p| p.len() >= 4 && p[3] & FLAG_OPEN == 0 && p[3] & flags::FLAG_CLOSE == 0)
        .collect();
    assert_eq!(
        echo_after.len(),
        0,
        "no packet should be sent when pending is full"
    );

    client.close().unwrap();
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

    assert_eq!(
        client.session.as_ref().unwrap().highest_received_seq,
        Some(0),
        "highest_received_seq should not be updated by unmatched future reply"
    );

    thread::sleep(Duration::from_millis(30));
    let ev1 = client.recv_once().unwrap();
    assert!(
        matches!(&ev1[0], ClientEvent::EchoReply { seq: 1, .. }),
        "valid pending reply seq=1 should not be poisoned, got {:?}",
        ev1[0]
    );
    server.join();
}

#[test]
fn connect_rejects_zero_max_pending_probes() {
    let config = ClientConfig {
        max_pending_probes: 0,
        ..ClientConfig::default()
    };
    assert!(matches!(
        Client::connect(config),
        Err(ClientError::InvalidConfig { .. })
    ));
}

#[test]
fn connect_rejects_zero_probe_timeout() {
    let config = ClientConfig {
        probe_timeout: Duration::ZERO,
        ..ClientConfig::default()
    };
    assert!(matches!(
        Client::connect(config),
        Err(ClientError::InvalidConfig { .. })
    ));
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
fn process_received_packet_uses_supplied_receive_metadata() {
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

    client.send_probe_at(ClientTimestamp::now()).unwrap();
    let recv_ts = ClientTimestamp::now();
    let reply = echo_reply_packet(TOKEN, 0, &params, &TimestampFields::default(), None);
    let events = client
        .process_received_packet(
            &reply,
            recv_ts,
            ReceiveMeta {
                traffic_class: Some(186),
                kernel_rx_timestamp: Some(SystemTime::UNIX_EPOCH + Duration::new(12, 34)),
            },
        )
        .unwrap();

    assert_eq!(events.len(), 1);
    match &events[0] {
        ClientEvent::EchoReply { packet_meta, .. } => {
            assert_eq!(packet_meta.traffic_class, Some(186));
            assert_eq!(packet_meta.dscp, Some(46));
            assert_eq!(packet_meta.ecn, Some(2));
            assert_eq!(
                packet_meta.kernel_rx_timestamp,
                Some(SystemTime::UNIX_EPOCH + Duration::new(12, 34))
            );
        }
        other => panic!("expected EchoReply, got {other:?}"),
    }

    client.close().unwrap();
    server.join();
}

#[test]
fn process_received_packet_uses_supplied_receive_metadata_for_late_reply() {
    let params = default_params();
    let server = silent_open_server(params.clone());
    let config = ClientConfig {
        probe_timeout: Duration::from_millis(40),
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(50)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());

    client.send_probe_at(ClientTimestamp::now()).unwrap();
    thread::sleep(Duration::from_millis(60));
    let losses = client.poll_timeouts(ClientTimestamp::now()).unwrap();
    assert!(matches!(&losses[0], ClientEvent::EchoLoss { seq: 0, .. }));

    let recv_ts = ClientTimestamp::now();
    let reply = echo_reply_packet(TOKEN, 0, &params, &TimestampFields::default(), None);
    let events = client
        .process_received_packet(
            &reply,
            recv_ts,
            ReceiveMeta {
                traffic_class: Some(0),
                kernel_rx_timestamp: Some(SystemTime::UNIX_EPOCH + Duration::new(56, 78)),
            },
        )
        .unwrap();

    assert_eq!(events.len(), 1);
    match &events[0] {
        ClientEvent::LateReply { packet_meta, .. } => {
            assert_eq!(packet_meta.traffic_class, Some(0));
            assert_eq!(packet_meta.dscp, Some(0));
            assert_eq!(packet_meta.ecn, Some(0));
            assert_eq!(
                packet_meta.kernel_rx_timestamp,
                Some(SystemTime::UNIX_EPOCH + Duration::new(56, 78))
            );
        }
        other => panic!("expected LateReply, got {other:?}"),
    }

    client.close().unwrap();
    server.join();
}

#[test]
fn receive_metadata_does_not_broaden_malformed_warning() {
    let params = default_params();
    let server = silent_open_server(params);
    let config = ClientConfig {
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(50)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());

    let events = client
        .process_received_packet(
            b"not an irtt reply",
            ClientTimestamp::now(),
            ReceiveMeta {
                traffic_class: Some(184),
                kernel_rx_timestamp: None,
            },
        )
        .unwrap();

    assert_eq!(events.len(), 1);
    match &events[0] {
        ClientEvent::Warning { kind, message } => {
            assert_eq!(*kind, WarningKind::MalformedOrUnrelatedPacket);
            assert_eq!(message, "dropped malformed or unrelated packet");
        }
        other => panic!("expected Warning, got {other:?}"),
    }

    client.close().unwrap();
    server.join();
}

#[test]
fn recv_available_drains_burst_replies() {
    let params = default_params();
    let (mut client, server) = open_client_with_echo_server(&params);

    client.send_probe().unwrap();
    client.send_probe().unwrap();
    client.send_probe().unwrap();
    thread::sleep(Duration::from_millis(80));

    let events = client
        .recv_available(RecvBudget { max_packets: 8 })
        .unwrap();
    assert_eq!(events.len(), 3);
    for (seq, event) in events.iter().enumerate() {
        assert!(matches!(
            event,
            ClientEvent::EchoReply {
                seq: actual_seq,
                ..
            } if *actual_seq == seq as u32
        ));
    }

    client.close().unwrap();
    server.join();
}

#[test]
fn recv_available_respects_packet_budget() {
    let params = default_params();
    let (mut client, server) = open_client_with_echo_server(&params);

    client.send_probe().unwrap();
    client.send_probe().unwrap();
    thread::sleep(Duration::from_millis(80));

    let first = client
        .recv_available(RecvBudget { max_packets: 1 })
        .unwrap();
    assert_eq!(first.len(), 1);
    assert!(matches!(&first[0], ClientEvent::EchoReply { seq: 0, .. }));

    let second = client
        .recv_available(RecvBudget { max_packets: 8 })
        .unwrap();
    assert_eq!(second.len(), 1);
    assert!(matches!(&second[0], ClientEvent::EchoReply { seq: 1, .. }));

    client.close().unwrap();
    server.join();
}

#[test]
fn send_probe_wraps_wire_sequence_at_u32_max() {
    let params = default_params();
    let server = silent_open_server(params);
    let config = ClientConfig {
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(50)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());

    let session = client.session.as_mut().unwrap();
    session.next_wire_seq = u32::MAX;
    session.next_logical_seq = 41;

    client.send_probe().unwrap();
    client.send_probe().unwrap();
    thread::sleep(Duration::from_millis(30));

    let packets: Vec<_> = server.rx.try_iter().collect();
    let seqs: Vec<u32> = packets
        .iter()
        .filter(|p| p.len() >= 16 && p[3] & FLAG_OPEN == 0 && p[3] & flags::FLAG_CLOSE == 0)
        .map(|p| u32::from_le_bytes(p[12..16].try_into().unwrap()))
        .collect();
    assert_eq!(seqs, vec![u32::MAX, 0]);

    client.close().unwrap();
    server.join();
}

#[test]
fn wrapped_reply_after_u32_max_is_not_late() {
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
            let (size, _) = socket.recv_from(&mut buf).unwrap();
            tx.send(buf[..size].to_vec()).unwrap();
            seqs.push(u32::from_le_bytes(buf[12..16].try_into().unwrap()));
        }
        assert_eq!(seqs, vec![u32::MAX, 0]);

        let ts = TimestampFields::default();
        let reply_max = echo_reply_packet(TOKEN, u32::MAX, &params, &ts, None);
        socket.send_to(&reply_max, peer).unwrap();
        thread::sleep(Duration::from_millis(10));
        let reply_zero = echo_reply_packet(TOKEN, 0, &params, &ts, None);
        socket.send_to(&reply_zero, peer).unwrap();
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
    let session = client.session.as_mut().unwrap();
    session.next_wire_seq = u32::MAX;
    session.next_logical_seq = 41;

    client.send_probe().unwrap();
    client.send_probe().unwrap();
    thread::sleep(Duration::from_millis(50));

    let first = client.recv_once().unwrap();
    assert!(matches!(
        &first[0],
        ClientEvent::EchoReply { seq: u32::MAX, .. }
    ));

    thread::sleep(Duration::from_millis(30));
    let second = client.recv_once().unwrap();
    assert!(
        matches!(&second[0], ClientEvent::EchoReply { seq: 0, .. }),
        "freshly wrapped reply should not be late, got {:?}",
        second[0]
    );
    server.join();
}

#[test]
fn send_probe_after_sending_done_is_noop() {
    let params = default_params();
    let server = silent_open_server(params);
    let config = ClientConfig {
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(50)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());

    client.session.as_mut().unwrap().sending_done = true;
    let events = client.send_probe().unwrap();
    assert!(events.is_empty());
    assert_eq!(client.session.as_ref().unwrap().packets_sent, 0);

    thread::sleep(Duration::from_millis(30));
    let packets: Vec<_> = server.rx.try_iter().collect();
    let echo_count = packets
        .iter()
        .filter(|p| p.len() >= 4 && p[3] & FLAG_OPEN == 0 && p[3] & flags::FLAG_CLOSE == 0)
        .count();
    assert_eq!(echo_count, 0);

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

#[test]
fn recv_buffer_uses_negotiated_packet_length() {
    let params = Params {
        protocol_version: 1,
        duration_ns: 3_000_000_000,
        interval_ns: 1_000_000_000,
        length: 4096,
        received_stats: ReceivedStats::Both,
        stamp_at: StampAt::Both,
        clock: Clock::Both,
        ..Params::default()
    };
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
        socket.send_to(&reply, peer).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        loop {
            let mut buf = [0_u8; 8192];
            match socket.recv_from(&mut buf) {
                Ok((size, _)) => {
                    tx.send(buf[..size].to_vec()).unwrap();
                }
                Err(_) => break,
            }
        }
    });
    let config = ClientConfig {
        length: 4096,
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(200)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());
    let buf_size = client.recv_buffer_size();
    assert!(
        buf_size > 4096,
        "recv buffer should include truncation-detection byte, got {buf_size}"
    );
    assert_eq!(buf_size, 4097);
    assert_eq!(client.recv_buffer.len(), buf_size);
    client.close().unwrap();
    server.join();
}

#[test]
fn short_echo_reply_does_not_emit_echo_reply() {
    let params = Params {
        length: 64,
        ..default_params()
    };
    let server = silent_open_server(params.clone());
    let config = ClientConfig {
        length: 64,
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(200)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());
    client.send_probe().unwrap();

    let mut packet = echo_reply_packet(TOKEN, 0, &params, &TimestampFields::default(), None);
    packet.truncate(echo_packet_len(false, &params) - 1);
    let events = client
        .process_received_packet(&packet, ClientTimestamp::now(), ReceiveMeta::default())
        .unwrap();

    assert!(matches!(
        events.as_slice(),
        [ClientEvent::Warning {
            kind: WarningKind::MalformedOrUnrelatedPacket,
            ..
        }]
    ));
    client.close().unwrap();
    server.join();
}

#[test]
fn overlong_echo_reply_does_not_emit_echo_reply() {
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
    client.send_probe().unwrap();

    let mut packet = echo_reply_packet(TOKEN, 0, &params, &TimestampFields::default(), None);
    packet.push(0);
    let events = client
        .process_received_packet(&packet, ClientTimestamp::now(), ReceiveMeta::default())
        .unwrap();

    assert!(matches!(
        events.as_slice(),
        [ClientEvent::Warning {
            kind: WarningKind::MalformedOrUnrelatedPacket,
            ..
        }]
    ));
    client.close().unwrap();
    server.join();
}

#[test]
fn overlong_datagram_detection_uses_extra_receive_byte() {
    let params = Params {
        length: 4096,
        ..default_params()
    };
    let server = start_fake_server({
        let params = params.clone();
        move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
            socket.send_to(&reply, peer).unwrap();
            let (request, _) = recv_request(&socket, &tx);
            let seq = u32::from_le_bytes(request[12..16].try_into().unwrap());
            let mut reply =
                echo_reply_packet(TOKEN, seq, &params, &TimestampFields::default(), None);
            reply.push(0);
            socket.send_to(&reply, peer).unwrap();
            let _ = recv_request_timeout(&socket, &tx);
        }
    });
    let config = ClientConfig {
        length: 4096,
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(200)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());
    assert_eq!(
        client.recv_buffer.len(),
        echo_packet_len(false, &params) + 1
    );
    client.send_probe().unwrap();

    let events = client.recv_once().unwrap();
    assert!(matches!(
        events.as_slice(),
        [ClientEvent::Warning {
            kind: WarningKind::MalformedOrUnrelatedPacket,
            ..
        }]
    ));
    client.close().unwrap();
    server.join();
}

#[test]
fn exact_length_echo_reply_still_emits_echo_reply() {
    let params = Params {
        length: 4096,
        ..default_params()
    };
    let server = start_fake_server({
        let params = params.clone();
        move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
            socket.send_to(&reply, peer).unwrap();
            let (request, _) = recv_request(&socket, &tx);
            let seq = u32::from_le_bytes(request[12..16].try_into().unwrap());
            let reply = echo_reply_packet(TOKEN, seq, &params, &TimestampFields::default(), None);
            socket.send_to(&reply, peer).unwrap();
            let _ = recv_request_timeout(&socket, &tx);
        }
    });
    let config = ClientConfig {
        length: 4096,
        socket_config: crate::SocketConfig {
            recv_timeout: Some(Duration::from_millis(200)),
            ..Default::default()
        },
        ..default_test_config(server.addr)
    };
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());
    client.send_probe().unwrap();

    let events = client.recv_once().unwrap();
    assert!(matches!(
        events.as_slice(),
        [ClientEvent::EchoReply { bytes, .. }] if *bytes == echo_packet_len(false, &params)
    ));
    client.close().unwrap();
    server.join();
}

#[test]
fn recv_once_at_test_helper_provides_deterministic_timestamp() {
    let params = default_params();
    let (mut client, server) = open_client_with_echo_server(&params);
    client.send_probe().unwrap();
    thread::sleep(Duration::from_millis(50));

    let fixed_ts = ClientTimestamp {
        mono: Instant::now(),
        wall: SystemTime::now(),
    };
    let events = client.recv_once_at(fixed_ts).unwrap();
    assert_eq!(events.len(), 1);
    if let ClientEvent::EchoReply { received_at, .. } = &events[0] {
        assert_eq!(*received_at, fixed_ts);
    } else {
        panic!("expected EchoReply");
    }
    client.close().unwrap();
    server.join();
}
