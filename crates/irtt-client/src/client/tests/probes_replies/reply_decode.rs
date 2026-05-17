use super::*;

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
            rtt,
            received_stats,
            server_timing,
            bytes,
            packet_meta,
            ..
        } => {
            assert_eq!(*seq, 0);
            assert_eq!(*bytes, echo_packet_len(false, &params));
            assert!(rtt.raw > Duration::ZERO);
            assert_eq!(
                rtt.effective,
                rtt.adjusted
                    .unwrap_or_else(|| SignedDuration::from_duration(rtt.raw))
            );
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
        ClientEvent::EchoReply { seq, bytes, .. } => {
            assert_eq!(*seq, 1);
            assert_eq!(*bytes, echo_packet_len(false, &params));
        }
        other => panic!("expected EchoReply after shorter datagram, got {other:?}"),
    }
    server.join();
}
