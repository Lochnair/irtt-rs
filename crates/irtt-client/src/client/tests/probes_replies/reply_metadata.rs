use super::*;

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
