use super::*;

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
