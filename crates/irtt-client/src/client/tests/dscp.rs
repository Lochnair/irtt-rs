use super::*;
use crate::socket_options::socket_traffic_class;

#[test]
#[cfg(not(any(
    target_os = "fuchsia",
    target_os = "redox",
    target_os = "solaris",
    target_os = "illumos",
    target_os = "haiku",
)))]
fn normal_open_applies_negotiated_dscp_after_open_and_close_clears_it() {
    let mut params = default_params();
    params.dscp = 46;
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
        socket.send_to(&reply, peer).unwrap();
        let _ = recv_request(&socket, &tx);
    });
    let mut config = default_test_config(server.addr);
    config.dscp = 46;
    let mut client = Client::connect(config).unwrap();

    assert_eq!(
        socket_traffic_class(&client.socket, client.remote).unwrap() & 0xfc,
        0
    );
    assert_open_started(client.open().unwrap());
    assert_eq!(
        socket_traffic_class(&client.socket, client.remote).unwrap() & 0xfc,
        184
    );

    client.close().unwrap();
    assert_eq!(
        socket_traffic_class(&client.socket, client.remote).unwrap(),
        0
    );
    server.join();
}

#[test]
#[cfg(not(any(
    target_os = "fuchsia",
    target_os = "redox",
    target_os = "solaris",
    target_os = "illumos",
    target_os = "haiku",
)))]
fn normal_open_uses_negotiated_dscp_not_requested_dscp() {
    let mut returned = default_params();
    returned.dscp = 0;
    let server = open_success_server(returned);

    let mut config = default_test_config(server.addr);
    config.dscp = 46;
    config.negotiation_policy = NegotiationPolicy::Loose;
    let mut client = Client::connect(config).unwrap();

    let negotiated = assert_open_started(client.open().unwrap());
    assert_eq!(negotiated.params.dscp, 0);
    assert_eq!(
        negotiated.restrictions,
        vec![crate::NegotiationRestriction::DscpChanged {
            requested: 46,
            negotiated: 0,
        }]
    );
    assert_eq!(
        socket_traffic_class(&client.socket, client.remote).unwrap(),
        0
    );
    server.join();
}

#[test]
#[cfg(not(any(
    target_os = "fuchsia",
    target_os = "redox",
    target_os = "solaris",
    target_os = "illumos",
    target_os = "haiku",
)))]
fn failed_close_send_restores_negotiated_dscp_and_keeps_session_open() {
    let mut params = default_params();
    params.dscp = 46;
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
    let mut config = default_test_config(server.addr);
    config.dscp = 46;
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());
    client.test_hooks.fail_close_send.set(true);

    let error = client.close().unwrap_err();

    assert!(matches!(error, ClientError::Socket(_)));
    assert!(client.runtime.is_open());
    assert!(client.schedule.is_some());
    assert_eq!(client.applied_dscp, Some(46));
    assert_eq!(
        socket_traffic_class(&client.socket, client.remote).unwrap() & 0xfc,
        184
    );
    client.close().unwrap();
    server.join();
}

#[test]
fn failed_close_keeps_send_error_primary_when_dscp_restoration_also_fails() {
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
    let mut client = Client::connect(default_test_config(server.addr)).unwrap();
    assert_open_started(client.open().unwrap());
    client.test_hooks.fail_close_send.set(true);
    client.test_hooks.fail_dscp_restore.set(true);

    let error = client.close().unwrap_err();

    assert!(matches!(error, ClientError::Socket(_)));
    assert!(error.to_string().contains("injected close send failure"));
    assert!(client.runtime.is_open());
    assert!(client.schedule.is_some());
    assert_eq!(client.applied_dscp, Some(0));
    client.close().unwrap();
    server.join();
}

#[test]
#[cfg(not(any(
    target_os = "fuchsia",
    target_os = "redox",
    target_os = "solaris",
    target_os = "illumos",
    target_os = "haiku",
)))]
fn authenticated_peer_close_clears_schedule_and_negotiated_dscp() {
    let mut params = default_params();
    params.dscp = 46;
    let server = start_fake_server(move |socket, tx| {
        let (_, peer) = recv_request(&socket, &tx);
        socket
            .send_to(
                &open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None),
                peer,
            )
            .unwrap();
        let (request, _) = recv_request(&socket, &tx);
        let seq = u32::from_le_bytes(request[12..16].try_into().unwrap());
        socket
            .send_to(
                &echo_reply_packet_with_flags(
                    TOKEN,
                    seq,
                    &params,
                    &TimestampFields::default(),
                    None,
                    FLAG_REPLY | flags::FLAG_CLOSE,
                ),
                peer,
            )
            .unwrap();
    });
    let mut config = default_test_config(server.addr);
    config.dscp = 46;
    let mut client = Client::connect(config).unwrap();
    assert_open_started(client.open().unwrap());
    client.send_probe().unwrap();

    let events = client.recv_once().unwrap();

    assert!(matches!(
        events.as_slice(),
        [
            ClientEvent::EchoReply { .. },
            ClientEvent::SessionClosed { .. }
        ]
    ));
    assert!(client.schedule.is_none());
    assert_eq!(client.applied_dscp, None);
    assert_eq!(
        socket_traffic_class(&client.socket, client.remote).unwrap(),
        0
    );
    server.join();
}
