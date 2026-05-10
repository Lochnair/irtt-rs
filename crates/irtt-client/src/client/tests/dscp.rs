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

    client.close(ClientTimestamp::now()).unwrap();
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
        socket_traffic_class(&client.socket, client.remote).unwrap(),
        0
    );
    server.join();
}
