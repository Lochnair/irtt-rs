use crate::socket_options::socket_ttl;

use super::*;

#[test]
fn connect_accepts_valid_ttl_config_values() {
    for ttl in [None, Some(1), Some(255)] {
        let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 9)));
        config.socket_config.ttl = ttl;
        Client::connect(config).unwrap();
    }
}

#[test]
fn connect_applies_configured_ipv4_ttl_before_open_and_close_preserves_it() {
    let params = Params {
        protocol_version: PROTOCOL_VERSION,
        duration_ns: 3_000_000_000,
        interval_ns: 1_000_000_000,
        received_stats: ReceivedStats::Both,
        stamp_at: StampAt::Both,
        clock: Clock::Both,
        ..Params::default()
    };
    let server = open_success_server(params.clone());
    let mut config = default_test_config(server.addr);
    config.socket_config.ttl = Some(64);
    let mut client = Client::connect(config).unwrap();

    assert_eq!(socket_ttl(&client.socket, client.remote).unwrap(), 64);

    client.open().unwrap();
    server.join();

    client.close(ClientTimestamp::now()).unwrap();
    assert_eq!(socket_ttl(&client.socket, client.remote).unwrap(), 64);
}

#[test]
fn connect_rejects_invalid_ttl_values() {
    for ttl in [0, 256] {
        let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 9)));
        config.socket_config.ttl = Some(ttl);
        assert!(matches!(
            Client::connect(config),
            Err(ClientError::InvalidConfig { .. })
        ));
    }
}
