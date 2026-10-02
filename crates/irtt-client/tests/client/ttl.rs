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
