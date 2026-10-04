use super::*;

#[test]
fn connect_accepts_valid_ttl_config_values() {
    for ttl in [None, Some(1), Some(255)] {
        let mut config = default_test_config();
        config.socket.ttl = ttl;
        Client::connect(SocketAddr::from(([127, 0, 0, 1], 9)).to_string(), config).unwrap();
    }
}

#[test]
fn connect_rejects_invalid_ttl_values() {
    for ttl in [0, 256] {
        let mut config = default_test_config();
        config.socket.ttl = Some(ttl);
        assert!(matches!(
            Client::connect(SocketAddr::from(([127, 0, 0, 1], 9)).to_string(), config),
            Err(ClientError::InvalidConfig { .. })
        ));
    }
}
