use super::*;

#[test]
fn client_config_default() {
    let config = ClientConfig::default();
    assert_eq!(config.duration, Some(Duration::from_secs(3)));
    assert_eq!(config.interval, Duration::from_secs(1));
    assert_eq!(config.length, 0);
    assert_eq!(config.received_stats, ReceivedStats::Both);
    assert_eq!(config.stamp_at, StampAt::Both);
    assert_eq!(config.clock, Clock::Both);
    assert_eq!(config.dscp, 0);
    assert_eq!(config.hmac_key, None);
    assert_eq!(config.server_fill, None);
    assert_eq!(config.open_timeouts, DEFAULT_OPEN_TIMEOUTS);
    assert_eq!(config.run_mode, RunMode::Normal);
    assert_eq!(config.negotiation_policy, NegotiationPolicy::Strict);
    assert_eq!(config.probe_timeout, Duration::from_secs(4));
    assert_eq!(config.max_pending_probes, 4096);
}

#[test]
fn params_from_config_maps_compatibility_fields() {
    let config = ClientConfig {
        duration: Some(Duration::from_secs(5)),
        interval: Duration::from_millis(250),
        length: 1472,
        received_stats: ReceivedStats::Window,
        stamp_at: StampAt::Midpoint,
        clock: Clock::Wall,
        dscp: 46,
        hmac_key: Some(b"secret".to_vec()),
        server_fill: Some("rand".to_owned()),
        ..ClientConfig::default()
    };

    let params = params_from_config(&config).unwrap();
    assert_eq!(params.protocol_version, PROTOCOL_VERSION);
    assert_eq!(params.duration_ns, 5_000_000_000);
    assert_eq!(params.interval_ns, 250_000_000);
    assert_eq!(params.length, 1472);
    assert_eq!(params.received_stats, ReceivedStats::Window);
    assert_eq!(params.stamp_at, StampAt::Midpoint);
    assert_eq!(params.clock, Clock::Wall);
    assert_eq!(params.dscp, 46, "config DSCP codepoint must not be shifted");
    assert_eq!(
        params.server_fill.as_ref().map(|fill| fill.value.as_str()),
        Some("rand")
    );
    assert_eq!(config.hmac_key.as_deref(), Some(b"secret".as_slice()));
}

#[test]
fn params_from_config_accepts_udp_payload_length_boundary_values() {
    for length in [0, 1, 1472, 4096, MAX_UDP_PAYLOAD_LENGTH] {
        let config = ClientConfig {
            length,
            ..ClientConfig::default()
        };
        assert_eq!(
            params_from_config(&config).unwrap().length,
            i64::from(length)
        );
    }
}

#[test]
fn params_from_config_rejects_oversized_udp_payload_length() {
    let config = ClientConfig {
        length: MAX_UDP_PAYLOAD_LENGTH + 1,
        ..ClientConfig::default()
    };
    assert!(matches!(
        params_from_config(&config),
        Err(ClientError::InvalidConfig { reason }) if reason.contains("packet length")
    ));
}

#[test]
fn params_from_config_encodes_continuous_duration_as_zero() {
    let config = ClientConfig {
        duration: None,
        ..ClientConfig::default()
    };
    assert_eq!(params_from_config(&config).unwrap().duration_ns, 0);
}

#[test]
fn params_from_config_rejects_zero_finite_duration() {
    let config = ClientConfig {
        duration: Some(Duration::ZERO),
        ..ClientConfig::default()
    };
    assert!(matches!(
        params_from_config(&config),
        Err(ClientError::InvalidConfig { reason })
            if reason == "duration must be greater than zero; use None for continuous mode"
    ));
}

#[test]
fn params_from_config_rejects_zero_interval() {
    let config = ClientConfig {
        interval: Duration::ZERO,
        ..ClientConfig::default()
    };
    assert!(matches!(
        params_from_config(&config),
        Err(ClientError::InvalidConfig { reason }) if reason == "interval must be greater than zero"
    ));
}

#[test]
fn params_from_config_rejects_nanosecond_encoding_overflow_as_invalid_config() {
    let config = ClientConfig {
        duration: Some(Duration::from_nanos(i64::MAX as u64) + Duration::from_nanos(1)),
        ..ClientConfig::default()
    };
    assert!(matches!(
        params_from_config(&config),
        Err(ClientError::InvalidConfig { reason })
            if reason == "duration is too large to encode as nanoseconds"
    ));

    let config = ClientConfig {
        interval: Duration::from_nanos(i64::MAX as u64) + Duration::from_nanos(1),
        ..ClientConfig::default()
    };
    assert!(matches!(
        params_from_config(&config),
        Err(ClientError::InvalidConfig { reason })
            if reason == "interval is too large to encode as nanoseconds"
    ));
}

#[test]
fn params_from_config_accepts_max_dscp_codepoint() {
    let config = ClientConfig {
        dscp: 63,
        ..ClientConfig::default()
    };
    assert_eq!(params_from_config(&config).unwrap().dscp, 63);
}

#[test]
fn params_from_config_rejects_invalid_dscp_codepoint() {
    let config = ClientConfig {
        dscp: 64,
        ..ClientConfig::default()
    };
    assert!(matches!(
        params_from_config(&config),
        Err(ClientError::InvalidConfig { .. })
    ));
}

#[test]
fn params_from_config_rejects_empty_server_fill() {
    let config = ClientConfig {
        server_fill: Some("".to_owned()),
        ..ClientConfig::default()
    };
    assert!(matches!(
        params_from_config(&config),
        Err(ClientError::InvalidConfig { .. })
    ));
}

#[test]
fn params_from_config_rejects_oversized_server_fill() {
    let config = ClientConfig {
        server_fill: Some("0123456789abcdef0123456789abcdefx".to_owned()),
        ..ClientConfig::default()
    };
    assert!(matches!(
        params_from_config(&config),
        Err(ClientError::InvalidConfig { .. })
    ));
}

#[test]
fn minimum_open_timeout_under_200ms_is_rejected() {
    let config = ClientConfig {
        open_timeouts: vec![Duration::from_millis(199)],
        ..ClientConfig::default()
    };
    assert!(matches!(
        Client::connect(config),
        Err(ClientError::OpenTimeoutTooSmall { .. })
    ));
}

#[test]
fn empty_open_timeouts_is_rejected() {
    let config = ClientConfig {
        open_timeouts: vec![],
        ..ClientConfig::default()
    };
    assert!(matches!(
        Client::connect(config),
        Err(ClientError::NoOpenTimeouts)
    ));
}
