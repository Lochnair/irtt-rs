use super::*;

#[test]
fn connect_rejects_invalid_configuration() {
    let i64_max_ns = u64::try_from(i64::MAX).unwrap();
    let too_large = Duration::from_nanos(i64_max_ns) + Duration::from_nanos(1);
    let cases = [
        (
            "oversized UDP payload",
            ClientConfig {
                length: MAX_UDP_PAYLOAD_LENGTH + 1,
                ..ClientConfig::default()
            },
            "packet length",
        ),
        (
            "zero finite duration",
            ClientConfig {
                duration: Some(Duration::ZERO),
                ..ClientConfig::default()
            },
            "duration must be greater than zero; use None for continuous mode",
        ),
        (
            "zero interval",
            ClientConfig {
                interval: Duration::ZERO,
                ..ClientConfig::default()
            },
            "interval must be greater than zero",
        ),
        (
            "duration nanosecond overflow",
            ClientConfig {
                duration: Some(too_large),
                ..ClientConfig::default()
            },
            "duration is too large to encode as nanoseconds",
        ),
        (
            "interval nanosecond overflow",
            ClientConfig {
                interval: too_large,
                ..ClientConfig::default()
            },
            "interval is too large to encode as nanoseconds",
        ),
        (
            "invalid DSCP codepoint",
            ClientConfig {
                dscp: 64,
                ..ClientConfig::default()
            },
            "dscp",
        ),
        (
            // The unspecified clock is the absent-tag wire default, not a
            // client mode; a client that needs timestamps must name a clock.
            "unspecified clock",
            ClientConfig {
                clock: Clock::Unspecified,
                ..ClientConfig::default()
            },
            "clock must be wall, monotonic, or both",
        ),
        (
            "empty server fill",
            ClientConfig {
                server_fill: Some("".to_owned()),
                ..ClientConfig::default()
            },
            "server_fill",
        ),
        (
            "oversized server fill",
            ClientConfig {
                server_fill: Some("0123456789abcdef0123456789abcdefx".to_owned()),
                ..ClientConfig::default()
            },
            "server_fill",
        ),
    ];

    for (name, config, expected_reason) in cases {
        assert!(
            matches!(
                Client::connect(config),
                Err(ClientError::InvalidConfig { .. })
            ),
            "{name} should fail with InvalidConfig ({expected_reason})"
        );
    }
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
