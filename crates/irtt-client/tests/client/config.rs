use super::*;

#[test]
fn connect_rejects_invalid_configuration() {
    let i64_max_ns = u64::try_from(i64::MAX).unwrap();
    let too_large = Duration::from_nanos(i64_max_ns) + Duration::from_nanos(1);
    let cases = [
        (
            "oversized UDP payload",
            ClientConfig {
                request: SessionRequest {
                    length: MAX_UDP_PAYLOAD_LENGTH + 1,
                    ..Default::default()
                },
                ..ClientConfig::default()
            },
            "packet length",
        ),
        (
            "zero finite duration",
            ClientConfig {
                request: SessionRequest {
                    duration: Some(Duration::ZERO),
                    ..Default::default()
                },
                ..ClientConfig::default()
            },
            "duration must be greater than zero; use None for continuous mode",
        ),
        (
            "zero interval",
            ClientConfig {
                request: SessionRequest {
                    interval: Duration::ZERO,
                    ..Default::default()
                },
                ..ClientConfig::default()
            },
            "interval must be greater than zero",
        ),
        (
            "duration nanosecond overflow",
            ClientConfig {
                request: SessionRequest {
                    duration: Some(too_large),
                    ..Default::default()
                },
                ..ClientConfig::default()
            },
            "duration is too large to encode as nanoseconds",
        ),
        (
            "interval nanosecond overflow",
            ClientConfig {
                request: SessionRequest {
                    interval: too_large,
                    ..Default::default()
                },
                ..ClientConfig::default()
            },
            "interval is too large to encode as nanoseconds",
        ),
        (
            "invalid DSCP codepoint",
            ClientConfig {
                request: SessionRequest {
                    dscp: 64,
                    ..Default::default()
                },
                ..ClientConfig::default()
            },
            "dscp",
        ),
        (
            // The unspecified clock is the absent-tag wire default, not a
            // client mode; a client that needs timestamps must name a clock.
            "unspecified clock",
            ClientConfig {
                request: SessionRequest {
                    clock: Clock::Unspecified,
                    ..Default::default()
                },
                ..ClientConfig::default()
            },
            "clock must be wall, monotonic, or both",
        ),
        (
            "empty server fill",
            ClientConfig {
                request: SessionRequest {
                    server_fill: Some("".to_owned()),
                    ..Default::default()
                },
                ..ClientConfig::default()
            },
            "server_fill",
        ),
        (
            "oversized server fill",
            ClientConfig {
                request: SessionRequest {
                    server_fill: Some("0123456789abcdef0123456789abcdefx".to_owned()),
                    ..Default::default()
                },
                ..ClientConfig::default()
            },
            "server_fill",
        ),
    ];

    for (name, config, expected_reason) in cases {
        assert!(
            matches!(
                Client::connect("127.0.0.1:2112", config),
                Err(ClientError::InvalidConfig { .. })
            ),
            "{name} should fail with InvalidConfig ({expected_reason})"
        );
    }
}

#[test]
fn minimum_open_timeout_under_200ms_is_rejected() {
    let config = ClientConfig {
        open: OpenPolicy {
            timeouts: vec![Duration::from_millis(199)],
            ..Default::default()
        },
        ..ClientConfig::default()
    };
    assert!(matches!(
        Client::connect("127.0.0.1:2112", config),
        Err(ClientError::OpenTimeoutTooSmall { .. })
    ));
}

#[test]
fn empty_open_timeouts_is_rejected() {
    let config = ClientConfig {
        open: OpenPolicy {
            timeouts: vec![],
            ..Default::default()
        },
        ..ClientConfig::default()
    };
    assert!(matches!(
        Client::connect("127.0.0.1:2112", config),
        Err(ClientError::NoOpenTimeouts)
    ));
}
