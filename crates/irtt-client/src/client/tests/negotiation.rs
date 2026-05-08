use super::*;

#[test]
fn strict_negotiation_accepts_identical_params() {
    let config = ClientConfig::default();
    let params = params_from_config(&config).unwrap();
    assert!(validate_negotiated_params(&params, &params, NegotiationPolicy::Strict).is_ok());
}

#[test]
fn strict_negotiation_rejects_changed_params() {
    let config = ClientConfig::default();
    let requested = params_from_config(&config).unwrap();
    let mut returned = requested.clone();
    returned.dscp = 1;
    assert!(matches!(
        validate_negotiated_params(&requested, &returned, NegotiationPolicy::Strict),
        Err(ClientError::NegotiationRejected { .. })
    ));
}

#[test]
fn loose_negotiation_accepts_server_restricted_params() {
    let config = ClientConfig::default();
    let requested = params_from_config(&config).unwrap();
    let mut returned = requested.clone();
    returned.duration_ns /= 2;
    returned.length = 0;
    assert!(validate_negotiated_params(&requested, &returned, NegotiationPolicy::Loose).is_ok());
}

#[test]
fn loose_negotiation_rejects_non_positive_returned_interval() {
    let config = ClientConfig::default();
    let requested = params_from_config(&config).unwrap();

    for interval_ns in [0, -1] {
        let mut returned = requested.clone();
        returned.interval_ns = interval_ns;
        assert!(matches!(
            validate_negotiated_params(&requested, &returned, NegotiationPolicy::Loose),
            Err(ClientError::NegotiationRejected { reason })
                if reason == "interval must be positive"
        ));
    }
}

#[test]
fn loose_negotiation_accepts_documented_duration_combinations() {
    let finite_config = ClientConfig::default();
    let requested_finite = params_from_config(&finite_config).unwrap();

    let returned_same_finite = requested_finite.clone();
    assert!(validate_negotiated_params(
        &requested_finite,
        &returned_same_finite,
        NegotiationPolicy::Loose
    )
    .is_ok());

    let mut returned_shorter_finite = requested_finite.clone();
    returned_shorter_finite.duration_ns /= 2;
    assert!(validate_negotiated_params(
        &requested_finite,
        &returned_shorter_finite,
        NegotiationPolicy::Loose
    )
    .is_ok());

    let continuous_config = ClientConfig {
        duration: None,
        ..ClientConfig::default()
    };
    let requested_continuous = params_from_config(&continuous_config).unwrap();

    let returned_continuous = requested_continuous.clone();
    assert!(validate_negotiated_params(
        &requested_continuous,
        &returned_continuous,
        NegotiationPolicy::Loose
    )
    .is_ok());

    let mut returned_finite = requested_continuous.clone();
    returned_finite.duration_ns = 1_000_000_000;
    assert!(validate_negotiated_params(
        &requested_continuous,
        &returned_finite,
        NegotiationPolicy::Loose
    )
    .is_ok());
}

#[test]
fn loose_negotiation_rejects_finite_request_returned_continuous() {
    let config = ClientConfig::default();
    let requested = params_from_config(&config).unwrap();
    let mut returned = requested.clone();
    returned.duration_ns = 0;

    assert!(matches!(
        validate_negotiated_params(&requested, &returned, NegotiationPolicy::Loose),
        Err(ClientError::NegotiationRejected { reason })
            if reason == "server returned continuous duration for finite request"
    ));
}

#[test]
fn loose_negotiation_rejects_negative_returned_length() {
    let config = ClientConfig::default();
    let requested = params_from_config(&config).unwrap();
    let mut returned = requested.clone();
    returned.length = -1;
    assert!(matches!(
        validate_negotiated_params(&requested, &returned, NegotiationPolicy::Loose),
        Err(ClientError::NegotiationRejected { reason }) if reason == "length must be non-negative"
    ));
}

#[test]
fn strict_negotiation_rejects_negative_returned_length() {
    let config = ClientConfig::default();
    let requested = params_from_config(&config).unwrap();
    let mut returned = requested.clone();
    returned.length = -1;
    assert!(matches!(
        validate_negotiated_params(&requested, &returned, NegotiationPolicy::Strict),
        Err(ClientError::NegotiationRejected { reason }) if reason == "length must be non-negative"
    ));
}

#[test]
fn loose_negotiation_rejects_runtime_invalid_returned_dscp() {
    let config = ClientConfig::default();
    let requested = params_from_config(&config).unwrap();

    for dscp in [-1, 64] {
        let mut returned = requested.clone();
        returned.dscp = dscp;
        assert!(matches!(
            validate_negotiated_params(&requested, &returned, NegotiationPolicy::Loose),
            Err(ClientError::NegotiationRejected { reason })
                if reason == "dscp must be in range 0..=63"
        ));
    }
}
