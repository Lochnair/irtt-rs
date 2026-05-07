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
