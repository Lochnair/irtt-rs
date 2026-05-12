use super::*;
use crate::NegotiationRestriction;

fn assert_negotiates(
    requested: &Params,
    returned: &Params,
    policy: NegotiationPolicy,
) -> NegotiatedParams {
    negotiate_params(requested, returned.clone(), policy)
        .unwrap_or_else(|err| panic!("expected negotiation success, got {err:?}"))
}

fn rejection_reason(requested: &Params, returned: &Params, policy: NegotiationPolicy) -> String {
    match negotiate_params(requested, returned.clone(), policy) {
        Err(ClientError::NegotiationRejected { reason }) => reason,
        other => panic!("expected negotiation rejection, got {other:?}"),
    }
}

#[test]
fn strict_negotiation_accepts_identical_params() {
    let config = ClientConfig::default();
    let params = params_from_config(&config).unwrap();
    let negotiated = assert_negotiates(&params, &params, NegotiationPolicy::Strict);
    assert_eq!(negotiated.params, params);
    assert!(negotiated.restrictions.is_empty());
}

#[test]
fn strict_negotiation_rejects_changed_params() {
    let config = ClientConfig {
        dscp: 46,
        ..ClientConfig::default()
    };
    let requested = params_from_config(&config).unwrap();
    let mut returned = requested.clone();
    returned.dscp = 0;
    assert_eq!(
        rejection_reason(&requested, &returned, NegotiationPolicy::Strict),
        NegotiationRestriction::DscpChanged {
            requested: requested.dscp,
            negotiated: returned.dscp,
        }
        .message()
    );
}

#[test]
fn loose_negotiation_accepts_server_restricted_params() {
    let config = ClientConfig::default();
    let mut requested = params_from_config(&config).unwrap();
    requested.length = 128;
    let mut returned = requested.clone();
    returned.duration_ns /= 2;
    returned.length = 0;
    let negotiated = assert_negotiates(&requested, &returned, NegotiationPolicy::Loose);
    assert_eq!(
        negotiated.restrictions,
        vec![
            NegotiationRestriction::DurationReduced {
                requested_ns: requested.duration_ns,
                negotiated_ns: returned.duration_ns,
            },
            NegotiationRestriction::LengthReduced {
                requested: requested.length,
                negotiated: returned.length,
            },
        ]
    );
}

#[test]
fn loose_negotiation_rejects_non_positive_returned_interval() {
    let config = ClientConfig::default();
    let requested = params_from_config(&config).unwrap();

    for interval_ns in [0, -1] {
        let mut returned = requested.clone();
        returned.interval_ns = interval_ns;
        assert_eq!(
            rejection_reason(&requested, &returned, NegotiationPolicy::Loose),
            "interval must be positive"
        );
    }
}

#[test]
fn loose_negotiation_accepts_documented_duration_combinations() {
    let finite_config = ClientConfig::default();
    let requested_finite = params_from_config(&finite_config).unwrap();

    let returned_same_finite = requested_finite.clone();
    assert!(assert_negotiates(
        &requested_finite,
        &returned_same_finite,
        NegotiationPolicy::Loose
    )
    .restrictions
    .is_empty());

    let mut returned_shorter_finite = requested_finite.clone();
    returned_shorter_finite.duration_ns /= 2;
    assert_eq!(
        assert_negotiates(
            &requested_finite,
            &returned_shorter_finite,
            NegotiationPolicy::Loose
        )
        .restrictions,
        vec![NegotiationRestriction::DurationReduced {
            requested_ns: requested_finite.duration_ns,
            negotiated_ns: returned_shorter_finite.duration_ns,
        }]
    );

    let continuous_config = ClientConfig {
        duration: None,
        ..ClientConfig::default()
    };
    let requested_continuous = params_from_config(&continuous_config).unwrap();

    let returned_continuous = requested_continuous.clone();
    assert!(assert_negotiates(
        &requested_continuous,
        &returned_continuous,
        NegotiationPolicy::Loose
    )
    .restrictions
    .is_empty());

    let mut returned_finite = requested_continuous.clone();
    returned_finite.duration_ns = 1_000_000_000;
    assert_eq!(
        assert_negotiates(
            &requested_continuous,
            &returned_finite,
            NegotiationPolicy::Loose
        )
        .restrictions,
        vec![NegotiationRestriction::DurationReduced {
            requested_ns: requested_continuous.duration_ns,
            negotiated_ns: returned_finite.duration_ns,
        }]
    );
}

#[test]
fn loose_negotiation_rejects_finite_request_returned_continuous() {
    let config = ClientConfig::default();
    let requested = params_from_config(&config).unwrap();
    let mut returned = requested.clone();
    returned.duration_ns = 0;

    assert_eq!(
        rejection_reason(&requested, &returned, NegotiationPolicy::Loose),
        "server returned continuous duration for finite request"
    );
}

#[test]
fn loose_negotiation_rejects_negative_returned_length() {
    let config = ClientConfig::default();
    let requested = params_from_config(&config).unwrap();
    let mut returned = requested.clone();
    returned.length = -1;
    assert_eq!(
        rejection_reason(&requested, &returned, NegotiationPolicy::Loose),
        "length must be non-negative"
    );
}

#[test]
fn strict_negotiation_rejects_negative_returned_length() {
    let config = ClientConfig::default();
    let requested = params_from_config(&config).unwrap();
    let mut returned = requested.clone();
    returned.length = -1;
    assert_eq!(
        rejection_reason(&requested, &returned, NegotiationPolicy::Strict),
        "length must be non-negative"
    );
}

#[test]
fn loose_negotiation_rejects_runtime_invalid_returned_dscp() {
    let config = ClientConfig::default();
    let requested = params_from_config(&config).unwrap();

    for dscp in [-1, 64] {
        let mut returned = requested.clone();
        returned.dscp = dscp;
        assert_eq!(
            rejection_reason(&requested, &returned, NegotiationPolicy::Loose),
            "dscp must be in range 0..=63"
        );
    }
}

#[test]
fn loose_negotiation_records_dscp_disabled_by_server() {
    let config = ClientConfig {
        dscp: 46,
        ..ClientConfig::default()
    };
    let requested = params_from_config(&config).unwrap();
    let mut returned = requested.clone();
    returned.dscp = 0;

    let negotiated = assert_negotiates(&requested, &returned, NegotiationPolicy::Loose);

    assert_eq!(
        negotiated.restrictions,
        vec![NegotiationRestriction::DscpChanged {
            requested: 46,
            negotiated: 0,
        }]
    );
}

#[test]
fn strict_negotiation_rejects_dscp_disabled_by_server_as_specific_restriction() {
    let config = ClientConfig {
        dscp: 46,
        ..ClientConfig::default()
    };
    let requested = params_from_config(&config).unwrap();
    let mut returned = requested.clone();
    returned.dscp = 0;

    assert_eq!(
        rejection_reason(&requested, &returned, NegotiationPolicy::Strict),
        NegotiationRestriction::DscpChanged {
            requested: 46,
            negotiated: 0,
        }
        .message()
    );
}

#[test]
fn negotiation_rejects_unsupported_dscp_changes() {
    let config = ClientConfig {
        dscp: 46,
        ..ClientConfig::default()
    };
    let requested = params_from_config(&config).unwrap();
    let mut returned = requested.clone();
    returned.dscp = 8;

    assert_eq!(
        rejection_reason(&requested, &returned, NegotiationPolicy::Loose),
        "server returned unsupported DSCP change"
    );
    assert_eq!(
        rejection_reason(&requested, &returned, NegotiationPolicy::Strict),
        "server returned unsupported DSCP change"
    );

    let zero_config = ClientConfig::default();
    let zero_requested = params_from_config(&zero_config).unwrap();
    let mut returned = zero_requested.clone();
    returned.dscp = 46;

    assert_eq!(
        rejection_reason(&zero_requested, &returned, NegotiationPolicy::Loose),
        "server returned unsupported DSCP change"
    );
}
