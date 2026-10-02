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
    let config = ClientConfig {
        dscp: 46,
        ..ClientConfig::default()
    };
    let requested = params_from_config(&config).unwrap();
    assert_eq!(
        requested.dscp, 184,
        "codepoint 46 must become raw wire byte 184"
    );

    for dscp in [-1, 256] {
        let mut returned = requested.clone();
        returned.dscp = dscp;
        assert_eq!(
            rejection_reason(&requested, &returned, NegotiationPolicy::Loose),
            "dscp must be in range 0..=255"
        );
    }

    // A raw returned value of 184 must be accepted even though it exceeds the
    // 0..=63 codepoint range; it is the correctly negotiated wire value for
    // codepoint 46 and is not subject to the codepoint bound at all.
    let returned = requested.clone();
    assert!(
        assert_negotiates(&requested, &returned, NegotiationPolicy::Loose)
            .restrictions
            .is_empty()
    );
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
