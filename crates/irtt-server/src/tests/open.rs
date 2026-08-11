use irtt_proto::{Clock, Params, ReceivedStats, ServerFill, StampAt};

use super::support::{
    client_params, core_with_tokens, expect_normal_open_reply, open_request,
    open_request_with_raw_params, param_int, peer, ScriptedTokens,
};
use crate::ServerConfig;

const TOKEN_A: u64 = 0x7896_b6ab_8771_5213;

#[test]
fn an_ordinary_open_is_answered_and_creates_one_session() {
    let mut core = core_with_tokens(ServerConfig::default(), ScriptedTokens::new([TOKEN_A]));

    let packet = core
        .handle_datagram(peer(), &open_request(&client_params(), None))
        .unwrap()
        .expect("an ordinary open must be answered");

    let reply = expect_normal_open_reply(&packet, None);
    assert_eq!(reply.token, TOKEN_A);
    assert_eq!(core.session_count(), 1);

    let session = core.session(TOKEN_A).expect("the session must be live");
    assert_eq!(session.params(), &reply.params);
}

#[test]
fn a_session_records_the_exact_source_endpoint() {
    // Address family, address, port and — where present — the IPv6 scope all
    // form part of session identity, so the endpoint is stored verbatim rather
    // than normalized to an address.
    for endpoint in [
        "198.51.100.7:41234",
        "198.51.100.7:41235",
        "[2001:db8::1]:2112",
        "[fe80::1%3]:2112",
    ] {
        let endpoint = endpoint.parse().unwrap();
        let mut core = core_with_tokens(ServerConfig::default(), ScriptedTokens::new([TOKEN_A]));

        core.handle_datagram(endpoint, &open_request(&client_params(), None))
            .unwrap()
            .expect("open must be answered");

        assert_eq!(core.session(TOKEN_A).unwrap().peer(), endpoint);
    }
}

#[test]
fn any_requested_protocol_version_is_accepted_and_answered_with_one() {
    // Version mismatch is detected client-side only: the server rewrites the
    // version and creates a normal session whatever was asked for.
    for requested in [Some(1), Some(0), Some(2), Some(-1), None] {
        let mut core = core_with_tokens(ServerConfig::default(), ScriptedTokens::new([TOKEN_A]));
        let params = Params {
            protocol_version: requested.unwrap_or(0),
            ..client_params()
        };
        let request = match requested {
            // An explicit zero cannot be encoded by `Params::encode`, which
            // omits zero-valued integers, so encode that tag by hand.
            Some(0) => open_request_with_raw_params(&param_int(1, 0), None),
            Some(_) => open_request(&params, None),
            None => open_request(
                &Params {
                    protocol_version: 0,
                    ..params
                },
                None,
            ),
        };

        let packet = core
            .handle_datagram(peer(), &request)
            .unwrap()
            .unwrap_or_else(|| panic!("protocol version {requested:?} must be answered"));

        let reply = expect_normal_open_reply(&packet, None);
        assert_eq!(reply.params.protocol_version, 1);
        assert_eq!(core.session_count(), 1);
    }
}

#[test]
fn an_empty_parameter_payload_is_answered_with_only_a_version() {
    let mut core = core_with_tokens(ServerConfig::default(), ScriptedTokens::new([TOKEN_A]));

    let packet = core
        .handle_datagram(peer(), &open_request_with_raw_params(&[], None))
        .unwrap()
        .expect("an empty parameter payload is a valid open");

    // header (4) + token (8) + ProtocolVersion=1 (2). Every other parameter
    // negotiates to its wire-default zero and is omitted entirely, which is
    // only reachable if absent parameters really did decode as zero.
    assert_eq!(packet.len(), 14);

    let reply = expect_normal_open_reply(&packet, None);
    assert_eq!(
        reply.params,
        Params {
            protocol_version: 1,
            ..Params::default()
        }
    );
    assert_eq!(reply.params.clock, Clock::Unspecified);
    assert_eq!(core.session_count(), 1);
}

#[test]
fn unknown_parameter_tags_are_ignored_and_not_reflected_in_the_reply() {
    let mut core = core_with_tokens(ServerConfig::default(), ScriptedTokens::new([TOKEN_A]));

    let mut payload = param_int(1, 1);
    payload.extend_from_slice(&param_int(42, 7));
    payload.extend_from_slice(&param_int(200, 9));

    let packet = core
        .handle_datagram(peer(), &open_request_with_raw_params(&payload, None))
        .unwrap()
        .expect("unknown tags must not prevent an open");

    let reply = expect_normal_open_reply(&packet, None);
    assert_eq!(
        reply.params,
        Params {
            protocol_version: 1,
            ..Params::default()
        }
    );
    // The reply is the same 14 bytes an empty payload produces: no unknown tag
    // was copied through.
    assert_eq!(packet.len(), 14);
    assert_eq!(core.session_count(), 1);
}

#[test]
fn this_slice_restricts_nothing_but_the_protocol_version() {
    // These are the values a later restriction-policy slice will deliberately
    // start changing. Pinning them now makes that a visible decision rather
    // than an accident.
    let unrestricted = Params {
        protocol_version: 2,
        duration_ns: 3_600_000_000_000,
        interval_ns: 1,
        length: 65_535,
        received_stats: ReceivedStats::Window,
        stamp_at: StampAt::Midpoint,
        clock: Clock::Monotonic,
        dscp: 184,
        server_fill: Some(ServerFill {
            value: "pattern:abc".to_owned(),
        }),
    };

    let mut core = core_with_tokens(ServerConfig::default(), ScriptedTokens::new([TOKEN_A]));
    let packet = core
        .handle_datagram(peer(), &open_request(&unrestricted, None))
        .unwrap()
        .expect("open must be answered");

    let reply = expect_normal_open_reply(&packet, None);
    assert_eq!(
        reply.params,
        Params {
            protocol_version: 1,
            ..unrestricted
        }
    );
}

#[test]
fn negative_and_out_of_range_length_and_dscp_survive_negotiation() {
    // Length and DSCP are accepted as decoded in this slice, including values a
    // socket could never carry. Restricting a DSCP the server cannot apply is a
    // later policy decision, not an admission rule.
    for (length, dscp) in [(-1, -1), (0, 0), (1472, 184), (-4096, 512)] {
        let requested = Params {
            protocol_version: 1,
            length,
            dscp,
            ..Params::default()
        };
        let mut core = core_with_tokens(ServerConfig::default(), ScriptedTokens::new([TOKEN_A]));

        let packet = core
            .handle_datagram(peer(), &open_request(&requested, None))
            .unwrap()
            .unwrap_or_else(|| panic!("length {length} dscp {dscp} must be answered"));

        let reply = expect_normal_open_reply(&packet, None);
        assert_eq!(reply.params.length, length);
        assert_eq!(reply.params.dscp, dscp);
        assert_eq!(core.session_count(), 1);
    }
}
