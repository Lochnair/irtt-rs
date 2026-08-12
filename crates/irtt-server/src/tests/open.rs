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
    // An unspecified clock is not itself a defect. Only pairing it with a
    // selected `stamp_at` is, so a session requesting no timestamps at all is
    // fully valid with no Clock tag.
    assert_eq!(reply.params.stamp_at, StampAt::None);
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
fn selecting_timestamps_without_a_clock_is_silently_refused() {
    // Timestamps from no clock at all describe a session the server could not
    // run: the negotiated layout carries no timestamp field to fill. The open
    // is discarded rather than repaired — synthesizing a clock would answer
    // with a session the client never asked for, and dropping `stamp_at` would
    // silently discard the measurement it did ask for.
    //
    // Only an *absent* Clock tag reaches this: an explicit zero is already out
    // of range for the decoder, and a conforming client always sends a clock
    // when it selects timestamps.
    for stamp_at in [
        StampAt::Send,
        StampAt::Receive,
        StampAt::Both,
        StampAt::Midpoint,
    ] {
        let requested = Params {
            clock: Clock::Unspecified,
            stamp_at,
            ..client_params()
        };
        let tokens = ScriptedTokens::new([TOKEN_A]);
        let mut core = core_with_tokens(ServerConfig::default(), tokens.clone());

        let reply = core
            .handle_datagram(peer(), &open_request(&requested, None))
            .expect("an unusable timestamp request is not a server error");

        assert!(
            reply.is_none(),
            "{stamp_at:?} without a clock must be silent"
        );
        assert_eq!(
            core.session_count(),
            0,
            "{stamp_at:?} without a clock must create no session"
        );
        assert_eq!(
            tokens.remaining(),
            1,
            "{stamp_at:?} without a clock must not consume a token"
        );
    }
}

#[test]
fn negotiation_restricts_nothing_but_the_protocol_version() {
    // These are the values a later restriction-policy slice will deliberately
    // start changing. Pinning them now makes that a visible decision rather
    // than an accident.
    //
    // This is about what *negotiation* rewrites, which is a separate question
    // from whether the core will acknowledge the result: an open whose
    // effective parameters are incoherent is still discarded, as
    // `selecting_timestamps_without_a_clock_is_silently_refused` shows.
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
    //
    // A negative length in particular stays negative in both the reply and the
    // stored session. It does not need clamping here to be executable: echo
    // packet sizing in `irtt-proto` floors it at the required field block, so
    // the session this creates can be run as negotiated.
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
        let session = core.session(TOKEN_A).expect("the session must be live");
        assert_eq!(
            session.params(),
            &reply.params,
            "the session must hold the negotiated values verbatim"
        );
    }
}
