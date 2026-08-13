//! Which traffic class the core asks for each reply to be sent with.
//!
//! The negotiated DSCP parameter is the **raw IPv4 TOS / IPv6 Traffic Class
//! byte**, not a six-bit codepoint: the codepoint is its upper six bits and the
//! low two are ECN. Nothing here shifts, masks or rounds it, and these tests
//! exist to keep it that way.
//!
//! The clean specification's Section 20 settles the observable rules for values
//! in `0..=255`: an echo reply carries its own session's value verbatim, the
//! marking follows the session rather than the listener, and an open reply is
//! unmarked even on a listener that has already sent a marked echo reply. The
//! server-close case is covered where the maximum-duration close is tested, and
//! the socket-level proof that these bytes reach the wire lives in the runtime's
//! Linux tests.

use irtt_proto::Params;

use super::support::{
    client_params, core_with_tokens, echo_request, expect_echo_reply, expect_no_test_reply,
    expect_normal_open_reply, no_test_request, open_request, peer, unthrottled, ScriptedTokens,
};

const TOKEN_A: u64 = 0x7896_b6ab_8771_5213;

/// A raw byte whose low two bits are set on purpose: ECN is part of the value,
/// and a server rebuilding the byte from a codepoint would lose them.
const RAW_TRAFFIC_CLASS: i64 = 0xbb;

#[test]
fn an_open_reply_is_unmarked_whatever_the_session_negotiates() {
    let requested = Params {
        dscp: 0xb8,
        ..client_params()
    };
    let mut core = core_with_tokens(unthrottled(), ScriptedTokens::new([TOKEN_A]));

    let reply = core
        .handle_datagram(peer(), &open_request(&requested, None))
        .unwrap()
        .expect("an ordinary open must be answered");

    assert_eq!(
        expect_normal_open_reply(&reply, None).params.dscp,
        0xb8,
        "the negotiated value is returned to the client"
    );
    assert_eq!(
        reply.traffic_class(),
        0,
        "an open reply is unmarked however the session it creates is marked"
    );

    let no_test = core
        .handle_datagram(peer(), &no_test_request(&requested, None))
        .unwrap()
        .expect("a no-test open must be answered");
    assert_eq!(expect_no_test_reply(&no_test, None).params.dscp, 0xb8);
    assert_eq!(
        no_test.traffic_class(),
        0,
        "a no-test reply is unmarked too"
    );
}

#[test]
fn an_echo_reply_carries_the_sessions_raw_negotiated_byte() {
    let requested = Params {
        dscp: RAW_TRAFFIC_CLASS,
        ..client_params()
    };
    let mut core = core_with_tokens(unthrottled(), ScriptedTokens::new([TOKEN_A]));
    let open = core
        .handle_datagram(peer(), &open_request(&requested, None))
        .unwrap()
        .expect("the session-creating open must be answered");
    let negotiated = expect_normal_open_reply(&open, None).params;

    let reply = core
        .handle_datagram(peer(), &echo_request(TOKEN_A, 0, &negotiated, &[], None))
        .unwrap()
        .expect("an admissible echo must be answered");

    expect_echo_reply(&reply, &negotiated, None);
    assert_eq!(
        i64::from(reply.traffic_class()),
        RAW_TRAFFIC_CLASS,
        "the raw byte, unshifted and unmasked"
    );
}

#[test]
fn a_negotiated_value_that_is_not_a_byte_is_transported_unmarked() {
    // `irtt-rs` policy, not observed behavior. The server deliberately accepts
    // a DSCP parameter outside `0..=255` and returns and stores it unchanged;
    // the clean specification records the reference host's handling of such
    // values as platform-specific and explicitly not a compatibility
    // requirement. A value that is not a byte cannot be a traffic-class byte,
    // so the transport asks for none — rather than wrapping −1 into 255,
    // truncating 256 to 0, or refusing a session the client can otherwise use.
    for dscp in [-1, 256, i64::MAX] {
        let requested = Params {
            dscp,
            ..client_params()
        };
        let mut core = core_with_tokens(unthrottled(), ScriptedTokens::new([TOKEN_A]));

        let open = core
            .handle_datagram(peer(), &open_request(&requested, None))
            .unwrap()
            .unwrap_or_else(|| panic!("an open requesting DSCP {dscp} must still be answered"));
        let negotiated = expect_normal_open_reply(&open, None).params;
        assert_eq!(negotiated.dscp, dscp, "the value is negotiated unchanged");
        assert_eq!(open.traffic_class(), 0);
        assert_eq!(
            core.session(TOKEN_A)
                .expect("the session must be live")
                .params()
                .dscp,
            dscp,
            "and stored unchanged"
        );

        let reply = core
            .handle_datagram(peer(), &echo_request(TOKEN_A, 0, &negotiated, &[], None))
            .unwrap()
            .unwrap_or_else(|| panic!("the session must still serve echoes with DSCP {dscp}"));
        assert_eq!(
            expect_echo_reply(&reply, &negotiated, None).recv_count,
            Some(1),
            "an unmarkable session is an ordinary session in every other way"
        );
        assert_eq!(reply.traffic_class(), 0, "transported unmarked");
    }
}
