//! Server fill negotiation: what an open reply reports, and what the session
//! will actually fill with.
//!
//! The two are deliberately different questions. A client that expressed no
//! preference keeps its absent or empty descriptor in the negotiated parameters
//! while the session uses this server's default fill, and only a descriptor the
//! server could not honor is rewritten. Asserting the reply alone would not
//! separate those, so these tests read the session's effective mode too.

use irtt_proto::{Params, ServerFill};

use super::support::{
    client_params, core_with_tokens, expect_no_test_reply, expect_normal_open_reply,
    no_test_request, open_request, peer, unthrottled, ScriptedTokens,
};
use crate::{core::ServerCore, fill::FillMode};

const TOKEN_A: u64 = 0x0102_0304_0506_0708;

/// Params requesting `descriptor` as the server fill.
fn requesting(descriptor: Option<&str>) -> Params {
    Params {
        server_fill: descriptor.map(|value| ServerFill {
            value: value.to_owned(),
        }),
        ..client_params()
    }
}

/// Opens a session requesting `descriptor` and returns the descriptor the reply
/// carried together with the fill the session actually holds.
fn negotiated(descriptor: Option<&str>) -> (Option<String>, FillMode) {
    let mut core = core_with_tokens(unthrottled(), ScriptedTokens::new([TOKEN_A]));
    let packet = core
        .handle_datagram(peer(), &open_request(&requesting(descriptor), None))
        .unwrap()
        .expect("the open must be answered");

    let reply = expect_normal_open_reply(&packet, None);
    let fill = effective_fill(&core, reply.token);
    (reply.params.server_fill.map(|fill| fill.value), fill)
}

/// The effective fill of the session `token` names.
fn effective_fill(core: &ServerCore, token: u64) -> FillMode {
    core.session(token)
        .expect("the open must have created a session")
        .fill()
        .clone()
}

/// The descriptor a no-test open reports for `descriptor`, which creates no
/// session and so retains no fill.
fn no_test_negotiated(descriptor: Option<&str>) -> Option<String> {
    let tokens = ScriptedTokens::new([TOKEN_A]);
    let mut core = core_with_tokens(unthrottled(), tokens.clone());
    let packet = core
        .handle_datagram(peer(), &no_test_request(&requesting(descriptor), None))
        .unwrap()
        .expect("the no-test open must be answered");

    let reply = expect_no_test_reply(&packet, None);
    assert_eq!(core.session_count(), 0, "no-test creates no session");
    assert_eq!(tokens.remaining(), 1, "no-test draws no session token");
    reply.params.server_fill.map(|fill| fill.value)
}

#[test]
fn no_preference_keeps_an_absent_descriptor_and_uses_the_default_fill() {
    // The important half is the first: a client that asked for nothing must be
    // answered with nothing, or a strict client rejects a restriction the
    // server never actually imposed. The server's own default is an internal
    // choice and stays internal.
    let (returned, fill) = negotiated(None);
    assert_eq!(returned, None, "an absent request stays absent");
    assert_eq!(fill, FillMode::default_fill());
}

#[test]
fn an_explicitly_empty_descriptor_is_preserved_and_uses_the_default_fill() {
    // A low-level peer can send this even though `ClientConfig` rejects an
    // empty fill before it reaches the wire. The clean evidence groups empty
    // with absent as "no preference", so it is neither refused nor rewritten:
    // the requested wire value comes back exactly, and the server picks its own
    // behavior behind it.
    let (returned, fill) = negotiated(Some(""));
    assert_eq!(
        returned.as_deref(),
        Some(""),
        "an empty request stays empty"
    );
    assert_eq!(fill, FillMode::default_fill());
}

#[test]
fn a_valid_descriptor_is_returned_exactly_as_requested() {
    for (descriptor, expected) in [
        ("none", FillMode::None),
        ("rand", FillMode::Random),
        ("pattern:00", FillMode::Pattern(vec![0x00])),
        ("pattern:ff00", FillMode::Pattern(vec![0xff, 0x00])),
        // Returned with its own hexadecimal case, not normalized to the bytes
        // it decodes to: rewriting it would report a restriction that did not
        // happen, and a strict client would reject the session for it.
        ("pattern:AaBb", FillMode::Pattern(vec![0xaa, 0xbb])),
    ] {
        let (returned, fill) = negotiated(Some(descriptor));
        assert_eq!(returned.as_deref(), Some(descriptor), "{descriptor}");
        assert_eq!(fill, expected, "{descriptor}");
    }
}

#[test]
fn an_unknown_or_malformed_descriptor_is_replaced_by_the_default() {
    // Here the server really did change what was asked for, so the reply says
    // so and a strict client may refuse the session. The requested string is
    // not retained anywhere.
    for descriptor in [
        "bogus",
        "RAND",
        "None",
        "Pattern:aabb",
        "pattern:",
        "pattern:f",
        "pattern:abc",
        "pattern:zz",
        "pattern:0g",
        "0123456789abcdef0123456789abcdef",
    ] {
        let (returned, fill) = negotiated(Some(descriptor));
        assert_eq!(
            returned.as_deref(),
            Some(crate::fill::DEFAULT_FILL_DESCRIPTOR),
            "{descriptor}"
        );
        assert_eq!(fill, FillMode::default_fill(), "{descriptor}");
    }
}

#[test]
fn a_no_test_open_negotiates_fill_exactly_as_a_normal_open_does() {
    // Same negotiation, no session, and therefore no effective mode to retain.
    assert_eq!(no_test_negotiated(None), None);
    assert_eq!(no_test_negotiated(Some("")).as_deref(), Some(""));
    assert_eq!(
        no_test_negotiated(Some("pattern:AaBb")).as_deref(),
        Some("pattern:AaBb")
    );
    assert_eq!(no_test_negotiated(Some("none")).as_deref(), Some("none"));
    assert_eq!(
        no_test_negotiated(Some("pattern:zz")).as_deref(),
        Some(crate::fill::DEFAULT_FILL_DESCRIPTOR)
    );
}
