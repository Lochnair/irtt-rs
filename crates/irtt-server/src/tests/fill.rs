//! Server fill: what an open reply reports, what the session will fill with,
//! and the bytes an echo reply actually carries.
//!
//! The first two are deliberately different questions. A client that expressed
//! no preference keeps its absent or empty descriptor in the negotiated
//! parameters while the session uses this server's default fill, and only a
//! descriptor the server could not honor is rewritten. Asserting the reply alone
//! would not separate those, so the negotiation tests read the session's
//! effective mode too, and the payload tests then pin the bytes on the wire.
//!
//! Payload lengths here are derived from `irtt-proto`'s own layout rather than
//! written as offsets, and are deliberately not multiples of the pattern length.

use irtt_proto::{echo_header_len, Clock, Params, ReceivedStats, ServerFill, StampAt};

use super::support::{
    client_params, core_for, core_with_tokens, echo_params, echo_request, expect_echo_reply,
    expect_no_test_reply, expect_normal_open_reply, no_test_request, open_negotiated, open_request,
    other_peer, peer, unthrottled, ScriptedTokens, KEY,
};
use crate::{core::ServerCore, fill::FillMode};

const TOKEN_A: u64 = 0x0102_0304_0506_0708;
const TOKEN_B: u64 = 0x1112_1314_1516_1718;

/// Request payload bytes an echo carries, so a reply reflecting them would be
/// obvious.
const REQUEST_FILLER: u8 = 0xa5;

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

/// Params for a session requesting `descriptor` whose echo replies have exactly
/// `payload_len` payload bytes.
///
/// The negotiated length is the mandatory field block plus the region wanted,
/// asked of `irtt-proto` rather than written as a constant: the block grows with
/// the negotiated statistics, the timestamps and authentication's 16 bytes, and
/// a test that hard-coded an offset would silently drift from the encoder.
fn payload_params(descriptor: Option<&str>, payload_len: usize, hmac: bool) -> Params {
    let mut params = Params {
        server_fill: requesting(descriptor).server_fill,
        ..echo_params(ReceivedStats::None, StampAt::None, Clock::Unspecified, 0)
    };
    params.length = (echo_header_len(hmac, &params) + payload_len) as i64;
    params
}

/// The payload regions of `count` successive echo replies on one session
/// requesting `descriptor`.
///
/// Every request carries [`REQUEST_FILLER`] payload bytes, so a reply that
/// reflected its request would be visible in any of these tests rather than only
/// in the one about it.
fn echo_payloads(
    descriptor: Option<&str>,
    payload_len: usize,
    hmac_key: Option<&[u8]>,
    count: u32,
) -> Vec<Vec<u8>> {
    let requested = payload_params(descriptor, payload_len, hmac_key.is_some());
    let mut core = core_for(hmac_key, ScriptedTokens::new([TOKEN_A]));
    let (token, negotiated) = open_negotiated(&mut core, peer(), &requested, hmac_key);

    // The request fills its own payload region, which is the same size as the
    // reply's: the strongest shape for "a reply never reflects its request".
    let request_payload = vec![REQUEST_FILLER; payload_len];
    (0..count)
        .map(|sequence| {
            let request = echo_request(token, sequence, &negotiated, &request_payload, hmac_key);
            let packet = core
                .handle_datagram(peer(), &request)
                .unwrap()
                .expect("an admissible echo must be answered");
            let reply = expect_echo_reply(&packet, &negotiated, hmac_key);
            assert_eq!(
                packet.bytes().len() as i64,
                negotiated.length,
                "a reply is exactly the negotiated length"
            );
            assert_eq!(reply.payload.len(), payload_len, "payload region length");
            reply.payload
        })
        .collect()
}

/// The payload region of one echo reply on a session requesting `descriptor`.
fn echo_payload(descriptor: Option<&str>, payload_len: usize) -> Vec<u8> {
    echo_payloads(descriptor, payload_len, None, 1).remove(0)
}

/// `pattern` repeated to `len` bytes, starting at its first byte.
fn repeated(pattern: &[u8], len: usize) -> Vec<u8> {
    pattern.iter().copied().cycle().take(len).collect()
}

#[test]
fn no_preference_fills_with_the_default_pattern() {
    // The wire consequence of the negotiation test above: absent descriptor,
    // default bytes. 7 is not a multiple of 4, so the last repeat is partial.
    assert_eq!(echo_payload(None, 7), b"irttirt".to_vec());
}

#[test]
fn an_empty_preference_fills_with_the_default_pattern() {
    assert_eq!(echo_payload(Some(""), 7), b"irttirt".to_vec());
}

#[test]
fn an_unknown_or_malformed_descriptor_fills_with_the_default_pattern() {
    for descriptor in ["bogus", "RAND", "pattern:", "pattern:abc", "pattern:zz"] {
        assert_eq!(
            echo_payload(Some(descriptor), 7),
            b"irttirt".to_vec(),
            "{descriptor}"
        );
    }
}

#[test]
fn no_fill_produces_a_zero_payload_and_never_the_request() {
    // `irtt-rs` policy, and a deliberate divergence: the clean evidence records
    // the reference server leaving this region as unspecified residual buffer
    // content — returning the requester's own payload, or fragments of earlier
    // traffic from another client. Zeroes disclose nothing.
    let payload = echo_payload(Some("none"), 7);
    assert_eq!(payload, vec![0; 7]);
    assert!(!payload.contains(&REQUEST_FILLER));
}

#[test]
fn patterns_repeat_from_their_first_byte() {
    for (descriptor, expected) in [
        ("pattern:00", &[0x00][..]),
        ("pattern:ff00", &[0xff, 0x00][..]),
        // The reply follows the bytes the descriptor decodes to, whatever case
        // it was written in.
        ("pattern:AaBb", &[0xaa, 0xbb][..]),
        ("pattern:69727474", b"irtt"),
    ] {
        assert_eq!(
            echo_payload(Some(descriptor), 7),
            repeated(expected, 7),
            "{descriptor}"
        );
    }
}

#[test]
fn a_pattern_restarts_for_every_reply() {
    // Deliberate `irtt-rs` policy. The clean evidence records the reference
    // implementation advancing one phase continuously across replies, sessions
    // and listeners, and states that payload phase carries no protocol meaning
    // and that a conforming client must not depend on it — so this server keeps
    // no fill state at all and every reply starts at byte zero.
    let payloads = echo_payloads(Some("pattern:010203"), 5, None, 2);
    assert_eq!(payloads[0], vec![0x01, 0x02, 0x03, 0x01, 0x02]);
    assert_eq!(
        payloads[1], payloads[0],
        "the phase resets rather than continuing at 03 01 02 03 01"
    );
}

#[test]
fn sessions_fill_independently() {
    // Interleaved on one core, so a shared cursor or a global phase would show
    // up as one session's bytes drifting into the other's.
    let mut core = core_with_tokens(unthrottled(), ScriptedTokens::new([TOKEN_A, TOKEN_B]));
    let a_requested = payload_params(Some("pattern:aabb"), 5, false);
    let b_requested = payload_params(Some("pattern:112233"), 5, false);
    let (a_token, a) = open_negotiated(&mut core, peer(), &a_requested, None);
    let (b_token, b) = open_negotiated(&mut core, other_peer(), &b_requested, None);

    let mut payload_of = |endpoint, token, sequence, params: &Params| {
        let packet = core
            .handle_datagram(endpoint, &echo_request(token, sequence, params, &[], None))
            .unwrap()
            .expect("an admissible echo must be answered");
        expect_echo_reply(&packet, params, None).payload
    };

    assert_eq!(
        payload_of(peer(), a_token, 0, &a),
        vec![0xaa, 0xbb, 0xaa, 0xbb, 0xaa]
    );
    assert_eq!(
        payload_of(other_peer(), b_token, 0, &b),
        vec![0x11, 0x22, 0x33, 0x11, 0x22]
    );
    assert_eq!(
        payload_of(peer(), a_token, 1, &a),
        vec![0xaa, 0xbb, 0xaa, 0xbb, 0xaa],
        "session A is unaffected by session B's reply"
    );
}

#[test]
fn a_random_session_fills_the_whole_region() {
    // Deterministic assertions only: the region is the negotiated one and the
    // reply decodes and authenticates. What the operating system's random
    // source returns is not a property this suite can assert without flipping a
    // coin, and `fill`'s unit tests cover propagation and the failure fallback.
    for hmac_key in [None, Some(KEY)] {
        // `echo_payloads` already asserts the datagram length and the region
        // length of each reply, and decoding verifies the MAC. Nothing is
        // asserted about the byte values, deliberately: a random payload
        // containing any particular byte is chance, not behavior.
        assert_eq!(echo_payloads(Some("rand"), 7, hmac_key, 2).len(), 2);
    }
}

#[test]
fn authentication_consumes_payload_space_at_the_same_negotiated_length() {
    // One negotiated length, two field blocks: authentication's 16 bytes come
    // out of the payload region, not out of the datagram. Every remaining
    // payload byte is still filled, and the authenticated reply's MAC covers
    // those bytes — `expect_echo_reply` verifies it while decoding.
    let unauthenticated = payload_params(Some("pattern:aa"), 48, false);
    let length = unauthenticated.length;
    let authenticated = Params {
        length,
        ..payload_params(Some("pattern:aa"), 0, true)
    };

    let plain = {
        let mut core = core_for(None, ScriptedTokens::new([TOKEN_A]));
        let (token, negotiated) = open_negotiated(&mut core, peer(), &unauthenticated, None);
        let packet = core
            .handle_datagram(peer(), &echo_request(token, 0, &negotiated, &[], None))
            .unwrap()
            .expect("an admissible echo must be answered");
        assert_eq!(packet.bytes().len() as i64, length);
        expect_echo_reply(&packet, &negotiated, None).payload
    };
    let signed = {
        let mut core = core_for(Some(KEY), ScriptedTokens::new([TOKEN_A]));
        let (token, negotiated) = open_negotiated(&mut core, peer(), &authenticated, Some(KEY));
        let packet = core
            .handle_datagram(peer(), &echo_request(token, 0, &negotiated, &[], Some(KEY)))
            .unwrap()
            .expect("an admissible echo must be answered");
        assert_eq!(packet.bytes().len() as i64, length);
        expect_echo_reply(&packet, &negotiated, Some(KEY)).payload
    };

    assert_eq!(
        signed.len() + irtt_proto::HMAC_SIZE,
        plain.len(),
        "authentication takes its 16 bytes from the payload region"
    );
    assert!(plain.iter().chain(&signed).all(|byte| *byte == 0xaa));
}
