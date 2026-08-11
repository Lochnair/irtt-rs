use irtt_proto::{verify_packet_hmac, FLAG_HMAC};

use super::support::{
    client_params, core_with_tokens, expect_normal_open_reply, open_request,
    open_request_with_raw_params, param_int, peer, ScriptedTokens, KEY, OTHER_KEY,
};
use crate::ServerConfig;

const TOKEN_A: u64 = 0x0102_0304_0506_0708;

/// Asserts a datagram is silently discarded, leaving no trace at all: no reply,
/// no session, and no session token drawn.
fn assert_discarded(config: ServerConfig, name: &str, packet: &[u8]) {
    let tokens = ScriptedTokens::new([TOKEN_A]);
    let mut core = core_with_tokens(config, tokens.clone());

    let reply = core
        .handle_datagram(peer(), packet)
        .expect("a rejected datagram is not a server error");

    assert!(reply.is_none(), "{name} must be answered with silence");
    assert_eq!(core.session_count(), 0, "{name} must create no session");
    assert_eq!(
        tokens.remaining(),
        1,
        "{name} must not consume session capacity or a token"
    );
}

#[test]
fn an_authenticating_server_discards_everything_that_does_not_authenticate() {
    let config = ServerConfig::default().with_hmac_key(KEY);

    assert_discarded(
        config.clone(),
        "open with no MAC field at all",
        &open_request(&client_params(), None),
    );
    assert_discarded(
        config.clone(),
        "open signed with a different key",
        &open_request(&client_params(), Some(OTHER_KEY)),
    );

    let mut corrupted = open_request(&client_params(), Some(KEY));
    corrupted[5] ^= 0x80;
    assert_discarded(
        config.clone(),
        "open with one bit flipped in the MAC",
        &corrupted,
    );

    let mut zeroed = open_request(&client_params(), Some(KEY));
    zeroed[4..20].fill(0);
    assert_discarded(config.clone(), "open with an all-zero MAC", &zeroed);

    let mut truncated = open_request(&client_params(), Some(KEY));
    truncated.drain(4..12);
    assert_discarded(
        config.clone(),
        "open with a truncated MAC field",
        &truncated,
    );

    let mut flag_cleared = open_request(&client_params(), Some(KEY));
    flag_cleared[3] &= !FLAG_HMAC;
    assert_discarded(
        config,
        "open with a valid MAC but the HMAC flag cleared",
        &flag_cleared,
    );
}

#[test]
fn a_non_authenticating_server_discards_authenticated_requests() {
    assert_discarded(
        ServerConfig::default(),
        "authenticated open to a server with no key",
        &open_request(&client_params(), Some(KEY)),
    );
}

#[test]
fn an_authenticating_server_answers_a_valid_open_with_an_authenticated_reply() {
    let mut core = core_with_tokens(
        ServerConfig::default().with_hmac_key(KEY),
        ScriptedTokens::new([TOKEN_A]),
    );

    let packet = core
        .handle_datagram(peer(), &open_request(&client_params(), Some(KEY)))
        .unwrap()
        .expect("a correctly authenticated open must be answered");

    let reply = expect_normal_open_reply(&packet, Some(KEY));
    assert_eq!(reply.token, TOKEN_A);
    assert_ne!(packet[3] & FLAG_HMAC, 0, "the reply must set FLAG_HMAC");
    verify_packet_hmac(KEY, &packet).expect("the reply must carry a valid MAC");
    assert_eq!(core.session_count(), 1);
}

#[test]
fn a_non_authenticating_server_answers_an_ordinary_open() {
    let mut core = core_with_tokens(ServerConfig::default(), ScriptedTokens::new([TOKEN_A]));

    let packet = core
        .handle_datagram(peer(), &open_request(&client_params(), None))
        .unwrap()
        .expect("an unauthenticated open must be answered");

    expect_normal_open_reply(&packet, None);
    assert_eq!(packet[3] & FLAG_HMAC, 0);
    assert_eq!(core.session_count(), 1);
}

#[test]
fn authentication_is_checked_before_parameters_are_decoded() {
    // Both of these are dropped, and neither reveals which stage rejected it.
    // The pairing is the point: bad parameters under a valid MAC still drop,
    // and a bad MAC drops without the parameters ever being parsed.
    let config = ServerConfig::default().with_hmac_key(KEY);
    let bad_params = param_int(2, 0);

    assert_discarded(
        config.clone(),
        "authenticated open with an explicit zero duration",
        &open_request_with_raw_params(&bad_params, Some(KEY)),
    );
    assert_discarded(
        config,
        "wrongly keyed open with an explicit zero duration",
        &open_request_with_raw_params(&bad_params, Some(OTHER_KEY)),
    );
}

#[test]
fn a_failed_authentication_leaves_a_live_session_untouched() {
    let mut core = core_with_tokens(
        ServerConfig::default().with_hmac_key(KEY),
        ScriptedTokens::new([TOKEN_A]),
    );
    core.handle_datagram(peer(), &open_request(&client_params(), Some(KEY)))
        .unwrap()
        .expect("the session-creating open must succeed");
    let session = core.session(TOKEN_A).cloned().expect("session must exist");

    for bad in [
        open_request(&client_params(), None),
        open_request(&client_params(), Some(OTHER_KEY)),
    ] {
        assert_eq!(core.handle_datagram(peer(), &bad).unwrap(), None);
    }

    assert_eq!(core.session_count(), 1);
    assert_eq!(core.session(TOKEN_A), Some(&session));
}
