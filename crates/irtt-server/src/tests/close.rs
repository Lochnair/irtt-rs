//! Client-initiated close.
//!
//! A close request that matches a live token *and* arrived from that session's
//! exact endpoint releases the session and is answered with nothing. Every
//! other close — unknown, stale or zero token, foreign endpoint, failed
//! authentication — is the same silence with the session left live, so these
//! tests assert the table as well as the reply.

use irtt_proto::{encode_request, RequestToEncode};

use super::support::{
    client_params, close_request, close_request_with_trailing, core_for, core_with_tokens,
    open_request, open_session, other_peer, peer, ScriptedTokens, KEY, OTHER_KEY,
};
use crate::ServerConfig;

const TOKEN_A: u64 = 0x0102_0304_0506_0708;
const TOKEN_B: u64 = 0x1112_1314_1516_1718;
const UNKNOWN_TOKEN: u64 = 0x2122_2324_2526_2728;

#[test]
fn a_valid_close_removes_the_session_and_answers_nothing() {
    for hmac_key in [None, Some(KEY)] {
        let tokens = ScriptedTokens::new([TOKEN_A]);
        let mut core = core_for(hmac_key, tokens.clone());
        let token = open_session(&mut core, peer(), hmac_key);
        assert_eq!(core.session_count(), 1);

        // Exactly `None`, not an empty or acknowledging packet: version 1 has
        // no close reply, so the client learns nothing about the outcome.
        assert_eq!(
            core.handle_datagram(peer(), &close_request(token, hmac_key))
                .unwrap(),
            None
        );

        assert_eq!(core.session_count(), 0);
        assert_eq!(core.session(token), None);
        assert_eq!(
            tokens.remaining(),
            0,
            "a close must not draw from the token source"
        );
    }
}

#[test]
fn a_repeated_close_is_a_stale_token_and_changes_nothing() {
    for hmac_key in [None, Some(KEY)] {
        let mut core = core_for(hmac_key, ScriptedTokens::new([TOKEN_A]));
        let token = open_session(&mut core, peer(), hmac_key);
        let close = close_request(token, hmac_key);

        assert_eq!(core.handle_datagram(peer(), &close).unwrap(), None);
        assert_eq!(core.handle_datagram(peer(), &close).unwrap(), None);

        assert_eq!(core.session_count(), 0, "nothing may be resurrected");
        assert_eq!(core.session(token), None);
    }
}

#[test]
fn an_echo_after_a_close_is_a_stale_token() {
    // Echo processing does not exist yet; the point here is that the token is
    // gone, so the request has no session to reach even once it does.
    for hmac_key in [None, Some(KEY)] {
        let mut core = core_for(hmac_key, ScriptedTokens::new([TOKEN_A]));
        let token = open_session(&mut core, peer(), hmac_key);
        let params = core.session(token).expect("session must exist").params();
        let echo = encode_request(
            RequestToEncode::Echo {
                token,
                sequence: 0,
                params,
                payload: &[],
            },
            hmac_key,
        )
        .unwrap();

        core.handle_datagram(peer(), &close_request(token, hmac_key))
            .unwrap();

        assert_eq!(core.handle_datagram(peer(), &echo).unwrap(), None);
        assert_eq!(core.session_count(), 0);
        assert_eq!(core.session(token), None);
    }
}

#[test]
fn a_close_from_any_other_endpoint_leaves_the_session_live() {
    // Endpoint identity is exact: family, address, UDP source port and IPv6
    // scope all count. Possessing the token is not authority to close.
    for (bound, foreign) in [
        ("198.51.100.7:41234", "198.51.100.7:41235"),
        ("198.51.100.7:41234", "198.51.100.9:41234"),
        ("198.51.100.7:41234", "[::ffff:198.51.100.7]:41234"),
        ("[2001:db8::1]:2112", "[2001:db8::2]:2112"),
        ("[fe80::1%3]:2112", "[fe80::1%4]:2112"),
    ] {
        let bound = bound.parse().unwrap();
        let foreign = foreign.parse().unwrap();
        let mut core = core_with_tokens(ServerConfig::default(), ScriptedTokens::new([TOKEN_A]));
        let token = open_session(&mut core, bound, None);
        let session = core.session(token).cloned().expect("session must exist");
        let close = close_request(token, None);

        assert_eq!(
            core.handle_datagram(foreign, &close).unwrap(),
            None,
            "a close from {foreign} must be silent"
        );
        assert_eq!(core.session_count(), 1);
        assert_eq!(
            core.session(token),
            Some(&session),
            "a close from {foreign} must not remove the session bound to {bound}"
        );

        // The same request from the session's own endpoint does close it, so
        // the request itself was never the reason for the silence.
        assert_eq!(core.handle_datagram(bound, &close).unwrap(), None);
        assert_eq!(core.session_count(), 0);
    }
}

#[test]
fn an_unknown_or_zero_token_closes_nothing() {
    // Zero is reserved for no-test replies and is never issued to a session, so
    // it can only ever miss the table.
    for (name, token) in [("an unknown token", UNKNOWN_TOKEN), ("a zero token", 0)] {
        let mut core = core_with_tokens(ServerConfig::default(), ScriptedTokens::new([TOKEN_A]));
        let live = open_session(&mut core, peer(), None);
        let session = core.session(live).cloned().expect("session must exist");

        assert_eq!(
            core.handle_datagram(peer(), &close_request(token, None))
                .unwrap(),
            None,
            "{name} must be answered with silence"
        );
        assert_eq!(core.session_count(), 1, "{name} must remove nothing");
        assert_eq!(core.session(live), Some(&session));
    }
}

#[test]
fn a_close_that_fails_authentication_leaves_the_session_live() {
    // Authentication runs before the session is looked up, so a close that
    // fails it is indistinguishable from one bearing an unknown token — and it
    // must not be able to tear down an authenticated session.
    let mut core = core_for(Some(KEY), ScriptedTokens::new([TOKEN_A]));
    let token = open_session(&mut core, peer(), Some(KEY));
    let session = core.session(token).cloned().expect("session must exist");

    let mut corrupted = close_request(token, Some(KEY));
    corrupted[5] ^= 0x80;

    for (name, packet) in [
        (
            "a close with no MAC field at all",
            close_request(token, None),
        ),
        (
            "a close signed with a different key",
            close_request(token, Some(OTHER_KEY)),
        ),
        ("a close with one bit flipped in the MAC", corrupted),
    ] {
        assert_eq!(
            core.handle_datagram(peer(), &packet).unwrap(),
            None,
            "{name} must be answered with silence"
        );
        assert_eq!(core.session_count(), 1, "{name} must not remove a session");
        assert_eq!(core.session(token), Some(&session));
    }
}

#[test]
fn a_non_authenticating_server_ignores_an_authenticated_close() {
    let mut core = core_with_tokens(ServerConfig::default(), ScriptedTokens::new([TOKEN_A]));
    let token = open_session(&mut core, peer(), None);
    let session = core.session(token).cloned().expect("session must exist");

    assert_eq!(
        core.handle_datagram(peer(), &close_request(token, Some(KEY)))
            .unwrap(),
        None
    );

    assert_eq!(core.session_count(), 1);
    assert_eq!(core.session(token), Some(&session));
}

#[test]
fn trailing_bytes_after_the_token_do_not_prevent_the_close() {
    for hmac_key in [None, Some(KEY)] {
        let mut core = core_for(hmac_key, ScriptedTokens::new([TOKEN_A]));
        let token = open_session(&mut core, peer(), hmac_key);

        let close = close_request_with_trailing(token, &[0xde, 0xad, 0xbe, 0xef], hmac_key);

        assert_eq!(core.handle_datagram(peer(), &close).unwrap(), None);
        assert_eq!(core.session_count(), 0);
    }
}

#[test]
fn a_close_removes_only_the_session_it_names() {
    // Repeated opens from one endpoint are independent sessions, so a close
    // must be resolved by token, never by endpoint.
    let mut core = core_with_tokens(
        ServerConfig::default(),
        ScriptedTokens::new([TOKEN_A, TOKEN_B]),
    );
    let first = open_session(&mut core, peer(), None);
    let second = open_session(&mut core, peer(), None);
    let survivor = core.session(second).cloned().expect("session must exist");
    assert_eq!(core.session_count(), 2);

    assert_eq!(
        core.handle_datagram(peer(), &close_request(first, None))
            .unwrap(),
        None
    );
    assert_eq!(core.session_count(), 1, "only one session may be released");
    assert_eq!(core.session(first), None);
    assert_eq!(core.session(second), Some(&survivor));

    assert_eq!(
        core.handle_datagram(peer(), &close_request(second, None))
            .unwrap(),
        None
    );
    assert_eq!(core.session_count(), 0);
}

#[test]
fn a_close_frees_session_capacity_immediately() {
    // The session table is the only record of how full the server is, so a
    // removal is reclaimed at once with no separate counter to drift.
    let tokens = ScriptedTokens::new([TOKEN_A, TOKEN_B]);
    let mut core = core_with_tokens(ServerConfig::default().with_max_sessions(1), tokens.clone());
    let request = open_request(&client_params(), None);

    let first = open_session(&mut core, peer(), None);
    assert_eq!(
        core.handle_datagram(other_peer(), &request).unwrap(),
        None,
        "the bound is reached, so the second open is refused"
    );
    assert_eq!(core.session_count(), 1);
    assert_eq!(
        tokens.remaining(),
        1,
        "a refused open must not commit a token"
    );

    core.handle_datagram(peer(), &close_request(first, None))
        .unwrap();
    assert_eq!(core.session_count(), 0);
    assert_eq!(
        tokens.remaining(),
        1,
        "a close must not draw from the token source"
    );

    let second = open_session(&mut core, other_peer(), None);
    assert_eq!(second, TOKEN_B);
    assert_ne!(second, 0);
    assert_eq!(core.session_count(), 1);
    assert_eq!(core.session(second).unwrap().peer(), other_peer());
}
