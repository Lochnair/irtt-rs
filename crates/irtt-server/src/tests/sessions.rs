use super::support::{
    client_params, core_with_tokens, expect_normal_open_reply, open_request, other_peer, peer,
    ScriptedTokens,
};
use crate::{ServerConfig, DEFAULT_MAX_SESSIONS};

const TOKEN_A: u64 = 0x0102_0304_0506_0708;
const TOKEN_B: u64 = 0x1112_1314_1516_1718;
const TOKEN_C: u64 = 0x2122_2324_2526_2728;

#[test]
fn the_default_session_bound_is_finite() {
    let bound = ServerConfig::default().max_sessions();

    assert_eq!(bound, DEFAULT_MAX_SESSIONS);
    assert!(bound > 0, "the default bound must admit sessions");
    assert!(
        bound < usize::MAX,
        "there is deliberately no unlimited default"
    );
}

#[test]
fn repeated_opens_from_one_endpoint_create_independent_sessions() {
    // Opens are never deduplicated by endpoint or by parameters: a client
    // retransmission legitimately produces a second, independent session.
    let mut core = core_with_tokens(
        ServerConfig::default(),
        ScriptedTokens::new([TOKEN_A, TOKEN_B]),
    );
    let request = open_request(&client_params(), None);

    let first = core
        .handle_datagram(peer(), &request)
        .unwrap()
        .expect("first open must be answered");
    let second = core
        .handle_datagram(peer(), &request)
        .unwrap()
        .expect("second open must be answered");

    let first = expect_normal_open_reply(&first, None);
    let second = expect_normal_open_reply(&second, None);

    assert_ne!(first.token, 0);
    assert_ne!(second.token, 0);
    assert_ne!(first.token, second.token);
    assert_eq!(core.session_count(), 2);
    assert_eq!(core.session(first.token).unwrap().peer(), peer());
    assert_eq!(core.session(second.token).unwrap().peer(), peer());
}

#[test]
fn a_full_session_table_drops_new_opens_without_evicting_anything() {
    let tokens = ScriptedTokens::new([TOKEN_A, TOKEN_B, TOKEN_C]);
    let mut core = core_with_tokens(ServerConfig::default().with_max_sessions(2), tokens.clone());
    let request = open_request(&client_params(), None);

    core.handle_datagram(peer(), &request)
        .unwrap()
        .expect("first open must be answered");
    core.handle_datagram(other_peer(), &request)
        .unwrap()
        .expect("second open must be answered");
    assert_eq!(core.session_count(), 2);
    assert_eq!(core.session(TOKEN_A).unwrap().peer(), peer());
    assert_eq!(core.session(TOKEN_B).unwrap().peer(), other_peer());

    // Refusal is silence: the protocol has no rejection reply, so the client
    // simply sees an open timeout.
    assert_eq!(core.handle_datagram(peer(), &request).unwrap(), None);

    assert_eq!(core.session_count(), 2, "nothing may be evicted");
    assert_eq!(core.session(TOKEN_A).unwrap().peer(), peer());
    assert_eq!(core.session(TOKEN_B).unwrap().peer(), other_peer());
    assert_eq!(core.session(TOKEN_C), None);
    assert_eq!(
        tokens.remaining(),
        1,
        "a refused open must not commit a token"
    );
}

#[test]
fn a_zero_session_bound_refuses_every_session_creating_open() {
    let tokens = ScriptedTokens::new([TOKEN_A]);
    let mut core = core_with_tokens(ServerConfig::default().with_max_sessions(0), tokens.clone());

    assert_eq!(
        core.handle_datagram(peer(), &open_request(&client_params(), None))
            .unwrap(),
        None
    );
    assert_eq!(core.session_count(), 0);
    assert_eq!(tokens.remaining(), 1);
}
