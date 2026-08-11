use super::support::{
    client_params, core_with_tokens, expect_normal_open_reply, open_request, peer, ScriptedTokens,
};
use crate::{token::TOKEN_ATTEMPTS, ServerConfig, ServerError};

const TOKEN_A: u64 = 0x0102_0304_0506_0708;
const TOKEN_B: u64 = 0x1112_1314_1516_1718;

#[test]
fn a_zero_draw_is_discarded_and_redrawn() {
    // Zero is reserved for no-test replies and is never issued to a session.
    let mut core = core_with_tokens(
        ServerConfig::default(),
        ScriptedTokens::new([0, 0, TOKEN_A]),
    );

    let packet = core
        .handle_datagram(peer(), &open_request(&client_params(), None))
        .unwrap()
        .expect("a zero draw must be retried, not surfaced");

    assert_eq!(expect_normal_open_reply(&packet, None).token, TOKEN_A);
    assert_eq!(core.session_count(), 1);
}

#[test]
fn a_colliding_draw_is_discarded_and_never_overwrites_the_live_session() {
    let mut core = core_with_tokens(
        ServerConfig::default(),
        ScriptedTokens::new([TOKEN_A, TOKEN_A, TOKEN_B]),
    );
    let request = open_request(&client_params(), None);

    core.handle_datagram(peer(), &request)
        .unwrap()
        .expect("first open must be answered");
    let first = core.session(TOKEN_A).cloned().expect("session must exist");

    let packet = core
        .handle_datagram(peer(), &request)
        .unwrap()
        .expect("a collision must be retried, not surfaced");

    assert_eq!(expect_normal_open_reply(&packet, None).token, TOKEN_B);
    assert_eq!(core.session_count(), 2);
    assert_eq!(
        core.session(TOKEN_A),
        Some(&first),
        "the colliding token's session must be untouched"
    );
}

#[test]
fn exhausting_the_retry_budget_is_an_internal_error_that_creates_nothing() {
    let mut core = core_with_tokens(
        ServerConfig::default(),
        ScriptedTokens::new([TOKEN_A, 0, 0, 0, 0, 0, 0, 0, 0]),
    );
    let request = open_request(&client_params(), None);

    core.handle_datagram(peer(), &request)
        .unwrap()
        .expect("first open must be answered");
    let first = core.session(TOKEN_A).cloned().expect("session must exist");

    assert_eq!(
        core.handle_datagram(peer(), &request),
        Err(ServerError::TokenExhausted {
            attempts: TOKEN_ATTEMPTS
        })
    );

    assert_eq!(core.session_count(), 1, "no session may be half-created");
    assert_eq!(core.session(TOKEN_A), Some(&first));
}

#[test]
fn a_failing_random_source_is_reported_and_creates_nothing() {
    let mut core = core_with_tokens(ServerConfig::default(), ScriptedTokens::failing());

    assert!(matches!(
        core.handle_datagram(peer(), &open_request(&client_params(), None)),
        Err(ServerError::RandomSource { .. })
    ));
    assert_eq!(core.session_count(), 0);
}
