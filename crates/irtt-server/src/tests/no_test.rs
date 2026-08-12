use irtt_proto::{verify_packet_hmac, Clock, Params, StampAt};

use super::support::{
    client_params, core_with_tokens, expect_no_test_reply, no_test_request, open_request,
    open_request_with_raw_params, param_int, peer, unthrottled, ScriptedTokens, KEY,
};
use crate::ServerConfig;

const TOKEN_A: u64 = 0x0102_0304_0506_0708;
const TOKEN_B: u64 = 0x1112_1314_1516_1718;

#[test]
fn a_no_test_open_replies_with_a_zero_token_and_creates_nothing() {
    let tokens = ScriptedTokens::new([TOKEN_A]);
    let mut core = core_with_tokens(ServerConfig::default(), tokens.clone());

    let packet = core
        .handle_datagram(peer(), &no_test_request(&client_params(), None))
        .unwrap()
        .expect("a no-test open must be answered");

    let reply = expect_no_test_reply(&packet, None);
    assert_eq!(
        reply.params,
        Params {
            protocol_version: 1,
            ..client_params()
        }
    );
    assert_eq!(core.session_count(), 0, "no-test creates no session");
    assert_eq!(
        tokens.remaining(),
        1,
        "no-test must not draw a session token"
    );
}

#[test]
fn a_no_test_open_negotiates_exactly_as_a_normal_open_does() {
    let requested = client_params();

    let mut normal_core = core_with_tokens(ServerConfig::default(), ScriptedTokens::new([TOKEN_A]));
    let normal = normal_core
        .handle_datagram(peer(), &open_request(&requested, None))
        .unwrap()
        .expect("normal open must be answered");

    let mut no_test_core =
        core_with_tokens(ServerConfig::default(), ScriptedTokens::new([TOKEN_A]));
    let no_test = no_test_core
        .handle_datagram(peer(), &no_test_request(&requested, None))
        .unwrap()
        .expect("no-test open must be answered");

    assert_eq!(
        expect_no_test_reply(&no_test, None).params,
        super::support::decode_reply(&normal, None).params
    );
}

#[test]
fn a_no_test_open_is_rejected_on_the_same_parameters_a_normal_open_is() {
    let mut core = core_with_tokens(ServerConfig::default(), ScriptedTokens::new([TOKEN_A]));

    let request = {
        let mut packet = no_test_request(&client_params(), None);
        // Replace the parameter payload with an explicit zero duration.
        packet.truncate(4);
        packet.extend_from_slice(&param_int(2, 0));
        packet
    };

    assert_eq!(core.handle_datagram(peer(), &request).unwrap(), None);
    assert_eq!(core.session_count(), 0);
}

#[test]
fn a_no_test_open_selecting_timestamps_without_a_clock_is_silently_refused() {
    // No-test exists to tell a client what the session would be, so it must not
    // report a session that could not exist. The effective-parameter check runs
    // before the no-test path branches off, so both open forms refuse the same
    // incoherent timestamp request.
    let tokens = ScriptedTokens::new([TOKEN_A]);
    let mut core = core_with_tokens(ServerConfig::default(), tokens.clone());

    let requested = Params {
        clock: Clock::Unspecified,
        stamp_at: StampAt::Both,
        ..client_params()
    };

    assert_eq!(
        core.handle_datagram(peer(), &no_test_request(&requested, None))
            .expect("an unusable timestamp request is not a server error"),
        None
    );
    assert_eq!(core.session_count(), 0);
    assert_eq!(tokens.remaining(), 1, "no token may be drawn");
}

#[test]
fn a_no_test_open_reports_the_reduced_packet_length() {
    // No-test answers what the session *would* be, so it has to report the same
    // reduced length a session-creating open would enforce — while still
    // creating nothing and drawing no token.
    let tokens = ScriptedTokens::new([TOKEN_A]);
    let mut core = core_with_tokens(
        ServerConfig::default().with_max_packet_length(64),
        tokens.clone(),
    );
    let requested = Params {
        length: 1000,
        ..client_params()
    };

    let packet = core
        .handle_datagram(peer(), &no_test_request(&requested, None))
        .unwrap()
        .expect("a reducible length must still be answered");

    assert_eq!(expect_no_test_reply(&packet, None).params.length, 64);
    assert_eq!(core.session_count(), 0);
    assert_eq!(tokens.remaining(), 1, "no-test must not draw a token");
}

#[test]
fn a_no_test_open_is_refused_when_its_echo_field_block_exceeds_the_maximum() {
    // A maximum below the mandatory echo block describes a session that could
    // not exist at all, so no-test must not report one. Both open forms run the
    // same effective-parameter check before no-test branches off.
    let tokens = ScriptedTokens::new([TOKEN_A]);
    let mut core = core_with_tokens(
        ServerConfig::default().with_max_packet_length(0),
        tokens.clone(),
    );

    assert_eq!(
        core.handle_datagram(peer(), &no_test_request(&client_params(), None))
            .expect("an unservable packet size is not a server error"),
        None
    );
    assert_eq!(core.session_count(), 0);
    assert_eq!(tokens.remaining(), 1, "no token may be drawn");
}

#[test]
fn a_no_test_open_is_still_answered_when_no_session_could_be_created() {
    // No-test consumes no capacity, so it must keep working exactly where a
    // session-creating open would be refused.
    let mut refuses_everything = core_with_tokens(
        ServerConfig::default().with_max_sessions(0),
        ScriptedTokens::new([TOKEN_A]),
    );
    assert_eq!(
        refuses_everything
            .handle_datagram(peer(), &open_request(&client_params(), None))
            .unwrap(),
        None,
        "max_sessions 0 refuses a session-creating open"
    );
    let packet = refuses_everything
        .handle_datagram(peer(), &no_test_request(&client_params(), None))
        .unwrap()
        .expect("no-test must be served at max_sessions 0");
    expect_no_test_reply(&packet, None);

    let mut full = core_with_tokens(
        ServerConfig::default().with_max_sessions(1),
        ScriptedTokens::new([TOKEN_A, TOKEN_B]),
    );
    full.handle_datagram(peer(), &open_request(&client_params(), None))
        .unwrap()
        .expect("the first open fills the table");
    assert_eq!(full.session_count(), 1);

    let packet = full
        .handle_datagram(peer(), &no_test_request(&client_params(), None))
        .unwrap()
        .expect("no-test must be served with a full session table");
    expect_no_test_reply(&packet, None);
    assert_eq!(full.session_count(), 1, "no-test disturbed the table");
}

#[test]
fn an_authenticated_no_test_reply_is_authenticated_and_still_creates_nothing() {
    let mut core = core_with_tokens(
        ServerConfig::default().with_hmac_key(KEY),
        ScriptedTokens::new([TOKEN_A]),
    );

    let packet = core
        .handle_datagram(peer(), &no_test_request(&client_params(), Some(KEY)))
        .unwrap()
        .expect("an authenticated no-test open must be answered");

    expect_no_test_reply(&packet, Some(KEY));
    verify_packet_hmac(KEY, &packet).expect("the no-test reply must carry a valid MAC");
    assert_eq!(core.session_count(), 0);
}

#[test]
fn an_empty_no_test_open_is_answered() {
    // No interval floor, so the reply shows the wire defaults an empty payload
    // decoded to rather than the timing policy negotiation would apply to them.
    let mut core = core_with_tokens(unthrottled(), ScriptedTokens::new([TOKEN_A]));

    let mut request = open_request_with_raw_params(&[], None);
    request[3] |= irtt_proto::FLAG_CLOSE;

    let packet = core
        .handle_datagram(peer(), &request)
        .unwrap()
        .expect("an empty no-test open must be answered");

    let reply = expect_no_test_reply(&packet, None);
    assert_eq!(
        reply.params,
        Params {
            protocol_version: 1,
            ..Params::default()
        }
    );
    assert_eq!(core.session_count(), 0);
}
