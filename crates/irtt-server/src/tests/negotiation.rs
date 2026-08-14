//! Parameter restrictions the server applies to an open request.
//!
//! Every case drives a real open through the core and reads the restriction out
//! of the open reply, because the reply is the contract: a client enforces its
//! own restriction policy against exactly these values, and the server has to
//! honor what it returned.

use std::time::Duration;

use irtt_proto::{Clock, Params, StampAt};

use super::support::{
    client_params, core_with_tokens, echo_request, expect_echo_reply, expect_normal_open_reply,
    open_request, peer, unthrottled, ScriptedTokens,
};
use crate::{ServerConfig, TimestampAllowance};

const TOKEN_A: u64 = 0x2f45_9a01_c37d_6e88;

/// Negotiates one open against `config` and returns the restricted parameters.
fn negotiated(config: ServerConfig, requested: &Params) -> Params {
    let mut core = core_with_tokens(config, ScriptedTokens::new([TOKEN_A]));
    let packet = core
        .handle_datagram(peer(), &open_request(requested, None))
        .unwrap()
        .expect("the open must be answered");
    expect_normal_open_reply(&packet, None).params
}

fn requesting(duration_ns: i64, interval_ns: i64) -> Params {
    Params {
        duration_ns,
        interval_ns,
        ..client_params()
    }
}

#[test]
fn a_requested_duration_is_reduced_to_the_configured_maximum() {
    let config = || ServerConfig::default().with_max_test_duration(Duration::from_secs(2));

    assert_eq!(
        negotiated(config(), &requesting(10_000_000_000, 1_000_000_000)).duration_ns,
        2_000_000_000,
        "a longer duration is reduced to the maximum"
    );
    assert_eq!(
        negotiated(config(), &requesting(1_000_000_000, 1_000_000_000)).duration_ns,
        1_000_000_000,
        "a duration under the maximum is left alone"
    );
    // Duration zero is absent from the request, which means continuous. A
    // server with a finite maximum is restricting continuous mode to a finite
    // test, which is a reduction the client already models.
    assert_eq!(
        negotiated(config(), &requesting(0, 1_000_000_000)).duration_ns,
        2_000_000_000,
        "a continuous request takes the maximum"
    );
}

#[test]
fn without_a_configured_maximum_a_duration_is_never_restricted() {
    let config = || ServerConfig::default();

    assert_eq!(
        negotiated(config(), &requesting(86_400_000_000_000, 1_000_000_000)).duration_ns,
        86_400_000_000_000,
        "a day-long test is the operator's business, not negotiation's"
    );
    assert_eq!(
        negotiated(config(), &requesting(0, 1_000_000_000)).duration_ns,
        0,
        "continuous stays continuous"
    );

    // A zero maximum is stored as no maximum rather than negotiated: Duration
    // zero on the wire means continuous, so a finite maximum of zero could only
    // be expressed by turning a client's finite test into an endless one.
    let zero = ServerConfig::default().with_max_test_duration(Duration::ZERO);
    assert_eq!(zero.max_test_duration(), None);
    assert_eq!(
        negotiated(zero, &requesting(1_000_000_000, 1_000_000_000)).duration_ns,
        1_000_000_000
    );
}

#[test]
fn a_requested_interval_is_raised_to_the_configured_minimum() {
    let config = || ServerConfig::default().with_min_send_interval(Duration::from_millis(100));

    assert_eq!(
        negotiated(config(), &requesting(3_000_000_000, 20_000_000)).interval_ns,
        100_000_000,
        "a faster interval is raised to the minimum"
    );
    assert_eq!(
        negotiated(config(), &requesting(3_000_000_000, 500_000_000)).interval_ns,
        500_000_000,
        "an interval above the minimum is left alone"
    );
    // An absent Interval is the wire default zero, not a refusal — an explicit
    // zero is rejected during request admission and never reaches negotiation —
    // so it is raised like any other value below the floor.
    assert_eq!(
        negotiated(config(), &requesting(3_000_000_000, 0)).interval_ns,
        100_000_000,
        "an absent interval takes the minimum"
    );
}

#[test]
fn a_negotiated_interval_is_capped_at_a_quarter_of_the_idle_timeout() {
    // Without the cap, a client sending at the interval it was handed would
    // idle its own session out.
    let config = ServerConfig::default().with_idle_timeout(Duration::from_secs(8));

    assert_eq!(
        negotiated(config, &requesting(3_000_000_000, 5_000_000_000)).interval_ns,
        2_000_000_000
    );

    // A timeout whose quarter is past what the wire can express caps nothing,
    // because no representable interval reaches it. The quarter is taken before
    // the conversion to nanoseconds for exactly this reason: saturating first
    // would put the cap at a quarter of `i64::MAX` and reduce a long interval
    // the configured timeout never meant to touch.
    let absurd = ServerConfig::default().with_idle_timeout(Duration::MAX);
    assert_eq!(
        negotiated(absurd, &requesting(3_000_000_000, i64::MAX)).interval_ns,
        i64::MAX
    );
}

#[test]
fn the_idle_cap_wins_over_a_larger_configured_minimum_interval() {
    // The floor is applied first and the cap second, so a minimum above a
    // quarter of the idle timeout produces a negotiated interval below that
    // minimum. This is the configuration the specification records as an
    // upstream hazard: upstream returns 2 s here and still replenishes reply
    // allowance every 5 s, so a client obeying the reply is rate-limited.
    //
    // `irtt-rs` returns the same 2 s — the cap has to win, or the session
    // cannot stay alive — and refills at 2 s instead. `rate` proves the
    // enforcement half; this pins the value the client is told.
    let config = ServerConfig::default()
        .with_min_send_interval(Duration::from_secs(5))
        .with_idle_timeout(Duration::from_secs(8));

    assert_eq!(
        negotiated(config, &requesting(3_000_000_000, 10_000_000)).interval_ns,
        2_000_000_000
    );
}

#[test]
fn a_zero_idle_timeout_caps_nothing() {
    // A zero timeout expires a session at the first evaluation, so there is no
    // liveness to protect and a quarter of zero must not be read as a cap of
    // zero — which would negotiate an interval the client would reject.
    let config = ServerConfig::default()
        .with_idle_timeout(Duration::ZERO)
        .with_min_send_interval(Duration::from_millis(100));

    assert_eq!(
        negotiated(config, &requesting(3_000_000_000, 3_600_000_000_000)).interval_ns,
        3_600_000_000_000
    );
}

#[test]
fn the_default_configuration_restricts_neither_capability() {
    // Both controls are opt-in, so an existing configuration negotiates exactly
    // what it did before they existed.
    let config = ServerConfig::default();

    assert_eq!(config.timestamp_allowance(), TimestampAllowance::Dual);
    assert!(config.dscp_allowed());
    assert_eq!(TimestampAllowance::default(), TimestampAllowance::Dual);

    let restricted = config
        .clone()
        .with_timestamp_allowance(TimestampAllowance::Single)
        .with_dscp_allowed(false);
    assert_eq!(restricted.timestamp_allowance(), TimestampAllowance::Single);
    assert!(!restricted.dscp_allowed());
    assert_ne!(
        restricted, config,
        "the policy is part of the configuration"
    );
}

#[test]
fn a_requested_stamp_at_is_reduced_to_the_configured_allowance() {
    // The whole observed mapping, driven through real opens and read out of the
    // reply, because the reply is what a client enforces its own restriction
    // policy against. Only one row substitutes rather than removes: a request
    // for both instants under a single-timestamp allowance is answered with the
    // *midpoint*, which still describes both, rather than with whichever of the
    // two the server preferred.
    for (requested, allowance, expected) in [
        (StampAt::None, TimestampAllowance::Dual, StampAt::None),
        (StampAt::None, TimestampAllowance::Single, StampAt::None),
        (StampAt::None, TimestampAllowance::None, StampAt::None),
        (StampAt::Send, TimestampAllowance::Dual, StampAt::Send),
        (StampAt::Send, TimestampAllowance::Single, StampAt::Send),
        (StampAt::Send, TimestampAllowance::None, StampAt::None),
        (StampAt::Receive, TimestampAllowance::Dual, StampAt::Receive),
        (
            StampAt::Receive,
            TimestampAllowance::Single,
            StampAt::Receive,
        ),
        (StampAt::Receive, TimestampAllowance::None, StampAt::None),
        (StampAt::Both, TimestampAllowance::Dual, StampAt::Both),
        (StampAt::Both, TimestampAllowance::Single, StampAt::Midpoint),
        (StampAt::Both, TimestampAllowance::None, StampAt::None),
        (
            StampAt::Midpoint,
            TimestampAllowance::Dual,
            StampAt::Midpoint,
        ),
        (
            StampAt::Midpoint,
            TimestampAllowance::Single,
            StampAt::Midpoint,
        ),
        (StampAt::Midpoint, TimestampAllowance::None, StampAt::None),
    ] {
        let params = Params {
            stamp_at: requested,
            clock: Clock::Both,
            ..client_params()
        };
        let negotiated = negotiated(
            ServerConfig::default().with_timestamp_allowance(allowance),
            &params,
        );

        assert_eq!(
            negotiated.stamp_at, expected,
            "{requested:?} under {allowance:?}"
        );
        assert_eq!(
            negotiated.clock,
            Clock::Both,
            "{requested:?} under {allowance:?}: the allowance restricts placement, not the clock"
        );
    }
}

#[test]
fn a_timestamp_restriction_never_rewrites_the_requested_clock() {
    // The clean evidence describes a timestamp *allowance*, and says nothing
    // about restricting clock domains. A server that quietly changed the clock
    // too would report a session the client never asked for — and a client
    // enforcing strict negotiation would refuse an otherwise serviceable one.
    let single = negotiated(
        ServerConfig::default().with_timestamp_allowance(TimestampAllowance::Single),
        &Params {
            stamp_at: StampAt::Both,
            clock: Clock::Wall,
            ..client_params()
        },
    );
    assert_eq!(single.stamp_at, StampAt::Midpoint);
    assert_eq!(single.clock, Clock::Wall);

    let none = negotiated(
        ServerConfig::default().with_timestamp_allowance(TimestampAllowance::None),
        &Params {
            stamp_at: StampAt::Both,
            clock: Clock::Both,
            ..client_params()
        },
    );
    assert_eq!(none.stamp_at, StampAt::None);
    assert_eq!(
        none.clock,
        Clock::Both,
        "the clock tag stays exactly as requested, selecting nothing"
    );
}

#[test]
fn an_allowed_dscp_is_negotiated_stored_and_transported_unchanged() {
    // The default, and the behavioral compatibility this policy must not
    // disturb: the raw byte reaches the reply, the session and the transport.
    let requested = Params {
        dscp: 0xbb,
        ..client_params()
    };
    let mut core = core_with_tokens(unthrottled(), ScriptedTokens::new([TOKEN_A]));
    let open = core
        .handle_datagram(peer(), &open_request(&requested, None))
        .unwrap()
        .expect("the open must be answered");

    let negotiated = expect_normal_open_reply(&open, None).params;
    assert_eq!(negotiated.dscp, 0xbb);
    assert_eq!(
        core.session(TOKEN_A)
            .expect("the session must be live")
            .params()
            .dscp,
        0xbb
    );

    let reply = core
        .handle_datagram(peer(), &echo_request(TOKEN_A, 0, &negotiated, &[], None))
        .unwrap()
        .expect("an admissible echo must be answered");
    expect_echo_reply(&reply, &negotiated, None);
    assert_eq!(i64::from(reply.traffic_class()), 0xbb);
}

#[test]
fn a_disallowed_dscp_is_negotiated_to_zero_and_the_session_runs_unmarked() {
    // The open is not refused: the server simply will not provide the marking,
    // and says so in the value it returns. Nothing downstream needs a second
    // record of the policy — the negotiated zero is what the transport reads.
    let requested = Params {
        dscp: 0xb8,
        ..client_params()
    };
    let mut core = core_with_tokens(
        unthrottled().with_dscp_allowed(false),
        ScriptedTokens::new([TOKEN_A]),
    );
    let open = core
        .handle_datagram(peer(), &open_request(&requested, None))
        .unwrap()
        .expect("a disallowed DSCP must not refuse the open");

    let negotiated = expect_normal_open_reply(&open, None).params;
    assert_eq!(negotiated.dscp, 0, "restricted to zero");
    assert_eq!(
        core.session(TOKEN_A)
            .expect("the session must be live")
            .params()
            .dscp,
        0,
        "and stored as the value the session will be run with"
    );

    let reply = core
        .handle_datagram(peer(), &echo_request(TOKEN_A, 0, &negotiated, &[], None))
        .unwrap()
        .expect("an unmarked session is an ordinary session");
    expect_echo_reply(&reply, &negotiated, None);
    assert_eq!(reply.traffic_class(), 0);
}

#[test]
fn a_disallowed_dscp_restricts_a_value_no_socket_could_carry_too() {
    // Restriction runs during negotiation, before the transport's
    // "out-of-range means unmarked" fallback is relevant at all: these values
    // never reach the session, so the reply is honest rather than merely
    // harmless.
    for dscp in [-1, 256, i64::MAX] {
        let requested = Params {
            dscp,
            ..client_params()
        };
        let mut core = core_with_tokens(
            unthrottled().with_dscp_allowed(false),
            ScriptedTokens::new([TOKEN_A]),
        );
        let open = core
            .handle_datagram(peer(), &open_request(&requested, None))
            .unwrap()
            .unwrap_or_else(|| panic!("an open requesting DSCP {dscp} must still be answered"));

        let negotiated = expect_normal_open_reply(&open, None).params;
        assert_eq!(negotiated.dscp, 0, "requested {dscp}");
        assert_eq!(
            core.session(TOKEN_A)
                .expect("the session must be live")
                .params()
                .dscp,
            0,
            "requested {dscp}"
        );

        let reply = core
            .handle_datagram(peer(), &echo_request(TOKEN_A, 0, &negotiated, &[], None))
            .unwrap()
            .unwrap_or_else(|| panic!("the session must serve echoes after restricting {dscp}"));
        expect_echo_reply(&reply, &negotiated, None);
        assert_eq!(reply.traffic_class(), 0, "requested {dscp}");
    }
}
