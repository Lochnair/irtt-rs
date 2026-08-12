//! Parameter restrictions the server applies to an open request.
//!
//! Every case drives a real open through the core and reads the restriction out
//! of the open reply, because the reply is the contract: a client enforces its
//! own restriction policy against exactly these values, and the server has to
//! honor what it returned.

use std::time::Duration;

use irtt_proto::Params;

use super::support::{
    client_params, core_with_tokens, expect_normal_open_reply, open_request, peer, ScriptedTokens,
};
use crate::ServerConfig;

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
