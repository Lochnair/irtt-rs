//! Per-session reply allowance.
//!
//! The behavior pinned here is the clean specification's Section 9.3: a session
//! starts with its full burst, spends one unit per echo it is answered,
//! replenishes at one unit per refill interval, and drops an echo arriving with
//! no allowance without a reply and without advancing either reception
//! statistic. That last part is what the counts in the *later* replies prove.
//!
//! The token bucket itself is a black-box inference about the reference server,
//! not a protocol requirement, so nothing here reaches into the limiter's
//! representation: every assertion is about which datagrams came back.

use std::time::Duration;

use irtt_proto::{Clock, Params, ReceivedStats, StampAt};

use super::support::{
    core_with_sources, echo_at, echo_params, echo_request, expect_echo_reply, manual_core,
    open_negotiated, other_peer, peer, ManualClock, ScriptedTokens,
};
use crate::ServerConfig;

const TOKEN_A: u64 = 0x0102_0304_0506_0708;
const TOKEN_B: u64 = 0x1112_1314_1516_1718;

const MS: i64 = 1_000_000;
const SECOND: i64 = 1_000_000_000;

/// Params reporting both statistics — the count is how a test tells a served
/// request from a dropped one — and requesting `interval_ns`.
fn params_at(interval_ns: i64) -> Params {
    Params {
        interval_ns,
        ..echo_params(
            ReceivedStats::Both,
            StampAt::None,
            Clock::Unspecified,
            /* length */ 0,
        )
    }
}

/// The received count a reply reported, or `None` if there was no reply.
fn count_of(packet: Option<Vec<u8>>, params: &Params) -> Option<u32> {
    let packet = packet?;
    expect_echo_reply(&packet, params, None).recv_count
}

#[test]
fn a_blast_is_answered_up_to_the_burst_and_then_replenishes() {
    // The measured case: a 100 ms minimum interval and a burst of 5 answered
    // exactly 5 of a blast of 12, and the next 3 requests after a 600 ms pause
    // were all answered, with the count running 1..5 and then 6, 7, 8.
    let config = ServerConfig::default()
        .with_min_send_interval(Duration::from_millis(100))
        .with_burst_allowance(5);
    let (mut core, clock, token, negotiated) = manual_core(
        config,
        &params_at(SECOND),
        ScriptedTokens::new([TOKEN_A]),
        None,
    );
    // The refill cadence is the configured minimum here, not the negotiated
    // interval: the ordinary case, where the minimum is the shorter of the two.
    assert_eq!(negotiated.interval_ns, SECOND);

    for sequence in 0..12 {
        let count = count_of(
            echo_at(&mut core, &clock, 0, token, sequence, &negotiated, None),
            &negotiated,
        );
        let expected = (sequence < 5).then_some(sequence + 1);
        assert_eq!(count, expected, "request {sequence} of the blast");
    }

    // A pause long enough to refill the whole burst, then three requests spaced
    // by one refill interval each. The counts continuing 6, 7, 8 is what proves
    // the seven dropped requests advanced nothing.
    for (offset, sequence, expected) in [(600, 12, 6), (700, 13, 7), (800, 14, 8)] {
        let count = count_of(
            echo_at(
                &mut core,
                &clock,
                offset * MS,
                token,
                sequence,
                &negotiated,
                None,
            ),
            &negotiated,
        );
        assert_eq!(count, Some(expected), "request at {offset} ms");
    }
}

#[test]
fn a_burst_of_one_answers_one_request_per_interval() {
    // The other measured case: with a burst of 1, a blast of 4 produced 1 reply.
    let config = ServerConfig::default()
        .with_min_send_interval(Duration::from_millis(100))
        .with_burst_allowance(1);
    let (mut core, clock, token, negotiated) = manual_core(
        config,
        &params_at(SECOND),
        ScriptedTokens::new([TOKEN_A]),
        None,
    );

    for sequence in 0..4 {
        let count = count_of(
            echo_at(&mut core, &clock, 0, token, sequence, &negotiated, None),
            &negotiated,
        );
        assert_eq!(count, (sequence == 0).then_some(1), "request {sequence}");
    }

    // Exactly one interval later the allowance is back, and the count says the
    // three dropped requests were never counted.
    let count = count_of(
        echo_at(&mut core, &clock, 100 * MS, token, 4, &negotiated, None),
        &negotiated,
    );
    assert_eq!(count, Some(2));
}

#[test]
fn the_refill_cadence_is_never_slower_than_the_negotiated_interval() {
    // The deliberate `irtt-rs` divergence, and the reason this test exists.
    //
    // A 5 s minimum interval against an 8 s idle timeout negotiates 2 s, because
    // the idle cap is applied after the floor. The clean specification records
    // the reference server continuing to replenish every 5 s in that
    // configuration, so a fully conforming client sending at the 2 s interval it
    // was handed is rate-limited on every request. `irtt-rs` refills at the
    // interval it advertised instead.
    let config = ServerConfig::default()
        .with_min_send_interval(Duration::from_secs(5))
        .with_idle_timeout(Duration::from_secs(8))
        .with_burst_allowance(1);
    let (mut core, clock, token, negotiated) = manual_core(
        config,
        &params_at(10 * MS),
        ScriptedTokens::new([TOKEN_A]),
        None,
    );
    assert_eq!(
        negotiated.interval_ns,
        2 * SECOND,
        "the idle cap wins over the larger configured minimum"
    );

    assert_eq!(
        count_of(
            echo_at(&mut core, &clock, 0, token, 0, &negotiated, None),
            &negotiated
        ),
        Some(1),
        "the first request spends the burst"
    );

    // One negotiated interval later — and well short of the 5 s the reference
    // server would have made this client wait.
    assert_eq!(
        count_of(
            echo_at(&mut core, &clock, 2 * SECOND, token, 1, &negotiated, None),
            &negotiated
        ),
        Some(2),
        "a client sending at the interval it was given must not be limited"
    );
    assert_eq!(
        count_of(
            echo_at(&mut core, &clock, 4 * SECOND, token, 2, &negotiated, None),
            &negotiated
        ),
        Some(3),
        "and must keep being answered at that cadence"
    );
}

#[test]
fn a_zero_burst_refuses_every_echo_and_a_zero_interval_refuses_none() {
    // The two zero cases are deliberately not the same. A zero burst is no
    // allowance at all; a zero interval is no waiting for allowance.
    let (mut core, clock, token, negotiated) = manual_core(
        ServerConfig::default().with_burst_allowance(0),
        &params_at(SECOND),
        ScriptedTokens::new([TOKEN_A]),
        None,
    );
    for (offset, sequence) in [(0, 0), (SECOND, 1), (60 * SECOND, 2)] {
        assert!(
            echo_at(
                &mut core,
                &clock,
                offset,
                token,
                sequence,
                &negotiated,
                None
            )
            .is_none(),
            "a zero burst answers nothing, however long the wait"
        );
    }

    let (mut core, clock, token, negotiated) = manual_core(
        ServerConfig::default().with_min_send_interval(Duration::ZERO),
        &params_at(SECOND),
        ScriptedTokens::new([TOKEN_A]),
        None,
    );
    for sequence in 0..12 {
        assert_eq!(
            count_of(
                echo_at(&mut core, &clock, 0, token, sequence, &negotiated, None),
                &negotiated
            ),
            Some(sequence + 1),
            "a zero interval never withholds allowance, whatever the burst"
        );
    }
}

#[test]
fn sibling_sessions_hold_their_allowance_independently() {
    let config = ServerConfig::default()
        .with_min_send_interval(Duration::from_millis(100))
        .with_burst_allowance(1);
    let clock = ManualClock::at(0);
    let mut core = core_with_sources(
        config,
        ScriptedTokens::new([TOKEN_A, TOKEN_B]),
        clock.clone(),
    );
    let requested = params_at(SECOND);
    let (token_a, negotiated) = open_negotiated(&mut core, peer(), &requested, None);
    let (token_b, _) = open_negotiated(&mut core, other_peer(), &requested, None);

    // The first session spends its whole allowance and is then limited.
    assert!(echo_at(&mut core, &clock, 0, token_a, 0, &negotiated, None).is_some());
    assert!(echo_at(&mut core, &clock, 0, token_a, 1, &negotiated, None).is_none());

    // The second still has its own, from its own endpoint.
    clock.set(0);
    let packet = core
        .handle_datagram(
            other_peer(),
            &echo_request(token_b, 0, &negotiated, &[], None),
        )
        .unwrap()
        .expect("a sibling session's allowance is its own");
    assert_eq!(
        expect_echo_reply(&packet, &negotiated, None).recv_count,
        Some(1)
    );
}
