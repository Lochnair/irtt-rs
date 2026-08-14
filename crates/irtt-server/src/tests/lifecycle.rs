//! Session lifetime: idle expiry and the maximum-duration close.
//!
//! Time is a hand-moved monotonic clock, so every deadline here is exact and no
//! test sleeps, tolerates or races.
//!
//! Two of the behaviors pinned here are deliberate `irtt-rs` policy rather than
//! observed reference behavior, and each says so where it is tested: expiry is
//! immediate at the configured deadline with no reply of any kind, and the
//! deadline covers a session that has never carried an echo request. The clean
//! specification records the reference server's five-second grace, its final
//! lazy-release reply and its never-expiring unused sessions as upstream policy,
//! states that expiry is entirely a server's own business, and recommends
//! against reproducing the resource leak. The one interoperability constraint is
//! negative — a server must never *signal* expiry — and silence satisfies it.

use std::time::Duration;

use irtt_proto::{echo_header_len, Clock, Params, ReceivedStats, ServerFill, StampAt, FLAG_CLOSE};

use super::support::{
    close_request, core_with_sources, echo_at, echo_params, echo_request,
    expect_closing_echo_reply, expect_echo_reply, expect_no_test_reply, manual_core,
    no_test_request, open_request, other_peer, peer, ManualClock, ScriptedTokens, KEY,
};
use crate::{OutboundDatagram, ServerConfig};

const TOKEN_A: u64 = 0x0102_0304_0506_0708;
const TOKEN_B: u64 = 0x1112_1314_1516_1718;

const MS: i64 = 1_000_000;
const SECOND: i64 = 1_000_000_000;

/// Params reporting both statistics and requesting `interval_ns`.
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

/// A configuration that expires after `idle` and rate-limits nothing.
fn idle_after(idle: Duration) -> ServerConfig {
    ServerConfig::default()
        .with_idle_timeout(idle)
        .with_min_send_interval(Duration::ZERO)
}

fn count_of(reply: Option<OutboundDatagram>, params: &Params) -> Option<u32> {
    let reply = reply?;
    expect_echo_reply(reply, params, None).recv_count
}

#[test]
fn a_session_that_never_carried_an_echo_still_expires() {
    // `irtt-rs` policy. The reference server starts the idle clock at the first
    // echo request, so a session left unused never expires at all; a session is
    // created by one unauthenticated datagram and opens are never deduplicated,
    // which makes that an unbounded-state hazard rather than a compatibility
    // feature. Here the deadline runs from the open.
    //
    // A one-session table makes the reclamation externally visible: the second
    // open can only succeed if the first session was released before capacity
    // was judged.
    let config = idle_after(Duration::from_secs(2)).with_max_sessions(1);
    let clock = ManualClock::at(0);
    let mut core = core_with_sources(
        config,
        ScriptedTokens::new([TOKEN_A, TOKEN_B]),
        clock.clone(),
    );
    let requested = params_at(SECOND);

    clock.set(0);
    assert!(core
        .handle_datagram(peer(), &open_request(&requested, None))
        .unwrap()
        .is_some());
    assert_eq!(core.session_count(), 1);

    // Still inside the deadline: the table is full and a second open is refused.
    clock.set(2 * SECOND - 1);
    assert_eq!(
        core.handle_datagram(other_peer(), &open_request(&requested, None))
            .unwrap(),
        None,
        "a live session must still occupy the table"
    );
    assert_eq!(core.session_count(), 1);

    // At the deadline the unused session is reclaimed, before capacity is
    // judged, so the new open fits.
    clock.set(2 * SECOND);
    assert!(
        core.handle_datagram(other_peer(), &open_request(&requested, None))
            .unwrap()
            .is_some(),
        "an expired session must not deny capacity to a new one"
    );
    assert_eq!(core.session_count(), 1, "one replaced the other");

    // And the reclaimed token names nothing.
    assert!(
        echo_at(&mut core, &clock, 2 * SECOND, TOKEN_A, 0, &requested, None).is_none(),
        "the released token must be unusable"
    );
}

#[test]
fn maintenance_reclaims_an_idle_session_without_traffic() {
    let clock = ManualClock::at(0);
    let mut core = core_with_sources(
        idle_after(Duration::from_secs(2)),
        ScriptedTokens::new([TOKEN_A]),
        clock.clone(),
    );

    assert!(core
        .handle_datagram(peer(), &open_request(&params_at(SECOND), None))
        .unwrap()
        .is_some());
    assert_eq!(core.session_count(), 1);

    clock.set(2 * SECOND);
    core.maintain();
    assert_eq!(core.session_count(), 0);
}

#[test]
fn a_served_echo_moves_the_idle_deadline_along() {
    let (mut core, clock, token, negotiated) = manual_core(
        idle_after(Duration::from_secs(2)),
        &params_at(SECOND),
        ScriptedTokens::new([TOKEN_A]),
        None,
    );

    // Inside the deadline the open set, and past the one it would have had.
    assert_eq!(
        count_of(
            echo_at(&mut core, &clock, SECOND, token, 0, &negotiated, None),
            &negotiated
        ),
        Some(1)
    );
    assert_eq!(
        count_of(
            echo_at(&mut core, &clock, 2_900 * MS, token, 1, &negotiated, None),
            &negotiated
        ),
        Some(2),
        "the echo at 1 s moved the deadline from 2 s to 3 s"
    );

    // Exactly at the deadline the last served echo set: gone, and silently.
    assert!(
        echo_at(&mut core, &clock, 4_900 * MS, token, 2, &negotiated, None).is_none(),
        "the deadline is inclusive"
    );
    assert_eq!(core.session_count(), 0);
}

#[test]
fn an_expired_session_answers_nothing_at_all() {
    // `irtt-rs` policy, and the reason this is its own test. The reference
    // server was observed answering the first otherwise-serviceable echo to
    // reach an expired session — a final lazy-release reply — and only dropping
    // the one after it, with the release boundary a further five seconds past
    // the configured timeout. `irtt-rs` reproduces neither: at the deadline the
    // session is simply gone, and the client observes silence, which is the only
    // thing the specification actually requires of expiry.
    let (mut core, clock, token, negotiated) = manual_core(
        idle_after(Duration::from_secs(2)),
        &params_at(SECOND),
        ScriptedTokens::new([TOKEN_A]),
        None,
    );

    assert_eq!(
        count_of(
            echo_at(
                &mut core,
                &clock,
                2 * SECOND - 1,
                token,
                0,
                &negotiated,
                None
            ),
            &negotiated
        ),
        Some(1),
        "one nanosecond short of the deadline is still served"
    );

    // Two seconds after that echo, to the nanosecond.
    assert!(
        echo_at(
            &mut core,
            &clock,
            4 * SECOND - 1,
            token,
            1,
            &negotiated,
            None
        )
        .is_none(),
        "no final reply: an expired session is simply gone"
    );
    assert_eq!(core.session_count(), 0);
    assert!(
        echo_at(&mut core, &clock, 4 * SECOND, token, 2, &negotiated, None).is_none(),
        "and stays gone"
    );
}

#[test]
fn a_rate_limited_echo_keeps_its_session_alive_without_counting() {
    // The one drop class the clean evidence records as refreshing the idle
    // deadline. It was established against every other tested class, which does
    // not, so `irtt-rs` keeps it.
    //
    // A 2 s minimum interval against an 8 s idle timeout negotiates 2 s and
    // refills every 2 s; a burst of one then makes the echo at 8.5 s — 1.5 s
    // after the previous one — rate-limited.
    let config = ServerConfig::default()
        .with_min_send_interval(Duration::from_secs(2))
        .with_idle_timeout(Duration::from_secs(8))
        .with_burst_allowance(1);
    let (mut core, clock, token, negotiated) = manual_core(
        config,
        &params_at(2 * SECOND),
        ScriptedTokens::new([TOKEN_A]),
        None,
    );
    assert_eq!(negotiated.interval_ns, 2 * SECOND);

    // Served, spending the burst. The idle deadline is now 15 s.
    assert_eq!(
        count_of(
            echo_at(&mut core, &clock, 7 * SECOND, token, 0, &negotiated, None),
            &negotiated
        ),
        Some(1)
    );

    // Rate-limited, 1.5 s later: no reply, and the deadline moves to 16.5 s.
    assert!(
        echo_at(&mut core, &clock, 8_500 * MS, token, 1, &negotiated, None).is_none(),
        "no allowance, so no reply"
    );

    // Past the deadline the served echo alone would have set, and short of the
    // one the rate-limited echo set.
    assert_eq!(
        count_of(
            echo_at(&mut core, &clock, 16 * SECOND, token, 2, &negotiated, None),
            &negotiated
        ),
        Some(2),
        "the rate-limited echo kept the session alive, and was not counted"
    );
}

#[test]
fn a_rejected_echo_does_not_move_the_idle_deadline() {
    // Two representative classes; the admission rules themselves are pinned by
    // the echo and hmac tests, and are not repeated here.
    let config = idle_after(Duration::from_secs(2)).with_max_packet_length(64);
    let (mut core, clock, token, negotiated) = manual_core(
        config,
        &params_at(SECOND),
        ScriptedTokens::new([TOKEN_A]),
        None,
    );

    let oversized = echo_request(
        token,
        0,
        &Params {
            length: 65,
            ..negotiated.clone()
        },
        &[],
        None,
    );
    assert_eq!(oversized.len(), 65);

    clock.set(2 * SECOND - 1);
    assert_eq!(core.handle_datagram(peer(), &oversized).unwrap(), None);
    assert_eq!(
        core.handle_datagram(
            other_peer(),
            &echo_request(token, 0, &negotiated, &[], None)
        )
        .unwrap(),
        None,
        "a foreign endpoint is refused whatever the session's state"
    );
    assert_eq!(core.session_count(), 1, "neither released it early either");

    // The deadline the open set, unmoved.
    assert!(
        echo_at(&mut core, &clock, 2 * SECOND, token, 0, &negotiated, None).is_none(),
        "a rejected echo must not have extended the session"
    );
    assert_eq!(core.session_count(), 0);
}

#[test]
fn any_authenticated_request_reclaims_expired_sessions_including_a_no_test_open() {
    // `irtt-rs` policy. Expiry is a decision about the table, so it runs before
    // a request is dispatched and does not depend on what that request is for.
    // The reference server was observed releasing an expired session on a burst
    // of ordinary opens but not on no-test ones; a client cannot tell, since
    // expiry is never signalled, and the distinction is not worth reproducing.
    //
    // A no-test open still creates nothing of its own, which the token source
    // and the session count both check here.
    let tokens = ScriptedTokens::new([TOKEN_A]);
    let clock = ManualClock::at(0);
    let mut core = core_with_sources(
        idle_after(Duration::from_secs(2)),
        tokens.clone(),
        clock.clone(),
    );
    let requested = params_at(SECOND);

    assert!(core
        .handle_datagram(peer(), &open_request(&requested, None))
        .unwrap()
        .is_some());
    assert_eq!(core.session_count(), 1);
    assert_eq!(tokens.remaining(), 0);

    clock.set(2 * SECOND);
    let packet = core
        .handle_datagram(other_peer(), &no_test_request(&requested, None))
        .unwrap()
        .expect("a no-test open is answered whatever the table holds");
    expect_no_test_reply(&packet, None);

    assert_eq!(
        core.session_count(),
        0,
        "the expired session was reclaimed, and no-test created none of its own"
    );
}

#[test]
fn a_client_close_still_releases_its_session_without_a_reply() {
    // Unchanged by this slice, and worth pinning next to the close the *server*
    // sends: a client close is answered with nothing at all.
    let (mut core, clock, token, negotiated) = manual_core(
        idle_after(Duration::from_secs(60)),
        &params_at(SECOND),
        ScriptedTokens::new([TOKEN_A]),
        None,
    );
    assert!(echo_at(&mut core, &clock, SECOND, token, 0, &negotiated, None).is_some());

    clock.set(2 * SECOND);
    assert_eq!(
        core.handle_datagram(peer(), &close_request(token, None))
            .unwrap(),
        None
    );
    assert_eq!(core.session_count(), 0);
}

/// A configuration whose sessions run for `maximum` and are never idle-expired
/// within a test's timescale.
fn max_duration_of(maximum: Duration) -> ServerConfig {
    ServerConfig::default()
        .with_max_test_duration(maximum)
        .with_idle_timeout(Duration::from_secs(600))
}

#[test]
fn the_maximum_duration_deadline_starts_at_the_first_served_echo() {
    // Measured behavior: opening a session well before its first echo does not
    // move the deadline, and neither a rejected nor a rate-limited first request
    // starts it.
    let (mut core, clock, token, negotiated) = manual_core(
        max_duration_of(Duration::from_secs(1)),
        &params_at(SECOND),
        ScriptedTokens::new([TOKEN_A]),
        None,
    );
    assert_eq!(
        negotiated.duration_ns, SECOND,
        "the requested duration was reduced to the maximum"
    );

    // Ten seconds after the open — far past maximum + grace, had the open
    // started the clock.
    let packet = echo_at(&mut core, &clock, 10 * SECOND, token, 0, &negotiated, None)
        .expect("the first echo must be answered");
    let reply = expect_echo_reply(&packet, &negotiated, None);
    assert_eq!(reply.recv_count, Some(1));

    // The deadline is 13 s: this echo, three seconds after the open but two
    // before the origin's own deadline, is ordinary.
    let packet = echo_at(&mut core, &clock, 12_999 * MS, token, 1, &negotiated, None)
        .expect("an echo inside the deadline must be answered");
    assert_eq!(
        expect_echo_reply(&packet, &negotiated, None).recv_count,
        Some(2)
    );
    assert_eq!(core.session_count(), 1);
}

#[test]
fn the_first_echo_past_the_maximum_duration_carries_close_and_ends_the_session() {
    // The session negotiates a fill and a payload region, so this covers the
    // close's payload as well: a server close is an ordinary echo reply with a
    // flag added, and it fills exactly as the session's other replies do.
    let requested = Params {
        server_fill: Some(ServerFill {
            value: "pattern:aabb".to_owned(),
        }),
        length: (echo_header_len(false, &params_at(SECOND)) + 5) as i64,
        ..params_at(SECOND)
    };
    let patterned = vec![0xaa, 0xbb, 0xaa, 0xbb, 0xaa];
    let (mut core, clock, token, negotiated) = manual_core(
        max_duration_of(Duration::from_secs(1)),
        &requested,
        ScriptedTokens::new([TOKEN_A]),
        None,
    );

    assert!(echo_at(&mut core, &clock, 10 * SECOND, token, 0, &negotiated, None).is_some());
    // One nanosecond short of maximum + 2 s grace, measured from that echo.
    let packet = echo_at(
        &mut core,
        &clock,
        13 * SECOND - 1,
        token,
        1,
        &negotiated,
        None,
    )
    .expect("short of the deadline must be answered normally");
    let reply = expect_echo_reply(&packet, &negotiated, None);
    assert_eq!(reply.flags & FLAG_CLOSE, 0, "not yet");
    assert_eq!(reply.payload, patterned, "an ordinary reply's fill");

    // At the deadline exactly.
    let packet = echo_at(&mut core, &clock, 13 * SECOND, token, 2, &negotiated, None)
        .expect("the deadline-crossing echo is answered, with Close");
    let reply = expect_closing_echo_reply(&packet, &negotiated, None);
    assert_eq!(reply.token, token, "an ordinary reply in every other way");
    assert_eq!(reply.sequence, 2, "the triggering sequence, copied through");
    assert_eq!(
        (reply.recv_count, reply.recv_window),
        (Some(3), Some(0x7)),
        "the triggering request is included in the statistics it reports"
    );
    assert_eq!(
        reply.payload, patterned,
        "the close keeps the session's fill; it is not a special packet"
    );
    assert_eq!(
        packet.bytes().len(),
        irtt_proto::echo_packet_len(false, &negotiated).unwrap(),
        "the negotiated layout, unchanged by the flag"
    );
    assert_eq!(
        i64::from(packet.traffic_class()),
        negotiated.dscp,
        "a server close is an echo reply and keeps the session's marking"
    );

    // The token is unusable from that reply onward.
    assert_eq!(core.session_count(), 0);
    assert!(
        echo_at(
            &mut core,
            &clock,
            13 * SECOND + 1,
            token,
            3,
            &negotiated,
            None
        )
        .is_none(),
        "every later request is an unknown token"
    );
}

#[test]
fn a_rate_limited_echo_at_the_deadline_defers_the_close_rather_than_losing_it() {
    // The measured dropped-trigger case, and the reason rate allowance is
    // judged before the deadline: a deadline-crossing echo with no allowance is
    // silently dropped, the session stays usable, and the next echo that *is*
    // served carries the close.
    //
    // A 5 s minimum interval against a 600 s idle timeout negotiates 5 s and
    // refills every 5 s, so the echo three seconds after the first is limited.
    let config = max_duration_of(Duration::from_secs(1))
        .with_min_send_interval(Duration::from_secs(5))
        .with_burst_allowance(1);
    let (mut core, clock, token, negotiated) = manual_core(
        config,
        &params_at(5 * SECOND),
        ScriptedTokens::new([TOKEN_A]),
        None,
    );
    assert_eq!(negotiated.interval_ns, 5 * SECOND);

    assert_eq!(
        count_of(
            echo_at(&mut core, &clock, 10 * SECOND, token, 0, &negotiated, None),
            &negotiated
        ),
        Some(1),
        "the origin, which also spends the burst"
    );

    assert!(
        echo_at(&mut core, &clock, 13 * SECOND, token, 1, &negotiated, None).is_none(),
        "past the deadline but with no allowance: dropped, not closed"
    );
    assert_eq!(core.session_count(), 1, "and the session is still usable");

    // Two seconds later the allowance is back.
    let packet = echo_at(&mut core, &clock, 15 * SECOND, token, 2, &negotiated, None)
        .expect("the next served echo carries the close");
    let reply = expect_closing_echo_reply(&packet, &negotiated, None);
    assert_eq!(
        reply.recv_count,
        Some(2),
        "the rate-limited request was never counted"
    );
    assert_eq!(core.session_count(), 0);
}

#[test]
fn a_rejected_echo_past_the_deadline_neither_closes_nor_releases() {
    // The close trigger runs only after ordinary admission and rate allowance
    // succeed, so a datagram the server would refuse at any time refuses in the
    // ordinary way here too. The specification records the corresponding
    // upstream behavior as untested for classes other than rate limiting, so
    // this is the deterministic `irtt-rs` reading rather than an observed one.
    let (mut core, clock, token, negotiated) = manual_core(
        max_duration_of(Duration::from_secs(1)),
        &params_at(SECOND),
        ScriptedTokens::new([TOKEN_A]),
        None,
    );
    assert!(echo_at(&mut core, &clock, 10 * SECOND, token, 0, &negotiated, None).is_some());

    clock.set(14 * SECOND);
    assert_eq!(
        core.handle_datagram(
            other_peer(),
            &echo_request(token, 1, &negotiated, &[], None)
        )
        .unwrap(),
        None,
        "a foreign endpoint gets silence, not a close"
    );
    assert_eq!(core.session_count(), 1, "and does not release the session");

    let packet = echo_at(&mut core, &clock, 14_100 * MS, token, 2, &negotiated, None)
        .expect("the owning endpoint still receives the close");
    let reply = expect_closing_echo_reply(&packet, &negotiated, None);
    assert_eq!(
        reply.recv_count,
        Some(2),
        "the foreign request advanced nothing"
    );
    assert_eq!(core.session_count(), 0);
}

#[test]
fn an_authenticated_session_closes_through_the_ordinary_reply_path() {
    // No special close authentication exists, and none should: the flag rides on
    // a reply the normal encoder and key path produced.
    let config = max_duration_of(Duration::from_secs(1)).with_hmac_key(KEY);
    let (mut core, clock, token, negotiated) = manual_core(
        config,
        &params_at(SECOND),
        ScriptedTokens::new([TOKEN_A]),
        Some(KEY),
    );

    assert!(echo_at(
        &mut core,
        &clock,
        10 * SECOND,
        token,
        0,
        &negotiated,
        Some(KEY)
    )
    .is_some());

    let packet = echo_at(
        &mut core,
        &clock,
        13 * SECOND,
        token,
        1,
        &negotiated,
        Some(KEY),
    )
    .expect("the deadline-crossing echo is answered");
    // Decoding verifies the MAC, and the flag comparison pins HMAC and Close
    // together.
    let reply = expect_closing_echo_reply(&packet, &negotiated, Some(KEY));
    assert_eq!(reply.recv_count, Some(2));
    irtt_proto::verify_packet_hmac(KEY, packet.bytes())
        .expect("the close-flagged reply is authenticated");
    assert_eq!(core.session_count(), 0);
}
