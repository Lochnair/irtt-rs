//! Consuming a kernel-observed receive timestamp.
//!
//! A Linux listener reports the kernel's arrival time for each datagram, and an
//! echo reply may report it as the wall-clock instant the request was received.
//! These tests pin what that is allowed to change and — mostly — what it is
//! not: only `recv_wall`, only under [`StampAt::Receive`] and [`StampAt::Both`],
//! only on a clock that reports wall time, and never a monotonic value, a
//! midpoint, or any lifecycle, rate or deadline decision.
//!
//! Whether the transport actually captures a timestamp is settled by the socket
//! layer's own tests; nothing here recertifies `SCM_TIMESTAMPNS`. These drive
//! the core with fabricated values, so they run identically on every target.

use std::{
    net::SocketAddr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use irtt_proto::{Clock, Params, ReceivedStats, StampAt, TimestampFields};

use super::support::{
    close_request, core_with_sources, echo_at, echo_params, echo_request,
    expect_closing_echo_reply, expect_echo_reply, expect_normal_open_reply, manual_core,
    open_negotiated, open_request, peer, sample, ManualClock, ScriptedClock, ScriptedTokens,
};
use crate::{
    clock::ClockSample,
    core::{preferred_receive_wall_ns, OutboundDatagram, ServerCore, MAX_KERNEL_RX_LAG},
    ServerConfig,
};

const TOKEN_A: u64 = 0x0102_0304_0506_0708;
const MS: i64 = 1_000_000;
const SECOND: i64 = 1_000 * MS;

/// The lifecycle instant the session-creating open consumes.
fn open_sample() -> ClockSample {
    sample(900_000, 9_000)
}

/// The core's own receive reading for the echo every matrix test sends.
fn received() -> ClockSample {
    sample(1_000_000, 10_000)
}

/// The core's send reading for the same echo.
fn sent() -> ClockSample {
    sample(1_200_000, 10_300)
}

/// A kernel arrival reading 100 µs before [`received`], which is the ordinary
/// shape: earlier than the server's own sample and well inside the bound.
fn kernel_rx() -> SystemTime {
    at_epoch_ns(900_000)
}

/// The [`SystemTime`] `ns` nanoseconds from the Unix epoch, before it when
/// negative.
fn at_epoch_ns(ns: i64) -> SystemTime {
    let magnitude = Duration::from_nanos(ns.unsigned_abs());
    if ns.is_negative() {
        UNIX_EPOCH - magnitude
    } else {
        UNIX_EPOCH + magnitude
    }
}

/// Sends one echo carrying `kernel_rx_timestamp` through the runtime's entry
/// point and returns the reply's timestamp fields.
fn echo_timestamps(
    stamp_at: StampAt,
    clock: Clock,
    kernel_rx_timestamp: Option<SystemTime>,
) -> TimestampFields {
    let mut core = core_with_sources(
        ServerConfig::default(),
        ScriptedTokens::new([TOKEN_A]),
        ScriptedClock::new([open_sample(), received(), sent()]),
    );
    let params = echo_params(ReceivedStats::None, stamp_at, clock, /* length */ 0);
    let (token, negotiated) = open_negotiated(&mut core, peer(), &params, None);

    let packet = core
        .handle_received_datagram(
            peer(),
            &echo_request(token, 0, &negotiated, &[], None),
            kernel_rx_timestamp,
        )
        .unwrap()
        .unwrap_or_else(|| panic!("{stamp_at:?} on {clock:?} must be answered"));
    expect_echo_reply(&packet, &negotiated, None).timestamps
}

#[test]
fn the_preferred_receive_wall_is_the_kernel_reading_only_when_it_is_plausible() {
    let userspace = received();
    let userspace_wall = userspace.wall_ns;
    let bound_ns = i64::try_from(MAX_KERNEL_RX_LAG.as_nanos()).unwrap();

    // No observation at all: the server's own reading, which is the whole of
    // the pre-existing behavior and what every non-Linux target sees.
    assert_eq!(preferred_receive_wall_ns(None, userspace), userspace_wall);

    // Earlier, and inside the bound.
    assert_eq!(
        preferred_receive_wall_ns(Some(kernel_rx()), userspace),
        900_000
    );

    // Equal is accepted: the two clocks may simply have read the same
    // nanosecond.
    assert_eq!(
        preferred_receive_wall_ns(Some(at_epoch_ns(userspace_wall)), userspace),
        userspace_wall
    );

    // Later than the sample that observed the same datagram is impossible, so
    // it is metadata this server will not measure against. This is the shape a
    // backward wall step between the kernel's reading and the core's sample
    // produces.
    assert_eq!(
        preferred_receive_wall_ns(Some(at_epoch_ns(userspace_wall + 1)), userspace),
        userspace_wall
    );

    // The lag bound is inclusive.
    assert_eq!(
        preferred_receive_wall_ns(Some(at_epoch_ns(userspace_wall - bound_ns)), userspace),
        userspace_wall - bound_ns
    );
    assert_eq!(
        preferred_receive_wall_ns(Some(at_epoch_ns(userspace_wall - bound_ns - 1)), userspace),
        userspace_wall
    );

    // A pre-epoch reading is representable as a negative value and is judged
    // by exactly the same rule, so against a host clock set anywhere near the
    // present it simply lags far too much. Nothing here special-cases the sign.
    let present = sample(1_700_000_000 * SECOND, 10_000);
    assert_eq!(
        preferred_receive_wall_ns(Some(UNIX_EPOCH - Duration::from_nanos(1)), present),
        present.wall_ns
    );
    assert_eq!(
        preferred_receive_wall_ns(Some(at_epoch_ns(present.wall_ns - bound_ns)), present),
        present.wall_ns - bound_ns,
        "and a realistic wall clock accepts a reading at the bound like any other"
    );
}

#[test]
fn an_unrepresentable_kernel_reading_falls_back_without_panicking() {
    // Far outside the wire's signed-nanosecond field in both directions, and
    // judged against a userspace sample at the field's own limits, so nothing
    // here can be rescued by a lucky comparison. A conversion that saturated
    // instead of reporting failure would turn one of these into an ordinary
    // instant.
    let beyond = Duration::from_nanos(u64::MAX) + Duration::from_secs(1);

    for userspace_wall in [0, i64::MAX, i64::MIN] {
        let userspace = sample(userspace_wall, 10_000);
        for kernel in [UNIX_EPOCH + beyond, UNIX_EPOCH - beyond] {
            assert_eq!(
                preferred_receive_wall_ns(Some(kernel), userspace),
                userspace_wall,
                "an unrepresentable reading is never a measurement endpoint"
            );
        }
    }
}

#[test]
fn only_the_receive_wall_field_can_come_from_the_kernel() {
    // The whole StampAt x Clock matrix, each asserted as one struct so that a
    // field appearing where it should not is as loud as a wrong instant. Every
    // expectation below is what the same session emits with no kernel metadata,
    // except `recv_wall` under Receive and Both on a wall-reporting clock.
    for (stamp_at, clock, expected) in [
        // A: the kernel reading is the reported arrival time.
        (
            StampAt::Receive,
            Clock::Wall,
            TimestampFields {
                recv_wall: Some(900_000),
                ..TimestampFields::default()
            },
        ),
        // B: and the monotonic half of the same arrival stays the core's own.
        // The two fields deliberately name different physical instants.
        (
            StampAt::Receive,
            Clock::Both,
            TimestampFields {
                recv_wall: Some(900_000),
                recv_mono: Some(10_000),
                ..TimestampFields::default()
            },
        ),
        // C: nothing about the departure side moves, in either domain.
        (
            StampAt::Both,
            Clock::Both,
            TimestampFields {
                recv_wall: Some(900_000),
                recv_mono: Some(10_000),
                send_wall: Some(1_200_000),
                send_mono: Some(10_300),
                ..TimestampFields::default()
            },
        ),
        // D: wall only. The interval between these two now includes the
        // kernel-to-userspace part of the server's residency, which is the one
        // intended behavior change.
        (
            StampAt::Both,
            Clock::Wall,
            TimestampFields {
                recv_wall: Some(900_000),
                send_wall: Some(1_200_000),
                ..TimestampFields::default()
            },
        ),
        // E: no wall field exists, so there is nothing to improve.
        (
            StampAt::Both,
            Clock::Monotonic,
            TimestampFields {
                recv_mono: Some(10_000),
                send_mono: Some(10_300),
                ..TimestampFields::default()
            },
        ),
        (
            StampAt::Receive,
            Clock::Monotonic,
            TimestampFields {
                recv_mono: Some(10_000),
                ..TimestampFields::default()
            },
        ),
        // F: a reply reporting only its departure never learns of an arrival
        // observation.
        (
            StampAt::Send,
            Clock::Wall,
            TimestampFields {
                send_wall: Some(1_200_000),
                ..TimestampFields::default()
            },
        ),
        (
            StampAt::Send,
            Clock::Both,
            TimestampFields {
                send_wall: Some(1_200_000),
                send_mono: Some(10_300),
                ..TimestampFields::default()
            },
        ),
        // G: no timestamp at all.
        (StampAt::None, Clock::Both, TimestampFields::default()),
        // H and I: a midpoint is the mean of the core's own paired samples and
        // stays exactly that. Substituting only the wall arrival endpoint would
        // pull `midpoint_wall` to 1_050_000 — half the kernel-to-userspace
        // delay early — improving apparent upstream delay by precisely as much
        // as it worsened apparent downstream delay, and leaving the two
        // midpoint fields describing different midpoints.
        (
            StampAt::Midpoint,
            Clock::Wall,
            TimestampFields {
                midpoint_wall: Some(1_100_000),
                ..TimestampFields::default()
            },
        ),
        (
            StampAt::Midpoint,
            Clock::Both,
            TimestampFields {
                midpoint_wall: Some(1_100_000),
                midpoint_mono: Some(10_150),
                ..TimestampFields::default()
            },
        ),
        (
            StampAt::Midpoint,
            Clock::Monotonic,
            TimestampFields {
                midpoint_mono: Some(10_150),
                ..TimestampFields::default()
            },
        ),
    ] {
        assert_eq!(
            echo_timestamps(stamp_at, clock, Some(kernel_rx())),
            expected,
            "{stamp_at:?} on {clock:?} with a plausible kernel reading"
        );
    }
}

#[test]
fn a_datagram_with_no_kernel_reading_emits_what_it_always_did() {
    // The same matrix without metadata, which is every non-Linux target and
    // every Linux listener whose kernel offered nothing. `recv_wall` is the
    // core's own 1_000_000 throughout, and the two paths are otherwise the same
    // implementation.
    for (stamp_at, clock, expected) in [
        (
            StampAt::Receive,
            Clock::Wall,
            TimestampFields {
                recv_wall: Some(1_000_000),
                ..TimestampFields::default()
            },
        ),
        (
            StampAt::Both,
            Clock::Both,
            TimestampFields {
                recv_wall: Some(1_000_000),
                recv_mono: Some(10_000),
                send_wall: Some(1_200_000),
                send_mono: Some(10_300),
                ..TimestampFields::default()
            },
        ),
        (
            StampAt::Midpoint,
            Clock::Both,
            TimestampFields {
                midpoint_wall: Some(1_100_000),
                midpoint_mono: Some(10_150),
                ..TimestampFields::default()
            },
        ),
    ] {
        assert_eq!(
            echo_timestamps(stamp_at, clock, None),
            expected,
            "{stamp_at:?} on {clock:?} without kernel metadata"
        );
    }
}

#[test]
fn an_implausible_kernel_reading_falls_back_to_the_servers_own_sample() {
    // Case A of the wall-step analysis: the wall clock stepped *backwards*
    // between the kernel's reading and the core's sample, so the kernel value
    // is later than a sample that observed the same datagram. Case B: it
    // stepped forwards so far that the reading lags beyond the bound. Neither
    // is a reason to drop the datagram or to answer differently in any other
    // respect.
    let userspace_wall = received().wall_ns;
    for kernel in [
        at_epoch_ns(userspace_wall + 1),
        at_epoch_ns(userspace_wall - i64::try_from(MAX_KERNEL_RX_LAG.as_nanos()).unwrap() - 1),
    ] {
        assert_eq!(
            echo_timestamps(StampAt::Both, Clock::Both, Some(kernel)),
            TimestampFields {
                recv_wall: Some(1_000_000),
                recv_mono: Some(10_000),
                send_wall: Some(1_200_000),
                send_mono: Some(10_300),
                ..TimestampFields::default()
            },
        );
    }
}

#[test]
fn a_backward_wall_step_after_receive_never_reports_receive_after_send() {
    // The kernel's reading and the send sample no longer come from one paired
    // source, so the ordering rule has to reach the reported arrival value on
    // its own: an accepted kernel reading of 900_000, against a send sample the
    // wall clock stepped back to 850_000, must still settle on the send
    // reading.
    let mut core = core_with_sources(
        ServerConfig::default(),
        ScriptedTokens::new([TOKEN_A]),
        ScriptedClock::new([
            open_sample(),
            sample(1_000_000, 10_000),
            sample(850_000, 10_300),
        ]),
    );
    let params = echo_params(ReceivedStats::None, StampAt::Both, Clock::Both, 0);
    let (token, negotiated) = open_negotiated(&mut core, peer(), &params, None);

    let packet = core
        .handle_received_datagram(
            peer(),
            &echo_request(token, 0, &negotiated, &[], None),
            Some(at_epoch_ns(900_000)),
        )
        .unwrap()
        .expect("a backward wall step must not stop the echo being answered");
    let timestamps = expect_echo_reply(&packet, &negotiated, None).timestamps;

    assert_eq!(
        timestamps,
        TimestampFields {
            recv_wall: Some(850_000),
            recv_mono: Some(10_000),
            send_wall: Some(850_000),
            send_mono: Some(10_300),
            ..TimestampFields::default()
        },
        "the reported arrival wall value is held back to the send reading"
    );
    assert!(timestamps.recv_wall <= timestamps.send_wall);
    // The monotonic pair is ordered by its own source and owes nothing to what
    // the wall clock did.
    assert_eq!(timestamps.recv_mono, Some(10_000));
    assert!(timestamps.recv_mono <= timestamps.send_mono);
}

#[test]
fn the_public_core_path_emits_exactly_the_userspace_receive_wall() {
    // A direct `ServerCore` user has no transport metadata and must see the
    // behavior it always did, from the same implementation the runtime path
    // uses.
    let mut core = core_with_sources(
        ServerConfig::default(),
        ScriptedTokens::new([TOKEN_A]),
        ScriptedClock::new([open_sample(), received(), sent()]),
    );
    let params = echo_params(ReceivedStats::None, StampAt::Both, Clock::Both, 0);
    let (token, negotiated) = open_negotiated(&mut core, peer(), &params, None);

    let packet = core
        .handle_datagram(peer(), &echo_request(token, 0, &negotiated, &[], None))
        .unwrap()
        .expect("the public path must answer an ordinary echo");

    assert_eq!(
        expect_echo_reply(&packet, &negotiated, None).timestamps,
        TimestampFields {
            recv_wall: Some(1_000_000),
            recv_mono: Some(10_000),
            send_wall: Some(1_200_000),
            send_mono: Some(10_300),
            ..TimestampFields::default()
        },
    );
}

/// One echo's outcome, in the terms a lifecycle test cares about: whether it was
/// answered at all, what it counted, and how much session state the server still
/// holds afterwards.
#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    answered: bool,
    closing: bool,
    recv_count: Option<u32>,
    recv_window: Option<u64>,
    session_count: usize,
    recv_wall: Option<i64>,
}

fn outcome_of(
    core: &ServerCore,
    reply: Option<OutboundDatagram>,
    params: &Params,
    closing: bool,
) -> Outcome {
    let Some(reply) = reply else {
        return Outcome {
            answered: false,
            closing: false,
            recv_count: None,
            recv_window: None,
            session_count: core.session_count(),
            recv_wall: None,
        };
    };
    let decoded = if closing {
        expect_closing_echo_reply(&reply, params, None)
    } else {
        expect_echo_reply(&reply, params, None)
    };
    Outcome {
        answered: true,
        closing,
        recv_count: decoded.recv_count,
        recv_window: decoded.recv_window,
        session_count: core.session_count(),
        recv_wall: decoded.timestamps.recv_wall,
    }
}

/// Sends one echo as it would arrive at `at_ns`, with the kernel having observed
/// it 1 µs earlier when `with_kernel_rx` is set.
fn echo_at_with_kernel_rx(
    core: &mut ServerCore,
    clock: &ManualClock,
    at_ns: i64,
    token: u64,
    sequence: u32,
    params: &Params,
    with_kernel_rx: bool,
) -> Option<OutboundDatagram> {
    if !with_kernel_rx {
        return echo_at(core, clock, at_ns, token, sequence, params, None);
    }
    clock.set(at_ns);
    core.handle_received_datagram(
        peer(),
        &echo_request(token, sequence, params, &[], None),
        Some(at_epoch_ns(at_ns - 1_000)),
    )
    .expect("a rejected or rate-limited echo is not an internal error")
}

#[test]
fn kernel_metadata_changes_no_lifecycle_rate_or_deadline_decision() {
    // Two identical runs over one scripted arrival schedule, one with a
    // plausible kernel reading on every echo and one with none. The schedule
    // crosses a rate-limit burst, a refill, an idle refresh that only a
    // rate-limited request can have produced, and the maximum-duration
    // boundary — and the two runs must agree on every one of those outcomes.
    //
    // The session reports its receive wall, so the one permitted difference is
    // visible rather than merely assumed absent.
    let params = Params {
        interval_ns: 100 * MS,
        ..echo_params(ReceivedStats::Both, StampAt::Receive, Clock::Wall, 0)
    };
    let config = ServerConfig::default()
        .with_min_send_interval(Duration::from_millis(100))
        .with_burst_allowance(2)
        .with_idle_timeout(Duration::from_secs(2))
        .with_max_test_duration(Duration::from_secs(1));

    let run = |with_kernel_rx: bool| {
        let (mut core, clock, token, negotiated) = manual_core(
            config.clone(),
            &params,
            ScriptedTokens::new([TOKEN_A]),
            None,
        );
        let mut outcomes = Vec::new();
        let mut send = |core: &mut ServerCore, at_ns: i64, sequence: u32, closing: bool| {
            let reply = echo_at_with_kernel_rx(
                core,
                &clock,
                at_ns,
                token,
                sequence,
                &negotiated,
                with_kernel_rx,
            );
            outcomes.push(outcome_of(core, reply, &negotiated, closing));
        };

        // A burst of three against an allowance of two: the third is
        // rate-limited, answers nothing and advances no statistic, but does
        // refresh the idle deadline.
        send(&mut core, 10 * MS, 0, false);
        send(&mut core, 10 * MS, 1, false);
        send(&mut core, 10 * MS, 2, false);
        // Past the refill, and out of order, so the window transition is
        // exercised too.
        send(&mut core, 200 * MS, 4, false);
        send(&mut core, 400 * MS, 3, false);
        // The idle timeout is 2 s and the last activity was at 400 ms, so this
        // request at 2.3 s finds the session alive only because activity — not
        // the open — is what the deadline runs from.
        send(&mut core, 2_300 * MS, 5, false);
        // The maximum-duration deadline is the first served echo plus one
        // second plus the two-second grace: 10 ms + 3 s. This crosses it and
        // carries the close, releasing the session.
        send(&mut core, 3_100 * MS, 6, true);
        // And every later request is an unknown token.
        send(&mut core, 3_200 * MS, 7, false);
        assert_eq!(core.session_count(), 0);
        outcomes
    };

    let without = run(false);
    let with = run(true);

    assert_eq!(
        without.len(),
        8,
        "the schedule must actually have run to the end"
    );
    for (index, (without, with)) in without.iter().zip(&with).enumerate() {
        assert_eq!(
            (
                with.answered,
                with.closing,
                with.recv_count,
                with.recv_window,
                with.session_count
            ),
            (
                without.answered,
                without.closing,
                without.recv_count,
                without.recv_window,
                without.session_count
            ),
            "echo {index}: kernel metadata may not change any same-host decision"
        );
    }

    // The permitted difference, and proof the runs were not simply identical:
    // every answered reply reported the kernel's reading rather than the
    // server's own, which is 1 µs later.
    let reported: Vec<_> = with
        .iter()
        .filter_map(|outcome| outcome.recv_wall)
        .collect();
    let userspace: Vec<_> = without
        .iter()
        .filter_map(|outcome| outcome.recv_wall)
        .collect();
    assert_eq!(reported.len(), 6, "six of the eight echoes were answered");
    assert_eq!(
        reported,
        userspace
            .iter()
            .map(|wall| wall - 1_000)
            .collect::<Vec<_>>(),
        "each answered reply reported the kernel's arrival reading"
    );
}

#[test]
fn an_open_or_close_ignores_kernel_metadata_entirely() {
    // Neither emits a receive timestamp, and neither may take a different
    // lifecycle decision for having been observed. The clock script is what
    // pins that: an open consumes exactly its own lifecycle sample whatever the
    // transport reported.
    let clock = ScriptedClock::new([open_sample(), sample(1_000_000, 10_000)]);
    let mut core = core_with_sources(
        ServerConfig::default(),
        ScriptedTokens::new([TOKEN_A]),
        clock.clone(),
    );
    let params = echo_params(ReceivedStats::None, StampAt::Both, Clock::Both, 0);

    let packet = core
        .handle_received_datagram(peer(), &open_request(&params, None), Some(kernel_rx()))
        .unwrap()
        .expect("an open must be answered");
    let reply = expect_normal_open_reply(&packet, None);
    assert_eq!(
        clock.remaining(),
        1,
        "an open consumes one lifecycle sample"
    );
    assert_eq!(core.session_count(), 1);

    let closed: SocketAddr = peer();
    assert!(core
        .handle_received_datagram(closed, &close_request(reply.token, None), Some(kernel_rx()))
        .unwrap()
        .is_none());
    assert_eq!(core.session_count(), 0, "the close released its session");
    assert_eq!(
        clock.remaining(),
        0,
        "and a close consumes one of its own, whatever the transport observed"
    );
}
