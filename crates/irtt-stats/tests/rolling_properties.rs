//! Property tests for the count-based and time-based rolling windows.
//!
//! [`RollingEvents`] (in `src/rolling.rs`) keeps two independent `VecDeque`s:
//! one bounded by [`StatsConfig::rolling_count`] and one bounded by
//! [`StatsConfig::rolling_time`]. They are pushed to together but evicted
//! independently, and each exposes its own snapshot
//! ([`StatsCollector::rolling_count`] / [`StatsCollector::rolling_time`]).
//! Reading `rolling.rs` and the existing `rolling_count_eviction_recomputes_*`
//! / `rolling_time_eviction_uses_event_timestamps` tests in `tests/stats.rs`
//! confirms there is no single window bounded by *both* limits at once: the
//! two bounds do not compose as AND or OR on shared storage, they are simply
//! two separate parallel windows over the same input stream. This file proves
//! that independence generatively: with both bounds configured
//! simultaneously, each window must behave exactly as if the *other* bound
//! did not exist.
//!
//! The reference model is a plain `Vec<(ClientEvent, at_ms)>` history that is
//! re-filtered from scratch after every operation (once by count, once by
//! time), then replayed through a fresh [`StatsCollector`] to obtain a
//! [`Snapshot`]. Because [`StatsCollector::rolling_count`] /
//! [`StatsCollector::rolling_time`] are themselves computed by replaying the
//! retained events through a fresh `CoreStats` on every call (see
//! `rolling.rs`'s `snapshot_window`), and because both the production window
//! and this reference model process the *same* `ClientEvent` values in the
//! *same* order through the *same* pure, deterministic normalization and
//! aggregation code, the two snapshots are expected to be bit-for-bit equal,
//! not merely close. `Snapshot` derives `PartialEq`, so the comparison below
//! is exact `assert_eq!` with no floating-point tolerance: there is no
//! independent numerical computation on the reference side to accumulate
//! error against, both sides run the identical arithmetic in the identical
//! order.
//!
//! The reference model's time-window recomputation (filter the whole history
//! by `at_ms >= latest_at_ms - window_ms`) is only equivalent to the
//! production incremental sliding-eviction (which pops from the front using
//! the cutoff computed at each push) when event timestamps are pushed in
//! non-decreasing order, exactly as `normalization.rs` documents ("Rolling-
//! window eviction assumes events are pushed in non-decreasing `at()`
//! order"). The operation generator below enforces that by construction: a
//! shared clock only ever moves forward, and every generated event borrows
//! its `at()` timestamp from the clock's current value.

use std::time::{Duration, Instant, UNIX_EPOCH};

use irtt_client::{
    ClientEvent, ClientTimestamp, OneWayDelaySample, PacketMeta, ReceivedStatsSample, RttSample,
    ServerTiming, SignedDuration,
};
use irtt_stats::{LateReplyMode, SampleMode, Snapshot, StatsCollector, StatsConfig};
use proptest::prelude::*;

/// One generated operation. `Advance` moves the shared monotonic clock
/// forward without producing an event; every other variant produces exactly
/// one normalized event at the clock's current value.
#[derive(Debug, Clone)]
enum Op {
    /// Advance the shared clock by this many milliseconds.
    Advance(u16),
    /// A probe send.
    Send,
    /// An on-time unique reply with the given client-observed RTT.
    Reply { raw_ms: u16 },
    /// A late reply matched to retained send state (measurable).
    LateReplyMatched { raw_ms: u16 },
    /// A late reply that could not be matched to retained state.
    LateReplyUnmatched,
    /// A duplicate reply for an already-completed sequence.
    Duplicate,
    /// A probe timeout / loss.
    Loss,
    /// A diagnostic warning event.
    Warning,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (0u16..40).prop_map(Op::Advance),
        4 => Just(Op::Send),
        4 => (0u16..60).prop_map(|raw_ms| Op::Reply { raw_ms }),
        2 => (0u16..60).prop_map(|raw_ms| Op::LateReplyMatched { raw_ms }),
        1 => Just(Op::LateReplyUnmatched),
        1 => Just(Op::Duplicate),
        2 => Just(Op::Loss),
        1 => Just(Op::Warning),
    ]
}

const ADDR: &str = "127.0.0.1:2112";

/// Builds a `ClientTimestamp` purely by adding a millisecond offset to a
/// fixed `base` `Instant` (and to `UNIX_EPOCH` for the wall-clock half). No
/// operation below ever reads the real clock more than once (to establish
/// `base`), so the resulting timestamps are fully deterministic functions of
/// the generated `Op`s, with no wall-clock jitter to make a proptest shrink
/// or CI run flaky.
fn ts_at(base: Instant, ms: u64) -> ClientTimestamp {
    ClientTimestamp {
        mono: base + Duration::from_millis(ms),
        wall: UNIX_EPOCH + Duration::from_millis(ms),
    }
}

fn server_timing(seq: u32) -> ServerTiming {
    let base_ns = i64::from(seq) * 10_000_000;
    ServerTiming {
        receive_mono_ns: Some(base_ns + 1_000_000),
        send_mono_ns: Some(base_ns + 2_000_000),
        receive_wall_ns: Some(base_ns + 1_000_000),
        send_wall_ns: Some(base_ns + 2_000_000),
        midpoint_mono_ns: None,
        midpoint_wall_ns: None,
        processing: Some(Duration::from_micros(100)),
    }
}

fn one_way(seq: u32) -> OneWayDelaySample {
    OneWayDelaySample {
        client_to_server: Some(SignedDuration::from_nanos(1_000_000 + i128::from(seq))),
        server_to_client: Some(SignedDuration::from_nanos(2_000_000 + i128::from(seq))),
    }
}

/// Builds the `ClientEvent` for one non-`Advance` op. `clock_ms` is the
/// shared clock's current value, used as the event's windowing timestamp
/// (`at()`, see `normalization.rs`) for every variant.
fn build_event(op: &Op, seq: u32, clock_ms: u64, base: Instant) -> ClientEvent {
    match *op {
        Op::Advance(_) => unreachable!("Advance does not produce an event"),
        Op::Send => ClientEvent::EchoSent {
            seq,
            remote: ADDR.parse().unwrap(),
            scheduled_at: ts_at(base, clock_ms).mono,
            sent_at: ts_at(base, clock_ms),
            bytes: 32,
            send_call: Duration::from_micros(10),
            timer_error: Duration::from_micros(2),
        },
        Op::Reply { raw_ms } => {
            let sent_at = ts_at(base, clock_ms.saturating_sub(u64::from(raw_ms)));
            let received_at = ts_at(base, clock_ms);
            ClientEvent::EchoReply {
                seq,
                remote: ADDR.parse().unwrap(),
                sent_at,
                received_at,
                rtt: RttSample {
                    raw: Duration::from_millis(u64::from(raw_ms)),
                    adjusted: None,
                    effective: SignedDuration::from_duration(Duration::from_millis(u64::from(
                        raw_ms,
                    ))),
                },
                server_timing: Some(server_timing(seq)),
                one_way: Some(one_way(seq)),
                received_stats: Some(ReceivedStatsSample {
                    count: Some(seq + 1),
                    window: Some(0xff),
                }),
                bytes: 64,
                packet_meta: PacketMeta::default(),
            }
        }
        Op::LateReplyMatched { raw_ms } => {
            let sent_at = ts_at(base, clock_ms.saturating_sub(u64::from(raw_ms)));
            let received_at = ts_at(base, clock_ms);
            ClientEvent::LateReply {
                seq,
                highest_seen: seq + 1,
                remote: ADDR.parse().unwrap(),
                sent_at: Some(sent_at),
                received_at,
                rtt: Some(RttSample {
                    raw: Duration::from_millis(u64::from(raw_ms)),
                    adjusted: None,
                    effective: SignedDuration::from_duration(Duration::from_millis(u64::from(
                        raw_ms,
                    ))),
                }),
                server_timing: Some(server_timing(seq)),
                one_way: Some(one_way(seq)),
                received_stats: Some(ReceivedStatsSample {
                    count: Some(seq + 1),
                    window: Some(0xff),
                }),
                bytes: 64,
                packet_meta: PacketMeta::default(),
            }
        }
        Op::LateReplyUnmatched => ClientEvent::LateReply {
            seq,
            highest_seen: seq + 1,
            remote: ADDR.parse().unwrap(),
            sent_at: None,
            received_at: ts_at(base, clock_ms),
            rtt: None,
            server_timing: None,
            one_way: None,
            received_stats: None,
            bytes: 64,
            packet_meta: PacketMeta::default(),
        },
        Op::Duplicate => ClientEvent::DuplicateReply {
            seq,
            remote: ADDR.parse().unwrap(),
            received_at: ts_at(base, clock_ms),
            bytes: 64,
        },
        Op::Loss => ClientEvent::EchoLoss {
            seq,
            sent_at: ts_at(base, clock_ms),
            timeout_at: ts_at(base, clock_ms).mono,
        },
        Op::Warning => ClientEvent::Warning {
            kind: irtt_client::WarningKind::UntrackedReply,
            message: "generated".to_owned(),
            at: ts_at(base, clock_ms),
        },
    }
}

/// Replays `events` in order through a fresh collector using the rolling
/// windows' own semantics (running statistics only, same late-reply policy),
/// mirroring exactly what `rolling.rs`'s private `snapshot_window` does for
/// the production windows. This is the "recompute from scratch" reference
/// primitive both the count and time reference windows below are built on.
fn replay(events: &[ClientEvent], late_replies: LateReplyMode) -> Snapshot {
    let mut collector = StatsCollector::new(StatsConfig {
        samples: SampleMode::RunningOnly,
        rolling_count: None,
        rolling_time: None,
        late_replies,
    });
    for event in events {
        collector.process(event);
    }
    collector.snapshot()
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 128, .. ProptestConfig::default() })]

    /// With `rolling_count` and `rolling_time` configured simultaneously,
    /// each window must match a reference model that applies *only its own*
    /// bound to the full event history, re-filtered from scratch after every
    /// operation. Any accidental AND/OR coupling between the two bounds (for
    /// example the count window also dropping events outside the time
    /// window, or vice versa) would show up here as a mismatch.
    ///
    /// The cumulative snapshot is checked too: it must equal a reference
    /// that replays the *entire* history, proving rolling eviction never
    /// reaches into the cumulative accounting.
    #[test]
    fn rolling_count_and_rolling_time_windows_are_independent(
        ops in prop::collection::vec(op_strategy(), 1..40),
        count_limit in 1usize..6,
        time_limit_ms in 1u64..80,
        count_only in prop::bool::ANY,
    ) {
        let late_replies = if count_only {
            LateReplyMode::CountOnly
        } else {
            LateReplyMode::Measure
        };
        let config = StatsConfig {
            samples: SampleMode::RunningOnly,
            rolling_count: Some(count_limit),
            rolling_time: Some(Duration::from_millis(time_limit_ms)),
            late_replies,
        };
        let mut collector = StatsCollector::new(config);
        let base = Instant::now();
        let mut clock_ms: u64 = 0;
        // Full, unfiltered event history alongside each event's windowing
        // timestamp, so the reference windows below can be recomputed from
        // scratch after every step.
        let mut history: Vec<(ClientEvent, u64)> = Vec::new();

        for (idx, op) in ops.iter().enumerate() {
            if let Op::Advance(delta) = op {
                clock_ms += u64::from(*delta);
            } else {
                let seq = u32::try_from(idx).unwrap();
                let event = build_event(op, seq, clock_ms, base);
                collector.process(&event);
                history.push((event, clock_ms));
            }

            // Cumulative snapshot: unaffected by rolling eviction, so it must
            // equal a fresh replay of the entire history so far.
            let full: Vec<ClientEvent> = history.iter().map(|(event, _)| event.clone()).collect();
            prop_assert_eq!(
                collector.snapshot(),
                replay(&full, late_replies),
                "cumulative snapshot diverged from a full replay after op {}", idx
            );

            // Count window reference: the last `count_limit` retained
            // events, ignoring the time bound entirely.
            let count_start = history.len().saturating_sub(count_limit);
            let count_ref: Vec<ClientEvent> = history[count_start..]
                .iter()
                .map(|(event, _)| event.clone())
                .collect();
            prop_assert_eq!(
                collector.rolling_count(),
                Some(replay(&count_ref, late_replies)),
                "count window diverged from the count-only reference after op {}", idx
            );

            // Time window reference: every retained event whose timestamp is
            // within `time_limit_ms` of the *latest processed event's*
            // timestamp, ignoring the count bound entirely. This recompute-
            // from-scratch filter is equivalent to the production window's
            // incremental sliding-eviction only because `clock_ms` is
            // non-decreasing across pushes (enforced by construction above:
            // `Advance` never subtracts).
            if let Some((_, latest_at_ms)) = history.last() {
                let cutoff = latest_at_ms.checked_sub(time_limit_ms);
                let time_ref: Vec<ClientEvent> = history
                    .iter()
                    .filter(|(_, at_ms)| cutoff.is_none_or(|cutoff| *at_ms >= cutoff))
                    .map(|(event, _)| event.clone())
                    .collect();
                prop_assert_eq!(
                    collector.rolling_time(),
                    Some(replay(&time_ref, late_replies)),
                    "time window diverged from the time-only reference after op {}", idx
                );
            } else {
                prop_assert_eq!(
                    collector.rolling_time(),
                    Some(replay(&[], late_replies)),
                    "time window should be empty before any event after op {}", idx
                );
            }
        }
    }
}
