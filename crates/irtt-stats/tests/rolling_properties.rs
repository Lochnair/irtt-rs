//! Independent count and time windows must match a filtered replay through
//! StatsCollector for ordinary metrics, while directional loss has an independent
//! interval model and cumulative accounting retains the complete history.
//! Includes delayed timeout discovery and replies with backdated timestamps.

use std::time::{Duration, Instant, UNIX_EPOCH};

use irtt_client::{
    ClientEvent, ClientTimestamp, OneWayDelaySample, PacketMeta, ReceivedStatsSample, RttSample,
    ServerTiming, SignedDuration,
};
use irtt_stats::{LateReplyMode, SampleMode, Snapshot, StatsCollector, StatsConfig};
use proptest::prelude::*;

/// One generated operation. `Advance` moves the shared monotonic clock
/// forward without producing an event; other variants produce a normalized
/// event, with delayed events timestamped before the clock's current value.
#[derive(Debug, Clone)]
enum Op {
    /// Advance the shared clock by this many milliseconds.
    Advance(u16),
    /// A probe send.
    Send,
    /// An on-time unique reply with the given client-observed RTT.
    Reply { raw_ms: u16 },
    /// A reply processed after later events, but timestamped at receipt.
    DelayedReply { raw_ms: u16, age_ms: u16 },
    /// A late reply matched to retained send state (measurable).
    LateReplyMatched { raw_ms: u16 },
    /// A late reply that could not be matched to retained state.
    LateReplyUnmatched,
    /// A duplicate reply for an already-completed sequence.
    Duplicate,
    /// A probe timeout / loss.
    Loss,
    /// A timeout discovered this many milliseconds after its deadline.
    DelayedLoss { age_ms: u16 },
    /// A diagnostic warning event.
    Warning,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (0u16..40).prop_map(Op::Advance),
        4 => Just(Op::Send),
        4 => (0u16..60).prop_map(|raw_ms| Op::Reply { raw_ms }),
        2 => (0u16..60, 0u16..100).prop_map(|(raw_ms, age_ms)| Op::DelayedReply { raw_ms, age_ms }),
        2 => (0u16..60).prop_map(|raw_ms| Op::LateReplyMatched { raw_ms }),
        1 => Just(Op::LateReplyUnmatched),
        1 => Just(Op::Duplicate),
        2 => Just(Op::Loss),
        2 => (0u16..100).prop_map(|age_ms| Op::DelayedLoss { age_ms }),
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
/// shared clock's current value; delayed events use their earlier deadline or
/// receipt time as the event's windowing timestamp.
fn build_event(op: &Op, seq: u32, clock_ms: u64, base: Instant) -> ClientEvent {
    let clock_ms = match *op {
        Op::DelayedReply { age_ms, .. } => clock_ms.saturating_sub(u64::from(age_ms)),
        _ => clock_ms,
    };
    match *op {
        Op::Advance(_) => unreachable!("Advance does not produce an event"),
        Op::Send => ClientEvent::EchoSent {
            seq,
            remote: ADDR.parse().unwrap(),
            scheduled_at: Some(ts_at(base, clock_ms).mono),
            sent_at: ts_at(base, clock_ms),
            bytes: 32,
            send_call: Duration::from_micros(10),
            timer_error: Some(Duration::from_micros(2)),
        },
        Op::Reply { raw_ms } | Op::DelayedReply { raw_ms, .. } => {
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
            timeout_at: ts_at(base, clock_ms).mono - Duration::from_millis(10),
        },
        Op::DelayedLoss { age_ms } => {
            let deadline_ms = clock_ms.saturating_sub(u64::from(age_ms));
            ClientEvent::EchoLoss {
                seq,
                sent_at: ts_at(base, deadline_ms.saturating_sub(3)),
                timeout_at: ts_at(base, deadline_ms).mono,
            }
        }
        Op::Warning => ClientEvent::Warning {
            kind: irtt_client::WarningKind::UntrackedReply,
            message: "generated".to_owned(),
            at: ts_at(base, clock_ms),
        },
    }
}

/// Public reference: collect exactly the events retained by one window.
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

/// Retain ordinary metrics through replay, but derive directional loss directly
/// from the full public event history, including the discarded prefix.
fn rolling_reference(
    history: &[(ClientEvent, u64)],
    retain: impl Fn(usize, u64) -> bool,
    late_replies: LateReplyMode,
) -> Snapshot {
    let retained: Vec<ClientEvent> = history
        .iter()
        .enumerate()
        .filter(|(index, (_, at))| retain(*index, *at))
        .map(|(_, (event, _))| event.clone())
        .collect();
    let mut snapshot = replay(&retained, late_replies);
    let packets: Vec<_> = history
        .iter()
        .enumerate()
        .filter(|(_, (event, _))| {
            matches!(
                event,
                ClientEvent::EchoSent { .. }
                    | ClientEvent::EchoReply { .. }
                    | ClientEvent::LateReply { .. }
                    | ClientEvent::DuplicateReply { .. }
            )
        })
        .collect();
    let selected: Vec<_> = packets
        .iter()
        .enumerate()
        .filter(|(_, (index, (_, at)))| retain(*index, *at))
        .collect();
    let count = |event: &ClientEvent| match event {
        ClientEvent::EchoReply { received_stats, .. }
        | ClientEvent::LateReply {
            sent_at: Some(_),
            rtt: Some(_),
            received_stats,
            ..
        } => received_stats.and_then(|stats| stats.count).map(u64::from),
        _ => None,
    };
    let delta = selected
        .first()
        .zip(selected.last())
        .and_then(|((first, _), (last, _))| {
            // Timestamp filtering must not omit a packet between the endpoints.
            if last - first + 1 != selected.len() {
                return None;
            }
            let baseline = if *first == 0 {
                Some(0)
            } else {
                packets[..*first]
                    .iter()
                    .filter_map(|(_, (event, _))| count(event))
                    .max()
            }?;
            let endpoint = packets[..=*last]
                .iter()
                .filter_map(|(_, (event, _))| count(event))
                .max()?;
            Some(endpoint - baseline)
        });
    snapshot.loss.upstream_loss_packets =
        delta.map(|delta| i128::from(snapshot.packets.packets_sent) - i128::from(delta));
    snapshot.loss.downstream_loss_packets =
        delta.map(|delta| i128::from(delta) - i128::from(snapshot.packets.packets_received));
    snapshot.loss.upstream_loss_percent = if snapshot.packets.packets_sent == 0 {
        0.0
    } else {
        snapshot.loss.upstream_loss_packets.map_or(0.0, |loss| {
            100.0 * loss as f64 / snapshot.packets.packets_sent as f64
        })
    };
    snapshot.loss.downstream_loss_percent = match delta {
        Some(delta) if delta != 0 => {
            100.0 * snapshot.loss.downstream_loss_packets.unwrap() as f64 / delta as f64
        }
        _ => 0.0,
    };
    snapshot
}

#[test]
fn collector_equality_ignores_expired_time_window_bookkeeping() {
    let base = Instant::now();
    let config = StatsConfig {
        rolling_time: Some(Duration::from_millis(3)),
        ..StatsConfig::continuous()
    };
    let mut left = StatsCollector::new(config);
    let mut right = StatsCollector::new(config);
    for at_ms in [1, 2, 4, 5, 6] {
        left.process(&build_event(&Op::Warning, 0, at_ms, base));
    }
    for at_ms in [1, 4, 2, 5, 6] {
        right.process(&build_event(&Op::Warning, 0, at_ms, base));
    }
    // Different expired histories leave the same live events in arrival order.
    assert_eq!(left.snapshot(), right.snapshot());
    assert_eq!(left.rolling_time(), right.rolling_time());
    assert_eq!(left, right);
}

#[test]
fn time_expiry_preserves_arrival_order_for_server_receive_window() {
    let base = Instant::now();
    let mut collector = StatsCollector::new(StatsConfig {
        rolling_time: Some(Duration::from_millis(3)),
        ..StatsConfig::continuous()
    });
    for (seq, at_ms, window) in [(1, 5, 0x5), (0, 4, 0x4)] {
        let mut event = build_event(&Op::Reply { raw_ms: 1 }, seq, at_ms, base);
        let ClientEvent::EchoReply { received_stats, .. } = &mut event else {
            unreachable!();
        };
        received_stats.as_mut().unwrap().window = Some(window);
        collector.process(&event);
    }
    // The last arrival owns the observation even with an older receive time.
    assert_eq!(
        collector
            .rolling_time()
            .unwrap()
            .packets
            .server_received_window,
        Some(0x4)
    );
    collector.process(&build_event(&Op::Warning, 2, 8, base));
    let rolling = collector.rolling_time().unwrap();
    assert_eq!(rolling.rtt.primary.count, 1);
    assert_eq!(rolling.packets.server_received_window, Some(0x5));
    // The hidden, expired observation remains in cumulative accounting.
    assert_eq!(
        collector.snapshot().packets.server_received_window,
        Some(0x4)
    );
    collector.process(&build_event(&Op::Warning, 3, 9, base));
    let rolling = collector.rolling_time().unwrap();
    assert_eq!(rolling.rtt.primary.count, 0);
    assert_eq!(rolling.packets.server_received_window, None);
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
        time_limit_ms in 0u64..80,
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

        // Force a loss whose clamped time evicts the initial send, while its
        // raw pre-send deadline would retain it. Every generated case must
        // therefore distinguish the two normalization choices.
        let ops = [Op::Send, Op::Advance(u16::try_from(time_limit_ms + 5).unwrap()), Op::Loss, Op::Advance(10), Op::Warning]
            .into_iter().chain(ops).collect::<Vec<_>>();
        for (idx, op) in ops.iter().enumerate() {
            if let Op::Advance(delta) = op {
                clock_ms += u64::from(*delta);
            } else {
                let seq = u32::try_from(idx).unwrap();
                let event = build_event(op, seq, clock_ms, base);
                collector.process(&event);
                let at_ms = match op {
                    Op::DelayedLoss { age_ms } | Op::DelayedReply { age_ms, .. } => clock_ms.saturating_sub(u64::from(*age_ms)),
                    _ => clock_ms,
                };
                history.push((event, at_ms));
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
            prop_assert_eq!(
                collector.rolling_count(),
                Some(rolling_reference(&history, |index, _| index >= count_start, late_replies)),
                "count window diverged from the count-only reference after op {}", idx
            );

            // Time window reference: every retained event whose timestamp is
            // within `time_limit_ms` of the maximum observed event timestamp,
            // ignoring the count bound entirely. Delayed losses cannot move
            // this anchor backwards.
            if let Some(latest_at_ms) = history.iter().map(|(_, at_ms)| at_ms).max() {
                let cutoff = latest_at_ms.checked_sub(time_limit_ms);
                prop_assert_eq!(
                    collector.rolling_time(),
                    Some(rolling_reference(&history, |_, at_ms| cutoff.is_none_or(|cutoff| at_ms >= cutoff), late_replies)),
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
