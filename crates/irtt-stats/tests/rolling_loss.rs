use std::time::{Duration, Instant, UNIX_EPOCH};

use irtt_client::{
    ClientEvent, ClientTimestamp, PacketMeta, ReceivedStatsSample, RttSample, SignedDuration,
};
use irtt_stats::{LateReplyMode, Snapshot, StatsCollector, StatsConfig};

fn ts(base: Instant, ms: u64) -> ClientTimestamp {
    ClientTimestamp {
        mono: base + Duration::from_millis(ms),
        wall: UNIX_EPOCH + Duration::from_millis(ms),
    }
}

fn sent(base: Instant, seq: u32) -> ClientEvent {
    ClientEvent::EchoSent {
        seq,
        remote: "127.0.0.1:2112".parse().unwrap(),
        scheduled_at: None,
        sent_at: ts(base, u64::from(seq) * 10),
        bytes: 32,
        send_call: Duration::ZERO,
        timer_error: None,
    }
}

fn reply(base: Instant, seq: u32, count: u32) -> ClientEvent {
    ClientEvent::EchoReply {
        seq,
        remote: "127.0.0.1:2112".parse().unwrap(),
        sent_at: ts(base, u64::from(seq) * 10),
        received_at: ts(base, u64::from(seq) * 10 + 1),
        rtt: RttSample {
            raw: Duration::from_millis(1),
            adjusted: None,
            effective: SignedDuration::from_duration(Duration::from_millis(1)),
        },
        server_timing: None,
        one_way: None,
        received_stats: Some(ReceivedStatsSample {
            count: Some(count),
            window: None,
        }),
        bytes: 64,
        packet_meta: PacketMeta::default(),
    }
}

fn assert_directional(snapshot: &Snapshot, upstream: Option<i128>, downstream: Option<i128>) {
    assert_eq!(snapshot.loss.upstream_loss_packets, upstream);
    assert_eq!(snapshot.loss.downstream_loss_packets, downstream);
}

#[test]
fn lossless_count_and_time_windows_use_the_server_counter_increase() {
    let base = Instant::now();
    let mut collector = StatsCollector::new(StatsConfig {
        rolling_count: Some(2),
        rolling_time: Some(Duration::from_millis(2)),
        ..StatsConfig::continuous()
    });
    for seq in 0..100 {
        collector.process(&sent(base, seq));
        collector.process(&reply(base, seq, seq + 1));
        for rolling in [collector.rolling_count(), collector.rolling_time()] {
            let rolling = rolling.unwrap();
            assert_eq!(rolling.packets.packets_sent, 1);
            assert_eq!(rolling.packets.packets_received, 1);
            // Preserve the raw cumulative field, independently of the loss delta.
            assert_eq!(
                rolling.packets.server_packets_received,
                Some(u64::from(seq + 1))
            );
            assert_directional(&rolling, Some(0), Some(0));
            assert_eq!(rolling.loss.upstream_loss_percent, 0.0);
            assert_eq!(rolling.loss.downstream_loss_percent, 0.0);
        }
    }
    let cumulative = collector.snapshot();
    assert_eq!(cumulative.packets.packets_sent, 100);
    assert_directional(&cumulative, Some(0), Some(0));
}

#[test]
fn rolling_windows_distinguish_upstream_and_downstream_loss() {
    let base = Instant::now();
    // After the first pair expires, three sends and two replies remain.
    // A server delta of two means upstream loss; three means downstream loss.
    // Four also protects signed estimates when server counts exceed local sends.
    for (final_count, upstream, downstream, upstream_percent, downstream_percent) in [
        (3, 1, 0, 100.0 / 3.0, 0.0),
        (4, 0, 1, 0.0, 100.0 / 3.0),
        (5, -1, 2, -100.0 / 3.0, 50.0),
    ] {
        let mut collector = StatsCollector::new(StatsConfig {
            rolling_count: Some(5),
            rolling_time: Some(Duration::from_millis(25)),
            ..StatsConfig::continuous()
        });
        for event in [
            sent(base, 0),
            reply(base, 0, 1),
            sent(base, 1),
            reply(base, 1, 2),
            sent(base, 2),
            sent(base, 3),
            reply(base, 3, final_count),
        ] {
            collector.process(&event);
        }
        for rolling in [collector.rolling_count(), collector.rolling_time()] {
            let rolling = rolling.unwrap();
            assert_eq!(rolling.packets.packets_sent, 3);
            assert_eq!(rolling.packets.packets_received, 2);
            assert_directional(&rolling, Some(upstream), Some(downstream));
            assert_eq!(rolling.loss.upstream_loss_percent, upstream_percent);
            assert_eq!(rolling.loss.downstream_loss_percent, downstream_percent);
        }
        assert_directional(&collector.snapshot(), Some(upstream), Some(downstream));
    }
}

#[test]
fn rolling_baselines_cannot_include_unobserved_evicted_packets() {
    let base = Instant::now();
    let mut collector = StatsCollector::new(StatsConfig {
        rolling_count: Some(2),
        rolling_time: Some(Duration::from_millis(2)),
        ..StatsConfig::continuous()
    });
    for event in [
        sent(base, 0),
        reply(base, 0, 1),
        sent(base, 1),
        sent(base, 2),
        sent(base, 3),
        reply(base, 3, 4),
    ] {
        collector.process(&event);
    }
    for rolling in [collector.rolling_count(), collector.rolling_time()] {
        let rolling = rolling.unwrap();
        assert_eq!(rolling.packets.packets_sent, 1);
        assert_eq!(rolling.packets.packets_received, 1);
        assert_eq!(rolling.packets.server_packets_received, Some(4));
        assert_directional(&rolling, None, None);
    }
    assert_directional(&collector.snapshot(), Some(0), Some(2));

    // The next pair has an observation at its boundary, so estimates recover.
    collector.process(&sent(base, 4));
    collector.process(&reply(base, 4, 5));
    for rolling in [collector.rolling_count(), collector.rolling_time()] {
        assert_directional(&rolling.unwrap(), Some(0), Some(0));
    }
}

#[test]
fn rolling_counter_discontinuities_are_unavailable_until_a_fresh_interval() {
    let base = Instant::now();
    let mut collector = StatsCollector::new(StatsConfig {
        rolling_count: Some(2),
        rolling_time: Some(Duration::from_millis(2)),
        ..StatsConfig::continuous()
    });
    // Also cover an ambiguous high observation after wrap, followed by a return
    // to post-wrap counts. Neither jump may be treated as a precise delta.
    let counts = [u32::MAX - 1, u32::MAX, 0, 1, u32::MAX - 2, 2, 3];
    for (seq, count) in counts.into_iter().enumerate() {
        let seq = u32::try_from(seq).unwrap();
        collector.process(&sent(base, seq));
        collector.process(&reply(base, seq, count));
        if seq == 0 {
            continue;
        }
        for rolling in [collector.rolling_count(), collector.rolling_time()] {
            let rolling = rolling.unwrap();
            if matches!(seq, 1 | 3 | 6) {
                assert_directional(&rolling, Some(0), Some(0));
            } else {
                assert_directional(&rolling, None, None);
                assert_eq!(rolling.loss.upstream_loss_percent, 0.0);
                assert_eq!(rolling.loss.downstream_loss_percent, 0.0);
                assert_eq!(
                    rolling.packets.server_packets_received,
                    Some(u64::from(count))
                );
            }
        }
    }
    let cumulative = collector.snapshot();
    assert_eq!(
        cumulative.packets.server_packets_received,
        Some(u64::from(u32::MAX))
    );
    assert_directional(
        &cumulative,
        Some(7 - i128::from(u32::MAX)),
        Some(i128::from(u32::MAX) - 7),
    );
}

#[test]
fn split_probe_boundaries_require_a_current_server_baseline() {
    let base = Instant::now();
    let mut collector = StatsCollector::new(StatsConfig {
        rolling_count: Some(1),
        rolling_time: Some(Duration::ZERO),
        ..StatsConfig::continuous()
    });
    collector.process(&sent(base, 0));
    for rolling in [collector.rolling_count(), collector.rolling_time()] {
        assert_directional(&rolling.unwrap(), None, None);
    }
    collector.process(&reply(base, 0, 1));
    // The first send has gone and no earlier server observation exists.
    for rolling in [collector.rolling_count(), collector.rolling_time()] {
        let rolling = rolling.unwrap();
        assert_eq!(rolling.packets.server_packets_received, Some(1));
        assert_directional(&rolling, None, None);
        assert_eq!(rolling.loss.upstream_loss_percent, 0.0);
        assert_eq!(rolling.loss.downstream_loss_percent, 0.0);
    }
    collector.process(&sent(base, 1));
    for rolling in [collector.rolling_count(), collector.rolling_time()] {
        // No new server observation yet: one outstanding send, no counter increase.
        assert_directional(&rolling.unwrap(), Some(1), Some(0));
    }
    collector.process(&reply(base, 1, 2));
    for rolling in [collector.rolling_count(), collector.rolling_time()] {
        let rolling = rolling.unwrap();
        assert_eq!(rolling.packets.packets_sent, 0);
        assert_eq!(rolling.packets.packets_received, 1);
        // The evicted send makes the earlier observation stale at this boundary.
        assert_directional(&rolling, None, None);
    }
    assert_directional(&collector.snapshot(), Some(0), Some(0));

    // A first observation after many evicted sends does not reveal how much
    // of the cumulative counter belongs to the final retained send/reply pair.
    let mut collector = StatsCollector::new(StatsConfig {
        rolling_count: Some(2),
        rolling_time: Some(Duration::from_millis(2)),
        ..StatsConfig::continuous()
    });
    for seq in 0..100 {
        collector.process(&sent(base, seq));
    }
    collector.process(&reply(base, 99, 100));
    for rolling in [collector.rolling_count(), collector.rolling_time()] {
        let rolling = rolling.unwrap();
        assert_eq!(rolling.packets.packets_sent, 1);
        assert_eq!(rolling.packets.packets_received, 1);
        assert_eq!(rolling.packets.server_packets_received, Some(100));
        assert_directional(&rolling, None, None);
    }
    assert_directional(&collector.snapshot(), Some(0), Some(99));
}

#[test]
fn time_filtering_with_an_interior_packet_gap_has_no_directional_estimate() {
    let base = Instant::now();
    let mut collector = StatsCollector::new(StatsConfig {
        rolling_count: Some(3),
        rolling_time: Some(Duration::from_millis(2)),
        ..StatsConfig::continuous()
    });
    collector.process(&sent(base, 0));
    collector.process(&reply(base, 0, 1));
    collector.process(&sent(base, 1));
    // Arrives after send 1, but the time window excludes its backdated receipt.
    collector.process(&reply(base, 0, 2));
    collector.process(&reply(base, 1, 3));
    let rolling = collector.rolling_time().unwrap();
    assert_eq!(rolling.packets.packets_sent, 1);
    assert_eq!(rolling.packets.packets_received, 1);
    assert_directional(&rolling, None, None);
    // Count retention has no gap, and the anomalous server increase stays signed.
    assert_directional(&collector.rolling_count().unwrap(), Some(-1), Some(0));
}

#[test]
fn reordered_late_and_nonprimary_replies_preserve_directional_accounting() {
    let base = Instant::now();
    for policy in [LateReplyMode::Measure, LateReplyMode::CountOnly] {
        let mut collector = StatsCollector::new(StatsConfig {
            rolling_count: Some(6),
            rolling_time: Some(Duration::from_millis(25)),
            late_replies: policy,
            ..StatsConfig::continuous()
        });
        collector.process(&sent(base, 0));
        collector.process(&reply(base, 0, 1));
        collector.process(&sent(base, 1));
        collector.process(&sent(base, 2));
        collector.process(&reply(base, 2, 3));
        let ClientEvent::EchoReply {
            sent_at,
            rtt,
            received_stats,
            remote,
            ..
        } = reply(base, 1, 2)
        else {
            unreachable!();
        };
        collector.process(&ClientEvent::LateReply {
            seq: 1,
            highest_seen: 2,
            remote,
            sent_at: Some(sent_at),
            received_at: ts(base, 22),
            rtt: Some(rtt),
            server_timing: None,
            one_way: None,
            received_stats,
            bytes: 64,
            packet_meta: PacketMeta::default(),
        });
        collector.process(&ClientEvent::DuplicateReply {
            seq: 2,
            remote,
            received_at: ts(base, 23),
            bytes: 64,
        });
        // Even a supplied large counter on an untracked late reply is not a
        // primary server measurement under the existing normalization policy.
        collector.process(&ClientEvent::LateReply {
            seq: 0,
            highest_seen: 2,
            remote,
            sent_at: None,
            received_at: ts(base, 30),
            rtt: None,
            server_timing: None,
            one_way: None,
            received_stats: Some(ReceivedStatsSample {
                count: Some(100),
                window: None,
            }),
            bytes: 64,
            packet_meta: PacketMeta::default(),
        });
        for rolling in [collector.rolling_count(), collector.rolling_time()] {
            let rolling = rolling.unwrap();
            assert_eq!(rolling.packets.unique_replies, 2);
            assert_eq!(rolling.packets.packets_received, 4);
            assert_eq!(rolling.packets.server_packets_received, Some(3));
            assert_directional(&rolling, Some(0), Some(-2));
            assert_eq!(
                rolling.rtt.primary.count,
                if policy == LateReplyMode::Measure {
                    2
                } else {
                    1
                }
            );
        }
        assert_directional(&collector.snapshot(), Some(0), Some(-2));
    }
}
