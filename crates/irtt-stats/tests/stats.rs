mod common;

use std::time::{Duration, Instant, UNIX_EPOCH};

use common::{adjusted_reply, sent, ts, unadjusted_late_reply, unadjusted_reply};
use irtt_client::{
    ClientEvent, ClientTimestamp, OneWayDelaySample, PacketMeta, RttSample, SignedDuration,
};
use irtt_stats::{EventStatsUpdate, IpdvPairUpdate, SampleMode, StatsCollector, StatsConfig};

#[test]
fn running_only_samples_avoid_finite_retention() {
    let mut collector = StatsCollector::new(StatsConfig::continuous());
    collector.process(&unadjusted_reply(0, 10));
    collector.process(&unadjusted_reply(1, 20));

    let snapshot = collector.snapshot();
    assert_eq!(snapshot.rtt.primary.median_ns, None);
    assert_eq!(snapshot.rtt.raw.median_ns, None);
    assert_eq!(snapshot.rtt.adjusted.count, 0);
    assert_eq!(snapshot.rtt.adjusted.median_ns, None);
    assert_eq!(snapshot.ipdv.round_trip.median_ns, None);
    assert_eq!(snapshot.one_way_delay.send_delay.median_ns, None);
    assert_eq!(snapshot.one_way_delay.receive_delay.median_ns, None);
}

#[test]
fn continuous_mode_tracks_running_samples_without_exact_medians() {
    let mut collector = StatsCollector::new(StatsConfig::continuous());
    for seq in 0..4104 {
        collector.process(&unadjusted_reply(seq, 10));
    }

    let snapshot = collector.snapshot();
    assert_eq!(snapshot.rtt.primary.count, 4104);
    assert_eq!(snapshot.ipdv.round_trip.count, 4103);
    assert_eq!(snapshot.rtt.primary.median_ns, None);
    assert_eq!(snapshot.ipdv.round_trip.median_ns, None);
}

#[test]
fn cumulative_rtt_uses_signed_effective_and_tracks_raw() {
    let mut collector = StatsCollector::new(StatsConfig::finite());
    collector.process(&adjusted_reply(0, 1, -2));
    collector.process(&adjusted_reply(1, 10, 8));

    let snapshot = collector.snapshot();
    assert_eq!(snapshot.rtt.primary.count, 2);
    assert_eq!(snapshot.rtt.primary.min_ns, Some(-2_000_000));
    assert_eq!(snapshot.rtt.primary.median_ns, Some(3_000_000.0));
    assert_eq!(snapshot.rtt.raw.total_ns, 11_000_000);
    assert_eq!(snapshot.rtt.adjusted.count, 2);
}

#[test]
fn late_unique_counts_and_duplicates_do_not_update_duplicate_measurements() {
    let mut collector = StatsCollector::new(StatsConfig::finite());
    collector.process(&sent(0, ts(0)));
    collector.process(&sent(1, ts(10)));
    collector.process(&unadjusted_reply(1, 10));
    collector.process(&unadjusted_late_reply(0, 20));
    collector.process(&ClientEvent::DuplicateReply {
        seq: 0,
        remote: "127.0.0.1:2112".parse().unwrap(),
        received_at: ts(50),
        bytes: 64,
    });

    let snapshot = collector.snapshot();
    assert_eq!(snapshot.packets.packets_sent, 2);
    assert_eq!(snapshot.packets.packets_received, 3);
    assert_eq!(snapshot.packets.unique_replies, 2);
    assert_eq!(snapshot.packets.duplicates, 1);
    assert_eq!(snapshot.packets.late_packets, 1);
    assert_eq!(snapshot.packets.bytes_received, 64 + 64 + 64);
    assert_eq!(snapshot.rtt.primary.count, 2);
    assert_eq!(snapshot.loss.lost_packets, 0);
    assert_eq!(snapshot.loss.duplicate_percent, 100.0 / 3.0);
}

#[test]
fn final_loss_uses_sent_minus_unique_replies_not_echo_loss_events() {
    let mut collector = StatsCollector::new(StatsConfig::finite());
    collector.process(&sent(0, ts(0)));
    collector.process(&sent(1, ts(10)));
    collector.process(&ClientEvent::EchoLoss {
        seq: 0,
        sent_at: ts(0),
        timeout_at: Instant::now(),
    });
    collector.process(&unadjusted_late_reply(0, 10));

    let snapshot = collector.snapshot();
    assert_eq!(snapshot.events.loss_events, 1);
    assert_eq!(snapshot.packets.unique_replies, 1);
    assert_eq!(snapshot.loss.lost_packets, 1);
    assert_eq!(snapshot.loss.packet_loss_percent, 50.0);
}

fn assert_no_ipdv_pairs(update: &EventStatsUpdate) {
    assert!(update.ipdv_pairs.is_empty(), "{update:?}");
}

fn assert_one_ipdv_pair(
    update: &EventStatsUpdate,
    previous_seq: u32,
    current_seq: u32,
    rtt_ipdv: Duration,
) -> &IpdvPairUpdate {
    assert_eq!(update.ipdv_pairs.len(), 1, "{update:?}");
    let pair = &update.ipdv_pairs[0];
    assert_eq!(pair.previous_seq, previous_seq);
    assert_eq!(pair.current_seq, current_seq);
    assert_eq!(pair.rtt_ipdv, rtt_ipdv);
    pair
}

fn reply_at(seq: u32, raw_ms: u64, sent_ms: u64) -> ClientEvent {
    let sent_at = ClientTimestamp {
        mono: Instant::now() + Duration::from_millis(sent_ms),
        wall: UNIX_EPOCH + Duration::from_millis(sent_ms),
    };
    let received_at = ClientTimestamp {
        mono: sent_at.mono + Duration::from_millis(raw_ms),
        wall: sent_at.wall + Duration::from_millis(raw_ms),
    };
    let raw = Duration::from_millis(raw_ms);
    ClientEvent::EchoReply {
        seq,
        remote: "127.0.0.1:2112".parse().unwrap(),
        sent_at,
        received_at,
        rtt: RttSample {
            raw,
            adjusted: None,
            effective: SignedDuration::from_duration(raw),
        },
        server_timing: None,
        one_way: None,
        received_stats: None,
        bytes: 64,
        packet_meta: PacketMeta::default(),
    }
}

#[test]
fn ipdv_is_sequence_adjacent_and_gap_preserving() {
    let mut collector = StatsCollector::new(StatsConfig::finite());
    let first = collector.process(&unadjusted_reply(0, 10));
    let gap = collector.process(&unadjusted_reply(2, 15));
    let adjacent = collector.process(&unadjusted_reply(3, 12));

    let snapshot = collector.snapshot();
    assert!(first.contributed_sample);
    assert_no_ipdv_pairs(&first);

    assert!(gap.contributed_sample);
    assert_no_ipdv_pairs(&gap);

    assert!(adjacent.contributed_sample);
    assert_one_ipdv_pair(&adjacent, 2, 3, Duration::from_millis(3));
    assert_eq!(snapshot.ipdv.round_trip.count, 1);
    assert_eq!(snapshot.ipdv.round_trip.total_ns, 3_000_000);
}

#[test]
fn ipdv_wraparound_sequence_is_adjacent() {
    let mut collector = StatsCollector::new(StatsConfig::finite());
    let first = collector.process(&reply_at(u32::MAX, 10, 0));
    let wrapped = collector.process(&reply_at(0, 14, 10));

    assert!(first.contributed_sample);
    assert_no_ipdv_pairs(&first);

    assert!(wrapped.contributed_sample);
    assert_one_ipdv_pair(&wrapped, u32::MAX, 0, Duration::from_millis(4));

    let snapshot = collector.snapshot();
    assert_eq!(snapshot.ipdv.round_trip.count, 1);
    assert_eq!(snapshot.ipdv.round_trip.total_ns, 4_000_000);
}

#[test]
fn ipdv_wraparound_gap_is_preserved() {
    let mut collector = StatsCollector::new(StatsConfig::finite());
    let before_wrap = collector.process(&reply_at(u32::MAX - 1, 10, 0));
    let wrapped = collector.process(&reply_at(0, 14, 20));

    assert!(before_wrap.contributed_sample);
    assert_no_ipdv_pairs(&before_wrap);

    assert!(wrapped.contributed_sample);
    assert_no_ipdv_pairs(&wrapped);

    let snapshot = collector.snapshot();
    assert_eq!(snapshot.ipdv.round_trip.count, 0);
}

#[test]
fn late_reply_can_complete_ipdv_pair() {
    let mut collector = StatsCollector::new(StatsConfig::finite());
    collector.process(&unadjusted_reply(1, 20));
    let update = collector.process(&unadjusted_late_reply(0, 10));

    let snapshot = collector.snapshot();

    assert!(update.contributed_sample);
    assert_one_ipdv_pair(&update, 0, 1, Duration::from_millis(10));

    assert_eq!(snapshot.ipdv.round_trip.count, 1);
    assert_eq!(snapshot.ipdv.round_trip.total_ns, 10_000_000);
}

#[test]
fn update_exposes_directional_ipdv_when_available() {
    let mut collector = StatsCollector::new(StatsConfig::finite());
    collector.process(&unadjusted_reply(0, 10));
    let update = collector.process(&unadjusted_reply(1, 13));

    assert!(update.contributed_sample);

    let pair = assert_one_ipdv_pair(&update, 0, 1, Duration::from_millis(3));
    assert!(pair.send_ipdv.is_some());
    assert!(pair.receive_ipdv.is_some());
}

#[test]
fn gap_fill_update_exposes_both_completed_ipdv_pairs() {
    let mut collector = StatsCollector::new(StatsConfig::finite());

    let first = collector.process(&unadjusted_reply(0, 10));
    let gap = collector.process(&unadjusted_reply(2, 20));
    let fill = collector.process(&unadjusted_reply(1, 13));

    assert!(first.contributed_sample);
    assert!(first.ipdv_pairs.is_empty());

    assert!(gap.contributed_sample);
    assert!(gap.ipdv_pairs.is_empty());

    assert!(fill.contributed_sample);
    assert_eq!(fill.ipdv_pairs.len(), 2);

    assert_eq!(fill.ipdv_pairs[0].previous_seq, 0);
    assert_eq!(fill.ipdv_pairs[0].current_seq, 1);
    assert_eq!(fill.ipdv_pairs[0].rtt_ipdv, Duration::from_millis(3));

    assert_eq!(fill.ipdv_pairs[1].previous_seq, 1);
    assert_eq!(fill.ipdv_pairs[1].current_seq, 2);
    assert_eq!(fill.ipdv_pairs[1].rtt_ipdv, Duration::from_millis(7));

    let snapshot = collector.snapshot();
    assert_eq!(snapshot.ipdv.round_trip.count, 2);
    assert_eq!(snapshot.ipdv.round_trip.total_ns, 10_000_000);
}

#[test]
fn server_processing_and_one_way_require_available_samples() {
    let mut collector = StatsCollector::new(StatsConfig::finite());
    collector.process(&unadjusted_reply(0, 10));
    collector.process(&ClientEvent::LateReply {
        seq: 9,
        highest_seen: 10,
        remote: "127.0.0.1:2112".parse().unwrap(),
        sent_at: None,
        received_at: ts(100),
        rtt: None,
        server_timing: None,
        one_way: None,
        received_stats: None,
        bytes: 64,
        packet_meta: PacketMeta::default(),
    });

    let snapshot = collector.snapshot();
    assert_eq!(snapshot.server_processing.processing.count, 1);
    assert_eq!(snapshot.one_way_delay.send_delay.count, 1);
    assert_eq!(snapshot.events.untracked_late_replies, 1);
    assert_eq!(snapshot.packets.packets_received, 2);
    assert_eq!(snapshot.packets.unique_replies, 1);
    assert_eq!(snapshot.packets.late_packets, 1);
    assert_eq!(snapshot.packets.bytes_received, 128);
}

#[test]
fn negative_one_way_delay_samples_are_included_in_stats() {
    let mut collector = StatsCollector::new(StatsConfig::finite());
    let sent_at = ts(0);
    let received_at = ClientTimestamp {
        mono: sent_at.mono + Duration::from_millis(10),
        wall: sent_at.wall + Duration::from_millis(10),
    };

    collector.process(&ClientEvent::EchoReply {
        seq: 0,
        remote: "127.0.0.1:2112".parse().unwrap(),
        sent_at,
        received_at,
        rtt: RttSample {
            raw: Duration::from_millis(10),
            adjusted: None,
            effective: SignedDuration::from_nanos(10_000_000),
        },
        server_timing: None,
        one_way: Some(OneWayDelaySample {
            client_to_server: Some(SignedDuration::from_nanos(-1_000_000)),
            server_to_client: Some(SignedDuration::from_nanos(-2_000_000)),
        }),
        received_stats: None,
        bytes: 64,
        packet_meta: PacketMeta::default(),
    });

    let snapshot = collector.snapshot();

    assert_eq!(snapshot.one_way_delay.send_delay.count, 1);
    assert_eq!(snapshot.one_way_delay.send_delay.min_ns, Some(-1_000_000));
    assert_eq!(snapshot.one_way_delay.send_delay.total_ns, -1_000_000);
    assert_eq!(snapshot.one_way_delay.receive_delay.count, 1);
    assert_eq!(
        snapshot.one_way_delay.receive_delay.min_ns,
        Some(-2_000_000)
    );
    assert_eq!(snapshot.one_way_delay.receive_delay.total_ns, -2_000_000);
}

#[test]
fn older_cumulative_server_receive_count_does_not_regress() {
    let mut collector = StatsCollector::new(StatsConfig::finite());
    collector.process(&unadjusted_reply(1, 10));
    collector.process(&unadjusted_late_reply(0, 10));

    let snapshot = collector.snapshot();
    assert_eq!(snapshot.packets.server_packets_received, Some(2));
}

#[test]
fn rolling_count_eviction_recomputes_from_bounded_events() {
    let mut collector = StatsCollector::new(StatsConfig {
        samples: SampleMode::RunningOnly,
        rolling_count: Some(2),
        rolling_time: None,
    });
    collector.process(&sent(0, ts(0)));
    collector.process(&unadjusted_reply(0, 10));
    collector.process(&unadjusted_reply(1, 20));

    let rolling = collector.rolling_count().unwrap();
    assert_eq!(rolling.packets.packets_sent, 0);
    assert_eq!(rolling.packets.unique_replies, 2);
    assert_eq!(rolling.rtt.primary.count, 2);
}

#[test]
fn rolling_time_eviction_uses_event_timestamps() {
    let mut collector = StatsCollector::new(StatsConfig {
        samples: SampleMode::RunningOnly,
        rolling_count: None,
        rolling_time: Some(Duration::from_millis(15)),
    });
    collector.process(&sent(0, ts(0)));
    collector.process(&sent(1, ts(10)));
    collector.process(&sent(2, ts(30)));

    let rolling = collector.rolling_time().unwrap();
    assert_eq!(rolling.packets.packets_sent, 1);
}

#[test]
fn empty_and_all_lost_edges_are_defined() {
    let empty = StatsCollector::new(StatsConfig::finite()).snapshot();
    assert_eq!(empty.loss.packet_loss_percent, 0.0);

    let mut collector = StatsCollector::new(StatsConfig::finite());
    collector.process(&sent(0, ts(0)));
    let all_lost = collector.snapshot();
    assert_eq!(all_lost.loss.lost_packets, 1);
    assert_eq!(all_lost.loss.packet_loss_percent, 100.0);
}

#[test]
fn directional_loss_uses_server_received_count_when_available() {
    let mut collector = StatsCollector::new(StatsConfig::finite());
    collector.process(&sent(0, ts(0)));
    collector.process(&sent(1, ts(10)));
    collector.process(&unadjusted_reply(0, 10));

    let loss = collector.snapshot().loss;
    assert_eq!(loss.lost_packets, 1);
    assert_eq!(loss.upstream_loss_packets, Some(1));
    assert_eq!(loss.downstream_loss_packets, Some(0));
    assert_eq!(loss.packet_loss_percent, 50.0);
    assert_eq!(loss.upstream_loss_percent, 50.0);
}
