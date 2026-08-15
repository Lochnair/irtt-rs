mod common;

use std::time::{Duration, Instant, UNIX_EPOCH};

use common::{
    adjusted_reply, reply_with_processing, reply_with_received_stats, reply_without_server_timing,
    sent, sent_with_timings, ts, unadjusted_late_reply, unadjusted_reply,
};
use irtt_client::{
    ClientEvent, ClientTimestamp, OneWayDelaySample, PacketMeta, RttSample, SignedDuration,
};
use irtt_stats::{
    EventStatsUpdate, IpdvPairUpdate, LateReplyMode, SampleMode, StatsCollector, StatsConfig,
};

#[test]
fn exact_mode_reports_a_send_call_median() {
    let mut collector = StatsCollector::new(StatsConfig::finite());
    for (seq, send_call_us) in [10_u64, 30, 20].into_iter().enumerate() {
        collector.process(&sent_with_timings(
            seq as u32,
            ts(seq as u64 * 10),
            send_call_us,
            2,
        ));
    }

    let snapshot = collector.snapshot();
    assert_eq!(snapshot.send_call.count, 3);
    assert_eq!(snapshot.send_call.min_ns, Some(10_000));
    assert_eq!(snapshot.send_call.max_ns, Some(30_000));
    // Sorted 10, 20, 30 us: the median is the middle sample, not the mean.
    assert_eq!(snapshot.send_call.median_ns, Some(20_000.0));
}

#[test]
fn exact_mode_reports_a_timer_error_median() {
    let mut collector = StatsCollector::new(StatsConfig::finite());
    for (seq, timer_error_us) in [2_u64, 8, 4, 6].into_iter().enumerate() {
        collector.process(&sent_with_timings(
            seq as u32,
            ts(seq as u64 * 10),
            10,
            timer_error_us,
        ));
    }

    let snapshot = collector.snapshot();
    assert_eq!(snapshot.timer_error.count, 4);
    // Sorted 2, 4, 6, 8 us: an even count averages the two middle samples.
    assert_eq!(snapshot.timer_error.median_ns, Some(5_000.0));
}

#[test]
fn exact_mode_reports_a_server_processing_median() {
    let mut collector = StatsCollector::new(StatsConfig::finite());
    for (seq, processing_ms) in [1_u64, 5, 3].into_iter().enumerate() {
        collector.process(&reply_with_processing(seq as u32, 10, processing_ms));
    }

    let snapshot = collector.snapshot();
    assert_eq!(snapshot.server_processing.processing.count, 3);
    assert_eq!(
        snapshot.server_processing.processing.median_ns,
        Some(3_000_000.0)
    );
}

#[test]
fn absent_server_processing_measurements_retain_no_samples() {
    let mut collector = StatsCollector::new(StatsConfig::finite());
    for seq in 0..3 {
        collector.process(&reply_without_server_timing(seq, 10));
    }

    let snapshot = collector.snapshot();
    // The replies measured RTT, so the run is not simply empty.
    assert_eq!(snapshot.rtt.primary.count, 3);
    assert!(snapshot.rtt.primary.median_ns.is_some());
    // ...but a metric the server never supplied stays empty rather than
    // gaining zero-valued samples.
    assert_eq!(snapshot.server_processing.processing.count, 0);
    assert_eq!(snapshot.server_processing.processing.median_ns, None);
    assert_eq!(snapshot.server_processing.processing.total_ns, 0);
}

#[test]
fn exact_mode_reports_medians_for_every_timing_metric_with_samples() {
    let mut collector = StatsCollector::new(StatsConfig::finite());
    for seq in 0..3 {
        collector.process(&sent(seq, ts(u64::from(seq) * 10)));
        collector.process(&adjusted_reply(
            seq,
            10 + u64::from(seq),
            9 + i128::from(seq),
        ));
    }

    let snapshot = collector.snapshot();
    for (label, stats) in [
        ("send call", &snapshot.send_call),
        ("timer error", &snapshot.timer_error),
        ("primary RTT", &snapshot.rtt.primary),
        ("raw RTT", &snapshot.rtt.raw),
        ("adjusted RTT", &snapshot.rtt.adjusted),
        ("round-trip IPDV", &snapshot.ipdv.round_trip),
        ("send IPDV", &snapshot.ipdv.send),
        ("receive IPDV", &snapshot.ipdv.receive),
        ("send delay", &snapshot.one_way_delay.send_delay),
        ("receive delay", &snapshot.one_way_delay.receive_delay),
        ("server processing", &snapshot.server_processing.processing),
    ] {
        assert!(stats.count > 0, "{label} should have samples");
        assert!(
            stats.median_ns.is_some(),
            "{label} has samples, so exact mode should report its median"
        );
    }
}

#[test]
fn running_only_tracks_send_side_and_processing_without_medians() {
    let mut collector = StatsCollector::new(StatsConfig::continuous());
    for (seq, value) in [10_u64, 30, 20].into_iter().enumerate() {
        let seq = seq as u32;
        collector.process(&sent_with_timings(
            seq,
            ts(u64::from(seq) * 10),
            value,
            value,
        ));
        collector.process(&reply_with_processing(seq, 10, 1 + u64::from(seq)));
    }

    let snapshot = collector.snapshot();
    for (label, stats) in [
        ("send call", &snapshot.send_call),
        ("timer error", &snapshot.timer_error),
        ("server processing", &snapshot.server_processing.processing),
    ] {
        assert_eq!(stats.count, 3, "{label} should still count its samples");
        assert!(
            stats.min_ns.is_some(),
            "{label} should still track a minimum"
        );
        assert!(stats.mean_ns > 0.0, "{label} should still track a mean");
        assert_eq!(
            stats.median_ns, None,
            "{label} retains no exact samples in running-only mode"
        );
    }
}

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
        late_replies: LateReplyMode::Measure,
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
        late_replies: LateReplyMode::Measure,
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
fn receive_window_is_stored_raw_and_never_drives_directional_loss() {
    // Window-only negotiation: the server reports a bitmap but no receive count.
    let mut collector = StatsCollector::new(StatsConfig::finite());
    collector.process(&sent(0, ts(0)));
    collector.process(&reply_with_received_stats(0, 10, None, Some(0x1)));

    // `0x1` represents no earlier history. It must be preserved verbatim and
    // must not be read as 63 lost packets or stand in for a receive count.
    let snapshot = collector.snapshot();
    assert_eq!(snapshot.packets.server_received_window, Some(0x1));
    assert_eq!(snapshot.packets.server_packets_received, None);
    assert_eq!(snapshot.loss.upstream_loss_packets, None);
    assert_eq!(snapshot.loss.downstream_loss_packets, None);
    assert_eq!(snapshot.loss.upstream_loss_percent, 0.0);
    assert_eq!(snapshot.loss.downstream_loss_percent, 0.0);

    // An advancing bitmap is stored verbatim across the full 64-bit range.
    collector.process(&sent(1, ts(10)));
    collector.process(&reply_with_received_stats(1, 10, None, Some(u64::MAX)));
    assert_eq!(
        collector.snapshot().packets.server_received_window,
        Some(u64::MAX)
    );
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

fn count_only(base: StatsConfig) -> StatsConfig {
    StatsConfig {
        late_replies: LateReplyMode::CountOnly,
        ..base
    }
}

fn untracked_late_reply(seq: u32, received_ms: u64) -> ClientEvent {
    ClientEvent::LateReply {
        seq,
        highest_seen: seq + 1,
        remote: "127.0.0.1:2112".parse().unwrap(),
        sent_at: None,
        received_at: ts(received_ms),
        rtt: None,
        server_timing: None,
        one_way: None,
        received_stats: None,
        bytes: 64,
        packet_meta: PacketMeta::default(),
    }
}

#[test]
fn matched_late_reply_measures_under_the_default_policy() {
    let mut collector = StatsCollector::new(StatsConfig::finite());
    collector.process(&sent(0, ts(0)));
    let update = collector.process(&unadjusted_late_reply(0, 10));

    assert!(update.contributed_sample);

    let snapshot = collector.snapshot();
    assert_eq!(snapshot.events.late_unique_replies, 1);
    assert_eq!(snapshot.events.untracked_late_replies, 0);
    assert_eq!(snapshot.packets.unique_replies, 1);
    assert_eq!(snapshot.packets.late_packets, 1);
    assert_eq!(snapshot.packets.packets_received, 1);
    assert_eq!(snapshot.packets.bytes_received, 64);
    assert_eq!(snapshot.rtt.primary.count, 1);
    assert_eq!(snapshot.server_processing.processing.count, 1);
    assert_eq!(snapshot.one_way_delay.send_delay.count, 1);
}

#[test]
fn matched_late_reply_counts_without_measuring_under_count_only() {
    let mut collector = StatsCollector::new(count_only(StatsConfig::finite()));
    collector.process(&sent(0, ts(0)));
    let update = collector.process(&unadjusted_late_reply(0, 10));

    assert!(!update.contributed_sample);
    assert!(update.ipdv_pairs.is_empty());

    let snapshot = collector.snapshot();
    // Still a matched late unique reply, not a reclassified untracked one.
    assert_eq!(snapshot.events.late_unique_replies, 1);
    assert_eq!(snapshot.events.untracked_late_replies, 0);
    assert_eq!(snapshot.packets.unique_replies, 1);
    assert_eq!(snapshot.packets.late_packets, 1);
    assert_eq!(snapshot.packets.packets_received, 1);
    assert_eq!(snapshot.packets.bytes_received, 64);
    // ...but it measures nothing.
    assert_eq!(snapshot.rtt.primary.count, 0);
    assert_eq!(snapshot.rtt.raw.count, 0);
    assert_eq!(snapshot.server_processing.processing.count, 0);
    assert_eq!(snapshot.one_way_delay.send_delay.count, 0);
    assert_eq!(snapshot.one_way_delay.receive_delay.count, 0);
    assert_eq!(snapshot.ipdv.round_trip.count, 0);
    // The reply carried a processing time, but the policy withheld it, so
    // exact retention has nothing to report a median from either.
    assert_eq!(snapshot.server_processing.processing.median_ns, None);
}

#[test]
fn count_only_late_replies_do_not_complete_ipdv_pairs() {
    let mut collector = StatsCollector::new(count_only(StatsConfig::finite()));
    collector.process(&unadjusted_reply(0, 10));
    let update = collector.process(&unadjusted_late_reply(1, 13));

    assert!(update.ipdv_pairs.is_empty());
    assert_eq!(collector.snapshot().ipdv.round_trip.count, 0);
}

#[test]
fn untracked_late_replies_stay_untracked_in_both_policies() {
    for config in [StatsConfig::finite(), count_only(StatsConfig::finite())] {
        let mut collector = StatsCollector::new(config);
        let update = collector.process(&untracked_late_reply(9, 100));

        assert!(!update.contributed_sample);

        let snapshot = collector.snapshot();
        assert_eq!(snapshot.events.untracked_late_replies, 1);
        assert_eq!(snapshot.events.late_unique_replies, 0);
        assert_eq!(snapshot.packets.unique_replies, 0);
        assert_eq!(snapshot.packets.late_packets, 1);
        assert_eq!(snapshot.packets.packets_received, 1);
        assert_eq!(snapshot.rtt.primary.count, 0);
    }
}

#[test]
fn rolling_snapshots_use_the_same_late_reply_policy() {
    let mut collector = StatsCollector::new(StatsConfig {
        rolling_count: Some(8),
        rolling_time: None,
        ..count_only(StatsConfig::continuous())
    });
    collector.process(&sent(0, ts(0)));
    collector.process(&unadjusted_reply(0, 10));
    collector.process(&unadjusted_late_reply(1, 10));

    let rolling = collector.rolling_count().unwrap();
    assert_eq!(rolling.events.late_unique_replies, 1);
    assert_eq!(rolling.events.untracked_late_replies, 0);
    assert_eq!(rolling.packets.unique_replies, 2);
    assert_eq!(rolling.packets.late_packets, 1);
    assert_eq!(rolling.rtt.primary.count, 1);
    assert_eq!(rolling.ipdv.round_trip.count, 0);
}
