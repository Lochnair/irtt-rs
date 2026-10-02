use std::time::{Duration, Instant, UNIX_EPOCH};

use irtt_client::{
    ClientEvent, ClientTimestamp, PacketMeta, RttSample, ServerTiming, SignedDuration,
};
use irtt_stats::{StatsCollector, StatsConfig};

fn reply(
    base: Instant,
    seq: u32,
    send_ms: u64,
    receive_ms: u64,
    timing: ServerTiming,
) -> ClientEvent {
    let raw = Duration::from_millis(receive_ms - send_ms);
    ClientEvent::EchoReply {
        seq,
        remote: "127.0.0.1:2112".parse().unwrap(),
        sent_at: ClientTimestamp {
            mono: base + Duration::from_millis(send_ms),
            wall: UNIX_EPOCH + Duration::from_millis(200 + u64::from(seq) * 10),
        },
        received_at: ClientTimestamp {
            mono: base + Duration::from_millis(receive_ms),
            wall: UNIX_EPOCH + Duration::from_millis(240 + u64::from(seq) * 20),
        },
        rtt: RttSample {
            raw,
            adjusted: None,
            effective: SignedDuration::from_duration(raw),
        },
        server_timing: Some(timing),
        one_way: None,
        received_stats: None,
        bytes: 64,
        packet_meta: PacketMeta::default(),
    }
}

#[test]
fn directional_ipdv_uses_matching_clock_domains_and_preserves_zero_and_absence() {
    let first = ServerTiming {
        receive_mono_ns: Some(100_000_000),
        send_mono_ns: Some(101_000_000),
        receive_wall_ns: Some(1_000_000_000),
        send_wall_ns: Some(1_001_000_000),
        midpoint_mono_ns: None,
        midpoint_wall_ns: None,
        processing: None,
    };
    let second = ServerTiming {
        receive_mono_ns: Some(105_000_000),
        send_mono_ns: Some(108_000_000),
        receive_wall_ns: Some(1_020_000_000),
        send_wall_ns: Some(1_026_000_000),
        ..first
    };
    // Wall and monotonic deltas deliberately disagree: availability alone
    // cannot prove the formulas or which clock domain wins.
    let mut wall_only = second;
    wall_only.receive_mono_ns = None;
    wall_only.send_mono_ns = None;
    let mut no_send = wall_only;
    no_send.receive_wall_ns = None;
    let mut no_receive = wall_only;
    no_receive.send_wall_ns = None;
    let zero = ServerTiming {
        receive_mono_ns: Some(110_000_000),
        send_mono_ns: Some(111_000_000),
        ..second
    };
    for (label, current, send_ms, expected_send_ms, expected_receive_ms) in [
        ("monotonic preferred", second, 30, Some(5), Some(3)),
        ("wall fallback", wall_only, 30, Some(10), Some(5)),
        ("missing send endpoint", no_send, 30, None, Some(5)),
        ("missing receive endpoint", no_receive, 30, Some(10), None),
        ("reordered client send times", second, 10, Some(15), Some(3)),
        ("zero variation", zero, 30, Some(0), Some(0)),
    ] {
        let base = Instant::now();
        let mut collector = StatsCollector::new(StatsConfig::finite());
        assert!(collector
            .process(&reply(base, 0, 20, 40, first))
            .ipdv_pairs
            .is_empty());
        let update = collector.process(&reply(base, 1, send_ms, 50, current));
        assert_eq!(update.ipdv_pairs.len(), 1, "{label}");
        let pair = &update.ipdv_pairs[0];
        assert_eq!(
            pair.send_ipdv,
            expected_send_ms.map(Duration::from_millis),
            "{label}"
        );
        assert_eq!(
            pair.receive_ipdv,
            expected_receive_ms.map(Duration::from_millis),
            "{label}"
        );
        let snapshot = collector.snapshot();
        for (expected, actual) in [
            (expected_send_ms, snapshot.ipdv.send),
            (expected_receive_ms, snapshot.ipdv.receive),
        ] {
            assert_eq!(actual.count, u64::from(expected.is_some()), "{label}");
            assert_eq!(
                actual.total_ns,
                i128::from(expected.unwrap_or(0)) * 1_000_000,
                "{label}"
            );
        }
    }
}
