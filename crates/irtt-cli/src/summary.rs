use std::fmt::Write as _;

use irtt_stats::{
    DurationStats, DurationStatsWithMedian, FiniteSummary, SignedDurationStatsWithMedian,
};

pub fn format_summary(summary: &FiniteSummary) -> String {
    let mut out = String::new();
    let packets = summary.packets;
    let loss = summary.loss;

    writeln!(out).unwrap();
    writeln!(out, "--- irtt-rs statistics ---").unwrap();
    writeln!(
        out,
        "packets: sent={} received={} lost={} loss={}",
        packets.packets_sent,
        packets.packets_received,
        loss.lost_packets,
        format_percent(loss.packet_loss_percent)
    )
    .unwrap();
    writeln!(
        out,
        "replies: unique={} duplicates={} ({}) late={} ({})",
        packets.unique_replies,
        packets.duplicates,
        format_percent(loss.duplicate_percent),
        packets.late_packets,
        format_percent(loss.late_packets_percent)
    )
    .unwrap();
    writeln!(
        out,
        "bytes: sent={} received={}",
        packets.bytes_sent, packets.bytes_received
    )
    .unwrap();

    if let Some(count) = packets.server_packets_received {
        writeln!(out, "server_received_count: {count}").unwrap();
    }
    if let Some(window) = packets.server_received_window {
        writeln!(out, "server_received_window: {window:#x}").unwrap();
    }

    if let Some(line) = format_signed_duration_stats("rtt", &summary.rtt.primary) {
        writeln!(out, "{line}").unwrap();
    }
    if let Some(line) = format_duration_stats("raw_rtt", &summary.rtt.raw) {
        writeln!(out, "{line}").unwrap();
    }
    if let Some(line) = format_signed_duration_stats("adjusted_rtt", &summary.rtt.adjusted) {
        writeln!(out, "{line}").unwrap();
    }
    if let Some(line) = format_duration_stats("ipdv", &summary.ipdv.round_trip) {
        writeln!(out, "{line}").unwrap();
    }
    if let Some(line) = format_duration_stats("send_ipdv", &summary.ipdv.send) {
        writeln!(out, "{line}").unwrap();
    }
    if let Some(line) = format_duration_stats("receive_ipdv", &summary.ipdv.receive) {
        writeln!(out, "{line}").unwrap();
    }
    if let Some(line) = format_duration_stats("send_delay", &summary.one_way_delay.send_delay) {
        writeln!(out, "{line}").unwrap();
    }
    if let Some(line) = format_duration_stats("receive_delay", &summary.one_way_delay.receive_delay)
    {
        writeln!(out, "{line}").unwrap();
    }
    if let Some(line) =
        format_duration_stats_no_median("server_processing", &summary.server_processing.processing)
    {
        writeln!(out, "{line}").unwrap();
    }
    if let Some(line) = format_duration_stats_no_median("send_call", &summary.send_call) {
        writeln!(out, "{line}").unwrap();
    }
    if let Some(line) = format_duration_stats_no_median("timer_error", &summary.timer_error) {
        writeln!(out, "{line}").unwrap();
    }

    out
}

fn format_duration_stats(label: &str, value: &DurationStatsWithMedian) -> Option<String> {
    if value.stats.count == 0 {
        return None;
    }
    Some(format!(
        "{}: n={} min={} mean={} median={} max={} stddev={}",
        label,
        value.stats.count,
        format_ns_u64(value.stats.min_ns),
        format_ns_f64(value.stats.mean_ns),
        format_ns_f64_opt(value.median_ns),
        format_ns_u64(value.stats.max_ns),
        format_ns_f64(value.stddev_ns())
    ))
}

fn format_duration_stats_no_median(label: &str, value: &DurationStats) -> Option<String> {
    if value.count == 0 {
        return None;
    }
    Some(format!(
        "{}: n={} min={} mean={} max={} stddev={}",
        label,
        value.count,
        format_ns_u64(value.min_ns),
        format_ns_f64(value.mean_ns),
        format_ns_u64(value.max_ns),
        format_ns_f64(value.stddev_ns())
    ))
}

fn format_signed_duration_stats(
    label: &str,
    value: &SignedDurationStatsWithMedian,
) -> Option<String> {
    if value.stats.count == 0 {
        return None;
    }
    Some(format!(
        "{}: n={} min={} mean={} median={} max={} stddev={}",
        label,
        value.stats.count,
        format_ns_i128(value.stats.min_ns),
        format_ns_f64(value.stats.mean_ns),
        format_ns_f64_opt(value.median_ns),
        format_ns_i128(value.stats.max_ns),
        format_ns_f64(value.stddev_ns())
    ))
}

fn format_percent(value: f64) -> String {
    format!("{value:.2}%")
}

fn format_ns_u64(value: Option<u64>) -> String {
    value
        .map(|value| format_ns_f64(value as f64))
        .unwrap_or_else(|| "-".to_owned())
}

fn format_ns_i128(value: Option<i128>) -> String {
    value
        .map(|value| format_ns_f64(value as f64))
        .unwrap_or_else(|| "-".to_owned())
}

fn format_ns_f64_opt(value: Option<f64>) -> String {
    value.map(format_ns_f64).unwrap_or_else(|| "-".to_owned())
}

fn format_ns_f64(value: f64) -> String {
    let us = value / 1_000.0;
    format!("{us:.3} us")
}

#[cfg(test)]
mod tests {
    use super::*;
    use irtt_client::{
        ClientEvent, ClientTimestamp, PacketMeta, RttSample, ServerTiming, SignedDuration,
    };
    use irtt_stats::{SignedDurationStats, StatsCollector, StatsConfig};
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        time::{Duration, Instant, UNIX_EPOCH},
    };

    fn test_timestamp(offset: Duration) -> ClientTimestamp {
        ClientTimestamp {
            wall: UNIX_EPOCH + offset,
            mono: Instant::now() + offset,
        }
    }

    fn test_remote() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 2112)
    }

    #[test]
    fn empty_summary_omits_optional_metric_sections() {
        let summary = StatsCollector::new(StatsConfig::finite()).summary();
        let output = format_summary(&summary);

        assert!(output.contains("packets: sent=0 received=0 lost=0"));
        assert!(!output.contains("rtt: n="));
        assert!(!output.contains("server_processing: n="));
    }

    #[test]
    fn summary_formats_counts_and_available_metrics() {
        let mut collector = StatsCollector::new(StatsConfig::finite());
        let sent_at = test_timestamp(Duration::from_secs(1));
        let received_at = test_timestamp(Duration::from_secs(1) + Duration::from_micros(1500));

        collector.process(&ClientEvent::EchoSent {
            seq: 1,
            logical_seq: 1,
            remote: test_remote(),
            scheduled_at: sent_at.mono,
            sent_at,
            bytes: 64,
            send_call: Duration::from_micros(10),
            timer_error: Duration::from_micros(2),
        });
        collector.process(&ClientEvent::EchoReply {
            seq: 1,
            logical_seq: 1,
            remote: test_remote(),
            sent_at,
            received_at,
            rtt: RttSample {
                raw: Duration::from_micros(1500),
                adjusted: Some(Duration::from_micros(1200)),
                effective: Duration::from_micros(1200),
                adjusted_signed: Some(SignedDuration { ns: 1_200_000 }),
                effective_signed: SignedDuration { ns: 1_200_000 },
            },
            server_timing: Some(ServerTiming {
                receive_wall_ns: None,
                receive_mono_ns: None,
                send_wall_ns: None,
                send_mono_ns: None,
                midpoint_wall_ns: None,
                midpoint_mono_ns: None,
                processing: Some(Duration::from_micros(300)),
            }),
            one_way: None,
            received_stats: None,
            bytes: 64,
            packet_meta: PacketMeta::default(),
        });

        let output = format_summary(&collector.summary());

        assert!(output.contains("packets: sent=1 received=1 lost=0 loss=0.00%"));
        assert!(output.contains("bytes: sent=64 received=64"));
        assert!(output.contains("rtt: n=1 min=1200.000 us"));
        assert!(output.contains("server_processing: n=1 min=300.000 us"));
        assert!(output.contains("send_call: n=1 min=10.000 us"));
        assert!(output.contains("timer_error: n=1 min=2.000 us"));
    }

    #[test]
    fn signed_stats_can_format_negative_values() {
        let stats = SignedDurationStatsWithMedian {
            stats: SignedDurationStats {
                count: 1,
                total_ns: -500,
                min_ns: Some(-500),
                max_ns: Some(-500),
                mean_ns: -500.0,
                variance_ns2: 0.0,
            },
            median_ns: Some(-500.0),
        };

        let line = format_signed_duration_stats("rtt", &stats).unwrap();
        assert!(line.contains("min=-0.500 us"));
        assert!(line.contains("mean=-0.500 us"));
        assert!(line.contains("median=-0.500 us"));
    }
}
