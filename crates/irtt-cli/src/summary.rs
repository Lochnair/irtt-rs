use std::fmt::Write as _;

use irtt_stats::{
    DurationStats, DurationStatsWithMedian, FiniteSummary, SignedDurationStatsWithMedian,
};

pub fn format_summary(summary: &FiniteSummary) -> String {
    let mut out = String::new();
    let packets = summary.packets;
    let loss = summary.loss;

    writeln!(out).unwrap();
    writeln!(out, "irtt-rs summary").unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "  {:<18} {:>7} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "Metric", "Count", "Min", "Mean", "Median", "Max", "Stddev"
    )
    .unwrap();
    writeln!(out, "  {}", "-".repeat(82)).unwrap();

    write_signed_duration_row(&mut out, "RTT", &summary.rtt.primary);
    write_duration_row(&mut out, "raw RTT", &summary.rtt.raw);
    write_signed_duration_row(&mut out, "adjusted RTT", &summary.rtt.adjusted);
    write_duration_row(&mut out, "IPDV/jitter", &summary.ipdv.round_trip);
    write_duration_row(&mut out, "send IPDV", &summary.ipdv.send);
    write_duration_row(&mut out, "receive IPDV", &summary.ipdv.receive);
    write_duration_row(&mut out, "send delay", &summary.one_way_delay.send_delay);
    write_duration_row(
        &mut out,
        "receive delay",
        &summary.one_way_delay.receive_delay,
    );
    write_duration_row_no_median(
        &mut out,
        "server processing",
        &summary.server_processing.processing,
    );
    write_duration_row_no_median(&mut out, "send call", &summary.send_call);
    write_duration_row_no_median(&mut out, "timer error", &summary.timer_error);

    writeln!(out).unwrap();
    writeln!(
        out,
        "packets: sent={} received={} unique={} lost={} loss={}",
        packets.packets_sent,
        packets.packets_received,
        packets.unique_replies,
        loss.lost_packets,
        format_percent(loss.packet_loss_percent)
    )
    .unwrap();
    if packets.duplicates != 0 || packets.late_packets != 0 {
        writeln!(
            out,
            "replies: duplicates={} ({}) late={} ({})",
            packets.duplicates,
            format_percent(loss.duplicate_percent),
            packets.late_packets,
            format_percent(loss.late_packets_percent)
        )
        .unwrap();
    }
    writeln!(
        out,
        "bytes: sent={} received={}",
        packets.bytes_sent, packets.bytes_received
    )
    .unwrap();

    if packets.server_packets_received.is_some() || packets.server_received_window.is_some() {
        write!(out, "server:").unwrap();
        if let Some(count) = packets.server_packets_received {
            write!(out, " received={count}").unwrap();
        }
        if let Some(window) = packets.server_received_window {
            write!(out, " window={window:#x}").unwrap();
        }
        writeln!(out).unwrap();
    }

    out
}

fn write_duration_row(out: &mut String, label: &str, value: &DurationStatsWithMedian) {
    if value.stats.count == 0 {
        return;
    }
    writeln!(
        out,
        "  {label:<18} {:>7} {:>10} {:>10} {:>10} {:>10} {:>10}",
        value.stats.count,
        format_ns_u64(value.stats.min_ns),
        format_ns_f64(value.stats.mean_ns),
        format_ns_f64_opt(value.median_ns),
        format_ns_u64(value.stats.max_ns),
        format_ns_f64(value.stddev_ns())
    )
    .unwrap();
}

fn write_duration_row_no_median(out: &mut String, label: &str, value: &DurationStats) {
    if value.count == 0 {
        return;
    }
    writeln!(
        out,
        "  {label:<18} {:>7} {:>10} {:>10} {:>10} {:>10} {:>10}",
        value.count,
        format_ns_u64(value.min_ns),
        format_ns_f64(value.mean_ns),
        "-",
        format_ns_u64(value.max_ns),
        format_ns_f64(value.stddev_ns())
    )
    .unwrap();
}

fn write_signed_duration_row(out: &mut String, label: &str, value: &SignedDurationStatsWithMedian) {
    if value.stats.count == 0 {
        return;
    }
    writeln!(
        out,
        "  {label:<18} {:>7} {:>10} {:>10} {:>10} {:>10} {:>10}",
        value.stats.count,
        format_ns_i128(value.stats.min_ns),
        format_ns_f64(value.stats.mean_ns),
        format_ns_f64_opt(value.median_ns),
        format_ns_i128(value.stats.max_ns),
        format_ns_f64(value.stddev_ns())
    )
    .unwrap();
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
    let sign = if value.is_sign_negative() { "-" } else { "" };
    let value = value.abs();
    if value < 1_000.0 {
        format!("{sign}{value:.0}ns")
    } else if value < 1_000_000.0 {
        format!("{sign}{:.1}µs", value / 1_000.0)
    } else if value < 1_000_000_000.0 {
        format!("{sign}{:.1}ms", value / 1_000_000.0)
    } else {
        format!("{sign}{:.3}s", value / 1_000_000_000.0)
    }
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

        assert!(output.contains("Metric"));
        assert!(output.contains("Min"));
        assert!(output.contains("Mean"));
        assert!(output.contains("Median"));
        assert!(output.contains("Max"));
        assert!(output.contains("Stddev"));
        assert!(output.contains("packets: sent=0 received=0 unique=0 lost=0"));
        assert!(!output.contains("RTT                  0"));
        assert!(!output.contains("server processing"));
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

        assert!(output.contains("packets: sent=1 received=1 unique=1 lost=0 loss=0.00%"));
        assert!(output.contains("bytes: sent=64 received=64"));
        assert!(output.contains("RTT"));
        assert!(output.contains("1.2ms"));
        assert!(output.contains("server processing"));
        assert!(output.contains("300.0µs"));
        assert!(output.contains("send call"));
        assert!(output.contains("10.0µs"));
        assert!(output.contains("timer error"));
        assert!(output.contains("2.0µs"));
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

        let mut out = String::new();
        write_signed_duration_row(&mut out, "RTT", &stats);
        assert!(out.contains("-500ns"));
    }
}
