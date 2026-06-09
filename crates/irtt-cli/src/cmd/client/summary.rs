use std::fmt::Write as _;

use irtt_stats::{Snapshot, TimeStats};

pub fn format_summary(summary: &Snapshot) -> String {
    format_summary_with_options(summary, SummaryFormatOptions::default())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SummaryFormatOptions {
    pub verbose: bool,
    pub show_running_only_note: bool,
}

pub fn format_summary_with_options(summary: &Snapshot, options: SummaryFormatOptions) -> String {
    let mut out = String::new();
    let packets = summary.packets;
    let loss = summary.loss;

    writeln!(out).unwrap();
    writeln!(out, "irtt-rs summary").unwrap();
    if options.show_running_only_note {
        writeln!(
            out,
            "note: medians unavailable in continuous mode; running statistics are bounded-memory"
        )
        .unwrap();
    }
    writeln!(out).unwrap();
    writeln!(
        out,
        "  {:<18} {:>7} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "Metric", "Count", "Min", "Mean", "Median", "Max", "Stddev"
    )
    .unwrap();
    writeln!(out, "  {}", "-".repeat(82)).unwrap();

    write_time_row(&mut out, "RTT", &summary.rtt.primary);
    if options.verbose {
        write_time_row(&mut out, "raw RTT", &summary.rtt.raw);
        write_time_row(&mut out, "adjusted RTT", &summary.rtt.adjusted);
    }
    write_time_row(&mut out, "IPDV/jitter", &summary.ipdv.round_trip);
    write_time_row(&mut out, "send IPDV", &summary.ipdv.send);
    write_time_row(&mut out, "receive IPDV", &summary.ipdv.receive);
    write_time_row(&mut out, "send delay", &summary.one_way_delay.send_delay);
    write_time_row(
        &mut out,
        "receive delay",
        &summary.one_way_delay.receive_delay,
    );
    write_time_row(
        &mut out,
        "server processing",
        &summary.server_processing.processing,
    );
    write_time_row(&mut out, "send call", &summary.send_call);
    write_time_row(&mut out, "timer error", &summary.timer_error);

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

fn write_time_row(out: &mut String, label: &str, value: &TimeStats) {
    if value.count == 0 {
        return;
    }
    writeln!(
        out,
        "  {label:<18} {:>7} {:>10} {:>10} {:>10} {:>10} {:>10}",
        value.count,
        format_ns_i128(value.min_ns),
        format_ns_f64(value.mean_ns),
        format_ns_f64_opt(value.median_ns),
        format_ns_i128(value.max_ns),
        format_ns_f64(value.stddev_ns())
    )
    .unwrap();
}

fn format_percent(value: f64) -> String {
    format!("{value:.2}%")
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
    use irtt_stats::{StatsCollector, StatsConfig};
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
        let summary = StatsCollector::new(StatsConfig::finite()).snapshot();
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
            remote: test_remote(),
            scheduled_at: sent_at.mono,
            sent_at,
            bytes: 64,
            send_call: Duration::from_micros(10),
            timer_error: Duration::from_micros(2),
        });
        collector.process(&ClientEvent::EchoReply {
            seq: 1,
            remote: test_remote(),
            sent_at,
            received_at,
            rtt: RttSample {
                raw: Duration::from_micros(1500),
                adjusted: Some(SignedDuration::from_nanos(1_200_000)),
                effective: SignedDuration::from_nanos(1_200_000),
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

        let output = format_summary(&collector.snapshot());

        assert!(output.contains("packets: sent=1 received=1 unique=1 lost=0 loss=0.00%"));
        assert!(output.contains("bytes: sent=64 received=64"));
        assert!(output.contains("RTT"));
        assert!(output.contains("1.2ms"));
        assert!(!output.contains("raw RTT"));
        assert!(!output.contains("adjusted RTT"));
        assert!(output.contains("server processing"));
        assert!(output.contains("300.0µs"));
        assert!(output.contains("send call"));
        assert!(output.contains("10.0µs"));
        assert!(output.contains("timer error"));
        assert!(output.contains("2.0µs"));
        assert!(!output.contains("medians unavailable"));
    }

    #[test]
    fn verbose_summary_includes_raw_and_adjusted_rtt_rows() {
        let mut collector = StatsCollector::new(StatsConfig::finite());
        let received_at = test_timestamp(Duration::from_secs(1) + Duration::from_micros(1500));
        collector.process(&ClientEvent::EchoReply {
            seq: 1,
            remote: test_remote(),
            sent_at: test_timestamp(Duration::from_secs(1)),
            received_at,
            rtt: RttSample {
                raw: Duration::from_micros(1500),
                adjusted: Some(SignedDuration::from_nanos(1_200_000)),
                effective: SignedDuration::from_nanos(1_200_000),
            },
            server_timing: None,
            one_way: None,
            received_stats: None,
            bytes: 64,
            packet_meta: PacketMeta::default(),
        });

        let output = format_summary_with_options(
            &collector.snapshot(),
            SummaryFormatOptions {
                verbose: true,
                ..SummaryFormatOptions::default()
            },
        );

        assert!(output.contains("raw RTT"));
        assert!(output.contains("adjusted RTT"));
    }

    #[test]
    fn running_only_note_is_format_option() {
        let summary = StatsCollector::new(StatsConfig::continuous()).snapshot();
        let output = format_summary_with_options(
            &summary,
            SummaryFormatOptions {
                show_running_only_note: true,
                ..SummaryFormatOptions::default()
            },
        );

        assert!(output.contains("irtt-rs summary"));
        assert!(output.contains("medians unavailable"));
        assert!(output.contains("continuous mode"));
    }

    #[test]
    fn signed_stats_can_format_negative_values() {
        let stats = TimeStats {
            count: 1,
            total_ns: -500,
            min_ns: Some(-500),
            max_ns: Some(-500),
            mean_ns: -500.0,
            median_ns: Some(-500.0),
            variance_ns2: 0.0,
        };

        let mut out = String::new();
        write_time_row(&mut out, "RTT", &stats);
        assert!(out.contains("-500ns"));
    }
}
