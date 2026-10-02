use std::fmt::Write as _;

use irtt_stats::{Snapshot, TimeStats};

use crate::cmd::format::{
    format_ns_f64, format_optional_ns_f64 as format_ns_f64_opt,
    format_optional_ns_i128 as format_ns_i128, format_percent,
};

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
