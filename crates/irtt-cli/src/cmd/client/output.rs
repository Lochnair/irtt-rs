use std::{
    fmt::Write as _,
    net::SocketAddr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use irtt_client::{
    ClientEvent, NegotiatedParams, OneWayDelaySample, PacketMeta, ReceivedStatsSample, RttSample,
    ServerTiming, SignedDuration, WarningKind,
};

use super::args::OutputMode;

pub fn format_event(event: &ClientEvent, mode: OutputMode) -> Option<String> {
    if matches!(event, ClientEvent::EchoSent { .. }) {
        return None;
    }
    match mode {
        OutputMode::RttUs => format_rtt_us(event),
        OutputMode::Human => Some(format_human_event(event, None)),
        OutputMode::Machine => Some(format_machine(event)),
        OutputMode::Simple => Some(format_simple(event)),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HumanEventStats {
    pub contributed_sample: bool,
    pub ipdv_pairs: Vec<HumanIpdvPair>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HumanIpdvPair {
    pub previous_seq: u32,
    pub current_seq: u32,
    pub rtt_ipdv: Duration,
    pub send_ipdv: Option<Duration>,
    pub receive_ipdv: Option<Duration>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HumanOutputOptions {
    pub verbose: bool,
}

#[cfg(feature = "stats")]
impl From<irtt_stats::EventStatsUpdate> for HumanEventStats {
    fn from(value: irtt_stats::EventStatsUpdate) -> Self {
        Self {
            contributed_sample: value.contributed_sample,
            ipdv_pairs: value.ipdv_pairs.into_iter().map(Into::into).collect(),
        }
    }
}

#[cfg(feature = "stats")]
impl From<irtt_stats::IpdvPairUpdate> for HumanIpdvPair {
    fn from(value: irtt_stats::IpdvPairUpdate) -> Self {
        Self {
            previous_seq: value.previous_seq,
            current_seq: value.current_seq,
            rtt_ipdv: value.rtt_ipdv,
            send_ipdv: value.send_ipdv,
            receive_ipdv: value.receive_ipdv,
        }
    }
}

pub fn format_human_event(event: &ClientEvent, stats: Option<HumanEventStats>) -> String {
    format_human_event_with_options(event, stats, HumanOutputOptions::default())
}

pub fn format_human_event_with_options(
    event: &ClientEvent,
    stats: Option<HumanEventStats>,
    options: HumanOutputOptions,
) -> String {
    let stats = stats.as_ref();

    match event {
        ClientEvent::SessionStarted { remote, token, .. } => {
            format!("session started  remote={remote}  token={token:#x}")
        }
        ClientEvent::NoTestCompleted { remote, .. } => {
            format!("no-test completed  remote={remote}")
        }
        ClientEvent::SessionClosed { remote, token, .. } => {
            format!("session closed  remote={remote}  token={token:#x}")
        }
        ClientEvent::EchoSent { .. } => String::new(),
        ClientEvent::EchoReply {
            seq,
            rtt,
            server_timing,
            one_way,
            received_stats,
            ..
        } => {
            let mut out = format!("seq={seq}");
            write!(out, "  rtt={}", format_signed_duration(rtt.effective)).unwrap();
            write_human_one_way(&mut out, *one_way);
            write!(out, "  ipdv={}", format_human_ipdv(stats, *seq)).unwrap();
            if let Some(processing) = server_timing.and_then(|timing| timing.processing) {
                write!(out, "  proc={}", format_duration(processing)).unwrap();
            }
            if options.verbose {
                write_human_received_stats(&mut out, *received_stats);
            }
            out
        }
        ClientEvent::EchoLoss { seq, .. } => {
            format!("loss  seq={seq}")
        }
        ClientEvent::DuplicateReply { seq, remote, .. } => {
            format!("duplicate  seq={seq}  remote={remote}")
        }
        ClientEvent::LateReply {
            seq,
            highest_seen,
            remote,
            rtt,
            one_way,
            received_stats,
            ..
        } => {
            let mut out = format!("late  seq={seq}  highest_seen={highest_seen}  remote={remote}",);
            if let Some(rtt) = rtt {
                write!(out, "  rtt={}", format_signed_duration(rtt.effective)).unwrap();
                write_human_one_way(&mut out, *one_way);
                write!(out, "  ipdv={}", format_human_ipdv(stats, *seq)).unwrap();
            }
            write_human_received_stats(&mut out, *received_stats);
            out
        }
        ClientEvent::Warning { kind, message, .. } => {
            format!("warning  kind={}  message={message}", warning_kind(*kind))
        }
    }
}

fn format_rtt_us(event: &ClientEvent) -> Option<String> {
    match event {
        ClientEvent::EchoReply { rtt, .. } => Some(signed_duration_us(rtt.effective).to_string()),
        _ => None,
    }
}

fn format_machine(event: &ClientEvent) -> String {
    let mut out = String::new();
    match event {
        ClientEvent::SessionStarted {
            remote,
            token,
            negotiated,
            at,
        } => {
            write_common(&mut out, "session_started");
            write_remote(&mut out, *remote);
            write_token(&mut out, *token);
            write_wall(&mut out, "event_wall_ns", at.wall);
            write_negotiated(&mut out, negotiated);
        }
        ClientEvent::NoTestCompleted {
            remote,
            negotiated,
            at,
        } => {
            write_common(&mut out, "no_test_completed");
            write_remote(&mut out, *remote);
            write_wall(&mut out, "event_wall_ns", at.wall);
            write_negotiated(&mut out, negotiated);
        }
        ClientEvent::SessionClosed { remote, token, at } => {
            write_common(&mut out, "session_closed");
            write_remote(&mut out, *remote);
            write_token(&mut out, *token);
            write_wall(&mut out, "event_wall_ns", at.wall);
        }
        ClientEvent::EchoSent { .. } => {}
        ClientEvent::EchoReply {
            seq,
            remote,
            sent_at,
            received_at,
            rtt,
            server_timing,
            one_way,
            received_stats,
            bytes: _,
            packet_meta,
        } => {
            write_common(&mut out, "echo_reply");
            write_seq(&mut out, *seq);
            write_remote(&mut out, *remote);
            write_wall(&mut out, "client_send_wall_ns", sent_at.wall);
            write_wall(&mut out, "client_receive_wall_ns", received_at.wall);
            write_rtt(&mut out, rtt);
            write_server_timing(&mut out, *server_timing);
            write_one_way(&mut out, *one_way);
            write_received_stats(&mut out, *received_stats);
            write_packet_meta(&mut out, *packet_meta);
        }
        ClientEvent::EchoLoss { seq, sent_at, .. } => {
            write_common(&mut out, "loss");
            write_seq(&mut out, *seq);
            write_wall(&mut out, "client_send_wall_ns", sent_at.wall);
            out.push_str(" warning=loss");
        }
        ClientEvent::DuplicateReply {
            seq,
            remote,
            received_at,
            bytes: _,
        } => {
            write_common(&mut out, "duplicate");
            write_seq(&mut out, *seq);
            write_remote(&mut out, *remote);
            write_wall(&mut out, "client_receive_wall_ns", received_at.wall);
            out.push_str(" warning=duplicate");
        }
        ClientEvent::LateReply {
            seq,
            highest_seen,
            remote,
            sent_at,
            received_at,
            rtt,
            server_timing,
            one_way,
            received_stats,
            bytes: _,
            packet_meta,
        } => {
            write_common(&mut out, "late");
            write_seq(&mut out, *seq);
            write_remote(&mut out, *remote);
            write!(out, " highest_seen={highest_seen}").unwrap();
            if let Some(sent_at) = sent_at {
                write_wall(&mut out, "client_send_wall_ns", sent_at.wall);
            }
            write_wall(&mut out, "client_receive_wall_ns", received_at.wall);
            if let Some(rtt) = rtt {
                write_rtt(&mut out, rtt);
            }
            write_server_timing(&mut out, *server_timing);
            write_one_way(&mut out, *one_way);
            write_received_stats(&mut out, *received_stats);
            write_packet_meta(&mut out, *packet_meta);
            out.push_str(" warning=late");
        }
        ClientEvent::Warning { kind, message, at } => {
            write_common(&mut out, "warning");
            write_wall(&mut out, "at", at.wall);
            write!(
                out,
                " warning_kind={} message={}",
                warning_kind(*kind),
                escape_value(message)
            )
            .unwrap();
        }
    }
    out
}

fn format_simple(event: &ClientEvent) -> String {
    match event {
        ClientEvent::SessionStarted { remote, token, .. } => {
            format!("session started remote={remote} token={token:#x}")
        }
        ClientEvent::NoTestCompleted { remote, .. } => {
            format!("no-test completed remote={remote}")
        }
        ClientEvent::SessionClosed { remote, token, .. } => {
            format!("session closed remote={remote} token={token:#x}")
        }
        ClientEvent::EchoSent { .. } => String::new(),
        ClientEvent::EchoReply {
            seq,
            remote,
            rtt,
            server_timing,
            bytes: _,
            ..
        } => {
            let mut out = format!(
                "reply seq={seq} remote={remote} rtt_us={}",
                signed_duration_us(rtt.effective)
            );
            if rtt.adjusted.is_some() {
                write!(out, " raw_rtt_us={}", duration_us(rtt.raw)).unwrap();
            }
            if let Some(processing) = server_timing.and_then(|timing| timing.processing) {
                write!(out, " server_processing_us={}", duration_us(processing)).unwrap();
            }
            out
        }
        ClientEvent::EchoLoss { seq, .. } => {
            format!("loss seq={seq}")
        }
        ClientEvent::DuplicateReply { seq, remote, .. } => {
            format!("duplicate seq={seq} remote={remote}")
        }
        ClientEvent::LateReply {
            seq,
            highest_seen,
            remote,
            rtt,
            ..
        } => {
            let mut out = format!("late seq={seq} highest_seen={highest_seen} remote={remote}",);
            if let Some(rtt) = rtt {
                write!(out, " rtt_us={}", signed_duration_us(rtt.effective)).unwrap();
            }
            out
        }
        ClientEvent::Warning { kind, message, .. } => {
            format!("warning kind={} message={message}", warning_kind(*kind))
        }
    }
}

fn write_common(out: &mut String, event: &str) {
    write!(out, "event={event}").unwrap();
}

fn write_seq(out: &mut String, seq: u32) {
    write!(out, " seq={seq}").unwrap();
}

fn write_remote(out: &mut String, remote: SocketAddr) {
    write!(out, " remote={remote}").unwrap();
}

fn write_token(out: &mut String, token: u64) {
    write!(out, " token={token:#x}").unwrap();
}

fn write_wall(out: &mut String, key: &str, wall: SystemTime) {
    if let Ok(duration) = wall.duration_since(UNIX_EPOCH) {
        write!(out, " {key}={}", duration.as_nanos()).unwrap();
    }
}

fn write_negotiated(out: &mut String, negotiated: &NegotiatedParams) {
    write!(
        out,
        " duration_ns={} interval_ns={} payload_length={}",
        negotiated.params.duration_ns, negotiated.params.interval_ns, negotiated.params.length
    )
    .unwrap();
}

fn write_rtt(out: &mut String, rtt: &RttSample) {
    write!(
        out,
        " raw_rtt_us={} effective_rtt_us={}",
        duration_us(rtt.raw),
        signed_duration_us(rtt.effective)
    )
    .unwrap();
    if let Some(adjusted) = rtt.adjusted {
        write!(out, " adjusted_rtt_us={}", signed_duration_us(adjusted)).unwrap();
    }
}

fn write_server_timing(out: &mut String, timing: Option<ServerTiming>) {
    if let Some(timing) = timing {
        write_optional_i64(out, "server_receive_wall_ns", timing.receive_wall_ns);
        write_optional_i64(out, "server_receive_mono_ns", timing.receive_mono_ns);
        write_optional_i64(out, "server_send_wall_ns", timing.send_wall_ns);
        write_optional_i64(out, "server_send_mono_ns", timing.send_mono_ns);
        write_optional_i64(out, "server_midpoint_wall_ns", timing.midpoint_wall_ns);
        write_optional_i64(out, "server_midpoint_mono_ns", timing.midpoint_mono_ns);
        if let Some(processing) = timing.processing {
            write!(out, " server_processing_us={}", duration_us(processing)).unwrap();
        }
    }
}

fn write_one_way(out: &mut String, one_way: Option<OneWayDelaySample>) {
    if let Some(one_way) = one_way {
        if let Some(value) = one_way.client_to_server {
            write!(out, " client_to_server_us={}", signed_duration_us(value)).unwrap();
        }
        if let Some(value) = one_way.server_to_client {
            write!(out, " server_to_client_us={}", signed_duration_us(value)).unwrap();
        }
    }
}

fn write_received_stats(out: &mut String, stats: Option<ReceivedStatsSample>) {
    if let Some(stats) = stats {
        if let Some(count) = stats.count {
            write!(out, " server_received_count={count}").unwrap();
        }
        if let Some(window) = stats.window {
            write!(out, " server_received_window={window:#x}").unwrap();
        }
    }
}

fn write_packet_meta(out: &mut String, meta: PacketMeta) {
    write_optional_u8(out, "traffic_class", meta.traffic_class);
    write_optional_u8(out, "dscp", meta.dscp);
    write_optional_u8(out, "ecn", meta.ecn);
    match meta.kernel_rx_timestamp {
        Some(timestamp) => write_wall(out, "kernel_rx_ns", timestamp),
        None => write!(out, " kernel_rx_ns=none").unwrap(),
    }
}

fn write_human_one_way(out: &mut String, one_way: Option<OneWayDelaySample>) {
    match one_way {
        Some(one_way) => {
            write!(
                out,
                "  rd={}  sd={}",
                format_optional_signed_duration(one_way.server_to_client),
                format_optional_signed_duration(one_way.client_to_server)
            )
            .unwrap();
        }
        None => out.push_str("  rd=n/a  sd=n/a"),
    }
}

fn write_human_received_stats(out: &mut String, stats: Option<ReceivedStatsSample>) {
    if let Some(stats) = stats {
        if let Some(count) = stats.count {
            write!(out, "  server_received={count}").unwrap();
        }
        if let Some(window) = stats.window {
            write!(out, "  server_window={window:#x}").unwrap();
        }
    }
}

fn format_human_ipdv(stats: Option<&HumanEventStats>, seq: u32) -> String {
    let Some(stats) = stats else {
        return "n/a".to_owned();
    };

    let pair = stats
        .ipdv_pairs
        .iter()
        .find(|pair| pair.current_seq == seq)
        .or_else(|| {
            stats
                .ipdv_pairs
                .iter()
                .find(|pair| pair.previous_seq == seq)
        });

    pair.map(|pair| format_duration(pair.rtt_ipdv))
        .unwrap_or_else(|| "n/a".to_owned())
}

fn write_optional_u8(out: &mut String, key: &str, value: Option<u8>) {
    match value {
        Some(value) => write!(out, " {key}={value}").unwrap(),
        None => write!(out, " {key}=none").unwrap(),
    }
}

fn write_optional_i64(out: &mut String, key: &str, value: Option<i64>) {
    if let Some(value) = value {
        write!(out, " {key}={value}").unwrap();
    }
}

fn duration_us(duration: Duration) -> u128 {
    duration.as_micros()
}

fn signed_duration_us(duration: SignedDuration) -> i128 {
    duration.as_micros()
}

fn format_optional_signed_duration(duration: Option<SignedDuration>) -> String {
    duration
        .map(format_signed_duration)
        .unwrap_or_else(|| "n/a".to_owned())
}

fn format_duration(duration: Duration) -> String {
    format_ns(duration.as_nanos() as f64)
}

fn format_signed_duration(duration: SignedDuration) -> String {
    format_signed_ns(duration.as_nanos() as f64)
}

fn format_signed_ns(ns: f64) -> String {
    if ns < 0.0 {
        format!("-{}", format_ns(-ns))
    } else {
        format_ns(ns)
    }
}

fn format_ns(ns: f64) -> String {
    if ns < 1_000.0 {
        format!("{ns:.0}ns")
    } else if ns < 1_000_000.0 {
        format!("{:.1}µs", ns / 1_000.0)
    } else if ns < 1_000_000_000.0 {
        format!("{:.1}ms", ns / 1_000_000.0)
    } else {
        format!("{:.3}s", ns / 1_000_000_000.0)
    }
}

fn warning_kind(kind: WarningKind) -> &'static str {
    match kind {
        WarningKind::MalformedOrUnrelatedPacket => "malformed_or_unrelated_packet",
        WarningKind::WrongToken => "wrong_token",
        WarningKind::UntrackedReply => "untracked_reply",
        _ => "unknown",
    }
}

fn escape_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace(' ', "\\s")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

#[cfg(test)]
mod tests {
    use super::*;
    use irtt_client::{ClientTimestamp, OneWayDelaySample, PacketMeta, ReceivedStatsSample};
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        time::{Instant, UNIX_EPOCH},
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

    fn reply_event() -> ClientEvent {
        ClientEvent::EchoReply {
            seq: 7,
            remote: test_remote(),
            sent_at: test_timestamp(Duration::from_secs(1)),
            received_at: test_timestamp(Duration::from_secs(1) + Duration::from_micros(1500)),
            rtt: RttSample {
                raw: Duration::from_micros(1500),
                adjusted: Some(SignedDuration::from_nanos(1_200_000)),
                effective: SignedDuration::from_nanos(1_200_000),
            },
            server_timing: Some(ServerTiming {
                receive_wall_ns: Some(1_000),
                receive_mono_ns: Some(2_000),
                send_wall_ns: Some(301_000),
                send_mono_ns: Some(302_000),
                midpoint_wall_ns: None,
                midpoint_mono_ns: None,
                processing: Some(Duration::from_micros(300)),
            }),
            one_way: Some(OneWayDelaySample {
                client_to_server: Some(SignedDuration::from_nanos(400_000)),
                server_to_client: Some(SignedDuration::from_nanos(500_000)),
            }),
            received_stats: Some(ReceivedStatsSample {
                count: Some(9),
                window: Some(0x7),
            }),
            bytes: 64,
            packet_meta: PacketMeta::default(),
        }
    }

    #[test]
    fn rtt_us_prints_only_effective_reply_rtt() {
        assert_eq!(
            format_event(&reply_event(), OutputMode::RttUs),
            Some("1200".to_owned())
        );
        assert_eq!(
            format_event(
                &ClientEvent::Warning {
                    kind: WarningKind::WrongToken,
                    message: "bad".to_owned(),
                    at: ClientTimestamp::now()
                },
                OutputMode::RttUs
            ),
            None
        );
    }

    #[test]
    fn machine_prints_stable_key_value_fields() {
        let line = format_event(&reply_event(), OutputMode::Machine).unwrap();
        assert!(line.starts_with("event=echo_reply "));
        assert!(line.contains("seq=7"));
        assert!(line.contains("remote=127.0.0.1:2112"));
        assert!(line.contains("client_send_wall_ns=1000000000"));
        assert!(line.contains("client_receive_wall_ns=1001500000"));
        assert!(line.contains("raw_rtt_us=1500"));
        assert!(line.contains("adjusted_rtt_us=1200"));
        assert!(line.contains("effective_rtt_us=1200"));
        assert!(line.contains("server_processing_us=300"));
        assert!(line.contains("server_received_count=9"));
    }

    #[test]
    fn simple_and_human_use_readable_reply_lines() {
        assert_eq!(
            format_event(&reply_event(), OutputMode::Simple),
            Some(
                "reply seq=7 remote=127.0.0.1:2112 rtt_us=1200 raw_rtt_us=1500 server_processing_us=300"
                    .to_owned()
            )
        );

        let human = format_event(&reply_event(), OutputMode::Human).unwrap();
        assert!(human.starts_with("seq=7  rtt=1.2ms"));
        assert!(human.contains("rd=500.0µs"));
        assert!(human.contains("sd=400.0µs"));
        assert!(human.contains("ipdv=n/a"));
        assert!(human.contains("proc=300.0µs"));
        assert!(!human.contains("rtt_us="));
    }

    #[test]
    fn echo_sent_is_not_formatted_for_stream_outputs() {
        let ts = test_timestamp(Duration::from_secs(1));
        let event = ClientEvent::EchoSent {
            seq: 1,
            remote: test_remote(),
            scheduled_at: ts.mono,
            sent_at: ts,
            bytes: 64,
            send_call: Duration::from_micros(10),
            timer_error: Duration::ZERO,
        };

        assert!(format_event(&event, OutputMode::RttUs).is_none());
        assert!(format_event(&event, OutputMode::Human).is_none());
        assert!(format_event(&event, OutputMode::Machine).is_none());
        assert!(format_event(&event, OutputMode::Simple).is_none());
    }
}
