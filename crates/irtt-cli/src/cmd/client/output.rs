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
    format_event_with_context(event, RenderContext::new(mode))
}

pub fn format_event_with_context(
    event: &ClientEvent,
    context: RenderContext<'_>,
) -> Option<String> {
    match context.mode {
        OutputMode::Human => {
            format_human_event_line(event, context.human_stats.cloned(), context.human_options)
        }
        OutputMode::Simple => format_simple_event(event),
        OutputMode::Machine => format_machine_event(event),
        OutputMode::RttUs => format_rtt_us_event(event),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RenderContext<'a> {
    pub mode: OutputMode,
    pub human_stats: Option<&'a HumanEventStats>,
    pub human_options: HumanOutputOptions,
}

impl RenderContext<'_> {
    pub fn new(mode: OutputMode) -> Self {
        Self {
            mode,
            human_stats: None,
            human_options: HumanOutputOptions::default(),
        }
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
    format_human_event_line(event, stats, options).unwrap_or_default()
}

fn format_human_event_line(
    event: &ClientEvent,
    stats: Option<HumanEventStats>,
    options: HumanOutputOptions,
) -> Option<String> {
    let stats = stats.as_ref();

    let line = match event {
        ClientEvent::SessionStarted { remote, token, .. } => {
            format!("session started  remote={remote}  token={token:#x}")
        }
        ClientEvent::NoTestCompleted { remote, .. } => {
            format!("no-test completed  remote={remote}")
        }
        ClientEvent::SessionClosed { remote, token, .. } => {
            format!("session closed  remote={remote}  token={token:#x}")
        }
        ClientEvent::EchoSent { .. } => return None,
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
    };
    Some(line)
}

fn format_rtt_us_event(event: &ClientEvent) -> Option<String> {
    match event {
        ClientEvent::EchoReply { rtt, .. } => Some(signed_duration_us(rtt.effective).to_string()),
        _ => None,
    }
}

fn format_machine_event(event: &ClientEvent) -> Option<String> {
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
        ClientEvent::EchoSent {
            seq,
            remote,
            sent_at,
            bytes,
            send_call,
            timer_error,
            ..
        } => {
            write_common(&mut out, "echo_sent");
            write_seq(&mut out, *seq);
            write_remote(&mut out, *remote);
            write_wall(&mut out, "client_send_wall_ns", sent_at.wall);
            write!(out, " bytes={bytes}").unwrap();
            write!(out, " send_call_us={}", duration_us(*send_call)).unwrap();
            write!(out, " timer_error_us={}", duration_us(*timer_error)).unwrap();
        }
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
    Some(out)
}

fn format_simple_event(event: &ClientEvent) -> Option<String> {
    let line = match event {
        ClientEvent::SessionStarted { remote, token, .. } => {
            format!("session started remote={remote} token={token:#x}")
        }
        ClientEvent::NoTestCompleted { remote, .. } => {
            format!("no-test completed remote={remote}")
        }
        ClientEvent::SessionClosed { remote, token, .. } => {
            format!("session closed remote={remote} token={token:#x}")
        }
        ClientEvent::EchoSent { .. } => return None,
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
    };
    Some(line)
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
    use irtt_proto::Params;
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

    fn negotiated() -> NegotiatedParams {
        let mut params = Params::with_protocol_defaults();
        params.duration_ns = 10_000_000_000;
        params.interval_ns = 250_000_000;
        params.length = 64;
        NegotiatedParams {
            params,
            restrictions: Vec::new(),
        }
    }

    fn sent_event() -> ClientEvent {
        let ts = test_timestamp(Duration::from_secs(1));
        ClientEvent::EchoSent {
            seq: 7,
            remote: test_remote(),
            scheduled_at: ts.mono,
            sent_at: ts,
            bytes: 64,
            send_call: Duration::from_micros(10),
            timer_error: Duration::from_micros(2),
        }
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

    fn negative_reply_event() -> ClientEvent {
        ClientEvent::EchoReply {
            seq: 8,
            remote: test_remote(),
            sent_at: test_timestamp(Duration::from_secs(2)),
            received_at: test_timestamp(Duration::from_secs(2) + Duration::from_micros(900)),
            rtt: RttSample {
                raw: Duration::from_micros(900),
                adjusted: Some(SignedDuration::from_nanos(-200_000)),
                effective: SignedDuration::from_nanos(-200_000),
            },
            server_timing: Some(ServerTiming {
                receive_wall_ns: Some(-1_000),
                receive_mono_ns: None,
                send_wall_ns: Some(1_100_000),
                send_mono_ns: None,
                midpoint_wall_ns: Some(500_000),
                midpoint_mono_ns: None,
                processing: Some(Duration::from_micros(1100)),
            }),
            one_way: Some(OneWayDelaySample {
                client_to_server: Some(SignedDuration::from_nanos(-300_000)),
                server_to_client: Some(SignedDuration::from_nanos(100_000)),
            }),
            received_stats: None,
            bytes: 64,
            packet_meta: PacketMeta {
                traffic_class: Some(1),
                dscp: Some(0),
                ecn: Some(1),
                kernel_rx_timestamp: Some(UNIX_EPOCH + Duration::from_secs(3)),
            },
        }
    }

    fn late_event_with_rtt() -> ClientEvent {
        ClientEvent::LateReply {
            seq: 3,
            highest_seen: 5,
            remote: test_remote(),
            sent_at: Some(test_timestamp(Duration::from_secs(1))),
            received_at: test_timestamp(Duration::from_secs(1) + Duration::from_micros(1700)),
            rtt: Some(RttSample {
                raw: Duration::from_micros(1700),
                adjusted: None,
                effective: SignedDuration::from_nanos(1_700_000),
            }),
            server_timing: None,
            one_way: Some(OneWayDelaySample {
                client_to_server: None,
                server_to_client: Some(SignedDuration::from_nanos(800_000)),
            }),
            received_stats: Some(ReceivedStatsSample {
                count: Some(11),
                window: Some(0xb),
            }),
            bytes: 64,
            packet_meta: PacketMeta::default(),
        }
    }

    fn late_event_without_rtt() -> ClientEvent {
        ClientEvent::LateReply {
            seq: 2,
            highest_seen: 5,
            remote: test_remote(),
            sent_at: None,
            received_at: test_timestamp(Duration::from_secs(1) + Duration::from_micros(1700)),
            rtt: None,
            server_timing: None,
            one_way: None,
            received_stats: None,
            bytes: 64,
            packet_meta: PacketMeta::default(),
        }
    }

    fn warning_event(message: &str) -> ClientEvent {
        ClientEvent::Warning {
            kind: WarningKind::WrongToken,
            message: message.to_owned(),
            at: test_timestamp(Duration::from_secs(4)),
        }
    }

    fn assert_line(event: &ClientEvent, mode: OutputMode, expected: &str) {
        assert_eq!(format_event(event, mode), Some(expected.to_owned()));
    }

    #[test]
    fn human_formats_rendered_events() {
        assert_line(
            &ClientEvent::SessionStarted {
                remote: test_remote(),
                token: 0xabc,
                negotiated: negotiated(),
                at: test_timestamp(Duration::from_secs(1)),
            },
            OutputMode::Human,
            "session started  remote=127.0.0.1:2112  token=0xabc",
        );
        assert_line(
            &ClientEvent::NoTestCompleted {
                remote: test_remote(),
                negotiated: negotiated(),
                at: test_timestamp(Duration::from_secs(1)),
            },
            OutputMode::Human,
            "no-test completed  remote=127.0.0.1:2112",
        );
        assert_line(
            &ClientEvent::SessionClosed {
                remote: test_remote(),
                token: 0xabc,
                at: test_timestamp(Duration::from_secs(1)),
            },
            OutputMode::Human,
            "session closed  remote=127.0.0.1:2112  token=0xabc",
        );
        assert_line(
            &reply_event(),
            OutputMode::Human,
            "seq=7  rtt=1.2ms  rd=500.0µs  sd=400.0µs  ipdv=n/a  proc=300.0µs",
        );
        assert_line(
            &ClientEvent::EchoLoss {
                seq: 9,
                sent_at: test_timestamp(Duration::from_secs(1)),
                timeout_at: Instant::now(),
            },
            OutputMode::Human,
            "loss  seq=9",
        );
        assert_line(
            &ClientEvent::DuplicateReply {
                seq: 7,
                remote: test_remote(),
                received_at: test_timestamp(Duration::from_secs(2)),
                bytes: 64,
            },
            OutputMode::Human,
            "duplicate  seq=7  remote=127.0.0.1:2112",
        );
        assert_line(
            &late_event_with_rtt(),
            OutputMode::Human,
            "late  seq=3  highest_seen=5  remote=127.0.0.1:2112  rtt=1.7ms  rd=800.0µs  sd=n/a  ipdv=n/a  server_received=11  server_window=0xb",
        );
        assert_line(
            &late_event_without_rtt(),
            OutputMode::Human,
            "late  seq=2  highest_seen=5  remote=127.0.0.1:2112",
        );
        assert_line(
            &warning_event("token mismatch"),
            OutputMode::Human,
            "warning  kind=wrong_token  message=token mismatch",
        );
        assert_eq!(format_event(&sent_event(), OutputMode::Human), None);
    }

    #[test]
    fn human_uses_stats_ipdv_decoration_and_verbose_stats() {
        let stats = HumanEventStats {
            contributed_sample: true,
            ipdv_pairs: vec![HumanIpdvPair {
                previous_seq: 6,
                current_seq: 7,
                rtt_ipdv: Duration::from_micros(50),
                send_ipdv: None,
                receive_ipdv: None,
            }],
        };
        assert_eq!(
            format_event_with_context(
                &reply_event(),
                RenderContext {
                    mode: OutputMode::Human,
                    human_stats: Some(&stats),
                    human_options: HumanOutputOptions { verbose: true },
                },
            ),
            Some(
                "seq=7  rtt=1.2ms  rd=500.0µs  sd=400.0µs  ipdv=50.0µs  proc=300.0µs  server_received=9  server_window=0x7"
                    .to_owned()
            )
        );
    }

    #[test]
    fn simple_formats_rendered_events() {
        assert_line(
            &reply_event(),
            OutputMode::Simple,
            "reply seq=7 remote=127.0.0.1:2112 rtt_us=1200 raw_rtt_us=1500 server_processing_us=300",
        );
        assert_line(
            &ClientEvent::EchoLoss {
                seq: 9,
                sent_at: test_timestamp(Duration::from_secs(1)),
                timeout_at: Instant::now(),
            },
            OutputMode::Simple,
            "loss seq=9",
        );
        assert_line(
            &ClientEvent::DuplicateReply {
                seq: 7,
                remote: test_remote(),
                received_at: test_timestamp(Duration::from_secs(2)),
                bytes: 64,
            },
            OutputMode::Simple,
            "duplicate seq=7 remote=127.0.0.1:2112",
        );
        assert_line(
            &late_event_with_rtt(),
            OutputMode::Simple,
            "late seq=3 highest_seen=5 remote=127.0.0.1:2112 rtt_us=1700",
        );
        assert_line(
            &late_event_without_rtt(),
            OutputMode::Simple,
            "late seq=2 highest_seen=5 remote=127.0.0.1:2112",
        );
        assert_line(
            &warning_event("token mismatch"),
            OutputMode::Simple,
            "warning kind=wrong_token message=token mismatch",
        );
        assert_eq!(format_event(&sent_event(), OutputMode::Simple), None);
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
        assert_eq!(
            format_event(&negative_reply_event(), OutputMode::RttUs),
            Some("-200".to_owned())
        );
    }

    #[test]
    fn machine_formats_lifecycle_events() {
        assert_line(
            &ClientEvent::SessionStarted {
                remote: test_remote(),
                token: 0xabc,
                negotiated: negotiated(),
                at: test_timestamp(Duration::from_secs(1)),
            },
            OutputMode::Machine,
            "event=session_started remote=127.0.0.1:2112 token=0xabc event_wall_ns=1000000000 duration_ns=10000000000 interval_ns=250000000 payload_length=64",
        );
        assert_line(
            &ClientEvent::NoTestCompleted {
                remote: test_remote(),
                negotiated: negotiated(),
                at: test_timestamp(Duration::from_secs(1)),
            },
            OutputMode::Machine,
            "event=no_test_completed remote=127.0.0.1:2112 event_wall_ns=1000000000 duration_ns=10000000000 interval_ns=250000000 payload_length=64",
        );
        assert_line(
            &ClientEvent::SessionClosed {
                remote: test_remote(),
                token: 0xabc,
                at: test_timestamp(Duration::from_secs(1)),
            },
            OutputMode::Machine,
            "event=session_closed remote=127.0.0.1:2112 token=0xabc event_wall_ns=1000000000",
        );
    }

    #[test]
    fn machine_formats_reply_and_diagnostic_events() {
        assert_line(
            &sent_event(),
            OutputMode::Machine,
            "event=echo_sent seq=7 remote=127.0.0.1:2112 client_send_wall_ns=1000000000 bytes=64 send_call_us=10 timer_error_us=2",
        );
        assert_line(
            &reply_event(),
            OutputMode::Machine,
            "event=echo_reply seq=7 remote=127.0.0.1:2112 client_send_wall_ns=1000000000 client_receive_wall_ns=1001500000 raw_rtt_us=1500 effective_rtt_us=1200 adjusted_rtt_us=1200 server_receive_wall_ns=1000 server_receive_mono_ns=2000 server_send_wall_ns=301000 server_send_mono_ns=302000 server_processing_us=300 client_to_server_us=400 server_to_client_us=500 server_received_count=9 server_received_window=0x7 traffic_class=none dscp=none ecn=none kernel_rx_ns=none",
        );
        assert_line(
            &ClientEvent::EchoLoss {
                seq: 9,
                sent_at: test_timestamp(Duration::from_secs(1)),
                timeout_at: Instant::now(),
            },
            OutputMode::Machine,
            "event=loss seq=9 client_send_wall_ns=1000000000 warning=loss",
        );
        assert_line(
            &ClientEvent::DuplicateReply {
                seq: 7,
                remote: test_remote(),
                received_at: test_timestamp(Duration::from_secs(2)),
                bytes: 64,
            },
            OutputMode::Machine,
            "event=duplicate seq=7 remote=127.0.0.1:2112 client_receive_wall_ns=2000000000 warning=duplicate",
        );
        assert_line(
            &late_event_with_rtt(),
            OutputMode::Machine,
            "event=late seq=3 remote=127.0.0.1:2112 highest_seen=5 client_send_wall_ns=1000000000 client_receive_wall_ns=1001700000 raw_rtt_us=1700 effective_rtt_us=1700 server_to_client_us=800 server_received_count=11 server_received_window=0xb traffic_class=none dscp=none ecn=none kernel_rx_ns=none warning=late",
        );
        assert_line(
            &late_event_without_rtt(),
            OutputMode::Machine,
            "event=late seq=2 remote=127.0.0.1:2112 highest_seen=5 client_receive_wall_ns=1001700000 traffic_class=none dscp=none ecn=none kernel_rx_ns=none warning=late",
        );
    }

    #[test]
    fn machine_escapes_warning_messages() {
        assert_line(
            &warning_event("bad token\tline one\nline two\\tail"),
            OutputMode::Machine,
            "event=warning at=4000000000 warning_kind=wrong_token message=bad\\stoken\\tline\\sone\\nline\\stwo\\\\tail",
        );
    }

    #[test]
    fn machine_preserves_negative_signed_timing_values() {
        assert_line(
            &negative_reply_event(),
            OutputMode::Machine,
            "event=echo_reply seq=8 remote=127.0.0.1:2112 client_send_wall_ns=2000000000 client_receive_wall_ns=2000900000 raw_rtt_us=900 effective_rtt_us=-200 adjusted_rtt_us=-200 server_receive_wall_ns=-1000 server_send_wall_ns=1100000 server_midpoint_wall_ns=500000 server_processing_us=1100 client_to_server_us=-300 server_to_client_us=100 traffic_class=1 dscp=0 ecn=1 kernel_rx_ns=3000000000",
        );
    }

    #[test]
    fn echo_sent_is_suppressed_except_machine_output() {
        let event = sent_event();
        assert!(format_event(&event, OutputMode::RttUs).is_none());
        assert!(format_event(&event, OutputMode::Human).is_none());
        assert!(format_event(&event, OutputMode::Simple).is_none());
        assert!(format_event(&event, OutputMode::Machine).is_some());
    }
}
