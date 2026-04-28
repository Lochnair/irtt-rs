#![forbid(unsafe_code)]

use std::{
    fmt::Write as _,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use clap::{Parser, ValueEnum};
use irtt_client::{
    ClientConfig, ClientEvent, ClientTimestamp, NegotiatedParams, OneWayDelaySample, PacketMeta,
    ReceivedStatsSample, RttSample, ServerTiming, SocketConfig, WarningKind,
};
use irtt_proto::{Clock, ReceivedStats, StampAt};

const DEFAULT_RECV_TIMEOUT: Duration = Duration::from_millis(20);
const MAX_UDP_PAYLOAD_LENGTH: u32 = 65_507;

#[derive(Debug, Clone, Parser)]
#[command(name = "irtt-rs", about = "Minimal IRTT-compatible client")]
pub struct CliArgs {
    pub server: String,

    #[arg(long, default_value = "10s", value_parser = parse_duration)]
    pub duration: Duration,

    #[arg(long, default_value = "1s", value_parser = parse_duration)]
    pub interval: Duration,

    #[arg(long, default_value_t = 0, value_parser = parse_length)]
    pub length: u32,

    #[arg(long)]
    pub hmac: Option<String>,

    #[arg(long, value_enum, default_value_t = ClockArg::Both)]
    pub clock: ClockArg,

    #[arg(long, value_enum, default_value_t = TimestampArg::Both)]
    pub timestamps: TimestampArg,

    #[arg(long, value_enum, default_value_t = OutputMode::Simple)]
    pub output: OutputMode,
}

impl CliArgs {
    pub fn to_client_config(&self) -> ClientConfig {
        ClientConfig {
            server_addr: self.server.clone(),
            duration: Some(self.duration),
            interval: self.interval,
            length: self.length,
            received_stats: ReceivedStats::Both,
            stamp_at: self.timestamps.into(),
            clock: self.clock.into(),
            hmac_key: self.hmac.as_ref().map(|key| key.as_bytes().to_vec()),
            socket_config: SocketConfig {
                recv_timeout: Some(DEFAULT_RECV_TIMEOUT),
                ..SocketConfig::default()
            },
            ..ClientConfig::default()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ClockArg {
    Wall,
    Monotonic,
    Both,
}

impl From<ClockArg> for Clock {
    fn from(value: ClockArg) -> Self {
        match value {
            ClockArg::Wall => Self::Wall,
            ClockArg::Monotonic => Self::Monotonic,
            ClockArg::Both => Self::Both,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum TimestampArg {
    None,
    Send,
    Receive,
    Both,
}

impl From<TimestampArg> for StampAt {
    fn from(value: TimestampArg) -> Self {
        match value {
            TimestampArg::None => Self::None,
            TimestampArg::Send => Self::Send,
            TimestampArg::Receive => Self::Receive,
            TimestampArg::Both => Self::Both,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputMode {
    Machine,
    Simple,
    RttUs,
}

pub fn parse_duration(input: &str) -> Result<Duration, String> {
    let (number, unit) = split_duration(input)?;
    let value: u64 = number
        .parse()
        .map_err(|_| format!("invalid duration value {input:?}"))?;
    if value == 0 {
        return Err("duration must be greater than zero".to_owned());
    }
    match unit {
        "ms" => Ok(Duration::from_millis(value)),
        "s" => Ok(Duration::from_secs(value)),
        "m" => value
            .checked_mul(60)
            .map(Duration::from_secs)
            .ok_or_else(|| "duration is too large".to_owned()),
        _ => Err(format!(
            "unsupported duration unit {unit:?}; use ms, s, or m"
        )),
    }
}

fn split_duration(input: &str) -> Result<(&str, &str), String> {
    let split = input
        .find(|ch: char| !ch.is_ascii_digit())
        .ok_or_else(|| "duration must include a unit: ms, s, or m".to_owned())?;
    let (number, unit) = input.split_at(split);
    if number.is_empty() || unit.is_empty() {
        return Err("duration must include a positive value and unit".to_owned());
    }
    if number.starts_with('-') {
        return Err("duration must be greater than zero".to_owned());
    }
    Ok((number, unit))
}

pub fn parse_length(input: &str) -> Result<u32, String> {
    let length: u32 = input
        .parse()
        .map_err(|_| format!("invalid packet length {input:?}"))?;
    if length > MAX_UDP_PAYLOAD_LENGTH {
        return Err(format!("packet length must be <= {MAX_UDP_PAYLOAD_LENGTH}"));
    }
    Ok(length)
}

pub fn format_event(event: &ClientEvent, mode: OutputMode) -> Option<String> {
    match mode {
        OutputMode::RttUs => format_rtt_us(event),
        OutputMode::Machine => Some(format_machine(event)),
        OutputMode::Simple => Some(format_simple(event)),
    }
}

fn format_rtt_us(event: &ClientEvent) -> Option<String> {
    match event {
        ClientEvent::EchoReply { rtt, .. } => Some(duration_us(rtt.effective).to_string()),
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
            write_wall(&mut out, "at_ns", at.wall);
            write_negotiated(&mut out, negotiated);
        }
        ClientEvent::NoTestCompleted {
            remote,
            negotiated,
            at,
        } => {
            write_common(&mut out, "no_test_completed");
            write_remote(&mut out, *remote);
            write_wall(&mut out, "at_ns", at.wall);
            write_negotiated(&mut out, negotiated);
        }
        ClientEvent::SessionClosed { remote, token, at } => {
            write_common(&mut out, "session_closed");
            write_remote(&mut out, *remote);
            write_token(&mut out, *token);
            write_wall(&mut out, "at_ns", at.wall);
        }
        ClientEvent::EchoReply {
            seq,
            logical_seq,
            remote,
            sent_at,
            received_at,
            rtt,
            server_timing,
            one_way,
            received_stats,
            packet_meta,
        } => {
            write_common(&mut out, "echo_reply");
            write_seq(&mut out, *seq, Some(*logical_seq));
            write_remote(&mut out, *remote);
            write_wall(&mut out, "sent_ns", sent_at.wall);
            write_wall(&mut out, "received_ns", received_at.wall);
            write_rtt(&mut out, rtt);
            write_server_timing(&mut out, *server_timing);
            write_one_way(&mut out, *one_way);
            write_received_stats(&mut out, *received_stats);
            write_packet_meta(&mut out, *packet_meta);
        }
        ClientEvent::EchoLoss {
            seq,
            logical_seq,
            sent_at,
            ..
        } => {
            write_common(&mut out, "loss");
            write_seq(&mut out, *seq, Some(*logical_seq));
            write_wall(&mut out, "sent_ns", sent_at.wall);
            out.push_str(" warning=loss");
        }
        ClientEvent::DuplicateReply {
            seq,
            remote,
            received_at,
        } => {
            write_common(&mut out, "duplicate");
            write_seq(&mut out, *seq, None);
            write_remote(&mut out, *remote);
            write_wall(&mut out, "received_ns", received_at.wall);
            out.push_str(" warning=duplicate");
        }
        ClientEvent::LateReply {
            seq,
            logical_seq,
            highest_seen,
            remote,
            sent_at,
            received_at,
            rtt,
            server_timing,
            one_way,
            received_stats,
            packet_meta,
        } => {
            write_common(&mut out, "late");
            write_seq(&mut out, *seq, *logical_seq);
            write_remote(&mut out, *remote);
            write!(out, " highest_seen={highest_seen}").unwrap();
            if let Some(sent_at) = sent_at {
                write_wall(&mut out, "sent_ns", sent_at.wall);
            }
            write_wall(&mut out, "received_ns", received_at.wall);
            if let Some(rtt) = rtt {
                write_rtt(&mut out, rtt);
            }
            write_server_timing(&mut out, *server_timing);
            write_one_way(&mut out, *one_way);
            write_received_stats(&mut out, *received_stats);
            write_packet_meta(&mut out, *packet_meta);
            out.push_str(" warning=late");
        }
        ClientEvent::Warning { kind, message } => {
            write_common(&mut out, "warning");
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
        ClientEvent::EchoReply {
            seq,
            logical_seq,
            remote,
            rtt,
            server_timing,
            ..
        } => {
            let mut out = format!(
                "reply seq={seq} logical_seq={logical_seq} remote={remote} rtt_us={}",
                duration_us(rtt.effective)
            );
            if let Some(raw) = rtt.adjusted.map(|_| rtt.raw) {
                write!(out, " raw_rtt_us={}", duration_us(raw)).unwrap();
            }
            if let Some(processing) = server_timing.and_then(|timing| timing.processing) {
                write!(out, " server_processing_us={}", duration_us(processing)).unwrap();
            }
            out
        }
        ClientEvent::EchoLoss {
            seq, logical_seq, ..
        } => {
            format!("loss seq={seq} logical_seq={logical_seq}")
        }
        ClientEvent::DuplicateReply { seq, remote, .. } => {
            format!("duplicate seq={seq} remote={remote}")
        }
        ClientEvent::LateReply {
            seq,
            logical_seq,
            highest_seen,
            remote,
            rtt,
            ..
        } => {
            let mut out = format!(
                "late seq={seq} logical_seq={} highest_seen={highest_seen} remote={remote}",
                optional_u64(*logical_seq)
            );
            if let Some(rtt) = rtt {
                write!(out, " rtt_us={}", duration_us(rtt.effective)).unwrap();
            }
            out
        }
        ClientEvent::Warning { kind, message } => {
            format!("warning kind={} message={message}", warning_kind(*kind))
        }
    }
}

fn write_common(out: &mut String, event: &str) {
    write!(out, "event={event}").unwrap();
}

fn write_seq(out: &mut String, seq: u32, logical_seq: Option<u64>) {
    write!(out, " seq={seq}").unwrap();
    if let Some(logical_seq) = logical_seq {
        write!(out, " logical_seq={logical_seq}").unwrap();
    }
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
        " duration_ns={} interval_ns={} length={}",
        negotiated.params.duration_ns, negotiated.params.interval_ns, negotiated.params.length
    )
    .unwrap();
}

fn write_rtt(out: &mut String, rtt: &RttSample) {
    write!(
        out,
        " raw_rtt_us={} effective_rtt_us={}",
        duration_us(rtt.raw),
        duration_us(rtt.effective)
    )
    .unwrap();
    if let Some(adjusted) = rtt.adjusted {
        write!(out, " adjusted_rtt_us={}", duration_us(adjusted)).unwrap();
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
            write!(out, " client_to_server_us={}", duration_us(value)).unwrap();
        }
        if let Some(value) = one_way.server_to_client {
            write!(out, " server_to_client_us={}", duration_us(value)).unwrap();
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
    if let Some(traffic_class) = meta.traffic_class {
        write!(out, " traffic_class={traffic_class}").unwrap();
    }
    if let Some(dscp) = meta.dscp {
        write!(out, " dscp={dscp}").unwrap();
    }
    if let Some(ecn) = meta.ecn {
        write!(out, " ecn={ecn}").unwrap();
    }
    if let Some(timestamp) = meta.kernel_rx_timestamp {
        write_wall(out, "kernel_rx_ns", timestamp);
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

fn optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_owned())
}

fn warning_kind(kind: WarningKind) -> &'static str {
    match kind {
        WarningKind::MalformedOrUnrelatedPacket => "malformed_or_unrelated_packet",
        WarningKind::WrongToken => "wrong_token",
        WarningKind::UntrackedReply => "untracked_reply",
    }
}

fn escape_value(value: &str) -> String {
    value.replace('\\', "\\\\").replace(' ', "\\s")
}

pub fn test_timestamp(offset: Duration) -> ClientTimestamp {
    ClientTimestamp {
        wall: UNIX_EPOCH + offset,
        mono: Instant::now() + offset,
    }
}

pub fn test_remote() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 2112)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use irtt_client::{PacketMeta, RttSample};
    use irtt_proto::{Params, PROTOCOL_VERSION};

    fn parse(args: &[&str]) -> Result<CliArgs, clap::Error> {
        let mut argv = vec!["irtt-rs"];
        argv.extend_from_slice(args);
        CliArgs::try_parse_from(argv)
    }

    fn reply_event() -> ClientEvent {
        ClientEvent::EchoReply {
            seq: 7,
            logical_seq: 8,
            remote: test_remote(),
            sent_at: test_timestamp(Duration::from_secs(1)),
            received_at: test_timestamp(Duration::from_secs(1) + Duration::from_micros(1500)),
            rtt: RttSample {
                raw: Duration::from_micros(1500),
                adjusted: Some(Duration::from_micros(1200)),
                effective: Duration::from_micros(1200),
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
                client_to_server: Some(Duration::from_micros(400)),
                server_to_client: Some(Duration::from_micros(500)),
            }),
            received_stats: Some(ReceivedStatsSample {
                count: Some(9),
                window: Some(0x7),
            }),
            packet_meta: PacketMeta::default(),
        }
    }

    #[test]
    fn parses_valid_defaults() {
        let args = parse(&["127.0.0.1:2112"]).unwrap();
        assert_eq!(args.server, "127.0.0.1:2112");
        assert_eq!(args.duration, Duration::from_secs(10));
        assert_eq!(args.interval, Duration::from_secs(1));
        assert_eq!(args.length, 0);
        assert_eq!(args.hmac, None);
        assert_eq!(args.clock, ClockArg::Both);
        assert_eq!(args.timestamps, TimestampArg::Both);
        assert_eq!(args.output, OutputMode::Simple);

        let config = args.to_client_config();
        assert_eq!(config.duration, Some(Duration::from_secs(10)));
        assert_eq!(config.interval, Duration::from_secs(1));
        assert_eq!(config.length, 0);
        assert_eq!(config.stamp_at, StampAt::Both);
        assert_eq!(config.clock, Clock::Both);
    }

    #[test]
    fn parses_durations() {
        assert_eq!(parse_duration("1ms").unwrap(), Duration::from_millis(1));
        assert_eq!(parse_duration("2s").unwrap(), Duration::from_secs(2));
        assert_eq!(parse_duration("3m").unwrap(), Duration::from_secs(180));
    }

    #[test]
    fn rejects_invalid_duration() {
        assert!(parse(&["--duration", "0s", "127.0.0.1:2112"]).is_err());
        assert!(parse(&["--duration", "-1s", "127.0.0.1:2112"]).is_err());
        assert!(parse(&["--duration", "5", "127.0.0.1:2112"]).is_err());
        assert!(parse(&["--duration", "1h", "127.0.0.1:2112"]).is_err());
    }

    #[test]
    fn rejects_invalid_interval() {
        assert!(parse(&["--interval", "0ms", "127.0.0.1:2112"]).is_err());
        assert!(parse(&["--interval", "-1ms", "127.0.0.1:2112"]).is_err());
    }

    #[test]
    fn parses_output_clock_and_timestamp_modes() {
        let args = parse(&[
            "--output",
            "machine",
            "--clock",
            "wall",
            "--timestamps",
            "send",
            "127.0.0.1:2112",
        ])
        .unwrap();
        assert_eq!(args.output, OutputMode::Machine);
        assert_eq!(args.clock, ClockArg::Wall);
        assert_eq!(args.timestamps, TimestampArg::Send);

        let args = parse(&[
            "--output",
            "rtt-us",
            "--clock",
            "monotonic",
            "--timestamps",
            "receive",
            "127.0.0.1:2112",
        ])
        .unwrap();
        assert_eq!(args.output, OutputMode::RttUs);
        assert_eq!(args.clock, ClockArg::Monotonic);
        assert_eq!(args.timestamps, TimestampArg::Receive);
    }

    #[test]
    fn rejects_invalid_length() {
        assert!(parse(&["--length", "-1", "127.0.0.1:2112"]).is_err());
        assert!(parse(&["--length", "65508", "127.0.0.1:2112"]).is_err());
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
        assert!(line.contains("logical_seq=8"));
        assert!(line.contains("remote=127.0.0.1:2112"));
        assert!(line.contains("sent_ns=1000000000"));
        assert!(line.contains("received_ns=1001500000"));
        assert!(line.contains("raw_rtt_us=1500"));
        assert!(line.contains("adjusted_rtt_us=1200"));
        assert!(line.contains("effective_rtt_us=1200"));
        assert!(line.contains("server_processing_us=300"));
        assert!(line.contains("server_received_count=9"));
    }

    #[test]
    fn simple_prints_readable_reply_line() {
        let line = format_event(&reply_event(), OutputMode::Simple).unwrap();
        assert_eq!(
            line,
            "reply seq=7 logical_seq=8 remote=127.0.0.1:2112 rtt_us=1200 raw_rtt_us=1500 server_processing_us=300"
        );
    }

    #[test]
    fn warning_and_loss_variants_format_without_panicking() {
        let ts = test_timestamp(Duration::from_secs(1));
        let events = [
            ClientEvent::EchoLoss {
                seq: 1,
                logical_seq: 2,
                sent_at: ts,
                timeout_at: ts.mono + Duration::from_secs(1),
            },
            ClientEvent::DuplicateReply {
                seq: 3,
                remote: test_remote(),
                received_at: ts,
            },
            ClientEvent::LateReply {
                seq: 4,
                logical_seq: None,
                highest_seen: 9,
                remote: test_remote(),
                sent_at: None,
                received_at: ts,
                rtt: None,
                server_timing: None,
                one_way: None,
                received_stats: None,
                packet_meta: PacketMeta::default(),
            },
            ClientEvent::Warning {
                kind: WarningKind::UntrackedReply,
                message: "untracked reply".to_owned(),
            },
        ];

        for event in events {
            let machine = format_event(&event, OutputMode::Machine).unwrap();
            let simple = format_event(&event, OutputMode::Simple).unwrap();
            assert!(!machine.is_empty());
            assert!(!simple.is_empty());
        }
    }

    #[test]
    fn session_events_do_not_print_summary() {
        let event = ClientEvent::SessionStarted {
            remote: test_remote(),
            token: 0x1234,
            negotiated: NegotiatedParams {
                params: Params {
                    protocol_version: PROTOCOL_VERSION,
                    duration_ns: 10_000_000_000,
                    interval_ns: 1_000_000_000,
                    ..Params::default()
                },
            },
            at: test_timestamp(Duration::from_secs(1)),
        };
        let line = format_event(&event, OutputMode::Simple).unwrap();
        assert!(!line.contains("summary"));
        assert!(!line.contains("packets_sent"));
        assert!(!line.contains("packet_loss"));
    }
}
