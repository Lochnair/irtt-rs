#![forbid(unsafe_code)]

#[cfg(feature = "stats")]
pub mod summary;

use std::{
    fmt::Write as _,
    net::SocketAddr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use clap::{Parser, ValueEnum};
use irtt_client::{
    ClientConfig, ClientEvent, NegotiatedParams, NegotiationPolicy, OneWayDelaySample, PacketMeta,
    ReceivedStatsSample, RttSample, ServerTiming, SignedDuration, SocketConfig, WarningKind,
    MAX_DSCP_CODEPOINT, MAX_SERVER_FILL_BYTES, MAX_TTL, MAX_UDP_PAYLOAD_LENGTH,
};
use irtt_proto::{Clock, ReceivedStats, StampAt};

const DEFAULT_RECV_TIMEOUT: Duration = Duration::from_millis(20);
#[derive(Debug, Clone, Parser)]
#[command(name = "irtt-rs", about = "Minimal IRTT-compatible client")]
pub struct CliArgs {
    /// Server address or host, with optional port.
    pub server: String,

    #[arg(
        long,
        default_value = "10s",
        value_parser = parse_test_duration,
        help = "Test duration; use 0 for continuous mode",
        long_help = "Test duration; use 0 for continuous mode.\n\nFinite runs retain exact statistics for final summaries. Continuous mode uses bounded-memory running statistics and prints a final summary only when interrupted."
    )]
    pub duration: Duration,

    /// Probe interval.
    #[arg(long, default_value = "1s", value_parser = parse_duration)]
    pub interval: Duration,

    /// UDP payload length.
    #[arg(long, default_value_t = 0, value_parser = parse_length)]
    pub length: u32,

    /// HMAC key.
    #[arg(long)]
    pub hmac: Option<String>,

    /// Clock mode to request.
    #[arg(long, value_enum, default_value_t = ClockArg::Both)]
    pub clock: ClockArg,

    /// Timestamp mode to request.
    #[arg(
        long = "tstamp",
        visible_alias = "timestamps",
        value_enum,
        value_name = "MODE",
        default_value_t = TimestampArg::Both
    )]
    pub tstamp: TimestampArg,

    /// Received-stats mode to request.
    #[arg(
        long = "stats",
        value_enum,
        default_value_t = ReceivedStatsArg::Both,
        help = "Server received-stats negotiation mode"
    )]
    pub stats: ReceivedStatsArg,

    /// Server payload fill string to request, up to 32 bytes.
    #[arg(
        long = "sfill",
        visible_alias = "server-fill",
        value_name = "STRING",
        value_parser = parse_server_fill
    )]
    pub server_fill: Option<String>,

    /// DSCP codepoint to request; this is not a raw TOS or Traffic Class byte.
    #[arg(long, default_value_t = 0, value_name = "0..=63", value_parser = parse_dscp)]
    pub dscp: u8,

    /// Local outgoing IPv4 TTL or IPv6 unicast hop limit; not negotiated.
    #[arg(long, value_name = "1..=255", value_parser = parse_ttl)]
    pub ttl: Option<u32>,

    /// Accept safe server restrictions during negotiation.
    #[arg(long)]
    pub loose: bool,

    /// Output format: human, simple, machine, or rtt-us.
    #[arg(long, value_enum, default_value_t = OutputMode::Human)]
    pub output: OutputMode,

    /// Include extra fields in human output.
    #[arg(long)]
    pub verbose: bool,
}

impl CliArgs {
    pub fn to_client_config(&self) -> ClientConfig {
        ClientConfig {
            server_addr: self.server.clone(),
            duration: (!self.is_continuous()).then_some(self.duration),
            interval: self.interval,
            length: self.length,
            received_stats: self.stats.into(),
            stamp_at: self.timestamp_mode().into(),
            clock: self.clock.into(),
            dscp: self.dscp,
            hmac_key: self.hmac.as_ref().map(|key| key.as_bytes().to_vec()),
            server_fill: self.server_fill.clone(),
            negotiation_policy: if self.loose {
                NegotiationPolicy::Loose
            } else {
                NegotiationPolicy::Strict
            },
            socket_config: SocketConfig {
                ttl: self.ttl,
                recv_timeout: Some(DEFAULT_RECV_TIMEOUT),
                ..SocketConfig::default()
            },
            ..ClientConfig::default()
        }
    }

    pub fn is_continuous(&self) -> bool {
        self.duration == Duration::ZERO
    }

    pub fn timestamp_mode(&self) -> TimestampArg {
        self.tstamp
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
    Midpoint,
}

impl From<TimestampArg> for StampAt {
    fn from(value: TimestampArg) -> Self {
        match value {
            TimestampArg::None => Self::None,
            TimestampArg::Send => Self::Send,
            TimestampArg::Receive => Self::Receive,
            TimestampArg::Both => Self::Both,
            TimestampArg::Midpoint => Self::Midpoint,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ReceivedStatsArg {
    None,
    Count,
    Window,
    Both,
}

impl From<ReceivedStatsArg> for ReceivedStats {
    fn from(value: ReceivedStatsArg) -> Self {
        match value {
            ReceivedStatsArg::None => Self::None,
            ReceivedStatsArg::Count => Self::Count,
            ReceivedStatsArg::Window => Self::Window,
            ReceivedStatsArg::Both => Self::Both,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputMode {
    /// Readable terminal output with a final summary.
    Human,
    /// Parseable full event fields.
    Machine,
    /// Simple key=value-ish event stream.
    Simple,
    /// RTT microseconds only.
    RttUs,
}

impl OutputMode {
    pub fn prints_summary(self) -> bool {
        matches!(self, Self::Human)
    }
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

pub fn parse_test_duration(input: &str) -> Result<Duration, String> {
    if input == "0" {
        return Ok(Duration::ZERO);
    }
    let (number, unit) = split_duration(input)?;
    let value: u64 = number
        .parse()
        .map_err(|_| format!("invalid duration value {input:?}"))?;
    if value == 0 {
        return Ok(Duration::ZERO);
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

pub fn parse_server_fill(input: &str) -> Result<String, String> {
    if input.is_empty() {
        return Err("server fill must not be empty".to_owned());
    }
    let len = input.len();
    if len > MAX_SERVER_FILL_BYTES {
        return Err(format!(
            "server fill must be <= {MAX_SERVER_FILL_BYTES} bytes, got {len}"
        ));
    }
    Ok(input.to_owned())
}

pub fn parse_dscp(input: &str) -> Result<u8, String> {
    let value: u8 = input
        .parse()
        .map_err(|_| format!("invalid DSCP codepoint {input:?}"))?;
    if value > MAX_DSCP_CODEPOINT {
        return Err(format!("DSCP codepoint must be <= {MAX_DSCP_CODEPOINT}"));
    }
    Ok(value)
}

pub fn parse_ttl(input: &str) -> Result<u32, String> {
    let value: u32 = input
        .parse()
        .map_err(|_| format!("invalid TTL/hop limit {input:?}"))?;
    if value == 0 || value > MAX_TTL {
        return Err(format!("TTL/hop limit must be in range 1..={MAX_TTL}"));
    }
    Ok(value)
}

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
                format_optional_duration(one_way.server_to_client),
                format_optional_duration(one_way.client_to_server)
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

fn format_optional_duration(duration: Option<Duration>) -> String {
    duration
        .map(format_duration)
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
    use clap::{CommandFactory, Parser};
    use irtt_client::{ClientTimestamp, PacketMeta, RttSample, SignedDuration};
    use irtt_proto::{Params, PROTOCOL_VERSION};
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        time::Instant,
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

    fn parse(args: &[&str]) -> Result<CliArgs, clap::Error> {
        let mut argv = vec!["irtt-rs"];
        argv.extend_from_slice(args);
        CliArgs::try_parse_from(argv)
    }

    fn reply_event() -> ClientEvent {
        reply_event_with_meta(PacketMeta::default())
    }

    fn reply_event_with_meta(packet_meta: PacketMeta) -> ClientEvent {
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
                client_to_server: Some(Duration::from_micros(400)),
                server_to_client: Some(Duration::from_micros(500)),
            }),
            received_stats: Some(ReceivedStatsSample {
                count: Some(9),
                window: Some(0x7),
            }),
            bytes: 64,
            packet_meta,
        }
    }

    #[cfg(feature = "stats")]
    fn negative_adjusted_reply_event() -> ClientEvent {
        ClientEvent::EchoReply {
            seq: 8,
            remote: test_remote(),
            sent_at: test_timestamp(Duration::from_secs(1)),
            received_at: test_timestamp(Duration::from_secs(1) + Duration::from_micros(500)),
            rtt: RttSample {
                raw: Duration::from_micros(500),
                adjusted: Some(SignedDuration::from_nanos(-1_500_000)),
                effective: SignedDuration::from_nanos(-1_500_000),
            },
            server_timing: Some(ServerTiming {
                receive_wall_ns: Some(1_000),
                receive_mono_ns: Some(2_000),
                send_wall_ns: Some(2_001_000),
                send_mono_ns: Some(2_002_000),
                midpoint_wall_ns: None,
                midpoint_mono_ns: None,
                processing: Some(Duration::from_micros(2_000)),
            }),
            one_way: None,
            received_stats: None,
            bytes: 64,
            packet_meta: PacketMeta::default(),
        }
    }

    fn late_event_with_meta(packet_meta: PacketMeta) -> ClientEvent {
        ClientEvent::LateReply {
            seq: 4,
            highest_seen: 9,
            remote: test_remote(),
            sent_at: None,
            received_at: test_timestamp(Duration::from_secs(1)),
            rtt: None,
            server_timing: None,
            one_way: None,
            received_stats: None,
            bytes: 64,
            packet_meta,
        }
    }

    fn packet_meta(traffic_class: u8, dscp: u8, ecn: u8) -> PacketMeta {
        PacketMeta {
            traffic_class: Some(traffic_class),
            dscp: Some(dscp),
            ecn: Some(ecn),
            kernel_rx_timestamp: None,
        }
    }

    fn packet_meta_with_timestamp(timestamp: Option<SystemTime>) -> PacketMeta {
        PacketMeta {
            kernel_rx_timestamp: timestamp,
            ..PacketMeta::default()
        }
    }

    fn assert_machine_packet_meta(
        line: &str,
        traffic_class: &str,
        dscp: &str,
        ecn: &str,
        kernel_rx_ns: &str,
    ) {
        assert!(line.contains(&format!("traffic_class={traffic_class}")));
        assert!(line.contains(&format!("dscp={dscp}")));
        assert!(line.contains(&format!("ecn={ecn}")));
        assert!(line.contains(&format!("kernel_rx_ns={kernel_rx_ns}")));
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
        assert_eq!(args.tstamp, TimestampArg::Both);
        assert_eq!(args.timestamp_mode(), TimestampArg::Both);
        assert_eq!(args.stats, ReceivedStatsArg::Both);
        assert_eq!(args.server_fill, None);
        assert_eq!(args.dscp, 0);
        assert_eq!(args.ttl, None);
        assert!(!args.loose);
        assert_eq!(args.output, OutputMode::Human);

        let config = args.to_client_config();
        assert_eq!(config.duration, Some(Duration::from_secs(10)));
        assert_eq!(config.interval, Duration::from_secs(1));
        assert_eq!(config.length, 0);
        assert_eq!(config.received_stats, ReceivedStats::Both);
        assert_eq!(config.stamp_at, StampAt::Both);
        assert_eq!(config.clock, Clock::Both);
        assert_eq!(config.dscp, 0);
        assert_eq!(config.socket_config.ttl, None);
        assert_eq!(config.server_fill, None);
        assert_eq!(config.negotiation_policy, NegotiationPolicy::Strict);
        assert!(!args.is_continuous());
    }

    #[test]
    fn parses_durations() {
        assert_eq!(parse_duration("1ms").unwrap(), Duration::from_millis(1));
        assert_eq!(parse_duration("2s").unwrap(), Duration::from_secs(2));
        assert_eq!(parse_duration("3m").unwrap(), Duration::from_secs(180));
        assert_eq!(parse_test_duration("0").unwrap(), Duration::ZERO);
        assert_eq!(parse_test_duration("0s").unwrap(), Duration::ZERO);
        assert_eq!(
            parse_test_duration("1ms").unwrap(),
            Duration::from_millis(1)
        );
    }

    #[test]
    fn cli_options_map_to_client_config() {
        let args = parse(&["--duration", "0", "127.0.0.1:2112"]).unwrap();
        assert!(args.is_continuous());
        assert_eq!(args.duration, Duration::ZERO);
        assert_eq!(args.to_client_config().duration, None);

        let args = parse(&[
            "--output",
            "machine",
            "--clock",
            "wall",
            "--tstamp",
            "send",
            "127.0.0.1:2112",
        ])
        .unwrap();
        assert_eq!(args.output, OutputMode::Machine);
        assert_eq!(args.clock, ClockArg::Wall);
        assert_eq!(args.timestamp_mode(), TimestampArg::Send);
        assert_eq!(args.to_client_config().stamp_at, StampAt::Send);
        assert_eq!(args.to_client_config().clock, Clock::Wall);

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
        assert_eq!(args.timestamp_mode(), TimestampArg::Receive);
        assert_eq!(args.to_client_config().stamp_at, StampAt::Receive);
        assert_eq!(args.to_client_config().clock, Clock::Monotonic);

        let args = parse(&["--output", "human", "127.0.0.1:2112"]).unwrap();
        assert_eq!(args.output, OutputMode::Human);

        for (value, expected) in [
            ("none", StampAt::None),
            ("send", StampAt::Send),
            ("receive", StampAt::Receive),
            ("both", StampAt::Both),
            ("midpoint", StampAt::Midpoint),
        ] {
            let args = parse(&["--tstamp", value, "127.0.0.1:2112"]).unwrap();
            assert_eq!(args.to_client_config().stamp_at, expected);
        }

        let args = parse(&["--timestamps", "midpoint", "127.0.0.1:2112"]).unwrap();
        assert_eq!(args.timestamp_mode(), TimestampArg::Midpoint);
        assert_eq!(args.to_client_config().stamp_at, StampAt::Midpoint);

        for (value, expected) in [
            ("none", ReceivedStats::None),
            ("count", ReceivedStats::Count),
            ("window", ReceivedStats::Window),
            ("both", ReceivedStats::Both),
        ] {
            let args = parse(&["--stats", value, "127.0.0.1:2112"]).unwrap();
            assert_eq!(args.to_client_config().received_stats, expected);
        }

        let args = parse(&["127.0.0.1:2112"]).unwrap();
        assert_eq!(args.to_client_config().received_stats, ReceivedStats::Both);

        let args = parse(&["--sfill", "abc", "127.0.0.1:2112"]).unwrap();
        assert_eq!(args.to_client_config().server_fill.as_deref(), Some("abc"));

        let args = parse(&["--server-fill", "abc", "127.0.0.1:2112"]).unwrap();
        assert_eq!(args.to_client_config().server_fill.as_deref(), Some("abc"));

        let max = "0123456789abcdef0123456789abcdef";
        let args = parse(&["--sfill", max, "127.0.0.1:2112"]).unwrap();
        assert_eq!(args.to_client_config().server_fill.as_deref(), Some(max));

        for value in ["0", "46", "63"] {
            let args = parse(&["--dscp", value, "127.0.0.1:2112"]).unwrap();
            assert_eq!(args.to_client_config().dscp, value.parse::<u8>().unwrap());
        }

        for value in ["1", "64", "255"] {
            let args = parse(&["--ttl", value, "127.0.0.1:2112"]).unwrap();
            assert_eq!(
                args.to_client_config().socket_config.ttl,
                Some(value.parse::<u32>().unwrap())
            );
        }

        let args = parse(&["--length", "1472", "127.0.0.1:2112"]).unwrap();
        assert_eq!(args.length, 1472);
        assert_eq!(args.to_client_config().length, 1472);

        let args = parse(&["127.0.0.1:2112"]).unwrap();
        assert_eq!(args.length, 0);
        assert_eq!(args.to_client_config().length, 0);

        let args = parse(&["127.0.0.1:2112"]).unwrap();
        assert_eq!(
            args.to_client_config().negotiation_policy,
            NegotiationPolicy::Strict
        );

        let args = parse(&["--loose", "127.0.0.1:2112"]).unwrap();
        assert_eq!(
            args.to_client_config().negotiation_policy,
            NegotiationPolicy::Loose
        );
    }

    #[test]
    fn rejects_duplicate_timestamp_options() {
        assert!(parse(&[
            "--tstamp",
            "send",
            "--timestamps",
            "receive",
            "127.0.0.1:2112"
        ])
        .is_err());
    }

    #[test]
    fn help_lists_advanced_protocol_options() {
        let help = CliArgs::command().render_help().to_string();
        assert!(help.contains("--tstamp <MODE>"));
        assert!(help.contains("--stats <STATS>"));
        assert!(help.contains("--sfill <STRING>"));
        assert!(help.contains("--dscp <0..=63>"));
        assert!(help.contains("--ttl <1..=255>"));
        assert!(help.contains("--loose"));
        assert!(help.contains("DSCP codepoint"));
        assert!(help.contains("up to 32 bytes"));
        assert!(help.contains("not negotiated"));
    }

    #[test]
    fn rejects_invalid_argument_values() {
        for args in [
            &["--duration", "-1s", "127.0.0.1:2112"][..],
            &["--duration", "5", "127.0.0.1:2112"],
            &["--duration", "1h", "127.0.0.1:2112"],
            &["--interval", "0ms", "127.0.0.1:2112"],
            &["--interval", "-1ms", "127.0.0.1:2112"],
            &["--sfill", "", "127.0.0.1:2112"],
            &[
                "--sfill",
                "0123456789abcdef0123456789abcdefx",
                "127.0.0.1:2112",
            ],
            &["--dscp", "64", "127.0.0.1:2112"],
            &["--dscp", "-1", "127.0.0.1:2112"],
            &["--dscp", "abc", "127.0.0.1:2112"],
            &["--ttl", "0", "127.0.0.1:2112"],
            &["--ttl", "256", "127.0.0.1:2112"],
            &["--ttl", "-1", "127.0.0.1:2112"],
            &["--ttl", "abc", "127.0.0.1:2112"],
            &["--length", "-1", "127.0.0.1:2112"],
            &["--length", "65508", "127.0.0.1:2112"],
        ] {
            assert!(parse(args).is_err(), "expected parse failure for {args:?}");
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
    #[cfg(feature = "stats")]
    fn streamed_and_summary_rtt_use_signed_effective_policy() {
        let event = negative_adjusted_reply_event();

        assert_eq!(
            format_event(&event, OutputMode::RttUs),
            Some("-1500".to_owned())
        );

        let machine = format_event(&event, OutputMode::Machine).unwrap();
        assert!(machine.contains("raw_rtt_us=500"));
        assert!(machine.contains("effective_rtt_us=-1500"));
        assert!(machine.contains("adjusted_rtt_us=-1500"));

        let mut collector = irtt_stats::StatsCollector::new(irtt_stats::StatsConfig::finite());
        collector.process(&event);
        let summary = collector.snapshot();
        assert_eq!(summary.rtt.primary.min_ns, Some(-1_500_000));
        assert_eq!(summary.rtt.adjusted.min_ns, Some(-1_500_000));
    }

    #[test]
    fn machine_prints_stable_key_value_fields() {
        let line = format_event(&reply_event(), OutputMode::Machine).unwrap();
        assert!(line.starts_with("event=echo_reply "));
        assert!(line.contains("seq=7"));
        assert!(line.contains("remote=127.0.0.1:2112"));
        assert!(line.contains("client_send_wall_ns=1000000000"));
        assert!(line.contains("client_receive_wall_ns=1001500000"));
        assert!(!line.contains(" sent_ns="));
        assert!(!line.contains(" received_ns="));
        assert!(line.contains("raw_rtt_us=1500"));
        assert!(line.contains("adjusted_rtt_us=1200"));
        assert!(line.contains("effective_rtt_us=1200"));
        assert!(line.contains("server_processing_us=300"));
        assert!(line.contains("server_received_count=9"));
    }

    #[test]
    fn machine_echo_reply_metadata_prints_observed_and_unavailable_values() {
        let cases = [
            (PacketMeta::default(), ("none", "none", "none", "none")),
            (packet_meta(0, 0, 0), ("0", "0", "0", "none")),
            (packet_meta(186, 46, 2), ("186", "46", "2", "none")),
            (
                packet_meta_with_timestamp(Some(UNIX_EPOCH + Duration::new(1, 234))),
                ("none", "none", "none", "1000000234"),
            ),
        ];

        for (packet_meta, (traffic_class, dscp, ecn, kernel_rx_ns)) in cases {
            let line =
                format_event(&reply_event_with_meta(packet_meta), OutputMode::Machine).unwrap();

            assert_machine_packet_meta(&line, traffic_class, dscp, ecn, kernel_rx_ns);
        }
    }

    #[test]
    fn machine_late_reply_metadata_observed_values_prints_values() {
        let line = format_event(
            &late_event_with_meta(packet_meta(186, 46, 2)),
            OutputMode::Machine,
        )
        .unwrap();

        assert!(line.starts_with("event=late "));
        assert_machine_packet_meta(&line, "186", "46", "2", "none");
    }

    #[test]
    fn simple_prints_readable_reply_line() {
        let line = format_event(&reply_event(), OutputMode::Simple).unwrap();
        assert_eq!(
            line,
            "reply seq=7 remote=127.0.0.1:2112 rtt_us=1200 raw_rtt_us=1500 server_processing_us=300"
        );
    }

    #[test]
    fn human_uses_readable_per_event_lines() {
        let line = format_event(&reply_event(), OutputMode::Human).unwrap();
        assert!(line.starts_with("seq=7  rtt=1.2ms"));
        assert!(line.contains("rd=500.0µs"));
        assert!(line.contains("sd=400.0µs"));
        assert!(line.contains("ipdv=n/a"));
        assert!(line.contains("proc=300.0µs"));
        assert!(!line.contains("server_received="));
        assert!(!line.contains("server_window="));
        assert!(!line.contains("rtt_us="));
        assert_ne!(
            line,
            format_event(&reply_event(), OutputMode::Simple).unwrap()
        );
        assert!(OutputMode::Human.prints_summary());
        assert!(!OutputMode::Simple.prints_summary());
        assert!(!OutputMode::Machine.prints_summary());
        assert!(!OutputMode::RttUs.prints_summary());
    }

    #[test]
    fn verbose_human_reply_includes_extra_fields() {
        let line = format_human_event_with_options(
            &reply_event(),
            None,
            HumanOutputOptions { verbose: true },
        );

        assert!(line.contains("server_received=9"));
        assert!(line.contains("server_window=0x7"));
    }

    #[test]
    fn human_reply_uses_supplied_ipdv_update() {
        let line = format_human_event(
            &reply_event(),
            Some(HumanEventStats {
                contributed_sample: true,
                ipdv_pairs: vec![HumanIpdvPair {
                    previous_seq: 7,
                    current_seq: 8,
                    rtt_ipdv: Duration::from_micros(47),
                    send_ipdv: None,
                    receive_ipdv: None,
                }],
            }),
        );

        assert!(line.contains("ipdv=47.0µs"));
    }

    #[test]
    fn human_reply_marks_missing_one_way_delay_unavailable() {
        let ClientEvent::EchoReply {
            seq,
            remote,
            sent_at,
            received_at,
            rtt,
            server_timing,
            received_stats,
            bytes,
            packet_meta,
            ..
        } = reply_event()
        else {
            unreachable!();
        };
        let event = ClientEvent::EchoReply {
            seq,
            remote,
            sent_at,
            received_at,
            rtt,
            server_timing,
            one_way: None,
            received_stats,
            bytes,
            packet_meta,
        };

        let line = format_human_event(&event, None);

        assert!(line.contains("rd=n/a"));
        assert!(line.contains("sd=n/a"));
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

    #[test]
    fn warning_and_loss_variants_format_without_panicking() {
        let ts = test_timestamp(Duration::from_secs(1));
        let events = [
            ClientEvent::EchoLoss {
                seq: 1,
                sent_at: ts,
                timeout_at: ts.mono + Duration::from_secs(1),
            },
            ClientEvent::DuplicateReply {
                seq: 3,
                remote: test_remote(),
                received_at: ts,
                bytes: 64,
            },
            ClientEvent::LateReply {
                seq: 4,
                highest_seen: 9,
                remote: test_remote(),
                sent_at: None,
                received_at: ts,
                rtt: None,
                server_timing: None,
                one_way: None,
                received_stats: None,
                bytes: 64,
                packet_meta: PacketMeta::default(),
            },
            ClientEvent::Warning {
                kind: WarningKind::UntrackedReply,
                message: "untracked reply".to_owned(),
                at: ClientTimestamp::now(),
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
                restrictions: vec![],
            },
            at: test_timestamp(Duration::from_secs(1)),
        };
        let line = format_event(&event, OutputMode::Simple).unwrap();
        assert!(!line.contains("summary"));
        assert!(!line.contains("packets_sent"));
        assert!(!line.contains("packet_loss"));

        let line = format_event(&event, OutputMode::Machine).unwrap();
        assert!(line.contains("event_wall_ns=1000000000"));
        assert!(line.contains("payload_length=0"));
        assert!(!line.contains(" at_ns="));
        assert!(!line.contains(" length=0"));
    }
}
