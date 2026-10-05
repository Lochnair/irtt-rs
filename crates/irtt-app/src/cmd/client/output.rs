use std::{
    fmt::Write as _,
    net::SocketAddr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use irtt_client::{
    ClientEvent, NegotiationResult, OneWayDelaySample, PacketMeta, ReceivedStatsSample, RttSample,
    ServerTiming, SignedDuration, WarningKind,
};

use irtt_stats::{EventStatsUpdate, IpdvPairUpdate};

use super::args::{HeaderMode, OutputFormat};
use crate::cmd::format::{format_duration, format_signed_duration, ABSENT};

#[derive(Debug, Clone)]
pub(super) struct OutputConfig {
    format: OutputFormat,
    columns: Vec<Column>,
    header: HeaderMode,
    default_table_rows: bool,
    verbose: bool,
}

impl OutputConfig {
    pub(super) fn new(
        format: OutputFormat,
        columns: Option<&str>,
        header: HeaderMode,
        verbose: bool,
    ) -> Result<Self, String> {
        let uses_default_columns = columns
            .map(|columns| columns.trim() == "default")
            .unwrap_or(true);
        let default_table_rows = format == OutputFormat::Table && uses_default_columns;
        let columns = match columns {
            Some(columns) => parse_columns(columns, format, verbose)?,
            None => default_columns(format, verbose),
        };

        Ok(Self {
            format,
            columns,
            header,
            default_table_rows,
            verbose,
        })
    }

    pub(super) fn prints_summary(&self) -> bool {
        self.format.prints_summary()
    }

    pub(super) fn summary_verbose(&self) -> bool {
        self.verbose
    }

    pub(super) fn should_print_header(&self) -> bool {
        match self.header {
            HeaderMode::Always => self.format != OutputFormat::Jsonl,
            HeaderMode::Never => false,
            HeaderMode::Auto => matches!(
                self.format,
                OutputFormat::Table | OutputFormat::Csv | OutputFormat::Tsv
            ),
        }
    }

    pub(super) fn render_header(&self) -> Option<String> {
        if !self.should_print_header() {
            return None;
        }

        match self.format {
            OutputFormat::Table => Some(render_table_header(&self.columns)),
            OutputFormat::Csv => Some(
                self.columns
                    .iter()
                    .map(|column| escape_csv(column.name()))
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            OutputFormat::Tsv => Some(
                self.columns
                    .iter()
                    .map(|column| escape_tsv(column.name()))
                    .collect::<Vec<_>>()
                    .join("\t"),
            ),
            OutputFormat::Jsonl => None,
        }
    }

    pub(super) fn render_event(
        &self,
        event: &ClientEvent,
        target: Option<&str>,
        stats: Option<&EventStatsUpdate>,
    ) -> Option<String> {
        let row = OutputRow::from_event(event);
        if self.default_table_rows && row.is_default_table_hidden() {
            return None;
        }

        let context = RenderContext {
            stats,
            target,
            verbose: self.verbose,
        };

        match self.format {
            OutputFormat::Table => Some(render_table_row(&row, &self.columns, context)),
            OutputFormat::Csv => Some(render_delimited_row(
                &row,
                &self.columns,
                context,
                DelimitedFormat::Csv,
            )),
            OutputFormat::Tsv => Some(render_delimited_row(
                &row,
                &self.columns,
                context,
                DelimitedFormat::Tsv,
            )),
            OutputFormat::Jsonl => Some(render_jsonl_row(&row, &self.columns, context)),
        }
    }

    pub(super) fn list_columns() -> String {
        let mut out = String::new();
        writeln!(out, "Available event columns:").unwrap();
        for column in ALL_COLUMNS {
            writeln!(out, "  {:<24} {}", column.name(), column.description()).unwrap();
        }
        writeln!(out).unwrap();
        writeln!(out, "Aliases:").unwrap();
        for column in ALL_COLUMNS {
            let aliases = column.aliases();
            if !aliases.is_empty() {
                writeln!(out, "  {} ({})", aliases.join(", "), column.name()).unwrap();
            }
        }
        writeln!(out).unwrap();
        writeln!(out, "Special column sets:").unwrap();
        writeln!(
            out,
            "  default  format default; compact for table, all columns for csv/tsv/jsonl"
        )
        .unwrap();
        writeln!(out, "  all      every column").unwrap();
        out
    }
}

#[derive(Debug, Clone, Copy)]
struct RenderContext<'a> {
    stats: Option<&'a EventStatsUpdate>,
    target: Option<&'a str>,
    verbose: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum OutputRow {
    SessionStarted(LifecycleRow),
    NoTestCompleted(LifecycleRow),
    SessionClosed {
        remote: SocketAddr,
        token: u64,
        event_wall: SystemTime,
    },
    EchoSent {
        seq: u32,
        remote: SocketAddr,
        client_send_wall: SystemTime,
        bytes: usize,
        send_call: Duration,
        timer_error: Option<Duration>,
    },
    EchoReply(ReplyRow),
    Loss {
        seq: u32,
        client_send_wall: SystemTime,
    },
    Duplicate {
        seq: u32,
        remote: SocketAddr,
        client_receive_wall: SystemTime,
        bytes: usize,
    },
    Late {
        reply: ReplyRow,
        highest_seen: u32,
    },
    Warning {
        kind: WarningKind,
        message: String,
        event_wall: SystemTime,
    },
}

impl OutputRow {
    fn from_event(event: &ClientEvent) -> Self {
        match event {
            ClientEvent::SessionStarted(irtt_client::SessionStarted {
                remote,
                token,
                negotiation: negotiated,
                at,
            }) => Self::SessionStarted(LifecycleRow::new(
                *remote,
                Some(*token),
                negotiated,
                at.wall,
            )),
            ClientEvent::NoTestCompleted(irtt_client::NoTestCompleted {
                remote,
                negotiation: negotiated,
                at,
            }) => Self::NoTestCompleted(LifecycleRow::new(*remote, None, negotiated, at.wall)),
            ClientEvent::SessionClosed { remote, token, at } => Self::SessionClosed {
                remote: *remote,
                token: *token,
                event_wall: at.wall,
            },
            ClientEvent::EchoSent {
                seq,
                remote,
                sent_at,
                bytes,
                send_call,
                timer_error,
                ..
            } => Self::EchoSent {
                seq: *seq,
                remote: *remote,
                client_send_wall: sent_at.wall,
                bytes: *bytes,
                send_call: *send_call,
                timer_error: *timer_error,
            },
            ClientEvent::EchoReply {
                seq,
                remote,
                sent_at,
                received_at,
                rtt,
                server_timing,
                one_way,
                received_stats,
                bytes,
                packet_meta,
            } => Self::EchoReply(ReplyRow {
                seq: *seq,
                remote: *remote,
                client_send_wall: Some(sent_at.wall),
                client_receive_wall: received_at.wall,
                rtt: Some(*rtt),
                server_timing: *server_timing,
                one_way: *one_way,
                received_stats: *received_stats,
                bytes: *bytes,
                packet_meta: *packet_meta,
            }),
            ClientEvent::EchoLoss { seq, sent_at, .. } => Self::Loss {
                seq: *seq,
                client_send_wall: sent_at.wall,
            },
            ClientEvent::DuplicateReply {
                seq,
                remote,
                received_at,
                bytes,
            } => Self::Duplicate {
                seq: *seq,
                remote: *remote,
                client_receive_wall: received_at.wall,
                bytes: *bytes,
            },
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
                bytes,
                packet_meta,
            } => Self::Late {
                reply: ReplyRow {
                    seq: *seq,
                    remote: *remote,
                    client_send_wall: sent_at.map(|sent_at| sent_at.wall),
                    client_receive_wall: received_at.wall,
                    rtt: *rtt,
                    server_timing: *server_timing,
                    one_way: *one_way,
                    received_stats: *received_stats,
                    bytes: *bytes,
                    packet_meta: *packet_meta,
                },
                highest_seen: *highest_seen,
            },
            ClientEvent::Warning { kind, message, at } => Self::Warning {
                kind: *kind,
                message: message.clone(),
                event_wall: at.wall,
            },
        }
    }

    fn event_name(&self) -> &'static str {
        match self {
            Self::SessionStarted(_) => "session_started",
            Self::NoTestCompleted(_) => "no_test_completed",
            Self::SessionClosed { .. } => "session_closed",
            Self::EchoSent { .. } => "echo_sent",
            Self::EchoReply(_) => "echo_reply",
            Self::Loss { .. } => "loss",
            Self::Duplicate { .. } => "duplicate",
            Self::Late { .. } => "late",
            Self::Warning { .. } => "warning",
        }
    }

    fn is_default_table_hidden(&self) -> bool {
        matches!(self, Self::EchoSent { .. })
    }

    fn reply(&self) -> Option<&ReplyRow> {
        match self {
            Self::EchoReply(reply) | Self::Late { reply, .. } => Some(reply),
            _ => None,
        }
    }

    fn seq(&self) -> Option<u32> {
        match self {
            Self::EchoSent { seq, .. } | Self::Loss { seq, .. } | Self::Duplicate { seq, .. } => {
                Some(*seq)
            }
            Self::EchoReply(reply) | Self::Late { reply, .. } => Some(reply.seq),
            _ => None,
        }
    }

    fn remote(&self) -> Option<SocketAddr> {
        match self {
            Self::SessionStarted(row) | Self::NoTestCompleted(row) => Some(row.remote),
            Self::SessionClosed { remote, .. }
            | Self::EchoSent { remote, .. }
            | Self::Duplicate { remote, .. } => Some(*remote),
            Self::EchoReply(reply) | Self::Late { reply, .. } => Some(reply.remote),
            _ => None,
        }
    }

    fn token(&self) -> Option<u64> {
        match self {
            Self::SessionStarted(row) | Self::NoTestCompleted(row) => row.token,
            Self::SessionClosed { token, .. } => Some(*token),
            _ => None,
        }
    }

    fn bytes(&self) -> Option<usize> {
        match self {
            Self::EchoSent { bytes, .. } | Self::Duplicate { bytes, .. } => Some(*bytes),
            Self::EchoReply(reply) | Self::Late { reply, .. } => Some(reply.bytes),
            _ => None,
        }
    }

    fn message(&self, context: RenderContext<'_>) -> Option<String> {
        match self {
            Self::SessionStarted(row) => Some(format!(
                "token={:#x} duration_ns={} interval_ns={} length={}",
                row.token?, row.duration_ns, row.interval_ns, row.payload_length
            )),
            Self::NoTestCompleted(row) => Some(format!(
                "duration_ns={} interval_ns={} length={}",
                row.duration_ns, row.interval_ns, row.payload_length
            )),
            Self::SessionClosed { token, .. } => Some(format!("token={token:#x}")),
            Self::Late { highest_seen, .. } => Some(format!("highest_seen={highest_seen}")),
            Self::Warning { message, .. } => Some(message.clone()),
            Self::Loss { .. } => Some("timeout".to_owned()),
            Self::Duplicate { .. } if context.verbose => Some("duplicate reply".to_owned()),
            _ => None,
        }
    }

    fn event_wall(&self) -> Option<SystemTime> {
        match self {
            Self::SessionStarted(row) | Self::NoTestCompleted(row) => Some(row.event_wall),
            Self::SessionClosed { event_wall, .. } | Self::Warning { event_wall, .. } => {
                Some(*event_wall)
            }
            Self::EchoSent {
                client_send_wall, ..
            }
            | Self::Loss {
                client_send_wall, ..
            } => Some(*client_send_wall),
            Self::Duplicate {
                client_receive_wall,
                ..
            } => Some(*client_receive_wall),
            Self::EchoReply(reply) | Self::Late { reply, .. } => Some(reply.client_receive_wall),
        }
    }

    fn client_send_wall(&self) -> Option<SystemTime> {
        match self {
            Self::EchoSent {
                client_send_wall, ..
            }
            | Self::Loss {
                client_send_wall, ..
            } => Some(*client_send_wall),
            Self::EchoReply(reply) | Self::Late { reply, .. } => reply.client_send_wall,
            _ => None,
        }
    }

    fn client_receive_wall(&self) -> Option<SystemTime> {
        match self {
            Self::Duplicate {
                client_receive_wall,
                ..
            } => Some(*client_receive_wall),
            Self::EchoReply(reply) | Self::Late { reply, .. } => Some(reply.client_receive_wall),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LifecycleRow {
    remote: SocketAddr,
    token: Option<u64>,
    event_wall: SystemTime,
    duration_ns: i128,
    interval_ns: i128,
    payload_length: i128,
}

impl LifecycleRow {
    fn new(
        remote: SocketAddr,
        token: Option<u64>,
        negotiated: &NegotiationResult,
        event_wall: SystemTime,
    ) -> Self {
        Self {
            remote,
            token,
            event_wall,
            duration_ns: negotiated
                .accepted
                .duration
                .map_or(0, |duration| duration.as_nanos() as i128),
            interval_ns: negotiated.accepted.interval.as_nanos() as i128,
            payload_length: i128::from(negotiated.accepted.length),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplyRow {
    seq: u32,
    remote: SocketAddr,
    client_send_wall: Option<SystemTime>,
    client_receive_wall: SystemTime,
    rtt: Option<RttSample>,
    server_timing: Option<ServerTiming>,
    one_way: Option<OneWayDelaySample>,
    received_stats: Option<ReceivedStatsSample>,
    bytes: usize,
    packet_meta: PacketMeta,
}

// Only schema metadata is generated here; cell_for keeps rendering behavior explicit.
macro_rules! columns {
    ($($variant:ident($name:literal, [$($alias:literal),*], $width:expr, $align:ident, $description:literal);)*) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        enum Column { $($variant),* }

        const ALL_COLUMNS: &[Column] = &[$(Column::$variant),*];

        impl Column {
            fn name(self) -> &'static str {
                match self { $(Self::$variant => $name),* }
            }

            fn parse(input: &str) -> Option<Self> {
                match input {
                    $($name $(| $alias)* => Some(Self::$variant),)*
                    _ => None,
                }
            }

            fn aliases(self) -> &'static [&'static str] {
                match self { $(Self::$variant => &[$($alias),*]),* }
            }

            fn description(self) -> &'static str {
                match self { $(Self::$variant => $description),* }
            }

            fn table_width(self) -> usize {
                match self { $(Self::$variant => $width),* }
            }

            fn align_right(self) -> bool {
                match self { $(Self::$variant => columns!(@align $align)),* }
            }
        }
    };
    (@align left) => { false };
    (@align right) => { true };
}

// Variant(canonical name, aliases, table width, alignment, description).
columns! {
    Target("target", [], 18, left, "logical CLI target label");
    Event("event", [], 17, left, "event kind");
    Seq("seq", [], 6, right, "probe sequence number");
    Remote("remote", [], 21, left, "remote socket address");
    Token("token", [], 18, left, "session token as hexadecimal");
    Rtt("rtt", [], 9, right, "human-readable effective RTT");
    RttUs("rtt_us", [], 10, right, "effective RTT in signed microseconds");
    RawRttUs("raw_rtt_us", [], 10, right, "raw client send-to-receive RTT in microseconds");
    EffectiveRttUs("effective_rtt_us", [], 16, right, "effective RTT in signed microseconds");
    AdjustedRttUs("adjusted_rtt_us", [], 15, right, "adjusted RTT in signed microseconds");
    ReceiveDelay("rd", ["receive_delay"], 9, right, "human-readable server-to-client delay");
    ReceiveDelayUs("rd_us", ["receive_delay_us"], 10, right, "server-to-client delay in signed microseconds");
    SendDelay("sd", ["send_delay"], 9, right, "human-readable client-to-server delay");
    SendDelayUs("sd_us", ["send_delay_us"], 10, right, "client-to-server delay in signed microseconds");
    Ipdv("ipdv", [], 9, right, "human-readable round-trip IPDV for adjacent samples");
    IpdvUs("ipdv_us", [], 10, right, "round-trip IPDV in microseconds");
    ServerProcessing("proc", ["server_processing"], 9, right, "human-readable server processing time");
    ServerProcessingUs("server_processing_us", [], 20, right, "server processing time in microseconds");
    Bytes("bytes", [], 10, right, "packet bytes for packet events");
    SendCallUs("send_call_us", [], 12, right, "send system call duration in microseconds");
    TimerErrorUs("timer_error_us", [], 14, right, "scheduled-vs-actual send timer error in microseconds");
    HighestSeen("highest_seen", [], 15, right, "highest sequence seen when a late reply arrived");
    ServerReceivedCount("server_received", ["server_received_count"], 15, right, "server-reported received packet count");
    ServerReceivedWindow("server_window", ["server_received_window"], 13, left, "server-reported received window as hexadecimal");
    Dscp("dscp", [], 4, right, "received packet DSCP codepoint");
    Ecn("ecn", [], 4, right, "received packet ECN bits");
    TrafficClass("traffic_class", [], 13, right, "received packet traffic class byte");
    KernelRxNs("kernel_rx_ns", [], 12, right, "kernel receive timestamp as Unix nanoseconds");
    WarningKind("warning_kind", [], 28, left, "warning classifier");
    Message("message", [], 24, left, "warning or lifecycle message");
    EventWallNs("event_wall_ns", [], 13, right, "event wall timestamp as Unix nanoseconds");
    ClientSendWallNs("client_send_wall_ns", [], 19, right, "client send wall timestamp as Unix nanoseconds");
    ClientReceiveWallNs("client_receive_wall_ns", [], 22, right, "client receive wall timestamp as Unix nanoseconds");
    DurationNs("duration_ns", [], 11, right, "negotiated test duration in nanoseconds");
    IntervalNs("interval_ns", [], 11, right, "negotiated probe interval in nanoseconds");
    PayloadLength("payload_length", [], 14, right, "negotiated payload length");
    ServerReceiveWallNs("server_receive_wall_ns", [], 22, right, "server receive wall timestamp in nanoseconds");
    ServerReceiveMonoNs("server_receive_mono_ns", [], 22, right, "server receive monotonic timestamp in nanoseconds");
    ServerSendWallNs("server_send_wall_ns", [], 19, right, "server send wall timestamp in nanoseconds");
    ServerSendMonoNs("server_send_mono_ns", [], 19, right, "server send monotonic timestamp in nanoseconds");
    ServerMidpointWallNs("server_midpoint_wall_ns", [], 23, right, "server midpoint wall timestamp in nanoseconds");
    ServerMidpointMonoNs("server_midpoint_mono_ns", [], 23, right, "server midpoint monotonic timestamp in nanoseconds");
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CellValue {
    Text(String),
    Integer(i128),
    Unsigned(u128),
    Hex(u128),
}

impl CellValue {
    fn text(value: impl Into<String>) -> Self {
        Self::Text(value.into())
    }

    fn display(&self) -> String {
        match self {
            Self::Text(value) => value.clone(),
            Self::Integer(value) => value.to_string(),
            Self::Unsigned(value) => value.to_string(),
            Self::Hex(value) => format!("{value:#x}"),
        }
    }

    fn write_json(&self, out: &mut String) {
        match self {
            Self::Text(value) => {
                out.push('"');
                write_json_string_content(out, value);
                out.push('"');
            }
            Self::Integer(value) => write!(out, "{value}").unwrap(),
            Self::Unsigned(value) => write!(out, "{value}").unwrap(),
            Self::Hex(value) => {
                out.push('"');
                write!(out, "{value:#x}").unwrap();
                out.push('"');
            }
        }
    }
}

fn parse_columns(input: &str, format: OutputFormat, verbose: bool) -> Result<Vec<Column>, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("--columns requires at least one column".to_owned());
    }
    if trimmed == "all" {
        return Ok(ALL_COLUMNS.to_vec());
    }
    if trimmed == "default" {
        return Ok(default_columns(format, verbose));
    }

    let mut columns = Vec::new();
    for raw in trimmed.split(',') {
        let name = raw.trim();
        if name.is_empty() {
            return Err("empty column in --columns list".to_owned());
        }
        let column = Column::parse(name).ok_or_else(|| {
            format!("unknown output column {name:?}; run --list-columns to see valid names")
        })?;
        columns.push(column);
    }
    Ok(columns)
}

fn default_columns(format: OutputFormat, verbose: bool) -> Vec<Column> {
    match format {
        OutputFormat::Table => {
            let mut columns = vec![
                Column::Target,
                Column::Event,
                Column::Seq,
                Column::Rtt,
                Column::ReceiveDelay,
                Column::SendDelay,
                Column::Ipdv,
                Column::ServerProcessing,
                Column::Message,
            ];
            if verbose {
                columns.extend([
                    Column::Remote,
                    Column::ServerReceivedCount,
                    Column::ServerReceivedWindow,
                    Column::RawRttUs,
                    Column::AdjustedRttUs,
                    Column::Bytes,
                    Column::Dscp,
                    Column::Ecn,
                ]);
            }
            columns
        }
        OutputFormat::Csv | OutputFormat::Tsv | OutputFormat::Jsonl => ALL_COLUMNS.to_vec(),
    }
}

fn render_table_header(columns: &[Column]) -> String {
    columns
        .iter()
        .map(|column| format_table_cell(column, Some(column.name().to_owned())))
        .collect::<Vec<_>>()
        .join("  ")
}

fn render_table_row(row: &OutputRow, columns: &[Column], context: RenderContext<'_>) -> String {
    columns
        .iter()
        .map(|column| {
            let value = cell_for(row, *column, context).map(|value| value.display());
            format_table_cell(column, value)
        })
        .collect::<Vec<_>>()
        .join("  ")
}

fn format_table_cell(column: &Column, value: Option<String>) -> String {
    let value = value.unwrap_or_else(|| ABSENT.to_owned());
    let width = column.table_width();
    let value = if matches!(column, Column::Target) {
        truncate_for_table(&value, width)
    } else {
        value
    };
    if column.align_right() {
        format!("{value:>width$}")
    } else {
        format!("{value:<width$}")
    }
}

/// Shortens `value` to fit within `max_width` display characters for TABLE
/// presentation only. Values within the limit are returned unchanged. Longer
/// values keep a prefix and end with `"..."` so the result is exactly
/// `max_width` characters. Operates on `char`s (never raw byte indices) so a
/// multibyte UTF-8 value cannot be split mid-code-point.
///
/// This is a presentation-only concern: callers must keep the untruncated
/// value for CSV/TSV/JSONL and any non-display use.
fn truncate_for_table(value: &str, max_width: usize) -> String {
    if value.chars().count() <= max_width {
        return value.to_owned();
    }
    let suffix = "...";
    let prefix_len = max_width.saturating_sub(suffix.len());
    let mut truncated: String = value.chars().take(prefix_len).collect();
    truncated.push_str(suffix);
    truncated
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DelimitedFormat {
    Csv,
    Tsv,
}

fn render_delimited_row(
    row: &OutputRow,
    columns: &[Column],
    context: RenderContext<'_>,
    format: DelimitedFormat,
) -> String {
    let separator = match format {
        DelimitedFormat::Csv => ",",
        DelimitedFormat::Tsv => "\t",
    };
    columns
        .iter()
        .map(|column| {
            let value = cell_for(row, *column, context)
                .map(|value| value.display())
                .unwrap_or_default();
            match format {
                DelimitedFormat::Csv => escape_csv(&value),
                DelimitedFormat::Tsv => escape_tsv(&value),
            }
        })
        .collect::<Vec<_>>()
        .join(separator)
}

fn render_jsonl_row(row: &OutputRow, columns: &[Column], context: RenderContext<'_>) -> String {
    let mut out = String::from("{");
    for (index, column) in columns.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push('"');
        write_json_string_content(&mut out, column.name());
        out.push_str("\":");
        if let Some(value) = cell_for(row, *column, context) {
            value.write_json(&mut out);
        } else {
            out.push_str("null");
        }
    }
    out.push('}');
    out
}

fn cell_for(row: &OutputRow, column: Column, context: RenderContext<'_>) -> Option<CellValue> {
    match column {
        Column::Target => context.target.map(CellValue::text),
        Column::Event => Some(CellValue::text(row.event_name())),
        Column::Seq => row.seq().map(|seq| CellValue::Unsigned(u128::from(seq))),
        Column::Remote => row
            .remote()
            .map(|remote| CellValue::text(remote.to_string())),
        Column::Token => row.token().map(|token| CellValue::Hex(u128::from(token))),
        Column::Rtt => row.reply().and_then(|reply| {
            reply
                .rtt
                .map(|rtt| CellValue::text(format_signed_duration(rtt.effective)))
        }),
        Column::RttUs | Column::EffectiveRttUs => row.reply().and_then(|reply| {
            reply
                .rtt
                .map(|rtt| CellValue::Integer(signed_duration_us(rtt.effective)))
        }),
        Column::RawRttUs => row.reply().and_then(|reply| {
            reply
                .rtt
                .map(|rtt| CellValue::Unsigned(duration_us(rtt.raw)))
        }),
        Column::AdjustedRttUs => row.reply().and_then(|reply| {
            reply
                .rtt
                .and_then(|rtt| rtt.adjusted)
                .map(|adjusted| CellValue::Integer(signed_duration_us(adjusted)))
        }),
        Column::ReceiveDelay => row
            .reply()
            .and_then(|reply| reply.one_way.and_then(|one_way| one_way.server_to_client))
            .map(|value| CellValue::text(format_signed_duration(value))),
        Column::ReceiveDelayUs => row
            .reply()
            .and_then(|reply| reply.one_way.and_then(|one_way| one_way.server_to_client))
            .map(|value| CellValue::Integer(signed_duration_us(value))),
        Column::SendDelay => row
            .reply()
            .and_then(|reply| reply.one_way.and_then(|one_way| one_way.client_to_server))
            .map(|value| CellValue::text(format_signed_duration(value))),
        Column::SendDelayUs => row
            .reply()
            .and_then(|reply| reply.one_way.and_then(|one_way| one_way.client_to_server))
            .map(|value| CellValue::Integer(signed_duration_us(value))),
        Column::Ipdv => row
            .seq()
            .and_then(|seq| ipdv_pair(context.stats, seq))
            .map(|pair| CellValue::text(format_duration(pair.rtt_ipdv))),
        Column::IpdvUs => row
            .seq()
            .and_then(|seq| ipdv_pair(context.stats, seq))
            .map(|pair| CellValue::Unsigned(duration_us(pair.rtt_ipdv))),
        Column::ServerProcessing => row
            .reply()
            .and_then(|reply| reply.server_timing.and_then(|timing| timing.processing))
            .map(|value| CellValue::text(format_duration(value))),
        Column::ServerProcessingUs => row
            .reply()
            .and_then(|reply| reply.server_timing.and_then(|timing| timing.processing))
            .map(|value| CellValue::Unsigned(duration_us(value))),
        Column::Bytes => row.bytes().map(|bytes| CellValue::Unsigned(bytes as u128)),
        Column::SendCallUs => match row {
            OutputRow::EchoSent { send_call, .. } => {
                Some(CellValue::Unsigned(duration_us(*send_call)))
            }
            _ => None,
        },
        Column::TimerErrorUs => match row {
            OutputRow::EchoSent { timer_error, .. } => {
                timer_error.map(|error| CellValue::Unsigned(duration_us(error)))
            }
            _ => None,
        },
        Column::HighestSeen => match row {
            OutputRow::Late { highest_seen, .. } => {
                Some(CellValue::Unsigned(u128::from(*highest_seen)))
            }
            _ => None,
        },
        Column::ServerReceivedCount => row
            .reply()
            .and_then(|reply| reply.received_stats.and_then(|stats| stats.count))
            .map(|value| CellValue::Unsigned(u128::from(value))),
        Column::ServerReceivedWindow => row
            .reply()
            .and_then(|reply| reply.received_stats.and_then(|stats| stats.window))
            .map(|value| CellValue::Hex(u128::from(value))),
        Column::Dscp => row
            .reply()
            .and_then(|reply| reply.packet_meta.dscp)
            .map(|value| CellValue::Unsigned(u128::from(value))),
        Column::Ecn => row
            .reply()
            .and_then(|reply| reply.packet_meta.ecn)
            .map(|value| CellValue::Unsigned(u128::from(value))),
        Column::TrafficClass => row
            .reply()
            .and_then(|reply| reply.packet_meta.traffic_class)
            .map(|value| CellValue::Unsigned(u128::from(value))),
        Column::KernelRxNs => row
            .reply()
            .and_then(|reply| reply.packet_meta.kernel_rx_timestamp)
            .and_then(wall_time_ns)
            .map(CellValue::Unsigned),
        Column::WarningKind => match row {
            OutputRow::Warning { kind, .. } => Some(CellValue::text(warning_kind(*kind))),
            _ => None,
        },
        Column::Message => row.message(context).map(CellValue::text),
        Column::EventWallNs => row
            .event_wall()
            .and_then(wall_time_ns)
            .map(CellValue::Unsigned),
        Column::ClientSendWallNs => row
            .client_send_wall()
            .and_then(wall_time_ns)
            .map(CellValue::Unsigned),
        Column::ClientReceiveWallNs => row
            .client_receive_wall()
            .and_then(wall_time_ns)
            .map(CellValue::Unsigned),
        Column::DurationNs => match row {
            OutputRow::SessionStarted(row) | OutputRow::NoTestCompleted(row) => {
                Some(CellValue::Integer(row.duration_ns))
            }
            _ => None,
        },
        Column::IntervalNs => match row {
            OutputRow::SessionStarted(row) | OutputRow::NoTestCompleted(row) => {
                Some(CellValue::Integer(row.interval_ns))
            }
            _ => None,
        },
        Column::PayloadLength => match row {
            OutputRow::SessionStarted(row) | OutputRow::NoTestCompleted(row) => {
                Some(CellValue::Integer(row.payload_length))
            }
            _ => None,
        },
        Column::ServerReceiveWallNs => server_timing_i64(row, |timing| timing.receive_wall_ns),
        Column::ServerReceiveMonoNs => server_timing_i64(row, |timing| timing.receive_mono_ns),
        Column::ServerSendWallNs => server_timing_i64(row, |timing| timing.send_wall_ns),
        Column::ServerSendMonoNs => server_timing_i64(row, |timing| timing.send_mono_ns),
        Column::ServerMidpointWallNs => server_timing_i64(row, |timing| timing.midpoint_wall_ns),
        Column::ServerMidpointMonoNs => server_timing_i64(row, |timing| timing.midpoint_mono_ns),
    }
}

fn server_timing_i64(
    row: &OutputRow,
    select: impl FnOnce(ServerTiming) -> Option<i64>,
) -> Option<CellValue> {
    row.reply()
        .and_then(|reply| reply.server_timing)
        .and_then(select)
        .map(|value| CellValue::Integer(i128::from(value)))
}

fn ipdv_pair(stats: Option<&EventStatsUpdate>, seq: u32) -> Option<&IpdvPairUpdate> {
    let stats = stats?;
    stats
        .ipdv_pairs
        .iter()
        .find(|pair| pair.current_seq == seq)
        .or_else(|| {
            stats
                .ipdv_pairs
                .iter()
                .find(|pair| pair.previous_seq == seq)
        })
}

fn duration_us(duration: Duration) -> u128 {
    duration.as_micros()
}

fn signed_duration_us(duration: SignedDuration) -> i128 {
    duration.as_micros()
}

fn wall_time_ns(wall: SystemTime) -> Option<u128> {
    wall.duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_nanos())
}

fn warning_kind(kind: WarningKind) -> &'static str {
    match kind {
        WarningKind::MalformedOrUnrelatedPacket => "malformed_or_unrelated_packet",
        WarningKind::WrongToken => "wrong_token",
        WarningKind::UntrackedReply => "untracked_reply",
        _ => "unknown",
    }
}

fn escape_csv(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        let escaped = value.replace('"', "\"\"");
        format!("\"{escaped}\"")
    } else {
        value.to_owned()
    }
}

fn escape_tsv(value: &str) -> String {
    value.replace(['\t', '\n', '\r'], " ")
}

fn write_json_string_content(out: &mut String, value: &str) {
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if c < '\u{20}' => write!(out, "\\u{:04x}", c as u32).unwrap(),
            c => out.push(c),
        }
    }
}
