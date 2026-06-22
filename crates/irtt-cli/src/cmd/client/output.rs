use std::{
    fmt::Write as _,
    net::SocketAddr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use irtt_client::{
    ClientEvent, NegotiatedParams, OneWayDelaySample, PacketMeta, ReceivedStatsSample, RttSample,
    ServerTiming, SignedDuration, WarningKind,
};

use super::args::{HeaderMode, OutputFormat};

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
        let default_table_rows = format == OutputFormat::Table && columns.is_none();
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
        stats: Option<&EventRenderStats>,
    ) -> Option<String> {
        let row = OutputRow::from_event(event);
        if self.default_table_rows && row.is_default_table_hidden() {
            return None;
        }

        let context = RenderContext {
            stats,
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
        writeln!(
            out,
            "Aliases: rd=receive_delay, sd=send_delay, proc=server_processing, \
             server_received=server_received_count, server_window=server_received_window."
        )
        .unwrap();
        writeln!(
            out,
            "Use --columns default for the format default, or --columns all for every column."
        )
        .unwrap();
        out
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EventRenderStats {
    pub contributed_sample: bool,
    pub ipdv_pairs: Vec<IpdvPair>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpdvPair {
    pub previous_seq: u32,
    pub current_seq: u32,
    pub rtt_ipdv: Duration,
    pub send_ipdv: Option<Duration>,
    pub receive_ipdv: Option<Duration>,
}

#[cfg(feature = "stats")]
impl From<irtt_stats::EventStatsUpdate> for EventRenderStats {
    fn from(value: irtt_stats::EventStatsUpdate) -> Self {
        Self {
            contributed_sample: value.contributed_sample,
            ipdv_pairs: value.ipdv_pairs.into_iter().map(Into::into).collect(),
        }
    }
}

#[cfg(feature = "stats")]
impl From<irtt_stats::IpdvPairUpdate> for IpdvPair {
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

#[derive(Debug, Clone, Copy)]
struct RenderContext<'a> {
    stats: Option<&'a EventRenderStats>,
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
        timer_error: Duration,
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
            ClientEvent::SessionStarted {
                remote,
                token,
                negotiated,
                at,
            } => Self::SessionStarted(LifecycleRow::new(
                *remote,
                Some(*token),
                negotiated,
                at.wall,
            )),
            ClientEvent::NoTestCompleted {
                remote,
                negotiated,
                at,
            } => Self::NoTestCompleted(LifecycleRow::new(*remote, None, negotiated, at.wall)),
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
        negotiated: &NegotiatedParams,
        event_wall: SystemTime,
    ) -> Self {
        Self {
            remote,
            token,
            event_wall,
            duration_ns: i128::from(negotiated.params.duration_ns),
            interval_ns: i128::from(negotiated.params.interval_ns),
            payload_length: i128::from(negotiated.params.length),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Column {
    Event,
    Seq,
    Remote,
    Token,
    Rtt,
    RttUs,
    RawRttUs,
    EffectiveRttUs,
    AdjustedRttUs,
    ReceiveDelay,
    ReceiveDelayUs,
    SendDelay,
    SendDelayUs,
    Ipdv,
    IpdvUs,
    ServerProcessing,
    ServerProcessingUs,
    Bytes,
    SendCallUs,
    TimerErrorUs,
    HighestSeen,
    ServerReceivedCount,
    ServerReceivedWindow,
    Dscp,
    Ecn,
    TrafficClass,
    KernelRxNs,
    WarningKind,
    Message,
    EventWallNs,
    ClientSendWallNs,
    ClientReceiveWallNs,
    DurationNs,
    IntervalNs,
    PayloadLength,
    ServerReceiveWallNs,
    ServerReceiveMonoNs,
    ServerSendWallNs,
    ServerSendMonoNs,
    ServerMidpointWallNs,
    ServerMidpointMonoNs,
}

const ALL_COLUMNS: &[Column] = &[
    Column::Event,
    Column::Seq,
    Column::Remote,
    Column::Token,
    Column::Rtt,
    Column::RttUs,
    Column::RawRttUs,
    Column::EffectiveRttUs,
    Column::AdjustedRttUs,
    Column::ReceiveDelay,
    Column::ReceiveDelayUs,
    Column::SendDelay,
    Column::SendDelayUs,
    Column::Ipdv,
    Column::IpdvUs,
    Column::ServerProcessing,
    Column::ServerProcessingUs,
    Column::Bytes,
    Column::SendCallUs,
    Column::TimerErrorUs,
    Column::HighestSeen,
    Column::ServerReceivedCount,
    Column::ServerReceivedWindow,
    Column::Dscp,
    Column::Ecn,
    Column::TrafficClass,
    Column::KernelRxNs,
    Column::WarningKind,
    Column::Message,
    Column::EventWallNs,
    Column::ClientSendWallNs,
    Column::ClientReceiveWallNs,
    Column::DurationNs,
    Column::IntervalNs,
    Column::PayloadLength,
    Column::ServerReceiveWallNs,
    Column::ServerReceiveMonoNs,
    Column::ServerSendWallNs,
    Column::ServerSendMonoNs,
    Column::ServerMidpointWallNs,
    Column::ServerMidpointMonoNs,
];

impl Column {
    fn name(self) -> &'static str {
        match self {
            Self::Event => "event",
            Self::Seq => "seq",
            Self::Remote => "remote",
            Self::Token => "token",
            Self::Rtt => "rtt",
            Self::RttUs => "rtt_us",
            Self::RawRttUs => "raw_rtt_us",
            Self::EffectiveRttUs => "effective_rtt_us",
            Self::AdjustedRttUs => "adjusted_rtt_us",
            Self::ReceiveDelay => "rd",
            Self::ReceiveDelayUs => "rd_us",
            Self::SendDelay => "sd",
            Self::SendDelayUs => "sd_us",
            Self::Ipdv => "ipdv",
            Self::IpdvUs => "ipdv_us",
            Self::ServerProcessing => "proc",
            Self::ServerProcessingUs => "server_processing_us",
            Self::Bytes => "bytes",
            Self::SendCallUs => "send_call_us",
            Self::TimerErrorUs => "timer_error_us",
            Self::HighestSeen => "highest_seen",
            Self::ServerReceivedCount => "server_received",
            Self::ServerReceivedWindow => "server_window",
            Self::Dscp => "dscp",
            Self::Ecn => "ecn",
            Self::TrafficClass => "traffic_class",
            Self::KernelRxNs => "kernel_rx_ns",
            Self::WarningKind => "warning_kind",
            Self::Message => "message",
            Self::EventWallNs => "event_wall_ns",
            Self::ClientSendWallNs => "client_send_wall_ns",
            Self::ClientReceiveWallNs => "client_receive_wall_ns",
            Self::DurationNs => "duration_ns",
            Self::IntervalNs => "interval_ns",
            Self::PayloadLength => "payload_length",
            Self::ServerReceiveWallNs => "server_receive_wall_ns",
            Self::ServerReceiveMonoNs => "server_receive_mono_ns",
            Self::ServerSendWallNs => "server_send_wall_ns",
            Self::ServerSendMonoNs => "server_send_mono_ns",
            Self::ServerMidpointWallNs => "server_midpoint_wall_ns",
            Self::ServerMidpointMonoNs => "server_midpoint_mono_ns",
        }
    }

    fn parse(input: &str) -> Option<Self> {
        Some(match input {
            "event" => Self::Event,
            "seq" => Self::Seq,
            "remote" => Self::Remote,
            "token" => Self::Token,
            "rtt" => Self::Rtt,
            "rtt_us" => Self::RttUs,
            "raw_rtt_us" => Self::RawRttUs,
            "effective_rtt_us" => Self::EffectiveRttUs,
            "adjusted_rtt_us" => Self::AdjustedRttUs,
            "rd" | "receive_delay" => Self::ReceiveDelay,
            "rd_us" | "receive_delay_us" => Self::ReceiveDelayUs,
            "sd" | "send_delay" => Self::SendDelay,
            "sd_us" | "send_delay_us" => Self::SendDelayUs,
            "ipdv" => Self::Ipdv,
            "ipdv_us" => Self::IpdvUs,
            "proc" | "server_processing" => Self::ServerProcessing,
            "server_processing_us" => Self::ServerProcessingUs,
            "bytes" => Self::Bytes,
            "send_call_us" => Self::SendCallUs,
            "timer_error_us" => Self::TimerErrorUs,
            "highest_seen" => Self::HighestSeen,
            "server_received" | "server_received_count" => Self::ServerReceivedCount,
            "server_window" | "server_received_window" => Self::ServerReceivedWindow,
            "dscp" => Self::Dscp,
            "ecn" => Self::Ecn,
            "traffic_class" => Self::TrafficClass,
            "kernel_rx_ns" => Self::KernelRxNs,
            "warning_kind" => Self::WarningKind,
            "message" => Self::Message,
            "event_wall_ns" => Self::EventWallNs,
            "client_send_wall_ns" => Self::ClientSendWallNs,
            "client_receive_wall_ns" => Self::ClientReceiveWallNs,
            "duration_ns" => Self::DurationNs,
            "interval_ns" => Self::IntervalNs,
            "payload_length" => Self::PayloadLength,
            "server_receive_wall_ns" => Self::ServerReceiveWallNs,
            "server_receive_mono_ns" => Self::ServerReceiveMonoNs,
            "server_send_wall_ns" => Self::ServerSendWallNs,
            "server_send_mono_ns" => Self::ServerSendMonoNs,
            "server_midpoint_wall_ns" => Self::ServerMidpointWallNs,
            "server_midpoint_mono_ns" => Self::ServerMidpointMonoNs,
            _ => return None,
        })
    }

    fn description(self) -> &'static str {
        match self {
            Self::Event => "event kind",
            Self::Seq => "probe sequence number",
            Self::Remote => "remote socket address",
            Self::Token => "session token as hexadecimal",
            Self::Rtt => "human-readable effective RTT",
            Self::RttUs => "effective RTT in signed microseconds",
            Self::RawRttUs => "raw client send-to-receive RTT in microseconds",
            Self::EffectiveRttUs => "effective RTT in signed microseconds",
            Self::AdjustedRttUs => "adjusted RTT in signed microseconds",
            Self::ReceiveDelay => "human-readable server-to-client delay",
            Self::ReceiveDelayUs => "server-to-client delay in signed microseconds",
            Self::SendDelay => "human-readable client-to-server delay",
            Self::SendDelayUs => "client-to-server delay in signed microseconds",
            Self::Ipdv => "human-readable round-trip IPDV for adjacent samples",
            Self::IpdvUs => "round-trip IPDV in microseconds",
            Self::ServerProcessing => "human-readable server processing time",
            Self::ServerProcessingUs => "server processing time in microseconds",
            Self::Bytes => "packet bytes for packet events",
            Self::SendCallUs => "send system call duration in microseconds",
            Self::TimerErrorUs => "scheduled-vs-actual send timer error in microseconds",
            Self::HighestSeen => "highest sequence seen when a late reply arrived",
            Self::ServerReceivedCount => "server-reported received packet count",
            Self::ServerReceivedWindow => "server-reported received window as hexadecimal",
            Self::Dscp => "received packet DSCP codepoint",
            Self::Ecn => "received packet ECN bits",
            Self::TrafficClass => "received packet traffic class byte",
            Self::KernelRxNs => "kernel receive timestamp as Unix nanoseconds",
            Self::WarningKind => "warning classifier",
            Self::Message => "warning or lifecycle message",
            Self::EventWallNs => "event wall timestamp as Unix nanoseconds",
            Self::ClientSendWallNs => "client send wall timestamp as Unix nanoseconds",
            Self::ClientReceiveWallNs => "client receive wall timestamp as Unix nanoseconds",
            Self::DurationNs => "negotiated test duration in nanoseconds",
            Self::IntervalNs => "negotiated probe interval in nanoseconds",
            Self::PayloadLength => "negotiated payload length",
            Self::ServerReceiveWallNs => "server receive wall timestamp in nanoseconds",
            Self::ServerReceiveMonoNs => "server receive monotonic timestamp in nanoseconds",
            Self::ServerSendWallNs => "server send wall timestamp in nanoseconds",
            Self::ServerSendMonoNs => "server send monotonic timestamp in nanoseconds",
            Self::ServerMidpointWallNs => "server midpoint wall timestamp in nanoseconds",
            Self::ServerMidpointMonoNs => "server midpoint monotonic timestamp in nanoseconds",
        }
    }

    fn table_width(self) -> usize {
        match self {
            Self::Event => 17,
            Self::Seq => 6,
            Self::Remote => 21,
            Self::Token => 18,
            Self::Rtt
            | Self::ReceiveDelay
            | Self::SendDelay
            | Self::Ipdv
            | Self::ServerProcessing => 9,
            Self::Message => 24,
            Self::WarningKind => 28,
            Self::ServerReceivedWindow => 13,
            Self::ServerReceivedCount | Self::HighestSeen => 15,
            Self::Dscp | Self::Ecn => 4,
            Self::TrafficClass => 13,
            _ => self.name().len().max(10),
        }
    }

    fn align_right(self) -> bool {
        !matches!(
            self,
            Self::Event
                | Self::Remote
                | Self::Token
                | Self::Message
                | Self::WarningKind
                | Self::ServerReceivedWindow
        )
    }
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
                Column::Event,
                Column::Seq,
                Column::Remote,
                Column::Rtt,
                Column::ReceiveDelay,
                Column::SendDelay,
                Column::Ipdv,
                Column::ServerProcessing,
                Column::ServerReceivedCount,
                Column::ServerReceivedWindow,
                Column::Message,
            ];
            if verbose {
                columns.extend([
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
    let value = value.unwrap_or_else(|| "-".to_owned());
    let width = column.table_width();
    if column.align_right() {
        format!("{value:>width$}")
    } else {
        format!("{value:<width$}")
    }
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
        Column::Event => Some(CellValue::text(row.event_name())),
        Column::Seq => row_seq(row).map(|seq| CellValue::Unsigned(u128::from(seq))),
        Column::Remote => row_remote(row).map(|remote| CellValue::text(remote.to_string())),
        Column::Token => row_token(row).map(|token| CellValue::Hex(u128::from(token))),
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
        Column::Ipdv => row_seq(row)
            .and_then(|seq| ipdv_pair(context.stats, seq))
            .map(|pair| CellValue::text(format_duration(pair.rtt_ipdv))),
        Column::IpdvUs => row_seq(row)
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
        Column::Bytes => row_bytes(row).map(|bytes| CellValue::Unsigned(bytes as u128)),
        Column::SendCallUs => match row {
            OutputRow::EchoSent { send_call, .. } => {
                Some(CellValue::Unsigned(duration_us(*send_call)))
            }
            _ => None,
        },
        Column::TimerErrorUs => match row {
            OutputRow::EchoSent { timer_error, .. } => {
                Some(CellValue::Unsigned(duration_us(*timer_error)))
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
        Column::Message => row_message(row, context).map(CellValue::text),
        Column::EventWallNs => row_event_wall(row)
            .and_then(wall_time_ns)
            .map(CellValue::Unsigned),
        Column::ClientSendWallNs => row_client_send_wall(row)
            .and_then(wall_time_ns)
            .map(CellValue::Unsigned),
        Column::ClientReceiveWallNs => row_client_receive_wall(row)
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

fn row_seq(row: &OutputRow) -> Option<u32> {
    match row {
        OutputRow::EchoSent { seq, .. }
        | OutputRow::Loss { seq, .. }
        | OutputRow::Duplicate { seq, .. } => Some(*seq),
        OutputRow::EchoReply(reply) | OutputRow::Late { reply, .. } => Some(reply.seq),
        _ => None,
    }
}

fn row_remote(row: &OutputRow) -> Option<SocketAddr> {
    match row {
        OutputRow::SessionStarted(row) | OutputRow::NoTestCompleted(row) => Some(row.remote),
        OutputRow::SessionClosed { remote, .. }
        | OutputRow::EchoSent { remote, .. }
        | OutputRow::Duplicate { remote, .. } => Some(*remote),
        OutputRow::EchoReply(reply) | OutputRow::Late { reply, .. } => Some(reply.remote),
        _ => None,
    }
}

fn row_token(row: &OutputRow) -> Option<u64> {
    match row {
        OutputRow::SessionStarted(row) | OutputRow::NoTestCompleted(row) => row.token,
        OutputRow::SessionClosed { token, .. } => Some(*token),
        _ => None,
    }
}

fn row_bytes(row: &OutputRow) -> Option<usize> {
    match row {
        OutputRow::EchoSent { bytes, .. } | OutputRow::Duplicate { bytes, .. } => Some(*bytes),
        OutputRow::EchoReply(reply) | OutputRow::Late { reply, .. } => Some(reply.bytes),
        _ => None,
    }
}

fn row_message(row: &OutputRow, context: RenderContext<'_>) -> Option<String> {
    match row {
        OutputRow::SessionStarted(row) => Some(format!(
            "token={:#x} duration_ns={} interval_ns={} length={}",
            row.token?, row.duration_ns, row.interval_ns, row.payload_length
        )),
        OutputRow::NoTestCompleted(row) => Some(format!(
            "duration_ns={} interval_ns={} length={}",
            row.duration_ns, row.interval_ns, row.payload_length
        )),
        OutputRow::SessionClosed { token, .. } => Some(format!("token={token:#x}")),
        OutputRow::Late { highest_seen, .. } => Some(format!("highest_seen={highest_seen}")),
        OutputRow::Warning { message, .. } => Some(message.clone()),
        OutputRow::Loss { .. } => Some("timeout".to_owned()),
        OutputRow::Duplicate { .. } if context.verbose => Some("duplicate reply".to_owned()),
        _ => None,
    }
}

fn row_event_wall(row: &OutputRow) -> Option<SystemTime> {
    match row {
        OutputRow::SessionStarted(row) | OutputRow::NoTestCompleted(row) => Some(row.event_wall),
        OutputRow::SessionClosed { event_wall, .. } | OutputRow::Warning { event_wall, .. } => {
            Some(*event_wall)
        }
        OutputRow::EchoSent {
            client_send_wall, ..
        }
        | OutputRow::Loss {
            client_send_wall, ..
        } => Some(*client_send_wall),
        OutputRow::Duplicate {
            client_receive_wall,
            ..
        } => Some(*client_receive_wall),
        OutputRow::EchoReply(reply) | OutputRow::Late { reply, .. } => {
            Some(reply.client_receive_wall)
        }
    }
}

fn row_client_send_wall(row: &OutputRow) -> Option<SystemTime> {
    match row {
        OutputRow::EchoSent {
            client_send_wall, ..
        }
        | OutputRow::Loss {
            client_send_wall, ..
        } => Some(*client_send_wall),
        OutputRow::EchoReply(reply) | OutputRow::Late { reply, .. } => reply.client_send_wall,
        _ => None,
    }
}

fn row_client_receive_wall(row: &OutputRow) -> Option<SystemTime> {
    match row {
        OutputRow::Duplicate {
            client_receive_wall,
            ..
        } => Some(*client_receive_wall),
        OutputRow::EchoReply(reply) | OutputRow::Late { reply, .. } => {
            Some(reply.client_receive_wall)
        }
        _ => None,
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

fn ipdv_pair(stats: Option<&EventRenderStats>, seq: u32) -> Option<&IpdvPair> {
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
