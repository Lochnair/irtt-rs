use std::time::Duration;

use clap::{Parser, ValueEnum};
use irtt_client::ClientConfig;

use crate::shared::client::{parse_test_duration, CommonClientArgs, TimestampArg};

pub const DEFAULT_CLIENT_DURATION: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Parser)]
#[command(name = "irtt-cli", about = "Minimal IRTT-compatible stream client")]
pub struct ClientArgs {
    /// Server address or host, with optional port.
    #[arg(required_unless_present = "list_columns")]
    pub server: Option<String>,

    #[arg(
        long,
        default_value = "10s",
        value_parser = parse_test_duration,
        help = "Test duration; use 0 for continuous mode",
        long_help = "Test duration; use 0 for continuous mode.\n\nFinite runs retain exact statistics for final summaries. Continuous mode uses bounded-memory running statistics and prints a final summary only when interrupted."
    )]
    pub duration: Duration,

    #[command(flatten)]
    pub common: CommonClientArgs,

    /// Event row output format.
    #[arg(
        long,
        value_enum,
        default_value_t = OutputFormat::Table,
        long_help = "Event row output format.\n\nTable is the default interactive format. CSV, TSV, and JSON Lines default to all columns for structured export."
    )]
    pub format: OutputFormat,

    /// Comma-separated event row columns, or default/all.
    #[arg(
        short = 'c',
        long,
        value_name = "COLUMNS",
        long_help = "Comma-separated event row columns, or default/all.\n\nThe default table columns are compact and hide echo_sent rows. Custom table columns include all event rows. Run --list-columns to see valid names."
    )]
    pub columns: Option<String>,

    /// List available event row columns and aliases, then exit.
    #[arg(
        long,
        long_help = "List available event row columns and aliases, then exit.\n\nA server argument is not required when listing columns."
    )]
    pub list_columns: bool,

    /// Header policy for table, CSV, and TSV output.
    #[arg(
        long,
        value_enum,
        default_value_t = HeaderMode::Auto,
        long_help = "Header policy for table, CSV, and TSV output.\n\nJSON Lines never prints a header."
    )]
    pub header: HeaderMode,

    /// Include extra fields in table output and final summaries.
    #[arg(long)]
    pub verbose: bool,
}

impl ClientArgs {
    pub fn to_client_config(&self) -> ClientConfig {
        self.common.to_client_config(
            self.server
                .as_deref()
                .expect("server is required unless --list-columns is set"),
            self.duration,
        )
    }

    pub fn is_continuous(&self) -> bool {
        self.duration == Duration::ZERO
    }

    pub fn timestamp_mode(&self) -> TimestampArg {
        self.common.tstamp
    }
}

impl std::ops::Deref for ClientArgs {
    type Target = CommonClientArgs;

    fn deref(&self) -> &Self::Target {
        &self.common
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    /// Readable terminal table output.
    Table,
    /// Comma-separated event rows.
    Csv,
    /// Tab-separated event rows.
    Tsv,
    /// One JSON object per event row.
    Jsonl,
}

impl OutputFormat {
    pub fn prints_summary(self) -> bool {
        matches!(self, Self::Table)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum HeaderMode {
    /// Print headers for table, CSV, and TSV output.
    Auto,
    /// Always print headers where the format supports them.
    Always,
    /// Never print headers.
    Never,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser};
    use irtt_client::NegotiationPolicy;
    use irtt_proto::{Clock, ReceivedStats, StampAt};

    fn parse(args: &[&str]) -> Result<ClientArgs, clap::Error> {
        let mut argv = vec!["irtt-cli"];
        argv.extend_from_slice(args);
        ClientArgs::try_parse_from(argv)
    }

    #[test]
    fn client_parser_keeps_finite_default_duration() {
        let args = parse(&["127.0.0.1:2112"]).unwrap();

        assert_eq!(args.server.as_deref(), Some("127.0.0.1:2112"));
        assert_eq!(args.duration, DEFAULT_CLIENT_DURATION);
        assert_eq!(args.interval, Duration::from_secs(1));
        assert_eq!(args.format, OutputFormat::Table);
        assert_eq!(args.columns, None);
        assert_eq!(args.header, HeaderMode::Auto);
        assert!(!args.is_continuous());
        assert_eq!(args.timestamp_mode(), TimestampArg::Both);

        let config = args.to_client_config();
        assert_eq!(config.duration, Some(DEFAULT_CLIENT_DURATION));
        assert_eq!(config.received_stats, ReceivedStats::Both);
        assert_eq!(config.stamp_at, StampAt::Both);
        assert_eq!(config.clock, Clock::Both);
        assert_eq!(config.negotiation_policy, NegotiationPolicy::Strict);
    }

    #[test]
    fn client_parser_accepts_stream_formats_and_rejects_tui_output() {
        for format in ["table", "csv", "tsv", "jsonl"] {
            assert!(parse(&["--format", format, "127.0.0.1:2112"]).is_ok());
        }
        assert!(parse(&["--format", "tui", "127.0.0.1:2112"]).is_err());
    }

    #[test]
    fn list_columns_does_not_require_server() {
        let args = parse(&["--list-columns"]).unwrap();
        assert_eq!(args.server, None);
        assert!(args.list_columns);
    }

    #[test]
    fn client_help_lists_shared_protocol_options() {
        let help = ClientArgs::command().render_help().to_string();
        assert!(help.contains("--format <FORMAT>"));
        assert!(help.contains("--columns <COLUMNS>"));
        assert!(help.contains("--list-columns"));
        assert!(help.contains("--header <HEADER>"));
        assert!(help.contains("--tstamp <MODE>"));
        assert!(help.contains("--stats <STATS>"));
        assert!(help.contains("--sfill <STRING>"));
        assert!(help.contains("--dscp <0..=63>"));
        assert!(help.contains("--ttl <1..=255>"));
        assert!(help.contains("--loose"));
    }

    #[test]
    fn output_format_summary_policy_is_table_only() {
        assert!(OutputFormat::Table.prints_summary());
        assert!(!OutputFormat::Csv.prints_summary());
        assert!(!OutputFormat::Tsv.prints_summary());
        assert!(!OutputFormat::Jsonl.prints_summary());
    }
}
