use std::time::Duration;

use crate::shared::client::{
    parse_target, parse_test_duration, prepare_managed_run, CommonClientArgs, GroupPacingArg,
    ManagedRunSetup, TargetArg, TargetSelection, TimestampArg,
};
use clap::{Parser, ValueEnum};

pub const DEFAULT_CLIENT_DURATION: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Parser)]
#[command(name = "irtt-client", about = "Minimal IRTT-compatible stream client")]
pub struct ClientArgs {
    /// Server address/host, optionally prefixed with LABEL= and suffixed with @hmac=KEY. Repeat for multi-target mode.
    #[arg(
        value_name = "TARGET",
        num_args = 0..,
        value_parser = parse_target,
        long_help = "Server address/host, optionally prefixed with LABEL= and suffixed with @hmac=KEY. Repeat for multi-target mode.\n\nExamples:\n  irtt-client host-a:2112 host-b:2112\n  irtt-client eu=host-a:2112 us=host-b:2112\n  irtt-client ams=host-a:2112@hmac=secret public=host-b:2112@hmac=\n\nA target without @hmac= inherits --hmac. @hmac= explicitly disables it for that target."
    )]
    pub targets: Vec<TargetArg>,

    /// Read complete desired target sets from standard input in continuous mode.
    #[arg(
        long,
        long_help = "Read one complete desired target set per stdin line in continuous mode. Commas separate targets and backslash escapes commas within a target. [] selects an empty desired set. Records are limited to 64 KiB. EOF stops the client gracefully and may discard a revision that has not yet been acknowledged as applied."
    )]
    pub targets_stdin: bool,

    /// Managed group pacing for multi-target mode.
    #[arg(
        long,
        value_enum,
        default_value_t = GroupPacingArg::Staggered,
        long_help = "Managed group pacing for multi-target mode.\n\nstaggered spaces active targets across the probe interval. burst sends one probe to every active target back-to-back once per interval."
    )]
    pub pacing: GroupPacingArg,

    #[arg(
        long,
        default_value = "10s",
        value_parser = parse_test_duration,
        help = "Test duration; use 0 for continuous mode",
        long_help = "Test duration; use 0 for continuous mode.\n\nFinite runs retain exact statistics for final summaries. Continuous mode uses bounded-memory running statistics and prints a final summary when interrupted; --targets-stdin also prints the final retained summary on EOF."
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
    /// Validate the selected targets and prepare the managed run.
    ///
    /// Targets are only required here. `--list-columns` returns before this is
    /// called, so listing columns still needs no target.
    pub fn prepare(&self) -> Result<ManagedRunSetup, String> {
        if self.targets_stdin && !self.is_continuous() {
            return Err("--targets-stdin requires --duration 0".to_owned());
        }
        prepare_managed_run(
            &self.common,
            self.duration,
            TargetSelection {
                targets: &self.targets,
                pacing: self.pacing,
                stdin_controlled: self.targets_stdin,
            },
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
