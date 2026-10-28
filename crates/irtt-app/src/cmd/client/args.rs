use std::time::Duration;

use crate::shared::client::{
    parse_target, parse_test_duration, prepare_managed_run, CommonClientArgs, GroupPacingArg,
    ManagedRunSetup, TargetArg, TargetSelection, TimestampArg,
};
use clap::{Parser, ValueEnum};

pub const DEFAULT_CLIENT_DURATION: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Parser)]
#[command(
    name = "irtt-client",
    version,
    about = "Minimal IRTT-compatible stream client"
)]
pub struct ClientArgs {
    /// Server address/host, optionally prefixed with LABEL= and suffixed with @hmac=KEY. Repeat for multi-target mode.
    #[arg(
        value_name = "TARGET",
        num_args = 0..,
        value_parser = parse_target,
        long_help = "Server address/host, optionally prefixed with LABEL= and suffixed with @hmac=KEY. Repeat for multi-target mode. Keys use literal UTF-8 or lowercase 0x followed by hexadecimal bytes.\n\nExamples:\n  irtt-client host-a:2112 host-b:2112\n  irtt-client eu=host-a:2112 us=host-b:2112\n  irtt-client ams=host-a:2112@hmac=secret public=host-b:2112@hmac=\n\nA target without @hmac= inherits --hmac. @hmac= explicitly disables it for that target."
    )]
    pub targets: Vec<TargetArg>,

    /// Read complete desired target sets from standard input in continuous mode.
    #[arg(
        long,
        long_help = "Read one complete desired target set per stdin line in continuous mode. Commas separate targets and backslash escapes commas within a target. [] selects an empty desired set. Records are limited to 64 KiB. EOF stops the client gracefully and may discard a revision that has not yet been acknowledged as applied."
    )]
    pub targets_stdin: bool,

    /// Negotiate parameters and exit without sending measurement probes.
    #[arg(
        short = 'n',
        long,
        conflicts_with = "targets_stdin",
        long_help = "Negotiate parameters with each target and exit without sending measurement probes. Duration options still set the requested duration, but do not keep this mode running. Reports negotiated parameters using the selected output format, without a measurement summary."
    )]
    pub no_test: bool,

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
        short = 'd',
        default_value = "10s",
        value_parser = parse_test_duration,
        help = "Test duration (e.g. 2.5s, 1m30s, 1h); use --continuous for continuous mode (0 is also accepted)",
        long_help = "Test duration (e.g. 2.5s, 1m30s, 500us, 100ns, 1h); use --continuous for continuous mode (0 is also accepted).\n\nWhen statistics support is compiled in and --no-stats is not selected, finite runs retain exact statistics for final summaries. Continuous mode uses bounded-memory running statistics and prints a final summary when interrupted; --targets-stdin also prints the final retained summary on EOF."
    )]
    pub duration: Duration,

    /// Run continuously until interrupted (equivalent to --duration 0).
    #[arg(long, conflicts_with = "duration")]
    pub continuous: bool,

    /// Disable statistics collection and final summaries; retain measurement event output.
    /// Accepted even when statistics support is not compiled in.
    #[arg(long)]
    pub no_stats: bool,

    /// Retry failed, closed, or silent sessions (requires continuous mode).
    #[arg(
        long,
        conflicts_with = "no_test",
        long_help = "Retry failed initial opens and closed, failed, or silent sessions independently per target. Requires --continuous or --duration 0. Unsuccessful connections use exponential backoff (defaults: 250ms initial, 10s maximum), configurable with --reconnect-initial-delay and --reconnect-max-delay. Use --reconnect-timeout to configure silence detection and --no-stats when only measurement events are needed."
    )]
    pub reconnect: bool,

    /// Reconnect after this long without valid replies while sending probes.
    #[arg(long, default_value = "10s", value_parser = crate::shared::client::parse_duration, requires = "reconnect")]
    pub reconnect_timeout: Duration,

    /// Initial retry delay, restored after a valid reply.
    #[arg(long, default_value = "250ms", value_parser = crate::shared::client::parse_duration, requires = "reconnect")]
    pub reconnect_initial_delay: Duration,

    /// Maximum retry delay (must be at least twice the initial delay).
    #[arg(long, default_value = "10s", value_parser = crate::shared::client::parse_duration, requires = "reconnect")]
    pub reconnect_max_delay: Duration,

    #[command(flatten)]
    pub common: CommonClientArgs,

    /// Event row output format.
    #[arg(
        long,
        value_enum,
        short = 'f',
        default_value_t = OutputFormat::Table,
        long_help = "Event row output format.\n\nTable is the default interactive format. CSV, TSV, and JSON Lines default to all columns for structured export."
    )]
    pub format: OutputFormat,

    /// Filter output by event type.
    #[arg(
        long,
        value_name = "EVENTS",
        value_delimiter = ',',
        value_enum,
        long_help = "Filter output by event type. Accepts comma-separated names or repeated options. If omitted, the normal output behavior is unchanged."
    )]
    pub events: Vec<EventKind>,

    /// Comma-separated event row columns, or default/all.
    #[arg(
        short = 'c',
        long,
        value_name = "COLUMNS",
        long_help = "Comma-separated event row columns, or default/all.\n\nThe default table columns are compact and hide echo_sent rows unless explicitly selected with --events. Custom table columns include all event rows. Run --list-columns to see valid names."
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
    #[arg(short = 'v', long)]
    pub verbose: bool,
}

impl ClientArgs {
    /// Validate the selected targets and prepare the managed run.
    ///
    /// Targets are only required here. `--list-columns` returns before this is
    /// called, so listing columns still needs no target.
    pub async fn prepare(&self) -> Result<ManagedRunSetup, String> {
        if self.reconnect && !self.is_continuous() {
            return Err("--reconnect requires --continuous or --duration 0".to_owned());
        }
        if self
            .reconnect_initial_delay
            .checked_mul(2)
            .is_none_or(|minimum| self.reconnect_max_delay < minimum)
        {
            return Err(
                "--reconnect-max-delay must be at least twice --reconnect-initial-delay".to_owned(),
            );
        }
        if self.targets_stdin && !self.is_continuous() {
            return Err("--targets-stdin requires --continuous or --duration 0".to_owned());
        }
        let mut setup = prepare_managed_run(
            &self.common,
            if self.continuous {
                Duration::ZERO
            } else {
                self.duration
            },
            TargetSelection {
                targets: &self.targets,
                pacing: self.pacing,
                stdin_controlled: self.targets_stdin,
            },
        )
        .await?;
        if self.no_test {
            setup.client.request.run_mode = irtt_client::RunMode::NoTest;
        }
        Ok(setup)
    }

    pub fn is_continuous(&self) -> bool {
        self.continuous || self.duration == Duration::ZERO
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
#[value(rename_all = "snake_case")]
pub enum EventKind {
    SessionStarted,
    NoTestCompleted,
    SessionClosed,
    EchoSent,
    EchoReply,
    Loss,
    Duplicate,
    Late,
    Warning,
}

impl EventKind {
    pub(super) fn name(self) -> String {
        self.to_possible_value()
            .expect("all event kinds have a CLI value")
            .get_name()
            .to_owned()
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
