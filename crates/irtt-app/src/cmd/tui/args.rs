use std::time::Duration;

use clap::Parser;

use crate::shared::client::{
    parse_target, parse_test_duration, prepare_managed_run, CommonClientArgs, GroupPacingArg,
    ManagedRunSetup, TargetArg, TargetSelection,
};

pub const DEFAULT_TUI_DURATION: Duration = Duration::ZERO;

#[derive(Debug, Clone, Parser)]
#[command(name = "irtt-tui", about = "Minimal IRTT-compatible TUI client")]
pub struct TuiArgs {
    /// Server address/host, optionally prefixed with LABEL= and suffixed with @hmac=KEY. Repeat for multi-target mode.
    #[arg(
        value_name = "TARGET",
        num_args = 1..,
        required = true,
        value_parser = parse_target,
        long_help = "Server address/host, optionally prefixed with LABEL= and suffixed with @hmac=KEY. Repeat for multi-target mode.\n\nExplicit labels are used in the legend and status table. A target without @hmac= inherits --hmac; @hmac= disables it for that target.\n\nExamples:\n  irtt-tui host.example\n  irtt-tui eu=host.example\n  irtt-tui eu=host-a.example@hmac=secret us=host-b.example"
    )]
    pub targets: Vec<TargetArg>,

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
        default_value = "0",
        value_parser = parse_test_duration,
        help = "Test duration; use 0 for continuous mode",
        long_help = "Test duration; use 0 for continuous mode. The TUI defaults to continuous mode."
    )]
    pub duration: Duration,

    #[command(flatten)]
    pub common: CommonClientArgs,
}

impl TuiArgs {
    /// Validate the selected targets and prepare the managed run.
    ///
    /// The TUI always requires a target: the parser rejects an empty target
    /// list, and this rejects a target set that cannot be labelled uniquely.
    pub async fn prepare(&self) -> Result<ManagedRunSetup, String> {
        prepare_managed_run(
            &self.common,
            self.duration,
            TargetSelection {
                targets: &self.targets,
                pacing: self.pacing,
                stdin_controlled: false,
            },
        )
        .await
    }

    pub fn is_continuous(&self) -> bool {
        self.duration == Duration::ZERO
    }
}

impl std::ops::Deref for TuiArgs {
    type Target = CommonClientArgs;

    fn deref(&self) -> &Self::Target {
        &self.common
    }
}
