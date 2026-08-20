use std::time::Duration;

use clap::Parser;

#[cfg(test)]
use crate::shared::client::TimestampArg;
use crate::shared::client::{
    parse_target, parse_test_duration, prepare_managed_run, CommonClientArgs, GroupPacingArg,
    ManagedRunSetup, TargetArg, TargetSelection,
};

pub const DEFAULT_TUI_DURATION: Duration = Duration::ZERO;

#[derive(Debug, Clone, Parser)]
#[command(name = "irtt-tui", about = "Minimal IRTT-compatible TUI client")]
pub struct TuiArgs {
    /// Server address/host, optionally prefixed with LABEL=. Repeat for multi-target mode.
    #[arg(
        value_name = "TARGET",
        num_args = 1..,
        required = true,
        value_parser = parse_target,
        long_help = "Server address/host, optionally prefixed with LABEL=. Repeat for multi-target mode.\n\nExplicit labels are used in the legend and status table.\n\nExamples:\n  irtt-tui host.example\n  irtt-tui eu=host.example\n  irtt-tui eu=host-a.example us=host-b.example"
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
    pub fn prepare(&self) -> Result<ManagedRunSetup, String> {
        prepare_managed_run(
            &self.common,
            self.duration,
            TargetSelection {
                targets: &self.targets,
                pacing: self.pacing,
            },
        )
    }

    pub fn is_continuous(&self) -> bool {
        self.duration == Duration::ZERO
    }

    #[cfg(test)]
    pub fn timestamp_mode(&self) -> TimestampArg {
        self.common.tstamp
    }
}

impl std::ops::Deref for TuiArgs {
    type Target = CommonClientArgs;

    fn deref(&self) -> &Self::Target {
        &self.common
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser};

    #[derive(Parser)]
    struct SharedOnlyArgs {
        #[command(flatten)]
        common: CommonClientArgs,
    }

    fn parse(args: &[&str]) -> Result<TuiArgs, clap::Error> {
        let mut argv = vec!["irtt-tui"];
        argv.extend_from_slice(args);
        TuiArgs::try_parse_from(argv)
    }

    fn parse_shared(args: &[&str]) -> Result<SharedOnlyArgs, clap::Error> {
        let mut argv = vec!["shared-only"];
        argv.extend_from_slice(args);
        SharedOnlyArgs::try_parse_from(argv)
    }

    #[test]
    fn tui_parser_defaults_to_continuous_and_has_no_output_option() {
        let args = parse(&["127.0.0.1:2112"]).unwrap();
        assert_eq!(args.targets.len(), 1);
        assert_eq!(args.targets[0].label, None);
        assert_eq!(args.targets[0].addr, "127.0.0.1:2112");
        assert_eq!(args.pacing, GroupPacingArg::Staggered);
        assert_eq!(args.duration, DEFAULT_TUI_DURATION);
        assert!(args.is_continuous());
        assert_eq!(args.prepare().unwrap().client.duration, None);

        let finite = parse(&["--duration", "30s", "127.0.0.1:2112"]).unwrap();
        assert_eq!(finite.duration, Duration::from_secs(30));
        assert_eq!(
            finite.prepare().unwrap().client.duration,
            Some(Duration::from_secs(30))
        );

        assert!(parse(&["--output", "human", "127.0.0.1:2112"]).is_err());
        let help = TuiArgs::command().render_help().to_string();
        assert!(!help.contains("--output"));
    }

    #[test]
    fn multiple_positional_targets_parse() {
        let args = parse(&["host-a:2112", "host-b:2112"]).unwrap();
        let specs = args.prepare().unwrap().targets;

        assert_eq!(specs[0].label, "host-a:2112");
        assert_eq!(specs[0].managed.server_addr, "host-a:2112");
        assert_eq!(specs[1].label, "host-b:2112");
        assert_eq!(specs[1].managed.server_addr, "host-b:2112");
    }

    #[test]
    fn repeated_labeled_targets_parse() {
        let args = parse(&["ams=ams.example.com:2112", "sg=sg.example.com:2112"]).unwrap();
        let specs = args.prepare().unwrap().targets;

        assert_eq!(specs[0].label, "ams");
        assert_eq!(specs[0].managed.server_addr, "ams.example.com:2112");
        assert_eq!(specs[1].label, "sg");
        assert_eq!(specs[1].managed.server_addr, "sg.example.com:2112");
    }

    #[test]
    fn at_least_one_target_is_required() {
        // The TUI has no target-free mode, so this is rejected at parse time
        // rather than during preparation.
        assert!(parse(&[]).is_err());
        assert!(parse(&["ams=ams.example.com:2112"]).is_ok());
    }

    #[test]
    fn duplicate_labels_are_rejected() {
        let args = parse(&["host-a:2112", "host-a:2112=host-b:2112"]).unwrap();
        let err = args.prepare().unwrap_err();

        assert!(err.contains("duplicate target label"));
    }

    #[test]
    fn duplicate_positional_target_strings_get_stable_suffixes() {
        let args = parse(&["host-a:2112", "host-a:2112"]).unwrap();
        let specs = args.prepare().unwrap().targets;

        assert_eq!(specs[0].label, "host-a:2112");
        assert_eq!(specs[1].label, "host-a:2112#2");
    }

    #[test]
    fn duplicate_target_endpoints_are_allowed() {
        let args = parse(&["127.0.0.1:2112", "127.0.0.1"]).unwrap();
        assert_eq!(args.prepare().unwrap().target_count(), 2);
    }

    #[test]
    fn invalid_labelled_target_syntax_is_rejected() {
        assert!(parse(&["=127.0.0.1:2112"]).is_err());
        assert!(parse(&["label="]).is_err());
    }

    #[test]
    fn old_target_option_is_rejected() {
        assert!(parse(&["--target", "eu=host.example"]).is_err());
    }

    #[test]
    fn pacing_option_accepts_supported_values() {
        assert_eq!(
            parse(&["--pacing", "staggered", "127.0.0.1:2112"])
                .unwrap()
                .pacing,
            GroupPacingArg::Staggered
        );
        assert_eq!(
            parse(&["--pacing", "burst", "127.0.0.1:2112"])
                .unwrap()
                .pacing,
            GroupPacingArg::Burst
        );
    }

    #[test]
    fn shared_client_options_match_tui_config_mapping() {
        let shared = [
            "--interval",
            "250ms",
            "--length",
            "128",
            "--hmac",
            "secret",
            "--clock",
            "monotonic",
            "--tstamp",
            "receive",
            "--stats",
            "count",
            "--sfill",
            "abc",
            "--dscp",
            "46",
            "--ttl",
            "64",
            "--loose",
        ];
        let tui_args = [
            "--duration",
            "30s",
            "--interval",
            "250ms",
            "--length",
            "128",
            "--hmac",
            "secret",
            "--clock",
            "monotonic",
            "--tstamp",
            "receive",
            "--stats",
            "count",
            "--sfill",
            "abc",
            "--dscp",
            "46",
            "--ttl",
            "64",
            "--loose",
            "127.0.0.1:2112",
        ];
        let tui = parse(&tui_args).unwrap().prepare().unwrap().client;
        let common = parse_shared(&shared).unwrap().common;
        let shared = common.to_client_config(Duration::from_secs(30));

        assert_eq!(shared.duration, tui.duration);
        assert_eq!(shared.interval, tui.interval);
        assert_eq!(shared.length, tui.length);
        assert_eq!(shared.received_stats, tui.received_stats);
        assert_eq!(shared.stamp_at, tui.stamp_at);
        assert_eq!(shared.clock, tui.clock);
        assert_eq!(shared.dscp, tui.dscp);
        assert_eq!(shared.hmac_key, tui.hmac_key);
        assert_eq!(shared.server_fill, tui.server_fill);
        assert_eq!(shared.negotiation_policy, tui.negotiation_policy);
        assert_eq!(shared.socket_config.ttl, tui.socket_config.ttl);
    }

    #[test]
    fn an_empty_hmac_key_is_rejected_via_the_shared_args() {
        // CommonClientArgs is flattened into both the client and TUI parsers,
        // so proving rejection once here covers both without duplicating it.
        let err = parse(&["--hmac", "", "127.0.0.1:2112"]).unwrap_err();
        assert!(err.to_string().contains("HMAC key must not be empty"));
    }

    #[test]
    fn tui_help_lists_multi_target_options() {
        let help = TuiArgs::command().render_help().to_string();
        assert!(!help.contains("--target "));
        assert!(help.contains("--pacing <PACING>"));
    }
}
