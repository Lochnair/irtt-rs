use std::time::Duration;

use clap::Parser;
use irtt_client::ClientConfig;

#[cfg(test)]
use crate::shared::client::TimestampArg;
use crate::shared::client::{
    parse_labelled_target, parse_test_duration, resolved_managed_targets, target_specs,
    CommonClientArgs, GroupPacingArg, LabelledTargetArg, ResolvedTarget, TargetSpec,
};

pub const DEFAULT_TUI_DURATION: Duration = Duration::ZERO;

#[derive(Debug, Clone, Parser)]
#[command(name = "irtt-tui", about = "Minimal IRTT-compatible TUI client")]
pub struct TuiArgs {
    /// Server address or host, with optional port. Repeat for multi-target mode.
    #[arg(
        value_name = "TARGET",
        num_args = 0..,
        required_unless_present = "labelled_targets",
        long_help = "Server address or host, with optional port. Repeat the positional target for multi-target mode, for example: irtt-tui host-a:2112 host-b:2112."
    )]
    pub targets: Vec<String>,

    /// Explicit labelled target, as label=host:port. Repeat for multi-target mode.
    #[arg(
        long = "target",
        value_name = "LABEL=TARGET",
        value_parser = parse_labelled_target,
        long_help = "Explicit labelled target, as label=host:port. Repeat for multi-target mode. The label is used in the legend and status table and must be unique."
    )]
    pub labelled_targets: Vec<LabelledTargetArg>,

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
    pub fn to_client_config(&self) -> ClientConfig {
        self.common.to_client_config(
            self.primary_target_addr()
                .expect("at least one target is required"),
            self.duration,
        )
    }

    pub fn is_continuous(&self) -> bool {
        self.duration == Duration::ZERO
    }

    pub fn target_specs(&self) -> Result<Vec<TuiTargetSpec>, String> {
        target_specs(&self.targets, &self.labelled_targets)
    }

    pub fn resolved_managed_targets(&self) -> Result<Vec<ResolvedTuiTarget>, String> {
        let specs = self.target_specs()?;
        let config = self.to_client_config();
        resolved_managed_targets(specs, &config)
    }

    #[cfg(test)]
    pub fn timestamp_mode(&self) -> TimestampArg {
        self.common.tstamp
    }

    fn primary_target_addr(&self) -> Option<&str> {
        self.targets.first().map(String::as_str).or_else(|| {
            self.labelled_targets
                .first()
                .map(|target| target.addr.as_str())
        })
    }
}

pub type TuiTargetSpec = TargetSpec;
pub type ResolvedTuiTarget = ResolvedTarget;

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
        assert_eq!(args.targets, ["127.0.0.1:2112"]);
        assert!(args.labelled_targets.is_empty());
        assert_eq!(args.pacing, GroupPacingArg::Staggered);
        assert_eq!(args.duration, DEFAULT_TUI_DURATION);
        assert!(args.is_continuous());
        assert_eq!(args.to_client_config().duration, None);

        let finite = parse(&["--duration", "30s", "127.0.0.1:2112"]).unwrap();
        assert_eq!(finite.duration, Duration::from_secs(30));
        assert_eq!(
            finite.to_client_config().duration,
            Some(Duration::from_secs(30))
        );

        assert!(parse(&["--output", "human", "127.0.0.1:2112"]).is_err());
        let help = TuiArgs::command().render_help().to_string();
        assert!(!help.contains("--output"));
    }

    #[test]
    fn multiple_positional_targets_parse() {
        let args = parse(&["host-a:2112", "host-b:2112"]).unwrap();
        let specs = args.target_specs().unwrap();

        assert_eq!(specs[0].label, "host-a:2112");
        assert_eq!(specs[0].addr, "host-a:2112");
        assert_eq!(specs[1].label, "host-b:2112");
        assert_eq!(specs[1].addr, "host-b:2112");
    }

    #[test]
    fn repeated_labelled_targets_parse() {
        let args = parse(&[
            "--target",
            "ams=ams.example.com:2112",
            "--target",
            "sg=sg.example.com:2112",
        ])
        .unwrap();
        let specs = args.target_specs().unwrap();

        assert_eq!(specs[0].label, "ams");
        assert_eq!(specs[0].addr, "ams.example.com:2112");
        assert_eq!(specs[1].label, "sg");
        assert_eq!(specs[1].addr, "sg.example.com:2112");
    }

    #[test]
    fn at_least_one_target_is_required() {
        assert!(parse(&[]).is_err());
    }

    #[test]
    fn duplicate_labels_are_rejected() {
        let args = parse(&["host-a:2112", "--target", "host-a:2112=host-b:2112"]).unwrap();
        let err = args.target_specs().unwrap_err();

        assert!(err.contains("duplicate target label"));
    }

    #[test]
    fn duplicate_positional_target_strings_get_stable_suffixes() {
        let args = parse(&["host-a:2112", "host-a:2112"]).unwrap();
        let specs = args.target_specs().unwrap();

        assert_eq!(specs[0].label, "host-a:2112");
        assert_eq!(specs[1].label, "host-a:2112#2");
    }

    #[test]
    fn duplicate_resolved_target_addresses_are_rejected() {
        let args = parse(&["127.0.0.1:2112", "127.0.0.1"]).unwrap();
        let err = args.resolved_managed_targets().unwrap_err();

        assert!(err.contains("duplicate resolved target address 127.0.0.1:2112"));
    }

    #[test]
    fn invalid_labelled_target_syntax_is_rejected() {
        assert!(parse(&["--target", "missing-equals"]).is_err());
        assert!(parse(&["--target", "=127.0.0.1:2112"]).is_err());
        assert!(parse(&["--target", "label="]).is_err());
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
        let tui = parse(&tui_args).unwrap().to_client_config();
        let common = parse_shared(&shared).unwrap().common;
        let shared = common.to_client_config("127.0.0.1:2112", Duration::from_secs(30));

        assert_eq!(shared.server_addr, tui.server_addr);
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
    fn tui_help_lists_multi_target_options() {
        let help = TuiArgs::command().render_help().to_string();
        assert!(help.contains("--target <LABEL=TARGET>"));
        assert!(help.contains("--pacing <PACING>"));
    }
}
