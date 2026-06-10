use std::time::Duration;

use clap::Parser;
use irtt_client::ClientConfig;

#[cfg(test)]
use crate::shared::client::TimestampArg;
use crate::shared::client::{parse_test_duration, CommonClientArgs};

pub const DEFAULT_TUI_DURATION: Duration = Duration::ZERO;

#[derive(Debug, Clone, Parser)]
#[command(name = "irtt-tui", about = "Minimal IRTT-compatible TUI client")]
pub struct TuiArgs {
    /// Server address or host, with optional port.
    pub server: String,

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
        self.common.to_client_config(&self.server, self.duration)
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
        assert_eq!(args.server, "127.0.0.1:2112");
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
}
