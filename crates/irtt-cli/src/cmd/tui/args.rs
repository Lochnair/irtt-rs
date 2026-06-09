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
    use crate::cmd::client::ClientArgs;
    use clap::{CommandFactory, Parser};

    fn parse(args: &[&str]) -> Result<TuiArgs, clap::Error> {
        let mut argv = vec!["irtt-tui"];
        argv.extend_from_slice(args);
        TuiArgs::try_parse_from(argv)
    }

    fn parse_client(args: &[&str]) -> Result<ClientArgs, clap::Error> {
        let mut argv = vec!["irtt-cli"];
        argv.extend_from_slice(args);
        ClientArgs::try_parse_from(argv)
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
    fn shared_client_options_match_between_stream_and_tui_parsers() {
        let shared = [
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
        let client = parse_client(&shared).unwrap().to_client_config();
        let tui = parse(&shared).unwrap().to_client_config();

        assert_eq!(client.server_addr, tui.server_addr);
        assert_eq!(client.duration, tui.duration);
        assert_eq!(client.interval, tui.interval);
        assert_eq!(client.length, tui.length);
        assert_eq!(client.received_stats, tui.received_stats);
        assert_eq!(client.stamp_at, tui.stamp_at);
        assert_eq!(client.clock, tui.clock);
        assert_eq!(client.dscp, tui.dscp);
        assert_eq!(client.hmac_key, tui.hmac_key);
        assert_eq!(client.server_fill, tui.server_fill);
        assert_eq!(client.negotiation_policy, tui.negotiation_policy);
        assert_eq!(client.socket_config.ttl, tui.socket_config.ttl);
    }
}
