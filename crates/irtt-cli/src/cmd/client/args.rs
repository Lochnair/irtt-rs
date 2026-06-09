use std::time::Duration;

use clap::{Parser, ValueEnum};
use irtt_client::ClientConfig;

use crate::shared::client::{parse_test_duration, CommonClientArgs, TimestampArg};

pub const DEFAULT_CLIENT_DURATION: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Parser)]
#[command(name = "irtt-cli", about = "Minimal IRTT-compatible stream client")]
pub struct ClientArgs {
    /// Server address or host, with optional port.
    pub server: String,

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

    /// Output format.
    #[arg(long, value_enum, default_value_t = OutputMode::Human)]
    pub output: OutputMode,

    /// Include extra fields in human output.
    #[arg(long)]
    pub verbose: bool,
}

impl ClientArgs {
    pub fn to_client_config(&self) -> ClientConfig {
        self.common.to_client_config(&self.server, self.duration)
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
pub enum OutputMode {
    /// Readable terminal output with a final summary.
    Human,
    /// Parseable full event fields.
    Machine,
    /// Simple key=value-ish event stream.
    Simple,
    /// RTT microseconds only.
    RttUs,
}

impl OutputMode {
    pub fn prints_summary(self) -> bool {
        matches!(self, Self::Human)
    }
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

        assert_eq!(args.server, "127.0.0.1:2112");
        assert_eq!(args.duration, DEFAULT_CLIENT_DURATION);
        assert_eq!(args.interval, Duration::from_secs(1));
        assert_eq!(args.output, OutputMode::Human);
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
    fn client_parser_accepts_stream_outputs_and_rejects_tui_output() {
        for output in ["human", "simple", "machine", "rtt-us"] {
            assert!(parse(&["--output", output, "127.0.0.1:2112"]).is_ok());
        }
        assert!(parse(&["--output", "tui", "127.0.0.1:2112"]).is_err());
    }

    #[test]
    fn client_help_lists_shared_protocol_options() {
        let help = ClientArgs::command().render_help().to_string();
        assert!(help.contains("--output <OUTPUT>"));
        assert!(help.contains("--tstamp <MODE>"));
        assert!(help.contains("--stats <STATS>"));
        assert!(help.contains("--sfill <STRING>"));
        assert!(help.contains("--dscp <0..=63>"));
        assert!(help.contains("--ttl <1..=255>"));
        assert!(help.contains("--loose"));
    }

    #[test]
    fn output_mode_summary_policy_is_human_only() {
        assert!(OutputMode::Human.prints_summary());
        assert!(!OutputMode::Simple.prints_summary());
        assert!(!OutputMode::Machine.prints_summary());
        assert!(!OutputMode::RttUs.prints_summary());
    }
}
