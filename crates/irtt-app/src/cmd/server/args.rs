use std::{
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    time::Duration,
};

use clap::{Parser, ValueEnum};
use irtt_server::{ServerConfig, TimestampAllowance};

/// The ordinary IRTT port, used to build the zero-argument default binds.
const DEFAULT_PORT: u16 = 2112;

// Every policy option is optional. An omitted option leaves the corresponding
// `ServerConfig` default in place rather than restating it here, so library and
// CLI defaults cannot drift apart.
#[derive(Debug, Clone, Parser)]
#[command(
    name = "irtt-server",
    about = "Minimal IRTT-compatible UDP server",
    after_help = "With no --bind, the server listens on the wildcard IRTT port for both address families ([::]:2112 and 0.0.0.0:2112) where the platform supports wildcard reply-source selection.\nRepeat --bind to serve multiple addresses in one process; any explicit --bind replaces the default pair entirely. All listeners use the same policy but keep independent session namespaces."
)]
pub struct ServerArgs {
    /// Local UDP address to bind, as ADDR:PORT. Repeatable.
    #[arg(
        long,
        value_name = "ADDR",
        long_help = "Local UDP address to bind, as ADDR:PORT, for example 127.0.0.1:2112 or [::1]:2112. Host names are not resolved.\n\nRepeat the option to serve several addresses from one process, in the order given. Every listener applies the same policy options, but each keeps its own sessions and tokens, so a session belongs to the address it was opened on. Binding is all or nothing: if any address cannot be bound, none are served.\n\nWith no --bind at all, the server binds the wildcard IRTT port on both address families, [::]:2112 then 0.0.0.0:2112, on platforms with wildcard reply-source support; elsewhere it fails and asks for an explicit address instead of guessing one. Any explicit --bind replaces that default pair rather than adding to it.\n\nA wildcard bind such as 0.0.0.0:2112 answers each request from the address it was sent to, using per-packet destination metadata. That is supported on Linux, macOS and FreeBSD; on other systems a wildcard bind is refused and an explicit interface address is required.\n\nA port of 0 selects an unused port per listener, so two such binds get two different ports."
    )]
    pub bind: Vec<SocketAddr>,

    /// HMAC key; every request must then carry a valid MAC.
    #[arg(
        long,
        value_name = "KEY",
        value_parser = parse_hmac_key,
        long_help = "HMAC key, taken as the UTF-8 bytes of this argument.\n\nAuthentication is global: with a key configured, every request must carry a valid MAC and every reply is authenticated. Without one, authenticated requests are dropped. The key is visible in the process arguments."
    )]
    pub hmac: Option<String>,

    /// Maximum simultaneously live sessions per listener.
    #[arg(
        long,
        value_name = "COUNT",
        long_help = "Maximum number of simultaneously live sessions per listener. Once a listener's table is full, a session-creating open to it is dropped silently; nothing is evicted. Zero refuses every session-creating open.\n\nThis is a per-listener bound, not a process-wide one: with two --bind options, each listener admits up to this many sessions."
    )]
    pub max_sessions: Option<usize>,

    /// Maximum echo datagram size a session may negotiate, in bytes.
    #[arg(
        long,
        value_name = "BYTES",
        long_help = "Maximum echo datagram size a session may negotiate, in bytes. A longer requested length is reduced during negotiation, an open whose mandatory field block would not fit is refused, and inbound echo requests are admitted against the same limit. This is a resource bound, not an MTU."
    )]
    pub max_packet_length: Option<usize>,

    /// Floor on the negotiated send interval.
    #[arg(
        long,
        value_name = "DURATION",
        value_parser = parse_duration_allow_zero,
        long_help = "Floor on the send interval a session may negotiate. The negotiated interval is still capped at a quarter of the idle timeout afterwards, so a value above that cap is not what actually gets negotiated.\n\nA session's reply allowance refills at the shorter of this floor and the interval it actually negotiated. Ordinarily that is this floor itself; it is the shorter, negotiated value only where the idle-timeout cap above pulled the negotiated interval below it. Use 0 for no time-based throttling."
    )]
    pub min_interval: Option<Duration>,

    /// Echo replies a session may burst before its allowance refills.
    #[arg(
        long,
        value_name = "COUNT",
        long_help = "Echo replies one session may have answered before its allowance has to refill. Use 0 for no allowance at all, which rate-limits every echo request whatever the interval is."
    )]
    pub burst: Option<u32>,

    /// Release a session after this long without an echo request.
    #[arg(
        long,
        value_name = "DURATION",
        value_parser = parse_duration_allow_zero,
        long_help = "Release a session after this long without a served or rate-limited echo request. The deadline runs from the open, and release is silent. Use 0 to expire a session at the next evaluation; it is not a way to disable expiry."
    )]
    pub idle_timeout: Option<Duration>,

    /// Maximum test duration a session may negotiate.
    #[arg(
        long,
        value_name = "DURATION",
        value_parser = parse_positive_duration,
        long_help = "Maximum test duration a session may negotiate. A longer request is reduced to it, and a continuous request is answered with it. Omit this flag for no maximum; a maximum of zero cannot be expressed, because a negotiated duration of zero means continuous."
    )]
    pub max_duration: Option<Duration>,

    /// How many timestamps this server will provide.
    #[arg(
        long,
        value_name = "MODE",
        value_enum,
        long_help = "How many timestamps this server will provide.\n\ndual: honor a request for send, receive, both or midpoint timestamps.\nsingle: provide at most one timestamp instant; a request for both is negotiated to midpoint.\nnone: provide no timestamps at all.\n\nThe requested clock source is never changed, only which instants are reported, so a single instant is still reported once per requested clock. Omit this flag to honor every requested placement."
    )]
    pub timestamp_allowance: Option<TimestampAllowanceArg>,

    /// Refuse to provide requested DSCP marking.
    #[arg(
        long,
        long_help = "Refuse to provide requested traffic-class marking. Any requested DSCP is negotiated to zero, so the client is told its echo replies will be unmarked, and they are sent unmarked. The session is not refused."
    )]
    pub no_dscp: bool,
}

/// The command-line spelling of [`TimestampAllowance`].
///
/// A CLI-local enum rather than a `clap` derive on the library type: `irtt-server`
/// is a reusable crate and has no business depending on an argument parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum TimestampAllowanceArg {
    /// Honor every requested timestamp placement.
    Dual,
    /// Provide at most one timestamp instant; both becomes midpoint.
    Single,
    /// Provide no timestamps.
    None,
}

impl From<TimestampAllowanceArg> for TimestampAllowance {
    fn from(value: TimestampAllowanceArg) -> Self {
        match value {
            TimestampAllowanceArg::Dual => Self::Dual,
            TimestampAllowanceArg::Single => Self::Single,
            TimestampAllowanceArg::None => Self::None,
        }
    }
}

impl ServerArgs {
    /// The addresses this invocation binds: the requested `--bind` list, or
    /// the wildcard IRTT port on both address families if none was given.
    #[must_use]
    pub fn resolve_binds(&self) -> Vec<SocketAddr> {
        if self.bind.is_empty() {
            vec![
                SocketAddr::from((Ipv6Addr::UNSPECIFIED, DEFAULT_PORT)),
                SocketAddr::from((Ipv4Addr::UNSPECIFIED, DEFAULT_PORT)),
            ]
        } else {
            self.bind.clone()
        }
    }

    /// Builds the server configuration this invocation asks for.
    #[must_use]
    pub fn server_config(&self) -> ServerConfig {
        let mut config = ServerConfig::default();
        if let Some(key) = self.hmac.as_ref() {
            config = config.with_hmac_key(key.as_bytes());
        }
        if let Some(value) = self.max_sessions {
            config = config.with_max_sessions(value);
        }
        if let Some(value) = self.max_packet_length {
            config = config.with_max_packet_length(value);
        }
        if let Some(value) = self.min_interval {
            config = config.with_min_send_interval(value);
        }
        if let Some(value) = self.burst {
            config = config.with_burst_allowance(value);
        }
        if let Some(value) = self.idle_timeout {
            config = config.with_idle_timeout(value);
        }
        if let Some(value) = self.max_duration {
            config = config.with_max_test_duration(value);
        }
        if let Some(value) = self.timestamp_allowance {
            config = config.with_timestamp_allowance(value.into());
        }
        // A negative capability flag, so absence is the ordinary enabled state
        // and the library default is what an unset flag leaves in place.
        if self.no_dscp {
            config = config.with_dscp_allowed(false);
        }
        config
    }
}

/// Rejects an empty HMAC key so a blank shell expansion cannot silently
/// enable authentication with predictable, empty key material.
fn parse_hmac_key(input: &str) -> Result<String, String> {
    if input.is_empty() {
        return Err("HMAC key must not be empty".to_owned());
    }
    Ok(input.to_owned())
}

/// Parses a duration whose zero is meaningful server policy.
fn parse_duration_allow_zero(input: &str) -> Result<Duration, String> {
    if input == "0" {
        return Ok(Duration::ZERO);
    }
    parse_duration_units(input)
}

/// Parses a duration that must be positive, where absence means "no limit".
fn parse_positive_duration(input: &str) -> Result<Duration, String> {
    let value = if input == "0" {
        Duration::ZERO
    } else {
        parse_duration_units(input)?
    };
    if value.is_zero() {
        return Err(
            "duration must be greater than zero; omit the option for no maximum".to_owned(),
        );
    }
    Ok(value)
}

fn parse_duration_units(input: &str) -> Result<Duration, String> {
    let split = input
        .find(|ch: char| !ch.is_ascii_digit())
        .ok_or_else(|| "duration must include a unit: ms, s, or m".to_owned())?;
    let (number, unit) = input.split_at(split);
    if number.is_empty() {
        return Err("duration must include a value and a unit: ms, s, or m".to_owned());
    }
    let value: u64 = number
        .parse()
        .map_err(|_| format!("invalid duration value {input:?}"))?;
    match unit {
        "ms" => Ok(Duration::from_millis(value)),
        "s" => Ok(Duration::from_secs(value)),
        "m" => value
            .checked_mul(60)
            .map(Duration::from_secs)
            .ok_or_else(|| "duration is too large".to_owned()),
        _ => Err(format!(
            "unsupported duration unit {unit:?}; use ms, s, or m"
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use super::*;

    fn parse(args: &[&str]) -> Result<ServerArgs, clap::Error> {
        let mut argv = vec!["irtt-server"];
        argv.extend_from_slice(args);
        ServerArgs::try_parse_from(argv)
    }

    fn bound(args: &[&str]) -> ServerArgs {
        let mut argv = vec!["--bind", "127.0.0.1:2112"];
        argv.extend_from_slice(args);
        parse(&argv).unwrap()
    }

    #[test]
    fn bind_is_optional_and_parses_both_families() {
        let ipv4 = parse(&["--bind", "127.0.0.1:2112"]).unwrap();
        assert_eq!(ipv4.bind, [SocketAddr::from((Ipv4Addr::LOCALHOST, 2112))]);

        let ipv6 = parse(&["--bind", "[::1]:2112"]).unwrap();
        assert_eq!(ipv6.bind, [SocketAddr::from((Ipv6Addr::LOCALHOST, 2112))]);

        assert!(parse(&["--bind", "127.0.0.1"]).is_err());
        assert!(parse(&["--bind", "localhost:2112"]).is_err());
        // Still one address per option: comma-separated syntax is not a thing
        // here, and accepting it silently would be worse than rejecting it.
        assert!(parse(&["--bind", "127.0.0.1:2112,[::1]:2112"]).is_err());
    }

    #[test]
    fn no_explicit_bind_resolves_to_the_wildcard_default_pair() {
        let args = parse(&[]).unwrap();
        assert!(
            args.bind.is_empty(),
            "the empty list is the parsed representation of \"no explicit bind\""
        );
        assert_eq!(
            args.resolve_binds(),
            [
                SocketAddr::from((Ipv6Addr::UNSPECIFIED, 2112)),
                SocketAddr::from((Ipv4Addr::UNSPECIFIED, 2112)),
            ],
            "zero explicit binds means the ordinary port on both wildcard families, IPv6 first"
        );
    }

    #[test]
    fn an_explicit_bind_replaces_the_defaults_rather_than_augmenting_them() {
        let args = parse(&["--bind", "127.0.0.1:2112"]).unwrap();
        assert_eq!(
            args.resolve_binds(),
            [SocketAddr::from((Ipv4Addr::LOCALHOST, 2112))],
            "one explicit bind means exactly that one listener, not that listener plus defaults"
        );
    }

    #[test]
    fn repeated_explicit_binds_are_unaffected_by_the_default_resolution() {
        let args = parse(&["--bind", "127.0.0.1:2112", "--bind", "[::1]:2112"]).unwrap();
        assert_eq!(args.resolve_binds(), args.bind);
    }

    #[test]
    fn bind_is_repeatable_and_keeps_the_requested_order() {
        let args = parse(&[
            "--bind",
            "[::1]:2112",
            "--bind",
            "127.0.0.1:2112",
            "--bind",
            "127.0.0.1:2113",
        ])
        .unwrap();

        assert_eq!(
            args.bind,
            [
                SocketAddr::from((Ipv6Addr::LOCALHOST, 2112)),
                SocketAddr::from((Ipv4Addr::LOCALHOST, 2112)),
                SocketAddr::from((Ipv4Addr::LOCALHOST, 2113)),
            ],
            "listeners are served in the order they were requested"
        );
    }

    #[test]
    fn the_configuration_is_one_policy_however_many_listeners_there_are() {
        // Addresses never enter the server configuration: every listener is
        // built from a clone of this one value.
        let one = parse(&["--bind", "127.0.0.1:2112", "--max-sessions", "4"])
            .unwrap()
            .server_config();
        let several = parse(&[
            "--bind",
            "127.0.0.1:2112",
            "--bind",
            "[::1]:2112",
            "--max-sessions",
            "4",
        ])
        .unwrap()
        .server_config();

        assert_eq!(one, several);
        assert_eq!(several.max_sessions(), 4, "and it is a per-listener bound");
    }

    #[test]
    fn omitted_policy_options_keep_the_library_defaults() {
        assert_eq!(bound(&[]).server_config(), ServerConfig::default());
    }

    #[test]
    fn policy_options_map_onto_the_server_config() {
        let config = bound(&[
            "--hmac",
            "secret",
            "--max-sessions",
            "512",
            "--max-packet-length",
            "1472",
            "--min-interval",
            "20ms",
            "--burst",
            "3",
            "--idle-timeout",
            "30s",
            "--max-duration",
            "2m",
        ])
        .server_config();

        assert_eq!(config.hmac_key(), Some(b"secret".as_slice()));
        assert_eq!(config.max_sessions(), 512);
        assert_eq!(config.max_packet_length(), 1472);
        assert_eq!(config.min_send_interval(), Duration::from_millis(20));
        assert_eq!(config.burst_allowance(), 3);
        assert_eq!(config.idle_timeout(), Duration::from_secs(30));
        assert_eq!(config.max_test_duration(), Some(Duration::from_secs(120)));
    }

    #[test]
    fn the_capability_restrictions_map_onto_the_server_config() {
        let config = bound(&["--timestamp-allowance", "single", "--no-dscp"]).server_config();

        assert_eq!(
            config.timestamp_allowance(),
            TimestampAllowance::Single,
            "a request for both timestamps is negotiated to midpoint"
        );
        assert!(!config.dscp_allowed());
    }

    #[test]
    fn every_timestamp_allowance_mode_parses() {
        for (argument, expected) in [
            ("dual", TimestampAllowance::Dual),
            ("single", TimestampAllowance::Single),
            ("none", TimestampAllowance::None),
        ] {
            assert_eq!(
                bound(&["--timestamp-allowance", argument])
                    .server_config()
                    .timestamp_allowance(),
                expected,
                "--timestamp-allowance {argument}"
            );
        }

        assert!(parse(&[
            "--bind",
            "127.0.0.1:2112",
            "--timestamp-allowance",
            "midpoint"
        ])
        .is_err());
        assert!(parse(&["--bind", "127.0.0.1:2112", "--timestamp-allowance", "both"]).is_err());
    }

    #[test]
    fn the_capability_restrictions_are_off_when_their_flags_are_absent() {
        // Explicitly, and not only through the whole-config comparison above:
        // these are the two values that would silently change every existing
        // deployment's negotiation if the CLI restated them.
        let config = bound(&["--idle-timeout", "30s"]).server_config();

        assert_eq!(config.timestamp_allowance(), TimestampAllowance::Dual);
        assert!(config.dscp_allowed());
    }

    #[test]
    fn explicit_zero_policy_values_are_preserved() {
        let config = bound(&[
            "--min-interval",
            "0",
            "--idle-timeout",
            "0ms",
            "--burst",
            "0",
            "--max-sessions",
            "0",
            "--max-packet-length",
            "0",
        ])
        .server_config();

        assert_eq!(config.min_send_interval(), Duration::ZERO);
        assert_eq!(config.idle_timeout(), Duration::ZERO);
        assert_eq!(config.burst_allowance(), 0);
        assert_eq!(config.max_sessions(), 0);
        assert_eq!(config.max_packet_length(), 0);
    }

    #[test]
    fn durations_require_a_supported_unit() {
        assert!(parse(&["--bind", "127.0.0.1:2112", "--idle-timeout", "30"]).is_err());
        assert!(parse(&["--bind", "127.0.0.1:2112", "--min-interval", "20us"]).is_err());
        assert!(parse(&["--bind", "127.0.0.1:2112", "--max-duration", "2h"]).is_err());
        assert!(parse(&["--bind", "127.0.0.1:2112", "--idle-timeout", "ms"]).is_err());
    }

    #[test]
    fn an_empty_hmac_key_is_rejected() {
        let err = parse(&["--bind", "127.0.0.1:2112", "--hmac", ""]).unwrap_err();
        assert!(err.to_string().contains("HMAC key must not be empty"));
    }

    #[test]
    fn a_whitespace_only_hmac_key_is_accepted_literally() {
        let config = bound(&["--hmac", " "]).server_config();
        assert_eq!(config.hmac_key(), Some(b" ".as_slice()));
    }

    #[test]
    fn a_zero_maximum_duration_is_not_a_spelling_of_no_maximum() {
        assert!(parse(&["--bind", "127.0.0.1:2112", "--max-duration", "0"]).is_err());
        assert!(parse(&["--bind", "127.0.0.1:2112", "--max-duration", "0s"]).is_err());
        assert_eq!(bound(&[]).server_config().max_test_duration(), None);
    }
}
