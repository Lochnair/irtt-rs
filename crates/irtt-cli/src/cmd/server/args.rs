use std::{net::SocketAddr, time::Duration};

use clap::Parser;
use irtt_server::ServerConfig;

// Every policy option is optional. An omitted option leaves the corresponding
// `ServerConfig` default in place rather than restating it here, so library and
// CLI defaults cannot drift apart.
#[derive(Debug, Clone, Parser)]
#[command(
    name = "irtt-server",
    about = "Minimal IRTT-compatible UDP server",
    after_help = "Policy options left unset keep the irtt-server library defaults.\nOne process serves exactly one listener; run a second process for a second address."
)]
pub struct ServerArgs {
    /// Local UDP address to bind, as ADDR:PORT.
    #[arg(
        long,
        value_name = "ADDR",
        long_help = "Local UDP address to bind, as ADDR:PORT, for example 127.0.0.1:2112 or [::1]:2112. Host names are not resolved.\n\nAn explicit interface address is preferred. A wildcard bind such as 0.0.0.0:2112 is allowed, but reply source-address selection is then left to the kernel: per-packet destination-address handling for multi-homed hosts is not implemented."
    )]
    pub bind: SocketAddr,

    /// HMAC key; every request must then carry a valid MAC.
    #[arg(
        long,
        value_name = "KEY",
        long_help = "HMAC key, taken as the UTF-8 bytes of this argument.\n\nAuthentication is global: with a key configured, every request must carry a valid MAC and every reply is authenticated. Without one, authenticated requests are dropped. The key is visible in the process arguments."
    )]
    pub hmac: Option<String>,

    /// Maximum number of simultaneously live sessions.
    #[arg(
        long,
        value_name = "COUNT",
        long_help = "Maximum number of simultaneously live sessions. Once the table is full, a session-creating open is dropped silently; nothing is evicted. Zero refuses every session-creating open."
    )]
    pub max_sessions: Option<usize>,

    /// Maximum echo datagram size a session may negotiate, in bytes.
    #[arg(
        long,
        value_name = "BYTES",
        long_help = "Maximum echo datagram size a session may negotiate, in bytes. A longer requested length is reduced during negotiation, an open whose mandatory field block would not fit is refused, and inbound echo requests are admitted against the same limit. This is a resource bound, not an MTU."
    )]
    pub max_packet_length: Option<usize>,

    /// Floor on the negotiated send interval, and the allowance refill cadence.
    #[arg(
        long,
        value_name = "DURATION",
        value_parser = parse_duration_allow_zero,
        long_help = "Floor on the send interval a session may negotiate, and the cadence its reply allowance refills at. The negotiated interval is still capped at a quarter of the idle timeout afterwards. Use 0 for no time-based throttling."
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
}

impl ServerArgs {
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
        config
    }
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
    fn bind_is_required_and_parses_both_families() {
        assert!(parse(&[]).is_err());

        let ipv4 = parse(&["--bind", "127.0.0.1:2112"]).unwrap();
        assert_eq!(ipv4.bind, SocketAddr::from((Ipv4Addr::LOCALHOST, 2112)));

        let ipv6 = parse(&["--bind", "[::1]:2112"]).unwrap();
        assert_eq!(ipv6.bind, SocketAddr::from((Ipv6Addr::LOCALHOST, 2112)));

        assert!(parse(&["--bind", "127.0.0.1"]).is_err());
        assert!(parse(&["--bind", "localhost:2112"]).is_err());
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
    fn a_zero_maximum_duration_is_not_a_spelling_of_no_maximum() {
        assert!(parse(&["--bind", "127.0.0.1:2112", "--max-duration", "0"]).is_err());
        assert!(parse(&["--bind", "127.0.0.1:2112", "--max-duration", "0s"]).is_err());
        assert_eq!(bound(&[]).server_config().max_test_duration(), None);
    }
}
