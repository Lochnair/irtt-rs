use std::time::Duration;

use clap::ValueEnum;
use irtt_client::{MAX_DSCP_CODEPOINT, MAX_SERVER_FILL_BYTES, MAX_TTL, MAX_UDP_PAYLOAD_LENGTH};
use irtt_proto::{Clock, ReceivedStats, StampAt};

#[derive(Debug, Clone, clap::Args)]
pub struct CommonClientArgs {
    /// Probe interval.
    #[arg(long, default_value = "1s", value_parser = parse_duration)]
    pub interval: Duration,

    /// UDP payload length.
    #[arg(long, default_value_t = 0, value_parser = parse_length)]
    pub length: u32,

    /// HMAC key.
    #[arg(long)]
    pub hmac: Option<String>,

    /// Clock mode to request.
    #[arg(long, value_enum, default_value_t = ClockArg::Both)]
    pub clock: ClockArg,

    /// Timestamp mode to request.
    #[arg(
        long = "tstamp",
        visible_alias = "timestamps",
        value_enum,
        value_name = "MODE",
        default_value_t = TimestampArg::Both
    )]
    pub tstamp: TimestampArg,

    /// Received-stats mode to request.
    #[arg(
        long = "stats",
        value_enum,
        default_value_t = ReceivedStatsArg::Both,
        help = "Server received-stats negotiation mode"
    )]
    pub stats: ReceivedStatsArg,

    /// Server payload fill string to request, up to 32 bytes.
    #[arg(
        long = "sfill",
        visible_alias = "server-fill",
        value_name = "STRING",
        value_parser = parse_server_fill
    )]
    pub server_fill: Option<String>,

    /// DSCP codepoint to request; this is not a raw TOS or Traffic Class byte.
    #[arg(long, default_value_t = 0, value_name = "0..=63", value_parser = parse_dscp)]
    pub dscp: u8,

    /// Local outgoing IPv4 TTL or IPv6 unicast hop limit; not negotiated.
    #[arg(long, value_name = "1..=255", value_parser = parse_ttl)]
    pub ttl: Option<u32>,

    /// Accept safe server restrictions during negotiation.
    #[arg(long)]
    pub loose: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ClockArg {
    Wall,
    Monotonic,
    Both,
}

impl From<ClockArg> for Clock {
    fn from(value: ClockArg) -> Self {
        match value {
            ClockArg::Wall => Self::Wall,
            ClockArg::Monotonic => Self::Monotonic,
            ClockArg::Both => Self::Both,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum TimestampArg {
    None,
    Send,
    Receive,
    Both,
    Midpoint,
}

impl From<TimestampArg> for StampAt {
    fn from(value: TimestampArg) -> Self {
        match value {
            TimestampArg::None => Self::None,
            TimestampArg::Send => Self::Send,
            TimestampArg::Receive => Self::Receive,
            TimestampArg::Both => Self::Both,
            TimestampArg::Midpoint => Self::Midpoint,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ReceivedStatsArg {
    None,
    Count,
    Window,
    Both,
}

impl From<ReceivedStatsArg> for ReceivedStats {
    fn from(value: ReceivedStatsArg) -> Self {
        match value {
            ReceivedStatsArg::None => Self::None,
            ReceivedStatsArg::Count => Self::Count,
            ReceivedStatsArg::Window => Self::Window,
            ReceivedStatsArg::Both => Self::Both,
        }
    }
}

pub fn parse_duration(input: &str) -> Result<Duration, String> {
    let (number, unit) = split_duration(input)?;
    let value: u64 = number
        .parse()
        .map_err(|_| format!("invalid duration value {input:?}"))?;
    if value == 0 {
        return Err("duration must be greater than zero".to_owned());
    }
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

pub fn parse_test_duration(input: &str) -> Result<Duration, String> {
    if input == "0" {
        return Ok(Duration::ZERO);
    }
    let (number, unit) = split_duration(input)?;
    let value: u64 = number
        .parse()
        .map_err(|_| format!("invalid duration value {input:?}"))?;
    if value == 0 {
        return Ok(Duration::ZERO);
    }
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

fn split_duration(input: &str) -> Result<(&str, &str), String> {
    let split = input
        .find(|ch: char| !ch.is_ascii_digit())
        .ok_or_else(|| "duration must include a unit: ms, s, or m".to_owned())?;
    let (number, unit) = input.split_at(split);
    if number.is_empty() || unit.is_empty() {
        return Err("duration must include a positive value and unit".to_owned());
    }
    if number.starts_with('-') {
        return Err("duration must be greater than zero".to_owned());
    }
    Ok((number, unit))
}

pub fn parse_length(input: &str) -> Result<u32, String> {
    let length: u32 = input
        .parse()
        .map_err(|_| format!("invalid packet length {input:?}"))?;
    if length > MAX_UDP_PAYLOAD_LENGTH {
        return Err(format!("packet length must be <= {MAX_UDP_PAYLOAD_LENGTH}"));
    }
    Ok(length)
}

pub fn parse_server_fill(input: &str) -> Result<String, String> {
    if input.is_empty() {
        return Err("server fill must not be empty".to_owned());
    }
    let len = input.len();
    if len > MAX_SERVER_FILL_BYTES {
        return Err(format!(
            "server fill must be <= {MAX_SERVER_FILL_BYTES} bytes, got {len}"
        ));
    }
    Ok(input.to_owned())
}

pub fn parse_dscp(input: &str) -> Result<u8, String> {
    let value: u8 = input
        .parse()
        .map_err(|_| format!("invalid DSCP codepoint {input:?}"))?;
    if value > MAX_DSCP_CODEPOINT {
        return Err(format!("DSCP codepoint must be <= {MAX_DSCP_CODEPOINT}"));
    }
    Ok(value)
}

pub fn parse_ttl(input: &str) -> Result<u32, String> {
    let value: u32 = input
        .parse()
        .map_err(|_| format!("invalid TTL/hop limit {input:?}"))?;
    if value == 0 || value > MAX_TTL {
        return Err(format!("TTL/hop limit must be in range 1..={MAX_TTL}"));
    }
    Ok(value)
}
