use std::{fmt, net::IpAddr, net::SocketAddr, time::Duration};

use clap::ValueEnum;
use irtt_client::{MAX_DSCP_CODEPOINT, MAX_SERVER_FILL_BYTES, MAX_TTL, MAX_UDP_PAYLOAD_LENGTH};
use irtt_proto::{Clock, ReceivedStats, StampAt};

#[derive(Clone, clap::Args)]
pub struct CommonClientArgs {
    /// Use IPv4 only.
    #[arg(short = '4', long, conflicts_with_all = ["ipv6", "dual_stack"])]
    pub ipv4: bool,

    /// Use IPv6 only.
    #[arg(short = '6', long, conflicts_with_all = ["ipv4", "dual_stack"])]
    pub ipv6: bool,

    /// Probe available IPv4 and IPv6 paths separately for each hostname.
    #[arg(long, conflicts_with_all = ["ipv4", "ipv6"], long_help = "Probe available IPv4 and IPv6 paths separately for each hostname. Labels gain /v4 or /v6; IP literals remain single targets. Families are discovered when a target is added; reconnects resolve the original hostname again within the chosen family.")]
    pub dual_stack: bool,

    /// Positive probe interval (e.g. 2.5s, 1m30s, 500us, 100ns, 1h).
    #[arg(short = 'i', long, default_value = "1s", value_parser = parse_duration)]
    pub interval: Duration,

    /// Comma-separated session-opening timeouts, one per attempt (each >= 200ms).
    #[arg(long, value_name = "DURATIONS", value_delimiter = ',', value_parser = parse_duration, long_help = "Comma-separated session-opening receive timeouts, one per attempt (each at least 200ms), e.g. 1s,2s,4s,8s. Omit to use the library default retry schedule.")]
    pub timeouts: Vec<Duration>,

    /// UDP payload length.
    #[arg(short = 'l', long, default_value_t = 0, value_parser = parse_length)]
    pub length: u32,

    // Decode during configuration preparation so Clap cannot echo rejected keys.
    /// HMAC key: literal UTF-8 or lowercase 0x followed by hexadecimal bytes.
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

    #[arg(
        long = "bind-addr",
        value_name = "IP[:PORT]",
        value_parser = parse_bind_addr,
        long_help = "Local IP address and optional UDP port to bind. \
                     If no port is specified, the operating system selects \
                     an ephemeral port. IPv6 addresses with an explicit \
                     port must use [ADDR]:PORT syntax."
    )]
    pub bind_addr: Option<SocketAddr>,

    #[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
    #[arg(
        long = "bind-device",
        value_name = "DEVICE",
        help = "Bind the socket to a network device"
    )]
    pub bind_to_device: Option<String>,

    #[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
    #[arg(
        long,
        value_name = "MARK",
        value_parser = clap_num::maybe_hex::<u32>,
        help = "Set the outgoing socket firewall mark"
    )]
    pub mark: Option<u32>,

    #[cfg(target_os = "freebsd")]
    #[arg(long, value_name = "FIB", help = "Set the socket routing table (FIB)")]
    pub fib: Option<u32>,

    /// Accept safe server restrictions during negotiation.
    #[arg(long)]
    pub loose: bool,
}

impl fmt::Debug for CommonClientArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("CommonClientArgs");
        debug
            .field("ipv4", &self.ipv4)
            .field("ipv6", &self.ipv6)
            .field("dual_stack", &self.dual_stack)
            .field("interval", &self.interval)
            .field("timeouts", &self.timeouts)
            .field("length", &self.length)
            .field("hmac", &self.hmac.as_ref().map(|_| "[REDACTED]"))
            .field("clock", &self.clock)
            .field("tstamp", &self.tstamp)
            .field("stats", &self.stats)
            .field("server_fill", &self.server_fill)
            .field("dscp", &self.dscp)
            .field("ttl", &self.ttl)
            .field("bind_addr", &self.bind_addr);
        #[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
        debug
            .field("bind_to_device", &self.bind_to_device)
            .field("mark", &self.mark);
        #[cfg(target_os = "freebsd")]
        debug.field("fib", &self.fib);
        debug.field("loose", &self.loose).finish()
    }
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

fn parse_bind_addr(input: &str) -> Result<SocketAddr, String> {
    input
        .parse::<SocketAddr>()
        .or_else(|_| input.parse::<IpAddr>().map(|ip| SocketAddr::new(ip, 0)))
        .map_err(|_| "expected an IP address, optionally with a port".to_owned())
}

pub fn parse_duration(input: &str) -> Result<Duration, String> {
    let duration = parse_test_duration(input)?;
    if duration.is_zero() {
        return Err("duration must be greater than zero".to_owned());
    }
    Ok(duration)
}

pub fn parse_test_duration(input: &str) -> Result<Duration, String> {
    if input == "0" {
        return Ok(Duration::ZERO);
    }
    humantime::parse_duration(input).map_err(|error| error.to_string())
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
