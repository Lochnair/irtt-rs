use std::{net::SocketAddr, time::Duration};

use irtt_proto::{Clock, ReceivedStats, StampAt};

pub(crate) const DEFAULT_PORT: u16 = 2112;
pub(crate) const DEFAULT_DURATION: Duration = Duration::from_secs(3);
pub(crate) const DEFAULT_INTERVAL: Duration = Duration::from_secs(1);
pub(crate) const DEFAULT_OPEN_TIMEOUTS: [Duration; 4] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(8),
];
pub(crate) const MIN_OPEN_TIMEOUT: Duration = Duration::from_millis(200);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfig {
    pub server_addr: String,
    pub duration: Option<Duration>,
    pub interval: Duration,
    pub length: u32,
    pub received_stats: ReceivedStats,
    pub stamp_at: StampAt,
    pub clock: Clock,
    pub dscp: u8,
    pub hmac_key: Option<Vec<u8>>,
    pub server_fill: Option<String>,
    pub open_timeouts: Vec<Duration>,
    pub run_mode: RunMode,
    pub negotiation_policy: NegotiationPolicy,
    pub socket_config: SocketConfig,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            server_addr: format!("127.0.0.1:{DEFAULT_PORT}"),
            duration: Some(DEFAULT_DURATION),
            interval: DEFAULT_INTERVAL,
            length: 0,
            received_stats: ReceivedStats::Both,
            stamp_at: StampAt::Both,
            clock: Clock::Both,
            dscp: 0,
            hmac_key: None,
            server_fill: None,
            open_timeouts: DEFAULT_OPEN_TIMEOUTS.to_vec(),
            run_mode: RunMode::Normal,
            negotiation_policy: NegotiationPolicy::Strict,
            socket_config: SocketConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SocketConfig {
    pub bind_addr: Option<SocketAddr>,
    pub ttl: Option<u32>,
    pub ipv4_only: bool,
    pub ipv6_only: bool,
    pub recv_timeout: Option<Duration>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NegotiationPolicy {
    Strict,
    Loose,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunMode {
    Normal,
    NoTest,
}
