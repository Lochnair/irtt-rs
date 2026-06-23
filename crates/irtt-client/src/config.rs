use std::{net::SocketAddr, time::Duration};

/// Protocol compatibility bound for a requested `server_fill` value, in UTF-8
/// bytes.
///
/// This is the maximum encoded server-fill string accepted by this client and
/// protocol decoder. [`ClientConfig::server_fill`] enforces the same bound
/// before opening a session.
pub use irtt_proto::MAX_SERVER_FILL_BYTES;
use irtt_proto::{Clock, ReceivedStats, StampAt};

pub(crate) const DEFAULT_PORT: u16 = 2112;
/// Largest valid DSCP codepoint accepted by client configuration.
///
/// DSCP is a six-bit value in the range `0..=63`. The client maps it into the
/// socket traffic-class field when configuring packet priority.
pub const MAX_DSCP_CODEPOINT: u8 = 63;
/// Largest IPv4 TTL or IPv6 hop-limit value accepted by client configuration.
///
/// This is the public user-configuration bound for
/// [`SocketConfig::ttl`]. A value of zero is rejected separately because socket
/// TTL and hop-limit settings are configured as `1..=255`.
pub const MAX_TTL: u32 = 255;
/// Largest UDP payload length accepted by client configuration, in bytes.
///
/// This is the maximum UDP payload size excluding IP and UDP headers. It caps
/// [`ClientConfig::length`] before protocol packets are encoded or sent.
pub const MAX_UDP_PAYLOAD_LENGTH: u32 = 65_507;
pub(crate) const DEFAULT_DURATION: Duration = Duration::from_secs(3);
pub(crate) const DEFAULT_INTERVAL: Duration = Duration::from_secs(1);
pub(crate) const DEFAULT_OPEN_TIMEOUTS: [Duration; 4] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(8),
];
pub(crate) const MIN_OPEN_TIMEOUT: Duration = Duration::from_millis(200);
pub(crate) const DEFAULT_PROBE_TIMEOUT: Duration = Duration::from_secs(4);
pub(crate) const DEFAULT_MAX_PENDING: usize = 4096;

/// Configuration for opening and running an IRTT client session.
///
/// This type describes both the protocol parameters sent in the IRTT open
/// request and the local client behavior used to drive the UDP socket. Values
/// that are negotiated by the server are available after opening the session
/// through [`NegotiatedParams`](crate::NegotiatedParams).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfig {
    /// Remote server name or address.
    ///
    /// If no port is present, the default IRTT port 2112 is used. IPv6
    /// literals may be supplied either bracketed or unbracketed when the
    /// default port should be used.
    pub server_addr: String,
    /// Requested run duration.
    ///
    /// `Some(duration)` requests a finite test and must be greater than zero.
    /// `None` requests continuous mode and is encoded on the wire as a zero
    /// duration. Use [`RunMode::NoTest`] when the caller wants negotiation only
    /// without sending probes.
    pub duration: Option<Duration>,
    /// Requested spacing between probe sends.
    ///
    /// The interval is encoded as nanoseconds in the open request and must be
    /// greater than zero. The server may return a different interval depending
    /// on the negotiated policy and server restrictions.
    pub interval: Duration,
    /// Requested echo packet payload length, in bytes.
    ///
    /// The value must fit within the UDP payload limit after protocol overhead.
    /// A server may reduce the requested length during negotiation.
    pub length: u32,
    /// Requested server receive-statistics fields in echo replies.
    pub received_stats: ReceivedStats,
    /// Requested timestamp placement in echo replies.
    pub stamp_at: StampAt,
    /// Requested server clock sources for timestamp fields.
    pub clock: Clock,
    /// Requested DSCP codepoint.
    ///
    /// This is the six-bit DSCP value, not the full traffic-class byte, and
    /// must be less than or equal to [`MAX_DSCP_CODEPOINT`].
    pub dscp: u8,
    /// Optional HMAC key used to authenticate IRTT packets.
    ///
    /// When present, open, echo, and close packets are encoded and decoded with
    /// HMAC authentication. The peer must be configured with the same key.
    pub hmac_key: Option<Vec<u8>>,
    /// Optional server payload fill request.
    ///
    /// `None` leaves server fill behavior unspecified. `Some(value)` requests
    /// a non-empty server fill mode/value and must not exceed
    /// [`MAX_SERVER_FILL_BYTES`] bytes when UTF-8 encoded.
    pub server_fill: Option<String>,
    /// Per-attempt receive timeouts used while opening the session.
    ///
    /// The client sends an open request for each entry until a valid open reply
    /// is received. The list must not be empty, and each timeout must be at
    /// least 200 ms.
    pub open_timeouts: Vec<Duration>,
    /// Whether opening the session should start a probe test or perform a
    /// negotiation-only no-test exchange.
    pub run_mode: RunMode,
    /// Policy for server changes to negotiable protocol parameters.
    pub negotiation_policy: NegotiationPolicy,
    /// Local UDP socket configuration.
    pub socket_config: SocketConfig,
    /// Time after sending a probe before the client reports it as lost.
    ///
    /// This timeout is local client behavior; it is not negotiated with the
    /// server. It must be greater than zero.
    pub probe_timeout: Duration,
    /// Maximum number of probes tracked as pending/timed-out/completed.
    ///
    /// This bounds memory used for reply classification and must be greater
    /// than zero. A very small value can reject sends when replies are still
    /// outstanding.
    pub max_pending_probes: usize,
}

/// Authentication settings used by client sessions.
///
/// `ClientConfig` keeps its existing top-level HMAC field for source
/// compatibility. Managed multi-target callers can use this smaller type for
/// per-target auth overrides without carrying a full independent
/// [`ClientConfig`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ClientAuthConfig {
    /// Optional HMAC key used to authenticate IRTT packets.
    pub hmac_key: Option<Vec<u8>>,
}

impl From<Option<Vec<u8>>> for ClientAuthConfig {
    fn from(hmac_key: Option<Vec<u8>>) -> Self {
        Self { hmac_key }
    }
}

impl From<ClientAuthConfig> for Option<Vec<u8>> {
    fn from(auth: ClientAuthConfig) -> Self {
        auth.hmac_key
    }
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
            probe_timeout: DEFAULT_PROBE_TIMEOUT,
            max_pending_probes: DEFAULT_MAX_PENDING,
        }
    }
}

/// Local UDP socket options used by [`ClientConfig`].
///
/// These settings affect how the client binds, resolves, and receives from the
/// socket. They do not change the IRTT protocol parameters negotiated with the
/// server.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SocketConfig {
    /// Local address to bind before connecting the UDP socket.
    ///
    /// `None` binds to an unspecified address with an ephemeral port matching
    /// the selected remote address family.
    pub bind_addr: Option<SocketAddr>,
    /// Optional IPv4 TTL or IPv6 hop limit applied to sent packets.
    ///
    /// `None` leaves the platform default unchanged. Values must fit in the
    /// platform socket option range; [`MAX_TTL`] is the public configuration
    /// bound used by this crate.
    pub ttl: Option<u32>,
    /// Restrict name resolution to IPv4 addresses.
    pub ipv4_only: bool,
    /// Restrict name resolution to IPv6 addresses and set IPV6_V6ONLY for IPv6
    /// sockets.
    pub ipv6_only: bool,
    /// Socket read timeout used after the session is open.
    ///
    /// `None` leaves reads blocking for APIs that perform a single receive.
    /// Managed sessions may replace `None` or long timeouts with a short
    /// timeout so cooperative cancellation can be observed promptly.
    pub recv_timeout: Option<Duration>,
}

/// How strictly to handle server-side negotiation restrictions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NegotiationPolicy {
    /// Reject any negotiated parameter that is more restrictive or different
    /// than requested.
    Strict,
    /// Accept documented server restrictions and report them in
    /// [`NegotiatedParams`](crate::NegotiatedParams).
    Loose,
}

/// Mode requested during the IRTT open exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunMode {
    /// Open a session and send echo probes for the negotiated run duration.
    Normal,
    /// Complete negotiation without running the echo probe test.
    NoTest,
}

/// Bound for a single receive-drain operation.
///
/// This is used by lower-level callers that drive [`Client`](crate::Client)
/// directly and want to cap how many datagrams are processed before returning
/// to their own event loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecvBudget {
    /// Maximum number of datagrams to process before returning.
    pub max_packets: usize,
}

impl Default for RecvBudget {
    fn default() -> Self {
        Self { max_packets: 64 }
    }
}
