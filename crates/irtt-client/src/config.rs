use std::{net::SocketAddr, time::Duration};

use crate::{Authentication, ClientError};

/// Protocol compatibility bound for a requested `server_fill` value, in UTF-8
/// bytes.
///
/// This is the maximum encoded server-fill string accepted by this client and
/// protocol decoder. [`SessionRequest::server_fill`] enforces the same bound
/// before opening a session.
pub use irtt_proto::MAX_SERVER_FILL_BYTES;
use irtt_proto::{Clock, ReceivedStats, StampAt};

pub(crate) const DEFAULT_PORT: u16 = 2112;
/// Largest valid DSCP codepoint accepted by client configuration.
///
/// DSCP is a six-bit value in the range `0..=63`. The client shifts it left
/// by two bits into the raw IP TOS / Traffic Class byte carried by
/// [`irtt_proto::Params::dscp`] and applied to the socket during the active
/// test phase.
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
/// [`SessionRequest::length`] before protocol packets are encoded or sent.
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
/// through [`NegotiationResult`](crate::NegotiationResult).
///
/// # Example
///
/// Configure a finite run with larger probes and accept documented server
/// restrictions. Use `duration: None` for continuous mode, or
/// [`RunMode::NoTest`] for negotiation without probes.
///
/// ```
/// use std::time::Duration;
///
/// use irtt_client::{ClientConfig, NegotiationPolicy, OpenPolicy, SessionRequest};
///
/// let config = ClientConfig {
///     request: SessionRequest {
///         duration: Some(Duration::from_secs(10)),
///         interval: Duration::from_millis(250),
///         length: 1200,
///         ..SessionRequest::default()
///     },
///     open: OpenPolicy {
///         negotiation: NegotiationPolicy::Loose,
///         ..OpenPolicy::default()
///     },
///     ..ClientConfig::default()
/// };
///
/// assert_eq!(config.request.duration, Some(Duration::from_secs(10)));
/// assert_eq!(config.request.interval, Duration::from_millis(250));
/// assert_eq!(config.request.length, 1200);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfig {
    /// Remote address families allowed during resolution and socket creation.
    pub address_family: AddressFamily,
    /// Local UDP socket properties.
    pub socket: SocketConfig,
    /// Protocol and session values requested from the server.
    pub request: SessionRequest,
    /// Open retry timeouts and negotiation policy.
    pub open: OpenPolicy,
    /// Concrete authentication for open, echo, and close packets.
    ///
    /// HMAC peers must use the same key. An empty HMAC key remains authenticated.
    pub auth: Authentication,
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

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            address_family: AddressFamily::default(),
            socket: SocketConfig::default(),
            request: SessionRequest::default(),
            open: OpenPolicy::default(),
            auth: Authentication::Unauthenticated,
            probe_timeout: DEFAULT_PROBE_TIMEOUT,
            max_pending_probes: DEFAULT_MAX_PENDING,
        }
    }
}

/// Allowed remote address families. IPv6-only selection also sets IPV6_V6ONLY.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AddressFamily {
    /// Accept IPv4 and IPv6 addresses.
    #[default]
    Any,
    /// Accept only IPv4 addresses.
    Ipv4,
    /// Accept only IPv6 addresses and use IPv6-only sockets.
    Ipv6,
}

/// Requested IRTT protocol and session semantics, reusable across endpoints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRequest {
    /// Requested run duration.
    ///
    /// `Some(duration)` requests a finite test and must be greater than zero.
    /// `None` requests continuous mode and is encoded on the wire as a zero
    /// duration. Use [`RunMode::NoTest`] when the caller wants negotiation only
    /// without sending probes.
    ///
    /// The managed driver stops sending after the negotiated duration.
    /// Low-level adapters leave run duration to the caller.
    pub duration: Option<Duration>,
    /// Requested spacing between probe sends.
    ///
    /// The interval is encoded as nanoseconds in the open request and must be
    /// greater than zero. The server may return a different interval depending
    /// on the negotiated policy and server restrictions.
    ///
    /// The managed driver uses the negotiated interval for cadence.
    /// Low-level adapters send whenever called.
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
    ///
    /// [`Clock::Unspecified`] is the wire default meaning "no clock tag was
    /// sent" and is not a client mode; opening a session with it is rejected.
    /// Request [`Clock::Wall`], [`Clock::Monotonic`], or [`Clock::Both`].
    pub clock: Clock,
    /// Requested DSCP codepoint.
    ///
    /// This is the six-bit DSCP value, not the full traffic-class byte, and
    /// must be less than or equal to [`MAX_DSCP_CODEPOINT`].
    pub dscp: u8,
    /// Optional server payload fill request.
    ///
    /// `None` leaves server fill behavior unspecified. `Some(value)` requests
    /// a non-empty server fill mode/value and must not exceed
    /// [`MAX_SERVER_FILL_BYTES`] bytes when UTF-8 encoded.
    pub server_fill: Option<String>,
    /// Whether opening the session should start a probe test or perform a
    /// negotiation-only no-test exchange.
    pub run_mode: RunMode,
}

impl Default for SessionRequest {
    fn default() -> Self {
        Self {
            duration: Some(DEFAULT_DURATION),
            interval: DEFAULT_INTERVAL,
            length: 0,
            received_stats: ReceivedStats::Both,
            stamp_at: StampAt::Both,
            clock: Clock::Both,
            dscp: 0,
            server_fill: None,
            run_mode: RunMode::Normal,
        }
    }
}

/// Opening behavior shared by blocking, async, and managed clients.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenPolicy {
    /// Per-attempt receive timeouts used while opening the session.
    ///
    /// The client sends an open request for each entry until a valid open reply
    /// is received. The list must not be empty, and each timeout must be at
    /// least 200 ms.
    pub timeouts: Vec<Duration>,
    /// Policy for server changes to negotiable protocol parameters.
    pub negotiation: NegotiationPolicy,
}

impl OpenPolicy {
    pub(crate) fn validate(&self) -> Result<(), ClientError> {
        if self.timeouts.is_empty() {
            return Err(ClientError::NoOpenTimeouts);
        }
        for timeout in &self.timeouts {
            if *timeout < MIN_OPEN_TIMEOUT {
                return Err(ClientError::OpenTimeoutTooSmall {
                    timeout: *timeout,
                    minimum: MIN_OPEN_TIMEOUT,
                });
            }
        }
        Ok(())
    }
}

impl Default for OpenPolicy {
    fn default() -> Self {
        Self {
            timeouts: DEFAULT_OPEN_TIMEOUTS.to_vec(),
            negotiation: NegotiationPolicy::Strict,
        }
    }
}

/// Local UDP socket options used by [`ClientConfig`].
///
/// These settings affect how the client binds and sends from the socket. They
/// do not change the IRTT protocol parameters negotiated with the server.
///
/// # Example
///
/// Bind to a chosen local endpoint and set the outgoing IPv4 TTL:
///
/// ```
/// use std::net::{Ipv4Addr, SocketAddr};
///
/// use irtt_client::{AddressFamily, ClientConfig, SocketConfig};
///
/// let config = ClientConfig {
///     address_family: AddressFamily::Ipv4,
///     socket: SocketConfig {
///         bind_addr: Some(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))),
///         ttl: Some(32),
///         ..SocketConfig::default()
///     },
///     ..ClientConfig::default()
/// };
///
/// assert_eq!(config.socket.ttl, Some(32));
/// assert_eq!(config.address_family, AddressFamily::Ipv4);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SocketConfig {
    /// Local address to bind before connecting the UDP socket.
    ///
    /// `None` binds to an unspecified address with an ephemeral port matching
    /// the selected remote address family.
    pub bind_addr: Option<SocketAddr>,
    /// Network interface to bind this socket to (`SO_BINDTODEVICE`).
    #[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
    pub bind_to_device: Option<String>,
    /// Firewall mark applied to outgoing packets (`SO_MARK`).
    #[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
    pub mark: Option<u32>,
    /// FreeBSD FIB used for routing this socket (`SO_SETFIB`).
    #[cfg(target_os = "freebsd")]
    pub fib: Option<u32>,
    /// Optional IPv4 TTL or IPv6 hop limit applied to sent packets.
    ///
    /// `None` leaves the platform default unchanged. Values must fit in the
    /// platform socket option range; [`MAX_TTL`] is the public configuration
    /// bound used by this crate.
    pub ttl: Option<u32>,
}

/// How strictly to handle server-side negotiation restrictions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NegotiationPolicy {
    /// Reject any negotiated parameter that is more restrictive or different
    /// than requested.
    Strict,
    /// Accept documented server restrictions and report them in
    /// [`NegotiationResult`](crate::NegotiationResult).
    Loose,
}

/// Mode requested during the IRTT open exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunMode {
    /// Open a probe session. The managed driver runs for the negotiated duration;
    /// low-level adapters leave sending and stopping to the caller.
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
