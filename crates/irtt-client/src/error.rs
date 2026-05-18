use std::{io, time::Duration};

use thiserror::Error;

/// Failure reported while configuring, opening, running, receiving, closing, or
/// joining an IRTT client session.
///
/// Configuration validation errors usually require changing
/// [`ClientConfig`](crate::ClientConfig) or related options before retrying.
/// Network, protocol, and runtime errors may be transient, but can also reflect
/// peer incompatibility or an invalid session state.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ClientError {
    /// The configured server address could not be resolved to a socket address.
    ///
    /// Check the host, port, address-family restrictions, and name service
    /// configuration before retrying.
    #[error("failed to resolve server address {addr:?}")]
    Resolve { addr: String },
    /// A UDP socket operation failed while binding, connecting, sending, or
    /// receiving.
    ///
    /// This can be a transient network or OS error, but may also reflect local
    /// permissions, routing, or socket configuration.
    #[error("socket error: {0}")]
    Socket(#[from] io::Error),
    /// Applying or reading a socket option failed.
    ///
    /// This is usually caused by an unsupported option, an invalid local value,
    /// or a platform-specific socket limitation. Changing socket configuration
    /// is more likely to help than retrying unchanged.
    #[error("socket option error while trying to {operation} for {remote}: {source}")]
    SocketOption {
        operation: &'static str,
        remote: std::net::SocketAddr,
        source: io::Error,
    },
    /// Restoring the configured read timeout after open negotiation failed.
    ///
    /// The session may have opened successfully, but the socket could not be
    /// returned to the caller's requested receive-timeout behavior.
    #[error("failed to restore configured socket read timeout after open negotiation: {source}")]
    ReadTimeoutRestore { source: io::Error },
    /// A packet failed IRTT protocol parsing or authentication.
    ///
    /// Likely causes include malformed input, unsupported protocol data, or an
    /// HMAC mismatch between client and server.
    #[error("protocol error: {0}")]
    Protocol(#[from] irtt_proto::ProtoError),
    /// No valid open reply was received before all configured open attempts
    /// timed out.
    ///
    /// Retrying can succeed when the server or network is temporarily
    /// unavailable. Persistent failures usually require changing the address,
    /// authentication key, server configuration, or timeout settings.
    #[error("all open requests timed out")]
    OpenTimeout,
    /// An open-attempt timeout is below the client's supported minimum.
    ///
    /// Increase the configured timeout before retrying.
    #[error("open timeout {timeout:?} is below the minimum {minimum:?}")]
    OpenTimeoutTooSmall {
        timeout: Duration,
        minimum: Duration,
    },
    /// The configured open-attempt timeout list is empty.
    ///
    /// Provide at least one timeout before opening the session.
    #[error("open_timeouts must not be empty")]
    NoOpenTimeouts,
    /// The server explicitly rejected the open request.
    ///
    /// Retrying unchanged is unlikely to help unless the server configuration or
    /// load has changed.
    #[error("server rejected the open request")]
    ServerRejected,
    /// A no-test open request received a reply that did not have no-test close
    /// semantics.
    ///
    /// This indicates an incompatible or unexpected peer response.
    #[error("unexpected no-test open reply")]
    UnexpectedNoTestReply,
    /// A no-test open reply included a connection token.
    ///
    /// No-test negotiation must not establish a probe session, so a non-zero
    /// token indicates an incompatible or unexpected peer response.
    #[error("no-test reply included a non-zero connection token: {token}")]
    NonZeroNoTestToken { token: u64 },
    /// The peer replied with a protocol version different from the requested
    /// version.
    ///
    /// This is a protocol compatibility error and requires compatible client
    /// and server versions.
    #[error("protocol version mismatch: requested {requested}, received {received}")]
    ProtocolVersionMismatch { requested: i64, received: i64 },
    /// The server returned a zero connection token for a normal test session.
    ///
    /// A normal session requires a non-zero token to correlate echo traffic, so
    /// this indicates an invalid peer response.
    #[error("server returned a zero connection token")]
    ZeroToken,
    /// Negotiated parameters were rejected by the configured negotiation policy.
    ///
    /// Use a less restrictive configuration or
    /// [`NegotiationPolicy::Loose`](crate::NegotiationPolicy::Loose) when the
    /// caller accepts documented server restrictions.
    #[error("strict negotiation rejected changed params: {reason}")]
    NegotiationRejected { reason: String },
    /// The requested operation requires an open session, but the client is only
    /// connected.
    #[error("client is not open")]
    NotOpen,
    /// The session is already open.
    ///
    /// Continue running, receiving, or closing the existing session instead of
    /// opening it again.
    #[error("client session is already open")]
    AlreadyOpen,
    /// The no-test session has already completed.
    ///
    /// Create a new client session to perform another negotiation.
    #[error("client session is already completed")]
    AlreadyCompleted,
    /// The session is already closed.
    ///
    /// Create a new client session before opening or running another test.
    #[error("client session is already closed")]
    AlreadyClosed,
    /// A duration could not be represented as nanoseconds for scheduling or
    /// protocol encoding.
    ///
    /// Use smaller configured or negotiated durations.
    #[error("duration is too large to encode as nanoseconds")]
    DurationOverflow,
    /// A client-maintained counter would overflow.
    ///
    /// This is a runtime limit rather than a network failure. Start a new
    /// session if the test duration or send volume reaches this limit.
    #[error("counter {counter} overflowed")]
    CounterOverflow { counter: &'static str },
    /// Sending another probe would exceed the configured pending-probe limit.
    ///
    /// Increase [`ClientConfig::max_pending_probes`](crate::ClientConfig::max_pending_probes),
    /// reduce send rate, or receive replies and timeouts more often before
    /// sending again.
    #[error("pending probe limit exceeded ({limit})")]
    PendingLimitExceeded { limit: usize },
    /// Client configuration failed validation before the requested operation.
    ///
    /// The reported reason names a value that must be changed before retrying.
    #[error("invalid configuration: {reason}")]
    InvalidConfig { reason: String },
    /// A managed-session worker thread panicked before producing an outcome.
    ///
    /// This is a runtime failure from joining the managed session, not a
    /// protocol response from the server.
    #[error("managed client worker thread panicked")]
    WorkerPanicked,
}

/// Failure reported while setting up or receiving from an event subscription.
///
/// Zero-capacity subscriber queues are rejected when subscriptions are created.
/// Once a subscription exists, this error type reports delivery failures observed
/// by [`EventSubscription`](crate::EventSubscription).
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum EventSubscriptionError {
    /// The subscription can no longer receive new events.
    ///
    /// Already queued events are delivered first. After the queue is drained,
    /// `recv` and `try_recv` return this error when the hub disconnects the
    /// subscriber, the hub is shut down, or the subscription is disconnected by
    /// its overflow policy.
    #[error("event subscription is disconnected")]
    Disconnected,
}
