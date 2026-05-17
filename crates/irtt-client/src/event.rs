use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use crate::{session::NegotiatedParams, timing::ClientTimestamp};

/// Result of the IRTT open exchange.
///
/// The lower-level [`Client`](crate::Client) API returns this before any echo
/// probes are driven. Managed sessions publish the contained lifecycle event to
/// subscribers immediately after a successful open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenOutcome {
    /// A normal probe session was opened.
    Started {
        /// Resolved remote socket address used by the UDP socket.
        remote: SocketAddr,
        /// Session token assigned by the server and used on echo/close packets.
        token: u64,
        /// Parameters returned by the server after negotiation.
        negotiated: NegotiatedParams,
        /// Lifecycle event corresponding to this outcome.
        event: ClientEvent,
    },
    /// A no-test open exchange completed without starting a probe session.
    NoTestCompleted {
        /// Resolved remote socket address used by the UDP socket.
        remote: SocketAddr,
        /// Parameters returned by the server after negotiation.
        negotiated: NegotiatedParams,
        /// Lifecycle event corresponding to this outcome.
        event: ClientEvent,
    },
}

/// Event emitted by an IRTT client session.
///
/// Events form the public lifecycle and measurement stream for callers. UIs
/// usually display lifecycle, warning, and per-packet events directly; summary
/// reporting should aggregate `EchoSent`, `EchoReply`, `EchoLoss`,
/// `LateReply`, and `DuplicateReply` events or pass the stream to
/// `irtt-stats`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientEvent {
    /// A normal session has opened and is ready to send echo probes.
    SessionStarted {
        /// Resolved remote socket address.
        remote: SocketAddr,
        /// Session token assigned by the server.
        token: u64,
        /// Negotiated protocol parameters for this session.
        negotiated: NegotiatedParams,
        /// Client timestamp when the open reply was accepted.
        at: ClientTimestamp,
    },
    /// A negotiation-only no-test exchange completed.
    ///
    /// No `EchoSent`, `EchoReply`, `EchoLoss`, or `SessionClosed` events are
    /// expected for this open outcome.
    NoTestCompleted {
        /// Resolved remote socket address.
        remote: SocketAddr,
        /// Negotiated protocol parameters returned by the server.
        negotiated: NegotiatedParams,
        /// Client timestamp when the open reply was accepted.
        at: ClientTimestamp,
    },
    /// The client sent a close request and considers the session closed.
    ///
    /// This is a local lifecycle event, not an acknowledgement from the server.
    /// Managed sessions disconnect event subscribers after the worker exits,
    /// while leaving already queued events available to drain.
    SessionClosed {
        /// Resolved remote socket address.
        remote: SocketAddr,
        /// Session token that was closed.
        token: u64,
        /// Client timestamp when the close request was sent.
        at: ClientTimestamp,
    },

    /// An echo request was sent.
    ///
    /// This records local scheduling and send-call timing. It does not imply a
    /// reply will arrive; callers should pair it with `EchoReply`, `EchoLoss`,
    /// `LateReply`, or `DuplicateReply` by sequence number when displaying
    /// packet lifecycle state.
    EchoSent {
        /// Wire sequence number sent in the echo request.
        seq: u32,
        /// Resolved remote socket address.
        remote: SocketAddr,
        /// Monotonic deadline at which the probe was scheduled to be sent.
        scheduled_at: Instant,
        /// Client wall/monotonic timestamp captured for the send.
        sent_at: ClientTimestamp,
        /// Number of bytes passed to the UDP socket.
        bytes: usize,
        /// Elapsed time spent in the socket send call.
        send_call: Duration,
        /// Absolute difference between the scheduled send time and actual send
        /// timestamp.
        timer_error: Duration,
    },

    /// First in-window reply for a pending echo request.
    ///
    /// This is the primary successful measurement event. RTT and optional
    /// server timing/statistics fields should generally be aggregated from this
    /// variant.
    EchoReply {
        /// Wire sequence number from the echo reply.
        seq: u32,
        /// Resolved remote socket address.
        remote: SocketAddr,
        /// Client timestamp recorded when the matching request was sent.
        sent_at: ClientTimestamp,
        /// Client timestamp recorded when the reply was received.
        received_at: ClientTimestamp,
        /// Round-trip timing sample for this reply.
        rtt: RttSample,
        /// Optional server-reported timestamps and processing time.
        server_timing: Option<ServerTiming>,
        /// Optional one-way delay values derived from client/server wall clocks.
        one_way: Option<OneWayDelaySample>,
        /// Optional server-reported received-statistics sample.
        received_stats: Option<ReceivedStatsSample>,
        /// UDP datagram size received from the socket.
        bytes: usize,
        /// Receive-side metadata observed outside the IRTT wire payload.
        packet_meta: PacketMeta,
    },

    /// A sent echo request exceeded the local probe timeout before a reply was
    /// matched.
    ///
    /// A later datagram for the same sequence may still arrive and will be
    /// reported as [`ClientEvent::LateReply`] rather than changing this event.
    EchoLoss {
        /// Wire sequence number that timed out.
        seq: u32,
        /// Client timestamp recorded when the request was sent.
        sent_at: ClientTimestamp,
        /// Monotonic deadline at which the probe was declared lost.
        timeout_at: std::time::Instant,
    },

    /// Additional reply for a sequence that was already completed.
    ///
    /// Duplicates are not primary RTT samples. They are useful for diagnostics
    /// and packet counters, but should not be aggregated as successful first
    /// replies.
    DuplicateReply {
        /// Wire sequence number from the duplicate reply.
        seq: u32,
        /// Resolved remote socket address.
        remote: SocketAddr,
        /// Client timestamp recorded when the duplicate was received.
        received_at: ClientTimestamp,
        /// UDP datagram size received from the socket.
        bytes: usize,
    },

    /// Reply for a sequence that was no longer pending.
    ///
    /// This commonly means the probe had already been reported as lost, or the
    /// reply arrived out of the client's current tracking window. When the
    /// original send metadata is still retained, RTT and one-way timing are
    /// included; otherwise those fields are `None`.
    LateReply {
        /// Wire sequence number from the late reply.
        seq: u32,
        /// Highest sequence number accepted as an in-window first reply so far.
        highest_seen: u32,
        /// Resolved remote socket address.
        remote: SocketAddr,
        /// Original client send timestamp, when still retained.
        sent_at: Option<ClientTimestamp>,
        /// Client timestamp recorded when the late reply was received.
        received_at: ClientTimestamp,
        /// RTT sample computed when the original send timestamp is available.
        rtt: Option<RttSample>,
        /// Optional server-reported timestamps and processing time.
        server_timing: Option<ServerTiming>,
        /// Optional one-way delay values derived from client/server wall clocks.
        one_way: Option<OneWayDelaySample>,
        /// Optional server-reported received-statistics sample.
        received_stats: Option<ReceivedStatsSample>,
        /// UDP datagram size received from the socket.
        bytes: usize,
        /// Receive-side metadata observed outside the IRTT wire payload.
        packet_meta: PacketMeta,
    },

    /// Non-fatal condition observed while receiving or classifying packets.
    ///
    /// Warning events are diagnostic. They do not close the session by
    /// themselves and are not measurement samples.
    Warning {
        /// Machine-readable warning category.
        kind: WarningKind,
        /// Human-readable diagnostic text.
        message: String,
        /// Client timestamp when the warning was emitted.
        at: ClientTimestamp,
    },
}

/// Category for a non-fatal [`ClientEvent::Warning`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WarningKind {
    /// A received datagram could not be decoded as a relevant IRTT packet.
    MalformedOrUnrelatedPacket,
    /// A decoded packet used a token that does not match the open session.
    WrongToken,
    /// A decoded reply could not be matched to tracked probe state.
    UntrackedReply,
}

/// Round-trip timing for one echo reply.
///
/// `raw` is the client-observed elapsed time from sending the echo request to
/// receiving the reply and is therefore non-negative. When the server reports
/// enough monotonic timing to compute processing time, `adjusted` subtracts
/// that server processing from `raw`. `effective` is the value used by reporting
/// and statistics: adjusted when available, otherwise raw converted to a signed
/// value.
///
/// Adjusted and effective RTT values are signed. They may be negative if the
/// reported server processing time is greater than the raw client-observed RTT.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RttSample {
    /// Client-observed RTT from send to receive.
    pub raw: Duration,
    /// Signed RTT adjusted for server processing when server processing is available.
    pub adjusted: Option<SignedDuration>,
    /// Signed effective RTT used by stats and CLI reporting.
    ///
    /// This is `adjusted` when server processing is available, otherwise `raw`
    /// converted to a signed duration.
    pub effective: SignedDuration,
}

/// Signed nanosecond-backed duration used for derived timing values.
///
/// This type preserves negative values that can arise after subtracting server
/// processing time or comparing wall-clock timestamps from different hosts. It
/// should not be used to represent raw elapsed wall-clock or monotonic time,
/// which is non-negative and represented by [`Duration`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SignedDuration {
    ns: i128,
}

impl SignedDuration {
    /// Construct a signed duration from nanoseconds.
    pub const fn from_nanos(ns: i128) -> Self {
        Self { ns }
    }

    /// Convert a non-negative [`Duration`] into a signed duration.
    ///
    /// Durations larger than `i128::MAX` nanoseconds saturate to `i128::MAX`.
    pub fn from_duration(duration: Duration) -> Self {
        Self {
            ns: i128::try_from(duration.as_nanos()).unwrap_or(i128::MAX),
        }
    }

    /// Return the duration in nanoseconds.
    pub const fn as_nanos(self) -> i128 {
        self.ns
    }

    /// Return the duration in whole microseconds, truncated toward zero.
    pub const fn as_micros(self) -> i128 {
        self.ns / 1_000
    }

    /// Return the duration in whole milliseconds, truncated toward zero.
    pub const fn as_millis(self) -> i128 {
        self.ns / 1_000_000
    }

    /// Return whether this duration is less than zero.
    pub const fn is_negative(self) -> bool {
        self.ns < 0
    }
}

impl From<Duration> for SignedDuration {
    fn from(value: Duration) -> Self {
        Self::from_duration(value)
    }
}

/// Server-reported timing fields decoded from an echo reply.
///
/// These values are optional because the negotiated timestamp mode, clock mode,
/// server implementation, or packet contents may omit individual fields.
/// Timestamp values are nanoseconds in the server's reported wall-clock or
/// monotonic clock domain. `None` means unavailable; `Some(0)` means the server
/// reported a zero timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerTiming {
    /// Server wall-clock receive timestamp, in nanoseconds.
    pub receive_wall_ns: Option<i64>,
    /// Server monotonic receive timestamp, in nanoseconds.
    pub receive_mono_ns: Option<i64>,
    /// Server wall-clock send timestamp, in nanoseconds.
    pub send_wall_ns: Option<i64>,
    /// Server monotonic send timestamp, in nanoseconds.
    pub send_mono_ns: Option<i64>,
    /// Server wall-clock midpoint timestamp, in nanoseconds.
    pub midpoint_wall_ns: Option<i64>,
    /// Server monotonic midpoint timestamp, in nanoseconds.
    pub midpoint_mono_ns: Option<i64>,
    /// Server processing time computed from monotonic receive/send timestamps.
    ///
    /// `None` means the required monotonic timestamps were unavailable or
    /// unusable. `Some(Duration::ZERO)` means the computed processing interval
    /// was available and zero.
    pub processing: Option<Duration>,
}

/// Directional one-way delay samples computed from wall-clock timestamps.
///
/// Each directional field is an `Option<SignedDuration>`. `None` means the
/// required client or server wall-clock timestamp was unavailable, or the
/// timestamp arithmetic was out of range. `Some(value)` means the delay was
/// computed from available timestamps.
///
/// Negative computed values are preserved as `Some` instead of being clamped to
/// zero or dropped. A negative one-way delay usually indicates wall-clock skew
/// between the client and server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OneWayDelaySample {
    /// Client-to-server delay.
    ///
    /// This is computed from the client send wall time and the server receive
    /// wall time when both are available.
    pub client_to_server: Option<SignedDuration>,
    /// Server-to-client delay.
    ///
    /// This is computed from the server send wall time and the client receive
    /// wall time when both are available.
    pub server_to_client: Option<SignedDuration>,
}

/// Server-reported receive counters decoded from an echo reply.
///
/// These fields are optional because the negotiated received-statistics mode or
/// server implementation may omit them. `None` means unavailable; `Some(0)`
/// means the server reported the value and it was zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceivedStatsSample {
    /// Server-reported count of received packets, when available.
    pub count: Option<u32>,
    /// Server-reported receive window value, when available.
    pub window: Option<u64>,
}

/// Receive-side packet metadata observed outside the IRTT wire protocol.
///
/// `None` means the metadata was unavailable. `Some(0)` means the metadata was
/// observed and its value was zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PacketMeta {
    pub traffic_class: Option<u8>,
    pub dscp: Option<u8>,
    pub ecn: Option<u8>,
    pub kernel_rx_timestamp: Option<std::time::SystemTime>,
}
