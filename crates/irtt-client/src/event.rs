use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use crate::{session::NegotiatedParams, timing::ClientTimestamp};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenOutcome {
    Started {
        remote: SocketAddr,
        token: u64,
        negotiated: NegotiatedParams,
        event: ClientEvent,
    },
    NoTestCompleted {
        remote: SocketAddr,
        negotiated: NegotiatedParams,
        event: ClientEvent,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientEvent {
    SessionStarted {
        remote: SocketAddr,
        token: u64,
        negotiated: NegotiatedParams,
        at: ClientTimestamp,
    },
    NoTestCompleted {
        remote: SocketAddr,
        negotiated: NegotiatedParams,
        at: ClientTimestamp,
    },
    SessionClosed {
        remote: SocketAddr,
        token: u64,
        at: ClientTimestamp,
    },

    EchoSent {
        seq: u32,
        remote: SocketAddr,
        scheduled_at: Instant,
        sent_at: ClientTimestamp,
        bytes: usize,
        send_call: Duration,
        timer_error: Duration,
    },

    EchoReply {
        seq: u32,
        remote: SocketAddr,
        sent_at: ClientTimestamp,
        received_at: ClientTimestamp,
        rtt: RttSample,
        server_timing: Option<ServerTiming>,
        one_way: Option<OneWayDelaySample>,
        received_stats: Option<ReceivedStatsSample>,
        bytes: usize,
        packet_meta: PacketMeta,
    },

    EchoLoss {
        seq: u32,
        sent_at: ClientTimestamp,
        timeout_at: std::time::Instant,
    },

    DuplicateReply {
        seq: u32,
        remote: SocketAddr,
        received_at: ClientTimestamp,
        bytes: usize,
    },

    LateReply {
        seq: u32,
        highest_seen: u32,
        remote: SocketAddr,
        sent_at: Option<ClientTimestamp>,
        received_at: ClientTimestamp,
        rtt: Option<RttSample>,
        server_timing: Option<ServerTiming>,
        one_way: Option<OneWayDelaySample>,
        received_stats: Option<ReceivedStatsSample>,
        bytes: usize,
        packet_meta: PacketMeta,
    },

    Warning {
        kind: WarningKind,
        message: String,
        at: ClientTimestamp,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WarningKind {
    MalformedOrUnrelatedPacket,
    WrongToken,
    UntrackedReply,
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignedDuration {
    pub ns: i128,
}

impl SignedDuration {
    pub fn from_duration(duration: Duration) -> Self {
        Self {
            ns: i128::try_from(duration.as_nanos()).unwrap_or(i128::MAX),
        }
    }
}

impl From<Duration> for SignedDuration {
    fn from(value: Duration) -> Self {
        Self::from_duration(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerTiming {
    pub receive_wall_ns: Option<i64>,
    pub receive_mono_ns: Option<i64>,
    pub send_wall_ns: Option<i64>,
    pub send_mono_ns: Option<i64>,
    pub midpoint_wall_ns: Option<i64>,
    pub midpoint_mono_ns: Option<i64>,
    pub processing: Option<Duration>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OneWayDelaySample {
    pub client_to_server: Option<Duration>,
    pub server_to_client: Option<Duration>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceivedStatsSample {
    pub count: Option<u32>,
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

#[cfg(test)]
mod tests {
    use super::PacketMeta;

    #[test]
    fn packet_meta_default_is_unavailable() {
        let meta = PacketMeta::default();

        assert_eq!(meta.traffic_class, None);
        assert_eq!(meta.dscp, None);
        assert_eq!(meta.ecn, None);
        assert_eq!(meta.kernel_rx_timestamp, None);
    }
}
