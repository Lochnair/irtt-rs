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
        logical_seq: u64,
        remote: SocketAddr,
        scheduled_at: Instant,
        sent_at: ClientTimestamp,
        bytes: usize,
        send_call: Duration,
        timer_error: Duration,
    },

    EchoReply {
        seq: u32,
        logical_seq: u64,
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
        logical_seq: u64,
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
        logical_seq: Option<u64>,
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
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarningKind {
    MalformedOrUnrelatedPacket,
    WrongToken,
    UntrackedReply,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RttSample {
    pub raw: Duration,
    pub adjusted: Option<Duration>,
    pub effective: Duration,
    pub adjusted_signed: Option<SignedDuration>,
    pub effective_signed: SignedDuration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignedDuration {
    pub ns: i128,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PacketMeta {
    pub traffic_class: Option<u8>,
    pub dscp: Option<u8>,
    pub ecn: Option<u8>,
    pub kernel_rx_timestamp: Option<std::time::SystemTime>,
}
