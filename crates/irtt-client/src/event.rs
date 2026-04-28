use std::net::SocketAddr;

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
    Warning {
        message: String,
    },
}
