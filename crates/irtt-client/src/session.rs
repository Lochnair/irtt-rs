use std::time::Instant;

use irtt_proto::{Params, PROTOCOL_VERSION};

use crate::{
    config::NegotiationPolicy,
    error::ClientError,
    probe::{CompletedSet, PendingMap},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegotiatedParams {
    pub params: Params,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClientPhase {
    Connected,
    Open { token: u64 },
    NoTestCompleted,
    Closed,
}

#[derive(Debug)]
pub(crate) struct ActiveSession {
    pub next_wire_seq: u32,
    pub next_logical_seq: u64,
    pub highest_received_seq: Option<u32>,
    pub packets_sent: u64,
    pub start_mono: Instant,
    pub end_mono: Option<Instant>,
    pub next_send_at: Instant,
    pub pending: PendingMap,
    pub completed: CompletedSet,
    pub sending_done: bool,
}

pub(crate) fn validate_negotiated_params(
    requested: &Params,
    returned: &Params,
    policy: NegotiationPolicy,
) -> Result<(), ClientError> {
    if returned.protocol_version != PROTOCOL_VERSION {
        return Err(ClientError::ProtocolVersionMismatch {
            requested: PROTOCOL_VERSION,
            received: returned.protocol_version,
        });
    }
    if returned.duration_ns > requested.duration_ns {
        return Err(ClientError::NegotiationRejected {
            reason: "duration increased".to_owned(),
        });
    }
    if returned.length > requested.length {
        return Err(ClientError::NegotiationRejected {
            reason: "length increased".to_owned(),
        });
    }
    if returned.interval_ns <= 0 {
        return Err(ClientError::NegotiationRejected {
            reason: "interval must be positive".to_owned(),
        });
    }

    if policy == NegotiationPolicy::Strict && returned != requested {
        return Err(ClientError::NegotiationRejected {
            reason: "returned params differ from requested params".to_owned(),
        });
    }
    Ok(())
}
