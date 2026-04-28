use irtt_proto::{Params, PROTOCOL_VERSION};

use crate::{config::NegotiationPolicy, error::ClientError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegotiatedParams {
    pub params: Params,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClientState {
    Connected,
    Open { token: u64 },
    NoTestCompleted,
    Closed,
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
    // TODO/OPEN: apply a documented negotiated interval safety floor once the
    // clean spec or black-box report defines one. For now, only reject
    // impossible non-positive intervals.
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
