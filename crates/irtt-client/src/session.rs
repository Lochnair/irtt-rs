use std::time::Instant;

use irtt_proto::{Params, PROTOCOL_VERSION};

use crate::{
    config::{NegotiationPolicy, MAX_DSCP_CODEPOINT},
    error::ClientError,
    probe::{CompletedSet, PendingMap, TimedOutMap},
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
    pub timed_out: TimedOutMap,
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
    validate_duration_restriction(requested.duration_ns, returned.duration_ns)?;
    if returned.length < 0 {
        return Err(ClientError::NegotiationRejected {
            reason: "length must be non-negative".to_owned(),
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
    validate_dscp_restriction(returned.dscp)?;

    if policy == NegotiationPolicy::Strict && returned != requested {
        return Err(ClientError::NegotiationRejected {
            reason: "returned params differ from requested params".to_owned(),
        });
    }
    Ok(())
}

fn validate_duration_restriction(requested: i64, returned: i64) -> Result<(), ClientError> {
    if returned < 0 {
        return Err(ClientError::NegotiationRejected {
            reason: "duration must be non-negative".to_owned(),
        });
    }

    if requested > 0 && returned == 0 {
        return Err(ClientError::NegotiationRejected {
            reason: "server returned continuous duration for finite request".to_owned(),
        });
    }

    if requested > 0 && returned > requested {
        return Err(ClientError::NegotiationRejected {
            reason: "duration increased".to_owned(),
        });
    }

    Ok(())
}

fn validate_dscp_restriction(returned: i64) -> Result<(), ClientError> {
    if !(0..=i64::from(MAX_DSCP_CODEPOINT)).contains(&returned) {
        return Err(ClientError::NegotiationRejected {
            reason: format!("dscp must be in range 0..={MAX_DSCP_CODEPOINT}"),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use irtt_proto::{Clock, ReceivedStats, ServerFill, StampAt};

    fn default_params() -> Params {
        Params {
            protocol_version: PROTOCOL_VERSION,
            duration_ns: 3_000_000_000,
            interval_ns: 1_000_000_000,
            length: 256,
            received_stats: ReceivedStats::Both,
            stamp_at: StampAt::Both,
            clock: Clock::Both,
            dscp: 46,
            server_fill: Some(ServerFill {
                value: "rand".to_owned(),
            }),
        }
    }

    fn assert_rejected(requested: &Params, returned: &Params, policy: NegotiationPolicy) {
        assert!(matches!(
            validate_negotiated_params(requested, returned, policy),
            Err(ClientError::NegotiationRejected { .. })
        ));
    }

    fn rejection_reason(
        requested: &Params,
        returned: &Params,
        policy: NegotiationPolicy,
    ) -> String {
        match validate_negotiated_params(requested, returned, policy) {
            Err(ClientError::NegotiationRejected { reason }) => reason,
            other => panic!("expected negotiation rejection, got {other:?}"),
        }
    }

    #[test]
    fn strict_rejects_changed_negotiated_fields() {
        let requested = default_params();

        let mut returned = requested.clone();
        returned.length = 128;
        assert_rejected(&requested, &returned, NegotiationPolicy::Strict);

        let mut returned = requested.clone();
        returned.dscp = 8;
        assert_rejected(&requested, &returned, NegotiationPolicy::Strict);

        let mut returned = requested.clone();
        returned.received_stats = ReceivedStats::Count;
        assert_rejected(&requested, &returned, NegotiationPolicy::Strict);

        let mut returned = requested.clone();
        returned.stamp_at = StampAt::Midpoint;
        assert_rejected(&requested, &returned, NegotiationPolicy::Strict);

        let mut returned = requested.clone();
        returned.clock = Clock::Wall;
        assert_rejected(&requested, &returned, NegotiationPolicy::Strict);

        let mut returned = requested.clone();
        returned.server_fill = None;
        assert_rejected(&requested, &returned, NegotiationPolicy::Strict);
    }

    #[test]
    fn loose_duration_negotiation_uses_run_duration_semantics() {
        let requested = default_params();

        let mut returned = requested.clone();
        returned.duration_ns = requested.duration_ns / 2;
        assert!(
            validate_negotiated_params(&requested, &returned, NegotiationPolicy::Loose).is_ok()
        );

        let mut returned = requested.clone();
        returned.duration_ns = requested.duration_ns + 1;
        assert_eq!(
            rejection_reason(&requested, &returned, NegotiationPolicy::Loose),
            "duration increased"
        );

        let mut returned = requested.clone();
        returned.duration_ns = 0;
        assert_eq!(
            rejection_reason(&requested, &returned, NegotiationPolicy::Loose),
            "server returned continuous duration for finite request"
        );

        let mut continuous_requested = requested.clone();
        continuous_requested.duration_ns = 0;
        let mut finite_returned = continuous_requested.clone();
        finite_returned.duration_ns = 1_000_000_000;
        assert!(validate_negotiated_params(
            &continuous_requested,
            &finite_returned,
            NegotiationPolicy::Loose
        )
        .is_ok());

        assert_rejected(
            &continuous_requested,
            &finite_returned,
            NegotiationPolicy::Strict,
        );

        assert!(validate_negotiated_params(
            &continuous_requested,
            &continuous_requested,
            NegotiationPolicy::Strict
        )
        .is_ok());
    }
}
