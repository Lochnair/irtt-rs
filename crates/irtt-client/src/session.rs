use std::{fmt, time::Instant};

use irtt_proto::{Clock, Params, ReceivedStats, StampAt, PROTOCOL_VERSION};

use crate::{
    config::{NegotiationPolicy, MAX_DSCP_CODEPOINT},
    error::ClientError,
    probe::{CompletedSet, PendingMap, TimedOutMap},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegotiatedParams {
    pub params: Params,
    pub restrictions: Vec<NegotiationRestriction>,
}

/// A server-side restriction applied during session parameter negotiation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NegotiationRestriction {
    /// Run duration was reduced.
    ///
    /// A requested duration of `0` means the client requested continuous mode.
    /// When `requested_ns == 0` and `negotiated_ns > 0`, the server limited
    /// that continuous request to a finite duration.
    DurationReduced {
        requested_ns: i64,
        negotiated_ns: i64,
    },
    /// Probe interval was increased.
    IntervalIncreased {
        requested_ns: i64,
        negotiated_ns: i64,
    },
    /// Probe interval was reduced.
    IntervalReduced {
        requested_ns: i64,
        negotiated_ns: i64,
    },
    /// Packet length was reduced.
    LengthReduced { requested: i64, negotiated: i64 },
    /// Returned received-statistics mode differs from the request.
    ReceivedStatsChanged {
        requested: ReceivedStats,
        negotiated: ReceivedStats,
    },
    /// Returned timestamp placement differs from the request.
    StampAtChanged {
        requested: StampAt,
        negotiated: StampAt,
    },
    /// Returned clock source differs from the request.
    ClockChanged { requested: Clock, negotiated: Clock },
    /// Returned DSCP codepoint differs from the request.
    DscpChanged { requested: i64, negotiated: i64 },
    /// Returned server payload fill behavior differs from the request.
    ServerFillChanged,
}

impl NegotiationRestriction {
    pub fn message(&self) -> String {
        self.to_string()
    }
}

impl fmt::Display for NegotiationRestriction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DurationReduced {
                requested_ns: 0,
                negotiated_ns,
            } => {
                write!(
                    f,
                    "server limited continuous duration to {negotiated_ns} ns"
                )
            }
            Self::DurationReduced {
                requested_ns,
                negotiated_ns,
            } => {
                write!(
                    f,
                    "server reduced duration from {requested_ns} ns to {negotiated_ns} ns"
                )
            }
            Self::IntervalIncreased {
                requested_ns,
                negotiated_ns,
            } => {
                write!(
                    f,
                    "server increased interval from {requested_ns} ns to {negotiated_ns} ns"
                )
            }
            Self::IntervalReduced {
                requested_ns,
                negotiated_ns,
            } => {
                write!(
                    f,
                    "server reduced interval from {requested_ns} ns to {negotiated_ns} ns"
                )
            }
            Self::LengthReduced {
                requested,
                negotiated,
            } => {
                write!(
                    f,
                    "server reduced packet length from {requested} bytes to {negotiated} bytes"
                )
            }
            Self::ReceivedStatsChanged {
                requested,
                negotiated,
            } => {
                write!(
                    f,
                    "server changed received-stats from {requested:?} to {negotiated:?}"
                )
            }
            Self::StampAtChanged {
                requested,
                negotiated,
            } => {
                write!(
                    f,
                    "server changed stamp-at from {requested:?} to {negotiated:?}"
                )
            }
            Self::ClockChanged {
                requested,
                negotiated,
            } => {
                write!(
                    f,
                    "server changed clock from {requested:?} to {negotiated:?}"
                )
            }
            Self::DscpChanged {
                requested,
                negotiated,
            } => {
                write!(f, "server changed DSCP from {requested} to {negotiated}")
            }
            Self::ServerFillChanged => write!(f, "server changed payload fill behavior"),
        }
    }
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

pub(crate) fn negotiate_params(
    requested: &Params,
    returned: Params,
    policy: NegotiationPolicy,
) -> Result<NegotiatedParams, ClientError> {
    if returned.protocol_version != PROTOCOL_VERSION {
        return Err(ClientError::ProtocolVersionMismatch {
            requested: PROTOCOL_VERSION,
            received: returned.protocol_version,
        });
    }
    let mut restrictions = Vec::new();

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

    if returned.duration_ns < requested.duration_ns
        || (requested.duration_ns == 0 && returned.duration_ns > 0)
    {
        record_restriction(
            policy,
            &mut restrictions,
            NegotiationRestriction::DurationReduced {
                requested_ns: requested.duration_ns,
                negotiated_ns: returned.duration_ns,
            },
        )?;
    }
    if returned.interval_ns > requested.interval_ns {
        record_restriction(
            policy,
            &mut restrictions,
            NegotiationRestriction::IntervalIncreased {
                requested_ns: requested.interval_ns,
                negotiated_ns: returned.interval_ns,
            },
        )?;
    }
    if returned.interval_ns < requested.interval_ns {
        record_restriction(
            policy,
            &mut restrictions,
            NegotiationRestriction::IntervalReduced {
                requested_ns: requested.interval_ns,
                negotiated_ns: returned.interval_ns,
            },
        )?;
    }
    if returned.length < requested.length {
        record_restriction(
            policy,
            &mut restrictions,
            NegotiationRestriction::LengthReduced {
                requested: requested.length,
                negotiated: returned.length,
            },
        )?;
    }
    if returned.received_stats != requested.received_stats {
        record_restriction(
            policy,
            &mut restrictions,
            NegotiationRestriction::ReceivedStatsChanged {
                requested: requested.received_stats,
                negotiated: returned.received_stats,
            },
        )?;
    }
    if returned.stamp_at != requested.stamp_at {
        record_restriction(
            policy,
            &mut restrictions,
            NegotiationRestriction::StampAtChanged {
                requested: requested.stamp_at,
                negotiated: returned.stamp_at,
            },
        )?;
    }
    if returned.clock != requested.clock {
        record_restriction(
            policy,
            &mut restrictions,
            NegotiationRestriction::ClockChanged {
                requested: requested.clock,
                negotiated: returned.clock,
            },
        )?;
    }
    if returned.dscp != requested.dscp && returned.dscp != 0 {
        return Err(ClientError::NegotiationRejected {
            reason: "server returned unsupported DSCP change".to_owned(),
        });
    }
    if returned.dscp == 0 && requested.dscp != 0 {
        record_restriction(
            policy,
            &mut restrictions,
            NegotiationRestriction::DscpChanged {
                requested: requested.dscp,
                negotiated: returned.dscp,
            },
        )?;
    }
    if returned.server_fill != requested.server_fill {
        record_restriction(
            policy,
            &mut restrictions,
            NegotiationRestriction::ServerFillChanged,
        )?;
    }

    Ok(NegotiatedParams {
        params: returned,
        restrictions,
    })
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

fn record_restriction(
    policy: NegotiationPolicy,
    restrictions: &mut Vec<NegotiationRestriction>,
    restriction: NegotiationRestriction,
) -> Result<(), ClientError> {
    if policy == NegotiationPolicy::Strict {
        return Err(ClientError::NegotiationRejected {
            reason: restriction.message(),
        });
    }

    restrictions.push(restriction);
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
            negotiate_params(requested, returned.clone(), policy),
            Err(ClientError::NegotiationRejected { .. })
        ));
    }

    fn assert_negotiates(
        requested: &Params,
        returned: &Params,
        policy: NegotiationPolicy,
    ) -> NegotiatedParams {
        negotiate_params(requested, returned.clone(), policy)
            .unwrap_or_else(|err| panic!("expected negotiation success, got {err:?}"))
    }

    fn rejection_reason(
        requested: &Params,
        returned: &Params,
        policy: NegotiationPolicy,
    ) -> String {
        match negotiate_params(requested, returned.clone(), policy) {
            Err(ClientError::NegotiationRejected { reason }) => reason,
            other => panic!("expected negotiation rejection, got {other:?}"),
        }
    }

    #[test]
    fn strict_rejects_changed_negotiated_fields() {
        let requested = default_params();

        let mut returned = requested.clone();
        returned.length = 128;
        assert_eq!(
            rejection_reason(&requested, &returned, NegotiationPolicy::Strict),
            NegotiationRestriction::LengthReduced {
                requested: requested.length,
                negotiated: returned.length,
            }
            .message()
        );

        let mut returned = requested.clone();
        returned.dscp = 8;
        assert_eq!(
            rejection_reason(&requested, &returned, NegotiationPolicy::Strict),
            NegotiationRestriction::DscpChanged {
                requested: requested.dscp,
                negotiated: returned.dscp,
            }
            .message()
        );

        let mut returned = requested.clone();
        returned.received_stats = ReceivedStats::Count;
        assert_eq!(
            rejection_reason(&requested, &returned, NegotiationPolicy::Strict),
            NegotiationRestriction::ReceivedStatsChanged {
                requested: requested.received_stats,
                negotiated: returned.received_stats,
            }
            .message()
        );

        let mut returned = requested.clone();
        returned.stamp_at = StampAt::Midpoint;
        assert_eq!(
            rejection_reason(&requested, &returned, NegotiationPolicy::Strict),
            NegotiationRestriction::StampAtChanged {
                requested: requested.stamp_at,
                negotiated: returned.stamp_at,
            }
            .message()
        );

        let mut returned = requested.clone();
        returned.clock = Clock::Wall;
        assert_eq!(
            rejection_reason(&requested, &returned, NegotiationPolicy::Strict),
            NegotiationRestriction::ClockChanged {
                requested: requested.clock,
                negotiated: returned.clock,
            }
            .message()
        );

        let mut returned = requested.clone();
        returned.server_fill = None;
        assert_eq!(
            rejection_reason(&requested, &returned, NegotiationPolicy::Strict),
            NegotiationRestriction::ServerFillChanged.message()
        );
    }

    #[test]
    fn loose_duration_negotiation_uses_run_duration_semantics() {
        let requested = default_params();

        let mut returned = requested.clone();
        returned.duration_ns = requested.duration_ns / 2;
        let negotiated = assert_negotiates(&requested, &returned, NegotiationPolicy::Loose);
        assert_eq!(
            negotiated.restrictions,
            vec![NegotiationRestriction::DurationReduced {
                requested_ns: requested.duration_ns,
                negotiated_ns: returned.duration_ns,
            }]
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
        let negotiated = assert_negotiates(
            &continuous_requested,
            &finite_returned,
            NegotiationPolicy::Loose,
        );
        assert_eq!(
            negotiated.restrictions,
            vec![NegotiationRestriction::DurationReduced {
                requested_ns: 0,
                negotiated_ns: finite_returned.duration_ns,
            }]
        );

        assert_rejected(
            &continuous_requested,
            &finite_returned,
            NegotiationPolicy::Strict,
        );

        assert!(negotiate_params(
            &continuous_requested,
            continuous_requested.clone(),
            NegotiationPolicy::Strict
        )
        .is_ok());
    }
}
