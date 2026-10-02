use std::fmt;

use irtt_proto::{Clock, Params, ReceivedStats, StampAt, PROTOCOL_VERSION};

use crate::{config::NegotiationPolicy, error::ClientError};

/// Protocol parameters accepted for a session.
///
/// `params` contains the server-returned values that the client will use for
/// echo packets. `restrictions` records accepted differences from the request
/// when loose negotiation is enabled.
///
/// `params.dscp` is the raw IP TOS / Traffic Class byte, not a DSCP
/// codepoint: for a configured codepoint of 46 (EF), an unrestricted
/// negotiation leaves `params.dscp == 184`. [`NegotiationRestriction::DscpChanged`]
/// reports codepoints instead, for consistency with [`crate::ClientConfig::dscp`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegotiatedParams {
    /// Server-returned protocol parameters used for the session.
    pub params: Params,
    /// Accepted server restrictions or parameter changes.
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
    ///
    /// These values are DSCP codepoints (`0..=63`) for human-facing
    /// consistency with [`crate::ClientConfig::dscp`], not the raw wire
    /// `Params::dscp` byte carried by [`NegotiatedParams::params`].
    DscpChanged { requested: i64, negotiated: i64 },
    /// Returned server payload fill behavior differs from the request.
    ServerFillChanged,
}

impl NegotiationRestriction {
    /// Return a human-readable description of this negotiated restriction.
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
                // `requested`/`negotiated` here are raw wire TOS/Traffic Class
                // bytes; shift back to the codepoint the user actually
                // configured for a human-facing restriction.
                requested: requested.dscp >> 2,
                negotiated: returned.dscp >> 2,
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

/// Validates a server-returned `Params::dscp`, which is a raw IP TOS /
/// Traffic Class byte and therefore valid across the full `0..=255` wire
/// range, not the `0..=`[`MAX_DSCP_CODEPOINT`](crate::config::MAX_DSCP_CODEPOINT)
/// range of the public codepoint.
fn validate_dscp_restriction(returned: i64) -> Result<(), ClientError> {
    if !(0..=255).contains(&returned) {
        return Err(ClientError::NegotiationRejected {
            reason: "dscp must be in range 0..=255".to_owned(),
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
