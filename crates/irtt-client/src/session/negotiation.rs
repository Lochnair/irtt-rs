use std::{fmt, time::Duration};

use irtt_proto::{Clock, Params, ReceivedStats, StampAt, PROTOCOL_VERSION};

use crate::{config::NegotiationPolicy, error::ClientError};

/// Validated session semantics accepted after a successful Open exchange.
///
/// These values drive ordinary client behavior. The exact peer-returned wire
/// representation is available separately in [`NegotiationResult::peer_params`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptedSessionParameters {
    /// `None` means continuous mode; finite durations are positive.
    pub duration: Option<Duration>,
    /// Positive interval between probes when driven by a scheduler.
    pub interval: Duration,
    /// Accepted packet length in the usable unsigned domain.
    pub length: u32,
    pub received_stats: ReceivedStats,
    pub stamp_at: StampAt,
    pub clock: Clock,
    /// DSCP codepoint (`0..=63`), as in [`crate::SessionRequest::dscp`].
    pub dscp: u8,
    /// Accepted server payload fill, preserving the peer's string value.
    pub server_fill: Option<String>,
}

/// Accepted session semantics, exact protocol evidence, and accepted changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegotiationResult {
    pub accepted: AcceptedSessionParameters,
    /// Exact decoded parameters returned by the peer, without normalization.
    ///
    /// Echo encoding/decoding uses these accepted wire values. `dscp` here is
    /// the raw IP TOS / Traffic Class byte, unlike [`AcceptedSessionParameters::dscp`].
    pub peer_params: Params,
    /// Differences accepted under [`crate::NegotiationPolicy::Loose`].
    pub changes: Vec<NegotiationChange>,
}

/// A semantic parameter change accepted during negotiation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NegotiationChange {
    /// A finite duration was reduced, or continuous mode was limited.
    DurationReduced {
        /// `None` means the request was continuous.
        requested: Option<Duration>,
        negotiated: Duration,
    },
    IntervalIncreased {
        requested: Duration,
        negotiated: Duration,
    },
    /// Loose policy accepts any positive reduced interval.
    IntervalReduced {
        requested: Duration,
        negotiated: Duration,
    },
    LengthReduced {
        requested: u32,
        negotiated: u32,
    },
    ReceivedStatsChanged {
        requested: ReceivedStats,
        negotiated: ReceivedStats,
    },
    StampAtChanged {
        requested: StampAt,
        negotiated: StampAt,
    },
    ClockChanged {
        requested: Clock,
        negotiated: Clock,
    },
    /// Both values are DSCP codepoints, rather than raw Traffic Class bytes.
    DscpChanged {
        requested: u8,
        negotiated: u8,
    },
    ServerFillChanged {
        requested: Option<String>,
        negotiated: Option<String>,
    },
}

impl NegotiationChange {
    /// Return a human-readable description of this accepted change.
    pub fn message(&self) -> String {
        self.to_string()
    }
}

impl fmt::Display for NegotiationChange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DurationReduced {
                requested: None,
                negotiated,
            } => {
                write!(
                    f,
                    "server limited continuous duration to {} ns",
                    negotiated.as_nanos()
                )
            }
            Self::DurationReduced {
                requested: Some(requested),
                negotiated,
            } => {
                write!(
                    f,
                    "server reduced duration from {} ns to {} ns",
                    requested.as_nanos(),
                    negotiated.as_nanos()
                )
            }
            Self::IntervalIncreased {
                requested,
                negotiated,
            } => {
                write!(
                    f,
                    "server increased interval from {} ns to {} ns",
                    requested.as_nanos(),
                    negotiated.as_nanos()
                )
            }
            Self::IntervalReduced {
                requested,
                negotiated,
            } => {
                write!(
                    f,
                    "server reduced interval from {} ns to {} ns",
                    requested.as_nanos(),
                    negotiated.as_nanos()
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
            Self::ServerFillChanged { .. } => write!(f, "server changed payload fill behavior"),
        }
    }
}

// Called only after validating the request and returned numeric domains.
fn positive_duration(nanos: i64) -> Duration {
    Duration::from_nanos(u64::try_from(nanos).expect("validated non-negative nanoseconds"))
}

fn finite_duration(nanos: i64) -> Option<Duration> {
    (nanos != 0).then(|| positive_duration(nanos))
}

pub(crate) fn negotiate_params(
    requested: &Params,
    returned: Params,
    policy: NegotiationPolicy,
) -> Result<NegotiationResult, ClientError> {
    if returned.protocol_version != PROTOCOL_VERSION {
        return Err(ClientError::ProtocolVersionMismatch {
            requested: PROTOCOL_VERSION,
            received: returned.protocol_version,
        });
    }
    let mut changes = Vec::new();

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
        record_change(
            policy,
            &mut changes,
            NegotiationChange::DurationReduced {
                requested: finite_duration(requested.duration_ns),
                negotiated: positive_duration(returned.duration_ns),
            },
        )?;
    }
    if returned.interval_ns > requested.interval_ns {
        record_change(
            policy,
            &mut changes,
            NegotiationChange::IntervalIncreased {
                requested: positive_duration(requested.interval_ns),
                negotiated: positive_duration(returned.interval_ns),
            },
        )?;
    }
    if returned.interval_ns < requested.interval_ns {
        record_change(
            policy,
            &mut changes,
            NegotiationChange::IntervalReduced {
                requested: positive_duration(requested.interval_ns),
                negotiated: positive_duration(returned.interval_ns),
            },
        )?;
    }
    if returned.length < requested.length {
        record_change(
            policy,
            &mut changes,
            NegotiationChange::LengthReduced {
                requested: u32::try_from(requested.length).expect("validated requested length"),
                negotiated: u32::try_from(returned.length).expect("validated returned length"),
            },
        )?;
    }
    if returned.received_stats != requested.received_stats {
        record_change(
            policy,
            &mut changes,
            NegotiationChange::ReceivedStatsChanged {
                requested: requested.received_stats,
                negotiated: returned.received_stats,
            },
        )?;
    }
    if returned.stamp_at != requested.stamp_at {
        record_change(
            policy,
            &mut changes,
            NegotiationChange::StampAtChanged {
                requested: requested.stamp_at,
                negotiated: returned.stamp_at,
            },
        )?;
    }
    if returned.clock != requested.clock {
        record_change(
            policy,
            &mut changes,
            NegotiationChange::ClockChanged {
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
        record_change(
            policy,
            &mut changes,
            NegotiationChange::DscpChanged {
                // `requested`/`negotiated` here are raw wire TOS/Traffic Class
                // bytes; shift back to the codepoint the user actually
                // configured for a semantic change record.
                requested: u8::try_from(requested.dscp >> 2).expect("validated requested DSCP"),
                negotiated: u8::try_from(returned.dscp >> 2).expect("validated returned DSCP"),
            },
        )?;
    }
    if returned.server_fill != requested.server_fill {
        record_change(
            policy,
            &mut changes,
            NegotiationChange::ServerFillChanged {
                requested: requested
                    .server_fill
                    .as_ref()
                    .map(|fill| fill.value.clone()),
                negotiated: returned.server_fill.as_ref().map(|fill| fill.value.clone()),
            },
        )?;
    }

    // Compare the exact wire values above before deriving accepted semantics.
    // In particular, do not normalize fill or traffic class before Strict policy.
    let accepted = AcceptedSessionParameters {
        duration: finite_duration(returned.duration_ns),
        interval: positive_duration(returned.interval_ns),
        length: u32::try_from(returned.length).expect("validated returned length"),
        received_stats: returned.received_stats,
        stamp_at: returned.stamp_at,
        clock: returned.clock,
        dscp: u8::try_from(returned.dscp >> 2).expect("validated returned DSCP"),
        server_fill: returned.server_fill.as_ref().map(|fill| fill.value.clone()),
    };
    Ok(NegotiationResult {
        accepted,
        peer_params: returned,
        changes,
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

fn record_change(
    policy: NegotiationPolicy,
    changes: &mut Vec<NegotiationChange>,
    change: NegotiationChange,
) -> Result<(), ClientError> {
    if policy == NegotiationPolicy::Strict {
        return Err(ClientError::NegotiationRejected {
            reason: change.message(),
        });
    }

    changes.push(change);
    Ok(())
}
