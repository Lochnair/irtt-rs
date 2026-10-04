use std::time::{Duration, Instant};

use crate::{error::ClientError, session::NegotiatedParams};

#[derive(Debug)]
pub(crate) struct ProbeSchedule {
    end_at: Option<Instant>,
    interval: Duration,
    next_send_at: Option<Instant>,
}

#[derive(Debug)]
pub(crate) struct ScheduleCommit {
    pub(crate) scheduled_at: Instant,
    pub(crate) next_send_at: Option<Instant>,
}

impl ProbeSchedule {
    pub(crate) fn new(
        start_at: Instant,
        negotiated: &NegotiatedParams,
    ) -> Result<Self, ClientError> {
        let interval_ns = u64::try_from(negotiated.params.interval_ns)
            .expect("validated positive negotiated interval");
        let interval = Duration::from_nanos(interval_ns);
        let end_at = if negotiated.params.duration_ns > 0 {
            let duration_ns = u64::try_from(negotiated.params.duration_ns)
                .expect("validated positive negotiated duration");
            Some(
                start_at
                    .checked_add(Duration::from_nanos(duration_ns))
                    .ok_or_else(|| ClientError::NegotiationRejected {
                        reason: "duration is too large to schedule".to_owned(),
                    })?,
            )
        } else {
            None
        };

        Ok(Self {
            end_at,
            interval,
            next_send_at: Some(start_at),
        })
    }

    pub(crate) fn next_send_deadline(&self) -> Option<Instant> {
        self.next_send_at
    }

    /// Wake for finite expiry independently of send readiness until finished.
    pub(crate) fn end_deadline(&self) -> Option<Instant> {
        self.end_at.filter(|_| !self.is_finished())
    }

    pub(crate) fn interval(&self) -> Duration {
        self.interval
    }

    pub(crate) fn permit_probe_at(&mut self, now: Instant) -> bool {
        if self.next_send_at.is_none() {
            return false;
        }
        if self.end_at.is_some_and(|end| now >= end) {
            self.next_send_at = None;
            return false;
        }
        true
    }

    pub(crate) fn preflight_managed_commit(
        &self,
        scheduled_at: Instant,
        permission_at: Instant,
    ) -> Result<ScheduleCommit, ClientError> {
        let (scheduled_at, next_send_at) =
            advance_cadence(scheduled_at, self.interval, permission_at)?;
        Ok(ScheduleCommit {
            scheduled_at,
            next_send_at: if self.end_at.is_some_and(|end| next_send_at >= end) {
                None
            } else {
                Some(next_send_at)
            },
        })
    }

    pub(crate) fn commit(&mut self, commit: ScheduleCommit) {
        self.next_send_at = commit.next_send_at;
    }

    /// Reclaim slots crossed while a successful socket submission was in flight.
    /// This follows the send commit; it never advances a failed submission.
    pub(crate) fn skip_missed_slots_at(&mut self, now: Instant) -> Result<(), ClientError> {
        if let Some(deadline) = self.next_send_at.filter(|deadline| *deadline <= now) {
            let commit = self.preflight_managed_commit(deadline, now)?;
            self.commit(commit);
        }
        Ok(())
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.next_send_at.is_none()
    }
}

pub(crate) fn advance_cadence(
    deadline: Instant,
    interval: Duration,
    now: Instant,
) -> Result<(Instant, Instant), ClientError> {
    if interval.is_zero() {
        return Err(ClientError::InvalidConfig {
            reason: "probe interval must be greater than zero".to_owned(),
        });
    }

    let elapsed_slots = now
        .checked_duration_since(deadline)
        .map_or(0, |elapsed| elapsed.as_nanos() / interval.as_nanos());
    let scheduled_offset = duration_from_nanos(
        interval
            .as_nanos()
            .checked_mul(elapsed_slots)
            .ok_or(ClientError::DurationOverflow)?,
    )?;
    let next_offset = duration_from_nanos(
        interval
            .as_nanos()
            .checked_mul(
                elapsed_slots
                    .checked_add(1)
                    .ok_or(ClientError::DurationOverflow)?,
            )
            .ok_or(ClientError::DurationOverflow)?,
    )?;
    let scheduled_at = deadline
        .checked_add(scheduled_offset)
        .ok_or(ClientError::DurationOverflow)?;
    let next_at = deadline
        .checked_add(next_offset)
        .ok_or(ClientError::DurationOverflow)?;
    Ok((scheduled_at, next_at))
}

fn duration_from_nanos(nanos: u128) -> Result<Duration, ClientError> {
    const NANOS_PER_SECOND: u128 = 1_000_000_000;
    let seconds =
        u64::try_from(nanos / NANOS_PER_SECOND).map_err(|_| ClientError::DurationOverflow)?;
    let subsec_nanos = u32::try_from(nanos % NANOS_PER_SECOND)
        .expect("nanosecond remainder is always less than one second");
    Ok(Duration::new(seconds, subsec_nanos))
}

pub(crate) fn instant_abs_diff(left: Instant, right: Instant) -> Duration {
    left.checked_duration_since(right)
        .or_else(|| right.checked_duration_since(left))
        .unwrap_or(Duration::ZERO)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifetime_deadline_exists_only_for_an_unfinished_finite_schedule() {
        let start = Instant::now();
        let end = start + Duration::from_millis(500);
        let mut negotiated = NegotiatedParams {
            params: irtt_proto::Params {
                duration_ns: 500_000_000,
                interval_ns: 1_000_000,
                ..irtt_proto::Params::default()
            },
            restrictions: Vec::new(),
        };
        let mut finite = ProbeSchedule::new(start, &negotiated).unwrap();
        assert_eq!(finite.end_deadline(), Some(end));
        assert!(finite.permit_probe_at(end - Duration::from_nanos(1)));
        assert_eq!(finite.end_deadline(), Some(end));
        assert!(!finite.permit_probe_at(end));
        assert!(finite.is_finished());
        assert_eq!(finite.end_deadline(), None);

        let mut finite = ProbeSchedule::new(start, &negotiated).unwrap();
        let commit = finite
            .preflight_managed_commit(start, end - Duration::from_nanos(1))
            .unwrap();
        finite.commit(commit);
        assert!(finite.is_finished());
        assert_eq!(finite.end_deadline(), None);

        negotiated.params.duration_ns = 0;
        let mut continuous = ProbeSchedule::new(start, &negotiated).unwrap();
        assert!(continuous.permit_probe_at(end));
        assert_eq!(continuous.end_deadline(), None);
    }
}
