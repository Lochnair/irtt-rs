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
    use irtt_proto::Params;

    fn negotiated(duration_ns: i64) -> NegotiatedParams {
        NegotiatedParams {
            params: Params {
                interval_ns: 10_000_000,
                duration_ns,
                ..Params::default()
            },
            restrictions: Vec::new(),
        }
    }

    #[test]
    fn missed_slots_skip_on_the_absolute_grid_only_after_commit() {
        let start = Instant::now();
        let mut schedule = ProbeSchedule::new(start, &negotiated(0)).unwrap();
        for (now_ms, intended_ms, next_ms) in [(0, 0, 10), (45, 40, 50), (50, 50, 60)] {
            let deadline = schedule.next_send_deadline().unwrap();
            let commit = schedule
                .preflight_managed_commit(deadline, start + Duration::from_millis(now_ms))
                .unwrap();
            assert_eq!(
                commit.scheduled_at,
                start + Duration::from_millis(intended_ms)
            );
            // A failed or pending submission discards this preflight.
            assert_eq!(schedule.next_send_deadline(), Some(deadline));
            schedule.commit(commit);
            assert_eq!(
                schedule.next_send_deadline(),
                Some(start + Duration::from_millis(next_ms))
            );
        }
        schedule
            .skip_missed_slots_at(start + Duration::from_millis(85))
            .unwrap();
        assert_eq!(
            schedule.next_send_deadline(),
            Some(start + Duration::from_millis(90))
        );
    }

    #[test]
    fn finite_duration_excludes_its_end_even_when_a_slot_is_missed() {
        let start = Instant::now();
        let mut schedule = ProbeSchedule::new(start, &negotiated(20_000_000)).unwrap();
        assert!(schedule.permit_probe_at(start + Duration::from_millis(19)));
        let commit = schedule
            .preflight_managed_commit(start, start + Duration::from_millis(19))
            .unwrap();
        schedule.commit(commit);
        assert!(schedule.is_finished());

        let mut missed = ProbeSchedule::new(start, &negotiated(20_000_000)).unwrap();
        assert!(!missed.permit_probe_at(start + Duration::from_millis(20)));
        assert!(missed.is_finished());
    }

    #[test]
    fn cadence_rejects_overflow_before_commit() {
        assert!(matches!(
            advance_cadence(Instant::now(), Duration::MAX, Instant::now()),
            Err(ClientError::DurationOverflow)
        ));
    }
}
