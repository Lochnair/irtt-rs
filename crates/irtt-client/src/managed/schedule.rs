use std::time::{Duration, Instant};

use super::{ManagedPacing, ManagedTargetConfig};
use crate::{error::ClientError, session::AcceptedSessionParameters};

#[derive(Clone, Debug)]
pub(crate) struct ProbeSchedule {
    end_at: Option<Instant>,
    interval: Duration,
    next_send_at: Option<Instant>,
    last_sent_at: Option<Instant>,
}

#[derive(Debug)]
pub(crate) struct ScheduleCommit {
    pub(crate) scheduled_at: Instant,
    pub(crate) next_send_at: Option<Instant>,
}

impl ProbeSchedule {
    pub(crate) fn new(
        start_at: Instant,
        accepted: &AcceptedSessionParameters,
    ) -> Result<Self, ClientError> {
        let interval = accepted.interval;
        let end_at = accepted
            .duration
            .map(|duration| {
                start_at
                    .checked_add(duration)
                    .ok_or_else(|| ClientError::NegotiationRejected {
                        reason: "duration is too large to schedule".to_owned(),
                    })
            })
            .transpose()?;

        Ok(Self {
            end_at,
            interval,
            next_send_at: Some(start_at),
            last_sent_at: None,
        })
    }

    pub(crate) fn explicit(
        start_at: Instant,
        accepted: &AcceptedSessionParameters,
        origin: Instant,
        now: Instant,
    ) -> Result<Self, ClientError> {
        let mut schedule = Self::new(start_at, accepted)?;
        schedule.align(origin, now)?;
        Ok(schedule)
    }

    /// Prepare a replacement schedule without changing session lifetime/state.
    pub(crate) fn rephased(&self, origin: Instant, now: Instant) -> Result<Self, ClientError> {
        let mut schedule = self.clone();
        if !self.is_finished() {
            let earliest = self
                .last_sent_at
                .map(|last| {
                    last.checked_add(self.interval)
                        .ok_or(ClientError::DurationOverflow)
                })
                .transpose()?
                .map_or(now, |minimum| now.max(minimum));
            schedule.align(origin, earliest)?;
        }
        Ok(schedule)
    }

    fn align(&mut self, origin: Instant, earliest: Instant) -> Result<(), ClientError> {
        // Expiry wins even when rounding a later slot would overflow.
        self.next_send_at = if self
            .end_at
            .is_some_and(|end| earliest >= end || origin >= end)
        {
            None
        } else {
            let deadline = grid_at_or_after(origin, self.interval, earliest)?;
            Some(deadline).filter(|deadline| self.end_at.is_none_or(|end| *deadline < end))
        };
        Ok(())
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

    pub(crate) fn record_send(&mut self, sent_at: Instant) {
        self.last_sent_at = Some(sent_at);
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

/// Configuration-only checks; accepted-interval arithmetic belongs to activation
/// or to the preflight of a live target-update transaction.
pub(crate) fn explicit_origin(
    pacing: ManagedPacing,
    target: &ManagedTargetConfig,
) -> Result<Option<Instant>, ClientError> {
    match (pacing, target.send_offset) {
        (ManagedPacing::Explicit { epoch }, Some(offset)) => epoch
            .checked_add(offset)
            .map(Some)
            .ok_or(ClientError::DurationOverflow),
        (ManagedPacing::Explicit { .. }, None) => Err(ClientError::InvalidConfig {
            reason: "explicit pacing requires a target send offset".to_owned(),
        }),
        (_, Some(_)) => Err(ClientError::InvalidConfig {
            reason: "target send offsets require explicit pacing".to_owned(),
        }),
        (_, None) => Ok(None),
    }
}

fn grid_at_or_after(
    origin: Instant,
    interval: Duration,
    earliest: Instant,
) -> Result<Instant, ClientError> {
    if interval.is_zero() {
        return Err(ClientError::InvalidConfig {
            reason: "probe interval must be greater than zero".to_owned(),
        });
    }
    let elapsed = earliest
        .checked_duration_since(origin)
        .unwrap_or_default()
        .as_nanos();
    let slots = elapsed.div_ceil(interval.as_nanos());
    let offset = duration_from_nanos(
        interval
            .as_nanos()
            .checked_mul(slots)
            .ok_or(ClientError::DurationOverflow)?,
    )?;
    origin
        .checked_add(offset)
        .ok_or(ClientError::DurationOverflow)
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

    // Pure arithmetic protects exact boundaries without relying on timer precision.
    #[test]
    fn explicit_grids_rephase_without_drift_or_extending_lifetime() {
        let epoch = Instant::now();
        let ms = Duration::from_millis;
        let at = |millis| epoch + ms(millis);
        let mut accepted = AcceptedSessionParameters {
            duration: None,
            interval: ms(500),
            length: 0,
            received_stats: irtt_proto::ReceivedStats::None,
            stamp_at: irtt_proto::StampAt::None,
            clock: irtt_proto::Clock::Both,
            dscp: 0,
            server_fill: None,
        };
        for (offset, activation, expected) in [
            (0, 0, 0),
            (0, 1, 500),
            (100, 0, 100),
            (100, 100, 100),
            (100, 101, 600),
            (100, 1600, 1600),
            (900, 100, 900),
        ] {
            let schedule =
                ProbeSchedule::explicit(at(activation), &accepted, at(offset), at(activation))
                    .unwrap();
            assert_eq!(schedule.next_send_deadline(), Some(at(expected)));
        }
        let mut schedule = ProbeSchedule::explicit(epoch, &accepted, epoch, epoch).unwrap();
        let commit = schedule.preflight_managed_commit(epoch, at(1280)).unwrap();
        assert_eq!(commit.scheduled_at, at(1000));
        schedule.commit(commit);
        schedule.record_send(at(1510));
        schedule.skip_missed_slots_at(at(1510)).unwrap();
        assert_eq!(schedule.next_send_deadline(), Some(at(2000)));
        assert_eq!(
            schedule
                .rephased(at(100), at(1600))
                .unwrap()
                .next_send_deadline(),
            Some(at(2100))
        );
        assert_eq!(
            schedule
                .rephased(at(10), at(1600))
                .unwrap()
                .next_send_deadline(),
            Some(at(2010))
        );
        // A reconnect has no carry-over minimum gap from the previous session.
        let fresh = ProbeSchedule::explicit(at(1600), &accepted, at(100), at(1600)).unwrap();
        assert_eq!(fresh.next_send_deadline(), Some(at(1600)));

        accepted.duration = Some(ms(500));
        for (origin, activation) in [(0, 1), (500, 0), (501, 0), (0, 500)] {
            let mut finite =
                ProbeSchedule::explicit(epoch, &accepted, at(origin), at(activation)).unwrap();
            assert!(finite.is_finished());
            assert!(!finite.permit_probe_at(at(activation)));
            assert!(finite.rephased(epoch, epoch).unwrap().is_finished());
        }
        let finite = ProbeSchedule::explicit(epoch, &accepted, epoch, epoch).unwrap();
        assert_eq!(
            finite
                .rephased(at(500), epoch)
                .unwrap()
                .next_send_deadline(),
            None
        );
        assert!(finite.rephased(epoch, at(500)).unwrap().is_finished());
        accepted.duration = None;
        accepted.interval = Duration::MAX;
        assert!(matches!(
            ProbeSchedule::explicit(epoch, &accepted, epoch, at(1)),
            Err(ClientError::DurationOverflow)
        ));
    }

    #[test]
    fn lifetime_deadline_exists_only_for_an_unfinished_finite_schedule() {
        let start = Instant::now();
        let end = start + Duration::from_millis(500);
        let mut accepted = AcceptedSessionParameters {
            duration: Some(Duration::from_millis(500)),
            interval: Duration::from_millis(1),
            length: 0,
            received_stats: irtt_proto::ReceivedStats::None,
            stamp_at: irtt_proto::StampAt::None,
            clock: irtt_proto::Clock::Both,
            dscp: 0,
            server_fill: None,
        };
        let mut finite = ProbeSchedule::new(start, &accepted).unwrap();
        assert_eq!(finite.end_deadline(), Some(end));
        assert!(finite.permit_probe_at(end - Duration::from_nanos(1)));
        assert_eq!(finite.end_deadline(), Some(end));
        assert!(!finite.permit_probe_at(end));
        assert!(finite.is_finished());
        assert_eq!(finite.end_deadline(), None);

        let mut finite = ProbeSchedule::new(start, &accepted).unwrap();
        let commit = finite
            .preflight_managed_commit(start, end - Duration::from_nanos(1))
            .unwrap();
        finite.commit(commit);
        assert!(finite.is_finished());
        assert_eq!(finite.end_deadline(), None);

        accepted.duration = None;
        let mut continuous = ProbeSchedule::new(start, &accepted).unwrap();
        assert!(continuous.permit_probe_at(end));
        assert_eq!(continuous.end_deadline(), None);
    }
}
