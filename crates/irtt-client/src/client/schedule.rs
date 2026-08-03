use std::time::{Duration, Instant};

use crate::{error::ClientError, session::NegotiatedParams};

#[derive(Debug)]
pub(crate) struct ProbeSchedule {
    start_at: Instant,
    end_at: Option<Instant>,
    interval: Duration,
    next_send_at: Option<Instant>,
}

#[derive(Debug)]
pub(crate) struct ScheduleCommit {
    pub(crate) scheduled_at: Instant,
    pub(crate) next_send_at: Option<Instant>,
    pub(crate) timer_error: Duration,
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
            start_at,
            end_at,
            interval,
            next_send_at: Some(start_at),
        })
    }

    pub(crate) fn next_send_deadline(&self) -> Option<Instant> {
        self.next_send_at
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

    pub(crate) fn preflight_caller_commit(
        &self,
        sent_at: Instant,
        next_packets_sent: u64,
    ) -> Result<ScheduleCommit, ClientError> {
        let scheduled_at = self
            .next_send_at
            .expect("a caller schedule commit requires a permitted probe");
        let interval_ns =
            u64::try_from(self.interval.as_nanos()).map_err(|_| ClientError::DurationOverflow)?;
        let next_send_at = next_probe_deadline(self.start_at, interval_ns, next_packets_sent)?;
        Ok(self.finish_commit(scheduled_at, next_send_at, sent_at))
    }

    pub(crate) fn preflight_managed_commit(
        &self,
        scheduled_at: Instant,
        sent_at: Instant,
    ) -> Result<ScheduleCommit, ClientError> {
        let (scheduled_at, next_send_at, _) =
            advance_cadence(scheduled_at, self.interval, sent_at)?;
        Ok(self.finish_commit(scheduled_at, next_send_at, sent_at))
    }

    pub(crate) fn commit(&mut self, commit: ScheduleCommit) {
        self.next_send_at = commit.next_send_at;
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.next_send_at.is_none()
    }

    fn finish_commit(
        &self,
        scheduled_at: Instant,
        next_send_at: Instant,
        sent_at: Instant,
    ) -> ScheduleCommit {
        ScheduleCommit {
            scheduled_at,
            next_send_at: if self.end_at.is_some_and(|end| next_send_at >= end) {
                None
            } else {
                Some(next_send_at)
            },
            timer_error: instant_abs_diff(sent_at, scheduled_at),
        }
    }
}

fn next_probe_deadline(
    start: Instant,
    interval_ns: u64,
    packets_sent: u64,
) -> Result<Instant, ClientError> {
    let offset_ns = interval_ns
        .checked_mul(packets_sent)
        .ok_or(ClientError::DurationOverflow)?;
    start
        .checked_add(Duration::from_nanos(offset_ns))
        .ok_or(ClientError::DurationOverflow)
}

pub(crate) fn advance_cadence(
    deadline: Instant,
    interval: Duration,
    now: Instant,
) -> Result<(Instant, Instant, u128), ClientError> {
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
    Ok((scheduled_at, next_at, elapsed_slots))
}

fn duration_from_nanos(nanos: u128) -> Result<Duration, ClientError> {
    const NANOS_PER_SECOND: u128 = 1_000_000_000;
    let seconds =
        u64::try_from(nanos / NANOS_PER_SECOND).map_err(|_| ClientError::DurationOverflow)?;
    let subsec_nanos = u32::try_from(nanos % NANOS_PER_SECOND)
        .expect("nanosecond remainder is always less than one second");
    Ok(Duration::new(seconds, subsec_nanos))
}

fn instant_abs_diff(left: Instant, right: Instant) -> Duration {
    left.checked_duration_since(right)
        .or_else(|| right.checked_duration_since(left))
        .unwrap_or(Duration::ZERO)
}

#[cfg(test)]
mod tests {
    use super::*;
    use irtt_proto::Params;

    fn negotiated(interval_ns: i64, duration_ns: i64) -> NegotiatedParams {
        NegotiatedParams {
            params: Params {
                interval_ns,
                duration_ns,
                ..Params::default()
            },
            restrictions: Vec::new(),
        }
    }

    #[test]
    fn initial_deadline_is_the_opening_time() {
        let opened_at = Instant::now();

        let schedule = ProbeSchedule::new(opened_at, &negotiated(10_000_000, 0)).unwrap();

        assert_eq!(schedule.next_send_deadline(), Some(opened_at));
        assert!(!schedule.is_finished());
    }

    #[test]
    fn caller_preflight_keeps_absolute_cadence_without_mutation() {
        let start = Instant::now();
        let schedule = ProbeSchedule::new(start, &negotiated(10_000_000, 0)).unwrap();
        let sent_at = start + Duration::from_millis(4);

        let commit = schedule.preflight_caller_commit(sent_at, 3).unwrap();

        assert_eq!(commit.scheduled_at, start);
        assert_eq!(commit.next_send_at, Some(start + Duration::from_millis(30)));
        assert_eq!(commit.timer_error, Duration::from_millis(4));
        assert_eq!(schedule.next_send_deadline(), Some(start));
    }

    #[test]
    fn caller_commit_advances_exactly_one_cadence_slot() {
        let start = Instant::now();
        let mut schedule = ProbeSchedule::new(start, &negotiated(10_000_000, 0)).unwrap();

        let commit = schedule.preflight_caller_commit(start, 1).unwrap();
        assert_eq!(schedule.next_send_deadline(), Some(start));

        schedule.commit(commit);
        assert_eq!(
            schedule.next_send_deadline(),
            Some(start + Duration::from_millis(10))
        );
    }

    #[test]
    fn finite_duration_uses_an_exclusive_end_boundary() {
        let start = Instant::now();
        let mut schedule = ProbeSchedule::new(start, &negotiated(10_000_000, 20_000_000)).unwrap();

        let first = schedule.preflight_caller_commit(start, 1).unwrap();
        schedule.commit(first);
        assert_eq!(
            schedule.next_send_deadline(),
            Some(start + Duration::from_millis(10))
        );

        let second = schedule
            .preflight_caller_commit(start + Duration::from_millis(10), 2)
            .unwrap();
        schedule.commit(second);

        assert!(schedule.is_finished());
        assert_eq!(schedule.next_send_deadline(), None);
        assert!(!schedule.permit_probe_at(start + Duration::from_millis(20)));
    }

    #[test]
    fn permit_at_exclusive_end_finishes_schedule_without_a_send() {
        let start = Instant::now();
        let mut schedule = ProbeSchedule::new(start, &negotiated(10_000_000, 20_000_000)).unwrap();

        assert!(!schedule.permit_probe_at(start + Duration::from_millis(20)));
        assert!(schedule.is_finished());
        assert_eq!(schedule.next_send_deadline(), None);
    }

    #[test]
    fn managed_preflight_skips_missed_slots_without_mutation() {
        let start = Instant::now();
        let schedule = ProbeSchedule::new(start, &negotiated(10_000_000, 0)).unwrap();
        let sent_at = start + Duration::from_millis(45);

        let commit = schedule.preflight_managed_commit(start, sent_at).unwrap();

        assert_eq!(commit.scheduled_at, start + Duration::from_millis(40));
        assert_eq!(commit.next_send_at, Some(start + Duration::from_millis(50)));
        assert_eq!(commit.timer_error, Duration::from_millis(5));
        assert_eq!(schedule.next_send_deadline(), Some(start));
    }

    #[test]
    fn delayed_managed_commit_does_not_replay_catch_up_slots() {
        let start = Instant::now();
        let mut schedule = ProbeSchedule::new(start, &negotiated(10_000_000, 0)).unwrap();
        let delayed_send = start + Duration::from_millis(45);

        let commit = schedule
            .preflight_managed_commit(start, delayed_send)
            .unwrap();
        schedule.commit(commit);

        assert_eq!(
            schedule.next_send_deadline(),
            Some(start + Duration::from_millis(50))
        );
        assert!(schedule.next_send_deadline().unwrap() > delayed_send);
    }

    #[test]
    fn deadline_arithmetic_overflow_is_detected_before_commit() {
        let start = Instant::now();
        let schedule = ProbeSchedule::new(start, &negotiated(2, 0)).unwrap();

        assert!(matches!(
            schedule.preflight_caller_commit(start, u64::MAX),
            Err(ClientError::DurationOverflow)
        ));
        assert_eq!(schedule.next_send_deadline(), Some(start));
    }
}
