//! Burst/stagger send selection and committed-send cadence.

use std::{
    task::{Context, Poll},
    time::{Duration, Instant},
};

use crate::{
    managed::{schedule::instant_abs_diff, ManagedTargetEndReason, ManagedTargetFailurePhase},
    ClientEvent, SendProbeError,
};

use super::{
    target::{OpenSessionFailureCleanup, TargetPhase},
    ManagedClientTask, TARGET_WORK_BUDGET,
};

impl ManagedClientTask {
    pub(super) fn active_count(&self) -> usize {
        self.targets
            .iter()
            .filter(|target| target.is_paced_active())
            .count()
    }

    fn active_stagger_spacing(&self) -> Option<Duration> {
        let mut active = 0;
        let mut minimum: Option<Duration> = None;
        for target in &self.targets {
            let TargetPhase::Active { schedule, .. } = target.phase() else {
                continue;
            };
            if !target.membership.is_desired() {
                continue;
            }
            active += 1;
            minimum = minimum.into_iter().chain(Some(schedule.interval())).min();
        }
        minimum.map(|interval| stagger_spacing(interval, active))
    }

    pub(super) fn stagger_target_added(&mut self, now: Instant) {
        let Some(existing) = self.send_gate.filter(|gate| *gate > now) else {
            self.send_gate = None;
            return;
        };
        let candidate = self
            .last_stagger_send
            .zip(self.active_stagger_spacing())
            .and_then(|(last, spacing)| last.checked_add(spacing))
            .filter(|gate| *gate > now);
        self.send_gate = candidate.map(|candidate| existing.min(candidate));
    }

    pub(super) fn stagger_target_removed(&mut self, now: Instant) {
        self.send_gate = self.send_gate.filter(|gate| *gate > now);
    }

    /// The next stagger gate after a send was accepted at `accepted_at`.
    ///
    /// The gate advances on its **own** cadence rather than being re-anchored
    /// on every observed accept time. Anchoring on `accepted_at` folds each
    /// send's wakeup latency into the next slot and then into every slot after
    /// it, so the gate — and with it the whole group's send cadence — drifts
    /// later without bound while the per-target schedules stay on their
    /// absolute grid. The drift is not hypothetical: it shows up as a
    /// monotonically growing `EchoSent::timer_error` and, once it exceeds one
    /// interval, as probe slots the schedule then skips.
    ///
    /// `accepted_at` is still the anchor in the two cases where continuing the
    /// old cadence would be wrong: there is no previous gate, or the pacer has
    /// fallen a full slot behind it. Re-anchoring there is what keeps a gate
    /// stranded in the past from releasing a burst of catch-up sends once the
    /// group starts moving again.
    fn next_stagger_gate(&self, spacing: Duration, accepted_at: Instant) -> Option<Instant> {
        let base = match self.send_gate {
            Some(gate) if gate <= accepted_at && accepted_at.duration_since(gate) < spacing => gate,
            _ => accepted_at,
        };
        base.checked_add(spacing)
    }

    fn record_stagger_acceptance(
        &mut self,
        result: SendResult,
        stagger_spacing: Option<Duration>,
        accepted_at: Instant,
    ) {
        if !result.accepted() {
            return;
        }
        let Some(spacing) = stagger_spacing else {
            return;
        };
        self.last_stagger_send = Some(accepted_at);
        self.send_gate = self.next_stagger_gate(spacing, accepted_at);
    }

    fn poll_one_send(
        &mut self,
        index: usize,
        cx: &mut Context<'_>,
        now: Instant,
        stagger_spacing: Option<Duration>,
    ) -> SendResult {
        if !self.targets[index].membership.is_desired() {
            if let TargetPhase::Active { send_waiting, .. } = self.targets[index].phase_mut() {
                *send_waiting = false;
            }
            return SendResult::NotAttempted;
        }
        let phase = self.targets[index].take_phase();
        let TargetPhase::Active {
            mut client,
            mut schedule,
            mut send_waiting,
        } = phase
        else {
            self.targets[index].restore_phase(phase);
            return SendResult::NotAttempted;
        };
        // A burst pass may span several slots; validate this target against
        // fresh time rather than the pass's earlier scheduling snapshot.
        let now = now.max(Instant::now());
        if client
            .next_probe_timeout_deadline()
            .is_some_and(|deadline| deadline <= now)
        {
            send_waiting = false;
            self.targets[index].restore_phase(TargetPhase::Active {
                client,
                schedule,
                send_waiting,
            });
            return SendResult::NotAttempted;
        }
        if !schedule.permit_probe_at(now) {
            send_waiting = false;
            let pending = client.has_pending_probes();
            if !pending {
                self.targets[index].sync_packets_sent(&client);
            }
            self.targets[index].restore_phase(TargetPhase::Active {
                client,
                schedule,
                send_waiting,
            });
            if pending {
                return SendResult::NotAttempted;
            }
            self.begin_drain(index, ManagedTargetEndReason::TestComplete, now);
            return SendResult::Ready { accepted: false };
        }
        if schedule
            .next_send_deadline()
            .is_none_or(|deadline| deadline > now)
        {
            send_waiting = false;
            self.targets[index].restore_phase(TargetPhase::Active {
                client,
                schedule,
                send_waiting,
            });
            return SendResult::NotAttempted;
        }
        let commit =
            match schedule.preflight_managed_commit(schedule.next_send_deadline().unwrap(), now) {
                Ok(commit) => commit,
                Err(error) => {
                    self.targets[index].restore_phase(TargetPhase::Active {
                        client,
                        schedule,
                        send_waiting,
                    });
                    self.begin_open_session_failure(
                        index,
                        ManagedTargetFailurePhase::Timing,
                        error,
                        now,
                        OpenSessionFailureCleanup::Drain,
                    );
                    return SendResult::Failed { accepted: false };
                }
            };
        let scheduled_at = commit.scheduled_at;
        let result = client.poll_send_probe(cx);
        let receipt = match &result {
            Poll::Ready(Ok(receipt))
            | Poll::Ready(Err(SendProbeError::AfterCommit { receipt, .. })) => Some(*receipt),
            Poll::Pending | Poll::Ready(Err(SendProbeError::NotCommitted(_))) => None,
        };
        let accepted = receipt.is_some();
        let send_result = match &result {
            Poll::Pending => SendResult::Pending,
            Poll::Ready(Ok(_)) => SendResult::Ready { accepted },
            Poll::Ready(Err(_)) => SendResult::Failed { accepted },
        };
        self.record_stagger_acceptance(send_result, stagger_spacing, Instant::now());
        if accepted {
            schedule.commit(commit);
        }
        let schedule_error = if accepted {
            schedule.skip_missed_slots_at(Instant::now()).err()
        } else {
            None
        };
        self.targets[index].sync_packets_sent(&client);
        send_waiting = result.is_pending();
        self.targets[index].restore_phase(TargetPhase::Active {
            client,
            schedule,
            send_waiting,
        });
        // A committed send must reach the lifecycle/accounting stream even
        // when post-send processing fails. Publish before failure cleanup.
        let sent_event = receipt.map(|receipt| {
            let mut event = ClientEvent::from(receipt);
            if let ClientEvent::EchoSent {
                scheduled_at: intended,
                timer_error,
                ..
            } = &mut event
            {
                *intended = Some(scheduled_at);
                *timer_error = Some(instant_abs_diff(receipt.sent_at.mono, scheduled_at));
            }
            event
        });
        match result {
            Poll::Pending => SendResult::Pending,
            Poll::Ready(Ok(_)) => {
                self.publish_client_events(index, vec![sent_event.unwrap()]);
                if let Some(error) = schedule_error {
                    self.begin_open_session_failure(
                        index,
                        ManagedTargetFailurePhase::Timing,
                        error,
                        now,
                        OpenSessionFailureCleanup::Drain,
                    );
                    SendResult::Failed { accepted }
                } else {
                    SendResult::Ready { accepted }
                }
            }
            Poll::Ready(Err(error)) => {
                if let Some(event) = sent_event {
                    self.publish_client_events(index, vec![event]);
                }
                let error = match error {
                    SendProbeError::NotCommitted(source) => source,
                    SendProbeError::AfterCommit { source, .. } => *source,
                };
                self.begin_open_session_failure(
                    index,
                    ManagedTargetFailurePhase::Sending,
                    error,
                    now,
                    OpenSessionFailureCleanup::Drain,
                );
                SendResult::Failed { accepted }
            }
        }
    }

    pub(super) fn poll_staggered_send(&mut self, cx: &mut Context<'_>, now: Instant) -> bool {
        if self.active_count() == 0 || self.send_gate.is_some_and(|gate| gate > now) {
            return false;
        }
        if self.stagger_remaining == 0 {
            self.stagger_remaining = self.targets.len();
        }
        let work = self.stagger_remaining.min(TARGET_WORK_BUDGET);
        for _ in 0..work {
            let index = self.send_cursor % self.targets.len();
            self.send_cursor = (self.send_cursor + 1) % self.targets.len();
            self.stagger_remaining -= 1;
            if !self.targets[index].is_paced_active() {
                continue;
            }
            let stagger_spacing = self.active_stagger_spacing();
            let result = self.poll_one_send(index, cx, now, stagger_spacing);
            if result == SendResult::NotAttempted {
                continue;
            }
            self.stagger_remaining = 0;
            return matches!(result, SendResult::Ready { .. } | SendResult::Failed { .. });
        }
        self.stagger_remaining > 0
    }

    pub(super) fn poll_burst_sends(&mut self, cx: &mut Context<'_>, now: Instant) -> bool {
        if self.targets.is_empty() {
            return false;
        }
        if self.burst_remaining == 0 {
            self.burst_remaining = self.targets.len();
        }
        let work = self.burst_remaining.min(TARGET_WORK_BUDGET);
        let mut immediate = false;
        for _ in 0..work {
            let index = self.send_cursor % self.targets.len();
            self.send_cursor = (self.send_cursor + 1) % self.targets.len();
            self.burst_remaining -= 1;
            let result = self.poll_one_send(index, cx, now, None);
            immediate |= matches!(result, SendResult::Ready { .. } | SendResult::Failed { .. });
        }
        immediate || self.burst_remaining > 0
    }
}

// A release gate delays a send opportunity; it cannot create one.
pub(super) fn gated_send_deadline(
    cadence: Option<Instant>,
    gate: Option<Instant>,
) -> Option<Instant> {
    cadence.map(|deadline| gate.map_or(deadline, |gate| deadline.max(gate)))
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SendResult {
    NotAttempted,
    Pending,
    Ready { accepted: bool },
    Failed { accepted: bool },
}

impl SendResult {
    fn accepted(self) -> bool {
        matches!(
            self,
            Self::Ready { accepted: true } | Self::Failed { accepted: true }
        )
    }
}

fn stagger_spacing(interval: Duration, active_targets: usize) -> Duration {
    let divisor = u128::try_from(active_targets.max(1)).unwrap_or(u128::MAX);
    let nanos = (interval.as_nanos() / divisor).max(1);
    let seconds = u64::try_from(nanos / 1_000_000_000).unwrap_or(u64::MAX);
    let subsec = u32::try_from(nanos % 1_000_000_000).unwrap_or(999_999_999);
    Duration::new(seconds, subsec)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_gate_only_delays_an_existing_send_deadline() {
        let earlier = Instant::now();
        let later = earlier + Duration::from_secs(1);
        for (cadence, gate, expected) in [
            (None, None, None),
            (None, Some(earlier), None),
            (None, Some(later), None),
            (Some(earlier), None, Some(earlier)),
            (Some(later), Some(earlier), Some(later)),
            (Some(earlier), Some(earlier), Some(earlier)),
            (Some(earlier), Some(later), Some(later)),
        ] {
            assert_eq!(gated_send_deadline(cadence, gate), expected);
        }
    }
}
