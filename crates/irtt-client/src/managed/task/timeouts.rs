//! Bounded timeout discovery and processing across target generations.

use std::time::Instant;

use crate::managed::{classify_client_error, ManagedTargetFailurePhase};

use super::{
    target::{duration_overflow_failure, OpenSessionFailureCleanup, TargetPhase},
    ManagedClientTask,
};

pub(super) const TIMEOUT_WORK_BUDGET: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct TimeoutBacklogEntry {
    index: usize,
    generation: u64,
}

#[derive(Clone, Copy)]
struct TimeoutStep {
    more_due: bool,
}

impl TimeoutStep {
    const NONE: Self = Self { more_due: false };
}

impl ManagedClientTask {
    pub(super) fn poll_timeout_pass(&mut self, now: Instant) -> bool {
        let target_len = self.targets.len();
        if target_len == 0 {
            return false;
        }

        let mut transitions_remaining = TIMEOUT_WORK_BUDGET;
        for _ in 0..target_len.min(TIMEOUT_WORK_BUDGET) {
            if transitions_remaining == 0 {
                break;
            }
            let index = self.timeout_cursor;
            self.timeout_cursor = (self.timeout_cursor + 1) % target_len;
            if self.target_has_due_timeout(index, now) {
                let generation = self.targets[index].instance.generation;
                let step = self.poll_target_timeout(index, now);
                transitions_remaining -= 1;
                if step.more_due {
                    self.enqueue_timeout_backlog(index, generation);
                }
            }
        }

        while transitions_remaining > 0 {
            let Some(entry) = self.timeout_backlog.pop_front() else {
                break;
            };
            let valid = self
                .targets
                .get(entry.index)
                .is_some_and(|target| target.instance.generation == entry.generation)
                && self.target_has_due_timeout(entry.index, now);
            if !valid {
                continue;
            }

            let step = self.poll_target_timeout(entry.index, now);
            transitions_remaining -= 1;
            if step.more_due {
                self.enqueue_timeout_backlog(entry.index, entry.generation);
            }
        }

        !self.timeout_backlog.is_empty()
    }

    fn enqueue_timeout_backlog(&mut self, index: usize, generation: u64) {
        if self.timeout_backlog.len() == TIMEOUT_WORK_BUDGET
            || self
                .timeout_backlog
                .iter()
                .any(|entry| entry.index == index && entry.generation == generation)
        {
            return;
        }
        self.timeout_backlog
            .push_back(TimeoutBacklogEntry { index, generation });
    }

    fn target_has_due_timeout(&self, index: usize, now: Instant) -> bool {
        #[cfg(test)]
        self.timeout_inspections.borrow_mut().push(index);
        match self.targets[index].phase() {
            TargetPhase::Active { client, .. } => {
                !self.phase.is_stopping()
                    && self.effective_retirement(index).is_none()
                    && client
                        .next_probe_timeout_deadline()
                        .is_some_and(|deadline| deadline <= now)
            }
            TargetPhase::Draining { client, .. } => client
                .next_probe_timeout_deadline()
                .is_some_and(|timeout| timeout <= now),
            _ => false,
        }
    }

    fn poll_target_timeout(&mut self, index: usize, now: Instant) -> TimeoutStep {
        match self.targets[index].phase_mut() {
            TargetPhase::Active { client, .. } => match client.poll_timeouts_bounded_at(now, 1) {
                Ok(batch) => {
                    self.publish_client_events(index, batch.events);
                    TimeoutStep {
                        more_due: batch.more_due,
                    }
                }
                Err(error) => {
                    self.begin_open_session_failure(
                        index,
                        ManagedTargetFailurePhase::Timing,
                        error,
                        now,
                        OpenSessionFailureCleanup::Close,
                    );
                    TimeoutStep::NONE
                }
            },
            TargetPhase::Draining { client, .. } => match client.poll_timeouts_bounded_at(now, 1) {
                Ok(batch) => {
                    self.publish_client_events(index, batch.events);
                    let TargetPhase::Draining {
                        client,
                        drain_started_at,
                        ..
                    } = self.targets[index].phase()
                    else {
                        unreachable!()
                    };
                    let candidate = self.drain_deadline(client, *drain_started_at);
                    let TargetPhase::Draining {
                        deadline,
                        primary_end,
                        cleanup_failure,
                        ..
                    } = self.targets[index].phase_mut()
                    else {
                        unreachable!()
                    };
                    match candidate {
                        Some(candidate) => *deadline = (*deadline).min(candidate),
                        None => {
                            cleanup_failure.get_or_insert_with(duration_overflow_failure);
                            let primary_end = primary_end.clone();
                            let cleanup_failure = cleanup_failure.clone();
                            self.begin_close(index, primary_end, cleanup_failure, now);
                            return TimeoutStep::NONE;
                        }
                    }
                    TimeoutStep {
                        more_due: batch.more_due,
                    }
                }
                Err(error) => {
                    self.targets[index].sync_live_packets_sent();
                    let TargetPhase::Draining {
                        primary_end,
                        cleanup_failure,
                        ..
                    } = self.targets[index].phase_mut()
                    else {
                        unreachable!()
                    };
                    cleanup_failure.get_or_insert_with(|| {
                        classify_client_error(ManagedTargetFailurePhase::Timing, &error)
                    });
                    let primary_end = primary_end.clone();
                    let cleanup_failure = cleanup_failure.clone();
                    self.begin_close(index, primary_end, cleanup_failure, now);
                    TimeoutStep::NONE
                }
            },
            _ => TimeoutStep::NONE,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::managed::{ManagedClientConfig, ManagedTargetConfig};

    use super::{super::ManagedClient, Instant, TIMEOUT_WORK_BUDGET};

    // Poll latency through public events cannot distinguish an O(n) discovery
    // scan from a bounded scan reliably. This tiny inspection trace measures
    // work directly, including empty targets, and checks eventual coverage
    // without asserting a cursor value or a particular visitation order.
    #[test]
    fn timeout_discovery_has_bounded_work_and_eventually_visits_every_target() {
        let count = TIMEOUT_WORK_BUDGET * 2 + 1;
        let targets = (0..count)
            .map(|i| ManagedTargetConfig::new(i.to_string(), "127.0.0.1:2112"))
            .collect();
        let (mut task, _handle) = ManagedClient::task(
            ManagedClientConfig {
                max_live_target_generations: count,
                ..ManagedClientConfig::default()
            },
            targets,
        )
        .unwrap();
        let mut visited = HashSet::new();
        for _ in 0..count.div_ceil(TIMEOUT_WORK_BUDGET) {
            task.timeout_inspections.borrow_mut().clear();
            task.poll_timeout_pass(Instant::now());
            let inspected = task.timeout_inspections.borrow();
            assert!(inspected.len() <= TIMEOUT_WORK_BUDGET);
            visited.extend(inspected.iter().copied());
        }
        assert_eq!(
            visited.len(),
            count,
            "timeout discovery must reach all targets"
        );
    }
}
