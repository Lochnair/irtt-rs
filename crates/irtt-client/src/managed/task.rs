//! Task ownership, durable status/history, and top-level polling and sealing.
//!
//! Private children implement control, target lifecycle, updates, pacing, and
//! bounded timeout work on this same task; they do not introduce other owners.

mod control;
mod pacing;
mod target;
mod timeouts;
mod update;

pub use control::{ManagedClient, ManagedClientHandle, ManagedCommandReceipt, ManagedStopReceipt};

use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

use tokio::{
    sync::{broadcast, mpsc, watch},
    time::{self, Sleep},
};

use super::{
    ManagedClientConfig, ManagedCommandApplyError, ManagedCompletionPolicy, ManagedDriverFailure,
    ManagedEndReason, ManagedEvent, ManagedLifecycle, ManagedOutcome, ManagedPacing, ManagedStatus,
    ManagedTargetEndReason, ManagedTargetLifecycle, ManagedTargetOutcome, ManagedTargetStatus,
};
use control::{ManagedCommand, StopSignal};
use pacing::gated_send_deadline;
use target::{TargetPhase, TargetRuntime};
use timeouts::TimeoutBacklogEntry;

const TARGET_WORK_BUDGET: usize = 128;

type WakeFuture = Pin<Box<dyn Future<Output = Option<watch::Receiver<()>>> + Send + 'static>>;

fn arm_wake(mut receiver: watch::Receiver<()>) -> WakeFuture {
    Box::pin(async move {
        receiver.changed().await.ok()?;
        Some(receiver)
    })
}

#[derive(Default)]
struct OutcomeHistory {
    limit: usize,
    recent: VecDeque<ManagedTargetOutcome>,
    total: u64,
    successful: u64,
    failed: u64,
    peer_closed: u64,
    discarded: u64,
}

impl OutcomeHistory {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            ..Self::default()
        }
    }

    fn record(&mut self, outcome: ManagedTargetOutcome) {
        self.total = self.total.saturating_add(1);
        match &outcome.end_reason {
            ManagedTargetEndReason::Failed(_) => self.failed = self.failed.saturating_add(1),
            ManagedTargetEndReason::PeerClosed => {
                self.successful = self.successful.saturating_add(1);
                self.peer_closed = self.peer_closed.saturating_add(1);
            }
            ManagedTargetEndReason::TestComplete
            | ManagedTargetEndReason::NoTestComplete
            | ManagedTargetEndReason::Removed
            | ManagedTargetEndReason::Replaced
            | ManagedTargetEndReason::Stopped => {
                self.successful = self.successful.saturating_add(1);
            }
        }
        if self.limit == 0 {
            self.discarded = self.discarded.saturating_add(1);
            return;
        }
        if self.recent.len() == self.limit {
            self.recent.pop_front();
            self.discarded = self.discarded.saturating_add(1);
        }
        self.recent.push_back(outcome);
    }

    fn recent(&self) -> Arc<[ManagedTargetOutcome]> {
        Arc::from(
            self.recent
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        )
    }

    fn outcome(
        &self,
        end_reason: ManagedEndReason,
        applied_command_sequence: u64,
    ) -> ManagedOutcome {
        ManagedOutcome {
            end_reason,
            applied_command_sequence,
            total_target_outcomes: self.total,
            successful_target_outcomes: self.successful,
            failed_target_outcomes: self.failed,
            peer_closed_target_outcomes: self.peer_closed,
            discarded_target_outcomes: self.discarded,
            recent_target_outcomes: self.recent(),
        }
    }
}

struct TaskResources {
    status: watch::Sender<Arc<ManagedStatus>>,
    events: Option<broadcast::Sender<ManagedEvent>>,
    stop: Arc<StopSignal>,
}

enum TaskPhase {
    NotStarted,
    Running,
    Stopping,
    Completed { outcome: Arc<ManagedOutcome> },
    Failed { outcome: Arc<ManagedOutcome> },
    Abandoned,
}

impl TaskPhase {
    fn lifecycle(&self) -> ManagedLifecycle {
        match self {
            Self::NotStarted => ManagedLifecycle::NotStarted,
            Self::Running => ManagedLifecycle::Running,
            Self::Stopping => ManagedLifecycle::Stopping,
            Self::Completed { .. } => ManagedLifecycle::Completed,
            Self::Failed { .. } => ManagedLifecycle::Failed,
            Self::Abandoned => ManagedLifecycle::Abandoned,
        }
    }

    fn final_outcome(&self) -> Option<Arc<ManagedOutcome>> {
        match self {
            Self::Completed { outcome } | Self::Failed { outcome } => Some(Arc::clone(outcome)),
            _ => None,
        }
    }

    fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed { .. } | Self::Failed { .. } | Self::Abandoned
        )
    }

    fn is_running(&self) -> bool {
        matches!(self, Self::Running)
    }

    fn is_stopping(&self) -> bool {
        matches!(self, Self::Stopping)
    }
}

/// The sole authoritative zero-or-many target driver.
#[must_use = "ManagedClientTask must be awaited or deliberately dropped"]
pub struct ManagedClientTask {
    #[cfg(test)]
    timeout_inspections: std::cell::RefCell<Vec<usize>>,
    phase: TaskPhase,
    config: ManagedClientConfig,
    targets: Vec<TargetRuntime>,
    commands: mpsc::Receiver<ManagedCommand>,
    history: OutcomeHistory,
    resources: Option<TaskResources>,
    wake: Option<WakeFuture>,
    timer: Option<Pin<Box<Sleep>>>,
    cursor: usize,
    timeout_cursor: usize,
    timeout_backlog: VecDeque<TimeoutBacklogEntry>,
    scan_remaining: usize,
    send_cursor: usize,
    burst_remaining: usize,
    stagger_remaining: usize,
    send_gate: Option<Instant>,
    last_stagger_send: Option<Instant>,
    // Explicit stop observation is independent of natural stopping or terminality.
    stop_observed: bool,
    next_generation: u64,
    applied_command_sequence: u64,
}

impl ManagedClientTask {
    fn resources(&self) -> &TaskResources {
        self.resources
            .as_ref()
            .expect("managed task resources exist before terminal sealing")
    }

    fn publish_event(&self, event: ManagedEvent) {
        if let Some(events) = &self.resources().events {
            let _ = events.send(event);
        }
    }

    fn snapshot(&self) -> Arc<ManagedStatus> {
        let mut connecting = 0;
        let mut opening = 0;
        let mut active = 0;
        let mut draining = 0;
        let mut closing = 0;
        let mut terminal = 0;
        let targets = self
            .targets
            .iter()
            .map(|target| {
                let lifecycle = target.phase().lifecycle();
                match lifecycle {
                    ManagedTargetLifecycle::Pending => {}
                    ManagedTargetLifecycle::Connecting => connecting += 1,
                    ManagedTargetLifecycle::Opening => opening += 1,
                    ManagedTargetLifecycle::Active => active += 1,
                    ManagedTargetLifecycle::Draining => draining += 1,
                    ManagedTargetLifecycle::Closing => closing += 1,
                    ManagedTargetLifecycle::Terminal => terminal += 1,
                }
                ManagedTargetStatus {
                    target: target.instance.clone(),
                    desired: target.membership.is_desired(),
                    lifecycle,
                    outcome: match target.phase() {
                        TargetPhase::Terminal { outcome } => Some(Arc::clone(outcome)),
                        _ => None,
                    },
                    server_addr: Arc::clone(&target.server_addr),
                    remote: target.remote,
                }
            })
            .collect::<Vec<_>>();
        Arc::new(ManagedStatus {
            lifecycle: self.phase.lifecycle(),
            stop_requested: self.stop_observed,
            applied_command_sequence: self.applied_command_sequence,
            desired_target_count: self
                .targets
                .iter()
                .filter(|target| target.membership.is_desired())
                .count(),
            connecting_target_count: connecting,
            opening_target_count: opening,
            active_target_count: active,
            draining_target_count: draining,
            closing_target_count: closing,
            terminal_target_count: terminal,
            total_target_outcomes: self.history.total,
            successful_target_outcomes: self.history.successful,
            failed_target_outcomes: self.history.failed,
            peer_closed_target_outcomes: self.history.peer_closed,
            discarded_target_outcomes: self.history.discarded,
            targets: Arc::from(targets.into_boxed_slice()),
            recent_target_outcomes: self.history.recent(),
            final_outcome: self.phase.final_outcome(),
        })
    }

    fn replace_status(&self) {
        self.resources().status.send_replace(self.snapshot());
    }

    fn begin_running(&mut self) -> Result<(), ManagedDriverFailure> {
        tokio::runtime::Handle::try_current().map_err(|_| ManagedDriverFailure::NoTokioRuntime)?;
        self.phase = TaskPhase::Running;
        self.replace_status();
        self.publish_event(ManagedEvent::Started);
        Ok(())
    }

    fn observe_stop(&mut self) {
        if self.stop_observed {
            return;
        }
        self.stop_observed = true;
        self.resources().stop.begin_stopping();
        let lifecycle_transition = !self.phase.is_stopping();
        self.phase = TaskPhase::Stopping;
        self.replace_status();
        if lifecycle_transition {
            self.publish_event(ManagedEvent::Stopping);
        }
        self.scan_remaining = self.targets.len();
        self.burst_remaining = 0;
    }

    fn poll_target_pass(&mut self, cx: &mut Context<'_>, now: Instant) -> bool {
        if self.targets.is_empty() {
            return false;
        }
        if self.scan_remaining == 0 {
            self.scan_remaining = self.targets.len();
        }
        let work = self.scan_remaining.min(TARGET_WORK_BUDGET);
        let mut immediate = false;
        for _ in 0..work {
            let index = self.cursor % self.targets.len();
            self.cursor = (self.cursor + 1) % self.targets.len();
            self.scan_remaining -= 1;
            immediate |= self.poll_target(index, cx, now);
        }
        immediate || self.scan_remaining > 0
    }

    fn all_targets_terminal(&self) -> bool {
        self.targets
            .iter()
            .all(|target| matches!(target.phase(), TargetPhase::Terminal { .. }))
    }

    fn next_deadline(&self) -> Option<Instant> {
        let mut non_send_deadline = None;
        let mut send_deadline = None;
        for target in &self.targets {
            match target.phase() {
                TargetPhase::Active {
                    client,
                    schedule,
                    send_waiting,
                } => {
                    non_send_deadline = non_send_deadline
                        .into_iter()
                        .chain(client.next_probe_timeout_deadline())
                        .chain(schedule.end_deadline())
                        .min();
                    if target.membership.is_desired() && !send_waiting {
                        send_deadline = send_deadline
                            .into_iter()
                            .chain(schedule.next_send_deadline())
                            .min();
                    }
                }
                TargetPhase::Draining {
                    client, deadline, ..
                } => {
                    non_send_deadline = non_send_deadline
                        .into_iter()
                        .chain(client.next_probe_timeout_deadline())
                        .chain(Some(*deadline))
                        .min();
                }
                TargetPhase::Closing { deadline, .. } => {
                    non_send_deadline = non_send_deadline.into_iter().chain(Some(*deadline)).min();
                }
                _ => {}
            }
        }
        if self.phase.is_running()
            && self.config.pacing == ManagedPacing::Staggered
            && self.active_count() > 0
        {
            send_deadline = gated_send_deadline(send_deadline, self.send_gate);
        }
        non_send_deadline.into_iter().chain(send_deadline).min()
    }

    fn register_timer(&mut self, cx: &mut Context<'_>) -> bool {
        let Some(deadline) = self.next_deadline() else {
            self.timer = None;
            return false;
        };
        let timer = self
            .timer
            .get_or_insert_with(|| Box::pin(time::sleep_until(deadline.into())));
        timer.as_mut().reset(deadline.into());
        timer.as_mut().poll(cx).is_ready()
    }

    fn register_stop_wake(&mut self, cx: &mut Context<'_>) -> bool {
        let Some(mut wake) = self.wake.take() else {
            return false;
        };
        match wake.as_mut().poll(cx) {
            Poll::Pending => {
                self.wake = Some(wake);
                false
            }
            Poll::Ready(Some(receiver)) => {
                self.wake = Some(arm_wake(receiver));
                self.resources().stop.is_requested()
            }
            Poll::Ready(None) => false,
        }
    }

    fn seal(&mut self, mut end_reason: ManagedEndReason, failed: bool) -> Poll<ManagedOutcome> {
        // Sealing is the final stop-observation point.  A stop that reaches the
        // latch before this terminal linearization becomes durable even when
        // quiescence had already entered `Stopping`.
        if self.resources().stop.is_requested() {
            self.observe_stop();
        }
        if self.stop_observed && matches!(end_reason, ManagedEndReason::TargetsComplete) {
            end_reason = ManagedEndReason::StopRequested;
        }

        // Closing admission before closing and draining the receiver makes every
        // accepted command task-owned.  Tokio still permits buffered commands
        // to be drained after `Receiver::close`, but rejects later sends.
        self.resources().stop.close_updates();
        self.commands.close();
        while let Ok(ManagedCommand::UpdateTargets {
            acknowledgement, ..
        }) = self.commands.try_recv()
        {
            let error = match &end_reason {
                ManagedEndReason::DriverFailed(failure) => {
                    ManagedCommandApplyError::DriverFailed(failure.clone())
                }
                _ => ManagedCommandApplyError::Stopping,
            };
            let _ = acknowledgement.send(Err(error));
        }
        let outcome = Arc::new(
            self.history
                .outcome(end_reason, self.applied_command_sequence),
        );
        self.phase = if failed {
            TaskPhase::Failed {
                outcome: Arc::clone(&outcome),
            }
        } else {
            TaskPhase::Completed {
                outcome: Arc::clone(&outcome),
            }
        };
        self.replace_status();
        self.publish_event(if failed {
            ManagedEvent::Failed {
                outcome: Arc::clone(&outcome),
            }
        } else {
            ManagedEvent::Completed {
                outcome: Arc::clone(&outcome),
            }
        });
        if let Some(resources) = self.resources.as_mut() {
            resources.events.take();
        }
        self.wake = None;
        self.timer = None;
        self.resources.take();
        Poll::Ready((*outcome).clone())
    }

    fn fail_driver(&mut self, failure: ManagedDriverFailure) -> Poll<ManagedOutcome> {
        self.seal(ManagedEndReason::DriverFailed(failure), true)
    }
}

impl Future for ManagedClientTask {
    type Output = ManagedOutcome;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().get_mut();
        if this.phase.is_terminal() {
            panic!("ManagedClientTask polled after completion");
        }

        if matches!(this.phase, TaskPhase::NotStarted) {
            if this.resources().stop.is_requested() {
                this.observe_stop();
            } else if let Err(failure) = this.begin_running() {
                return this.fail_driver(failure);
            }
        }
        if this.resources().stop.is_requested() {
            this.observe_stop();
        }

        let mut immediate = this.process_commands(cx);
        if this.resources().stop.is_requested() {
            this.observe_stop();
        }
        let now = Instant::now();
        immediate |= this.poll_timeout_pass(now);
        immediate |= this.poll_target_pass(cx, now);
        this.prune_undesired_terminal();
        if this.phase.is_running() {
            immediate |= match this.config.pacing {
                ManagedPacing::Staggered => this.poll_staggered_send(cx, now),
                ManagedPacing::Burst => this.poll_burst_sends(cx, now),
            };
        }

        if this.resources().stop.is_requested() {
            this.observe_stop();
        }

        if this.all_targets_terminal() {
            match this.phase {
                TaskPhase::Stopping => {
                    return this.seal(
                        if this.stop_observed {
                            ManagedEndReason::StopRequested
                        } else {
                            ManagedEndReason::TargetsComplete
                        },
                        false,
                    );
                }
                TaskPhase::Running
                    if this.config.completion == ManagedCompletionPolicy::FinishWhenQuiescent =>
                {
                    this.phase = TaskPhase::Stopping;
                    this.resources().stop.begin_stopping();
                    this.replace_status();
                    this.publish_event(ManagedEvent::Stopping);
                    return this.seal(ManagedEndReason::TargetsComplete, false);
                }
                _ => {}
            }
        }

        immediate |= this.register_stop_wake(cx);
        if !immediate {
            immediate |= this.register_timer(cx);
        }
        if immediate {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }
}

impl Drop for ManagedClientTask {
    fn drop(&mut self) {
        if self.phase.is_terminal() || self.resources.is_none() {
            return;
        }
        self.phase = TaskPhase::Abandoned;
        self.resources().stop.close_updates();
        self.commands.close();
        self.replace_status();
        self.publish_event(ManagedEvent::Abandoned);
        if let Some(resources) = self.resources.as_mut() {
            resources.events.take();
        }
        self.resources.take();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        async_client::AsyncClient,
        managed::{schedule::ProbeSchedule, ManagedTargetConfig},
        ClientConfig,
    };
    use std::time::Duration;

    // Real EAGAIN needs privileged queue shaping. Inspect only the owning
    // deadline selection here, using a production socket without sending.
    #[test]
    fn finite_lifetime_deadline_survives_send_waiting_and_stagger_gating() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let start = Instant::now();
            let end = start + Duration::from_millis(500);
            let accepted = crate::AcceptedSessionParameters {
                duration: Some(Duration::from_millis(500)),
                interval: Duration::from_millis(1),
                length: 0,
                received_stats: irtt_proto::ReceivedStats::None,
                stamp_at: irtt_proto::StampAt::None,
                clock: irtt_proto::Clock::Both,
                dscp: 0,
                server_fill: None,
            };
            for pacing in [ManagedPacing::Burst, ManagedPacing::Staggered] {
                let (mut task, _handle) = ManagedClient::task(
                    ManagedClientConfig {
                        pacing,
                        ..ManagedClientConfig::default()
                    },
                    vec![ManagedTargetConfig::new("target", "127.0.0.1:9")],
                )
                .unwrap();
                let client = AsyncClient::connect("127.0.0.1:9", ClientConfig::default())
                    .await
                    .unwrap();
                task.phase = TaskPhase::Running;
                task.install_target_phase(
                    0,
                    TargetPhase::Active {
                        client,
                        schedule: ProbeSchedule::new(start, &accepted).unwrap(),
                        send_waiting: false,
                    },
                );
                task.send_gate = Some(end + Duration::from_secs(1));

                for send_waiting in [false, true] {
                    let TargetPhase::Active {
                        schedule,
                        send_waiting: waiting,
                        ..
                    } = task.targets[0].phase_mut()
                    else {
                        unreachable!()
                    };
                    *waiting = send_waiting;
                    *schedule = ProbeSchedule::new(start, &accepted).unwrap();
                    let expected = if pacing == ManagedPacing::Burst && !send_waiting {
                        start
                    } else {
                        end
                    };
                    assert_eq!(task.next_deadline(), Some(expected));

                    let TargetPhase::Active { schedule, .. } = task.targets[0].phase_mut() else {
                        unreachable!()
                    };
                    schedule.permit_probe_at(end);
                    assert_eq!(task.next_deadline(), None);
                }

                let mut continuous = accepted.clone();
                continuous.duration = None;
                let TargetPhase::Active { schedule, .. } = task.targets[0].phase_mut() else {
                    unreachable!()
                };
                *schedule = ProbeSchedule::new(start, &continuous).unwrap();
                assert_eq!(task.next_deadline(), None);
            }
        });
    }
}
