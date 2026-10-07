//! Atomic desired-target transactions, generation retirement, and pruning.

use std::{
    collections::HashSet,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

use crate::{
    managed::{
        ManagedCommandAcknowledgement, ManagedCommandApplyError, ManagedCompletionPolicy,
        ManagedEvent, ManagedTargetConfig, ManagedTargetLifecycle, TargetInstance,
    },
    ClientConfig,
};

use super::{
    control::{validate_target_config, ManagedCommand, UpdateAdmission},
    target::{RetirementReason, TargetCounters, TargetMembership, TargetPhase, TargetRuntime},
    ManagedClientTask, TaskPhase,
};

const COMMAND_WORK_BUDGET: usize = 32;

struct PlannedTarget {
    target: ManagedTargetConfig,
    client_config: ClientConfig,
    generation: u64,
}

struct PlannedRetirement {
    index: usize,
    reason: RetirementReason,
    synchronous: bool,
}

struct UpdatePlan {
    retirements: Vec<PlannedRetirement>,
    created: Vec<PlannedTarget>,
    next_generation: u64,
    next_command_sequence: u64,
    prospective_live_count: usize,
}

fn synchronously_retireable(phase: &TargetPhase) -> bool {
    match phase {
        TargetPhase::Pending { .. }
        | TargetPhase::Connecting { .. }
        | TargetPhase::Terminal { .. } => true,
        TargetPhase::Opening { open, .. } => !open.has_in_flight_work(),
        TargetPhase::Active { .. } | TargetPhase::Draining { .. } | TargetPhase::Closing { .. } => {
            false
        }
    }
}

fn runtime_satisfies_target(runtime: &TargetRuntime, target: &ManagedTargetConfig) -> bool {
    runtime.membership.is_desired()
        && !matches!(runtime.phase(), TargetPhase::Terminal { .. })
        && runtime.config == *target
}

impl ManagedClientTask {
    pub(super) fn process_commands(&mut self, cx: &mut Context<'_>) -> bool {
        let mut immediate = false;
        for _ in 0..COMMAND_WORK_BUDGET {
            match self.commands.poll_recv(cx) {
                Poll::Ready(Some(ManagedCommand::UpdateTargets {
                    targets,
                    acknowledgement,
                })) => {
                    // This check is the driver-side application linearization point.
                    // `apply_targets` cannot suspend, so a stop observed here wins over
                    // this command; once it passes, this command may complete atomically.
                    let result = if !self.phase.is_running()
                        || self.resources().stop.update_admission() != UpdateAdmission::Open
                    {
                        Err(ManagedCommandApplyError::Stopping)
                    } else {
                        self.apply_targets(targets)
                    };
                    let _ = acknowledgement.send(result);
                    immediate = true;
                }
                Poll::Ready(None) | Poll::Pending => break,
            }
        }
        immediate
    }

    fn apply_targets(
        &mut self,
        incoming: Vec<ManagedTargetConfig>,
    ) -> Result<ManagedCommandAcknowledgement, ManagedCommandApplyError> {
        let plan = self.plan_targets(incoming)?;
        debug_assert!(plan.prospective_live_count <= self.config.max_live_target_generations);

        let now = Instant::now();
        let mut synchronous = HashSet::with_capacity(plan.retirements.len());
        let mut finished = Vec::new();
        for retirement in &plan.retirements {
            let target = &mut self.targets[retirement.index];
            let was_paced_active = target.is_paced_active();
            target.membership = TargetMembership::Withdrawn(retirement.reason);
            if retirement.synchronous {
                synchronous.insert(retirement.index);
                if !matches!(target.phase(), TargetPhase::Terminal { .. }) {
                    let outcome = Arc::new(target.outcome(retirement.reason.into(), None));
                    self.history.record(outcome.as_ref().clone());
                    finished.push(ManagedEvent::TargetFinished { outcome });
                }
            } else if was_paced_active {
                // Removal is directional: it may discard an elapsed gate but never
                // lengthens an existing future stagger gate.
                self.stagger_target_removed(now);
            }
        }
        if !synchronous.is_empty() {
            let mut index = 0;
            self.targets.retain(|_| {
                let keep = !synchronous.contains(&index);
                index += 1;
                keep
            });
            self.rebase_target_cursors();
        }

        let mut created_instances = Vec::with_capacity(plan.created.len());
        for planned in plan.created {
            let instance = TargetInstance {
                id: planned.target.id.clone(),
                generation: planned.generation,
            };
            created_instances.push(instance.clone());
            self.targets.push(TargetRuntime {
                instance,
                server_addr: Arc::from(planned.target.server_addr.clone()),
                config: planned.target,
                membership: TargetMembership::Desired,
                remote: None,
                counters: TargetCounters::default(),
                phase: Some(TargetPhase::Pending {
                    client_config: planned.client_config,
                }),
            });
        }
        self.next_generation = plan.next_generation;
        self.applied_command_sequence = plan.next_command_sequence;
        let stopping = self
            .targets
            .iter()
            .all(|runtime| !runtime.membership.is_desired())
            && self.config.completion == ManagedCompletionPolicy::FinishWhenQuiescent;
        if stopping {
            self.resources().stop.begin_stopping();
            self.phase = TaskPhase::Stopping;
        }

        // A transaction has one externally visible durable point: all runtime and
        // history changes above, then this exact status snapshot, then its events and
        // acknowledgement.  Do not use per-transition publishing helpers here.
        let status = self.snapshot();
        self.resources().status.send_replace(Arc::clone(&status));
        if stopping {
            self.publish_event(ManagedEvent::Stopping);
        }
        for event in finished {
            self.publish_event(event);
        }
        for target in created_instances {
            self.publish_event(ManagedEvent::TargetStateChanged {
                target,
                lifecycle: ManagedTargetLifecycle::Pending,
            });
        }
        Ok(ManagedCommandAcknowledgement {
            sequence: self.applied_command_sequence,
            status,
        })
    }

    fn plan_targets(
        &self,
        incoming: Vec<ManagedTargetConfig>,
    ) -> Result<UpdatePlan, ManagedCommandApplyError> {
        let mut ids = HashSet::with_capacity(incoming.len());
        let mut prepared = Vec::with_capacity(incoming.len());
        for target in incoming {
            if !ids.insert(target.id.clone()) {
                return Err(ManagedCommandApplyError::DuplicateTargetId { id: target.id });
            }
            let mut client_config = self.config.client.clone();
            client_config.address_family = target
                .address_family
                .unwrap_or(self.config.client.address_family);
            client_config.auth = target.auth.resolve(&self.config.client.auth);
            validate_target_config(&client_config).map_err(|source| {
                ManagedCommandApplyError::InvalidTarget {
                    id: target.id.clone(),
                    source,
                }
            })?;
            prepared.push((target, client_config));
        }
        let mut retirements = Vec::new();
        for (index, runtime) in self.targets.iter().enumerate() {
            if !runtime.membership.is_desired()
                || prepared
                    .iter()
                    .any(|(target, _)| runtime_satisfies_target(runtime, target))
            {
                continue;
            }
            let reason = if prepared
                .iter()
                .any(|(target, _)| target.id == runtime.instance.id)
            {
                RetirementReason::Replaced
            } else {
                RetirementReason::Removed
            };
            retirements.push(PlannedRetirement {
                index,
                reason,
                synchronous: synchronously_retireable(runtime.phase()),
            });
        }

        let mut next_generation = self.next_generation;
        let mut created = Vec::new();
        for (target, client_config) in prepared {
            if self
                .targets
                .iter()
                .any(|runtime| runtime_satisfies_target(runtime, &target))
            {
                continue;
            }
            let generation = next_generation;
            next_generation = next_generation
                .checked_add(1)
                .ok_or(ManagedCommandApplyError::GenerationExhausted)?;
            created.push(PlannedTarget {
                target,
                client_config,
                generation,
            });
        }
        let next_command_sequence = self
            .applied_command_sequence
            .checked_add(1)
            .ok_or(ManagedCommandApplyError::CommandSequenceExhausted)?;
        let synchronous = retirements
            .iter()
            .filter(|retirement| retirement.synchronous)
            .map(|retirement| retirement.index)
            .collect::<HashSet<_>>();
        let prospective_live_count = self
            .targets
            .iter()
            .enumerate()
            .filter(|(index, _)| !synchronous.contains(index))
            .count()
            .saturating_add(created.len());
        if prospective_live_count > self.config.max_live_target_generations {
            return Err(ManagedCommandApplyError::LiveGenerationLimitExceeded {
                required: prospective_live_count,
                limit: self.config.max_live_target_generations,
            });
        }
        Ok(UpdatePlan {
            retirements,
            created,
            next_generation,
            next_command_sequence,
            prospective_live_count,
        })
    }

    pub(super) fn prune_undesired_terminal(&mut self) {
        if !self.targets.iter().any(|target| {
            !target.membership.is_desired()
                && matches!(target.phase(), TargetPhase::Terminal { .. })
        }) {
            return;
        }
        self.targets.retain(|target| {
            target.membership.is_desired()
                || !matches!(target.phase(), TargetPhase::Terminal { .. })
        });
        self.rebase_target_cursors();
        self.replace_status();
    }

    fn rebase_target_cursors(&mut self) {
        let len = self.targets.len();
        if len == 0 {
            self.cursor = 0;
            self.timeout_cursor = 0;
            self.send_cursor = 0;
        } else {
            self.cursor %= len;
            self.timeout_cursor %= len;
            self.send_cursor %= len;
        }
        self.scan_remaining = self.scan_remaining.min(len);
        self.burst_remaining = self.burst_remaining.min(len);
        self.stagger_remaining = self.stagger_remaining.min(len);
    }
}
