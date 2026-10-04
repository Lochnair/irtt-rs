//! Target-generation state, phase transitions, accounting, and completion.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

use crate::{
    async_client::{AsyncClient, AsyncOpenState},
    managed::schedule::ProbeSchedule,
    managed::{
        classify_client_error, ManagedEvent, ManagedTargetConfig, ManagedTargetEndReason,
        ManagedTargetFailure, ManagedTargetFailureKind, ManagedTargetFailurePhase,
        ManagedTargetLifecycle, ManagedTargetOutcome, TargetInstance,
    },
    ClientConfig, ClientError, ClientEvent, OpenOutcome,
};

use super::{ManagedClientTask, TARGET_WORK_BUDGET};

const POST_DEADLINE_RECEIVE_BUDGET: usize = TARGET_WORK_BUDGET;

type ConnectFuture =
    Pin<Box<dyn Future<Output = Result<AsyncClient, ClientError>> + Send + 'static>>;

pub(super) enum TargetPhase {
    Pending {
        client_config: ClientConfig,
    },
    Connecting {
        future: ConnectFuture,
    },
    Opening {
        client: AsyncClient,
        open: Box<AsyncOpenState>,
    },
    Active {
        client: AsyncClient,
        schedule: ProbeSchedule,
        send_waiting: bool,
    },
    Draining {
        client: AsyncClient,
        drain_started_at: Instant,
        deadline: Instant,
        primary_end: ManagedTargetEndReason,
        cleanup_failure: Option<ManagedTargetFailure>,
        post_deadline_receives_remaining: usize,
    },
    Closing {
        client: AsyncClient,
        deadline: Instant,
        primary_end: ManagedTargetEndReason,
        cleanup_failure: Option<ManagedTargetFailure>,
    },
    /// Terminal state always owns its finalized authoritative outcome.
    Terminal {
        outcome: Arc<ManagedTargetOutcome>,
    },
}

impl TargetPhase {
    pub(super) fn lifecycle(&self) -> ManagedTargetLifecycle {
        match self {
            Self::Pending { .. } => ManagedTargetLifecycle::Pending,
            Self::Connecting { .. } => ManagedTargetLifecycle::Connecting,
            Self::Opening { .. } => ManagedTargetLifecycle::Opening,
            Self::Active { .. } => ManagedTargetLifecycle::Active,
            Self::Draining { .. } => ManagedTargetLifecycle::Draining,
            Self::Closing { .. } => ManagedTargetLifecycle::Closing,
            Self::Terminal { .. } => ManagedTargetLifecycle::Terminal,
        }
    }

    pub(super) fn client(&self) -> &AsyncClient {
        match self {
            Self::Opening { client, .. }
            | Self::Active { client, .. }
            | Self::Draining { client, .. }
            | Self::Closing { client, .. } => client,
            _ => panic!("managed target has no client in this phase"),
        }
    }

    fn into_client(self) -> AsyncClient {
        match self {
            Self::Opening { client, .. }
            | Self::Active { client, .. }
            | Self::Draining { client, .. }
            | Self::Closing { client, .. } => client,
            _ => panic!("managed target has no client in this phase"),
        }
    }
}

#[derive(Default)]
pub(super) struct TargetCounters {
    packets_sent: u64,
    /// Unique echo replies that arrived in order and before their probe timed
    /// out.
    ///
    /// A reply that arrives behind an already-received sequence, or after its
    /// own probe timed out, is counted by `late` and deliberately not here.
    /// These counters are durable, unlike the lossy managed presentation
    /// events.
    replies_received: u64,
    duplicates: u64,
    late: u64,
    warning_events: u64,
}

impl TargetCounters {
    /// Account for one client event.
    ///
    /// Events that carry no durable target-local count are ignored. Probe
    /// sends are not counted here: `packets_sent` is authoritative in the
    /// underlying client and is synchronized through
    /// [`TargetRuntime::sync_packets_sent`].
    fn observe(&mut self, event: &ClientEvent) {
        match event {
            ClientEvent::EchoReply { .. } => {
                self.replies_received = self.replies_received.saturating_add(1);
            }
            ClientEvent::DuplicateReply { .. } => {
                self.duplicates = self.duplicates.saturating_add(1);
            }
            ClientEvent::LateReply { .. } => {
                self.late = self.late.saturating_add(1);
            }
            ClientEvent::Warning { .. } => {
                self.warning_events = self.warning_events.saturating_add(1);
            }
            ClientEvent::EchoSent { .. }
            | ClientEvent::EchoLoss { .. }
            | ClientEvent::SessionStarted(_)
            | ClientEvent::NoTestCompleted(_)
            | ClientEvent::SessionClosed { .. } => {}
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum RetirementReason {
    Removed,
    Replaced,
}

impl From<RetirementReason> for ManagedTargetEndReason {
    fn from(reason: RetirementReason) -> Self {
        match reason {
            RetirementReason::Removed => Self::Removed,
            RetirementReason::Replaced => Self::Replaced,
        }
    }
}

pub(super) enum TargetMembership {
    Desired,
    Withdrawn(RetirementReason),
}

impl TargetMembership {
    pub(super) fn is_desired(&self) -> bool {
        matches!(self, Self::Desired)
    }

    fn retirement_reason(&self) -> Option<RetirementReason> {
        match self {
            Self::Desired => None,
            Self::Withdrawn(reason) => Some(*reason),
        }
    }
}

pub(super) struct TargetRuntime {
    pub(super) instance: TargetInstance,
    pub(super) config: ManagedTargetConfig,
    pub(super) membership: TargetMembership,
    pub(super) server_addr: Arc<str>,
    pub(super) remote: Option<std::net::SocketAddr>,
    pub(super) counters: TargetCounters,
    // None only while a synchronous operation owns the phase locally.
    pub(super) phase: Option<TargetPhase>,
}

impl TargetRuntime {
    pub(super) fn phase(&self) -> &TargetPhase {
        self.phase
            .as_ref()
            .expect("managed target phase is extracted")
    }

    pub(super) fn phase_mut(&mut self) -> &mut TargetPhase {
        self.phase
            .as_mut()
            .expect("managed target phase is extracted")
    }

    pub(super) fn take_phase(&mut self) -> TargetPhase {
        self.phase
            .take()
            .expect("managed target phase is extracted")
    }

    pub(super) fn restore_phase(&mut self, phase: TargetPhase) {
        assert!(
            self.phase.is_none(),
            "managed target phase is already installed"
        );
        self.phase = Some(phase);
    }

    pub(super) fn is_paced_active(&self) -> bool {
        let phase = self.phase();
        self.membership.is_desired() && matches!(phase, TargetPhase::Active { .. })
    }

    /// Adopt the underlying client's authoritative sent count.
    ///
    /// The client owns `packets_sent`; this runtime only mirrors it so a
    /// terminal outcome can report it after the client is gone.
    pub(super) fn sync_packets_sent(&mut self, client: &AsyncClient) {
        self.counters.packets_sent = client.packets_sent();
    }

    pub(super) fn sync_live_packets_sent(&mut self) {
        self.counters.packets_sent = self.phase().client().packets_sent();
    }

    /// Build this target's durable outcome.
    pub(super) fn outcome(
        &self,
        end_reason: ManagedTargetEndReason,
        cleanup_failure: Option<ManagedTargetFailure>,
    ) -> ManagedTargetOutcome {
        ManagedTargetOutcome {
            target: self.instance.clone(),
            server_addr: Arc::clone(&self.server_addr),
            remote: self.remote,
            end_reason,
            packets_sent: self.counters.packets_sent,
            replies_received: self.counters.replies_received,
            duplicates: self.counters.duplicates,
            late: self.counters.late,
            warning_events: self.counters.warning_events,
            cleanup_failure,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum OpenSessionFailureCleanup {
    Drain,
    Close,
}

impl ManagedClientTask {
    pub(super) fn install_target_phase(&mut self, index: usize, phase: TargetPhase) {
        let previous = self.targets[index].phase().lifecycle();
        self.targets[index].phase = Some(phase);
        self.publish_target_phase(index, previous);
    }

    fn publish_target_phase(&mut self, index: usize, previous: ManagedTargetLifecycle) {
        let lifecycle = self.targets[index].phase().lifecycle();
        let desired = self.targets[index].membership.is_desired();
        let was_paced_active = desired && previous == ManagedTargetLifecycle::Active;
        let paced_active = self.targets[index].is_paced_active();
        let now = Instant::now();
        match (was_paced_active, paced_active) {
            (false, true) => self.stagger_target_added(now),
            (true, false) => self.stagger_target_removed(now),
            _ => {}
        }
        self.replace_status();
        self.publish_event(ManagedEvent::TargetStateChanged {
            target: self.targets[index].instance.clone(),
            lifecycle,
        });
    }

    pub(super) fn publish_client_events(&mut self, index: usize, events: Vec<ClientEvent>) {
        for event in events {
            self.targets[index].counters.observe(&event);
            self.publish_event(ManagedEvent::Client {
                target: self.targets[index].instance.clone(),
                event,
            });
        }
    }

    fn finish_target(
        &mut self,
        index: usize,
        end_reason: ManagedTargetEndReason,
        cleanup_failure: Option<ManagedTargetFailure>,
    ) {
        let outcome = Arc::new(self.targets[index].outcome(end_reason, cleanup_failure));
        // Finalize history before the phase publisher installs Terminal, updates
        // pacing and publishes one complete status followed by the lifecycle event.
        self.history.record(outcome.as_ref().clone());
        self.install_target_phase(
            index,
            TargetPhase::Terminal {
                outcome: Arc::clone(&outcome),
            },
        );
        self.publish_event(ManagedEvent::TargetFinished { outcome });
    }

    fn fail_target(&mut self, index: usize, phase: ManagedTargetFailurePhase, error: ClientError) {
        let failure = classify_client_error(phase, &error);
        self.finish_target(index, ManagedTargetEndReason::Failed(failure), None);
    }

    pub(super) fn begin_open_session_failure(
        &mut self,
        index: usize,
        phase: ManagedTargetFailurePhase,
        error: ClientError,
        now: Instant,
        cleanup: OpenSessionFailureCleanup,
    ) -> bool {
        let primary_end = ManagedTargetEndReason::Failed(classify_client_error(phase, &error));
        let (TargetPhase::Opening { client, .. } | TargetPhase::Active { client, .. }) =
            self.targets[index].phase_mut()
        else {
            unreachable!("open-session failure requires an opening or active target")
        };
        client.discard_prepared_probe();
        self.targets[index].sync_live_packets_sent();
        match cleanup {
            OpenSessionFailureCleanup::Drain => self.begin_drain(index, primary_end, now),
            OpenSessionFailureCleanup::Close => self.begin_close(index, primary_end, None, now),
        }
    }

    pub(super) fn effective_retirement(&self, index: usize) -> Option<ManagedTargetEndReason> {
        self.targets[index]
            .membership
            .retirement_reason()
            .map(Into::into)
    }

    fn start_connecting(&mut self, index: usize) {
        let TargetPhase::Pending { client_config } = self.targets[index].take_phase() else {
            unreachable!("connecting starts from a pending target")
        };
        let endpoint = Arc::clone(&self.targets[index].server_addr);
        let future = Box::pin(async move { AsyncClient::connect(endpoint, client_config).await });
        self.targets[index].restore_phase(TargetPhase::Connecting { future });
        self.publish_target_phase(index, ManagedTargetLifecycle::Pending);
    }

    pub(super) fn poll_target(&mut self, index: usize, cx: &mut Context<'_>, now: Instant) -> bool {
        let retirement = self.effective_retirement(index);
        let stopping = self.phase.is_stopping() || retirement.is_some();
        match self.targets[index].phase_mut() {
            TargetPhase::Pending { .. } => {
                if stopping {
                    self.finish_target(
                        index,
                        retirement
                            .clone()
                            .unwrap_or(ManagedTargetEndReason::Stopped),
                        None,
                    );
                    false
                } else {
                    self.start_connecting(index);
                    true
                }
            }
            TargetPhase::Connecting { future } => {
                if stopping {
                    self.finish_target(
                        index,
                        retirement
                            .clone()
                            .unwrap_or(ManagedTargetEndReason::Stopped),
                        None,
                    );
                    return false;
                }
                match future.as_mut().poll(cx) {
                    Poll::Pending => false,
                    Poll::Ready(Ok(client)) => {
                        self.targets[index].remote = Some(client.remote_addr());
                        self.install_target_phase(
                            index,
                            TargetPhase::Opening {
                                client,
                                open: Box::new(AsyncOpenState::new()),
                            },
                        );
                        true
                    }
                    Poll::Ready(Err(error)) => {
                        self.fail_target(index, ManagedTargetFailurePhase::Connecting, error);
                        false
                    }
                }
            }
            TargetPhase::Opening { client, open } => {
                if stopping {
                    let retirement = retirement
                        .clone()
                        .unwrap_or(ManagedTargetEndReason::Stopped);
                    if !open.has_in_flight_work() {
                        self.targets[index].sync_live_packets_sent();
                        self.finish_target(index, retirement, None);
                        return false;
                    }
                    open.request_stop_after_current_attempt();
                }
                match client.poll_open(open, cx) {
                    Poll::Pending => false,
                    Poll::Ready(Ok(OpenOutcome::Started(started))) => {
                        let schedule = match ProbeSchedule::new(
                            started.at.mono,
                            &started.negotiation.accepted,
                        ) {
                            Ok(schedule) => schedule,
                            Err(error) => {
                                return self.begin_open_session_failure(
                                    index,
                                    ManagedTargetFailurePhase::Timing,
                                    error,
                                    now,
                                    OpenSessionFailureCleanup::Close,
                                )
                            }
                        };
                        self.publish_client_events(
                            index,
                            vec![ClientEvent::SessionStarted(started)],
                        );
                        if stopping {
                            self.targets[index].sync_live_packets_sent();
                            self.begin_drain(
                                index,
                                retirement
                                    .clone()
                                    .unwrap_or(ManagedTargetEndReason::Stopped),
                                now,
                            )
                        } else {
                            let previous = self.targets[index].take_phase();
                            self.targets[index].restore_phase(TargetPhase::Active {
                                client: previous.into_client(),
                                schedule,
                                send_waiting: false,
                            });
                            self.publish_target_phase(index, ManagedTargetLifecycle::Opening);
                            true
                        }
                    }
                    Poll::Ready(Ok(OpenOutcome::NoTestCompleted(completed))) => {
                        self.publish_client_events(
                            index,
                            vec![ClientEvent::NoTestCompleted(completed)],
                        );
                        let end_reason = if stopping {
                            retirement
                                .clone()
                                .unwrap_or(ManagedTargetEndReason::Stopped)
                        } else {
                            ManagedTargetEndReason::NoTestComplete
                        };
                        self.finish_target(index, end_reason, None);
                        false
                    }
                    Poll::Ready(Err(error)) => {
                        if stopping {
                            let TargetPhase::Opening { open, .. } = self.targets[index].phase()
                            else {
                                unreachable!()
                            };
                            let cleanup_failure =
                                (!open.stopped_after_current_attempt()).then(|| {
                                    classify_client_error(
                                        ManagedTargetFailurePhase::Opening,
                                        &error,
                                    )
                                });
                            self.finish_target(
                                index,
                                retirement
                                    .clone()
                                    .unwrap_or(ManagedTargetEndReason::Stopped),
                                cleanup_failure,
                            );
                        } else {
                            self.fail_target(index, ManagedTargetFailurePhase::Opening, error);
                        }
                        false
                    }
                }
            }
            TargetPhase::Active { client, .. } => {
                if stopping {
                    client.discard_prepared_probe();
                    self.targets[index].sync_live_packets_sent();
                    return self.begin_drain(
                        index,
                        retirement
                            .clone()
                            .unwrap_or(ManagedTargetEndReason::Stopped),
                        now,
                    );
                }
                if client
                    .next_probe_timeout_deadline()
                    .is_some_and(|deadline| deadline <= now)
                {
                    return true;
                }
                let received = match client.poll_recv(cx) {
                    Poll::Pending => false,
                    Poll::Ready(Ok(events)) => {
                        self.publish_client_events(index, events);
                        true
                    }
                    Poll::Ready(Err(error)) => {
                        return self.begin_open_session_failure(
                            index,
                            ManagedTargetFailurePhase::Receiving,
                            error,
                            now,
                            OpenSessionFailureCleanup::Close,
                        );
                    }
                };
                if self.targets[index].phase().client().is_peer_closed() {
                    self.targets[index].sync_live_packets_sent();
                    self.finish_target(index, ManagedTargetEndReason::PeerClosed, None);
                    return false;
                }
                let TargetPhase::Active {
                    client, schedule, ..
                } = self.targets[index].phase_mut()
                else {
                    unreachable!()
                };
                schedule.permit_probe_at(now);
                if schedule.is_finished() && !client.has_pending_probes() {
                    self.targets[index].sync_live_packets_sent();
                    return self.begin_drain(index, ManagedTargetEndReason::TestComplete, now);
                }
                received
            }
            TargetPhase::Draining { client, .. } => {
                if client
                    .next_probe_timeout_deadline()
                    .is_some_and(|timeout| timeout <= now)
                {
                    return true;
                }
                let mut retained_state_changed = false;
                let received = match client.poll_recv(cx) {
                    Poll::Pending => false,
                    Poll::Ready(Ok(events)) => {
                        self.publish_client_events(index, events);
                        retained_state_changed = true;
                        true
                    }
                    Poll::Ready(Err(error)) => {
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
                            classify_client_error(ManagedTargetFailurePhase::Receiving, &error)
                        });
                        let primary_end = primary_end.clone();
                        let cleanup_failure = cleanup_failure.clone();
                        return self.begin_close(index, primary_end, cleanup_failure, now);
                    }
                };
                if self.targets[index].phase().client().is_peer_closed() {
                    self.targets[index].sync_live_packets_sent();
                    let TargetPhase::Draining {
                        cleanup_failure, ..
                    } = self.targets[index].phase()
                    else {
                        unreachable!()
                    };
                    self.finish_target(
                        index,
                        ManagedTargetEndReason::PeerClosed,
                        cleanup_failure.clone(),
                    );
                    return false;
                }
                if retained_state_changed {
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
                            return self.begin_close(index, primary_end, cleanup_failure, now);
                        }
                    }
                }
                let TargetPhase::Draining {
                    deadline,
                    primary_end,
                    cleanup_failure,
                    post_deadline_receives_remaining,
                    ..
                } = self.targets[index].phase_mut()
                else {
                    unreachable!()
                };
                if now >= *deadline {
                    if received && *post_deadline_receives_remaining > 1 {
                        *post_deadline_receives_remaining -= 1;
                        true
                    } else {
                        let primary_end = primary_end.clone();
                        let cleanup_failure = cleanup_failure.clone();
                        self.begin_close(index, primary_end, cleanup_failure, now)
                    }
                } else {
                    received
                }
            }
            TargetPhase::Closing { client, .. } => {
                let result = client.poll_close(cx);
                let TargetPhase::Closing {
                    deadline,
                    primary_end,
                    cleanup_failure,
                    ..
                } = self.targets[index].phase()
                else {
                    unreachable!()
                };
                let deadline = *deadline;
                let primary_end = primary_end.clone();
                let cleanup_failure = cleanup_failure.clone();
                match result {
                    Poll::Pending => {
                        if now >= deadline {
                            self.targets[index].sync_live_packets_sent();
                            self.finish_target(
                                index,
                                primary_end,
                                cleanup_failure.or_else(|| Some(close_timeout_failure())),
                            );
                        }
                        false
                    }
                    Poll::Ready(Ok(events)) => {
                        self.targets[index].sync_live_packets_sent();
                        self.publish_client_events(index, events);
                        self.finish_target(index, primary_end, cleanup_failure);
                        false
                    }
                    Poll::Ready(Err(error)) => {
                        self.targets[index].sync_live_packets_sent();
                        let cleanup =
                            classify_client_error(ManagedTargetFailurePhase::Closing, &error);
                        self.finish_target(index, primary_end, cleanup_failure.or(Some(cleanup)));
                        false
                    }
                }
            }
            TargetPhase::Terminal { .. } => false,
        }
    }

    pub(super) fn begin_drain(
        &mut self,
        index: usize,
        primary_end: ManagedTargetEndReason,
        _now: Instant,
    ) -> bool {
        let drain_started_at = Instant::now();
        let Some(deadline) =
            self.drain_deadline(self.targets[index].phase().client(), drain_started_at)
        else {
            return self.begin_close(
                index,
                primary_end,
                Some(duration_overflow_failure()),
                drain_started_at,
            );
        };
        let previous = self.targets[index].take_phase();
        let lifecycle = previous.lifecycle();
        self.targets[index].restore_phase(TargetPhase::Draining {
            client: previous.into_client(),
            drain_started_at,
            deadline,
            primary_end,
            cleanup_failure: None,
            post_deadline_receives_remaining: POST_DEADLINE_RECEIVE_BUDGET,
        });
        self.publish_target_phase(index, lifecycle);
        true
    }

    pub(super) fn drain_deadline(
        &self,
        client: &AsyncClient,
        drain_started_at: Instant,
    ) -> Option<Instant> {
        client
            .latest_probe_timeout_deadline()
            .unwrap_or(drain_started_at)
            .max(drain_started_at)
            .checked_add(self.config.final_drain)
    }

    pub(super) fn begin_close(
        &mut self,
        index: usize,
        primary_end: ManagedTargetEndReason,
        mut cleanup_failure: Option<ManagedTargetFailure>,
        now: Instant,
    ) -> bool {
        let deadline = now.checked_add(self.config.final_drain).unwrap_or_else(|| {
            cleanup_failure.get_or_insert_with(duration_overflow_failure);
            now
        });
        let previous = self.targets[index].take_phase();
        let lifecycle = previous.lifecycle();
        self.targets[index].restore_phase(TargetPhase::Closing {
            client: previous.into_client(),
            deadline,
            primary_end,
            cleanup_failure,
        });
        self.publish_target_phase(index, lifecycle);
        true
    }
}

pub(super) fn duration_overflow_failure() -> ManagedTargetFailure {
    classify_client_error(
        ManagedTargetFailurePhase::Timing,
        &ClientError::DurationOverflow,
    )
}

fn close_timeout_failure() -> ManagedTargetFailure {
    ManagedTargetFailure {
        phase: ManagedTargetFailurePhase::Closing,
        kind: ManagedTargetFailureKind::Timeout,
        message: Arc::from("best-effort close did not become writable before its deadline"),
    }
}
