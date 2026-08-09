use std::{
    collections::{HashSet, VecDeque},
    fmt,
    future::Future,
    mem,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use tokio::{
    sync::{broadcast, mpsc, oneshot, watch},
    time::{self, Sleep},
};

use crate::{
    async_client::{AsyncClient, AsyncOpenState},
    session::machine::SessionMachine,
    socket::validate_open_timeouts,
    socket_options::validate_ttl,
    ClientConfig, ClientError, ClientEvent, OpenOutcome,
};

use super::{
    classify_client_error, ManagedClientConfig, ManagedCommandAcknowledgement,
    ManagedCommandApplyError, ManagedCommandError, ManagedCompletionPolicy, ManagedConfigError,
    ManagedDriverFailure, ManagedEndReason, ManagedEvent, ManagedEventSubscription,
    ManagedLifecycle, ManagedOutcome, ManagedPacing, ManagedStatus, ManagedSubscribeError,
    ManagedTargetConfig, ManagedTargetEndReason, ManagedTargetFailure, ManagedTargetFailureKind,
    ManagedTargetFailurePhase, ManagedTargetLifecycle, ManagedTargetOutcome, ManagedTargetStatus,
    TargetInstance,
};

const TARGET_WORK_BUDGET: usize = 128;
const COMMAND_WORK_BUDGET: usize = 32;
const POST_DEADLINE_RECEIVE_BUDGET: usize = TARGET_WORK_BUDGET;

/// Entry point for constructing a unified Tokio managed task.
#[derive(Debug, Default)]
pub struct ManagedClient;

impl ManagedClient {
    /// Validate configuration and construct a runtime-independent task/handle pair.
    pub fn task(
        config: ManagedClientConfig,
        targets: Vec<ManagedTargetConfig>,
    ) -> Result<(ManagedClientTask, ManagedClientHandle), ManagedConfigError> {
        build_task(config, targets)
    }
}

#[derive(Debug)]
struct StopSignal {
    requested: AtomicBool,
    updates_accepted: AtomicBool,
    wake: watch::Sender<()>,
    acknowledged: AtomicBool,
    acknowledgement: watch::Sender<()>,
}

impl StopSignal {
    fn request(&self) {
        self.updates_accepted.store(false, Ordering::Release);
        if !self.requested.swap(true, Ordering::AcqRel) {
            self.wake.send_replace(());
        }
    }

    fn updates_accepted(&self) -> bool {
        self.updates_accepted.load(Ordering::Acquire)
    }
    fn close_updates(&self) {
        self.updates_accepted.store(false, Ordering::Release);
    }

    fn is_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }

    fn acknowledge(&self) {
        if !self.acknowledged.swap(true, Ordering::AcqRel) {
            self.acknowledgement.send_replace(());
        }
    }
}

/// Cloneable control and observation capability for a managed task.
#[derive(Clone)]
pub struct ManagedClientHandle {
    stop: Arc<StopSignal>,
    commands: mpsc::Sender<ManagedCommand>,
    status: watch::Receiver<Arc<ManagedStatus>>,
    events: broadcast::WeakSender<ManagedEvent>,
}

impl fmt::Debug for ManagedClientHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedClientHandle")
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}

impl ManagedClientHandle {
    /// Return the latest durable immutable status snapshot.
    pub fn status(&self) -> Arc<ManagedStatus> {
        Arc::clone(&self.status.borrow())
    }

    /// Subscribe to future lossy presentation events.
    pub fn subscribe(&self) -> Result<ManagedEventSubscription, ManagedSubscribeError> {
        self.events
            .upgrade()
            .map(|sender| sender.subscribe())
            .ok_or(ManagedSubscribeError::Closed)
    }

    /// Request idempotent graceful stop and return a durable-status receipt.
    pub fn stop(&self) -> ManagedStopReceipt {
        self.stop.request();
        ManagedStopReceipt::new(Arc::clone(&self.stop))
    }

    /// Submit one complete desired target set without awaiting queue capacity.
    pub fn update_targets(
        &self,
        targets: Vec<ManagedTargetConfig>,
    ) -> Result<ManagedCommandReceipt, ManagedCommandError> {
        if !self.stop.updates_accepted() {
            return Err(ManagedCommandError::Stopping);
        }
        let (acknowledgement, receiver) = oneshot::channel();
        match self.commands.try_send(ManagedCommand::UpdateTargets {
            targets,
            acknowledgement,
        }) {
            Ok(()) => Ok(ManagedCommandReceipt { receiver }),
            Err(mpsc::error::TrySendError::Full(_)) => Err(ManagedCommandError::QueueFull),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(ManagedCommandError::DriverClosed),
        }
    }
}

/// Receipt for a target-set transaction accepted by the driver queue.
#[must_use = "await the receipt to observe target-update application"]
pub struct ManagedCommandReceipt {
    receiver: oneshot::Receiver<Result<ManagedCommandAcknowledgement, ManagedCommandApplyError>>,
}

impl Future for ManagedCommandReceipt {
    type Output = Result<ManagedCommandAcknowledgement, ManagedCommandApplyError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(_)) => {
                Poll::Ready(Err(ManagedCommandApplyError::AcknowledgementDisconnected))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

enum ManagedCommand {
    UpdateTargets {
        targets: Vec<ManagedTargetConfig>,
        acknowledgement:
            oneshot::Sender<Result<ManagedCommandAcknowledgement, ManagedCommandApplyError>>,
    },
}

/// Receipt resolving once stop is durably observed or the task is terminal.
#[must_use = "await the receipt to observe durable stop acknowledgement"]
pub struct ManagedStopReceipt {
    future: Pin<Box<dyn Future<Output = ()> + Send>>,
}

impl ManagedStopReceipt {
    fn new(stop: Arc<StopSignal>) -> Self {
        let mut acknowledgement = stop.acknowledgement.subscribe();
        let future = Box::pin(async move {
            while !stop.acknowledged.load(Ordering::Acquire) {
                let _ = acknowledgement.changed().await;
            }
        });
        Self { future }
    }
}

impl Future for ManagedStopReceipt {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.future.as_mut().poll(cx)
    }
}

type ConnectFuture =
    Pin<Box<dyn Future<Output = Result<AsyncClient, ClientError>> + Send + 'static>>;
type WakeFuture = Pin<Box<dyn Future<Output = Option<watch::Receiver<()>>> + Send + 'static>>;
#[cfg(test)]
type EventObservations = Arc<std::sync::Mutex<Vec<(ManagedEvent, Arc<ManagedStatus>)>>>;
#[cfg(test)]
type StaggerObservations = Arc<std::sync::Mutex<Vec<(usize, Duration)>>>;

fn arm_wake(mut receiver: watch::Receiver<()>) -> WakeFuture {
    Box::pin(async move {
        receiver.changed().await.ok()?;
        Some(receiver)
    })
}

enum TargetState {
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
    Terminal,
}

impl TargetState {
    fn lifecycle(&self) -> ManagedTargetLifecycle {
        match self {
            Self::Pending { .. } => ManagedTargetLifecycle::Pending,
            Self::Connecting { .. } => ManagedTargetLifecycle::Connecting,
            Self::Opening { .. } => ManagedTargetLifecycle::Opening,
            Self::Active { .. } => ManagedTargetLifecycle::Active,
            Self::Draining { .. } => ManagedTargetLifecycle::Draining,
            Self::Closing { .. } => ManagedTargetLifecycle::Closing,
            Self::Terminal => ManagedTargetLifecycle::Terminal,
        }
    }
}

#[derive(Default)]
struct TargetCounters {
    packets_sent: u64,
    replies_received: u64,
    duplicates: u64,
    late: u64,
    warning_events: u64,
}

struct TargetRuntime {
    instance: TargetInstance,
    config: ManagedTargetConfig,
    desired: bool,
    retirement: Option<ManagedTargetEndReason>,
    server_addr: Arc<str>,
    remote: Option<std::net::SocketAddr>,
    counters: TargetCounters,
    send_waiting: bool,
    active: bool,
    state: TargetState,
}

struct PlannedTarget {
    target: ManagedTargetConfig,
    client_config: ClientConfig,
    generation: u64,
}

struct PlannedRetirement {
    index: usize,
    reason: ManagedTargetEndReason,
    synchronous: bool,
}

struct UpdatePlan {
    retirements: Vec<PlannedRetirement>,
    created: Vec<PlannedTarget>,
    next_generation: u64,
    next_command_sequence: u64,
    prospective_live_count: usize,
}

fn synchronously_retireable(state: &TargetState) -> bool {
    match state {
        TargetState::Pending { .. } | TargetState::Connecting { .. } | TargetState::Terminal => {
            true
        }
        TargetState::Opening { open, .. } => !open.has_in_flight_work(),
        TargetState::Active { .. } | TargetState::Draining { .. } | TargetState::Closing { .. } => {
            false
        }
    }
}

fn target_outcome(
    target: &TargetRuntime,
    end_reason: ManagedTargetEndReason,
    cleanup_failure: Option<ManagedTargetFailure>,
) -> ManagedTargetOutcome {
    ManagedTargetOutcome {
        target: target.instance.clone(),
        server_addr: Arc::clone(&target.server_addr),
        remote: target.remote,
        end_reason,
        packets_sent: target.counters.packets_sent,
        replies_received: target.counters.replies_received,
        duplicates: target.counters.duplicates,
        late: target.counters.late,
        warning_events: target.counters.warning_events,
        cleanup_failure,
    }
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

#[derive(Clone, Copy, Eq, PartialEq)]
enum DriverState {
    NotStarted,
    Running,
    Stopping,
    Finished,
}

#[derive(Clone, Copy)]
enum OpenSessionFailureCleanup {
    Drain,
    Close,
}

/// The sole authoritative zero-or-many target driver.
#[must_use = "ManagedClientTask must be awaited or deliberately dropped"]
pub struct ManagedClientTask {
    state: DriverState,
    lifecycle: ManagedLifecycle,
    config: ManagedClientConfig,
    targets: Vec<TargetRuntime>,
    commands: mpsc::Receiver<ManagedCommand>,
    history: OutcomeHistory,
    resources: Option<TaskResources>,
    wake: Option<WakeFuture>,
    timer: Option<Pin<Box<Sleep>>>,
    cursor: usize,
    scan_remaining: usize,
    send_cursor: usize,
    burst_remaining: usize,
    stagger_remaining: usize,
    send_gate: Option<Instant>,
    last_stagger_send: Option<Instant>,
    stop_observed: bool,
    final_outcome: Option<Arc<ManagedOutcome>>,
    next_generation: u64,
    applied_command_sequence: u64,
    #[cfg(test)]
    event_observations: Option<EventObservations>,
    #[cfg(test)]
    stagger_observations: Option<StaggerObservations>,
    #[cfg(test)]
    drain_test_hook: DrainTestHook,
}

#[cfg(test)]
#[derive(Default)]
struct DrainTestHook {
    defer_work_until_deadline: bool,
    fail_receive: bool,
    fail_close: bool,
}

impl ManagedClientTask {
    fn resources(&self) -> &TaskResources {
        self.resources
            .as_ref()
            .expect("managed task resources exist before terminal sealing")
    }

    fn publish_event(&self, event: ManagedEvent) {
        #[cfg(test)]
        if let Some(observations) = &self.event_observations {
            observations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((event.clone(), Arc::clone(&self.resources().status.borrow())));
        }
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
                let lifecycle = target.state.lifecycle();
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
                    desired: target.desired,
                    lifecycle,
                    server_addr: Arc::clone(&target.server_addr),
                    remote: target.remote,
                }
            })
            .collect::<Vec<_>>();
        Arc::new(ManagedStatus {
            lifecycle: self.lifecycle,
            stop_requested: self.stop_observed,
            applied_command_sequence: self.applied_command_sequence,
            desired_target_count: self.targets.iter().filter(|target| target.desired).count(),
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
            final_outcome: self.final_outcome.clone(),
        })
    }

    fn replace_status(&self) {
        self.resources().status.send_replace(self.snapshot());
    }

    fn install_target_state(&mut self, index: usize, state: TargetState) {
        let lifecycle = state.lifecycle();
        let active = matches!(state, TargetState::Active { .. });
        let was_active = self.targets[index].active;
        self.targets[index].active = active;
        self.targets[index].state = state;
        let now = Instant::now();
        match (was_active, active) {
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

    fn publish_client_events(&mut self, index: usize, events: Vec<ClientEvent>) {
        for event in events {
            match event {
                ClientEvent::EchoReply { .. } => {
                    self.targets[index].counters.replies_received = self.targets[index]
                        .counters
                        .replies_received
                        .saturating_add(1);
                }
                ClientEvent::DuplicateReply { .. } => {
                    self.targets[index].counters.duplicates =
                        self.targets[index].counters.duplicates.saturating_add(1);
                }
                ClientEvent::LateReply { .. } => {
                    self.targets[index].counters.late =
                        self.targets[index].counters.late.saturating_add(1);
                }
                ClientEvent::Warning { .. } => {
                    self.targets[index].counters.warning_events = self.targets[index]
                        .counters
                        .warning_events
                        .saturating_add(1);
                }
                _ => {}
            }
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
        self.install_target_state(index, TargetState::Terminal);
        let target = &self.targets[index];
        let outcome = ManagedTargetOutcome {
            target: target.instance.clone(),
            server_addr: Arc::clone(&target.server_addr),
            remote: target.remote,
            end_reason,
            packets_sent: target.counters.packets_sent,
            replies_received: target.counters.replies_received,
            duplicates: target.counters.duplicates,
            late: target.counters.late,
            warning_events: target.counters.warning_events,
            cleanup_failure,
        };
        self.history.record(outcome.clone());
        self.replace_status();
        self.publish_event(ManagedEvent::TargetFinished {
            outcome: Arc::new(outcome),
        });
    }

    fn fail_target(&mut self, index: usize, phase: ManagedTargetFailurePhase, error: ClientError) {
        let failure = classify_client_error(phase, &error);
        self.finish_target(index, ManagedTargetEndReason::Failed(failure), None);
    }

    fn begin_open_session_failure(
        &mut self,
        index: usize,
        mut client: AsyncClient,
        phase: ManagedTargetFailurePhase,
        error: ClientError,
        now: Instant,
        cleanup: OpenSessionFailureCleanup,
    ) -> bool {
        let primary_end = ManagedTargetEndReason::Failed(classify_client_error(phase, &error));
        client.discard_prepared_probe();
        self.targets[index].counters.packets_sent = client.packets_sent();
        match cleanup {
            OpenSessionFailureCleanup::Drain => self.begin_drain(index, client, primary_end, now),
            OpenSessionFailureCleanup::Close => {
                self.begin_close(index, client, primary_end, None, now)
            }
        }
    }

    fn begin_running(&mut self) -> Result<(), ManagedDriverFailure> {
        tokio::runtime::Handle::try_current().map_err(|_| ManagedDriverFailure::NoTokioRuntime)?;
        self.state = DriverState::Running;
        self.lifecycle = ManagedLifecycle::Running;
        self.replace_status();
        self.publish_event(ManagedEvent::Started);
        Ok(())
    }

    fn observe_stop(&mut self) {
        if self.stop_observed {
            return;
        }
        self.stop_observed = true;
        self.resources().stop.close_updates();
        self.state = DriverState::Stopping;
        self.lifecycle = ManagedLifecycle::Stopping;
        self.replace_status();
        self.publish_event(ManagedEvent::Stopping);
        self.resources().stop.acknowledge();
        self.scan_remaining = self.targets.len();
        self.burst_remaining = 0;
    }

    fn effective_retirement(&self, index: usize) -> Option<ManagedTargetEndReason> {
        self.targets[index].retirement.clone()
    }

    fn process_commands(&mut self, cx: &mut Context<'_>) -> bool {
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
                    let result = if self.state != DriverState::Running
                        || self.resources().stop.is_requested()
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
            target.desired = false;
            if retirement.synchronous {
                synchronous.insert(retirement.index);
                if !matches!(target.state, TargetState::Terminal) {
                    let outcome = target_outcome(target, retirement.reason.clone(), None);
                    self.history.record(outcome.clone());
                    finished.push(ManagedEvent::TargetFinished {
                        outcome: Arc::new(outcome),
                    });
                }
            } else {
                target.retirement = Some(retirement.reason.clone());
                if target.active {
                    target.active = false;
                    // Removal is directional: it may discard an elapsed gate but never
                    // lengthens an existing future stagger gate.
                    self.stagger_target_removed(now);
                }
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
                desired: true,
                retirement: None,
                remote: None,
                counters: TargetCounters::default(),
                send_waiting: false,
                active: false,
                state: TargetState::Pending {
                    client_config: planned.client_config,
                },
            });
        }
        self.next_generation = plan.next_generation;
        self.applied_command_sequence = plan.next_command_sequence;
        let stopping = self.targets.iter().all(|runtime| !runtime.desired)
            && self.config.completion == ManagedCompletionPolicy::FinishWhenQuiescent;
        if stopping {
            self.resources().stop.close_updates();
            self.state = DriverState::Stopping;
            self.lifecycle = ManagedLifecycle::Stopping;
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
            client_config.server_addr.clone_from(&target.server_addr);
            if let Some(auth) = &target.auth {
                client_config.hmac_key.clone_from(&auth.hmac_key);
            }
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
            if !runtime.desired || prepared.iter().any(|(target, _)| target == &runtime.config) {
                continue;
            }
            let reason = if prepared
                .iter()
                .any(|(target, _)| target.id == runtime.instance.id)
            {
                ManagedTargetEndReason::Replaced
            } else {
                ManagedTargetEndReason::Removed
            };
            retirements.push(PlannedRetirement {
                index,
                reason,
                synchronous: synchronously_retireable(&runtime.state),
            });
        }

        let mut next_generation = self.next_generation;
        let mut created = Vec::new();
        for (target, client_config) in prepared {
            if self
                .targets
                .iter()
                .any(|runtime| runtime.desired && runtime.config == target)
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

    fn prune_undesired_terminal(&mut self) {
        if !self
            .targets
            .iter()
            .any(|target| !target.desired && matches!(target.state, TargetState::Terminal))
        {
            return;
        }
        self.targets
            .retain(|target| target.desired || !matches!(target.state, TargetState::Terminal));
        self.rebase_target_cursors();
        self.replace_status();
    }

    fn rebase_target_cursors(&mut self) {
        let len = self.targets.len();
        if len == 0 {
            self.cursor = 0;
            self.send_cursor = 0;
        } else {
            self.cursor %= len;
            self.send_cursor %= len;
        }
        self.scan_remaining = self.scan_remaining.min(len);
        self.burst_remaining = self.burst_remaining.min(len);
        self.stagger_remaining = self.stagger_remaining.min(len);
    }

    fn start_connecting(&mut self, index: usize, config: ClientConfig) {
        let future = Box::pin(AsyncClient::connect(config));
        self.install_target_state(index, TargetState::Connecting { future });
    }

    fn poll_target(&mut self, index: usize, cx: &mut Context<'_>, now: Instant) -> bool {
        let state = mem::replace(&mut self.targets[index].state, TargetState::Terminal);
        match state {
            TargetState::Pending { client_config } => {
                if self.state == DriverState::Stopping || self.effective_retirement(index).is_some()
                {
                    self.finish_target(
                        index,
                        self.effective_retirement(index)
                            .unwrap_or(ManagedTargetEndReason::Stopped),
                        None,
                    );
                    false
                } else {
                    self.start_connecting(index, client_config);
                    true
                }
            }
            TargetState::Connecting { mut future } => {
                if self.state == DriverState::Stopping || self.effective_retirement(index).is_some()
                {
                    self.finish_target(
                        index,
                        self.effective_retirement(index)
                            .unwrap_or(ManagedTargetEndReason::Stopped),
                        None,
                    );
                    return false;
                }
                match future.as_mut().poll(cx) {
                    Poll::Pending => {
                        self.targets[index].state = TargetState::Connecting { future };
                        false
                    }
                    Poll::Ready(Ok(client)) => {
                        self.targets[index].remote = Some(client.remote_addr());
                        self.install_target_state(
                            index,
                            TargetState::Opening {
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
            TargetState::Opening {
                mut client,
                mut open,
            } => {
                if self.state == DriverState::Stopping || self.effective_retirement(index).is_some()
                {
                    let retirement = self
                        .effective_retirement(index)
                        .unwrap_or(ManagedTargetEndReason::Stopped);
                    if !open.has_in_flight_work() {
                        self.targets[index].counters.packets_sent = client.packets_sent();
                        self.finish_target(index, retirement, None);
                        return false;
                    }
                    open.request_stop_after_current_attempt();
                }
                match client.poll_open(&mut open, cx) {
                    Poll::Pending => {
                        self.targets[index].state = TargetState::Opening { client, open };
                        false
                    }
                    Poll::Ready(Ok(OpenOutcome::Started { event, .. })) => {
                        self.publish_client_events(index, vec![event]);
                        if self.state == DriverState::Stopping
                            || self.effective_retirement(index).is_some()
                        {
                            self.targets[index].counters.packets_sent = client.packets_sent();
                            self.begin_drain(
                                index,
                                client,
                                self.effective_retirement(index)
                                    .unwrap_or(ManagedTargetEndReason::Stopped),
                                now,
                            )
                        } else {
                            self.install_target_state(index, TargetState::Active { client });
                            true
                        }
                    }
                    Poll::Ready(Ok(OpenOutcome::NoTestCompleted { event, .. })) => {
                        self.publish_client_events(index, vec![event]);
                        let end_reason = if self.state == DriverState::Stopping
                            || self.effective_retirement(index).is_some()
                        {
                            self.effective_retirement(index)
                                .unwrap_or(ManagedTargetEndReason::Stopped)
                        } else {
                            ManagedTargetEndReason::NoTestComplete
                        };
                        self.finish_target(index, end_reason, None);
                        false
                    }
                    Poll::Ready(Err(error)) => {
                        if self.state == DriverState::Stopping
                            || self.effective_retirement(index).is_some()
                        {
                            let cleanup_failure =
                                (!open.stopped_after_current_attempt()).then(|| {
                                    classify_client_error(
                                        ManagedTargetFailurePhase::Opening,
                                        &error,
                                    )
                                });
                            self.finish_target(
                                index,
                                self.effective_retirement(index)
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
            TargetState::Active { mut client } => {
                if self.state == DriverState::Stopping || self.effective_retirement(index).is_some()
                {
                    client.discard_prepared_probe();
                    self.targets[index].counters.packets_sent = client.packets_sent();
                    return self.begin_drain(
                        index,
                        client,
                        self.effective_retirement(index)
                            .unwrap_or(ManagedTargetEndReason::Stopped),
                        now,
                    );
                }
                if client
                    .next_probe_timeout_deadline()
                    .is_some_and(|deadline| deadline <= now)
                {
                    match client.poll_timeouts_at(now) {
                        Ok(events) => self.publish_client_events(index, events),
                        Err(error) => {
                            return self.begin_open_session_failure(
                                index,
                                client,
                                ManagedTargetFailurePhase::Timing,
                                error,
                                now,
                                OpenSessionFailureCleanup::Close,
                            );
                        }
                    }
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
                            client,
                            ManagedTargetFailurePhase::Receiving,
                            error,
                            now,
                            OpenSessionFailureCleanup::Close,
                        );
                    }
                };
                if client.is_peer_closed() {
                    self.targets[index].counters.packets_sent = client.packets_sent();
                    self.finish_target(index, ManagedTargetEndReason::PeerClosed, None);
                    return false;
                }
                if client.is_run_complete() {
                    self.targets[index].counters.packets_sent = client.packets_sent();
                    return self.begin_drain(
                        index,
                        client,
                        ManagedTargetEndReason::TestComplete,
                        now,
                    );
                }
                self.targets[index].state = TargetState::Active { client };
                received
            }
            TargetState::Draining {
                mut client,
                drain_started_at,
                deadline,
                primary_end,
                mut cleanup_failure,
                mut post_deadline_receives_remaining,
            } => {
                #[cfg(test)]
                let defer_work = self.drain_test_hook.defer_work_until_deadline && now < deadline;
                #[cfg(not(test))]
                let defer_work = false;
                if defer_work {
                    self.targets[index].state = TargetState::Draining {
                        client,
                        drain_started_at,
                        deadline,
                        primary_end,
                        cleanup_failure,
                        post_deadline_receives_remaining,
                    };
                    return false;
                }

                #[cfg(test)]
                let injected_receive_failure = self.take_drain_receive_failure();
                #[cfg(not(test))]
                let injected_receive_failure = None;
                let mut retained_state_changed = false;
                if client
                    .next_probe_timeout_deadline()
                    .is_some_and(|timeout| timeout <= now)
                {
                    match client.poll_timeouts_at(now) {
                        Ok(events) => {
                            self.publish_client_events(index, events);
                            retained_state_changed = true;
                        }
                        Err(error) => {
                            self.targets[index].counters.packets_sent = client.packets_sent();
                            cleanup_failure.get_or_insert_with(|| {
                                classify_client_error(ManagedTargetFailurePhase::Timing, &error)
                            });
                            return self.begin_close(
                                index,
                                client,
                                primary_end,
                                cleanup_failure,
                                now,
                            );
                        }
                    }
                }
                let received = match client.poll_recv(cx) {
                    Poll::Pending => false,
                    Poll::Ready(Ok(events)) => {
                        self.publish_client_events(index, events);
                        retained_state_changed = true;
                        true
                    }
                    Poll::Ready(Err(error)) => {
                        self.targets[index].counters.packets_sent = client.packets_sent();
                        cleanup_failure.get_or_insert_with(|| {
                            classify_client_error(ManagedTargetFailurePhase::Receiving, &error)
                        });
                        return self.begin_close(index, client, primary_end, cleanup_failure, now);
                    }
                };
                if let Some(error) = injected_receive_failure {
                    self.targets[index].counters.packets_sent = client.packets_sent();
                    cleanup_failure.get_or_insert_with(|| {
                        classify_client_error(ManagedTargetFailurePhase::Receiving, &error)
                    });
                    return self.begin_close(index, client, primary_end, cleanup_failure, now);
                }
                if client.is_peer_closed() {
                    self.targets[index].counters.packets_sent = client.packets_sent();
                    self.finish_target(index, ManagedTargetEndReason::PeerClosed, cleanup_failure);
                    return false;
                }
                let deadline = if retained_state_changed {
                    match self.drain_deadline(&client, drain_started_at) {
                        Some(candidate) => deadline.min(candidate),
                        None => {
                            cleanup_failure.get_or_insert_with(duration_overflow_failure);
                            return self.begin_close(
                                index,
                                client,
                                primary_end,
                                cleanup_failure,
                                now,
                            );
                        }
                    }
                } else {
                    deadline
                };
                if now >= deadline {
                    if received && post_deadline_receives_remaining > 1 {
                        post_deadline_receives_remaining -= 1;
                        self.targets[index].state = TargetState::Draining {
                            client,
                            drain_started_at,
                            deadline,
                            primary_end,
                            cleanup_failure,
                            post_deadline_receives_remaining,
                        };
                        true
                    } else {
                        self.begin_close(index, client, primary_end, cleanup_failure, now)
                    }
                } else {
                    self.targets[index].state = TargetState::Draining {
                        client,
                        drain_started_at,
                        deadline,
                        primary_end,
                        cleanup_failure,
                        post_deadline_receives_remaining,
                    };
                    received
                }
            }
            TargetState::Closing {
                mut client,
                deadline,
                primary_end,
                cleanup_failure,
            } => match client.poll_close(cx) {
                Poll::Pending => {
                    if now >= deadline {
                        self.targets[index].counters.packets_sent = client.packets_sent();
                        self.finish_target(
                            index,
                            primary_end,
                            cleanup_failure.or_else(|| Some(close_timeout_failure())),
                        );
                        false
                    } else {
                        self.targets[index].state = TargetState::Closing {
                            client,
                            deadline,
                            primary_end,
                            cleanup_failure,
                        };
                        false
                    }
                }
                Poll::Ready(Ok(events)) => {
                    self.targets[index].counters.packets_sent = client.packets_sent();
                    self.publish_client_events(index, events);
                    #[cfg(test)]
                    let injected_close_failure = self.take_drain_close_failure().map(|error| {
                        classify_client_error(ManagedTargetFailurePhase::Closing, &error)
                    });
                    #[cfg(not(test))]
                    let injected_close_failure = None;
                    self.finish_target(
                        index,
                        primary_end,
                        cleanup_failure.or(injected_close_failure),
                    );
                    false
                }
                Poll::Ready(Err(error)) => {
                    self.targets[index].counters.packets_sent = client.packets_sent();
                    let cleanup = classify_client_error(ManagedTargetFailurePhase::Closing, &error);
                    self.finish_target(index, primary_end, cleanup_failure.or(Some(cleanup)));
                    false
                }
            },
            TargetState::Terminal => {
                self.targets[index].state = TargetState::Terminal;
                false
            }
        }
    }

    fn begin_drain(
        &mut self,
        index: usize,
        client: AsyncClient,
        primary_end: ManagedTargetEndReason,
        _now: Instant,
    ) -> bool {
        let drain_started_at = Instant::now();
        let Some(deadline) = self.drain_deadline(&client, drain_started_at) else {
            let cleanup_failure = Some(duration_overflow_failure());
            return self.begin_close(
                index,
                client,
                primary_end,
                cleanup_failure,
                drain_started_at,
            );
        };
        self.install_target_state(
            index,
            TargetState::Draining {
                client,
                drain_started_at,
                deadline,
                primary_end,
                cleanup_failure: None,
                post_deadline_receives_remaining: POST_DEADLINE_RECEIVE_BUDGET,
            },
        );
        true
    }

    fn drain_deadline(&self, client: &AsyncClient, drain_started_at: Instant) -> Option<Instant> {
        client
            .latest_probe_timeout_deadline()
            .unwrap_or(drain_started_at)
            .max(drain_started_at)
            .checked_add(self.config.final_drain)
    }

    fn begin_close(
        &mut self,
        index: usize,
        client: AsyncClient,
        primary_end: ManagedTargetEndReason,
        mut cleanup_failure: Option<ManagedTargetFailure>,
        now: Instant,
    ) -> bool {
        let deadline = now.checked_add(self.config.final_drain).unwrap_or_else(|| {
            cleanup_failure.get_or_insert_with(duration_overflow_failure);
            now
        });
        self.install_target_state(
            index,
            TargetState::Closing {
                client,
                deadline,
                primary_end,
                cleanup_failure,
            },
        );
        true
    }

    #[cfg(test)]
    fn take_drain_receive_failure(&mut self) -> Option<ClientError> {
        if !mem::take(&mut self.drain_test_hook.fail_receive) {
            return None;
        }
        Some(ClientError::Socket(std::io::Error::other(
            "injected drain receive failure",
        )))
    }

    #[cfg(test)]
    fn take_drain_close_failure(&mut self) -> Option<ClientError> {
        if !mem::take(&mut self.drain_test_hook.fail_close) {
            return None;
        }
        Some(ClientError::Socket(std::io::Error::other(
            "injected drain close failure",
        )))
    }

    fn active_count(&self) -> usize {
        self.targets
            .iter()
            .filter(|target| target.active && target.desired && target.retirement.is_none())
            .count()
    }

    fn active_stagger_spacing(&self) -> Option<(usize, Duration)> {
        let mut active = 0;
        let mut minimum: Option<Duration> = None;
        for target in &self.targets {
            if !target.desired || target.retirement.is_some() {
                continue;
            }
            let TargetState::Active { client } = &target.state else {
                continue;
            };
            active += 1;
            let interval = client
                .probe_interval()
                .expect("active managed targets have a committed probe schedule");
            minimum = minimum.into_iter().chain(Some(interval)).min();
        }
        minimum.map(|interval| (active, stagger_spacing(interval, active)))
    }

    fn stagger_target_added(&mut self, now: Instant) {
        let Some(existing) = self.send_gate.filter(|gate| *gate > now) else {
            self.send_gate = None;
            return;
        };
        let candidate = self
            .last_stagger_send
            .zip(self.active_stagger_spacing())
            .and_then(|(last, (_, spacing))| last.checked_add(spacing))
            .filter(|gate| *gate > now);
        self.send_gate = candidate.map(|candidate| existing.min(candidate));
    }

    fn stagger_target_removed(&mut self, now: Instant) {
        self.send_gate = self.send_gate.filter(|gate| *gate > now);
    }

    fn record_stagger_acceptance(
        &mut self,
        result: SendResult,
        stagger_spacing: Option<(usize, Duration)>,
        accepted_at: Instant,
    ) {
        if !result.accepted() {
            return;
        }
        let Some((_active, spacing)) = stagger_spacing else {
            return;
        };
        self.last_stagger_send = Some(accepted_at);
        self.send_gate = accepted_at.checked_add(spacing);
        #[cfg(test)]
        if let Some(observations) = &self.stagger_observations {
            observations.lock().unwrap().push((_active, spacing));
        }
    }

    fn poll_one_send(
        &mut self,
        index: usize,
        cx: &mut Context<'_>,
        now: Instant,
        stagger_spacing: Option<(usize, Duration)>,
    ) -> SendResult {
        if !self.targets[index].desired || self.targets[index].retirement.is_some() {
            self.targets[index].send_waiting = false;
            return SendResult::NotAttempted;
        }
        let state = mem::replace(&mut self.targets[index].state, TargetState::Terminal);
        let TargetState::Active { mut client } = state else {
            self.targets[index].state = state;
            return SendResult::NotAttempted;
        };
        if client
            .next_send_deadline()
            .is_none_or(|deadline| deadline > now)
        {
            self.targets[index].send_waiting = false;
            self.targets[index].state = TargetState::Active { client };
            return SendResult::NotAttempted;
        }
        let before = client.packets_sent();
        let result = client.poll_send_probe(cx);
        let after = client.packets_sent();
        let accepted = after > before;
        let send_result = match &result {
            Poll::Pending => SendResult::Pending,
            Poll::Ready(Ok(_)) => SendResult::Ready { accepted },
            Poll::Ready(Err(_)) => SendResult::Failed { accepted },
        };
        self.record_stagger_acceptance(send_result, stagger_spacing, Instant::now());
        let schedule_error = accepted
            .then(|| client.skip_missed_probe_slots_at(Instant::now()))
            .transpose()
            .err();
        self.targets[index].counters.packets_sent = after;
        match result {
            Poll::Pending => {
                self.targets[index].send_waiting = true;
                self.targets[index].state = TargetState::Active { client };
                SendResult::Pending
            }
            Poll::Ready(Ok(events)) => {
                self.targets[index].send_waiting = false;
                self.targets[index].state = TargetState::Active { client };
                self.publish_client_events(index, events);
                if let Some(error) = schedule_error {
                    let TargetState::Active { client } =
                        mem::replace(&mut self.targets[index].state, TargetState::Terminal)
                    else {
                        unreachable!("probe sender remained active while publishing send events");
                    };
                    self.begin_open_session_failure(
                        index,
                        client,
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
                self.targets[index].send_waiting = false;
                self.begin_open_session_failure(
                    index,
                    client,
                    ManagedTargetFailurePhase::Sending,
                    error,
                    now,
                    OpenSessionFailureCleanup::Drain,
                );
                SendResult::Failed { accepted }
            }
        }
    }

    fn poll_staggered_send(&mut self, cx: &mut Context<'_>, now: Instant) -> bool {
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
            if !self.targets[index].desired
                || self.targets[index].retirement.is_some()
                || !matches!(self.targets[index].state, TargetState::Active { .. })
            {
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

    fn poll_burst_sends(&mut self, cx: &mut Context<'_>, now: Instant) -> bool {
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
            .all(|target| matches!(target.state, TargetState::Terminal))
    }

    fn next_deadline(&self) -> Option<Instant> {
        let mut non_send_deadline = None;
        let mut send_deadline = None;
        for target in &self.targets {
            match &target.state {
                TargetState::Active { client } => {
                    non_send_deadline = non_send_deadline
                        .into_iter()
                        .chain(client.next_probe_timeout_deadline())
                        .min();
                    if target.desired && target.retirement.is_none() && !target.send_waiting {
                        send_deadline = send_deadline
                            .into_iter()
                            .chain(client.next_send_deadline())
                            .min();
                    }
                }
                TargetState::Draining {
                    client, deadline, ..
                } => {
                    #[cfg(test)]
                    let defer_work = self.drain_test_hook.defer_work_until_deadline;
                    #[cfg(not(test))]
                    let defer_work = false;
                    non_send_deadline = non_send_deadline
                        .into_iter()
                        .chain(
                            (!defer_work)
                                .then(|| client.next_probe_timeout_deadline())
                                .flatten(),
                        )
                        .chain(Some(*deadline))
                        .min();
                }
                TargetState::Closing { deadline, .. } => {
                    non_send_deadline = non_send_deadline.into_iter().chain(Some(*deadline)).min();
                }
                _ => {}
            }
        }
        if self.state == DriverState::Running
            && self.config.pacing == ManagedPacing::Staggered
            && self.active_count() > 0
        {
            send_deadline = match (send_deadline, self.send_gate) {
                (Some(target), Some(gate)) => Some(target.max(gate)),
                (None, gate) => gate,
                (target, None) => target,
            };
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

    fn seal(&mut self, end_reason: ManagedEndReason, failed: bool) -> Poll<ManagedOutcome> {
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
        self.final_outcome = Some(Arc::clone(&outcome));
        self.lifecycle = if failed {
            ManagedLifecycle::Failed
        } else {
            ManagedLifecycle::Completed
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
        self.resources().stop.acknowledge();
        if let Some(resources) = self.resources.as_mut() {
            resources.events.take();
        }
        self.state = DriverState::Finished;
        self.wake = None;
        self.timer = None;
        self.resources.take();
        Poll::Ready((*outcome).clone())
    }

    fn fail_driver(&mut self, failure: ManagedDriverFailure) -> Poll<ManagedOutcome> {
        self.seal(ManagedEndReason::DriverFailed(failure), true)
    }
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

impl Future for ManagedClientTask {
    type Output = ManagedOutcome;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().get_mut();
        if this.state == DriverState::Finished {
            panic!("ManagedClientTask polled after completion");
        }

        if this.state == DriverState::NotStarted {
            if this.resources().stop.is_requested() {
                this.observe_stop();
            } else if let Err(failure) = this.begin_running() {
                return this.fail_driver(failure);
            }
        }
        if this.state == DriverState::Running && this.resources().stop.is_requested() {
            this.observe_stop();
        }

        let mut immediate = this.process_commands(cx);
        let now = Instant::now();
        immediate |= this.poll_target_pass(cx, now);
        this.prune_undesired_terminal();
        if this.state == DriverState::Running {
            immediate |= match this.config.pacing {
                ManagedPacing::Staggered => this.poll_staggered_send(cx, now),
                ManagedPacing::Burst => this.poll_burst_sends(cx, now),
            };
        }

        if this.all_targets_terminal() {
            match this.state {
                DriverState::Stopping => {
                    return this.seal(
                        if this.stop_observed {
                            ManagedEndReason::StopRequested
                        } else {
                            ManagedEndReason::TargetsComplete
                        },
                        false,
                    );
                }
                DriverState::Running
                    if this.config.completion == ManagedCompletionPolicy::FinishWhenQuiescent =>
                {
                    this.state = DriverState::Stopping;
                    this.lifecycle = ManagedLifecycle::Stopping;
                    this.resources().stop.close_updates();
                    this.replace_status();
                    this.publish_event(ManagedEvent::Stopping);
                    return this.seal(ManagedEndReason::TargetsComplete, false);
                }
                _ => {}
            }
        }

        immediate |= this.register_stop_wake(cx);
        immediate |= this.register_timer(cx);
        if immediate {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }
}

impl Drop for ManagedClientTask {
    fn drop(&mut self) {
        if self.state == DriverState::Finished || self.resources.is_none() {
            return;
        }
        self.lifecycle = ManagedLifecycle::Abandoned;
        self.final_outcome = None;
        self.replace_status();
        self.publish_event(ManagedEvent::Abandoned);
        self.resources().stop.acknowledge();
        if let Some(resources) = self.resources.as_mut() {
            resources.events.take();
        }
        self.resources.take();
    }
}

fn build_task(
    config: ManagedClientConfig,
    targets: Vec<ManagedTargetConfig>,
) -> Result<(ManagedClientTask, ManagedClientHandle), ManagedConfigError> {
    if config.event_capacity == 0 {
        return Err(ManagedConfigError::ZeroEventCapacity);
    }
    if config.command_capacity == 0 {
        return Err(ManagedConfigError::ZeroCommandCapacity);
    }
    if config.max_live_target_generations == 0 {
        return Err(ManagedConfigError::ZeroLiveGenerationLimit);
    }
    if Instant::now().checked_add(config.final_drain).is_none() {
        return Err(ManagedConfigError::UnschedulableFinalDrain {
            duration: config.final_drain,
        });
    }
    if targets.len() > config.max_live_target_generations {
        return Err(ManagedConfigError::TooManyTargets {
            configured: targets.len(),
            limit: config.max_live_target_generations,
        });
    }
    if targets.is_empty() && config.completion == ManagedCompletionPolicy::FinishWhenQuiescent {
        return Err(ManagedConfigError::EmptyInitialTargets);
    }

    let mut ids = HashSet::with_capacity(targets.len());
    let mut runtimes = Vec::with_capacity(targets.len());
    let mut next_generation = 1_u64;
    for target in targets {
        if !ids.insert(target.id.clone()) {
            return Err(ManagedConfigError::DuplicateTargetId { id: target.id });
        }
        let generation = next_generation;
        next_generation = next_generation
            .checked_add(1)
            .ok_or(ManagedConfigError::GenerationExhausted)?;
        let mut client_config = config.client.clone();
        client_config.server_addr.clone_from(&target.server_addr);
        if let Some(auth) = &target.auth {
            client_config.hmac_key.clone_from(&auth.hmac_key);
        }
        validate_target_config(&client_config).map_err(|source| {
            ManagedConfigError::InvalidTarget {
                id: target.id.clone(),
                source,
            }
        })?;
        runtimes.push(TargetRuntime {
            instance: TargetInstance {
                id: target.id.clone(),
                generation,
            },
            config: target.clone(),
            desired: true,
            retirement: None,
            server_addr: Arc::from(target.server_addr),
            remote: None,
            counters: TargetCounters::default(),
            send_waiting: false,
            active: false,
            state: TargetState::Pending { client_config },
        });
    }

    let (event_sender, _) = broadcast::channel(config.event_capacity);
    let weak_events = event_sender.downgrade();
    let (wake_sender, wake_receiver) = watch::channel(());
    let (acknowledgement_sender, _) = watch::channel(());
    let stop = Arc::new(StopSignal {
        requested: AtomicBool::new(false),
        updates_accepted: AtomicBool::new(true),
        wake: wake_sender,
        acknowledged: AtomicBool::new(false),
        acknowledgement: acknowledgement_sender,
    });
    let history = OutcomeHistory::new(config.outcome_history_limit);
    let initial_targets = runtimes
        .iter()
        .map(|target| ManagedTargetStatus {
            target: target.instance.clone(),
            desired: true,
            lifecycle: ManagedTargetLifecycle::Pending,
            server_addr: Arc::clone(&target.server_addr),
            remote: None,
        })
        .collect::<Vec<_>>();
    let initial = Arc::new(ManagedStatus {
        lifecycle: ManagedLifecycle::NotStarted,
        stop_requested: false,
        applied_command_sequence: 0,
        desired_target_count: runtimes.len(),
        connecting_target_count: 0,
        opening_target_count: 0,
        active_target_count: 0,
        draining_target_count: 0,
        closing_target_count: 0,
        terminal_target_count: 0,
        total_target_outcomes: 0,
        successful_target_outcomes: 0,
        failed_target_outcomes: 0,
        peer_closed_target_outcomes: 0,
        discarded_target_outcomes: 0,
        targets: Arc::from(initial_targets.into_boxed_slice()),
        recent_target_outcomes: history.recent(),
        final_outcome: None,
    });
    let (status_sender, status_receiver) = watch::channel(initial);
    let (command_sender, command_receiver) = mpsc::channel(config.command_capacity);
    let resources = TaskResources {
        status: status_sender,
        events: Some(event_sender),
        stop: Arc::clone(&stop),
    };
    let task = ManagedClientTask {
        state: DriverState::NotStarted,
        lifecycle: ManagedLifecycle::NotStarted,
        config,
        targets: runtimes,
        commands: command_receiver,
        history,
        resources: Some(resources),
        wake: Some(arm_wake(wake_receiver)),
        timer: None,
        cursor: 0,
        scan_remaining: 0,
        send_cursor: 0,
        burst_remaining: 0,
        stagger_remaining: 0,
        send_gate: None,
        last_stagger_send: None,
        stop_observed: false,
        final_outcome: None,
        next_generation,
        applied_command_sequence: 0,
        #[cfg(test)]
        event_observations: None,
        #[cfg(test)]
        stagger_observations: None,
        #[cfg(test)]
        drain_test_hook: DrainTestHook::default(),
    };
    let handle = ManagedClientHandle {
        stop,
        commands: command_sender,
        status: status_receiver,
        events: weak_events,
    };
    Ok((task, handle))
}

fn validate_target_config(config: &ClientConfig) -> Result<(), ClientError> {
    validate_open_timeouts(&config.open_timeouts)?;
    SessionMachine::validate_config(config)?;
    if config.socket_config.ipv4_only && config.socket_config.ipv6_only {
        return Err(ClientError::InvalidConfig {
            reason: "ipv4_only and ipv6_only cannot both be true".to_owned(),
        });
    }
    if let Some(ttl) = config.socket_config.ttl {
        validate_ttl(ttl)?;
    }
    Ok(())
}

fn stagger_spacing(interval: Duration, active_targets: usize) -> Duration {
    let divisor = u128::try_from(active_targets.max(1)).unwrap_or(u128::MAX);
    let nanos = (interval.as_nanos() / divisor).max(1);
    let seconds = u64::try_from(nanos / 1_000_000_000).unwrap_or(u64::MAX);
    let subsec = u32::try_from(nanos % 1_000_000_000).unwrap_or(999_999_999);
    Duration::new(seconds, subsec)
}

fn duration_overflow_failure() -> ManagedTargetFailure {
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

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
