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
    sync::{broadcast, watch},
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
    classify_client_error, ManagedClientConfig, ManagedCompletionPolicy, ManagedConfigError,
    ManagedDriverFailure, ManagedEndReason, ManagedEvent, ManagedEventSubscription,
    ManagedLifecycle, ManagedOutcome, ManagedPacing, ManagedStatus, ManagedSubscribeError,
    ManagedTargetConfig, ManagedTargetEndReason, ManagedTargetFailure, ManagedTargetFailureKind,
    ManagedTargetFailurePhase, ManagedTargetLifecycle, ManagedTargetOutcome, ManagedTargetStatus,
    TargetInstance,
};

const TARGET_WORK_BUDGET: usize = 128;

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
    wake: watch::Sender<()>,
    acknowledged: AtomicBool,
    acknowledgement: watch::Sender<()>,
}

impl StopSignal {
    fn request(&self) {
        if !self.requested.swap(true, Ordering::AcqRel) {
            self.wake.send_replace(());
        }
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
        deadline: Instant,
        primary_end: ManagedTargetEndReason,
    },
    Closing {
        client: AsyncClient,
        deadline: Instant,
        primary_end: ManagedTargetEndReason,
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
    server_addr: Arc<str>,
    remote: Option<std::net::SocketAddr>,
    counters: TargetCounters,
    latest_committed_timeout: Option<Instant>,
    send_waiting: bool,
    state: TargetState,
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

    fn outcome(&self, end_reason: ManagedEndReason) -> ManagedOutcome {
        ManagedOutcome {
            end_reason,
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

/// The sole authoritative zero-or-many target driver.
#[must_use = "ManagedClientTask must be awaited or deliberately dropped"]
pub struct ManagedClientTask {
    state: DriverState,
    lifecycle: ManagedLifecycle,
    config: ManagedClientConfig,
    targets: Vec<TargetRuntime>,
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
    stop_observed: bool,
    final_outcome: Option<Arc<ManagedOutcome>>,
    _next_generation: u64,
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
                    lifecycle,
                    server_addr: Arc::clone(&target.server_addr),
                    remote: target.remote,
                }
            })
            .collect::<Vec<_>>();
        Arc::new(ManagedStatus {
            lifecycle: self.lifecycle,
            stop_requested: self.stop_observed,
            desired_target_count: self.targets.len(),
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
        self.targets[index].state = state;
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
        self.state = DriverState::Stopping;
        self.lifecycle = ManagedLifecycle::Stopping;
        self.replace_status();
        self.publish_event(ManagedEvent::Stopping);
        self.resources().stop.acknowledge();
        self.scan_remaining = self.targets.len();
        self.burst_remaining = 0;
    }

    fn start_connecting(&mut self, index: usize, config: ClientConfig) {
        let future = Box::pin(AsyncClient::connect(config));
        self.install_target_state(index, TargetState::Connecting { future });
    }

    fn poll_target(&mut self, index: usize, cx: &mut Context<'_>, now: Instant) -> bool {
        let state = mem::replace(&mut self.targets[index].state, TargetState::Terminal);
        match state {
            TargetState::Pending { client_config } => {
                if self.state == DriverState::Stopping {
                    self.finish_target(index, ManagedTargetEndReason::Stopped, None);
                    false
                } else {
                    self.start_connecting(index, client_config);
                    true
                }
            }
            TargetState::Connecting { mut future } => {
                if self.state == DriverState::Stopping {
                    self.finish_target(index, ManagedTargetEndReason::Stopped, None);
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
                if self.state == DriverState::Stopping {
                    self.targets[index].counters.packets_sent = client.packets_sent();
                    self.finish_target(index, ManagedTargetEndReason::Stopped, None);
                    return false;
                }
                match client.poll_open(&mut open, cx) {
                    Poll::Pending => {
                        self.targets[index].state = TargetState::Opening { client, open };
                        false
                    }
                    Poll::Ready(Ok(OpenOutcome::Started { event, .. })) => {
                        self.install_target_state(index, TargetState::Active { client });
                        self.publish_client_events(index, vec![event]);
                        true
                    }
                    Poll::Ready(Ok(OpenOutcome::NoTestCompleted { event, .. })) => {
                        self.publish_client_events(index, vec![event]);
                        self.finish_target(index, ManagedTargetEndReason::NoTestComplete, None);
                        false
                    }
                    Poll::Ready(Err(error)) => {
                        self.fail_target(index, ManagedTargetFailurePhase::Opening, error);
                        false
                    }
                }
            }
            TargetState::Active { mut client } => {
                if self.state == DriverState::Stopping {
                    client.discard_prepared_probe();
                    self.targets[index].counters.packets_sent = client.packets_sent();
                    return self.begin_drain(index, client, ManagedTargetEndReason::Stopped, now);
                }
                let received = match client.poll_recv(cx) {
                    Poll::Pending => false,
                    Poll::Ready(Ok(events)) => {
                        self.publish_client_events(index, events);
                        true
                    }
                    Poll::Ready(Err(error)) => {
                        self.targets[index].counters.packets_sent = client.packets_sent();
                        self.fail_target(index, ManagedTargetFailurePhase::Receiving, error);
                        return false;
                    }
                };
                if client.is_peer_closed() {
                    self.targets[index].counters.packets_sent = client.packets_sent();
                    self.finish_target(index, ManagedTargetEndReason::PeerClosed, None);
                    return false;
                }
                if client
                    .next_probe_timeout_deadline()
                    .is_some_and(|deadline| deadline <= now)
                {
                    match client.poll_timeouts_at(now) {
                        Ok(events) => self.publish_client_events(index, events),
                        Err(error) => {
                            self.targets[index].counters.packets_sent = client.packets_sent();
                            self.fail_target(index, ManagedTargetFailurePhase::Timing, error);
                            return false;
                        }
                    }
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
                deadline,
                primary_end,
            } => {
                let received = match client.poll_recv(cx) {
                    Poll::Pending => false,
                    Poll::Ready(Ok(events)) => {
                        self.publish_client_events(index, events);
                        true
                    }
                    Poll::Ready(Err(error)) => {
                        self.targets[index].counters.packets_sent = client.packets_sent();
                        self.fail_target(index, ManagedTargetFailurePhase::Receiving, error);
                        return false;
                    }
                };
                if client.is_peer_closed() {
                    self.targets[index].counters.packets_sent = client.packets_sent();
                    self.finish_target(index, ManagedTargetEndReason::PeerClosed, None);
                    return false;
                }
                if client
                    .next_probe_timeout_deadline()
                    .is_some_and(|timeout| timeout <= now)
                {
                    match client.poll_timeouts_at(now) {
                        Ok(events) => self.publish_client_events(index, events),
                        Err(error) => {
                            self.targets[index].counters.packets_sent = client.packets_sent();
                            self.fail_target(index, ManagedTargetFailurePhase::Timing, error);
                            return false;
                        }
                    }
                }
                if now >= deadline {
                    let close_deadline = now.checked_add(self.config.final_drain).unwrap_or(now);
                    self.install_target_state(
                        index,
                        TargetState::Closing {
                            client,
                            deadline: close_deadline,
                            primary_end,
                        },
                    );
                    true
                } else {
                    self.targets[index].state = TargetState::Draining {
                        client,
                        deadline,
                        primary_end,
                    };
                    received
                }
            }
            TargetState::Closing {
                mut client,
                deadline,
                primary_end,
            } => match client.poll_close(cx) {
                Poll::Pending => {
                    if now >= deadline {
                        self.targets[index].counters.packets_sent = client.packets_sent();
                        self.finish_target(index, primary_end, Some(close_timeout_failure()));
                        false
                    } else {
                        self.targets[index].state = TargetState::Closing {
                            client,
                            deadline,
                            primary_end,
                        };
                        false
                    }
                }
                Poll::Ready(Ok(events)) => {
                    self.targets[index].counters.packets_sent = client.packets_sent();
                    self.publish_client_events(index, events);
                    self.finish_target(index, primary_end, None);
                    false
                }
                Poll::Ready(Err(error)) => {
                    self.targets[index].counters.packets_sent = client.packets_sent();
                    let cleanup = classify_client_error(ManagedTargetFailurePhase::Closing, &error);
                    self.finish_target(index, primary_end, Some(cleanup));
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
        now: Instant,
    ) -> bool {
        let latest = self.targets[index]
            .latest_committed_timeout
            .into_iter()
            .chain(client.latest_probe_timeout_deadline())
            .max()
            .unwrap_or(now)
            .max(now);
        let Some(deadline) = latest.checked_add(self.config.final_drain) else {
            self.fail_target(
                index,
                ManagedTargetFailurePhase::Timing,
                ClientError::DurationOverflow,
            );
            return false;
        };
        self.install_target_state(
            index,
            TargetState::Draining {
                client,
                deadline,
                primary_end,
            },
        );
        true
    }

    fn active_count(&self) -> usize {
        self.targets
            .iter()
            .filter(|target| matches!(target.state, TargetState::Active { .. }))
            .count()
    }

    fn poll_one_send(&mut self, index: usize, cx: &mut Context<'_>, now: Instant) -> SendResult {
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
        let schedule_error = accepted
            .then(|| client.skip_missed_probe_slots_at(Instant::now()))
            .transpose()
            .err();
        self.targets[index].counters.packets_sent = after;
        if accepted {
            self.targets[index].latest_committed_timeout = client
                .latest_probe_timeout_deadline()
                .into_iter()
                .chain(self.targets[index].latest_committed_timeout)
                .max();
        }
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
                    self.fail_target(index, ManagedTargetFailurePhase::Timing, error);
                    SendResult::Failed { accepted }
                } else {
                    SendResult::Ready { accepted }
                }
            }
            Poll::Ready(Err(error)) => {
                self.targets[index].send_waiting = false;
                self.fail_target(index, ManagedTargetFailurePhase::Sending, error);
                SendResult::Failed { accepted }
            }
        }
    }

    fn poll_staggered_send(&mut self, cx: &mut Context<'_>, now: Instant) -> bool {
        let active = self.active_count();
        if active == 0 || self.send_gate.is_some_and(|gate| gate > now) {
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
            if !matches!(self.targets[index].state, TargetState::Active { .. }) {
                continue;
            }
            let result = self.poll_one_send(index, cx, now);
            if result == SendResult::NotAttempted {
                continue;
            }
            self.stagger_remaining = 0;
            if result.accepted() {
                self.send_gate = Instant::now()
                    .checked_add(stagger_spacing(self.config.client.interval, active));
            }
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
            let result = self.poll_one_send(index, cx, now);
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
                    if !target.send_waiting {
                        send_deadline = send_deadline
                            .into_iter()
                            .chain(client.next_send_deadline())
                            .min();
                    }
                }
                TargetState::Draining {
                    client, deadline, ..
                } => {
                    non_send_deadline = non_send_deadline
                        .into_iter()
                        .chain(client.next_probe_timeout_deadline())
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
        let outcome = Arc::new(self.history.outcome(end_reason));
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

        let now = Instant::now();
        let mut immediate = this.poll_target_pass(cx, now);
        if this.state == DriverState::Running {
            immediate |= match this.config.pacing {
                ManagedPacing::Staggered => this.poll_staggered_send(cx, now),
                ManagedPacing::Burst => this.poll_burst_sends(cx, now),
            };
        }

        if this.all_targets_terminal() {
            match this.state {
                DriverState::Stopping => {
                    return this.seal(ManagedEndReason::StopRequested, false);
                }
                DriverState::Running
                    if this.config.completion == ManagedCompletionPolicy::FinishWhenQuiescent =>
                {
                    this.state = DriverState::Stopping;
                    this.lifecycle = ManagedLifecycle::Stopping;
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
    if config.max_live_target_generations == 0 {
        return Err(ManagedConfigError::ZeroLiveGenerationLimit);
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
                id: target.id,
                generation,
            },
            server_addr: Arc::from(target.server_addr),
            remote: None,
            counters: TargetCounters::default(),
            latest_committed_timeout: None,
            send_waiting: false,
            state: TargetState::Pending { client_config },
        });
    }

    let (event_sender, _) = broadcast::channel(config.event_capacity);
    let weak_events = event_sender.downgrade();
    let (wake_sender, wake_receiver) = watch::channel(());
    let (acknowledgement_sender, _) = watch::channel(());
    let stop = Arc::new(StopSignal {
        requested: AtomicBool::new(false),
        wake: wake_sender,
        acknowledged: AtomicBool::new(false),
        acknowledgement: acknowledgement_sender,
    });
    let history = OutcomeHistory::new(config.outcome_history_limit);
    let initial_targets = runtimes
        .iter()
        .map(|target| ManagedTargetStatus {
            target: target.instance.clone(),
            lifecycle: ManagedTargetLifecycle::Pending,
            server_addr: Arc::clone(&target.server_addr),
            remote: None,
        })
        .collect::<Vec<_>>();
    let initial = Arc::new(ManagedStatus {
        lifecycle: ManagedLifecycle::NotStarted,
        stop_requested: false,
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
        stop_observed: false,
        final_outcome: None,
        _next_generation: next_generation,
    };
    let handle = ManagedClientHandle {
        stop,
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

fn close_timeout_failure() -> ManagedTargetFailure {
    ManagedTargetFailure {
        phase: ManagedTargetFailurePhase::Closing,
        kind: ManagedTargetFailureKind::Timeout,
        message: Arc::from("best-effort close did not become writable before its deadline"),
    }
}
