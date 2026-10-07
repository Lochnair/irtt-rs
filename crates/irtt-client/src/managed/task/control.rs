//! Task construction, public control/observation handles, and stop/update admission.

use std::{
    collections::{HashSet, VecDeque},
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Instant,
};

use tokio::sync::{broadcast, mpsc, oneshot, watch};

use crate::{
    managed::{
        ManagedClientConfig, ManagedCommandAcknowledgement, ManagedCommandApplyError,
        ManagedCommandError, ManagedCompletionPolicy, ManagedConfigError, ManagedEvent,
        ManagedEventSubscription, ManagedLifecycle, ManagedStatus, ManagedStatusSubscription,
        ManagedSubscribeError, ManagedTargetConfig, ManagedTargetLifecycle, ManagedTargetStatus,
        TargetInstance,
    },
    session::machine::SessionMachine,
    socket_options::validate_ttl,
    ClientConfig, ClientError,
};

use super::{
    arm_wake,
    target::{TargetCounters, TargetMembership, TargetPhase, TargetRuntime},
    timeouts::TIMEOUT_WORK_BUDGET,
    ManagedClientTask, OutcomeHistory, TaskPhase, TaskResources,
};

// Tokio 1.53 broadcast asserts this exact bound before rounding capacity to a
// power of two; Tokio does not expose it as a public constant.
const MAX_BROADCAST_CHANNEL_CAPACITY: usize = usize::MAX >> 1;

/// Entry point for constructing a unified Tokio managed task.
///
/// # Example
///
/// Construct the task and its separate control handle, subscribe before
/// starting the task, then run it under the caller's Tokio runtime. A reachable
/// server is required at runtime, so this example only type-checks.
///
/// ```no_run
/// use irtt_client::{ClientConfig, ClientEvent};
/// use irtt_client::managed::{
///     ManagedClient, ManagedClientConfig, ManagedEvent, ManagedTargetConfig,
/// };
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let config = ManagedClientConfig {
///     client: ClientConfig {
///         request: irtt_client::SessionRequest {
///             duration: Some(std::time::Duration::from_secs(10)),
///             ..irtt_client::SessionRequest::default()
///         },
///         ..ClientConfig::default()
///     },
///     ..ManagedClientConfig::default()
/// };
/// let targets = vec![ManagedTargetConfig::new("edge", "127.0.0.1:2112")];
/// let (task, handle) = ManagedClient::task(config, targets)?;
/// let mut events = handle.subscribe()?;
/// let driver = tokio::spawn(task);
///
/// while let Ok(event) = events.recv().await {
///     if let ManagedEvent::Client { event: ClientEvent::EchoReply { .. }, .. } = event {
///         println!("received a probe reply");
///     }
/// }
/// let outcome = driver.await?;
/// println!("run ended: {:?}", outcome.end_reason);
/// # Ok(())
/// # }
/// ```
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
pub(super) struct StopSignal {
    requested: AtomicBool,
    update_admission: AtomicU8,
    wake: watch::Sender<()>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(super) enum UpdateAdmission {
    Open,
    Stopping,
    Closed,
}

impl UpdateAdmission {
    fn from_raw(value: u8) -> Self {
        match value {
            value if value == Self::Open as u8 => Self::Open,
            value if value == Self::Stopping as u8 => Self::Stopping,
            value if value == Self::Closed as u8 => Self::Closed,
            _ => unreachable!("managed update admission stores a known state"),
        }
    }
}

impl StopSignal {
    fn request(&self) {
        self.begin_stopping();
        if !self.requested.swap(true, Ordering::AcqRel) {
            self.wake.send_replace(());
        }
    }

    pub(super) fn update_admission(&self) -> UpdateAdmission {
        UpdateAdmission::from_raw(self.update_admission.load(Ordering::Acquire))
    }

    pub(super) fn begin_stopping(&self) {
        let mut current = self.update_admission.load(Ordering::Acquire);
        loop {
            match UpdateAdmission::from_raw(current) {
                UpdateAdmission::Open => match self.update_admission.compare_exchange_weak(
                    UpdateAdmission::Open as u8,
                    UpdateAdmission::Stopping as u8,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return,
                    Err(observed) => current = observed,
                },
                UpdateAdmission::Stopping | UpdateAdmission::Closed => return,
            }
        }
    }

    pub(super) fn close_updates(&self) {
        self.update_admission
            .store(UpdateAdmission::Closed as u8, Ordering::Release);
    }

    pub(super) fn is_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }
}

/// Cloneable control and observation capability for a managed task.
#[derive(Clone)]
pub struct ManagedClientHandle {
    stop: Arc<StopSignal>,
    commands: mpsc::Sender<ManagedCommand>,
    max_live_target_generations: usize,
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

    /// Subscribe to authoritative durable latest-state observation.
    ///
    /// The current snapshot is marked as seen when subscribing.
    /// The current snapshot is immediately available through `borrow()`. Updates
    /// may coalesce; this is not a lossless event log. After the task terminates,
    /// the sender closes but the final snapshot remains readable. Use
    /// [`Self::subscribe`] for the lossy presentation-event stream.
    pub fn subscribe_status(&self) -> ManagedStatusSubscription {
        let mut status = self.status.clone();
        drop(status.borrow_and_update());
        status
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
        ManagedStopReceipt::new(self.subscribe_status())
    }

    /// Submit the complete desired target set, replacing any previous one.
    ///
    /// `targets` is not a delta: any live target whose id is missing from
    /// `targets` is stopped and removed. A target that is still present with an
    /// unchanged configuration continues undisturbed — it is not restarted and
    /// does not get a new generation. A target present with a *changed*
    /// configuration, or whose current generation already reached a terminal
    /// state (e.g. failed, completed, or was closed by the peer), is treated as
    /// a fresh start: it is retired and a new generation is created for it, even
    /// if the configuration is byte-for-byte identical to the one that just
    /// finished.
    ///
    /// This call enqueues the update without waiting for queue capacity; await
    /// the returned [`ManagedCommandReceipt`] to observe whether — and how — it
    /// was applied.
    pub fn update_targets(
        &self,
        targets: Vec<ManagedTargetConfig>,
    ) -> Result<ManagedCommandReceipt, ManagedCommandError> {
        match self.stop.update_admission() {
            UpdateAdmission::Open => {}
            UpdateAdmission::Stopping => return Err(ManagedCommandError::Stopping),
            UpdateAdmission::Closed => return Err(ManagedCommandError::DriverClosed),
        }
        if targets.len() > self.max_live_target_generations {
            return Err(ManagedCommandError::TooManyTargets {
                configured: targets.len(),
                limit: self.max_live_target_generations,
            });
        }

        // Observing `Open` only permits attempting submission. A successful
        // `try_send` transfers command ownership to the driver. A concurrent
        // stop may move admission to `Stopping` before this send, allowing the
        // command to enqueue; the driver-side check immediately before
        // `apply_targets` determines whether it applies or resolves as
        // `Stopping`. Terminal sealing closes the receiver before draining
        // accepted commands.
        let (acknowledgement, receiver) = oneshot::channel();
        match self.commands.try_send(ManagedCommand::UpdateTargets {
            targets,
            acknowledgement,
        }) {
            Ok(()) => Ok(ManagedCommandReceipt { receiver }),
            Err(mpsc::error::TrySendError::Full(_)) => match self.stop.update_admission() {
                UpdateAdmission::Open => Err(ManagedCommandError::QueueFull),
                UpdateAdmission::Stopping => Err(ManagedCommandError::Stopping),
                UpdateAdmission::Closed => Err(ManagedCommandError::DriverClosed),
            },
            Err(mpsc::error::TrySendError::Closed(_)) => Err(ManagedCommandError::DriverClosed),
        }
    }
}

/// Receipt for a target-set transaction accepted by the driver queue.
#[must_use = "await the receipt to observe target-update application"]
pub struct ManagedCommandReceipt {
    receiver: oneshot::Receiver<Result<ManagedCommandAcknowledgement, ManagedCommandApplyError>>,
}

impl ManagedCommandReceipt {
    /// Block until the target-set transaction is applied or rejected.
    ///
    /// This is for synchronous callers such as [`crate::managed::BlockingManagedClient`]. It
    /// must not be called from an asynchronous runtime; use the [`Future`]
    /// implementation there instead.
    pub fn blocking_wait(self) -> Result<ManagedCommandAcknowledgement, ManagedCommandApplyError> {
        self.receiver
            .blocking_recv()
            .unwrap_or(Err(ManagedCommandApplyError::AcknowledgementDisconnected))
    }
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

pub(super) enum ManagedCommand {
    UpdateTargets {
        targets: Vec<ManagedTargetConfig>,
        acknowledgement:
            oneshot::Sender<Result<ManagedCommandAcknowledgement, ManagedCommandApplyError>>,
    },
}

/// Receipt resolving once stop is durably observed or the task is terminal.
///
/// Resolves from authoritative status as soon as `stop_requested` is true or
/// the lifecycle is Completed, Failed, or Abandoned. It need not wait for
/// graceful cleanup after stop observation.
#[must_use = "await the receipt to observe durable stop acknowledgement"]
pub struct ManagedStopReceipt {
    future: Pin<Box<dyn Future<Output = ()> + Send>>,
}

impl ManagedStopReceipt {
    fn new(mut status: ManagedStatusSubscription) -> Self {
        let future = Box::pin(async move {
            while !{
                let snapshot = status.borrow();
                snapshot.stop_requested
                    || matches!(
                        snapshot.lifecycle,
                        ManagedLifecycle::Completed
                            | ManagedLifecycle::Failed
                            | ManagedLifecycle::Abandoned
                    )
            } {
                // A closed sender retains the terminal snapshot. Never wait
                // indefinitely if the channel closes.
                if status.changed().await.is_err() {
                    break;
                }
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

fn build_task(
    config: ManagedClientConfig,
    targets: Vec<ManagedTargetConfig>,
) -> Result<(ManagedClientTask, ManagedClientHandle), ManagedConfigError> {
    if config.event_capacity == 0 {
        return Err(ManagedConfigError::ZeroEventCapacity);
    }
    if config.event_capacity > MAX_BROADCAST_CHANNEL_CAPACITY {
        return Err(ManagedConfigError::EventCapacityTooLarge {
            configured: config.event_capacity,
            maximum: MAX_BROADCAST_CHANNEL_CAPACITY,
        });
    }
    if config.command_capacity == 0 {
        return Err(ManagedConfigError::ZeroCommandCapacity);
    }
    if config.command_capacity > tokio::sync::Semaphore::MAX_PERMITS {
        return Err(ManagedConfigError::CommandCapacityTooLarge {
            configured: config.command_capacity,
            maximum: tokio::sync::Semaphore::MAX_PERMITS,
        });
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
        client_config.address_family = target
            .address_family
            .unwrap_or(config.client.address_family);
        client_config.auth = target.auth.resolve(&config.client.auth);
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
            membership: TargetMembership::Desired,
            server_addr: Arc::from(target.server_addr),
            remote: None,
            counters: TargetCounters::default(),
            phase: Some(TargetPhase::Pending { client_config }),
        });
    }

    let (event_sender, _) = broadcast::channel(config.event_capacity);
    let weak_events = event_sender.downgrade();
    let (wake_sender, wake_receiver) = watch::channel(());
    let stop = Arc::new(StopSignal {
        requested: AtomicBool::new(false),
        update_admission: AtomicU8::new(UpdateAdmission::Open as u8),
        wake: wake_sender,
    });
    let history = OutcomeHistory::new(config.outcome_history_limit);
    let initial_targets = runtimes
        .iter()
        .map(|target| ManagedTargetStatus {
            target: target.instance.clone(),
            desired: target.membership.is_desired(),
            lifecycle: ManagedTargetLifecycle::Pending,
            outcome: None,
            server_addr: Arc::clone(&target.server_addr),
            remote: None,
        })
        .collect::<Vec<_>>();
    let phase = TaskPhase::NotStarted;
    let initial = Arc::new(ManagedStatus {
        lifecycle: phase.lifecycle(),
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
        final_outcome: phase.final_outcome(),
    });
    let (status_sender, status_receiver) = watch::channel(initial);
    let (command_sender, command_receiver) = mpsc::channel(config.command_capacity);
    let max_live_target_generations = config.max_live_target_generations;
    let resources = TaskResources {
        status: status_sender,
        events: Some(event_sender),
        stop: Arc::clone(&stop),
    };
    let task = ManagedClientTask {
        #[cfg(test)]
        timeout_inspections: std::cell::RefCell::new(Vec::new()),
        phase,
        config,
        targets: runtimes,
        commands: command_receiver,
        history,
        resources: Some(resources),
        wake: Some(arm_wake(wake_receiver)),
        timer: None,
        cursor: 0,
        timeout_cursor: 0,
        timeout_backlog: VecDeque::with_capacity(TIMEOUT_WORK_BUDGET),
        scan_remaining: 0,
        send_cursor: 0,
        burst_remaining: 0,
        stagger_remaining: 0,
        send_gate: None,
        last_stagger_send: None,
        stop_observed: false,
        next_generation,
        applied_command_sequence: 0,
    };
    let handle = ManagedClientHandle {
        stop,
        commands: command_sender,
        max_live_target_generations,
        status: status_receiver,
        events: weak_events,
    };
    Ok((task, handle))
}

pub(super) fn validate_target_config(config: &ClientConfig) -> Result<(), ClientError> {
    config.open.validate()?;
    SessionMachine::validate_config(config)?;
    if let Some(ttl) = config.socket.ttl {
        validate_ttl(ttl)?;
    }
    Ok(())
}
