use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    hash::{Hash, Hasher},
    net::{SocketAddr, UdpSocket},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc, Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crate::{
    client::{
        echo_sent_event,
        schedule::{advance_cadence, instant_abs_diff, ProbeSchedule},
        validate_datagram_length,
    },
    config::{ClientAuthConfig, ClientConfig},
    error::ClientError,
    event::{ClientEvent, OpenOutcome},
    metadata::ReceiveMeta,
    receive::{recv_datagram_from, ReceivedDatagramFrom},
    session::machine::{
        params_from_config, OpenDatagramDisposition, PreparedOpenAcceptance, PreparedOpenRequest,
        SessionMachine,
    },
    socket::{bind_unconnected_udp_socket, validate_open_timeouts},
    socket_options::apply_dscp_to_socket,
    timing::ClientTimestamp,
};

#[cfg(test)]
use crate::client::ProbeSendTimestamps;

use super::{
    cancellation::CancellationToken,
    hub::{EventHub, EventSubscription, SubscriberConfig},
};

const GROUP_RECV_TIMEOUT: Duration = Duration::from_millis(20);
const GROUP_FINAL_DRAIN: Duration = Duration::from_millis(100);
const MAX_SLEEP: Duration = Duration::from_millis(20);
const RECV_BUFFER_SIZE: usize = 65_536;
/// Maximum number of recent target outcomes retained in [`ManagedGroupOutcome`].
pub const MANAGED_GROUP_OUTCOME_HISTORY_LIMIT: usize = 256;

/// Caller-owned target identity for managed multi-target probing.
#[derive(Clone, Eq)]
pub struct TargetId(Arc<str>);

impl TargetId {
    /// Borrow the identifier as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for TargetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("TargetId").field(&self.0).finish()
    }
}

impl fmt::Display for TargetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl PartialEq for TargetId {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Hash for TargetId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl From<&str> for TargetId {
    fn from(value: &str) -> Self {
        Self(Arc::from(value))
    }
}

impl From<String> for TargetId {
    fn from(value: String) -> Self {
        Self(Arc::from(value))
    }
}

impl AsRef<str> for TargetId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// Per-target configuration for [`ManagedClientGroup`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedTargetConfig {
    /// Caller-owned stable target identity.
    pub id: TargetId,
    /// Resolved UDP remote address. Group v1 does not perform DNS resolution.
    pub remote: SocketAddr,
    /// Optional auth override for this target.
    pub auth: Option<ClientAuthConfig>,
}

/// Group send pacing strategy.
///
/// Both modes keep an absolute cadence. If a scheduler delay spans multiple
/// slots, missed historical slots are skipped instead of sent as a catch-up
/// burst.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ManagedGroupPacing {
    /// Send active targets one at a time, spaced approximately interval / N.
    #[default]
    Staggered,
    /// Send one probe to every active target back-to-back once per interval.
    Burst,
}

/// Policy controlling when a managed group completes naturally.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ManagedGroupCompletionPolicy {
    /// Complete after a non-empty desired target set reaches terminal outcomes.
    ///
    /// Replacing the desired set with an empty set leaves the group alive and
    /// idle so a later [`ManagedClientGroupSession::update_targets`] call can
    /// resume probing.
    #[default]
    AllTargetsComplete,
    /// Remain alive until explicit cancellation, including after all targets finish.
    ExplicitCancellation,
}

/// Configuration for a managed multi-target client group.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ManagedClientGroupConfig {
    /// Shared client configuration template.
    ///
    /// Most protocol, socket, timing, and negotiation settings are group-wide.
    /// `server_addr` is ignored because targets already carry resolved
    /// [`SocketAddr`] values. `hmac_key` is the default auth unless a target
    /// supplies [`ManagedTargetConfig::auth`].
    pub client: ClientConfig,
    /// Coordinated group pacing mode.
    pub pacing: ManagedGroupPacing,
    /// Policy controlling natural group completion.
    pub completion: ManagedGroupCompletionPolicy,
}

/// Target-scoped managed event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetEvent {
    /// Target that produced the event.
    pub target: TargetId,
    /// Client event produced by that target's session runtime.
    pub event: ClientEvent,
}

/// Event published by a managed client group.
///
/// Per-session events are followed by exactly one [`TargetFinished`](Self::TargetFinished)
/// event when that target incarnation reaches a terminal state.
// ClientEvent intentionally owns its structured measurement payload. EventHub
// queues are explicitly bounded, so preserving the direct event API does not
// create unbounded retention.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManagedGroupEvent {
    /// A protocol lifecycle, measurement, or warning event from one target.
    Client(TargetEvent),
    /// Authoritative terminal outcome for one target incarnation.
    TargetFinished(ManagedTargetOutcome),
}

impl ManagedGroupEvent {
    /// Return the target associated with this event.
    pub fn target(&self) -> &TargetId {
        match self {
            Self::Client(event) => &event.target,
            Self::TargetFinished(outcome) => &outcome.id,
        }
    }
}

/// Subscription type for managed group events.
pub type TargetEventSubscription = EventSubscription<ManagedGroupEvent>;

/// Entry point for running a shared-socket multi-target managed client group.
///
/// Each opening attempt sends one reusable request and may inspect several
/// datagrams until its absolute deadline. Malformed, unrelated, and
/// unauthenticated traffic is ignored; authenticated incompatibility is
/// terminal. A trusted token followed by group-policy or schedule rejection
/// triggers a best-effort cleanup close without replacing the primary failure.
#[derive(Debug)]
pub struct ManagedClientGroup;

/// Running managed client group.
///
/// Dropping the session requests cooperative cancellation. Use
/// [`join`](Self::join) to wait for scheduler and receive threads and obtain
/// the final [`ManagedGroupOutcome`].
#[must_use = "dropping the session cancels the managed client group; call join() to wait for completion"]
#[derive(Debug)]
pub struct ManagedClientGroupSession {
    hub: EventHub<ManagedGroupEvent>,
    control_tx: mpsc::Sender<ControlMessage>,
    cancellation: CancellationToken,
    peer_closed_target_count: Arc<AtomicU64>,
    scheduler: Option<JoinHandle<Result<ManagedGroupOutcome, ClientError>>>,
    receiver: Option<JoinHandle<Result<(), ClientError>>>,
}

/// Outcome returned by a completed managed client group.
#[must_use = "managed group outcomes contain completion status and per-target lifecycle counters"]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedGroupOutcome {
    /// Why the group scheduler stopped.
    pub end_reason: ManagedGroupEndReason,
    /// Recent per-target lifecycle records in completion order.
    ///
    /// This snapshot retains at most [`MANAGED_GROUP_OUTCOME_HISTORY_LIMIT`]
    /// entries. Every outcome is published as
    /// [`ManagedGroupEvent::TargetFinished`] before an older snapshot entry can
    /// be evicted.
    pub targets: Vec<ManagedTargetOutcome>,
    /// Total number of target incarnations that reached a terminal outcome.
    pub total_target_outcomes: u64,
    /// Number of target outcomes classified as successful.
    pub successful_target_outcomes: u64,
    /// Number of target outcomes that ended because the peer closed the session.
    ///
    /// Peer closure remains a successful library-level outcome and is also
    /// included in [`Self::successful_target_outcomes`]. This aggregate covers
    /// outcomes omitted from the bounded [`Self::targets`] snapshot.
    pub peer_closed_target_outcomes: u64,
    /// Number of target outcomes carrying structured failure details.
    pub failed_target_outcomes: u64,
    /// Number of older outcomes omitted from [`Self::targets`].
    pub discarded_target_outcomes: u64,
}

/// Reason the managed group stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedGroupEndReason {
    /// All currently desired targets reached a terminal state.
    AllTargetsComplete,
    /// Cancellation was requested through stop, drop, or a worker failure.
    Cancelled,
}

/// Per-target lifecycle outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedTargetOutcome {
    /// Target identity.
    pub id: TargetId,
    /// Resolved remote address used for the target.
    pub remote: SocketAddr,
    /// Why this target stopped.
    pub end_reason: ManagedTargetEndReason,
    /// Number of echo requests sent to this target.
    pub packets_sent: u64,
    /// Number of first in-window echo replies received from this target.
    pub replies_received: u64,
    /// Number of duplicate reply events emitted for this target.
    pub duplicates: u64,
    /// Number of late reply events emitted for this target.
    pub late: u64,
    /// Number of warning events emitted for this target.
    pub warning_events: u64,
}

impl ManagedTargetOutcome {
    /// Return whether this target completed successfully.
    pub fn is_success(&self) -> bool {
        self.end_reason.is_success()
    }
}

/// Reason an individual target stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManagedTargetEndReason {
    /// The negotiated finite test duration completed.
    TestComplete,
    /// Cancellation stopped the target.
    Cancelled,
    /// The target was removed by `update_targets`.
    Removed,
    /// The target completed a no-test open exchange.
    NoTestComplete,
    /// The server closed the target session.
    PeerClosed,
    /// Opening failed before a session became active.
    OpenFailed(ManagedTargetFailure),
    /// Runtime send/receive/close handling failed for this target.
    Failed(ManagedTargetFailure),
}

impl ManagedTargetEndReason {
    /// Return whether this reason represents a successfully completed run.
    pub fn is_success(&self) -> bool {
        matches!(
            self,
            Self::TestComplete | Self::NoTestComplete | Self::PeerClosed
        )
    }

    /// Return structured failure details, when this target failed.
    pub fn failure(&self) -> Option<&ManagedTargetFailure> {
        match self {
            Self::OpenFailed(failure) | Self::Failed(failure) => Some(failure),
            _ => None,
        }
    }
}

/// Structured failure details for a managed target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedTargetFailure {
    /// Machine-readable failure category.
    pub kind: ManagedTargetFailureKind,
    /// Human-readable diagnostic suitable for logs and user interfaces.
    pub message: String,
}

impl ManagedTargetFailure {
    fn opening(error: &ClientError) -> Self {
        Self {
            kind: classify_target_failure(error, true),
            message: error.to_string(),
        }
    }

    fn runtime(error: &ClientError) -> Self {
        Self {
            kind: classify_target_failure(error, false),
            message: error.to_string(),
        }
    }
}

/// Machine-readable category for a managed target failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedTargetFailureKind {
    /// No valid open reply arrived before the configured attempts expired.
    OpeningTimeout,
    /// Opening failed during protocol parsing, authentication, or negotiation.
    OpeningProtocol,
    /// A socket or socket-option operation failed.
    Socket,
    /// Runtime protocol or session-state processing failed.
    RuntimeProtocol,
    /// Target or client configuration was invalid.
    InvalidConfiguration,
    /// A managed worker failed internally.
    InternalWorker,
}

impl fmt::Display for ManagedTargetFailureKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::OpeningTimeout => "opening timeout",
            Self::OpeningProtocol => "opening protocol/authentication/negotiation",
            Self::Socket => "socket",
            Self::RuntimeProtocol => "runtime protocol",
            Self::InvalidConfiguration => "invalid configuration",
            Self::InternalWorker => "internal worker",
        })
    }
}

fn classify_target_failure(error: &ClientError, opening: bool) -> ManagedTargetFailureKind {
    match error {
        ClientError::OpenTimeout => ManagedTargetFailureKind::OpeningTimeout,
        ClientError::Socket(_)
        | ClientError::SocketOption { .. }
        | ClientError::ReadTimeoutRestore { .. } => ManagedTargetFailureKind::Socket,
        ClientError::InvalidConfig { .. }
        | ClientError::OpenTimeoutTooSmall { .. }
        | ClientError::NoOpenTimeouts => ManagedTargetFailureKind::InvalidConfiguration,
        ClientError::WorkerPanicked => ManagedTargetFailureKind::InternalWorker,
        _ if opening => ManagedTargetFailureKind::OpeningProtocol,
        _ => ManagedTargetFailureKind::RuntimeProtocol,
    }
}

impl ManagedClientGroup {
    /// Start a managed group without creating an initial event subscription.
    ///
    /// At least one initial target is required to choose the address family for
    /// the shared socket. After startup, [`ManagedClientGroupSession::update_targets`]
    /// may replace the desired set with an empty set.
    pub fn start(
        config: ManagedClientGroupConfig,
        targets: Vec<ManagedTargetConfig>,
    ) -> Result<ManagedClientGroupSession, ClientError> {
        Self::start_inner(config, targets, None).map(|(session, _)| session)
    }

    /// Start a managed group and subscribe before worker threads run.
    ///
    /// As with [`start`](Self::start), construction requires at least one
    /// initial target.
    pub fn start_with_subscription(
        config: ManagedClientGroupConfig,
        targets: Vec<ManagedTargetConfig>,
        subscriber_config: SubscriberConfig,
    ) -> Result<(ManagedClientGroupSession, TargetEventSubscription), ClientError> {
        let (session, subscription) = Self::start_inner(config, targets, Some(subscriber_config))?;
        Ok((
            session,
            subscription.expect("initial subscription must be present"),
        ))
    }

    fn start_inner(
        mut config: ManagedClientGroupConfig,
        targets: Vec<ManagedTargetConfig>,
        subscriber_config: Option<SubscriberConfig>,
    ) -> Result<(ManagedClientGroupSession, Option<TargetEventSubscription>), ClientError> {
        if targets.is_empty() {
            return Err(ClientError::InvalidConfig {
                reason: "managed client group requires at least one target".to_owned(),
            });
        }
        if config.client.socket_config.recv_timeout.is_none()
            || config.client.socket_config.recv_timeout > Some(GROUP_RECV_TIMEOUT)
        {
            config.client.socket_config.recv_timeout = Some(GROUP_RECV_TIMEOUT);
        }

        validate_open_timeouts(&config.client.open_timeouts)?;
        let requested = params_from_config(&config.client)?;
        let family_remote = validate_target_configs(&targets, None)?;
        validate_group_family_flags(&config.client, family_remote)?;

        let now = Instant::now();
        let mut next_order = 0_u64;
        let mut registry = TargetRegistry::default();
        for target in targets {
            let state = Arc::new(Mutex::new(TargetState::new(
                &config.client,
                target,
                next_order,
                now,
            )?));
            next_order = next_order
                .checked_add(1)
                .ok_or(ClientError::CounterOverflow {
                    counter: "target_order",
                })?;
            let (id, remote) = {
                let target = state.lock().expect("target mutex poisoned");
                (target.id.clone(), target.remote)
            };
            registry.remotes.insert(remote, id.clone());
            registry.desired.insert(id.clone());
            registry.targets.insert(id, state);
        }

        let socket = bind_unconnected_udp_socket(&config.client.socket_config, family_remote)?;
        apply_dscp_to_socket(&socket, family_remote, config.client.dscp)?;

        let hub = EventHub::new();
        let initial_subscription = subscriber_config
            .map(|config| hub.subscribe(config))
            .transpose()?;

        let cancellation = CancellationToken::new();
        let peer_closed_target_count = Arc::new(AtomicU64::new(0));
        let (control_tx, control_rx) = mpsc::channel();
        let shared = Arc::new(GroupShared {
            registry: Mutex::new(registry),
            hub: hub.clone(),
            cancellation: cancellation.clone(),
            control_tx: control_tx.clone(),
            peer_closed_target_count: peer_closed_target_count.clone(),
            receiver_drain: Mutex::new(ReceiverDrainState::default()),
            requested_interval_ns: requested.interval_ns,
            requested_dscp: requested.dscp,
            family_remote,
        });

        let send_socket = socket;
        let recv_socket = send_socket.try_clone()?;
        let scheduler_shared = shared.clone();
        let scheduler_config = config.clone();
        let scheduler = thread::spawn(move || {
            let _cleanup = GroupSchedulerCleanup {
                shared: scheduler_shared.clone(),
            };
            run_group_scheduler(
                scheduler_config,
                send_socket,
                scheduler_shared,
                control_rx,
                next_order,
            )
        });

        let receiver_shared = shared;
        let receiver = thread::spawn(move || run_group_receiver(recv_socket, receiver_shared));

        Ok((
            ManagedClientGroupSession {
                hub,
                control_tx,
                cancellation,
                peer_closed_target_count,
                scheduler: Some(scheduler),
                receiver: Some(receiver),
            },
            initial_subscription,
        ))
    }
}

impl ManagedClientGroupSession {
    /// Add another target-event subscriber to this running group.
    pub fn subscribe(
        &self,
        config: SubscriberConfig,
    ) -> Result<TargetEventSubscription, ClientError> {
        self.hub.subscribe(config)
    }

    /// Replace the desired target set.
    ///
    /// The update is authoritative. Existing targets with the same
    /// [`TargetId`], remote address, and auth setting remain running. In v1,
    /// changing the remote address or auth for an existing target id is
    /// rejected instead of mutating the active session in place. An empty set
    /// removes every target but leaves the group alive and idle.
    pub fn update_targets(&self, targets: Vec<ManagedTargetConfig>) -> Result<(), ClientError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.control_tx
            .send(ControlMessage::Update {
                targets,
                reply: reply_tx,
            })
            .map_err(|_| ClientError::AlreadyClosed)?;
        reply_rx.recv().unwrap_or(Err(ClientError::AlreadyClosed))
    }

    /// Request cooperative cancellation of the group.
    pub fn stop(&self) {
        self.cancellation.cancel();
        let _ = self.control_tx.send(ControlMessage::Wake);
    }

    /// Return the number of scheduler-recorded peer-closed target incarnations.
    ///
    /// The count is monotonic and saturating for the lifetime of this group.
    /// It becomes visible before the corresponding lossy
    /// [`ManagedGroupEvent::TargetFinished`] publication and includes outcomes
    /// no longer retained in the bounded recent outcome history.
    pub fn peer_closed_target_count(&self) -> u64 {
        self.peer_closed_target_count.load(Ordering::Acquire)
    }

    /// Wait for the scheduler and receive threads to finish.
    ///
    /// The returned outcome contains aggregate counts plus a bounded recent
    /// target-outcome snapshot. Authoritative outcomes are published through
    /// [`ManagedGroupEvent::TargetFinished`] before the subscription is sealed,
    /// and queued events remain drainable after this method returns.
    pub fn join(mut self) -> Result<ManagedGroupOutcome, ClientError> {
        let scheduler = self.scheduler.take().expect(
            "ManagedClientGroupSession invariant violated: scheduler handle missing before join",
        );
        let scheduler_result = scheduler.join().unwrap_or(Err(ClientError::WorkerPanicked));

        let receiver = self.receiver.take().expect(
            "ManagedClientGroupSession invariant violated: receiver handle missing before join",
        );
        let receiver_result = receiver.join().unwrap_or(Err(ClientError::WorkerPanicked));

        self.hub.disconnect_all();
        match (scheduler_result, receiver_result) {
            (Ok(outcome), Ok(())) => Ok(outcome),
            (Err(err), _) => Err(err),
            (Ok(_), Err(err)) => Err(err),
        }
    }
}

impl Drop for ManagedClientGroupSession {
    fn drop(&mut self) {
        self.cancellation.cancel();
        let _ = self.control_tx.send(ControlMessage::Wake);
    }
}

#[derive(Debug)]
enum ControlMessage {
    Update {
        targets: Vec<ManagedTargetConfig>,
        reply: mpsc::Sender<Result<(), ClientError>>,
    },
    Wake,
}

#[derive(Debug)]
struct GroupShared {
    registry: Mutex<TargetRegistry>,
    hub: EventHub<ManagedGroupEvent>,
    cancellation: CancellationToken,
    control_tx: mpsc::Sender<ControlMessage>,
    peer_closed_target_count: Arc<AtomicU64>,
    receiver_drain: Mutex<ReceiverDrainState>,
    requested_interval_ns: i64,
    requested_dscp: i64,
    family_remote: SocketAddr,
}

#[derive(Debug, Default)]
struct ReceiverDrainState {
    requested: u64,
    completed: u64,
}

impl ReceiverDrainState {
    fn request_or_join(&mut self) -> Result<u64, ClientError> {
        if self.requested == self.completed {
            self.requested = self
                .requested
                .checked_add(1)
                .ok_or(ClientError::CounterOverflow {
                    counter: "receiver_drain_generation",
                })?;
        }
        Ok(self.requested)
    }

    fn complete_observed(&mut self, observed_generation: u64) -> bool {
        let observed_generation = observed_generation.min(self.requested);
        if observed_generation > self.completed {
            self.completed = observed_generation;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
fn request_receiver_drain(shared: &GroupShared) -> Result<u64, ClientError> {
    shared
        .receiver_drain
        .lock()
        .expect("receiver drain mutex poisoned")
        .request_or_join()
}

fn requested_receiver_drain(shared: &GroupShared) -> u64 {
    shared
        .receiver_drain
        .lock()
        .expect("receiver drain mutex poisoned")
        .requested
}

fn completed_receiver_drain(shared: &GroupShared) -> u64 {
    shared
        .receiver_drain
        .lock()
        .expect("receiver drain mutex poisoned")
        .completed
}

fn complete_receiver_drain(shared: &GroupShared, observed_generation: u64) {
    let advanced = {
        let mut state = shared
            .receiver_drain
            .lock()
            .expect("receiver drain mutex poisoned");
        state.complete_observed(observed_generation)
    };
    if advanced {
        let _ = shared.control_tx.send(ControlMessage::Wake);
    }
}

struct GroupSchedulerCleanup {
    shared: Arc<GroupShared>,
}

impl Drop for GroupSchedulerCleanup {
    fn drop(&mut self) {
        self.shared.cancellation.cancel();
        let _ = self.shared.control_tx.send(ControlMessage::Wake);
        self.shared.hub.disconnect_all();
    }
}

#[derive(Debug, Default)]
struct TargetRegistry {
    desired: HashSet<TargetId>,
    targets: HashMap<TargetId, Arc<Mutex<TargetState>>>,
    remotes: HashMap<SocketAddr, TargetId>,
}

#[derive(Debug)]
struct TargetState {
    id: TargetId,
    remote: SocketAddr,
    configured_auth: Option<ClientAuthConfig>,
    runtime: SessionMachine,
    schedule: Option<ProbeSchedule>,
    status: TargetStatus,
    open_request: PreparedOpenRequest,
    counters: TargetCounters,
    order: u64,
    final_reason: Option<ManagedTargetEndReason>,
    #[cfg(test)]
    probe_reported_len: Option<usize>,
    #[cfg(test)]
    probe_send_error: bool,
    #[cfg(test)]
    probe_send_timestamps: Option<ProbeSendTimestamps>,
    #[cfg(test)]
    close_reported_len: Option<usize>,
    #[cfg(test)]
    close_send_error: bool,
    #[cfg(test)]
    close_send_attempts: usize,
    #[cfg(test)]
    close_event_reserve_error: bool,
}

type ScheduledTarget = (Arc<Mutex<TargetState>>, Instant);

#[derive(Debug)]
enum TargetStatus {
    Opening {
        attempt: usize,
        next_send_at: Instant,
        awaiting_receiver_generation: Option<u64>,
    },
    Active,
    Draining {
        deadline: Instant,
    },
    Finished,
}

#[derive(Debug)]
struct PreparedGroupOpen {
    machine: PreparedOpenAcceptance,
    schedule: Option<ProbeSchedule>,
}

#[derive(Debug)]
struct PreparedGroupOpenFailure {
    primary: ClientError,
    machine: PreparedOpenAcceptance,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct TargetCounters {
    replies_received: u64,
    duplicates: u64,
    late: u64,
    warning_events: u64,
}

#[derive(Debug, Default)]
struct TargetOutcomeHistory {
    recent: VecDeque<ManagedTargetOutcome>,
    total: u64,
    successful: u64,
    peer_closed: u64,
    failed: u64,
    discarded: u64,
}

impl TargetOutcomeHistory {
    fn record(&mut self, outcome: ManagedTargetOutcome) {
        self.total = self.total.saturating_add(1);
        if outcome.is_success() {
            self.successful = self.successful.saturating_add(1);
        }
        if matches!(&outcome.end_reason, ManagedTargetEndReason::PeerClosed) {
            self.peer_closed = self.peer_closed.saturating_add(1);
        }
        if outcome.end_reason.failure().is_some() {
            self.failed = self.failed.saturating_add(1);
        }
        if self.recent.len() == MANAGED_GROUP_OUTCOME_HISTORY_LIMIT {
            self.recent.pop_front();
            self.discarded = self.discarded.saturating_add(1);
        }
        self.recent.push_back(outcome);
    }

    fn into_group_outcome(self, end_reason: ManagedGroupEndReason) -> ManagedGroupOutcome {
        ManagedGroupOutcome {
            end_reason,
            targets: self.recent.into_iter().collect(),
            total_target_outcomes: self.total,
            successful_target_outcomes: self.successful,
            peer_closed_target_outcomes: self.peer_closed,
            failed_target_outcomes: self.failed,
            discarded_target_outcomes: self.discarded,
        }
    }
}

impl TargetState {
    fn new(
        group_config: &ClientConfig,
        target: ManagedTargetConfig,
        order: u64,
        now: Instant,
    ) -> Result<Self, ClientError> {
        let mut config = group_config.clone();
        if let Some(auth) = &target.auth {
            config.hmac_key = auth.hmac_key.clone();
        }
        let runtime = SessionMachine::new(config, target.remote)?;
        let open_request = runtime.prepare_open_request()?;
        Ok(Self {
            id: target.id,
            remote: target.remote,
            configured_auth: target.auth,
            runtime,
            schedule: None,
            status: TargetStatus::Opening {
                attempt: 0,
                next_send_at: now,
                awaiting_receiver_generation: None,
            },
            open_request,
            counters: TargetCounters::default(),
            order,
            final_reason: None,
            #[cfg(test)]
            probe_reported_len: None,
            #[cfg(test)]
            probe_send_error: false,
            #[cfg(test)]
            probe_send_timestamps: None,
            #[cfg(test)]
            close_reported_len: None,
            #[cfg(test)]
            close_send_error: false,
            #[cfg(test)]
            close_send_attempts: 0,
            #[cfg(test)]
            close_event_reserve_error: false,
        })
    }

    fn same_config(&self, config: &ManagedTargetConfig) -> bool {
        self.remote == config.remote && self.configured_auth == config.auth
    }

    fn observe(&mut self, event: &ClientEvent) {
        match event {
            ClientEvent::EchoReply { .. } => self.counters.replies_received += 1,
            ClientEvent::DuplicateReply { .. } => self.counters.duplicates += 1,
            ClientEvent::LateReply { .. } => self.counters.late += 1,
            ClientEvent::Warning { .. } => self.counters.warning_events += 1,
            _ => {}
        }
    }

    fn mark_finished(&mut self, reason: ManagedTargetEndReason) {
        if self.final_reason.is_some() {
            return;
        }
        self.status = TargetStatus::Finished;
        self.final_reason = Some(reason);
    }

    fn is_run_complete(&self) -> bool {
        self.runtime.is_terminal()
            || self
                .schedule
                .as_ref()
                .is_some_and(|schedule| schedule.is_finished() && self.runtime.pending_is_empty())
    }

    fn outcome(&self, reason: ManagedTargetEndReason) -> ManagedTargetOutcome {
        ManagedTargetOutcome {
            id: self.id.clone(),
            remote: self.remote,
            end_reason: reason,
            packets_sent: self.runtime.packets_sent(),
            replies_received: self.counters.replies_received,
            duplicates: self.counters.duplicates,
            late: self.counters.late,
            warning_events: self.counters.warning_events,
        }
    }
}

fn run_group_scheduler(
    config: ManagedClientGroupConfig,
    socket: UdpSocket,
    shared: Arc<GroupShared>,
    control_rx: mpsc::Receiver<ControlMessage>,
    mut next_order: u64,
) -> Result<ManagedGroupOutcome, ClientError> {
    let mut records = TargetOutcomeHistory::default();
    let mut pacing = PacingRuntime::new(config.pacing);
    let mut pending_control = None;

    loop {
        drain_control_messages(
            &config.client,
            &socket,
            &shared,
            &control_rx,
            pending_control.take(),
            &mut next_order,
            &mut records,
        );

        if shared.cancellation.is_cancelled() {
            cancel_remaining_targets(&socket, &shared, &mut records);
            return Ok(records.into_group_outcome(ManagedGroupEndReason::Cancelled));
        }

        let now = Instant::now();
        drive_open_attempts(&config.client, &socket, &shared, now);
        poll_active_timeouts(&shared, now);
        finish_completed_targets(&socket, &shared, now);
        collect_finished_targets(&shared, &mut records);

        if group_should_complete(&shared, config.completion) {
            return Ok(records.into_group_outcome(ManagedGroupEndReason::AllTargetsComplete));
        }

        let active = active_targets(&shared);
        pacing.reconcile(&active, now);
        if pacing.send_due(&active, now) {
            match config.pacing {
                ManagedGroupPacing::Staggered => {
                    if let Some((target, scheduled_at)) =
                        pacing.next_staggered(&active, config.client.interval, now)?
                    {
                        send_echo_to_target(&socket, &shared, target, scheduled_at);
                    }
                }
                ManagedGroupPacing::Burst => {
                    if let Some(scheduled_at) = pacing.next_burst(config.client.interval, now)? {
                        for target in active {
                            send_echo_to_target(&socket, &shared, target, scheduled_at);
                        }
                    }
                }
            }
            continue;
        }

        pending_control = wait_for_next_scheduler_wakeup(&control_rx, &shared, &pacing, &active);
    }
}

fn run_group_receiver(socket: UdpSocket, shared: Arc<GroupShared>) -> Result<(), ClientError> {
    let mut buf = vec![0_u8; RECV_BUFFER_SIZE];
    while !shared.cancellation.is_cancelled() {
        // A timeout only proves requests visible before this receive began.
        let observed_drain_generation = requested_receiver_drain(&shared);
        let datagram = match recv_datagram_from(&socket, &mut buf) {
            Ok(datagram) => datagram,
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                complete_receiver_drain(&shared, observed_drain_generation);
                continue;
            }
            Err(err) => {
                let failure = ManagedTargetFailure {
                    kind: ManagedTargetFailureKind::Socket,
                    message: ClientError::Socket(std::io::Error::new(err.kind(), err.to_string()))
                        .to_string(),
                };
                fail_all_targets(&shared, failure);
                shared.cancellation.cancel();
                let _ = shared.control_tx.send(ControlMessage::Wake);
                return Err(ClientError::Socket(err));
            }
        };

        process_group_datagram(&socket, &shared, datagram, &buf[..datagram.len]);
    }
    Ok(())
}

fn process_group_datagram(
    socket: &UdpSocket,
    shared: &GroupShared,
    datagram: ReceivedDatagramFrom,
    packet: &[u8],
) {
    // The single receiver thread cannot advance completion while processing
    // this datagram, so one snapshot consistently classifies its target.
    let completed_drain_generation = completed_receiver_drain(shared);
    let target = {
        let registry = shared
            .registry
            .lock()
            .expect("target registry mutex poisoned");
        let Some(id) = registry.remotes.get(&datagram.source) else {
            return;
        };
        registry.targets.get(id).cloned()
    };
    let Some(target) = target else {
        return;
    };

    let mut wake_scheduler = false;
    {
        let mut target = target.lock().expect("target mutex poisoned");
        match target.status {
            TargetStatus::Opening {
                attempt,
                awaiting_receiver_generation,
                ..
            } => {
                if attempt == 0
                    || awaiting_receiver_generation
                        .is_some_and(|generation| completed_drain_generation >= generation)
                {
                    return;
                }
                let opened_at = datagram.received_at;
                let reply = match target.runtime.inspect_open_datagram(packet) {
                    Ok(OpenDatagramDisposition::Ignore) => return,
                    Ok(OpenDatagramDisposition::Trusted(reply)) => reply,
                    Err(err) => {
                        target.mark_finished(ManagedTargetEndReason::OpenFailed(
                            ManagedTargetFailure::opening(&err),
                        ));
                        let _ = shared.control_tx.send(ControlMessage::Wake);
                        return;
                    }
                };
                let machine = match target.runtime.prepare_open_acceptance(reply, opened_at) {
                    Ok(machine) => machine,
                    Err(failure) => {
                        send_group_cleanup_close_best_effort(
                            socket,
                            target.remote,
                            failure.cleanup_close.as_deref(),
                        );
                        target.mark_finished(ManagedTargetEndReason::OpenFailed(
                            ManagedTargetFailure::opening(&failure.primary),
                        ));
                        let _ = shared.control_tx.send(ControlMessage::Wake);
                        return;
                    }
                };

                match prepare_group_open(shared, machine, opened_at) {
                    Ok(prepared) => {
                        let outcome = target.runtime.commit_open(prepared.machine);
                        target.schedule = prepared.schedule;
                        publish_open_outcome(&shared.hub, &mut target, outcome);
                        wake_scheduler = true;
                    }
                    Err(failure) => {
                        send_group_cleanup_close_best_effort(
                            socket,
                            target.remote,
                            failure.machine.cleanup_close_packet(),
                        );
                        target.mark_finished(ManagedTargetEndReason::OpenFailed(
                            ManagedTargetFailure::opening(&failure.primary),
                        ));
                        wake_scheduler = true;
                    }
                }
            }
            TargetStatus::Active | TargetStatus::Draining { .. } => {
                wake_scheduler = process_active_target_packet(
                    &shared.hub,
                    &mut target,
                    packet,
                    datagram.received_at,
                    datagram.meta,
                );
            }
            TargetStatus::Finished => {}
        }
    }

    if wake_scheduler {
        let _ = shared.control_tx.send(ControlMessage::Wake);
    }
}

fn validate_group_negotiation(
    shared: &GroupShared,
    negotiated: &crate::NegotiatedParams,
) -> Result<(), ClientError> {
    if negotiated.params.interval_ns != shared.requested_interval_ns {
        return Err(ClientError::NegotiationRejected {
            reason: "managed group v1 requires the negotiated interval to match the group interval"
                .to_owned(),
        });
    }
    if negotiated.params.dscp != shared.requested_dscp {
        return Err(ClientError::NegotiationRejected {
            reason: "managed group v1 requires the negotiated DSCP to match the group DSCP"
                .to_owned(),
        });
    }
    Ok(())
}

fn prepare_group_open(
    shared: &GroupShared,
    machine: PreparedOpenAcceptance,
    opened_at: ClientTimestamp,
) -> Result<PreparedGroupOpen, Box<PreparedGroupOpenFailure>> {
    let schedule = if let Some(negotiated) = machine.normal_negotiated() {
        if let Err(primary) = validate_group_negotiation(shared, negotiated) {
            return Err(Box::new(PreparedGroupOpenFailure { primary, machine }));
        }
        match ProbeSchedule::new(opened_at.mono, negotiated) {
            Ok(schedule) => Some(schedule),
            Err(primary) => return Err(Box::new(PreparedGroupOpenFailure { primary, machine })),
        }
    } else {
        None
    };
    Ok(PreparedGroupOpen { machine, schedule })
}

fn send_group_cleanup_close_best_effort(
    socket: &UdpSocket,
    remote: SocketAddr,
    packet: Option<&[u8]>,
) {
    if let Some(packet) = packet {
        let _ = socket.send_to(packet, remote);
    }
}

fn publish_open_outcome(
    hub: &EventHub<ManagedGroupEvent>,
    target: &mut TargetState,
    outcome: OpenOutcome,
) {
    match outcome {
        OpenOutcome::Started { event, .. } => {
            target.status = TargetStatus::Active;
            hub.publish(ManagedGroupEvent::Client(TargetEvent {
                target: target.id.clone(),
                event,
            }));
        }
        OpenOutcome::NoTestCompleted { event, .. } => {
            target.observe(&event);
            hub.publish(ManagedGroupEvent::Client(TargetEvent {
                target: target.id.clone(),
                event,
            }));
            target.mark_finished(ManagedTargetEndReason::NoTestComplete);
        }
    }
}

fn publish_events(
    hub: &EventHub<ManagedGroupEvent>,
    target: &mut TargetState,
    events: Vec<ClientEvent>,
) {
    for event in events {
        target.observe(&event);
        hub.publish(ManagedGroupEvent::Client(TargetEvent {
            target: target.id.clone(),
            event,
        }));
    }
}

fn process_active_target_packet(
    hub: &EventHub<ManagedGroupEvent>,
    target: &mut TargetState,
    packet: &[u8],
    received_at: ClientTimestamp,
    meta: ReceiveMeta,
) -> bool {
    let was_draining = matches!(target.status, TargetStatus::Draining { .. });
    match target
        .runtime
        .process_received_echo_packet(packet, received_at, meta)
    {
        Ok(events) => {
            publish_events(hub, target, events);
            if target.runtime.is_peer_closed() {
                target.schedule = None;
                target.mark_finished(ManagedTargetEndReason::PeerClosed);
                true
            } else {
                was_draining && !target.runtime.has_timed_out_metadata()
            }
        }
        Err(err) => {
            target.mark_finished(ManagedTargetEndReason::Failed(
                ManagedTargetFailure::runtime(&err),
            ));
            true
        }
    }
}

fn drain_control_messages(
    config: &ClientConfig,
    socket: &UdpSocket,
    shared: &GroupShared,
    control_rx: &mpsc::Receiver<ControlMessage>,
    first_message: Option<ControlMessage>,
    next_order: &mut u64,
    records: &mut TargetOutcomeHistory,
) {
    if let Some(message) = first_message {
        handle_control_message(config, socket, shared, next_order, records, message);
    }
    while let Ok(message) = control_rx.try_recv() {
        handle_control_message(config, socket, shared, next_order, records, message);
    }
}

fn handle_control_message(
    config: &ClientConfig,
    socket: &UdpSocket,
    shared: &GroupShared,
    next_order: &mut u64,
    records: &mut TargetOutcomeHistory,
    message: ControlMessage,
) {
    match message {
        ControlMessage::Update { targets, reply } => {
            let result = apply_target_update(config, socket, shared, targets, next_order, records);
            let _ = reply.send(result);
        }
        ControlMessage::Wake => {}
    }
}

fn apply_target_update(
    config: &ClientConfig,
    socket: &UdpSocket,
    shared: &GroupShared,
    targets: Vec<ManagedTargetConfig>,
    next_order: &mut u64,
    records: &mut TargetOutcomeHistory,
) -> Result<(), ClientError> {
    validate_open_timeouts(&config.open_timeouts)?;
    if !targets.is_empty() {
        validate_target_configs(&targets, Some(shared.family_remote))?;
    }

    let desired_ids: HashSet<TargetId> = targets.iter().map(|target| target.id.clone()).collect();
    let mut additions = Vec::new();

    {
        let registry = shared
            .registry
            .lock()
            .expect("target registry mutex poisoned");
        for target in &targets {
            if let Some(existing) = registry.targets.get(&target.id) {
                let existing = existing.lock().expect("target mutex poisoned");
                if !existing.same_config(target) {
                    return Err(ClientError::InvalidConfig {
                        reason: format!(
                            "managed group v1 rejects changed remote/auth for existing target {}",
                            target.id
                        ),
                    });
                }
            } else {
                additions.push(target.clone());
            }
        }
    }

    let mut prepared = Vec::new();
    let now = Instant::now();
    for target in additions {
        let order = *next_order;
        *next_order = next_order
            .checked_add(1)
            .ok_or(ClientError::CounterOverflow {
                counter: "target_order",
            })?;
        prepared.push(Arc::new(Mutex::new(TargetState::new(
            config, target, order, now,
        )?)));
    }

    let removed = {
        let mut registry = shared
            .registry
            .lock()
            .expect("target registry mutex poisoned");
        let removed_ids: Vec<TargetId> = registry
            .targets
            .keys()
            .filter(|id| !desired_ids.contains(*id))
            .cloned()
            .collect();
        let mut removed = Vec::with_capacity(removed_ids.len());
        for id in removed_ids {
            if let Some(target) = registry.targets.remove(&id) {
                let remote = target.lock().expect("target mutex poisoned").remote;
                registry.remotes.remove(&remote);
                removed.push(target);
            }
        }
        for target in prepared {
            let (id, remote) = {
                let target = target.lock().expect("target mutex poisoned");
                (target.id.clone(), target.remote)
            };
            registry.remotes.insert(remote, id.clone());
            registry.targets.insert(id, target);
        }
        registry.desired = desired_ids;
        removed
    };

    for target in removed {
        let outcome = close_target(socket, &shared.hub, target, ManagedTargetEndReason::Removed);
        record_target_outcome(shared, records, outcome);
    }

    Ok(())
}

fn validate_target_configs(
    targets: &[ManagedTargetConfig],
    expected_family: Option<SocketAddr>,
) -> Result<SocketAddr, ClientError> {
    let mut ids = HashSet::new();
    let mut remotes = HashSet::new();
    let mut family_remote = expected_family;

    for target in targets {
        if !ids.insert(target.id.clone()) {
            return Err(ClientError::InvalidConfig {
                reason: format!("duplicate managed target id {}", target.id),
            });
        }
        if !remotes.insert(target.remote) {
            return Err(ClientError::InvalidConfig {
                reason: format!("duplicate managed target remote {}", target.remote),
            });
        }
        if let Some(family) = family_remote {
            if family.is_ipv4() != target.remote.is_ipv4() {
                return Err(ClientError::InvalidConfig {
                    reason: "managed client group targets must use one address family".to_owned(),
                });
            }
        } else {
            family_remote = Some(target.remote);
        }
    }

    family_remote.ok_or_else(|| ClientError::InvalidConfig {
        reason: "managed client group requires at least one target".to_owned(),
    })
}

fn validate_group_family_flags(
    config: &ClientConfig,
    remote: SocketAddr,
) -> Result<(), ClientError> {
    if config.socket_config.ipv4_only && config.socket_config.ipv6_only {
        return Err(ClientError::InvalidConfig {
            reason: "ipv4_only and ipv6_only cannot both be true".to_owned(),
        });
    }
    if config.socket_config.ipv4_only && remote.is_ipv6() {
        return Err(ClientError::InvalidConfig {
            reason: "ipv4_only cannot be used with IPv6 group targets".to_owned(),
        });
    }
    if config.socket_config.ipv6_only && remote.is_ipv4() {
        return Err(ClientError::InvalidConfig {
            reason: "ipv6_only cannot be used with IPv4 group targets".to_owned(),
        });
    }
    Ok(())
}

fn drive_open_attempts(
    config: &ClientConfig,
    socket: &UdpSocket,
    shared: &GroupShared,
    now: Instant,
) {
    let targets = all_targets(shared);
    register_due_open_drains(shared, &targets, now);
    let completed_drain_generation = completed_receiver_drain(shared);

    for target in targets {
        let mut target = target.lock().expect("target mutex poisoned");
        let TargetStatus::Opening {
            attempt,
            next_send_at,
            awaiting_receiver_generation,
        } = target.status
        else {
            continue;
        };

        if next_send_at > now {
            continue;
        }

        if attempt > 0 {
            match awaiting_receiver_generation {
                None => continue,
                Some(generation) if completed_drain_generation < generation => continue,
                Some(_) => {}
            }
        }

        if attempt >= config.open_timeouts.len() {
            target.mark_finished(ManagedTargetEndReason::OpenFailed(
                ManagedTargetFailure::opening(&ClientError::OpenTimeout),
            ));
            continue;
        }

        let sent_at = Instant::now();
        let deadline = match sent_at.checked_add(config.open_timeouts[attempt]) {
            Some(deadline) => deadline,
            None => {
                target.mark_finished(ManagedTargetEndReason::OpenFailed(
                    ManagedTargetFailure::opening(&ClientError::DurationOverflow),
                ));
                continue;
            }
        };

        match socket.send_to(&target.open_request.bytes, target.remote) {
            Ok(_) => {
                target.status = TargetStatus::Opening {
                    attempt: attempt + 1,
                    next_send_at: deadline,
                    awaiting_receiver_generation: None,
                };
            }
            Err(err) => {
                target.mark_finished(ManagedTargetEndReason::OpenFailed(
                    ManagedTargetFailure::opening(&ClientError::Socket(err)),
                ));
            }
        }
    }
}

fn register_due_open_drains(
    shared: &GroupShared,
    targets: &[Arc<Mutex<TargetState>>],
    now: Instant,
) {
    // Register every currently due target while completion is excluded so one
    // empty-queue boundary can release the whole batch.
    let mut drain = shared
        .receiver_drain
        .lock()
        .expect("receiver drain mutex poisoned");
    for target in targets {
        let mut target = target.lock().expect("target mutex poisoned");
        let TargetStatus::Opening {
            attempt,
            next_send_at,
            awaiting_receiver_generation: None,
        } = target.status
        else {
            continue;
        };
        if attempt == 0 || next_send_at > now {
            continue;
        }
        match drain.request_or_join() {
            Ok(generation) => {
                target.status = TargetStatus::Opening {
                    attempt,
                    next_send_at,
                    awaiting_receiver_generation: Some(generation),
                };
            }
            Err(err) => target.mark_finished(ManagedTargetEndReason::OpenFailed(
                ManagedTargetFailure::opening(&err),
            )),
        }
    }
}

fn poll_active_timeouts(shared: &GroupShared, now: Instant) {
    for target in all_targets(shared) {
        let mut target = target.lock().expect("target mutex poisoned");
        if !matches!(
            target.status,
            TargetStatus::Active | TargetStatus::Draining { .. }
        ) {
            continue;
        }
        if target.runtime.is_peer_closed() {
            target.mark_finished(ManagedTargetEndReason::PeerClosed);
            continue;
        }
        match target.runtime.poll_timeouts_at(now) {
            Ok(events) => publish_events(&shared.hub, &mut target, events),
            Err(err) => target.mark_finished(ManagedTargetEndReason::Failed(
                ManagedTargetFailure::runtime(&err),
            )),
        }
    }
}

fn finish_completed_targets(socket: &UdpSocket, shared: &GroupShared, now: Instant) {
    for target in all_targets(shared) {
        let mut target = target.lock().expect("target mutex poisoned");
        match target.status {
            TargetStatus::Active if target.is_run_complete() => {
                if target.runtime.is_peer_closed() {
                    target.mark_finished(ManagedTargetEndReason::PeerClosed);
                } else if target.runtime.has_timed_out_metadata() {
                    target.status = TargetStatus::Draining {
                        deadline: now + GROUP_FINAL_DRAIN,
                    };
                } else {
                    close_locked_target(
                        socket,
                        &shared.hub,
                        &mut target,
                        ManagedTargetEndReason::TestComplete,
                    );
                }
            }
            TargetStatus::Draining { deadline } => {
                if target.runtime.is_peer_closed() {
                    target.mark_finished(ManagedTargetEndReason::PeerClosed);
                } else if now >= deadline || !target.runtime.has_timed_out_metadata() {
                    close_locked_target(
                        socket,
                        &shared.hub,
                        &mut target,
                        ManagedTargetEndReason::TestComplete,
                    );
                }
            }
            _ => {}
        }
    }
}

fn send_echo_to_target(
    socket: &UdpSocket,
    shared: &GroupShared,
    target: Arc<Mutex<TargetState>>,
    scheduled_at: Instant,
) {
    let mut target = target.lock().expect("target mutex poisoned");
    if !matches!(target.status, TargetStatus::Active) {
        return;
    }
    let result = send_echo_to_locked_target(socket, &mut target, scheduled_at);

    match result {
        Ok(events) => {
            // Publish while the target lock is still held. EventHub publish is
            // bounded and nonblocking, and this preserves per-target
            // EchoSent-before-EchoReply ordering for immediately returned UDP
            // replies.
            publish_events(&shared.hub, &mut target, events);
        }
        Err(err) => target.mark_finished(ManagedTargetEndReason::Failed(
            ManagedTargetFailure::runtime(&err),
        )),
    }
}

fn send_echo_to_locked_target(
    socket: &UdpSocket,
    target: &mut TargetState,
    scheduled_at: Instant,
) -> Result<Vec<ClientEvent>, ClientError> {
    send_echo_to_locked_target_inner(socket, target, scheduled_at)
}

#[cfg(test)]
fn send_echo_to_locked_target_at(
    socket: &UdpSocket,
    target: &mut TargetState,
    scheduled_at: Instant,
    timestamps: ProbeSendTimestamps,
) -> Result<Vec<ClientEvent>, ClientError> {
    target.probe_send_timestamps = Some(timestamps);
    send_echo_to_locked_target_inner(socket, target, scheduled_at)
}

fn send_echo_to_locked_target_inner(
    socket: &UdpSocket,
    target: &mut TargetState,
    scheduled_at: Instant,
) -> Result<Vec<ClientEvent>, ClientError> {
    target.runtime.ensure_open()?;
    let remote = target.remote;
    #[cfg(test)]
    let test_timestamps = target.probe_send_timestamps.take();
    #[cfg(test)]
    let reported_bytes = target.probe_reported_len.take();
    #[cfg(test)]
    let fail_send = std::mem::take(&mut target.probe_send_error);
    let TargetState {
        runtime, schedule, ..
    } = target;
    let schedule = schedule
        .as_mut()
        .expect("active targets always have a probe schedule");
    #[cfg(not(test))]
    let permission_at = Instant::now();
    #[cfg(test)]
    let permission_at = test_timestamps
        .map(|timestamps| timestamps.permission_at)
        .unwrap_or_else(Instant::now);
    if !schedule.permit_probe_at(permission_at) {
        return Ok(Vec::new());
    }

    let Some(prepared) = runtime.prepare_probe()? else {
        return Ok(Vec::new());
    };
    let machine_preflight = runtime.preflight_probe_commit(&prepared)?;
    let schedule_commit = schedule.preflight_managed_commit(scheduled_at, permission_at)?;
    let mut events = Vec::new();
    events
        .try_reserve(1)
        .map_err(|source| ClientError::AllocationFailed {
            operation: "managed probe event result",
            source,
        })?;

    let expected_bytes = prepared.bytes.len();
    let event_scheduled_at = schedule_commit.scheduled_at;
    #[cfg(not(test))]
    let sent_at = ClientTimestamp::now();
    #[cfg(test)]
    let sent_at = test_timestamps
        .map(|timestamps| timestamps.sent_at)
        .unwrap_or_else(ClientTimestamp::now);
    let machine_commit = runtime.finalize_probe_commit(machine_preflight, sent_at)?;
    #[cfg(not(test))]
    let send_call_start = Instant::now();
    #[cfg(test)]
    let send_call_start = test_timestamps
        .map(|timestamps| timestamps.send_call_start)
        .unwrap_or_else(Instant::now);
    #[cfg(test)]
    if fail_send {
        return Err(ClientError::Socket(std::io::Error::other(
            "injected managed probe send failure",
        )));
    }
    let bytes = socket.send_to(&prepared.bytes, remote)?;
    #[cfg(not(test))]
    let send_finished_at = Instant::now();
    #[cfg(test)]
    let send_finished_at = test_timestamps
        .map(|timestamps| timestamps.send_finished_at)
        .unwrap_or_else(Instant::now);
    let sent = runtime.commit_probe_sent(machine_commit, bytes);
    schedule.commit(schedule_commit);

    let send_call = send_finished_at.saturating_duration_since(send_call_start);
    let timer_error = instant_abs_diff(sent_at.mono, event_scheduled_at);
    #[cfg(test)]
    let bytes = reported_bytes.unwrap_or(bytes);
    validate_datagram_length(expected_bytes, bytes)?;
    events.push(echo_sent_event(
        remote,
        sent,
        send_call,
        event_scheduled_at,
        timer_error,
    ));
    Ok(events)
}

fn collect_finished_targets(shared: &GroupShared, records: &mut TargetOutcomeHistory) {
    let finished = {
        let mut registry = shared
            .registry
            .lock()
            .expect("target registry mutex poisoned");
        let ids: Vec<TargetId> = registry
            .targets
            .iter()
            .filter_map(|(id, target)| {
                let target = target.lock().expect("target mutex poisoned");
                matches!(target.status, TargetStatus::Finished).then(|| id.clone())
            })
            .collect();
        let mut finished = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(target) = registry.targets.remove(&id) {
                let remote = target.lock().expect("target mutex poisoned").remote;
                registry.remotes.remove(&remote);
                finished.push(target);
            }
        }
        finished
    };

    for target in finished {
        let target = target.lock().expect("target mutex poisoned");
        let reason = target.final_reason.clone().unwrap_or_else(|| {
            ManagedTargetEndReason::Failed(ManagedTargetFailure {
                kind: ManagedTargetFailureKind::InternalWorker,
                message: "target finished without a reason".to_owned(),
            })
        });
        record_target_outcome(shared, records, target.outcome(reason));
    }
}

fn cancel_remaining_targets(
    socket: &UdpSocket,
    shared: &GroupShared,
    records: &mut TargetOutcomeHistory,
) {
    let targets = drain_registry(shared);
    for target in targets {
        let outcome = close_target(
            socket,
            &shared.hub,
            target,
            ManagedTargetEndReason::Cancelled,
        );
        record_target_outcome(shared, records, outcome);
    }
}

fn record_target_outcome(
    shared: &GroupShared,
    records: &mut TargetOutcomeHistory,
    outcome: ManagedTargetOutcome,
) {
    if matches!(&outcome.end_reason, ManagedTargetEndReason::PeerClosed) {
        let _ = shared.peer_closed_target_count.fetch_update(
            Ordering::Release,
            Ordering::Relaxed,
            |count| Some(count.saturating_add(1)),
        );
    }
    shared
        .hub
        .publish(ManagedGroupEvent::TargetFinished(outcome.clone()));
    records.record(outcome);
}

fn fail_all_targets(shared: &GroupShared, failure: ManagedTargetFailure) {
    for target in all_targets(shared) {
        target
            .lock()
            .expect("target mutex poisoned")
            .mark_finished(ManagedTargetEndReason::Failed(failure.clone()));
    }
}

fn drain_registry(shared: &GroupShared) -> Vec<Arc<Mutex<TargetState>>> {
    let mut registry = shared
        .registry
        .lock()
        .expect("target registry mutex poisoned");
    registry.remotes.clear();
    registry.targets.drain().map(|(_, target)| target).collect()
}

fn close_target(
    socket: &UdpSocket,
    hub: &EventHub<ManagedGroupEvent>,
    target: Arc<Mutex<TargetState>>,
    reason: ManagedTargetEndReason,
) -> ManagedTargetOutcome {
    let mut target = target.lock().expect("target mutex poisoned");
    close_locked_target(socket, hub, &mut target, reason.clone());
    let reason = target.final_reason.clone().unwrap_or(reason);
    target.outcome(reason)
}

fn close_locked_target(
    socket: &UdpSocket,
    hub: &EventHub<ManagedGroupEvent>,
    target: &mut TargetState,
    reason: ManagedTargetEndReason,
) {
    if target.runtime.is_open() && !target.runtime.is_peer_closed() {
        let remote = target.remote;
        let prepared = match target.runtime.prepare_close() {
            Ok(prepared) => prepared,
            Err(err) => {
                target.mark_finished(ManagedTargetEndReason::Failed(
                    ManagedTargetFailure::runtime(&err),
                ));
                return;
            }
        };
        let mut events = Vec::new();
        #[cfg(test)]
        if std::mem::take(&mut target.close_event_reserve_error) {
            if let Err(source) = events.try_reserve(usize::MAX) {
                let err = ClientError::AllocationFailed {
                    operation: "managed close event result",
                    source,
                };
                target.mark_finished(ManagedTargetEndReason::Failed(
                    ManagedTargetFailure::runtime(&err),
                ));
                return;
            }
        }
        if let Err(source) = events.try_reserve(1) {
            let err = ClientError::AllocationFailed {
                operation: "managed close event result",
                source,
            };
            target.mark_finished(ManagedTargetEndReason::Failed(
                ManagedTargetFailure::runtime(&err),
            ));
            return;
        }
        let expected_bytes = prepared.bytes.len();
        #[cfg(test)]
        let reported_bytes = target.close_reported_len.take();
        #[cfg(test)]
        let fail_send = std::mem::take(&mut target.close_send_error);
        #[cfg(test)]
        {
            target.close_send_attempts += 1;
            if fail_send {
                let err = ClientError::Socket(std::io::Error::other(
                    "injected managed close send failure",
                ));
                target.mark_finished(ManagedTargetEndReason::Failed(
                    ManagedTargetFailure::runtime(&err),
                ));
                return;
            }
        }
        let bytes = match socket.send_to(prepared.bytes, remote) {
            Ok(bytes) => bytes,
            Err(err) => {
                let err = ClientError::Socket(err);
                target.mark_finished(ManagedTargetEndReason::Failed(
                    ManagedTargetFailure::runtime(&err),
                ));
                return;
            }
        };
        let event = target.runtime.commit_local_close(prepared.commit);
        target.schedule = None;
        #[cfg(test)]
        let bytes = reported_bytes.unwrap_or(bytes);
        if let Err(err) = validate_datagram_length(expected_bytes, bytes) {
            target.mark_finished(ManagedTargetEndReason::Failed(
                ManagedTargetFailure::runtime(&err),
            ));
            return;
        }
        events.push(event);
        publish_events(hub, target, events);
    }
    target.mark_finished(reason);
}

fn active_targets(shared: &GroupShared) -> Vec<Arc<Mutex<TargetState>>> {
    let mut targets: Vec<_> = all_targets(shared)
        .into_iter()
        .filter(|target| {
            let target = target.lock().expect("target mutex poisoned");
            matches!(target.status, TargetStatus::Active)
        })
        .collect();
    targets.sort_by_key(|target| target.lock().expect("target mutex poisoned").order);
    targets
}

fn all_targets(shared: &GroupShared) -> Vec<Arc<Mutex<TargetState>>> {
    shared
        .registry
        .lock()
        .expect("target registry mutex poisoned")
        .targets
        .values()
        .cloned()
        .collect()
}

fn group_should_complete(shared: &GroupShared, completion: ManagedGroupCompletionPolicy) -> bool {
    if completion == ManagedGroupCompletionPolicy::ExplicitCancellation {
        return false;
    }
    let registry = shared
        .registry
        .lock()
        .expect("target registry mutex poisoned");
    !registry.desired.is_empty() && registry.targets.is_empty()
}

#[derive(Debug)]
struct PacingRuntime {
    mode: ManagedGroupPacing,
    signature: Vec<TargetId>,
    next_slot_at: Option<Instant>,
    slot_index: usize,
    next_burst_at: Option<Instant>,
}

impl PacingRuntime {
    fn new(mode: ManagedGroupPacing) -> Self {
        Self {
            mode,
            signature: Vec::new(),
            next_slot_at: None,
            slot_index: 0,
            next_burst_at: None,
        }
    }

    fn reconcile(&mut self, active: &[Arc<Mutex<TargetState>>], now: Instant) {
        let signature: Vec<TargetId> = active
            .iter()
            .map(|target| target.lock().expect("target mutex poisoned").id.clone())
            .collect();
        if signature == self.signature {
            return;
        }
        self.signature = signature;
        self.slot_index = 0;
        if active.is_empty() {
            self.next_slot_at = None;
            self.next_burst_at = None;
        } else {
            match self.mode {
                ManagedGroupPacing::Staggered => self.next_slot_at = Some(now),
                ManagedGroupPacing::Burst => {
                    self.next_burst_at.get_or_insert(now);
                }
            }
        }
    }

    fn send_due(&self, active: &[Arc<Mutex<TargetState>>], now: Instant) -> bool {
        if active.is_empty() {
            return false;
        }
        match self.mode {
            ManagedGroupPacing::Staggered => {
                self.next_slot_at.is_some_and(|deadline| deadline <= now)
            }
            ManagedGroupPacing::Burst => self.next_burst_at.is_some_and(|deadline| deadline <= now),
        }
    }

    fn next_staggered(
        &mut self,
        active: &[Arc<Mutex<TargetState>>],
        interval: Duration,
        now: Instant,
    ) -> Result<Option<ScheduledTarget>, ClientError> {
        let Some(next_slot_at) = self.next_slot_at else {
            return Ok(None);
        };
        if active.is_empty() {
            self.next_slot_at = None;
            return Ok(None);
        }
        let (scheduled_at, next_slot_at, first_index) =
            advance_staggered_slot(next_slot_at, interval, active.len(), self.slot_index, now)?;
        self.next_slot_at = Some(next_slot_at);

        for offset in 0..active.len() {
            let index = (first_index + offset) % active.len();
            if !target_is_due_for_slot(&active[index], scheduled_at) {
                continue;
            }
            let target = active[index].clone();
            self.slot_index = (index + 1) % active.len();
            return Ok(Some((target, scheduled_at)));
        }

        self.slot_index = (first_index + 1) % active.len();
        if let Some(target_deadline) = active.iter().filter_map(target_next_send_deadline).min() {
            self.next_slot_at = Some(self.next_slot_at.map_or(target_deadline, |group_deadline| {
                group_deadline.min(target_deadline)
            }));
        }
        Ok(None)
    }

    fn next_burst(
        &mut self,
        interval: Duration,
        now: Instant,
    ) -> Result<Option<Instant>, ClientError> {
        let Some(next_burst_at) = self.next_burst_at else {
            return Ok(None);
        };
        let (scheduled_at, next_burst_at, _) = advance_cadence(next_burst_at, interval, now)?;
        self.next_burst_at = Some(next_burst_at);
        Ok(Some(scheduled_at))
    }

    fn next_wakeup(&self) -> Option<Instant> {
        match self.mode {
            ManagedGroupPacing::Staggered => self.next_slot_at,
            ManagedGroupPacing::Burst => self.next_burst_at,
        }
    }
}

fn advance_staggered_slot(
    next_slot_at: Instant,
    interval: Duration,
    active_len: usize,
    slot_index: usize,
    now: Instant,
) -> Result<(Instant, Instant, usize), ClientError> {
    debug_assert!(active_len > 0);
    let spacing = divide_duration(interval, active_len);
    let (scheduled_at, following_slot_at, skipped_slots) =
        advance_cadence(next_slot_at, spacing, now)?;
    let skipped_mod = usize::try_from(skipped_slots % active_len as u128)
        .expect("staggered slot remainder fits usize");
    let first_index = (slot_index + skipped_mod) % active_len;
    Ok((scheduled_at, following_slot_at, first_index))
}

fn target_is_due_for_slot(target: &Arc<Mutex<TargetState>>, scheduled_at: Instant) -> bool {
    target_next_send_deadline(target).is_some_and(|deadline| deadline <= scheduled_at)
}

fn target_next_send_deadline(target: &Arc<Mutex<TargetState>>) -> Option<Instant> {
    let target = target.lock().expect("target mutex poisoned");
    if matches!(target.status, TargetStatus::Active) {
        target.schedule.as_ref()?.next_send_deadline()
    } else {
        None
    }
}

fn wait_for_next_scheduler_wakeup(
    control_rx: &mpsc::Receiver<ControlMessage>,
    shared: &GroupShared,
    pacing: &PacingRuntime,
    active: &[Arc<Mutex<TargetState>>],
) -> Option<ControlMessage> {
    let open_deadline = next_open_deadline(shared);
    let pacing_deadline = (!active.is_empty()).then(|| pacing.next_wakeup()).flatten();
    let drain_deadline = next_drain_deadline(shared);
    let deadline = [open_deadline, pacing_deadline, drain_deadline]
        .into_iter()
        .flatten()
        .min();

    wait_for_scheduler_control(control_rx, &shared.cancellation, deadline)
}

fn wait_for_scheduler_control(
    control_rx: &mpsc::Receiver<ControlMessage>,
    cancellation: &CancellationToken,
    deadline: Option<Instant>,
) -> Option<ControlMessage> {
    match deadline {
        Some(deadline) => {
            let sleep_for = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or_default()
                .min(MAX_SLEEP);
            match control_rx.recv_timeout(sleep_for) {
                Ok(message) => Some(message),
                Err(mpsc::RecvTimeoutError::Timeout) => None,
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    cancellation.cancel();
                    None
                }
            }
        }
        None => match control_rx.recv() {
            Ok(message) => Some(message),
            Err(_) => {
                cancellation.cancel();
                None
            }
        },
    }
}

fn next_open_deadline(shared: &GroupShared) -> Option<Instant> {
    all_targets(shared)
        .into_iter()
        .filter_map(|target| {
            let target = target.lock().expect("target mutex poisoned");
            match target.status {
                TargetStatus::Opening {
                    next_send_at,
                    awaiting_receiver_generation: None,
                    ..
                } => Some(next_send_at),
                _ => None,
            }
        })
        .min()
}

fn next_drain_deadline(shared: &GroupShared) -> Option<Instant> {
    all_targets(shared)
        .into_iter()
        .filter_map(|target| {
            let target = target.lock().expect("target mutex poisoned");
            match target.status {
                TargetStatus::Draining { deadline } => Some(deadline),
                _ => None,
            }
        })
        .min()
}

fn divide_duration(duration: Duration, divisor: usize) -> Duration {
    if divisor == 0 {
        return duration;
    }
    let nanos = (duration.as_nanos() / divisor as u128).max(1);
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::NegotiationPolicy, managed::SubscriberOverflow, EventSubscriptionError, WarningKind,
    };
    use irtt_proto::{
        compute_hmac_in_place, echo_packet_len,
        flags::{self, FLAG_OPEN, FLAG_REPLY},
        layout::PacketLayout,
        Clock, OpenReply, Params, ReceivedStats, StampAt, TimestampFields, HMAC_SIZE, MAGIC,
        PROTOCOL_VERSION,
    };
    use std::sync::mpsc;

    const TOKEN: u64 = 0x1234_5678_90ab_cdef;

    fn probe_timestamps(permission_at: Instant, sent_at: ClientTimestamp) -> ProbeSendTimestamps {
        ProbeSendTimestamps {
            permission_at,
            sent_at,
            send_call_start: sent_at.mono,
            send_finished_at: sent_at.mono,
        }
    }

    #[derive(Debug)]
    struct FakeServer {
        addr: SocketAddr,
        _observations: mpsc::Receiver<ServerObservation>,
        done: JoinHandle<()>,
    }

    impl FakeServer {
        fn join(self) {
            self.done.join().unwrap();
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ServerObservation {
        Open { at: Instant },
        Echo { seq: u32, at: Instant },
        Close { at: Instant },
    }

    fn test_params(duration: Option<Duration>, interval: Duration) -> Params {
        Params {
            protocol_version: PROTOCOL_VERSION,
            duration_ns: duration.map_or(0, duration_ns_i64),
            interval_ns: duration_ns_i64(interval),
            length: 0,
            received_stats: ReceivedStats::Both,
            stamp_at: StampAt::Both,
            clock: Clock::Both,
            dscp: 0,
            server_fill: None,
        }
    }

    fn duration_ns_i64(duration: Duration) -> i64 {
        i64::try_from(duration.as_nanos()).expect("test duration fits i64 nanoseconds")
    }

    fn group_config(
        duration: Option<Duration>,
        interval: Duration,
        pacing: ManagedGroupPacing,
    ) -> ManagedClientGroupConfig {
        ManagedClientGroupConfig {
            client: ClientConfig {
                server_addr: "127.0.0.1:1".to_owned(),
                duration,
                interval,
                negotiation_policy: NegotiationPolicy::Strict,
                open_timeouts: vec![Duration::from_millis(200)],
                probe_timeout: Duration::from_millis(80),
                socket_config: crate::SocketConfig {
                    recv_timeout: Some(Duration::from_millis(20)),
                    ..Default::default()
                },
                ..ClientConfig::default()
            },
            pacing,
            completion: ManagedGroupCompletionPolicy::AllTargetsComplete,
        }
    }

    struct ActiveTargetFixture {
        client_config: ClientConfig,
        hub: EventHub<ManagedGroupEvent>,
        subscription: TargetEventSubscription,
        target: Arc<Mutex<TargetState>>,
        reply: Vec<u8>,
        close_reply: Vec<u8>,
    }

    fn active_target_fixture() -> ActiveTargetFixture {
        active_target_fixture_at("127.0.0.1:2112".parse().unwrap())
    }

    fn active_target_fixture_at(remote: SocketAddr) -> ActiveTargetFixture {
        let interval = Duration::from_millis(10);
        let params = test_params(None, interval);
        let client_config = group_config(None, interval, ManagedGroupPacing::Staggered).client;
        let hub = EventHub::new();
        let subscription = hub
            .subscribe(SubscriberConfig {
                capacity: 16,
                overflow: SubscriberOverflow::DropNewest,
            })
            .unwrap();
        let mut target =
            TargetState::new(&client_config, target("peer", remote), 0, Instant::now()).unwrap();

        let open_packet = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params);
        let open_reply = match target.runtime.inspect_open_datagram(&open_packet).unwrap() {
            OpenDatagramDisposition::Trusted(reply) => reply,
            OpenDatagramDisposition::Ignore => panic!("fixture open reply must be trusted"),
        };
        let opened_at = ClientTimestamp::now();
        let prepared = target
            .runtime
            .prepare_open_acceptance(open_reply, opened_at)
            .unwrap();
        let opened_schedule = Some(
            ProbeSchedule::new(opened_at.mono, prepared.normal_negotiated().unwrap()).unwrap(),
        );
        let open_outcome = target.runtime.commit_open(prepared);
        target.schedule = opened_schedule;
        publish_open_outcome(&hub, &mut target, open_outcome);

        let scheduled_at = Instant::now();
        let sent_at = ClientTimestamp::now();
        let sent_events = {
            let remote = target.remote;
            let TargetState {
                runtime, schedule, ..
            } = &mut target;
            let schedule = schedule.as_mut().unwrap();
            assert!(schedule.permit_probe_at(sent_at.mono));
            let prepared = runtime.prepare_probe().unwrap().unwrap();
            let machine_preflight = runtime.preflight_probe_commit(&prepared).unwrap();
            let schedule_commit = schedule
                .preflight_managed_commit(scheduled_at, sent_at.mono)
                .unwrap();
            let event_scheduled_at = schedule_commit.scheduled_at;
            let machine_commit = runtime
                .finalize_probe_commit(machine_preflight, sent_at)
                .unwrap();
            let sent = runtime.commit_probe_sent(machine_commit, prepared.bytes.len());
            schedule.commit(schedule_commit);
            let timer_error = instant_abs_diff(sent_at.mono, event_scheduled_at);
            vec![echo_sent_event(
                remote,
                sent,
                Duration::ZERO,
                event_scheduled_at,
                timer_error,
            )]
        };
        publish_events(&hub, &mut target, sent_events);

        ActiveTargetFixture {
            client_config,
            hub,
            subscription,
            target: Arc::new(Mutex::new(target)),
            reply: echo_reply_packet(TOKEN, 0, &params, &TimestampFields::default()),
            close_reply: echo_reply_packet_with_flags(
                TOKEN,
                0,
                &params,
                &TimestampFields::default(),
                FLAG_REPLY | flags::FLAG_CLOSE,
            ),
        }
    }

    fn shared_with_target(
        client_config: &ClientConfig,
        hub: EventHub<ManagedGroupEvent>,
        target: Arc<Mutex<TargetState>>,
    ) -> GroupShared {
        shared_with_targets(client_config, hub, vec![target]).0
    }

    fn shared_with_targets(
        client_config: &ClientConfig,
        hub: EventHub<ManagedGroupEvent>,
        targets: Vec<Arc<Mutex<TargetState>>>,
    ) -> (GroupShared, mpsc::Receiver<ControlMessage>) {
        let requested = params_from_config(client_config).unwrap();
        let mut registry = TargetRegistry::default();
        let mut family_remote = None;
        for target in targets {
            let (id, remote) = {
                let target = target.lock().expect("target mutex poisoned");
                (target.id.clone(), target.remote)
            };
            family_remote.get_or_insert(remote);
            registry.desired.insert(id.clone());
            registry.remotes.insert(remote, id.clone());
            registry.targets.insert(id, target);
        }
        let (control_tx, control_rx) = mpsc::channel();

        (
            GroupShared {
                registry: Mutex::new(registry),
                hub,
                cancellation: CancellationToken::new(),
                control_tx,
                peer_closed_target_count: Arc::new(AtomicU64::new(0)),
                receiver_drain: Mutex::new(ReceiverDrainState::default()),
                requested_interval_ns: requested.interval_ns,
                requested_dscp: requested.dscp,
                family_remote: family_remote.expect("test shared state requires a target"),
            },
            control_rx,
        )
    }

    fn drain_available_group_events(
        subscription: &TargetEventSubscription,
    ) -> Vec<ManagedGroupEvent> {
        let mut events = Vec::new();
        while let Some(event) = subscription.try_recv().unwrap() {
            events.push(event);
        }
        events
    }

    fn target(id: &str, remote: SocketAddr) -> ManagedTargetConfig {
        ManagedTargetConfig {
            id: TargetId::from(id),
            remote,
            auth: None,
        }
    }

    fn active_probe_target(
        config: &ClientConfig,
        params: &Params,
        remote: SocketAddr,
        opened_at: ClientTimestamp,
    ) -> TargetState {
        let mut target =
            TargetState::new(config, target("peer", remote), 0, opened_at.mono).unwrap();
        let packet = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, params);
        let reply = match target.runtime.inspect_open_datagram(&packet).unwrap() {
            OpenDatagramDisposition::Trusted(reply) => reply,
            OpenDatagramDisposition::Ignore => panic!("test open reply must be trusted"),
        };
        let prepared = target
            .runtime
            .prepare_open_acceptance(reply, opened_at)
            .unwrap();
        target.schedule = Some(
            ProbeSchedule::new(opened_at.mono, prepared.normal_negotiated().unwrap()).unwrap(),
        );
        target.runtime.commit_open(prepared);
        target.status = TargetStatus::Active;
        target
    }

    fn target_outcome(
        id: &str,
        remote: SocketAddr,
        end_reason: ManagedTargetEndReason,
    ) -> ManagedTargetOutcome {
        ManagedTargetOutcome {
            id: TargetId::from(id),
            remote,
            end_reason,
            packets_sent: 0,
            replies_received: 0,
            duplicates: 0,
            late: 0,
            warning_events: 0,
        }
    }

    fn start_echo_server(params: Params, open_delay: Duration) -> FakeServer {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = socket.local_addr().unwrap();
        let (tx, rx) = mpsc::channel();
        let done = thread::spawn(move || {
            socket
                .set_read_timeout(Some(Duration::from_millis(800)))
                .unwrap();
            let mut opened = false;
            while let Some((packet, peer)) = recv_request_timeout(&socket) {
                if packet[3] & FLAG_OPEN != 0 {
                    if !opened {
                        opened = true;
                        tx.send(ServerObservation::Open { at: Instant::now() })
                            .unwrap();
                        if open_delay > Duration::ZERO {
                            thread::sleep(open_delay);
                        }
                    }
                    socket
                        .send_to(&open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params), peer)
                        .unwrap();
                    continue;
                }

                if packet[3] & flags::FLAG_CLOSE != 0 {
                    tx.send(ServerObservation::Close { at: Instant::now() })
                        .unwrap();
                    break;
                }

                let seq = u32::from_le_bytes(packet[12..16].try_into().unwrap());
                tx.send(ServerObservation::Echo {
                    seq,
                    at: Instant::now(),
                })
                .unwrap();
                let ts = TimestampFields {
                    recv_wall: Some(1_000_000_000),
                    recv_mono: Some(100_000),
                    send_wall: Some(1_000_000_000),
                    send_mono: Some(100_000),
                    ..Default::default()
                };
                socket
                    .send_to(&echo_reply_packet(TOKEN, seq, &params, &ts), peer)
                    .unwrap();
            }
        });
        FakeServer {
            addr,
            _observations: rx,
            done,
        }
    }

    fn start_peer_close_server(params: Params) -> FakeServer {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = socket.local_addr().unwrap();
        let (tx, rx) = mpsc::channel();
        let done = thread::spawn(move || {
            let (open, peer) = recv_request_timeout(&socket).expect("missing open request");
            assert_ne!(open[3] & FLAG_OPEN, 0);
            tx.send(ServerObservation::Open { at: Instant::now() })
                .unwrap();
            socket
                .send_to(&open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params), peer)
                .unwrap();

            let (echo, peer) = recv_request_timeout(&socket).expect("missing echo request");
            let seq = u32::from_le_bytes(echo[12..16].try_into().unwrap());
            tx.send(ServerObservation::Echo {
                seq,
                at: Instant::now(),
            })
            .unwrap();
            socket
                .send_to(
                    &echo_reply_packet_with_flags(
                        TOKEN,
                        seq,
                        &params,
                        &TimestampFields::default(),
                        FLAG_REPLY | flags::FLAG_CLOSE,
                    ),
                    peer,
                )
                .unwrap();
        });
        FakeServer {
            addr,
            _observations: rx,
            done,
        }
    }

    fn start_open_failure_server(params: Params) -> FakeServer {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = socket.local_addr().unwrap();
        let (tx, rx) = mpsc::channel();
        let done = thread::spawn(move || {
            let (open, peer) = recv_request_timeout(&socket).expect("missing open request");
            assert_ne!(open[3] & FLAG_OPEN, 0);
            tx.send(ServerObservation::Open { at: Instant::now() })
                .unwrap();
            socket
                .send_to(&open_reply(FLAG_OPEN | FLAG_REPLY, 0, &params), peer)
                .unwrap();
        });
        FakeServer {
            addr,
            _observations: rx,
            done,
        }
    }

    fn start_no_test_server(params: Params) -> FakeServer {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = socket.local_addr().unwrap();
        let (tx, rx) = mpsc::channel();
        let done = thread::spawn(move || {
            let (open, peer) = recv_request_timeout(&socket).expect("missing open request");
            assert_ne!(open[3] & flags::FLAG_CLOSE, 0);
            tx.send(ServerObservation::Open { at: Instant::now() })
                .unwrap();
            socket
                .send_to(
                    &open_reply(FLAG_OPEN | FLAG_REPLY | flags::FLAG_CLOSE, 0, &params),
                    peer,
                )
                .unwrap();
        });
        FakeServer {
            addr,
            _observations: rx,
            done,
        }
    }

    fn start_silent_runtime_server(params: Params) -> FakeServer {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = socket.local_addr().unwrap();
        let (tx, rx) = mpsc::channel();
        let done = thread::spawn(move || {
            let (open, peer) = recv_request_timeout(&socket).expect("missing open request");
            assert_ne!(open[3] & FLAG_OPEN, 0);
            tx.send(ServerObservation::Open { at: Instant::now() })
                .unwrap();
            socket
                .send_to(&open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params), peer)
                .unwrap();
            socket
                .set_read_timeout(Some(Duration::from_millis(800)))
                .unwrap();
            while let Some((packet, _)) = recv_request_timeout(&socket) {
                if packet[3] & flags::FLAG_CLOSE != 0 {
                    tx.send(ServerObservation::Close { at: Instant::now() })
                        .unwrap();
                    break;
                }
                let seq = u32::from_le_bytes(packet[12..16].try_into().unwrap());
                tx.send(ServerObservation::Echo {
                    seq,
                    at: Instant::now(),
                })
                .unwrap();
            }
        });
        FakeServer {
            addr,
            _observations: rx,
            done,
        }
    }

    fn start_gated_reply_server(params: Params) -> (FakeServer, mpsc::SyncSender<()>) {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = socket.local_addr().unwrap();
        let (tx, rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let done = thread::spawn(move || {
            let (open, peer) = recv_request_timeout(&socket).expect("missing open request");
            assert_ne!(open[3] & FLAG_OPEN, 0);
            tx.send(ServerObservation::Open { at: Instant::now() })
                .unwrap();
            socket
                .send_to(&open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params), peer)
                .unwrap();
            socket
                .set_read_timeout(Some(Duration::from_millis(800)))
                .unwrap();

            let (echo, peer) = recv_request_timeout(&socket).expect("missing echo request");
            assert_eq!(echo[3] & flags::FLAG_CLOSE, 0);
            assert!(echo.len() >= 16, "echo request too short: {}", echo.len());
            let seq = u32::from_le_bytes(echo[12..16].try_into().unwrap());
            tx.send(ServerObservation::Echo {
                seq,
                at: Instant::now(),
            })
            .unwrap();
            release_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("timed out waiting to release delayed reply");

            let timestamps = TimestampFields {
                recv_wall: Some(1_000_000_000),
                recv_mono: Some(100_000),
                send_wall: Some(1_001_000_000),
                send_mono: Some(1_100_000),
                ..Default::default()
            };
            socket
                .send_to(&echo_reply_packet(TOKEN, seq, &params, &timestamps), peer)
                .unwrap();

            while let Some((packet, _)) = recv_request_timeout(&socket) {
                if packet[3] & flags::FLAG_CLOSE != 0 {
                    tx.send(ServerObservation::Close { at: Instant::now() })
                        .unwrap();
                    break;
                }
            }
        });
        (
            FakeServer {
                addr,
                _observations: rx,
                done,
            },
            release_tx,
        )
    }

    fn recv_request_timeout(socket: &UdpSocket) -> Option<(Vec<u8>, SocketAddr)> {
        let mut buf = [0_u8; 2048];
        socket
            .recv_from(&mut buf)
            .ok()
            .map(|(size, peer)| (buf[..size].to_vec(), peer))
    }

    fn recv_group_datagram(socket: &UdpSocket) -> (ReceivedDatagramFrom, Vec<u8>) {
        let mut packet = vec![0_u8; RECV_BUFFER_SIZE];
        let datagram = recv_datagram_from(socket, &mut packet).unwrap();
        packet.truncate(datagram.len);
        (datagram, packet)
    }

    fn open_reply(flags: u8, token: u64, params: &Params) -> Vec<u8> {
        let mut packet = Vec::new();
        packet.extend_from_slice(&MAGIC);
        packet.push(flags);
        packet.extend_from_slice(&token.to_le_bytes());
        packet.extend_from_slice(&params.encode());
        packet
    }

    fn hmac_open_reply(flags: u8, token: u64, params: &Params, key: &[u8]) -> Vec<u8> {
        let mut packet = Vec::new();
        packet.extend_from_slice(&MAGIC);
        packet.push(flags | flags::FLAG_HMAC);
        packet.extend_from_slice(&[0_u8; HMAC_SIZE]);
        packet.extend_from_slice(&token.to_le_bytes());
        packet.extend_from_slice(&params.encode());
        compute_hmac_in_place(key, &mut packet, 4).unwrap();
        packet
    }

    fn echo_reply_packet(
        token: u64,
        seq: u32,
        params: &Params,
        timestamps: &TimestampFields,
    ) -> Vec<u8> {
        echo_reply_packet_with_flags(token, seq, params, timestamps, FLAG_REPLY)
    }

    fn echo_reply_packet_with_flags(
        token: u64,
        seq: u32,
        params: &Params,
        timestamps: &TimestampFields,
        flags: u8,
    ) -> Vec<u8> {
        let layout = PacketLayout::echo(false, params);
        let packet_len = echo_packet_len(false, params).unwrap();
        let mut packet = Vec::with_capacity(packet_len);

        packet.extend_from_slice(&MAGIC);
        packet.push(flags);
        packet.extend_from_slice(&token.to_le_bytes());
        packet.extend_from_slice(&seq.to_le_bytes());

        if layout.recv_count {
            packet.extend_from_slice(&42_u32.to_le_bytes());
        }
        if layout.recv_window {
            packet.extend_from_slice(&0x07_u64.to_le_bytes());
        }
        if layout.recv_wall {
            packet.extend_from_slice(&timestamps.recv_wall.unwrap_or(0).to_le_bytes());
        }
        if layout.recv_mono {
            packet.extend_from_slice(&timestamps.recv_mono.unwrap_or(0).to_le_bytes());
        }
        if layout.midpoint_wall {
            packet.extend_from_slice(&timestamps.midpoint_wall.unwrap_or(0).to_le_bytes());
        }
        if layout.midpoint_mono {
            packet.extend_from_slice(&timestamps.midpoint_mono.unwrap_or(0).to_le_bytes());
        }
        if layout.send_wall {
            packet.extend_from_slice(&timestamps.send_wall.unwrap_or(0).to_le_bytes());
        }
        if layout.send_mono {
            packet.extend_from_slice(&timestamps.send_mono.unwrap_or(0).to_le_bytes());
        }

        packet.resize(packet_len, 0);
        packet
    }

    fn recv_event_with_timeout(sub: &TargetEventSubscription) -> TargetEvent {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match sub.try_recv() {
                Ok(Some(ManagedGroupEvent::Client(event))) => return event,
                Ok(Some(ManagedGroupEvent::TargetFinished(_))) => {}
                Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(1)),
                Ok(None) => panic!("timed out waiting for group event"),
                Err(err) => panic!("subscription ended while waiting for event: {err}"),
            }
        }
    }

    fn drain_after_join(sub: &TargetEventSubscription) -> Vec<TargetEvent> {
        let mut events = Vec::new();
        loop {
            match sub.try_recv() {
                Ok(Some(ManagedGroupEvent::Client(event))) => events.push(event),
                Ok(Some(ManagedGroupEvent::TargetFinished(_))) => {}
                Ok(None) => thread::sleep(Duration::from_millis(1)),
                Err(EventSubscriptionError::Disconnected) => return events,
            }
        }
    }

    fn collect_group_events_until_disconnected(
        sub: &TargetEventSubscription,
    ) -> Vec<ManagedGroupEvent> {
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut events = Vec::new();
        loop {
            match sub.try_recv() {
                Ok(Some(event)) => events.push(event),
                Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(1)),
                Ok(None) => panic!("timed out waiting for managed group disconnection"),
                Err(EventSubscriptionError::Disconnected) => return events,
            }
        }
    }

    fn final_drain_event_sequence(events: &[ManagedGroupEvent]) -> Vec<&'static str> {
        events
            .iter()
            .filter_map(|event| match event {
                ManagedGroupEvent::Client(TargetEvent {
                    event: ClientEvent::SessionStarted { .. },
                    ..
                }) => Some("started"),
                ManagedGroupEvent::Client(TargetEvent {
                    event: ClientEvent::EchoSent { seq: 0, .. },
                    ..
                }) => Some("sent"),
                ManagedGroupEvent::Client(TargetEvent {
                    event: ClientEvent::EchoLoss { seq: 0, .. },
                    ..
                }) => Some("loss"),
                ManagedGroupEvent::Client(TargetEvent {
                    event: ClientEvent::LateReply { seq: 0, .. },
                    ..
                }) => Some("late"),
                ManagedGroupEvent::Client(TargetEvent {
                    event: ClientEvent::SessionClosed { .. },
                    ..
                }) => Some("closed"),
                ManagedGroupEvent::TargetFinished(_) => Some("finished"),
                _ => None,
            })
            .collect()
    }

    fn join_group_with_timeout(
        session: ManagedClientGroupSession,
        timeout: Duration,
    ) -> ManagedGroupOutcome {
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let joiner = thread::spawn(move || {
            result_tx.send(session.join()).unwrap();
        });
        let outcome = result_rx
            .recv_timeout(timeout)
            .expect("timed out joining managed group")
            .unwrap();
        joiner.join().unwrap();
        outcome
    }

    #[test]
    fn target_event_hub_uses_generic_overflow_behavior() {
        let hub = EventHub::<TargetEvent>::new();
        let sub = hub
            .subscribe(SubscriberConfig {
                capacity: 1,
                overflow: SubscriberOverflow::DropOldest,
            })
            .unwrap();

        hub.publish(TargetEvent {
            target: TargetId::from("a"),
            event: ClientEvent::Warning {
                kind: WarningKind::UntrackedReply,
                message: "old".to_owned(),
                at: ClientTimestamp::now(),
            },
        });
        hub.publish(TargetEvent {
            target: TargetId::from("b"),
            event: ClientEvent::Warning {
                kind: WarningKind::UntrackedReply,
                message: "new".to_owned(),
                at: ClientTimestamp::now(),
            },
        });

        let event = sub.try_recv().unwrap().unwrap();
        assert_eq!(event.target, TargetId::from("b"));
        assert!(sub.try_recv().unwrap().is_none());
    }

    #[test]
    fn rejects_duplicate_resolved_remote_targets() {
        let remote: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        let err = ManagedClientGroup::start(
            group_config(
                Some(Duration::from_millis(10)),
                Duration::from_millis(10),
                ManagedGroupPacing::Staggered,
            ),
            vec![target("a", remote), target("b", remote)],
        )
        .unwrap_err();

        assert!(matches!(err, ClientError::InvalidConfig { .. }));
        assert!(err.to_string().contains("duplicate managed target remote"));
    }

    #[test]
    fn rejects_mixed_address_families() {
        let err = ManagedClientGroup::start(
            group_config(
                Some(Duration::from_millis(10)),
                Duration::from_millis(10),
                ManagedGroupPacing::Staggered,
            ),
            vec![
                target("a", "127.0.0.1:12345".parse().unwrap()),
                target("b", "[::1]:12345".parse().unwrap()),
            ],
        )
        .unwrap_err();

        assert!(matches!(err, ClientError::InvalidConfig { .. }));
        assert!(err.to_string().contains("one address family"));
    }

    #[test]
    fn shared_socket_smoke_two_targets_emit_scoped_events() {
        let duration = Duration::from_millis(70);
        let interval = Duration::from_millis(20);
        let params = test_params(Some(duration), interval);
        let a = start_echo_server(params.clone(), Duration::ZERO);
        let b = start_echo_server(params, Duration::ZERO);

        let (session, sub) = ManagedClientGroup::start_with_subscription(
            group_config(Some(duration), interval, ManagedGroupPacing::Staggered),
            vec![target("a", a.addr), target("b", b.addr)],
            SubscriberConfig {
                capacity: 256,
                overflow: SubscriberOverflow::DropNewest,
            },
        )
        .unwrap();

        let outcome = session.join().unwrap();
        let events = drain_after_join(&sub);
        a.join();
        b.join();

        assert_eq!(
            outcome.end_reason,
            ManagedGroupEndReason::AllTargetsComplete
        );
        assert_eq!(outcome.targets.len(), 2);
        assert!(outcome
            .targets
            .iter()
            .all(|target| target.packets_sent > 0 && target.replies_received > 0));
        for id in [TargetId::from("a"), TargetId::from("b")] {
            assert!(events.iter().any(|event| {
                event.target == id && matches!(event.event, ClientEvent::SessionStarted { .. })
            }));
            assert!(events.iter().any(|event| {
                event.target == id && matches!(event.event, ClientEvent::EchoSent { .. })
            }));
            assert!(events.iter().any(|event| {
                event.target == id && matches!(event.event, ClientEvent::EchoReply { .. })
            }));
        }
    }

    #[test]
    fn receiver_peer_close_wins_send_and_removal_races() {
        let fixture = active_target_fixture();
        let shared = shared_with_target(
            &fixture.client_config,
            fixture.hub.clone(),
            fixture.target.clone(),
        );
        {
            let mut target = fixture.target.lock().expect("target mutex poisoned");
            assert!(process_active_target_packet(
                &fixture.hub,
                &mut target,
                &fixture.close_reply,
                ClientTimestamp::now(),
                ReceiveMeta::default(),
            ));
            assert!(matches!(target.status, TargetStatus::Finished));
            assert!(target.schedule.is_none());
            assert_eq!(
                target.final_reason,
                Some(ManagedTargetEndReason::PeerClosed)
            );
        }
        assert_eq!(shared.peer_closed_target_count.load(Ordering::Acquire), 0);
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();

        send_echo_to_target(&socket, &shared, fixture.target.clone(), Instant::now());
        assert_eq!(
            fixture
                .target
                .lock()
                .expect("target mutex poisoned")
                .final_reason,
            Some(ManagedTargetEndReason::PeerClosed)
        );
        assert_eq!(shared.peer_closed_target_count.load(Ordering::Acquire), 0);

        let mut next_order = 1;
        let mut records = TargetOutcomeHistory::default();
        apply_target_update(
            &fixture.client_config,
            &socket,
            &shared,
            Vec::new(),
            &mut next_order,
            &mut records,
        )
        .unwrap();
        assert_eq!(shared.peer_closed_target_count.load(Ordering::Acquire), 1);
        collect_finished_targets(&shared, &mut records);
        assert_eq!(shared.peer_closed_target_count.load(Ordering::Acquire), 1);

        let events = drain_available_group_events(&fixture.subscription);
        let terminal_sequence: Vec<&str> = events
            .iter()
            .filter_map(|event| match event {
                ManagedGroupEvent::Client(TargetEvent {
                    event: ClientEvent::EchoReply { .. },
                    ..
                }) => Some("reply"),
                ManagedGroupEvent::Client(TargetEvent {
                    event: ClientEvent::SessionClosed { .. },
                    ..
                }) => Some("closed"),
                ManagedGroupEvent::TargetFinished(_) => Some("finished"),
                _ => None,
            })
            .collect();
        assert_eq!(terminal_sequence, ["reply", "closed", "finished"]);

        let outcome = records.into_group_outcome(ManagedGroupEndReason::AllTargetsComplete);
        assert_eq!(outcome.total_target_outcomes, 1);
        assert_eq!(outcome.successful_target_outcomes, 1);
        assert_eq!(outcome.peer_closed_target_outcomes, 1);
        assert_eq!(outcome.failed_target_outcomes, 0);
        assert_eq!(
            outcome.targets[0].end_reason,
            ManagedTargetEndReason::PeerClosed
        );
        assert_eq!(outcome.targets[0].packets_sent, 1);
        assert_eq!(outcome.targets[0].replies_received, 1);
    }

    #[test]
    fn managed_close_success_commits_clears_schedule_and_publishes_once() {
        let peer = UdpSocket::bind("127.0.0.1:0").unwrap();
        peer.set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();
        let fixture = active_target_fixture_at(peer.local_addr().unwrap());
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();

        {
            let mut target = fixture.target.lock().expect("target mutex poisoned");
            close_locked_target(
                &socket,
                &fixture.hub,
                &mut target,
                ManagedTargetEndReason::TestComplete,
            );
            assert!(!target.runtime.is_open());
            assert!(target.schedule.is_none());
            assert_eq!(target.close_send_attempts, 1);
            assert_eq!(
                target.final_reason,
                Some(ManagedTargetEndReason::TestComplete)
            );
        }

        let mut packet = [0_u8; 512];
        let (len, _) = peer.recv_from(&mut packet).unwrap();
        assert_eq!(packet[3], flags::FLAG_CLOSE);
        assert_eq!(u64::from_le_bytes(packet[4..12].try_into().unwrap()), TOKEN);
        assert_eq!(len, 12);
        let events = drain_available_group_events(&fixture.subscription);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event,
                    ManagedGroupEvent::Client(TargetEvent {
                        event: ClientEvent::SessionClosed { token: TOKEN, .. },
                        ..
                    })
                ))
                .count(),
            1
        );
    }

    #[test]
    fn managed_removal_and_cancellation_each_send_one_close() {
        for reason in [
            ManagedTargetEndReason::Removed,
            ManagedTargetEndReason::Cancelled,
        ] {
            let peer = UdpSocket::bind("127.0.0.1:0").unwrap();
            peer.set_read_timeout(Some(Duration::from_millis(200)))
                .unwrap();
            let fixture = active_target_fixture_at(peer.local_addr().unwrap());
            let socket = UdpSocket::bind("127.0.0.1:0").unwrap();

            {
                let mut target = fixture.target.lock().expect("target mutex poisoned");
                close_locked_target(&socket, &fixture.hub, &mut target, reason.clone());
                assert_eq!(target.final_reason, Some(reason));
                assert_eq!(target.close_send_attempts, 1);
                assert!(!target.runtime.is_open());
                assert!(target.schedule.is_none());
            }

            let mut packet = [0_u8; 512];
            let (len, _) = peer.recv_from(&mut packet).unwrap();
            assert_eq!(packet[3], flags::FLAG_CLOSE);
            assert_eq!(len, 12);
            assert!(matches!(
                peer.recv_from(&mut packet),
                Err(err)
                    if matches!(
                        err.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    )
            ));
        }
    }

    #[test]
    fn managed_close_send_failure_preserves_machine_and_requested_reason_is_not_recorded() {
        let peer = UdpSocket::bind("127.0.0.1:0").unwrap();
        peer.set_nonblocking(true).unwrap();
        let fixture = active_target_fixture_at(peer.local_addr().unwrap());
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();

        {
            let mut target = fixture.target.lock().expect("target mutex poisoned");
            target.close_send_error = true;
            close_locked_target(
                &socket,
                &fixture.hub,
                &mut target,
                ManagedTargetEndReason::Removed,
            );
            assert!(target.runtime.is_open());
            assert!(target.schedule.is_some());
            assert_eq!(target.close_send_attempts, 1);
            assert!(matches!(
                target.final_reason,
                Some(ManagedTargetEndReason::Failed(_))
            ));
        }

        let mut packet = [0_u8; 512];
        assert!(matches!(
            peer.recv_from(&mut packet),
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock
        ));
        assert!(!drain_available_group_events(&fixture.subscription)
            .iter()
            .any(|event| matches!(
                event,
                ManagedGroupEvent::Client(TargetEvent {
                    event: ClientEvent::SessionClosed { .. },
                    ..
                })
            )));
    }

    #[test]
    fn managed_short_close_commits_and_clears_schedule_before_failure() {
        let peer = UdpSocket::bind("127.0.0.1:0").unwrap();
        peer.set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();
        let fixture = active_target_fixture_at(peer.local_addr().unwrap());
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();

        {
            let mut target = fixture.target.lock().expect("target mutex poisoned");
            let expected = target.runtime.prepare_close().unwrap().bytes.len();
            target.close_reported_len = Some(expected - 1);
            close_locked_target(
                &socket,
                &fixture.hub,
                &mut target,
                ManagedTargetEndReason::Cancelled,
            );
            assert!(!target.runtime.is_open());
            assert!(target.schedule.is_none());
            assert_eq!(target.close_send_attempts, 1);
            let failure = target
                .final_reason
                .as_ref()
                .and_then(ManagedTargetEndReason::failure)
                .expect("short close must use managed failure policy");
            assert_eq!(failure.kind, ManagedTargetFailureKind::RuntimeProtocol);
            assert!(failure.message.contains("UDP accepted"));
        }

        let mut packet = [0_u8; 512];
        peer.recv_from(&mut packet).unwrap();
        assert!(!drain_available_group_events(&fixture.subscription)
            .iter()
            .any(|event| matches!(
                event,
                ManagedGroupEvent::Client(TargetEvent {
                    event: ClientEvent::SessionClosed { .. },
                    ..
                })
            )));
    }

    #[test]
    fn managed_close_reservation_failure_precedes_send() {
        let peer = UdpSocket::bind("127.0.0.1:0").unwrap();
        peer.set_nonblocking(true).unwrap();
        let fixture = active_target_fixture_at(peer.local_addr().unwrap());
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();

        {
            let mut target = fixture.target.lock().expect("target mutex poisoned");
            target.close_event_reserve_error = true;
            close_locked_target(
                &socket,
                &fixture.hub,
                &mut target,
                ManagedTargetEndReason::TestComplete,
            );
            assert!(target.runtime.is_open());
            assert!(target.schedule.is_some());
            assert_eq!(target.close_send_attempts, 0);
            assert!(matches!(
                target.final_reason,
                Some(ManagedTargetEndReason::Failed(_))
            ));
        }

        let mut packet = [0_u8; 512];
        assert!(matches!(
            peer.recv_from(&mut packet),
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock
        ));
    }

    #[test]
    fn managed_peer_close_affects_only_the_matching_target() {
        let peer = active_target_fixture_at("127.0.0.1:2112".parse().unwrap());
        let other = active_target_fixture_at("127.0.0.1:2113".parse().unwrap());

        {
            let mut target = peer.target.lock().expect("target mutex poisoned");
            assert!(process_active_target_packet(
                &peer.hub,
                &mut target,
                &peer.close_reply,
                ClientTimestamp::now(),
                ReceiveMeta::default(),
            ));
            assert!(target.runtime.is_peer_closed());
            assert!(target.schedule.is_none());
            assert_eq!(
                target.final_reason,
                Some(ManagedTargetEndReason::PeerClosed)
            );
        }

        let target = other.target.lock().expect("target mutex poisoned");
        assert!(target.runtime.is_open());
        assert!(matches!(target.status, TargetStatus::Active));
        assert!(target.schedule.is_some());
        assert_eq!(target.final_reason, None);
    }

    #[test]
    fn managed_non_active_states_do_not_attempt_local_close() {
        let remote: SocketAddr = "127.0.0.1:2112".parse().unwrap();
        let mut normal_config = group_config(
            None,
            Duration::from_millis(10),
            ManagedGroupPacing::Staggered,
        )
        .client;
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let hub = EventHub::new();
        let mut opening =
            TargetState::new(&normal_config, target("opening", remote), 0, Instant::now()).unwrap();
        close_locked_target(&socket, &hub, &mut opening, ManagedTargetEndReason::Removed);
        assert_eq!(opening.close_send_attempts, 0);

        normal_config.run_mode = crate::RunMode::NoTest;
        let no_test_params = params_from_config(&normal_config).unwrap();
        let mut no_test =
            TargetState::new(&normal_config, target("no-test", remote), 0, Instant::now()).unwrap();
        let reply = OpenReply {
            flags: FLAG_OPEN | FLAG_REPLY | flags::FLAG_CLOSE,
            token: 0,
            params: no_test_params,
        };
        let prepared = no_test
            .runtime
            .prepare_open_acceptance(reply, ClientTimestamp::now())
            .unwrap();
        no_test.runtime.commit_open(prepared);
        close_locked_target(
            &socket,
            &hub,
            &mut no_test,
            ManagedTargetEndReason::Cancelled,
        );
        assert_eq!(no_test.close_send_attempts, 0);
    }

    #[test]
    fn authoritative_status_counts_only_peer_closed_outcomes() {
        let remote: SocketAddr = "127.0.0.1:2112".parse().unwrap();
        let config = group_config(
            None,
            Duration::from_millis(10),
            ManagedGroupPacing::Staggered,
        )
        .client;
        let target = Arc::new(Mutex::new(
            TargetState::new(&config, target("active", remote), 0, Instant::now()).unwrap(),
        ));
        let hub = EventHub::new();
        let shared = shared_with_target(&config, hub.clone(), target);
        let mut records = TargetOutcomeHistory::default();

        hub.publish(ManagedGroupEvent::Client(TargetEvent {
            target: TargetId::from("active"),
            event: ClientEvent::EchoLoss {
                seq: 0,
                sent_at: ClientTimestamp::now(),
                timeout_at: Instant::now(),
            },
        }));
        assert_eq!(shared.peer_closed_target_count.load(Ordering::Acquire), 0);

        let failure = ManagedTargetFailure {
            kind: ManagedTargetFailureKind::RuntimeProtocol,
            message: "test failure".to_owned(),
        };
        for (index, reason) in [
            ManagedTargetEndReason::TestComplete,
            ManagedTargetEndReason::NoTestComplete,
            ManagedTargetEndReason::Removed,
            ManagedTargetEndReason::Cancelled,
            ManagedTargetEndReason::OpenFailed(failure.clone()),
            ManagedTargetEndReason::Failed(failure),
        ]
        .into_iter()
        .enumerate()
        {
            record_target_outcome(
                &shared,
                &mut records,
                target_outcome(&format!("non-peer-{index}"), remote, reason),
            );
            assert_eq!(shared.peer_closed_target_count.load(Ordering::Acquire), 0);
        }

        record_target_outcome(
            &shared,
            &mut records,
            target_outcome("peer", remote, ManagedTargetEndReason::PeerClosed),
        );
        assert_eq!(shared.peer_closed_target_count.load(Ordering::Acquire), 1);

        let outcome = records.into_group_outcome(ManagedGroupEndReason::AllTargetsComplete);
        assert_eq!(outcome.total_target_outcomes, 7);
        assert_eq!(outcome.peer_closed_target_outcomes, 1);
    }

    #[test]
    fn drop_oldest_can_evict_peer_close_event_without_status_loss() {
        let interval = Duration::from_millis(10);
        let config = group_config(None, interval, ManagedGroupPacing::Staggered).client;
        let peer_remote: SocketAddr = "127.0.0.1:2112".parse().unwrap();
        let healthy_remote: SocketAddr = "127.0.0.1:2113".parse().unwrap();
        let peer = Arc::new(Mutex::new(
            TargetState::new(&config, target("peer", peer_remote), 0, Instant::now()).unwrap(),
        ));
        peer.lock()
            .expect("target mutex poisoned")
            .mark_finished(ManagedTargetEndReason::PeerClosed);
        let healthy = Arc::new(Mutex::new(
            TargetState::new(
                &config,
                target("healthy", healthy_remote),
                1,
                Instant::now(),
            )
            .unwrap(),
        ));
        healthy.lock().expect("target mutex poisoned").status = TargetStatus::Active;

        let hub = EventHub::new();
        let subscription = hub
            .subscribe(SubscriberConfig {
                capacity: 1,
                overflow: SubscriberOverflow::DropOldest,
            })
            .unwrap();
        let shared = shared_with_target(&config, hub.clone(), peer);
        {
            let mut registry = shared
                .registry
                .lock()
                .expect("target registry mutex poisoned");
            registry.desired.insert(TargetId::from("healthy"));
            registry
                .remotes
                .insert(healthy_remote, TargetId::from("healthy"));
            registry
                .targets
                .insert(TargetId::from("healthy"), healthy.clone());
        }

        let mut records = TargetOutcomeHistory::default();
        collect_finished_targets(&shared, &mut records);
        assert_eq!(shared.peer_closed_target_count.load(Ordering::Acquire), 1);
        assert!(matches!(
            healthy.lock().expect("target mutex poisoned").status,
            TargetStatus::Active
        ));

        hub.publish(ManagedGroupEvent::Client(TargetEvent {
            target: TargetId::from("healthy"),
            event: ClientEvent::EchoLoss {
                seq: 7,
                sent_at: ClientTimestamp::now(),
                timeout_at: Instant::now(),
            },
        }));

        assert_eq!(subscription.dropped_events(), 1);
        assert!(matches!(
            subscription.try_recv(),
            Ok(Some(ManagedGroupEvent::Client(TargetEvent {
                target,
                event: ClientEvent::EchoLoss { seq: 7, .. },
            }))) if target.as_str() == "healthy"
        ));
        assert_eq!(subscription.try_recv(), Ok(None));
        assert_eq!(shared.peer_closed_target_count.load(Ordering::Acquire), 1);
        assert_eq!(records.peer_closed, 1);
        assert_eq!(
            shared
                .registry
                .lock()
                .expect("target registry mutex poisoned")
                .targets
                .len(),
            1
        );
    }

    #[test]
    fn receiver_non_close_reply_leaves_target_active() {
        let fixture = active_target_fixture();
        {
            let mut target = fixture.target.lock().expect("target mutex poisoned");
            assert!(!process_active_target_packet(
                &fixture.hub,
                &mut target,
                &fixture.reply,
                ClientTimestamp::now(),
                ReceiveMeta::default(),
            ));
            assert!(matches!(target.status, TargetStatus::Active));
            assert_eq!(target.final_reason, None);
            assert_eq!(target.counters.replies_received, 1);
            assert!(!target.runtime.is_peer_closed());
        }

        let events = drain_available_group_events(&fixture.subscription);
        assert!(events.iter().any(|event| matches!(
            event,
            ManagedGroupEvent::Client(TargetEvent {
                event: ClientEvent::EchoReply { .. },
                ..
            })
        )));
        assert!(!events.iter().any(|event| matches!(
            event,
            ManagedGroupEvent::Client(TargetEvent {
                event: ClientEvent::SessionClosed { .. },
                ..
            }) | ManagedGroupEvent::TargetFinished(_)
        )));
    }

    #[test]
    fn peer_close_preserves_group_outcome_counters_and_provenance() {
        let interval = Duration::from_millis(10);
        let server = start_peer_close_server(test_params(None, interval));

        let session = ManagedClientGroup::start(
            group_config(None, interval, ManagedGroupPacing::Staggered),
            vec![target("peer", server.addr)],
        )
        .unwrap();

        let outcome = session.join().unwrap();
        server.join();

        assert_eq!(
            outcome.end_reason,
            ManagedGroupEndReason::AllTargetsComplete
        );
        assert_eq!(outcome.targets.len(), 1);
        let target = &outcome.targets[0];
        assert_eq!(target.end_reason, ManagedTargetEndReason::PeerClosed);
        assert_eq!(target.packets_sent, 1);
        assert_eq!(target.replies_received, 1);
        assert_eq!(outcome.total_target_outcomes, 1);
        assert_eq!(outcome.successful_target_outcomes, 1);
        assert_eq!(outcome.peer_closed_target_outcomes, 1);
        assert_eq!(outcome.failed_target_outcomes, 0);
    }

    #[test]
    fn peer_close_aggregate_survives_bounded_recent_history() {
        let remote: SocketAddr = "127.0.0.1:2112".parse().unwrap();
        let mut history = TargetOutcomeHistory::default();
        history.record(ManagedTargetOutcome {
            id: TargetId::from("peer"),
            remote,
            end_reason: ManagedTargetEndReason::PeerClosed,
            packets_sent: 1,
            replies_received: 1,
            duplicates: 0,
            late: 0,
            warning_events: 0,
        });
        for index in 0..MANAGED_GROUP_OUTCOME_HISTORY_LIMIT {
            history.record(ManagedTargetOutcome {
                id: TargetId::from(format!("removed-{index}")),
                remote,
                end_reason: ManagedTargetEndReason::Removed,
                packets_sent: 0,
                replies_received: 0,
                duplicates: 0,
                late: 0,
                warning_events: 0,
            });
        }

        let outcome = history.into_group_outcome(ManagedGroupEndReason::AllTargetsComplete);
        let expected_total = u64::try_from(MANAGED_GROUP_OUTCOME_HISTORY_LIMIT + 1).unwrap();
        assert_eq!(outcome.total_target_outcomes, expected_total);
        assert_eq!(outcome.successful_target_outcomes, 1);
        assert_eq!(outcome.peer_closed_target_outcomes, 1);
        assert_eq!(outcome.failed_target_outcomes, 0);
        assert_eq!(outcome.targets.len(), MANAGED_GROUP_OUTCOME_HISTORY_LIMIT);
        assert_eq!(outcome.discarded_target_outcomes, 1);
        assert!(outcome
            .targets
            .iter()
            .all(|target| target.end_reason == ManagedTargetEndReason::Removed));
    }

    #[test]
    fn mixed_peer_close_and_open_failure_publish_terminal_outcomes_and_disconnect() {
        let interval = Duration::from_millis(10);
        let params = test_params(None, interval);
        let success = start_peer_close_server(params.clone());
        let failure = start_open_failure_server(params);
        let (session, sub) = ManagedClientGroup::start_with_subscription(
            group_config(None, interval, ManagedGroupPacing::Staggered),
            vec![
                target("success", success.addr),
                target("failure", failure.addr),
            ],
            SubscriberConfig {
                capacity: 64,
                overflow: SubscriberOverflow::DropNewest,
            },
        )
        .unwrap();

        let events = collect_group_events_until_disconnected(&sub);
        let outcome = session.join().unwrap();
        success.join();
        failure.join();

        assert!(events.iter().any(|event| matches!(
            event,
            ManagedGroupEvent::Client(TargetEvent {
                target,
                event: ClientEvent::EchoReply { .. },
            }) if target.as_str() == "success"
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            ManagedGroupEvent::TargetFinished(target)
                if target.id.as_str() == "success"
                    && target.end_reason == ManagedTargetEndReason::PeerClosed
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            ManagedGroupEvent::TargetFinished(target)
                if target.id.as_str() == "failure"
                    && matches!(
                        &target.end_reason,
                        ManagedTargetEndReason::OpenFailed(failure)
                            if failure.kind == ManagedTargetFailureKind::OpeningProtocol
                    )
        )));
        assert_eq!(outcome.targets.len(), 2);
        assert_eq!(outcome.total_target_outcomes, 2);
        assert_eq!(outcome.successful_target_outcomes, 1);
        assert_eq!(outcome.peer_closed_target_outcomes, 1);
        assert_eq!(outcome.failed_target_outcomes, 1);
    }

    #[test]
    fn all_open_failures_publish_terminal_outcomes_and_disconnect() {
        let interval = Duration::from_millis(10);
        let params = test_params(None, interval);
        let a = start_open_failure_server(params.clone());
        let b = start_open_failure_server(params);
        let (session, sub) = ManagedClientGroup::start_with_subscription(
            group_config(None, interval, ManagedGroupPacing::Burst),
            vec![target("a", a.addr), target("b", b.addr)],
            SubscriberConfig {
                capacity: 16,
                overflow: SubscriberOverflow::DropNewest,
            },
        )
        .unwrap();

        let events = collect_group_events_until_disconnected(&sub);
        let outcome = session.join().unwrap();
        a.join();
        b.join();

        let failures = events
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    ManagedGroupEvent::TargetFinished(ManagedTargetOutcome {
                        end_reason: ManagedTargetEndReason::OpenFailed(_),
                        ..
                    })
                )
            })
            .count();
        assert_eq!(failures, 2);
        assert_eq!(outcome.targets.len(), 2);
        assert!(outcome
            .targets
            .iter()
            .all(|target| target.end_reason.failure().is_some()));
    }

    #[test]
    fn bad_open_packet_followed_by_valid_reply_succeeds_without_retry() {
        let interval = Duration::from_millis(10);
        let params = test_params(None, interval);
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = socket.local_addr().unwrap();
        let (tx, rx) = mpsc::channel();
        let done = thread::spawn(move || {
            let (open, peer) = recv_request_timeout(&socket).expect("missing open request");
            assert_ne!(open[3] & FLAG_OPEN, 0);
            tx.send(ServerObservation::Open { at: Instant::now() })
                .unwrap();
            socket.send_to(&[0_u8], peer).unwrap();
            socket
                .send_to(
                    &open_reply(FLAG_OPEN | FLAG_REPLY | flags::FLAG_CLOSE, 0, &params),
                    peer,
                )
                .unwrap();
        });
        let server = FakeServer {
            addr,
            _observations: rx,
            done,
        };
        let mut config = group_config(None, interval, ManagedGroupPacing::Staggered);
        config.client.run_mode = crate::RunMode::NoTest;

        let outcome = ManagedClientGroup::start(config, vec![target("peer", server.addr)])
            .unwrap()
            .join()
            .unwrap();

        assert_eq!(outcome.successful_target_outcomes, 1);
        assert_eq!(server._observations.try_iter().collect::<Vec<_>>().len(), 1);
        server.join();
    }

    #[test]
    fn bad_hmac_open_packet_followed_by_valid_reply_succeeds_without_retry() {
        let interval = Duration::from_millis(10);
        let params = test_params(None, interval);
        let key = b"group-secret".to_vec();
        let wrong_key = b"wrong".to_vec();
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = socket.local_addr().unwrap();
        let (tx, rx) = mpsc::channel();
        let server_key = key.clone();
        let done = thread::spawn(move || {
            let (open, peer) = recv_request_timeout(&socket).expect("missing open request");
            assert_ne!(open[3] & FLAG_OPEN, 0);
            tx.send(ServerObservation::Open { at: Instant::now() })
                .unwrap();
            socket
                .send_to(
                    &hmac_open_reply(
                        FLAG_OPEN | FLAG_REPLY | flags::FLAG_CLOSE,
                        0,
                        &params,
                        &wrong_key,
                    ),
                    peer,
                )
                .unwrap();
            socket
                .send_to(
                    &hmac_open_reply(
                        FLAG_OPEN | FLAG_REPLY | flags::FLAG_CLOSE,
                        0,
                        &params,
                        &server_key,
                    ),
                    peer,
                )
                .unwrap();
        });
        let server = FakeServer {
            addr,
            _observations: rx,
            done,
        };
        let mut config = group_config(None, interval, ManagedGroupPacing::Staggered);
        config.client.run_mode = crate::RunMode::NoTest;
        config.client.hmac_key = Some(key);

        let outcome = ManagedClientGroup::start(config, vec![target("peer", server.addr)])
            .unwrap()
            .join()
            .unwrap();

        assert_eq!(outcome.successful_target_outcomes, 1);
        assert_eq!(server._observations.try_iter().collect::<Vec<_>>().len(), 1);
        server.join();
    }

    #[test]
    fn trusted_group_incompatibility_sends_cleanup_and_fails_without_retry() {
        let interval = Duration::from_millis(10);
        let mut returned = test_params(None, interval);
        returned.interval_ns = duration_ns_i64(Duration::from_millis(20));
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let addr = socket.local_addr().unwrap();
        let (tx, rx) = mpsc::channel();
        let done = thread::spawn(move || {
            let (open, peer) = recv_request_timeout(&socket).expect("missing open request");
            assert_ne!(open[3] & FLAG_OPEN, 0);
            tx.send(ServerObservation::Open { at: Instant::now() })
                .unwrap();
            socket
                .send_to(&open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &returned), peer)
                .unwrap();
            let (cleanup, _) =
                recv_request_timeout(&socket).expect("missing post-token cleanup close");
            assert_eq!(cleanup[3], flags::FLAG_CLOSE);
            assert_eq!(
                u64::from_le_bytes(cleanup[4..12].try_into().unwrap()),
                TOKEN
            );
            tx.send(ServerObservation::Close { at: Instant::now() })
                .unwrap();
        });
        let server = FakeServer {
            addr,
            _observations: rx,
            done,
        };
        let mut config = group_config(None, interval, ManagedGroupPacing::Staggered);
        config.client.negotiation_policy = NegotiationPolicy::Loose;

        let outcome = ManagedClientGroup::start(config, vec![target("peer", server.addr)])
            .unwrap()
            .join()
            .unwrap();

        assert_eq!(outcome.failed_target_outcomes, 1);
        assert!(matches!(
            outcome.targets[0].end_reason,
            ManagedTargetEndReason::OpenFailed(ManagedTargetFailure {
                kind: ManagedTargetFailureKind::OpeningProtocol,
                ..
            })
        ));
        assert_eq!(server._observations.try_iter().collect::<Vec<_>>().len(), 2);
        server.join();
    }

    #[test]
    fn no_test_non_close_reply_sends_cleanup_and_fails_without_retry() {
        let interval = Duration::from_millis(10);
        let params = test_params(None, interval);
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let addr = socket.local_addr().unwrap();
        let (tx, rx) = mpsc::channel();
        let done = thread::spawn(move || {
            let (open, peer) = recv_request_timeout(&socket).expect("missing open request");
            assert_ne!(open[3] & flags::FLAG_CLOSE, 0);
            tx.send(ServerObservation::Open { at: Instant::now() })
                .unwrap();
            socket
                .send_to(&open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params), peer)
                .unwrap();
            let (cleanup, _) =
                recv_request_timeout(&socket).expect("missing no-test cleanup close");
            assert_eq!(cleanup[3], flags::FLAG_CLOSE);
            assert_eq!(
                u64::from_le_bytes(cleanup[4..12].try_into().unwrap()),
                TOKEN
            );
            tx.send(ServerObservation::Close { at: Instant::now() })
                .unwrap();
        });
        let server = FakeServer {
            addr,
            _observations: rx,
            done,
        };
        let mut config = group_config(None, interval, ManagedGroupPacing::Staggered);
        config.client.run_mode = crate::RunMode::NoTest;

        let outcome = ManagedClientGroup::start(config, vec![target("peer", server.addr)])
            .unwrap()
            .join()
            .unwrap();

        assert_eq!(outcome.failed_target_outcomes, 1);
        assert!(matches!(
            outcome.targets[0].end_reason,
            ManagedTargetEndReason::OpenFailed(ManagedTargetFailure {
                kind: ManagedTargetFailureKind::OpeningProtocol,
                ..
            })
        ));
        assert_eq!(server._observations.try_iter().collect::<Vec<_>>().len(), 2);
        server.join();
    }

    #[test]
    fn loose_no_test_group_skips_active_interval_and_dscp_policy() {
        let interval = Duration::from_millis(10);
        let mut changed_interval = test_params(None, interval);
        changed_interval.interval_ns += 1;
        let changed_dscp = test_params(None, interval);

        for (requested_dscp, params) in [(0, changed_interval), (1, changed_dscp)] {
            let peer = UdpSocket::bind("127.0.0.1:0").unwrap();
            let remote = peer.local_addr().unwrap();
            let mut config = group_config(None, interval, ManagedGroupPacing::Staggered).client;
            config.run_mode = crate::RunMode::NoTest;
            config.negotiation_policy = NegotiationPolicy::Loose;
            config.dscp = requested_dscp;
            let target = Arc::new(Mutex::new(
                TargetState::new(&config, target("peer", remote), 0, Instant::now()).unwrap(),
            ));
            let shared = shared_with_target(&config, EventHub::new(), target.clone());
            let opened_at = ClientTimestamp::now();

            let mut target = target.lock().expect("target mutex poisoned");
            let packet = open_reply(FLAG_OPEN | FLAG_REPLY | flags::FLAG_CLOSE, 0, &params);
            let reply = match target.runtime.inspect_open_datagram(&packet).unwrap() {
                OpenDatagramDisposition::Trusted(reply) => reply,
                OpenDatagramDisposition::Ignore => panic!("no-test reply must be trusted"),
            };
            let machine = target
                .runtime
                .prepare_open_acceptance(reply, opened_at)
                .unwrap();
            let prepared = prepare_group_open(&shared, machine, opened_at).unwrap();
            assert!(prepared.schedule.is_none());
            let outcome = target.runtime.commit_open(prepared.machine);
            target.schedule = prepared.schedule;
            publish_open_outcome(&shared.hub, &mut target, outcome);

            assert!(matches!(target.status, TargetStatus::Finished));
            assert_eq!(
                target.final_reason,
                Some(ManagedTargetEndReason::NoTestComplete)
            );
        }
    }

    #[test]
    fn normal_and_strict_sessions_preserve_group_negotiation_checks() {
        let interval = Duration::from_millis(10);
        let mut changed_interval = test_params(None, interval);
        changed_interval.interval_ns += 1;
        let changed_dscp = test_params(None, interval);

        for (requested_dscp, params) in [(0, changed_interval.clone()), (1, changed_dscp)] {
            let peer = UdpSocket::bind("127.0.0.1:0").unwrap();
            let remote = peer.local_addr().unwrap();
            let mut config = group_config(None, interval, ManagedGroupPacing::Staggered).client;
            config.negotiation_policy = NegotiationPolicy::Loose;
            config.dscp = requested_dscp;
            let target = Arc::new(Mutex::new(
                TargetState::new(&config, target("peer", remote), 0, Instant::now()).unwrap(),
            ));
            let shared = shared_with_target(&config, EventHub::new(), target.clone());
            let opened_at = ClientTimestamp::now();
            let target = target.lock().expect("target mutex poisoned");
            let packet = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params);
            let reply = match target.runtime.inspect_open_datagram(&packet).unwrap() {
                OpenDatagramDisposition::Trusted(reply) => reply,
                OpenDatagramDisposition::Ignore => panic!("normal reply must be trusted"),
            };
            let machine = target
                .runtime
                .prepare_open_acceptance(reply, opened_at)
                .unwrap();
            let failure = prepare_group_open(&shared, machine, opened_at).unwrap_err();
            assert!(matches!(
                failure.primary,
                ClientError::NegotiationRejected { .. }
            ));
        }

        let peer = UdpSocket::bind("127.0.0.1:0").unwrap();
        let remote = peer.local_addr().unwrap();
        let mut config = group_config(None, interval, ManagedGroupPacing::Staggered).client;
        config.run_mode = crate::RunMode::NoTest;
        let target = TargetState::new(&config, target("peer", remote), 0, Instant::now()).unwrap();
        let packet = open_reply(
            FLAG_OPEN | FLAG_REPLY | flags::FLAG_CLOSE,
            0,
            &changed_interval,
        );
        let reply = match target.runtime.inspect_open_datagram(&packet).unwrap() {
            OpenDatagramDisposition::Trusted(reply) => reply,
            OpenDatagramDisposition::Ignore => panic!("strict no-test reply must be trusted"),
        };
        let failure = target
            .runtime
            .prepare_open_acceptance(reply, ClientTimestamp::now())
            .unwrap_err();
        assert!(matches!(
            failure.primary,
            ClientError::NegotiationRejected { .. }
        ));
    }

    #[test]
    fn group_open_deadline_overflow_fails_before_send() {
        let silent_peer = UdpSocket::bind("127.0.0.1:0").unwrap();
        let remote = silent_peer.local_addr().unwrap();
        let mut config = group_config(
            None,
            Duration::from_millis(10),
            ManagedGroupPacing::Staggered,
        );
        config.client.open_timeouts = vec![Duration::MAX];

        let outcome = ManagedClientGroup::start(config, vec![target("peer", remote)])
            .unwrap()
            .join()
            .unwrap();

        assert_eq!(outcome.failed_target_outcomes, 1);
        let failure = outcome.targets[0].end_reason.failure().unwrap();
        assert!(failure.message.contains("duration"));
        silent_peer.set_nonblocking(true).unwrap();
        let mut packet = [0_u8; 512];
        assert!(matches!(
            silent_peer.recv_from(&mut packet),
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock
        ));
    }

    #[test]
    fn receiver_drain_completion_is_monotonic_and_wakes_waiting_scheduler() {
        let interval = Duration::from_millis(10);
        let config = group_config(None, interval, ManagedGroupPacing::Staggered).client;
        let peer = UdpSocket::bind("127.0.0.1:0").unwrap();
        let remote = peer.local_addr().unwrap();
        let now = Instant::now();
        let target = Arc::new(Mutex::new(
            TargetState::new(&config, target("peer", remote), 0, now).unwrap(),
        ));
        let (shared, control_rx) = shared_with_targets(&config, EventHub::new(), vec![target]);
        let shared = Arc::new(shared);
        let generation = request_receiver_drain(&shared).unwrap();
        let waiter_shared = shared.clone();
        let waiter = thread::spawn(move || {
            wait_for_scheduler_control(&control_rx, &waiter_shared.cancellation, None)
        });

        complete_receiver_drain(&shared, generation);

        assert!(matches!(waiter.join().unwrap(), Some(ControlMessage::Wake)));
        assert_eq!(completed_receiver_drain(&shared), generation);
        complete_receiver_drain(&shared, 0);
        complete_receiver_drain(&shared, generation);
        assert_eq!(completed_receiver_drain(&shared), generation);
    }

    #[test]
    fn expired_attempt_waits_for_receiver_drain_before_retrying() {
        let interval = Duration::from_millis(10);
        let mut config = group_config(None, interval, ManagedGroupPacing::Staggered).client;
        config.open_timeouts = vec![Duration::from_millis(200), Duration::from_millis(200)];
        let peer = UdpSocket::bind("127.0.0.1:0").unwrap();
        peer.set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        let remote = peer.local_addr().unwrap();
        let now = Instant::now();
        let target = Arc::new(Mutex::new(
            TargetState::new(&config, target("peer", remote), 0, now).unwrap(),
        ));
        let shared = shared_with_target(&config, EventHub::new(), target.clone());
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();

        drive_open_attempts(&config, &socket, &shared, now);
        let deadline = {
            let target = target.lock().expect("target mutex poisoned");
            match target.status {
                TargetStatus::Opening {
                    attempt: 1,
                    next_send_at,
                    ..
                } => next_send_at,
                ref status => panic!("unexpected target status after open send: {status:?}"),
            }
        };
        assert!(recv_request_timeout(&peer).is_some());

        drive_open_attempts(&config, &socket, &shared, deadline);
        let generation = match target.lock().expect("target mutex poisoned").status {
            TargetStatus::Opening {
                attempt: 1,
                awaiting_receiver_generation: Some(generation),
                ..
            } => generation,
            ref status => panic!("unexpected waiting status: {status:?}"),
        };
        assert!(recv_request_timeout(&peer).is_none());

        complete_receiver_drain(&shared, generation);
        drive_open_attempts(&config, &socket, &shared, deadline);
        assert!(matches!(
            target.lock().expect("target mutex poisoned").status,
            TargetStatus::Opening {
                attempt: 2,
                awaiting_receiver_generation: None,
                ..
            }
        ));
        assert!(recv_request_timeout(&peer).is_some());
    }

    #[test]
    fn queued_reply_dequeued_after_deadline_opens_without_retry() {
        let interval = Duration::from_millis(10);
        let mut config = group_config(None, interval, ManagedGroupPacing::Staggered).client;
        config.open_timeouts = vec![Duration::from_millis(200)];
        let params = test_params(None, interval);
        let peer = UdpSocket::bind("127.0.0.1:0").unwrap();
        peer.set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        let remote = peer.local_addr().unwrap();
        let now = Instant::now();
        let target = Arc::new(Mutex::new(
            TargetState::new(&config, target("peer", remote), 0, now).unwrap(),
        ));
        let shared = shared_with_target(&config, EventHub::new(), target.clone());
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();

        drive_open_attempts(&config, &socket, &shared, now);
        let deadline = {
            let target = target.lock().expect("target mutex poisoned");
            match target.status {
                TargetStatus::Opening { next_send_at, .. } => next_send_at,
                ref status => panic!("unexpected opening status: {status:?}"),
            }
        };
        let (_, client_addr) = recv_request_timeout(&peer).expect("missing first open request");
        peer.send_to(
            &open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params),
            client_addr,
        )
        .unwrap();
        assert!(
            Instant::now() < deadline,
            "reply must queue before deadline"
        );
        while Instant::now() < deadline {
            thread::yield_now();
        }

        drive_open_attempts(&config, &socket, &shared, deadline);
        assert!(matches!(
            target.lock().expect("target mutex poisoned").status,
            TargetStatus::Opening {
                awaiting_receiver_generation: Some(_),
                ..
            }
        ));

        let (datagram, packet) = recv_group_datagram(&socket);
        assert!(datagram.received_at.mono >= deadline);
        process_group_datagram(&socket, &shared, datagram, &packet);

        let target = target.lock().expect("target mutex poisoned");
        assert!(matches!(target.status, TargetStatus::Active));
        assert!(target.final_reason.is_none());
        drop(target);
        assert!(recv_request_timeout(&peer).is_none());
    }

    #[test]
    fn generation_requested_after_receive_snapshot_needs_later_empty_boundary() {
        let interval = Duration::from_millis(10);
        let config = group_config(None, interval, ManagedGroupPacing::Staggered).client;
        let peer = UdpSocket::bind("127.0.0.1:0").unwrap();
        let remote = peer.local_addr().unwrap();
        let target = Arc::new(Mutex::new(
            TargetState::new(&config, target("peer", remote), 0, Instant::now()).unwrap(),
        ));
        let shared = shared_with_target(&config, EventHub::new(), target);

        let observed_before_request = requested_receiver_drain(&shared);
        let generation = request_receiver_drain(&shared).unwrap();
        complete_receiver_drain(&shared, observed_before_request);
        assert!(completed_receiver_drain(&shared) < generation);

        let observed_after_request = requested_receiver_drain(&shared);
        complete_receiver_drain(&shared, observed_after_request);
        assert_eq!(completed_receiver_drain(&shared), generation);
    }

    #[test]
    fn malformed_packet_requires_empty_receive_to_complete_drain() {
        let interval = Duration::from_millis(10);
        let config = group_config(None, interval, ManagedGroupPacing::Staggered).client;
        let peer = UdpSocket::bind("127.0.0.1:0").unwrap();
        peer.set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        let remote = peer.local_addr().unwrap();
        let now = Instant::now();
        let target = Arc::new(Mutex::new(
            TargetState::new(&config, target("peer", remote), 0, now).unwrap(),
        ));
        let (shared, control_rx) =
            shared_with_targets(&config, EventHub::new(), vec![target.clone()]);
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();

        drive_open_attempts(&config, &socket, &shared, now);
        let deadline = match target.lock().expect("target mutex poisoned").status {
            TargetStatus::Opening { next_send_at, .. } => next_send_at,
            ref status => panic!("unexpected opening status: {status:?}"),
        };
        let (_, client_addr) = recv_request_timeout(&peer).expect("missing open request");
        drive_open_attempts(&config, &socket, &shared, deadline);
        let generation = match target.lock().expect("target mutex poisoned").status {
            TargetStatus::Opening {
                awaiting_receiver_generation: Some(generation),
                ..
            } => generation,
            ref status => panic!("unexpected waiting status: {status:?}"),
        };

        peer.send_to(&[0_u8], client_addr).unwrap();
        let (datagram, packet) = recv_group_datagram(&socket);
        process_group_datagram(&socket, &shared, datagram, &packet);
        assert!(completed_receiver_drain(&shared) < generation);
        assert!(matches!(
            control_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        let observed_generation = requested_receiver_drain(&shared);
        socket.set_nonblocking(true).unwrap();
        let mut buf = [0_u8; 1];
        assert!(matches!(
            recv_datagram_from(&socket, &mut buf),
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock
        ));
        complete_receiver_drain(&shared, observed_generation);
        assert_eq!(completed_receiver_drain(&shared), generation);
        assert!(matches!(control_rx.recv().unwrap(), ControlMessage::Wake));
    }

    #[test]
    fn opening_reply_before_first_request_is_ignored() {
        let interval = Duration::from_millis(10);
        let config = group_config(None, interval, ManagedGroupPacing::Staggered).client;
        let params = test_params(None, interval);
        let peer = UdpSocket::bind("127.0.0.1:0").unwrap();
        let remote = peer.local_addr().unwrap();
        let target = Arc::new(Mutex::new(
            TargetState::new(&config, target("peer", remote), 0, Instant::now()).unwrap(),
        ));
        let shared = shared_with_target(&config, EventHub::new(), target.clone());
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();

        peer.send_to(
            &open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params),
            socket.local_addr().unwrap(),
        )
        .unwrap();
        let (datagram, packet) = recv_group_datagram(&socket);
        process_group_datagram(&socket, &shared, datagram, &packet);

        let target = target.lock().expect("target mutex poisoned");
        assert!(matches!(
            target.status,
            TargetStatus::Opening {
                attempt: 0,
                awaiting_receiver_generation: None,
                ..
            }
        ));
        assert!(target.runtime.prepare_open_request().is_ok());
    }

    #[test]
    fn post_drain_reply_cannot_open_expired_attempt() {
        let interval = Duration::from_millis(10);
        let mut config = group_config(None, interval, ManagedGroupPacing::Staggered).client;
        config.open_timeouts = vec![Duration::from_millis(200), Duration::from_millis(200)];
        let params = test_params(None, interval);
        let peer = UdpSocket::bind("127.0.0.1:0").unwrap();
        peer.set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        let remote = peer.local_addr().unwrap();
        let now = Instant::now();
        let target = Arc::new(Mutex::new(
            TargetState::new(&config, target("peer", remote), 0, now).unwrap(),
        ));
        let shared = shared_with_target(&config, EventHub::new(), target.clone());
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();

        drive_open_attempts(&config, &socket, &shared, now);
        let deadline = match target.lock().expect("target mutex poisoned").status {
            TargetStatus::Opening { next_send_at, .. } => next_send_at,
            ref status => panic!("unexpected opening status: {status:?}"),
        };
        let (_, client_addr) = recv_request_timeout(&peer).expect("missing open request");
        drive_open_attempts(&config, &socket, &shared, deadline);
        let generation = match target.lock().expect("target mutex poisoned").status {
            TargetStatus::Opening {
                awaiting_receiver_generation: Some(generation),
                ..
            } => generation,
            ref status => panic!("unexpected waiting status: {status:?}"),
        };
        complete_receiver_drain(&shared, generation);

        peer.send_to(
            &open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params),
            client_addr,
        )
        .unwrap();
        let (datagram, packet) = recv_group_datagram(&socket);
        process_group_datagram(&socket, &shared, datagram, &packet);
        {
            let target = target.lock().expect("target mutex poisoned");
            assert!(matches!(
                target.status,
                TargetStatus::Opening {
                    attempt: 1,
                    awaiting_receiver_generation: Some(waiting),
                    ..
                } if waiting == generation
            ));
            assert!(target.runtime.prepare_open_request().is_ok());
        }

        drive_open_attempts(&config, &socket, &shared, deadline);
        assert!(matches!(
            target.lock().expect("target mutex poisoned").status,
            TargetStatus::Opening {
                attempt: 2,
                awaiting_receiver_generation: None,
                ..
            }
        ));
        assert!(recv_request_timeout(&peer).is_some());
    }

    #[test]
    fn multiple_expired_targets_share_one_receiver_drain_generation() {
        let interval = Duration::from_millis(10);
        let mut config = group_config(None, interval, ManagedGroupPacing::Staggered).client;
        config.open_timeouts = vec![Duration::from_millis(200), Duration::from_millis(200)];
        let first_peer = UdpSocket::bind("127.0.0.1:0").unwrap();
        let second_peer = UdpSocket::bind("127.0.0.1:0").unwrap();
        for peer in [&first_peer, &second_peer] {
            peer.set_read_timeout(Some(Duration::from_millis(100)))
                .unwrap();
        }
        let now = Instant::now();
        let first = Arc::new(Mutex::new(
            TargetState::new(
                &config,
                target("first", first_peer.local_addr().unwrap()),
                0,
                now,
            )
            .unwrap(),
        ));
        let second = Arc::new(Mutex::new(
            TargetState::new(
                &config,
                target("second", second_peer.local_addr().unwrap()),
                1,
                now,
            )
            .unwrap(),
        ));
        let (shared, _control_rx) = shared_with_targets(
            &config,
            EventHub::new(),
            vec![first.clone(), second.clone()],
        );
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();

        drive_open_attempts(&config, &socket, &shared, now);
        assert!(recv_request_timeout(&first_peer).is_some());
        assert!(recv_request_timeout(&second_peer).is_some());
        let deadline = [&first, &second]
            .into_iter()
            .filter_map(|target| match target.lock().unwrap().status {
                TargetStatus::Opening { next_send_at, .. } => Some(next_send_at),
                _ => None,
            })
            .max()
            .unwrap();

        drive_open_attempts(&config, &socket, &shared, deadline);
        let generations = [&first, &second].map(|target| match target.lock().unwrap().status {
            TargetStatus::Opening {
                awaiting_receiver_generation: Some(generation),
                ..
            } => generation,
            ref status => panic!("unexpected waiting status: {status:?}"),
        });
        assert_eq!(generations[0], generations[1]);

        complete_receiver_drain(&shared, generations[0]);
        drive_open_attempts(&config, &socket, &shared, deadline);
        for target in [&first, &second] {
            assert!(matches!(
                target.lock().unwrap().status,
                TargetStatus::Opening {
                    attempt: 2,
                    awaiting_receiver_generation: None,
                    ..
                }
            ));
        }
        assert!(recv_request_timeout(&first_peer).is_some());
        assert!(recv_request_timeout(&second_peer).is_some());

        drive_open_attempts(&config, &socket, &shared, deadline);
        assert!(recv_request_timeout(&first_peer).is_none());
        assert!(recv_request_timeout(&second_peer).is_none());
    }

    #[test]
    fn removal_and_cancellation_release_targets_waiting_for_receiver_drain() {
        let interval = Duration::from_millis(10);
        let config = group_config(None, interval, ManagedGroupPacing::Staggered).client;
        let first_peer = UdpSocket::bind("127.0.0.1:0").unwrap();
        let second_peer = UdpSocket::bind("127.0.0.1:0").unwrap();
        let now = Instant::now();
        let first_config = target("first", first_peer.local_addr().unwrap());
        let second_config = target("second", second_peer.local_addr().unwrap());
        let first = Arc::new(Mutex::new(
            TargetState::new(&config, first_config.clone(), 0, now).unwrap(),
        ));
        let second = Arc::new(Mutex::new(
            TargetState::new(&config, second_config, 1, now).unwrap(),
        ));
        let (shared, _control_rx) = shared_with_targets(
            &config,
            EventHub::new(),
            vec![first.clone(), second.clone()],
        );
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();

        drive_open_attempts(&config, &socket, &shared, now);
        let deadline = [&first, &second]
            .into_iter()
            .filter_map(|target| match target.lock().unwrap().status {
                TargetStatus::Opening { next_send_at, .. } => Some(next_send_at),
                _ => None,
            })
            .max()
            .unwrap();
        drive_open_attempts(&config, &socket, &shared, deadline);
        for target in [&first, &second] {
            assert!(matches!(
                target.lock().unwrap().status,
                TargetStatus::Opening {
                    awaiting_receiver_generation: Some(_),
                    ..
                }
            ));
        }

        let mut next_order = 2;
        let mut records = TargetOutcomeHistory::default();
        apply_target_update(
            &config,
            &socket,
            &shared,
            vec![first_config],
            &mut next_order,
            &mut records,
        )
        .unwrap();
        assert!(matches!(
            second.lock().unwrap().final_reason,
            Some(ManagedTargetEndReason::Removed)
        ));

        shared.cancellation.cancel();
        cancel_remaining_targets(&socket, &shared, &mut records);
        assert!(matches!(
            first.lock().unwrap().final_reason,
            Some(ManagedTargetEndReason::Cancelled)
        ));
        assert_eq!(records.total, 2);
        assert!(matches!(
            records.recent.front().map(|outcome| &outcome.end_reason),
            Some(ManagedTargetEndReason::Removed)
        ));
        assert!(matches!(
            records.recent.back().map(|outcome| &outcome.end_reason),
            Some(ManagedTargetEndReason::Cancelled)
        ));
    }

    #[test]
    fn final_open_timeout_waits_until_receiver_drain_completes() {
        let interval = Duration::from_millis(10);
        let config = group_config(None, interval, ManagedGroupPacing::Staggered).client;
        let peer = UdpSocket::bind("127.0.0.1:0").unwrap();
        let remote = peer.local_addr().unwrap();
        let now = Instant::now();
        let target = Arc::new(Mutex::new(
            TargetState::new(&config, target("peer", remote), 0, now).unwrap(),
        ));
        let shared = shared_with_target(&config, EventHub::new(), target.clone());
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();

        drive_open_attempts(&config, &socket, &shared, now);
        let deadline = {
            let target = target.lock().expect("target mutex poisoned");
            match target.status {
                TargetStatus::Opening {
                    attempt: 1,
                    next_send_at,
                    ..
                } => next_send_at,
                ref status => panic!("unexpected target status after open send: {status:?}"),
            }
        };

        drive_open_attempts(&config, &socket, &shared, deadline);
        let generation = {
            let target = target.lock().expect("target mutex poisoned");
            let generation = match target.status {
                TargetStatus::Opening {
                    attempt: 1,
                    awaiting_receiver_generation: Some(generation),
                    ..
                } => generation,
                ref status => panic!("unexpected waiting status: {status:?}"),
            };
            assert!(target.final_reason.is_none());
            generation
        };

        complete_receiver_drain(&shared, generation);
        drive_open_attempts(&config, &socket, &shared, deadline);
        let reason = {
            let target = target.lock().expect("target mutex poisoned");
            assert!(matches!(target.status, TargetStatus::Finished));
            assert!(matches!(
                target.final_reason,
                Some(ManagedTargetEndReason::OpenFailed(ManagedTargetFailure {
                    kind: ManagedTargetFailureKind::OpeningTimeout,
                    ..
                }))
            ));
            target.final_reason.clone()
        };

        drive_open_attempts(&config, &socket, &shared, deadline);
        assert_eq!(
            target.lock().expect("target mutex poisoned").final_reason,
            reason
        );
    }

    #[test]
    fn group_probe_separates_permission_committed_send_and_send_call_timing() {
        let duration = Duration::from_secs(1);
        let interval = Duration::from_millis(100);
        let params = test_params(Some(duration), interval);
        let config = group_config(Some(duration), interval, ManagedGroupPacing::Staggered).client;
        let peer = UdpSocket::bind("127.0.0.1:0").unwrap();
        let remote = peer.local_addr().unwrap();
        let opened_mono = Instant::now()
            .checked_sub(Duration::from_secs(2))
            .expect("test host monotonic clock has at least two seconds of history");
        let opened_at = ClientTimestamp {
            wall: std::time::SystemTime::now(),
            mono: opened_mono,
        };
        let mut target = active_probe_target(&config, &params, remote, opened_at);
        let permission_at = opened_mono + Duration::from_millis(500);
        let sent_at = ClientTimestamp {
            wall: std::time::SystemTime::now(),
            mono: opened_mono + Duration::from_millis(525),
        };
        let timestamps = ProbeSendTimestamps {
            permission_at,
            sent_at,
            send_call_start: opened_mono + Duration::from_millis(526),
            send_finished_at: opened_mono + Duration::from_millis(533),
        };
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();

        let events =
            send_echo_to_locked_target_at(&socket, &mut target, opened_mono, timestamps).unwrap();

        assert!(matches!(
            events.as_slice(),
            [ClientEvent::EchoSent {
                scheduled_at,
                sent_at: event_sent_at,
                send_call,
                timer_error,
                ..
            }] if *scheduled_at == permission_at
                && *event_sent_at == sent_at
                && *send_call == Duration::from_millis(7)
                && *timer_error == Duration::from_millis(25)
        ));
        assert_eq!(
            target
                .schedule
                .as_ref()
                .and_then(ProbeSchedule::next_send_deadline),
            Some(opened_mono + Duration::from_millis(600))
        );
        assert_eq!(target.runtime.packets_sent(), 1);
        let timeout_at = sent_at.mono + target.runtime.probe_timeout();
        assert!(target
            .runtime
            .poll_timeouts_at(timeout_at - Duration::from_nanos(1))
            .unwrap()
            .is_empty());
        assert!(matches!(
            target.runtime.poll_timeouts_at(timeout_at).unwrap().as_slice(),
            [ClientEvent::EchoLoss {
                sent_at: loss_sent_at,
                ..
            }] if *loss_sent_at == sent_at
        ));
    }

    #[test]
    fn group_failed_probe_send_preserves_machine_and_schedule() {
        let duration = Duration::from_secs(1);
        let interval = Duration::from_millis(100);
        let params = test_params(Some(duration), interval);
        let config = group_config(Some(duration), interval, ManagedGroupPacing::Staggered).client;
        let peer = UdpSocket::bind("127.0.0.1:0").unwrap();
        let remote = peer.local_addr().unwrap();
        let opened_mono = Instant::now()
            .checked_sub(Duration::from_secs(2))
            .expect("test host monotonic clock has at least two seconds of history");
        let opened_at = ClientTimestamp {
            wall: std::time::SystemTime::now(),
            mono: opened_mono,
        };
        let mut target = active_probe_target(&config, &params, remote, opened_at);
        let sent_at = ClientTimestamp {
            wall: std::time::SystemTime::now(),
            mono: opened_mono + Duration::from_millis(500),
        };
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        target.probe_send_error = true;

        assert!(matches!(
            send_echo_to_locked_target_at(
                &socket,
                &mut target,
                opened_mono,
                probe_timestamps(sent_at.mono, sent_at),
            ),
            Err(ClientError::Socket(_))
        ));
        assert_eq!(target.runtime.packets_sent(), 0);
        assert_eq!(
            target
                .schedule
                .as_ref()
                .and_then(ProbeSchedule::next_send_deadline),
            Some(opened_mono)
        );

        assert!(matches!(
            send_echo_to_locked_target_at(
                &socket,
                &mut target,
                opened_mono,
                probe_timestamps(sent_at.mono, sent_at),
            )
            .unwrap()
            .as_slice(),
            [ClientEvent::EchoSent { seq: 0, .. }]
        ));
    }

    #[test]
    fn group_short_probe_commits_before_presentation_length_error() {
        let duration = Duration::from_secs(1);
        let interval = Duration::from_millis(100);
        let params = test_params(Some(duration), interval);
        let config = group_config(Some(duration), interval, ManagedGroupPacing::Staggered).client;
        let peer = UdpSocket::bind("127.0.0.1:0").unwrap();
        let remote = peer.local_addr().unwrap();
        let opened_mono = Instant::now()
            .checked_sub(Duration::from_secs(2))
            .expect("test host monotonic clock has at least two seconds of history");
        let opened_at = ClientTimestamp {
            wall: std::time::SystemTime::now(),
            mono: opened_mono,
        };
        let mut target = active_probe_target(&config, &params, remote, opened_at);
        let sent_at = ClientTimestamp {
            wall: std::time::SystemTime::now(),
            mono: opened_mono + Duration::from_millis(500),
        };
        let expected = echo_packet_len(false, &params).unwrap();
        target.probe_reported_len = Some(expected - 1);
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();

        assert!(matches!(
            send_echo_to_locked_target_at(
                &socket,
                &mut target,
                opened_mono,
                probe_timestamps(sent_at.mono, sent_at),
            ),
            Err(ClientError::DatagramLengthMismatch {
                expected: error_expected,
                actual,
            }) if error_expected == expected && actual + 1 == expected
        ));
        assert_eq!(target.runtime.packets_sent(), 1);
        assert_eq!(
            target
                .schedule
                .as_ref()
                .and_then(ProbeSchedule::next_send_deadline),
            Some(opened_mono + Duration::from_millis(600))
        );
    }

    #[test]
    fn silent_opening_peer_times_out_all_attempts_and_publishes_once() {
        let silent_peer = UdpSocket::bind("127.0.0.1:0").unwrap();
        let remote = silent_peer.local_addr().unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut config = group_config(
            None,
            Duration::from_millis(10),
            ManagedGroupPacing::Staggered,
        );
        config.client.open_timeouts = vec![Duration::from_millis(200), Duration::from_millis(200)];
        let (session, sub) = ManagedClientGroup::start_with_subscription(
            config,
            vec![target("silent", remote)],
            SubscriberConfig {
                capacity: 8,
                overflow: SubscriberOverflow::DropNewest,
            },
        )
        .unwrap();

        let (join_tx, join_rx) = mpsc::sync_channel(1);
        let joiner = thread::spawn(move || {
            join_tx.send(session.join()).unwrap();
        });
        let mut events = Vec::new();
        loop {
            match sub.try_recv() {
                Ok(Some(event)) => events.push(event),
                Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(1)),
                Ok(None) => panic!("timed out waiting for silent opening peer completion"),
                Err(EventSubscriptionError::Disconnected) => break,
            }
        }

        let outcome = join_rx
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .expect("timed out joining silent opening peer group")
            .unwrap();
        joiner.join().unwrap();
        assert_eq!(sub.try_recv(), Err(EventSubscriptionError::Disconnected));

        silent_peer.set_nonblocking(true).unwrap();
        let mut open_attempts = 0;
        let mut packet = [0_u8; 2048];
        loop {
            match silent_peer.recv_from(&mut packet) {
                Ok((size, _)) => {
                    assert!(size > 3);
                    assert_ne!(packet[3] & FLAG_OPEN, 0);
                    open_attempts += 1;
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(err) => panic!("failed reading queued open request: {err}"),
            }
        }
        assert_eq!(open_attempts, 2);

        let terminal_outcomes = events
            .iter()
            .filter_map(|event| match event {
                ManagedGroupEvent::TargetFinished(outcome) => Some(outcome.clone()),
                ManagedGroupEvent::Client(_) => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(terminal_outcomes.len(), 1);
        let published = &terminal_outcomes[0];
        let failure = match &published.end_reason {
            ManagedTargetEndReason::OpenFailed(failure) => failure,
            reason => panic!("expected OpenFailed, got {reason:?}"),
        };
        assert_eq!(failure.kind, ManagedTargetFailureKind::OpeningTimeout);
        assert!(failure.message.contains("all open requests timed out"));
        assert_eq!(outcome.total_target_outcomes, 1);
        assert_eq!(outcome.failed_target_outcomes, 1);
        assert_eq!(outcome.targets, vec![published.clone()]);
    }

    #[test]
    fn runtime_failure_publishes_terminal_outcome() {
        let interval = Duration::from_millis(10);
        let server = start_silent_runtime_server(test_params(None, interval));
        let mut config = group_config(None, interval, ManagedGroupPacing::Staggered);
        config.client.max_pending_probes = 1;
        let (session, sub) = ManagedClientGroup::start_with_subscription(
            config,
            vec![target("runtime", server.addr)],
            SubscriberConfig {
                capacity: 16,
                overflow: SubscriberOverflow::DropNewest,
            },
        )
        .unwrap();

        let events = collect_group_events_until_disconnected(&sub);
        let outcome = session.join().unwrap();
        server.join();

        let terminal = events
            .iter()
            .find_map(|event| match event {
                ManagedGroupEvent::TargetFinished(outcome) => Some(outcome),
                ManagedGroupEvent::Client(_) => None,
            })
            .expect("missing terminal outcome");
        let failure = terminal
            .end_reason
            .failure()
            .expect("runtime target should fail");
        assert_eq!(failure.kind, ManagedTargetFailureKind::RuntimeProtocol);
        assert!(failure.message.contains("pending probe limit"));
        assert_eq!(outcome.targets, vec![terminal.clone()]);
    }

    #[test]
    fn no_test_event_precedes_terminal_outcome() {
        let interval = Duration::from_millis(10);
        let server = start_no_test_server(test_params(Some(interval), interval));
        let mut config = group_config(Some(interval), interval, ManagedGroupPacing::Staggered);
        config.client.run_mode = crate::RunMode::NoTest;
        let (session, sub) = ManagedClientGroup::start_with_subscription(
            config,
            vec![target("no-test", server.addr)],
            SubscriberConfig {
                capacity: 8,
                overflow: SubscriberOverflow::DropNewest,
            },
        )
        .unwrap();

        let events = collect_group_events_until_disconnected(&sub);
        let outcome = session.join().unwrap();
        server.join();

        assert!(matches!(
            events.as_slice(),
            [
                ManagedGroupEvent::Client(TargetEvent {
                    event: ClientEvent::NoTestCompleted { .. },
                    ..
                }),
                ManagedGroupEvent::TargetFinished(ManagedTargetOutcome {
                    end_reason: ManagedTargetEndReason::NoTestComplete,
                    ..
                })
            ]
        ));
        assert_eq!(
            outcome.targets[0].end_reason,
            ManagedTargetEndReason::NoTestComplete
        );
    }

    #[test]
    fn delayed_burst_pacing_emits_one_current_slot_and_advances_to_future() {
        let interval = Duration::from_millis(10);
        let first_slot = Instant::now();
        let delayed_now = first_slot + Duration::from_millis(45);
        let mut pacing = PacingRuntime::new(ManagedGroupPacing::Burst);
        pacing.next_burst_at = Some(first_slot);

        let scheduled_at = pacing.next_burst(interval, delayed_now).unwrap().unwrap();

        assert_eq!(scheduled_at, first_slot + Duration::from_millis(40));
        assert_eq!(
            pacing.next_wakeup(),
            Some(first_slot + Duration::from_millis(50))
        );
        assert!(pacing.next_wakeup().unwrap() > delayed_now);
    }

    #[test]
    fn delayed_staggered_pacing_skips_historical_slots() {
        let first_slot = Instant::now();
        let delayed_now = first_slot + Duration::from_millis(35);

        let (scheduled_at, next_slot_at, target_index) =
            advance_staggered_slot(first_slot, Duration::from_millis(30), 3, 0, delayed_now)
                .unwrap();

        assert_eq!(scheduled_at, first_slot + Duration::from_millis(30));
        assert_eq!(next_slot_at, first_slot + Duration::from_millis(40));
        assert_eq!(target_index, 0);
        assert!(next_slot_at > delayed_now);
    }

    #[test]
    fn scheduler_without_deadline_blocks_until_control_message() {
        let (control_tx, control_rx) = mpsc::channel();
        let cancellation = CancellationToken::new();
        let worker_cancellation = cancellation.clone();
        let (started_tx, started_rx) = mpsc::sync_channel(0);
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let worker = thread::spawn(move || {
            started_tx.send(()).unwrap();
            result_tx
                .send(wait_for_scheduler_control(
                    &control_rx,
                    &worker_cancellation,
                    None,
                ))
                .unwrap();
        });

        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("scheduler wait did not start");
        assert!(matches!(
            result_rx.recv_timeout(Duration::from_millis(30)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));

        control_tx.send(ControlMessage::Wake).unwrap();
        assert!(matches!(
            result_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("control message did not wake scheduler"),
            Some(ControlMessage::Wake)
        ));
        worker.join().unwrap();
        assert!(!cancellation.is_cancelled());
    }

    #[test]
    fn scheduler_control_disconnection_cancels_blocking_wait() {
        let (control_tx, control_rx) = mpsc::channel();
        let cancellation = CancellationToken::new();
        drop(control_tx);

        assert!(wait_for_scheduler_control(&control_rx, &cancellation, None).is_none());
        assert!(cancellation.is_cancelled());
    }

    #[test]
    fn scheduler_deadline_wakes_without_control_message() {
        let (_control_tx, control_rx) = mpsc::channel();
        let cancellation = CancellationToken::new();
        let started_at = Instant::now();

        assert!(wait_for_scheduler_control(
            &control_rx,
            &cancellation,
            Some(started_at + Duration::from_millis(10)),
        )
        .is_none());
        assert!(started_at.elapsed() >= Duration::from_millis(5));
        assert!(!cancellation.is_cancelled());
    }

    #[test]
    fn staggered_pacing_assigns_distinct_target_slots() {
        let interval = Duration::from_millis(80);
        let first_slot = Instant::now();

        let (first_scheduled_at, second_slot, first_target) =
            advance_staggered_slot(first_slot, interval, 2, 0, first_slot).unwrap();
        let (second_scheduled_at, third_slot, second_target) =
            advance_staggered_slot(second_slot, interval, 2, 1, second_slot).unwrap();

        assert_eq!(first_scheduled_at, first_slot);
        assert_eq!(second_slot, first_slot + Duration::from_millis(40));
        assert_eq!(first_target, 0);
        assert_eq!(second_scheduled_at, second_slot);
        assert_eq!(third_slot, first_slot + interval);
        assert_eq!(second_target, 1);
        assert_ne!(first_scheduled_at, second_scheduled_at);
    }

    #[test]
    fn burst_pacing_uses_one_shared_target_deadline() {
        let interval = Duration::from_millis(80);
        let first_burst = Instant::now();
        let mut pacing = PacingRuntime::new(ManagedGroupPacing::Burst);
        pacing.next_burst_at = Some(first_burst);

        let shared_deadline = pacing.next_burst(interval, first_burst).unwrap().unwrap();

        assert_eq!(shared_deadline, first_burst);
        assert_eq!(pacing.next_wakeup(), Some(first_burst + interval));

        let next_deadline = pacing
            .next_burst(interval, first_burst + interval)
            .unwrap()
            .unwrap();
        assert_eq!(next_deadline, first_burst + interval);
        assert_eq!(pacing.next_wakeup(), Some(first_burst + interval * 2));
    }

    #[test]
    fn delayed_open_target_joins_after_active_target_is_already_sending() {
        let duration = Duration::from_millis(240);
        let interval = Duration::from_millis(50);
        let params = test_params(Some(duration), interval);
        let a = start_echo_server(params.clone(), Duration::ZERO);
        let b = start_echo_server(params, Duration::from_millis(120));

        let (session, sub) = ManagedClientGroup::start_with_subscription(
            group_config(Some(duration), interval, ManagedGroupPacing::Staggered),
            vec![target("a", a.addr), target("b", b.addr)],
            SubscriberConfig {
                capacity: 512,
                overflow: SubscriberOverflow::DropNewest,
            },
        )
        .unwrap();

        let _ = session.join().unwrap();
        let events = drain_after_join(&sub);
        a.join();
        b.join();

        let b_started_at = events
            .iter()
            .find_map(|event| match (&event.target, &event.event) {
                (id, ClientEvent::SessionStarted { at, .. }) if id.as_str() == "b" => Some(at.mono),
                _ => None,
            })
            .expect("target b should eventually open");
        let a_sent_before_b = events
            .iter()
            .filter(|event| {
                event.target.as_str() == "a"
                    && matches!(
                        &event.event,
                        ClientEvent::EchoSent { sent_at, .. } if sent_at.mono < b_started_at
                    )
            })
            .count();

        assert!(
            a_sent_before_b >= 2,
            "active target did not continue sending while b was opening"
        );
        assert!(events.iter().any(|event| {
            event.target.as_str() == "b" && matches!(event.event, ClientEvent::EchoReply { .. })
        }));
    }

    #[test]
    fn staggered_active_set_change_does_not_resend_unchanged_target_early() {
        let duration = Duration::from_millis(360);
        let interval = Duration::from_millis(100);
        let params = test_params(Some(duration), interval);
        let a = start_echo_server(params.clone(), Duration::ZERO);
        let b = start_echo_server(params, Duration::from_millis(150));

        let (session, sub) = ManagedClientGroup::start_with_subscription(
            group_config(Some(duration), interval, ManagedGroupPacing::Staggered),
            vec![target("a", a.addr), target("b", b.addr)],
            SubscriberConfig {
                capacity: 512,
                overflow: SubscriberOverflow::DropNewest,
            },
        )
        .unwrap();

        let _ = session.join().unwrap();
        let events = drain_after_join(&sub);
        a.join();
        b.join();

        let a_sends: Vec<Instant> = events
            .iter()
            .filter_map(|event| match (&event.target, &event.event) {
                (id, ClientEvent::EchoSent { sent_at, .. }) if id.as_str() == "a" => {
                    Some(sent_at.mono)
                }
                _ => None,
            })
            .collect();
        assert!(
            a_sends.len() >= 3,
            "expected enough target a sends to verify spacing, got {a_sends:?}"
        );

        let min_delta = a_sends
            .windows(2)
            .map(|window| window[1].duration_since(window[0]))
            .min()
            .unwrap();
        assert!(
            min_delta >= Duration::from_millis(75),
            "target a was resent too early after active set changed: {min_delta:?}"
        );
    }

    #[test]
    fn update_targets_removes_and_adds_without_restarting_unchanged_target() {
        let interval = Duration::from_millis(30);
        let params = test_params(None, interval);
        let a = start_echo_server(params.clone(), Duration::ZERO);
        let b = start_echo_server(params.clone(), Duration::ZERO);
        let c = start_echo_server(params, Duration::ZERO);

        let (session, sub) = ManagedClientGroup::start_with_subscription(
            group_config(None, interval, ManagedGroupPacing::Burst),
            vec![target("a", a.addr), target("b", b.addr)],
            SubscriberConfig {
                capacity: 512,
                overflow: SubscriberOverflow::DropNewest,
            },
        )
        .unwrap();

        let mut saw_a_reply = false;
        let mut saw_b_reply = false;
        while !(saw_a_reply && saw_b_reply) {
            let event = recv_event_with_timeout(&sub);
            saw_a_reply |= event.target.as_str() == "a"
                && matches!(event.event, ClientEvent::EchoReply { .. });
            saw_b_reply |= event.target.as_str() == "b"
                && matches!(event.event, ClientEvent::EchoReply { .. });
        }

        session
            .update_targets(vec![target("a", a.addr), target("c", c.addr)])
            .unwrap();

        let deadline = Instant::now() + Duration::from_millis(220);
        let mut saw_a_after_update = false;
        let mut saw_c_after_update = false;
        let mut saw_b_echo_after_update = false;
        let mut saw_b_removed = false;
        while Instant::now() < deadline && !(saw_a_after_update && saw_c_after_update) {
            match sub.try_recv() {
                Ok(Some(ManagedGroupEvent::Client(event))) => {
                    saw_a_after_update |= event.target.as_str() == "a"
                        && matches!(event.event, ClientEvent::EchoReply { .. });
                    saw_c_after_update |= event.target.as_str() == "c"
                        && matches!(event.event, ClientEvent::EchoReply { .. });
                    saw_b_echo_after_update |= event.target.as_str() == "b"
                        && matches!(
                            event.event,
                            ClientEvent::EchoSent { .. } | ClientEvent::EchoReply { .. }
                        );
                }
                Ok(Some(ManagedGroupEvent::TargetFinished(target))) => {
                    saw_b_removed |= target.id.as_str() == "b"
                        && target.end_reason == ManagedTargetEndReason::Removed;
                }
                Ok(None) => thread::sleep(Duration::from_millis(1)),
                Err(err) => panic!("subscription ended unexpectedly: {err}"),
            }
        }

        session.stop();
        let outcome = session.join().unwrap();
        a.join();
        b.join();
        c.join();

        assert_eq!(outcome.end_reason, ManagedGroupEndReason::Cancelled);
        assert!(saw_a_after_update);
        assert!(saw_c_after_update);
        assert!(!saw_b_echo_after_update);
        assert!(saw_b_removed);
        assert!(outcome
            .targets
            .iter()
            .any(|target| target.id.as_str() == "b"
                && target.end_reason == ManagedTargetEndReason::Removed));
    }

    #[test]
    fn start_rejects_empty_target_set_with_documented_error() {
        let err = ManagedClientGroup::start(
            group_config(
                None,
                Duration::from_millis(20),
                ManagedGroupPacing::Staggered,
            ),
            Vec::new(),
        )
        .unwrap_err();

        assert!(matches!(err, ClientError::InvalidConfig { .. }));
        assert!(err
            .to_string()
            .contains("managed client group requires at least one target"));
    }

    #[test]
    fn dynamic_group_can_remove_to_empty_and_add_targets_again() {
        let interval = Duration::from_millis(20);
        let params = test_params(None, interval);
        let a = start_echo_server(params.clone(), Duration::ZERO);
        let b = start_echo_server(params, Duration::ZERO);
        let mut config = group_config(None, interval, ManagedGroupPacing::Burst);
        config.completion = ManagedGroupCompletionPolicy::ExplicitCancellation;

        let (session, sub) = ManagedClientGroup::start_with_subscription(
            config,
            vec![target("a", a.addr)],
            SubscriberConfig {
                capacity: 128,
                overflow: SubscriberOverflow::DropNewest,
            },
        )
        .unwrap();

        while {
            let event = recv_event_with_timeout(&sub);
            event.target.as_str() != "a" || !matches!(event.event, ClientEvent::EchoReply { .. })
        } {}

        session.update_targets(Vec::new()).unwrap();
        let removal_deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match sub.try_recv() {
                Ok(Some(ManagedGroupEvent::TargetFinished(outcome)))
                    if outcome.id.as_str() == "a"
                        && outcome.end_reason == ManagedTargetEndReason::Removed =>
                {
                    break;
                }
                Ok(Some(_)) | Ok(None) if Instant::now() < removal_deadline => {
                    thread::sleep(Duration::from_millis(1));
                }
                result => panic!("timed out waiting for removed outcome: {result:?}"),
            }
        }
        assert_eq!(sub.recv_timeout(Duration::from_millis(30)), Ok(None));

        session.update_targets(vec![target("b", b.addr)]).unwrap();
        while {
            let event = recv_event_with_timeout(&sub);
            event.target.as_str() != "b" || !matches!(event.event, ClientEvent::EchoReply { .. })
        } {}

        session.stop();
        let events = collect_group_events_until_disconnected(&sub);
        let outcome = join_group_with_timeout(session, Duration::from_secs(1));
        a.join();
        b.join();

        assert_eq!(outcome.end_reason, ManagedGroupEndReason::Cancelled);
        assert!(events.iter().any(|event| matches!(
            event,
            ManagedGroupEvent::TargetFinished(ManagedTargetOutcome {
                id,
                end_reason: ManagedTargetEndReason::Cancelled,
                ..
            }) if id.as_str() == "b"
        )));
    }

    #[test]
    fn removing_final_target_keeps_control_channel_responsive() {
        let interval = Duration::from_millis(20);
        let server = start_echo_server(test_params(None, interval), Duration::ZERO);
        let mut config = group_config(None, interval, ManagedGroupPacing::Staggered);
        config.completion = ManagedGroupCompletionPolicy::ExplicitCancellation;
        let session = ManagedClientGroup::start(config, vec![target("only", server.addr)]).unwrap();

        session.update_targets(Vec::new()).unwrap();
        session.update_targets(Vec::new()).unwrap();
        let idle_subscription = session
            .subscribe(SubscriberConfig {
                capacity: 4,
                overflow: SubscriberOverflow::DropNewest,
            })
            .expect("an idle dynamic group must still accept subscribers");
        assert_eq!(
            idle_subscription.recv_timeout(Duration::from_millis(30)),
            Ok(None)
        );

        session.stop();
        let outcome = join_group_with_timeout(session, Duration::from_secs(1));
        server.join();

        assert_eq!(outcome.end_reason, ManagedGroupEndReason::Cancelled);
        assert_eq!(
            idle_subscription.try_recv(),
            Err(EventSubscriptionError::Disconnected)
        );
    }

    #[test]
    fn explicit_cancellation_while_idle_publishes_removal_and_disconnects() {
        let interval = Duration::from_millis(20);
        let server = start_echo_server(test_params(None, interval), Duration::ZERO);
        let mut config = group_config(None, interval, ManagedGroupPacing::Staggered);
        config.completion = ManagedGroupCompletionPolicy::ExplicitCancellation;
        let (session, sub) = ManagedClientGroup::start_with_subscription(
            config,
            vec![target("idle", server.addr)],
            SubscriberConfig {
                capacity: 16,
                overflow: SubscriberOverflow::DropNewest,
            },
        )
        .unwrap();

        session.update_targets(Vec::new()).unwrap();
        session.stop();
        let events = collect_group_events_until_disconnected(&sub);
        let outcome = join_group_with_timeout(session, Duration::from_secs(1));
        server.join();

        assert_eq!(outcome.end_reason, ManagedGroupEndReason::Cancelled);
        assert!(events.iter().any(|event| matches!(
            event,
            ManagedGroupEvent::TargetFinished(ManagedTargetOutcome {
                id,
                end_reason: ManagedTargetEndReason::Removed,
                ..
            }) if id.as_str() == "idle"
        )));
    }

    #[test]
    fn dropping_idle_dynamic_group_wakes_scheduler_and_disconnects() {
        let interval = Duration::from_millis(20);
        let server = start_echo_server(test_params(None, interval), Duration::ZERO);
        let mut config = group_config(None, interval, ManagedGroupPacing::Staggered);
        config.completion = ManagedGroupCompletionPolicy::ExplicitCancellation;
        let (session, sub) = ManagedClientGroup::start_with_subscription(
            config,
            vec![target("idle", server.addr)],
            SubscriberConfig {
                capacity: 16,
                overflow: SubscriberOverflow::DropNewest,
            },
        )
        .unwrap();

        session.update_targets(Vec::new()).unwrap();
        drop(session);
        let events = collect_group_events_until_disconnected(&sub);
        server.join();

        assert!(events.iter().any(|event| matches!(
            event,
            ManagedGroupEvent::TargetFinished(ManagedTargetOutcome {
                id,
                end_reason: ManagedTargetEndReason::Removed,
                ..
            }) if id.as_str() == "idle"
        )));
    }

    #[test]
    fn target_churn_retains_bounded_history_after_publishing_every_outcome() {
        let sink = UdpSocket::bind("127.0.0.1:0").unwrap();
        let remote = sink.local_addr().unwrap();
        let mut config = group_config(
            None,
            Duration::from_millis(20),
            ManagedGroupPacing::Staggered,
        );
        config.completion = ManagedGroupCompletionPolicy::ExplicitCancellation;
        config.client.open_timeouts = vec![Duration::from_secs(5)];
        let churn_count = MANAGED_GROUP_OUTCOME_HISTORY_LIMIT + 16;
        let (session, sub) = ManagedClientGroup::start_with_subscription(
            config,
            vec![target("churn", remote)],
            SubscriberConfig {
                capacity: churn_count + 8,
                overflow: SubscriberOverflow::DropNewest,
            },
        )
        .unwrap();

        session.update_targets(Vec::new()).unwrap();
        for _ in 0..churn_count {
            session
                .update_targets(vec![target("churn", remote)])
                .unwrap();
            session.update_targets(Vec::new()).unwrap();
        }

        session.stop();
        let events = collect_group_events_until_disconnected(&sub);
        let outcome = session.join().unwrap();
        drop(sink);

        let expected_total = u64::try_from(churn_count + 1).unwrap();
        let published = events
            .iter()
            .filter(|event| matches!(event, ManagedGroupEvent::TargetFinished(_)))
            .count();
        assert_eq!(published, churn_count + 1);
        assert_eq!(outcome.total_target_outcomes, expected_total);
        assert_eq!(outcome.successful_target_outcomes, 0);
        assert_eq!(outcome.peer_closed_target_outcomes, 0);
        assert_eq!(outcome.failed_target_outcomes, 0);
        assert_eq!(outcome.targets.len(), MANAGED_GROUP_OUTCOME_HISTORY_LIMIT);
        assert_eq!(
            outcome.discarded_target_outcomes,
            expected_total - u64::try_from(MANAGED_GROUP_OUTCOME_HISTORY_LIMIT).unwrap()
        );
        assert!(outcome
            .targets
            .iter()
            .all(|target| target.end_reason == ManagedTargetEndReason::Removed));
    }

    #[test]
    fn finite_group_drains_late_reply_during_final_drain() {
        let interval = Duration::from_millis(100);
        let duration = interval;
        let params = test_params(Some(duration), interval);
        let (server, release_reply) = start_gated_reply_server(params);
        let target = target("late", server.addr);
        let mut config = group_config(Some(duration), interval, ManagedGroupPacing::Staggered);
        config.client.probe_timeout = Duration::from_millis(20);
        let (session, sub) = ManagedClientGroup::start_with_subscription(
            config,
            vec![target.clone()],
            SubscriberConfig {
                capacity: 16,
                overflow: SubscriberOverflow::DropNewest,
            },
        )
        .unwrap();

        let mut events = Vec::new();
        loop {
            let event = sub
                .recv_timeout(Duration::from_secs(2))
                .expect("subscription disconnected before EchoLoss")
                .expect("timed out waiting for EchoLoss");
            let saw_loss = matches!(
                &event,
                ManagedGroupEvent::Client(TargetEvent {
                    event: ClientEvent::EchoLoss { seq: 0, .. },
                    ..
                })
            );
            events.push(event);
            if saw_loss {
                break;
            }
        }

        // EchoLoss is published before this scheduler iteration transitions the
        // target to Draining. A synchronous no-op update is handled at the next
        // loop head, so its return proves that transition completed before the
        // server releases the retained reply.
        session.update_targets(vec![target]).unwrap();
        release_reply.send(()).unwrap();
        let outcome = join_group_with_timeout(session, Duration::from_secs(1));
        events.extend(collect_group_events_until_disconnected(&sub));
        server.join();

        assert_eq!(
            final_drain_event_sequence(&events),
            ["started", "sent", "loss", "late", "closed", "finished"]
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event,
                    ManagedGroupEvent::Client(TargetEvent {
                        event: ClientEvent::EchoLoss { seq: 0, .. },
                        ..
                    })
                ))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event,
                    ManagedGroupEvent::Client(TargetEvent {
                        event: ClientEvent::LateReply {
                            seq: 0,
                            sent_at: Some(_),
                            rtt: Some(_),
                            ..
                        },
                        ..
                    })
                ))
                .count(),
            1
        );

        assert_eq!(
            outcome.end_reason,
            ManagedGroupEndReason::AllTargetsComplete
        );
        assert_eq!(outcome.targets.len(), 1);
        let target = &outcome.targets[0];
        assert_eq!(target.end_reason, ManagedTargetEndReason::TestComplete);
        assert_eq!(target.packets_sent, 1);
        assert_eq!(target.replies_received, 0);
        assert_eq!(target.duplicates, 0);
        assert_eq!(target.late, 1);
        assert_eq!(target.warning_events, 0);
        assert_eq!(outcome.total_target_outcomes, 1);
        assert_eq!(outcome.successful_target_outcomes, 1);
        assert_eq!(outcome.peer_closed_target_outcomes, 0);
        assert_eq!(outcome.failed_target_outcomes, 0);
        assert_eq!(outcome.discarded_target_outcomes, 0);
        assert_eq!(
            events.iter().find_map(|event| match event {
                ManagedGroupEvent::TargetFinished(target) => Some(target),
                _ => None,
            }),
            Some(target)
        );
        assert_eq!(sub.dropped_events(), 0);
        assert_eq!(sub.try_recv(), Err(EventSubscriptionError::Disconnected));
    }

    #[test]
    fn finite_group_final_drain_expires_without_late_reply() {
        let interval = Duration::from_millis(100);
        let duration = interval;
        let server = start_silent_runtime_server(test_params(Some(duration), interval));
        let mut config = group_config(Some(duration), interval, ManagedGroupPacing::Staggered);
        config.client.probe_timeout = Duration::from_millis(20);
        let (session, sub) = ManagedClientGroup::start_with_subscription(
            config,
            vec![target("silent", server.addr)],
            SubscriberConfig {
                capacity: 16,
                overflow: SubscriberOverflow::DropNewest,
            },
        )
        .unwrap();

        let outcome = join_group_with_timeout(session, Duration::from_secs(1));
        let events = collect_group_events_until_disconnected(&sub);
        server.join();

        assert_eq!(
            final_drain_event_sequence(&events),
            ["started", "sent", "loss", "closed", "finished"]
        );
        assert!(!events.iter().any(|event| matches!(
            event,
            ManagedGroupEvent::Client(TargetEvent {
                event: ClientEvent::LateReply { .. },
                ..
            })
        )));
        let timeout_at = events
            .iter()
            .find_map(|event| match event {
                ManagedGroupEvent::Client(TargetEvent {
                    event: ClientEvent::EchoLoss { timeout_at, .. },
                    ..
                }) => Some(*timeout_at),
                _ => None,
            })
            .expect("missing EchoLoss");
        let closed_at = events
            .iter()
            .find_map(|event| match event {
                ManagedGroupEvent::Client(TargetEvent {
                    event: ClientEvent::SessionClosed { at, .. },
                    ..
                }) => Some(at.mono),
                _ => None,
            })
            .expect("missing SessionClosed");
        assert!(closed_at >= timeout_at + GROUP_FINAL_DRAIN);

        assert_eq!(
            outcome.end_reason,
            ManagedGroupEndReason::AllTargetsComplete
        );
        assert_eq!(outcome.targets.len(), 1);
        let target = &outcome.targets[0];
        assert_eq!(target.end_reason, ManagedTargetEndReason::TestComplete);
        assert_eq!(target.packets_sent, 1);
        assert_eq!(target.replies_received, 0);
        assert_eq!(target.duplicates, 0);
        assert_eq!(target.late, 0);
        assert_eq!(target.warning_events, 0);
        assert_eq!(outcome.total_target_outcomes, 1);
        assert_eq!(outcome.successful_target_outcomes, 1);
        assert_eq!(outcome.peer_closed_target_outcomes, 0);
        assert_eq!(outcome.failed_target_outcomes, 0);
        assert_eq!(outcome.discarded_target_outcomes, 0);
        assert_eq!(
            events.iter().find_map(|event| match event {
                ManagedGroupEvent::TargetFinished(target) => Some(target),
                _ => None,
            }),
            Some(target)
        );
        assert_eq!(sub.dropped_events(), 0);
        assert_eq!(sub.try_recv(), Err(EventSubscriptionError::Disconnected));
    }

    #[test]
    fn finite_static_group_still_completes_naturally() {
        let interval = Duration::from_millis(10);
        let duration = Duration::from_millis(30);
        let server = start_echo_server(test_params(Some(duration), interval), Duration::ZERO);
        let session = ManagedClientGroup::start(
            group_config(Some(duration), interval, ManagedGroupPacing::Staggered),
            vec![target("finite", server.addr)],
        )
        .unwrap();

        let outcome = session.join().unwrap();
        server.join();

        assert_eq!(
            outcome.end_reason,
            ManagedGroupEndReason::AllTargetsComplete
        );
        assert_eq!(outcome.targets.len(), 1);
        assert_eq!(
            outcome.targets[0].end_reason,
            ManagedTargetEndReason::TestComplete
        );
    }

    #[test]
    fn stop_join_cleans_up_group_threads() {
        let interval = Duration::from_millis(20);
        let params = test_params(None, interval);
        let a = start_echo_server(params.clone(), Duration::ZERO);
        let b = start_echo_server(params, Duration::ZERO);

        let (session, sub) = ManagedClientGroup::start_with_subscription(
            group_config(None, interval, ManagedGroupPacing::Staggered),
            vec![target("a", a.addr), target("b", b.addr)],
            SubscriberConfig {
                capacity: 64,
                overflow: SubscriberOverflow::DropNewest,
            },
        )
        .unwrap();

        thread::sleep(Duration::from_millis(60));
        session.stop();
        session.stop();
        let events = collect_group_events_until_disconnected(&sub);
        let outcome = session.join().unwrap();
        a.join();
        b.join();

        assert_eq!(outcome.end_reason, ManagedGroupEndReason::Cancelled);
        assert_eq!(outcome.targets.len(), 2);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event,
                    ManagedGroupEvent::TargetFinished(ManagedTargetOutcome {
                        end_reason: ManagedTargetEndReason::Cancelled,
                        ..
                    })
                ))
                .count(),
            2
        );
    }
}
