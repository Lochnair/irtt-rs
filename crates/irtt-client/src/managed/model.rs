use std::{fmt, net::SocketAddr, sync::Arc, time::Duration};

use thiserror::Error;

use crate::{AddressFamily, Authentication, ClientConfig, ClientError, ClientEvent};

use super::TargetId;

/// Default capacity of the lossy managed event channel.
pub const DEFAULT_MANAGED_EVENT_CAPACITY: usize = 256;
/// Default capacity of the bounded managed control queue.
pub const DEFAULT_MANAGED_COMMAND_CAPACITY: usize = 64;
/// Default number of recent target outcomes retained in status and output.
pub const DEFAULT_MANAGED_OUTCOME_HISTORY_LIMIT: usize = 256;
/// Default maximum number of simultaneously live target generations.
pub const DEFAULT_MANAGED_MAX_LIVE_TARGET_GENERATIONS: usize = 256;
/// Default time retained for late replies after the final committed timeout.
pub const DEFAULT_MANAGED_FINAL_DRAIN: Duration = Duration::from_millis(100);

/// One globally allocated incarnation of a target within a managed task.
///
/// Generations increase globally across the task, not independently per ID.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TargetInstance {
    pub id: TargetId,
    pub generation: u64,
}

/// Authentication specification for a managed target.
///
/// Equality preserves the specification: inheritance and an explicit override
/// remain distinct even when they currently resolve to the same authentication.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum TargetAuth {
    /// Use the shared client configuration's authentication.
    #[default]
    Inherit,
    /// Replace shared authentication. `Unauthenticated` explicitly disables it.
    Override(Authentication),
}

impl TargetAuth {
    pub(crate) fn resolve(&self, shared: &Authentication) -> Authentication {
        match self {
            Self::Inherit => shared.clone(),
            Self::Override(auth) => auth.clone(),
        }
    }
}

/// Endpoint, address-family and authentication specification for one managed target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedTargetConfig {
    pub id: TargetId,
    pub server_addr: String,
    /// Override the shared address-family policy. `None` inherits it.
    ///
    /// The endpoint remains a hostname when supplied as one, so every new
    /// generation resolves it again using the effective family policy.
    pub address_family: Option<AddressFamily>,
    pub auth: TargetAuth,
}

impl ManagedTargetConfig {
    pub fn new(id: impl Into<TargetId>, server_addr: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            server_addr: server_addr.into(),
            address_family: None,
            auth: TargetAuth::Inherit,
        }
    }
}

/// Coordination mode for sends across active targets.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ManagedPacing {
    #[default]
    Staggered,
    Burst,
}

/// Policy controlling top-level natural completion.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ManagedCompletionPolicy {
    #[default]
    FinishWhenQuiescent,
    ExplicitStop,
}

/// Shared configuration for one managed task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedClientConfig {
    /// Reusable session/socket configuration shared across target generations.
    /// Each target supplies its endpoint and resolves its family and authentication policies.
    pub client: ClientConfig,
    pub pacing: ManagedPacing,
    pub completion: ManagedCompletionPolicy,
    pub event_capacity: usize,
    pub command_capacity: usize,
    pub outcome_history_limit: usize,
    pub max_live_target_generations: usize,
    pub final_drain: Duration,
}

impl Default for ManagedClientConfig {
    fn default() -> Self {
        Self {
            client: ClientConfig::default(),
            pacing: ManagedPacing::default(),
            completion: ManagedCompletionPolicy::default(),
            event_capacity: DEFAULT_MANAGED_EVENT_CAPACITY,
            command_capacity: DEFAULT_MANAGED_COMMAND_CAPACITY,
            outcome_history_limit: DEFAULT_MANAGED_OUTCOME_HISTORY_LIMIT,
            max_live_target_generations: DEFAULT_MANAGED_MAX_LIVE_TARGET_GENERATIONS,
            final_drain: DEFAULT_MANAGED_FINAL_DRAIN,
        }
    }
}

/// Durable top-level lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagedLifecycle {
    NotStarted,
    Running,
    Stopping,
    Completed,
    Failed,
    Abandoned,
}

/// Durable lifecycle for one target generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagedTargetLifecycle {
    Pending,
    Connecting,
    Opening,
    Active,
    Draining,
    Closing,
    Terminal,
}

/// Status for one target generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedTargetStatus {
    pub target: TargetInstance,
    pub desired: bool,
    pub lifecycle: ManagedTargetLifecycle,
    /// Authoritative completed accounting for this target generation.
    ///
    /// Present if and only if `lifecycle` is [`ManagedTargetLifecycle::Terminal`].
    /// This shares the terminal phase's outcome with [`ManagedEvent::TargetFinished`],
    /// so terminal details remain durable even when presentation events are lost.
    pub outcome: Option<Arc<ManagedTargetOutcome>>,
    pub server_addr: Arc<str>,
    pub remote: Option<SocketAddr>,
}

/// Authoritative immutable managed status snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedStatus {
    pub lifecycle: ManagedLifecycle,
    /// Whether the task durably observed an explicit stop request.
    ///
    /// This remains false until the task observes the request latch. Natural
    /// stopping and terminality alone do not imply a stop request was observed.
    pub stop_requested: bool,
    pub applied_command_sequence: u64,
    pub desired_target_count: usize,
    pub connecting_target_count: usize,
    pub opening_target_count: usize,
    pub active_target_count: usize,
    pub draining_target_count: usize,
    pub closing_target_count: usize,
    pub terminal_target_count: usize,
    pub total_target_outcomes: u64,
    pub successful_target_outcomes: u64,
    pub failed_target_outcomes: u64,
    pub peer_closed_target_outcomes: u64,
    pub discarded_target_outcomes: u64,
    pub targets: Arc<[ManagedTargetStatus]>,
    pub recent_target_outcomes: Arc<[ManagedTargetOutcome]>,
    /// Authoritative final accounting, present if and only if `lifecycle` is
    /// [`ManagedLifecycle::Completed`] or [`ManagedLifecycle::Failed`].
    ///
    /// Shares the terminal task phase's outcome with [`ManagedEvent::Completed`]
    /// or [`ManagedEvent::Failed`]. Abandoned deliberately has no outcome.
    pub final_outcome: Option<Arc<ManagedOutcome>>,
}

/// Receiving half of authoritative durable latest-state observation.
///
/// The current snapshot is immediately readable. Updates may coalesce, and the
/// final snapshot remains available after the sender closes at task termination.
/// Use [`ManagedEventSubscription`] for lossy presentation events.
pub type ManagedStatusSubscription = tokio::sync::watch::Receiver<Arc<ManagedStatus>>;

/// Lossy presentation event emitted by the managed task.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ManagedEvent {
    Started,
    TargetStateChanged {
        target: TargetInstance,
        lifecycle: ManagedTargetLifecycle,
    },
    Client {
        target: TargetInstance,
        event: ClientEvent,
    },
    TargetFinished {
        outcome: Arc<ManagedTargetOutcome>,
    },
    Stopping,
    Completed {
        outcome: Arc<ManagedOutcome>,
    },
    Failed {
        outcome: Arc<ManagedOutcome>,
    },
    Abandoned,
}

/// Receiving half of the lossy managed event stream.
pub type ManagedEventSubscription = tokio::sync::broadcast::Receiver<ManagedEvent>;

/// Immediate result of attempting to receive a managed presentation event.
pub type ManagedEventTryRecvError = tokio::sync::broadcast::error::TryRecvError;

/// Final authoritative outcome returned by the task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedOutcome {
    pub end_reason: ManagedEndReason,
    pub applied_command_sequence: u64,
    pub total_target_outcomes: u64,
    pub successful_target_outcomes: u64,
    pub failed_target_outcomes: u64,
    pub peer_closed_target_outcomes: u64,
    pub discarded_target_outcomes: u64,
    pub recent_target_outcomes: Arc<[ManagedTargetOutcome]>,
}

/// Why the managed task ended.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ManagedEndReason {
    TargetsComplete,
    StopRequested,
    DriverFailed(ManagedDriverFailure),
}

/// Durable outcome for one target generation.
///
/// These counters are durable: unlike [`ManagedEvent`], which is a lossy
/// presentation stream, they account for everything the target observed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedTargetOutcome {
    pub target: TargetInstance,
    pub server_addr: Arc<str>,
    pub remote: Option<SocketAddr>,
    pub end_reason: ManagedTargetEndReason,
    /// Probes the underlying client reported as sent.
    pub packets_sent: u64,
    /// Unique echo replies that arrived in order and before their probe timed
    /// out.
    ///
    /// A unique reply that arrives behind an already-received sequence, or
    /// after its own probe timed out, is counted by `late` instead.
    pub replies_received: u64,
    /// Replies for a sequence that had already been answered.
    pub duplicates: u64,
    /// Late replies: those arriving behind an already-received sequence
    /// (reordering) or after their own probe timed out.
    ///
    /// This counts every late reply, whether or not the client could still
    /// match it to retained send state.
    pub late: u64,
    /// Warning events observed for this target.
    pub warning_events: u64,
    pub cleanup_failure: Option<ManagedTargetFailure>,
}

/// Why one target generation ended.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ManagedTargetEndReason {
    TestComplete,
    NoTestComplete,
    PeerClosed,
    Removed,
    Replaced,
    Stopped,
    Failed(ManagedTargetFailure),
}

/// Phase in which a target-local failure occurred.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagedTargetFailurePhase {
    Connecting,
    Opening,
    Sending,
    Receiving,
    Timing,
    Closing,
}

impl fmt::Display for ManagedTargetFailurePhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            Self::Connecting => "connecting",
            Self::Opening => "opening",
            Self::Sending => "sending",
            Self::Receiving => "receiving",
            Self::Timing => "timing",
            Self::Closing => "closing",
        };
        f.write_str(text)
    }
}

/// Stable category for a target-local failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagedTargetFailureKind {
    Resolve,
    Socket,
    SocketOption,
    Protocol,
    Timeout,
    Configuration,
    ResourceExhausted,
    InvalidState,
    /// A failure that deliberately has no more specific category.
    ///
    /// This is a classification decision, not a fallback: every current
    /// [`ClientError`] variant is classified explicitly, so nothing reaches
    /// this category by default.
    Other,
}

impl fmt::Display for ManagedTargetFailureKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            Self::Resolve => "resolve",
            Self::Socket => "socket",
            Self::SocketOption => "socket option",
            Self::Protocol => "protocol",
            Self::Timeout => "timeout",
            Self::Configuration => "configuration",
            Self::ResourceExhausted => "resource exhausted",
            Self::InvalidState => "invalid state",
            Self::Other => "other",
        };
        f.write_str(text)
    }
}

/// Durable target-local failure details.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedTargetFailure {
    pub phase: ManagedTargetFailurePhase,
    pub kind: ManagedTargetFailureKind,
    pub message: Arc<str>,
}

/// Configuration failure detected before any runtime work begins.
#[derive(Debug, Error)]
pub enum ManagedConfigError {
    #[error("FinishWhenQuiescent requires at least one initial target")]
    EmptyInitialTargets,
    #[error("duplicate managed target id {id}")]
    DuplicateTargetId { id: TargetId },
    #[error("configured {configured} targets exceeds live-generation limit {limit}")]
    TooManyTargets { configured: usize, limit: usize },
    #[error("managed event capacity must be greater than zero")]
    ZeroEventCapacity,
    #[error("managed event capacity {configured} exceeds Tokio's maximum {maximum}")]
    EventCapacityTooLarge { configured: usize, maximum: usize },
    #[error("managed command capacity must be greater than zero")]
    ZeroCommandCapacity,
    #[error("managed command capacity {configured} exceeds Tokio's maximum {maximum}")]
    CommandCapacityTooLarge { configured: usize, maximum: usize },
    #[error("managed live-generation limit must be greater than zero")]
    ZeroLiveGenerationLimit,
    #[error("managed final drain {duration:?} cannot be scheduled from the current instant")]
    UnschedulableFinalDrain { duration: Duration },
    #[error("invalid managed target {id}: {source}")]
    InvalidTarget {
        id: TargetId,
        #[source]
        source: ClientError,
    },
    #[error("managed target generation space is exhausted")]
    GenerationExhausted,
}

/// Immediate failure to submit a managed target update.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ManagedCommandError {
    #[error("managed target updates are no longer accepted")]
    Stopping,
    #[error("managed target update contains {configured} targets but the limit is {limit}")]
    TooManyTargets { configured: usize, limit: usize },
    #[error("managed command queue is full")]
    QueueFull,
    #[error("managed command receiver is closed")]
    DriverClosed,
}

/// Failure while applying an accepted managed target update.
#[derive(Debug, Error)]
pub enum ManagedCommandApplyError {
    #[error("managed target updates are no longer accepted")]
    Stopping,
    #[error("duplicate target id {id}")]
    DuplicateTargetId { id: TargetId },
    #[error("invalid configuration for target {id}: {source}")]
    InvalidTarget {
        id: TargetId,
        #[source]
        source: ClientError,
    },
    #[error("target update requires {required} live generations but limit is {limit}")]
    LiveGenerationLimitExceeded { required: usize, limit: usize },
    #[error("managed target generation space is exhausted")]
    GenerationExhausted,
    #[error("managed command sequence is exhausted")]
    CommandSequenceExhausted,
    #[error("managed task ended before command acknowledgement")]
    AcknowledgementDisconnected,
    #[error("managed driver failed before command application: {0}")]
    DriverFailed(ManagedDriverFailure),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedCommandAcknowledgement {
    pub sequence: u64,
    pub status: Arc<ManagedStatus>,
}

/// Failure to create a new event subscription.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ManagedSubscribeError {
    #[error("managed event stream is closed")]
    Closed,
}

/// Whole-driver failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ManagedDriverFailure {
    #[error("ManagedClientTask requires a current Tokio runtime")]
    NoTokioRuntime,
    #[error("managed driver exhausted resources for {operation}")]
    ResourceExhausted { operation: &'static str },
    #[error("managed driver invariant failed: {message}")]
    Internal { message: Arc<str> },
}

/// Map a [`ClientError`] onto its durable managed failure classification.
///
/// The match is deliberately exhaustive over every [`ClientError`] variant. A
/// new variant must fail to compile here until its real classification is
/// chosen, rather than silently degrading to
/// [`ManagedTargetFailureKind::Other`].
pub(crate) fn classify_client_error(
    phase: ManagedTargetFailurePhase,
    error: &ClientError,
) -> ManagedTargetFailure {
    let kind = match error {
        ClientError::Resolve { .. } => ManagedTargetFailureKind::Resolve,
        ClientError::Socket(_) => ManagedTargetFailureKind::Socket,
        ClientError::SocketOption { .. } | ClientError::ReadTimeoutRestore { .. } => {
            ManagedTargetFailureKind::SocketOption
        }
        ClientError::Protocol(_)
        | ClientError::ProtocolVersionMismatch { .. }
        | ClientError::ZeroToken
        | ClientError::UnexpectedNoTestReply
        | ClientError::NonZeroNoTestToken { .. }
        | ClientError::ServerRejected
        | ClientError::NegotiationRejected { .. } => ManagedTargetFailureKind::Protocol,
        ClientError::OpenTimeout => ManagedTargetFailureKind::Timeout,
        ClientError::InvalidConfig { .. }
        | ClientError::OpenTimeoutTooSmall { .. }
        | ClientError::NoOpenTimeouts => ManagedTargetFailureKind::Configuration,
        ClientError::AllocationFailed { .. }
        | ClientError::CounterOverflow { .. }
        | ClientError::DurationOverflow
        | ClientError::PendingLimitExceeded { .. } => ManagedTargetFailureKind::ResourceExhausted,
        ClientError::DatagramLengthMismatch { .. } => ManagedTargetFailureKind::Socket,
        #[cfg(feature = "tokio")]
        ClientError::NoTokioRuntime => ManagedTargetFailureKind::InvalidState,
        ClientError::NotOpen
        | ClientError::AlreadyOpen
        | ClientError::AlreadyCompleted
        | ClientError::AlreadyClosed
        | ClientError::StalePreparedProbe { .. }
        | ClientError::PendingSequenceCollision { .. } => ManagedTargetFailureKind::InvalidState,
    };
    ManagedTargetFailure {
        phase,
        kind,
        message: Arc::from(error.to_string()),
    }
}
