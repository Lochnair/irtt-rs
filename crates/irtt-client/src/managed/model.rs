use std::{net::SocketAddr, sync::Arc, time::Duration};

use thiserror::Error;

use crate::{ClientAuthConfig, ClientConfig, ClientError, ClientEvent};

use super::TargetId;

/// Default capacity of the lossy managed event channel.
pub const DEFAULT_MANAGED_EVENT_CAPACITY: usize = 256;
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

/// Endpoint and optional authentication override for one managed target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedTargetConfig {
    pub id: TargetId,
    pub server_addr: String,
    pub auth: Option<ClientAuthConfig>,
}

impl ManagedTargetConfig {
    pub fn new(id: impl Into<TargetId>, server_addr: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            server_addr: server_addr.into(),
            auth: None,
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
    pub client: ClientConfig,
    pub pacing: ManagedPacing,
    pub completion: ManagedCompletionPolicy,
    pub event_capacity: usize,
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
    pub lifecycle: ManagedTargetLifecycle,
    pub server_addr: Arc<str>,
    pub remote: Option<SocketAddr>,
}

/// Authoritative immutable managed status snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedStatus {
    pub lifecycle: ManagedLifecycle,
    pub stop_requested: bool,
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
    pub final_outcome: Option<Arc<ManagedOutcome>>,
}

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

/// Final authoritative outcome returned by the task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedOutcome {
    pub end_reason: ManagedEndReason,
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedTargetOutcome {
    pub target: TargetInstance,
    pub server_addr: Arc<str>,
    pub remote: Option<SocketAddr>,
    pub end_reason: ManagedTargetEndReason,
    pub packets_sent: u64,
    pub replies_received: u64,
    pub duplicates: u64,
    pub late: u64,
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
    Other,
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
    #[error("managed live-generation limit must be greater than zero")]
    ZeroLiveGenerationLimit,
    #[error("invalid managed target {id}: {source}")]
    InvalidTarget {
        id: TargetId,
        #[source]
        source: ClientError,
    },
    #[error("managed target generation space is exhausted")]
    GenerationExhausted,
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
        ClientError::NotOpen
        | ClientError::AlreadyOpen
        | ClientError::AlreadyCompleted
        | ClientError::AlreadyClosed
        | ClientError::StalePreparedProbe { .. }
        | ClientError::PendingSequenceCollision { .. } => ManagedTargetFailureKind::InvalidState,
        _ => ManagedTargetFailureKind::Other,
    };
    ManagedTargetFailure {
        phase,
        kind,
        message: Arc::from(error.to_string()),
    }
}
