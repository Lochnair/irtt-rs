use std::{fmt, net::SocketAddr, sync::Arc, time::Duration};

use thiserror::Error;

use crate::{ClientAuthConfig, ClientConfig, ClientError, ClientEvent};

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
    pub server_addr: Arc<str>,
    pub remote: Option<SocketAddr>,
}

/// Authoritative immutable managed status snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedStatus {
    pub lifecycle: ManagedLifecycle,
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

#[cfg(test)]
mod tests {
    use std::{collections::TryReserveError, io};

    use super::*;

    fn io_error() -> io::Error {
        io::Error::other("boom")
    }

    fn try_reserve_error() -> TryReserveError {
        Vec::<u8>::new().try_reserve(usize::MAX).unwrap_err()
    }

    fn classification_cases() -> Vec<(ClientError, ManagedTargetFailureKind)> {
        vec![
            (
                ClientError::Resolve {
                    addr: "example.invalid:2112".to_owned(),
                },
                ManagedTargetFailureKind::Resolve,
            ),
            (
                ClientError::Socket(io_error()),
                ManagedTargetFailureKind::Socket,
            ),
            (
                ClientError::DatagramLengthMismatch {
                    expected: 64,
                    actual: 32,
                },
                ManagedTargetFailureKind::Socket,
            ),
            (
                ClientError::SocketOption {
                    operation: "set read timeout",
                    remote: "127.0.0.1:2112".parse().unwrap(),
                    source: io_error(),
                },
                ManagedTargetFailureKind::SocketOption,
            ),
            (
                ClientError::ReadTimeoutRestore { source: io_error() },
                ManagedTargetFailureKind::SocketOption,
            ),
            (
                ClientError::Protocol(irtt_proto::ProtoError::BadMagic),
                ManagedTargetFailureKind::Protocol,
            ),
            (
                ClientError::ProtocolVersionMismatch {
                    requested: 1,
                    received: 2,
                },
                ManagedTargetFailureKind::Protocol,
            ),
            (ClientError::ZeroToken, ManagedTargetFailureKind::Protocol),
            (
                ClientError::UnexpectedNoTestReply,
                ManagedTargetFailureKind::Protocol,
            ),
            (
                ClientError::NonZeroNoTestToken { token: 7 },
                ManagedTargetFailureKind::Protocol,
            ),
            (
                ClientError::ServerRejected,
                ManagedTargetFailureKind::Protocol,
            ),
            (
                ClientError::NegotiationRejected {
                    reason: "duration".to_owned(),
                },
                ManagedTargetFailureKind::Protocol,
            ),
            (ClientError::OpenTimeout, ManagedTargetFailureKind::Timeout),
            (
                ClientError::InvalidConfig {
                    reason: "interval".to_owned(),
                },
                ManagedTargetFailureKind::Configuration,
            ),
            (
                ClientError::OpenTimeoutTooSmall {
                    timeout: Duration::from_millis(1),
                    minimum: Duration::from_millis(10),
                },
                ManagedTargetFailureKind::Configuration,
            ),
            (
                ClientError::NoOpenTimeouts,
                ManagedTargetFailureKind::Configuration,
            ),
            (
                ClientError::AllocationFailed {
                    operation: "pending probes",
                    source: try_reserve_error(),
                },
                ManagedTargetFailureKind::ResourceExhausted,
            ),
            (
                ClientError::CounterOverflow {
                    counter: "packets_sent",
                },
                ManagedTargetFailureKind::ResourceExhausted,
            ),
            (
                ClientError::DurationOverflow,
                ManagedTargetFailureKind::ResourceExhausted,
            ),
            (
                ClientError::PendingLimitExceeded { limit: 8 },
                ManagedTargetFailureKind::ResourceExhausted,
            ),
            (
                ClientError::NoTokioRuntime,
                ManagedTargetFailureKind::InvalidState,
            ),
            (ClientError::NotOpen, ManagedTargetFailureKind::InvalidState),
            (
                ClientError::AlreadyOpen,
                ManagedTargetFailureKind::InvalidState,
            ),
            (
                ClientError::AlreadyCompleted,
                ManagedTargetFailureKind::InvalidState,
            ),
            (
                ClientError::AlreadyClosed,
                ManagedTargetFailureKind::InvalidState,
            ),
            (
                ClientError::StalePreparedProbe {
                    prepared_seq: 3,
                    next_wire_seq: 4,
                },
                ManagedTargetFailureKind::InvalidState,
            ),
            (
                ClientError::PendingSequenceCollision { seq: 9 },
                ManagedTargetFailureKind::InvalidState,
            ),
        ]
    }

    #[test]
    fn every_client_error_classifies_to_its_documented_kind() {
        for (error, expected) in classification_cases() {
            let failure = classify_client_error(ManagedTargetFailurePhase::Opening, &error);
            assert_eq!(
                failure.kind, expected,
                "unexpected classification for {error:?}"
            );
            assert_eq!(failure.phase, ManagedTargetFailurePhase::Opening);
            assert_eq!(&*failure.message, error.to_string());
        }
    }

    #[test]
    fn classification_covers_every_failure_kind_except_other() {
        let observed: Vec<_> = classification_cases()
            .into_iter()
            .map(|(error, _)| {
                classify_client_error(ManagedTargetFailurePhase::Sending, &error).kind
            })
            .collect();
        for kind in [
            ManagedTargetFailureKind::Resolve,
            ManagedTargetFailureKind::Socket,
            ManagedTargetFailureKind::SocketOption,
            ManagedTargetFailureKind::Protocol,
            ManagedTargetFailureKind::Timeout,
            ManagedTargetFailureKind::Configuration,
            ManagedTargetFailureKind::ResourceExhausted,
            ManagedTargetFailureKind::InvalidState,
        ] {
            assert!(observed.contains(&kind), "no case classifies as {kind}");
        }
        assert!(
            !observed.contains(&ManagedTargetFailureKind::Other),
            "Other must remain a deliberate category, not a fallback"
        );
    }

    #[test]
    fn classification_preserves_the_reported_phase() {
        for phase in [
            ManagedTargetFailurePhase::Connecting,
            ManagedTargetFailurePhase::Opening,
            ManagedTargetFailurePhase::Sending,
            ManagedTargetFailurePhase::Receiving,
            ManagedTargetFailurePhase::Timing,
            ManagedTargetFailurePhase::Closing,
        ] {
            let failure = classify_client_error(phase, &ClientError::OpenTimeout);
            assert_eq!(failure.phase, phase);
        }
    }

    #[test]
    fn phase_and_kind_display_without_debug_spelling() {
        assert_eq!(
            ManagedTargetFailurePhase::Connecting.to_string(),
            "connecting"
        );
        assert_eq!(ManagedTargetFailurePhase::Closing.to_string(), "closing");
        assert_eq!(
            ManagedTargetFailureKind::SocketOption.to_string(),
            "socket option"
        );
        assert_eq!(
            ManagedTargetFailureKind::ResourceExhausted.to_string(),
            "resource exhausted"
        );
        assert_eq!(
            ManagedTargetFailureKind::InvalidState.to_string(),
            "invalid state"
        );
        assert_eq!(ManagedTargetFailureKind::Other.to_string(), "other");
    }
}
