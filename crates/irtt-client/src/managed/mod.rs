mod cancellation;
mod group;
mod hub;
mod runner;
mod target_id;

pub use target_id::TargetId;

/// Temporary exports for the obsolete thread-based managed implementations.
#[doc(hidden)]
pub mod legacy {
    pub use super::cancellation::CancellationToken;
    pub use super::group::{
        ManagedClientGroup, ManagedClientGroupConfig, ManagedClientGroupSession,
        ManagedGroupCompletionPolicy, ManagedGroupEndReason, ManagedGroupEvent,
        ManagedGroupOutcome, ManagedGroupPacing, ManagedTargetConfig, ManagedTargetEndReason,
        ManagedTargetFailure, ManagedTargetFailureKind, ManagedTargetOutcome, TargetEvent,
        TargetEventSubscription, MANAGED_GROUP_OUTCOME_HISTORY_LIMIT,
    };
    pub use super::hub::{EventHub, EventSubscription, SubscriberConfig, SubscriberOverflow};
    pub use super::runner::{
        ManagedClient, ManagedClientSession, SessionEndReason, SessionOutcome,
    };
    pub use super::TargetId;
}

#[cfg(feature = "tokio")]
mod blocking;
#[cfg(feature = "tokio")]
mod model;
#[cfg(feature = "tokio")]
mod task;

#[cfg(feature = "tokio")]
pub use blocking::*;
#[cfg(feature = "tokio")]
pub use model::*;
#[cfg(feature = "tokio")]
pub use task::*;
