mod cancellation;
mod group;
mod hub;
#[cfg(not(feature = "tokio"))]
mod runner;
#[cfg(feature = "tokio")]
mod tokio_driver;

pub use cancellation::CancellationToken;
pub use group::{
    ManagedClientGroup, ManagedClientGroupConfig, ManagedClientGroupSession,
    ManagedGroupCompletionPolicy, ManagedGroupEndReason, ManagedGroupEvent, ManagedGroupOutcome,
    ManagedGroupPacing, ManagedTargetConfig, ManagedTargetEndReason, ManagedTargetFailure,
    ManagedTargetFailureKind, ManagedTargetOutcome, TargetEvent, TargetEventSubscription, TargetId,
    MANAGED_GROUP_OUTCOME_HISTORY_LIMIT,
};
pub use hub::{EventHub, EventSubscription, SubscriberConfig, SubscriberOverflow};
#[cfg(not(feature = "tokio"))]
pub use runner::{ManagedClient, ManagedClientSession, SessionEndReason, SessionOutcome};
#[cfg(feature = "tokio")]
pub use tokio_driver::{
    ManagedClient, ManagedClientHandle, ManagedClientTask, ManagedCommandAck, ManagedCommandError,
    ManagedCommandReceipt, ManagedEvent, ManagedEventSubscription, ManagedOutcome, ManagedRunError,
    ManagedStartError, ManagedStatus, ManagedSubscribeError, ManagedTaskResult,
};
