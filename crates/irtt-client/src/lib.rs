#![forbid(unsafe_code)]

mod client;
mod config;
mod error;
mod event;
mod managed;
mod probe;
mod session;
mod socket;
mod timing;

pub use client::Client;
pub use config::{ClientConfig, NegotiationPolicy, RecvBudget, RunMode, SocketConfig};
pub use error::{ClientError, EventSubscriptionError};
pub use event::{
    ClientEvent, OneWayDelaySample, OpenOutcome, PacketMeta, ReceivedStatsSample, RttSample,
    ServerTiming, WarningKind,
};
pub use managed::{
    CancellationToken, EventHub, EventSubscription, ManagedClient, ManagedClientSession,
    SessionEndReason, SessionOutcome, SubscriberConfig, SubscriberOverflow,
};
pub use session::NegotiatedParams;
pub use timing::ClientTimestamp;
