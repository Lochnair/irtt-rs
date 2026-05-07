#![cfg_attr(
    not(all(target_os = "linux", feature = "ancillary")),
    forbid(unsafe_code)
)]
#![cfg_attr(all(target_os = "linux", feature = "ancillary"), deny(unsafe_code))]

mod client;
mod config;
mod error;
mod event;
mod managed;
mod metadata;
mod probe;
mod receive;
mod session;
mod socket;
mod socket_options;
mod timing;

pub use client::Client;
pub use config::{
    ClientConfig, NegotiationPolicy, RecvBudget, RunMode, SocketConfig, MAX_DSCP_CODEPOINT,
    MAX_SERVER_FILL_BYTES, MAX_TTL, MAX_UDP_PAYLOAD_LENGTH,
};
pub use error::{ClientError, EventSubscriptionError};
pub use event::{
    ClientEvent, OneWayDelaySample, OpenOutcome, PacketMeta, ReceivedStatsSample, RttSample,
    ServerTiming, SignedDuration, WarningKind,
};
pub use managed::{
    CancellationToken, EventHub, EventSubscription, ManagedClient, ManagedClientSession,
    SessionEndReason, SessionOutcome, SubscriberConfig, SubscriberOverflow,
};
pub use session::NegotiatedParams;
pub use timing::ClientTimestamp;
