//! Reusable client/session/event layer for IRTT-compatible probes.
//!
//! `irtt-client` opens IRTT-compatible sessions, sends echo probes, receives
//! replies, classifies loss/late/duplicate packets, and emits [`ClientEvent`]
//! values for callers to consume directly or aggregate with `irtt-stats`.
//!
//! Timing values intentionally preserve signed measurement semantics. When
//! server timing is available, [`RttSample::effective`] is adjusted for server
//! processing and can be negative if the reported server processing time exceeds
//! the raw client-observed RTT. Directional [`OneWayDelaySample`] values are
//! also signed when the required wall-clock timestamps are available; negative
//! values usually indicate client/server clock skew.
//!
//! A managed session can drive the socket loop on a worker thread and publish
//! events through a subscription:
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use irtt_client::{ClientConfig, ClientEvent, ManagedClient, SubscriberConfig};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let config = ClientConfig {
//!     server_addr: "127.0.0.1:2112".to_owned(),
//!     duration: Some(Duration::from_secs(10)),
//!     interval: Duration::from_secs(1),
//!     ..ClientConfig::default()
//! };
//!
//! let (session, events) =
//!     ManagedClient::start_with_subscription(config, SubscriberConfig::default())?;
//!
//! while let Ok(event) = events.recv() {
//!     match event {
//!         ClientEvent::EchoReply { seq, rtt, .. } => {
//!             println!("seq={seq} effective_rtt_us={}", rtt.effective.as_micros());
//!         }
//!         ClientEvent::SessionClosed { .. } => break,
//!         _ => {}
//!     }
//! }
//!
//! let _outcome = session.join()?;
//! # Ok(())
//! # }
//! ```
//!
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
mod runtime;
mod session;
mod socket;
mod socket_options;
mod timing;

pub use client::Client;
pub use config::{
    ClientAuthConfig, ClientConfig, NegotiationPolicy, RecvBudget, RunMode, SocketConfig,
    MAX_DSCP_CODEPOINT, MAX_SERVER_FILL_BYTES, MAX_TTL, MAX_UDP_PAYLOAD_LENGTH,
};
pub use error::{ClientError, EventSubscriptionError};
pub use event::{
    ClientEvent, OneWayDelaySample, OpenOutcome, PacketMeta, ReceivedStatsSample, RttSample,
    ServerTiming, SignedDuration, WarningKind,
};
pub use managed::{
    CancellationToken, EventHub, EventSubscription, ManagedClient, ManagedClientGroup,
    ManagedClientGroupConfig, ManagedClientGroupSession, ManagedClientSession,
    ManagedGroupEndReason, ManagedGroupOutcome, ManagedGroupPacing, ManagedTargetConfig,
    ManagedTargetEndReason, ManagedTargetOutcome, SessionEndReason, SessionOutcome,
    SubscriberConfig, SubscriberOverflow, TargetEvent, TargetEventSubscription, TargetId,
};
pub use session::{NegotiatedParams, NegotiationRestriction};
pub use timing::ClientTimestamp;
