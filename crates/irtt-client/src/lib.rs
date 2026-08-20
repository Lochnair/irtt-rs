//! Reusable client/session/event layer for IRTT-compatible probes.
//!
//! `irtt-client` opens IRTT-compatible sessions, sends echo probes, receives
//! replies, classifies loss/late/duplicate packets, and emits [`ClientEvent`]
//! values for callers to consume directly or aggregate with `irtt-stats`.
//! With the `tokio` feature enabled, `AsyncClient` provides the same low-level
//! lifecycle for callers that own a Tokio runtime and drive readiness directly.
//!
//! Timing values intentionally preserve signed measurement semantics. When
//! server timing is available, [`RttSample::effective`] is adjusted for server
//! processing and can be negative if the reported server processing time exceeds
//! the raw client-observed RTT. Directional [`OneWayDelaySample`] values are
//! also signed when the required wall-clock timestamps are available; negative
//! values usually indicate client/server clock skew.
//!
//! [`Client`] is the runtime-free low-level blocking adapter and requires no
//! Tokio dependency at all; it is what a caller without a runtime, or that
//! wants full control over its own event loop, should start from. With the
//! `tokio` feature, `AsyncClient` provides the corresponding low-level Tokio
//! adapter for a caller that owns a runtime and drives socket readiness
//! itself. [`managed`] sits above both: `ManagedClientTask` /
//! `ManagedClientHandle` (`tokio` feature) run and control one or more
//! targets under a Tokio runtime, and `BlockingManagedClient` (`tokio`
//! feature) wraps that in a synchronous owner backed by its own dedicated
//! current-thread runtime for callers that want managed multi-target
//! behavior without touching Tokio themselves.
//!
//! See `examples/` in the repository for a runnable example of each tier.
//!
#![forbid(unsafe_code)]

#[cfg(feature = "tokio")]
mod async_client;
mod client;
mod config;
mod error;
mod event;
pub mod managed;
mod metadata;
mod probe;
mod receive;
mod session;
mod socket;
mod socket_options;
mod timing;

#[cfg(feature = "tokio")]
pub use async_client::AsyncClient;
pub use client::Client;
pub use config::{
    ClientAuthConfig, ClientConfig, NegotiationPolicy, RecvBudget, RunMode, SocketConfig,
    MAX_DSCP_CODEPOINT, MAX_SERVER_FILL_BYTES, MAX_TTL, MAX_UDP_PAYLOAD_LENGTH,
};
pub use error::ClientError;
pub use event::{
    ClientEvent, OneWayDelaySample, OpenOutcome, PacketMeta, ReceivedStatsSample, RttSample,
    ServerTiming, SignedDuration, WarningKind,
};
pub use session::{NegotiatedParams, NegotiationRestriction};
pub use timing::ClientTimestamp;
