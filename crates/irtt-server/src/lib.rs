//! IRTT-compatible server behavior.
//!
//! This crate shares wire semantics with the client through `irtt-proto` and
//! does not depend on `irtt-client`.
//!
//! # Structure
//!
//! [`ServerCore`] is the deterministic protocol and session engine: packet
//! admission, authentication policy, open negotiation, the session table,
//! resource decisions and reply construction. It performs no I/O. Socket and
//! runtime orchestration — the UDP listener, address handling, timers, expiry
//! and shutdown — will live around it and is intentionally Tokio-native. There
//! is no blocking or alternate-runtime counterpart and no transport
//! abstraction.
//!
//! # Rejection is silence
//!
//! The protocol defines no error reply, no reset and no NACK. Malformed,
//! unauthenticated and policy-refused datagrams are discarded without a
//! response and without disturbing any live session, and a client tells
//! "rejected" from "lost" only by timing out. [`ServerError`] is therefore
//! reserved for internal failures on the server's own side.
//!
//! # Resource policy
//!
//! Total session state is bounded (see [`DEFAULT_MAX_SESSIONS`]). A single
//! unauthenticated datagram creates a session and opens are never deduplicated,
//! so an unbounded table is a liability rather than a compatibility feature.
//! Upstream's observed lack of session bounds is explicitly not a compatibility
//! target.
//!
//! # Current scope
//!
//! Open handling and session creation only. Echo processing, client-initiated
//! close, session lifetime and expiry, and the Tokio runtime are separate
//! slices; structurally valid echo and close requests are currently accepted by
//! the admission boundary and then ignored.
#![forbid(unsafe_code)]

mod config;
mod core;
mod error;
mod negotiate;
mod session;
mod token;

#[cfg(test)]
mod tests;

pub use config::{ServerConfig, DEFAULT_MAX_SESSIONS};
pub use core::ServerCore;
pub use error::ServerError;
