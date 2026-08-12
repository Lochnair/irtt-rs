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
//! Total session state is bounded (see [`DEFAULT_MAX_SESSIONS`]), and so is the
//! echo datagram a single session can negotiate (see
//! [`DEFAULT_MAX_PACKET_LENGTH`]). A single unauthenticated datagram creates a
//! session and opens are never deduplicated, so neither an unbounded table nor a
//! remotely chosen buffer size is a compatibility feature. Upstream's observed
//! lack of session bounds, and its unlimited default packet length, are
//! explicitly not compatibility targets.
//!
//! # Current scope
//!
//! Open handling, session creation and client-initiated close. Echo
//! processing, server-initiated close, session lifetime and expiry, and the
//! Tokio runtime are separate slices; a structurally valid echo request is
//! currently accepted by the admission boundary and then ignored.
#![forbid(unsafe_code)]

mod config;
mod core;
mod error;
mod negotiate;
mod session;
mod token;

#[cfg(test)]
mod tests;

pub use config::{ServerConfig, DEFAULT_MAX_PACKET_LENGTH, DEFAULT_MAX_SESSIONS};
pub use core::ServerCore;
pub use error::ServerError;
