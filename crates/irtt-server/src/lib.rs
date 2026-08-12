//! IRTT-compatible server behavior.
//!
//! This crate shares wire semantics with the client through `irtt-proto` and
//! does not depend on `irtt-client`.
//!
//! # Structure
//!
//! [`ServerCore`] is the deterministic protocol and session engine: packet
//! admission, authentication policy, open negotiation, the session table, echo
//! receive state, rate and lifetime policy, resource decisions and reply
//! construction. It performs no I/O; the clock its timestamps and deadlines
//! come from is a private injected seam, not a runtime abstraction, so the
//! engine stays testable. Socket and runtime orchestration — the UDP listener,
//! address handling, timers, scheduled sweeps and shutdown — will live around
//! it and is intentionally Tokio-native. There is no blocking or
//! alternate-runtime counterpart and no transport abstraction.
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
//! [`DEFAULT_MAX_PACKET_LENGTH`]), the rate one session is answered at (see
//! [`DEFAULT_MIN_SEND_INTERVAL`] and [`DEFAULT_BURST_ALLOWANCE`]) and how long
//! one lives without traffic (see [`DEFAULT_IDLE_TIMEOUT`]). A single
//! unauthenticated datagram creates a session and opens are never deduplicated,
//! so neither an unbounded table nor a remotely chosen buffer size is a
//! compatibility feature. Upstream's observed lack of session bounds, its
//! unlimited default packet length, and its never expiring a session that has
//! not carried an echo request are explicitly not compatibility targets: an
//! `irtt-rs` session ages from the moment it is opened.
//!
//! # Current scope
//!
//! Open handling and negotiation, session creation, normal echo processing,
//! per-session rate limiting, idle expiry and the maximum-duration
//! server-initiated close. DSCP application, the full server fill policy and
//! the Tokio runtime are separate slices: echo payloads are zero-filled, and
//! expiry is evaluated when a datagram arrives rather than on a timer.
#![forbid(unsafe_code)]

mod clock;
mod config;
mod core;
mod error;
mod negotiate;
mod session;
mod token;

#[cfg(test)]
mod tests;

pub use config::{
    ServerConfig, DEFAULT_BURST_ALLOWANCE, DEFAULT_IDLE_TIMEOUT, DEFAULT_MAX_PACKET_LENGTH,
    DEFAULT_MAX_SESSIONS, DEFAULT_MIN_SEND_INTERVAL,
};
pub use core::ServerCore;
pub use error::ServerError;
