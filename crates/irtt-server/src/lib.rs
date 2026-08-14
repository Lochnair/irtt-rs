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
//! engine stays testable. Each reply it produces is an [`OutboundDatagram`]:
//! the packet and the raw traffic class it must be sent with. [`Server`] owns
//! one Tokio UDP listener and one core, and provides sequential receive, reply,
//! scheduled expiry maintenance and caller-controlled shutdown. The crate is
//! intentionally Tokio-native; there is no blocking or alternate-runtime
//! counterpart and no transport abstraction.
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
//! # Reply source address
//!
//! A reply leaves from the address its request was sent to. An
//! explicit-address listener gets that from the bind and uses ordinary
//! receive/send. A wildcard listener (`0.0.0.0` or `[::]`) instead reads each
//! request's local destination from packet metadata and sends that request's
//! reply from it, so a client on the host's second address is answered from the
//! endpoint it contacted rather than from whichever one the routing table would
//! have chosen.
//!
//! That path is implemented for Linux, macOS and FreeBSD. On other targets a
//! wildcard bind is refused by [`Server::from_socket`] — and so by
//! [`Server::bind`] — rather than served incorrectly; explicit-address
//! listeners work everywhere they did before.
//!
//! # Current scope
//!
//! Open handling and negotiation, session creation, normal echo processing,
//! per-session rate limiting, idle expiry, the maximum-duration
//! server-initiated close, the negotiated per-session reply traffic class,
//! server fill, wildcard reply-source selection and Tokio UDP orchestration.
//! Running several listeners in one process remains a separate slice, as do
//! optional timestamp and DSCP restriction controls.
//!
//! # Server fill
//!
//! An echo reply's payload region is filled from the session's negotiated
//! ServerFill descriptor: `none`, `rand`, or `pattern:` followed by a repeating
//! hexadecimal pattern. Every valid descriptor is accepted — there is no
//! allow-list to configure — and one this server cannot parse is answered with
//! its default, `pattern:69727474`, which is also what a client expressing no
//! preference is served without any change to the negotiated parameters.
//!
//! A `none` payload is zeroes. Payload bytes carry no protocol meaning, and a
//! server must never emit residue from another request or client. Request
//! payload bytes never reach a reply under any mode.
#![forbid(unsafe_code)]

mod clock;
mod config;
mod core;
mod error;
mod fill;
mod negotiate;
mod runtime;
mod session;
mod socket_io;
mod socket_options;
mod token;

#[cfg(test)]
mod tests;

pub use config::{
    ServerConfig, DEFAULT_BURST_ALLOWANCE, DEFAULT_IDLE_TIMEOUT, DEFAULT_MAX_PACKET_LENGTH,
    DEFAULT_MAX_SESSIONS, DEFAULT_MIN_SEND_INTERVAL,
};
pub use core::{OutboundDatagram, ServerCore};
pub use error::ServerError;
pub use runtime::{Server, ServerRuntimeError};
