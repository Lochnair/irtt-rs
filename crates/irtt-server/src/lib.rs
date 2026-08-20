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
//! scheduled expiry maintenance and caller-controlled shutdown. [`ServerSet`]
//! owns one or more `Server`s as a single service: it binds them all or none,
//! runs each in its own task, fans one shutdown out to them and fails the group
//! if any listener does. The crate is intentionally Tokio-native; there is no
//! blocking or alternate-runtime counterpart and no transport abstraction.
//!
//! # One listener or several
//!
//! `Server` is the primitive and stays directly usable. `ServerSet` is the
//! service around it, and a set of one is an ordinary set rather than a special
//! case — the server CLI runs every invocation, single-bind included, through
//! one.
//!
//! Listeners in a set share nothing but the configuration they were cloned
//! from. Each has its own socket, core, session table and tokens, so a token
//! issued by one is unknown at the others, and every bound in
//! [`ServerConfig`] — `max_sessions` included — applies **per listener**, not
//! per process.
//!
//! See `examples/` in the repository for a runnable single-listener and
//! multi-listener example.
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
//! server fill, the optional timestamp-allowance and DSCP capability
//! restrictions, wildcard reply-source selection, Tokio UDP orchestration and
//! multi-listener supervision through [`ServerSet`].
//!
//! # Capability restrictions
//!
//! Two optional negotiation policies restrict what a session may ask this server
//! to provide, and both are off by default, so a configuration that sets neither
//! negotiates exactly what it always did.
//!
//! [`TimestampAllowance`] reduces the requested timestamp placement: `Single`
//! provides at most one timestamp instant, answering a request for both instants
//! with the midpoint, and `None` provides no timestamps at all. The requested
//! clock is never rewritten, so a single placement still reports one field per
//! negotiated clock domain. [`ServerConfig::with_dscp_allowed`] refuses to provide
//! traffic-class marking, negotiating any requested DSCP value to zero, after
//! which the session's replies leave unmarked like any other zero-DSCP session.
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
mod set;
mod socket_io;
mod socket_options;
mod token;

#[cfg(test)]
mod tests;

pub use config::{
    ServerConfig, TimestampAllowance, DEFAULT_BURST_ALLOWANCE, DEFAULT_IDLE_TIMEOUT,
    DEFAULT_MAX_PACKET_LENGTH, DEFAULT_MAX_SESSIONS, DEFAULT_MIN_SEND_INTERVAL,
};
pub use core::{OutboundDatagram, ServerCore};
pub use error::ServerError;
pub use runtime::{Server, ServerRuntimeError};
pub use set::{address_family_available, ServerSet, ServerSetError};
