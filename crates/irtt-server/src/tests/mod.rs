//! Deterministic tests for the server core.
//!
//! The core takes a source endpoint and datagram bytes and returns reply bytes,
//! so these drive it directly; no socket is bound. Requests are built with the
//! production encoders wherever the wire form is well-formed, and by hand only
//! where the point of the test is a payload a compliant encoder cannot produce.
//!
//! The two nondeterministic parts are injected. Tokens come from a scripted
//! source so session identity, collisions and allocation failure are all
//! assertable; the clock an echo reply's timestamps come from is scripted the
//! same way, so timestamp behavior is pinned exactly, with no sleeps, no
//! runtime and no timing tolerances.

mod admission;
mod close;
mod echo;
mod fill;
mod hmac;
mod kernel_rx;
mod lifecycle;
mod negotiation;
mod no_test;
mod open;
mod params;
mod rate;
mod sessions;
mod support;
mod tokens;
mod traffic_class;
