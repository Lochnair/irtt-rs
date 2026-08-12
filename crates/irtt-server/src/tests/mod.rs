//! Deterministic tests for the server core.
//!
//! The core takes a source endpoint and datagram bytes and returns reply bytes,
//! so these drive it directly; no socket is bound. Requests are built with the
//! production encoders wherever the wire form is well-formed, and by hand only
//! where the point of the test is a payload a compliant encoder cannot produce.
//!
//! Tokens come from a scripted source so session identity, collisions and
//! allocation failure are all assertable.

mod close;
mod hmac;
mod no_test;
mod open;
mod params;
mod sessions;
mod support;
mod tokens;
mod unsupported;
