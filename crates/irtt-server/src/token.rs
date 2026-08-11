//! Session token generation.
//!
//! A token is the only thing standing between an off-path attacker and a usable
//! session on an unauthenticated server, so production tokens come from the
//! operating system's cryptographically secure random source. Nothing here
//! derives a token from a counter, a timestamp, a hash of the source address,
//! or any other predictable value, and there is no fallback that would do so if
//! the random source fails.

use std::fmt;

use crate::error::ServerError;

/// How many values token allocation may draw before giving up.
///
/// Allocation rejects a drawn value that is zero (reserved for no-test replies)
/// or that collides with a live session, and draws again. With a working random
/// source neither outcome is realistically reachable even once; the bound
/// exists so a degenerate source cannot spin the allocator forever.
pub(crate) const TOKEN_ATTEMPTS: u32 = 8;

/// A source of candidate session tokens.
///
/// Deliberately private: it exists so tests can drive the zero, collision and
/// failure paths deterministically, not as a public extension point. The
/// production implementation is [`OsTokenSource`].
pub(crate) trait TokenSource: fmt::Debug + Send {
    /// Draws the next candidate token. The value is *not* required to be
    /// non-zero or unique; that is the allocator's job.
    fn next_token(&mut self) -> Result<u64, ServerError>;
}

/// Draws tokens from the operating system's random source.
#[derive(Debug, Default)]
pub(crate) struct OsTokenSource;

impl TokenSource for OsTokenSource {
    fn next_token(&mut self) -> Result<u64, ServerError> {
        getrandom::u64().map_err(|source| ServerError::RandomSource {
            reason: source.to_string(),
        })
    }
}
