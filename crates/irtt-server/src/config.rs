/// Default bound on the number of simultaneously live sessions.
///
/// This is `irtt-rs` resource policy, not an interoperability requirement. The
/// protocol has no rejection response, so a server refuses by staying silent
/// and the client sees an open timeout it already has to tolerate. Upstream was
/// observed to impose no session bound at all; the server specification
/// explicitly records that as an upstream policy choice which a compatible
/// server is not expected to reproduce.
pub const DEFAULT_MAX_SESSIONS: usize = 1024;

/// Configuration for a [`ServerCore`](crate::ServerCore).
///
/// Fields are private and set through consuming builder methods, so later
/// slices can add configuration without breaking construction. Only the
/// settings this OPEN/session slice actually uses exist yet; parameter
/// restriction knobs, lifetime policy and rate limits arrive with the slices
/// that implement them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    hmac_key: Option<Vec<u8>>,
    max_sessions: usize,
}

impl ServerConfig {
    /// Sets the HMAC key, enabling authentication for this server.
    ///
    /// Authentication is global: with a key configured, every request must
    /// carry `FLAG_HMAC` and a valid MAC, and every reply is authenticated.
    /// Without one, any request carrying `FLAG_HMAC` is dropped.
    #[must_use]
    pub fn with_hmac_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.hmac_key = Some(key.into());
        self
    }

    /// Sets the maximum number of simultaneously live sessions.
    ///
    /// Once the table holds this many sessions, a session-creating open is
    /// silently dropped; nothing is evicted. Zero refuses every
    /// session-creating open while still answering no-test opens, which create
    /// no state. There is deliberately no unbounded setting.
    #[must_use]
    pub fn with_max_sessions(mut self, max_sessions: usize) -> Self {
        self.max_sessions = max_sessions;
        self
    }

    /// The configured HMAC key, if this server authenticates.
    #[must_use]
    pub fn hmac_key(&self) -> Option<&[u8]> {
        self.hmac_key.as_deref()
    }

    /// The maximum number of simultaneously live sessions.
    #[must_use]
    pub fn max_sessions(&self) -> usize {
        self.max_sessions
    }
}

impl Default for ServerConfig {
    /// An unauthenticated server bounded to [`DEFAULT_MAX_SESSIONS`] sessions.
    fn default() -> Self {
        Self {
            hmac_key: None,
            max_sessions: DEFAULT_MAX_SESSIONS,
        }
    }
}
