/// Default bound on the number of simultaneously live sessions.
///
/// This is `irtt-rs` resource policy, not an interoperability requirement. The
/// protocol has no rejection response, so a server refuses by staying silent
/// and the client sees an open timeout it already has to tolerate. Upstream was
/// observed to impose no session bound at all; the server specification
/// explicitly records that as an upstream policy choice which a compatible
/// server is not expected to reproduce.
pub const DEFAULT_MAX_SESSIONS: usize = 1024;

/// Default bound on the executable echo datagram size of one session, in bytes.
///
/// This is `irtt-rs` resource policy, not an interoperability requirement and
/// not a protocol constant. The server specification records upstream defaulting
/// to *unlimited*, which leaves a remotely negotiated allocation unbounded; that
/// is explicitly not a compatibility target.
///
/// 65,507 is the largest payload an IPv4 UDP datagram can carry, so it is the
/// conservative ceiling above which no negotiated length could be emitted as one
/// datagram on any address family. It bounds a session's echo buffer to roughly
/// 64 KiB, and it is already the maximum a normal `irtt-rs` client will ask for,
/// so ordinary traffic never meets it.
///
/// It is **not** an MTU and promises nothing about what a path will carry. Path
/// and interface policy is a separate, later concern; this value exists to keep
/// allocation bounded, not to predict fragmentation.
pub const DEFAULT_MAX_PACKET_LENGTH: usize = 65_507;

/// Configuration for a [`ServerCore`](crate::ServerCore).
///
/// Fields are private and set through consuming builder methods, so later
/// slices can add configuration without breaking construction. Only the
/// settings this OPEN/session slice actually uses exist yet; the remaining
/// parameter restriction knobs, lifetime policy and rate limits arrive with the
/// slices that implement them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    hmac_key: Option<Vec<u8>>,
    max_sessions: usize,
    max_packet_length: usize,
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

    /// Sets the maximum echo datagram size a session may negotiate, in bytes.
    ///
    /// This governs the *executable* echo datagram of a session, not the size of
    /// the open exchange: a requested positive Length above this value is
    /// reduced to it during negotiation, and an open whose mandatory echo field
    /// block would not fit is silently refused, since the server could not emit
    /// a compliant reply for it. Authentication counts toward that field block.
    /// The same limit additionally admits inbound echo request datagrams by
    /// their received length, before any receive state moves.
    ///
    /// The value is trusted local configuration, like
    /// [`with_max_sessions`](Self::with_max_sessions): an operator may
    /// deliberately raise it far past what any path carries, and that is their
    /// resource choice to make. What must stay bounded is the *default*, which
    /// is why there is deliberately no unlimited setting.
    ///
    /// Zero is accepted and is not a synonym for unlimited. No session can
    /// describe an echo packet smaller than its mandatory field block, so a zero
    /// maximum silently refuses every session-creating open — and every no-test
    /// open too, since both are validated against the same effective session.
    #[must_use]
    pub fn with_max_packet_length(mut self, max_packet_length: usize) -> Self {
        self.max_packet_length = max_packet_length;
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

    /// The maximum echo datagram size a session may negotiate, in bytes.
    #[must_use]
    pub fn max_packet_length(&self) -> usize {
        self.max_packet_length
    }
}

impl Default for ServerConfig {
    /// An unauthenticated server bounded to [`DEFAULT_MAX_SESSIONS`] sessions,
    /// each negotiating at most [`DEFAULT_MAX_PACKET_LENGTH`] bytes per echo.
    fn default() -> Self {
        Self {
            hmac_key: None,
            max_sessions: DEFAULT_MAX_SESSIONS,
            max_packet_length: DEFAULT_MAX_PACKET_LENGTH,
        }
    }
}
