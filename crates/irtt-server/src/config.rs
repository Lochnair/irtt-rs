use std::time::Duration;

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

/// Default floor on the send interval a session may negotiate, and the cadence
/// its reply allowance replenishes at.
///
/// This matches the reference server's default policy. It is policy on both
/// sides: the server specification lists the minimum send interval among the
/// reference server's freely changeable defaults, not among the values the wire
/// format fixes.
pub const DEFAULT_MIN_SEND_INTERVAL: Duration = Duration::from_millis(10);

/// Default number of echo requests a session may have answered before its
/// allowance has to replenish.
///
/// This matches the reference server's default policy, and like it, the
/// allowance is per session: one session's burst is not another's.
pub const DEFAULT_BURST_ALLOWANCE: u32 = 5;

/// Default idle lifetime of a session.
///
/// A session that goes this long without an echo request is released. This
/// matches the reference server's default of one minute, but the deadline it
/// measures is deliberately not upstream's: `irtt-rs` expires immediately at the
/// deadline, applies it to sessions that have never carried an echo request, and
/// emits no final reply. See [`ServerConfig::with_idle_timeout`].
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// How many timestamps a server is willing to provide.
///
/// This is **server policy**, deliberately a separate type from the wire
/// [`StampAt`](irtt_proto::StampAt) a session requests and negotiates: one
/// describes what a client asked for, the other what this server will hand out.
/// Negotiation reduces the requested `StampAt` accordingly, and only it — the
/// requested [`Clock`](irtt_proto::Clock) is never rewritten, because the clean
/// evidence records a timestamp allowance restricting timestamp *placement*, not
/// the clock domains it is read from.
///
/// See [`ServerConfig::with_timestamp_allowance`] for the mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimestampAllowance {
    /// No timestamps at all: every request is negotiated to
    /// [`StampAt::None`](irtt_proto::StampAt::None).
    None,
    /// At most one timestamp *instant*. A request for both receive and send
    /// becomes [`StampAt::Midpoint`](irtt_proto::StampAt::Midpoint); every
    /// placement that already names a single instant is left alone.
    ///
    /// This restricts placement, not field count: the reply still carries one
    /// field per negotiated clock domain, so a session on
    /// [`Clock::Both`](irtt_proto::Clock::Both) receives both a wall and a
    /// monotonic midpoint field.
    Single,
    /// Every requested placement is honored. This is the default, and the
    /// behavior of a server that configures nothing.
    #[default]
    Dual,
}

/// Configuration for a [`ServerCore`](crate::ServerCore).
///
/// Fields are private and set through consuming builder methods, so later
/// slices can add configuration without breaking construction.
///
/// **Server fill is deliberately not configurable.** The policy is fixed and
/// safe: every valid descriptor is honored, one that cannot be parsed falls back
/// to `pattern:69727474`, and a client expressing no preference is served that
/// same default. There is no allow-list to configure, because refusing a valid
/// mode would prevent nothing — the payload carries no protocol meaning, a
/// descriptor is bounded to 32 wire bytes, a random fill is bounded by
/// [`max_packet_length`](Self::max_packet_length) like every other reply, and
/// `none` is zero-filled here rather than left as residue. An operator
/// restriction can be added if a real use case turns up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    hmac_key: Option<Vec<u8>>,
    max_sessions: usize,
    max_packet_length: usize,
    min_send_interval: Duration,
    burst_allowance: u32,
    idle_timeout: Duration,
    max_test_duration: Option<Duration>,
    timestamp_allowance: TimestampAllowance,
    dscp_allowed: bool,
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

    /// Sets the floor on the send interval a session may negotiate, which is
    /// also the cadence its reply allowance replenishes at.
    ///
    /// A requested interval below this is raised to it during negotiation, and
    /// an absent Interval — which means the wire default of zero, not an
    /// explicit zero, which is refused — is answered with it. The negotiated
    /// value may still end up *below* this floor, because the idle-timeout cap
    /// of [`with_idle_timeout`](Self::with_idle_timeout) is applied afterwards;
    /// where that happens, the session's allowance replenishes at the shorter
    /// negotiated interval instead, so this server never enforces a cadence
    /// slower than the one it handed the client.
    ///
    /// [`Duration::ZERO`] disables both: no interval is raised, and an
    /// otherwise admissible echo never waits for allowance. It does **not**
    /// disable the burst allowance, which refuses everything when it is zero.
    #[must_use]
    pub fn with_min_send_interval(mut self, min_send_interval: Duration) -> Self {
        self.min_send_interval = min_send_interval;
        self
    }

    /// Sets how many echo requests a session may have answered before its
    /// allowance has to replenish.
    ///
    /// The allowance is per session, starts full, is spent one unit per served
    /// echo and refills at one unit per
    /// [`min_send_interval`](Self::min_send_interval), never above this value.
    /// An echo arriving with no allowance is dropped without a reply and
    /// without advancing any reception statistic.
    ///
    /// Zero is accepted and means no allowance at all: every echo is
    /// rate-limited, whatever the interval is set to. It is deliberately not a
    /// synonym for unlimited.
    #[must_use]
    pub fn with_burst_allowance(mut self, burst_allowance: u32) -> Self {
        self.burst_allowance = burst_allowance;
        self
    }

    /// Sets how long a session may go without a served or rate-limited echo
    /// request before it is released.
    ///
    /// The deadline runs from the open, not from the first echo, so a session
    /// that never carries one still ages out. Release is silent — the protocol
    /// defines no expiry notification — and immediate at the deadline.
    ///
    /// This additionally caps the negotiated send interval at a quarter of the
    /// timeout, so a client sending at the interval it was given cannot idle
    /// itself out.
    ///
    /// [`Duration::ZERO`] is accepted and is not a synonym for "never expire":
    /// it releases a session the next time expiry is evaluated. There is
    /// deliberately no unbounded setting.
    #[must_use]
    pub fn with_idle_timeout(mut self, idle_timeout: Duration) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }

    /// Sets the maximum test duration a session may negotiate.
    ///
    /// A requested Duration above this is reduced to it, and a *continuous*
    /// request — an absent Duration, meaning the wire default of zero — is
    /// answered with it too, since restricting continuous mode to a finite test
    /// is the whole point of configuring a maximum. An explicit zero Duration
    /// is refused before negotiation and never reaches this.
    ///
    /// [`Duration::ZERO`] means *no maximum*, exactly as
    /// [`without_max_test_duration`](Self::without_max_test_duration) does. A
    /// finite maximum of zero could only be honored by negotiating a Duration of
    /// zero, which on the wire means continuous — the opposite of a limit — so
    /// rewriting a client's finite request into a continuous one is refused
    /// rather than encoded.
    #[must_use]
    pub fn with_max_test_duration(mut self, max_test_duration: Duration) -> Self {
        self.max_test_duration = (!max_test_duration.is_zero()).then_some(max_test_duration);
        self
    }

    /// Removes any configured maximum test duration, which is the default.
    ///
    /// A session then negotiates the duration it asked for, including
    /// continuous, and no server-initiated close is ever triggered by duration.
    /// The session table bound and the idle timeout still apply, so this is not
    /// an unbounded setting.
    #[must_use]
    pub fn without_max_test_duration(mut self) -> Self {
        self.max_test_duration = None;
        self
    }

    /// Sets how many timestamps this server is willing to provide.
    ///
    /// A requested placement above the allowance is reduced during negotiation,
    /// so the open reply is honest about what the session's echo replies will
    /// carry:
    ///
    /// | Requested | [`Dual`] | [`Single`] | [`None`] |
    /// |-----------|----------|------------|----------|
    /// | `None` | `None` | `None` | `None` |
    /// | `Send` | `Send` | `Send` | `None` |
    /// | `Receive` | `Receive` | `Receive` | `None` |
    /// | `Both` | `Both` | **`Midpoint`** | `None` |
    /// | `Midpoint` | `Midpoint` | `Midpoint` | `None` |
    ///
    /// The one substitution is the observed one: under a single-timestamp
    /// allowance a request for both instants is answered with the *midpoint*,
    /// not with whichever of the two the server felt like keeping.
    ///
    /// The requested [`Clock`](irtt_proto::Clock) is left exactly as it was in
    /// every case. The allowance restricts which *instants* are reported, and
    /// each reported instant still carries one field per negotiated clock domain
    /// — [`Single`] on [`Clock::Both`](irtt_proto::Clock::Both) reports a wall
    /// and a monotonic midpoint. Only [`None`] removes timestamp fields
    /// outright, and it does so because the echo layout carries none once the
    /// placement is [`StampAt::None`](irtt_proto::StampAt::None).
    ///
    /// Restriction runs *before* the effective-session check, which is what
    /// makes a [`None`] allowance accept a request that selected timestamps
    /// without sending a Clock: such a session is refused as requested, because
    /// timestamps from no clock cannot be produced, but under this policy its
    /// effective placement is none and the absent clock no longer matters.
    ///
    /// [`Dual`]: TimestampAllowance::Dual
    /// [`Single`]: TimestampAllowance::Single
    /// [`None`]: TimestampAllowance::None
    #[must_use]
    pub fn with_timestamp_allowance(mut self, timestamp_allowance: TimestampAllowance) -> Self {
        self.timestamp_allowance = timestamp_allowance;
        self
    }

    /// Sets whether this server provides the traffic-class marking a session
    /// requests.
    ///
    /// `true`, the default, negotiates the requested DSCP parameter unchanged —
    /// including a raw value outside `0..=255`, which stays in the negotiated
    /// parameters and is transported unmarked.
    ///
    /// `false` negotiates it to **zero**, whatever was requested, so the reply
    /// tells the client its replies will be unmarked and the session's stored
    /// parameters say the same. The open is never refused over DSCP, and nothing
    /// is clamped or wrapped: this is an operator policy about providing
    /// traffic-class marking at all, not a judgement about a particular value.
    ///
    /// It is also not socket capability detection. What a given host can apply
    /// to a socket is a transport concern, settled per send by the runtime.
    #[must_use]
    pub fn with_dscp_allowed(mut self, dscp_allowed: bool) -> Self {
        self.dscp_allowed = dscp_allowed;
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

    /// The floor on a negotiated send interval, and the allowance refill
    /// cadence.
    #[must_use]
    pub fn min_send_interval(&self) -> Duration {
        self.min_send_interval
    }

    /// How many echo requests a session may have answered before its allowance
    /// has to replenish.
    #[must_use]
    pub fn burst_allowance(&self) -> u32 {
        self.burst_allowance
    }

    /// How long a session may go without an echo request before it is released.
    #[must_use]
    pub fn idle_timeout(&self) -> Duration {
        self.idle_timeout
    }

    /// The maximum test duration a session may negotiate, if one is configured.
    ///
    /// Never `Some(Duration::ZERO)`: a zero maximum is stored as `None`, since
    /// a negotiated Duration of zero means continuous rather than instant.
    #[must_use]
    pub fn max_test_duration(&self) -> Option<Duration> {
        self.max_test_duration
    }

    /// How many timestamps this server is willing to provide.
    #[must_use]
    pub fn timestamp_allowance(&self) -> TimestampAllowance {
        self.timestamp_allowance
    }

    /// Whether a requested DSCP value is negotiated as asked, rather than to
    /// zero.
    #[must_use]
    pub fn dscp_allowed(&self) -> bool {
        self.dscp_allowed
    }
}

impl Default for ServerConfig {
    /// An unauthenticated server bounded to [`DEFAULT_MAX_SESSIONS`] sessions,
    /// each negotiating at most [`DEFAULT_MAX_PACKET_LENGTH`] bytes per echo,
    /// no faster than [`DEFAULT_MIN_SEND_INTERVAL`] with a burst of
    /// [`DEFAULT_BURST_ALLOWANCE`], and released after
    /// [`DEFAULT_IDLE_TIMEOUT`] without an echo request.
    ///
    /// No maximum test duration is configured, matching the reference server's
    /// ordinary policy. A session is still bounded, by the table size and the
    /// idle timeout.
    ///
    /// Both capability restrictions are off: every requested timestamp placement
    /// is honored ([`TimestampAllowance::Dual`]) and a requested DSCP value is
    /// negotiated as asked. They are opt-in, so an existing configuration
    /// negotiates exactly what it did before they existed.
    fn default() -> Self {
        Self {
            hmac_key: None,
            max_sessions: DEFAULT_MAX_SESSIONS,
            max_packet_length: DEFAULT_MAX_PACKET_LENGTH,
            min_send_interval: DEFAULT_MIN_SEND_INTERVAL,
            burst_allowance: DEFAULT_BURST_ALLOWANCE,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            max_test_duration: None,
            timestamp_allowance: TimestampAllowance::Dual,
            dscp_allowed: true,
        }
    }
}
