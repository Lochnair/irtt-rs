use std::{net::SocketAddr, time::Duration};

use irtt_proto::Params;

use crate::{clock::saturating_ns, config::ServerConfig};

/// How long past its negotiated maximum test duration a session is served
/// before the next echo it answers carries the Close flag.
///
/// This is `irtt-rs` policy, not a protocol constant, chosen to match the
/// additive two-second margin the clean evidence measured of the reference
/// server at maxima of 500 ms, 1 s and 3 s. It is deliberately not
/// configurable: a conforming client stops at the negotiated duration and never
/// reaches the deadline at all, so the margin exists only to be forgiving of
/// clients that do not, and an operator has the maximum itself to tune.
const MAX_DURATION_CLOSE_GRACE: Duration = Duration::from_secs(2);

/// A live session created by an accepted open request.
///
/// A session records what identifies it, what was negotiated for it, what its
/// accepted echo requests have added up to, how much reply allowance it has
/// left, and how long it has to live.
///
/// The three mutable groups are kept apart because they move on different
/// events: [`ReceiveState`] advances only for an echo that is answered,
/// [`RateState`] for every echo that is judged, and [`LifetimeState`] for both —
/// a rate-limited echo refreshes the idle deadline while advancing no statistic.
///
/// Deliberately crate-private. The remaining slices may still add and
/// restructure fields here, and nothing outside the crate needs a session;
/// publishing internal session state would buy nothing and commit us to a shape
/// we may yet change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Session {
    peer: SocketAddr,
    params: Params,
    receive: ReceiveState,
    rate: RateState,
    lifetime: LifetimeState,
}

impl Session {
    /// Creates a session for parameters that have already been negotiated and
    /// found executable, as of monotonic instant `now_ns`.
    ///
    /// Rate policy is derived from the configuration and the *negotiated*
    /// parameters once, here, rather than recomputed per echo — see
    /// [`RateState::new`] for why the two together decide it. The idle deadline
    /// starts running immediately: a session that never carries an echo request
    /// ages out like any other.
    pub(crate) fn new(
        peer: SocketAddr,
        params: Params,
        config: &ServerConfig,
        now_ns: i64,
    ) -> Self {
        Self {
            rate: RateState::new(config, &params, now_ns),
            lifetime: LifetimeState::opened_at(now_ns),
            peer,
            params,
            receive: ReceiveState::default(),
        }
    }

    /// The exact source endpoint the open request arrived from.
    ///
    /// A session is bound to this endpoint, not merely to its address: the
    /// address family, the address and the UDP source port all form part of its
    /// identity, as — by `irtt-rs` policy rather than observed behavior — does
    /// the IPv6 scope identifier. Close and echo processing both require a
    /// token match *and* an endpoint match before touching the session;
    /// `same_endpoint` in `core` is where the comparison and its reasoning
    /// live.
    ///
    /// The address is stored exactly as received, including the flow label that
    /// identity ignores, so a reply goes back to the endpoint the request
    /// actually came from.
    pub(crate) fn peer(&self) -> SocketAddr {
        self.peer
    }

    /// The negotiated parameters the server returned for this session and
    /// enforces for it.
    ///
    /// Echo replies are laid out and sized from these, never from the request
    /// that provoked them.
    pub(crate) fn params(&self) -> &Params {
        &self.params
    }

    /// What this session's accepted echo requests have added up to so far.
    pub(crate) fn receive_state(&self) -> ReceiveState {
        self.receive
    }

    /// Adopts the state computed for an accepted echo request.
    ///
    /// Kept separate from [`ReceiveState::accepted`] on purpose. The transition
    /// is pure and this is the irreversible step, so echo handling runs every
    /// fallible part — building and encoding the reply — before calling it, and
    /// a reply that fails to encode leaves the session exactly as it was.
    pub(crate) fn commit_receive_state(&mut self, next: ReceiveState) {
        self.receive = next;
    }

    /// This session's reply allowance as it stands.
    pub(crate) fn rate_state(&self) -> RateState {
        self.rate
    }

    /// Adopts the allowance computed for a judged echo request.
    ///
    /// Unlike the receive state, this is committed for a *rate-limited* request
    /// too: declining to answer is itself bookkeeping the limiter has to keep,
    /// or the replenishment it just accounted for would be recomputed from a
    /// stale instant on the next request.
    pub(crate) fn commit_rate_state(&mut self, next: RateState) {
        self.rate = next;
    }

    /// How long this session has to live, and when its test duration started.
    pub(crate) fn lifetime_state(&self) -> LifetimeState {
        self.lifetime
    }

    /// Adopts the lifetime state computed for a request.
    pub(crate) fn commit_lifetime_state(&mut self, next: LifetimeState) {
        self.lifetime = next;
    }

    /// Whether this session has gone `idle_timeout_ns` without an echo request.
    pub(crate) fn is_idle_expired(&self, now_ns: i64, idle_timeout_ns: i64) -> bool {
        self.lifetime.is_idle_expired(now_ns, idle_timeout_ns)
    }
}

/// What a session's accepted echo requests have added up to.
///
/// The semantics are those of the clean server specification, Section 10, and
/// the input → output vectors in `test-vectors/SERVER_BEHAVIORAL_VECTORS.md`
/// Section 1. Both values are per session, start at zero, and move only for a
/// request that reaches the reply stage: a datagram dropped for its size, its
/// authentication, an unknown token or a foreign endpoint leaves them alone.
///
/// The state is maintained whatever the session negotiated. Which of the two
/// values actually appears in a reply is a separate question, answered by the
/// negotiated `ReceivedStats` when the reply is built.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct ReceiveState {
    /// Accepted echo requests, including the one currently being answered.
    pub(crate) count: u32,
    /// The received window: bit 0 is the request being answered and bit *k* is
    /// the request with sequence number (current − *k*), for 1 ≤ *k* ≤ 63.
    pub(crate) window: u64,
    /// The sequence number the previous accepted request carried, which the
    /// next transition is measured against. `None` before the session's first.
    last_sequence: Option<u32>,
}

impl ReceiveState {
    /// The state after accepting a request carrying `sequence`.
    ///
    /// Pure, so the caller can compute the next state, build and encode a reply
    /// from it, and only then commit it.
    ///
    /// The window transition is the observed one, and it is **not** a
    /// selective-acknowledgement bitmap: history is shifted along by the
    /// unsigned distance from the previous accepted sequence number, and a
    /// distance it cannot represent throws all of it away. The consequence is
    /// the distinctive part — because the distance is unsigned, a *late or
    /// reordered* request produces a very large one, so it resets the window to
    /// `0x1` instead of setting an old bit, and the history it discards is
    /// never recovered. That late sequence number then becomes the reference
    /// for the next transition like any other.
    pub(crate) fn accepted(self, sequence: u32) -> Self {
        let window = match self.last_sequence {
            // The first accepted request of a session reports only itself,
            // whatever its sequence number.
            None => 0x1,
            Some(last) => match sequence.wrapping_sub(last) {
                // A duplicate of the most recent sequence number leaves the
                // window unchanged, since bit 0 is already set for it.
                0 => self.window | 1,
                // In order, or a gap the window can still represent: shift the
                // history along by the distance and record this request.
                distance @ 1..=63 => (self.window << distance) | 1,
                // A gap of 64 or more discards every earlier bit — and so does
                // any late or reordered request, which lands here because the
                // unsigned distance wrapped.
                _ => 0x1,
            },
        };
        Self {
            // The wire field is 32 bits. No clean evidence reaches 2^32
            // requests in one session, so the wrap is not verified behavior: it
            // is simply what maintaining a fixed-width counter means, and it is
            // chosen over a debug-build overflow panic a peer could drive.
            count: self.count.wrapping_add(1),
            window,
            last_sequence: Some(sequence),
        }
    }
}

/// One session's reply allowance, and the policy that replenishes it.
///
/// The externally visible behavior is what the clean specification's Section 9.3
/// records: a session starts with its full burst allowance, spends one unit per
/// echo it is answered, replenishes at one unit per refill interval and never
/// exceeds the burst, and an echo arriving with no allowance gets no reply and
/// advances no reception statistic. The specification is explicit that a
/// token bucket is a *black-box inference* about the reference server, so this
/// representation is an implementation choice, not a protocol requirement — any
/// structure producing those outcomes would do.
///
/// The policy is copied in per session rather than read from the configuration
/// per echo, because the refill interval depends on what that session
/// negotiated. Sibling sessions therefore limit independently, as they do
/// upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RateState {
    /// The most allowance this session may hold.
    burst: u32,
    /// How long one unit of allowance takes to accrue. Zero means no
    /// time-based throttling at all.
    interval_ns: i64,
    /// Allowance available now, never above `burst`.
    allowance: u32,
    /// The monotonic instant `allowance` was last accounted at.
    refilled_at_ns: i64,
}

impl RateState {
    /// The full allowance a new session starts with, and the policy it will
    /// replenish under.
    ///
    /// **The refill interval is the shorter of the configured minimum send
    /// interval and the interval this session actually negotiated.** Normally
    /// they are the configured minimum: a 10 ms minimum against a 1 s
    /// negotiated interval refills every 10 ms, which is the ordinary reference
    /// behavior. They differ only where negotiation's idle cap pulled the
    /// returned interval *below* the configured minimum — a 5 s minimum against
    /// an 8 s idle timeout returns 2 s — and there the negotiated value wins.
    ///
    /// That is a deliberate `irtt-rs` divergence. The clean specification
    /// records the reference server replenishing on the configured minimum
    /// regardless, so a fully conforming client sending at the 2 s interval it
    /// was handed is rate-limited every time by a 5 s allowance. A server must
    /// not enforce a cadence it did not advertise, so this one refills at
    /// whichever cadence it told the client it would accept.
    ///
    /// Zero on either side is not throttling. A zero configured minimum
    /// disables the time-based floor outright, and a session with no negotiated
    /// interval — an absent Interval, which only survives negotiation when the
    /// floor is zero as well — has no advertised cadence to honor, so the
    /// configured minimum stands alone. A zero refill interval means every
    /// otherwise admissible echo is allowed; a zero *burst* still means none is.
    fn new(config: &ServerConfig, params: &Params, now_ns: i64) -> Self {
        let configured = saturating_ns(config.min_send_interval());
        let interval_ns = if configured <= 0 || params.interval_ns <= 0 {
            configured
        } else {
            configured.min(params.interval_ns)
        };
        Self {
            burst: config.burst_allowance(),
            interval_ns,
            allowance: config.burst_allowance(),
            refilled_at_ns: now_ns,
        }
    }

    /// Judges one echo request arriving at monotonic instant `now_ns`.
    ///
    /// Pure, like [`ReceiveState::accepted`]: it returns the state the caller
    /// should commit rather than mutating, so an allowed request can have its
    /// reply built and encoded before any of the session moves. Both outcomes
    /// carry a next state, because replenishment is accounted for whether or not
    /// the request is served.
    pub(crate) fn charge(mut self, now_ns: i64) -> RateDecision {
        if self.burst == 0 {
            // No allowance exists to replenish into, so nothing accrues and
            // nothing is served. This is not a synonym for "unlimited".
            return RateDecision::Limited { next: self };
        }
        if self.interval_ns == 0 {
            // No time-based throttling: allowance never has to be spent,
            // because it never has to be waited for.
            return RateDecision::Allowed { next: self };
        }
        self.replenish(now_ns);
        if self.allowance == 0 {
            return RateDecision::Limited { next: self };
        }
        self.allowance -= 1;
        RateDecision::Allowed { next: self }
    }

    /// Credits whatever whole intervals have elapsed, up to the burst.
    fn replenish(&mut self, now_ns: i64) {
        if self.allowance >= self.burst {
            // Already full. Idle time is not bankable, so the next unit is
            // measured from now rather than from a stretch the session spent
            // sending nothing.
            self.allowance = self.burst;
            self.refilled_at_ns = now_ns;
            return;
        }
        // Saturating, so a monotonic reading that somehow ran backwards yields a
        // negative elapsed time and credits nothing rather than panicking.
        let elapsed = now_ns.saturating_sub(self.refilled_at_ns);
        if elapsed < self.interval_ns {
            return;
        }
        let gained = elapsed / self.interval_ns;
        let allowance = self
            .allowance
            .saturating_add(u32::try_from(gained).unwrap_or(u32::MAX))
            .min(self.burst);
        self.refilled_at_ns = if allowance == self.burst {
            now_ns
        } else {
            // Carry the unspent remainder of the interval, so a session sending
            // at exactly its cadence does not lose a fraction of an interval on
            // every request and drift into being limited.
            self.refilled_at_ns
                .saturating_add(gained.saturating_mul(self.interval_ns))
        };
        self.allowance = allowance;
    }
}

/// What the rate limiter decided about one echo request.
///
/// Both variants carry the state to commit; they differ in whether a reply is
/// owed. `Limited` is a silent drop — the protocol has no rejection response —
/// and it must leave the reception statistics exactly where they were.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RateDecision {
    Allowed { next: RateState },
    Limited { next: RateState },
}

/// How long a session has left, and when its test duration started running.
///
/// Both instants are monotonic. Lifetime decisions never consult the wall
/// clock: a session must not outlive or predecease its deadline because an
/// administrator or NTP stepped the host clock, and the wall readings a reply
/// carries are a separate concern with the opposite requirement — that they be
/// reported honestly, corrections and all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LifetimeState {
    /// When this session last did something that keeps it alive: it was opened,
    /// it had an echo answered, or it had one rate-limited.
    last_activity_ns: i64,
    /// When this session's first echo request was actually *served*, if one has
    /// been. The maximum test duration is measured from here.
    first_served_echo_ns: Option<i64>,
}

impl LifetimeState {
    /// The state of a session at the moment it is created.
    ///
    /// The idle deadline runs from the open, which is a deliberate `irtt-rs`
    /// divergence: the clean evidence records the reference server starting it
    /// at the first echo request instead, so a session that never carries one
    /// never expires. A session is created by a single unauthenticated
    /// datagram, so that is an unbounded-state hazard rather than an
    /// interoperability feature, and the specification recommends against
    /// reproducing it.
    fn opened_at(now_ns: i64) -> Self {
        Self {
            last_activity_ns: now_ns,
            first_served_echo_ns: None,
        }
    }

    /// Whether this session has gone `idle_timeout_ns` without an echo request.
    ///
    /// The deadline is exact and the comparison inclusive: at
    /// `last_activity + idle_timeout` the session is already gone. `irtt-rs`
    /// deliberately does not reproduce the reference server's additional
    /// five-second grace, nor its lazy final reply to the first echo that finds
    /// an expired session. The specification records both as observed upstream
    /// policy and states explicitly that the one interoperability constraint is
    /// negative — a server must never *signal* expiry — which silence satisfies.
    ///
    /// A zero timeout expires a session at the first evaluation. It is not a
    /// synonym for "never expire".
    fn is_idle_expired(&self, now_ns: i64, idle_timeout_ns: i64) -> bool {
        now_ns.saturating_sub(self.last_activity_ns) >= idle_timeout_ns
    }

    /// Whether an echo arriving now is the one that should carry Close.
    ///
    /// The deadline is `first served echo + maximum + `
    /// [`MAX_DURATION_CLOSE_GRACE`], so it does not exist at all until an echo
    /// has actually been served: neither the open, nor an echo that was
    /// rejected, nor one that was rate-limited starts it. That origin is
    /// externally measured behavior, not an inference.
    pub(crate) fn max_duration_reached(&self, now_ns: i64, max_duration_ns: i64) -> bool {
        self.first_served_echo_ns.is_some_and(|first| {
            let deadline = first
                .saturating_add(max_duration_ns)
                .saturating_add(saturating_ns(MAX_DURATION_CLOSE_GRACE));
            now_ns >= deadline
        })
    }

    /// The state after an echo that kept the session alive without being
    /// served — which is to say, one that was rate-limited.
    ///
    /// This is the one drop class the clean evidence records as refreshing the
    /// idle deadline, and it was established against every other tested class,
    /// which does not. It does **not** move the maximum-duration origin: a
    /// request that was never answered cannot start the test.
    pub(crate) fn refreshed(mut self, now_ns: i64) -> Self {
        self.last_activity_ns = now_ns;
        self
    }

    /// The state after an echo that is about to be answered.
    ///
    /// Sets the maximum-duration origin if this is the session's first served
    /// echo, and refreshes the idle deadline either way.
    pub(crate) fn served(mut self, now_ns: i64) -> Self {
        self.last_activity_ns = now_ns;
        self.first_served_echo_ns.get_or_insert(now_ns);
        self
    }
}
