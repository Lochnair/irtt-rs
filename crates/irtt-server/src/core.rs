use std::{collections::HashMap, net::SocketAddr};

use irtt_proto::{
    decode_request, echo_packet_len, encode_echo_reply, encode_open_reply, verify_packet_hmac,
    Clock, DecodedParams, DecodedRequestKind, EchoReply, OpenReply, Params, StampAt,
    TimestampFields, FLAG_CLOSE, FLAG_OPEN, FLAG_REPLY,
};

use crate::{
    clock::{saturating_ns, ClockSample, ClockSource, SystemClock},
    config::ServerConfig,
    error::ServerError,
    negotiate::negotiate_params,
    session::{RateDecision, Session},
    token::{OsTokenSource, TokenSource, TOKEN_ATTEMPTS},
};

/// The deterministic protocol and session engine of the server.
///
/// `ServerCore` owns admission, negotiation, the session table and reply
/// construction. It performs no I/O: a caller hands it a source endpoint and
/// the bytes of one received datagram, and gets back the bytes to send to that
/// endpoint, or nothing. Everything it does is a pure function of its state and
/// that input, except for drawing session tokens and sampling the clock an echo
/// reply's timestamps come from — both of which are private injected seams, so
/// the whole engine stays deterministically testable.
///
/// The Tokio runtime slice will wrap it directly:
///
/// ```text
/// recv_from -> core.handle_datagram(peer, packet) -> Some(reply) -> send_to
///                                                 -> None        -> send nothing
/// ```
///
/// It is public as a low-level engine for a pre-1.0 crate, not as a promise of
/// runtime independence. The server is intentionally Tokio-native; this is not
/// a transport abstraction, and there is no blocking or alternate-runtime
/// counterpart.
///
/// # Scope
///
/// This slice implements open handling, session creation and negotiation,
/// normal echo processing, per-session rate limiting, idle expiry,
/// maximum-duration close and client-initiated close. DSCP application and the
/// full server fill policy are separate slices, as is the Tokio runtime — so
/// expiry is evaluated when a datagram arrives rather than on a timer, and
/// echo payloads are zero-filled.
#[derive(Debug)]
pub struct ServerCore {
    config: ServerConfig,
    sessions: HashMap<u64, Session>,
    tokens: Box<dyn TokenSource>,
    clock: Box<dyn ClockSource>,
}

impl ServerCore {
    /// Creates a server core drawing session tokens from the operating system's
    /// random source and timestamps from the system clock.
    #[must_use]
    pub fn new(config: ServerConfig) -> Self {
        Self::with_token_source(config, Box::new(OsTokenSource))
    }

    /// A core with a chosen token source and the production clock, which is
    /// what tests that care about session identity but not about timestamp
    /// values want.
    pub(crate) fn with_token_source(config: ServerConfig, tokens: Box<dyn TokenSource>) -> Self {
        Self::with_sources(config, tokens, Box::new(SystemClock::new()))
    }

    /// A core with both nondeterministic sources chosen.
    pub(crate) fn with_sources(
        config: ServerConfig,
        tokens: Box<dyn TokenSource>,
        clock: Box<dyn ClockSource>,
    ) -> Self {
        Self {
            config,
            sessions: HashMap::new(),
            tokens,
            clock,
        }
    }

    /// This core's configuration.
    #[must_use]
    pub fn config(&self) -> &ServerConfig {
        &self.config
    }

    /// The number of live sessions.
    ///
    /// This is the observable a caller genuinely needs — how much session state
    /// the server is holding against its bound. The sessions themselves stay
    /// crate-private until their shape settles.
    #[must_use]
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Looks up a live session by token.
    ///
    /// This is a lookup by token alone and is therefore *not* an admission
    /// check: a request is bound to a session only when the token matches and
    /// it arrived from the session's exact endpoint. Callers pair it with an
    /// endpoint comparison, as close handling does.
    pub(crate) fn session(&self, token: u64) -> Option<&Session> {
        self.sessions.get(&token)
    }

    /// Handles one received datagram, returning the reply to send, if any.
    ///
    /// `Ok(None)` means "answer nothing". The protocol defines no error reply,
    /// no reset and no NACK, so every rejection is a silent discard and a
    /// client distinguishes "rejected" from "lost" only by timing out. A
    /// datagram is discarded when it is structurally invalid, carries the reply
    /// flag, disagrees with this server's authentication configuration, fails
    /// authentication, carries a malformed or out-of-range open parameter
    /// payload, negotiates to parameters the server could not execute, would
    /// exceed the session bound, is longer than the configured maximum packet
    /// length, or names no live session from the endpoint that opened it. None
    /// of that mutates any live session.
    ///
    /// `Ok(None)` is not only a rejection, though. A close request that does
    /// release its session is answered with nothing as well, because protocol
    /// version 1 defines no acknowledgement for one — and so is an echo that
    /// found its session's rate allowance spent, which advances no statistic
    /// but does keep the session alive.
    ///
    /// Sessions whose idle deadline has passed are released before the request
    /// is dispatched, so a request may find the session it names already gone,
    /// and one request may release an unrelated session. Expiry is never
    /// signalled; a client sees only silence.
    ///
    /// Authentication is checked before open parameters are decoded and before
    /// any session is looked up. That ordering is deliberate: a packet that
    /// fails authentication is discarded without parsing untrusted parameter
    /// data or consulting the session table, and no reply behavior may reveal
    /// which stage rejected it.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] only for an internal failure — the random source
    /// failing, no unique token being found within the retry budget, or a
    /// server-generated reply failing to encode. Rejected input is never an
    /// error.
    pub fn handle_datagram(
        &mut self,
        peer: SocketAddr,
        packet: &[u8],
    ) -> Result<Option<Vec<u8>>, ServerError> {
        let Ok(request) = decode_request(packet) else {
            return Ok(None);
        };
        // An echo's arrival instant is sampled the moment the datagram is known
        // to be one, so that everything the reply's receive timestamp should
        // bracket — authentication, the session lookup, the receive-state
        // transition and encoding — happens after it. A datagram rejected later
        // has consumed one clock sample and nothing else; sampling is not
        // protocol state, and it is a strictly better trade than looking a
        // session up before authentication to discover whether it even wanted
        // timestamps.
        let kind = self.classify(&request.kind, packet.len());

        // The HMAC flag must agree with this server's configuration in both
        // directions of mismatch, and the MAC itself is verified separately.
        if request.hmac_present != self.config.hmac_key().is_some() {
            return Ok(None);
        }
        if let Some(key) = self.config.hmac_key() {
            if verify_packet_hmac(key, packet).is_err() {
                return Ok(None);
            }
        }

        // One monotonic instant per authenticated, structurally valid request,
        // which is the request's whole lifecycle time: what expiry is judged
        // against, what a new session's deadlines start from, and what the rate
        // limiter and maximum-duration deadline read. An echo already has one —
        // the instant its reply will report as the receive time — and taking a
        // second would let a reply's timestamps disagree with the policy
        // decisions made about the very same datagram.
        let now_ns = match kind {
            ClassifiedKind::Echo(ref echo) => echo.received_at.mono_ns,
            _ => self.clock.sample().mono_ns,
        };
        self.release_expired_sessions(now_ns);

        match kind {
            ClassifiedKind::Open { no_test, params } => {
                self.handle_open(peer, no_test, params, now_ns)
            }
            ClassifiedKind::Close { token } => {
                self.handle_close(peer, token);
                Ok(None)
            }
            ClassifiedKind::Echo(echo) => self.handle_echo(peer, echo),
        }
    }

    /// Drops every session whose idle deadline has passed, as of `now_ns`.
    ///
    /// Expiry is *logical* — a session is expired the instant its deadline
    /// passes — but reclamation here is *packet-driven*, because this slice has
    /// no runtime and so no timer. An expired session may therefore stay
    /// resident until the next authenticated, structurally valid request causes
    /// this sweep. Nothing observable turns on the difference: total state is
    /// already bounded by [`max_sessions`](ServerConfig::max_sessions), the
    /// sweep runs before a new open is admitted so a stale session cannot deny
    /// capacity to a live one, and while no traffic arrives at all there is
    /// no peer to observe anything. The Tokio slice may add scheduled sweeps.
    ///
    /// The sweep is global and runs before the request is dispatched, so a
    /// request touching one session can release another. That is deliberate:
    /// expiry is a resource decision about the table, not a side effect of what
    /// the current datagram happens to name. It also means a *no-test* open —
    /// which creates nothing of its own — reclaims expired sessions like any
    /// other open, where the reference server was observed not to. Both are
    /// upstream policy the clean specification declines to require, and a
    /// client cannot tell the difference: expiry is never signalled.
    fn release_expired_sessions(&mut self, now_ns: i64) {
        let idle_timeout_ns = saturating_ns(self.config.idle_timeout());
        self.sessions
            .retain(|_, session| !session.is_idle_expired(now_ns, idle_timeout_ns));
    }

    /// Pairs a structurally decoded request with the clock sample an echo
    /// needs, leaving every other kind untouched.
    fn classify<'a>(
        &mut self,
        kind: &DecodedRequestKind<'a>,
        datagram_len: usize,
    ) -> ClassifiedKind<'a> {
        match *kind {
            DecodedRequestKind::Open { no_test, params } => {
                ClassifiedKind::Open { no_test, params }
            }
            DecodedRequestKind::Close { token } => ClassifiedKind::Close { token },
            // Everything after the sequence number is opaque, so the tail is
            // read no further than its length.
            DecodedRequestKind::Echo {
                token, sequence, ..
            } => ClassifiedKind::Echo(EchoRequest {
                token,
                sequence,
                datagram_len,
                received_at: self.clock.sample(),
            }),
        }
    }

    /// Answers an echo request that names one of this server's sessions.
    ///
    /// Admission runs before any state moves, and each step is a silent
    /// discard: the datagram must not be longer than
    /// [`max_packet_length`](ServerConfig::max_packet_length), its token must
    /// name a live session, and it must have arrived from that session's own
    /// endpoint. Authentication was already settled by the caller, as was idle
    /// expiry — a request reaching here found its session still alive. A
    /// request failing any of these leaves the receive state — of this session
    /// and of every other — exactly where it was.
    ///
    /// Only the token and the sequence number mean anything. The request's own
    /// length is judged against the configured maximum and then forgotten: it
    /// does not have to equal the negotiated length, and it never sizes the
    /// reply, which is laid out and sized from the session's negotiated
    /// parameters alone.
    ///
    /// **Rate allowance is judged after admission and before the
    /// maximum-duration deadline.** That order is the observed one and it
    /// matters: a post-deadline echo with no allowance is simply dropped, and
    /// the close it would have carried is not lost but handed to the next echo
    /// that is served. A rate-limited request refreshes the session's idle
    /// deadline — the one drop class the clean evidence records doing so — and
    /// changes nothing else.
    ///
    /// **The maximum-duration close is an ordinary reply with the Close flag
    /// added**, never a standalone packet: protocol version 1 has no other
    /// in-band mechanism. It reports the triggering request in its statistics
    /// exactly as any reply would, and the session is released once it has
    /// encoded, so every later request is dropped as an unknown token.
    ///
    /// Every state transition is computed but not committed until the reply has
    /// been encoded, so a reply this server failed to build cannot leave a
    /// session claiming to have answered a request it never did — or, for a
    /// close, leave the session released with no reply carrying the flag.
    fn handle_echo(
        &mut self,
        peer: SocketAddr,
        request: EchoRequest,
    ) -> Result<Option<Vec<u8>>, ServerError> {
        // Resource policy, applied to the length actually received. This is
        // `irtt-rs` policy rather than upstream-mechanism emulation: there is
        // no interface-MTU effective length here, just the datagram the caller
        // handed over. The comparison is strict — a request of exactly the
        // maximum is served.
        if request.datagram_len > self.config.max_packet_length() {
            return Ok(None);
        }
        let Some(session) = self.sessions.get_mut(&request.token) else {
            return Ok(None);
        };
        if !same_endpoint(session.peer(), peer) {
            return Ok(None);
        }

        let now_ns = request.received_at.mono_ns;
        let next_rate = match session.rate_state().charge(now_ns) {
            RateDecision::Limited { next } => {
                // Silence, and no statistic moves — but the session lives on,
                // and its idle deadline moves with it.
                let next_lifetime = session.lifetime_state().refreshed(now_ns);
                session.commit_rate_state(next);
                session.commit_lifetime_state(next_lifetime);
                return Ok(None);
            }
            RateDecision::Allowed { next } => next,
        };

        // Only a session that has already served an echo has a deadline at all,
        // so this can never fire on the request that starts the test.
        let closing = self.config.max_test_duration().is_some_and(|maximum| {
            session
                .lifetime_state()
                .max_duration_reached(now_ns, saturating_ns(maximum))
        });

        let next = session.receive_state().accepted(request.sequence);
        let next_lifetime = session.lifetime_state().served(now_ns);
        // The departure side, sampled as late as the core can: after admission
        // and the transition, immediately before the reply is built. There is
        // no socket here, so this is the core's departure instant rather than a
        // kernel transmission time; the runtime slice does not need to rewrite
        // it.
        let sent_at = self.clock.sample();
        let params = session.params();
        let reply = EchoReply {
            // Open must be clear or upstream clients abort. Close is set only
            // for the maximum-duration limit, which is the sole trigger this
            // server has. The authentication flag and field are the encoder's,
            // decided by the key it is given — a close-flagged reply is
            // authenticated by exactly the same path as any other.
            flags: FLAG_REPLY | if closing { FLAG_CLOSE } else { 0 },
            // Both are copied through unchanged: no token is allocated for an
            // echo, and no sequence number is normalized or range-checked.
            token: request.token,
            sequence: request.sequence,
            recv_count: params.received_stats.has_count().then_some(next.count),
            recv_window: params.received_stats.has_window().then_some(next.window),
            timestamps: timestamp_fields(params, request.received_at, sent_at),
            // No payload bytes of our own: the encoder zero-fills the whole
            // region between the field block and the negotiated length. That is
            // the initial safe fill policy — payload bytes carry no protocol
            // meaning, and zeroes cannot disclose residue from another request
            // or another client. Request payload bytes never reach a reply.
            payload: Vec::new(),
        };
        let packet = encode_echo_reply(&reply, params, self.config.hmac_key())
            .map_err(|source| ServerError::ReplyEncoding { source })?;

        // The last fallible step is behind us, so the transitions can be made.
        if closing {
            // The reply already carries the state this request produced, so
            // there is nothing worth committing into a session about to go. A
            // failure above would have left it live and untouched instead.
            self.sessions.remove(&request.token);
        } else {
            session.commit_rate_state(next_rate);
            session.commit_receive_state(next);
            session.commit_lifetime_state(next_lifetime);
        }
        Ok(Some(packet))
    }

    /// Negotiates an open request and, unless it is a no-test one, creates the
    /// session it asks for.
    ///
    /// `now_ns` is the request's lifecycle instant, which becomes the new
    /// session's idle origin: a session starts aging the moment it exists, not
    /// when it first carries an echo, so one whose client never returns is
    /// still reclaimed. Expired sessions were already swept, so the capacity
    /// bound below is judged against sessions that are genuinely live.
    fn handle_open(
        &mut self,
        peer: SocketAddr,
        no_test: bool,
        encoded_params: &[u8],
        now_ns: i64,
    ) -> Result<Option<Vec<u8>>, ServerError> {
        // The parameter decoder already rejects a truncated tag or value, a
        // varint overflow, an out-of-range stats/stamp-at/clock enum, and a
        // server-fill value that is oversized, short-buffered or not UTF-8.
        let Ok(decoded) = Params::decode_with_presence(encoded_params) else {
            return Ok(None);
        };
        if !open_params_are_admissible(&decoded) {
            return Ok(None);
        }
        let params = negotiate_params(decoded.params, &self.config);
        // Deliberately after negotiation, and before either open path: what has
        // to be executable is the effective session, not the request.
        if !negotiated_params_are_admissible(&params, &self.config) {
            return Ok(None);
        }

        if no_test {
            // No-test validates parameters without running a test: same
            // negotiation, a zero token, and no state at all. It stays
            // serviceable at capacity precisely because it creates nothing.
            let reply = OpenReply {
                flags: FLAG_OPEN | FLAG_REPLY | FLAG_CLOSE,
                token: 0,
                params,
            };
            return self.encode_reply(&reply).map(Some);
        }

        if self.sessions.len() >= self.config.max_sessions() {
            return Ok(None);
        }

        // Every fallible step runs before the table is touched, so an internal
        // failure cannot leave a half-created session behind.
        let token = self.allocate_token()?;
        let reply = OpenReply {
            flags: FLAG_OPEN | FLAG_REPLY,
            token,
            params,
        };
        let packet = self.encode_reply(&reply)?;

        // The reply is encoded before its params move into the session, so
        // nothing is cloned to keep both.
        self.sessions.insert(
            token,
            Session::new(peer, reply.params, &self.config, now_ns),
        );
        Ok(Some(packet))
    }

    /// Releases the session a close request names, if that request owns one.
    ///
    /// A close is bound to a session only when the token matches *and* the
    /// datagram arrived from that session's exact endpoint, so holding a token
    /// is not by itself authority to tear the session down: a close from a
    /// foreign endpoint leaves it live, and one close cannot take down a
    /// sibling session that the same peer opened separately. A token that is
    /// unknown, already closed, or zero — zero is reserved for no-test replies
    /// and never issued — matches nothing and leaves the table untouched.
    ///
    /// Nothing is sent either way, which is why this returns no reply to
    /// encode. Protocol version 1 defines no acknowledgement for a client
    /// close, so a delivered close is indistinguishable to the client from a
    /// lost one, and the freed capacity is immediately reusable because the
    /// session table is the only record of it.
    fn handle_close(&mut self, peer: SocketAddr, token: u64) {
        let owns_session = self
            .session(token)
            .is_some_and(|session| same_endpoint(session.peer(), peer));
        if owns_session {
            self.sessions.remove(&token);
        }
    }

    /// Encodes a reply this server constructed. Any failure here is internal.
    fn encode_reply(&self, reply: &OpenReply) -> Result<Vec<u8>, ServerError> {
        encode_open_reply(reply, self.config.hmac_key())
            .map_err(|source| ServerError::ReplyEncoding { source })
    }

    /// Draws a token that is non-zero and unique among live sessions.
    ///
    /// Zero is reserved for no-test replies and is never issued to a session. A
    /// value colliding with a live token is discarded rather than allowed to
    /// overwrite that session. Both cases redraw, up to [`TOKEN_ATTEMPTS`].
    fn allocate_token(&mut self) -> Result<u64, ServerError> {
        for _ in 0..TOKEN_ATTEMPTS {
            let token = self.tokens.next_token()?;
            if token != 0 && !self.sessions.contains_key(&token) {
                return Ok(token);
            }
        }
        Err(ServerError::TokenExhausted {
            attempts: TOKEN_ATTEMPTS,
        })
    }
}

/// A structurally decoded request kind, with an echo's arrival instant already
/// sampled.
///
/// This mirrors [`DecodedRequestKind`] rather than replacing it: the decoder
/// classifies the packet, and this records what the core had to capture at that
/// moment. Keeping the two apart is what lets the clock be read before
/// authentication without any handler having to ask whether a sample exists.
enum ClassifiedKind<'a> {
    Open { no_test: bool, params: &'a [u8] },
    Close { token: u64 },
    Echo(EchoRequest),
}

/// The whole of what an echo request means to this server.
///
/// Everything after the sequence number is opaque, so nothing beyond these
/// fields is carried forward. `datagram_len` is the length the datagram
/// actually arrived with, which the configured maximum judges; it is not
/// required to match the negotiated length, and it never sizes the reply.
#[derive(Debug, Clone, Copy)]
struct EchoRequest {
    token: u64,
    sequence: u32,
    datagram_len: usize,
    received_at: ClockSample,
}

/// Builds the timestamp fields a session negotiated, and only those.
///
/// [`StampAt`] selects which instants are reported and [`Clock`] selects which
/// domains, exactly as the reply layout does, so a field this returns as `Some`
/// is a field the encoder has a slot for. A midpoint is the per-domain mean of
/// the two instants.
///
/// A single-clock midpoint therefore produces exactly one midpoint field. That
/// is the conforming representation; upstream 0.9.1's habit of emitting both
/// midpoint fields regardless of the negotiated clock is a defect this server
/// deliberately does not reproduce. `irtt-proto` still *decodes* that form, so
/// interoperating with a peer that sends it is unaffected.
///
/// [`Clock::Unspecified`] selects no domain and so produces no field. It cannot
/// be reached with a non-none `StampAt` on an acknowledged session, because
/// that combination is refused at open; nothing here needs to repair it.
///
/// The pair is ordered before anything is built from it, because a reply's
/// receive instant must not be later than its send instant and the wall clock
/// can be stepped backwards between the two readings. See
/// [`ClockSample::not_after`]; the correction is confined to this one reply.
fn timestamp_fields(params: &Params, received: ClockSample, sent: ClockSample) -> TimestampFields {
    let received = received.not_after(sent);
    let per_clock = |sample: ClockSample| {
        (
            params.clock.has_wall().then_some(sample.wall_ns),
            params.clock.has_mono().then_some(sample.mono_ns),
        )
    };
    let absent = (None, None);
    let (recv, midpoint, send) = match params.stamp_at {
        StampAt::None => (absent, absent, absent),
        StampAt::Receive => (per_clock(received), absent, absent),
        StampAt::Send => (absent, absent, per_clock(sent)),
        StampAt::Both => (per_clock(received), absent, per_clock(sent)),
        StampAt::Midpoint => (absent, per_clock(received.midpoint(sent)), absent),
    };
    TimestampFields {
        recv_wall: recv.0,
        recv_mono: recv.1,
        midpoint_wall: midpoint.0,
        midpoint_mono: midpoint.1,
        send_wall: send.0,
        send_mono: send.1,
    }
}

/// Whether two source endpoints identify the same session endpoint.
///
/// The address family, the IP address and the UDP source port are established
/// identity components: a request from a different port, address or family is
/// dropped and leaves its session live.
///
/// The remaining two fields of [`SocketAddrV6`] are decided here rather than
/// observed, which is why this is not simply `==`.
///
/// The flow label is excluded. It identifies no endpoint — it is routing
/// metadata a sender or forwarding path may legitimately vary within one flow,
/// and a receiver reports it only when asked to — so admitting it could strand
/// a session its own client can then never close. It grants nothing in
/// exchange: the token, address, port and scope are what an off-path attacker
/// would have to guess.
///
/// The scope identifier is included, and that is **`irtt-rs` policy, not
/// verified compatibility behavior**. The server specification lists the IPv6
/// zone as an identity component, but its remaining-unknowns section records
/// that multi-zone behavior could not be tested on a single-host platform, so
/// no evidence settles how a reference server compares scoped addresses. Two
/// peers reachable at the same link-local address in different zones are
/// genuinely different peers, so treating the zone as identity is the
/// conservative reading: the failure it prevents is one peer closing another's
/// session, while the failure it risks is a scoped client having to reopen.
/// Revisit if evidence ever settles the question.
///
/// [`SocketAddrV6`]: std::net::SocketAddrV6
fn same_endpoint(session: SocketAddr, peer: SocketAddr) -> bool {
    match (session, peer) {
        (SocketAddr::V6(session), SocketAddr::V6(peer)) => {
            session.ip() == peer.ip()
                && session.port() == peer.port()
                && session.scope_id() == peer.scope_id()
        }
        (session, peer) => session == peer,
    }
}

/// Applies the open parameter rules the decoder cannot express.
///
/// This judges the *request*, before negotiation, which is why it takes the
/// decoded form with its presence flags. Whether the resulting session is
/// coherent is a separate question, asked after negotiation by
/// [`negotiated_params_are_admissible`].
///
/// Duration and Interval are accepted when absent, taking the wire default
/// zero, and rejected when explicitly present and not positive. The presence
/// distinction is why the decoder reports it: testing the value alone would
/// wrongly reject an open that simply omitted the tag, which includes the empty
/// parameter payload.
///
/// Every other known parameter is already validated or deliberately
/// unrestricted here. Protocol version is accepted at any value and rewritten
/// during negotiation. Length and DSCP are accepted as decoded, including zero,
/// negative and out-of-byte-range values; a later slice may restrict a DSCP it
/// cannot actually apply to the socket. Unknown tags were ignored by the
/// decoder and are not reflected in the reply.
fn open_params_are_admissible(decoded: &DecodedParams) -> bool {
    let admissible = |present: bool, value: i64| !present || value > 0;
    admissible(decoded.presence.duration_ns, decoded.params.duration_ns)
        && admissible(decoded.presence.interval_ns, decoded.params.interval_ns)
}

/// Whether the *effective* parameters describe a session this server could
/// actually run.
///
/// This is the last gate before an open is acknowledged, and it asks a
/// different question from [`open_params_are_admissible`]: not whether the
/// request was well formed, but whether the parameters negotiation settled on
/// are coherent. It therefore runs on the negotiated params, after
/// [`negotiate_params`], and covers the no-test path too — a no-test open
/// exists to tell a client what the session would be, so it must not report a
/// session that could not exist.
///
/// Two rules so far.
///
/// **Selecting timestamps requires a specified clock.** A non-none `stamp_at`
/// with [`Clock::Unspecified`] asks for timestamp fields from no clock at all,
/// which lays out no timestamp field and leaves the session with a request the
/// server cannot honor.
///
/// [`Clock::Unspecified`] means the Clock tag was absent — an explicit zero is
/// already out of range for the decoder — so this cannot be reached by a
/// conforming client, which sends a clock whenever it selects timestamps. The
/// combination is silently rejected rather than repaired: synthesizing a clock
/// would answer with a session the client never asked for, and rewriting
/// `stamp_at` to none would silently drop the measurement it did ask for.
/// Neither is the server's to decide.
///
/// **The session's echo datagram must fit
/// [`max_packet_length`](ServerConfig::max_packet_length).** Negotiation already
/// reduced an oversized Length, but that only bounds what the *parameter* asks
/// for: the mandatory field block grows with the received statistics, the
/// timestamps and — by 16 bytes — authentication, and can exceed the configured
/// maximum on its own. A session whose smallest compliant reply is already too
/// large is one the server could never answer an echo for, so it is refused at
/// open rather than acknowledged and then found unserviceable.
///
/// The size comes from [`echo_packet_len`], the one packet-size authority, so
/// this cannot drift from what an echo reply actually encodes. Its
/// unrepresentable-length error is a rejection here, not a [`ServerError`]: a
/// pathological requested length is remote input, not an internal failure. The
/// default policy makes it unreachable for an acknowledged session anyway, since
/// negotiation has already reduced the length to a representable maximum.
///
/// The check is deliberately *after* negotiation so that a later policy slice
/// restricting timestamps can restrict `stamp_at` to [`StampAt::None`], at
/// which point an absent clock is safe and this predicate holds without
/// changing.
///
/// [`ServerError`]: crate::ServerError
fn negotiated_params_are_admissible(params: &Params, config: &ServerConfig) -> bool {
    if params.stamp_at != StampAt::None && params.clock == Clock::Unspecified {
        return false;
    }
    echo_packet_len(config.hmac_key().is_some(), params)
        .is_ok_and(|len| len <= config.max_packet_length())
}
