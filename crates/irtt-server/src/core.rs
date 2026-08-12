use std::{collections::HashMap, net::SocketAddr};

use irtt_proto::{
    decode_request, encode_open_reply, verify_packet_hmac, DecodedParams, DecodedRequestKind,
    OpenReply, Params, FLAG_CLOSE, FLAG_OPEN, FLAG_REPLY,
};

use crate::{
    config::ServerConfig,
    error::ServerError,
    negotiate::negotiate_params,
    session::Session,
    token::{OsTokenSource, TokenSource, TOKEN_ATTEMPTS},
};

/// The deterministic protocol and session engine of the server.
///
/// `ServerCore` owns admission, negotiation, the session table and reply
/// construction. It performs no I/O and keeps no clock: a caller hands it a
/// source endpoint and the bytes of one received datagram, and gets back the
/// bytes to send to that endpoint, or nothing. Everything it does is a pure
/// function of its state and that input, except for drawing session tokens.
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
/// This slice implements open handling, session creation and client-initiated
/// close. Echo requests are structurally recognized and then deliberately
/// ignored — no reply, no session mutation — until their own slice.
#[derive(Debug)]
pub struct ServerCore {
    config: ServerConfig,
    sessions: HashMap<u64, Session>,
    tokens: Box<dyn TokenSource>,
}

impl ServerCore {
    /// Creates a server core drawing session tokens from the operating system's
    /// random source.
    #[must_use]
    pub fn new(config: ServerConfig) -> Self {
        Self::with_token_source(config, Box::new(OsTokenSource))
    }

    pub(crate) fn with_token_source(config: ServerConfig, tokens: Box<dyn TokenSource>) -> Self {
        Self {
            config,
            sessions: HashMap::new(),
            tokens,
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
    /// payload, would exceed the session bound, or is a request kind this slice
    /// does not implement. None of that mutates any live session.
    ///
    /// `Ok(None)` is not only a rejection, though. A close request that does
    /// release its session is answered with nothing as well, because protocol
    /// version 1 defines no acknowledgement for one.
    ///
    /// Authentication is checked before open parameters are decoded. That
    /// ordering is deliberate: a packet that fails authentication is discarded
    /// without parsing untrusted parameter data, and no reply behavior may
    /// reveal which stage rejected it.
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

        match request.kind {
            DecodedRequestKind::Open { no_test, params } => self.handle_open(peer, no_test, params),
            DecodedRequestKind::Close { token } => {
                self.handle_close(peer, token);
                Ok(None)
            }
            // Not implemented in this slice. An echo must not advance receive
            // state until the slice that implements it; emitting a placeholder
            // reply now would be worse than silence.
            DecodedRequestKind::Echo { .. } => Ok(None),
        }
    }

    fn handle_open(
        &mut self,
        peer: SocketAddr,
        no_test: bool,
        encoded_params: &[u8],
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
        self.sessions
            .insert(token, Session::new(peer, reply.params));
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

/// Whether two source endpoints identify the same session endpoint.
///
/// Identity is the address family, the IP address, the UDP source port and —
/// for IPv6 — the scope identifier. That is the whole list, so this is not
/// simply `==`: [`SocketAddrV6`] equality also covers the flow label, which
/// identifies no endpoint. It is routing metadata that a sender or a forwarding
/// path may legitimately vary within one flow, and a receiver reports it only
/// when asked to, so admitting it into identity could strand a session that its
/// own client can then never close. It grants nothing in exchange: the token,
/// address, port and scope are what an off-path attacker would have to guess.
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
