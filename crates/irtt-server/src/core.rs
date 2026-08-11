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
/// This slice implements open handling and session creation. Echo and close
/// requests are structurally recognized and then deliberately ignored — no
/// reply, no session mutation — until their own slices.
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
    #[must_use]
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Looks up a live session by token.
    ///
    /// This is a lookup by token alone and is therefore *not* an admission
    /// check: a request is bound to a session only when the token matches and
    /// it arrived from the session's exact endpoint.
    #[must_use]
    pub fn session(&self, token: u64) -> Option<&Session> {
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
            // Not implemented in this slice. A close must not remove a session
            // and an echo must not advance receive state until the slices that
            // implement those behaviors; emitting a placeholder reply now would
            // be worse than silence.
            DecodedRequestKind::Close { .. } | DecodedRequestKind::Echo { .. } => Ok(None),
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
