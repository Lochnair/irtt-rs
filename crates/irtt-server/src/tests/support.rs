use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use irtt_proto::{
    decode_open_reply, encode_request, varint, OpenReply, PacketLayout, Params, RequestToEncode,
    FLAG_CLOSE, FLAG_HMAC, FLAG_OPEN, FLAG_REPLY,
};

use crate::{core::ServerCore, error::ServerError, token::TokenSource, ServerConfig};

pub(crate) const KEY: &[u8] = b"correctkey";
pub(crate) const OTHER_KEY: &[u8] = b"wrongkey";

pub(crate) fn peer() -> SocketAddr {
    "198.51.100.7:41234".parse().unwrap()
}

pub(crate) fn other_peer() -> SocketAddr {
    "198.51.100.7:41235".parse().unwrap()
}

/// A token source handing out a fixed script of values, then failing.
///
/// Draining the script is itself a failure rather than a wrap-around, so a test
/// that draws more tokens than it scripted is a loud test bug instead of a
/// silent repeat.
#[derive(Debug, Clone)]
pub(crate) struct ScriptedTokens {
    values: Arc<Mutex<VecDeque<Result<u64, ServerError>>>>,
}

impl ScriptedTokens {
    pub(crate) fn new<I>(values: I) -> Self
    where
        I: IntoIterator<Item = u64>,
    {
        Self {
            values: Arc::new(Mutex::new(values.into_iter().map(Ok).collect())),
        }
    }

    /// A source whose every draw reports a random-source failure.
    pub(crate) fn failing() -> Self {
        Self {
            values: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// How many scripted values are still unused.
    pub(crate) fn remaining(&self) -> usize {
        self.values.lock().unwrap().len()
    }
}

impl TokenSource for ScriptedTokens {
    fn next_token(&mut self) -> Result<u64, ServerError> {
        self.values
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Err(ServerError::RandomSource {
                reason: "scripted failure".to_owned(),
            }))
    }
}

/// A core with deterministic tokens, so session identity is assertable.
pub(crate) fn core_with_tokens(config: ServerConfig, tokens: ScriptedTokens) -> ServerCore {
    ServerCore::with_token_source(config, Box::new(tokens))
}

/// An otherwise default core that authenticates iff `hmac_key` is set.
///
/// Most behavior has to hold identically with and without authentication, so
/// tests loop over both and build the core from the key.
pub(crate) fn core_for(hmac_key: Option<&[u8]>, tokens: ScriptedTokens) -> ServerCore {
    let mut config = ServerConfig::default();
    if let Some(key) = hmac_key {
        config = config.with_hmac_key(key);
    }
    core_with_tokens(config, tokens)
}

/// Opens one ordinary session from `endpoint` and returns its token.
pub(crate) fn open_session(
    core: &mut ServerCore,
    endpoint: SocketAddr,
    hmac_key: Option<&[u8]>,
) -> u64 {
    let packet = core
        .handle_datagram(endpoint, &open_request(&client_params(), hmac_key))
        .unwrap()
        .expect("the session-creating open must be answered");
    expect_normal_open_reply(&packet, hmac_key).token
}

/// Encodes a normal open request carrying `params`.
pub(crate) fn open_request(params: &Params, hmac_key: Option<&[u8]>) -> Vec<u8> {
    encode_request(
        RequestToEncode::Open {
            params,
            no_test: false,
        },
        hmac_key,
    )
    .unwrap()
}

/// Encodes a no-test open request carrying `params`.
pub(crate) fn no_test_request(params: &Params, hmac_key: Option<&[u8]>) -> Vec<u8> {
    encode_request(
        RequestToEncode::Open {
            params,
            no_test: true,
        },
        hmac_key,
    )
    .unwrap()
}

/// Encodes a close request for `token`.
pub(crate) fn close_request(token: u64, hmac_key: Option<&[u8]>) -> Vec<u8> {
    encode_request(RequestToEncode::Close { token }, hmac_key).unwrap()
}

/// Encodes a close request carrying arbitrary bytes after the token.
///
/// The decoder tolerates them, and this is the shape that proves it. The MAC
/// covers the whole datagram, so it is recomputed over the final form rather
/// than left behind by the encoder's shorter packet.
pub(crate) fn close_request_with_trailing(
    token: u64,
    trailing: &[u8],
    hmac_key: Option<&[u8]>,
) -> Vec<u8> {
    let mut packet = close_request(token, hmac_key);
    packet.extend_from_slice(trailing);
    if let Some(key) = hmac_key {
        irtt_proto::compute_hmac_in_place(key, &mut packet, hmac_field_offset()).unwrap();
    }
    packet
}

/// Builds an open request around a raw parameter payload.
///
/// Used for payloads a compliant encoder cannot produce, such as a truncated
/// varint or an out-of-range enum value.
pub(crate) fn open_request_with_raw_params(payload: &[u8], hmac_key: Option<&[u8]>) -> Vec<u8> {
    let mut packet = irtt_proto::MAGIC.to_vec();
    packet.push(if hmac_key.is_some() {
        FLAG_OPEN | FLAG_HMAC
    } else {
        FLAG_OPEN
    });
    if hmac_key.is_some() {
        packet.extend_from_slice(&[0; irtt_proto::HMAC_SIZE]);
    }
    packet.extend_from_slice(payload);
    if let Some(key) = hmac_key {
        irtt_proto::compute_hmac_in_place(key, &mut packet, hmac_field_offset()).unwrap();
    }
    packet
}

/// The authentication field sits immediately after the fixed header.
fn hmac_field_offset() -> usize {
    PacketLayout::open_request(false).header_len()
}

/// Encodes one integer parameter as `tag` + zigzag value.
pub(crate) fn param_int(tag: u64, value: i64) -> Vec<u8> {
    let mut encoded = Vec::new();
    varint::encode_uvarint(tag, &mut encoded);
    varint::encode_varint(value, &mut encoded);
    encoded
}

/// Encodes a `server_fill` parameter with an explicit declared length, so tests
/// can declare a length the payload does not actually carry.
pub(crate) fn param_server_fill(declared_len: u64, value: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::new();
    varint::encode_uvarint(9, &mut encoded);
    varint::encode_uvarint(declared_len, &mut encoded);
    encoded.extend_from_slice(value);
    encoded
}

/// Params a well-behaved client would request.
pub(crate) fn client_params() -> Params {
    Params {
        protocol_version: 1,
        duration_ns: 3_000_000_000,
        interval_ns: 1_000_000_000,
        length: 1472,
        received_stats: irtt_proto::ReceivedStats::Both,
        stamp_at: irtt_proto::StampAt::Both,
        clock: irtt_proto::Clock::Both,
        dscp: 184,
        server_fill: None,
    }
}

/// Decodes a reply the server produced, checking it against the server's key.
pub(crate) fn decode_reply(packet: &[u8], hmac_key: Option<&[u8]>) -> OpenReply {
    decode_open_reply(packet, hmac_key).expect("server reply must decode")
}

pub(crate) fn expect_normal_open_reply(packet: &[u8], hmac_key: Option<&[u8]>) -> OpenReply {
    let reply = decode_reply(packet, hmac_key);
    let expected = FLAG_OPEN | FLAG_REPLY | if hmac_key.is_some() { FLAG_HMAC } else { 0 };
    assert_eq!(reply.flags, expected, "normal open reply flags");
    assert_ne!(reply.token, 0, "a session-creating reply needs a token");
    assert_eq!(reply.params.protocol_version, 1);
    reply
}

pub(crate) fn expect_no_test_reply(packet: &[u8], hmac_key: Option<&[u8]>) -> OpenReply {
    let reply = decode_reply(packet, hmac_key);
    let expected =
        FLAG_OPEN | FLAG_REPLY | FLAG_CLOSE | if hmac_key.is_some() { FLAG_HMAC } else { 0 };
    assert_eq!(reply.flags, expected, "no-test reply flags");
    assert_eq!(reply.token, 0, "a no-test reply carries a zero token");
    reply
}
