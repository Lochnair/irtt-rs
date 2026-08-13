use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use irtt_proto::{
    decode_echo_reply, decode_open_reply, encode_request, varint, EchoReply, OpenReply,
    PacketLayout, Params, RequestToEncode, FLAG_CLOSE, FLAG_HMAC, FLAG_OPEN, FLAG_REPLY,
};

use crate::{
    clock::{ClockSample, ClockSource},
    core::{OutboundDatagram, ServerCore},
    error::ServerError,
    token::TokenSource,
    ServerConfig,
};

pub(crate) const KEY: &[u8] = b"correctkey";
pub(crate) const OTHER_KEY: &[u8] = b"wrongkey";

/// A default configuration with the minimum send interval disabled.
///
/// One knob, two consequences, and tests about neither of them want both gone:
/// no requested interval is raised during negotiation, and no otherwise
/// admissible echo ever waits for rate allowance. That keeps a test about
/// parameter pass-through, receive statistics or timestamps from silently
/// becoming a test of interval policy — and, since these tests drive the core as
/// fast as the machine will run, from being rate-limited by a floor they never
/// meant to configure.
///
/// Tests that *are* about interval negotiation, rate limiting or lifetime build
/// their configuration explicitly.
pub(crate) fn unthrottled() -> ServerConfig {
    ServerConfig::default().with_min_send_interval(Duration::ZERO)
}

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

/// A clock handing out a fixed script of samples, then failing loudly.
///
/// Running the script dry is a test bug, not a wrap-around: a test that asserts
/// timestamps must know exactly which sample each field came from. The core
/// takes one sample per authenticated request — when it classifies a datagram as
/// an echo, or after authenticating an open or close — and one more when an
/// admitted echo is about to be answered.
///
/// Tests about lifetime and rate want [`ManualClock`] instead: they care about
/// *when* a datagram arrives, not about how many times the core reads the clock
/// on the way.
#[derive(Debug, Clone)]
pub(crate) struct ScriptedClock {
    samples: Arc<Mutex<VecDeque<ClockSample>>>,
}

impl ScriptedClock {
    pub(crate) fn new<I>(samples: I) -> Self
    where
        I: IntoIterator<Item = ClockSample>,
    {
        Self {
            samples: Arc::new(Mutex::new(samples.into_iter().collect())),
        }
    }

    /// How many scripted samples are still unused.
    pub(crate) fn remaining(&self) -> usize {
        self.samples.lock().unwrap().len()
    }
}

impl ClockSource for ScriptedClock {
    fn sample(&mut self) -> ClockSample {
        self.samples
            .lock()
            .unwrap()
            .pop_front()
            .expect("the scripted clock ran out: script one sample per echo received, and one more per echo answered")
    }
}

pub(crate) fn sample(wall_ns: i64, mono_ns: i64) -> ClockSample {
    ClockSample { wall_ns, mono_ns }
}

/// A clock standing still at an instant the test moves by hand.
///
/// Lifetime and rate behavior is a question of when a datagram arrives, so these
/// tests set the time, send a packet, and set it again — with no sleeps, no
/// tolerances and nothing riding on how many times the core reads the clock.
/// [`ScriptedClock`] remains for the timestamp tests, where every individual
/// reading is the point.
///
/// Both domains read the same value. Lifetime decisions are monotonic-only by
/// design, and no test here asserts a wall timestamp, so keeping the pair equal
/// makes the readings obviously ordered.
#[derive(Debug, Clone)]
pub(crate) struct ManualClock {
    now_ns: Arc<Mutex<i64>>,
}

impl ManualClock {
    pub(crate) fn at(now_ns: i64) -> Self {
        Self {
            now_ns: Arc::new(Mutex::new(now_ns)),
        }
    }

    /// Moves the clock to an exact instant. Deliberately absolute: a test that
    /// pins a deadline reads better stating the instant than accumulating
    /// offsets.
    pub(crate) fn set(&self, now_ns: i64) {
        *self.now_ns.lock().unwrap() = now_ns;
    }
}

impl ClockSource for ManualClock {
    fn sample(&mut self) -> ClockSample {
        let now_ns = *self.now_ns.lock().unwrap();
        ClockSample {
            wall_ns: now_ns,
            mono_ns: now_ns,
        }
    }
}

/// A core with deterministic tokens, so session identity is assertable. Its
/// clock is the production one, which is fine for everything that does not
/// assert timestamp values.
pub(crate) fn core_with_tokens(config: ServerConfig, tokens: ScriptedTokens) -> ServerCore {
    ServerCore::with_token_source(config, Box::new(tokens))
}

/// A core with both nondeterministic sources chosen, for timestamp, rate and
/// lifetime tests.
pub(crate) fn core_with_sources(
    config: ServerConfig,
    tokens: ScriptedTokens,
    clock: impl ClockSource + 'static,
) -> ServerCore {
    ServerCore::with_sources(config, Box::new(tokens), Box::new(clock))
}

/// An otherwise default core that authenticates iff `hmac_key` is set.
///
/// Most behavior has to hold identically with and without authentication, so
/// tests loop over both and build the core from the key.
pub(crate) fn core_for(hmac_key: Option<&[u8]>, tokens: ScriptedTokens) -> ServerCore {
    let mut config = unthrottled();
    if let Some(key) = hmac_key {
        config = config.with_hmac_key(key);
    }
    core_with_tokens(config, tokens)
}

/// A core whose clock the test moves by hand, with one session already open.
///
/// The clock starts at monotonic zero and the session is opened there, so every
/// instant a rate or lifetime test names reads as an offset from the open.
pub(crate) fn manual_core(
    config: ServerConfig,
    requested: &Params,
    tokens: ScriptedTokens,
    hmac_key: Option<&[u8]>,
) -> (ServerCore, ManualClock, u64, Params) {
    let clock = ManualClock::at(0);
    let mut core = core_with_sources(config, tokens, clock.clone());
    let (token, negotiated) = open_negotiated(&mut core, peer(), requested, hmac_key);
    (core, clock, token, negotiated)
}

/// Sends one echo as it would arrive at monotonic instant `at_ns`.
pub(crate) fn echo_at(
    core: &mut ServerCore,
    clock: &ManualClock,
    at_ns: i64,
    token: u64,
    sequence: u32,
    params: &Params,
    hmac_key: Option<&[u8]>,
) -> Option<OutboundDatagram> {
    clock.set(at_ns);
    core.handle_datagram(
        peer(),
        &echo_request(token, sequence, params, &[], hmac_key),
    )
    .expect("a rejected or rate-limited echo is not an internal error")
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

/// Opens one session carrying `params` and returns its token together with the
/// parameters the server negotiated for it.
///
/// Echo tests need both: the token addresses the session, and the negotiated
/// params are what the reply is laid out from and what decoding it requires.
pub(crate) fn open_negotiated(
    core: &mut ServerCore,
    endpoint: SocketAddr,
    params: &Params,
    hmac_key: Option<&[u8]>,
) -> (u64, Params) {
    let packet = core
        .handle_datagram(endpoint, &open_request(params, hmac_key))
        .unwrap()
        .expect("the session-creating open must be answered");
    let reply = expect_normal_open_reply(&packet, hmac_key);
    (reply.token, reply.params)
}

/// Params requesting exactly the echo layout a test cares about, with the
/// duration and interval a well-behaved client sends.
pub(crate) fn echo_params(
    received_stats: irtt_proto::ReceivedStats,
    stamp_at: irtt_proto::StampAt,
    clock: irtt_proto::Clock,
    length: i64,
) -> Params {
    Params {
        received_stats,
        stamp_at,
        clock,
        length,
        ..client_params()
    }
}

/// Encodes an echo request. `params` supplies only the *request's* own sizing;
/// it is deliberately a separate argument from the session's negotiated params
/// so tests can send requests shorter and longer than the session negotiated.
pub(crate) fn echo_request(
    token: u64,
    sequence: u32,
    params: &Params,
    payload: &[u8],
    hmac_key: Option<&[u8]>,
) -> Vec<u8> {
    encode_request(
        RequestToEncode::Echo {
            token,
            sequence,
            params,
            payload,
        },
        hmac_key,
    )
    .unwrap()
}

/// Decodes an echo reply the server produced and checks the flags every reply
/// must carry. Decoding verifies the MAC against `hmac_key` on the way.
pub(crate) fn expect_echo_reply(
    packet: impl AsRef<[u8]>,
    params: &Params,
    hmac_key: Option<&[u8]>,
) -> EchoReply {
    let reply = decode_echo_reply(packet.as_ref(), params, hmac_key)
        .expect("server echo reply must decode");
    // Exact equality, so Open and Close are pinned clear as well: Open makes an
    // upstream client abort, and Close belongs to the server-initiated close
    // slice.
    let expected = FLAG_REPLY | if hmac_key.is_some() { FLAG_HMAC } else { 0 };
    assert_eq!(reply.flags, expected, "echo reply flags");
    reply
}

/// Decodes a server-initiated close: an ordinary echo reply carrying Close.
///
/// The flags are compared exactly, so Open is pinned clear — a reply with it set
/// makes an upstream client abort — and the reply is decoded and its MAC checked
/// by the same path as any other, which is the point: the close is not a
/// separate packet kind.
pub(crate) fn expect_closing_echo_reply(
    packet: impl AsRef<[u8]>,
    params: &Params,
    hmac_key: Option<&[u8]>,
) -> EchoReply {
    let reply = decode_echo_reply(packet.as_ref(), params, hmac_key)
        .expect("a closing echo reply must decode");
    let expected = FLAG_REPLY | FLAG_CLOSE | if hmac_key.is_some() { FLAG_HMAC } else { 0 };
    assert_eq!(reply.flags, expected, "closing echo reply flags");
    reply
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
pub(crate) fn decode_reply(packet: impl AsRef<[u8]>, hmac_key: Option<&[u8]>) -> OpenReply {
    decode_open_reply(packet.as_ref(), hmac_key).expect("server reply must decode")
}

pub(crate) fn expect_normal_open_reply(
    packet: impl AsRef<[u8]>,
    hmac_key: Option<&[u8]>,
) -> OpenReply {
    let reply = decode_reply(packet, hmac_key);
    let expected = FLAG_OPEN | FLAG_REPLY | if hmac_key.is_some() { FLAG_HMAC } else { 0 };
    assert_eq!(reply.flags, expected, "normal open reply flags");
    assert_ne!(reply.token, 0, "a session-creating reply needs a token");
    assert_eq!(reply.params.protocol_version, 1);
    reply
}

pub(crate) fn expect_no_test_reply(packet: impl AsRef<[u8]>, hmac_key: Option<&[u8]>) -> OpenReply {
    let reply = decode_reply(packet, hmac_key);
    let expected =
        FLAG_OPEN | FLAG_REPLY | FLAG_CLOSE | if hmac_key.is_some() { FLAG_HMAC } else { 0 };
    assert_eq!(reply.flags, expected, "no-test reply flags");
    assert_eq!(reply.token, 0, "a no-test reply carries a zero token");
    reply
}
