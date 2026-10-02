use crate::{token::TokenSource, ServerConfig, ServerCore, ServerError};
use irtt_proto::{
    decode_open_reply, encode_request, OpenReply, Params, RequestToEncode, FLAG_HMAC, FLAG_OPEN,
    FLAG_REPLY,
};
use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

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

pub(crate) fn peer() -> SocketAddr {
    "198.51.100.7:41234".parse().unwrap()
}

pub(crate) fn core_with_tokens(config: ServerConfig, tokens: ScriptedTokens) -> ServerCore {
    ServerCore::with_token_source(config, Box::new(tokens))
}

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
