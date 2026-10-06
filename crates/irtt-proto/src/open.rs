use crate::{
    envelope::{self, FlagRule},
    flags::{has, FLAG_CLOSE, FLAG_OPEN, FLAG_REPLY},
    layout::PacketLayout,
    params::Params,
    ProtoError, Result, TOKEN_SIZE,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenReply {
    pub flags: u8,
    pub token: u64,
    pub params: Params,
}

/// Encodes an open reply.
///
/// `hmac_key` is authoritative: `Some(key)` sets `FLAG_HMAC` and signs the
/// finished datagram, while `None` produces an unauthenticated reply and clears
/// any caller-supplied `FLAG_HMAC`. A zero token is rejected unless the reply
/// also carries `FLAG_CLOSE` (a rejected open).
///
/// # Errors
///
/// Returns [`ProtoError::MissingFlag`] when the reply lacks `FLAG_OPEN` or
/// `FLAG_REPLY`, and [`ProtoError::ZeroToken`] for a zero token without
/// `FLAG_CLOSE`.
///
/// # Example
///
/// ```
/// use irtt_proto::{
///     decode_open_reply, encode_open_reply, OpenReply, Params, FLAG_OPEN, FLAG_REPLY,
/// };
///
/// let reply = OpenReply {
///     flags: FLAG_OPEN | FLAG_REPLY,
///     token: 0x1234_5678_9abc_def0,
///     params: Params::with_protocol_defaults(),
/// };
/// let packet = encode_open_reply(&reply, None).unwrap();
/// assert_eq!(decode_open_reply(&packet, None).unwrap(), reply);
/// ```
pub fn encode_open_reply(reply: &OpenReply, hmac_key: Option<&[u8]>) -> Result<Vec<u8>> {
    let params = reply.params.encode();
    let mut out = envelope::begin_checked(
        reply.flags,
        hmac_key,
        &[FlagRule::Require(FLAG_OPEN), FlagRule::Require(FLAG_REPLY)],
        PacketLayout::open_reply(hmac_key.is_some()).header_len() + params.len(),
    )?;
    if reply.token == 0 && !has(reply.flags, FLAG_CLOSE) {
        return Err(ProtoError::ZeroToken);
    }

    out.extend_from_slice(&reply.token.to_le_bytes());
    out.extend_from_slice(&params);
    envelope::finish(out, hmac_key)
}

/// Decodes an open reply.
///
/// Requires `FLAG_OPEN` and `FLAG_REPLY`, and — when `hmac_key` is supplied —
/// verifies the authentication field. The decoded [`Params`] are validated:
/// malformed or incompatible parameter values are rejected here.
///
/// # Errors
///
/// Returns [`ProtoError::MissingFlag`], [`ProtoError::HmacPresenceMismatch`],
/// [`ProtoError::BadHmac`], [`ProtoError::PacketTooShort`],
/// [`ProtoError::ZeroToken`], or a parameter decode error. See
/// [`encode_open_reply`] for a round-trip example.
pub fn decode_open_reply(packet: &[u8], hmac_key: Option<&[u8]>) -> Result<OpenReply> {
    let envelope = envelope::decode(
        packet,
        hmac_key,
        &[FlagRule::Require(FLAG_OPEN), FlagRule::Require(FLAG_REPLY)],
    )?;
    envelope::verify(packet, hmac_key)?;

    let mut pos = envelope.body_offset;
    let needed = pos + TOKEN_SIZE;
    if packet.len() < needed {
        return Err(ProtoError::PacketTooShort {
            needed,
            actual: packet.len(),
        });
    }
    let token = u64::from_le_bytes(packet[pos..pos + TOKEN_SIZE].try_into().unwrap());
    pos += TOKEN_SIZE;
    if token == 0 && !has(envelope.flags, FLAG_CLOSE) {
        return Err(ProtoError::ZeroToken);
    }

    Ok(OpenReply {
        flags: envelope.flags,
        token,
        params: Params::decode(&packet[pos..])?,
    })
}
