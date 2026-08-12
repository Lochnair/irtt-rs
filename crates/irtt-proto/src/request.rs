//! Directional request codecs: one sender-side encoder and one receiver-side
//! structural decoder.
//!
//! The two directions are deliberately asymmetric. Encoding an ECHO request
//! needs the negotiated [`Params`] because the sender constructs a
//! negotiated-length datagram; decoding one does not, because a receiver reads
//! only the token and sequence number and treats the rest as opaque.

use crate::{
    envelope,
    flags::{has, FLAG_CLOSE, FLAG_OPEN, FLAG_REPLY},
    layout::{echo_packet_len, PacketLayout},
    params::Params,
    ProtoError, Result, SEQ_SIZE, TOKEN_SIZE,
};

/// A request to construct, described in sender-side semantic terms.
///
/// Every variant borrows what it needs; the encoder owns nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestToEncode<'a> {
    /// Session-opening request. `no_test` selects the *no-test* form, which
    /// adds `FLAG_CLOSE` and asks the peer to answer without creating a
    /// session.
    Open { params: &'a Params, no_test: bool },
    /// Session teardown request.
    Close { token: u64 },
    /// Round-trip probe. `params` supplies the negotiated packet layout and
    /// length, and `payload` is placed at the negotiated payload offset.
    Echo {
        token: u64,
        sequence: u32,
        params: &'a Params,
        payload: &'a [u8],
    },
}

/// A structurally decoded inbound request, borrowed from the packet.
///
/// `hmac_present` reports only that `FLAG_HMAC` was set and that the packet is
/// laid out with an authentication field. It is **not** an authentication
/// result: the field may hold arbitrary bytes. Use [`verify_packet_hmac`] to
/// check the MAC once the applicable key is known.
///
/// [`verify_packet_hmac`]: crate::verify_packet_hmac
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodedRequest<'a> {
    pub hmac_present: bool,
    pub kind: DecodedRequestKind<'a>,
}

/// The request kinds a receiver can distinguish from the packet alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodedRequestKind<'a> {
    /// `FLAG_OPEN` was set. `params` is the still-encoded parameter payload;
    /// see [`decode_request`] for why it is not decoded here.
    Open { no_test: bool, params: &'a [u8] },
    /// `FLAG_CLOSE` was set without `FLAG_OPEN`. Trailing bytes after the token
    /// are accepted and not exposed, because they carry no meaning.
    Close { token: u64 },
    /// Neither `FLAG_OPEN` nor `FLAG_CLOSE` was set. `tail` is everything after
    /// the sequence number, which is opaque to a receiver.
    Echo {
        token: u64,
        sequence: u32,
        tail: &'a [u8],
    },
}

/// Encodes an outbound request.
///
/// This is sender-side semantic construction: the variant determines the flags,
/// so callers never assemble flag bits themselves.
///
/// `hmac_key` is authoritative. `Some(key)` sets `FLAG_HMAC`, reserves the
/// 16-byte authentication field after the header, and signs the finished
/// datagram; `None` produces an unauthenticated packet.
///
/// [`RequestToEncode::Echo`] requires the negotiated [`Params`] because the
/// sender must build a datagram of the negotiated length with the payload at
/// the negotiated offset. The receiving side needs no `Params` — see
/// [`decode_request`].
///
/// # Errors
///
/// Returns [`ProtoError::PayloadTooLarge`] when an ECHO payload does not fit
/// the negotiated length, and [`ProtoError::PacketLengthUnrepresentable`] when
/// the negotiated length is positive and wider than `usize`. A *negative*
/// negotiated length is not an error: see [`echo_packet_len`], which floors it
/// at the mandatory field block.
pub fn encode_request(request: RequestToEncode<'_>, hmac_key: Option<&[u8]>) -> Result<Vec<u8>> {
    match request {
        RequestToEncode::Open { params, no_test } => encode_open(params, no_test, hmac_key),
        RequestToEncode::Close { token } => encode_close(token, hmac_key),
        RequestToEncode::Echo {
            token,
            sequence,
            params,
            payload,
        } => encode_echo(token, sequence, params, payload, hmac_key),
    }
}

/// Structurally decodes an inbound request.
///
/// This is packet structure only — it is **not** admission control. It performs
/// no token or session lookup, applies no server policy, and needs no HMAC key,
/// precisely so that a receiver can extract a token *before* it knows which
/// session and key apply.
///
/// Classification, for a packet with valid magic and no reserved flag bits set:
///
/// - `FLAG_REPLY` set — rejected; a reply is never an inbound request.
/// - `FLAG_OPEN` set — [`DecodedRequestKind::Open`], with `no_test` from
///   `FLAG_CLOSE`. **Open takes precedence over Close and over an echo-shaped
///   body**: an `OPEN | CLOSE` packet is a no-test open, and bytes that look
///   like a token and sequence number are parameter data.
/// - otherwise `FLAG_CLOSE` set — [`DecodedRequestKind::Close`].
/// - otherwise — [`DecodedRequestKind::Echo`].
///
/// `FLAG_HMAC` is orthogonal to all of the above and is reported as
/// [`DecodedRequest::hmac_present`], which indicates field presence rather than
/// valid authentication.
///
/// Open parameters are returned as encoded bytes rather than a decoded
/// [`Params`], so that an authenticating receiver can verify a packet before
/// spending effort on untrusted parameter data. A packet with malformed
/// parameters decodes structurally and fails later at [`Params::decode`].
///
/// Only structural minimum lengths are enforced — 4/12/16 bytes for
/// Open/Close/Echo, plus 16 with `FLAG_HMAC`. Trailing bytes beyond a kind's
/// fields are tolerated, no negotiated length ceiling is applied, and a zero
/// token is accepted; discarding stale, unknown, or oversized requests is the
/// receiving layer's policy.
///
/// # Errors
///
/// Returns [`ProtoError::PacketTooShort`], [`ProtoError::BadMagic`],
/// [`ProtoError::ReservedFlags`], or [`ProtoError::UnexpectedFlag`] for
/// `FLAG_REPLY`.
pub fn decode_request(packet: &[u8]) -> Result<DecodedRequest<'_>> {
    let envelope = envelope::decode_structural(packet)?;
    if has(envelope.flags, FLAG_REPLY) {
        return Err(ProtoError::UnexpectedFlag(FLAG_REPLY));
    }

    let body = envelope.body_offset;
    let kind = if has(envelope.flags, FLAG_OPEN) {
        // Open wins over Close and over an echo-shaped body.
        require_len(packet, body)?;
        DecodedRequestKind::Open {
            no_test: has(envelope.flags, FLAG_CLOSE),
            params: &packet[body..],
        }
    } else if has(envelope.flags, FLAG_CLOSE) {
        require_len(packet, body + TOKEN_SIZE)?;
        DecodedRequestKind::Close {
            token: read_token(packet, body),
        }
    } else {
        let tail_offset = body + TOKEN_SIZE + SEQ_SIZE;
        require_len(packet, tail_offset)?;
        DecodedRequestKind::Echo {
            token: read_token(packet, body),
            sequence: read_sequence(packet, body + TOKEN_SIZE),
            tail: &packet[tail_offset..],
        }
    };

    Ok(DecodedRequest {
        hmac_present: envelope.hmac_present,
        kind,
    })
}

fn encode_open(params: &Params, no_test: bool, hmac_key: Option<&[u8]>) -> Result<Vec<u8>> {
    let flags = if no_test {
        FLAG_OPEN | FLAG_CLOSE
    } else {
        FLAG_OPEN
    };
    let encoded = params.encode();
    let mut out = envelope::begin(
        flags,
        hmac_key,
        PacketLayout::open_request(hmac_key.is_some()).header_len() + encoded.len(),
    )?;
    out.extend_from_slice(&encoded);
    envelope::finish(out, hmac_key)
}

fn encode_close(token: u64, hmac_key: Option<&[u8]>) -> Result<Vec<u8>> {
    let mut out = envelope::begin(
        FLAG_CLOSE,
        hmac_key,
        PacketLayout::close_request(hmac_key.is_some()).header_len(),
    )?;
    out.extend_from_slice(&token.to_le_bytes());
    envelope::finish(out, hmac_key)
}

fn encode_echo(
    token: u64,
    sequence: u32,
    params: &Params,
    payload: &[u8],
    hmac_key: Option<&[u8]>,
) -> Result<Vec<u8>> {
    let layout = PacketLayout::echo(hmac_key.is_some(), params);
    let len = echo_packet_len(hmac_key.is_some(), params)?;
    let payload_offset = layout.header_len();
    let available_payload_len = len.saturating_sub(payload_offset);
    if payload.len() > available_payload_len {
        return Err(ProtoError::PayloadTooLarge {
            available: available_payload_len,
            provided: payload.len(),
        });
    }

    let mut out = envelope::begin(0, hmac_key, len)?;
    out.extend_from_slice(&token.to_le_bytes());
    out.extend_from_slice(&sequence.to_le_bytes());
    // Everything from here to the negotiated length is zeroed: the reply-field
    // positions the sender does not fill in, then the payload region.
    out.resize(len, 0);
    out[payload_offset..payload_offset + payload.len()].copy_from_slice(payload);

    envelope::finish(out, hmac_key)
}

fn require_len(packet: &[u8], needed: usize) -> Result<()> {
    if packet.len() < needed {
        return Err(ProtoError::PacketTooShort {
            needed,
            actual: packet.len(),
        });
    }
    Ok(())
}

fn read_token(packet: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(packet[offset..offset + TOKEN_SIZE].try_into().unwrap())
}

fn read_sequence(packet: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(packet[offset..offset + SEQ_SIZE].try_into().unwrap())
}
