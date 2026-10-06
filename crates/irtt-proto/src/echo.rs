use crate::{
    envelope::{self, FlagRule},
    flags::{FLAG_OPEN, FLAG_REPLY},
    layout::{echo_packet_len, PacketLayout},
    params::{Clock, Params, StampAt},
    ProtoError, Result, SEQ_SIZE, TIMESTAMP_SIZE, TOKEN_SIZE,
};

/// Upstream irtt 0.9.1 always emits both midpoint timestamp fields (wall,
/// then monotonic) in an ECHO reply for single-clock `StampAt::Midpoint`
/// negotiations, even though only one clock was negotiated. That makes the
/// compatibility header exactly one timestamp longer than the negotiated
/// header.
const MIDPOINT_COMPAT_EXTRA: usize = TIMESTAMP_SIZE;

/// Whether the negotiation selects a midpoint timestamp from exactly one clock,
/// which is the only case the upstream dual-field compatibility form applies to.
///
/// [`Clock::Unspecified`] selects no clock and therefore lays out no midpoint
/// field at all, so it is not a single-clock negotiation.
fn is_midpoint_single_clock(params: &Params) -> bool {
    params.stamp_at == StampAt::Midpoint && matches!(params.clock, Clock::Wall | Clock::Monotonic)
}

/// Total datagram length of the upstream dual-midpoint compatibility form.
///
/// Both lengths are `max(negotiated_length, header)`, so this is *not*
/// `normal_len + TIMESTAMP_SIZE`. The extra midpoint field only lengthens the
/// datagram while the larger header is still pushing past the negotiated
/// length; beyond that it displaces payload instead, and the two forms have
/// identical length. The difference is therefore 8, 7..1, or 0 depending on
/// the negotiated length.
///
/// Deriving this from `normal_len` keeps the identity exact —
/// `max(negotiated_length, compat_header) == max(compat_header, normal_len)` —
/// and cannot overflow, because `compat_header` is a small bounded header
/// size rather than an attacker-influenced packet length.
fn midpoint_compat_packet_len(layout: PacketLayout, normal_len: usize) -> usize {
    (layout.header_len() + MIDPOINT_COMPAT_EXTRA).max(normal_len)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EchoReply {
    pub flags: u8,
    pub token: u64,
    pub sequence: u32,
    pub recv_count: Option<u32>,
    pub recv_window: Option<u64>,
    pub timestamps: TimestampFields,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TimestampFields {
    pub recv_wall: Option<i64>,
    pub recv_mono: Option<i64>,
    pub midpoint_wall: Option<i64>,
    pub midpoint_mono: Option<i64>,
    pub send_wall: Option<i64>,
    pub send_mono: Option<i64>,
}

/// Encodes an ECHO reply.
///
/// `params` supplies the negotiated layout and length, exactly as on the
/// request side, and every optional field the layout requires must be present
/// in `reply`: a `Some` where the layout has no field, or a `None` where it has
/// one, is an error. `hmac_key` is authoritative: `Some(key)` sets `FLAG_HMAC`
/// and signs the finished datagram.
///
/// # Errors
///
/// Returns [`ProtoError::MissingFlag`] or [`ProtoError::UnexpectedFlag`] when
/// the reply does not carry exactly the packet-type flags this encoder
/// requires, [`ProtoError::MissingField`] or [`ProtoError::UnexpectedField`]
/// when `reply`'s optional fields do not match the negotiated layout, and
/// [`ProtoError::PayloadTooLarge`] when the payload does not fit the negotiated
/// length.
///
/// # Example
///
/// ```
/// use irtt_proto::{
///     decode_echo_reply, encode_echo_reply, EchoReply, Params, ReceivedStats,
///     TimestampFields, FLAG_REPLY,
/// };
///
/// // `received_stats` negotiates a receive counter; four payload bytes.
/// let params = Params {
///     length: 24,
///     received_stats: ReceivedStats::Count,
///     ..Params::default()
/// };
/// let reply = EchoReply {
///     flags: FLAG_REPLY,
///     token: 0x1234_5678_9abc_def0,
///     sequence: 7,
///     recv_count: Some(3),
///     recv_window: None,
///     timestamps: TimestampFields::default(),
///     payload: vec![1, 2, 3, 4],
/// };
///
/// let packet = encode_echo_reply(&reply, &params, None).unwrap();
/// assert_eq!(decode_echo_reply(&packet, &params, None).unwrap(), reply);
/// ```
pub fn encode_echo_reply(
    reply: &EchoReply,
    params: &Params,
    hmac_key: Option<&[u8]>,
) -> Result<Vec<u8>> {
    let layout = PacketLayout::echo(hmac_key.is_some(), params);
    let len = echo_packet_len(hmac_key.is_some(), params)?;
    let payload_offset = layout.header_len();
    let available_payload_len = len.saturating_sub(payload_offset);
    if reply.payload.len() > available_payload_len {
        return Err(ProtoError::PayloadTooLarge {
            available: available_payload_len,
            provided: reply.payload.len(),
        });
    }

    let mut out = envelope::begin_checked(
        reply.flags,
        hmac_key,
        &[FlagRule::Reject(FLAG_OPEN), FlagRule::Require(FLAG_REPLY)],
        len,
    )?;
    out.extend_from_slice(&reply.token.to_le_bytes());
    out.extend_from_slice(&reply.sequence.to_le_bytes());
    push_echo_reply_tail(reply, layout, &mut out)?;
    out.resize(len, 0);
    out[payload_offset..payload_offset + reply.payload.len()].copy_from_slice(&reply.payload);

    envelope::finish(out, hmac_key)
}

/// Decodes an ECHO reply, accepting the upstream irtt 0.9.1 dual-midpoint
/// compatibility form when it is identifiable by datagram length.
///
/// For single-clock `StampAt::Midpoint` negotiations upstream emits both
/// midpoint fields (wall, then monotonic). When that larger header makes the
/// datagram longer than the negotiated length, the compatibility form is
/// unambiguous and both fields are parsed, exposing only the negotiated
/// clock's value.
///
/// When the negotiated length already covers the larger header the two forms
/// have identical length and are indistinguishable from the packet alone. The
/// reply is still accepted, parsed against the negotiated layout. For a
/// monotonic-only negotiation that means the value exposed as `midpoint_mono`
/// is whatever occupies the negotiated field position, which for an upstream
/// peer is its wall timestamp — this cannot be corrected deterministically
/// without out-of-band knowledge of the peer, so no heuristic is applied.
pub fn decode_echo_reply(
    packet: &[u8],
    params: &Params,
    hmac_key: Option<&[u8]>,
) -> Result<EchoReply> {
    let envelope = envelope::decode(
        packet,
        hmac_key,
        &[FlagRule::Reject(FLAG_OPEN), FlagRule::Require(FLAG_REPLY)],
    )?;
    let layout = PacketLayout::echo(hmac_key.is_some(), params);
    let midpoint_compat = validate_echo_length(packet, params, layout)?;
    envelope::verify(packet, hmac_key)?;

    let header_len = layout.header_len()
        + if midpoint_compat {
            MIDPOINT_COMPAT_EXTRA
        } else {
            0
        };
    let mut pos = envelope.body_offset;
    let token = read_u64(packet, &mut pos);
    let sequence = read_u32(packet, &mut pos);
    let recv_count = layout.recv_count.then(|| read_u32(packet, &mut pos));
    let recv_window = layout.recv_window.then(|| read_u64(packet, &mut pos));
    let recv_wall = layout.recv_wall.then(|| read_i64(packet, &mut pos));
    let recv_mono = layout.recv_mono.then(|| read_i64(packet, &mut pos));
    let (midpoint_wall, midpoint_mono) = if midpoint_compat {
        let wall = read_i64(packet, &mut pos);
        let mono = read_i64(packet, &mut pos);
        match params.clock {
            Clock::Wall => (Some(wall), None),
            Clock::Monotonic => (None, Some(mono)),
            Clock::Unspecified | Clock::Both => {
                unreachable!("midpoint compat only applies to single-clock negotiations")
            }
        }
    } else {
        (
            layout.midpoint_wall.then(|| read_i64(packet, &mut pos)),
            layout.midpoint_mono.then(|| read_i64(packet, &mut pos)),
        )
    };
    let send_wall = layout.send_wall.then(|| read_i64(packet, &mut pos));
    let send_mono = layout.send_mono.then(|| read_i64(packet, &mut pos));

    Ok(EchoReply {
        flags: envelope.flags,
        token,
        sequence,
        recv_count,
        recv_window,
        timestamps: TimestampFields {
            recv_wall,
            recv_mono,
            midpoint_wall,
            midpoint_mono,
            send_wall,
            send_mono,
        },
        payload: packet[header_len..].to_vec(),
    })
}

/// Validates the packet length against the negotiated layout. Returns
/// `Ok(true)` when the packet matched the upstream midpoint compatibility
/// length rather than the exact negotiated length.
///
/// The negotiated length is always checked first, so when the compatibility
/// form is not longer than the negotiated form the two are indistinguishable
/// by length and normal negotiated parsing wins. See [`decode_echo_reply`].
fn validate_echo_length(packet: &[u8], params: &Params, layout: PacketLayout) -> Result<bool> {
    let header_len = layout.header_len();
    if packet.len() < header_len {
        return Err(ProtoError::PacketTooShort {
            needed: header_len,
            actual: packet.len(),
        });
    }
    let expected_len = echo_packet_len(layout.hmac, params)?;
    if packet.len() == expected_len {
        return Ok(false);
    }
    if is_midpoint_single_clock(params) {
        let compat_len = midpoint_compat_packet_len(layout, expected_len);
        if compat_len > expected_len && packet.len() == compat_len {
            return Ok(true);
        }
    }
    Err(ProtoError::PacketLengthMismatch {
        expected: expected_len,
        actual: packet.len(),
    })
}

fn push_echo_reply_tail(reply: &EchoReply, layout: PacketLayout, out: &mut Vec<u8>) -> Result<()> {
    if let Some(value) = field_value(layout.recv_count, reply.recv_count, "recv_count")? {
        out.extend_from_slice(&value.to_le_bytes());
    }
    if let Some(value) = field_value(layout.recv_window, reply.recv_window, "recv_window")? {
        out.extend_from_slice(&value.to_le_bytes());
    }

    let timestamps = &reply.timestamps;
    for (present, value, name) in [
        (layout.recv_wall, timestamps.recv_wall, "recv_wall"),
        (layout.recv_mono, timestamps.recv_mono, "recv_mono"),
        (
            layout.midpoint_wall,
            timestamps.midpoint_wall,
            "midpoint_wall",
        ),
        (
            layout.midpoint_mono,
            timestamps.midpoint_mono,
            "midpoint_mono",
        ),
        (layout.send_wall, timestamps.send_wall, "send_wall"),
        (layout.send_mono, timestamps.send_mono, "send_mono"),
    ] {
        if let Some(value) = field_value(present, value, name)? {
            out.extend_from_slice(&value.to_le_bytes());
        }
    }
    Ok(())
}

fn field_value<T: Copy>(present: bool, value: Option<T>, name: &'static str) -> Result<Option<T>> {
    match (present, value) {
        (true, Some(value)) => Ok(Some(value)),
        (true, None) => Err(ProtoError::MissingField(name)),
        (false, Some(_)) => Err(ProtoError::UnexpectedField(name)),
        (false, None) => Ok(None),
    }
}

fn read_u32(packet: &[u8], pos: &mut usize) -> u32 {
    let value = u32::from_le_bytes(packet[*pos..*pos + SEQ_SIZE].try_into().unwrap());
    *pos += SEQ_SIZE;
    value
}

fn read_u64(packet: &[u8], pos: &mut usize) -> u64 {
    let value = u64::from_le_bytes(packet[*pos..*pos + TOKEN_SIZE].try_into().unwrap());
    *pos += TOKEN_SIZE;
    value
}

fn read_i64(packet: &[u8], pos: &mut usize) -> i64 {
    let value = i64::from_le_bytes(packet[*pos..*pos + TIMESTAMP_SIZE].try_into().unwrap());
    *pos += TIMESTAMP_SIZE;
    value
}
