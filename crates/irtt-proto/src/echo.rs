use crate::{
    envelope::{self, FlagRule},
    flags::{FLAG_CLOSE, FLAG_OPEN, FLAG_REPLY},
    layout::{echo_packet_len, PacketLayout},
    params::Params,
    ProtoError, Result, RECV_COUNT_SIZE, RECV_WINDOW_SIZE, SEQ_SIZE, TIMESTAMP_SIZE, TOKEN_SIZE,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EchoRequest {
    pub token: u64,
    pub sequence: u32,
    pub payload: Vec<u8>,
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

pub fn encode_echo_request(
    request: &EchoRequest,
    params: &Params,
    hmac_key: Option<&[u8]>,
) -> Result<Vec<u8>> {
    let layout = PacketLayout::echo(hmac_key.is_some(), params);
    let len = echo_packet_len(hmac_key.is_some(), params)?;
    let payload_offset = layout.header_len();
    let available_payload_len = len.saturating_sub(payload_offset);
    if request.payload.len() > available_payload_len {
        return Err(ProtoError::PayloadTooLarge {
            available: available_payload_len,
            provided: request.payload.len(),
        });
    }
    let mut out = envelope::begin(
        0,
        hmac_key,
        &[
            FlagRule::Reject(FLAG_OPEN),
            FlagRule::Reject(FLAG_REPLY),
            FlagRule::Reject(FLAG_CLOSE),
        ],
        len,
    )?;
    out.extend_from_slice(&request.token.to_le_bytes());
    out.extend_from_slice(&request.sequence.to_le_bytes());
    push_zeroed_layout_tail(layout, &mut out);
    out.resize(len, 0);
    out[payload_offset..payload_offset + request.payload.len()].copy_from_slice(&request.payload);

    envelope::finish(out, hmac_key)
}

pub fn decode_echo_request(
    packet: &[u8],
    params: &Params,
    hmac_key: Option<&[u8]>,
) -> Result<EchoRequest> {
    let envelope = envelope::decode(
        packet,
        hmac_key,
        &[
            FlagRule::Reject(FLAG_OPEN),
            FlagRule::Reject(FLAG_REPLY),
            FlagRule::Reject(FLAG_CLOSE),
        ],
    )?;
    let layout = PacketLayout::echo(hmac_key.is_some(), params);
    validate_echo_length(packet, params, layout)?;
    envelope::verify(packet, hmac_key)?;

    let mut pos = envelope.body_offset;
    let token = read_u64(packet, &mut pos);
    let sequence = read_u32(packet, &mut pos);

    Ok(EchoRequest {
        token,
        sequence,
        payload: packet[layout.header_len()..].to_vec(),
    })
}

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

    let mut out = envelope::begin(
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
    validate_echo_length(packet, params, layout)?;
    envelope::verify(packet, hmac_key)?;

    let header_len = layout.header_len();
    let mut pos = envelope.body_offset;
    let token = read_u64(packet, &mut pos);
    let sequence = read_u32(packet, &mut pos);
    let recv_count = layout.recv_count.then(|| read_u32(packet, &mut pos));
    let recv_window = layout.recv_window.then(|| read_u64(packet, &mut pos));
    let timestamps = TimestampFields {
        recv_wall: layout.recv_wall.then(|| read_i64(packet, &mut pos)),
        recv_mono: layout.recv_mono.then(|| read_i64(packet, &mut pos)),
        midpoint_wall: layout.midpoint_wall.then(|| read_i64(packet, &mut pos)),
        midpoint_mono: layout.midpoint_mono.then(|| read_i64(packet, &mut pos)),
        send_wall: layout.send_wall.then(|| read_i64(packet, &mut pos)),
        send_mono: layout.send_mono.then(|| read_i64(packet, &mut pos)),
    };

    Ok(EchoReply {
        flags: envelope.flags,
        token,
        sequence,
        recv_count,
        recv_window,
        timestamps,
        payload: packet[header_len..].to_vec(),
    })
}

fn validate_echo_length(packet: &[u8], params: &Params, layout: PacketLayout) -> Result<()> {
    let header_len = layout.header_len();
    if packet.len() < header_len {
        return Err(ProtoError::PacketTooShort {
            needed: header_len,
            actual: packet.len(),
        });
    }
    let expected_len = echo_packet_len(layout.hmac, params)?;
    if packet.len() != expected_len {
        return Err(ProtoError::PacketLengthMismatch {
            expected: expected_len,
            actual: packet.len(),
        });
    }
    Ok(())
}

fn push_zeroed_layout_tail(layout: PacketLayout, out: &mut Vec<u8>) {
    if layout.recv_count {
        out.extend_from_slice(&[0; RECV_COUNT_SIZE]);
    }
    if layout.recv_window {
        out.extend_from_slice(&[0; RECV_WINDOW_SIZE]);
    }
    for _ in 0..layout.timestamp_count() {
        out.extend_from_slice(&[0; TIMESTAMP_SIZE]);
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        hmac,
        params::{Clock, ReceivedStats, StampAt},
        write_header, FLAG_HMAC, HMAC_SIZE,
    };

    fn default_params() -> Params {
        Params {
            received_stats: ReceivedStats::Both,
            stamp_at: StampAt::Both,
            clock: Clock::Both,
            ..Params::default()
        }
    }

    fn params_with_payload_space(payload_space: usize) -> Params {
        let header_len = PacketLayout::echo(false, &Params::default()).header_len();
        Params {
            length: (header_len + payload_space) as i64,
            ..Params::default()
        }
    }

    fn echo_request_with_payload(payload: Vec<u8>) -> EchoRequest {
        EchoRequest {
            token: 0x7896_b6ab_8771_5213,
            sequence: 9,
            payload,
        }
    }

    #[test]
    fn echo_request_encodes_default_placeholders() {
        let params = default_params();
        let packet = encode_echo_request(
            &EchoRequest {
                token: 0x7896_b6ab_8771_5213,
                sequence: 0,
                payload: Vec::new(),
            },
            &params,
            None,
        )
        .unwrap();
        assert_eq!(packet.len(), 60);
        assert_eq!(&packet[..4], &[0x14, 0xa7, 0x5b, 0x00]);
        assert_eq!(&packet[4..12], &0x7896_b6ab_8771_5213u64.to_le_bytes());
        assert!(packet[12..].iter().all(|byte| *byte == 0));
    }

    #[test]
    fn echo_request_encodes_exact_fit_payload() {
        let params = params_with_payload_space(4);
        let request = echo_request_with_payload(vec![1, 2, 3, 4]);
        let packet = encode_echo_request(&request, &params, None).unwrap();
        let payload_offset = PacketLayout::echo(false, &params).header_len();

        assert_eq!(&packet[payload_offset..], &[1, 2, 3, 4]);
    }

    #[test]
    fn echo_request_encodes_shorter_payload_and_zero_fills_remainder() {
        let params = params_with_payload_space(4);
        let request = echo_request_with_payload(vec![1, 2]);
        let packet = encode_echo_request(&request, &params, None).unwrap();
        let payload_offset = PacketLayout::echo(false, &params).header_len();

        assert_eq!(&packet[payload_offset..], &[1, 2, 0, 0]);
    }

    #[test]
    fn echo_request_rejects_oversized_payload() {
        let params = params_with_payload_space(4);
        let request = echo_request_with_payload(vec![1, 2, 3, 4, 5]);
        let original = request.clone();

        assert_eq!(
            encode_echo_request(&request, &params, None),
            Err(ProtoError::PayloadTooLarge {
                available: 4,
                provided: 5,
            })
        );
        assert_eq!(request, original);
    }

    #[test]
    fn echo_request_rejects_negative_requested_length() {
        let request = echo_request_with_payload(Vec::new());
        let params = Params {
            length: -1,
            ..Params::default()
        };

        assert_eq!(
            encode_echo_request(&request, &params, None),
            Err(ProtoError::NegativePacketLength { length: -1 })
        );
    }

    #[test]
    fn hmac_echo_request_places_token_and_sequence_after_hmac() {
        let params = default_params();
        let packet = encode_echo_request(
            &EchoRequest {
                token: 0x7896_b6ab_8771_5213,
                sequence: 9,
                payload: Vec::new(),
            },
            &params,
            Some(b"testkey"),
        )
        .unwrap();

        assert_eq!(packet.len(), 76);
        assert_eq!(&packet[..4], &[0x14, 0xa7, 0x5b, FLAG_HMAC]);
        assert_eq!(
            &packet[4 + HMAC_SIZE..4 + HMAC_SIZE + TOKEN_SIZE],
            &0x7896_b6ab_8771_5213u64.to_le_bytes()
        );
        assert_eq!(
            &packet[4 + HMAC_SIZE + TOKEN_SIZE..4 + HMAC_SIZE + TOKEN_SIZE + SEQ_SIZE],
            &9_u32.to_le_bytes()
        );
        hmac::verify_hmac(b"testkey", &packet, hmac::hmac_offset()).unwrap();
    }

    #[test]
    fn echo_reply_decodes_default_fields() {
        let packet = [
            0x14, 0xa7, 0x5b, 0x02, 0x13, 0x52, 0x71, 0x87, 0xab, 0xb6, 0x96, 0x78, 0x02, 0x00,
            0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0xb8, 0x1a, 0x33, 0x0c, 0x86, 0x6d, 0xaa, 0x18, 0xde, 0x26, 0x35, 0x95, 0x00, 0x00,
            0x00, 0x00, 0x80, 0x4d, 0x33, 0x0c, 0x86, 0x6d, 0xaa, 0x18, 0xb2, 0x57, 0x35, 0x95,
            0x00, 0x00, 0x00, 0x00,
        ];
        let reply = decode_echo_reply(&packet, &default_params(), None).unwrap();
        assert_eq!(reply.token, 0x7896_b6ab_8771_5213);
        assert_eq!(reply.sequence, 2);
        assert_eq!(reply.recv_count, Some(3));
        assert_eq!(reply.recv_window, Some(7));
        assert!(reply.timestamps.recv_wall.is_some());
        assert!(reply.timestamps.midpoint_wall.is_none());
    }

    #[test]
    fn echo_reply_decodes_exact_negotiated_length_with_payload() {
        let params = params_with_payload_space(4);
        let mut packet = Vec::new();
        write_header(&mut packet, FLAG_REPLY);
        packet.extend_from_slice(&0x7896_b6ab_8771_5213u64.to_le_bytes());
        packet.extend_from_slice(&2_u32.to_le_bytes());
        packet.extend_from_slice(&[1, 2, 3, 4]);

        let reply = decode_echo_reply(&packet, &params, None).unwrap();
        assert_eq!(reply.payload, vec![1, 2, 3, 4]);
    }

    #[test]
    fn echo_reply_rejects_shorter_than_negotiated_length() {
        let params = params_with_payload_space(4);
        let mut packet = Vec::new();
        write_header(&mut packet, FLAG_REPLY);
        packet.extend_from_slice(&0x7896_b6ab_8771_5213u64.to_le_bytes());
        packet.extend_from_slice(&2_u32.to_le_bytes());

        assert_eq!(
            decode_echo_reply(&packet, &params, None),
            Err(ProtoError::PacketLengthMismatch {
                expected: 20,
                actual: 16,
            })
        );
    }

    #[test]
    fn echo_reply_rejects_longer_than_negotiated_length() {
        let params = Params::default();
        let mut packet = Vec::new();
        write_header(&mut packet, FLAG_REPLY);
        packet.extend_from_slice(&0x7896_b6ab_8771_5213u64.to_le_bytes());
        packet.extend_from_slice(&2_u32.to_le_bytes());
        packet.push(0);

        assert_eq!(
            decode_echo_reply(&packet, &params, None),
            Err(ProtoError::PacketLengthMismatch {
                expected: 16,
                actual: 17,
            })
        );
    }

    #[test]
    fn hmac_echo_reply_decodes_default_fields_after_hmac() {
        let params = default_params();
        let layout = PacketLayout::echo(true, &params);
        let mut packet = Vec::with_capacity(layout.header_len());
        write_header(&mut packet, FLAG_REPLY | FLAG_HMAC);
        packet.extend_from_slice(&[0; HMAC_SIZE]);
        packet.extend_from_slice(&0x7896_b6ab_8771_5213u64.to_le_bytes());
        packet.extend_from_slice(&2_u32.to_le_bytes());
        push_zeroed_layout_tail(layout, &mut packet);
        hmac::compute_hmac_in_place(b"testkey", &mut packet, hmac::hmac_offset()).unwrap();

        assert_eq!(packet.len(), 76);
        let reply = decode_echo_reply(&packet, &params, Some(b"testkey")).unwrap();
        assert_eq!(reply.token, 0x7896_b6ab_8771_5213);
        assert_eq!(reply.sequence, 2);
        assert_eq!(reply.recv_count, Some(0));
        assert_eq!(reply.recv_window, Some(0));
        assert_eq!(reply.payload.len(), 0);
    }

    #[test]
    fn hmac_echo_reply_decodes_exact_negotiated_length_with_payload() {
        let mut params = Params {
            length: 48,
            ..Params::default()
        };
        params.received_stats = ReceivedStats::Both;
        let layout = PacketLayout::echo(true, &params);
        let mut packet = Vec::with_capacity(echo_packet_len(true, &params).unwrap());
        write_header(&mut packet, FLAG_REPLY | FLAG_HMAC);
        packet.extend_from_slice(&[0; HMAC_SIZE]);
        packet.extend_from_slice(&0x7896_b6ab_8771_5213u64.to_le_bytes());
        packet.extend_from_slice(&2_u32.to_le_bytes());
        push_zeroed_layout_tail(layout, &mut packet);
        packet.extend_from_slice(&[1, 2, 3, 4]);
        hmac::compute_hmac_in_place(b"testkey", &mut packet, hmac::hmac_offset()).unwrap();

        let reply = decode_echo_reply(&packet, &params, Some(b"testkey")).unwrap();
        assert_eq!(reply.payload, vec![1, 2, 3, 4]);
    }

    #[test]
    fn hmac_echo_reply_rejects_length_mismatch_before_hmac_verification() {
        let params = Params::default();
        let mut packet = Vec::new();
        write_header(&mut packet, FLAG_REPLY | FLAG_HMAC);
        packet.extend_from_slice(&[0; HMAC_SIZE]);
        packet.extend_from_slice(&0x7896_b6ab_8771_5213u64.to_le_bytes());
        packet.extend_from_slice(&2_u32.to_le_bytes());
        hmac::compute_hmac_in_place(b"testkey", &mut packet, hmac::hmac_offset()).unwrap();
        packet.push(0);

        assert_eq!(
            decode_echo_reply(&packet, &params, Some(b"testkey")),
            Err(ProtoError::PacketLengthMismatch {
                expected: 32,
                actual: 33,
            })
        );
    }
}
