use crate::{
    flags::{has, FLAG_HMAC, FLAG_OPEN, FLAG_REPLY},
    hmac,
    layout::{echo_packet_len, PacketLayout},
    open::{check_hmac_presence, reject},
    params::Params,
    validate_header, write_header, ProtoError, Result, HEADER_SIZE, HMAC_SIZE, RECV_COUNT_SIZE,
    RECV_WINDOW_SIZE, SEQ_SIZE, TIMESTAMP_SIZE, TOKEN_SIZE,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EchoRequest {
    pub token: u64,
    pub sequence: u32,
    pub params: Params,
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

pub fn encode_echo_request(request: &EchoRequest, hmac_key: Option<&[u8]>) -> Result<Vec<u8>> {
    let mut flags = 0;
    if hmac_key.is_some() {
        flags |= FLAG_HMAC;
    }
    let layout = PacketLayout::echo(hmac_key.is_some(), &request.params);
    let len = echo_packet_len(hmac_key.is_some(), &request.params);
    let mut out = Vec::with_capacity(len);
    write_header(&mut out, flags);
    if hmac_key.is_some() {
        out.extend_from_slice(&[0; HMAC_SIZE]);
    }
    out.extend_from_slice(&request.token.to_le_bytes());
    out.extend_from_slice(&request.sequence.to_le_bytes());
    push_zeroed_layout_tail(layout, &mut out);
    out.resize(len, 0);
    let payload_offset = layout.header_len();
    let payload_len = out
        .len()
        .saturating_sub(payload_offset)
        .min(request.payload.len());
    out[payload_offset..payload_offset + payload_len]
        .copy_from_slice(&request.payload[..payload_len]);

    if let Some(key) = hmac_key {
        hmac::compute_hmac_in_place(key, &mut out, hmac::hmac_offset())?;
    }
    Ok(out)
}

pub fn decode_echo_reply(
    packet: &[u8],
    params: &Params,
    hmac_key: Option<&[u8]>,
) -> Result<EchoReply> {
    let flags = validate_header(packet)?;
    reject(flags, FLAG_OPEN)?;
    crate::open::require(flags, FLAG_REPLY)?;
    check_hmac_presence(flags, hmac_key)?;
    if let Some(key) = hmac_key {
        hmac::verify_hmac(key, packet, hmac::hmac_offset())?;
    }

    let layout = PacketLayout::echo(has(flags, FLAG_HMAC), params);
    let header_len = layout.header_len();
    if packet.len() < header_len {
        return Err(ProtoError::PacketTooShort {
            needed: header_len,
            actual: packet.len(),
        });
    }

    let mut pos = HEADER_SIZE;
    if layout.hmac {
        pos += HMAC_SIZE;
    }
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
        flags,
        token,
        sequence,
        recv_count,
        recv_window,
        timestamps,
        payload: packet[header_len..].to_vec(),
    })
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
    use crate::params::{Clock, ReceivedStats, StampAt};

    fn default_params() -> Params {
        Params {
            received_stats: ReceivedStats::Both,
            stamp_at: StampAt::Both,
            clock: Clock::Both,
            ..Params::default()
        }
    }

    #[test]
    fn echo_request_encodes_default_placeholders() {
        let packet = encode_echo_request(
            &EchoRequest {
                token: 0x7896_b6ab_8771_5213,
                sequence: 0,
                params: default_params(),
                payload: Vec::new(),
            },
            None,
        )
        .unwrap();
        assert_eq!(packet.len(), 60);
        assert_eq!(&packet[..4], &[0x14, 0xa7, 0x5b, 0x00]);
        assert_eq!(&packet[4..12], &0x7896_b6ab_8771_5213u64.to_le_bytes());
        assert!(packet[12..].iter().all(|byte| *byte == 0));
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
}
