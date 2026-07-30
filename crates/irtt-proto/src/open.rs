use crate::{
    envelope::{self, FlagRule},
    flags::{has, FLAG_CLOSE, FLAG_OPEN, FLAG_REPLY},
    layout::PacketLayout,
    params::Params,
    ProtoError, Result, TOKEN_SIZE,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenRequest {
    pub params: Params,
    pub close: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenReply {
    pub flags: u8,
    pub token: u64,
    pub params: Params,
}

pub fn encode_open_request(request: &OpenRequest, hmac_key: Option<&[u8]>) -> Result<Vec<u8>> {
    let mut flags = FLAG_OPEN;
    if request.close {
        flags |= FLAG_CLOSE;
    }
    let params = request.params.encode();
    let mut out = envelope::begin(
        flags,
        hmac_key,
        &[FlagRule::Require(FLAG_OPEN), FlagRule::Reject(FLAG_REPLY)],
        PacketLayout::open_request(hmac_key.is_some()).header_len() + params.len(),
    )?;
    out.extend_from_slice(&params);
    envelope::finish(out, hmac_key)
}

pub fn decode_open_request(packet: &[u8], hmac_key: Option<&[u8]>) -> Result<OpenRequest> {
    let envelope = envelope::decode(
        packet,
        hmac_key,
        &[FlagRule::Require(FLAG_OPEN), FlagRule::Reject(FLAG_REPLY)],
    )?;
    envelope::verify(packet, hmac_key)?;

    Ok(OpenRequest {
        params: Params::decode(&packet[envelope.body_offset..])?,
        close: has(envelope.flags, FLAG_CLOSE),
    })
}

pub fn encode_open_reply(reply: &OpenReply, hmac_key: Option<&[u8]>) -> Result<Vec<u8>> {
    let params = reply.params.encode();
    let mut out = envelope::begin(
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
    Ok(OpenReply {
        flags: envelope.flags,
        token,
        params: Params::decode(&packet[pos..])?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        hmac,
        params::{Clock, ReceivedStats, StampAt},
        FLAG_HMAC, HMAC_SIZE,
    };

    fn default_params() -> Params {
        Params {
            protocol_version: 1,
            duration_ns: 3_000_000_000,
            interval_ns: 1_000_000_000,
            received_stats: ReceivedStats::Both,
            stamp_at: StampAt::Both,
            clock: Clock::Both,
            ..Params::default()
        }
    }

    #[test]
    fn open_request_has_no_token() {
        let packet = encode_open_request(
            &OpenRequest {
                params: default_params(),
                close: false,
            },
            None,
        )
        .unwrap();

        assert_eq!(packet.len(), 24);
        assert_eq!(&packet[..4], &[0x14, 0xa7, 0x5b, 0x01]);
        assert_eq!(&packet[4..], &default_params().encode());
    }

    #[test]
    fn hmac_open_request_inserts_hmac_before_params() {
        let packet = encode_open_request(
            &OpenRequest {
                params: default_params(),
                close: false,
            },
            Some(b"testkey"),
        )
        .unwrap();

        assert_eq!(
            packet.len(),
            4 + HMAC_SIZE + default_params().encode().len()
        );
        assert_eq!(&packet[..4], &[0x14, 0xa7, 0x5b, FLAG_OPEN | FLAG_HMAC]);
        assert_eq!(&packet[4 + HMAC_SIZE..], &default_params().encode());
        hmac::verify_hmac(b"testkey", &packet, hmac::hmac_offset()).unwrap();
    }

    #[test]
    fn open_reply_decodes_token() {
        let mut packet = vec![0x14, 0xa7, 0x5b, 0x03];
        packet.extend_from_slice(&0x7896_b6ab_8771_5213u64.to_le_bytes());
        packet.extend_from_slice(&default_params().encode());

        let reply = decode_open_reply(&packet, None).unwrap();
        assert_eq!(reply.token, 0x7896_b6ab_8771_5213);
        assert_eq!(reply.params, default_params());
    }

    #[test]
    fn open_reply_preserves_zero_token_for_authenticated_session_validation() {
        let mut packet = vec![0x14, 0xa7, 0x5b, FLAG_OPEN | FLAG_REPLY];
        packet.extend_from_slice(&0_u64.to_le_bytes());
        packet.extend_from_slice(&default_params().encode());

        let reply = decode_open_reply(&packet, None).unwrap();
        assert_eq!(reply.token, 0);
    }

    #[test]
    fn hmac_open_reply_decodes_token_after_hmac_field() {
        let mut packet = vec![0x14, 0xa7, 0x5b, FLAG_OPEN | FLAG_REPLY | FLAG_HMAC];
        packet.extend_from_slice(&[0; HMAC_SIZE]);
        packet.extend_from_slice(&0x7896_b6ab_8771_5213u64.to_le_bytes());
        packet.extend_from_slice(&default_params().encode());
        hmac::compute_hmac_in_place(b"testkey", &mut packet, hmac::hmac_offset()).unwrap();

        assert_eq!(
            packet.len(),
            4 + HMAC_SIZE + TOKEN_SIZE + default_params().encode().len()
        );
        let reply = decode_open_reply(&packet, Some(b"testkey")).unwrap();
        assert_eq!(reply.token, 0x7896_b6ab_8771_5213);
        assert_eq!(reply.params, default_params());
    }
}
