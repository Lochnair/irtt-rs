use crate::{
    flags::{has, FLAG_CLOSE, FLAG_HMAC, FLAG_OPEN, FLAG_REPLY},
    hmac,
    params::Params,
    validate_header, write_header, ProtoError, Result, HEADER_SIZE, HMAC_SIZE, TOKEN_SIZE,
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
    if hmac_key.is_some() {
        flags |= FLAG_HMAC;
    }

    let mut out = Vec::new();
    write_header(&mut out, flags);
    if hmac_key.is_some() {
        out.extend_from_slice(&[0; HMAC_SIZE]);
    }
    out.extend_from_slice(&request.params.encode());

    if let Some(key) = hmac_key {
        hmac::compute_hmac_in_place(key, &mut out, hmac::hmac_offset())?;
    }
    Ok(out)
}

pub fn decode_open_reply(packet: &[u8], hmac_key: Option<&[u8]>) -> Result<OpenReply> {
    let flags = validate_header(packet)?;
    require(flags, FLAG_OPEN)?;
    require(flags, FLAG_REPLY)?;
    check_hmac_presence(flags, hmac_key)?;
    if let Some(key) = hmac_key {
        hmac::verify_hmac(key, packet, hmac::hmac_offset())?;
    }

    let mut pos = HEADER_SIZE;
    if has(flags, FLAG_HMAC) {
        pos += HMAC_SIZE;
    }
    let needed = pos + TOKEN_SIZE;
    if packet.len() < needed {
        return Err(ProtoError::PacketTooShort {
            needed,
            actual: packet.len(),
        });
    }
    let token = u64::from_le_bytes(packet[pos..pos + TOKEN_SIZE].try_into().unwrap());
    pos += TOKEN_SIZE;
    if token == 0 && !has(flags, FLAG_CLOSE) {
        return Err(ProtoError::ZeroToken);
    }

    Ok(OpenReply {
        flags,
        token,
        params: Params::decode(&packet[pos..])?,
    })
}

pub(crate) fn require(flags: u8, flag: u8) -> Result<()> {
    if has(flags, flag) {
        Ok(())
    } else {
        Err(ProtoError::MissingFlag(flag))
    }
}

pub(crate) fn reject(flags: u8, flag: u8) -> Result<()> {
    if has(flags, flag) {
        Err(ProtoError::UnexpectedFlag(flag))
    } else {
        Ok(())
    }
}

pub(crate) fn check_hmac_presence(flags: u8, hmac_key: Option<&[u8]>) -> Result<()> {
    if has(flags, FLAG_HMAC) == hmac_key.is_some() {
        Ok(())
    } else {
        Err(ProtoError::HmacPresenceMismatch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::params::{Clock, ReceivedStats, StampAt};

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
    fn open_reply_decodes_token() {
        let mut packet = vec![0x14, 0xa7, 0x5b, 0x03];
        packet.extend_from_slice(&0x7896_b6ab_8771_5213u64.to_le_bytes());
        packet.extend_from_slice(&default_params().encode());

        let reply = decode_open_reply(&packet, None).unwrap();
        assert_eq!(reply.token, 0x7896_b6ab_8771_5213);
        assert_eq!(reply.params, default_params());
    }
}
