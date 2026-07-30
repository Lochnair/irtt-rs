use crate::{
    flags::{self, has, FLAG_HMAC},
    hmac, validate_header, write_header, ProtoError, Result, HEADER_SIZE, HMAC_SIZE,
};

#[derive(Debug, Clone, Copy)]
pub(crate) enum FlagRule {
    Require(u8),
    Reject(u8),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Envelope {
    pub(crate) flags: u8,
    pub(crate) body_offset: usize,
}

pub(crate) fn decode(
    packet: &[u8],
    hmac_key: Option<&[u8]>,
    rules: &[FlagRule],
) -> Result<Envelope> {
    let flags = validate_header(packet)?;
    validate_packet_type(flags, rules)?;
    check_hmac_presence(flags, hmac_key)?;

    Ok(Envelope {
        flags,
        body_offset: HEADER_SIZE + if has(flags, FLAG_HMAC) { HMAC_SIZE } else { 0 },
    })
}

pub(crate) fn begin(
    flags: u8,
    hmac_key: Option<&[u8]>,
    rules: &[FlagRule],
    capacity: usize,
) -> Result<Vec<u8>> {
    // The encoder key is authoritative: authenticated encoders set FLAG_HMAC,
    // while unauthenticated encoders clear any caller-supplied FLAG_HMAC.
    let flags = with_hmac_flag(flags, hmac_key.is_some());
    flags::validate_flags(flags)?;
    validate_packet_type(flags, rules)?;

    let minimum_capacity = HEADER_SIZE + if hmac_key.is_some() { HMAC_SIZE } else { 0 };
    let mut out = Vec::with_capacity(capacity.max(minimum_capacity));
    write_header(&mut out, flags);
    if hmac_key.is_some() {
        out.extend_from_slice(&[0; HMAC_SIZE]);
    }
    Ok(out)
}

pub(crate) fn verify(packet: &[u8], hmac_key: Option<&[u8]>) -> Result<()> {
    if let Some(key) = hmac_key {
        hmac::verify_hmac(key, packet, hmac::hmac_offset())?;
    }
    Ok(())
}

pub(crate) fn finish(mut packet: Vec<u8>, hmac_key: Option<&[u8]>) -> Result<Vec<u8>> {
    if let Some(key) = hmac_key {
        hmac::compute_hmac_in_place(key, &mut packet, hmac::hmac_offset())?;
    }
    Ok(packet)
}

fn validate_packet_type(flags: u8, rules: &[FlagRule]) -> Result<()> {
    for rule in rules {
        match *rule {
            FlagRule::Require(flag) if !has(flags, flag) => {
                return Err(ProtoError::MissingFlag(flag));
            }
            FlagRule::Reject(flag) if has(flags, flag) => {
                return Err(ProtoError::UnexpectedFlag(flag));
            }
            FlagRule::Require(_) | FlagRule::Reject(_) => {}
        }
    }
    Ok(())
}

fn check_hmac_presence(flags: u8, hmac_key: Option<&[u8]>) -> Result<()> {
    if has(flags, FLAG_HMAC) == hmac_key.is_some() {
        Ok(())
    } else {
        Err(ProtoError::HmacPresenceMismatch)
    }
}

fn with_hmac_flag(flags: u8, authenticated: bool) -> u8 {
    if authenticated {
        flags | FLAG_HMAC
    } else {
        flags & !FLAG_HMAC
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        flags::{FLAG_OPEN, FLAG_REPLY},
        MAGIC,
    };

    const KEY: &[u8] = b"testkey";
    const OPEN_REQUEST_RULES: &[FlagRule] =
        &[FlagRule::Require(FLAG_OPEN), FlagRule::Reject(FLAG_REPLY)];

    fn header(flags: u8) -> Vec<u8> {
        let mut packet = MAGIC.to_vec();
        packet.push(flags);
        packet
    }

    #[test]
    fn decode_rejects_bad_magic_and_reserved_flags() {
        assert_eq!(
            decode(&[0, MAGIC[1], MAGIC[2], FLAG_OPEN], None, &[]),
            Err(ProtoError::BadMagic)
        );
        assert_eq!(
            decode(&header(FLAG_OPEN | 0x10), None, &[]),
            Err(ProtoError::ReservedFlags(0x10))
        );
    }

    #[test]
    fn decode_enforces_required_and_rejected_flags() {
        assert_eq!(
            decode(&header(0), None, OPEN_REQUEST_RULES),
            Err(ProtoError::MissingFlag(FLAG_OPEN))
        );
        assert_eq!(
            decode(&header(FLAG_OPEN | FLAG_REPLY), None, OPEN_REQUEST_RULES),
            Err(ProtoError::UnexpectedFlag(FLAG_REPLY))
        );
    }

    #[test]
    fn decode_rejects_hmac_presence_mismatch() {
        let mut authenticated = header(FLAG_OPEN | FLAG_HMAC);
        authenticated.extend_from_slice(&[0; HMAC_SIZE]);
        assert_eq!(
            decode(&authenticated, None, OPEN_REQUEST_RULES),
            Err(ProtoError::HmacPresenceMismatch)
        );
        assert_eq!(
            decode(&header(FLAG_OPEN), Some(KEY), OPEN_REQUEST_RULES),
            Err(ProtoError::HmacPresenceMismatch)
        );
    }

    #[test]
    fn verify_rejects_bad_hmac() {
        let mut packet = begin(FLAG_OPEN, Some(KEY), OPEN_REQUEST_RULES, 22).unwrap();
        packet.extend_from_slice(&[1, 2]);
        let mut packet = finish(packet, Some(KEY)).unwrap();
        *packet.last_mut().unwrap() ^= 0x80;

        decode(&packet, Some(KEY), OPEN_REQUEST_RULES).unwrap();
        assert_eq!(verify(&packet, Some(KEY)), Err(ProtoError::BadHmac));
    }

    #[test]
    fn encoder_key_authoritatively_normalizes_hmac_flag() {
        let plain = begin(FLAG_OPEN | FLAG_HMAC, None, OPEN_REQUEST_RULES, HEADER_SIZE).unwrap();
        assert_eq!(plain[3], FLAG_OPEN);

        let authenticated = begin(
            FLAG_OPEN,
            Some(KEY),
            OPEN_REQUEST_RULES,
            HEADER_SIZE + HMAC_SIZE,
        )
        .unwrap();
        assert_eq!(authenticated[3], FLAG_OPEN | FLAG_HMAC);
    }
}
