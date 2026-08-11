//! Shared structural envelope handling.
//!
//! Layering, from packet bytes upwards:
//!
//! 1. [`decode_structural`] — magic, reserved flag bits, and the HMAC-dependent
//!    body offset. No key, no packet-type semantics.
//! 2. [`require_flags`] — codec-specific semantic flag rules.
//! 3. [`check_hmac_presence`] — HMAC presence *policy* derived from a key.
//! 4. [`verify`] — cryptographic verification.
//!
//! [`decode`] composes 1–3 for the reply codecs, which know the packet type
//! they expect and hold the applicable key. Request decoding uses only step 1,
//! because a server must extract a token before it knows which key applies.

use crate::{
    flags::{self, has, FLAG_HMAC},
    hmac, validate_header, write_header, ProtoError, Result, HEADER_SIZE, HMAC_SIZE,
};

#[derive(Debug, Clone, Copy)]
pub(crate) enum FlagRule {
    Require(u8),
    Reject(u8),
}

/// Structural result of parsing the fixed 4-byte protocol header.
///
/// `hmac_present` reports only that `FLAG_HMAC` was set, and therefore that the
/// packet is laid out with an authentication field before its body. It carries
/// no claim about the contents of that field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Envelope {
    pub(crate) flags: u8,
    pub(crate) hmac_present: bool,
    pub(crate) body_offset: usize,
}

/// The single source of truth for protocol header structure: minimum length,
/// magic, reserved flag bits, and the HMAC-dependent body offset.
pub(crate) fn decode_structural(packet: &[u8]) -> Result<Envelope> {
    let flags = validate_header(packet)?;
    let hmac_present = has(flags, FLAG_HMAC);
    Ok(Envelope {
        flags,
        hmac_present,
        body_offset: HEADER_SIZE + if hmac_present { HMAC_SIZE } else { 0 },
    })
}

/// Structural decode plus the packet-type and HMAC-presence checks a codec that
/// already knows both the expected packet type and the applicable key performs.
pub(crate) fn decode(
    packet: &[u8],
    hmac_key: Option<&[u8]>,
    rules: &[FlagRule],
) -> Result<Envelope> {
    let envelope = decode_structural(packet)?;
    require_flags(envelope.flags, rules)?;
    check_hmac_presence(envelope.flags, hmac_key)?;
    Ok(envelope)
}

/// Begins a packet whose flags the encoder derived itself, so no packet-type
/// rule can fail. The key is authoritative for `FLAG_HMAC`.
pub(crate) fn begin(flags: u8, hmac_key: Option<&[u8]>, capacity: usize) -> Result<Vec<u8>> {
    begin_checked(flags, hmac_key, &[], capacity)
}

/// Begins a packet from caller-supplied flags, which must satisfy the codec's
/// packet-type rules.
pub(crate) fn begin_checked(
    flags: u8,
    hmac_key: Option<&[u8]>,
    rules: &[FlagRule],
    capacity: usize,
) -> Result<Vec<u8>> {
    // The encoder key is authoritative: authenticated encoders set FLAG_HMAC,
    // while unauthenticated encoders clear any caller-supplied FLAG_HMAC.
    let flags = with_hmac_flag(flags, hmac_key.is_some());
    flags::validate_flags(flags)?;
    require_flags(flags, rules)?;

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

pub(crate) fn require_flags(flags: u8, rules: &[FlagRule]) -> Result<()> {
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

/// Checks structural HMAC presence against the caller's expectation. This is
/// policy, not authentication: it only compares `FLAG_HMAC` with whether a key
/// was supplied.
pub(crate) fn check_hmac_presence(flags: u8, hmac_key: Option<&[u8]>) -> Result<()> {
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
    fn structural_decode_reports_hmac_presence_without_a_key() {
        let mut authenticated = header(FLAG_OPEN | FLAG_HMAC);
        authenticated.extend_from_slice(&[0xff; HMAC_SIZE]);

        let envelope = decode_structural(&authenticated).unwrap();
        assert!(envelope.hmac_present);
        assert_eq!(envelope.body_offset, HEADER_SIZE + HMAC_SIZE);

        let envelope = decode_structural(&header(FLAG_OPEN)).unwrap();
        assert!(!envelope.hmac_present);
        assert_eq!(envelope.body_offset, HEADER_SIZE);
    }

    #[test]
    fn verify_rejects_bad_hmac() {
        let mut packet = begin_checked(FLAG_OPEN, Some(KEY), OPEN_REQUEST_RULES, 22).unwrap();
        packet.extend_from_slice(&[1, 2]);
        let mut packet = finish(packet, Some(KEY)).unwrap();
        *packet.last_mut().unwrap() ^= 0x80;

        decode(&packet, Some(KEY), OPEN_REQUEST_RULES).unwrap();
        assert_eq!(verify(&packet, Some(KEY)), Err(ProtoError::BadHmac));
    }

    #[test]
    fn encoder_key_authoritatively_normalizes_hmac_flag() {
        let plain =
            begin_checked(FLAG_OPEN | FLAG_HMAC, None, OPEN_REQUEST_RULES, HEADER_SIZE).unwrap();
        assert_eq!(plain[3], FLAG_OPEN);

        let authenticated = begin_checked(
            FLAG_OPEN,
            Some(KEY),
            OPEN_REQUEST_RULES,
            HEADER_SIZE + HMAC_SIZE,
        )
        .unwrap();
        assert_eq!(authenticated[3], FLAG_OPEN | FLAG_HMAC);
    }
}
