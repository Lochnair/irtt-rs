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
