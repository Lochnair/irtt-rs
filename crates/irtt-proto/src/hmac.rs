use hmac::{Hmac, KeyInit, Mac};
use md5::Md5;
use subtle::ConstantTimeEq;

use crate::{envelope, flags::FLAG_HMAC, ProtoError, Result, HEADER_SIZE, HMAC_SIZE};

// HMAC-MD5 is used for compatibility with the IRTT wire protocol, not as a
// new cryptographic recommendation. Verification uses HMAC with constant-time
// comparison; do not swap digests unless protocol negotiation supports it.
type HmacMd5 = Hmac<Md5>;

pub fn compute_hmac(key: &[u8], packet: &[u8], hmac_offset: usize) -> Result<[u8; HMAC_SIZE]> {
    if packet.len().saturating_sub(hmac_offset) < HMAC_SIZE {
        return Err(ProtoError::InvalidHmacOffset);
    }
    let hmac_end = hmac_offset + HMAC_SIZE;
    let zero_hmac = [0u8; HMAC_SIZE];
    let mut mac = HmacMd5::new_from_slice(key).expect("HMAC accepts keys of any size");
    mac.update(&packet[..hmac_offset]);
    mac.update(&zero_hmac);
    mac.update(&packet[hmac_end..]);
    let bytes = mac.finalize().into_bytes();
    let mut out = [0u8; HMAC_SIZE];
    out.copy_from_slice(&bytes);
    Ok(out)
}

pub fn compute_hmac_in_place(key: &[u8], packet: &mut [u8], hmac_offset: usize) -> Result<()> {
    if packet.len().saturating_sub(hmac_offset) < HMAC_SIZE {
        return Err(ProtoError::InvalidHmacOffset);
    }
    let digest = compute_hmac(key, packet, hmac_offset)?;
    packet[hmac_offset..hmac_offset + HMAC_SIZE].copy_from_slice(&digest);
    Ok(())
}

/// Verifies an HMAC field at an explicit offset.
///
/// This is the low-level primitive, kept for hand-built and captured wire
/// vectors that name their own field position. Code handling real packets
/// should use [`verify_packet_hmac`], which locates the field itself and
/// rejects a packet that does not carry `FLAG_HMAC`.
pub fn verify_hmac(key: &[u8], packet: &[u8], hmac_offset: usize) -> Result<()> {
    if packet.len().saturating_sub(hmac_offset) < HMAC_SIZE {
        return Err(ProtoError::InvalidHmacOffset);
    }
    let expected = compute_hmac(key, packet, hmac_offset)?;
    let actual = &packet[hmac_offset..hmac_offset + HMAC_SIZE];
    if expected.as_slice().ct_eq(actual).into() {
        Ok(())
    } else {
        Err(ProtoError::BadHmac)
    }
}

/// Verifies the authentication field of a whole IRTT packet.
///
/// The field is the standard 16 bytes immediately after the 4-byte header, so
/// callers never compute an offset themselves. This is cryptographic
/// verification only: it says nothing about which key *should* apply to the
/// packet, nor about session state. Choosing the key is the caller's policy.
///
/// # Errors
///
/// Returns [`ProtoError::PacketTooShort`], [`ProtoError::BadMagic`] or
/// [`ProtoError::ReservedFlags`] for a structurally invalid header,
/// [`ProtoError::MissingFlag`] when the packet does not carry `FLAG_HMAC`,
/// [`ProtoError::InvalidHmacOffset`] when the field is truncated, and
/// [`ProtoError::BadHmac`] when the MAC does not match. The packet is never
/// modified.
///
/// # Example
///
/// ```
/// use irtt_proto::{encode_request, verify_packet_hmac, Params, RequestToEncode};
///
/// // Synthetic example key — use a real shared secret in production.
/// let key = b"example key";
/// let params = Params::with_protocol_defaults();
/// let packet = encode_request(
///     RequestToEncode::Open { params: &params, no_test: false },
///     Some(key),
/// ).unwrap();
///
/// verify_packet_hmac(key, &packet).unwrap();
/// assert!(verify_packet_hmac(b"wrong key", &packet).is_err());
/// ```
pub fn verify_packet_hmac(key: &[u8], packet: &[u8]) -> Result<()> {
    let envelope = envelope::decode_structural(packet)?;
    if !envelope.hmac_present {
        return Err(ProtoError::MissingFlag(FLAG_HMAC));
    }
    verify_hmac(key, packet, hmac_offset())
}

pub(crate) fn hmac_offset() -> usize {
    HEADER_SIZE
}
