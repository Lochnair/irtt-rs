use hmac::{Hmac, Mac};
use md5::Md5;
use subtle::ConstantTimeEq;

use crate::{ProtoError, Result, HEADER_SIZE, HMAC_SIZE};

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

pub(crate) fn hmac_offset() -> usize {
    HEADER_SIZE
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        flags::{FLAG_HMAC, FLAG_OPEN},
        MAGIC,
    };

    fn reference_copy_zero_hmac(
        key: &[u8],
        packet: &[u8],
        hmac_offset: usize,
    ) -> Result<[u8; HMAC_SIZE]> {
        if packet.len().saturating_sub(hmac_offset) < HMAC_SIZE {
            return Err(ProtoError::InvalidHmacOffset);
        }
        let mut copy = packet.to_vec();
        copy[hmac_offset..hmac_offset + HMAC_SIZE].fill(0);
        let mut mac = HmacMd5::new_from_slice(key).expect("HMAC accepts keys of any size");
        mac.update(&copy);
        let bytes = mac.finalize().into_bytes();
        let mut out = [0u8; HMAC_SIZE];
        out.copy_from_slice(&bytes);
        Ok(out)
    }

    fn packet_with_hmac_field(hmac_bytes: [u8; HMAC_SIZE]) -> Vec<u8> {
        let mut packet = Vec::new();
        packet.extend_from_slice(&MAGIC);
        packet.push(FLAG_OPEN | FLAG_HMAC);
        packet.extend_from_slice(&hmac_bytes);
        packet.extend_from_slice(&[1, 2, 3, 4]);
        packet
    }

    #[test]
    fn compute_and_verify_hmac() {
        let mut packet = packet_with_hmac_field([0; HMAC_SIZE]);

        compute_hmac_in_place(b"testkey", &mut packet, hmac_offset()).unwrap();
        verify_hmac(b"testkey", &packet, hmac_offset()).unwrap();
        assert_eq!(
            verify_hmac(b"wrong", &packet, hmac_offset()),
            Err(ProtoError::BadHmac)
        );
    }

    #[test]
    fn compute_hmac_does_not_mutate_input() {
        let packet = packet_with_hmac_field([0xa5; HMAC_SIZE]);
        let original = packet.clone();

        let _digest = compute_hmac(b"testkey", &packet, hmac_offset()).unwrap();

        assert_eq!(packet, original);
    }

    #[test]
    fn compute_hmac_matches_copy_and_zero_semantics() {
        let mut packet = packet_with_hmac_field([0xa5; HMAC_SIZE]);
        packet.extend_from_slice(&[5, 6, 7, 8, 9]);

        let digest = compute_hmac(b"testkey", &packet, hmac_offset()).unwrap();
        let reference = reference_copy_zero_hmac(b"testkey", &packet, hmac_offset()).unwrap();

        assert_eq!(digest, reference);
    }

    #[test]
    fn compute_hmac_in_place_does_not_set_hmac_flag() {
        let mut packet = Vec::new();
        packet.extend_from_slice(&MAGIC);
        packet.push(FLAG_OPEN);
        packet.extend_from_slice(&[0; HMAC_SIZE]);
        packet.extend_from_slice(&[1, 2, 3, 4]);

        compute_hmac_in_place(b"testkey", &mut packet, hmac_offset()).unwrap();

        assert_eq!(packet[3], FLAG_OPEN);
        assert!(packet[hmac_offset()..hmac_offset() + HMAC_SIZE]
            .iter()
            .any(|byte| *byte != 0));
        assert_eq!(&packet[hmac_offset() + HMAC_SIZE..], &[1, 2, 3, 4]);
    }

    #[test]
    fn compute_hmac_in_place_only_writes_hmac_field() {
        let mut packet = packet_with_hmac_field([0xa5; HMAC_SIZE]);
        packet.extend_from_slice(&[5, 6, 7, 8, 9]);
        let original = packet.clone();

        compute_hmac_in_place(b"testkey", &mut packet, hmac_offset()).unwrap();

        assert_eq!(&packet[..hmac_offset()], &original[..hmac_offset()]);
        assert_ne!(
            &packet[hmac_offset()..hmac_offset() + HMAC_SIZE],
            &original[hmac_offset()..hmac_offset() + HMAC_SIZE]
        );
        assert_eq!(
            &packet[hmac_offset() + HMAC_SIZE..],
            &original[hmac_offset() + HMAC_SIZE..]
        );
    }

    #[test]
    fn verify_hmac_rejects_modified_authenticated_byte() {
        let mut packet = packet_with_hmac_field([0; HMAC_SIZE]);
        compute_hmac_in_place(b"testkey", &mut packet, hmac_offset()).unwrap();

        verify_hmac(b"testkey", &packet, hmac_offset()).unwrap();
        packet[hmac_offset() + HMAC_SIZE + 1] ^= 0x01;

        assert_eq!(
            verify_hmac(b"testkey", &packet, hmac_offset()),
            Err(ProtoError::BadHmac)
        );
    }
}
