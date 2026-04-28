use hmac::{Hmac, Mac};
use md5::Md5;
use subtle::ConstantTimeEq;

use crate::{flags::FLAG_HMAC, ProtoError, Result, HEADER_SIZE, HMAC_SIZE};

type HmacMd5 = Hmac<Md5>;

pub fn compute_hmac(key: &[u8], packet: &[u8], hmac_offset: usize) -> Result<[u8; HMAC_SIZE]> {
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

pub fn compute_hmac_in_place(key: &[u8], packet: &mut [u8], hmac_offset: usize) -> Result<()> {
    if packet.len().saturating_sub(hmac_offset) < HMAC_SIZE {
        return Err(ProtoError::InvalidHmacOffset);
    }
    packet[3] |= FLAG_HMAC;
    packet[hmac_offset..hmac_offset + HMAC_SIZE].fill(0);
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
    use crate::{flags::FLAG_OPEN, MAGIC};

    #[test]
    fn compute_and_verify_hmac() {
        let mut packet = Vec::new();
        packet.extend_from_slice(&MAGIC);
        packet.push(FLAG_OPEN | FLAG_HMAC);
        packet.extend_from_slice(&[0; HMAC_SIZE]);
        packet.extend_from_slice(&[1, 2, 3, 4]);

        compute_hmac_in_place(b"testkey", &mut packet, hmac_offset()).unwrap();
        verify_hmac(b"testkey", &packet, hmac_offset()).unwrap();
        assert_eq!(
            verify_hmac(b"wrong", &packet, hmac_offset()),
            Err(ProtoError::BadHmac)
        );
    }
}
