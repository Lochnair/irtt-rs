//! Variable-length integer encoding for IRTT wire values.
//!
//! Tags use an unsigned base-128 varint ([`encode_uvarint`]), and signed
//! integers (such as parameter values) use the same encoding after a zigzag
//! transform ([`encode_varint`]), which keeps small negative numbers compact.
//!
//! # Example
//!
//! ```
//! use irtt_proto::varint::{decode_varint, encode_varint};
//!
//! let mut out = Vec::new();
//! encode_varint(1_000_000_000, &mut out);
//! assert_eq!(decode_varint(&out).unwrap(), (1_000_000_000, out.len()));
//! ```

use crate::{ProtoError, Result};

pub fn encode_uvarint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

pub fn decode_uvarint(input: &[u8]) -> Result<(u64, usize)> {
    let mut value = 0u64;
    for (idx, byte) in input.iter().copied().enumerate() {
        if idx == 10 {
            return Err(ProtoError::VarintOverflow);
        }
        let low = u64::from(byte & 0x7f);
        let shift = idx * 7;
        if shift == 63 && low > 1 {
            return Err(ProtoError::VarintOverflow);
        }
        value |= low << shift;
        if byte < 0x80 {
            return Ok((value, idx + 1));
        }
    }
    Err(ProtoError::TruncatedVarint)
}

pub fn zigzag_encode(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

pub fn zigzag_decode(value: u64) -> i64 {
    ((value >> 1) as i64) ^ (-((value & 1) as i64))
}

pub fn encode_varint(value: i64, out: &mut Vec<u8>) {
    encode_uvarint(zigzag_encode(value), out);
}

pub fn decode_varint(input: &[u8]) -> Result<(i64, usize)> {
    let (value, used) = decode_uvarint(input)?;
    Ok((zigzag_decode(value), used))
}
