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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uvarint_round_trips() {
        for value in [
            0,
            1,
            2,
            127,
            128,
            255,
            16_384,
            u32::MAX as u64,
            i64::MAX as u64,
            u64::MAX,
        ] {
            let mut encoded = Vec::new();
            encode_uvarint(value, &mut encoded);
            assert_eq!(decode_uvarint(&encoded), Ok((value, encoded.len())));
        }
    }

    #[test]
    fn signed_varint_round_trips() {
        for value in [
            i64::MIN,
            -9_223_372_036,
            -2,
            -1,
            0,
            1,
            2,
            1_000_000_000,
            3_000_000_000,
            i64::MAX,
        ] {
            let mut encoded = Vec::new();
            encode_varint(value, &mut encoded);
            assert_eq!(decode_varint(&encoded), Ok((value, encoded.len())));
        }
    }

    #[test]
    fn verified_byte_examples() {
        let cases: &[(i64, &[u8])] = &[
            (1, &[0x02]),
            (3_000_000_000, &[0x80, 0xf8, 0x82, 0xad, 0x16]),
            (1_000_000_000, &[0x80, 0xa8, 0xd6, 0xb9, 0x07]),
            (1472, &[0x80, 0x17]),
            (3, &[0x06]),
            (184, &[0xf0, 0x02]),
        ];
        for (value, expected) in cases {
            let mut encoded = Vec::new();
            encode_varint(*value, &mut encoded);
            assert_eq!(&encoded, expected);
            assert_eq!(decode_varint(expected), Ok((*value, expected.len())));
        }

        let mut tag = Vec::new();
        encode_uvarint(9, &mut tag);
        assert_eq!(tag, [0x09]);
        tag.clear();
        encode_uvarint(24, &mut tag);
        assert_eq!(tag, [0x18]);
    }
}
