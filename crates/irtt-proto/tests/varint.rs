use irtt_proto::{varint::*, ProtoError};
use proptest::prelude::*;

#[test]
fn boundary_values_round_trip() {
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
    for tag in [9, 24] {
        let mut encoded = Vec::new();
        encode_uvarint(tag, &mut encoded);
        assert_eq!(encoded, [tag as u8]);
    }
}

#[test]
fn truncated_and_overflowing_varints_are_rejected() {
    // Incomplete input differs from a tenth byte whose bits cannot fit u64,
    // or an eleventh byte after ten continuation bytes.
    for (bytes, expected) in [
        (vec![], ProtoError::TruncatedVarint),
        (vec![0x80], ProtoError::TruncatedVarint),
        (vec![0x80; 10], ProtoError::TruncatedVarint),
        (
            [vec![0x80; 9], vec![2]].concat(),
            ProtoError::VarintOverflow,
        ),
        (
            [vec![0x80; 10], vec![0]].concat(),
            ProtoError::VarintOverflow,
        ),
    ] {
        assert_eq!(decode_uvarint(&bytes), Err(expected), "{bytes:02x?}");
        assert_eq!(
            decode_varint(&bytes),
            decode_uvarint(&bytes).map(|(v, n)| (zigzag_decode(v), n))
        );
    }
}

proptest! {
    #[test]
    fn uvarint_round_trips(value: u64) {
        let mut encoded = Vec::new();
        encode_uvarint(value, &mut encoded);
        prop_assert!(!encoded.is_empty());
        prop_assert!(encoded.len() <= 10);
        prop_assert_eq!(decode_uvarint(&encoded).unwrap(), (value, encoded.len()));
        prop_assert_eq!(zigzag_encode(zigzag_decode(value)), value);
    }

    #[test]
    fn signed_varint_round_trips(value: i64) {
        let mut encoded = Vec::new();
        encode_varint(value, &mut encoded);
        prop_assert!(!encoded.is_empty());
        prop_assert!(encoded.len() <= 10);
        prop_assert_eq!(decode_varint(&encoded).unwrap(), (value, encoded.len()));
        prop_assert_eq!(zigzag_decode(zigzag_encode(value)), value);
    }
}
