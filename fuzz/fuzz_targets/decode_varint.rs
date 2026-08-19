#![no_main]

use irtt_proto::varint::{decode_uvarint, decode_varint, encode_uvarint, encode_varint};
use libfuzzer_sys::fuzz_target;

/// See `decode_request.rs` for the rationale behind this bound. Varint
/// grammar is trivial, but the bound is kept for consistency with the other
/// targets.
const MAX_INPUT_LEN: usize = 128 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_LEN {
        return;
    }

    if let Ok((value, used)) = decode_uvarint(data) {
        assert!(used > 0);
        assert!(used <= data.len());
        assert!(used <= 10);

        let mut encoded = Vec::new();
        encode_uvarint(value, &mut encoded);
        // Canonical re-encoding need not reproduce noncanonical fuzz input,
        // but it must decode back to the same value and consume exactly its
        // own bytes.
        assert_eq!(decode_uvarint(&encoded), Ok((value, encoded.len())));
    }

    if let Ok((value, used)) = decode_varint(data) {
        assert!(used > 0);
        assert!(used <= data.len());
        assert!(used <= 10);

        let mut encoded = Vec::new();
        encode_varint(value, &mut encoded);
        assert_eq!(decode_varint(&encoded), Ok((value, encoded.len())));
    }
});
