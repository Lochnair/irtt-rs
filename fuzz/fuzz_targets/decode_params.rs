#![no_main]

use irtt_proto::Params;
use libfuzzer_sys::fuzz_target;

/// See `decode_request.rs` for the rationale behind this bound.
const MAX_INPUT_LEN: usize = 128 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_LEN {
        return;
    }

    let decoded = Params::decode(data);
    let decoded_with_presence = Params::decode_with_presence(data);

    // Both entry points share one parser, so when both succeed they must
    // agree on the decoded values.
    match (&decoded, &decoded_with_presence) {
        (Ok(params), Ok(with_presence)) => {
            assert_eq!(params, &with_presence.params);
        }
        (Ok(_), Err(_)) | (Err(_), Ok(_)) => {
            panic!("decode and decode_with_presence disagreed on success for {data:02x?}");
        }
        (Err(_), Err(_)) => {}
    }

    // No panic is the primary invariant for malformed input; unknown tags are
    // tolerated and repeated known tags are last-value-wins by design, so we
    // do not assert a particular failure mode here.
    if let Ok(params) = decoded {
        let encoded = params.encode();
        // Re-encoding and decoding again must preserve the semantic value.
        // Presence information does not round-trip through `encode` for
        // absent/default fields, so we compare `Params` values, not the
        // original wire bytes.
        if let Ok(reencoded_params) = Params::decode(&encoded) {
            assert_eq!(params, reencoded_params);
        } else {
            panic!(
                "re-encoding a successfully decoded Params produced bytes that failed to decode"
            );
        }
    }
});
