#![no_main]

use irtt_proto::{decode_request, verify_packet_hmac, DecodedRequestKind, Params};
use libfuzzer_sys::fuzz_target;

/// Keeps individual cases bounded so libFuzzer explores protocol structure
/// instead of spending time allocating/copying megabytes. This is UDP wire
/// protocol code; production datagrams are well under this bound.
const MAX_INPUT_LEN: usize = 128 * 1024;

/// Fixed key used only to exercise the HMAC verification path. Never treated
/// as a secret.
const FUZZ_HMAC_KEY: &[u8] = b"fuzz-decode-request-key";

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_LEN {
        return;
    }

    // The core invariant: arbitrary bytes must never panic. Ok/Err are both
    // normal outcomes.
    let decoded = match decode_request(data) {
        Ok(decoded) => decoded,
        Err(_) => return,
    };

    match decoded.kind {
        DecodedRequestKind::Open { params, .. } => {
            // Structural Open decoding deliberately permits malformed
            // parameter bytes; both Ok and Err from the secondary parser are
            // acceptable, and a structurally decoded Open is not required to
            // contain valid Params.
            let _ = Params::decode(params);
        }
        DecodedRequestKind::Close { .. } | DecodedRequestKind::Echo { .. } => {
            // No token/session policy belongs here; that's above irtt-proto.
        }
    }

    if decoded.hmac_present {
        // Most arbitrary HMAC fields will fail authentication; both outcomes
        // are acceptable, we're only exercising the verifier for panics.
        let _ = verify_packet_hmac(FUZZ_HMAC_KEY, data);
    }
});
