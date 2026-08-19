#![no_main]

use irtt_proto::{
    decode_open_reply, hmac::compute_hmac_in_place, FLAG_HMAC, FLAG_OPEN, FLAG_REPLY,
};
use libfuzzer_sys::fuzz_target;

/// See `decode_request.rs` for the rationale behind this bound.
const MAX_INPUT_LEN: usize = 128 * 1024;

/// Offset of the HMAC field, right after the 4-byte header. `irtt-proto`
/// keeps the real constant crate-private; this mirrors it the same way the
/// crate's own external integration tests do.
const HMAC_OFFSET: usize = 4;
const HMAC_SIZE: usize = 16;

const FUZZ_HMAC_KEY: &[u8] = b"fuzz-decode-open-reply-key";

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_LEN {
        return;
    }

    // Unauthenticated decode path: no panic is the invariant, Ok/Err both
    // acceptable.
    let _ = decode_open_reply(data, None);

    // Authenticated decode path against arbitrary bytes. Most inputs will
    // fail on HMAC presence/verification before reaching the post-HMAC
    // parser, which is expected and fine on its own.
    let _ = decode_open_reply(data, Some(FUZZ_HMAC_KEY));

    // To also give the post-HMAC parser sustained coverage, derive a packet
    // that *will* authenticate: when the input is structurally capable of
    // carrying an HMAC field and its flags already indicate HMAC, patch in a
    // freshly-computed HMAC over the untouched body via the crate's own
    // helper (never reimplemented here) and decode that.
    if data.len() >= HMAC_OFFSET + HMAC_SIZE {
        let flags = data[3];
        let looks_like_open_reply =
            flags & FLAG_OPEN != 0 && flags & FLAG_REPLY != 0 && flags & FLAG_HMAC != 0;
        if looks_like_open_reply {
            let mut signed = data.to_vec();
            if compute_hmac_in_place(FUZZ_HMAC_KEY, &mut signed, HMAC_OFFSET).is_ok() {
                let _ = decode_open_reply(&signed, Some(FUZZ_HMAC_KEY));
            }
        }
    }
});
