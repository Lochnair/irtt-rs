//! The structural admission boundary.
//!
//! A datagram that never decodes as a request is discarded before any request
//! kind is chosen, so it can reach neither the session table nor an echo's
//! receive state.

use super::support::{client_params, core_for, open_request, open_session, peer, ScriptedTokens};

const TOKEN_A: u64 = 0x0102_0304_0506_0708;

#[test]
fn structurally_invalid_datagrams_are_discarded() {
    let mut core = core_for(None, ScriptedTokens::new([TOKEN_A]));
    open_session(&mut core, peer(), None);
    let session = core.session(TOKEN_A).cloned().expect("session must exist");

    let mut bad_magic = open_request(&client_params(), None);
    bad_magic[0] ^= 0x01;

    let mut reserved_flag = open_request(&client_params(), None);
    reserved_flag[3] |= 0x10;

    let mut reply_flag = open_request(&client_params(), None);
    reply_flag[3] |= irtt_proto::FLAG_REPLY;

    for (name, packet) in [
        ("empty datagram", Vec::new()),
        ("datagram shorter than the header", vec![0x14, 0xa7, 0x5b]),
        ("bad magic", bad_magic),
        ("reserved flag bit", reserved_flag),
        ("reply flag", reply_flag),
    ] {
        assert_eq!(
            core.handle_datagram(peer(), &packet).unwrap(),
            None,
            "{name} must be discarded"
        );
    }

    assert_eq!(core.session_count(), 1);
    assert_eq!(core.session(TOKEN_A), Some(&session));
}
