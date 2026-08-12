//! Requests this slice deliberately does not implement.
//!
//! Echo is recognized by the admission boundary and then ignored. These tests
//! exist to keep the scope boundary honest: receive state must not appear by
//! accident before the slice that owns it.

use irtt_proto::{encode_request, RequestToEncode};

use super::support::{
    client_params, core_for, open_request, open_session, peer, ScriptedTokens, KEY,
};

const TOKEN_A: u64 = 0x0102_0304_0506_0708;

fn open_one_session(hmac_key: Option<&[u8]>) -> crate::ServerCore {
    let mut core = core_for(hmac_key, ScriptedTokens::new([TOKEN_A]));
    open_session(&mut core, peer(), hmac_key);
    core
}

#[test]
fn a_valid_echo_is_neither_answered_nor_counted_yet() {
    for hmac_key in [None, Some(KEY)] {
        let mut core = open_one_session(hmac_key);
        let session = core.session(TOKEN_A).cloned().expect("session must exist");

        let echo = encode_request(
            RequestToEncode::Echo {
                token: TOKEN_A,
                sequence: 0,
                params: session.params(),
                payload: &[],
            },
            hmac_key,
        )
        .unwrap();

        assert_eq!(core.handle_datagram(peer(), &echo).unwrap(), None);
        assert_eq!(core.session_count(), 1);
        assert_eq!(core.session(TOKEN_A), Some(&session));
    }
}

#[test]
fn structurally_invalid_datagrams_are_discarded() {
    let mut core = open_one_session(None);
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
