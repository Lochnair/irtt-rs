use irtt_proto::{
    decode_close_request, decode_open_reply, decode_open_request, encode_close_request,
    encode_open_reply, encode_open_request, CloseRequest, OpenReply, OpenRequest, Params,
    ProtoError, FLAG_CLOSE, FLAG_HMAC, FLAG_OPEN, FLAG_REPLY, MAGIC,
};

const KEY: &[u8] = b"testkey";
const TOKEN: u64 = 0x7896_b6ab_8771_5213;

#[test]
fn authenticated_open_reply_encoder_sets_hmac_from_key() {
    let reply = OpenReply {
        flags: FLAG_OPEN | FLAG_REPLY,
        token: TOKEN,
        params: Params {
            protocol_version: 1,
            ..Params::default()
        },
    };
    let packet = encode_open_reply(&reply, Some(KEY)).unwrap();

    assert_eq!(packet[3], FLAG_OPEN | FLAG_REPLY | FLAG_HMAC);
    assert_eq!(
        decode_open_reply(&packet, Some(KEY)),
        Ok(OpenReply {
            flags: FLAG_OPEN | FLAG_REPLY | FLAG_HMAC,
            ..reply
        })
    );
}

#[test]
fn rejected_open_reply_round_trips_and_zero_token_requires_close() {
    let rejected = OpenReply {
        flags: FLAG_OPEN | FLAG_REPLY | FLAG_CLOSE,
        token: 0,
        params: Params {
            protocol_version: 1,
            ..Params::default()
        },
    };
    let packet = encode_open_reply(&rejected, None).unwrap();
    assert_eq!(decode_open_reply(&packet, None), Ok(rejected.clone()));

    assert_eq!(
        encode_open_reply(
            &OpenReply {
                flags: FLAG_OPEN | FLAG_REPLY,
                ..rejected
            },
            None,
        ),
        Err(ProtoError::ZeroToken)
    );
}

#[test]
fn open_and_close_codecs_enforce_directional_flags() {
    let params = Params::default();
    let open_reply_packet = encode_open_reply(
        &OpenReply {
            flags: FLAG_OPEN | FLAG_REPLY,
            token: TOKEN,
            params: params.clone(),
        },
        None,
    )
    .unwrap();
    assert_eq!(
        decode_open_request(&open_reply_packet, None),
        Err(ProtoError::UnexpectedFlag(FLAG_REPLY))
    );
    assert_eq!(
        encode_open_reply(
            &OpenReply {
                flags: FLAG_REPLY,
                token: TOKEN,
                params: params.clone(),
            },
            None,
        ),
        Err(ProtoError::MissingFlag(FLAG_OPEN))
    );

    let no_test_packet = encode_open_request(
        &OpenRequest {
            params,
            close: true,
        },
        None,
    )
    .unwrap();
    assert_eq!(
        decode_close_request(&no_test_packet, None),
        Err(ProtoError::UnexpectedFlag(FLAG_OPEN))
    );
}

#[test]
fn open_and_close_malformed_bodies_and_exact_lengths_are_rejected() {
    let mut malformed_open_request = MAGIC.to_vec();
    malformed_open_request.push(FLAG_OPEN);
    malformed_open_request.extend_from_slice(&[1, 0x80]);
    assert_eq!(
        decode_open_request(&malformed_open_request, None),
        Err(ProtoError::TruncatedVarint)
    );

    let mut malformed_open_reply = MAGIC.to_vec();
    malformed_open_reply.push(FLAG_OPEN | FLAG_REPLY);
    malformed_open_reply.extend_from_slice(&TOKEN.to_le_bytes());
    malformed_open_reply.extend_from_slice(&[1, 0x80]);
    assert_eq!(
        decode_open_reply(&malformed_open_reply, None),
        Err(ProtoError::TruncatedVarint)
    );

    let close_packet = encode_close_request(&CloseRequest { token: TOKEN }, None).unwrap();
    assert_eq!(
        decode_close_request(&close_packet[..11], None),
        Err(ProtoError::PacketLengthMismatch {
            expected: 12,
            actual: 11,
        })
    );
    let mut long_close_packet = close_packet;
    long_close_packet.push(0);
    assert_eq!(
        decode_close_request(&long_close_packet, None),
        Err(ProtoError::PacketLengthMismatch {
            expected: 12,
            actual: 13,
        })
    );
}
