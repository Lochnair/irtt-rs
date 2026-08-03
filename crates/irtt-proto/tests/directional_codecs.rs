use irtt_proto::{
    decode_close_request, decode_echo_reply, decode_echo_request, decode_open_reply,
    decode_open_request, encode_close_request, encode_echo_reply, encode_echo_request,
    encode_open_reply, encode_open_request, Clock, CloseRequest, EchoReply, EchoRequest, OpenReply,
    OpenRequest, PacketLayout, Params, ProtoError, ReceivedStats, StampAt, TimestampFields,
    FLAG_CLOSE, FLAG_HMAC, FLAG_OPEN, FLAG_REPLY, MAGIC,
};

const KEY: &[u8] = b"testkey";
const TOKEN: u64 = 0x7896_b6ab_8771_5213;

fn params() -> Params {
    Params {
        protocol_version: 1,
        duration_ns: 3_000_000_000,
        interval_ns: 1_000_000_000,
        received_stats: ReceivedStats::Both,
        stamp_at: StampAt::Both,
        clock: Clock::Both,
        ..Params::default()
    }
}

fn echo_request(payload: Vec<u8>) -> EchoRequest {
    EchoRequest {
        token: TOKEN,
        sequence: 17,
        payload,
    }
}

fn echo_reply(params: &Params, flags: u8) -> EchoReply {
    let layout = PacketLayout::echo(false, params);
    EchoReply {
        flags,
        token: TOKEN,
        sequence: u32::MAX,
        recv_count: layout.recv_count.then_some(u32::MAX),
        recv_window: layout.recv_window.then_some(u64::MAX),
        timestamps: TimestampFields {
            recv_wall: layout.recv_wall.then_some(i64::MIN),
            recv_mono: layout.recv_mono.then_some(-2),
            midpoint_wall: layout.midpoint_wall.then_some(-1),
            midpoint_mono: layout.midpoint_mono.then_some(0),
            send_wall: layout.send_wall.then_some(1),
            send_mono: layout.send_mono.then_some(i64::MAX),
        },
        payload: Vec::new(),
    }
}

#[test]
fn authenticated_reply_encoders_set_hmac_from_key() {
    let params = params();
    let open_reply = OpenReply {
        flags: FLAG_OPEN | FLAG_REPLY,
        token: TOKEN,
        params: params.clone(),
    };
    let packet = encode_open_reply(&open_reply, Some(KEY)).unwrap();
    assert_eq!(packet[3], FLAG_OPEN | FLAG_REPLY | FLAG_HMAC);
    assert_eq!(
        decode_open_reply(&packet, Some(KEY)),
        Ok(OpenReply {
            flags: FLAG_OPEN | FLAG_REPLY | FLAG_HMAC,
            ..open_reply
        })
    );

    let echo_reply = echo_reply(&params, FLAG_REPLY);
    let packet = encode_echo_reply(&echo_reply, &params, Some(KEY)).unwrap();
    assert_eq!(packet[3], FLAG_REPLY | FLAG_HMAC);
    assert_eq!(
        decode_echo_reply(&packet, &params, Some(KEY)),
        Ok(EchoReply {
            flags: FLAG_REPLY | FLAG_HMAC,
            ..echo_reply
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
fn peer_close_echo_reply_round_trips_without_a_close_reply_codec() {
    let params = Params::default();
    let reply = echo_reply(&params, FLAG_REPLY | FLAG_CLOSE);
    let packet = encode_echo_reply(&reply, &params, None).unwrap();

    assert_eq!(packet[3], FLAG_REPLY | FLAG_CLOSE);
    assert_eq!(decode_echo_reply(&packet, &params, None), Ok(reply));
}

#[test]
fn inverse_codecs_supply_their_packet_type_rules() {
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

    let echo_reply_packet =
        encode_echo_reply(&echo_reply(&params, FLAG_REPLY), &params, None).unwrap();
    assert_eq!(
        decode_echo_request(&echo_reply_packet, &params, None),
        Err(ProtoError::UnexpectedFlag(FLAG_REPLY))
    );
    assert_eq!(
        encode_echo_reply(
            &EchoReply {
                flags: 0,
                ..echo_reply(&params, FLAG_REPLY)
            },
            &params,
            None,
        ),
        Err(ProtoError::MissingFlag(FLAG_REPLY))
    );

    let close_packet = encode_close_request(&CloseRequest { token: TOKEN }, None).unwrap();
    assert_eq!(
        decode_echo_request(&close_packet, &params, None),
        Err(ProtoError::UnexpectedFlag(FLAG_CLOSE))
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
fn codec_specific_malformed_bodies_and_exact_lengths_are_rejected() {
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

    let params = Params {
        length: 20,
        ..Params::default()
    };
    let echo_packet = encode_echo_request(&echo_request(Vec::new()), &params, None).unwrap();
    assert_eq!(
        decode_echo_request(&echo_packet[..19], &params, None),
        Err(ProtoError::PacketLengthMismatch {
            expected: 20,
            actual: 19,
        })
    );
    let mut long_echo_packet = echo_packet;
    long_echo_packet.push(0);
    assert_eq!(
        decode_echo_request(&long_echo_packet, &params, None),
        Err(ProtoError::PacketLengthMismatch {
            expected: 20,
            actual: 21,
        })
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

#[test]
fn echo_layout_combinations_round_trip() {
    for received_stats in [
        ReceivedStats::None,
        ReceivedStats::Count,
        ReceivedStats::Window,
        ReceivedStats::Both,
    ] {
        for stamp_at in [
            StampAt::None,
            StampAt::Send,
            StampAt::Receive,
            StampAt::Both,
            StampAt::Midpoint,
        ] {
            for clock in [Clock::Wall, Clock::Monotonic, Clock::Both] {
                for key in [None, Some(KEY)] {
                    let params = Params {
                        protocol_version: 1,
                        received_stats,
                        stamp_at,
                        clock,
                        ..Params::default()
                    };
                    let reply = echo_reply(&params, FLAG_REPLY);
                    let packet = encode_echo_reply(&reply, &params, key).unwrap();
                    let decoded = decode_echo_reply(&packet, &params, key).unwrap();

                    assert_eq!(decoded.token, reply.token);
                    assert_eq!(decoded.sequence, reply.sequence);
                    assert_eq!(decoded.recv_count, reply.recv_count);
                    assert_eq!(decoded.recv_window, reply.recv_window);
                    assert_eq!(decoded.timestamps, reply.timestamps);
                    assert_eq!(decoded.payload, reply.payload);
                }
            }
        }
    }
}

#[test]
fn echo_reply_requires_exact_negotiated_optional_fields() {
    let params = Params {
        received_stats: ReceivedStats::Count,
        stamp_at: StampAt::Send,
        clock: Clock::Wall,
        ..Params::default()
    };
    let missing_count = EchoReply {
        recv_count: None,
        ..echo_reply(&params, FLAG_REPLY)
    };
    assert_eq!(
        encode_echo_reply(&missing_count, &params, None),
        Err(ProtoError::MissingField("recv_count"))
    );

    let no_optional_params = Params::default();
    let unexpected_timestamp = EchoReply {
        timestamps: TimestampFields {
            send_wall: Some(1),
            ..TimestampFields::default()
        },
        ..echo_reply(&no_optional_params, FLAG_REPLY)
    };
    assert_eq!(
        encode_echo_reply(&unexpected_timestamp, &no_optional_params, None),
        Err(ProtoError::UnexpectedField("send_wall"))
    );
}

#[test]
fn echo_payload_is_copied_zero_filled_and_bounded_in_both_directions() {
    let params = Params {
        length: 20,
        ..Params::default()
    };

    let request = echo_request(vec![1, 2]);
    let packet = encode_echo_request(&request, &params, None).unwrap();
    assert_eq!(&packet[16..], &[1, 2, 0, 0]);
    let decoded = decode_echo_request(&packet, &params, None).unwrap();
    assert_eq!(decoded.payload, vec![1, 2, 0, 0]);
    assert_eq!(
        encode_echo_request(&decoded, &params, None).unwrap(),
        packet
    );
    assert_eq!(
        encode_echo_request(&echo_request(vec![0; 5]), &params, None),
        Err(ProtoError::PayloadTooLarge {
            available: 4,
            provided: 5,
        })
    );

    let reply = EchoReply {
        payload: vec![3, 4],
        ..echo_reply(&params, FLAG_REPLY)
    };
    let packet = encode_echo_reply(&reply, &params, None).unwrap();
    assert_eq!(&packet[16..], &[3, 4, 0, 0]);
    let decoded = decode_echo_reply(&packet, &params, None).unwrap();
    assert_eq!(decoded.payload, vec![3, 4, 0, 0]);
    assert_eq!(encode_echo_reply(&decoded, &params, None).unwrap(), packet);
    assert_eq!(
        encode_echo_reply(
            &EchoReply {
                payload: vec![0; 5],
                ..echo_reply(&params, FLAG_REPLY)
            },
            &params,
            None,
        ),
        Err(ProtoError::PayloadTooLarge {
            available: 4,
            provided: 5,
        })
    );
}
