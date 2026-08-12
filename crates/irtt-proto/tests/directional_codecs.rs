use irtt_proto::{
    decode_echo_reply, decode_open_reply, decode_request, echo_packet_len, encode_echo_reply,
    encode_open_reply, encode_request, hmac::compute_hmac_in_place, Clock, DecodedRequestKind,
    EchoReply, OpenReply, PacketLayout, Params, ProtoError, ReceivedStats, RequestToEncode,
    StampAt, TimestampFields, FLAG_CLOSE, FLAG_HMAC, FLAG_OPEN, FLAG_REPLY, MAGIC,
};

/// Wire offset of the HMAC digest within an ECHO reply: the fixed 4-byte
/// header (magic + flags) always precedes it.
const HMAC_OFFSET: usize = 4;

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

fn echo_request<'a>(params: &'a Params, payload: &'a [u8]) -> RequestToEncode<'a> {
    RequestToEncode::Echo {
        token: TOKEN,
        sequence: 17,
        params,
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
fn reply_encoders_supply_their_packet_type_rules() {
    let params = Params::default();
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
}

/// Every packet the reply encoders produce must be rejected by the inbound
/// request decoder: a server never answers a datagram carrying `FLAG_REPLY`.
#[test]
fn encoded_replies_are_never_admitted_as_inbound_requests() {
    let params = Params::default();

    for reply_flags in [FLAG_OPEN | FLAG_REPLY, FLAG_OPEN | FLAG_REPLY | FLAG_CLOSE] {
        let packet = encode_open_reply(
            &OpenReply {
                flags: reply_flags,
                token: if reply_flags & FLAG_CLOSE == 0 {
                    TOKEN
                } else {
                    0
                },
                params: params.clone(),
            },
            None,
        )
        .unwrap();
        assert_eq!(
            decode_request(&packet),
            Err(ProtoError::UnexpectedFlag(FLAG_REPLY))
        );
    }

    for reply_flags in [FLAG_REPLY, FLAG_REPLY | FLAG_CLOSE] {
        let packet = encode_echo_reply(&echo_reply(&params, reply_flags), &params, None).unwrap();
        assert_eq!(
            decode_request(&packet),
            Err(ProtoError::UnexpectedFlag(FLAG_REPLY))
        );
    }
}

#[test]
fn reply_bodies_are_decoded_semantically_and_reject_malformed_params() {
    let mut malformed_open_reply = MAGIC.to_vec();
    malformed_open_reply.push(FLAG_OPEN | FLAG_REPLY);
    malformed_open_reply.extend_from_slice(&TOKEN.to_le_bytes());
    malformed_open_reply.extend_from_slice(&[1, 0x80]);
    assert_eq!(
        decode_open_reply(&malformed_open_reply, None),
        Err(ProtoError::TruncatedVarint)
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

    let packet = encode_request(echo_request(&params, &[1, 2]), None).unwrap();
    assert_eq!(&packet[16..], &[1, 2, 0, 0]);
    assert_eq!(
        encode_request(echo_request(&params, &[0; 5]), None),
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

#[test]
fn a_negative_negotiated_length_still_encodes_in_both_directions() {
    // A server accepts a negative negotiated length during open and returns it
    // unchanged, so a live session can hold one and must stay executable. There
    // is no datagram shorter than none, so both directions fall back to the
    // mandatory field block — header (4) + token (8) + sequence (4) — exactly
    // as a zero length would.
    let params = Params {
        length: -4096,
        ..Params::default()
    };
    assert_eq!(echo_packet_len(false, &params), Ok(16));

    let request = encode_request(echo_request(&params, &[]), None)
        .expect("a negative negotiated length must still encode a request");
    assert_eq!(request.len(), 16);

    let reply = encode_echo_reply(&echo_reply(&params, FLAG_REPLY), &params, None)
        .expect("a negative negotiated length must still encode a reply");
    assert_eq!(reply.len(), 16);
    assert_eq!(
        decode_echo_reply(&reply, &params, None).unwrap().token,
        TOKEN
    );
}

// ---------- Upstream midpoint ECHO-reply compatibility ----------
//
// Verified upstream irtt 0.9.1 behavior: for single-clock
// `StampAt::Midpoint` negotiations (`Clock::Wall` or `Clock::Monotonic`)
// upstream always emits BOTH midpoint timestamp fields (wall, then
// monotonic), so its header is one timestamp longer than the negotiated
// header.
//
// Crucially the datagram length is NOT `normal_len + 8`. Both forms are
// `max(negotiated_length, header)`, so the extra field lengthens the
// datagram only while the larger header still exceeds the negotiated
// length, and displaces payload beyond that:
//
//     compat_len - normal_len == 8, then 7..1, then 0
//
// `decode_echo_reply` must accept the longer form wherever it is
// identifiable by length, keep rejecting unrelated lengths, and fall back
// to plain negotiated parsing once the two forms are the same length.
//
// `encode_echo_reply` must never produce the dual-field shape, so these
// tests hand-build the wire form directly and narrowly.

/// Midpoint params whose negotiated length sits `payload_space` bytes past
/// the normal (single-midpoint) header.
fn midpoint_params(clock: Clock, payload_space: usize) -> Params {
    let base = Params {
        protocol_version: 1,
        received_stats: ReceivedStats::Count,
        stamp_at: StampAt::Midpoint,
        clock,
        ..Params::default()
    };
    let header_len = PacketLayout::echo(false, &base).header_len();
    Params {
        length: (header_len + payload_space) as i64,
        ..base
    }
}

fn midpoint_compat_len(params: &Params, hmac: bool) -> usize {
    let compat_header = PacketLayout::echo(hmac, params).header_len() + 8;
    compat_header.max(echo_packet_len(hmac, params).unwrap())
}

/// Hand-builds upstream's dual-midpoint ECHO reply: both midpoint fields on
/// the wire, total length `max(negotiated_length, normal_header + 8)`.
fn build_midpoint_dual_field_reply(
    params: &Params,
    flags: u8,
    seq: u32,
    midpoint_wire_fields: (i64, i64),
    hmac_key: Option<&[u8]>,
) -> Vec<u8> {
    let (wall, mono) = midpoint_wire_fields;
    let hmac = hmac_key.is_some();
    let layout = PacketLayout::echo(hmac, params);
    let compat_header_len = layout.header_len() + 8;
    let compat_len = midpoint_compat_len(params, hmac);

    let mut packet = MAGIC.to_vec();
    packet.push(flags);
    if hmac {
        packet.extend_from_slice(&[0u8; 16]);
    }
    packet.extend_from_slice(&TOKEN.to_le_bytes());
    packet.extend_from_slice(&seq.to_le_bytes());
    if layout.recv_count {
        packet.extend_from_slice(&7_u32.to_le_bytes());
    }
    if layout.recv_window {
        packet.extend_from_slice(&11_u64.to_le_bytes());
    }
    packet.extend_from_slice(&wall.to_le_bytes());
    packet.extend_from_slice(&mono.to_le_bytes());
    assert_eq!(packet.len(), compat_header_len);

    packet.resize(compat_len, 0);
    for (index, byte) in packet[compat_header_len..].iter_mut().enumerate() {
        *byte = (index as u8).wrapping_add(1);
    }

    if let Some(key) = hmac_key {
        compute_hmac_in_place(key, &mut packet, HMAC_OFFSET).unwrap();
    }
    packet
}

#[test]
fn midpoint_dual_field_reply_is_accepted_wherever_length_identifies_it() {
    // payload_space -> how much longer the compatibility datagram is.
    // Covers the full +8 regime and the intermediate +7..+1 regimes.
    for (payload_space, expected_extra) in [(0, 8), (1, 7), (4, 4), (7, 1)] {
        for clock in [Clock::Wall, Clock::Monotonic] {
            let params = midpoint_params(clock, payload_space);
            let normal_len = echo_packet_len(false, &params).unwrap();
            let packet = build_midpoint_dual_field_reply(&params, FLAG_REPLY, 3, (111, 222), None);
            assert_eq!(
                packet.len(),
                normal_len + expected_extra,
                "payload_space {payload_space} clock {clock:?}"
            );

            let reply = decode_echo_reply(&packet, &params, None).unwrap();
            let (expected_wall, expected_mono) = match clock {
                Clock::Wall => (Some(111), None),
                // The first wire field must not be mistaken for the
                // negotiated monotonic value.
                Clock::Monotonic => (None, Some(222)),
                Clock::Unspecified | Clock::Both => unreachable!(),
            };
            assert_eq!(
                reply.timestamps.midpoint_wall, expected_wall,
                "payload_space {payload_space} clock {clock:?}"
            );
            assert_eq!(
                reply.timestamps.midpoint_mono, expected_mono,
                "payload_space {payload_space} clock {clock:?}"
            );
            // Payload begins after BOTH compatibility timestamp fields.
            let compat_header_len = PacketLayout::echo(false, &params).header_len() + 8;
            assert_eq!(reply.payload, packet[compat_header_len..].to_vec());
        }
    }
}

#[test]
fn midpoint_dual_field_reply_of_equal_length_is_accepted_but_not_correctable() {
    // Once the negotiated length covers the larger header, the conforming
    // and upstream forms have identical length. The reply must still be
    // accepted, parsed against the negotiated layout.
    for payload_space in [8, 64] {
        for clock in [Clock::Wall, Clock::Monotonic] {
            let params = midpoint_params(clock, payload_space);
            let normal_len = echo_packet_len(false, &params).unwrap();
            let packet = build_midpoint_dual_field_reply(&params, FLAG_REPLY, 4, (111, 222), None);
            assert_eq!(
                packet.len(),
                normal_len,
                "compat and negotiated lengths coincide for payload_space {payload_space}"
            );

            let reply = decode_echo_reply(&packet, &params, None).unwrap();
            match clock {
                // Wall-only happens to line up: the negotiated field
                // position holds upstream's wall timestamp.
                Clock::Wall => {
                    assert_eq!(reply.timestamps.midpoint_wall, Some(111));
                    assert_eq!(reply.timestamps.midpoint_mono, None);
                }
                // Monotonic-only cannot be corrected deterministically from
                // the packet alone: the negotiated field position holds
                // upstream's WALL timestamp, and nothing in the datagram
                // distinguishes that from a conforming reply. Documented
                // limitation - we accept rather than guess.
                Clock::Monotonic => {
                    assert_eq!(reply.timestamps.midpoint_wall, None);
                    assert_eq!(reply.timestamps.midpoint_mono, Some(111));
                }
                Clock::Unspecified | Clock::Both => unreachable!(),
            }
        }
    }
}

#[test]
fn midpoint_dual_field_reply_authenticates_over_full_packet() {
    let params = midpoint_params(Clock::Wall, 0);
    let packet =
        build_midpoint_dual_field_reply(&params, FLAG_REPLY | FLAG_HMAC, 9, (555, 666), Some(KEY));
    assert_eq!(packet.len(), midpoint_compat_len(&params, true));

    let reply = decode_echo_reply(&packet, &params, Some(KEY)).unwrap();
    assert_eq!(reply.timestamps.midpoint_wall, Some(555));

    let mut corrupted = packet;
    corrupted[HMAC_OFFSET] ^= 0xFF;
    assert_eq!(
        decode_echo_reply(&corrupted, &params, Some(KEY)),
        Err(ProtoError::BadHmac)
    );
}

#[test]
fn only_the_exact_compat_length_is_accepted_as_an_extension() {
    let params = midpoint_params(Clock::Wall, 0);
    let packet = build_midpoint_dual_field_reply(&params, FLAG_REPLY, 1, (10, 20), None);
    let expected_len = echo_packet_len(false, &params).unwrap();
    assert_eq!(packet.len(), expected_len + 8);

    // Every other length around the compatibility size stays rejected,
    // including the intermediate +1..+7 sizes that are only valid under a
    // different negotiated length.
    for delta in [-7_i64, -1, 1, 92] {
        let target_len = (packet.len() as i64 + delta) as usize;
        let mut malformed = packet.clone();
        malformed.resize(target_len, 0);
        assert_eq!(
            decode_echo_reply(&malformed, &params, None),
            Err(ProtoError::PacketLengthMismatch {
                expected: expected_len,
                actual: target_len,
            }),
            "delta {delta} (target length {target_len}) must still be rejected"
        );
    }
}

#[test]
fn midpoint_both_clock_reply_has_no_additional_extension() {
    let params = midpoint_params(Clock::Both, 0);
    let reply = echo_reply(&params, FLAG_REPLY);
    let packet = encode_echo_reply(&reply, &params, None).unwrap();

    let mut too_long = packet.clone();
    too_long.extend_from_slice(&[0; 8]);
    assert_eq!(
        decode_echo_reply(&too_long, &params, None),
        Err(ProtoError::PacketLengthMismatch {
            expected: packet.len(),
            actual: too_long.len(),
        })
    );
}

#[test]
fn non_midpoint_stamp_at_has_no_longer_form_exception() {
    let params = Params {
        received_stats: ReceivedStats::None,
        stamp_at: StampAt::Receive,
        clock: Clock::Wall,
        ..Params::default()
    };
    let reply = echo_reply(&params, FLAG_REPLY);
    let packet = encode_echo_reply(&reply, &params, None).unwrap();

    let mut too_long = packet.clone();
    too_long.extend_from_slice(&[0; 8]);
    assert_eq!(
        decode_echo_reply(&too_long, &params, None),
        Err(ProtoError::PacketLengthMismatch {
            expected: packet.len(),
            actual: too_long.len(),
        })
    );
}

/// The midpoint compatibility length is a *reply* concept. An inbound ECHO
/// request of that length is just a longer request, which a receiver accepts
/// without applying any negotiated length rule.
#[test]
fn echo_request_decoding_has_no_compat_length_rule() {
    let params = midpoint_params(Clock::Wall, 0);
    let packet = encode_request(echo_request(&params, &[]), None).unwrap();

    let mut too_long = packet.clone();
    too_long.extend_from_slice(&[0; 8]);
    let request = decode_request(&too_long).unwrap();
    assert_eq!(
        request.kind,
        DecodedRequestKind::Echo {
            token: TOKEN,
            sequence: 17,
            tail: &too_long[16..],
        }
    );
}
