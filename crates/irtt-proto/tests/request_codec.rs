//! Coverage for the unified request codec: sender-side [`encode_request`] and
//! receiver-side structural [`decode_request`], plus the packet-level HMAC
//! verifier that separates authentication from structure.

use irtt_proto::{
    decode_request, encode_request, hmac::compute_hmac_in_place, verify_packet_hmac, Clock,
    DecodedRequest, DecodedRequestKind, Params, ProtoError, ReceivedStats, RequestToEncode,
    StampAt, FLAG_CLOSE, FLAG_HMAC, FLAG_OPEN, FLAG_REPLY, HMAC_SIZE, MAGIC,
};

const KEY: &[u8] = b"testkey";
const TOKEN: u64 = 0x7896_b6ab_8771_5213;
const SEQUENCE: u32 = 0x1122_3344;

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

/// Builds a raw request of exactly `len` bytes: header, then an HMAC field when
/// flagged, then `body`, then zero padding.
fn raw(flags: u8, body: &[u8], len: usize) -> Vec<u8> {
    let mut packet = MAGIC.to_vec();
    packet.push(flags);
    if flags & FLAG_HMAC != 0 {
        packet.extend_from_slice(&[0xAA; HMAC_SIZE]);
    }
    packet.extend_from_slice(body);
    packet.resize(len, 0);
    packet
}

fn header(flags: u8) -> Vec<u8> {
    raw(flags, &[], if flags & FLAG_HMAC != 0 { 20 } else { 4 })
}

fn token_and_sequence() -> Vec<u8> {
    let mut body = TOKEN.to_le_bytes().to_vec();
    body.extend_from_slice(&SEQUENCE.to_le_bytes());
    body
}

// ---------- A. Classification ----------

fn classify(kind: &DecodedRequestKind<'_>) -> &'static str {
    match kind {
        DecodedRequestKind::Open { no_test: false, .. } => "open",
        DecodedRequestKind::Open { no_test: true, .. } => "no-test open",
        DecodedRequestKind::Close { .. } => "close",
        DecodedRequestKind::Echo { .. } => "echo",
    }
}

#[test]
fn flag_combinations_classify_requests_with_orthogonal_hmac_presence() {
    let cases = [
        (0, false, "echo"),
        (FLAG_HMAC, true, "echo"),
        (FLAG_CLOSE, false, "close"),
        (FLAG_CLOSE | FLAG_HMAC, true, "close"),
        (FLAG_OPEN, false, "open"),
        (FLAG_OPEN | FLAG_CLOSE, false, "no-test open"),
        (FLAG_OPEN | FLAG_HMAC, true, "open"),
        (FLAG_OPEN | FLAG_CLOSE | FLAG_HMAC, true, "no-test open"),
    ];

    for (flags, hmac_present, expected) in cases {
        // Long enough for any kind, so only the flags decide classification.
        let packet = raw(flags, &token_and_sequence(), 64);
        let request = decode_request(&packet).unwrap_or_else(|error| {
            panic!("flags 0x{flags:02x} must decode structurally, got {error:?}")
        });

        assert_eq!(
            request.hmac_present, hmac_present,
            "flags 0x{flags:02x} HMAC presence"
        );
        assert_eq!(
            classify(&request.kind),
            expected,
            "flags 0x{flags:02x} classification"
        );
    }
}

// ---------- B. Invalid header / flags ----------

#[test]
fn structurally_invalid_headers_are_rejected() {
    assert_eq!(
        decode_request(&MAGIC),
        Err(ProtoError::PacketTooShort {
            needed: 4,
            actual: 3,
        })
    );
    assert_eq!(
        decode_request(&[0x00, MAGIC[1], MAGIC[2], FLAG_OPEN]),
        Err(ProtoError::BadMagic)
    );

    for reserved in [0x10_u8, 0x20, 0x40, 0x80, 0xF0] {
        assert_eq!(
            decode_request(&header(reserved)),
            Err(ProtoError::ReservedFlags(reserved)),
            "reserved bits 0x{reserved:02x}"
        );
    }
}

#[test]
fn any_reply_flagged_datagram_is_rejected_as_a_request() {
    for flags in [
        FLAG_REPLY,
        FLAG_OPEN | FLAG_REPLY,
        FLAG_CLOSE | FLAG_REPLY,
        FLAG_OPEN | FLAG_CLOSE | FLAG_REPLY,
        FLAG_REPLY | FLAG_HMAC,
        FLAG_OPEN | FLAG_REPLY | FLAG_HMAC,
        FLAG_CLOSE | FLAG_REPLY | FLAG_HMAC,
        FLAG_OPEN | FLAG_CLOSE | FLAG_REPLY | FLAG_HMAC,
    ] {
        assert_eq!(
            decode_request(&raw(flags, &token_and_sequence(), 64)),
            Err(ProtoError::UnexpectedFlag(FLAG_REPLY)),
            "flags 0x{flags:02x}"
        );
    }
}

// ---------- C. Structural minimum lengths ----------

#[test]
fn structural_minimum_lengths_are_enforced_per_kind() {
    // (flags, minimum accepted length)
    let cases = [
        (FLAG_OPEN, 4_usize),
        (FLAG_CLOSE, 12),
        (0, 16),
        (FLAG_OPEN | FLAG_HMAC, 20),
        (FLAG_CLOSE | FLAG_HMAC, 28),
        (FLAG_HMAC, 32),
    ];

    for (flags, minimum) in cases {
        let accepted = raw(flags, &token_and_sequence(), minimum);
        assert_eq!(accepted.len(), minimum);
        assert!(
            decode_request(&accepted).is_ok(),
            "flags 0x{flags:02x} length {minimum} must be accepted"
        );

        // One byte short of the minimum, for every kind that has bytes to lose.
        let short_len = minimum - 1;
        if short_len >= 4 {
            assert_eq!(
                decode_request(&accepted[..short_len]),
                Err(ProtoError::PacketTooShort {
                    needed: minimum,
                    actual: short_len,
                }),
                "flags 0x{flags:02x} length {short_len}"
            );
        }
    }
}

// ---------- D. Field offsets ----------

#[test]
fn token_and_sequence_are_read_from_the_hmac_dependent_offsets() {
    for hmac in [false, true] {
        let close_flags = FLAG_CLOSE | if hmac { FLAG_HMAC } else { 0 };
        let close = raw(
            close_flags,
            &TOKEN.to_le_bytes(),
            if hmac { 28 } else { 12 },
        );
        assert_eq!(
            decode_request(&close).unwrap(),
            DecodedRequest {
                hmac_present: hmac,
                kind: DecodedRequestKind::Close { token: TOKEN },
            }
        );

        let echo_flags = if hmac { FLAG_HMAC } else { 0 };
        let echo = raw(
            echo_flags,
            &token_and_sequence(),
            if hmac { 32 } else { 16 },
        );
        assert_eq!(
            decode_request(&echo).unwrap(),
            DecodedRequest {
                hmac_present: hmac,
                kind: DecodedRequestKind::Echo {
                    token: TOKEN,
                    sequence: SEQUENCE,
                    tail: &[],
                },
            }
        );
    }
}

// ---------- E. Open parameter slice ----------

#[test]
fn open_params_are_borrowed_after_the_header_and_decoded_later() {
    let encoded = params().encode();

    for hmac in [false, true] {
        let flags = FLAG_OPEN | if hmac { FLAG_HMAC } else { 0 };
        let expected_offset = if hmac { 4 + HMAC_SIZE } else { 4 };
        let packet = raw(flags, &encoded, expected_offset + encoded.len());

        match decode_request(&packet).unwrap().kind {
            DecodedRequestKind::Open { params, .. } => {
                assert_eq!(params, &packet[expected_offset..]);
                assert_eq!(Params::decode(params).unwrap(), self::params());
            }
            other => panic!("expected an open request, got {other:?}"),
        }
    }
}

#[test]
fn empty_and_malformed_open_params_decode_structurally() {
    let empty = header(FLAG_OPEN);
    match decode_request(&empty).unwrap().kind {
        DecodedRequestKind::Open { params, .. } => assert!(params.is_empty()),
        other => panic!("expected an open request, got {other:?}"),
    }

    // A truncated varint: structurally fine, semantically not. A receiver must
    // be able to authenticate before spending effort on this.
    let malformed = raw(FLAG_OPEN, &[1, 0x80], 6);
    match decode_request(&malformed).unwrap().kind {
        DecodedRequestKind::Open { params, .. } => {
            assert_eq!(params, &[1, 0x80]);
            assert_eq!(Params::decode(params), Err(ProtoError::TruncatedVarint));
        }
        other => panic!("expected an open request, got {other:?}"),
    }
}

// ---------- F. Open precedence ----------

#[test]
fn open_precedence_beats_close_and_an_echo_shaped_body() {
    // An OPEN|CLOSE datagram whose body looks exactly like a token and
    // sequence number is still a no-test open, and those bytes are parameters.
    let packet = raw(FLAG_OPEN | FLAG_CLOSE, &token_and_sequence(), 16);

    assert_eq!(
        decode_request(&packet).unwrap(),
        DecodedRequest {
            hmac_present: false,
            kind: DecodedRequestKind::Open {
                no_test: true,
                params: &packet[4..],
            },
        }
    );
}

// ---------- G. Trailing data ----------

#[test]
fn trailing_bytes_are_tolerated_and_no_length_ceiling_applies() {
    let mut close = raw(FLAG_CLOSE, &TOKEN.to_le_bytes(), 12);
    close.extend_from_slice(&[0xDE; 20]);
    assert_eq!(
        decode_request(&close).unwrap().kind,
        DecodedRequestKind::Close { token: TOKEN }
    );

    let mut echo = raw(0, &token_and_sequence(), 16);
    echo.extend_from_slice(&[0xBE; 4]);
    assert_eq!(
        decode_request(&echo).unwrap().kind,
        DecodedRequestKind::Echo {
            token: TOKEN,
            sequence: SEQUENCE,
            tail: &[0xBE; 4],
        }
    );

    // Far longer than any negotiated length: still just a request with a long
    // opaque tail. No `Params` are consulted at any point.
    let long = raw(0, &token_and_sequence(), 4096);
    match decode_request(&long).unwrap().kind {
        DecodedRequestKind::Echo { tail, .. } => assert_eq!(tail.len(), 4096 - 16),
        other => panic!("expected an echo request, got {other:?}"),
    }
}

// ---------- H. Zero token ----------

#[test]
fn a_zero_token_is_structurally_valid() {
    assert_eq!(
        decode_request(&raw(FLAG_CLOSE, &0_u64.to_le_bytes(), 12))
            .unwrap()
            .kind,
        DecodedRequestKind::Close { token: 0 }
    );
    assert_eq!(
        decode_request(&raw(0, &[0; 12], 16)).unwrap().kind,
        DecodedRequestKind::Echo {
            token: 0,
            sequence: 0,
            tail: &[],
        }
    );
}

// ---------- HMAC presence is not authentication ----------

#[test]
fn hmac_presence_is_reported_independently_of_mac_validity() {
    // A structurally complete authenticated echo whose MAC is garbage.
    let garbage = raw(FLAG_HMAC, &token_and_sequence(), 32);
    let request = decode_request(&garbage).unwrap();
    assert!(request.hmac_present);
    assert!(matches!(request.kind, DecodedRequestKind::Echo { .. }));

    // The same packet fails verification.
    assert_eq!(verify_packet_hmac(KEY, &garbage), Err(ProtoError::BadHmac));

    // Correctly signed: accepted, and verification leaves the bytes alone.
    let mut signed = garbage.clone();
    compute_hmac_in_place(KEY, &mut signed, 4).unwrap();
    let before = signed.clone();
    verify_packet_hmac(KEY, &signed).unwrap();
    assert_eq!(signed, before);
    assert_eq!(
        verify_packet_hmac(b"other-key", &signed),
        Err(ProtoError::BadHmac)
    );
}

#[test]
fn packet_verification_requires_a_present_and_complete_hmac_field() {
    // No FLAG_HMAC: there is no field to verify.
    let unauthenticated = raw(0, &token_and_sequence(), 16);
    assert_eq!(
        verify_packet_hmac(KEY, &unauthenticated),
        Err(ProtoError::MissingFlag(FLAG_HMAC))
    );

    // Flagged but truncated inside the field.
    let truncated = raw(FLAG_HMAC, &[], 12);
    assert_eq!(
        verify_packet_hmac(KEY, &truncated),
        Err(ProtoError::InvalidHmacOffset)
    );
    // Structural decoding rejects it too, for the kind's minimum length.
    assert_eq!(
        decode_request(&truncated),
        Err(ProtoError::PacketTooShort {
            needed: 32,
            actual: 12,
        })
    );

    // A malformed header is rejected before any MAC work.
    assert_eq!(
        verify_packet_hmac(KEY, &[0x00, MAGIC[1], MAGIC[2], FLAG_HMAC]),
        Err(ProtoError::BadMagic)
    );
}

// ---------- Encode -> decode cross-checks ----------

#[test]
fn encoded_requests_decode_back_to_their_sender_side_identity() {
    for key in [None, Some(KEY)] {
        for no_test in [false, true] {
            let packet = encode_request(
                RequestToEncode::Open {
                    params: &params(),
                    no_test,
                },
                key,
            )
            .unwrap();
            let request = decode_request(&packet).unwrap();
            assert_eq!(request.hmac_present, key.is_some());
            match request.kind {
                DecodedRequestKind::Open {
                    no_test: decoded,
                    params: encoded,
                } => {
                    assert_eq!(decoded, no_test);
                    assert_eq!(Params::decode(encoded).unwrap(), params());
                }
                other => panic!("expected an open request, got {other:?}"),
            }
            if let Some(key) = key {
                verify_packet_hmac(key, &packet).unwrap();
            }
        }

        let packet = encode_request(RequestToEncode::Close { token: TOKEN }, key).unwrap();
        let request = decode_request(&packet).unwrap();
        assert_eq!(request.hmac_present, key.is_some());
        assert_eq!(request.kind, DecodedRequestKind::Close { token: TOKEN });

        // The ECHO tail is deliberately not compared against the logical
        // payload: the sender placed it at a negotiated offset the receiver
        // cannot recover without `Params`.
        let echo_params = Params {
            length: 96,
            ..params()
        };
        let packet = encode_request(
            RequestToEncode::Echo {
                token: TOKEN,
                sequence: SEQUENCE,
                params: &echo_params,
                payload: &[1, 2, 3, 4],
            },
            key,
        )
        .unwrap();
        let request = decode_request(&packet).unwrap();
        assert_eq!(request.hmac_present, key.is_some());
        match request.kind {
            DecodedRequestKind::Echo {
                token, sequence, ..
            } => {
                assert_eq!(token, TOKEN);
                assert_eq!(sequence, SEQUENCE);
            }
            other => panic!("expected an echo request, got {other:?}"),
        }
        if let Some(key) = key {
            verify_packet_hmac(key, &packet).unwrap();
        }
    }
}

#[test]
fn echo_encoding_rejects_a_negative_negotiated_length() {
    let params = Params {
        length: -1,
        ..Params::default()
    };
    assert_eq!(
        encode_request(
            RequestToEncode::Echo {
                token: TOKEN,
                sequence: SEQUENCE,
                params: &params,
                payload: &[],
            },
            None,
        ),
        Err(ProtoError::NegativePacketLength { length: -1 })
    );
}
