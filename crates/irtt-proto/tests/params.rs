use irtt_proto::{
    varint, Clock, ParamPresence, Params, ProtoError, ReceivedStats, ServerFill, StampAt,
};
use proptest::prelude::*;

fn int(tag: u64, value: i64) -> Vec<u8> {
    let mut out = Vec::new();
    varint::encode_uvarint(tag, &mut out);
    varint::encode_varint(value, &mut out);
    out
}

fn fill(value: &[u8]) -> Vec<u8> {
    let mut out = vec![9];
    varint::encode_uvarint(value.len() as u64, &mut out);
    out.extend_from_slice(value);
    out
}

fn presence(p: ParamPresence) -> [bool; 9] {
    [
        p.protocol_version,
        p.duration_ns,
        p.interval_ns,
        p.length,
        p.received_stats,
        p.stamp_at,
        p.clock,
        p.dscp,
        p.server_fill,
    ]
}

fn assert_error(bytes: &[u8], expected: ProtoError) {
    assert_eq!(Params::decode(bytes).unwrap_err(), expected, "{bytes:02x?}");
    assert_eq!(
        Params::decode_with_presence(bytes).unwrap_err(),
        expected,
        "{bytes:02x?}"
    );
}

#[test]
fn known_encodings_preserve_raw_dscp_and_utf8_byte_lengths() {
    for (params, bytes) in [
        (
            Params {
                dscp: 46,
                ..Params::default()
            },
            vec![8, 92],
        ),
        (
            Params {
                dscp: 184,
                ..Params::default()
            },
            vec![8, 0xf0, 2],
        ),
        (
            Params {
                server_fill: Some(ServerFill {
                    value: "rand".into(),
                }),
                ..Params::default()
            },
            vec![9, 4, b'r', b'a', b'n', b'd'],
        ),
        (
            Params {
                server_fill: Some(ServerFill { value: "é".into() }),
                ..Params::default()
            },
            vec![9, 2, 0xc3, 0xa9],
        ),
    ] {
        assert_eq!(params.encode(), bytes);
        assert_eq!(Params::decode(&bytes).unwrap(), params);
    }
}

#[test]
fn wire_defaults_and_explicit_zero_presence() {
    let defaults = Params {
        protocol_version: 0,
        duration_ns: 0,
        interval_ns: 0,
        length: 0,
        received_stats: ReceivedStats::None,
        stamp_at: StampAt::None,
        clock: Clock::Unspecified,
        dscp: 0,
        server_fill: None,
    };
    let absent = Params::decode_with_presence(&[]).unwrap();
    assert_eq!(absent.params, defaults);
    assert_eq!(presence(absent.presence), [false; 9]);
    assert_eq!(defaults.encode(), Vec::<u8>::new());
    assert!(!defaults.clock.has_wall());
    assert!(!defaults.clock.has_mono());

    // Clock is the sole integer tag for which explicit zero is invalid.
    for tag in [1, 2, 3, 4, 5, 6, 8] {
        let bytes = int(tag, 0);
        let decoded = Params::decode_with_presence(&bytes).unwrap();
        assert_eq!(decoded.params, defaults);
        let mut expected = [false; 9];
        expected[tag as usize - 1] = true;
        assert_eq!(presence(decoded.presence), expected, "tag {tag}");
        assert_eq!(Params::decode(&bytes).unwrap(), decoded.params);
    }
    let empty_fill = Params::decode_with_presence(&[9, 0]).unwrap();
    assert_eq!(
        empty_fill.params.server_fill,
        Some(ServerFill {
            value: String::new()
        })
    );
    assert_eq!(
        presence(empty_fill.presence),
        [false, false, false, false, false, false, false, false, true]
    );
    assert_eq!(empty_fill.params.encode(), [9, 0]);
    assert_eq!(
        Params {
            protocol_version: 1,
            ..defaults
        }
        .encode(),
        [1, 2]
    );
}

#[test]
fn known_tags_map_to_their_fields_and_report_isolated_presence() {
    for (tag, value, expected) in [
        (
            1,
            1,
            Params {
                protocol_version: 1,
                ..Params::default()
            },
        ),
        (
            2,
            5,
            Params {
                duration_ns: 5,
                ..Params::default()
            },
        ),
        (
            3,
            6,
            Params {
                interval_ns: 6,
                ..Params::default()
            },
        ),
        (
            4,
            7,
            Params {
                length: 7,
                ..Params::default()
            },
        ),
        (
            8,
            184,
            Params {
                dscp: 184,
                ..Params::default()
            },
        ),
    ] {
        let decoded = Params::decode_with_presence(&int(tag, value)).unwrap();
        assert_eq!(decoded.params, expected);
        let mut expected_presence = [false; 9];
        expected_presence[tag as usize - 1] = true;
        assert_eq!(presence(decoded.presence), expected_presence);
    }
    for (tag, modes) in [
        (
            5,
            vec![
                ReceivedStats::None as i64,
                ReceivedStats::Count as i64,
                ReceivedStats::Window as i64,
                ReceivedStats::Both as i64,
            ],
        ),
        (
            6,
            vec![
                StampAt::None as i64,
                StampAt::Send as i64,
                StampAt::Receive as i64,
                StampAt::Both as i64,
                StampAt::Midpoint as i64,
            ],
        ),
        (
            7,
            vec![
                Clock::Wall as i64,
                Clock::Monotonic as i64,
                Clock::Both as i64,
            ],
        ),
    ] {
        for (index, mode) in modes.into_iter().enumerate() {
            let value = index as i64 + if tag == 7 { 1 } else { 0 };
            let decoded = Params::decode_with_presence(&int(tag, value)).unwrap();
            let actual = match tag {
                5 => decoded.params.received_stats as i64,
                6 => decoded.params.stamp_at as i64,
                7 => decoded.params.clock as i64,
                _ => unreachable!(),
            };
            assert_eq!(actual, mode);
            let mut expected_presence = [false; 9];
            expected_presence[tag as usize - 1] = true;
            assert_eq!(presence(decoded.presence), expected_presence);
        }
    }
}

#[test]
fn malformed_values_and_invalid_enums_are_rejected_by_both_decoders() {
    for (tag, value, name) in [
        (5, 4, "ReceivedStats"),
        (6, 5, "StampAt"),
        (7, 0, "Clock"),
        (7, 4, "Clock"),
        (5, -1, "ReceivedStats"),
        (6, -1, "StampAt"),
        (7, -1, "Clock"),
    ] {
        assert_error(&int(tag, value), ProtoError::InvalidEnum { name, value });
    }
    for bytes in [&[1, 0x80][..], &[99], &[0x80], &[9, 0x80]] {
        assert_error(bytes, ProtoError::TruncatedVarint);
    }
}

#[test]
fn server_fill_accepts_valid_bytes_and_enforces_length_and_utf8_boundaries() {
    for value in [
        "",
        "rand",
        "0123456789abcdef0123456789abcdef",
        "éééééééééééééééé",
    ] {
        let expected = Params {
            server_fill: Some(ServerFill {
                value: value.into(),
            }),
            ..Params::default()
        };
        let bytes = fill(value.as_bytes());
        assert_eq!(expected.encode(), bytes);
        let decoded = Params::decode_with_presence(&bytes).unwrap();
        assert_eq!(decoded.params, expected);
        assert!(decoded.presence.server_fill);
        assert_eq!(Params::decode(&bytes).unwrap(), expected);
    }
    assert_error(
        &fill(b"0123456789abcdef0123456789abcdefx"),
        ProtoError::ParameterLengthTooLarge { tag: 9, length: 33 },
    );
    assert_error(&[9, 4, b'a', b'b'], ProtoError::MalformedParams);
    assert_error(&[9, 1, 0xff], ProtoError::InvalidUtf8);
}

#[cfg(target_pointer_width = "32")]
#[test]
fn server_fill_length_too_large_for_usize_is_rejected() {
    let mut bytes = vec![9];
    let length = u64::from(u32::MAX) + 1;
    varint::encode_uvarint(length, &mut bytes);
    assert_error(
        &bytes,
        ProtoError::ParameterLengthTooLarge { tag: 9, length },
    );
}

fn string_strategy() -> impl Strategy<Value = String> {
    // Eight arbitrary Unicode scalars occupy at most the allowed 32 bytes.
    proptest::collection::vec(any::<char>(), 0..=8).prop_map(|chars| chars.into_iter().collect())
}

fn params_strategy() -> impl Strategy<Value = Params> {
    (
        any::<[i64; 5]>(),
        prop::sample::select(vec![
            ReceivedStats::None,
            ReceivedStats::Count,
            ReceivedStats::Window,
            ReceivedStats::Both,
        ]),
        prop::sample::select(vec![
            StampAt::None,
            StampAt::Send,
            StampAt::Receive,
            StampAt::Both,
            StampAt::Midpoint,
        ]),
        prop::sample::select(vec![
            Clock::Unspecified,
            Clock::Wall,
            Clock::Monotonic,
            Clock::Both,
        ]),
        prop::option::of(string_strategy()),
    )
        .prop_map(
            |(scalar, received_stats, stamp_at, clock, server_fill)| Params {
                protocol_version: scalar[0],
                duration_ns: scalar[1],
                interval_ns: scalar[2],
                length: scalar[3],
                dscp: scalar[4],
                received_stats,
                stamp_at,
                clock,
                server_fill: server_fill.map(|value| ServerFill { value }),
            },
        )
}

fn all_tags(params: &Params) -> Vec<u8> {
    let mut out = Vec::new();
    // Deliberately bypass encode's omission: duplicate zero values must be present.
    for (tag, value) in [
        (1, params.protocol_version),
        (2, params.duration_ns),
        (3, params.interval_ns),
        (4, params.length),
        (5, params.received_stats as i64),
        (6, params.stamp_at as i64),
        (7, params.clock as i64),
        (8, params.dscp),
    ] {
        out.extend_from_slice(&int(tag, value));
    }
    out.extend_from_slice(&fill(params.server_fill.as_ref().unwrap().value.as_bytes()));
    out
}

proptest! {
    #[test]
    fn valid_params_round_trip_with_omission_presence(params in params_strategy()) {
        let encoded = params.encode();
        let decoded = Params::decode_with_presence(&encoded).unwrap();
        prop_assert_eq!(&decoded.params, &params);
        prop_assert_eq!(Params::decode(&encoded).unwrap(), params.clone());
        prop_assert_eq!(presence(decoded.presence), [
            params.protocol_version != 0, params.duration_ns != 0,
            params.interval_ns != 0, params.length != 0,
            params.received_stats != ReceivedStats::None, params.stamp_at != StampAt::None,
            params.clock != Clock::Unspecified, params.dscp != 0, params.server_fill.is_some(),
        ]);
    }

    #[test]
    fn repeated_known_tags_are_last_wins(
        mut first in params_strategy(), mut second in params_strategy(),
        first_clock in 1_i64..=3, second_clock in 1_i64..=3,
        first_fill in string_strategy(), second_fill in string_strategy(),
    ) {
        first.clock = Clock::try_from(first_clock).unwrap();
        second.clock = Clock::try_from(second_clock).unwrap();
        first.server_fill = Some(ServerFill { value: first_fill });
        second.server_fill = Some(ServerFill { value: second_fill });
        let mut bytes = all_tags(&first);
        bytes.extend_from_slice(&all_tags(&second));
        let decoded = Params::decode_with_presence(&bytes).unwrap();
        prop_assert_eq!(&decoded.params, &second);
        prop_assert_eq!(Params::decode(&bytes).unwrap(), second);
        prop_assert_eq!(presence(decoded.presence), [true; 9]);
    }

    #[test]
    fn unknown_scalar_tags_do_not_disturb_known_params(
        params in params_strategy(), tag in 10_u64..=u16::MAX as u64, value: u64,
    ) {
        let encoded = params.encode();
        let expected = Params::decode_with_presence(&encoded).unwrap();
        let mut unknown = Vec::new();
        varint::encode_uvarint(tag, &mut unknown);
        varint::encode_uvarint(value, &mut unknown);
        for bytes in [[unknown.as_slice(), &encoded].concat(), [&encoded, unknown.as_slice()].concat()] {
            prop_assert_eq!(Params::decode_with_presence(&bytes).unwrap(), expected.clone());
            prop_assert_eq!(Params::decode(&bytes).unwrap(), params.clone());
        }
    }
}
