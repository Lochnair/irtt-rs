use crate::{varint, ProtoError, Result, PROTOCOL_VERSION};

/// Protocol compatibility bound for an encoded `server_fill` parameter.
pub const MAX_SERVER_FILL_BYTES: usize = 32;

/// Low-level wire representation of IRTT open parameters.
///
/// `Params` mirrors the protocol fields closely and can be constructed
/// directly by callers. Direct construction can therefore produce values that
/// should not be sent on the wire, such as an oversized `server_fill` value.
/// Higher-level callers should validate user or configuration input before
/// encoding. The normal `irtt-client` configuration path enforces
/// [`MAX_SERVER_FILL_BYTES`] for `server_fill`.
///
/// [`Params::default`] is the **wire** default: every integer field is zero and
/// [`clock`](Params::clock) is [`Clock::Unspecified`], which is what an open
/// request with an empty parameter payload means. It is not a set of sensible
/// client settings; a client builds its request from its own configuration.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Params {
    pub protocol_version: i64,
    pub duration_ns: i64,
    pub interval_ns: i64,
    pub length: i64,
    pub received_stats: ReceivedStats,
    pub stamp_at: StampAt,
    pub clock: Clock,
    /// Raw IP TOS / Traffic Class byte (`0..=255`), not a six-bit DSCP
    /// codepoint. A codepoint occupies the upper six bits of this byte;
    /// callers that accept a codepoint from users or configuration must shift
    /// it left by two before assigning it here. `encode`/`decode` carry this
    /// value as-is and never apply that shift themselves.
    pub dscp: i64,
    pub server_fill: Option<ServerFill>,
}

impl Params {
    pub fn with_protocol_defaults() -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            ..Self::default()
        }
    }

    /// Encodes these parameters without performing additional validation.
    ///
    /// Callers that construct `Params` directly are responsible for validating
    /// user or configuration input before encoding.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        push_int(1, self.protocol_version, &mut out);
        push_int(2, self.duration_ns, &mut out);
        push_int(3, self.interval_ns, &mut out);
        push_int(4, self.length, &mut out);
        push_int(5, self.received_stats as i64, &mut out);
        push_int(6, self.stamp_at as i64, &mut out);
        push_int(7, self.clock as i64, &mut out);
        push_int(8, self.dscp, &mut out);
        if let Some(fill) = &self.server_fill {
            varint::encode_uvarint(9, &mut out);
            varint::encode_uvarint(fill.value.len() as u64, &mut out);
            out.extend_from_slice(fill.value.as_bytes());
        }
        out
    }

    /// Decodes parameters and rejects malformed or incompatible incoming values.
    ///
    /// This includes invalid enum values, malformed UTF-8, and `server_fill`
    /// values longer than [`MAX_SERVER_FILL_BYTES`].
    ///
    /// Absent parameters take their wire default, so a caller cannot tell an
    /// omitted tag from one explicitly encoded as zero. Use
    /// [`decode_with_presence`](Params::decode_with_presence) when that
    /// distinction matters; both share one parser.
    pub fn decode(input: &[u8]) -> Result<Self> {
        Self::decode_with_presence(input).map(|decoded| decoded.params)
    }

    /// Decodes parameters and additionally reports which known tags appeared.
    ///
    /// This is the same parser and the same validation as [`decode`], with the
    /// presence of each known tag retained. A receiver needs it because the
    /// protocol gives an omitted tag and an explicit zero the same value but
    /// not the same meaning: an absent Duration or Interval is accepted as the
    /// wire default zero, while one explicitly encoded as zero is invalid.
    ///
    /// Presence means the tag appeared at least once. Repeated known tags keep
    /// last-value-wins, and unknown tags remain ignored and untracked.
    ///
    /// [`decode`]: Params::decode
    pub fn decode_with_presence(input: &[u8]) -> Result<DecodedParams> {
        let mut params = Self::default();
        let mut presence = ParamPresence::default();
        let mut pos = 0;
        while pos < input.len() {
            let (tag, used) = varint::decode_uvarint(&input[pos..])?;
            pos += used;
            match tag {
                1 => {
                    params.protocol_version = read_int(input, &mut pos)?;
                    presence.protocol_version = true;
                }
                2 => {
                    params.duration_ns = read_int(input, &mut pos)?;
                    presence.duration_ns = true;
                }
                3 => {
                    params.interval_ns = read_int(input, &mut pos)?;
                    presence.interval_ns = true;
                }
                4 => {
                    params.length = read_int(input, &mut pos)?;
                    presence.length = true;
                }
                5 => {
                    params.received_stats = ReceivedStats::try_from(read_int(input, &mut pos)?)?;
                    presence.received_stats = true;
                }
                6 => {
                    params.stamp_at = StampAt::try_from(read_int(input, &mut pos)?)?;
                    presence.stamp_at = true;
                }
                7 => {
                    params.clock = Clock::try_from(read_int(input, &mut pos)?)?;
                    presence.clock = true;
                }
                8 => {
                    params.dscp = read_int(input, &mut pos)?;
                    presence.dscp = true;
                }
                9 => {
                    let (len, used) = varint::decode_uvarint(&input[pos..])?;
                    pos += used;
                    let len = usize::try_from(len)
                        .map_err(|_| ProtoError::ParameterLengthTooLarge { tag, length: len })?;
                    if len > MAX_SERVER_FILL_BYTES {
                        return Err(ProtoError::ParameterLengthTooLarge {
                            tag,
                            length: len as u64,
                        });
                    }
                    if input.len().saturating_sub(pos) < len {
                        return Err(ProtoError::MalformedParams);
                    }
                    let value = std::str::from_utf8(&input[pos..pos + len])
                        .map_err(|_| ProtoError::InvalidUtf8)?
                        .to_owned();
                    pos += len;
                    params.server_fill = Some(ServerFill { value });
                    presence.server_fill = true;
                }
                _ => {
                    let (_, used) = varint::decode_uvarint(&input[pos..])?;
                    pos += used;
                }
            }
        }
        Ok(DecodedParams { params, presence })
    }
}

/// Decoded parameters together with which known tags the payload carried.
///
/// Produced by [`Params::decode_with_presence`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DecodedParams {
    /// The decoded values. Absent parameters hold their wire default.
    pub params: Params,
    /// Which known tags appeared in the payload.
    pub presence: ParamPresence,
}

/// Which known parameter tags a decoded payload carried.
///
/// A field is `true` when its tag appeared at least once, whatever value it
/// carried. Unknown tags are ignored and are not represented here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ParamPresence {
    pub protocol_version: bool,
    pub duration_ns: bool,
    pub interval_ns: bool,
    pub length: bool,
    pub received_stats: bool,
    pub stamp_at: bool,
    pub clock: bool,
    pub dscp: bool,
    pub server_fill: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerFill {
    pub value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i64)]
pub enum ReceivedStats {
    #[default]
    None = 0,
    Count = 1,
    Window = 2,
    Both = 3,
}

impl ReceivedStats {
    pub fn has_count(self) -> bool {
        matches!(self, Self::Count | Self::Both)
    }

    pub fn has_window(self) -> bool {
        matches!(self, Self::Window | Self::Both)
    }
}

impl TryFrom<i64> for ReceivedStats {
    type Error = ProtoError;

    fn try_from(value: i64) -> Result<Self> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::Count),
            2 => Ok(Self::Window),
            3 => Ok(Self::Both),
            _ => Err(ProtoError::InvalidEnum {
                name: "ReceivedStats",
                value,
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i64)]
pub enum StampAt {
    #[default]
    None = 0,
    Send = 1,
    Receive = 2,
    Both = 3,
    Midpoint = 4,
}

impl TryFrom<i64> for StampAt {
    type Error = ProtoError;

    fn try_from(value: i64) -> Result<Self> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::Send),
            2 => Ok(Self::Receive),
            3 => Ok(Self::Both),
            4 => Ok(Self::Midpoint),
            _ => Err(ProtoError::InvalidEnum {
                name: "StampAt",
                value,
            }),
        }
    }
}

/// Which server clock sources supply timestamp fields.
///
/// # The zero value
///
/// [`Clock::Unspecified`] is the **wire default**, meaning the Clock tag was
/// absent from an open parameter payload — which is valid, and is what an empty
/// payload decodes to. It does not mean a peer may send an explicit Clock tag
/// encoding zero: [`TryFrom<i64>`](Clock::try_from) still rejects an explicit
/// zero as an invalid enum value, so this state is only ever reached by
/// omission. [`encode`](Params::encode) omits the tag for `Unspecified` rather
/// than emitting an explicit zero, which round-trips absence faithfully.
///
/// `Unspecified` selects no clock, so [`has_wall`](Clock::has_wall) and
/// [`has_mono`](Clock::has_mono) are both false for it and no timestamp field
/// is laid out. A client that wants timestamps must request a real clock; the
/// `irtt-client` configuration path rejects `Unspecified`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i64)]
pub enum Clock {
    /// Wire default for an absent Clock tag. Never produced from an explicit
    /// encoded value.
    #[default]
    Unspecified = 0,
    Wall = 1,
    Monotonic = 2,
    Both = 3,
}

impl Clock {
    pub fn has_wall(self) -> bool {
        matches!(self, Self::Wall | Self::Both)
    }

    pub fn has_mono(self) -> bool {
        matches!(self, Self::Monotonic | Self::Both)
    }
}

impl TryFrom<i64> for Clock {
    type Error = ProtoError;

    /// Converts an **explicitly encoded** Clock value.
    ///
    /// Zero is rejected: it is only valid as an absent tag, never as an encoded
    /// one. [`Clock::Unspecified`] is therefore unreachable through this
    /// conversion by design.
    fn try_from(value: i64) -> Result<Self> {
        match value {
            1 => Ok(Self::Wall),
            2 => Ok(Self::Monotonic),
            3 => Ok(Self::Both),
            _ => Err(ProtoError::InvalidEnum {
                name: "Clock",
                value,
            }),
        }
    }
}

fn push_int(tag: u64, value: i64, out: &mut Vec<u8>) {
    if value == 0 {
        return;
    }
    varint::encode_uvarint(tag, out);
    varint::encode_varint(value, out);
}

fn read_int(input: &[u8], pos: &mut usize) -> Result<i64> {
    let (value, used) = varint::decode_varint(&input[*pos..])?;
    *pos += used;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_round_trip(params: Params) {
        assert_eq!(Params::decode(&params.encode()), Ok(params));
    }

    fn encode_server_fill_value(value: &[u8]) -> Vec<u8> {
        let mut encoded = Vec::new();
        varint::encode_uvarint(9, &mut encoded);
        varint::encode_uvarint(value.len() as u64, &mut encoded);
        encoded.extend_from_slice(value);
        encoded
    }

    fn encode_int(tag: u64, value: i64) -> Vec<u8> {
        let mut encoded = Vec::new();
        varint::encode_uvarint(tag, &mut encoded);
        varint::encode_varint(value, &mut encoded);
        encoded
    }

    #[test]
    fn params_round_trip() {
        let params = Params {
            protocol_version: 1,
            duration_ns: 3_000_000_000,
            interval_ns: 1_000_000_000,
            length: 1472,
            received_stats: ReceivedStats::Both,
            stamp_at: StampAt::Both,
            clock: Clock::Both,
            dscp: 184,
            server_fill: Some(ServerFill {
                value: "pattern:abc".to_owned(),
            }),
        };
        assert_round_trip(params);
    }

    #[test]
    fn params_round_trip_negotiated_option_modes() {
        for received_stats in [
            ReceivedStats::None,
            ReceivedStats::Count,
            ReceivedStats::Window,
            ReceivedStats::Both,
        ] {
            assert_round_trip(Params {
                protocol_version: 1,
                received_stats,
                ..Params::default()
            });
        }

        for stamp_at in [
            StampAt::None,
            StampAt::Send,
            StampAt::Receive,
            StampAt::Both,
            StampAt::Midpoint,
        ] {
            assert_round_trip(Params {
                protocol_version: 1,
                stamp_at,
                clock: Clock::Both,
                ..Params::default()
            });
        }

        for clock in [Clock::Wall, Clock::Monotonic, Clock::Both] {
            assert_round_trip(Params {
                protocol_version: 1,
                stamp_at: StampAt::Both,
                clock,
                ..Params::default()
            });
        }
    }

    #[test]
    fn params_round_trip_dscp_values_are_encoded_without_shifting() {
        // `Params::dscp` is a raw wire byte; encode/decode carry whatever
        // value is set without interpreting or shifting it, regardless of
        // whether it happens to look like a codepoint (46) or a raw TOS byte
        // (184).
        for dscp in [0, 46, 63, 64, 184, -1] {
            let params = Params {
                protocol_version: 1,
                dscp,
                ..Params::default()
            };
            assert_round_trip(params);
        }

        let params = Params {
            protocol_version: 1,
            dscp: 46,
            ..Params::default()
        };
        let encoded = params.encode();
        assert!(
            encoded.windows(2).any(|bytes| bytes == [8, 92]),
            "dscp value 46 must be encoded as param value 46"
        );
        assert!(
            !encoded.windows(3).any(|bytes| bytes == [8, 0xf0, 0x02]),
            "Params::encode must not shift dscp 46 to 184; any codepoint shift is a caller concern"
        );
        assert_eq!(Params::decode(&encoded).unwrap().dscp, 46);
    }

    #[test]
    fn server_fill_absent_short_and_max_length_round_trip() {
        assert_round_trip(Params {
            protocol_version: 1,
            server_fill: None,
            ..Params::default()
        });

        assert_round_trip(Params {
            protocol_version: 1,
            server_fill: Some(ServerFill {
                value: "rand".to_owned(),
            }),
            ..Params::default()
        });

        assert_round_trip(Params {
            protocol_version: 1,
            server_fill: Some(ServerFill {
                value: "0123456789abcdef0123456789abcdef".to_owned(),
            }),
            ..Params::default()
        });
    }

    #[test]
    fn server_fill_decode_accepts_max_length() {
        let value = b"0123456789abcdef0123456789abcdef";
        let params = Params::decode(&encode_server_fill_value(value)).unwrap();

        assert_eq!(
            params.server_fill,
            Some(ServerFill {
                value: "0123456789abcdef0123456789abcdef".to_owned(),
            })
        );
    }

    #[test]
    fn server_fill_decode_rejects_oversized_length() {
        let value = b"0123456789abcdef0123456789abcdefx";

        assert_eq!(
            Params::decode(&encode_server_fill_value(value)),
            Err(ProtoError::ParameterLengthTooLarge { tag: 9, length: 33 })
        );
    }

    #[test]
    fn server_fill_tag_and_length_are_encoded_before_utf8_bytes() {
        let params = Params {
            protocol_version: 1,
            server_fill: Some(ServerFill {
                value: "rand".to_owned(),
            }),
            ..Params::default()
        };
        let encoded = params.encode();
        assert!(encoded
            .windows(6)
            .any(|bytes| bytes == [9, 4, b'r', b'a', b'n', b'd']));
    }

    #[test]
    fn unknown_tags_are_ignored() {
        let mut encoded = Vec::new();
        varint::encode_uvarint(99, &mut encoded);
        varint::encode_varint(123, &mut encoded);
        varint::encode_uvarint(1, &mut encoded);
        varint::encode_varint(1, &mut encoded);

        let params = Params::decode(&encoded).unwrap();
        assert_eq!(params.protocol_version, 1);
    }

    #[test]
    fn invalid_received_stats_value_is_rejected() {
        let mut encoded = Vec::new();
        varint::encode_uvarint(5, &mut encoded);
        varint::encode_varint(4, &mut encoded);

        assert_eq!(
            Params::decode(&encoded),
            Err(ProtoError::InvalidEnum {
                name: "ReceivedStats",
                value: 4,
            })
        );
    }

    #[test]
    fn invalid_timestamp_value_is_rejected() {
        let mut encoded = Vec::new();
        varint::encode_uvarint(6, &mut encoded);
        varint::encode_varint(5, &mut encoded);

        assert_eq!(
            Params::decode(&encoded),
            Err(ProtoError::InvalidEnum {
                name: "StampAt",
                value: 5,
            })
        );
    }

    #[test]
    fn explicit_clock_zero_is_rejected() {
        let mut encoded = Vec::new();
        varint::encode_uvarint(7, &mut encoded);
        varint::encode_varint(0, &mut encoded);

        assert_eq!(
            Params::decode(&encoded),
            Err(ProtoError::InvalidEnum {
                name: "Clock",
                value: 0,
            })
        );
        assert_eq!(
            Params::decode_with_presence(&encoded),
            Err(ProtoError::InvalidEnum {
                name: "Clock",
                value: 0,
            })
        );
    }

    #[test]
    fn explicit_clock_values_one_to_three_are_accepted() {
        for (value, expected) in [(1, Clock::Wall), (2, Clock::Monotonic), (3, Clock::Both)] {
            let decoded = Params::decode_with_presence(&encode_int(7, value)).unwrap();
            assert_eq!(decoded.params.clock, expected);
            assert!(decoded.presence.clock);
        }
    }

    #[test]
    fn empty_payload_decodes_to_wire_defaults_with_nothing_present() {
        let decoded = Params::decode_with_presence(&[]).unwrap();

        assert_eq!(decoded.params, Params::default());
        assert_eq!(decoded.params.protocol_version, 0);
        assert_eq!(decoded.params.duration_ns, 0);
        assert_eq!(decoded.params.interval_ns, 0);
        assert_eq!(decoded.params.length, 0);
        assert_eq!(decoded.params.dscp, 0);
        assert_eq!(decoded.params.received_stats, ReceivedStats::None);
        assert_eq!(decoded.params.stamp_at, StampAt::None);
        // The wire default clock is zero, not `Both`.
        assert_eq!(decoded.params.clock, Clock::Unspecified);
        assert!(!decoded.params.clock.has_wall());
        assert!(!decoded.params.clock.has_mono());
        assert_eq!(decoded.params.server_fill, None);

        assert_eq!(decoded.presence, ParamPresence::default());
        assert_eq!(
            decoded.presence,
            ParamPresence {
                protocol_version: false,
                duration_ns: false,
                interval_ns: false,
                length: false,
                received_stats: false,
                stamp_at: false,
                clock: false,
                dscp: false,
                server_fill: false,
            }
        );
    }

    #[test]
    fn absent_and_explicitly_zero_parameters_share_a_value_but_not_a_presence() {
        // Duration and Interval are the pair a server must tell apart: absent is
        // accepted, explicitly zero is not.
        let absent = Params::decode_with_presence(&encode_int(1, 1)).unwrap();
        assert_eq!(absent.params.duration_ns, 0);
        assert!(!absent.presence.duration_ns);
        assert_eq!(absent.params.interval_ns, 0);
        assert!(!absent.presence.interval_ns);
        assert_eq!(absent.params.clock, Clock::Unspecified);
        assert!(!absent.presence.clock);

        let explicit_duration = Params::decode_with_presence(&encode_int(2, 0)).unwrap();
        assert_eq!(explicit_duration.params.duration_ns, 0);
        assert!(explicit_duration.presence.duration_ns);

        let explicit_interval = Params::decode_with_presence(&encode_int(3, 0)).unwrap();
        assert_eq!(explicit_interval.params.interval_ns, 0);
        assert!(explicit_interval.presence.interval_ns);
    }

    #[test]
    fn every_known_tag_reports_its_own_presence() {
        let mut encoded = Vec::new();
        for (tag, value) in [
            (1, 1),
            (2, 5),
            (3, 6),
            (4, 7),
            (5, 3),
            (6, 4),
            (7, 3),
            (8, 184),
        ] {
            encoded.extend_from_slice(&encode_int(tag, value));
        }
        encoded.extend_from_slice(&encode_server_fill_value(b"rand"));

        let decoded = Params::decode_with_presence(&encoded).unwrap();
        assert_eq!(
            decoded.presence,
            ParamPresence {
                protocol_version: true,
                duration_ns: true,
                interval_ns: true,
                length: true,
                received_stats: true,
                stamp_at: true,
                clock: true,
                dscp: true,
                server_fill: true,
            }
        );
    }

    #[test]
    fn unspecified_clock_is_encoded_by_omission() {
        let params = Params {
            protocol_version: 1,
            clock: Clock::Unspecified,
            ..Params::default()
        };
        let encoded = params.encode();

        assert_eq!(encoded, encode_int(1, 1));
        assert!(
            !encoded.windows(2).any(|bytes| bytes[0] == 7),
            "an unspecified clock must omit the tag rather than encode an explicit zero"
        );
        assert_round_trip(params);
    }

    #[test]
    fn decode_and_decode_with_presence_agree() {
        let mut payloads = vec![
            Vec::new(),
            encode_int(1, 1),
            encode_int(2, 0),
            encode_server_fill_value(b"rand"),
        ];
        payloads.push(
            Params {
                protocol_version: 1,
                duration_ns: 3_000_000_000,
                interval_ns: 1_000_000_000,
                length: 1472,
                received_stats: ReceivedStats::Both,
                stamp_at: StampAt::Both,
                clock: Clock::Both,
                dscp: 184,
                server_fill: Some(ServerFill {
                    value: "rand".to_owned(),
                }),
            }
            .encode(),
        );
        // Malformed payloads must fail identically through both entry points.
        payloads.push(vec![1, 0x80]);
        payloads.push(encode_int(7, 0));

        for payload in payloads {
            assert_eq!(
                Params::decode(&payload),
                Params::decode_with_presence(&payload).map(|decoded| decoded.params),
                "decode and decode_with_presence disagreed on {payload:02x?}"
            );
        }
    }

    #[test]
    fn malformed_server_fill_is_rejected() {
        let encoded = [9, 4, b'a', b'b'];
        assert_eq!(Params::decode(&encoded), Err(ProtoError::MalformedParams));
    }

    #[test]
    fn non_utf8_server_fill_is_rejected() {
        let encoded = [9, 1, 0xff];
        assert_eq!(Params::decode(&encoded), Err(ProtoError::InvalidUtf8));
    }

    #[cfg(target_pointer_width = "32")]
    #[test]
    fn server_fill_length_too_large_for_usize_is_rejected() {
        let mut encoded = Vec::new();
        varint::encode_uvarint(9, &mut encoded);
        varint::encode_uvarint(u64::from(u32::MAX) + 1, &mut encoded);

        assert_eq!(
            Params::decode(&encoded),
            Err(ProtoError::ParameterLengthTooLarge {
                tag: 9,
                length: u64::from(u32::MAX) + 1,
            })
        );
    }

    #[test]
    fn truncated_varint_parameter_is_rejected() {
        let encoded = [1, 0x80];
        assert_eq!(Params::decode(&encoded), Err(ProtoError::TruncatedVarint));
    }

    #[test]
    fn truncated_unknown_parameter_value_is_rejected() {
        let encoded = [99];
        assert_eq!(Params::decode(&encoded), Err(ProtoError::TruncatedVarint));
    }

    mod properties {
        use super::*;
        use proptest::prelude::*;

        fn received_stats_strategy() -> impl Strategy<Value = ReceivedStats> {
            prop_oneof![
                Just(ReceivedStats::None),
                Just(ReceivedStats::Count),
                Just(ReceivedStats::Window),
                Just(ReceivedStats::Both),
            ]
        }

        fn stamp_at_strategy() -> impl Strategy<Value = StampAt> {
            prop_oneof![
                Just(StampAt::None),
                Just(StampAt::Send),
                Just(StampAt::Receive),
                Just(StampAt::Both),
                Just(StampAt::Midpoint),
            ]
        }

        /// A `Clock` valid for direct construction, including `Unspecified`
        /// (wire omission). Never produces an explicit zero on the wire; see
        /// `explicit_clock_strategy` for that.
        fn clock_strategy() -> impl Strategy<Value = Clock> {
            prop_oneof![
                Just(Clock::Unspecified),
                Just(Clock::Wall),
                Just(Clock::Monotonic),
                Just(Clock::Both),
            ]
        }

        /// A `Clock` value valid to encode *explicitly* on the wire (tag 7).
        /// Excludes `Unspecified`/zero, which is only ever reached by omission.
        fn explicit_clock_value_strategy() -> impl Strategy<Value = i64> {
            1_i64..=3
        }

        fn server_fill_strategy() -> impl Strategy<Value = Option<ServerFill>> {
            prop_oneof![
                Just(None),
                proptest::collection::vec(proptest::char::any(), 0..=8).prop_map(|chars| Some(
                    ServerFill {
                        value: chars.into_iter().collect(),
                    }
                )),
            ]
        }

        fn params_strategy() -> impl Strategy<Value = Params> {
            (
                any::<i64>(),
                any::<i64>(),
                any::<i64>(),
                any::<i64>(),
                any::<i64>(),
                received_stats_strategy(),
                stamp_at_strategy(),
                clock_strategy(),
                server_fill_strategy(),
            )
                .prop_map(
                    |(
                        protocol_version,
                        duration_ns,
                        interval_ns,
                        length,
                        dscp,
                        received_stats,
                        stamp_at,
                        clock,
                        server_fill,
                    )| Params {
                        protocol_version,
                        duration_ns,
                        interval_ns,
                        length,
                        received_stats,
                        stamp_at,
                        clock,
                        dscp,
                        server_fill,
                    },
                )
        }

        fn encode_tagged_int(tag: u64, value: i64, out: &mut Vec<u8>) {
            varint::encode_uvarint(tag, out);
            varint::encode_varint(value, out);
        }

        fn encode_tagged_server_fill(value: &str, out: &mut Vec<u8>) {
            varint::encode_uvarint(9, out);
            varint::encode_uvarint(value.len() as u64, out);
            out.extend_from_slice(value.as_bytes());
        }

        proptest! {
            #[test]
            fn valid_params_round_trip(params in params_strategy()) {
                let encoded = params.encode();
                let decoded = Params::decode(&encoded).unwrap();
                prop_assert_eq!(decoded, params);
            }

            #[test]
            fn encoded_param_presence_matches_omission_rules(params in params_strategy()) {
                let encoded = params.encode();
                let decoded = Params::decode_with_presence(&encoded).unwrap();
                prop_assert_eq!(&decoded.params, &params);

                prop_assert_eq!(decoded.presence.protocol_version, params.protocol_version != 0);
                prop_assert_eq!(decoded.presence.duration_ns, params.duration_ns != 0);
                prop_assert_eq!(decoded.presence.interval_ns, params.interval_ns != 0);
                prop_assert_eq!(decoded.presence.length, params.length != 0);
                prop_assert_eq!(decoded.presence.dscp, params.dscp != 0);
                prop_assert_eq!(
                    decoded.presence.received_stats,
                    params.received_stats != ReceivedStats::None
                );
                prop_assert_eq!(decoded.presence.stamp_at, params.stamp_at != StampAt::None);
                prop_assert_eq!(decoded.presence.clock, params.clock != Clock::Unspecified);
                prop_assert_eq!(decoded.presence.server_fill, params.server_fill.is_some());
            }

            #[test]
            fn repeated_protocol_version_is_last_wins(first: i64, second: i64) {
                let mut encoded = Vec::new();
                encode_tagged_int(1, first, &mut encoded);
                encode_tagged_int(1, second, &mut encoded);
                let decoded = Params::decode_with_presence(&encoded).unwrap();
                prop_assert_eq!(decoded.params.protocol_version, second);
                prop_assert!(decoded.presence.protocol_version);
            }

            #[test]
            fn repeated_duration_ns_is_last_wins(first: i64, second: i64) {
                let mut encoded = Vec::new();
                encode_tagged_int(2, first, &mut encoded);
                encode_tagged_int(2, second, &mut encoded);
                let decoded = Params::decode_with_presence(&encoded).unwrap();
                prop_assert_eq!(decoded.params.duration_ns, second);
                prop_assert!(decoded.presence.duration_ns);
            }

            #[test]
            fn repeated_interval_ns_is_last_wins(first: i64, second: i64) {
                let mut encoded = Vec::new();
                encode_tagged_int(3, first, &mut encoded);
                encode_tagged_int(3, second, &mut encoded);
                let decoded = Params::decode_with_presence(&encoded).unwrap();
                prop_assert_eq!(decoded.params.interval_ns, second);
                prop_assert!(decoded.presence.interval_ns);
            }

            #[test]
            fn repeated_length_is_last_wins(first: i64, second: i64) {
                let mut encoded = Vec::new();
                encode_tagged_int(4, first, &mut encoded);
                encode_tagged_int(4, second, &mut encoded);
                let decoded = Params::decode_with_presence(&encoded).unwrap();
                prop_assert_eq!(decoded.params.length, second);
                prop_assert!(decoded.presence.length);
            }

            #[test]
            fn repeated_received_stats_is_last_wins(first in 0_i64..=3, second in 0_i64..=3) {
                let mut encoded = Vec::new();
                encode_tagged_int(5, first, &mut encoded);
                encode_tagged_int(5, second, &mut encoded);
                let decoded = Params::decode_with_presence(&encoded).unwrap();
                prop_assert_eq!(decoded.params.received_stats, ReceivedStats::try_from(second).unwrap());
                prop_assert!(decoded.presence.received_stats);
            }

            #[test]
            fn repeated_stamp_at_is_last_wins(first in 0_i64..=4, second in 0_i64..=4) {
                let mut encoded = Vec::new();
                encode_tagged_int(6, first, &mut encoded);
                encode_tagged_int(6, second, &mut encoded);
                let decoded = Params::decode_with_presence(&encoded).unwrap();
                prop_assert_eq!(decoded.params.stamp_at, StampAt::try_from(second).unwrap());
                prop_assert!(decoded.presence.stamp_at);
            }

            #[test]
            fn repeated_clock_is_last_wins(
                first in explicit_clock_value_strategy(),
                second in explicit_clock_value_strategy(),
            ) {
                let mut encoded = Vec::new();
                encode_tagged_int(7, first, &mut encoded);
                encode_tagged_int(7, second, &mut encoded);
                let decoded = Params::decode_with_presence(&encoded).unwrap();
                prop_assert_eq!(decoded.params.clock, Clock::try_from(second).unwrap());
                prop_assert!(decoded.presence.clock);
            }

            #[test]
            fn repeated_dscp_is_last_wins(first: i64, second: i64) {
                let mut encoded = Vec::new();
                encode_tagged_int(8, first, &mut encoded);
                encode_tagged_int(8, second, &mut encoded);
                let decoded = Params::decode_with_presence(&encoded).unwrap();
                prop_assert_eq!(decoded.params.dscp, second);
                prop_assert!(decoded.presence.dscp);
            }

            #[test]
            fn repeated_server_fill_is_last_wins(
                first in proptest::collection::vec(proptest::char::any(), 0..=8)
                    .prop_map(|chars| chars.into_iter().collect::<String>()),
                second in proptest::collection::vec(proptest::char::any(), 0..=8)
                    .prop_map(|chars| chars.into_iter().collect::<String>()),
            ) {
                let mut encoded = Vec::new();
                encode_tagged_server_fill(&first, &mut encoded);
                encode_tagged_server_fill(&second, &mut encoded);
                let decoded = Params::decode_with_presence(&encoded).unwrap();
                prop_assert_eq!(
                    decoded.params.server_fill,
                    Some(ServerFill { value: second })
                );
                prop_assert!(decoded.presence.server_fill);
            }

            #[test]
            fn unknown_scalar_tag_prefix_does_not_disturb_known_params(
                params in params_strategy(),
                unknown_tag in 10_u64..=u16::MAX as u64,
                unknown_value: u64,
            ) {
                let mut prefixed = Vec::new();
                varint::encode_uvarint(unknown_tag, &mut prefixed);
                varint::encode_uvarint(unknown_value, &mut prefixed);
                prefixed.extend_from_slice(&params.encode());

                let decoded = Params::decode(&prefixed).unwrap();
                prop_assert_eq!(decoded, params.clone());

                let decoded_with_presence = Params::decode_with_presence(&prefixed).unwrap();
                let expected_presence = Params::decode_with_presence(&params.encode()).unwrap().presence;
                prop_assert_eq!(decoded_with_presence.presence, expected_presence);
            }

            #[test]
            fn unknown_scalar_tag_suffix_does_not_disturb_known_params(
                params in params_strategy(),
                unknown_tag in 10_u64..=u16::MAX as u64,
                unknown_value: u64,
            ) {
                let mut suffixed = params.encode();
                varint::encode_uvarint(unknown_tag, &mut suffixed);
                varint::encode_uvarint(unknown_value, &mut suffixed);

                let decoded = Params::decode(&suffixed).unwrap();
                prop_assert_eq!(decoded, params.clone());

                let decoded_with_presence = Params::decode_with_presence(&suffixed).unwrap();
                let expected_presence = Params::decode_with_presence(&params.encode()).unwrap().presence;
                prop_assert_eq!(decoded_with_presence.presence, expected_presence);
            }
        }

        /// Omitted-vs-explicit-zero presence, for the fields where explicit
        /// zero is valid on the wire. `Clock` is deliberately excluded: an
        /// explicit zero there is rejected outright, not merely "present with
        /// the default value" (see `explicit_clock_zero_is_rejected`).
        #[test]
        fn omitted_vs_explicit_zero_presence_for_zero_valid_fields() {
            let scalar_tags = [1_u64, 2, 3, 4, 8];
            for tag in scalar_tags {
                let omitted = Params::decode_with_presence(&[]).unwrap();
                let mut explicit_zero = Vec::new();
                encode_tagged_int(tag, 0, &mut explicit_zero);
                let explicit = Params::decode_with_presence(&explicit_zero).unwrap();

                assert_eq!(omitted.params, explicit.params);
                assert_eq!(explicit.params, Params::default());

                let (omitted_presence, explicit_presence) = match tag {
                    1 => (
                        omitted.presence.protocol_version,
                        explicit.presence.protocol_version,
                    ),
                    2 => (omitted.presence.duration_ns, explicit.presence.duration_ns),
                    3 => (omitted.presence.interval_ns, explicit.presence.interval_ns),
                    4 => (omitted.presence.length, explicit.presence.length),
                    8 => (omitted.presence.dscp, explicit.presence.dscp),
                    _ => unreachable!(),
                };
                assert!(!omitted_presence);
                assert!(explicit_presence);
            }

            let enum_tags = [5_u64, 6];
            for tag in enum_tags {
                let omitted = Params::decode_with_presence(&[]).unwrap();
                let mut explicit_zero = Vec::new();
                encode_tagged_int(tag, 0, &mut explicit_zero);
                let explicit = Params::decode_with_presence(&explicit_zero).unwrap();

                assert_eq!(omitted.params, explicit.params);

                let (omitted_presence, explicit_presence) = match tag {
                    5 => (
                        omitted.presence.received_stats,
                        explicit.presence.received_stats,
                    ),
                    6 => (omitted.presence.stamp_at, explicit.presence.stamp_at),
                    _ => unreachable!(),
                };
                assert!(!omitted_presence);
                assert!(explicit_presence);
            }

            // `Clock` deliberately breaks the pattern above: explicit zero is
            // rejected rather than accepted-but-present.
            let omitted = Params::decode_with_presence(&[]).unwrap();
            assert_eq!(omitted.params.clock, Clock::Unspecified);
            assert!(!omitted.presence.clock);
            let mut explicit_clock_zero = Vec::new();
            encode_tagged_int(7, 0, &mut explicit_clock_zero);
            assert_eq!(
                Params::decode_with_presence(&explicit_clock_zero),
                Err(ProtoError::InvalidEnum {
                    name: "Clock",
                    value: 0,
                })
            );
        }
    }
}
