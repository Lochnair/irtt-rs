use crate::{varint, ProtoError, Result, PROTOCOL_VERSION};

pub const MAX_SERVER_FILL_BYTES: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Params {
    pub protocol_version: i64,
    pub duration_ns: i64,
    pub interval_ns: i64,
    pub length: i64,
    pub received_stats: ReceivedStats,
    pub stamp_at: StampAt,
    pub clock: Clock,
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

    pub fn decode(input: &[u8]) -> Result<Self> {
        let mut params = Self::default();
        let mut pos = 0;
        while pos < input.len() {
            let (tag, used) = varint::decode_uvarint(&input[pos..])?;
            pos += used;
            match tag {
                1 => params.protocol_version = read_int(input, &mut pos)?,
                2 => params.duration_ns = read_int(input, &mut pos)?,
                3 => params.interval_ns = read_int(input, &mut pos)?,
                4 => params.length = read_int(input, &mut pos)?,
                5 => params.received_stats = ReceivedStats::try_from(read_int(input, &mut pos)?)?,
                6 => params.stamp_at = StampAt::try_from(read_int(input, &mut pos)?)?,
                7 => params.clock = Clock::try_from(read_int(input, &mut pos)?)?,
                8 => params.dscp = read_int(input, &mut pos)?,
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
                }
                _ => {
                    let (_, used) = varint::decode_uvarint(&input[pos..])?;
                    pos += used;
                }
            }
        }
        Ok(params)
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i64)]
pub enum Clock {
    Wall = 1,
    Monotonic = 2,
    #[default]
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
    fn params_round_trip_dscp_codepoints_without_shifting() {
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
            "DSCP 46 must be encoded as param value 46"
        );
        assert!(
            !encoded.windows(3).any(|bytes| bytes == [8, 0xf0, 0x02]),
            "DSCP 46 must not be shifted to TOS byte 184 in Params encoding"
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
}
