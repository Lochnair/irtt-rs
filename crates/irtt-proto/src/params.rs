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
///
/// # Example
///
/// Construct a `Params`, encode it, and decode it back:
///
/// ```
/// use irtt_proto::{Clock, Params, ReceivedStats, ServerFill, StampAt};
///
/// let params = Params {
///     protocol_version: 1,
///     duration_ns: 3_000_000_000,
///     interval_ns: 1_000_000_000,
///     length: 1472,
///     received_stats: ReceivedStats::Both,
///     stamp_at: StampAt::Both,
///     clock: Clock::Both,
///     server_fill: Some(ServerFill { value: "rand".to_owned() }),
///     ..Params::default()
/// };
///
/// let encoded = params.encode();
/// assert_eq!(Params::decode(&encoded).unwrap(), params);
/// ```
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
    /// # Example
    ///
    /// ```
    /// use irtt_proto::Params;
    ///
    /// // Wire bytes: tag 2 (duration) carrying an explicit zero. Interval
    /// // (tag 3) is absent, so both fields decode to zero but only one is
    /// // reported present.
    /// let decoded = Params::decode_with_presence(&[0x02, 0x00]).unwrap();
    /// assert_eq!(decoded.params.duration_ns, 0);
    /// assert!(decoded.presence.duration_ns);
    /// assert!(!decoded.presence.interval_ns);
    /// ```
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
