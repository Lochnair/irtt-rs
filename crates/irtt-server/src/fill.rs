//! Server fill policy: what the ServerFill descriptor means, and what bytes it
//! puts in an echo reply's payload region.
//!
//! Descriptor *strings* are wire data and `irtt-proto` carries them verbatim;
//! deciding what one means is server policy and lives here. The result is a
//! [`FillMode`], which is deliberately crate-private: it is neither wire format
//! nor public configuration.

use irtt_proto::{Params, ServerFill};

/// The bytes of this server's default fill, repeated: `69 72 74 74`, or `irtt`.
///
/// The value matches the descriptor the clean evidence records the reference
/// server defaulting to, so a client that recognizes it sees what it expects.
/// Its use here is nevertheless `irtt-rs` policy: payload bytes carry no
/// protocol meaning, and nothing about interoperability requires this pattern
/// over any other.
const DEFAULT_FILL_PATTERN: &[u8] = b"irtt";

/// The descriptor naming [`DEFAULT_FILL_PATTERN`], as returned in an open reply
/// whose explicit request this server could not honor.
///
/// Held as a constant beside the bytes rather than derived from them, so the
/// descriptor a client is told and the bytes it is sent cannot drift apart
/// through a formatting or parsing step neither of them needs.
pub(crate) const DEFAULT_FILL_DESCRIPTOR: &str = "pattern:69727474";

/// The mode prefix of a repeating hexadecimal pattern descriptor.
const PATTERN_PREFIX: &str = "pattern:";

/// How this server fills the payload region of one session's echo replies.
///
/// Crate-private on purpose. It is an interpretation of a wire string, not the
/// string itself and not a configuration surface, so nothing outside the crate
/// has any use for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FillMode {
    /// Write no fill of the server's own. The payload region is zeroes — see
    /// [`FillMode::payload`].
    None,
    /// Fill the payload region with bytes from the operating system's random
    /// source.
    Random,
    /// Fill the payload region with these bytes, repeated. Never empty.
    Pattern(Vec<u8>),
}

impl FillMode {
    /// This server's default fill, used by a session whose client expressed no
    /// preference and by one whose explicit descriptor could not be honored.
    pub(crate) fn default_fill() -> Self {
        Self::Pattern(DEFAULT_FILL_PATTERN.to_vec())
    }

    /// The mode a descriptor asks for, or `None` if it names none.
    ///
    /// The three mode names are matched exactly and case-sensitively; only a
    /// pattern's hexadecimal body is case-insensitive. An unrecognized name, a
    /// differently-cased one, and a `pattern:` whose body is empty, odd-length
    /// or not hexadecimal all return `None`, and negotiation replaces them with
    /// the default. Nothing here rejects an open, panics, or half-parses a
    /// descriptor into a partial pattern.
    fn parse(descriptor: &str) -> Option<Self> {
        match descriptor {
            "none" => Some(Self::None),
            "rand" => Some(Self::Random),
            _ => descriptor
                .strip_prefix(PATTERN_PREFIX)
                .and_then(parse_hex_pattern)
                .map(Self::Pattern),
        }
    }
}

/// The pattern a `pattern:` descriptor's body names, or `None` if the body is
/// not a whole number of hexadecimal bytes.
///
/// A descriptor is at most [`MAX_SERVER_FILL_BYTES`] on the wire, so this
/// decodes at most twelve bytes and complexity is beside the point. Both digit
/// cases are accepted; nothing else is, including a non-ASCII character, whose
/// bytes are all rejected as digits.
///
/// [`MAX_SERVER_FILL_BYTES`]: irtt_proto::MAX_SERVER_FILL_BYTES
fn parse_hex_pattern(body: &str) -> Option<Vec<u8>> {
    let digits = body.as_bytes();
    if digits.is_empty() || !digits.len().is_multiple_of(2) {
        return None;
    }
    digits
        .chunks_exact(2)
        .map(|pair| Some(hex_digit(pair[0])? << 4 | hex_digit(pair[1])?))
        .collect()
}

/// The value of one ASCII hexadecimal digit.
fn hex_digit(digit: u8) -> Option<u8> {
    match digit {
        b'0'..=b'9' => Some(digit - b'0'),
        b'a'..=b'f' => Some(digit - b'a' + 10),
        b'A'..=b'F' => Some(digit - b'A' + 10),
        _ => None,
    }
}

/// Settles what fill a session will actually use, restricting `params` only
/// where this server could not honor what was asked for.
///
/// Four cases, and the distinction between the first two and the last is the
/// point:
///
/// - **Absent or empty descriptor.** The client expressed no preference, so
///   `params` is left exactly as it arrived — absent stays absent, an explicit
///   empty value stays empty — and the session uses this server's default fill.
///   Writing the default descriptor into the reply would manufacture a
///   restriction out of a request that made none, and a strict client rejects a
///   descriptor it never asked for.
/// - **A recognized, well-formed descriptor.** Honored exactly, and returned
///   byte-for-byte, including a pattern body whose hexadecimal case differs from
///   the decoded bytes. `irtt-rs` accepts every valid `none`, `rand` and
///   `pattern:` descriptor: this is deliberately more permissive than the
///   reference server's default glob allow-list, which is upstream policy rather
///   than an interoperability requirement, and refusing valid modes would create
///   strict-negotiation failures while preventing nothing — the payload carries
///   no protocol meaning, the descriptor is bounded to 32 wire bytes, a pattern
///   is tiny, `none` is safely zero-filled here, and a random fill is bounded by
///   the negotiated packet length like every other reply.
/// - **An unrecognized or malformed explicit descriptor.** The server really did
///   change what was asked for, so it says so: `params.server_fill` becomes
///   [`DEFAULT_FILL_DESCRIPTOR`] and the session uses the default fill. A strict
///   client is then free to reject the session — the restriction is honest, and
///   hiding it would be worse.
///
/// The descriptor is parsed exactly once, here, so echo processing never looks
/// at a string.
pub(crate) fn negotiate_server_fill(params: &mut Params) -> FillMode {
    let Some(requested) = params
        .server_fill
        .as_ref()
        .filter(|fill| !fill.value.is_empty())
    else {
        return FillMode::default_fill();
    };
    match FillMode::parse(&requested.value) {
        Some(mode) => mode,
        None => {
            params.server_fill = Some(ServerFill {
                value: DEFAULT_FILL_DESCRIPTOR.to_owned(),
            });
            FillMode::default_fill()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognized_descriptors_parse_to_their_mode() {
        for (descriptor, expected) in [
            ("none", FillMode::None),
            ("rand", FillMode::Random),
            ("pattern:00", FillMode::Pattern(vec![0x00])),
            ("pattern:ff00", FillMode::Pattern(vec![0xff, 0x00])),
            // Only the hexadecimal body is case-insensitive.
            ("pattern:AaBb", FillMode::Pattern(vec![0xaa, 0xbb])),
            (
                "pattern:69727474",
                FillMode::Pattern(DEFAULT_FILL_PATTERN.to_vec()),
            ),
        ] {
            assert_eq!(FillMode::parse(descriptor), Some(expected), "{descriptor}");
        }
    }

    #[test]
    fn unrecognized_and_malformed_descriptors_parse_to_nothing() {
        for descriptor in [
            "",
            "bogus",
            // The mode names themselves are case-sensitive.
            "RAND",
            "None",
            "Pattern:aabb",
            "rand ",
            "pattern:",
            "pattern:f",
            "pattern:abc",
            "pattern:zz",
            "pattern:0g",
            // A multi-byte character is not two hexadecimal digits, whatever
            // its byte length.
            "pattern:é",
        ] {
            assert_eq!(FillMode::parse(descriptor), None, "{descriptor:?}");
        }
    }

    #[test]
    fn the_default_descriptor_and_the_default_pattern_agree() {
        // The two constants are held separately so nothing has to parse a
        // string per open; this is what keeps them from drifting.
        assert_eq!(
            FillMode::parse(DEFAULT_FILL_DESCRIPTOR),
            Some(FillMode::default_fill())
        );
    }
}
