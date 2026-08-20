//! Low-level wire encode/decode support for the IRTT protocol.
//!
//! `irtt-proto` intentionally mirrors protocol fields closely: request/reply
//! structs expose wire-oriented values, flags, counters, timestamps, and
//! parameter enums rather than higher-level policy decisions. Higher-level
//! crates such as `irtt-client` are responsible for session behavior and
//! user-facing validation.
//!
//! Most applications should depend on `irtt-client` or `irtt-server` instead
//! of this crate directly; use `irtt-proto` only when implementing another
//! IRTT-compatible client or server that needs the raw packet layer itself.
//!
//! [`Params::decode`] rejects malformed or incompatible incoming parameter
//! values, including invalid enum discriminants, malformed UTF-8, and oversized
//! `server_fill` values. Direct construction followed by [`Params::encode`]
//! performs no additional validation, so callers that build `Params` manually
//! are responsible for validating those values before sending them.
//!
//! Parameters are optional on the wire, and an absent tag takes its wire
//! default — zero for every integer field, and [`Clock::Unspecified`] for the
//! clock. A receiver that must distinguish an omitted tag from one explicitly
//! encoded as zero uses [`Params::decode_with_presence`], which runs the same
//! parser and additionally reports a [`ParamPresence`].
//!
//! The crate also provides packet layout calculation, packet encoding and
//! decoding, and optional HMAC placement, computation, and verification
//! helpers.
//! Encoder `hmac_key` arguments are authoritative: `Some(key)` adds
//! `FLAG_HMAC`, while `None` removes it from caller-supplied reply flags.
//!
//! # Directional model
//!
//! Requests use one encoder and one decoder for all three request kinds:
//! [`encode_request`] builds a request from borrowed sender-side values, and
//! [`decode_request`] structurally classifies an inbound request without a key,
//! negotiated [`Params`], or any session state. [`verify_packet_hmac`]
//! authenticates a packet separately, once the applicable key is known —
//! `FLAG_HMAC` being present is a statement about layout, not about validity.
//!
//! Replies keep type-specific decoders ([`decode_open_reply`],
//! [`decode_echo_reply`]) because a client already knows from its session state
//! which reply kind to expect, and the two need genuinely different semantic
//! context.
//!
#![forbid(unsafe_code)]

pub mod echo;
mod envelope;
pub mod error;
pub mod flags;
pub mod hmac;
pub mod layout;
pub mod open;
pub mod params;
pub mod request;
pub mod varint;

pub use echo::{decode_echo_reply, encode_echo_reply, EchoReply, TimestampFields};
pub use error::{ProtoError, Result};
pub use flags::*;
pub use hmac::{compute_hmac, compute_hmac_in_place, verify_hmac, verify_packet_hmac};
pub use layout::{echo_header_len, echo_packet_len, PacketLayout};
pub use open::{decode_open_reply, encode_open_reply, OpenReply};
pub use params::{
    Clock, DecodedParams, ParamPresence, Params, ReceivedStats, ServerFill, StampAt,
    MAX_SERVER_FILL_BYTES,
};
pub use request::{
    decode_request, encode_request, DecodedRequest, DecodedRequestKind, RequestToEncode,
};

pub const MAGIC: [u8; 3] = [0x14, 0xA7, 0x5B];
pub const PROTOCOL_VERSION: i64 = 1;

pub const HMAC_SIZE: usize = 16;
pub const TOKEN_SIZE: usize = 8;
pub const SEQ_SIZE: usize = 4;
pub const RECV_COUNT_SIZE: usize = 4;
pub const RECV_WINDOW_SIZE: usize = 8;
pub const TIMESTAMP_SIZE: usize = 8;

pub(crate) const HEADER_SIZE: usize = 4;

pub(crate) fn write_header(out: &mut Vec<u8>, flags: u8) {
    out.extend_from_slice(&MAGIC);
    out.push(flags);
}

pub(crate) fn validate_header(packet: &[u8]) -> Result<u8> {
    if packet.len() < HEADER_SIZE {
        return Err(ProtoError::PacketTooShort {
            needed: HEADER_SIZE,
            actual: packet.len(),
        });
    }
    if packet[..3] != MAGIC {
        return Err(ProtoError::BadMagic);
    }
    let flags = packet[3];
    flags::validate_flags(flags)?;
    Ok(flags)
}
