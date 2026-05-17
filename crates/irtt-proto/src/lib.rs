#![forbid(unsafe_code)]

pub mod close;
pub mod echo;
pub mod error;
pub mod flags;
pub mod hmac;
pub mod layout;
pub mod open;
pub mod params;
pub mod varint;

pub use close::{encode_close_request, CloseRequest};
pub use echo::{decode_echo_reply, encode_echo_request, EchoReply, EchoRequest, TimestampFields};
pub use error::{ProtoError, Result};
pub use flags::*;
pub use hmac::{compute_hmac, compute_hmac_in_place, verify_hmac};
pub use layout::{echo_header_len, echo_packet_len, PacketLayout};
pub use open::{decode_open_reply, encode_open_request, OpenReply, OpenRequest};
pub use params::{Clock, Params, ReceivedStats, ServerFill, StampAt, MAX_SERVER_FILL_BYTES};

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
