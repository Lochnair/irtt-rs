use crate::{error::Result, ProtoError};

pub const FLAG_OPEN: u8 = 0x01;
pub const FLAG_REPLY: u8 = 0x02;
pub const FLAG_CLOSE: u8 = 0x04;
pub const FLAG_HMAC: u8 = 0x08;

pub const RESERVED_FLAGS_MASK: u8 = 0xF0;

pub fn validate_flags(flags: u8) -> Result<()> {
    let reserved = flags & RESERVED_FLAGS_MASK;
    if reserved != 0 {
        return Err(ProtoError::ReservedFlags(reserved));
    }
    Ok(())
}

pub fn has(flags: u8, flag: u8) -> bool {
    flags & flag != 0
}
