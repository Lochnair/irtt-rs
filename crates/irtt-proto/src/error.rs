use thiserror::Error;

pub type Result<T> = std::result::Result<T, ProtoError>;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProtoError {
    #[error("packet is too short: needed {needed} bytes, got {actual}")]
    PacketTooShort { needed: usize, actual: usize },
    #[error("bad magic bytes")]
    BadMagic,
    #[error("reserved flag bits are set: 0x{0:02x}")]
    ReservedFlags(u8),
    #[error("required flag 0x{0:02x} is missing")]
    MissingFlag(u8),
    #[error("unexpected flag 0x{0:02x} is set")]
    UnexpectedFlag(u8),
    #[error("HMAC flag/key mismatch")]
    HmacPresenceMismatch,
    #[error("HMAC verification failed")]
    BadHmac,
    #[error("invalid HMAC field offset")]
    InvalidHmacOffset,
    #[error("varint is truncated")]
    TruncatedVarint,
    #[error("varint is too long")]
    VarintOverflow,
    #[error("string value is not valid UTF-8")]
    InvalidUtf8,
    #[error("invalid enum value {value} for {name}")]
    InvalidEnum { name: &'static str, value: i64 },
    #[error("open reply has zero token without close flag")]
    ZeroToken,
    #[error("trailing or malformed parameter payload")]
    MalformedParams,
    #[error("payload is too large: {provided} bytes provided, {available} bytes available")]
    PayloadTooLarge { available: usize, provided: usize },
}
