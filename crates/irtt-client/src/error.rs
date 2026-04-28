use std::{io, time::Duration};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("failed to resolve server address {addr:?}")]
    Resolve { addr: String },
    #[error("socket error: {0}")]
    Socket(#[from] io::Error),
    #[error("protocol error: {0}")]
    Protocol(#[from] irtt_proto::ProtoError),
    #[error("all open requests timed out")]
    OpenTimeout,
    #[error("open timeout {timeout:?} is below the minimum {minimum:?}")]
    OpenTimeoutTooSmall {
        timeout: Duration,
        minimum: Duration,
    },
    #[error("open_timeouts must not be empty")]
    NoOpenTimeouts,
    #[error("server rejected the open request")]
    ServerRejected,
    #[error("unexpected no-test open reply")]
    UnexpectedNoTestReply,
    #[error("no-test reply included a non-zero connection token: {token}")]
    NonZeroNoTestToken { token: u64 },
    #[error("protocol version mismatch: requested {requested}, received {received}")]
    ProtocolVersionMismatch { requested: i64, received: i64 },
    #[error("server returned a zero connection token")]
    ZeroToken,
    #[error("strict negotiation rejected changed params: {reason}")]
    NegotiationRejected { reason: String },
    #[error("client is not open")]
    NotOpen,
    #[error("client session is already open")]
    AlreadyOpen,
    #[error("client session is already completed")]
    AlreadyCompleted,
    #[error("client session is already closed")]
    AlreadyClosed,
    #[error("duration is too large to encode as nanoseconds")]
    DurationOverflow,
    #[error("pending probe limit exceeded ({limit})")]
    PendingLimitExceeded { limit: usize },
    #[error("invalid configuration: {reason}")]
    InvalidConfig { reason: String },
}
