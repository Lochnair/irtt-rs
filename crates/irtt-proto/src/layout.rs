use crate::{
    params::{Params, StampAt},
    ProtoError, Result, HEADER_SIZE, HMAC_SIZE, RECV_COUNT_SIZE, RECV_WINDOW_SIZE, SEQ_SIZE,
    TIMESTAMP_SIZE, TOKEN_SIZE,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PacketLayout {
    pub hmac: bool,
    pub token: bool,
    pub sequence: bool,
    pub recv_count: bool,
    pub recv_window: bool,
    pub recv_wall: bool,
    pub recv_mono: bool,
    pub midpoint_wall: bool,
    pub midpoint_mono: bool,
    pub send_wall: bool,
    pub send_mono: bool,
}

impl PacketLayout {
    pub fn open_request(hmac: bool) -> Self {
        Self {
            hmac,
            token: false,
            sequence: false,
            recv_count: false,
            recv_window: false,
            recv_wall: false,
            recv_mono: false,
            midpoint_wall: false,
            midpoint_mono: false,
            send_wall: false,
            send_mono: false,
        }
    }

    pub fn open_reply(hmac: bool) -> Self {
        Self {
            token: true,
            ..Self::open_request(hmac)
        }
    }

    pub fn echo(hmac: bool, params: &Params) -> Self {
        let clock = params.clock;
        Self {
            hmac,
            token: true,
            sequence: true,
            recv_count: params.received_stats.has_count(),
            recv_window: params.received_stats.has_window(),
            recv_wall: matches!(params.stamp_at, StampAt::Receive | StampAt::Both)
                && clock.has_wall(),
            recv_mono: matches!(params.stamp_at, StampAt::Receive | StampAt::Both)
                && clock.has_mono(),
            midpoint_wall: matches!(params.stamp_at, StampAt::Midpoint) && clock.has_wall(),
            midpoint_mono: matches!(params.stamp_at, StampAt::Midpoint) && clock.has_mono(),
            send_wall: matches!(params.stamp_at, StampAt::Send | StampAt::Both) && clock.has_wall(),
            send_mono: matches!(params.stamp_at, StampAt::Send | StampAt::Both) && clock.has_mono(),
        }
    }

    pub fn close_request(hmac: bool) -> Self {
        Self {
            hmac,
            token: true,
            sequence: false,
            recv_count: false,
            recv_window: false,
            recv_wall: false,
            recv_mono: false,
            midpoint_wall: false,
            midpoint_mono: false,
            send_wall: false,
            send_mono: false,
        }
    }

    pub fn header_len(self) -> usize {
        HEADER_SIZE
            + if self.hmac { HMAC_SIZE } else { 0 }
            + if self.token { TOKEN_SIZE } else { 0 }
            + if self.sequence { SEQ_SIZE } else { 0 }
            + if self.recv_count { RECV_COUNT_SIZE } else { 0 }
            + if self.recv_window {
                RECV_WINDOW_SIZE
            } else {
                0
            }
            + self.timestamp_count() * TIMESTAMP_SIZE
    }

    pub fn timestamp_count(self) -> usize {
        [
            self.recv_wall,
            self.recv_mono,
            self.midpoint_wall,
            self.midpoint_mono,
            self.send_wall,
            self.send_mono,
        ]
        .into_iter()
        .filter(|present| *present)
        .count()
    }
}

pub fn echo_header_len(hmac: bool, params: &Params) -> usize {
    PacketLayout::echo(hmac, params).header_len()
}

/// The datagram length of an ECHO request or reply under `params`.
///
/// The negotiated length is a floor a peer asks for, never a ceiling: a packet
/// can never be shorter than the field block its negotiated layout requires, so
/// the result is `max(header, requested)`.
///
/// A negative negotiated length is a legitimate input rather than an encoder
/// error. It is accepted during open and echoed back unchanged, so a session
/// can genuinely carry one, and there is no datagram shorter than none — it
/// therefore requests no space beyond the mandatory field block, exactly as
/// zero does.
///
/// # Errors
///
/// Returns [`ProtoError::PacketLengthUnrepresentable`] for a positive length
/// this platform's `usize` cannot hold. That is a representability check, not a
/// size policy: a wire value that cannot even name a local buffer must not be
/// converted into one that can, because the result is handed to an allocator.
/// It is unreachable on a 64-bit target, where every positive `i64` converts,
/// and is the reason this returns a [`Result`] at all.
///
/// A ceiling on what a negotiated length may legitimately *be* — an MTU, a
/// resource bound, a maximum packet size — is deliberately not here. That is
/// server and runtime policy, and it belongs where the negotiation happens.
///
/// # Example
///
/// ```
/// use irtt_proto::{echo_packet_len, Params};
///
/// // The default layout's mandatory field block is 16 bytes (header, token,
/// // sequence), so a smaller requested length is floored up to it.
/// assert_eq!(echo_packet_len(false, &Params::default()).unwrap(), 16);
/// assert_eq!(
///     echo_packet_len(false, &Params { length: 40, ..Params::default() }).unwrap(),
///     40,
/// );
/// ```
pub fn echo_packet_len(hmac: bool, params: &Params) -> Result<usize> {
    let header_len = echo_header_len(hmac, params);
    let requested = if params.length <= 0 {
        // No datagram is shorter than none, so a negative length asks for
        // nothing beyond the mandatory field block, exactly as zero does.
        0
    } else {
        usize::try_from(params.length).map_err(|_| ProtoError::PacketLengthUnrepresentable {
            length: params.length,
        })?
    };
    Ok(header_len.max(requested))
}
