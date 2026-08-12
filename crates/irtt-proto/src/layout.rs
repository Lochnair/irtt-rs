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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::params::{Clock, ReceivedStats, StampAt};

    fn params(stats: ReceivedStats, stamp_at: StampAt, clock: Clock) -> Params {
        Params {
            received_stats: stats,
            stamp_at,
            clock,
            ..Params::default()
        }
    }

    fn expected_optional_len(stats: ReceivedStats, stamp_at: StampAt, clock: Clock) -> usize {
        let stats_len = match stats {
            ReceivedStats::None => 0,
            ReceivedStats::Count => RECV_COUNT_SIZE,
            ReceivedStats::Window => RECV_WINDOW_SIZE,
            ReceivedStats::Both => RECV_COUNT_SIZE + RECV_WINDOW_SIZE,
        };
        let clock_count = match clock {
            Clock::Unspecified => 0,
            Clock::Wall | Clock::Monotonic => 1,
            Clock::Both => 2,
        };
        let timestamp_groups = match stamp_at {
            StampAt::None => 0,
            StampAt::Send | StampAt::Receive | StampAt::Midpoint => 1,
            StampAt::Both => 2,
        };
        stats_len + timestamp_groups * clock_count * TIMESTAMP_SIZE
    }

    #[test]
    fn verified_layout_lengths() {
        assert_eq!(PacketLayout::open_request(false).header_len(), 4);
        assert_eq!(PacketLayout::open_request(true).header_len(), 20);
        assert_eq!(PacketLayout::open_reply(false).header_len(), 12);
        assert_eq!(PacketLayout::open_reply(true).header_len(), 28);
        assert_eq!(PacketLayout::close_request(false).header_len(), 12);
        assert_eq!(PacketLayout::close_request(true).header_len(), 28);
        assert_eq!(
            echo_header_len(
                false,
                &params(ReceivedStats::None, StampAt::None, Clock::Both)
            ),
            16
        );
        assert_eq!(
            echo_header_len(
                false,
                &params(ReceivedStats::Count, StampAt::Send, Clock::Wall)
            ),
            28
        );
        assert_eq!(
            echo_header_len(
                false,
                &params(ReceivedStats::Window, StampAt::Receive, Clock::Monotonic)
            ),
            32
        );
        assert_eq!(
            echo_header_len(
                false,
                &params(ReceivedStats::Both, StampAt::Midpoint, Clock::Both)
            ),
            44
        );
        assert_eq!(
            echo_header_len(
                false,
                &params(ReceivedStats::Both, StampAt::Both, Clock::Both)
            ),
            60
        );
        assert_eq!(
            echo_header_len(
                true,
                &params(ReceivedStats::Both, StampAt::Both, Clock::Both)
            ),
            76
        );
    }

    #[test]
    fn layout_matrix_matches_stats_timestamps_clock_and_hmac_rules() {
        for stats in [
            ReceivedStats::None,
            ReceivedStats::Count,
            ReceivedStats::Window,
            ReceivedStats::Both,
        ] {
            for stamp_at in [
                StampAt::None,
                StampAt::Send,
                StampAt::Receive,
                StampAt::Both,
                StampAt::Midpoint,
            ] {
                for clock in [Clock::Wall, Clock::Monotonic, Clock::Both] {
                    for hmac in [false, true] {
                        let params = params(stats, stamp_at, clock);
                        let layout = PacketLayout::echo(hmac, &params);
                        let expected_len = HEADER_SIZE
                            + TOKEN_SIZE
                            + SEQ_SIZE
                            + if hmac { HMAC_SIZE } else { 0 }
                            + expected_optional_len(stats, stamp_at, clock);

                        assert_eq!(
                            layout.header_len(),
                            expected_len,
                            "unexpected length for stats={stats:?} stamp_at={stamp_at:?} clock={clock:?} hmac={hmac}"
                        );
                        assert_eq!(layout.recv_count, stats.has_count());
                        assert_eq!(layout.recv_window, stats.has_window());
                        assert_eq!(
                            layout.recv_wall,
                            matches!(stamp_at, StampAt::Receive | StampAt::Both)
                                && clock.has_wall()
                        );
                        assert_eq!(
                            layout.recv_mono,
                            matches!(stamp_at, StampAt::Receive | StampAt::Both)
                                && clock.has_mono()
                        );
                        assert_eq!(
                            layout.midpoint_wall,
                            matches!(stamp_at, StampAt::Midpoint) && clock.has_wall()
                        );
                        assert_eq!(
                            layout.midpoint_mono,
                            matches!(stamp_at, StampAt::Midpoint) && clock.has_mono()
                        );
                        assert_eq!(
                            layout.send_wall,
                            matches!(stamp_at, StampAt::Send | StampAt::Both) && clock.has_wall()
                        );
                        assert_eq!(
                            layout.send_mono,
                            matches!(stamp_at, StampAt::Send | StampAt::Both) && clock.has_mono()
                        );

                        if hmac {
                            assert_eq!(
                                echo_header_len(true, &params) - echo_header_len(false, &params),
                                HMAC_SIZE
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn negotiated_length_is_floored_at_the_required_field_block() {
        // The 60-byte header of this layout is the floor. A negative negotiated
        // length is not an error: it asks for nothing beyond the mandatory
        // fields, exactly as zero does.
        let mut p = params(ReceivedStats::Both, StampAt::Both, Clock::Both);
        for (length, expected) in [(-4096, 60), (-1, 60), (0, 60), (20, 60), (92, 92)] {
            p.length = length;
            assert_eq!(
                echo_packet_len(false, &p),
                Ok(expected),
                "unexpected packet length for negotiated length {length}"
            );
        }
    }

    /// A positive length wider than `usize` must stay an error rather than
    /// becoming a saturated buffer size. Only reachable where `usize` is
    /// narrower than `i64`; on a 64-bit target every positive `i64` converts.
    #[cfg(target_pointer_width = "32")]
    #[test]
    fn a_length_wider_than_usize_is_rejected_rather_than_saturated() {
        let mut p = params(ReceivedStats::Both, StampAt::Both, Clock::Both);
        p.length = 5_000_000_000;

        assert_eq!(
            echo_packet_len(false, &p),
            Err(ProtoError::PacketLengthUnrepresentable {
                length: 5_000_000_000
            })
        );
    }
}
