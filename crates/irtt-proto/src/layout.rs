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

pub fn echo_packet_len(hmac: bool, params: &Params) -> Result<usize> {
    let header_len = echo_header_len(hmac, params);
    let requested =
        usize::try_from(params.length).map_err(|_| ProtoError::NegativePacketLength {
            length: params.length,
        })?;
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
    fn open_and_close_layout_lengths_account_for_hmac() {
        assert_eq!(PacketLayout::open_request(false).header_len(), 4);
        assert_eq!(PacketLayout::open_request(true).header_len(), 20);
        assert_eq!(PacketLayout::open_reply(false).header_len(), 12);
        assert_eq!(PacketLayout::open_reply(true).header_len(), 28);
        assert_eq!(PacketLayout::close_request(false).header_len(), 12);
        assert_eq!(PacketLayout::close_request(true).header_len(), 28);
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
    fn no_timestamp_layout_ignores_clock() {
        for clock in [Clock::Wall, Clock::Monotonic, Clock::Both] {
            assert_eq!(
                echo_header_len(false, &params(ReceivedStats::None, StampAt::None, clock)),
                16
            );
        }
    }

    #[test]
    fn negotiated_length_never_smaller_than_header() {
        let mut p = params(ReceivedStats::Both, StampAt::Both, Clock::Both);
        p.length = 20;
        assert_eq!(echo_packet_len(false, &p).unwrap(), 60);
        p.length = 92;
        assert_eq!(echo_packet_len(false, &p).unwrap(), 92);
    }

    #[test]
    fn checked_packet_len_rejects_negative_requested_length() {
        let mut p = params(ReceivedStats::Both, StampAt::Both, Clock::Both);
        p.length = -1;

        assert_eq!(
            echo_packet_len(false, &p),
            Err(ProtoError::NegativePacketLength { length: -1 })
        );
    }

    #[test]
    fn checked_packet_len_preserves_non_negative_header_floor() {
        let mut p = params(ReceivedStats::Both, StampAt::Both, Clock::Both);
        p.length = 0;
        assert_eq!(echo_packet_len(false, &p), Ok(60));

        p.length = 92;
        assert_eq!(echo_packet_len(false, &p), Ok(92));
    }
}
