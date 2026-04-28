use crate::{
    params::{Clock, Params, StampAt},
    HEADER_SIZE, HMAC_SIZE, RECV_COUNT_SIZE, RECV_WINDOW_SIZE, SEQ_SIZE, TIMESTAMP_SIZE,
    TOKEN_SIZE,
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

pub fn echo_packet_len(hmac: bool, params: &Params) -> usize {
    let header_len = echo_header_len(hmac, params);
    let requested = usize::try_from(params.length).unwrap_or(0);
    header_len.max(requested)
}

#[allow(dead_code)]
fn _assert_clock_is_used(_: Clock) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::params::{ReceivedStats, StampAt};

    fn params(stats: ReceivedStats, stamp_at: StampAt, clock: Clock) -> Params {
        Params {
            received_stats: stats,
            stamp_at,
            clock,
            ..Params::default()
        }
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
        assert_eq!(echo_packet_len(false, &p), 60);
        p.length = 92;
        assert_eq!(echo_packet_len(false, &p), 92);
    }
}
