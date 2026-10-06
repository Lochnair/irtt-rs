use irtt_proto::params::{Clock, ReceivedStats, StampAt};
use irtt_proto::{
    echo_header_len, echo_packet_len, PacketLayout, Params, HMAC_SIZE, RECV_COUNT_SIZE,
    RECV_WINDOW_SIZE, SEQ_SIZE, TIMESTAMP_SIZE, TOKEN_SIZE,
};

const HEADER_SIZE: usize = 4;

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
fn open_and_close_layout_lengths() {
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
            for clock in [
                Clock::Unspecified,
                Clock::Wall,
                Clock::Monotonic,
                Clock::Both,
            ] {
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
                    assert_eq!(
                        layout.recv_count,
                        matches!(stats, ReceivedStats::Count | ReceivedStats::Both)
                    );
                    assert_eq!(
                        layout.recv_window,
                        matches!(stats, ReceivedStats::Window | ReceivedStats::Both)
                    );
                    assert_eq!(
                        layout.recv_wall,
                        matches!(stamp_at, StampAt::Receive | StampAt::Both)
                            && matches!(clock, Clock::Wall | Clock::Both)
                    );
                    assert_eq!(
                        layout.recv_mono,
                        matches!(stamp_at, StampAt::Receive | StampAt::Both)
                            && matches!(clock, Clock::Monotonic | Clock::Both)
                    );
                    assert_eq!(
                        layout.midpoint_wall,
                        matches!(stamp_at, StampAt::Midpoint)
                            && matches!(clock, Clock::Wall | Clock::Both)
                    );
                    assert_eq!(
                        layout.midpoint_mono,
                        matches!(stamp_at, StampAt::Midpoint)
                            && matches!(clock, Clock::Monotonic | Clock::Both)
                    );
                    assert_eq!(
                        layout.send_wall,
                        matches!(stamp_at, StampAt::Send | StampAt::Both)
                            && matches!(clock, Clock::Wall | Clock::Both)
                    );
                    assert_eq!(
                        layout.send_mono,
                        matches!(stamp_at, StampAt::Send | StampAt::Both)
                            && matches!(clock, Clock::Monotonic | Clock::Both)
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
        Err(irtt_proto::ProtoError::PacketLengthUnrepresentable {
            length: 5_000_000_000
        })
    );
}
