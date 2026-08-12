use irtt_proto::{Clock, Params, ReceivedStats, ServerFill, StampAt};

use super::support::{
    core_with_tokens, expect_normal_open_reply, open_request, open_request_with_raw_params,
    param_int, param_server_fill, peer, unthrottled, ScriptedTokens,
};
use crate::ServerConfig;

const TOKEN_A: u64 = 0x0102_0304_0506_0708;

fn reject_case(name: &str, payload: Vec<u8>) {
    let tokens = ScriptedTokens::new([TOKEN_A]);
    let mut core = core_with_tokens(ServerConfig::default(), tokens.clone());

    let reply = core
        .handle_datagram(peer(), &open_request_with_raw_params(&payload, None))
        .expect("rejected parameters are not a server error");

    assert!(reply.is_none(), "{name} must be answered with silence");
    assert_eq!(core.session_count(), 0, "{name} must create no session");
    assert_eq!(
        tokens.remaining(),
        1,
        "{name} must not consume a session token"
    );
}

#[test]
fn malformed_or_out_of_range_parameters_are_dropped_without_a_session() {
    // Truncation and overflow.
    reject_case("truncated tag varint", vec![0x80]);
    reject_case("truncated value varint", vec![2]);
    reject_case("truncated multi-byte value", vec![2, 0x80]);
    reject_case(
        "varint overflow",
        vec![
            1, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        ],
    );

    // Duration and Interval: present and not positive.
    reject_case("explicit duration zero", param_int(2, 0));
    reject_case("negative duration", param_int(2, -1));
    reject_case("explicit interval zero", param_int(3, 0));
    reject_case("negative interval", param_int(3, -5));

    // Enum values outside their defined range.
    reject_case("received stats above range", param_int(5, 4));
    reject_case("negative received stats", param_int(5, -1));
    reject_case("stamp at above range", param_int(6, 5));
    reject_case("explicit clock zero", param_int(7, 0));
    reject_case("clock above range", param_int(7, 4));

    // Server fill.
    reject_case(
        "server fill declaring more than the payload holds",
        param_server_fill(10, b"ab"),
    );
    reject_case(
        "server fill longer than 32 bytes",
        param_server_fill(33, b"0123456789abcdef0123456789abcdefx"),
    );
    reject_case(
        "server fill that is not utf-8",
        param_server_fill(1, &[0xff]),
    );
}

#[test]
fn absent_duration_interval_and_clock_are_accepted_as_wire_defaults() {
    // The whole reason the decoder reports presence: an omitted tag and one
    // explicitly encoded as zero share a value but not a verdict.
    //
    // The interval floor is disabled so that the accepted wire default is
    // visible in the reply. An absent Interval is a value like any other to
    // negotiation, and the default floor would raise it; that is interval
    // policy, and `negotiation` tests it.
    let mut core = core_with_tokens(unthrottled(), ScriptedTokens::new([TOKEN_A]));

    let packet = core
        .handle_datagram(
            peer(),
            &open_request_with_raw_params(&param_int(1, 1), None),
        )
        .unwrap()
        .expect("absent duration, interval and clock are all valid");

    let reply = expect_normal_open_reply(&packet, None);
    assert_eq!(reply.params.duration_ns, 0);
    assert_eq!(reply.params.interval_ns, 0);
    assert_eq!(reply.params.clock, Clock::Unspecified);
    assert_eq!(core.session_count(), 1);
}

#[test]
fn valid_optional_parameter_values_survive_unchanged() {
    for (name, requested) in [
        (
            "clock wall",
            Params {
                clock: Clock::Wall,
                ..Params::default()
            },
        ),
        (
            "clock monotonic",
            Params {
                clock: Clock::Monotonic,
                ..Params::default()
            },
        ),
        (
            "clock both",
            Params {
                clock: Clock::Both,
                ..Params::default()
            },
        ),
        (
            "received stats and stamp at",
            Params {
                received_stats: ReceivedStats::Both,
                stamp_at: StampAt::Midpoint,
                clock: Clock::Wall,
                ..Params::default()
            },
        ),
        (
            "positive duration and interval",
            Params {
                duration_ns: 1,
                interval_ns: 1,
                ..Params::default()
            },
        ),
        (
            "maximum length server fill",
            Params {
                server_fill: Some(ServerFill {
                    value: "0123456789abcdef0123456789abcdef".to_owned(),
                }),
                ..Params::default()
            },
        ),
    ] {
        // No interval floor: these rows are about which *values* survive
        // decoding and negotiation, not about the timing policy applied to
        // them.
        let mut core = core_with_tokens(unthrottled(), ScriptedTokens::new([TOKEN_A]));

        let packet = core
            .handle_datagram(peer(), &open_request(&requested, None))
            .unwrap()
            .unwrap_or_else(|| panic!("{name} must be answered"));

        let reply = expect_normal_open_reply(&packet, None);
        assert_eq!(
            reply.params,
            Params {
                protocol_version: 1,
                ..requested
            },
            "{name} must survive negotiation unchanged"
        );
    }
}
