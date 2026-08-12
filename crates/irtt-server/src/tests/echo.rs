//! Normal echo processing.
//!
//! An echo request that authenticates, fits the configured maximum, names a
//! live session and arrives from that session's endpoint is answered from the
//! session's negotiated parameters. Everything else is silence that leaves the
//! receive state alone, which these tests prove from the wire — the count and
//! window in the *next* accepted reply are what show that a rejected datagram
//! changed nothing.
//!
//! The count and window vectors come from the clean
//! `test-vectors/SERVER_BEHAVIORAL_VECTORS.md` Section 1 and are hard-coded
//! here rather than recomputed, so that replacing the observed semantics with a
//! conventional selective-acknowledgement bitmap fails these tests.

use std::net::{SocketAddr, SocketAddrV6};

use irtt_proto::{
    echo_packet_len, verify_packet_hmac, Clock, Params, ReceivedStats, StampAt, TimestampFields,
};

use super::support::{
    core_for, core_with_sources, core_with_tokens, echo_params, echo_request, expect_echo_reply,
    open_negotiated, other_peer, peer, sample, ScriptedClock, ScriptedTokens, KEY, OTHER_KEY,
};
use crate::ServerConfig;

const TOKEN_A: u64 = 0x0102_0304_0506_0708;
const UNKNOWN_TOKEN: u64 = 0x2122_2324_2526_2728;

/// Params selecting no optional field at all, so the echo layout is the
/// mandatory field block and nothing else.
fn bare_params() -> Params {
    echo_params(
        ReceivedStats::None,
        StampAt::None,
        Clock::Unspecified,
        /* length */ 0,
    )
}

/// Params reporting both statistics, which is what most vectors are stated in.
fn counting_params(length: i64) -> Params {
    echo_params(
        ReceivedStats::Both,
        StampAt::None,
        Clock::Unspecified,
        length,
    )
}

/// Opens one session on a default core and returns the core, the token and the
/// negotiated params.
fn session_with(params: &Params) -> (crate::ServerCore, u64, Params) {
    let mut core = core_with_tokens(ServerConfig::default(), ScriptedTokens::new([TOKEN_A]));
    let (token, negotiated) = open_negotiated(&mut core, peer(), params, None);
    (core, token, negotiated)
}

#[test]
fn a_valid_echo_is_answered_from_the_negotiated_params() {
    let (mut core, token, negotiated) = session_with(&bare_params());

    let packet = core
        .handle_datagram(peer(), &echo_request(token, 42, &negotiated, &[], None))
        .unwrap()
        .expect("an admissible echo must be answered");

    let reply = expect_echo_reply(&packet, &negotiated, None);
    assert_eq!(reply.token, token, "the session token is copied through");
    assert_eq!(
        reply.sequence, 42,
        "the sequence number is copied unchanged"
    );
    assert_eq!(reply.recv_count, None, "no statistics were negotiated");
    assert_eq!(reply.recv_window, None);
    assert_eq!(
        reply.timestamps,
        TimestampFields::default(),
        "no timestamps were negotiated, so none are emitted"
    );
    assert!(reply.payload.is_empty());
    assert_eq!(packet.len(), echo_packet_len(false, &negotiated).unwrap());
    assert_eq!(packet.len(), 16, "magic, flags, token and sequence only");

    assert_eq!(
        core.session_count(),
        1,
        "an echo neither creates nor releases a session"
    );
}

#[test]
fn a_reply_is_sized_by_the_session_not_by_the_request() {
    // A request may be shorter or longer than the negotiated length, and its
    // bytes past the sequence number are opaque either way.
    let (mut core, token, negotiated) = session_with(&counting_params(64));
    assert_eq!(negotiated.length, 64);

    let short = Params {
        length: 0,
        ..negotiated.clone()
    };
    // Requesting no statistics moves this request's payload region up to where
    // the *session's* reply keeps its count and window, so the opaque bytes
    // land squarely on fields a server must not read from a request.
    let long = Params {
        length: 100,
        received_stats: ReceivedStats::None,
        ..negotiated.clone()
    };

    for (sequence, name, sizing, payload, expected) in [
        (
            0,
            "a request shorter than the negotiated length",
            &short,
            &[][..],
            (1, 0x1),
        ),
        (
            1,
            "a request longer than the negotiated length, with opaque bytes",
            &long,
            &[0xa5; 40][..],
            (2, 0x3),
        ),
    ] {
        let request = echo_request(token, sequence, sizing, payload, None);
        assert_ne!(request.len(), 64, "{name} must not be the reply's length");

        let packet = core
            .handle_datagram(peer(), &request)
            .unwrap()
            .unwrap_or_else(|| panic!("{name} must be answered"));
        let reply = expect_echo_reply(&packet, &negotiated, None);

        assert_eq!(
            packet.len(),
            64,
            "{name}: the reply is the negotiated length"
        );
        assert_eq!(reply.sequence, sequence);
        assert_eq!(
            (reply.recv_count, reply.recv_window),
            (Some(expected.0), Some(expected.1)),
            "{name}: the opaque tail must not influence the reply"
        );
    }
}

#[test]
fn an_echo_over_the_configured_maximum_is_dropped_before_anything_advances() {
    let config = ServerConfig::default().with_max_packet_length(64);
    let mut core = core_with_tokens(config, ScriptedTokens::new([TOKEN_A]));
    let (token, negotiated) = open_negotiated(&mut core, peer(), &counting_params(64), None);
    assert_eq!(negotiated.length, 64);

    let at_maximum = echo_request(token, 0, &negotiated, &[], None);
    assert_eq!(at_maximum.len(), 64);
    let over_maximum = echo_request(
        token,
        1,
        &Params {
            length: 65,
            ..negotiated.clone()
        },
        &[],
        None,
    );
    assert_eq!(over_maximum.len(), 65);

    // The comparison is strict, so exactly the maximum is served.
    let packet = core
        .handle_datagram(peer(), &at_maximum)
        .unwrap()
        .expect("a request of exactly the maximum must be answered");
    let reply = expect_echo_reply(&packet, &negotiated, None);
    assert_eq!((reply.recv_count, reply.recv_window), (Some(1), Some(0x1)));

    assert_eq!(
        core.handle_datagram(peer(), &over_maximum).unwrap(),
        None,
        "one byte over the maximum must be answered with silence"
    );

    // The next admissible request is what proves the drop advanced nothing: had
    // the oversized one counted, this would report 3 rather than 2.
    let packet = core
        .handle_datagram(peer(), &echo_request(token, 1, &negotiated, &[], None))
        .unwrap()
        .expect("an admissible echo after a dropped one must be answered");
    let reply = expect_echo_reply(&packet, &negotiated, None);
    assert_eq!((reply.recv_count, reply.recv_window), (Some(2), Some(0x3)));
}

#[test]
fn an_echo_that_owns_no_session_here_is_answered_with_silence() {
    let (mut core, token, negotiated) = session_with(&counting_params(0));

    for (name, endpoint, request_token) in [
        ("an unknown token", peer(), UNKNOWN_TOKEN),
        ("a zero token", peer(), 0),
        (
            "a valid token from another source port",
            other_peer(),
            token,
        ),
    ] {
        let request = echo_request(request_token, 0, &negotiated, &[], None);
        assert_eq!(
            core.handle_datagram(endpoint, &request).unwrap(),
            None,
            "{name} must be answered with silence"
        );
    }

    // Count 1 and window 0x1 are the first-request values, so none of the three
    // rejected datagrams touched this session's receive state.
    let packet = core
        .handle_datagram(peer(), &echo_request(token, 0, &negotiated, &[], None))
        .unwrap()
        .expect("the session's own echo must still be answered");
    let reply = expect_echo_reply(&packet, &negotiated, None);
    assert_eq!((reply.recv_count, reply.recv_window), (Some(1), Some(0x1)));
    assert_eq!(core.session_count(), 1);
}

#[test]
fn an_echo_resolves_ipv6_endpoints_by_the_same_identity_rule_as_a_close() {
    // The comparator itself is pinned by the close tests; this is the echo path
    // reaching the same one rather than growing a second rule.
    let global = "2001:db8::1".parse().unwrap();
    let link_local = "fe80::1".parse().unwrap();
    let unlabeled = SocketAddr::V6(SocketAddrV6::new(global, 2112, 0, 0));
    let labeled = SocketAddr::V6(SocketAddrV6::new(global, 2112, 0x000f_1234, 0));
    let zone_three = SocketAddr::V6(SocketAddrV6::new(link_local, 2112, 0, 3));
    let zone_four = SocketAddr::V6(SocketAddrV6::new(link_local, 2112, 0, 4));

    for (bound, sender, answered) in [
        // Flow information is routing metadata that identifies no endpoint.
        (unlabeled, labeled, true),
        (labeled, unlabeled, true),
        // The zone is identity by `irtt-rs` policy, not by observed behavior.
        (zone_three, zone_four, false),
    ] {
        let mut core = core_with_tokens(ServerConfig::default(), ScriptedTokens::new([TOKEN_A]));
        let (token, negotiated) = open_negotiated(&mut core, bound, &counting_params(0), None);

        let reply = core
            .handle_datagram(sender, &echo_request(token, 0, &negotiated, &[], None))
            .unwrap();

        assert_eq!(
            reply.is_some(),
            answered,
            "an echo from {sender} on a session opened from {bound}"
        );
    }
}

#[test]
fn received_count_and_window_follow_the_observed_vectors() {
    // Every row is `(request sequence, reply count, reply window)` exactly as
    // measured in Section 1 of the behavioral vectors.
    for (name, vectors) in [
        (
            "1.1 sequential, no loss",
            &[
                (0, 1, 0x1),
                (1, 2, 0x3),
                (2, 3, 0x7),
                (3, 4, 0xf),
                (4, 5, 0x1f),
                (5, 6, 0x3f),
            ][..],
        ),
        (
            "1.2 one gap",
            &[(0, 1, 0x1), (1, 2, 0x3), (3, 3, 0xd), (4, 4, 0x1b)][..],
        ),
        (
            "1.3 multiple gaps",
            &[(0, 1, 0x1), (3, 2, 0x9), (7, 3, 0x91), (8, 4, 0x123)][..],
        ),
        (
            // Filling a gap late discards the history rather than setting the
            // bit the gap left clear.
            "1.4 later gap fill",
            &[(0, 1, 0x1), (1, 2, 0x3), (5, 3, 0x31), (3, 4, 0x1)][..],
        ),
        (
            "1.5 duplicate sequence number",
            &[(0, 1, 0x1), (1, 2, 0x3), (1, 3, 0x3), (2, 4, 0x7)][..],
        ),
        (
            // Row 5 rebuilds from the reset: bit 1 stays clear even though
            // sequence 2 was received before the reordering.
            "1.6 reordering",
            &[
                (0, 1, 0x1),
                (1, 2, 0x3),
                (2, 3, 0x7),
                (1, 4, 0x1),
                (3, 5, 0x5),
            ][..],
        ),
        (
            "1.7 strictly descending sequence numbers",
            &[
                (5, 1, 0x1),
                (4, 2, 0x1),
                (3, 3, 0x1),
                (2, 4, 0x1),
                (1, 5, 0x1),
                (0, 6, 0x1),
            ][..],
        ),
        (
            "1.8 a gap of exactly 63",
            &[(0, 1, 0x1), (63, 2, 0x8000_0000_0000_0001)][..],
        ),
        ("1.8 a gap of 64", &[(0, 1, 0x1), (64, 2, 0x1)][..]),
        ("1.8 a gap of 65", &[(0, 1, 0x1), (65, 2, 0x1)][..]),
        ("1.8 a gap of 1000", &[(0, 1, 0x1), (1000, 2, 0x1)][..]),
        (
            "1.9 a non-zero starting sequence number",
            &[(100, 1, 0x1), (101, 2, 0x3), (102, 3, 0x7)][..],
        ),
        (
            "1.10 32-bit sequence wraparound",
            &[
                (0xffff_fffd, 1, 0x1),
                (0xffff_fffe, 2, 0x3),
                (0xffff_ffff, 3, 0x7),
                (0, 4, 0xf),
                (1, 5, 0x1f),
            ][..],
        ),
    ] {
        let (mut core, token, negotiated) = session_with(&counting_params(0));

        for &(sequence, count, window) in vectors {
            let packet = core
                .handle_datagram(
                    peer(),
                    &echo_request(token, sequence, &negotiated, &[], None),
                )
                .unwrap()
                .unwrap_or_else(|| panic!("{name}: sequence {sequence} must be answered"));
            let reply = expect_echo_reply(&packet, &negotiated, None);

            assert_eq!(
                reply.recv_count,
                Some(count),
                "{name}: count after sequence {sequence}"
            );
            assert_eq!(
                reply.recv_window,
                Some(window),
                "{name}: window after sequence {sequence}"
            );
        }
    }
}

#[test]
fn only_the_negotiated_received_statistics_are_reported() {
    // Field presence only; the transition itself is pinned by the vectors
    // above, so two in-order requests are enough here.
    for (stats, count, window) in [
        (ReceivedStats::None, None, None),
        (ReceivedStats::Count, Some(2), None),
        (ReceivedStats::Window, None, Some(0x3)),
        (ReceivedStats::Both, Some(2), Some(0x3)),
    ] {
        let params = echo_params(stats, StampAt::None, Clock::Unspecified, 0);
        let (mut core, token, negotiated) = session_with(&params);

        let mut reply = None;
        for sequence in 0..2 {
            let packet = core
                .handle_datagram(
                    peer(),
                    &echo_request(token, sequence, &negotiated, &[], None),
                )
                .unwrap()
                .unwrap_or_else(|| panic!("{stats:?}: sequence {sequence} must be answered"));
            reply = Some(expect_echo_reply(&packet, &negotiated, None));
        }

        let reply = reply.expect("two requests were sent");
        assert_eq!(reply.recv_count, count, "count presence for {stats:?}");
        assert_eq!(reply.recv_window, window, "window presence for {stats:?}");
    }
}

#[test]
fn an_authenticated_session_answers_only_authenticated_echoes() {
    let mut core = core_for(Some(KEY), ScriptedTokens::new([TOKEN_A]));
    let (token, negotiated) = open_negotiated(&mut core, peer(), &counting_params(0), Some(KEY));

    let packet = core
        .handle_datagram(peer(), &echo_request(token, 3, &negotiated, &[], Some(KEY)))
        .unwrap()
        .expect("a correctly authenticated echo must be answered");
    assert!(
        verify_packet_hmac(KEY, &packet).is_ok(),
        "the reply must carry a MAC that verifies with the server's key"
    );
    // `expect_echo_reply` pins the HMAC flag and verifies the MAC again while
    // decoding, so a reply that authenticated but omitted the flag would fail
    // here — the failure mode that silently breaks upstream clients.
    let reply = expect_echo_reply(&packet, &negotiated, Some(KEY));
    assert_eq!(reply.token, token);
    assert_eq!(reply.sequence, 3);
    assert_eq!((reply.recv_count, reply.recv_window), (Some(1), Some(0x1)));

    let mut corrupted = echo_request(token, 4, &negotiated, &[], Some(KEY));
    corrupted[5] ^= 0x80;
    for (name, packet) in [
        (
            "an echo with no MAC field at all",
            echo_request(token, 4, &negotiated, &[], None),
        ),
        (
            "an echo signed with a different key",
            echo_request(token, 4, &negotiated, &[], Some(OTHER_KEY)),
        ),
        ("an echo with one bit flipped in the MAC", corrupted),
    ] {
        assert_eq!(
            core.handle_datagram(peer(), &packet).unwrap(),
            None,
            "{name} must be answered with silence"
        );
    }

    let packet = core
        .handle_datagram(peer(), &echo_request(token, 4, &negotiated, &[], Some(KEY)))
        .unwrap()
        .expect("an authenticated echo after failed ones must be answered");
    let reply = expect_echo_reply(&packet, &negotiated, Some(KEY));
    assert_eq!(
        (reply.recv_count, reply.recv_window),
        (Some(2), Some(0x3)),
        "none of the packets that failed authentication may be counted"
    );
}

#[test]
fn only_the_negotiated_timestamps_are_emitted() {
    let received = sample(1_000, 10_000);
    let sent = sample(1_200, 10_300);

    for (stamp_at, clock, expected) in [
        (
            StampAt::Send,
            Clock::Wall,
            TimestampFields {
                send_wall: Some(1_200),
                ..TimestampFields::default()
            },
        ),
        (
            StampAt::Send,
            Clock::Monotonic,
            TimestampFields {
                send_mono: Some(10_300),
                ..TimestampFields::default()
            },
        ),
        (
            StampAt::Send,
            Clock::Both,
            TimestampFields {
                send_wall: Some(1_200),
                send_mono: Some(10_300),
                ..TimestampFields::default()
            },
        ),
        (
            StampAt::Receive,
            Clock::Wall,
            TimestampFields {
                recv_wall: Some(1_000),
                ..TimestampFields::default()
            },
        ),
        (
            StampAt::Receive,
            Clock::Monotonic,
            TimestampFields {
                recv_mono: Some(10_000),
                ..TimestampFields::default()
            },
        ),
        (
            StampAt::Receive,
            Clock::Both,
            TimestampFields {
                recv_wall: Some(1_000),
                recv_mono: Some(10_000),
                ..TimestampFields::default()
            },
        ),
        (
            StampAt::Both,
            Clock::Wall,
            TimestampFields {
                recv_wall: Some(1_000),
                send_wall: Some(1_200),
                ..TimestampFields::default()
            },
        ),
        (
            StampAt::Both,
            Clock::Monotonic,
            TimestampFields {
                recv_mono: Some(10_000),
                send_mono: Some(10_300),
                ..TimestampFields::default()
            },
        ),
        (
            StampAt::Both,
            Clock::Both,
            TimestampFields {
                recv_wall: Some(1_000),
                recv_mono: Some(10_000),
                send_wall: Some(1_200),
                send_mono: Some(10_300),
                ..TimestampFields::default()
            },
        ),
        (
            StampAt::Midpoint,
            Clock::Wall,
            TimestampFields {
                midpoint_wall: Some(1_100),
                ..TimestampFields::default()
            },
        ),
        (
            StampAt::Midpoint,
            Clock::Monotonic,
            TimestampFields {
                midpoint_mono: Some(10_150),
                ..TimestampFields::default()
            },
        ),
        (
            StampAt::Midpoint,
            Clock::Both,
            TimestampFields {
                midpoint_wall: Some(1_100),
                midpoint_mono: Some(10_150),
                ..TimestampFields::default()
            },
        ),
    ] {
        let clock_source = ScriptedClock::new([received, sent]);
        let mut core = core_with_sources(
            ServerConfig::default(),
            ScriptedTokens::new([TOKEN_A]),
            clock_source.clone(),
        );
        let params = echo_params(ReceivedStats::None, stamp_at, clock, 0);
        let (token, negotiated) = open_negotiated(&mut core, peer(), &params, None);
        assert_eq!(
            clock_source.remaining(),
            2,
            "an open must not consume a clock sample"
        );

        let packet = core
            .handle_datagram(peer(), &echo_request(token, 0, &negotiated, &[], None))
            .unwrap()
            .unwrap_or_else(|| panic!("{stamp_at:?} on {clock:?} must be answered"));
        let reply = expect_echo_reply(&packet, &negotiated, None);

        // Equality over the whole struct, so an unselected field appearing is a
        // failure as much as a selected one carrying the wrong instant.
        assert_eq!(reply.timestamps, expected, "{stamp_at:?} on {clock:?}");
        assert_eq!(
            packet.len(),
            echo_packet_len(false, &negotiated).unwrap(),
            "{stamp_at:?} on {clock:?} must be the negotiated layout's length"
        );
        assert_eq!(
            clock_source.remaining(),
            0,
            "{stamp_at:?} on {clock:?}: one receive and one send sample"
        );
    }
}

#[test]
fn a_single_clock_midpoint_emits_exactly_one_field() {
    // A guard against reproducing upstream 0.9.1's dual-field midpoint, which
    // emits both midpoint fields whatever the negotiated clock. `irtt-proto`
    // still decodes that form from a peer; this is about what this server
    // sends.
    for (clock, expected) in [
        (
            Clock::Wall,
            TimestampFields {
                midpoint_wall: Some(1_100),
                ..TimestampFields::default()
            },
        ),
        (
            Clock::Monotonic,
            TimestampFields {
                midpoint_mono: Some(10_150),
                ..TimestampFields::default()
            },
        ),
    ] {
        let mut core = core_with_sources(
            ServerConfig::default(),
            ScriptedTokens::new([TOKEN_A]),
            ScriptedClock::new([sample(1_000, 10_000), sample(1_200, 10_300)]),
        );
        let params = echo_params(ReceivedStats::None, StampAt::Midpoint, clock, 0);
        let (token, negotiated) = open_negotiated(&mut core, peer(), &params, None);

        let packet = core
            .handle_datagram(peer(), &echo_request(token, 0, &negotiated, &[], None))
            .unwrap()
            .unwrap_or_else(|| panic!("a midpoint echo on {clock:?} must be answered"));
        let reply = expect_echo_reply(&packet, &negotiated, None);

        assert_eq!(
            packet.len(),
            echo_packet_len(false, &negotiated).unwrap(),
            "{clock:?}: the negotiated length, not the compatibility one"
        );
        assert_eq!(
            packet.len(),
            24,
            "{clock:?}: one 8-byte midpoint field, not the upstream two"
        );
        assert_eq!(reply.timestamps, expected);
    }
}

#[test]
fn a_reply_payload_is_zero_filled_and_never_reflects_the_request() {
    let params = echo_params(ReceivedStats::None, StampAt::None, Clock::Unspecified, 64);
    let (mut core, token, negotiated) = session_with(&params);

    let request = echo_request(token, 0, &negotiated, &[0xa5; 48], None);
    assert!(
        request[16..].iter().all(|byte| *byte == 0xa5),
        "the request must carry non-zero payload bytes to reflect"
    );

    let packet = core
        .handle_datagram(peer(), &request)
        .unwrap()
        .expect("an admissible echo must be answered");
    let reply = expect_echo_reply(&packet, &negotiated, None);

    assert_eq!(packet.len(), 64);
    assert_eq!(reply.payload.len(), 48);
    assert!(
        reply.payload.iter().all(|byte| *byte == 0),
        "the payload region must be zero-filled, never request or buffer residue"
    );
}
