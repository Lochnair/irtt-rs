mod support;

use irtt_client::{Client, ClientConfig, ClientEvent, NegotiationPolicy};
use irtt_proto::{echo_header_len, Params, TimestampFields};

use support::{config_for_params, default_params, run_one_probe, BackendPeer, ServerObservation};

use crate::support::test_echo_packet_len;

fn params_with_length(length: i64) -> Params {
    Params {
        length,
        ..default_params()
    }
}

fn assert_echo_size(run: &support::OneProbeRun, expected: usize) {
    match run.observations[1] {
        ServerObservation::Echo { len, .. } => assert_eq!(len, expected),
        ref other => panic!("expected Echo observation, got {other:?}"),
    }
}

fn assert_reply(event: &ClientEvent) {
    assert!(matches!(event, ClientEvent::EchoReply { .. }));
}

fn assert_sent_bytes(event: &ClientEvent, expected: usize) {
    match event {
        ClientEvent::EchoSent { bytes, .. } => assert_eq!(*bytes, expected),
        other => panic!("expected EchoSent, got {other:?}"),
    }
}

fn assert_reply_bytes(event: &ClientEvent, expected: usize) {
    match event {
        ClientEvent::EchoReply { bytes, .. } => assert_eq!(*bytes, expected),
        other => panic!("expected EchoReply, got {other:?}"),
    }
}

// ─── Config/client validation ───

#[test]
fn length_zero_baseline() {
    let params = params_with_length(0);
    let expected = echo_packet_len(false, &params);
    let header = echo_header_len(false, &params);
    assert_eq!(expected, header);
}

#[test]
fn length_one_accepted() {
    let params = params_with_length(1);
    let expected = echo_packet_len(false, &params);
    assert!(expected >= 1);
}

#[test]
fn length_1472_accepted() {
    let params = params_with_length(1472);
    let expected = echo_packet_len(false, &params);
    assert_eq!(expected, 1472);
}

#[test]
fn length_4096_accepted() {
    let params = params_with_length(4096);
    let expected = echo_packet_len(false, &params);
    assert_eq!(expected, 4096);
}

// ─── Echo request size: FakeServer observes exact packet size ───

#[test]
fn echo_request_size_matches_echo_packet_len_for_zero_length() {
    let params = params_with_length(0);
    let run = run_one_probe(params.clone(), TimestampFields::default());
    let expected = echo_packet_len(false, &params);
    assert_echo_size(&run, expected);
    assert_sent_bytes(&run.sent, expected);
}

#[test]
fn echo_request_size_matches_echo_packet_len_above_header() {
    for length in [16, 60, 128, 1472, 4096] {
        let params = params_with_length(length);
        let run = run_one_probe(params.clone(), TimestampFields::default());
        let expected = echo_packet_len(false, &params);
        assert_eq!(
            expected,
            echo_header_len(false, &params).max(length as usize)
        );
        assert_echo_size(&run, expected);
        assert_sent_bytes(&run.sent, expected);
    }
}

// ─── Echo reply handling ───

#[test]
fn echo_reply_decoded_for_large_length() {
    let params = params_with_length(4096);
    let run = run_one_probe(params.clone(), TimestampFields::default());
    let expected = echo_packet_len(false, &params);
    assert_reply(&run.reply);
    assert_reply_bytes(&run.reply, expected);
}

#[test]
fn echo_reply_bytes_consistent_with_echo_packet_len_for_various_lengths() {
    for length in [0, 1472, 4096] {
        let params = params_with_length(length);
        let run = run_one_probe(params.clone(), TimestampFields::default());
        let expected = echo_packet_len(false, &params);
        assert_reply_bytes(&run.reply, expected);
    }
}

// ─── Backend-neutral smoke ───

#[test]
fn backend_large_packet_smoke() {
    let params = params_with_length(1472);
    let peer = BackendPeer::start_open_echo(params.clone(), None);
    let mut client = Client::connect(ClientConfig {
        negotiation_policy: NegotiationPolicy::Loose,
        length: 1472,
        ..config_for_params(peer.addr(), &params)
    })
    .unwrap();

    let outcome = client.open().unwrap();
    assert!(matches!(outcome, irtt_client::OpenOutcome::Started { .. }));

    client.send_probe().unwrap();
    let events = client.recv_once().unwrap();
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], ClientEvent::EchoReply { .. }));

    client.close().unwrap();
}
