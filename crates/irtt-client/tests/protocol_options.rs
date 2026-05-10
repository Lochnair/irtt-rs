mod support;

use irtt_client::ClientTimestamp;
use std::time::Duration;

use irtt_client::{
    Client, ClientConfig, ClientError, ClientEvent, OneWayDelaySample, ReceivedStatsSample,
    RttSample, ServerTiming,
};
use irtt_proto::{echo_packet_len, Clock, Params, ReceivedStats, StampAt, TimestampFields};

use support::{
    config_for_params, default_params, params_for_modes, run_one_probe, run_one_probe_with_config,
    server_fill, standard_timestamps, start_open_server, BackendPeer, OneProbeRun,
    ServerObservation, RECV_COUNT, RECV_WINDOW, TOKEN,
};

struct ReplyView<'a> {
    seq: u32,
    logical_seq: u64,
    rtt: &'a RttSample,
    server_timing: Option<&'a ServerTiming>,
    one_way: Option<&'a OneWayDelaySample>,
    received_stats: Option<&'a ReceivedStatsSample>,
    bytes: usize,
}

#[test]
fn received_stats_modes_drive_negotiated_echo_and_reply_events() {
    for (mode, expected_count, expected_window) in [
        (ReceivedStats::None, None, None),
        (ReceivedStats::Count, Some(RECV_COUNT), None),
        (ReceivedStats::Window, None, Some(RECV_WINDOW)),
        (ReceivedStats::Both, Some(RECV_COUNT), Some(RECV_WINDOW)),
    ] {
        let params = params_for_modes(mode, StampAt::None, Clock::Both);
        let run = run_one_probe(params.clone(), TimestampFields::default());
        assert_negotiated_echo_use(&run, &params, false);

        let reply = expect_echo_reply(&run.reply);
        assert_eq!(reply.seq, 0);
        assert_eq!(reply.logical_seq, 0);
        assert_eq!(reply.bytes, echo_packet_len(false, &params));
        assert!(reply.server_timing.is_none());
        assert!(reply.one_way.is_none());
        assert_received_stats(reply.received_stats, expected_count, expected_window);
    }
}

#[test]
fn timestamp_modes_drive_reply_event_timing_fields() {
    for mode in [
        StampAt::None,
        StampAt::Send,
        StampAt::Receive,
        StampAt::Both,
        StampAt::Midpoint,
    ] {
        let params = params_for_modes(ReceivedStats::None, mode, Clock::Both);
        let run = run_one_probe(params.clone(), standard_timestamps());
        assert_negotiated_echo_use(&run, &params, false);

        let reply = expect_echo_reply(&run.reply);
        assert!(reply.received_stats.is_none());
        match mode {
            StampAt::None => {
                assert!(reply.server_timing.is_none());
                assert!(reply.one_way.is_none());
                assert!(reply.rtt.adjusted.is_none());
                assert_eq!(reply.rtt.effective, reply.rtt.raw);
                assert!(reply.rtt.raw > Duration::ZERO);
            }
            StampAt::Send => {
                assert_timing_fields(reply.server_timing, false, false, false, false, true, true);
                assert_one_way_presence(reply.one_way, false, true);
                assert!(reply.rtt.adjusted.is_none());
            }
            StampAt::Receive => {
                assert_timing_fields(reply.server_timing, true, true, false, false, false, false);
                assert_one_way_presence(reply.one_way, true, false);
                assert!(reply.rtt.adjusted.is_none());
            }
            StampAt::Both => {
                assert_timing_fields(reply.server_timing, true, true, false, false, true, true);
                assert_one_way_presence(reply.one_way, true, true);
                assert_processing_subtracted(reply);
            }
            StampAt::Midpoint => {
                assert_timing_fields(reply.server_timing, false, false, true, true, false, false);
                assert_one_way_presence(reply.one_way, true, true);
                assert!(reply.rtt.adjusted.is_none());
            }
        }
    }
}

#[test]
fn clock_modes_drive_reply_event_clock_fields() {
    for clock in [Clock::Wall, Clock::Monotonic, Clock::Both] {
        let params = params_for_modes(ReceivedStats::None, StampAt::Both, clock);
        let run = run_one_probe(params.clone(), standard_timestamps());
        assert_negotiated_echo_use(&run, &params, false);

        let reply = expect_echo_reply(&run.reply);
        assert!(reply.received_stats.is_none());
        match clock {
            Clock::Wall => {
                assert_timing_fields(reply.server_timing, true, false, false, false, true, false);
                assert_one_way_presence(reply.one_way, true, true);
            }
            Clock::Monotonic => {
                assert_timing_fields(reply.server_timing, false, true, false, false, false, true);
                assert!(reply.one_way.is_none());
            }
            Clock::Both => {
                assert_timing_fields(reply.server_timing, true, true, false, false, true, true);
                assert_one_way_presence(reply.one_way, true, true);
            }
        }
        assert_processing_subtracted(reply);
    }
}

#[test]
fn rich_mode_exposes_stats_server_timing_and_adjusted_rtt() {
    let params = params_for_modes(ReceivedStats::Both, StampAt::Both, Clock::Both);
    let run = run_one_probe(params.clone(), standard_timestamps());
    assert_negotiated_echo_use(&run, &params, false);

    let reply = expect_echo_reply(&run.reply);
    assert_received_stats(reply.received_stats, Some(RECV_COUNT), Some(RECV_WINDOW));
    assert_timing_fields(reply.server_timing, true, true, false, false, true, true);
    assert_one_way_presence(reply.one_way, true, true);
    assert_processing_subtracted(reply);
}

#[test]
fn hmac_rich_mode_uses_negotiated_echo_layout_and_decodes_reply() {
    let key = b"compat-secret".to_vec();
    let params = params_for_modes(ReceivedStats::Both, StampAt::Both, Clock::Both);
    let config_params = params.clone();
    let config_key = key.clone();
    let run = run_one_probe_with_config(params.clone(), standard_timestamps(), Some(key), |addr| {
        ClientConfig {
            hmac_key: Some(config_key),
            ..config_for_params(addr, &config_params)
        }
    });
    assert_negotiated_echo_use(&run, &params, true);

    let reply = expect_echo_reply(&run.reply);
    assert_eq!(reply.bytes, echo_packet_len(true, &params));
    assert_received_stats(reply.received_stats, Some(RECV_COUNT), Some(RECV_WINDOW));
    assert_timing_fields(reply.server_timing, true, true, false, false, true, true);
}

#[test]
fn strict_open_rejects_changed_compatibility_params_from_server() {
    let mut requested = default_params();
    requested.length = 256;
    requested.dscp = 46;
    requested.server_fill = server_fill("rand");

    for returned in changed_compatibility_params(&requested) {
        let server = start_open_server(returned, None);
        let mut config = config_for_params(server.addr, &requested);
        config.server_addr = server.addr.to_string();
        let mut client = Client::connect(config).unwrap();
        assert!(matches!(
            client.open(),
            Err(ClientError::NegotiationRejected { .. })
        ));
        server.join();
    }
}

#[test]
fn loose_open_uses_returned_params_for_echo_layout_and_reply_parsing() {
    let mut requested = default_params();
    requested.length = 128;
    requested.dscp = 46;
    requested.server_fill = server_fill("rand");

    let mut returned = requested.clone();
    returned.length = 28;
    returned.received_stats = ReceivedStats::Count;
    returned.stamp_at = StampAt::Receive;
    returned.clock = Clock::Wall;
    returned.dscp = 8;
    returned.server_fill = None;

    let requested_for_config = requested.clone();
    let run = run_one_probe_with_config(returned.clone(), standard_timestamps(), None, |addr| {
        ClientConfig {
            negotiation_policy: irtt_client::NegotiationPolicy::Loose,
            ..config_for_params(addr, &requested_for_config)
        }
    });

    assert_eq!(open_params(&run), &requested);
    assert_eq!(run.negotiated.params, returned);
    assert_echo_uses_params(&run, &returned, false);

    let reply = expect_echo_reply(&run.reply);
    assert_received_stats(reply.received_stats, Some(RECV_COUNT), None);
    assert_timing_fields(reply.server_timing, true, false, false, false, false, false);
    assert_one_way_presence(reply.one_way, true, false);
    assert!(reply.rtt.adjusted.is_none());
}

fn changed_compatibility_params(requested: &Params) -> Vec<Params> {
    let mut changed = Vec::new();

    let mut returned = requested.clone();
    returned.received_stats = ReceivedStats::Count;
    changed.push(returned);

    let mut returned = requested.clone();
    returned.stamp_at = StampAt::Midpoint;
    changed.push(returned);

    let mut returned = requested.clone();
    returned.clock = Clock::Wall;
    changed.push(returned);

    let mut returned = requested.clone();
    returned.server_fill = None;
    changed.push(returned);

    let mut returned = requested.clone();
    returned.dscp = 8;
    changed.push(returned);

    let mut returned = requested.clone();
    returned.length = 128;
    changed.push(returned);

    changed
}

fn assert_negotiated_echo_use(run: &OneProbeRun, params: &Params, hmac: bool) {
    assert_eq!(run.negotiated.params, *params);
    assert_eq!(open_params(run), params);
    assert_echo_uses_params(run, params, hmac);
}

fn assert_echo_uses_params(run: &OneProbeRun, params: &Params, hmac: bool) {
    let (echo_len, echo_hmac, token, sequence) = echo_observation(run);
    assert_eq!(echo_len, echo_packet_len(hmac, params));
    assert_eq!(echo_hmac, hmac);
    assert_eq!(token, TOKEN);
    assert_eq!(sequence, 0);

    match &run.sent {
        ClientEvent::EchoSent {
            seq,
            logical_seq,
            bytes,
            ..
        } => {
            assert_eq!(*seq, 0);
            assert_eq!(*logical_seq, 0);
            assert_eq!(*bytes, echo_packet_len(hmac, params));
        }
        other => panic!("expected EchoSent, got {other:?}"),
    }
}

fn open_params(run: &OneProbeRun) -> &Params {
    match &run.observations[0] {
        ServerObservation::Open { params, .. } => params,
        other => panic!("expected open observation, got {other:?}"),
    }
}

fn echo_observation(run: &OneProbeRun) -> (usize, bool, u64, u32) {
    match run.observations[1] {
        ServerObservation::Echo {
            len,
            hmac,
            token,
            sequence,
        } => (len, hmac, token, sequence),
        ref other => panic!("expected echo observation, got {other:?}"),
    }
}

fn expect_echo_reply(event: &ClientEvent) -> ReplyView<'_> {
    match event {
        ClientEvent::EchoReply {
            seq,
            logical_seq,
            rtt,
            server_timing,
            one_way,
            received_stats,
            bytes,
            ..
        } => ReplyView {
            seq: *seq,
            logical_seq: *logical_seq,
            rtt,
            server_timing: server_timing.as_ref(),
            one_way: one_way.as_ref(),
            received_stats: received_stats.as_ref(),
            bytes: *bytes,
        },
        other => panic!("expected EchoReply, got {other:?}"),
    }
}

fn assert_received_stats(
    sample: Option<&ReceivedStatsSample>,
    expected_count: Option<u32>,
    expected_window: Option<u64>,
) {
    if expected_count.is_none() && expected_window.is_none() {
        assert!(sample.is_none());
    } else {
        let sample = sample.unwrap();
        assert_eq!(sample.count, expected_count);
        assert_eq!(sample.window, expected_window);
    }
}

fn assert_timing_fields(
    sample: Option<&ServerTiming>,
    recv_wall: bool,
    recv_mono: bool,
    midpoint_wall: bool,
    midpoint_mono: bool,
    send_wall: bool,
    send_mono: bool,
) {
    if !(recv_wall || recv_mono || midpoint_wall || midpoint_mono || send_wall || send_mono) {
        assert!(sample.is_none());
        return;
    }

    let sample = sample.unwrap();
    assert_eq!(sample.receive_wall_ns.is_some(), recv_wall);
    assert_eq!(sample.receive_mono_ns.is_some(), recv_mono);
    assert_eq!(sample.midpoint_wall_ns.is_some(), midpoint_wall);
    assert_eq!(sample.midpoint_mono_ns.is_some(), midpoint_mono);
    assert_eq!(sample.send_wall_ns.is_some(), send_wall);
    assert_eq!(sample.send_mono_ns.is_some(), send_mono);
    assert_eq!(
        sample.processing.is_some(),
        (recv_mono && send_mono) || (recv_wall && send_wall)
    );
}

fn assert_processing_subtracted(reply: ReplyView<'_>) {
    let processing = reply.server_timing.unwrap().processing.unwrap();
    assert_eq!(processing, Duration::from_nanos(1));
    let adjusted = reply.rtt.adjusted.unwrap();
    assert_eq!(reply.rtt.effective, adjusted);
    assert!(adjusted < reply.rtt.raw);
}

fn assert_one_way_presence(
    sample: Option<&OneWayDelaySample>,
    client_to_server: bool,
    server_to_client: bool,
) {
    if !(client_to_server || server_to_client) {
        assert!(sample.is_none());
        return;
    }

    let sample = sample.unwrap();
    assert_eq!(sample.client_to_server.is_some(), client_to_server);
    assert_eq!(sample.server_to_client.is_some(), server_to_client);
}

#[test]
fn backend_basic_open_echo_close() {
    let params = default_params();
    let peer = BackendPeer::start_open_echo(params.clone(), None);
    let mut client = Client::connect(config_for_params(peer.addr(), &params)).unwrap();

    let outcome = client.open().unwrap();
    assert!(matches!(outcome, irtt_client::OpenOutcome::Started { .. }));

    let sent = client.send_probe().unwrap();
    assert_eq!(sent.len(), 1);

    let events = client.recv_once().unwrap();
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], ClientEvent::EchoReply { .. }));

    client.close(ClientTimestamp::now()).unwrap();
}

#[test]
fn backend_ttl_smoke() {
    let params = default_params();
    let peer = BackendPeer::start_open_echo(params.clone(), None);
    let mut config = config_for_params(peer.addr(), &params);
    config.socket_config.ttl = Some(64);
    let mut client = Client::connect(config).unwrap();

    let outcome = client.open().unwrap();
    assert!(matches!(outcome, irtt_client::OpenOutcome::Started { .. }));

    let sent = client.send_probe().unwrap();
    assert_eq!(sent.len(), 1);

    let events = client.recv_once().unwrap();
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], ClientEvent::EchoReply { .. }));

    client.close(ClientTimestamp::now()).unwrap();
}

#[test]
fn dscp_configured_open_close_smoke() {
    let mut params = default_params();
    params.dscp = 46;
    let server = start_open_server(params.clone(), None);
    let mut client = Client::connect(config_for_params(server.addr, &params)).unwrap();

    let outcome = client.open().unwrap();
    assert!(matches!(outcome, irtt_client::OpenOutcome::Started { .. }));
    client.close(ClientTimestamp::now()).unwrap();
    server.join();
}

#[test]
fn backend_received_stats_smoke() {
    for mode in [
        ReceivedStats::None,
        ReceivedStats::Count,
        ReceivedStats::Window,
        ReceivedStats::Both,
    ] {
        let params = params_for_modes(mode, StampAt::None, Clock::Both);
        let peer = BackendPeer::start_open_echo(params.clone(), None);
        let mut client = Client::connect(config_for_params(peer.addr(), &params)).unwrap();

        client.open().unwrap();
        client.send_probe().unwrap();
        let events = client.recv_once().unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], ClientEvent::EchoReply { .. }));

        client.close(ClientTimestamp::now()).unwrap();
    }
}

#[test]
fn backend_timestamp_smoke() {
    for mode in [
        StampAt::None,
        StampAt::Send,
        StampAt::Receive,
        StampAt::Both,
        StampAt::Midpoint,
    ] {
        let params = params_for_modes(ReceivedStats::None, mode, Clock::Both);
        let peer = BackendPeer::start_open_echo(params.clone(), None);
        let mut client = Client::connect(config_for_params(peer.addr(), &params)).unwrap();

        client.open().unwrap();
        client.send_probe().unwrap();
        let events = client.recv_once().unwrap();
        assert_eq!(events.len(), 1);

        match &events[0] {
            ClientEvent::EchoReply {
                server_timing,
                one_way,
                ..
            } => match mode {
                StampAt::None => {
                    assert!(server_timing.is_none());
                    assert!(one_way.is_none());
                }
                StampAt::Send => {
                    let st = server_timing.as_ref().unwrap();
                    assert!(st.send_wall_ns.is_some() || st.send_mono_ns.is_some());
                }
                StampAt::Receive => {
                    let st = server_timing.as_ref().unwrap();
                    assert!(st.receive_wall_ns.is_some() || st.receive_mono_ns.is_some());
                }
                StampAt::Both => {
                    let st = server_timing.as_ref().unwrap();
                    assert!(st.receive_wall_ns.is_some() || st.receive_mono_ns.is_some());
                    assert!(st.send_wall_ns.is_some() || st.send_mono_ns.is_some());
                }
                StampAt::Midpoint => {
                    let st = server_timing.as_ref().unwrap();
                    assert!(st.midpoint_wall_ns.is_some() || st.midpoint_mono_ns.is_some());
                }
            },
            other => panic!("expected EchoReply, got {other:?}"),
        }

        client.close(ClientTimestamp::now()).unwrap();
    }
}

#[test]
fn backend_clock_smoke() {
    for clock in [Clock::Wall, Clock::Monotonic, Clock::Both] {
        let params = params_for_modes(ReceivedStats::None, StampAt::Both, clock);
        let peer = BackendPeer::start_open_echo(params.clone(), None);
        let mut client = Client::connect(config_for_params(peer.addr(), &params)).unwrap();

        client.open().unwrap();
        client.send_probe().unwrap();
        let events = client.recv_once().unwrap();
        assert_eq!(events.len(), 1);

        match &events[0] {
            ClientEvent::EchoReply { server_timing, .. } => {
                let st = server_timing.as_ref().unwrap();
                match clock {
                    Clock::Wall => {
                        assert!(st.receive_wall_ns.is_some());
                        assert!(st.send_wall_ns.is_some());
                    }
                    Clock::Monotonic => {
                        assert!(st.receive_mono_ns.is_some());
                        assert!(st.send_mono_ns.is_some());
                    }
                    Clock::Both => {
                        assert!(st.receive_wall_ns.is_some());
                        assert!(st.receive_mono_ns.is_some());
                        assert!(st.send_wall_ns.is_some());
                        assert!(st.send_mono_ns.is_some());
                    }
                }
            }
            other => panic!("expected EchoReply, got {other:?}"),
        }

        client.close(ClientTimestamp::now()).unwrap();
    }
}
