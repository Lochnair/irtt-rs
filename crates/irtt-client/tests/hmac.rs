mod support;

use std::time::Duration;

use irtt_client::{
    Client, ClientConfig, ClientError, ClientEvent, OpenOutcome, SocketConfig, WarningKind,
};
use irtt_proto::{echo_packet_len, ProtoError, TimestampFields};

use support::{
    config_for_params, default_params, params_for_modes, run_one_probe_with_config,
    standard_timestamps, start_bad_hmac_echo_reply_server, start_hmac_close_server,
    start_hmac_required_open_drop_server, BackendPeer, ServerObservation, TOKEN,
};

#[test]
fn hmac_open_success_negotiates_without_warnings() {
    let key = b"compat-secret".to_vec();
    let params = default_params();
    let config_params = params.clone();
    let config_key = key.clone();

    let run = run_one_probe_with_config(
        params.clone(),
        TimestampFields::default(),
        Some(key),
        |addr| ClientConfig {
            hmac_key: Some(config_key),
            ..config_for_params(addr, &config_params)
        },
    );

    assert_eq!(run.negotiated.params, params);
    match &run.observations[0] {
        ServerObservation::Open { params: got, hmac } => {
            assert_eq!(got, &params);
            assert!(*hmac);
        }
        other => panic!("expected open observation, got {other:?}"),
    }
    assert_no_warning_or_error(&[run.sent, run.reply]);
}

#[test]
fn hmac_echo_success_verifies_request_and_accepts_reply() {
    let key = b"compat-secret".to_vec();
    let params = default_params();
    let config_params = params.clone();
    let config_key = key.clone();

    let run = run_one_probe_with_config(params.clone(), standard_timestamps(), Some(key), |addr| {
        ClientConfig {
            hmac_key: Some(config_key),
            ..config_for_params(addr, &config_params)
        }
    });

    match &run.observations[1] {
        ServerObservation::Echo {
            len,
            hmac,
            token,
            sequence,
        } => {
            assert_eq!(*len, echo_packet_len(true, &params));
            assert!(*hmac);
            assert_eq!(*token, TOKEN);
            assert_eq!(*sequence, 0);
        }
        other => panic!("expected echo observation, got {other:?}"),
    }

    match &run.reply {
        ClientEvent::EchoReply {
            seq,
            bytes,
            server_timing,
            ..
        } => {
            assert_eq!(*seq, 0);
            assert_eq!(*bytes, echo_packet_len(true, &params));
            assert!(server_timing.is_some());
        }
        other => panic!("expected EchoReply, got {other:?}"),
    }
    assert_no_duplicate_late_or_warning(&[run.reply]);
}

#[test]
fn hmac_close_success_sends_authenticated_close_and_closes_session() {
    let key = b"compat-secret".to_vec();
    let params = default_params();
    let server = start_hmac_close_server(params.clone(), key.clone());
    let mut config = config_for_params(server.addr, &params);
    config.hmac_key = Some(key);

    let mut client = Client::connect(config).unwrap();
    let outcome = client.open().unwrap();
    assert_started(outcome, &params);

    let events = client.close().unwrap();
    assert!(matches!(
        events.as_slice(),
        [ClientEvent::SessionClosed { token: TOKEN, .. }]
    ));
    assert!(matches!(
        client.send_probe(),
        Err(ClientError::AlreadyClosed)
    ));

    let observations = server.observations(2);
    match &observations[1] {
        ServerObservation::Close { hmac, token } => {
            assert!(*hmac);
            assert_eq!(*token, TOKEN);
        }
        other => panic!("expected close observation, got {other:?}"),
    }
    server.join();
}

#[test]
fn missing_client_key_against_hmac_required_server_times_out() {
    let key = b"compat-secret".to_vec();
    let server = start_hmac_required_open_drop_server(key, Duration::from_millis(250));
    let mut config = config_for_params(server.addr, &default_params());
    config.open_timeouts = vec![Duration::from_millis(200)];
    config.hmac_key = None;

    let mut client = Client::connect(config).unwrap();
    assert!(matches!(client.open(), Err(ClientError::OpenTimeout)));

    let observations = server.observations(1);
    assert!(matches!(
        observations.as_slice(),
        [ServerObservation::RejectedHmac {
            hmac: false,
            bad_hmac: true
        }]
    ));
    server.join();
}

#[test]
fn wrong_client_key_against_hmac_required_server_times_out() {
    let server_key = b"compat-secret".to_vec();
    let server = start_hmac_required_open_drop_server(server_key, Duration::from_millis(250));
    let mut config = config_for_params(server.addr, &default_params());
    config.open_timeouts = vec![Duration::from_millis(200)];
    config.hmac_key = Some(b"wrong-secret".to_vec());

    let mut client = Client::connect(config).unwrap();
    assert!(matches!(client.open(), Err(ClientError::OpenTimeout)));

    let observations = server.observations(1);
    assert!(matches!(
        observations.as_slice(),
        [ServerObservation::RejectedHmac {
            hmac: true,
            bad_hmac: true
        }]
    ));
    server.join();
}

#[test]
fn bad_hmac_echo_reply_is_rejected_without_echo_reply_event() {
    let key = b"compat-secret".to_vec();
    let params = default_params();
    let server = start_bad_hmac_echo_reply_server(params.clone(), key.clone());
    let mut config = config_for_params(server.addr, &params);
    config.hmac_key = Some(key);
    config.socket_config = SocketConfig {
        recv_timeout: Some(Duration::from_millis(500)),
        ..Default::default()
    };

    let mut client = Client::connect(config).unwrap();
    let outcome = client.open().unwrap();
    assert_started(outcome, &params);

    let sent = client.send_probe().unwrap();
    assert_eq!(sent.len(), 1);
    let events = client.recv_once().unwrap();
    assert!(matches!(
        events.as_slice(),
        [ClientEvent::Warning {
            kind: WarningKind::MalformedOrUnrelatedPacket,
            ..
        }]
    ));
    assert!(!events
        .iter()
        .any(|event| matches!(event, ClientEvent::EchoReply { .. })));

    let observations = server.observations(2);
    assert!(matches!(
        observations.as_slice(),
        [
            ServerObservation::Open { hmac: true, .. },
            ServerObservation::Echo {
                hmac: true,
                token: TOKEN,
                sequence: 0,
                ..
            }
        ]
    ));
    server.join();
}

#[test]
fn hmac_open_reply_with_bad_hmac_fails_with_protocol_error() {
    let key = b"compat-secret".to_vec();
    let wrong_key = b"wrong-secret".to_vec();
    let params = default_params();
    let server = support::start_bad_hmac_open_reply_server(params.clone(), key.clone(), wrong_key);
    let mut config = config_for_params(server.addr, &params);
    config.hmac_key = Some(key);

    let mut client = Client::connect(config).unwrap();
    assert!(matches!(
        client.open(),
        Err(ClientError::Protocol(ProtoError::BadHmac))
    ));
    server.join();
}

#[test]
fn non_hmac_client_open_does_not_set_hmac_flag() {
    let params = params_for_modes(
        irtt_proto::ReceivedStats::None,
        irtt_proto::StampAt::None,
        irtt_proto::Clock::Both,
    );
    let server = support::start_open_server(params.clone(), None);
    let mut config = config_for_params(server.addr, &params);
    config.hmac_key = None;

    let mut client = Client::connect(config).unwrap();
    let outcome = client.open().unwrap();
    assert_started(outcome, &params);

    let observations = server.observations(1);
    assert!(matches!(
        observations.as_slice(),
        [ServerObservation::Open { hmac: false, .. }]
    ));
    server.join();
}

fn assert_started(outcome: OpenOutcome, expected: &irtt_proto::Params) {
    match outcome {
        OpenOutcome::Started {
            token,
            negotiated,
            event:
                ClientEvent::SessionStarted {
                    token: event_token,
                    negotiated: event_negotiated,
                    ..
                },
            ..
        } => {
            assert_eq!(token, TOKEN);
            assert_eq!(event_token, TOKEN);
            assert_eq!(negotiated.params, *expected);
            assert_eq!(event_negotiated.params, *expected);
        }
        other => panic!("expected started open outcome, got {other:?}"),
    }
}

fn assert_no_warning_or_error(events: &[ClientEvent]) {
    assert!(!events.iter().any(|event| {
        matches!(
            event,
            ClientEvent::Warning { .. }
                | ClientEvent::DuplicateReply { .. }
                | ClientEvent::LateReply { .. }
        )
    }));
}

fn assert_no_duplicate_late_or_warning(events: &[ClientEvent]) {
    assert_no_warning_or_error(events);
}

#[test]
fn backend_hmac_correct_key_succeeds() {
    let key = b"compat-secret".to_vec();
    let params = default_params();
    let peer = BackendPeer::start_open_echo(params.clone(), Some(key.clone()));
    let mut config = config_for_params(peer.addr(), &params);
    config.hmac_key = Some(key);

    let mut client = Client::connect(config).unwrap();
    let outcome = client.open().unwrap();
    assert!(matches!(outcome, OpenOutcome::Started { .. }));

    let sent = client.send_probe().unwrap();
    assert_eq!(sent.len(), 1);

    let events = client.recv_once().unwrap();
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], ClientEvent::EchoReply { .. }));

    client.close().unwrap();
}

#[test]
fn backend_hmac_wrong_key_fails() {
    let server_key = b"compat-secret".to_vec();
    let peer = BackendPeer::start_hmac_required(server_key);
    let mut config = config_for_params(peer.addr(), &default_params());
    config.open_timeouts = vec![Duration::from_millis(200)];
    config.hmac_key = Some(b"wrong-secret".to_vec());

    let mut client = Client::connect(config).unwrap();
    assert!(matches!(client.open(), Err(ClientError::OpenTimeout)));
}

#[test]
fn backend_hmac_missing_key_fails() {
    let server_key = b"compat-secret".to_vec();
    let peer = BackendPeer::start_hmac_required(server_key);
    let mut config = config_for_params(peer.addr(), &default_params());
    config.open_timeouts = vec![Duration::from_millis(200)];
    config.hmac_key = None;

    let mut client = Client::connect(config).unwrap();
    assert!(matches!(client.open(), Err(ClientError::OpenTimeout)));
}
