mod support;

use irtt_client::{Client, ClientConfig, ClientError, ClientEvent, NegotiationPolicy};
use irtt_proto::{echo_packet_len, Params, TimestampFields};

use support::{
    config_for_params, default_params, run_one_probe, run_one_probe_with_config, server_fill,
    start_open_server, OneProbeRun, ServerObservation,
};

fn open_observation_params(run: &OneProbeRun) -> &Params {
    match &run.observations[0] {
        ServerObservation::Open { params, .. } => params,
        other => panic!("expected Open observation, got {other:?}"),
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
        ref other => panic!("expected Echo observation, got {other:?}"),
    }
}

// ─── Config mapping ───

#[test]
fn server_fill_none_produces_no_fill_in_open_params() {
    let params = default_params();
    let run = run_one_probe(params, TimestampFields::default());
    assert!(open_observation_params(&run).server_fill.is_none());
}

#[test]
fn server_fill_short_value_maps_into_open_params() {
    let mut params = default_params();
    params.server_fill = server_fill("abc");
    let run = run_one_probe(params, TimestampFields::default());
    assert_eq!(
        open_observation_params(&run)
            .server_fill
            .as_ref()
            .map(|f| f.value.as_str()),
        Some("abc")
    );
}

#[test]
fn server_fill_32_byte_value_maps_into_open_params() {
    let mut params = default_params();
    let fill = "0123456789abcdef0123456789abcdef";
    params.server_fill = server_fill(fill);
    let run = run_one_probe(params, TimestampFields::default());
    assert_eq!(
        open_observation_params(&run)
            .server_fill
            .as_ref()
            .map(|f| f.value.as_str()),
        Some(fill)
    );
}

#[test]
fn server_fill_over_32_bytes_rejected_at_config_boundary() {
    let config = ClientConfig {
        server_fill: Some("0123456789abcdef0123456789abcdefx".to_owned()),
        ..ClientConfig::default()
    };
    assert!(matches!(
        Client::connect(config),
        Err(ClientError::InvalidConfig { .. })
    ));
}

#[test]
fn server_fill_empty_rejected_at_config_boundary() {
    let config = ClientConfig {
        server_fill: Some("".to_owned()),
        ..ClientConfig::default()
    };
    assert!(matches!(
        Client::connect(config),
        Err(ClientError::InvalidConfig { .. })
    ));
}

// ─── Open negotiation ───

#[test]
fn server_fill_negotiated_params_reflect_accepted_value() {
    let mut params = default_params();
    params.server_fill = server_fill("rand");
    let run = run_one_probe(params.clone(), TimestampFields::default());
    assert_eq!(run.negotiated.params.server_fill, params.server_fill);
}

#[test]
fn server_fill_strict_rejects_removed_fill() {
    let mut requested = default_params();
    requested.server_fill = server_fill("rand");

    let mut returned = requested.clone();
    returned.server_fill = None;

    let server = start_open_server(returned, None);
    let mut client = Client::connect(config_for_params(server.addr, &requested)).unwrap();
    assert!(matches!(
        client.open(),
        Err(ClientError::NegotiationRejected { .. })
    ));
    server.join();
}

#[test]
fn server_fill_strict_rejects_different_fill_value() {
    let mut requested = default_params();
    requested.server_fill = server_fill("abc");

    let mut returned = requested.clone();
    returned.server_fill = server_fill("xyz");

    let server = start_open_server(returned, None);
    let mut client = Client::connect(config_for_params(server.addr, &requested)).unwrap();
    assert!(matches!(
        client.open(),
        Err(ClientError::NegotiationRejected { .. })
    ));
    server.join();
}

#[test]
fn server_fill_strict_rejects_unexpected_fill_from_server() {
    let requested = default_params();

    let mut returned = requested.clone();
    returned.server_fill = server_fill("unexpected");

    let server = start_open_server(returned, None);
    let mut client = Client::connect(config_for_params(server.addr, &requested)).unwrap();
    assert!(matches!(
        client.open(),
        Err(ClientError::NegotiationRejected { .. })
    ));
    server.join();
}

#[test]
fn server_fill_loose_allows_server_to_remove_fill() {
    let mut requested = default_params();
    requested.server_fill = server_fill("rand");

    let mut returned = requested.clone();
    returned.server_fill = None;

    let requested_for_config = requested.clone();
    let run = run_one_probe_with_config(returned, TimestampFields::default(), None, |addr| {
        ClientConfig {
            negotiation_policy: NegotiationPolicy::Loose,
            ..config_for_params(addr, &requested_for_config)
        }
    });

    assert_eq!(
        open_observation_params(&run).server_fill,
        requested.server_fill
    );
    assert_eq!(run.negotiated.params.server_fill, None);
}

// ─── Echo behavior ───

#[test]
fn server_fill_echo_reply_decoded_with_fill_configured() {
    let mut params = default_params();
    params.server_fill = server_fill("rand");
    let run = run_one_probe(params, TimestampFields::default());
    assert!(matches!(run.reply, ClientEvent::EchoReply { .. }));
}

// server_fill is a negotiation/config parameter only; it does not affect
// client-observable reply payload layout. The client sends server_fill in
// the open request and expects the server to use it for reply payload
// content, but the client's decode/echo layout logic is independent of
// server_fill. Only the length parameter expands the packet beyond the
// protocol header.
#[test]
fn server_fill_does_not_affect_client_side_packet_layout() {
    let mut params_with_fill = default_params();
    params_with_fill.server_fill = server_fill("rand");

    let params_without_fill = default_params();

    let run_with = run_one_probe(params_with_fill, TimestampFields::default());
    let run_without = run_one_probe(params_without_fill, TimestampFields::default());

    let (echo_len_with, _, _, _) = echo_observation(&run_with);
    let (echo_len_without, _, _, _) = echo_observation(&run_without);
    assert_eq!(echo_len_with, echo_len_without);
    assert_eq!(
        echo_packet_len(false, &run_with.negotiated.params),
        echo_len_with
    );
    assert_eq!(
        echo_packet_len(false, &run_without.negotiated.params),
        echo_len_without
    );
}
