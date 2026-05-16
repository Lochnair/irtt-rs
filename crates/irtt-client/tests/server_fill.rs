mod support;

use irtt_client::{Client, ClientConfig, ClientError, NegotiationPolicy};
use irtt_proto::{Params, TimestampFields};

use support::{
    config_for_params, default_params, run_one_probe, run_one_probe_with_config, server_fill,
    start_open_server, OneProbeRun, ServerObservation,
};

use crate::support::test_echo_packet_len;

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
fn server_fill_none_short_and_max_map_into_open_params() {
    for fill in [None, Some("abc"), Some("0123456789abcdef0123456789abcdef")] {
        let mut params = default_params();
        params.server_fill = fill.and_then(server_fill);

        let run = run_one_probe(params, TimestampFields::default());
        assert_eq!(
            open_observation_params(&run)
                .server_fill
                .as_ref()
                .map(|f| f.value.as_str()),
            fill
        );
    }
}

#[test]
fn server_fill_empty_and_oversized_values_are_rejected_at_config_boundary() {
    for fill in ["", "0123456789abcdef0123456789abcdefx"] {
        let config = ClientConfig {
            server_fill: Some(fill.to_owned()),
            ..ClientConfig::default()
        };
        assert!(matches!(
            Client::connect(config),
            Err(ClientError::InvalidConfig { .. })
        ));
    }
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
fn server_fill_strict_rejects_removed_changed_or_unexpected_fill() {
    for (requested_fill, returned_fill) in [
        (Some("rand"), None),
        (Some("abc"), Some("xyz")),
        (None, Some("unexpected")),
    ] {
        let mut requested = default_params();
        requested.server_fill = requested_fill.and_then(server_fill);

        let mut returned = requested.clone();
        returned.server_fill = returned_fill.and_then(server_fill);

        let server = start_open_server(returned, None);
        let mut client = Client::connect(config_for_params(server.addr, &requested)).unwrap();
        assert!(matches!(
            client.open(),
            Err(ClientError::NegotiationRejected { .. })
        ));
        server.join();
    }
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
        test_echo_packet_len(false, &run_with.negotiated.params),
        echo_len_with
    );
    assert_eq!(
        test_echo_packet_len(false, &run_without.negotiated.params),
        echo_len_without
    );
}
