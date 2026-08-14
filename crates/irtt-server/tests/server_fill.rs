//! A negotiated fill over a real socket.
//!
//! The core tests are authoritative for fill bytes; this exists to prove the
//! whole path — negotiation, session, echo reply, UDP — puts the requested
//! pattern on the wire. It drives the server with the production protocol codecs
//! over a plain Tokio socket rather than through `AsyncClient`, because a
//! client's reply payload is not part of its event API and exposing it merely to
//! be asserted here would be the wrong trade.

use std::time::Duration;

use irtt_client::{
    AsyncClient, ClientConfig, NegotiationPolicy, NegotiationRestriction, OpenOutcome, SocketConfig,
};
use irtt_proto::{
    decode_echo_reply, decode_open_reply, echo_header_len, encode_request, Clock, Params,
    ReceivedStats, RequestToEncode, ServerFill, StampAt,
};
use irtt_server::{Server, ServerConfig};
use tokio::{net::UdpSocket, sync::oneshot, time::timeout};

const DEADBEEF: &[u8] = &[0xde, 0xad, 0xbe, 0xef];

#[tokio::test(flavor = "current_thread")]
async fn a_negotiated_pattern_reaches_the_wire() {
    timeout(Duration::from_secs(2), exercise_patterned_session())
        .await
        .expect("the fill integration exceeded its bounded runtime");
}

async fn exercise_patterned_session() {
    let mut server = Server::bind("127.0.0.1:0".parse().unwrap(), ServerConfig::default())
        .await
        .unwrap();
    let server_addr = server.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_task = tokio::spawn(async move {
        server
            .run(async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    socket.connect(server_addr).await.unwrap();

    let requested = requested_params(24);
    socket
        .send(
            &encode_request(
                RequestToEncode::Open {
                    params: &requested,
                    no_test: false,
                },
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let reply = decode_open_reply(&receive(&socket).await, None).unwrap();
    let negotiated = reply.params;
    assert_eq!(
        negotiated.server_fill, requested.server_fill,
        "a valid descriptor is returned unchanged, so strict negotiation holds"
    );

    // The request carries its own payload bytes, none of which may come back.
    socket
        .send(
            &encode_request(
                RequestToEncode::Echo {
                    token: reply.token,
                    sequence: 0,
                    params: &negotiated,
                    payload: &[0xa5; 24],
                },
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let packet = receive(&socket).await;
    let echo = decode_echo_reply(&packet, &negotiated, None).unwrap();

    assert_eq!(packet.len() as i64, negotiated.length);
    assert_eq!(
        echo.payload,
        DEADBEEF
            .iter()
            .copied()
            .cycle()
            .take(24)
            .collect::<Vec<_>>()
    );

    socket
        .send(&encode_request(RequestToEncode::Close { token: reply.token }, None).unwrap())
        .await
        .unwrap();

    shutdown_tx.send(()).unwrap();
    server_task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn a_client_sees_the_fallback_as_a_negotiation_restriction() {
    timeout(Duration::from_secs(2), exercise_fallback_negotiation())
        .await
        .expect("the negotiation integration exceeded its bounded runtime");
}

/// The fallback is deliberately visible to a client rather than hidden: a
/// descriptor this server could not honor comes back as the default one, which a
/// strict client refuses and a loose client accepts with a restriction recorded.
async fn exercise_fallback_negotiation() {
    let mut server = Server::bind("127.0.0.1:0".parse().unwrap(), ServerConfig::default())
        .await
        .unwrap();
    let server_addr = server.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_task = tokio::spawn(async move {
        server
            .run(async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    // `ClientConfig` accepts any non-empty descriptor within the wire bound, so
    // an unknown one genuinely reaches the server.
    let config = |policy| ClientConfig {
        server_addr: server_addr.to_string(),
        duration: Some(Duration::from_secs(1)),
        interval: Duration::from_millis(100),
        length: 64,
        server_fill: Some("bogus".to_owned()),
        negotiation_policy: policy,
        open_timeouts: vec![Duration::from_millis(200)],
        socket_config: SocketConfig {
            recv_timeout: Some(Duration::from_millis(200)),
            ..SocketConfig::default()
        },
        probe_timeout: Duration::from_millis(200),
        ..ClientConfig::default()
    };

    let mut strict = AsyncClient::connect(config(NegotiationPolicy::Strict))
        .await
        .unwrap();
    let rejected = strict
        .open()
        .await
        .expect_err("a strict client must refuse the changed fill");
    assert!(
        rejected.to_string().contains("fill"),
        "the restriction names the fill: {rejected}"
    );

    let mut loose = AsyncClient::connect(config(NegotiationPolicy::Loose))
        .await
        .unwrap();
    match loose.open().await.unwrap() {
        OpenOutcome::Started { negotiated, .. } => {
            assert_eq!(
                negotiated
                    .params
                    .server_fill
                    .map(|fill| fill.value)
                    .as_deref(),
                Some("pattern:69727474"),
                "the server reports the default descriptor it fell back to"
            );
            assert!(negotiated
                .restrictions
                .contains(&NegotiationRestriction::ServerFillChanged));
        }
        OpenOutcome::NoTestCompleted { .. } => panic!("normal client unexpectedly ran no-test"),
    }
    loose.close().await.unwrap();

    shutdown_tx.send(()).unwrap();
    server_task.await.unwrap().unwrap();
}

/// Params requesting a `deadbeef` fill and exactly `payload_len` payload bytes.
fn requested_params(payload_len: usize) -> Params {
    let mut params = Params {
        protocol_version: 1,
        duration_ns: 1_000_000_000,
        interval_ns: 100_000_000,
        length: 0,
        received_stats: ReceivedStats::Both,
        stamp_at: StampAt::Both,
        clock: Clock::Both,
        dscp: 0,
        server_fill: Some(ServerFill {
            value: "pattern:deadbeef".to_owned(),
        }),
    };
    params.length = (echo_header_len(false, &params) + payload_len) as i64;
    params
}

/// One datagram, with a bounded wait so a lost reply fails rather than hangs.
async fn receive(socket: &UdpSocket) -> Vec<u8> {
    let mut buffer = vec![0; 2048];
    let read = timeout(Duration::from_secs(1), socket.recv(&mut buffer))
        .await
        .expect("the server must answer within the test's bound")
        .unwrap();
    buffer.truncate(read);
    buffer
}
