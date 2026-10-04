//! The server's capability restrictions as a real client sees them.
//!
//! The core tests are authoritative for the negotiated values; these exist to
//! prove the two ends agree over a socket. A restriction this server applies has
//! to arrive at `irtt-client` as one of the restrictions it already models: a
//! strict client refuses the changed value, and a loose one accepts it, records
//! it and runs the session. Nothing in the client changed for this — that is
//! what is being checked.

use std::time::Duration;

use irtt_client::{
    AsyncClient, ClientConfig, NegotiationChange, NegotiationPolicy, OpenOutcome, OpenPolicy,
    SessionRequest,
};
use irtt_proto::{Clock, ReceivedStats, StampAt};
use irtt_server::{Server, ServerConfig, TimestampAllowance};
use tokio::{sync::oneshot, time::timeout};

#[tokio::test(flavor = "current_thread")]
async fn a_single_timestamp_allowance_reaches_a_strict_and_a_loose_client() {
    timeout(Duration::from_secs(2), exercise_timestamp_restriction())
        .await
        .expect("the timestamp negotiation integration exceeded its bounded runtime");
}

async fn exercise_timestamp_restriction() {
    let server = ServerConfig::default().with_timestamp_allowance(TimestampAllowance::Single);
    let (server_addr, shutdown_tx, server_task) = serve(server).await;

    let mut strict = client(server_addr, NegotiationPolicy::Strict).await;
    let rejected = strict
        .open()
        .await
        .expect_err("a strict client must refuse the reduced timestamp placement");
    assert!(
        rejected.to_string().contains("stamp-at"),
        "the restriction names the timestamp placement: {rejected}"
    );

    let mut loose = client(server_addr, NegotiationPolicy::Loose).await;
    let negotiated = match loose.open().await.unwrap() {
        OpenOutcome::Started(irtt_client::SessionStarted {
            negotiation: negotiated,
            ..
        }) => negotiated,
        OpenOutcome::NoTestCompleted(_) => panic!("normal client unexpectedly ran no-test"),
    };
    assert_eq!(
        negotiated.accepted.stamp_at,
        StampAt::Midpoint,
        "a request for both instants is answered with the midpoint"
    );
    assert_eq!(
        negotiated.accepted.clock,
        Clock::Both,
        "the clock is not restricted with it"
    );
    assert!(negotiated
        .changes
        .contains(&NegotiationChange::StampAtChanged {
            requested: StampAt::Both,
            negotiated: StampAt::Midpoint,
        }));

    probe(&mut loose).await;
    loose.close().await.unwrap();

    shutdown_tx.send(()).unwrap();
    server_task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn a_disallowed_dscp_reaches_a_strict_and_a_loose_client() {
    timeout(Duration::from_secs(2), exercise_dscp_restriction())
        .await
        .expect("the DSCP negotiation integration exceeded its bounded runtime");
}

async fn exercise_dscp_restriction() {
    let (server_addr, shutdown_tx, server_task) = serve(
        ServerConfig::default()
            .with_min_send_interval(Duration::ZERO)
            .with_dscp_allowed(false),
    )
    .await;

    // The client's configuration is a DSCP codepoint; 46 (EF) is raw byte 184 on
    // the wire, and this test does not change either convention.
    let with_dscp = |policy| ClientConfig {
        request: SessionRequest {
            dscp: 46,
            ..test_config(policy).request
        },
        ..test_config(policy)
    };

    let mut strict = AsyncClient::connect(
        server_addr.to_string(),
        with_dscp(NegotiationPolicy::Strict),
    )
    .await
    .unwrap();
    let rejected = strict
        .open()
        .await
        .expect_err("a strict client must refuse the removed marking");
    assert!(
        rejected.to_string().contains("DSCP"),
        "the restriction names DSCP: {rejected}"
    );

    let mut loose =
        AsyncClient::connect(server_addr.to_string(), with_dscp(NegotiationPolicy::Loose))
            .await
            .unwrap();
    let negotiated = match loose.open().await.unwrap() {
        OpenOutcome::Started(irtt_client::SessionStarted {
            negotiation: negotiated,
            ..
        }) => negotiated,
        OpenOutcome::NoTestCompleted(_) => panic!("normal client unexpectedly ran no-test"),
    };
    assert_eq!(
        negotiated.accepted.dscp, 0,
        "the accepted DSCP codepoint is zero"
    );
    assert!(negotiated
        .changes
        .contains(&NegotiationChange::DscpChanged {
            requested: 46,
            negotiated: 0,
        }));

    probe(&mut loose).await;
    loose.close().await.unwrap();

    shutdown_tx.send(()).unwrap();
    server_task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn a_default_server_restricts_nothing_an_ordinary_client_asks_for() {
    timeout(Duration::from_secs(2), exercise_default_policy())
        .await
        .expect("the default policy integration exceeded its bounded runtime");
}

/// The controls are opt-in, and this is what that has to mean: an ordinary
/// client against an unconfigured server negotiates with no restriction at all,
/// under the default *strict* policy that would refuse one.
async fn exercise_default_policy() {
    let (server_addr, shutdown_tx, server_task) = serve(ServerConfig::default()).await;

    let mut client = AsyncClient::connect(
        server_addr.to_string(),
        ClientConfig {
            request: SessionRequest {
                dscp: 46,
                ..test_config(NegotiationPolicy::Strict).request
            },
            ..test_config(NegotiationPolicy::Strict)
        },
    )
    .await
    .unwrap();
    let negotiated = match client.open().await.unwrap() {
        OpenOutcome::Started(irtt_client::SessionStarted {
            negotiation: negotiated,
            ..
        }) => negotiated,
        OpenOutcome::NoTestCompleted(_) => panic!("normal client unexpectedly ran no-test"),
    };

    assert_eq!(negotiated.changes, Vec::new());
    assert_eq!(negotiated.accepted.stamp_at, StampAt::Both);
    assert_eq!(negotiated.accepted.clock, Clock::Both);
    assert_eq!(negotiated.accepted.dscp, 46);
    assert_eq!(
        negotiated.peer_params.dscp, 184,
        "the raw byte for codepoint 46"
    );

    probe(&mut client).await;
    client.close().await.unwrap();

    shutdown_tx.send(()).unwrap();
    server_task.await.unwrap().unwrap();
}

/// Starts one listener and returns its address, its shutdown trigger and its
/// task.
async fn serve(
    config: ServerConfig,
) -> (
    std::net::SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<(), irtt_server::ServerRuntimeError>>,
) {
    let mut server = Server::bind("127.0.0.1:0".parse().unwrap(), config)
        .await
        .unwrap();
    let server_addr = server.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        server
            .run(async {
                let _ = shutdown_rx.await;
            })
            .await
    });
    (server_addr, shutdown_tx, task)
}

/// A short deterministic client requesting both timestamps from both clocks.
fn test_config(policy: NegotiationPolicy) -> ClientConfig {
    ClientConfig {
        open: OpenPolicy {
            negotiation: policy,
            timeouts: vec![Duration::from_millis(200)],
        },
        request: SessionRequest {
            duration: Some(Duration::from_secs(1)),
            interval: Duration::from_millis(100),
            length: 64,
            received_stats: ReceivedStats::Both,
            stamp_at: StampAt::Both,
            clock: Clock::Both,
            ..Default::default()
        },

        probe_timeout: Duration::from_millis(200),
        ..ClientConfig::default()
    }
}

async fn client(server_addr: std::net::SocketAddr, policy: NegotiationPolicy) -> AsyncClient {
    AsyncClient::connect(server_addr.to_string(), test_config(policy))
        .await
        .unwrap()
}

/// One probe, answered: a restricted session is an ordinary session.
async fn probe(client: &mut AsyncClient) {
    client.send_probe().await.unwrap();
    let events = client.recv().await.unwrap();
    assert!(
        events
            .iter()
            .any(|event| matches!(event, irtt_client::ClientEvent::EchoReply { seq: 0, .. })),
        "the restricted session must answer an ordinary probe: {events:?}"
    );
}
