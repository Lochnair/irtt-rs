use std::time::Duration;

use irtt_client::{AsyncClient, ClientConfig, ClientEvent, OpenOutcome, SocketConfig};
use irtt_proto::{Clock, ReceivedStats, StampAt};
use irtt_server::{Server, ServerConfig};
use tokio::{sync::oneshot, time::timeout};

#[tokio::test(flavor = "current_thread")]
async fn async_client_opens_echoes_receives_and_closes() {
    timeout(Duration::from_secs(2), exercise_client_server())
        .await
        .expect("client/server integration exceeded its bounded runtime");
}

async fn exercise_client_server() {
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

    let config = ClientConfig {
        server_addr: server_addr.to_string(),
        duration: Some(Duration::from_secs(1)),
        interval: Duration::from_millis(100),
        length: 64,
        received_stats: ReceivedStats::Both,
        stamp_at: StampAt::Both,
        clock: Clock::Both,
        open_timeouts: vec![Duration::from_millis(200)],
        socket_config: SocketConfig {
            recv_timeout: Some(Duration::from_millis(200)),
            ..SocketConfig::default()
        },
        probe_timeout: Duration::from_millis(200),
        ..ClientConfig::default()
    };
    let mut client = AsyncClient::connect(config).await.unwrap();

    let opened = client.open().await.unwrap();
    match &opened {
        OpenOutcome::Started {
            remote,
            token,
            negotiated,
            event,
        } => {
            assert_eq!(*remote, server_addr);
            assert_ne!(*token, 0);
            assert_eq!(negotiated.params.received_stats, ReceivedStats::Both);
            assert_eq!(negotiated.params.stamp_at, StampAt::Both);
            assert_eq!(negotiated.params.clock, Clock::Both);
            // A default client requests no fill, and this open succeeded under
            // the default strict negotiation policy — which it could not have
            // if the server had answered an absent request with its own default
            // descriptor. The server still fills the payload with it.
            assert_eq!(negotiated.params.server_fill, None);
            assert!(matches!(event, ClientEvent::SessionStarted { .. }));
        }
        OpenOutcome::NoTestCompleted { .. } => panic!("normal client unexpectedly ran no-test"),
    }

    assert!(matches!(
        client.send_probe().await.unwrap().as_slice(),
        [ClientEvent::EchoSent {
            seq: 0,
            remote,
            ..
        }] if *remote == server_addr
    ));
    assert!(matches!(
        client.recv().await.unwrap().as_slice(),
        [ClientEvent::EchoReply {
            seq: 0,
            remote,
            server_timing: Some(_),
            received_stats: Some(_),
            ..
        }] if *remote == server_addr
    ));
    assert!(matches!(
        client.close().await.unwrap().as_slice(),
        [ClientEvent::SessionClosed { remote, .. }] if *remote == server_addr
    ));

    shutdown_tx.send(()).unwrap();
    server_task.await.unwrap().unwrap();
}
