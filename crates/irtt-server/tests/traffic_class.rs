//! What the server's replies actually carry on the wire.
//!
//! Everything else about reply marking is decided deterministically in the
//! core, but "the core asked for 0xb8" is not the same claim as "the datagram
//! left the host with TOS 0xb8". These tests close that gap with real sockets:
//! a real [`AsyncClient`] against a real [`Server`], reading the traffic class
//! the kernel reports for each reply it receives.
//!
//! Linux only, because the receive-side ancillary metadata the client exposes
//! is. The production marking path is platform-independent and is compiled and
//! exercised everywhere; only this observation of it is not.

#![cfg(target_os = "linux")]

use std::{io, net::SocketAddr, time::Duration};

use irtt_client::{AsyncClient, ClientConfig, ClientEvent, OpenOutcome, PacketMeta, SocketConfig};
use irtt_proto::{Clock, ReceivedStats, StampAt};
use irtt_server::{Server, ServerConfig, ServerRuntimeError};
use tokio::{sync::oneshot, task::JoinHandle, time::timeout};

/// DSCP codepoints, as the client's configuration takes them, paired with the
/// raw TOS / Traffic Class byte each one occupies the upper six bits of. The
/// server negotiates and applies the raw byte; the shift is the client's
/// user-facing convention.
const EF: (u8, u8) = (46, 0xb8);
const CS1: (u8, u8) = (8, 0x20);
const UNMARKED: (u8, u8) = (0, 0x00);

#[tokio::test(flavor = "current_thread")]
async fn an_echo_reply_leaves_the_host_carrying_the_negotiated_traffic_class() {
    timeout(Duration::from_secs(5), async {
        let (server, addr) = bound_server("127.0.0.1:0".parse().unwrap())
            .await
            .expect("IPv4 loopback must be available");
        let running = spawn(server);

        let mut session = opened_session(addr, EF.0).await;
        let meta = session.echo_packet_meta().await;
        assert_eq!(meta.traffic_class, Some(EF.1), "the raw negotiated byte");
        assert_eq!(meta.dscp, Some(EF.0), "its upper six bits");
        assert_eq!(meta.ecn, Some(0), "and its low two");

        running.stop().await;
    })
    .await
    .expect("the wire traffic-class test exceeded its bounded runtime");
}

#[tokio::test(flavor = "current_thread")]
async fn one_listener_serving_many_sessions_leaks_no_marking_between_them() {
    // The invariant that justifies applying the class before *every* send. One
    // socket carries all three sessions, so a reply that did not set its own
    // class would go out under whichever one the previous reply left behind.
    // The order below returns to a marked session after another has sent, and
    // ends on the unmarked one, which is the case a "skip the call for zero"
    // optimization would break.
    timeout(Duration::from_secs(10), async {
        let (server, addr) = bound_server("127.0.0.1:0".parse().unwrap())
            .await
            .expect("IPv4 loopback must be available");
        let running = spawn(server);

        const MARKED: usize = 0;
        const LIGHTLY_MARKED: usize = 1;
        const UNMARKED_SESSION: usize = 2;
        let mut sessions = [
            opened_session(addr, EF.0).await,
            opened_session(addr, CS1.0).await,
            opened_session(addr, UNMARKED.0).await,
        ];

        for (label, session, expected) in [
            ("first marked echo", MARKED, EF.1),
            ("a differently marked session", LIGHTLY_MARKED, CS1.1),
            ("back to the first session", MARKED, EF.1),
            ("an unmarked session last", UNMARKED_SESSION, UNMARKED.1),
        ] {
            let meta = sessions[session].echo_packet_meta().await;
            assert_eq!(
                meta.traffic_class,
                Some(expected),
                "{label}: each reply carries its own session's marking"
            );
        }

        running.stop().await;
    })
    .await
    .expect("the cross-session leakage test exceeded its bounded runtime");
}

#[tokio::test(flavor = "current_thread")]
async fn an_ipv6_echo_reply_carries_the_negotiated_traffic_class() {
    // One marked echo is enough here: the cross-session matrix above already
    // pins the policy, and what this adds is the IPv6 Traffic Class option
    // rather than the IPv4 TOS one.
    timeout(Duration::from_secs(5), async {
        let Some((server, addr)) = bound_server("[::1]:0".parse().unwrap()).await else {
            return;
        };
        let running = spawn(server);

        let mut session = opened_session(addr, EF.0).await;
        let meta = session.echo_packet_meta().await;
        assert_eq!(meta.traffic_class, Some(EF.1));

        running.stop().await;
    })
    .await
    .expect("the IPv6 wire traffic-class test exceeded its bounded runtime");
}

/// Binds a listener, or reports the family as unavailable on this host.
///
/// Only an address family this host does not have is skipped; any other bind
/// failure is a real failure.
async fn bound_server(addr: SocketAddr) -> Option<(Server, SocketAddr)> {
    let server = match Server::bind(addr, unthrottled()).await {
        Ok(server) => server,
        Err(ServerRuntimeError::Bind { source, .. })
            if matches!(
                source.kind(),
                io::ErrorKind::AddrNotAvailable | io::ErrorKind::Unsupported
            ) =>
        {
            return None;
        }
        Err(error) => panic!("unexpected loopback bind failure: {error}"),
    };
    let addr = server.local_addr().unwrap();
    Some((server, addr))
}

/// A server that rate-limits nothing, so the cadence these tests drive replies
/// at cannot turn a marking assertion into a rate-limiting one.
fn unthrottled() -> ServerConfig {
    ServerConfig::default().with_min_send_interval(Duration::ZERO)
}

/// A running listener and the handle that stops it.
struct Running {
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<Result<(), ServerRuntimeError>>,
}

impl Running {
    async fn stop(self) {
        self.shutdown.send(()).unwrap();
        self.task.await.unwrap().unwrap();
    }
}

fn spawn(mut server: Server) -> Running {
    let (shutdown, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        server
            .run(async {
                let _ = shutdown_rx.await;
            })
            .await
    });
    Running { shutdown, task }
}

/// One client with an open session, and the sequence number its next echo will
/// carry — which the caller cannot infer once several sessions are interleaved.
struct OpenSession {
    client: AsyncClient,
    next_sequence: u32,
}

impl OpenSession {
    /// Sends one echo and returns the receive metadata of its reply.
    async fn echo_packet_meta(&mut self) -> PacketMeta {
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        self.client.send_probe().await.unwrap();
        loop {
            for event in self.client.recv().await.unwrap() {
                if let ClientEvent::EchoReply {
                    seq, packet_meta, ..
                } = event
                {
                    assert_eq!(seq, sequence);
                    return packet_meta;
                }
            }
        }
    }
}

/// A client with an open session requesting `dscp` as a codepoint.
async fn opened_session(server_addr: SocketAddr, dscp: u8) -> OpenSession {
    let config = ClientConfig {
        server_addr: server_addr.to_string(),
        duration: Some(Duration::from_secs(30)),
        interval: Duration::from_millis(10),
        length: 64,
        dscp,
        received_stats: ReceivedStats::Both,
        stamp_at: StampAt::Both,
        clock: Clock::Both,
        open_timeouts: vec![Duration::from_millis(500)],
        socket_config: SocketConfig {
            recv_timeout: Some(Duration::from_millis(500)),
            ..SocketConfig::default()
        },
        probe_timeout: Duration::from_millis(500),
        ..ClientConfig::default()
    };
    let mut client = AsyncClient::connect(config).await.unwrap();
    match client.open().await.unwrap() {
        OpenOutcome::Started { negotiated, .. } => assert_eq!(
            negotiated.params.dscp,
            i64::from(dscp) << 2,
            "the server negotiates the raw byte the codepoint asks for"
        ),
        OpenOutcome::NoTestCompleted { .. } => panic!("a normal client unexpectedly ran no-test"),
    }
    OpenSession {
        client,
        next_sequence: 0,
    }
}
