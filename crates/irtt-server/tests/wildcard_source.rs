//! A wildcard listener answers from the address the request was sent to.
//!
//! Every test here drives a real [`AsyncClient`], which reads from a *connected*
//! UDP socket. That connection is the assertion: the kernel discards any
//! datagram whose source is not the endpoint the client sent to, so a reply
//! that left from another of the host's addresses is indistinguishable from no
//! reply at all. An open, an echo and a close completing therefore prove the
//! source address, without the test having to inspect it.
//!
//! Compiled on the targets that implement wildcard source selection. Elsewhere
//! a wildcard bind is refused outright, which the runtime's own tests cover.

#![cfg(any(target_os = "linux", target_os = "macos", target_os = "freebsd"))]

use std::{
    io,
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    time::Duration,
};

use irtt_client::{AsyncClient, ClientConfig, ClientEvent, OpenOutcome, PacketMeta, SocketConfig};
use irtt_proto::{Clock, ReceivedStats, StampAt};
use irtt_server::{Server, ServerConfig, ServerRuntimeError};
use tokio::{sync::oneshot, task::JoinHandle, time::timeout};

/// EF, as the client's configuration takes it, and the raw TOS byte it occupies
/// the upper six bits of.
const EF_DSCP: u8 = 46;
const EF_TRAFFIC_CLASS: u8 = 0xb8;

#[tokio::test(flavor = "current_thread")]
async fn a_wildcard_ipv4_listener_replies_from_the_requested_address() {
    timeout(Duration::from_secs(5), async {
        let server = Server::bind("0.0.0.0:0".parse().unwrap(), unthrottled())
            .await
            .expect("a wildcard IPv4 listener must be supported on this target");
        let port = server.local_addr().unwrap().port();
        // A second loopback address makes this a genuine multi-homed case: the
        // listener is bound to none of the host's addresses in particular, and
        // the client contacts one the routing table would not have picked on
        // its own. Hosts carrying only 127.0.0.1 — macOS, unless an alias is
        // configured — still exercise the whole ancillary path, one address
        // wide.
        let target = SocketAddr::from((secondary_ipv4_loopback(), port));
        let running = spawn(server);

        let mut client = connected_client(target).await;
        assert_started(&mut client, target).await;

        let meta = echo_once(&mut client).await;
        // Source selection is per-packet ancillary data and the traffic class
        // is a socket-wide setting applied before the send. Both act on the
        // same reply, so this is where they have to agree.
        if cfg!(target_os = "linux") {
            assert_eq!(
                meta.traffic_class,
                Some(EF_TRAFFIC_CLASS),
                "a source-selected reply still carries its session's marking"
            );
            assert_eq!(meta.dscp, Some(EF_DSCP));
            assert_eq!(meta.ecn, Some(0));
        }

        assert!(matches!(
            client.close().await.unwrap().as_slice(),
            [ClientEvent::SessionClosed { .. }]
        ));
        running.stop().await;
    })
    .await
    .expect("the wildcard IPv4 source-selection test exceeded its bounded runtime");
}

#[tokio::test(flavor = "current_thread")]
async fn an_ipv4_mapped_wildcard_listener_replies_from_the_requested_address() {
    // `[::ffff:0.0.0.0]` is a wildcard that does not look like one. Linux
    // accepts it as an IPv4 wildcard bind and `getsockname` reports it back
    // unchanged, so a listener that only recognized `0.0.0.0` and `[::]` would
    // take this for an explicit address and answer from a routing-selected
    // source. macOS normalizes the bind to `[::]`, where this is simply the
    // IPv6 wildcard case again.
    timeout(Duration::from_secs(5), async {
        let server = match Server::bind("[::ffff:0.0.0.0]:0".parse().unwrap(), unthrottled()).await
        {
            Ok(server) => server,
            // FreeBSD refuses a mapped bind under its default `IPV6_V6ONLY`.
            Err(ServerRuntimeError::Bind { source, .. }) if is_family_unavailable(&source) => {
                eprintln!("skipping IPv4-mapped wildcard test: the bind is unavailable here");
                return;
            }
            Err(error) => panic!("unexpected IPv4-mapped wildcard bind failure: {error}"),
        };
        let port = server.local_addr().unwrap().port();
        let target = SocketAddr::from((secondary_ipv4_loopback(), port));
        let running = spawn(server);

        let mut client = connected_client(target).await;
        assert_started(&mut client, target).await;
        let meta = echo_once(&mut client).await;
        // A mapped listener is `AF_INET6` but emits IPv4, and the IPv6 Traffic
        // Class option does not reach those packets — it leaves their TOS at
        // zero. Only the IPv4 option marks them, so this asserts the listener
        // was read as the IPv4 one it is.
        if cfg!(target_os = "linux") {
            assert_eq!(
                meta.traffic_class,
                Some(EF_TRAFFIC_CLASS),
                "a mapped listener's reply still carries its session's marking"
            );
            assert_eq!(meta.dscp, Some(EF_DSCP));
        }
        assert!(matches!(
            client.close().await.unwrap().as_slice(),
            [ClientEvent::SessionClosed { .. }]
        ));

        running.stop().await;
    })
    .await
    .expect("the IPv4-mapped wildcard test exceeded its bounded runtime");
}

#[tokio::test(flavor = "current_thread")]
async fn a_wildcard_ipv6_listener_replies_from_the_requested_address() {
    timeout(Duration::from_secs(5), async {
        if !ipv6_loopback_available() {
            eprintln!("skipping wildcard IPv6 test: IPv6 loopback unavailable");
            return;
        }
        let server = match Server::bind("[::]:0".parse().unwrap(), unthrottled()).await {
            Ok(server) => server,
            Err(ServerRuntimeError::Bind { source, .. }) if is_family_unavailable(&source) => {
                eprintln!("skipping wildcard IPv6 test: IPv6 unavailable");
                return;
            }
            Err(error) => panic!("unexpected wildcard IPv6 bind failure: {error}"),
        };
        let port = server.local_addr().unwrap().port();
        let target: SocketAddr = format!("[::1]:{port}").parse().unwrap();
        let running = spawn(server);

        // Loopback is one address, so this does not prove multi-homed IPv6. It
        // does exercise the parts that differ from IPv4: the received packet
        // info, the interface index carried with it, and the source-selected
        // IPv6 send.
        let mut client = connected_client(target).await;
        assert_started(&mut client, target).await;
        echo_once(&mut client).await;
        assert!(matches!(
            client.close().await.unwrap().as_slice(),
            [ClientEvent::SessionClosed { .. }]
        ));

        running.stop().await;
    })
    .await
    .expect("the wildcard IPv6 source-selection test exceeded its bounded runtime");
}

/// A local IPv4 address that is not the primary loopback one, where the host
/// has one.
///
/// Bindability is the test: an address this process can bind is one the host
/// accepts datagrams for. `127.0.0.0/8` supplies a second local destination on
/// hosts that carry the whole range, without touching interface configuration.
fn secondary_ipv4_loopback() -> Ipv4Addr {
    let candidate = Ipv4Addr::new(127, 0, 0, 2);
    if UdpSocket::bind((candidate, 0)).is_ok() {
        return candidate;
    }
    eprintln!("{candidate} is not a local address on this host; using 127.0.0.1 instead");
    Ipv4Addr::LOCALHOST
}

fn ipv6_loopback_available() -> bool {
    match UdpSocket::bind("[::1]:0") {
        Ok(_) => true,
        Err(error) if is_family_unavailable(&error) => false,
        Err(error) => panic!("unexpected IPv6 loopback bind failure: {error}"),
    }
}

fn is_family_unavailable(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::AddrNotAvailable | io::ErrorKind::Unsupported
    )
}

fn unthrottled() -> ServerConfig {
    ServerConfig::default().with_min_send_interval(Duration::ZERO)
}

async fn connected_client(server_addr: SocketAddr) -> AsyncClient {
    let config = ClientConfig {
        server_addr: server_addr.to_string(),
        duration: Some(Duration::from_secs(30)),
        interval: Duration::from_millis(10),
        length: 64,
        dscp: EF_DSCP,
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
    AsyncClient::connect(config).await.unwrap()
}

async fn assert_started(client: &mut AsyncClient, server_addr: SocketAddr) {
    match client.open().await.unwrap() {
        OpenOutcome::Started { remote, token, .. } => {
            assert_eq!(remote, server_addr);
            assert_ne!(token, 0);
        }
        OpenOutcome::NoTestCompleted { .. } => panic!("a normal client unexpectedly ran no-test"),
    }
}

/// Sends one echo and returns its reply's receive metadata.
async fn echo_once(client: &mut AsyncClient) -> PacketMeta {
    client.send_probe().await.unwrap();
    loop {
        for event in client.recv().await.unwrap() {
            if let ClientEvent::EchoReply {
                seq, packet_meta, ..
            } = event
            {
                assert_eq!(seq, 0);
                return packet_meta;
            }
        }
    }
}

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
