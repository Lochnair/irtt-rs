//! One or more listeners run as a single service.
//!
//! Everything here is asserted from the outside, over real sockets, with the
//! production encoders: what the listeners answer, what they refuse, and what
//! survives one shared shutdown. The two properties worth the setup are that a
//! set of listeners is genuinely several independent servers — separate tokens,
//! separate session bounds — and that a set of *one* behaves exactly as a set of
//! several does, because the server CLI runs every invocation through this path.

use std::{
    future::Future,
    io,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket as StdUdpSocket},
    time::Duration,
};

use irtt_proto::{
    decode_echo_reply, decode_open_reply, encode_request, Clock, EchoReply, OpenReply, Params,
    ReceivedStats, RequestToEncode, StampAt,
};
use irtt_server::{ServerConfig, ServerSet, ServerSetError};
use tokio::{net::UdpSocket, sync::oneshot, task::JoinHandle, time::timeout};

/// How long a reply is waited for.
///
/// Rejection is silence, so this doubles as the window a listener that must not
/// answer is listened to: it only has to outlast a reply that would have come,
/// and loopback replies take microseconds.
const REPLY_TIMEOUT: Duration = Duration::from_secs(1);
/// Every test is bounded, so a hang is a failure with a message rather than a
/// suite that never finishes.
const TEST_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test(flavor = "current_thread")]
async fn a_single_listener_set_binds_serves_and_stops() {
    // The CLI's ordinary case, and the reason it has no second code path: one
    // requested address, one resolved listener, one whole session over it.
    bounded(async {
        let set = ServerSet::bind([loopback(0)], unthrottled()).await.unwrap();
        assert_eq!(set.local_addrs().len(), 1);
        let addr = set.local_addrs()[0];
        assert_eq!(addr.ip(), Ipv4Addr::LOCALHOST);
        assert_ne!(addr.port(), 0, "a requested port of 0 is resolved");

        let running = spawn(set);
        let mut peer = Peer::at(addr).await;
        let session = peer.open().await.expect("the listener answers an open");
        let reply = peer.echo(&session, 0).await.expect("and an echo");
        assert_eq!(reply.token, session.token);
        assert_eq!(reply.sequence, 0);
        peer.close(&session).await;
        assert!(
            peer.echo(&session, 1).await.is_none(),
            "a closed session is an unknown token"
        );

        running.stop().await;
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn two_listeners_keep_their_order_and_serve_interleaved_traffic() {
    bounded(async {
        let set = ServerSet::bind([loopback(0), loopback(0)], unthrottled())
            .await
            .unwrap();
        let addrs = set.local_addrs().to_vec();
        assert_eq!(addrs.len(), 2);
        assert!(addrs.iter().all(|addr| addr.port() != 0));
        assert_ne!(
            addrs[0].port(),
            addrs[1].port(),
            "each ephemeral bind selects its own port"
        );

        let running = spawn(set);
        let mut first = Peer::at(addrs[0]).await;
        let mut second = Peer::at(addrs[1]).await;

        // Interleaved on purpose: both listener tasks have to be live at the
        // same time under one service, not served one after the other.
        let first_session = first.open().await.expect("the first listener opens");
        let second_session = second.open().await.expect("the second listener opens");
        for sequence in 0..2 {
            assert_eq!(
                first
                    .echo(&first_session, sequence)
                    .await
                    .expect("the first listener echoes")
                    .sequence,
                sequence
            );
            assert_eq!(
                second
                    .echo(&second_session, sequence)
                    .await
                    .expect("the second listener echoes")
                    .sequence,
                sequence
            );
        }

        running.stop().await;
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_token_means_nothing_at_another_listener() {
    // Session namespaces are per listener because each listener owns its own
    // core. This is the externally visible consequence, and it is deliberate:
    // there is no process-wide token or session table to make a token portable.
    bounded(async {
        let set = ServerSet::bind([loopback(0), loopback(0)], unthrottled())
            .await
            .unwrap();
        let addrs = set.local_addrs().to_vec();
        let running = spawn(set);

        let mut owner = Peer::at(addrs[0]).await;
        let session = owner.open().await.expect("the first listener opens");

        let mut stranger = Peer::at(addrs[1]).await;
        assert!(
            stranger.echo(&session, 0).await.is_none(),
            "the second listener does not know a token it never issued"
        );
        assert!(
            owner.echo(&session, 0).await.is_some(),
            "and the listener that issued it still does"
        );

        running.stop().await;
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn the_session_bound_applies_to_each_listener_separately() {
    // A cloned configuration is per-listener policy, not a process-wide budget:
    // a maximum of one session means one session *each*.
    bounded(async {
        let set = ServerSet::bind(
            [loopback(0), loopback(0)],
            unthrottled().with_max_sessions(1),
        )
        .await
        .unwrap();
        let addrs = set.local_addrs().to_vec();
        let running = spawn(set);

        let mut first = Peer::at(addrs[0]).await;
        let mut second = Peer::at(addrs[1]).await;
        assert!(first.open().await.is_some());
        assert!(
            second.open().await.is_some(),
            "the second listener has its own session budget"
        );

        let mut crowd = Peer::at(addrs[0]).await;
        assert!(
            crowd.open().await.is_none(),
            "a full listener refuses a second session silently"
        );

        running.stop().await;
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn every_listener_serves_the_configured_policy() {
    // One representative policy is enough; the negotiation itself is covered
    // where it lives. What is under test is that the configuration reached every
    // listener, not what it does.
    bounded(async {
        let config = unthrottled().with_timestamp_allowance(irtt_server::TimestampAllowance::None);
        let set = ServerSet::bind([loopback(0), loopback(0)], config)
            .await
            .unwrap();
        let addrs = set.local_addrs().to_vec();
        let running = spawn(set);

        for addr in addrs {
            let mut peer = Peer::at(addr).await;
            let session = peer.open().await.expect("the listener opens");
            assert_eq!(
                session.params.stamp_at,
                StampAt::None,
                "{addr} negotiated its own configuration"
            );
        }

        running.stop().await;
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn one_shutdown_stops_every_listener_and_releases_its_socket() {
    bounded(async {
        let set = ServerSet::bind([loopback(0), loopback(0)], unthrottled())
            .await
            .unwrap();
        let addrs = set.local_addrs().to_vec();
        let running = spawn(set);

        // Prove both are actually serving before asking them to stop.
        for &addr in &addrs {
            assert!(Peer::at(addr).await.open().await.is_some());
        }
        running.stop().await;

        for addr in addrs {
            StdUdpSocket::bind(addr)
                .unwrap_or_else(|error| panic!("{addr} was not released: {error}"));
        }
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_listener_that_cannot_bind_leaves_the_whole_set_unbound() {
    // All or nothing: the set is built completely before anything is served, so
    // a failure part-way through releases what it had and starts no task.
    bounded(async {
        let occupied = UdpSocket::bind(loopback(0)).await.unwrap();
        let occupied_addr = occupied.local_addr().unwrap();
        let wanted = free_loopback_port();
        let wanted_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, wanted));

        let error = ServerSet::bind([wanted_addr, occupied_addr], unthrottled())
            .await
            .expect_err("the second listener cannot bind an occupied port");
        assert!(
            matches!(error, ServerSetError::ListenerSetup { addr, .. } if addr == occupied_addr),
            "the error names the listener that failed: {error}"
        );

        StdUdpSocket::bind(wanted_addr)
            .expect("the listener bound before the failure was released again");
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn an_ipv4_and_a_genuine_ipv6_listener_share_one_port() {
    // The configuration this exists for. On a host whose IPv6 sockets are
    // dual-stack by default, the IPv6 bind would take the IPv4 port with it and
    // the pair could not coexist; the set binds that listener IPv6-only first.
    bounded(async {
        if !ipv6_loopback_available() {
            eprintln!("skipping the same-port test: IPv6 loopback is unavailable");
            return;
        }
        let port = free_loopback_port();
        let v4 = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
        let v6 = SocketAddr::from((Ipv6Addr::LOCALHOST, port));

        let set = ServerSet::bind([v4, v6], unthrottled()).await.unwrap();
        assert_eq!(set.local_addrs(), &[v4, v6][..]);
        let running = spawn(set);

        for addr in [v4, v6] {
            let mut peer = Peer::at(addr).await;
            let session = peer.open().await.unwrap_or_else(|| panic!("{addr} opens"));
            assert!(peer.echo(&session, 0).await.is_some(), "{addr} echoes");
        }

        running.stop().await;
    })
    .await;
}

/// The wildcard form of the same coexistence, where a wildcard listener is
/// served at all. Reply source selection itself is covered by its own tests;
/// this only asserts that both families bind on one port and answer.
#[cfg(any(target_os = "linux", target_os = "macos", target_os = "freebsd"))]
#[tokio::test(flavor = "current_thread")]
async fn wildcard_listeners_of_both_families_share_one_port() {
    bounded(async {
        if !ipv6_loopback_available() {
            eprintln!("skipping the wildcard same-port test: IPv6 loopback is unavailable");
            return;
        }
        let port = free_loopback_port();
        let v4: SocketAddr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, port));
        let v6: SocketAddr = SocketAddr::from((Ipv6Addr::UNSPECIFIED, port));

        let set = ServerSet::bind([v4, v6], unthrottled()).await.unwrap();
        let running = spawn(set);

        for addr in [
            SocketAddr::from((Ipv4Addr::LOCALHOST, port)),
            SocketAddr::from((Ipv6Addr::LOCALHOST, port)),
        ] {
            let mut peer = Peer::at(addr).await;
            let session = peer.open().await.unwrap_or_else(|| panic!("{addr} opens"));
            assert!(peer.echo(&session, 0).await.is_some(), "{addr} echoes");
        }

        running.stop().await;
    })
    .await;
}

/// A minimal compliant client for one listener.
///
/// It uses the production encoders and decoders, and reads from an unconnected
/// socket so that a reply from the wrong endpoint would be visible as a failed
/// assertion rather than as silence.
struct Peer {
    socket: UdpSocket,
    server: SocketAddr,
    buffer: Vec<u8>,
}

impl Peer {
    async fn at(server: SocketAddr) -> Self {
        let bind = if server.is_ipv4() {
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
        } else {
            SocketAddr::from((Ipv6Addr::LOCALHOST, 0))
        };
        Self {
            socket: UdpSocket::bind(bind).await.unwrap(),
            server,
            buffer: vec![0; 4096],
        }
    }

    /// Opens a session, or reports the silence that means it was refused.
    async fn open(&mut self) -> Option<OpenReply> {
        let request = encode_request(
            RequestToEncode::Open {
                params: &requested_params(),
                no_test: false,
            },
            None,
        )
        .unwrap();
        let len = self.exchange(&request).await?;
        Some(decode_open_reply(&self.buffer[..len], None).unwrap())
    }

    async fn echo(&mut self, session: &OpenReply, sequence: u32) -> Option<EchoReply> {
        let request = encode_request(
            RequestToEncode::Echo {
                token: session.token,
                sequence,
                params: &session.params,
                payload: &[],
            },
            None,
        )
        .unwrap();
        let len = self.exchange(&request).await?;
        Some(decode_echo_reply(&self.buffer[..len], &session.params, None).unwrap())
    }

    async fn close(&mut self, session: &OpenReply) {
        let request = encode_request(
            RequestToEncode::Close {
                token: session.token,
            },
            None,
        )
        .unwrap();
        self.socket.send_to(&request, self.server).await.unwrap();
    }

    /// Sends one request and returns the length of its reply, or `None` if the
    /// listener stayed silent.
    async fn exchange(&mut self, request: &[u8]) -> Option<usize> {
        self.socket.send_to(request, self.server).await.unwrap();
        match timeout(REPLY_TIMEOUT, self.socket.recv_from(&mut self.buffer)).await {
            Ok(received) => {
                let (len, from) = received.unwrap();
                assert_eq!(from, self.server, "a reply came from the wrong endpoint");
                Some(len)
            }
            Err(_) => None,
        }
    }
}

/// The parameters every peer here asks for. Nothing in these tests depends on
/// the values beyond their being a session a server will run.
fn requested_params() -> Params {
    Params {
        protocol_version: 1,
        duration_ns: 10_000_000_000,
        interval_ns: 100_000_000,
        received_stats: ReceivedStats::Both,
        stamp_at: StampAt::Both,
        clock: Clock::Both,
        ..Params::default()
    }
}

fn unthrottled() -> ServerConfig {
    ServerConfig::default().with_min_send_interval(Duration::ZERO)
}

fn loopback(port: u16) -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, port))
}

/// A loopback port nothing currently holds.
fn free_loopback_port() -> u16 {
    StdUdpSocket::bind(loopback(0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn ipv6_loopback_available() -> bool {
    match StdUdpSocket::bind(SocketAddr::from((Ipv6Addr::LOCALHOST, 0))) {
        Ok(_) => true,
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::AddrNotAvailable | io::ErrorKind::Unsupported
            ) =>
        {
            false
        }
        Err(error) => panic!("unexpected IPv6 loopback bind failure: {error}"),
    }
}

struct Running {
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<Result<(), ServerSetError>>,
}

impl Running {
    async fn stop(self) {
        self.shutdown.send(()).unwrap();
        self.task
            .await
            .expect("the supervising task completed")
            .expect("every listener stopped cleanly");
    }
}

fn spawn(set: ServerSet) -> Running {
    let (shutdown, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        set.run(async {
            let _ = shutdown_rx.await;
        })
        .await
    });
    Running { shutdown, task }
}

async fn bounded<F>(test: F)
where
    F: Future<Output = ()>,
{
    timeout(TEST_TIMEOUT, test)
        .await
        .expect("the test exceeded its bounded runtime");
}
