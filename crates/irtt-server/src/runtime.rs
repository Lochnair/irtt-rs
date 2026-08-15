//! Tokio UDP orchestration around [`ServerCore`](crate::ServerCore).

use std::{
    future::Future,
    io,
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use socket2::SockRef;
use thiserror::Error;
use tokio::{net::UdpSocket, time::MissedTickBehavior};

use crate::{
    socket_io,
    socket_options::{marks_with_ipv4_option, set_reply_traffic_class},
    ServerConfig, ServerCore, ServerError,
};

/// Fixed transport receive capacity.
///
/// Standard UDP payloads fit in this buffer; IPv6 jumbograms are not supported.
/// This is allocation policy only: whether a received datagram may reach the
/// protocol core — the conservative rejection of one filling the buffer
/// included — belongs to [`socket_io::receive`].
const RECEIVE_BUFFER_LEN: usize = 65_536;
const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(1);

/// A Tokio UDP listener backed by one deterministic server core.
///
/// Processing is sequential: one task receives a datagram, lets the core
/// process it, and sends the optional reply from the same socket. No
/// per-datagram tasks or locks are involved.
///
/// **That sequential ownership is what makes the reply traffic class correct.**
/// The class is a socket-wide setting, and every reply explicitly applies its
/// own — zero included — immediately before its send, so the reply about to go
/// out is always the one that set it. Nothing else sends from this socket, not
/// even while a send is suspended, so no reply can inherit another session's
/// marking. Adding a concurrent sender would break that and require per-packet
/// control messages instead.
///
/// # Reply source address
///
/// A reply must leave from the exact address the request was sent to. An
/// explicit-address listener gets that from the bind: it can send from nothing
/// else. A wildcard listener (`0.0.0.0` or `[::]`) cannot, because the routing
/// table would choose the source, so it asks the kernel for each request's
/// local destination and sends that request's reply from it.
///
/// That per-packet path exists on Linux, macOS and FreeBSD. Elsewhere a
/// wildcard bind is **refused** at construction — see
/// [`ServerRuntimeError::WildcardSourceSelectionUnsupported`] — rather than
/// served by a listener whose replies a client on a second address would
/// silently discard. Explicit-address listeners are unaffected everywhere.
#[derive(Debug)]
pub struct Server {
    socket: UdpSocket,
    core: ServerCore,
    recv_buffer: Vec<u8>,
    /// The listener's own address family, which selects the IPv4 TOS or IPv6
    /// Traffic Class option.
    listener_is_ipv4: bool,
    /// Whether this listener is wildcard-bound and must therefore recover each
    /// request's local destination and reply from it.
    ///
    /// Decided once, from the bound address. It is never inferred later from a
    /// peer: a listener that accepted a wildcard bind owes correct reply
    /// sources to every request, not to the ones that look multi-homed.
    select_reply_source: bool,
    /// Whether this server has ever successfully applied a nonzero traffic
    /// class to the socket.
    ///
    /// This is not a cache and never elides a call — the class is applied
    /// before every send regardless. It is consulted only when applying one
    /// *fails*, to tell "the socket may be carrying a marking of ours" from
    /// "it cannot be carrying one".
    socket_is_marked: bool,
}

impl Server {
    /// Binds one UDP listener at `addr` and creates its independent session
    /// namespace.
    ///
    /// A wildcard `addr` additionally configures reply source selection, and
    /// fails where that is unavailable; see [`Server::from_socket`].
    pub async fn bind(addr: SocketAddr, config: ServerConfig) -> Result<Self, ServerRuntimeError> {
        let socket = UdpSocket::bind(addr)
            .await
            .map_err(|source| ServerRuntimeError::Bind { addr, source })?;
        Self::from_socket(socket, config)
    }

    /// Wraps an already prepared Tokio UDP socket in a server.
    ///
    /// # Errors
    ///
    /// Construction is fallible because a wildcard listener has setup to do
    /// before it may serve anything. It fails when the socket's bound address
    /// cannot be queried — which is what says whether this is a wildcard
    /// listener at all — and, for a wildcard socket, when this target has no
    /// reply source selection or the kernel refuses to configure it.
    ///
    /// Failing here is the point. A wildcard listener that cannot recover a
    /// request's local destination would start, run, and answer clients on a
    /// second local address from an endpoint they never contacted, which they
    /// discard as though the network had dropped it.
    pub fn from_socket(
        socket: UdpSocket,
        config: ServerConfig,
    ) -> Result<Self, ServerRuntimeError> {
        let addr = socket
            .local_addr()
            .map_err(|source| ServerRuntimeError::LocalAddr { source })?;
        let select_reply_source = is_wildcard(addr);

        if select_reply_source {
            if !socket_io::SUPPORTED {
                return Err(ServerRuntimeError::WildcardSourceSelectionUnsupported { addr });
            }
            socket_io::configure_destination_metadata(&socket, addr.is_ipv4())
                .map_err(|source| ServerRuntimeError::SourceSelectionSetup { addr, source })?;
        }

        Ok(Self {
            socket,
            core: ServerCore::new(config),
            recv_buffer: vec![0; RECEIVE_BUFFER_LEN],
            listener_is_ipv4: marks_with_ipv4_option(addr),
            select_reply_source,
            socket_is_marked: false,
        })
    }

    /// Applies one reply's raw traffic class to the listener socket, and
    /// reports whether that reply may be sent.
    ///
    /// The class is applied before *every* send, and zero as deliberately as
    /// any other value: a listener serves many sessions from one socket, so
    /// skipping the call for an unmarked reply — an open reply, or a session
    /// that negotiated nothing — would send it under whichever marking the
    /// previous reply left behind.
    ///
    /// When the option cannot be applied, [`may_send_unappliable`] decides. A
    /// marking this server asked for and did not get must never be replaced by
    /// silently sending under the previous one, so those replies are dropped;
    /// but a host that refuses the option outright — some Windows builds do not
    /// support `IP_TOS`, and a few targets have no safe setter at all — must
    /// still be able to run a server, and it can, because a socket this server
    /// has never marked has no marking of ours to clear.
    fn prepare_reply_traffic_class(&mut self, traffic_class: u8) -> bool {
        match self.apply_reply_traffic_class(traffic_class) {
            Ok(()) => {
                self.socket_is_marked = traffic_class != 0;
                true
            }
            Err(_) => may_send_unappliable(traffic_class, self.socket_is_marked),
        }
    }

    fn apply_reply_traffic_class(&self, traffic_class: u8) -> io::Result<()> {
        set_reply_traffic_class(
            SockRef::from(&self.socket),
            self.listener_is_ipv4,
            traffic_class,
        )
    }

    /// Returns the local endpoint selected for the listener.
    pub fn local_addr(&self) -> Result<SocketAddr, ServerRuntimeError> {
        self.socket
            .local_addr()
            .map_err(|source| ServerRuntimeError::LocalAddr { source })
    }

    /// Serves datagrams until `shutdown` completes.
    ///
    /// Graceful shutdown sends no session-close packets and leaves no hidden
    /// receive task behind, even while a reply send is waiting for socket
    /// writability. Per-packet send failures and short sends drop only that
    /// reply, and so does a failure to apply a marking that reply needed —
    /// preparing one packet is not a reason to stop serving every other
    /// session, and the next reply makes its own independent attempt. Receive
    /// failures and internal core failures terminate the loop with an error.
    ///
    /// A reply whose marking could not be applied is not sent instead: sending
    /// it would put it on the wire under the previous reply's marking. An
    /// *unmarked* reply on a socket this server has never marked is the one
    /// exception, so a host that does not support the option at all — some
    /// Windows builds do not support `IP_TOS` — still runs a working server.
    ///
    /// A wildcard listener drops a request whose local destination did not
    /// arrive with it, before the core sees it. There is no fall back to a
    /// routing-table source: the listener promised a correct reply source when
    /// it accepted the bind, and a request it cannot answer correctly must not
    /// move a session's receive, rate or lifetime state either.
    pub async fn run<F>(&mut self, shutdown: F) -> Result<(), ServerRuntimeError>
    where
        F: Future<Output = ()>,
    {
        tokio::pin!(shutdown);
        let mut maintenance = tokio::time::interval(MAINTENANCE_INTERVAL);
        maintenance.set_missed_tick_behavior(MissedTickBehavior::Skip);
        maintenance.tick().await;

        loop {
            tokio::select! {
                _ = &mut shutdown => return Ok(()),
                _ = maintenance.tick() => self.core.maintain(),
                received = socket_io::receive(
                    &self.socket,
                    &mut self.recv_buffer,
                    self.select_reply_source,
                ) => {
                    let received = match received {
                        Ok(Some(received)) => received,
                        Ok(None) => continue,
                        Err(source) if source.kind() == io::ErrorKind::Interrupted => continue,
                        Err(source) => return Err(ServerRuntimeError::Receive { source }),
                    };
                    let (len, peer) = (received.len, received.peer);
                    if let Some(reply) = self.core.handle_datagram(peer, &self.recv_buffer[..len])? {
                        if !self.prepare_reply_traffic_class(reply.traffic_class()) {
                            continue;
                        }
                        let send = socket_io::send(
                            &self.socket,
                            reply.bytes(),
                            peer,
                            received.reply_source,
                        );
                        tokio::pin!(send);
                        let sent = loop {
                            tokio::select! {
                                _ = &mut shutdown => return Ok(()),
                                _ = maintenance.tick() => self.core.maintain(),
                                sent = &mut send => break sent,
                            }
                        };
                        if !matches!(sent, Ok(len) if len == reply.bytes().len()) {
                            continue;
                        }
                    }
                }
            }
        }
    }
}

/// Whether a bound address names no particular local address, and so owes every
/// reply an explicitly selected source.
///
/// `0.0.0.0` and `[::]` are the obvious forms. The third is the IPv4-mapped
/// unspecified address, `[::ffff:0.0.0.0]`: Linux accepts it as an IPv4 wildcard
/// bind and reports it back verbatim from `getsockname`, so asking
/// `is_unspecified` alone would take a working wildcard listener for an explicit
/// one and answer its requests from whatever source the routing table picked.
/// (macOS normalizes that bind to `[::]` and never reaches the second test.)
///
/// A mapped address that names an actual host address — `[::ffff:127.0.0.1]` —
/// is explicit, and stays on the plain path.
fn is_wildcard(addr: SocketAddr) -> bool {
    match addr.ip() {
        IpAddr::V4(address) => address.is_unspecified(),
        IpAddr::V6(address) => {
            address.is_unspecified()
                || address
                    .to_ipv4_mapped()
                    .is_some_and(|mapped| mapped.is_unspecified())
        }
    }
}

/// Whether a reply may still be sent after its traffic class could not be
/// applied.
///
/// Only an unmarked reply on a socket this server has never successfully marked
/// qualifies. Both halves matter: a reply that wanted a marking must not go out
/// without one, and once this server has put a nonzero class on the socket, a
/// failure to clear it means the socket may still be carrying it.
///
/// A socket handed to [`Server::from_socket`] pre-marked by its creator is
/// outside what this can know; explicit marking is the server's own to manage
/// from a fresh listener.
fn may_send_unappliable(traffic_class: u8, socket_is_marked: bool) -> bool {
    traffic_class == 0 && !socket_is_marked
}

/// Failure to create or run a Tokio UDP server.
#[derive(Debug, Error)]
pub enum ServerRuntimeError {
    /// Binding the requested listener failed.
    #[error("could not bind UDP listener at {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        #[source]
        source: io::Error,
    },
    /// Querying the listener's selected local endpoint failed.
    #[error("could not query UDP listener address: {source}")]
    LocalAddr {
        #[source]
        source: io::Error,
    },
    /// A wildcard listener was requested on a target with no safe per-packet
    /// reply source selection.
    ///
    /// The listener would answer from whichever local address the routing table
    /// chose, which a client that contacted a different one discards.
    #[error(
        "wildcard listener {addr} cannot select its reply source address on this target: \
         bind an explicit local address instead"
    )]
    WildcardSourceSelectionUnsupported { addr: SocketAddr },
    /// Configuring destination-address metadata for a wildcard listener failed.
    #[error(
        "could not configure reply source-address selection for wildcard listener {addr}: \
         {source}; bind an explicit local address instead"
    )]
    SourceSelectionSetup {
        addr: SocketAddr,
        #[source]
        source: io::Error,
    },
    /// Receiving from the listener failed irrecoverably.
    #[error("UDP receive failed: {source}")]
    Receive {
        #[source]
        source: io::Error,
    },
    /// The deterministic protocol/session core failed internally.
    #[error(transparent)]
    Core(#[from] ServerError),
}

#[cfg(test)]
mod tests {
    use std::{net::Ipv4Addr, time::Duration};

    use irtt_proto::{
        decode_echo_reply, decode_open_reply, encode_request, Clock, Params, ReceivedStats,
        RequestToEncode, StampAt,
    };
    use tokio::{net::UdpSocket, sync::oneshot, time::timeout};

    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn bind_reports_local_address_and_shutdown_stops_run() {
        let requested = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let mut server = Server::bind(requested, ServerConfig::default())
            .await
            .unwrap();
        let bound = server.local_addr().unwrap();
        assert_eq!(bound.ip(), requested.ip());
        assert_ne!(bound.port(), 0);

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        shutdown_tx.send(()).unwrap();
        server
            .run(async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    }

    /// Construction is where a wildcard listener's promise is made or refused.
    ///
    /// Both outcomes are asserted from the same code, because which one a target
    /// gets is exactly the thing under test: a target with a source-selection
    /// path configures it and serves; one without refuses the bind rather than
    /// starting a listener whose replies may come from an address no client
    /// contacted. An explicit bind is unaffected either way.
    #[tokio::test(flavor = "current_thread")]
    async fn from_socket_settles_wildcard_source_selection_before_serving_anything() {
        let explicit = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        assert!(Server::from_socket(explicit, ServerConfig::default()).is_ok());

        let wildcard = UdpSocket::bind("0.0.0.0:0").await.unwrap();
        let wildcard = Server::from_socket(wildcard, ServerConfig::default());
        if socket_io::SUPPORTED {
            let server = wildcard.expect("a supported target configures source selection");
            assert!(server.select_reply_source);
        } else {
            assert!(matches!(
                wildcard,
                Err(ServerRuntimeError::WildcardSourceSelectionUnsupported { .. })
            ));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ipv4_open_echo_close_crosses_a_real_udp_socket() {
        exercise_udp_path(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ipv6_open_echo_close_when_loopback_is_available() {
        let addr = "[::1]:0".parse().unwrap();
        let server = match Server::bind(addr, unthrottled()).await {
            Ok(server) => server,
            Err(ServerRuntimeError::Bind { source, .. })
                if matches!(
                    source.kind(),
                    io::ErrorKind::AddrNotAvailable | io::ErrorKind::Unsupported
                ) =>
            {
                return;
            }
            Err(error) => panic!("unexpected IPv6 loopback bind failure: {error}"),
        };
        exercise_bound_server(server).await;
    }

    async fn exercise_udp_path(addr: SocketAddr) {
        let server = Server::bind(addr, unthrottled()).await.unwrap();
        exercise_bound_server(server).await;
    }

    async fn exercise_bound_server(mut server: Server) {
        let server_addr = server.local_addr().unwrap();
        let client_bind = match server_addr {
            SocketAddr::V4(_) => "127.0.0.1:0",
            SocketAddr::V6(_) => "[::1]:0",
        };
        let client = UdpSocket::bind(client_bind).await.unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server_task = tokio::spawn(async move {
            server
                .run(async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        let requested = Params {
            protocol_version: 1,
            duration_ns: 1_000_000_000,
            interval_ns: 100_000_000,
            received_stats: ReceivedStats::Both,
            stamp_at: StampAt::Both,
            clock: Clock::Both,
            ..Params::default()
        };
        let open = encode_request(
            RequestToEncode::Open {
                params: &requested,
                no_test: false,
            },
            None,
        )
        .unwrap();
        client.send_to(&open, server_addr).await.unwrap();

        let mut buffer = vec![0; RECEIVE_BUFFER_LEN];
        let (len, reply_source) = timeout(Duration::from_secs(1), client.recv_from(&mut buffer))
            .await
            .expect("OPEN reply timeout")
            .unwrap();
        assert_eq!(reply_source, server_addr);
        let open_reply = decode_open_reply(&buffer[..len], None).unwrap();
        assert_ne!(open_reply.token, 0);

        let echo = encode_request(
            RequestToEncode::Echo {
                token: open_reply.token,
                sequence: 0,
                params: &open_reply.params,
                payload: &[],
            },
            None,
        )
        .unwrap();
        client.send_to(&echo, server_addr).await.unwrap();
        let (len, reply_source) = timeout(Duration::from_secs(1), client.recv_from(&mut buffer))
            .await
            .expect("ECHO reply timeout")
            .unwrap();
        assert_eq!(reply_source, server_addr);
        let echo_reply = decode_echo_reply(&buffer[..len], &open_reply.params, None).unwrap();
        assert_eq!(echo_reply.token, open_reply.token);
        assert_eq!(echo_reply.sequence, 0);
        assert_eq!(echo_reply.recv_count, Some(1));
        assert_eq!(echo_reply.recv_window, Some(1));

        let close = encode_request(
            RequestToEncode::Close {
                token: open_reply.token,
            },
            None,
        )
        .unwrap();
        client.send_to(&close, server_addr).await.unwrap();
        client.send_to(&echo, server_addr).await.unwrap();
        assert!(
            timeout(Duration::from_millis(100), client.recv_from(&mut buffer))
                .await
                .is_err()
        );

        shutdown_tx.send(()).unwrap();
        server_task.await.unwrap().unwrap();
    }

    fn unthrottled() -> ServerConfig {
        ServerConfig::default().with_min_send_interval(Duration::ZERO)
    }

    /// Which bound addresses owe their replies a selected source.
    ///
    /// The IPv4-mapped unspecified address is the one that is not obvious, and
    /// it is not hypothetical: Linux accepts `[::ffff:0.0.0.0]` as an IPv4
    /// wildcard bind and reports it back unchanged, so missing it would leave a
    /// working wildcard listener on the plain send path. A mapped address
    /// naming a real host address is explicit and must stay there.
    #[test]
    fn a_bound_address_naming_no_particular_local_address_is_a_wildcard() {
        for (addr, wildcard) in [
            ("0.0.0.0:2112", true),
            ("[::]:2112", true),
            ("[::ffff:0.0.0.0]:2112", true),
            ("127.0.0.1:2112", false),
            ("192.0.2.10:2112", false),
            ("[::1]:2112", false),
            ("[::ffff:127.0.0.1]:2112", false),
        ] {
            assert_eq!(
                is_wildcard(addr.parse().unwrap()),
                wildcard,
                "{addr} classified wrongly"
            );
        }
    }

    /// The rule that decides what an unappliable traffic class means, which no
    /// normal interface can reach: it needs a host that refuses the socket
    /// option, and the hosts these tests run on do not.
    #[test]
    fn only_an_unmarked_reply_on_a_never_marked_socket_survives_an_apply_failure() {
        for (traffic_class, socket_is_marked, sendable, why) in [
            (
                0,
                false,
                true,
                "no marking wanted, and none of ours to clear",
            ),
            (0, true, false, "our own marking may still be on the socket"),
            (
                0xb8,
                false,
                false,
                "a marking that was wanted but not applied",
            ),
            (0xb8, true, false, "and likewise over a marking already set"),
        ] {
            assert_eq!(
                may_send_unappliable(traffic_class, socket_is_marked),
                sendable,
                "{why}"
            );
        }
    }
}
