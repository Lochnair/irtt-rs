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
    socket_io::{self, ReceivedDatagram},
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
///
/// # Example
///
/// Bind one listener, inspect the resolved endpoint, and let the caller decide
/// when serving ends. This example type-checks without opening a socket:
///
/// ```no_run
/// use std::{net::{Ipv4Addr, SocketAddr}, time::Duration};
///
/// use irtt_server::{Server, ServerConfig};
///
/// # async fn serve() -> Result<(), Box<dyn std::error::Error>> {
/// let mut server = Server::bind(
///     SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
///     ServerConfig::default(),
/// )
/// .await?;
/// println!("listening on {}", server.local_addr()?);
/// server.run(tokio::time::sleep(Duration::from_secs(60))).await?;
/// # Ok(())
/// # }
/// ```
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
        Self::with_core(socket, ServerCore::new(config))
    }

    /// Test-only seam: builds a listener around an already-constructed core
    /// instead of one this method would build from a [`ServerConfig`].
    ///
    /// This exists so a unit test can substitute a scripted
    /// [`TokenSource`](crate::token::TokenSource) and make one listener in a
    /// [`ServerSet`](crate::ServerSet) fail deterministically after it has
    /// already bound and answered real traffic — the runtime otherwise offers
    /// no way to make a core failure happen on command without OS-level
    /// socket sabotage. Every other code path here — wildcard setup, kernel
    /// timestamp configuration — is unchanged from [`Server::from_socket`].
    #[cfg(test)]
    pub(crate) fn from_socket_with_core(
        socket: UdpSocket,
        core: ServerCore,
    ) -> Result<Self, ServerRuntimeError> {
        Self::with_core(socket, core)
    }

    fn with_core(socket: UdpSocket, core: ServerCore) -> Result<Self, ServerRuntimeError> {
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

        // Kernel receive timestamps, for every listener and strictly after the
        // mandatory setup above so a refusal here can neither hide nor stand in
        // for a wildcard listener's failure to secure its reply sources. This
        // one is allowed to fail: it buys accuracy, not correctness, and a
        // server that cannot have it is still a correct server.
        let _ = socket_io::try_configure_kernel_rx_timestamp(&socket);

        Ok(Self {
            socket,
            core,
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
                    let ReceivedDatagram {
                        len,
                        peer,
                        reply_source,
                        // Handed to the core with the datagram it describes, as
                        // an explicit per-datagram input. The core samples its
                        // own receive instant regardless and decides whether
                        // this one is usable; nothing here judges it, and there
                        // is no "next receive" state between the two.
                        kernel_rx_timestamp,
                    } = received;
                    // `reply_source` stays on this side of the boundary: which
                    // of the host's addresses a request arrived on is transport
                    // state, not session policy.
                    if let Some(reply) = self.core.handle_received_datagram(
                        peer,
                        &self.recv_buffer[..len],
                        kernel_rx_timestamp,
                    )? {
                        if !self.prepare_reply_traffic_class(reply.traffic_class()) {
                            continue;
                        }
                        let send = socket_io::send(
                            &self.socket,
                            reply.bytes(),
                            peer,
                            reply_source,
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
    use super::may_send_unappliable;

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
