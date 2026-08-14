//! Service-level supervision of one or more independent [`Server`] listeners.
//!
//! [`Server`] is the one-listener primitive: one socket, one core, one session
//! namespace, one sequential receive/send loop. A host that must answer on more
//! than one address needs several of them, and something has to own their
//! construction, their shutdown and each other's failures. That owner is
//! [`ServerSet`], and it is deliberately not a protocol object: it holds no
//! session state, no tokens and no clock, and nothing it does is visible on the
//! wire.

use std::{
    collections::HashMap,
    future::Future,
    io,
    net::{IpAddr, SocketAddr},
};

use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use thiserror::Error;
use tokio::{
    net::UdpSocket,
    sync::watch,
    task::{Id, JoinError, JoinSet},
};

use crate::{Server, ServerConfig, ServerRuntimeError};

/// One or more independent UDP listeners run as a single service.
///
/// Every listener is a complete [`Server`]: its own socket, its own
/// [`ServerCore`](crate::ServerCore), its own session table and its own tokens.
/// Nothing is shared between them but the configuration they were built from
/// and the supervision described below. A token issued by one listener is an
/// unknown token at any other, which is what makes the listeners genuinely
/// independent rather than one server wearing several addresses.
///
/// # Configuration is cloned, so bounds are per listener
///
/// The [`ServerConfig`] passed to [`ServerSet::bind`] is cloned into each
/// listener, so every resource bound in it applies **per listener** and not to
/// the process. A set of two listeners built from a configuration with
/// `max_sessions` of 100 admits up to 100 sessions on each, 200 in total. There
/// is no process-wide session cap.
///
/// # A set of one is an ordinary set
///
/// One listener is not a special case and gets no shortcut: it is bound,
/// supervised and shut down exactly as a member of a larger set is. The server
/// CLI runs every invocation through a `ServerSet` for that reason, so the
/// single-listener case exercises this path continuously.
///
/// # Supervision
///
/// [`run`](ServerSet::run) takes one shutdown future and fans it out to every
/// listener, then waits for all of them. A listener that fails, or that stops on
/// its own before shutdown was asked for, shuts its siblings down and fails the
/// whole set: a service configured for IPv4 and IPv6 must not quietly continue
/// as IPv4 only. Every listener task is joined before `run` returns, so no task
/// outlives it.
#[derive(Debug)]
pub struct ServerSet {
    servers: Vec<Server>,
    /// The addresses the listeners actually bound, in requested order and
    /// positionally matching `servers`.
    local_addrs: Vec<SocketAddr>,
}

impl ServerSet {
    /// Binds every requested listener, or none of them.
    ///
    /// Requested order is preserved, and each listener is bound before any is
    /// served: a later failure drops the sockets already opened and starts
    /// nothing. `config` is cloned into every listener, so its bounds are per
    /// listener.
    ///
    /// A port of `0` is resolved per listener — two such requests select two
    /// unrelated ephemeral ports, not one shared one. Ask
    /// [`local_addrs`](ServerSet::local_addrs) for what was actually bound.
    ///
    /// # Same-port IPv4 and IPv6
    ///
    /// Requesting a genuine IPv6 listener and an IPv4 one on the same explicit
    /// port — `0.0.0.0:2112` with `[::]:2112`, say — is a normal configuration,
    /// but on hosts whose IPv6 sockets are dual-stack by default the IPv6 bind
    /// would claim the IPv4 port too and the second bind would fail. Such an
    /// IPv6 listener is therefore created `IPV6_V6ONLY` before it is bound, so
    /// the two coexist and each serves its own family. Bind order is not relied
    /// on, and a listener requested on its own is bound exactly as
    /// [`Server::bind`] would.
    ///
    /// An IPv4-mapped address such as `[::ffff:0.0.0.0]` is an IPv4 listener
    /// here, not an IPv6 one, matching how the runtime already reads it.
    ///
    /// # Errors
    ///
    /// Returns [`ServerSetError::NoListeners`] if `addrs` is empty — a set
    /// serving nothing has no meaning — and
    /// [`ServerSetError::ListenerSetup`] naming the requested address if any
    /// listener could not be bound or prepared.
    pub async fn bind<I>(addrs: I, config: ServerConfig) -> Result<Self, ServerSetError>
    where
        I: IntoIterator<Item = SocketAddr>,
    {
        let requested: Vec<SocketAddr> = addrs.into_iter().collect();
        if requested.is_empty() {
            return Err(ServerSetError::NoListeners);
        }

        let mut servers = Vec::with_capacity(requested.len());
        let mut local_addrs = Vec::with_capacity(requested.len());
        for &addr in &requested {
            let server = bind_listener(addr, config.clone(), needs_ipv6_only(addr, &requested))
                .await
                .map_err(|source| ServerSetError::ListenerSetup { addr, source })?;
            let local_addr = server
                .local_addr()
                .map_err(|source| ServerSetError::ListenerSetup { addr, source })?;
            servers.push(server);
            local_addrs.push(local_addr);
        }

        Ok(Self {
            servers,
            local_addrs,
        })
    }

    /// The addresses the listeners actually bound, in requested order.
    ///
    /// These are the resolved endpoints, so a requested port of `0` appears
    /// here as the port the host selected.
    #[must_use]
    pub fn local_addrs(&self) -> &[SocketAddr] {
        &self.local_addrs
    }

    /// Serves every listener until `shutdown` completes or one of them fails.
    ///
    /// Each listener runs in its own Tokio task and stays internally sequential
    /// there, so the one-sender-per-socket ownership the reply traffic class
    /// depends on is unchanged. The single `shutdown` future is fanned out
    /// internally; nothing about how the caller produces it — a signal, a
    /// channel, a timer — is this crate's business.
    ///
    /// The set consumes itself because each listener is moved into its task. A
    /// caller that wants to bind and serve again binds a new set; the
    /// lower-level [`Server`] keeps its reusable `&mut self` loop.
    ///
    /// # Errors
    ///
    /// A listener that fails, that returns before shutdown was requested, or
    /// whose task did not complete shuts down its siblings; every remaining
    /// listener is then drained and the first such failure is returned. An
    /// external shutdown that completes first still returns a listener's error
    /// if one appears while draining. `Ok(())` means every listener stopped
    /// cleanly after being asked to.
    pub async fn run<F>(self, shutdown: F) -> Result<(), ServerSetError>
    where
        F: Future<Output = ()>,
    {
        let (shutdown_tx, _) = watch::channel(false);
        let mut listeners = JoinSet::new();
        // A panicking task cannot report which listener it was, so the mapping
        // is kept here rather than recovered from the task's return value.
        let mut listener_addrs: HashMap<Id, SocketAddr> = HashMap::new();

        for (mut server, addr) in self.servers.into_iter().zip(self.local_addrs) {
            let mut shutdown_rx = shutdown_tx.subscribe();
            let handle = listeners.spawn(async move {
                let outcome = server
                    .run(async move {
                        // A closed channel means the set is being torn down and
                        // is as good a reason to stop as an explicit request.
                        let _ = shutdown_rx.wait_for(|requested| *requested).await;
                    })
                    .await;
                (addr, outcome)
            });
            listener_addrs.insert(handle.id(), addr);
        }

        tokio::pin!(shutdown);
        let mut shutting_down = false;
        let mut failure: Option<ServerSetError> = None;

        loop {
            tokio::select! {
                () = &mut shutdown, if !shutting_down => {
                    shutting_down = true;
                    let _ = shutdown_tx.send(true);
                }
                joined = listeners.join_next() => {
                    let Some(joined) = joined else { break };
                    let listener_failure = match joined {
                        // The only clean exit: it was asked to stop, and did.
                        Ok((_, Ok(()))) if shutting_down => None,
                        Ok((addr, Ok(()))) => Some(ServerSetError::ListenerStopped { addr }),
                        Ok((addr, Err(source))) => {
                            Some(ServerSetError::ListenerRun { addr, source })
                        }
                        Err(source) => Some(ServerSetError::ListenerTask {
                            addr: listener_addrs.get(&source.id()).copied(),
                            source,
                        }),
                    };
                    if let Some(listener_failure) = listener_failure {
                        failure.get_or_insert(listener_failure);
                        if !shutting_down {
                            shutting_down = true;
                            let _ = shutdown_tx.send(true);
                        }
                    }
                }
            }
        }

        failure.map_or(Ok(()), Err)
    }
}

/// Binds one listener, pre-configuring the socket where the bind itself depends
/// on it.
///
/// The ordinary path is [`Server::bind`] unchanged. Only an IPv6 listener that
/// has to leave an IPv4 sibling's port alone needs its own socket, because
/// `IPV6_V6ONLY` can only be set before the bind it affects.
async fn bind_listener(
    addr: SocketAddr,
    config: ServerConfig,
    ipv6_only: bool,
) -> Result<Server, ServerRuntimeError> {
    if !ipv6_only {
        return Server::bind(addr, config).await;
    }

    let socket =
        bind_ipv6_only(addr).map_err(|source| ServerRuntimeError::Bind { addr, source })?;
    Server::from_socket(socket, config)
}

/// Creates an IPv6-only UDP socket and binds it.
///
/// Everything past the bind — destination metadata for a wildcard listener,
/// the core, traffic-class state — stays with [`Server::from_socket`]. This
/// helper exists solely for the one option that must precede the bind.
fn bind_ipv6_only(addr: SocketAddr) -> io::Result<UdpSocket> {
    let socket = Socket::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_only_v6(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&SockAddr::from(addr))?;
    UdpSocket::from_std(socket.into())
}

/// Whether this listener must be bound IPv6-only to leave an IPv4 sibling's
/// port available.
///
/// Only an explicit port can collide: two ephemeral requests are resolved
/// independently and never name the same port to begin with.
fn needs_ipv6_only(addr: SocketAddr, requested: &[SocketAddr]) -> bool {
    is_genuine_ipv6(addr)
        && addr.port() != 0
        && requested
            .iter()
            .any(|other| other.port() == addr.port() && serves_ipv4(*other))
}

/// Whether a bind address names an IPv6 endpoint in its own right.
///
/// An IPv4-mapped address is not one: `[::ffff:0.0.0.0]` is an IPv4 wildcard
/// bind that the kernel reports back in IPv6 clothing, and the runtime already
/// reads it as IPv4 for wildcard handling and for the traffic-class option. It
/// must not be given `IPV6_V6ONLY` — that would refuse the very traffic it
/// exists to serve — nor counted as the IPv6 half of a same-port pair.
fn is_genuine_ipv6(addr: SocketAddr) -> bool {
    addr.is_ipv6() && !serves_ipv4(addr)
}

/// Whether a bind address will carry IPv4 traffic, in either spelling.
fn serves_ipv4(addr: SocketAddr) -> bool {
    match addr.ip() {
        IpAddr::V4(_) => true,
        IpAddr::V6(address) => address.to_ipv4_mapped().is_some(),
    }
}

/// Failure to construct or supervise a [`ServerSet`].
#[derive(Debug, Error)]
pub enum ServerSetError {
    /// No listener address was requested.
    #[error("a server set needs at least one listener address")]
    NoListeners,
    /// A requested listener could not be bound or prepared. Nothing was served:
    /// any listener already bound for this set has been released.
    #[error("could not set up listener {addr}: {source}")]
    ListenerSetup {
        addr: SocketAddr,
        #[source]
        source: ServerRuntimeError,
    },
    /// A running listener failed. Its siblings were shut down.
    #[error("listener {addr} failed: {source}")]
    ListenerRun {
        addr: SocketAddr,
        #[source]
        source: ServerRuntimeError,
    },
    /// A listener returned before the set was asked to shut down. A set does
    /// not continue with fewer listeners than it was configured for.
    #[error("listener {addr} stopped before the server set was asked to shut down")]
    ListenerStopped { addr: SocketAddr },
    /// A listener task did not complete — it panicked or was aborted. The
    /// address is absent only where the task could not be identified.
    #[error(
        "listener task{} did not complete: {source}",
        .addr.map(|addr| format!(" for {addr}")).unwrap_or_default()
    )]
    ListenerTask {
        addr: Option<SocketAddr>,
        #[source]
        source: JoinError,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Which listener of a requested set has to be bound IPv6-only.
    ///
    /// The interesting rows are the mapped ones. `[::ffff:0.0.0.0]` is an IPv4
    /// wildcard bind wearing an IPv6 address, so it is neither a candidate for
    /// `IPV6_V6ONLY` — which would refuse the traffic it exists to serve — nor
    /// the IPv6 half of a same-port pair. A genuine IPv6 address paired with it
    /// still is.
    #[test]
    fn only_a_genuine_ipv6_listener_sharing_an_explicit_port_is_bound_ipv6_only() {
        for (addr, requested, ipv6_only, why) in [
            (
                "[::]:2112",
                vec!["0.0.0.0:2112", "[::]:2112"],
                true,
                "the wildcard pair is the reason this exists",
            ),
            (
                "[::1]:2112",
                vec!["127.0.0.1:2112", "[::1]:2112"],
                true,
                "and so is the explicit loopback pair",
            ),
            (
                "0.0.0.0:2112",
                vec!["0.0.0.0:2112", "[::]:2112"],
                false,
                "the IPv4 half is bound exactly as it always was",
            ),
            (
                "[::]:2112",
                vec!["[::]:2112"],
                false,
                "an IPv6 listener alone keeps ordinary bind semantics",
            ),
            (
                "[::]:2112",
                vec!["0.0.0.0:2113", "[::]:2112"],
                false,
                "different ports cannot collide",
            ),
            (
                "[::]:0",
                vec!["0.0.0.0:0", "[::]:0"],
                false,
                "two ephemeral requests resolve independently",
            ),
            (
                "[::ffff:0.0.0.0]:2112",
                vec!["0.0.0.0:2112", "[::ffff:0.0.0.0]:2112"],
                false,
                "a mapped wildcard is an IPv4 listener, not an IPv6 one",
            ),
            (
                "[::]:2112",
                vec!["[::ffff:0.0.0.0]:2112", "[::]:2112"],
                true,
                "and it is the IPv4 half a genuine IPv6 listener must avoid",
            ),
            (
                "[::ffff:127.0.0.1]:2112",
                vec!["[::ffff:127.0.0.1]:2112", "[::1]:2112"],
                false,
                "a mapped host address is explicit IPv4 too",
            ),
        ] {
            let requested: Vec<SocketAddr> = requested
                .iter()
                .map(|addr| addr.parse().expect("a test address must parse"))
                .collect();
            assert_eq!(
                needs_ipv6_only(addr.parse().unwrap(), &requested),
                ipv6_only,
                "{addr} among {requested:?}: {why}"
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_set_serving_nothing_is_refused() {
        assert!(matches!(
            ServerSet::bind([], ServerConfig::default()).await,
            Err(ServerSetError::NoListeners)
        ));
    }
}
