//! Tokio UDP orchestration around [`ServerCore`](crate::ServerCore).

use std::{future::Future, io, net::SocketAddr, time::Duration};

use thiserror::Error;
use tokio::{net::UdpSocket, time::MissedTickBehavior};

use crate::{ServerConfig, ServerCore, ServerError};

/// Fixed transport receive capacity.
///
/// Standard UDP payloads fit in this buffer. A datagram reported as filling it
/// exactly is dropped conservatively instead of passing potentially truncated
/// bytes to the protocol core. IPv6 jumbograms are not supported.
const RECEIVE_BUFFER_LEN: usize = 65_536;
const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(1);

/// A Tokio UDP listener backed by one deterministic server core.
///
/// Processing is sequential: one task receives a datagram, lets the core
/// process it, and sends the optional reply from the same socket. No
/// per-datagram tasks or locks are involved.
///
/// Bind an explicit local address when reply source-address identity matters.
/// Wildcard binds rely on the kernel's source-address choice; destination
/// packet metadata and per-packet source selection are not yet implemented for
/// multi-homed hosts.
#[derive(Debug)]
pub struct Server {
    socket: UdpSocket,
    core: ServerCore,
    recv_buffer: Vec<u8>,
}

impl Server {
    /// Binds one UDP listener at `addr` and creates its independent session
    /// namespace.
    pub async fn bind(addr: SocketAddr, config: ServerConfig) -> Result<Self, ServerRuntimeError> {
        let socket = UdpSocket::bind(addr)
            .await
            .map_err(|source| ServerRuntimeError::Bind { addr, source })?;
        Ok(Self::from_socket(socket, config))
    }

    /// Wraps an already prepared Tokio UDP socket in a server.
    #[must_use]
    pub fn from_socket(socket: UdpSocket, config: ServerConfig) -> Self {
        Self {
            socket,
            core: ServerCore::new(config),
            recv_buffer: vec![0; RECEIVE_BUFFER_LEN],
        }
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
    /// reply; receive failures and internal core failures terminate the loop
    /// with an error.
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
                received = self.socket.recv_from(&mut self.recv_buffer) => {
                    let (len, peer) = match received {
                        Ok(received) => received,
                        Err(source) if source.kind() == io::ErrorKind::Interrupted => continue,
                        Err(source) => return Err(ServerRuntimeError::Receive { source }),
                    };
                    if len == RECEIVE_BUFFER_LEN {
                        continue;
                    }

                    if let Some(reply) = self.core.handle_datagram(peer, &self.recv_buffer[..len])? {
                        let send = self.socket.send_to(reply.bytes(), peer);
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
}
