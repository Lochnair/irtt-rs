use std::{
    net::{SocketAddr, ToSocketAddrs, UdpSocket},
    time::Duration,
};

use socket2::{Domain, Protocol, Socket, Type};

use crate::{
    config::{ClientConfig, SocketConfig, DEFAULT_PORT, MIN_OPEN_TIMEOUT},
    error::ClientError,
    receive::configure_receive_metadata,
    socket_options::apply_ttl_to_socket,
};

pub(crate) fn validate_open_timeouts(timeouts: &[Duration]) -> Result<(), ClientError> {
    if timeouts.is_empty() {
        return Err(ClientError::NoOpenTimeouts);
    }
    for timeout in timeouts {
        if *timeout < MIN_OPEN_TIMEOUT {
            return Err(ClientError::OpenTimeoutTooSmall {
                timeout: *timeout,
                minimum: MIN_OPEN_TIMEOUT,
            });
        }
    }
    Ok(())
}

pub(crate) fn resolve_remote(config: &ClientConfig) -> Result<SocketAddr, ClientError> {
    #[cfg(all(test, feature = "tokio"))]
    SYNC_RESOLVER_CALLS.with(|calls| calls.set(calls.get() + 1));

    let addr = normalize_server_addr(&config.server_addr);
    let mut addrs = addr
        .to_socket_addrs()
        .map_err(|_| ClientError::Resolve { addr: addr.clone() })?;
    addrs
        .find(|addr| address_family_allowed(config, *addr))
        .ok_or(ClientError::Resolve { addr })
}

#[cfg(feature = "tokio")]
pub(crate) async fn resolve_remote_tokio(config: &ClientConfig) -> Result<SocketAddr, ClientError> {
    let addr = normalize_server_addr(&config.server_addr);
    if let Ok(remote) = addr.parse::<SocketAddr>() {
        return address_family_allowed(config, remote)
            .then_some(remote)
            .ok_or(ClientError::Resolve { addr });
    }

    #[cfg(all(test, feature = "tokio"))]
    TOKIO_DNS_LOOKUPS.with(|calls| calls.set(calls.get() + 1));

    let mut addrs = tokio::net::lookup_host(&addr)
        .await
        .map_err(|_| ClientError::Resolve { addr: addr.clone() })?;
    addrs
        .find(|remote| address_family_allowed(config, *remote))
        .ok_or_else(|| ClientError::Resolve { addr: addr.clone() })
}

fn address_family_allowed(config: &ClientConfig, remote: SocketAddr) -> bool {
    (!config.socket_config.ipv4_only || remote.is_ipv4())
        && (!config.socket_config.ipv6_only || remote.is_ipv6())
}

#[cfg(all(test, feature = "tokio"))]
std::thread_local! {
    static SYNC_RESOLVER_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static TOKIO_DNS_LOOKUPS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(all(test, feature = "tokio"))]
pub(crate) fn resolution_call_counts() -> (usize, usize) {
    (
        SYNC_RESOLVER_CALLS.with(std::cell::Cell::get),
        TOKIO_DNS_LOOKUPS.with(std::cell::Cell::get),
    )
}

pub(crate) fn normalize_server_addr(addr: &str) -> String {
    if addr.parse::<SocketAddr>().is_ok() {
        return addr.to_owned();
    }
    if addr.starts_with('[') && addr.ends_with(']') {
        return format!("{addr}:{DEFAULT_PORT}");
    }
    if addr.starts_with('[') {
        return addr.to_owned();
    }
    if addr.parse::<std::net::Ipv6Addr>().is_ok() {
        return format!("[{addr}]:{DEFAULT_PORT}");
    }
    if addr
        .rsplit_once(':')
        .is_some_and(|(_, port)| port.parse::<u16>().is_ok())
    {
        return addr.to_owned();
    }
    format!("{addr}:{DEFAULT_PORT}")
}

pub(crate) fn connect_udp_socket(
    config: &SocketConfig,
    remote: SocketAddr,
) -> Result<UdpSocket, ClientError> {
    let socket = create_connected_udp_socket(config, remote)?;
    socket.set_read_timeout(config.recv_timeout)?;
    Ok(socket)
}

fn create_connected_udp_socket(
    config: &SocketConfig,
    remote: SocketAddr,
) -> Result<UdpSocket, ClientError> {
    let socket = create_prebind_udp_socket(config, remote)?;
    let bind_addr = config.bind_addr.unwrap_or_else(|| {
        if remote.is_ipv4() {
            SocketAddr::from(([0, 0, 0, 0], 0))
        } else {
            SocketAddr::from(([0_u16; 8], 0))
        }
    });
    socket.bind(&bind_addr.into())?;
    socket.connect(&remote.into())?;

    let socket: UdpSocket = socket.into();
    configure_receive_metadata(&socket, remote).map_err(|source| ClientError::SocketOption {
        operation: "enable receive metadata",
        remote,
        source,
    })?;
    if let Some(ttl) = config.ttl {
        apply_ttl_to_socket(&socket, remote, ttl)?;
    }
    Ok(socket)
}

fn create_prebind_udp_socket(
    config: &SocketConfig,
    remote: SocketAddr,
) -> Result<Socket, ClientError> {
    let domain = if remote.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;

    if config.ipv6_only && remote.is_ipv6() {
        socket.set_only_v6(true)?;
    }
    Ok(socket)
}

#[cfg(feature = "tokio")]
pub(crate) async fn connect_tokio_udp_socket(
    config: &SocketConfig,
    remote: SocketAddr,
) -> Result<tokio::net::UdpSocket, ClientError> {
    let socket = create_connected_udp_socket(config, remote)?;
    socket.set_nonblocking(true)?;
    Ok(tokio::net::UdpSocket::from_std(socket)?)
}

pub(crate) fn bind_unconnected_udp_socket(
    config: &SocketConfig,
    family_remote: SocketAddr,
) -> Result<UdpSocket, ClientError> {
    let domain = if family_remote.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;

    if config.ipv6_only && family_remote.is_ipv6() {
        socket.set_only_v6(true)?;
    }
    let bind_addr = config.bind_addr.unwrap_or_else(|| {
        if family_remote.is_ipv4() {
            SocketAddr::from(([0, 0, 0, 0], 0))
        } else {
            SocketAddr::from(([0_u16; 8], 0))
        }
    });
    socket.bind(&bind_addr.into())?;

    let socket: UdpSocket = socket.into();
    configure_receive_metadata(&socket, family_remote).map_err(|source| {
        ClientError::SocketOption {
            operation: "enable receive metadata",
            remote: family_remote,
            source,
        }
    })?;
    if let Some(ttl) = config.ttl {
        apply_ttl_to_socket(&socket, family_remote, ttl)?;
    }
    socket.set_read_timeout(config.recv_timeout)?;
    Ok(socket)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_server_addr_adds_default_ports() {
        assert_eq!(normalize_server_addr("127.0.0.1"), "127.0.0.1:2112");
        assert_eq!(normalize_server_addr("127.0.0.1:1234"), "127.0.0.1:1234");
        assert_eq!(normalize_server_addr("localhost"), "localhost:2112");
        assert_eq!(normalize_server_addr("localhost:1234"), "localhost:1234");
        assert_eq!(normalize_server_addr("::1"), "[::1]:2112");
        assert_eq!(normalize_server_addr("[::1]"), "[::1]:2112");
        assert_eq!(normalize_server_addr("[::1]:1234"), "[::1]:1234");
    }

    #[test]
    fn address_family_filtering_remains_unchanged() {
        let ipv4 = SocketAddr::from(([127, 0, 0, 1], DEFAULT_PORT));
        let ipv6 = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], DEFAULT_PORT));
        let mut config = ClientConfig::default();

        assert!(address_family_allowed(&config, ipv4));
        assert!(address_family_allowed(&config, ipv6));

        config.socket_config.ipv4_only = true;
        assert!(address_family_allowed(&config, ipv4));
        assert!(!address_family_allowed(&config, ipv6));

        config.socket_config.ipv4_only = false;
        config.socket_config.ipv6_only = true;
        assert!(!address_family_allowed(&config, ipv4));
        assert!(address_family_allowed(&config, ipv6));
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn tokio_ipv6_only_is_applied_before_bind() {
        let config = SocketConfig {
            ipv6_only: true,
            ..SocketConfig::default()
        };
        let remote = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], DEFAULT_PORT));

        let socket = create_prebind_udp_socket(&config, remote).unwrap();

        assert!(socket.only_v6().unwrap());
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn tokio_ipv6_socket_preserves_v6only_and_explicit_bind() {
        let bind_addr = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], 0));
        let peer = match UdpSocket::bind(bind_addr) {
            Ok(peer) => peer,
            Err(error) if ipv6_loopback_unavailable(&error) => {
                eprintln!("skipping IPv6 Tokio socket test: IPv6 loopback unavailable: {error}");
                return;
            }
            Err(error) => panic!("{error}"),
        };
        let remote = peer.local_addr().unwrap();
        let config = SocketConfig {
            bind_addr: Some(bind_addr),
            ipv6_only: true,
            ..SocketConfig::default()
        };

        tokio_runtime().block_on(async {
            let socket = connect_tokio_udp_socket(&config, remote).await.unwrap();

            assert!(socket2::SockRef::from(&socket).only_v6().unwrap());
            assert!(socket.local_addr().unwrap().is_ipv6());
            assert_eq!(socket.local_addr().unwrap().ip(), bind_addr.ip());
            assert_eq!(socket.peer_addr().unwrap(), remote);
        });
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn tokio_ipv4_socket_preserves_family_and_explicit_bind() {
        let peer = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let remote = peer.local_addr().unwrap();
        let bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let config = SocketConfig {
            bind_addr: Some(bind_addr),
            ..SocketConfig::default()
        };

        tokio_runtime().block_on(async {
            let socket = connect_tokio_udp_socket(&config, remote).await.unwrap();

            assert!(socket.local_addr().unwrap().is_ipv4());
            assert_eq!(socket.local_addr().unwrap().ip(), bind_addr.ip());
            assert_eq!(socket.peer_addr().unwrap(), remote);
        });
    }

    #[cfg(feature = "tokio")]
    fn tokio_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .unwrap()
    }

    #[cfg(feature = "tokio")]
    fn ipv6_loopback_unavailable(error: &std::io::Error) -> bool {
        matches!(
            error.kind(),
            std::io::ErrorKind::AddrNotAvailable | std::io::ErrorKind::Unsupported
        )
    }
}
