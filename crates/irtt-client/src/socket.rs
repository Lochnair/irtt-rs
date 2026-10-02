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

#[cfg(any(
    target_os = "android",
    target_os = "freebsd",
    target_os = "fuchsia",
    target_os = "linux"
))]
use crate::socket_options::apply_routing_options;

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
    #[cfg(any(
        target_os = "android",
        target_os = "freebsd",
        target_os = "fuchsia",
        target_os = "linux"
    ))]
    apply_routing_options(&socket, config, remote)?;
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
pub(crate) fn connect_tokio_udp_socket(
    config: &SocketConfig,
    remote: SocketAddr,
) -> Result<tokio::net::UdpSocket, ClientError> {
    let socket = create_connected_udp_socket(config, remote)?;
    socket.set_nonblocking(true)?;
    Ok(tokio::net::UdpSocket::from_std(socket)?)
}
