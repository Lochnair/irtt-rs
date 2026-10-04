use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};

use socket2::{Domain, Protocol, Socket, Type};

use crate::{
    config::{AddressFamily, SocketConfig, DEFAULT_PORT},
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

pub(crate) fn resolve_remote(
    endpoint: &str,
    family: AddressFamily,
) -> Result<SocketAddr, ClientError> {
    let addr = normalize_server_addr(endpoint);
    let mut addrs = addr
        .to_socket_addrs()
        .map_err(|_| ClientError::Resolve { addr: addr.clone() })?;
    addrs
        .find(|addr| address_family_allowed(family, *addr))
        .ok_or(ClientError::Resolve { addr })
}

#[cfg(feature = "tokio")]
pub(crate) async fn resolve_remote_tokio(
    endpoint: &str,
    family: AddressFamily,
) -> Result<SocketAddr, ClientError> {
    let addr = normalize_server_addr(endpoint);
    if let Ok(remote) = addr.parse::<SocketAddr>() {
        return address_family_allowed(family, remote)
            .then_some(remote)
            .ok_or(ClientError::Resolve { addr });
    }

    let mut addrs = tokio::net::lookup_host(&addr)
        .await
        .map_err(|_| ClientError::Resolve { addr: addr.clone() })?;
    addrs
        .find(|remote| address_family_allowed(family, *remote))
        .ok_or_else(|| ClientError::Resolve { addr: addr.clone() })
}

fn address_family_allowed(family: AddressFamily, remote: SocketAddr) -> bool {
    match family {
        AddressFamily::Any => true,
        AddressFamily::Ipv4 => remote.is_ipv4(),
        AddressFamily::Ipv6 => remote.is_ipv6(),
    }
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
    family: AddressFamily,
) -> Result<UdpSocket, ClientError> {
    let socket = create_prebind_udp_socket(remote, family)?;
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
    remote: SocketAddr,
    family: AddressFamily,
) -> Result<Socket, ClientError> {
    let domain = if remote.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;

    if family == AddressFamily::Ipv6 && remote.is_ipv6() {
        socket.set_only_v6(true)?;
    }
    Ok(socket)
}

#[cfg(feature = "tokio")]
pub(crate) fn connect_tokio_udp_socket(
    config: &SocketConfig,
    remote: SocketAddr,
    family: AddressFamily,
) -> Result<tokio::net::UdpSocket, ClientError> {
    let socket = connect_udp_socket(config, remote, family)?;
    socket.set_nonblocking(true)?;
    Ok(tokio::net::UdpSocket::from_std(socket)?)
}
