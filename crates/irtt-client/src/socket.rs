use std::{
    net::{SocketAddr, ToSocketAddrs, UdpSocket},
    time::Duration,
};

use socket2::{Domain, Protocol, Socket, Type};

use crate::{
    config::{ClientConfig, SocketConfig, DEFAULT_PORT, MIN_OPEN_TIMEOUT},
    error::ClientError,
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
    let addr = normalize_server_addr(&config.server_addr);
    let mut addrs = addr
        .to_socket_addrs()
        .map_err(|_| ClientError::Resolve { addr: addr.clone() })?;
    addrs
        .find(|addr| {
            (!config.socket_config.ipv4_only || addr.is_ipv4())
                && (!config.socket_config.ipv6_only || addr.is_ipv6())
        })
        .ok_or(ClientError::Resolve { addr })
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
    let domain = if remote.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;

    if config.ipv6_only && remote.is_ipv6() {
        socket.set_only_v6(true)?;
    }
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
    if let Some(ttl) = config.ttl {
        apply_ttl_to_socket(&socket, remote, ttl)?;
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
}
