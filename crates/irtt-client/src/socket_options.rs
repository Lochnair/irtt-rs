use std::{
    io,
    net::{SocketAddr, UdpSocket},
};

use socket2::SockRef;

#[cfg(any(
    target_os = "android",
    target_os = "freebsd",
    target_os = "fuchsia",
    target_os = "linux"
))]
use socket2::Socket;

use crate::{
    config::{MAX_DSCP_CODEPOINT, MAX_TTL},
    error::ClientError,
};

#[cfg(any(
    target_os = "android",
    target_os = "freebsd",
    target_os = "fuchsia",
    target_os = "linux"
))]
pub(crate) fn apply_routing_options(
    socket: &Socket,
    config: &crate::SocketConfig,
    remote: SocketAddr,
) -> Result<(), ClientError> {
    #[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
    {
        if let Some(device) = &config.bind_to_device {
            socket
                .bind_device(Some(device.as_bytes()))
                .map_err(|source| ClientError::SocketOption {
                    operation: "bind socket to device",
                    remote,
                    source,
                })?;
        }
        if let Some(mark) = config.mark {
            socket
                .set_mark(mark)
                .map_err(|source| ClientError::SocketOption {
                    operation: "set firewall mark",
                    remote,
                    source,
                })?;
        }
    }
    #[cfg(target_os = "freebsd")]
    if let Some(fib) = config.fib {
        socket
            .set_fib(fib)
            .map_err(|source| ClientError::SocketOption {
                operation: "set FIB",
                remote,
                source,
            })?;
    }
    Ok(())
}

/// Converts a public DSCP codepoint (`0..=`[`MAX_DSCP_CODEPOINT`]) into the
/// raw IP TOS / Traffic Class byte it occupies the upper six bits of.
///
/// This is the single place `codepoint << 2` is spelled out; both the
/// config-to-wire conversion and any direct codepoint-based socket
/// application go through this function so the shift cannot be duplicated or
/// accidentally applied twice.
pub(crate) fn dscp_codepoint_to_traffic_class(dscp: u8) -> Result<u32, ClientError> {
    if dscp > MAX_DSCP_CODEPOINT {
        return Err(ClientError::InvalidConfig {
            reason: format!("dscp must be <= {MAX_DSCP_CODEPOINT}"),
        });
    }
    Ok(u32::from(dscp) << 2)
}

/// Applies an already-raw IP TOS / Traffic Class byte to the socket directly,
/// without any codepoint shift.
pub(crate) fn apply_traffic_class_to_socket(
    socket: &UdpSocket,
    remote: SocketAddr,
    traffic_class: u8,
) -> Result<(), ClientError> {
    set_socket_traffic_class(
        socket,
        remote,
        u32::from(traffic_class),
        "set negotiated DSCP",
    )
}

pub(crate) fn clear_dscp_on_socket(
    socket: &UdpSocket,
    remote: SocketAddr,
) -> Result<(), ClientError> {
    set_socket_traffic_class(socket, remote, 0, "clear DSCP before close")
}

/// Applies an already-raw IP TOS / Traffic Class byte to the Tokio socket
/// directly, without any codepoint shift.
#[cfg(feature = "tokio")]
pub(crate) fn apply_traffic_class_to_tokio_socket(
    socket: &tokio::net::UdpSocket,
    remote: SocketAddr,
    traffic_class: u8,
) -> Result<(), ClientError> {
    set_tokio_socket_traffic_class(
        socket,
        remote,
        u32::from(traffic_class),
        "set negotiated DSCP",
    )
}

#[cfg(feature = "tokio")]
pub(crate) fn clear_dscp_on_tokio_socket(
    socket: &tokio::net::UdpSocket,
    remote: SocketAddr,
) -> Result<(), ClientError> {
    set_tokio_socket_traffic_class(socket, remote, 0, "clear DSCP before close")
}

pub(crate) fn validate_ttl(ttl: u32) -> Result<(), ClientError> {
    if ttl == 0 || ttl > MAX_TTL {
        return Err(ClientError::InvalidConfig {
            reason: format!("ttl must be in range 1..={MAX_TTL}"),
        });
    }
    Ok(())
}

pub(crate) fn apply_ttl_to_socket(
    socket: &UdpSocket,
    remote: SocketAddr,
    ttl: u32,
) -> Result<(), ClientError> {
    validate_ttl(ttl)?;
    set_socket_ttl_ref(SockRef::from(socket), remote, ttl).map_err(|source| {
        ClientError::SocketOption {
            operation: "set TTL/hop limit",
            remote,
            source,
        }
    })
}

fn set_socket_ttl_ref(socket: SockRef<'_>, remote: SocketAddr, ttl: u32) -> io::Result<()> {
    if remote.is_ipv4() {
        socket.set_ttl_v4(ttl)
    } else {
        socket.set_unicast_hops_v6(ttl)
    }
}

fn set_socket_traffic_class(
    socket: &UdpSocket,
    remote: SocketAddr,
    traffic_class: u32,
    operation: &'static str,
) -> Result<(), ClientError> {
    set_socket_traffic_class_ref(SockRef::from(socket), remote, traffic_class).map_err(|source| {
        ClientError::SocketOption {
            operation,
            remote,
            source,
        }
    })
}

#[cfg(feature = "tokio")]
fn set_tokio_socket_traffic_class(
    socket: &tokio::net::UdpSocket,
    remote: SocketAddr,
    traffic_class: u32,
    operation: &'static str,
) -> Result<(), ClientError> {
    set_socket_traffic_class_ref(SockRef::from(socket), remote, traffic_class).map_err(|source| {
        ClientError::SocketOption {
            operation,
            remote,
            source,
        }
    })
}

fn set_socket_traffic_class_ref(
    socket: SockRef<'_>,
    remote: SocketAddr,
    traffic_class: u32,
) -> io::Result<()> {
    if remote.is_ipv4() {
        set_ipv4_traffic_class(socket, traffic_class)
    } else {
        set_ipv6_traffic_class(socket, traffic_class)
    }
}

#[cfg(not(any(
    target_os = "fuchsia",
    target_os = "redox",
    target_os = "solaris",
    target_os = "illumos",
    target_os = "haiku",
)))]
fn set_ipv4_traffic_class(socket: SockRef<'_>, traffic_class: u32) -> io::Result<()> {
    socket.set_tos_v4(traffic_class)
}

#[cfg(any(
    target_os = "fuchsia",
    target_os = "redox",
    target_os = "solaris",
    target_os = "illumos",
    target_os = "haiku",
))]
fn set_ipv4_traffic_class(_socket: SockRef<'_>, traffic_class: u32) -> io::Result<()> {
    unsupported_traffic_class(traffic_class, "IPv4 DSCP socket options")
}

#[cfg(any(
    target_os = "android",
    target_os = "dragonfly",
    target_os = "freebsd",
    target_os = "fuchsia",
    target_os = "linux",
    target_os = "macos",
    target_os = "netbsd",
    target_os = "openbsd",
    target_os = "cygwin",
))]
fn set_ipv6_traffic_class(socket: SockRef<'_>, traffic_class: u32) -> io::Result<()> {
    socket.set_tclass_v6(traffic_class)
}

#[cfg(not(any(
    target_os = "android",
    target_os = "dragonfly",
    target_os = "freebsd",
    target_os = "fuchsia",
    target_os = "linux",
    target_os = "macos",
    target_os = "netbsd",
    target_os = "openbsd",
    target_os = "cygwin",
)))]
fn set_ipv6_traffic_class(_socket: SockRef<'_>, traffic_class: u32) -> io::Result<()> {
    unsupported_traffic_class(traffic_class, "IPv6 DSCP socket options")
}

#[cfg(any(
    target_os = "fuchsia",
    target_os = "redox",
    target_os = "solaris",
    target_os = "illumos",
    target_os = "haiku",
    not(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "fuchsia",
        target_os = "linux",
        target_os = "macos",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "cygwin",
    )),
))]
fn unsupported_traffic_class(traffic_class: u32, feature: &'static str) -> io::Result<()> {
    if traffic_class == 0 {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("{feature} are unsupported on this target"),
        ))
    }
}
