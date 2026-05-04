use std::{
    io,
    net::{SocketAddr, UdpSocket},
};

use crate::{
    config::{MAX_DSCP_CODEPOINT, MAX_TTL},
    error::ClientError,
};

pub(crate) fn dscp_codepoint_to_traffic_class(dscp: u8) -> Result<u32, ClientError> {
    if dscp > MAX_DSCP_CODEPOINT {
        return Err(ClientError::InvalidConfig {
            reason: format!("dscp must be <= {MAX_DSCP_CODEPOINT}"),
        });
    }
    Ok(u32::from(dscp) << 2)
}

pub(crate) fn apply_dscp_to_socket(
    socket: &UdpSocket,
    remote: SocketAddr,
    dscp_codepoint: u8,
) -> Result<(), ClientError> {
    let traffic_class = dscp_codepoint_to_traffic_class(dscp_codepoint)?;
    set_socket_traffic_class(socket, remote, traffic_class, "set negotiated DSCP")
}

pub(crate) fn clear_dscp_on_socket(
    socket: &UdpSocket,
    remote: SocketAddr,
) -> Result<(), ClientError> {
    set_socket_traffic_class(socket, remote, 0, "clear DSCP before close")
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
    set_socket_ttl(socket, remote, ttl).map_err(|source| ClientError::SocketOption {
        operation: "set TTL/hop limit",
        remote,
        source,
    })
}

#[cfg(test)]
pub(crate) fn socket_traffic_class(
    socket: &UdpSocket,
    remote: SocketAddr,
) -> Result<u32, ClientError> {
    get_socket_traffic_class(socket, remote).map_err(|source| ClientError::SocketOption {
        operation: "read DSCP socket option",
        remote,
        source,
    })
}

#[cfg(test)]
pub(crate) fn socket_ttl(socket: &UdpSocket, remote: SocketAddr) -> Result<u32, ClientError> {
    get_socket_ttl(socket, remote).map_err(|source| ClientError::SocketOption {
        operation: "read TTL/hop limit socket option",
        remote,
        source,
    })
}

fn set_socket_ttl(socket: &UdpSocket, remote: SocketAddr, ttl: u32) -> io::Result<()> {
    if remote.is_ipv4() {
        socket2::SockRef::from(socket).set_ttl(ttl)
    } else {
        socket2::SockRef::from(socket).set_unicast_hops_v6(ttl)
    }
}

#[cfg(test)]
fn get_socket_ttl(socket: &UdpSocket, remote: SocketAddr) -> io::Result<u32> {
    if remote.is_ipv4() {
        socket2::SockRef::from(socket).ttl()
    } else {
        socket2::SockRef::from(socket).unicast_hops_v6()
    }
}

fn set_socket_traffic_class(
    socket: &UdpSocket,
    remote: SocketAddr,
    traffic_class: u32,
    operation: &'static str,
) -> Result<(), ClientError> {
    set_socket_traffic_class_io(socket, remote, traffic_class).map_err(|source| {
        ClientError::SocketOption {
            operation,
            remote,
            source,
        }
    })
}

fn set_socket_traffic_class_io(
    socket: &UdpSocket,
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
fn set_ipv4_traffic_class(socket: &UdpSocket, traffic_class: u32) -> io::Result<()> {
    socket2::SockRef::from(socket).set_tos(traffic_class)
}

#[cfg(any(
    target_os = "fuchsia",
    target_os = "redox",
    target_os = "solaris",
    target_os = "illumos",
    target_os = "haiku",
))]
fn set_ipv4_traffic_class(_socket: &UdpSocket, traffic_class: u32) -> io::Result<()> {
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
fn set_ipv6_traffic_class(socket: &UdpSocket, traffic_class: u32) -> io::Result<()> {
    socket2::SockRef::from(socket).set_tclass_v6(traffic_class)
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
fn set_ipv6_traffic_class(_socket: &UdpSocket, traffic_class: u32) -> io::Result<()> {
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

#[cfg(all(
    test,
    not(any(
        target_os = "fuchsia",
        target_os = "redox",
        target_os = "solaris",
        target_os = "illumos",
        target_os = "haiku",
    ))
))]
fn get_ipv4_traffic_class(socket: &UdpSocket) -> io::Result<u32> {
    socket2::SockRef::from(socket).tos()
}

#[cfg(all(
    test,
    any(
        target_os = "fuchsia",
        target_os = "redox",
        target_os = "solaris",
        target_os = "illumos",
        target_os = "haiku",
    )
))]
fn get_ipv4_traffic_class(_socket: &UdpSocket) -> io::Result<u32> {
    unsupported_readback("IPv4 DSCP socket option readback")
}

#[cfg(all(
    test,
    any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "fuchsia",
        target_os = "linux",
        target_os = "macos",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "cygwin",
    )
))]
fn get_ipv6_traffic_class(socket: &UdpSocket) -> io::Result<u32> {
    socket2::SockRef::from(socket).tclass_v6()
}

#[cfg(all(
    test,
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
    ))
))]
fn get_ipv6_traffic_class(_socket: &UdpSocket) -> io::Result<u32> {
    unsupported_readback("IPv6 DSCP socket option readback")
}

#[cfg(test)]
fn get_socket_traffic_class(socket: &UdpSocket, remote: SocketAddr) -> io::Result<u32> {
    if remote.is_ipv4() {
        get_ipv4_traffic_class(socket)
    } else {
        get_ipv6_traffic_class(socket)
    }
}

#[cfg(all(
    test,
    any(
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
    )
))]
fn unsupported_readback(feature: &'static str) -> io::Result<u32> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        format!("{feature} is unsupported on this target"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_dscp_codepoint_to_traffic_class_byte() {
        for (dscp, traffic_class) in [(0, 0), (1, 4), (46, 184), (63, 252)] {
            let value = dscp_codepoint_to_traffic_class(dscp).unwrap();
            assert_eq!(value, traffic_class);
            assert_eq!(value & 0b11, 0);
        }
    }

    #[test]
    fn rejects_out_of_range_dscp_codepoint() {
        assert!(matches!(
            dscp_codepoint_to_traffic_class(64),
            Err(ClientError::InvalidConfig { .. })
        ));
    }

    #[test]
    fn validates_ttl_range() {
        assert!(validate_ttl(1).is_ok());
        assert!(validate_ttl(64).is_ok());
        assert!(validate_ttl(255).is_ok());
        assert!(matches!(
            validate_ttl(0),
            Err(ClientError::InvalidConfig { .. })
        ));
        assert!(matches!(
            validate_ttl(256),
            Err(ClientError::InvalidConfig { .. })
        ));
    }

    #[test]
    fn ipv4_socket_option_sets_ttl() {
        let remote = SocketAddr::from(([127, 0, 0, 1], 9));
        let socket = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        socket.connect(remote).unwrap();

        apply_ttl_to_socket(&socket, remote, 64).unwrap();
        assert_eq!(socket_ttl(&socket, remote).unwrap(), 64);

        apply_ttl_to_socket(&socket, remote, 1).unwrap();
        assert_eq!(socket_ttl(&socket, remote).unwrap(), 1);
    }

    #[test]
    fn ipv6_socket_option_sets_unicast_hop_limit() {
        let remote = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], 9));
        let Some(socket) = bind_connected_ipv6_loopback(remote) else {
            return;
        };

        apply_ttl_to_socket(&socket, remote, 64).unwrap();
        let ttl = match socket_ttl(&socket, remote) {
            Ok(ttl) => ttl,
            Err(error) if is_unsupported_socket_readback(&error) => {
                eprintln!("skipping IPv6 hop-limit readback test: {error}");
                return;
            }
            Err(error) => panic!("{error}"),
        };
        assert_eq!(ttl, 64);

        apply_ttl_to_socket(&socket, remote, 1).unwrap();
        assert_eq!(socket_ttl(&socket, remote).unwrap(), 1);
    }

    #[test]
    #[cfg(not(any(
        target_os = "fuchsia",
        target_os = "redox",
        target_os = "solaris",
        target_os = "illumos",
        target_os = "haiku",
    )))]
    fn ipv4_socket_option_sets_and_clears_traffic_class() {
        let remote = SocketAddr::from(([127, 0, 0, 1], 9));
        let socket = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        socket.connect(remote).unwrap();

        apply_dscp_to_socket(&socket, remote, 46).unwrap();
        let traffic_class = socket_traffic_class(&socket, remote).unwrap();
        assert_eq!(traffic_class & 0xfc, 184);
        assert_eq!(traffic_class & 0b11, 0);

        clear_dscp_on_socket(&socket, remote).unwrap();
        assert_eq!(socket_traffic_class(&socket, remote).unwrap(), 0);
    }

    #[test]
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
    fn ipv6_socket_option_sets_and_clears_traffic_class() {
        let remote = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], 9));
        let Some(socket) = bind_connected_ipv6_loopback(remote) else {
            return;
        };

        apply_dscp_to_socket(&socket, remote, 46).unwrap();
        let traffic_class = match socket_traffic_class(&socket, remote) {
            Ok(traffic_class) => traffic_class,
            Err(error) if is_unsupported_socket_readback(&error) => {
                eprintln!("skipping IPv6 traffic-class readback test: {error}");
                return;
            }
            Err(error) => panic!("{error}"),
        };
        assert_eq!(traffic_class & 0xfc, 184);
        assert_eq!(traffic_class & 0b11, 0);

        clear_dscp_on_socket(&socket, remote).unwrap();
        assert_eq!(socket_traffic_class(&socket, remote).unwrap(), 0);
    }

    fn bind_connected_ipv6_loopback(remote: SocketAddr) -> Option<UdpSocket> {
        let socket = match UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], 0))) {
            Ok(socket) => socket,
            Err(error) if is_unavailable_ipv6_loopback(&error) => {
                eprintln!("skipping IPv6 socket option test: IPv6 loopback unavailable: {error}");
                return None;
            }
            Err(error) => panic!("{error}"),
        };
        match socket.connect(remote) {
            Ok(()) => Some(socket),
            Err(error) if is_unavailable_ipv6_loopback(&error) => {
                eprintln!("skipping IPv6 socket option test: IPv6 loopback unavailable: {error}");
                None
            }
            Err(error) => panic!("{error}"),
        }
    }

    fn is_unavailable_ipv6_loopback(error: &io::Error) -> bool {
        matches!(
            error.kind(),
            io::ErrorKind::AddrNotAvailable | io::ErrorKind::Unsupported
        )
    }

    fn is_unsupported_socket_readback(error: &ClientError) -> bool {
        matches!(
            error,
            ClientError::SocketOption { source, .. }
                if matches!(source.kind(), io::ErrorKind::Unsupported)
        )
    }
}
