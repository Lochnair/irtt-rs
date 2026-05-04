use std::{
    io,
    net::{SocketAddr, UdpSocket},
};

use crate::{config::MAX_DSCP_CODEPOINT, error::ClientError};

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
        let socket = UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], 0))).unwrap();
        socket.connect(remote).unwrap();

        apply_dscp_to_socket(&socket, remote, 46).unwrap();
        let traffic_class = socket_traffic_class(&socket, remote).unwrap();
        assert_eq!(traffic_class & 0xfc, 184);
        assert_eq!(traffic_class & 0b11, 0);

        clear_dscp_on_socket(&socket, remote).unwrap();
        assert_eq!(socket_traffic_class(&socket, remote).unwrap(), 0);
    }
}
