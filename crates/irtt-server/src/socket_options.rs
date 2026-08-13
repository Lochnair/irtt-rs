//! Applying a reply's traffic class to the listener socket.
//!
//! The value is the raw IPv4 TOS / IPv6 Traffic Class byte an
//! [`OutboundDatagram`](crate::OutboundDatagram) asks for, and which option
//! carries it is decided by the listener's own address family — one `Server`
//! owns one bound socket, so the peer a reply happens to be going to never
//! selects the option.
//!
//! Everything here is `socket2`'s safe API; the crate forbids unsafe code and
//! there is no raw `setsockopt` or per-packet control message anywhere in the
//! server.

use std::io;

use socket2::SockRef;

/// Applies `traffic_class` to a socket bound in the given family.
///
/// `is_ipv4` is the *listener's* family, not the peer's.
pub(crate) fn set_reply_traffic_class(
    socket: SockRef<'_>,
    is_ipv4: bool,
    traffic_class: u8,
) -> io::Result<()> {
    let traffic_class = u32::from(traffic_class);
    if is_ipv4 {
        set_ipv4_traffic_class(socket, traffic_class)
    } else {
        set_ipv6_traffic_class(socket, traffic_class)
    }
}

#[cfg(not(any(
    target_os = "fuchsia",
    target_os = "redox",
    target_os = "solaris",
    target_os = "haiku",
    target_os = "wasi",
)))]
fn set_ipv4_traffic_class(socket: SockRef<'_>, traffic_class: u32) -> io::Result<()> {
    socket.set_tos_v4(traffic_class)
}

#[cfg(any(
    target_os = "fuchsia",
    target_os = "redox",
    target_os = "solaris",
    target_os = "haiku",
    target_os = "wasi",
))]
fn set_ipv4_traffic_class(_socket: SockRef<'_>, traffic_class: u32) -> io::Result<()> {
    unsupported_traffic_class(traffic_class, "IPv4 TOS socket options")
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
    target_os = "illumos",
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
    target_os = "illumos",
)))]
fn set_ipv6_traffic_class(_socket: SockRef<'_>, traffic_class: u32) -> io::Result<()> {
    unsupported_traffic_class(traffic_class, "IPv6 Traffic Class socket options")
}

/// A target where the safe API cannot express this option.
///
/// This reports [`io::ErrorKind::Unsupported`] for *every* value, zero
/// included. Returning success for zero would claim the socket had been cleared
/// when nothing was written to it, which is not the same statement — a caller
/// handing [`Server::from_socket`](crate::Server::from_socket) an already
/// prepared socket may have marked it by some other means. Whether an
/// unappliable zero is nevertheless safe to send is the runtime's decision, and
/// it is made from what this server itself has applied; this helper's job is
/// only to report honestly.
#[cfg(any(
    target_os = "fuchsia",
    target_os = "redox",
    target_os = "solaris",
    target_os = "haiku",
    target_os = "wasi",
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
        target_os = "illumos",
    )),
))]
fn unsupported_traffic_class(_traffic_class: u32, option: &'static str) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        format!("{option} are unsupported on this target"),
    ))
}
