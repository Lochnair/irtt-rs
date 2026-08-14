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

use std::{
    io,
    net::{IpAddr, SocketAddr},
};

use socket2::SockRef;

/// Whether this listener's replies are marked with the IPv4 TOS option rather
/// than the IPv6 Traffic Class one.
///
/// The listener's own bound family answers this, not the peer's — one socket,
/// one answer. The exception is an IPv4-mapped bound address, where the socket
/// is `AF_INET6` but every packet it emits is IPv4, and the platforms disagree
/// about it:
///
/// - **Linux** binds such a socket to the IPv4 side. `IPV6_TCLASS` does not
///   reach the packets it sends — their TOS stays zero and a negotiated marking
///   is silently lost — while `IP_TOS` on that same socket marks them. So a
///   mapped bind is read as the IPv4 listener it is.
/// - **macOS** rejects `IP_TOS` on an `AF_INET6` socket with `EINVAL` whatever
///   it is bound to, and marks nothing through either option. Reading a mapped
///   bind as IPv4 there would turn an unmarked reply into a *dropped* one, since
///   an unappliable marking stops the send — worse than the marking simply not
///   arriving. It keeps the IPv6 option and its existing behavior. (The mapped
///   *wildcard* never reaches this at all: macOS normalizes that bind to `[::]`.)
/// - **FreeBSD** refuses a mapped bind under its default `IPV6_V6ONLY`, so the
///   case does not arise.
pub(crate) fn marks_with_ipv4_option(addr: SocketAddr) -> bool {
    match addr.ip() {
        IpAddr::V4(_) => true,
        IpAddr::V6(address) => cfg!(target_os = "linux") && address.to_ipv4_mapped().is_some(),
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    /// Which listeners mark with the IPv4 option.
    ///
    /// The mapped forms are the point: such a socket is `AF_INET6` but sends
    /// IPv4, and `IPV6_TCLASS` does not reach those packets. Linux marks them
    /// through `IP_TOS`; the platforms that would reject that keep the IPv6
    /// option, because an unappliable marking stops the send and a dropped
    /// reply is worse than an unmarked one.
    #[test]
    fn a_mapped_listener_marks_with_the_ipv4_option_where_that_option_works() {
        let mapped = cfg!(target_os = "linux");

        for (addr, is_ipv4) in [
            ("0.0.0.0:2112", true),
            ("127.0.0.1:2112", true),
            ("[::ffff:0.0.0.0]:2112", mapped),
            ("[::ffff:127.0.0.1]:2112", mapped),
            ("[::]:2112", false),
            ("[::1]:2112", false),
            ("[2001:db8::1]:2112", false),
        ] {
            assert_eq!(
                marks_with_ipv4_option(addr.parse().unwrap()),
                is_ipv4,
                "{addr} would mark with the wrong option"
            );
        }
    }
}
