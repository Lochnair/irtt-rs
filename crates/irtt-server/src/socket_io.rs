//! Receiving a request's local destination and replying from it.
//!
//! A reply must leave from the exact address and port the request was addressed
//! to, because clients read from connected UDP sockets and discard anything
//! arriving from another endpoint. A listener bound to an explicit address
//! satisfies that for free: every datagram it sends carries the address it is
//! bound to, so ordinary `recv_from`/`send_to` is all it needs.
//!
//! A wildcard listener does not. On a multi-homed host the routing table
//! chooses the source address, so a request to one of the host's addresses can
//! be answered from another, and the client drops the reply. Such a listener
//! therefore asks the kernel for each request's destination address as
//! ancillary data and hands it back as the source of that request's reply.
//!
//! Which mechanism carries that is per-platform, and only [`SUPPORTED`] targets
//! have one reachable through a safe API. Everything here goes through `nix`'s
//! safe `recvmsg`/`sendmsg` wrappers; the crate forbids unsafe code, so there is
//! no raw syscall, no `CMSG_*` walking and no hand-written ABI layer.

use std::{io, net::SocketAddr};

use tokio::net::UdpSocket;

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "freebsd"))]
mod ancillary;

/// Whether this target can pin a wildcard listener's reply source address.
///
/// A wildcard bind is refused where this is false, rather than served by a
/// listener whose replies may leave from an address the client never contacted.
pub(crate) const SUPPORTED: bool = cfg!(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "freebsd"
));

/// Where one reply must leave from.
///
/// This is transport state belonging to a single datagram. It never reaches
/// [`ServerCore`](crate::ServerCore), which knows peers and sessions and has no
/// business knowing which of the host's addresses a request arrived on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplySource {
    /// Whatever the socket is bound to: an explicit-address listener already
    /// sends from the one address it can.
    Bound,
    /// The IPv4 address this request was addressed to.
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "freebsd"))]
    V4 { address: std::net::Ipv4Addr },
    /// The IPv6 address this request was addressed to, and the interface it
    /// arrived on. The interface is part of the identity of a *scoped* address,
    /// so it is carried rather than rediscovered — and it is used on the send
    /// only where the source address is one, so that a reply is never forced
    /// out of an interface the route to the peer does not use.
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "freebsd"))]
    V6 {
        address: std::net::Ipv6Addr,
        interface_index: u32,
    },
}

/// One received datagram and what answering it requires.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReceivedDatagram {
    pub(crate) len: usize,
    pub(crate) peer: SocketAddr,
    pub(crate) reply_source: ReplySource,
}

/// Asks the kernel to report each datagram's local destination address.
///
/// Called once, at construction, and only for a wildcard listener: an
/// explicit-address listener needs no ancillary metadata, and adding it would
/// widen the platform surface of the common path for nothing.
#[cfg(any(target_os = "linux", target_os = "macos", target_os = "freebsd"))]
pub(crate) fn configure_destination_metadata(socket: &UdpSocket, is_ipv4: bool) -> io::Result<()> {
    ancillary::configure_destination_metadata(socket, is_ipv4)
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "freebsd")))]
pub(crate) fn configure_destination_metadata(
    _socket: &UdpSocket,
    _is_ipv4: bool,
) -> io::Result<()> {
    Err(unsupported())
}

/// Receives one datagram, reporting where its reply must come from.
///
/// `select_reply_source` is the listener's own wildcard-ness, decided once at
/// construction. It is never inferred from a peer address.
///
/// `Ok(None)` is a datagram the runtime must drop without letting it reach the
/// core: it was truncated, its ancillary metadata was truncated, or a wildcard
/// listener could not recover its local destination. Answering it correctly is
/// impossible, so it must not advance any session's state either.
///
/// Cancel-safe: nothing is consumed until a datagram is fully received.
pub(crate) async fn receive(
    socket: &UdpSocket,
    buffer: &mut [u8],
    select_reply_source: bool,
) -> io::Result<Option<ReceivedDatagram>> {
    if select_reply_source {
        return receive_with_destination(socket, buffer).await;
    }

    let (len, peer) = socket.recv_from(buffer).await?;
    Ok(Some(ReceivedDatagram {
        len,
        peer,
        reply_source: ReplySource::Bound,
    }))
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "freebsd"))]
async fn receive_with_destination(
    socket: &UdpSocket,
    buffer: &mut [u8],
) -> io::Result<Option<ReceivedDatagram>> {
    ancillary::receive_with_destination(socket, buffer).await
}

/// Unreachable in practice: a wildcard bind is refused at construction here, so
/// no listener on this target ever asks for source selection.
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "freebsd")))]
async fn receive_with_destination(
    _socket: &UdpSocket,
    _buffer: &mut [u8],
) -> io::Result<Option<ReceivedDatagram>> {
    Err(unsupported())
}

/// Sends one reply to `peer` from `reply_source`.
///
/// A short send is reported as such and is the caller's to treat as the loss of
/// that one reply, exactly as an ordinary `send_to` short send is.
pub(crate) async fn send(
    socket: &UdpSocket,
    bytes: &[u8],
    peer: SocketAddr,
    reply_source: ReplySource,
) -> io::Result<usize> {
    match reply_source {
        ReplySource::Bound => socket.send_to(bytes, peer).await,
        #[cfg(any(target_os = "linux", target_os = "macos", target_os = "freebsd"))]
        selected => ancillary::send_from_source(socket, bytes, peer, selected).await,
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "freebsd")))]
fn unsupported() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "reply source-address selection is unsupported on this target",
    )
}
