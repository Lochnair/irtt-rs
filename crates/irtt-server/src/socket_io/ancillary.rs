//! Destination-address ancillary data on Linux, macOS and FreeBSD.
//!
//! Three platforms, two mechanisms per address family, all reached through
//! `nix`'s safe wrappers:
//!
//! | | receive | send |
//! |---|---|---|
//! | Linux IPv4 | `IP_PKTINFO` | `IP_PKTINFO` |
//! | Linux IPv6 | `IPV6_RECVPKTINFO` | `IPV6_PKTINFO` |
//! | macOS IPv4 | `IP_RECVDSTADDR` | `IP_PKTINFO` |
//! | macOS IPv6 | `IPV6_RECVPKTINFO` | `IPV6_PKTINFO` |
//! | FreeBSD IPv4 | `IP_RECVDSTADDR` | `IP_SENDSRCADDR` |
//! | FreeBSD IPv6 | `IPV6_RECVPKTINFO` | `IPV6_PKTINFO` |
//!
//! The BSD IPv4 options are not Linux's under another name, and the receive
//! structure is not the send structure: `in_pktinfo` carries three fields on
//! receive of which only two mean anything on send. Each direction is built
//! explicitly for that reason.
//!
//! Both directions run nonblocking through Tokio readiness, following the same
//! `readable()`/`try_io` shape the client's ancillary receive path uses. One
//! socket, one task, no blocking call and no spawned thread.

use std::{
    io::{self, IoSlice, IoSliceMut},
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6},
    os::fd::AsRawFd,
};

use nix::{
    libc,
    sys::socket::{
        recvmsg, sendmsg, setsockopt, sockopt, ControlMessage, ControlMessageOwned, MsgFlags,
        RecvMsg, SockaddrStorage,
    },
};
use tokio::{io::Interest, net::UdpSocket};

use super::{ReceivedDatagram, ReplySource};

/// Room for the destination control message. The largest of them is an
/// `in6_pktinfo`, and this leaves headroom for a second message the kernel may
/// add, so `MSG_CTRUNC` reflects a real problem rather than a tight buffer.
const CONTROL_LEN: usize = 128;

pub(super) fn configure_destination_metadata(socket: &UdpSocket, is_ipv4: bool) -> io::Result<()> {
    if is_ipv4 {
        configure_ipv4_destination(socket)
    } else {
        // A dual-stack wildcard socket reports IPv4-mapped destinations through
        // this option too, so the IPv6 listener needs only this one.
        setsockopt(socket, sockopt::Ipv6RecvPacketInfo, &true).map_err(io::Error::from)
    }
}

#[cfg(target_os = "linux")]
fn configure_ipv4_destination(socket: &UdpSocket) -> io::Result<()> {
    setsockopt(socket, sockopt::Ipv4PacketInfo, &true).map_err(io::Error::from)
}

#[cfg(any(target_os = "macos", target_os = "freebsd"))]
fn configure_ipv4_destination(socket: &UdpSocket) -> io::Result<()> {
    setsockopt(socket, sockopt::Ipv4RecvDstAddr, &true).map_err(io::Error::from)
}

pub(super) async fn receive_with_destination(
    socket: &UdpSocket,
    buffer: &mut [u8],
) -> io::Result<Option<ReceivedDatagram>> {
    loop {
        socket.readable().await?;
        match socket.try_io(Interest::READABLE, || receive_once(socket, buffer)) {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
            result => return result,
        }
    }
}

fn receive_once(socket: &UdpSocket, buffer: &mut [u8]) -> io::Result<Option<ReceivedDatagram>> {
    let mut control = ControlBuffer::new();
    let mut iov = [IoSliceMut::new(buffer)];
    let message = recvmsg::<SockaddrStorage>(
        socket.as_raw_fd(),
        &mut iov,
        Some(control.as_mut_slice()),
        MsgFlags::MSG_DONTWAIT,
    )?;

    Ok(received_datagram(&message))
}

/// Turns one received message into a datagram the runtime may serve, or `None`
/// for one it must drop.
///
/// A wildcard listener promised to answer from the address the request was sent
/// to. If the payload was truncated, if the control buffer was truncated, or if
/// no destination arrived with the datagram, that promise cannot be kept for
/// this request — so it is dropped here, before the core sees it and before any
/// session's receive, rate or lifetime state could move.
fn received_datagram(message: &RecvMsg<'_, '_, SockaddrStorage>) -> Option<ReceivedDatagram> {
    if message
        .flags
        .intersects(MsgFlags::MSG_TRUNC | MsgFlags::MSG_CTRUNC)
    {
        return None;
    }

    Some(ReceivedDatagram {
        len: message.bytes,
        peer: peer_endpoint(message.address.as_ref()?)?,
        reply_source: reply_source(message)?,
    })
}

/// The request's full source endpoint, keeping the IPv6 zone the core compares
/// endpoints by.
fn peer_endpoint(address: &SockaddrStorage) -> Option<SocketAddr> {
    if let Some(peer) = address.as_sockaddr_in() {
        return Some(SocketAddr::from(*peer));
    }
    address
        .as_sockaddr_in6()
        .map(|peer| SocketAddr::from(*peer))
}

fn reply_source(message: &RecvMsg<'_, '_, SockaddrStorage>) -> Option<ReplySource> {
    for control_message in message.cmsgs().ok()? {
        match control_message {
            // Linux reports the header destination in `ipi_addr`; `ipi_spec_dst`
            // is the route's local address, which is the same for the unicast
            // traffic a server sees and is not what the client addressed.
            #[cfg(target_os = "linux")]
            ControlMessageOwned::Ipv4PacketInfo(info) => {
                return ipv4_reply_source(ipv4_addr(info.ipi_addr));
            }
            #[cfg(any(target_os = "macos", target_os = "freebsd"))]
            ControlMessageOwned::Ipv4RecvDstAddr(address) => {
                return ipv4_reply_source(ipv4_addr(address));
            }
            ControlMessageOwned::Ipv6PacketInfo(info) => {
                let address = Ipv6Addr::from(info.ipi6_addr.s6_addr);
                if address.is_unspecified() {
                    return None;
                }
                return Some(ReplySource::V6 {
                    address,
                    interface_index: info.ipi6_ifindex,
                });
            }
            _ => {}
        }
    }
    None
}

/// An unspecified destination is no destination: nothing can be sent from
/// `0.0.0.0`, so the datagram is dropped rather than answered from a source the
/// kernel would pick instead.
fn ipv4_reply_source(address: Ipv4Addr) -> Option<ReplySource> {
    (!address.is_unspecified()).then_some(ReplySource::V4 { address })
}

fn ipv4_addr(address: libc::in_addr) -> Ipv4Addr {
    Ipv4Addr::from(address.s_addr.to_ne_bytes())
}

fn in_addr(address: Ipv4Addr) -> libc::in_addr {
    libc::in_addr {
        s_addr: u32::from_ne_bytes(address.octets()),
    }
}

pub(super) async fn send_from_source(
    socket: &UdpSocket,
    bytes: &[u8],
    peer: SocketAddr,
    reply_source: ReplySource,
) -> io::Result<usize> {
    loop {
        socket.writable().await?;
        match socket.try_io(Interest::WRITABLE, || {
            send_once(socket, bytes, peer, reply_source)
        }) {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
            result => return result,
        }
    }
}

fn send_once(
    socket: &UdpSocket,
    bytes: &[u8],
    peer: SocketAddr,
    reply_source: ReplySource,
) -> io::Result<usize> {
    let destination = SockaddrStorage::from(peer);
    let iov = [IoSlice::new(bytes)];

    let sent = match reply_source {
        ReplySource::Bound => sendmsg(
            socket.as_raw_fd(),
            &iov,
            &[],
            MsgFlags::MSG_DONTWAIT,
            Some(&destination),
        )?,
        ReplySource::V4 { address } => send_from_ipv4(socket, &iov, &destination, address)?,
        ReplySource::V6 {
            address,
            interface_index,
        } => {
            // For a send, `in6_pktinfo` names the source address and the
            // interface to leave by — the interface being what makes a scoped
            // address unambiguous.
            let info = libc::in6_pktinfo {
                ipi6_addr: libc::in6_addr {
                    s6_addr: address.octets(),
                },
                ipi6_ifindex: interface_index,
            };
            // The destination has to be stated in the same family the control
            // message is: an IPv6 source with an `AF_INET` destination is a
            // rejected send, not a mixed-family one. The two can disagree —
            // macOS reports an IPv4 peer for a listener bound to the IPv4-mapped
            // wildcard while still delivering IPv6 packet info for it — and the
            // peer identity the core sees stays exactly as it was received.
            let destination = SockaddrStorage::from(as_ipv6_endpoint(peer));
            sendmsg(
                socket.as_raw_fd(),
                &iov,
                &[ControlMessage::Ipv6PacketInfo(&info)],
                MsgFlags::MSG_DONTWAIT,
                Some(&destination),
            )?
        }
    };

    Ok(sent)
}

/// The same endpoint expressed in IPv6, mapping an IPv4 peer.
///
/// A datagram sent to an IPv4-mapped destination from an `AF_INET6` socket goes
/// out as ordinary IPv4 to that address; this changes how the endpoint is
/// spelled for one `sendmsg`, not what reaches the client.
fn as_ipv6_endpoint(peer: SocketAddr) -> SocketAddrV6 {
    match peer {
        SocketAddr::V6(peer) => peer,
        SocketAddr::V4(peer) => SocketAddrV6::new(peer.ip().to_ipv6_mapped(), peer.port(), 0, 0),
    }
}

/// `IP_PKTINFO` on a send reads only `ipi_spec_dst`, the source address, and
/// `ipi_ifindex`. `ipi_addr` is a receive-only field and is left zero.
///
/// The interface is left unset deliberately. What the protocol requires is the
/// source *address*; pinning the arrival interface as well would add a way for
/// a reply to fail on a host whose return path is not the one the request came
/// in on, and buy nothing.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn send_from_ipv4(
    socket: &UdpSocket,
    iov: &[IoSlice<'_>],
    destination: &SockaddrStorage,
    address: Ipv4Addr,
) -> io::Result<usize> {
    let info = libc::in_pktinfo {
        ipi_ifindex: 0,
        ipi_spec_dst: in_addr(address),
        ipi_addr: in_addr(Ipv4Addr::UNSPECIFIED),
    };

    sendmsg(
        socket.as_raw_fd(),
        iov,
        &[ControlMessage::Ipv4PacketInfo(&info)],
        MsgFlags::MSG_DONTWAIT,
        Some(destination),
    )
    .map_err(io::Error::from)
}

/// FreeBSD has no send-side `IP_PKTINFO`; `IP_SENDSRCADDR` carries the source
/// address on its own.
#[cfg(target_os = "freebsd")]
fn send_from_ipv4(
    socket: &UdpSocket,
    iov: &[IoSlice<'_>],
    destination: &SockaddrStorage,
    address: Ipv4Addr,
) -> io::Result<usize> {
    let source = in_addr(address);

    sendmsg(
        socket.as_raw_fd(),
        iov,
        &[ControlMessage::Ipv4SendSrcAddr(&source)],
        MsgFlags::MSG_DONTWAIT,
        Some(destination),
    )
    .map_err(io::Error::from)
}

/// Per-receive control buffer. Control data must satisfy `cmsghdr` alignment,
/// which 8-byte alignment covers on these targets. It lives on the stack for
/// the duration of one receive; nothing is allocated per datagram.
#[repr(align(8))]
struct ControlBuffer([u8; CONTROL_LEN]);

impl ControlBuffer {
    fn new() -> Self {
        Self([0; CONTROL_LEN])
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.0
    }
}
