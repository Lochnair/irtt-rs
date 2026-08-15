//! Ancillary receive data on Linux, macOS and FreeBSD.
//!
//! Two independent kinds of metadata travel with a received datagram here, and
//! they are not equally important:
//!
//! - the request's **local destination address**, which a wildcard listener
//!   must have to answer from the endpoint the client contacted. Losing it
//!   makes the request unanswerable, so the datagram is dropped.
//! - the kernel's **software receive timestamp**, an accuracy enhancement that
//!   Linux listeners request opportunistically. Losing it costs nothing but
//!   precision, so the datagram is served with no timestamp.
//!
//! Both are collected from one `recvmsg`, and neither is looked for at a fixed
//! position: the kernel may return them in either order, so the whole usable
//! control sequence is walked and each kind recorded where it appears.
//!
//! Three platforms, two destination mechanisms per address family, all reached
//! through `nix`'s safe wrappers:
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
//! On Linux the kernel receive timestamp arrives as `SCM_TIMESTAMPNS`, enabled
//! by `SO_TIMESTAMPNS`. It is a software timestamp taken by the kernel's
//! realtime clock as the datagram is received — not a NIC hardware timestamp,
//! not a monotonic reading, and nothing to do with transmission.
//!
//! Both directions run nonblocking through Tokio readiness, following the same
//! `readable()`/`try_io` shape the client's ancillary receive path uses. One
//! socket, one task, no blocking call and no spawned thread.

use std::{
    io::{self, IoSlice, IoSliceMut},
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6},
    os::fd::AsRawFd,
    time::SystemTime,
};

use nix::{
    libc,
    sys::socket::{
        cmsg_space, recvmsg, sendmsg, setsockopt, sockopt, ControlMessage, ControlMessageOwned,
        MsgFlags, RecvMsg, SockaddrStorage,
    },
};
use tokio::{io::Interest, net::UdpSocket};

#[cfg(target_os = "linux")]
use std::time::{Duration, UNIX_EPOCH};

#[cfg(target_os = "linux")]
use nix::sys::time::TimeSpec;

use super::{ReceivedDatagram, ReplySource};

/// Room for the IPv4 destination control message, whose payload is the
/// platform's own: Linux reports an `in_pktinfo`, the BSDs a bare `in_addr`.
#[cfg(target_os = "linux")]
const DESTINATION_V4_LEN: usize = cmsg_space::<libc::in_pktinfo>();
#[cfg(any(target_os = "macos", target_os = "freebsd"))]
const DESTINATION_V4_LEN: usize = cmsg_space::<libc::in_addr>();

/// Room for the IPv6 destination control message, which is an `in6_pktinfo`
/// everywhere here.
const DESTINATION_V6_LEN: usize = cmsg_space::<libc::in6_pktinfo>();

/// Room for the kernel receive timestamp, which only Linux is asked for.
#[cfg(target_os = "linux")]
const TIMESTAMP_LEN: usize = cmsg_space::<libc::timespec>();
#[cfg(not(target_os = "linux"))]
const TIMESTAMP_LEN: usize = 0;

/// Capacity for every control message this server asks the kernel for, derived
/// from the payload types themselves rather than from a round number believed
/// to be large enough.
///
/// Both address families are counted although one socket only ever receives
/// one of them: a few bytes of headroom is cheaper than a per-family buffer
/// type, and a dual-stack listener's family is a runtime fact. Undersizing this
/// would not fail loudly — it would set `MSG_CTRUNC` and turn valid wildcard
/// requests into silent drops — so the sum is stated in full and proved by a
/// receive test rather than by inspection.
const CONTROL_LEN: usize = DESTINATION_V4_LEN + DESTINATION_V6_LEN + TIMESTAMP_LEN;

/// Whichever family a socket turns out to be, its destination message and the
/// timestamp must fit together. Checked at compile time on every target this
/// module builds for, so a future control message that outgrows the buffer
/// fails the build rather than the wildcard listeners that depend on it.
const _: () = {
    assert!(CONTROL_LEN >= DESTINATION_V4_LEN + TIMESTAMP_LEN);
    assert!(CONTROL_LEN >= DESTINATION_V6_LEN + TIMESTAMP_LEN);
};

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

/// Asks the kernel for a software receive timestamp on each datagram, and
/// reports whether it agreed.
///
/// Deliberately infallible for the caller. Unlike destination metadata, this is
/// an accuracy enhancement and not a correctness requirement: a listener that
/// does not get it answers every request exactly as it otherwise would, just
/// without the kernel's view of when each one arrived.
#[cfg(target_os = "linux")]
pub(super) fn try_configure_kernel_rx_timestamp(socket: &UdpSocket) -> bool {
    setsockopt(socket, sockopt::ReceiveTimestampns, &true).is_ok()
}

pub(super) async fn receive_with_destination(
    socket: &UdpSocket,
    buffer: &mut [u8],
) -> io::Result<Option<ReceivedDatagram>> {
    receive_ancillary(socket, buffer, true).await
}

/// The Linux explicit-address receive: no destination metadata is needed or
/// asked for, and the datagram is taken through `recvmsg` only so an enabled
/// `SCM_TIMESTAMPNS` can be read off it.
#[cfg(target_os = "linux")]
pub(super) async fn receive_bound(
    socket: &UdpSocket,
    buffer: &mut [u8],
) -> io::Result<Option<ReceivedDatagram>> {
    receive_ancillary(socket, buffer, false).await
}

/// `require_destination` is the listener's own wildcard-ness. It decides both
/// what this receive must recover and how severe a truncated control buffer is.
async fn receive_ancillary(
    socket: &UdpSocket,
    buffer: &mut [u8],
    require_destination: bool,
) -> io::Result<Option<ReceivedDatagram>> {
    loop {
        socket.readable().await?;
        match socket.try_io(Interest::READABLE, || {
            receive_once(socket, buffer, require_destination)
        }) {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
            result => return result,
        }
    }
}

fn receive_once(
    socket: &UdpSocket,
    buffer: &mut [u8],
    require_destination: bool,
) -> io::Result<Option<ReceivedDatagram>> {
    let mut control = ControlBuffer::new();
    let mut iov = [IoSliceMut::new(buffer)];
    let message = recvmsg::<SockaddrStorage>(
        socket.as_raw_fd(),
        &mut iov,
        Some(control.as_mut_slice()),
        MsgFlags::MSG_DONTWAIT,
    )?;

    Ok(received_datagram(&message, require_destination))
}

/// Turns one received message into a datagram the runtime may serve, or `None`
/// for one it must drop.
///
/// A wildcard listener promised to answer from the address the request was sent
/// to. If the payload was truncated, if the control buffer was truncated, or if
/// no destination arrived with the datagram, that promise cannot be kept for
/// this request — so it is dropped here, before the core sees it and before any
/// session's receive, rate or lifetime state could move.
///
/// A kernel receive timestamp is never part of that judgement. Absent,
/// truncated away or structurally unrepresentable, it simply does not appear on
/// the datagram.
fn received_datagram(
    message: &RecvMsg<'_, '_, SockaddrStorage>,
    require_destination: bool,
) -> Option<ReceivedDatagram> {
    if is_unusable(message.flags, require_destination) {
        return None;
    }

    let metadata = receive_metadata(message);
    Some(ReceivedDatagram {
        len: message.bytes,
        peer: peer_endpoint(message.address.as_ref()?)?,
        reply_source: resolved_reply_source(metadata.reply_source, require_destination)?,
        kernel_rx_timestamp: metadata.kernel_rx_timestamp,
    })
}

/// Whether the kernel's own report of this receive rules the datagram out.
///
/// A truncated payload always does: what arrived is not what was sent. A
/// truncated *control* buffer only does where the destination was required,
/// because then the metadata that may have been cut is the metadata the reply
/// cannot be built without. For a listener that asked only for a timestamp,
/// the same flag means at worst that the timestamp was lost — a reason to serve
/// the request without one, not to discard a client's packet.
fn is_unusable(flags: MsgFlags, require_destination: bool) -> bool {
    flags.contains(MsgFlags::MSG_TRUNC)
        || (require_destination && flags.contains(MsgFlags::MSG_CTRUNC))
}

/// Where this datagram's reply must leave from, given what the control data
/// yielded.
///
/// An explicit-address listener has nothing to recover: it sends from the one
/// address it is bound to. A wildcard listener has everything to recover, and a
/// request whose destination did not arrive is one it cannot answer.
fn resolved_reply_source(
    recovered: Option<ReplySource>,
    require_destination: bool,
) -> Option<ReplySource> {
    if require_destination {
        recovered
    } else {
        Some(ReplySource::Bound)
    }
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

/// What one receive's control data carried, kind by kind.
///
/// Independent fields rather than an early return, because the kernel is free
/// to order control messages as it likes: stopping at the destination would
/// discard a timestamp that followed it, and stopping at the timestamp would
/// discard the destination a wildcard reply depends on.
#[derive(Default)]
struct ReceiveMetadata {
    reply_source: Option<ReplySource>,
    /// Only Linux is ever asked for one, so on the BSDs this stays absent and
    /// the datagram simply carries no kernel view of its arrival.
    kernel_rx_timestamp: Option<SystemTime>,
}

fn receive_metadata(message: &RecvMsg<'_, '_, SockaddrStorage>) -> ReceiveMetadata {
    let mut metadata = ReceiveMetadata::default();
    // `cmsgs` refuses to walk a control buffer the kernel had to truncate
    // (`MSG_CTRUNC`), because what is there cannot be trusted to be complete
    // messages. Whether that is fatal was already decided by `is_unusable`;
    // here it just means no metadata was recovered.
    let Ok(control_messages) = message.cmsgs() else {
        return metadata;
    };

    for control_message in control_messages {
        match control_message {
            // Linux reports the header destination in `ipi_addr`; `ipi_spec_dst`
            // is the route's local address, which is the same for the unicast
            // traffic a server sees and is not what the client addressed.
            #[cfg(target_os = "linux")]
            ControlMessageOwned::Ipv4PacketInfo(info) => {
                metadata.reply_source = ipv4_reply_source(ipv4_addr(info.ipi_addr));
            }
            #[cfg(any(target_os = "macos", target_os = "freebsd"))]
            ControlMessageOwned::Ipv4RecvDstAddr(address) => {
                metadata.reply_source = ipv4_reply_source(ipv4_addr(address));
            }
            ControlMessageOwned::Ipv6PacketInfo(info) => {
                metadata.reply_source = ipv6_reply_source(info);
            }
            #[cfg(target_os = "linux")]
            ControlMessageOwned::ScmTimestampns(timestamp) => {
                metadata.kernel_rx_timestamp = system_time_from_timespec(timestamp);
            }
            _ => {}
        }
    }
    metadata
}

/// An unspecified destination is no destination: nothing can be sent from `::`,
/// so the datagram is dropped rather than answered from a source the kernel
/// would pick instead.
fn ipv6_reply_source(info: libc::in6_pktinfo) -> Option<ReplySource> {
    let address = Ipv6Addr::from(info.ipi6_addr.s6_addr);
    (!address.is_unspecified()).then_some(ReplySource::V6 {
        address,
        interface_index: info.ipi6_ifindex,
    })
}

/// A kernel `SCM_TIMESTAMPNS` reading as a wall-clock instant, or `None` where
/// it is not one.
///
/// Purely structural: a negative or out-of-range `timespec`, or one past what
/// `SystemTime` can represent, describes no instant and is reported as absent
/// metadata. Whether a representable reading is *plausible* — close enough to
/// the server's own userspace sample to be worth measuring against — is a
/// question for whoever eventually measures with it, and is deliberately not
/// asked here.
#[cfg(target_os = "linux")]
fn system_time_from_timespec(timespec: TimeSpec) -> Option<SystemTime> {
    let seconds = u64::try_from(timespec.tv_sec()).ok()?;
    let nanos = u32::try_from(timespec.tv_nsec()).ok()?;
    if nanos >= 1_000_000_000 {
        return None;
    }
    UNIX_EPOCH.checked_add(Duration::new(seconds, nanos))
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
            // For a send, `in6_pktinfo` names the source address and, where it
            // is needed, the interface to leave by.
            let info = libc::in6_pktinfo {
                ipi6_addr: libc::in6_addr {
                    s6_addr: address.octets(),
                },
                ipi6_ifindex: egress_interface_index(address, interface_index),
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

/// The interface a reply must leave by, which is usually none of our business.
///
/// A link-local source address is not unique across a host's interfaces, so a
/// reply carrying one has to name the interface it belongs to. Every other
/// source — global, unique-local, loopback, IPv4-mapped — identifies itself
/// without help.
///
/// Pinning the arrival interface for those would only add a way for a reply to
/// fail: on a host whose route back to the peer leaves by a different interface
/// than the request arrived on, forcing the ingress interface can make the send
/// fail outright, losing a reply whose source address was perfectly valid. What
/// the protocol requires is the source *address*; the route to the peer is the
/// routing table's business. The IPv4 path makes the same choice for the same
/// reason.
fn egress_interface_index(address: Ipv6Addr, interface_index: u32) -> u32 {
    if address.is_unicast_link_local() {
        interface_index
    } else {
        0
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Which reply sources need their interface named, which no normal
    /// interface can show: it takes a host whose route back to a peer leaves by
    /// a different interface than the request arrived on, and a test host has
    /// one loopback.
    #[test]
    fn only_a_link_local_source_pins_the_egress_interface() {
        const ARRIVED_ON: u32 = 7;

        for (address, expected, why) in [
            (
                "fe80::1",
                ARRIVED_ON,
                "a link-local source needs its link named",
            ),
            (
                "fe80::7858:baff:fe91:183d",
                ARRIVED_ON,
                "and so does any other one",
            ),
            ("2001:db8::1", 0, "a global source identifies itself"),
            ("fd00::1", 0, "so does a unique-local one"),
            ("::1", 0, "and loopback"),
            ("::ffff:127.0.0.2", 0, "and an IPv4-mapped destination"),
        ] {
            assert_eq!(
                egress_interface_index(address.parse().unwrap(), ARRIVED_ON),
                expected,
                "{address}: {why}"
            );
        }
    }

    /// What the two listener kinds do with the metadata they did or did not
    /// get, which is the whole distinction this module rests on: a destination
    /// is correctness-critical for one of them and irrelevant to the other.
    #[test]
    fn only_a_listener_that_needs_a_destination_is_stopped_by_a_missing_one() {
        let recovered = ReplySource::V4 {
            address: Ipv4Addr::LOCALHOST,
        };

        assert_eq!(
            resolved_reply_source(Some(recovered), true),
            Some(recovered),
            "a wildcard listener replies from the destination it recovered"
        );
        assert_eq!(
            resolved_reply_source(None, true),
            None,
            "and cannot answer a request whose destination did not arrive"
        );
        assert_eq!(
            resolved_reply_source(None, false),
            Some(ReplySource::Bound),
            "an explicit listener never needed one"
        );
        assert_eq!(
            resolved_reply_source(Some(recovered), false),
            Some(ReplySource::Bound),
            "and sends from its bind even if one turned up"
        );
    }

    /// Which truncation is fatal to whom. A cut payload always is; cut control
    /// data is fatal only where it may have taken the destination with it.
    /// Turning a lost *timestamp* into a lost packet would trade a real client
    /// request for optional precision.
    #[test]
    fn control_truncation_only_condemns_a_datagram_whose_destination_it_may_have_taken() {
        for (flags, require_destination, unusable, why) in [
            (MsgFlags::empty(), true, false, "a clean wildcard receive"),
            (MsgFlags::empty(), false, false, "a clean explicit receive"),
            (
                MsgFlags::MSG_TRUNC,
                false,
                true,
                "a cut payload is not what the client sent",
            ),
            (
                MsgFlags::MSG_TRUNC,
                true,
                true,
                "and no less so for a wildcard listener",
            ),
            (
                MsgFlags::MSG_CTRUNC,
                true,
                true,
                "cut control data may have taken the destination",
            ),
            (
                MsgFlags::MSG_CTRUNC,
                false,
                false,
                "but for an explicit listener it can only have cost a timestamp",
            ),
            (
                MsgFlags::MSG_TRUNC | MsgFlags::MSG_CTRUNC,
                false,
                true,
                "the payload rule still applies",
            ),
        ] {
            assert_eq!(is_unusable(flags, require_destination), unusable, "{why}");
        }
    }

    /// A representable kernel reading becomes an instant; anything else becomes
    /// absent metadata. Note what is *not* rejected here: a reading far from
    /// now is structurally fine, and judging it belongs to whoever measures
    /// with it.
    #[cfg(target_os = "linux")]
    #[cfg_attr(target_env = "musl", allow(deprecated))]
    #[test]
    fn a_kernel_timestamp_converts_only_where_it_describes_an_instant() {
        assert_eq!(
            system_time_from_timespec(TimeSpec::new(1, 2))
                .unwrap()
                .duration_since(UNIX_EPOCH)
                .unwrap(),
            Duration::new(1, 2)
        );
        assert_eq!(
            system_time_from_timespec(TimeSpec::new(0, 0)),
            Some(UNIX_EPOCH),
            "the epoch itself is representable"
        );

        assert_eq!(system_time_from_timespec(TimeSpec::new(-1, 0)), None);
        assert_eq!(system_time_from_timespec(TimeSpec::new(0, -1)), None);
        assert_eq!(
            system_time_from_timespec(TimeSpec::new(0, 1_000_000_000)),
            None
        );
        assert_eq!(
            system_time_from_timespec(TimeSpec::new(libc::time_t::MIN, libc::c_long::MIN)),
            None,
            "and the extremes convert rather than panic"
        );
        let _ = system_time_from_timespec(TimeSpec::new(libc::time_t::MAX, 999_999_999));
    }
}
