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
//!
//! # Kernel receive timestamps
//!
//! A Linux listener additionally asks the kernel for a software receive
//! timestamp (`SO_TIMESTAMPNS`) on every datagram, wildcard or not, and reports
//! it on [`ReceivedDatagram::kernel_rx_timestamp`]. It says when the kernel
//! received the datagram, before the socket queue, Tokio readiness and
//! scheduling that separate that moment from the one the server would otherwise
//! observe.
//!
//! It is the exact opposite of destination metadata in how much it matters.
//! Enabling it is best-effort and never keeps a listener from starting; a
//! datagram that arrives without one, or with one describing no representable
//! instant, is served normally with no timestamp. Nothing about which datagrams
//! reach [`ServerCore`](crate::ServerCore) depends on it.
//!
//! **Nothing consumes it yet.** It is captured here so a follow-up change can
//! decide how a kernel-observed arrival time enters the protocol core; today
//! the core still samples its own receive instant and no protocol timing is
//! affected. It is a software wall-clock reading only — not a NIC hardware
//! timestamp, not monotonic, and not a transmit timestamp.

use std::{io, net::SocketAddr, time::SystemTime};

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

/// One received datagram, what answering it requires, and what the kernel
/// observed about its arrival.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReceivedDatagram {
    pub(crate) len: usize,
    pub(crate) peer: SocketAddr,
    pub(crate) reply_source: ReplySource,
    /// When the kernel received this datagram, where it said so.
    ///
    /// Optional accuracy metadata, absent on every target but Linux, on a
    /// Linux listener whose kernel refused the option, and on any datagram
    /// whose timestamp did not arrive or did not describe a representable
    /// instant. A wall-clock reading with no monotonic counterpart: none is
    /// synthesized, because there is no sound mapping from one to the other.
    ///
    /// Captured for a follow-up measurement change and consumed by nothing
    /// today — see the module documentation.
    pub(crate) kernel_rx_timestamp: Option<SystemTime>,
}

/// Asks the kernel to report each datagram's local destination address.
///
/// Called once, at construction, and only for a wildcard listener: an
/// explicit-address listener already replies from the one address it is bound
/// to, and asking for a destination it cannot use would widen the platform
/// surface of the common path for nothing.
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

/// Asks the kernel to report when it received each datagram, and reports
/// whether it agreed.
///
/// Called once, at construction, for every listener on a target that has this:
/// unlike destination metadata it costs an explicit listener nothing to have,
/// and both kinds of listener are equally worth timing accurately.
///
/// Deliberately not fallible. A listener without kernel timestamps serves every
/// request exactly as it otherwise would, so a refusal here is not a reason to
/// fail construction, and the returned flag exists to be asserted in tests
/// rather than acted on. The datagrams simply carry no timestamp.
#[cfg(target_os = "linux")]
pub(crate) fn try_configure_kernel_rx_timestamp(socket: &UdpSocket) -> bool {
    ancillary::try_configure_kernel_rx_timestamp(socket)
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn try_configure_kernel_rx_timestamp(_socket: &UdpSocket) -> bool {
    false
}

/// Receives one datagram, reporting where its reply must come from.
///
/// `select_reply_source` is the listener's own wildcard-ness, decided once at
/// construction. It is never inferred from a peer address.
///
/// This is the one place deciding whether a datagram may reach
/// [`ServerCore`](crate::ServerCore). `Ok(Some(..))` is a datagram the transport
/// layer accepts; `Ok(None)` is one the caller must consume and drop without
/// advancing any protocol or session state, because answering it correctly is
/// impossible. `Ok(None)` covers:
///
/// - payload truncation reported by an ancillary receive (`MSG_TRUNC`);
/// - ancillary metadata truncation (`MSG_CTRUNC`) on a wildcard listener;
/// - a wildcard listener's missing or unusable local destination;
/// - a datagram filling `buffer` exactly, which is conservatively treated as
///   potentially truncated.
///
/// That last rule exists because `recv_from` reports no truncation flag, so an
/// exactly-full result cannot be told apart from a larger datagram cut down to
/// the supplied capacity. It is expressed against `buffer.len()`, so the policy
/// follows whatever receive capacity the caller supplies, and it applies to
/// every receive path alike.
///
/// A missing or unusable *kernel receive timestamp* is on none of those lists.
/// It costs precision, not correctness, so the datagram is accepted without one
/// — including on an explicit listener whose control data was truncated, where
/// the timestamp is all that can have been lost.
///
/// Cancel-safe: nothing is consumed until a datagram is fully received.
pub(crate) async fn receive(
    socket: &UdpSocket,
    buffer: &mut [u8],
    select_reply_source: bool,
) -> io::Result<Option<ReceivedDatagram>> {
    let capacity = buffer.len();
    let received = if select_reply_source {
        receive_with_destination(socket, buffer).await?
    } else {
        receive_bound(socket, buffer).await?
    };

    Ok(received.filter(|datagram| datagram.len < capacity))
}

/// The explicit-address receive, which needs no ancillary data to answer
/// correctly.
///
/// Linux takes it through `recvmsg` anyway, purely so an enabled
/// `SCM_TIMESTAMPNS` can be read off the same call — `recv_from` cannot retrieve
/// control messages. Elsewhere there is no timestamp to collect, so the plain
/// path stays plain rather than widening the common path's platform surface for
/// nothing.
#[cfg(target_os = "linux")]
async fn receive_bound(
    socket: &UdpSocket,
    buffer: &mut [u8],
) -> io::Result<Option<ReceivedDatagram>> {
    ancillary::receive_bound(socket, buffer).await
}

#[cfg(not(target_os = "linux"))]
async fn receive_bound(
    socket: &UdpSocket,
    buffer: &mut [u8],
) -> io::Result<Option<ReceivedDatagram>> {
    let (len, peer) = socket.recv_from(buffer).await?;
    Ok(Some(ReceivedDatagram {
        len,
        peer,
        reply_source: ReplySource::Bound,
        kernel_rx_timestamp: None,
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

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use super::*;

    /// Sends `payload` to a fresh explicit-address listener and returns what the
    /// transport layer decided about it, received into a `capacity`-byte buffer.
    ///
    /// The buffer is deliberately tiny: the full-buffer rule is written against
    /// the caller's capacity, so it is provable without a 65,536-byte datagram.
    async fn receive_one(
        capacity: usize,
        payload: &[u8],
    ) -> (io::Result<Option<ReceivedDatagram>>, Vec<u8>) {
        let listener = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let sender = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        sender
            .send_to(payload, listener.local_addr().unwrap())
            .await
            .unwrap();

        let mut buffer = vec![0; capacity];
        let received = receive(&listener, &mut buffer, false).await;
        (received, buffer)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_datagram_shorter_than_the_buffer_reaches_the_core_intact() {
        let payload = b"irtt";
        let (received, buffer) = receive_one(16, payload).await;

        let received = received
            .unwrap()
            .expect("a datagram that cannot have been truncated is accepted");
        assert_eq!(received.len, payload.len());
        assert_eq!(received.peer.ip(), Ipv4Addr::LOCALHOST);
        assert_eq!(received.reply_source, ReplySource::Bound);
        assert_eq!(&buffer[..received.len], payload);
    }

    /// An exactly-full receive is ambiguous, not obviously fine: `recv_from`
    /// reports no truncation flag, so this datagram is indistinguishable from a
    /// larger one cut down to the same capacity. Both are dropped.
    #[tokio::test(flavor = "current_thread")]
    async fn a_datagram_filling_the_buffer_is_dropped_as_possibly_truncated() {
        let (received, _) = receive_one(4, b"irtt").await;
        assert!(received.unwrap().is_none());
    }

    /// What matters is that an oversized datagram never reaches the core, not
    /// how the platform says so: Unix truncates it to the buffer, which the
    /// full-buffer rule then drops, while Windows fails the receive outright
    /// with `WSAEMSGSIZE`. Asserting the shared guarantee keeps this test true
    /// on both.
    #[tokio::test(flavor = "current_thread")]
    async fn a_datagram_larger_than_the_buffer_never_reaches_the_core() {
        let (received, _) = receive_one(4, b"irtt-rs").await;
        assert!(!matches!(received, Ok(Some(_))));
    }

    /// A listener that never asked for kernel timestamps receives normally and
    /// reports none — the field is optional in the plainest sense, and its
    /// absence is not a defect to be worked around later.
    #[tokio::test(flavor = "current_thread")]
    async fn a_datagram_with_no_kernel_timestamp_is_served_all_the_same() {
        let (received, _) = receive_one(16, b"irtt").await;
        let received = received.unwrap().expect("an untimestamped datagram serves");
        assert_eq!(received.reply_source, ReplySource::Bound);
        assert_eq!(received.kernel_rx_timestamp, None);
    }

    /// Linux-only coverage of the two receive paths, which is where the
    /// timestamp actually comes from.
    #[cfg(target_os = "linux")]
    mod linux {
        use std::net::SocketAddr;

        use super::*;

        /// Sends `payload` from loopback to `listener` and receives it once.
        async fn exchange(
            listener: &UdpSocket,
            select_reply_source: bool,
        ) -> (Option<ReceivedDatagram>, Vec<u8>) {
            let payload = b"irtt";
            let destination =
                SocketAddr::from((Ipv4Addr::LOCALHOST, listener.local_addr().unwrap().port()));
            let sender = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
            sender.send_to(payload, destination).await.unwrap();

            let mut buffer = vec![0; 64];
            let received = receive(listener, &mut buffer, select_reply_source)
                .await
                .unwrap();
            (received, buffer)
        }

        /// Reports the timestamp, or says why it is skipping the assertion
        /// rather than inventing one. Loopback under a normal kernel supplies
        /// it; a container or emulator that does not must not fail the run.
        fn timestamp_or_skip(received: &ReceivedDatagram, what: &str) {
            match received.kernel_rx_timestamp {
                Some(timestamp) => assert!(
                    timestamp.duration_since(std::time::UNIX_EPOCH).is_ok(),
                    "a kernel receive timestamp is a wall-clock instant"
                ),
                None => eprintln!(
                    "skipping {what} kernel timestamp assertion: \
                     kernel did not provide SCM_TIMESTAMPNS"
                ),
            }
        }

        /// The explicit-address listener's new `recvmsg` path: it must keep
        /// reporting exactly what `recv_from` did, and additionally see the
        /// timestamp `recv_from` had no way to retrieve.
        #[tokio::test(flavor = "current_thread")]
        async fn an_explicit_listener_receives_a_datagram_and_its_kernel_timestamp() {
            let listener = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
            assert!(
                try_configure_kernel_rx_timestamp(&listener),
                "Linux loopback accepts SO_TIMESTAMPNS"
            );

            let (received, buffer) = exchange(&listener, false).await;
            let received = received.expect("an explicit listener serves a short datagram");

            assert_eq!(received.len, 4);
            assert_eq!(&buffer[..received.len], b"irtt");
            assert_eq!(received.peer.ip(), Ipv4Addr::LOCALHOST);
            assert_eq!(received.reply_source, ReplySource::Bound);
            timestamp_or_skip(&received, "explicit");
        }

        /// The regression this whole change risks: asking for a second control
        /// message must not crowd out the first. A wildcard listener with both
        /// enabled has to recover its destination *and* its timestamp from one
        /// datagram, with no `MSG_CTRUNC` drop and no dependence on the order
        /// the kernel returns them in.
        #[tokio::test(flavor = "current_thread")]
        async fn a_wildcard_listener_recovers_its_destination_and_timestamp_together() {
            let listener = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await.unwrap();
            configure_destination_metadata(&listener, true).unwrap();
            assert!(try_configure_kernel_rx_timestamp(&listener));

            let (received, buffer) = exchange(&listener, true).await;
            let received = received.expect(
                "a wildcard listener asking for two control messages still serves the datagram",
            );

            assert_eq!(received.len, 4);
            assert_eq!(&buffer[..received.len], b"irtt");
            assert_eq!(received.peer.ip(), Ipv4Addr::LOCALHOST);
            assert_eq!(
                received.reply_source,
                ReplySource::V4 {
                    address: Ipv4Addr::LOCALHOST
                },
                "the required destination survives the added timestamp request"
            );
            timestamp_or_skip(&received, "wildcard");
        }

        /// The timestamp is not a precondition for either listener: a wildcard
        /// one that never enabled it still recovers the destination its reply
        /// depends on and serves the request.
        #[tokio::test(flavor = "current_thread")]
        async fn a_wildcard_listener_without_timestamps_still_recovers_its_destination() {
            let listener = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await.unwrap();
            configure_destination_metadata(&listener, true).unwrap();

            let (received, _) = exchange(&listener, true).await;
            let received = received.expect("no timestamp requested, and none needed");

            assert_eq!(
                received.reply_source,
                ReplySource::V4 {
                    address: Ipv4Addr::LOCALHOST
                }
            );
            assert_eq!(received.kernel_rx_timestamp, None);
        }
    }
}
