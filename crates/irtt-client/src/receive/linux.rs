//! Linux ancillary receive and transmit metadata.
//!
//! Receive kernel timing uses `SO_TIMESTAMPING` with `RX_SOFTWARE` +
//! `SOFTWARE` (reported via `SCM_TIMESTAMPING`), not the older
//! `SO_TIMESTAMPNS`/`SCM_TIMESTAMPNS`. Linux documents fallback quirks when
//! `SO_TIMESTAMP[NS]` and `SO_TIMESTAMPING` are both enabled on one socket,
//! so this establishes one unambiguous software-timestamp facility. `ReceiveMeta`'s
//! semantics — an optional observed kernel receive wall time, used for
//! downstream one-way delay only when plausible relative to the userspace
//! receive sample — are unchanged by any of this module's TX-side additions.
//!
//! The adapter resolves each TX notification to a probe before passing it
//! to the session machine. The error queue is drained opportunistically with
//! a fixed work bound. Plausible software wall timestamps affect upstream
//! one-way delay. RTT uses the userspace monotonic endpoints.

use std::{
    io::{self, IoSliceMut},
    net::{SocketAddr, UdpSocket},
    os::fd::{AsFd, AsRawFd, RawFd},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use nix::sys::{
    socket::{
        cmsg_space, recvmsg, setsockopt, sockopt, ControlMessageOwned, MsgFlags, RecvMsg,
        TimestampingFlag, Timestamps,
    },
    time::TimeSpec,
};

use crate::{metadata::ReceiveMeta, receive::ReceivedDatagram, timing::ClientTimestamp};

pub(crate) mod error_queue;

use error_queue::ErrorQueueRecord;

/// Starvation guard on one opportunistic `MSG_ERRQUEUE` drain.
///
/// Each successfully submitted, TX-timestamped datagram generates at most
/// one record, and drains happen at several natural choke points (after a
/// probe send, after a normal receive, before timeout expiry), so this is
/// not a queue-capacity promise: it just bounds the work one drain call can
/// do. A handful of in-flight probes' worth of backlog is the realistic
/// case between choke points, so a small constant comfortably covers it
/// without any unbounded loop.
pub(crate) const MAX_TX_TIMESTAMP_RECORDS_PER_DRAIN: usize = 32;

/// Enable TX generation on top of the initialized RX state. Linux validates
/// the new flags before replacing the previous configuration.
pub(crate) fn set_tx_timestamping<S: AsFd>(socket: &S, rx_timestamping: bool) -> io::Result<()> {
    let rx_flags = if rx_timestamping {
        RX_TIMESTAMPING_FLAGS
    } else {
        TimestampingFlag::empty()
    };
    Ok(setsockopt(
        socket,
        sockopt::Timestamping,
        &(rx_flags | error_queue::TX_TIMESTAMPING_FLAGS),
    )?)
}

/// Bounded, nonblocking drain of `fd`'s `MSG_ERRQUEUE`.
///
/// Reads and classifies up to [`MAX_TX_TIMESTAMP_RECORDS_PER_DRAIN`] records,
/// one at a time with no intermediate allocation. Each usable TX timestamp
/// completion is reported to `on_timestamp` immediately. Malformed or
/// unsupported timestamp notifications and unrelated records are silently
/// dropped: timestamp metadata is optional and never fails a probe. A
/// genuine non-timestamp socket/network error stops the drain and is
/// returned to the caller, which decides how to surface it.
pub(crate) fn drain_tx_timestamps<S: AsRawFd>(
    socket: &S,
    mut on_timestamp: impl FnMut(u32, SystemTime),
) -> io::Result<()> {
    let fd = socket.as_raw_fd();
    for _ in 0..MAX_TX_TIMESTAMP_RECORDS_PER_DRAIN {
        match error_queue::try_recv_error_queue_record(fd)? {
            None => return Ok(()),
            Some(ErrorQueueRecord::TxTimestamp { id, timestamp }) => on_timestamp(id, timestamp),
            Some(ErrorQueueRecord::MalformedOrUnsupportedTimestamp | ErrorQueueRecord::Ignored) => {
            }
            Some(ErrorQueueRecord::SocketError { errno, .. }) => {
                return Err(io::Error::from_raw_os_error(errno));
            }
        }
    }
    Ok(())
}

/// `SO_TIMESTAMPING` flags requested on every production socket at
/// connect time, before any TX upgrade is attempted.
///
/// `SOF_TIMESTAMPING_RX_SOFTWARE` asks the kernel to generate a software
/// receive timestamp for each datagram; `SOF_TIMESTAMPING_SOFTWARE` asks it
/// to report that timestamp back via `SCM_TIMESTAMPING`. Mixing this API with
/// the older `SO_TIMESTAMP`/`SO_TIMESTAMPNS` on the same socket has
/// documented fallback quirks, so the client uses `SO_TIMESTAMPING`
/// exclusively for receive timing. Hardware and TX-side flags are
/// deliberately excluded here: a fresh socket is receive-only until (and
/// unless) [`set_tx_timestamping`] later upgrades it.
const RX_TIMESTAMPING_FLAGS: TimestampingFlag = TimestampingFlag::SOF_TIMESTAMPING_RX_SOFTWARE
    .union(TimestampingFlag::SOF_TIMESTAMPING_SOFTWARE);

/// Control buffer capacity for a normal receive: one `SCM_TIMESTAMPING`
/// record (three `TimeSpec`s) plus one traffic-class record. Only one of
/// `IP_TOS` (`u8`) or `IPV6_TCLASS` (`i32`) is ever requested on a given
/// socket, but `cmsg_space` already rounds both up to the same aligned
/// space, so sizing on the larger of the two costs nothing and covers
/// either family.
const CONTROL_LEN: usize = cmsg_space::<Timestamps>() + cmsg_space::<i32>();

pub(crate) fn configure_receive_metadata(socket: &UdpSocket, remote: SocketAddr) -> io::Result<()> {
    let socket = socket2::SockRef::from(socket);
    if remote.is_ipv4() {
        socket.set_recv_tos_v4(true)
    } else {
        socket.set_recv_tclass_v6(true)
    }
}

pub(crate) fn set_rx_timestamping(socket: &UdpSocket) -> io::Result<()> {
    Ok(setsockopt(
        socket,
        sockopt::Timestamping,
        &RX_TIMESTAMPING_FLAGS,
    )?)
}

pub(crate) fn recv_datagram(
    socket: &UdpSocket,
    buf: &mut [u8],
    rx_timestamping: bool,
) -> Result<ReceivedDatagram, io::Error> {
    recv_datagram_fd(socket.as_raw_fd(), buf, MsgFlags::empty(), rx_timestamping)
}

#[cfg(feature = "tokio")]
pub(crate) fn try_recv_tokio_datagram(
    socket: &tokio::net::UdpSocket,
    buf: &mut [u8],
    rx_timestamping: bool,
) -> Result<ReceivedDatagram, io::Error> {
    socket.try_io(tokio::io::Interest::READABLE, || {
        recv_datagram_fd(
            socket.as_raw_fd(),
            buf,
            MsgFlags::MSG_DONTWAIT,
            rx_timestamping,
        )
    })
}

fn recv_datagram_fd(
    socket_fd: RawFd,
    buf: &mut [u8],
    flags: MsgFlags,
    rx_timestamping: bool,
) -> Result<ReceivedDatagram, io::Error> {
    let mut control = ControlBuffer([0; CONTROL_LEN]);
    let mut iov = [IoSliceMut::new(buf)];

    // The socket is connected, so the source address is not needed; `()` skips
    // copying it out.
    let msg = recvmsg::<()>(socket_fd, &mut iov, Some(&mut control.0), flags)?;
    let received_at = ClientTimestamp::now();

    Ok(ReceivedDatagram {
        len: msg.bytes,
        received_at,
        meta: receive_meta(&msg, rx_timestamping),
    })
}

fn receive_meta<S>(msg: &RecvMsg<'_, '_, S>, rx_timestamping: bool) -> ReceiveMeta {
    let mut meta = ReceiveMeta::default();
    // A control buffer the kernel had to truncate (`MSG_CTRUNC`) cannot be
    // walked reliably, so `cmsgs` refuses to parse it. The datagram itself is
    // still valid; report it without ancillary metadata.
    let Ok(cmsgs) = msg.cmsgs() else {
        return meta;
    };

    for cmsg in cmsgs {
        match cmsg {
            // SOFTWARE reporting for TX can also surface RX timestamps
            // generated by other sockets on this host.
            ControlMessageOwned::ScmTimestampsns(timestamps) if rx_timestamping => {
                meta.kernel_rx_timestamp = system_time_from_timespec(timestamps.system);
            }
            ControlMessageOwned::Ipv4Tos(tos) => {
                meta.traffic_class = Some(tos);
            }
            ControlMessageOwned::Ipv6TClass(traffic_class) => {
                meta.traffic_class = u8::try_from(traffic_class).ok();
            }
            _ => {}
        }
    }
    meta
}

fn system_time_from_timespec(timespec: TimeSpec) -> Option<SystemTime> {
    let seconds = u64::try_from(timespec.tv_sec()).ok()?;
    let nanos = u32::try_from(timespec.tv_nsec()).ok()?;
    if nanos >= 1_000_000_000 || (seconds == 0 && nanos == 0) {
        return None;
    }
    UNIX_EPOCH.checked_add(Duration::new(seconds, nanos))
}

/// Reusable per-receive control buffer. `recvmsg` control data must satisfy
/// `cmsghdr` alignment, which 8-byte alignment covers on supported targets.
#[repr(align(8))]
struct ControlBuffer([u8; CONTROL_LEN]);
