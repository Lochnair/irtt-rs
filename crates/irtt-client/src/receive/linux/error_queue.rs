//! Safe `MSG_ERRQUEUE` primitives backing the client's TX timestamp capture.
//!
//! A production socket that successfully upgraded to
//! [`TX_TIMESTAMPING_FLAGS`] receives one extended-error record per
//! successfully submitted timestamped datagram on `MSG_ERRQUEUE`.
//! [`try_recv_error_queue_record`] performs one nonblocking read of that
//! queue and [`classify`] turns its ancillary data into an
//! [`ErrorQueueRecord`] without ever panicking or requiring the caller to
//! understand cmsg layout.

use std::{
    io::{self, IoSliceMut},
    os::fd::RawFd,
    time::SystemTime,
};

use nix::{
    errno::Errno,
    sys::socket::{
        cmsg_space, recvmsg, ControlMessageOwned, MsgFlags, TimestampingFlag, Timestamps,
    },
};

use super::system_time_from_timespec;

/// `SO_TIMESTAMPING` configuration for a TX-timestamped socket. Combines the
/// module's RX flags with the TX-side flags TX capture needs: `TX_SOFTWARE`
/// to generate a send timestamp, `OPT_ID` for automatic per-datagram
/// correlation, and `OPT_TSONLY` so the notification does not need to
/// retain or copy the original payload.
pub(crate) const TX_TIMESTAMPING_FLAGS: TimestampingFlag =
    TimestampingFlag::SOF_TIMESTAMPING_RX_SOFTWARE
        .union(TimestampingFlag::SOF_TIMESTAMPING_SOFTWARE)
        .union(TimestampingFlag::SOF_TIMESTAMPING_TX_SOFTWARE)
        .union(TimestampingFlag::SOF_TIMESTAMPING_OPT_ID)
        .union(TimestampingFlag::SOF_TIMESTAMPING_OPT_TSONLY);

/// `SCM_TSTAMP_SND`, the send-timestamp completion kind reported in the
/// `ee_info` field of a `SO_EE_ORIGIN_TIMESTAMPING` extended error.
///
/// Not exposed by `libc` or `nix`. Value verified against the kernel UAPI
/// enum in `include/uapi/linux/errqueue.h`:
/// `enum { SCM_TSTAMP_SND, SCM_TSTAMP_SCHED, SCM_TSTAMP_ACK };`, so
/// `SCM_TSTAMP_SND == 0`. This is a stable, long-documented UAPI value, not
/// a guess.
const SCM_TSTAMP_SND: u32 = 0;

/// A large-enough placeholder for the offender address `sock_extended_err`
/// is followed by in the kernel's error-queue cmsg payload (the address is
/// present for network-originated errors, absent for local/timestamp
/// ones). Sized generously so `cmsg_space` reserves enough control-buffer
/// capacity for either an `IPv4` or `IPv6` offender address; never read.
#[repr(C)]
struct ExtendedErrWithAddr {
    _err: nix::libc::sock_extended_err,
    _addr: nix::libc::sockaddr_in6,
}

/// Control buffer capacity for one error-queue receive: one extended-error
/// record (with room for the largest possible offender address) plus one
/// `SCM_TIMESTAMPING` record.
const ERROR_QUEUE_CONTROL_LEN: usize =
    cmsg_space::<ExtendedErrWithAddr>() + cmsg_space::<Timestamps>();

/// A classified record read from a socket's `MSG_ERRQUEUE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ErrorQueueRecord {
    /// A genuine send-timestamp completion: the extended error's origin was
    /// `SO_EE_ORIGIN_TIMESTAMPING`, its kind was `SCM_TSTAMP_SND`, and a
    /// usable software timestamp accompanied it.
    TxTimestamp { id: u32, timestamp: SystemTime },
    /// The extended error's origin was `SO_EE_ORIGIN_TIMESTAMPING` (i.e. this
    /// is unambiguously a timestamp facility record, never a real
    /// socket/network error), but it was not a usable send-timestamp
    /// completion: the completion kind was not `SCM_TSTAMP_SND` (this client
    /// requests only `TX_SOFTWARE`, so only `SND` is expected, but an
    /// unrequested kind must not be mistaken for a socket failure), the
    /// `SCM_TIMESTAMPING` cmsg was missing, or its value failed to convert
    /// to a `SystemTime`. An accuracy failure, not a socket failure.
    MalformedOrUnsupportedTimestamp,
    /// An extended error whose origin was not `SO_EE_ORIGIN_TIMESTAMPING`: a
    /// real network, local, or ICMP error surfaced on the error queue. Must
    /// not be misclassified as malformed timing metadata.
    SocketError { errno: i32, origin: u8 },
    /// No extended error was present in this record's control data (e.g.
    /// only unrelated/unknown cmsgs).
    Ignored,
}

/// Classify a decoded set of cmsgs from one `MSG_ERRQUEUE` datagram.
///
/// Collects the extended error and any accompanying timestamp
/// independently before classifying, so cmsg order never matters. Origin is
/// the sole discriminator between a timestamp facility record and a real
/// socket error: any `SO_EE_ORIGIN_TIMESTAMPING` record is timing metadata,
/// however unexpected its `errno`/`ee_info`, and only ever becomes
/// [`ErrorQueueRecord::MalformedOrUnsupportedTimestamp`] on the way to
/// `None` — never [`ErrorQueueRecord::SocketError`]. A timestamp is only
/// ever attached to a timestamp-origin record reporting `SCM_TSTAMP_SND`.
pub(crate) fn classify(cmsgs: impl Iterator<Item = ControlMessageOwned>) -> ErrorQueueRecord {
    let mut extended_error: Option<(u32, u8, u32, u32)> = None;
    let mut timestamp: Option<Timestamps> = None;

    for cmsg in cmsgs {
        match cmsg {
            ControlMessageOwned::Ipv4RecvErr(err, _) | ControlMessageOwned::Ipv6RecvErr(err, _) => {
                extended_error = Some((err.ee_errno, err.ee_origin, err.ee_info, err.ee_data));
            }
            ControlMessageOwned::ScmTimestampsns(observed) => {
                timestamp = Some(observed);
            }
            _ => {}
        }
    }

    let Some((errno, origin, info, id)) = extended_error else {
        return ErrorQueueRecord::Ignored;
    };

    if origin != nix::libc::SO_EE_ORIGIN_TIMESTAMPING {
        return ErrorQueueRecord::SocketError {
            errno: errno as i32,
            origin,
        };
    }

    let is_send_completion = errno == nix::libc::ENOMSG as u32 && info == SCM_TSTAMP_SND;
    if !is_send_completion {
        return ErrorQueueRecord::MalformedOrUnsupportedTimestamp;
    }

    match timestamp.and_then(|observed| system_time_from_timespec(observed.system)) {
        Some(timestamp) => ErrorQueueRecord::TxTimestamp { id, timestamp },
        None => ErrorQueueRecord::MalformedOrUnsupportedTimestamp,
    }
}

/// Reusable per-receive control buffer. `recvmsg` control data must satisfy
/// `cmsghdr` alignment; a plain `[u8; N]` only guarantees byte alignment, so
/// this mirrors the parent module's `ControlBuffer` rather than reading
/// `cmsghdr`s out of unaligned storage.
#[repr(align(8))]
struct ErrorQueueControlBuffer([u8; ERROR_QUEUE_CONTROL_LEN]);

/// Nonblocking drain of a single record from `fd`'s `MSG_ERRQUEUE`.
///
/// Returns `Ok(None)` when the queue is empty (`EAGAIN`/`EWOULDBLOCK`) — the
/// caller in [`super::drain_tx_timestamps`] stops its drain there, exactly
/// as it always has. An interrupted read (`EINTR`) is different: it says
/// nothing about whether the queue is empty, so treating it the same as
/// `EWOULDBLOCK` could end a drain early and strand a still-queued record
/// past the point its matching probe is looked up and removed (see the
/// crate's `AGENTS.md`). TX timestamp capture is optional best-effort
/// metadata, not a network operation, so an interruption here must also
/// never become a socket/network error; instead it is reported as
/// [`ErrorQueueRecord::Ignored`], which consumes one attempt of the bounded
/// caller's existing per-drain work budget and lets the loop immediately
/// retry the same slot rather than stopping or spinning unboundedly.
/// `SOF_TIMESTAMPING_OPT_TSONLY` notifications carry no meaningful payload,
/// so a zero-length iovec is enough: correlation is by `id`, never by
/// payload content.
pub(crate) fn try_recv_error_queue_record(fd: RawFd) -> io::Result<Option<ErrorQueueRecord>> {
    let mut payload: [u8; 0] = [];
    let mut iov = [IoSliceMut::new(&mut payload)];
    let mut control = ErrorQueueControlBuffer([0; ERROR_QUEUE_CONTROL_LEN]);

    match recvmsg::<()>(
        fd,
        &mut iov,
        Some(&mut control.0),
        MsgFlags::MSG_ERRQUEUE | MsgFlags::MSG_DONTWAIT,
    ) {
        Ok(msg) => {
            let cmsgs = msg
                .cmsgs()
                .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
            Ok(Some(classify(cmsgs)))
        }
        Err(Errno::EWOULDBLOCK) => Ok(None),
        Err(Errno::EINTR) => Ok(Some(ErrorQueueRecord::Ignored)),
        Err(errno) => Err(io::Error::from(errno)),
    }
}
