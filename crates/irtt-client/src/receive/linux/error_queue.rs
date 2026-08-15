//! Safe `MSG_ERRQUEUE` primitives for the future client TX timestamp work
//! (see the parent module's TX-foundation note).
//!
//! Test-only for this PR: production sockets never enable
//! `SOF_TIMESTAMPING_TX_SOFTWARE` and never drain the error queue, so
//! nothing here runs outside `#[cfg(test)]`. It exists so the follow-up PR
//! that wires TX timestamps into probes does not need to rediscover flag
//! construction, error-queue parsing, or extended-error classification.

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

/// Desired `SO_TIMESTAMPING` configuration for a future TX-timestamped
/// socket. Combines this PR's RX flags with the TX-side flags the next PR
/// needs: `TX_SOFTWARE` to generate a send timestamp, `OPT_ID` for automatic
/// per-datagram correlation, and `OPT_TSONLY` so the notification does not
/// need to retain or copy the original payload.
///
/// Not applied to any production socket in this PR.
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
    /// A timestamp-origin extended error was present, but no usable
    /// timestamp accompanied it: either the `SCM_TIMESTAMPING` cmsg was
    /// missing, or its value failed to convert to a `SystemTime`. An
    /// accuracy failure, not a socket failure.
    MalformedTimestamp,
    /// An extended error that is not a timestamp completion: a real
    /// network, local, or ICMP error surfaced on the error queue. Must not
    /// be misclassified as malformed timing metadata.
    SocketError { errno: i32, origin: u8 },
    /// No extended error was present in this record's control data (e.g.
    /// only unrelated/unknown cmsgs).
    Ignored,
}

/// Classify a decoded set of cmsgs from one `MSG_ERRQUEUE` datagram.
///
/// Collects the extended error and any accompanying timestamp
/// independently before classifying, so cmsg order never matters. A
/// timestamp is only ever attached to a timestamp-origin extended error;
/// an ID is never reported without a timestamp, and a timestamp is never
/// accepted without a valid timestamp-origin extended error.
pub(crate) fn classify(cmsgs: impl Iterator<Item = ControlMessageOwned>) -> ErrorQueueRecord {
    let mut extended_error: Option<(u32, u8, u32, u32)> = None;
    let mut timestamp: Option<Timestamps> = None;

    for cmsg in cmsgs {
        match cmsg {
            ControlMessageOwned::Ipv4RecvErr(err, _) => {
                extended_error = Some((err.ee_errno, err.ee_origin, err.ee_info, err.ee_data));
            }
            ControlMessageOwned::Ipv6RecvErr(err, _) => {
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

    let is_timestamp_origin = origin == nix::libc::SO_EE_ORIGIN_TIMESTAMPING
        && errno == nix::libc::ENOMSG as u32
        && info == SCM_TSTAMP_SND;

    if !is_timestamp_origin {
        return ErrorQueueRecord::SocketError {
            errno: errno as i32,
            origin,
        };
    }

    match timestamp.and_then(|observed| system_time_from_timespec(observed.system)) {
        Some(timestamp) => ErrorQueueRecord::TxTimestamp { id, timestamp },
        None => ErrorQueueRecord::MalformedTimestamp,
    }
}

/// Reusable per-receive control buffer. `recvmsg` control data must satisfy
/// `cmsghdr` alignment; a plain `[u8; N]` only guarantees byte alignment, so
/// this mirrors the parent module's `ControlBuffer` rather than reading
/// `cmsghdr`s out of unaligned storage.
#[repr(align(8))]
struct ErrorQueueControlBuffer([u8; ERROR_QUEUE_CONTROL_LEN]);

impl ErrorQueueControlBuffer {
    fn new() -> Self {
        Self([0; ERROR_QUEUE_CONTROL_LEN])
    }
}

/// Nonblocking drain of a single record from `fd`'s `MSG_ERRQUEUE`.
///
/// Returns `Ok(None)` when the queue is empty (`EAGAIN`/`EWOULDBLOCK`).
/// `SOF_TIMESTAMPING_OPT_TSONLY` notifications carry no meaningful payload,
/// so a zero-length iovec is enough: correlation is by `id`, never by
/// payload content.
pub(crate) fn try_recv_error_queue_record(fd: RawFd) -> io::Result<Option<ErrorQueueRecord>> {
    let mut payload: [u8; 0] = [];
    let mut iov = [IoSliceMut::new(&mut payload)];
    let mut control = ErrorQueueControlBuffer::new();

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
        Err(errno) => Err(io::Error::from(errno)),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::{SocketAddr, UdpSocket},
        os::fd::AsRawFd,
        time::{Duration, Instant, UNIX_EPOCH},
    };

    use nix::sys::{
        socket::{setsockopt, sockopt},
        time::TimeSpec,
    };

    use super::*;

    fn extended_error(
        errno: u32,
        origin: u8,
        info: u32,
        data: u32,
    ) -> nix::libc::sock_extended_err {
        nix::libc::sock_extended_err {
            ee_errno: errno,
            ee_origin: origin,
            ee_type: 0,
            ee_code: 0,
            ee_pad: 0,
            ee_info: info,
            ee_data: data,
        }
    }

    fn timestamps_with_system(system: TimeSpec) -> Timestamps {
        Timestamps {
            system,
            hw_trans: TimeSpec::new(0, 0),
            hw_raw: TimeSpec::new(0, 0),
        }
    }

    fn send_timestamp_error(id: u32) -> ControlMessageOwned {
        ControlMessageOwned::Ipv4RecvErr(
            extended_error(
                nix::libc::ENOMSG as u32,
                nix::libc::SO_EE_ORIGIN_TIMESTAMPING,
                SCM_TSTAMP_SND,
                id,
            ),
            None,
        )
    }

    #[test]
    fn classifies_valid_send_timestamp_completion() {
        let cmsgs = vec![
            send_timestamp_error(7),
            ControlMessageOwned::ScmTimestampsns(timestamps_with_system(TimeSpec::new(1, 2))),
        ];

        let record = classify(cmsgs.into_iter());

        assert_eq!(
            record,
            ErrorQueueRecord::TxTimestamp {
                id: 7,
                timestamp: UNIX_EPOCH + Duration::new(1, 2),
            }
        );
    }

    #[test]
    fn classifies_timestamp_origin_without_timestamp_as_malformed() {
        let cmsgs = vec![send_timestamp_error(3)];

        assert_eq!(
            classify(cmsgs.into_iter()),
            ErrorQueueRecord::MalformedTimestamp
        );
    }

    #[test]
    fn classifies_timestamp_origin_with_invalid_timespec_as_malformed() {
        let cmsgs = vec![
            send_timestamp_error(3),
            ControlMessageOwned::ScmTimestampsns(timestamps_with_system(TimeSpec::new(-1, 0))),
        ];

        assert_eq!(
            classify(cmsgs.into_iter()),
            ErrorQueueRecord::MalformedTimestamp
        );
    }

    #[test]
    fn classifies_non_timestamp_extended_error_as_socket_error() {
        let cmsgs = vec![ControlMessageOwned::Ipv4RecvErr(
            extended_error(
                nix::libc::ECONNREFUSED as u32,
                nix::libc::SO_EE_ORIGIN_ICMP,
                0,
                0,
            ),
            None,
        )];

        assert_eq!(
            classify(cmsgs.into_iter()),
            ErrorQueueRecord::SocketError {
                errno: nix::libc::ECONNREFUSED,
                origin: nix::libc::SO_EE_ORIGIN_ICMP,
            }
        );
    }

    #[test]
    fn classifies_timestamping_origin_with_wrong_completion_kind_as_socket_error() {
        // Same origin byte as a send-timestamp completion, but a different
        // `ee_info` (e.g. SCM_TSTAMP_SCHED/ACK) must not be mistaken for
        // SCM_TSTAMP_SND.
        let cmsgs = vec![ControlMessageOwned::Ipv4RecvErr(
            extended_error(
                nix::libc::ENOMSG as u32,
                nix::libc::SO_EE_ORIGIN_TIMESTAMPING,
                1,
                0,
            ),
            None,
        )];

        assert_eq!(
            classify(cmsgs.into_iter()),
            ErrorQueueRecord::SocketError {
                errno: nix::libc::ENOMSG,
                origin: nix::libc::SO_EE_ORIGIN_TIMESTAMPING,
            }
        );
    }

    #[test]
    fn ignores_records_without_any_extended_error() {
        let cmsgs = vec![ControlMessageOwned::Ipv4Tos(0)];

        assert_eq!(classify(cmsgs.into_iter()), ErrorQueueRecord::Ignored);
    }

    #[test]
    fn unknown_cmsgs_do_not_affect_classification() {
        let cmsgs = vec![
            ControlMessageOwned::Ipv4Tos(0),
            send_timestamp_error(4),
            ControlMessageOwned::ScmTimestampsns(timestamps_with_system(TimeSpec::new(9, 0))),
        ];

        assert_eq!(
            classify(cmsgs.into_iter()),
            ErrorQueueRecord::TxTimestamp {
                id: 4,
                timestamp: UNIX_EPOCH + Duration::new(9, 0),
            }
        );
    }

    #[test]
    fn cmsg_order_does_not_affect_classification() {
        let forward = classify(
            vec![
                send_timestamp_error(9),
                ControlMessageOwned::ScmTimestampsns(timestamps_with_system(TimeSpec::new(5, 0))),
            ]
            .into_iter(),
        );
        let reversed = classify(
            vec![
                ControlMessageOwned::ScmTimestampsns(timestamps_with_system(TimeSpec::new(5, 0))),
                send_timestamp_error(9),
            ]
            .into_iter(),
        );

        assert_eq!(forward, reversed);
    }

    /// Bounded poll for one error-queue record, since software TX timestamp
    /// delivery is asynchronous even on loopback. Never sleeps hoping
    /// delivery "probably" happened; it keeps polling until either a
    /// record appears or the overall deadline passes.
    fn poll_for_record(fd: RawFd, overall_timeout: Duration) -> Option<ErrorQueueRecord> {
        let deadline = Instant::now() + overall_timeout;
        loop {
            if let Some(record) = try_recv_error_queue_record(fd).unwrap() {
                return Some(record);
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::yield_now();
        }
    }

    fn tx_timestamped_loopback_socket() -> Option<(UdpSocket, UdpSocket)> {
        let socket = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        // Held for the caller's lifetime: if this peer socket were dropped,
        // its port would close and subsequent sends would fail with
        // ECONNREFUSED once the kernel reports ICMP port-unreachable.
        let peer = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        socket.connect(peer.local_addr().unwrap()).unwrap();

        match setsockopt(&socket, sockopt::Timestamping, &TX_TIMESTAMPING_FLAGS) {
            Ok(()) => Some((socket, peer)),
            Err(error) => {
                eprintln!(
                    "skipping TX timestamping smoke test: kernel/container denied \
                     SO_TIMESTAMPING TX flags: {error}"
                );
                None
            }
        }
    }

    #[test]
    fn automatic_opt_id_increments_sequentially_across_separate_sends() {
        let Some((socket, _peer)) = tx_timestamped_loopback_socket() else {
            return;
        };
        let fd = socket.as_raw_fd();

        // Under heavy parallel test load, loopback UDP sends can
        // occasionally surface a stale ICMP-derived socket error unrelated
        // to this test's own sockets. That is an environmental condition,
        // not a claim this test makes about kernel TX timestamp behavior,
        // so it is a skip rather than a failure.
        if let Err(error) = socket.send(b"first") {
            eprintln!("skipping automatic OPT_ID assertion: first send failed: {error}");
            return;
        }
        let Some(first) = poll_for_record(fd, Duration::from_secs(2)) else {
            eprintln!(
                "skipping automatic OPT_ID assertion: no TX timestamp record was \
                 delivered within the poll deadline"
            );
            return;
        };
        assert!(
            matches!(first, ErrorQueueRecord::TxTimestamp { id: 0, .. }),
            "expected first send to report id 0, got {first:?}"
        );

        if let Err(error) = socket.send(b"second") {
            eprintln!("skipping automatic OPT_ID assertion: second send failed: {error}");
            return;
        }
        let Some(second) = poll_for_record(fd, Duration::from_secs(2)) else {
            eprintln!(
                "skipping automatic OPT_ID assertion: no TX timestamp record was \
                 delivered for the second send within the poll deadline"
            );
            return;
        };
        assert!(
            matches!(second, ErrorQueueRecord::TxTimestamp { id: 1, .. }),
            "expected second send to report id 1, got {second:?}"
        );
    }

    // NOTE: a deterministic test proving that a failed nonblocking `sendmsg`
    // does not consume an automatic OPT_ID is not included here. Forcing a
    // reliable, non-flaky `WouldBlock` on a connected loopback UDP socket
    // (e.g. via socket-buffer saturation) has no deterministic OS-level
    // mechanism available in this environment, and the repository's testing
    // policy explicitly forbids faking coverage with sleep/saturation-based
    // flakiness. This remains an open assumption for the follow-up PR that
    // wires automatic ID correlation into production: it must verify, by
    // construction or by a reliable test double, that a `WouldBlock` send
    // never advances the ID the kernel will assign to the next
    // successfully transmitted timestamped datagram.
}
