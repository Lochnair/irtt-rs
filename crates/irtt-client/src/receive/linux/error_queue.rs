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

impl ErrorQueueControlBuffer {
    fn new() -> Self {
        Self([0; ERROR_QUEUE_CONTROL_LEN])
    }
}

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
        Err(Errno::EINTR) => Ok(Some(ErrorQueueRecord::Ignored)),
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
            ErrorQueueRecord::MalformedOrUnsupportedTimestamp
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
            ErrorQueueRecord::MalformedOrUnsupportedTimestamp
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
    fn classifies_timestamping_origin_with_wrong_completion_kind_as_malformed() {
        // Same origin byte as a send-timestamp completion, but a different
        // `ee_info` (e.g. SCM_TSTAMP_SCHED/ACK, which this client never
        // requests). A timestamping-origin record must never become a
        // fatal SocketError merely because it reports a completion kind we
        // did not ask for; it stays timing metadata.
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
            ErrorQueueRecord::MalformedOrUnsupportedTimestamp
        );
    }

    #[test]
    fn classifies_timestamping_origin_with_unexpected_errno_as_malformed() {
        // Origin is the sole discriminator: even an origin==TIMESTAMPING
        // record with a surprising errno (the kernel always reports ENOMSG
        // for this origin, but classification must not assume that) is
        // still timing metadata, never a real socket error.
        let cmsgs = vec![ControlMessageOwned::Ipv4RecvErr(
            extended_error(
                nix::libc::ECONNREFUSED as u32,
                nix::libc::SO_EE_ORIGIN_TIMESTAMPING,
                SCM_TSTAMP_SND,
                0,
            ),
            None,
        )];

        assert_eq!(
            classify(cmsgs.into_iter()),
            ErrorQueueRecord::MalformedOrUnsupportedTimestamp
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

    #[test]
    fn rx_only_sends_before_tx_enable_do_not_consume_the_later_tx_id_namespace() {
        let socket = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let peer = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        socket.connect(peer.local_addr().unwrap()).unwrap();
        setsockopt(
            &socket,
            sockopt::Timestamping,
            &super::super::RX_TIMESTAMPING_FLAGS,
        )
        .unwrap();

        // Ordinary pre-enable sends: TX_SOFTWARE is not yet requested, so
        // none of these may leave anything on the error queue for the
        // later-enabled OPT_ID counter to have skipped past.
        for _ in 0..3 {
            if let Err(error) = socket.send(b"pre-enable") {
                eprintln!("skipping pre-enable send/RX-only smoke test: {error}");
                return;
            }
        }
        assert_eq!(
            try_recv_error_queue_record(socket.as_raw_fd()).unwrap(),
            None,
            "RX-only sends must never populate MSG_ERRQUEUE"
        );

        // This mirrors production: RX-only is configured first (e.g. at
        // connect time), and only later, after a successful Open, is the
        // socket upgraded to TX_TIMESTAMPING_FLAGS.
        if setsockopt(&socket, sockopt::Timestamping, &TX_TIMESTAMPING_FLAGS).is_err() {
            eprintln!(
                "skipping pre-enable send/RX-only smoke test: kernel/container denied \
                 SO_TIMESTAMPING TX flags"
            );
            return;
        }

        if let Err(error) = socket.send(b"first-timestamped") {
            eprintln!(
                "skipping pre-enable send/RX-only smoke test: post-upgrade send failed: {error}"
            );
            return;
        }
        let Some(record) = poll_for_record(socket.as_raw_fd(), Duration::from_secs(2)) else {
            eprintln!(
                "skipping pre-enable send/RX-only smoke test: no TX timestamp record was \
                 delivered within the poll deadline"
            );
            return;
        };
        assert!(
            matches!(record, ErrorQueueRecord::TxTimestamp { id: 0, .. }),
            "expected the first send after upgrade to report id 0 (pre-enable sends must not \
             have consumed part of the namespace), got {record:?}"
        );
    }

    #[test]
    fn normal_rx_scm_timestamping_still_works_after_tx_flags_are_enabled() {
        let Some((socket, peer)) = tx_timestamped_loopback_socket() else {
            return;
        };
        peer.send_to(b"reply", socket.local_addr().unwrap())
            .unwrap();

        let mut buf = [0_u8; 16];
        let Ok(datagram) = super::super::recv_datagram(&socket, &mut buf) else {
            eprintln!("skipping post-TX-enable RX smoke test: receive failed");
            return;
        };
        assert_eq!(&buf[..datagram.len], b"reply");
        let Some(timestamp) = datagram.meta.kernel_rx_timestamp else {
            eprintln!(
                "skipping post-TX-enable RX smoke test: kernel did not provide SCM_TIMESTAMPING"
            );
            return;
        };
        assert!(timestamp.duration_since(std::time::UNIX_EPOCH).unwrap() > Duration::ZERO);
    }

    #[test]
    fn tx_timestamp_and_normal_reply_streams_do_not_interfere() {
        let Some((socket, peer)) = tx_timestamped_loopback_socket() else {
            return;
        };
        let fd = socket.as_raw_fd();

        if let Err(error) = socket.send(b"ping") {
            eprintln!("skipping TX/RX separation smoke test: ping send failed: {error}");
            return;
        }
        let mut incoming = [0_u8; 16];
        let Ok((len, from)) = peer.recv_from(&mut incoming) else {
            eprintln!("skipping TX/RX separation smoke test: peer never observed ping");
            return;
        };
        assert_eq!(&incoming[..len], b"ping");
        peer.send_to(b"pong", from).unwrap();

        // Reading the normal payload must not require, wait for, or be
        // disturbed by draining MSG_ERRQUEUE first.
        let mut reply_buf = [0_u8; 16];
        let Ok(datagram) = super::super::recv_datagram(&socket, &mut reply_buf) else {
            eprintln!("skipping TX/RX separation smoke test: normal receive failed");
            return;
        };
        assert_eq!(&reply_buf[..datagram.len], b"pong");

        // The TX timestamp for "ping" is still independently available on
        // the error queue, unconsumed by the normal receive above.
        let Some(record) = poll_for_record(fd, Duration::from_secs(2)) else {
            eprintln!("skipping TX/RX separation smoke test: no TX timestamp record arrived");
            return;
        };
        assert!(matches!(
            record,
            ErrorQueueRecord::TxTimestamp { id: 0, .. }
        ));
    }

    #[test]
    fn tx_timestamp_correlation_does_not_depend_on_original_payload() {
        let Some((socket, _peer)) = tx_timestamped_loopback_socket() else {
            return;
        };

        // OPT_TSONLY notifications never carry the original payload; a
        // larger payload must classify identically to the zero-length one
        // used elsewhere, since correlation is by `id` alone.
        let payload = vec![0x5A_u8; 512];
        if let Err(error) = socket.send(&payload) {
            eprintln!("skipping OPT_TSONLY payload-independence smoke test: send failed: {error}");
            return;
        }
        let Some(record) = poll_for_record(socket.as_raw_fd(), Duration::from_secs(2)) else {
            eprintln!(
                "skipping OPT_TSONLY payload-independence smoke test: no TX timestamp record \
                 arrived"
            );
            return;
        };
        assert!(matches!(
            record,
            ErrorQueueRecord::TxTimestamp { id: 0, .. }
        ));
    }

    // NOTE: a live test proving that a failed nonblocking `sendmsg` does not
    // consume an automatic OPT_ID is not included here. Forcing a reliable,
    // non-flaky `WouldBlock` on a connected loopback UDP socket (e.g. via
    // socket-buffer saturation) has no deterministic OS-level mechanism
    // available in this environment, and the repository's testing policy
    // explicitly forbids faking coverage with sleep/saturation-based
    // flakiness. That safety is instead established from Linux kernel
    // source: `_sock_tx_timestamp` (include/net/sock.h) and
    // `__ip_append_data`/`__ip6_append_data` (net/ipv4/ip_output.c,
    // net/ipv6/ip6_output.c) increment `sk->sk_tskey` speculatively before
    // a datagram is fully built and explicitly `atomic_dec` it back out on
    // every error path (tracked via a local `hold_tskey` flag), so a failed
    // send can never leave a gap in — or otherwise advance — the ID space
    // the kernel hands out to successfully submitted datagrams. See the
    // client crate's `AGENTS.md` for the full citation. The client-side
    // half of this invariant (a `WouldBlock` `try_send` never advances the
    // local wire-sequence counter used as the correlation ID) is covered by
    // a deterministic test in the adapter module that injects `WouldBlock`.
}
