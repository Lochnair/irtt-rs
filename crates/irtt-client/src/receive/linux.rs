use std::{
    io::{self, IoSliceMut},
    net::{SocketAddr, UdpSocket},
    os::fd::{AsRawFd, RawFd},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use nix::sys::{
    socket::{recvmsg, setsockopt, sockopt, ControlMessageOwned, MsgFlags, RecvMsg},
    time::TimeSpec,
};

use crate::{metadata::ReceiveMeta, receive::ReceivedDatagram, timing::ClientTimestamp};

const CONTROL_LEN: usize = 128;

pub(crate) fn configure_receive_metadata(socket: &UdpSocket, remote: SocketAddr) -> io::Result<()> {
    setsockopt(socket, sockopt::ReceiveTimestampns, &true)?;
    let socket = socket2::SockRef::from(socket);
    if remote.is_ipv4() {
        socket.set_recv_tos_v4(true)
    } else {
        socket.set_recv_tclass_v6(true)
    }
}

pub(crate) fn recv_datagram(
    socket: &UdpSocket,
    buf: &mut [u8],
) -> Result<ReceivedDatagram, io::Error> {
    recv_datagram_fd(socket.as_raw_fd(), buf, MsgFlags::empty())
}

#[cfg(feature = "tokio")]
pub(crate) fn try_recv_tokio_datagram(
    socket: &tokio::net::UdpSocket,
    buf: &mut [u8],
) -> Result<ReceivedDatagram, io::Error> {
    socket.try_io(tokio::io::Interest::READABLE, || {
        recv_datagram_fd(socket.as_raw_fd(), buf, MsgFlags::MSG_DONTWAIT)
    })
}

fn recv_datagram_fd(
    socket_fd: RawFd,
    buf: &mut [u8],
    flags: MsgFlags,
) -> Result<ReceivedDatagram, io::Error> {
    let mut control = ControlBuffer::new();
    let mut iov = [IoSliceMut::new(buf)];

    // The socket is connected, so the source address is not needed; `()` skips
    // copying it out.
    let msg = recvmsg::<()>(socket_fd, &mut iov, Some(control.as_mut_slice()), flags)?;
    let received_at = ClientTimestamp::now();

    Ok(ReceivedDatagram {
        len: msg.bytes,
        received_at,
        meta: receive_meta(&msg),
    })
}

fn receive_meta<S>(msg: &RecvMsg<'_, '_, S>) -> ReceiveMeta {
    let mut meta = ReceiveMeta::default();
    // A control buffer the kernel had to truncate (`MSG_CTRUNC`) cannot be
    // walked reliably, so `cmsgs` refuses to parse it. The datagram itself is
    // still valid; report it without ancillary metadata.
    let Ok(cmsgs) = msg.cmsgs() else {
        return meta;
    };

    for cmsg in cmsgs {
        match cmsg {
            ControlMessageOwned::ScmTimestampns(timestamp) => {
                meta.kernel_rx_timestamp = system_time_from_timespec(timestamp);
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
    if nanos >= 1_000_000_000 {
        return None;
    }
    UNIX_EPOCH.checked_add(Duration::new(seconds, nanos))
}

/// Reusable per-receive control buffer. `recvmsg` control data must satisfy
/// `cmsghdr` alignment, which 8-byte alignment covers on supported targets.
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
    use std::{
        io,
        net::{SocketAddr, UdpSocket},
        time::{Duration, UNIX_EPOCH},
    };

    use nix::sys::time::TimeSpec;

    use crate::{
        event::PacketMeta,
        receive::{configure_receive_metadata, recv_datagram},
        socket_options::apply_traffic_class_to_socket,
        timing::ClientTimestamp,
    };

    fn connected_ipv4_loopback_pair() -> (UdpSocket, UdpSocket) {
        let a = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let b = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        a.connect(b.local_addr().unwrap()).unwrap();
        b.connect(a.local_addr().unwrap()).unwrap();
        a.set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();
        b.set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();
        (a, b)
    }

    fn connected_ipv6_loopback_pair() -> Option<(UdpSocket, UdpSocket)> {
        let bind_addr = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], 0));
        let a = match UdpSocket::bind(bind_addr) {
            Ok(socket) => socket,
            Err(error) if is_unavailable_ipv6_loopback(&error) => {
                eprintln!(
                    "skipping IPv6 ancillary receive test: IPv6 loopback unavailable: {error}"
                );
                return None;
            }
            Err(error) => panic!("{error}"),
        };
        let b = match UdpSocket::bind(bind_addr) {
            Ok(socket) => socket,
            Err(error) if is_unavailable_ipv6_loopback(&error) => {
                eprintln!(
                    "skipping IPv6 ancillary receive test: IPv6 loopback unavailable: {error}"
                );
                return None;
            }
            Err(error) => panic!("{error}"),
        };
        match a.connect(b.local_addr().unwrap()) {
            Ok(()) => {}
            Err(error) if is_unavailable_ipv6_loopback(&error) => {
                eprintln!(
                    "skipping IPv6 ancillary receive test: IPv6 loopback unavailable: {error}"
                );
                return None;
            }
            Err(error) => panic!("{error}"),
        }
        match b.connect(a.local_addr().unwrap()) {
            Ok(()) => {}
            Err(error) if is_unavailable_ipv6_loopback(&error) => {
                eprintln!(
                    "skipping IPv6 ancillary receive test: IPv6 loopback unavailable: {error}"
                );
                return None;
            }
            Err(error) => panic!("{error}"),
        }
        a.set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();
        b.set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();
        Some((a, b))
    }

    #[test]
    fn recvmsg_receive_returns_length_payload_and_timestamp() {
        let (sender, receiver) = connected_ipv4_loopback_pair();
        configure_receive_metadata(&receiver, sender.local_addr().unwrap()).unwrap();
        sender.send(b"hello").unwrap();

        let before = ClientTimestamp::now();
        let mut buf = [0_u8; 16];
        let datagram = recv_datagram(&receiver, &mut buf).unwrap();
        let after = ClientTimestamp::now();

        assert_eq!(datagram.len, 5);
        assert_eq!(&buf[..datagram.len], b"hello");
        assert!(datagram.received_at.mono >= before.mono);
        assert!(datagram.received_at.mono <= after.mono);
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn tokio_recvmsg_would_block_clears_readiness() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .unwrap();

        runtime.block_on(async {
            let (sender, receiver) = connected_ipv4_loopback_pair();
            configure_receive_metadata(&receiver, sender.local_addr().unwrap()).unwrap();
            receiver.set_nonblocking(true).unwrap();
            let receiver = tokio::net::UdpSocket::from_std(receiver).unwrap();
            let mut buf = [0_u8; 16];

            sender.send(b"first").unwrap();
            tokio::time::timeout(Duration::from_secs(1), receiver.readable())
                .await
                .expect("initial readability timed out")
                .unwrap();

            let first = super::try_recv_tokio_datagram(&receiver, &mut buf).unwrap();
            assert_eq!(&buf[..first.len], b"first");

            let error = super::try_recv_tokio_datagram(&receiver, &mut buf).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::WouldBlock);

            assert!(
                tokio::time::timeout(Duration::from_millis(20), receiver.readable())
                    .await
                    .is_err(),
                "readable completed from stale readiness after recvmsg WouldBlock"
            );

            sender.send(b"second").unwrap();
            tokio::time::timeout(Duration::from_secs(1), receiver.readable())
                .await
                .expect("second readability timed out")
                .unwrap();

            let second = super::try_recv_tokio_datagram(&receiver, &mut buf).unwrap();
            assert_eq!(&buf[..second.len], b"second");
        });
    }

    #[test]
    fn kernel_rx_timestamp_metadata_is_observed_when_kernel_provides_it() {
        let (sender, receiver) = connected_ipv4_loopback_pair();
        configure_receive_metadata(&receiver, sender.local_addr().unwrap()).unwrap();
        sender.send(b"stamp").unwrap();

        let mut buf = [0_u8; 16];
        let datagram = recv_datagram(&receiver, &mut buf).unwrap();
        assert_eq!(datagram.len, 5);
        assert_eq!(&buf[..datagram.len], b"stamp");
        let Some(timestamp) = datagram.meta.kernel_rx_timestamp else {
            eprintln!(
                "skipping kernel timestamp assertion: kernel did not provide SCM_TIMESTAMPNS"
            );
            return;
        };

        let duration = timestamp.duration_since(UNIX_EPOCH).unwrap();
        assert!(duration.as_nanos() > 0);
    }

    #[test]
    fn ipv4_traffic_class_metadata_is_observed_when_kernel_provides_it() {
        let (sender, receiver) = connected_ipv4_loopback_pair();
        configure_receive_metadata(&receiver, sender.local_addr().unwrap()).unwrap();
        apply_traffic_class_to_socket(&sender, receiver.local_addr().unwrap(), 184).unwrap();
        sender.send(b"dscp").unwrap();

        let mut buf = [0_u8; 16];
        let datagram = recv_datagram(&receiver, &mut buf).unwrap();
        let Some(traffic_class) = datagram.meta.traffic_class else {
            eprintln!("skipping IPv4 ancillary metadata assertion: kernel did not provide IP_TOS");
            return;
        };

        let packet_meta = PacketMeta::from(datagram.meta);
        assert_eq!(traffic_class & 0xfc, 184);
        assert_eq!(packet_meta.dscp, Some(46));
        assert_eq!(packet_meta.ecn, Some(0));
    }

    #[test]
    fn ipv6_traffic_class_metadata_is_observed_when_kernel_provides_it() {
        let Some((sender, receiver)) = connected_ipv6_loopback_pair() else {
            return;
        };
        configure_receive_metadata(&receiver, sender.local_addr().unwrap()).unwrap();
        apply_traffic_class_to_socket(&sender, receiver.local_addr().unwrap(), 184).unwrap();
        sender.send(b"dscp").unwrap();

        let mut buf = [0_u8; 16];
        let datagram = recv_datagram(&receiver, &mut buf).unwrap();
        let Some(traffic_class) = datagram.meta.traffic_class else {
            eprintln!(
                "skipping IPv6 ancillary metadata assertion: kernel did not provide IPV6_TCLASS"
            );
            return;
        };

        let packet_meta = PacketMeta::from(datagram.meta);
        assert_eq!(traffic_class & 0xfc, 184);
        assert_eq!(packet_meta.dscp, Some(46));
        assert_eq!(packet_meta.ecn, Some(0));
    }

    #[test]
    fn observed_zero_traffic_class_preserves_some_zero() {
        let (sender, receiver) = connected_ipv4_loopback_pair();
        configure_receive_metadata(&receiver, sender.local_addr().unwrap()).unwrap();
        sender.send(b"zero").unwrap();

        let mut buf = [0_u8; 16];
        let datagram = recv_datagram(&receiver, &mut buf).unwrap();
        let Some(traffic_class) = datagram.meta.traffic_class else {
            eprintln!("skipping observed-zero assertion: kernel did not provide IP_TOS");
            return;
        };

        let packet_meta = PacketMeta::from(datagram.meta);
        assert_eq!(traffic_class, 0);
        assert_eq!(packet_meta.traffic_class, Some(0));
        assert_eq!(packet_meta.dscp, Some(0));
        assert_eq!(packet_meta.ecn, Some(0));
    }

    #[test]
    fn timespec_conversion_accepts_valid_unix_timestamp() {
        let timestamp = super::system_time_from_timespec(TimeSpec::new(1, 2)).unwrap();

        assert_eq!(
            timestamp.duration_since(UNIX_EPOCH).unwrap(),
            Duration::new(1, 2)
        );
    }

    #[test]
    fn timespec_conversion_rejects_negative_or_invalid_values() {
        assert_eq!(super::system_time_from_timespec(TimeSpec::new(-1, 0)), None);
        assert_eq!(super::system_time_from_timespec(TimeSpec::new(0, -1)), None);
        assert_eq!(
            super::system_time_from_timespec(TimeSpec::new(0, 1_000_000_000)),
            None
        );
    }

    fn is_unavailable_ipv6_loopback(error: &io::Error) -> bool {
        matches!(
            error.kind(),
            io::ErrorKind::AddrNotAvailable | io::ErrorKind::Unsupported
        )
    }
}
