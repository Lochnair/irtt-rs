#![allow(unsafe_code)]
use std::{
    io, mem,
    net::{SocketAddr, UdpSocket},
    os::fd::AsRawFd,
    ptr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{metadata::ReceiveMeta, receive::ReceivedDatagram, timing::ClientTimestamp};

const CONTROL_LEN: usize = 128;

pub(crate) fn configure_receive_metadata(socket: &UdpSocket, remote: SocketAddr) -> io::Result<()> {
    enable_kernel_rx_timestamps(socket)?;
    let socket = socket2::SockRef::from(socket);
    if remote.is_ipv4() {
        socket.set_recv_tos_v4(true)
    } else {
        socket.set_recv_tclass_v6(true)
    }
}

fn enable_kernel_rx_timestamps(socket: &UdpSocket) -> io::Result<()> {
    let enabled: libc::c_int = 1;
    let result = unsafe {
        // SAFETY: The file descriptor is borrowed from a valid `UdpSocket`.
        // `enabled` is a properly aligned `c_int`, and the length matches the
        // pointed-to value for the `SO_TIMESTAMPNS` boolean socket option.
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_TIMESTAMPNS,
            (&enabled as *const libc::c_int).cast(),
            mem::size_of_val(&enabled) as libc::socklen_t,
        )
    };
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub(crate) fn recv_datagram(
    socket: &UdpSocket,
    buf: &mut [u8],
) -> Result<ReceivedDatagram, io::Error> {
    let mut control = ControlBuffer::new();
    let mut iov = libc::iovec {
        iov_base: buf.as_mut_ptr().cast(),
        iov_len: buf.len(),
    };
    let mut msg: libc::msghdr = unsafe { mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.as_mut_ptr().cast();
    msg.msg_controllen = control.len();

    let len = unsafe {
        // SAFETY: `msg` points to one writable iovec backed by `buf`, and the
        // control buffer is writable and lives until `recvmsg` returns. The
        // socket file descriptor is borrowed from a valid `UdpSocket`.
        libc::recvmsg(socket.as_raw_fd(), &mut msg, 0)
    };
    if len < 0 {
        return Err(io::Error::last_os_error());
    }
    let len = usize::try_from(len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "recvmsg returned an unrepresentable datagram length",
        )
    })?;
    let received_at = ClientTimestamp::now();
    let meta = unsafe {
        // SAFETY: `msg` was initialized by a successful `recvmsg` call. The
        // parser only reads cmsghdr entries within `msg_controllen` and ignores
        // short or unrelated control messages.
        parse_receive_meta(&msg)
    };

    Ok(ReceivedDatagram {
        len,
        received_at,
        meta,
    })
}

unsafe fn parse_receive_meta(msg: &libc::msghdr) -> ReceiveMeta {
    let mut meta = ReceiveMeta::default();
    let mut cmsg = libc::CMSG_FIRSTHDR(msg);
    while !cmsg.is_null() {
        let cmsg_ref = &*cmsg;
        let header_len = libc::CMSG_LEN(0) as usize;
        if cmsg_ref.cmsg_len < header_len {
            cmsg = libc::CMSG_NXTHDR(msg, cmsg);
            continue;
        }
        let data_len = cmsg_ref.cmsg_len - header_len;
        if is_traffic_class_cmsg(cmsg_ref) {
            meta.traffic_class = read_int_cmsg_low_byte(cmsg, data_len);
        } else if is_kernel_rx_timestamp_cmsg(cmsg_ref) {
            meta.kernel_rx_timestamp = read_timespec_cmsg(cmsg, data_len);
        }
        cmsg = libc::CMSG_NXTHDR(msg, cmsg);
    }
    meta
}

fn is_traffic_class_cmsg(cmsg: &libc::cmsghdr) -> bool {
    (cmsg.cmsg_level == libc::IPPROTO_IP && cmsg.cmsg_type == libc::IP_TOS)
        || (cmsg.cmsg_level == libc::IPPROTO_IPV6 && cmsg.cmsg_type == libc::IPV6_TCLASS)
}

fn is_kernel_rx_timestamp_cmsg(cmsg: &libc::cmsghdr) -> bool {
    cmsg.cmsg_level == libc::SOL_SOCKET && cmsg.cmsg_type == libc::SCM_TIMESTAMPNS
}

unsafe fn read_u8_cmsg(cmsg: *mut libc::cmsghdr, data_len: usize) -> Option<u8> {
    if data_len >= mem::size_of::<u8>() {
        Some(*libc::CMSG_DATA(cmsg).cast::<u8>())
    } else {
        None
    }
}

unsafe fn read_int_cmsg_low_byte(cmsg: *mut libc::cmsghdr, data_len: usize) -> Option<u8> {
    if data_len >= mem::size_of::<libc::c_int>() {
        let value = ptr::read_unaligned(libc::CMSG_DATA(cmsg).cast::<libc::c_int>());
        Some((value & 0xff) as u8)
    } else {
        read_u8_cmsg(cmsg, data_len)
    }
}

unsafe fn read_timespec_cmsg(cmsg: *mut libc::cmsghdr, data_len: usize) -> Option<SystemTime> {
    if data_len < mem::size_of::<libc::timespec>() {
        return None;
    }
    let timespec = ptr::read_unaligned(libc::CMSG_DATA(cmsg).cast::<libc::timespec>());
    system_time_from_timespec(timespec)
}

fn system_time_from_timespec(timespec: libc::timespec) -> Option<SystemTime> {
    if timespec.tv_sec < 0 || timespec.tv_nsec < 0 || timespec.tv_nsec >= 1_000_000_000 {
        return None;
    }
    let seconds = u64::try_from(timespec.tv_sec).ok()?;
    let nanos = u32::try_from(timespec.tv_nsec).ok()?;
    UNIX_EPOCH.checked_add(Duration::new(seconds, nanos))
}

#[repr(align(8))]
struct ControlBuffer([u8; CONTROL_LEN]);

impl ControlBuffer {
    fn new() -> Self {
        Self([0; CONTROL_LEN])
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.0.as_mut_ptr()
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        net::{SocketAddr, UdpSocket},
        time::{Duration, UNIX_EPOCH},
    };

    use crate::{
        event::PacketMeta,
        receive::{configure_receive_metadata, recv_datagram},
        socket_options::apply_dscp_to_socket,
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
        apply_dscp_to_socket(&sender, receiver.local_addr().unwrap(), 46).unwrap();
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
        apply_dscp_to_socket(&sender, receiver.local_addr().unwrap(), 46).unwrap();
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
        let timestamp = super::system_time_from_timespec(libc::timespec {
            tv_sec: 1,
            tv_nsec: 2,
        })
        .unwrap();

        assert_eq!(
            timestamp.duration_since(UNIX_EPOCH).unwrap(),
            Duration::new(1, 2)
        );
    }

    #[test]
    fn timespec_conversion_rejects_negative_or_invalid_values() {
        assert_eq!(
            super::system_time_from_timespec(libc::timespec {
                tv_sec: -1,
                tv_nsec: 0,
            }),
            None
        );
        assert_eq!(
            super::system_time_from_timespec(libc::timespec {
                tv_sec: 0,
                tv_nsec: -1,
            }),
            None
        );
        assert_eq!(
            super::system_time_from_timespec(libc::timespec {
                tv_sec: 0,
                tv_nsec: 1_000_000_000,
            }),
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
