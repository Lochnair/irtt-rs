use std::{
    io,
    net::{SocketAddr, UdpSocket},
};

use crate::{metadata::ReceiveMeta, timing::ClientTimestamp};

#[cfg(all(target_os = "linux", feature = "ancillary"))]
mod linux;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReceivedDatagram {
    pub(crate) len: usize,
    pub(crate) received_at: ClientTimestamp,
    pub(crate) meta: ReceiveMeta,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReceivedDatagramFrom {
    pub(crate) len: usize,
    pub(crate) source: SocketAddr,
    pub(crate) received_at: ClientTimestamp,
    pub(crate) meta: ReceiveMeta,
}

#[cfg(not(all(target_os = "linux", feature = "ancillary")))]
pub(crate) fn recv_datagram(
    socket: &UdpSocket,
    buf: &mut [u8],
) -> Result<ReceivedDatagram, io::Error> {
    let len = socket.recv(buf)?;
    let received_at = ClientTimestamp::now();

    Ok(ReceivedDatagram {
        len,
        received_at,
        meta: ReceiveMeta::default(),
    })
}

#[cfg(not(all(target_os = "linux", feature = "ancillary")))]
pub(crate) fn recv_datagram_from(
    socket: &UdpSocket,
    buf: &mut [u8],
) -> Result<ReceivedDatagramFrom, io::Error> {
    let (len, source) = socket.recv_from(buf)?;
    let received_at = ClientTimestamp::now();

    Ok(ReceivedDatagramFrom {
        len,
        source,
        received_at,
        meta: ReceiveMeta::default(),
    })
}

#[cfg(all(target_os = "linux", feature = "ancillary"))]
pub(crate) fn recv_datagram(
    socket: &UdpSocket,
    buf: &mut [u8],
) -> Result<ReceivedDatagram, io::Error> {
    linux::recv_datagram(socket, buf)
}

#[cfg(all(target_os = "linux", feature = "ancillary"))]
pub(crate) fn recv_datagram_from(
    socket: &UdpSocket,
    buf: &mut [u8],
) -> Result<ReceivedDatagramFrom, io::Error> {
    linux::recv_datagram_from(socket, buf)
}

#[cfg(not(all(target_os = "linux", feature = "ancillary")))]
pub(crate) fn configure_receive_metadata(
    _socket: &UdpSocket,
    _remote: SocketAddr,
) -> io::Result<()> {
    Ok(())
}

#[cfg(all(target_os = "linux", feature = "ancillary"))]
pub(crate) fn configure_receive_metadata(socket: &UdpSocket, remote: SocketAddr) -> io::Result<()> {
    linux::configure_receive_metadata(socket, remote)
}

#[cfg(test)]
mod tests {
    use std::net::UdpSocket;

    use crate::{metadata::ReceiveMeta, receive::recv_datagram, timing::ClientTimestamp};

    fn connected_loopback_pair() -> (UdpSocket, UdpSocket) {
        let a = UdpSocket::bind("127.0.0.1:0").unwrap();
        let b = UdpSocket::bind("127.0.0.1:0").unwrap();
        a.connect(b.local_addr().unwrap()).unwrap();
        b.connect(a.local_addr().unwrap()).unwrap();
        (a, b)
    }

    #[test]
    fn fallback_receive_returns_length() {
        let (sender, receiver) = connected_loopback_pair();
        sender.send(b"hello").unwrap();

        let mut buf = [0_u8; 16];
        let datagram = recv_datagram(&receiver, &mut buf).unwrap();

        assert_eq!(datagram.len, 5);
        assert_eq!(&buf[..datagram.len], b"hello");
    }

    #[test]
    fn fallback_receive_returns_default_metadata() {
        let (sender, receiver) = connected_loopback_pair();
        sender.send(b"meta").unwrap();

        let mut buf = [0_u8; 16];
        let datagram = recv_datagram(&receiver, &mut buf).unwrap();

        assert_eq!(datagram.meta, ReceiveMeta::default());
    }

    #[test]
    fn fallback_receive_captures_timestamp_after_successful_receive() {
        let (sender, receiver) = connected_loopback_pair();
        sender.send(b"time").unwrap();

        let before = ClientTimestamp::now();
        let mut buf = [0_u8; 16];
        let datagram = recv_datagram(&receiver, &mut buf).unwrap();
        let after = ClientTimestamp::now();

        assert!(datagram.received_at.mono >= before.mono);
        assert!(datagram.received_at.mono <= after.mono);
    }
}
