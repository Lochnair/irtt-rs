use std::{
    io,
    net::{SocketAddr, UdpSocket},
    time::SystemTime,
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

#[cfg(all(target_os = "linux", feature = "ancillary"))]
pub(crate) fn recv_datagram(
    socket: &UdpSocket,
    buf: &mut [u8],
) -> Result<ReceivedDatagram, io::Error> {
    linux::recv_datagram(socket, buf)
}

#[cfg(all(
    feature = "tokio",
    not(all(target_os = "linux", feature = "ancillary"))
))]
pub(crate) fn try_recv_tokio_datagram(
    socket: &tokio::net::UdpSocket,
    buf: &mut [u8],
) -> Result<ReceivedDatagram, io::Error> {
    let len = socket.try_recv(buf)?;
    let received_at = ClientTimestamp::now();

    Ok(ReceivedDatagram {
        len,
        received_at,
        meta: ReceiveMeta::default(),
    })
}

#[cfg(all(feature = "tokio", target_os = "linux", feature = "ancillary"))]
pub(crate) fn try_recv_tokio_datagram(
    socket: &tokio::net::UdpSocket,
    buf: &mut [u8],
) -> Result<ReceivedDatagram, io::Error> {
    linux::try_recv_tokio_datagram(socket, buf)
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

/// Best-effort upgrade a socket from RX-only to RX+TX kernel software
/// timestamping. Returns whether the upgrade succeeded; a caller should
/// track this to skip later drains entirely rather than perform harmless
/// no-op ones. Always returns `false` off Linux or without the `ancillary`
/// feature, so callers never need their own platform `cfg`.
#[cfg(not(all(target_os = "linux", feature = "ancillary")))]
pub(crate) fn try_enable_tx_timestamping<S>(_socket: &S) -> bool {
    false
}

#[cfg(all(target_os = "linux", feature = "ancillary"))]
pub(crate) fn try_enable_tx_timestamping<S: std::os::fd::AsFd>(socket: &S) -> bool {
    linux::try_enable_tx_timestamping(socket)
}

/// Bounded, nonblocking drain of `socket`'s `MSG_ERRQUEUE`, reporting each
/// usable TX timestamp completion to `on_timestamp`. Always a no-op off
/// Linux or without the `ancillary` feature.
#[cfg(not(all(target_os = "linux", feature = "ancillary")))]
pub(crate) fn drain_tx_timestamps<S>(
    _socket: &S,
    _on_timestamp: impl FnMut(u32, SystemTime),
) -> io::Result<()> {
    Ok(())
}

#[cfg(all(target_os = "linux", feature = "ancillary"))]
pub(crate) fn drain_tx_timestamps<S: std::os::fd::AsRawFd>(
    socket: &S,
    on_timestamp: impl FnMut(u32, SystemTime),
) -> io::Result<()> {
    linux::drain_tx_timestamps(socket, on_timestamp)
}
