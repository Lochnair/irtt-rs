use std::{
    io,
    net::{SocketAddr, UdpSocket},
    time::SystemTime,
};

use crate::{metadata::ReceiveMeta, timing::ClientTimestamp, ClientError, TimestampMode};

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
    _rx_timestamping: bool,
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
    rx_timestamping: bool,
) -> Result<ReceivedDatagram, io::Error> {
    linux::recv_datagram(socket, buf, rx_timestamping)
}

#[cfg(all(
    feature = "tokio",
    not(all(target_os = "linux", feature = "ancillary"))
))]
pub(crate) fn try_recv_tokio_datagram(
    socket: &tokio::net::UdpSocket,
    buf: &mut [u8],
    _rx_timestamping: bool,
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
    rx_timestamping: bool,
) -> Result<ReceivedDatagram, io::Error> {
    linux::try_recv_tokio_datagram(socket, buf, rx_timestamping)
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

pub(crate) fn configure_rx_timestamping(
    socket: &UdpSocket,
    remote: SocketAddr,
    mode: TimestampMode,
) -> Result<bool, ClientError> {
    initialize_timestamping(
        mode,
        remote,
        "enable kernel RX timestamping (use --kernel-rx-timestamp=auto to allow fallback)",
        || set_rx_timestamping(socket),
    )
}

#[cfg(all(target_os = "linux", feature = "ancillary"))]
fn set_rx_timestamping(socket: &UdpSocket) -> io::Result<()> {
    linux::set_rx_timestamping(socket)
}

#[cfg(not(all(target_os = "linux", feature = "ancillary")))]
fn set_rx_timestamping(_socket: &UdpSocket) -> io::Result<()> {
    unsupported_timestamping()
}

#[cfg(not(all(target_os = "linux", feature = "ancillary")))]
fn unsupported_timestamping() -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "kernel timestamping requires Linux and the ancillary feature",
    ))
}

pub(crate) fn initialize_timestamping(
    mode: TimestampMode,
    remote: SocketAddr,
    operation: &'static str,
    initialize: impl FnOnce() -> io::Result<()>,
) -> Result<bool, ClientError> {
    if mode == TimestampMode::Off {
        return Ok(false);
    }
    match initialize() {
        Ok(()) => Ok(true),
        Err(_) if mode == TimestampMode::Auto => Ok(false),
        Err(source) => Err(ClientError::SocketOption {
            operation,
            remote,
            source,
        }),
    }
}

/// Configure TX generation without changing the successfully configured RX policy.
#[cfg(all(target_os = "linux", feature = "ancillary"))]
pub(crate) fn configure_tx_timestamping<S: std::os::fd::AsFd>(
    socket: &S,
    remote: SocketAddr,
    mode: TimestampMode,
    rx_timestamping: bool,
) -> Result<bool, ClientError> {
    initialize_timestamping(
        mode,
        remote,
        "enable kernel TX timestamping (use --kernel-tx-timestamp=auto to allow fallback)",
        || linux::set_tx_timestamping(socket, rx_timestamping),
    )
}

#[cfg(not(all(target_os = "linux", feature = "ancillary")))]
pub(crate) fn configure_tx_timestamping<S>(
    _socket: &S,
    remote: SocketAddr,
    mode: TimestampMode,
    _rx_timestamping: bool,
) -> Result<bool, ClientError> {
    initialize_timestamping(
        mode,
        remote,
        "enable kernel TX timestamping (use --kernel-tx-timestamp=auto to allow fallback)",
        unsupported_timestamping,
    )
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
