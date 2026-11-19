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
    let capacity = buf.len();
    let len = truncated_receive_len(socket.recv(buf), capacity)?;
    let received_at = ClientTimestamp::now();

    Ok(ReceivedDatagram {
        len,
        received_at,
        meta: ReceiveMeta::default(),
    })
}

/// Reports a datagram larger than the receive buffer as a full buffer.
///
/// Unix returns such a datagram truncated to the buffer. Windows fails the
/// receive with `WSAEMSGSIZE` instead, after filling the buffer with the
/// datagram's leading bytes. Either way the buffer holds `capacity` bytes, and
/// the receive buffer is sized one byte past the longest valid reply, so the
/// session classifies the result as an overlong packet and warns, rather than
/// an unsolicited datagram failing the receive.
#[cfg(windows)]
fn truncated_receive_len(received: io::Result<usize>, capacity: usize) -> io::Result<usize> {
    const WSAEMSGSIZE: i32 = 10040;
    match received {
        Err(error) if error.raw_os_error() == Some(WSAEMSGSIZE) => Ok(capacity),
        received => received,
    }
}

#[cfg(all(not(windows), not(all(target_os = "linux", feature = "ancillary"))))]
fn truncated_receive_len(received: io::Result<usize>, _capacity: usize) -> io::Result<usize> {
    received
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
    let capacity = buf.len();
    let len = truncated_receive_len(socket.try_recv(buf), capacity)?;
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
