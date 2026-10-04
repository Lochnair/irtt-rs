use std::{error::Error, fmt, net::SocketAddr, time::Duration};

use crate::{ClientError, ClientEvent, ClientTimestamp};

/// An Echo datagram accepted by the UDP socket and committed in session state.
///
/// The wire sequence and sent count have advanced, and the probe is pending.
/// This receipt remains authoritative even if subsequent validation or kernel
/// TX timestamp processing fails. It carries transport facts only; managed
/// scheduling annotations belong on the corresponding [`ClientEvent::EchoSent`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendReceipt {
    /// Committed wire sequence number.
    pub seq: u32,
    /// Resolved remote socket address.
    pub remote: SocketAddr,
    /// Public measurement timestamp sampled immediately after socket acceptance.
    /// The private pre-send anchor still determines the timeout deadline.
    pub sent_at: ClientTimestamp,
    /// Number of bytes the socket reported accepting.
    pub bytes: usize,
    /// Time spent in the successful socket send call.
    pub send_call: Duration,
}

impl From<SendReceipt> for ClientEvent {
    /// Construct the committed send event without scheduling annotations.
    fn from(receipt: SendReceipt) -> Self {
        Self::EchoSent {
            seq: receipt.seq,
            remote: receipt.remote,
            scheduled_at: None,
            sent_at: receipt.sent_at,
            bytes: receipt.bytes,
            send_call: receipt.send_call,
            timer_error: None,
        }
    }
}

/// Probe-send failure with an explicit socket-acceptance commitment boundary.
///
/// There is deliberately no blanket conversion from [`ClientError`]: each send
/// failure must be classified at the point where its commitment is known.
#[derive(Debug)]
pub enum SendProbeError {
    /// No datagram was accepted and no probe state or sequence was committed.
    NotCommitted(ClientError),
    /// The probe was committed, then post-send processing failed.
    /// The pending probe and advanced sequence/count remain committed.
    AfterCommit {
        /// Authoritative receipt for the accepted send.
        receipt: SendReceipt,
        /// Operational cause of the post-send failure.
        source: Box<ClientError>,
    },
}

impl fmt::Display for SendProbeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotCommitted(source) => write!(f, "probe send was not committed: {source}"),
            Self::AfterCommit { source, .. } => {
                write!(
                    f,
                    "probe send was committed, but post-send processing failed: {source}"
                )
            }
        }
    }
}

impl Error for SendProbeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(match self {
            Self::NotCommitted(source) => source,
            Self::AfterCommit { source, .. } => source.as_ref(),
        })
    }
}
