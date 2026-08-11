use std::net::SocketAddr;

use irtt_proto::Params;

/// A live session created by an accepted open request.
///
/// This slice records only what identifies the session and what was negotiated
/// for it. Echo receive state (counts, windows, sequence tracking) and lifetime
/// state (activity, deadlines, expiry) belong to the slices that implement
/// those behaviors, and are deliberately absent rather than present as
/// placeholders.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    peer: SocketAddr,
    params: Params,
}

impl Session {
    pub(crate) fn new(peer: SocketAddr, params: Params) -> Self {
        Self { peer, params }
    }

    /// The exact source endpoint the open request arrived from.
    ///
    /// A session is bound to this endpoint, not merely to its address: address
    /// family, address, UDP source port and — for IPv6 — the scope identifier
    /// all form part of its identity. Echo and close processing, once
    /// implemented, must require both a token match and an exact endpoint match
    /// before touching a session.
    #[must_use]
    pub fn peer(&self) -> SocketAddr {
        self.peer
    }

    /// The negotiated parameters the server returned for this session and will
    /// enforce for it.
    #[must_use]
    pub fn params(&self) -> &Params {
        &self.params
    }
}
