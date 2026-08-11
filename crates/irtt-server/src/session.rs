use std::net::SocketAddr;

use irtt_proto::Params;

/// A live session created by an accepted open request.
///
/// This slice records only what identifies the session and what was negotiated
/// for it. Echo receive state (counts, windows, sequence tracking) and lifetime
/// state (activity, deadlines, expiry) belong to the slices that implement
/// those behaviors, and are deliberately absent rather than present as
/// placeholders.
///
/// Deliberately crate-private. The next slices are all but certain to add and
/// restructure fields here, and nothing outside the crate needs a session yet;
/// publishing internal session state before ECHO even exists would buy nothing
/// and commit us to a shape we are about to change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Session {
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
    //
    // Read only by tests until then. The endpoint has to be captured at open
    // time — it is not recoverable later — so this slice stores state whose
    // production consumer is the next one, and the tests prove it is stored
    // exactly.
    #[allow(dead_code)]
    pub(crate) fn peer(&self) -> SocketAddr {
        self.peer
    }

    /// The negotiated parameters the server returned for this session and will
    /// enforce for it.
    //
    // Also awaiting its production consumer: echo replies are built from the
    // negotiated params.
    #[allow(dead_code)]
    pub(crate) fn params(&self) -> &Params {
        &self.params
    }
}
