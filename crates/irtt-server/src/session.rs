use std::net::SocketAddr;

use irtt_proto::Params;

/// A live session created by an accepted open request.
///
/// A session records what identifies it, what was negotiated for it, and what
/// its accepted echo requests have added up to. Lifetime state (activity,
/// deadlines, expiry) and rate state belong to the slices that implement those
/// behaviors, and are deliberately absent rather than present as placeholders.
///
/// Deliberately crate-private. The remaining slices are all but certain to add
/// and restructure fields here, and nothing outside the crate needs a session;
/// publishing internal session state would buy nothing and commit us to a shape
/// we are about to change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Session {
    peer: SocketAddr,
    params: Params,
    receive: ReceiveState,
}

impl Session {
    pub(crate) fn new(peer: SocketAddr, params: Params) -> Self {
        Self {
            peer,
            params,
            receive: ReceiveState::default(),
        }
    }

    /// The exact source endpoint the open request arrived from.
    ///
    /// A session is bound to this endpoint, not merely to its address: the
    /// address family, the address and the UDP source port all form part of its
    /// identity, as — by `irtt-rs` policy rather than observed behavior — does
    /// the IPv6 scope identifier. Close and echo processing both require a
    /// token match *and* an endpoint match before touching the session;
    /// `same_endpoint` in `core` is where the comparison and its reasoning
    /// live.
    ///
    /// The address is stored exactly as received, including the flow label that
    /// identity ignores, so a reply goes back to the endpoint the request
    /// actually came from.
    pub(crate) fn peer(&self) -> SocketAddr {
        self.peer
    }

    /// The negotiated parameters the server returned for this session and
    /// enforces for it.
    ///
    /// Echo replies are laid out and sized from these, never from the request
    /// that provoked them.
    pub(crate) fn params(&self) -> &Params {
        &self.params
    }

    /// What this session's accepted echo requests have added up to so far.
    pub(crate) fn receive_state(&self) -> ReceiveState {
        self.receive
    }

    /// Adopts the state computed for an accepted echo request.
    ///
    /// Kept separate from [`ReceiveState::accepted`] on purpose. The transition
    /// is pure and this is the irreversible step, so echo handling runs every
    /// fallible part — building and encoding the reply — before calling it, and
    /// a reply that fails to encode leaves the session exactly as it was.
    pub(crate) fn commit_receive_state(&mut self, next: ReceiveState) {
        self.receive = next;
    }
}

/// What a session's accepted echo requests have added up to.
///
/// The semantics are those of the clean server specification, Section 10, and
/// the input → output vectors in `test-vectors/SERVER_BEHAVIORAL_VECTORS.md`
/// Section 1. Both values are per session, start at zero, and move only for a
/// request that reaches the reply stage: a datagram dropped for its size, its
/// authentication, an unknown token or a foreign endpoint leaves them alone.
///
/// The state is maintained whatever the session negotiated. Which of the two
/// values actually appears in a reply is a separate question, answered by the
/// negotiated `ReceivedStats` when the reply is built.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct ReceiveState {
    /// Accepted echo requests, including the one currently being answered.
    pub(crate) count: u32,
    /// The received window: bit 0 is the request being answered and bit *k* is
    /// the request with sequence number (current − *k*), for 1 ≤ *k* ≤ 63.
    pub(crate) window: u64,
    /// The sequence number the previous accepted request carried, which the
    /// next transition is measured against. `None` before the session's first.
    last_sequence: Option<u32>,
}

impl ReceiveState {
    /// The state after accepting a request carrying `sequence`.
    ///
    /// Pure, so the caller can compute the next state, build and encode a reply
    /// from it, and only then commit it.
    ///
    /// The window transition is the observed one, and it is **not** a
    /// selective-acknowledgement bitmap: history is shifted along by the
    /// unsigned distance from the previous accepted sequence number, and a
    /// distance it cannot represent throws all of it away. The consequence is
    /// the distinctive part — because the distance is unsigned, a *late or
    /// reordered* request produces a very large one, so it resets the window to
    /// `0x1` instead of setting an old bit, and the history it discards is
    /// never recovered. That late sequence number then becomes the reference
    /// for the next transition like any other.
    pub(crate) fn accepted(self, sequence: u32) -> Self {
        let window = match self.last_sequence {
            // The first accepted request of a session reports only itself,
            // whatever its sequence number.
            None => 0x1,
            Some(last) => match sequence.wrapping_sub(last) {
                // A duplicate of the most recent sequence number leaves the
                // window unchanged, since bit 0 is already set for it.
                0 => self.window | 1,
                // In order, or a gap the window can still represent: shift the
                // history along by the distance and record this request.
                distance @ 1..=63 => (self.window << distance) | 1,
                // A gap of 64 or more discards every earlier bit — and so does
                // any late or reordered request, which lands here because the
                // unsigned distance wrapped.
                _ => 0x1,
            },
        };
        Self {
            // The wire field is 32 bits. No clean evidence reaches 2^32
            // requests in one session, so the wrap is not verified behavior: it
            // is simply what maintaining a fixed-width counter means, and it is
            // chosen over a debug-build overflow panic a peer could drive.
            count: self.count.wrapping_add(1),
            window,
            last_sequence: Some(sequence),
        }
    }
}
