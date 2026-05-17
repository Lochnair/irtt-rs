use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// Cloneable cooperative cancellation flag.
///
/// Managed sessions use this internally to ask their worker loop to stop. It is
/// exported for callers that want the same simple cancellation primitive in
/// code built around `irtt-client`.
#[derive(Debug, Clone, Default)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    /// Create a token in the non-cancelled state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark the token as cancelled.
    ///
    /// Cancellation is idempotent and is visible through all clones of the
    /// token.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    /// Return whether cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancel_is_idempotent_and_visible_through_clones() {
        let token = CancellationToken::new();
        let clone = token.clone();

        token.cancel();
        token.cancel();

        assert!(token.is_cancelled());
        assert!(clone.is_cancelled());
    }
}
