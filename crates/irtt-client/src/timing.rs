use std::time::{Instant, SystemTime};

/// Client-side timestamp captured in both wall-clock and monotonic time.
///
/// Wall-clock time is used for protocol timestamp comparison and one-way delay
/// calculations. Monotonic time is used for elapsed client-side timing such as
/// RTT, scheduling, and timeouts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientTimestamp {
    /// System wall-clock time.
    pub wall: SystemTime,
    /// Monotonic timestamp sampled alongside [`wall`](Self::wall).
    pub mono: Instant,
}

impl ClientTimestamp {
    /// Capture the current wall-clock and monotonic timestamps.
    pub fn now() -> Self {
        Self {
            wall: SystemTime::now(),
            mono: Instant::now(),
        }
    }
}
