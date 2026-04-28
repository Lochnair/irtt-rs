use std::time::{Instant, SystemTime};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientTimestamp {
    pub wall: SystemTime,
    pub mono: Instant,
}

impl ClientTimestamp {
    pub fn now() -> Self {
        Self {
            wall: SystemTime::now(),
            mono: Instant::now(),
        }
    }
}
