use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use measurement_stats::CountWindow;

use crate::{
    core::CoreStats, normalization::StatsEvent, LateReplyMode, SampleMode, Snapshot, StatsConfig,
};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RollingEvents {
    time_limit: Option<Duration>,
    count_events: Option<CountWindow<StatsEvent>>,
    time_events: Option<VecDeque<StatsEvent>>,
    time_anchor: Option<Instant>,
    late_replies: LateReplyMode,
}

impl RollingEvents {
    pub(crate) fn new(config: StatsConfig) -> Self {
        Self {
            time_limit: config.rolling_time,
            count_events: config.rolling_count.map(CountWindow::new),
            time_events: config.rolling_time.map(|_| VecDeque::new()),
            time_anchor: None,
            late_replies: config.late_replies,
        }
    }

    pub(crate) fn push(&mut self, event: StatsEvent) {
        if let Some(window) = self.count_events.as_mut() {
            window.push(event.clone());
        }

        if let (Some(duration), Some(window)) = (self.time_limit, self.time_events.as_mut()) {
            let anchor = self.time_anchor.map_or(event.at(), |at| at.max(event.at()));
            self.time_anchor = Some(anchor);
            let cutoff = anchor.checked_sub(duration);
            window.push_back(event);
            if let Some(cutoff) = cutoff {
                // Delayed timeout discovery can backdate events behind newer
                // ones. Filter the whole window, preserving replay order.
                window.retain(|event| event.at() >= cutoff);
            }
        }
    }

    pub(crate) fn count_snapshot(&self) -> Option<Snapshot> {
        self.count_events
            .as_ref()
            .map(|events| snapshot_window(events.iter(), self.late_replies))
    }

    pub(crate) fn time_snapshot(&self) -> Option<Snapshot> {
        self.time_events
            .as_ref()
            .map(|events| snapshot_window(events.iter(), self.late_replies))
    }
}

/// Recompute a window snapshot under the same normalized semantics as the
/// cumulative collector, including its late-reply measurement policy.
fn snapshot_window<'a>(
    events: impl Iterator<Item = &'a StatsEvent>,
    late_replies: LateReplyMode,
) -> Snapshot {
    let mut core = CoreStats::new(SampleMode::RunningOnly, late_replies);
    for event in events {
        core.apply(event.clone());
    }
    core.snapshot()
}
