use std::{collections::VecDeque, time::Duration};

use crate::{CoreStats, SampleMode, Snapshot, StatsConfig, StatsEvent};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RollingEvents {
    count_limit: Option<usize>,
    time_limit: Option<Duration>,
    count_events: Option<VecDeque<StatsEvent>>,
    time_events: Option<VecDeque<StatsEvent>>,
}

impl RollingEvents {
    pub(crate) fn new(config: StatsConfig) -> Self {
        Self {
            count_limit: config.rolling_count,
            time_limit: config.rolling_time,
            count_events: config.rolling_count.map(|_| VecDeque::new()),
            time_events: config.rolling_time.map(|_| VecDeque::new()),
        }
    }

    pub(crate) fn push(&mut self, event: StatsEvent) {
        if let (Some(limit), Some(window)) = (self.count_limit, self.count_events.as_mut()) {
            window.push_back(event.clone());
            while window.len() > limit {
                window.pop_front();
            }
        }

        if let (Some(duration), Some(window)) = (self.time_limit, self.time_events.as_mut()) {
            let cutoff = event.at().checked_sub(duration);
            window.push_back(event);
            if let Some(cutoff) = cutoff {
                while window.front().is_some_and(|event| event.at() < cutoff) {
                    window.pop_front();
                }
            }
        }
    }

    pub(crate) fn count_snapshot(&self) -> Option<Snapshot> {
        self.count_events.as_ref().map(snapshot_window)
    }

    pub(crate) fn time_snapshot(&self) -> Option<Snapshot> {
        self.time_events.as_ref().map(snapshot_window)
    }
}

fn snapshot_window(events: &VecDeque<StatsEvent>) -> Snapshot {
    let mut core = CoreStats::new(SampleMode::RunningOnly);
    for event in events {
        core.apply(event.clone());
    }
    core.snapshot()
}
