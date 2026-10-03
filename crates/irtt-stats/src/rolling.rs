use std::{
    collections::{BTreeSet, VecDeque},
    time::{Duration, Instant},
};

use measurement_stats::CountWindow;

use crate::{
    core::CoreStats, normalization::StatsEvent, LateReplyMode, SampleMode, Snapshot, StatsConfig,
};

#[derive(Debug, Clone)]
pub(crate) struct RollingEvents {
    time_limit: Option<Duration>,
    count_events: Option<CountWindow<StatsEvent>>,
    time_events: Option<VecDeque<Option<StatsEvent>>>,
    time_expiry: BTreeSet<(Instant, usize)>,
    time_front: usize,
    time_anchor: Option<Instant>,
    late_replies: LateReplyMode,
}

impl PartialEq for RollingEvents {
    fn eq(&self, other: &Self) -> bool {
        // Expiry ordinals and tombstones are bookkeeping, not retained events.
        let same_time_events = match (&self.time_events, &other.time_events) {
            (Some(left), Some(right)) => left.iter().flatten().eq(right.iter().flatten()),
            (None, None) => true,
            _ => false,
        };
        self.time_limit == other.time_limit
            && self.count_events == other.count_events
            && self.time_anchor == other.time_anchor
            && self.late_replies == other.late_replies
            && same_time_events
    }
}

impl RollingEvents {
    pub(crate) fn new(config: StatsConfig) -> Self {
        Self {
            time_limit: config.rolling_time,
            count_events: config.rolling_count.map(CountWindow::new),
            time_events: config.rolling_time.map(|_| VecDeque::new()),
            time_expiry: BTreeSet::new(),
            time_front: 0,
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
            if let Some(cutoff) = cutoff {
                // Expiry uses timestamp order; replay keeps arrival order.
                while self.time_expiry.first().is_some_and(|(at, _)| *at < cutoff) {
                    let (_, ordinal) = self.time_expiry.pop_first().unwrap();
                    window[ordinal.wrapping_sub(self.time_front)] = None;
                }
                while window.front().is_some_and(Option::is_none) {
                    window.pop_front();
                    self.time_front = self.time_front.wrapping_add(1);
                }
                if event.at() < cutoff {
                    return;
                }
            }
            // Logical deque positions allow constant-time expiry lookup. The
            // queue cannot hold enough slots for live ordinals to alias, so
            // wrapping arithmetic also handles a long-running ordinal rollover.
            let ordinal = self.time_front.wrapping_add(window.len());
            self.time_expiry.insert((event.at(), ordinal));
            window.push_back(Some(event));
        }
    }

    pub(crate) fn count_snapshot(&self) -> Option<Snapshot> {
        self.count_events
            .as_ref()
            .map(|events| snapshot_window(events.iter(), self.late_replies))
    }

    pub(crate) fn time_snapshot(&self) -> Option<Snapshot> {
        self.time_events.as_ref().map(|events| {
            snapshot_window(events.iter().filter_map(Option::as_ref), self.late_replies)
        })
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

#[cfg(test)]
mod tests {
    use super::*;

    // Storage reclamation and ordinal rollover cannot be observed in snapshots.
    #[test]
    fn expired_storage_is_reclaimed_across_ordinal_rollover() {
        let base = Instant::now();
        let mut rolling = RollingEvents::new(StatsConfig {
            rolling_time: Some(Duration::from_millis(10)),
            ..StatsConfig::continuous()
        });
        rolling.time_front = usize::MAX - 1;
        for cycle in 1..=100 {
            let anchor_ms = cycle * 10;
            // Each cycle hides an older event behind a newer one, then expires
            // it while that newer event is still live.
            for at_ms in [anchor_ms, anchor_ms - 9, anchor_ms + 2] {
                rolling.push(StatsEvent::Warning {
                    at: base + Duration::from_millis(at_ms),
                });
            }
            // Both stores must stay tied to recent volume, not total arrivals.
            assert!(rolling.time_events.as_ref().unwrap().len() <= 6);
            assert!(rolling.time_expiry.len() <= 6);
            assert_eq!(
                rolling.time_snapshot().unwrap().events.warning_events,
                2 + u64::from(cycle > 1)
            );
        }
        rolling.push(StatsEvent::Warning {
            at: base + Duration::from_millis(2_000),
        });
        assert_eq!(rolling.time_events.as_ref().unwrap().len(), 1);
        assert_eq!(rolling.time_expiry.len(), 1);
    }
}
