use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

use measurement_stats::CountWindow;

use crate::{
    core::CoreStats, normalization::StatsEvent, LateReplyMode, SampleMode, Snapshot, StatsConfig,
};

type ArrivalKey = (Instant, usize);

#[derive(Debug, Clone)]
pub(crate) struct RollingEvents {
    time_limit: Option<Duration>,
    count_events: Option<CountWindow<StatsEvent>>,
    // Both indexes contain exactly the live events; expiry removes from both.
    time_events: Option<BTreeMap<ArrivalKey, StatsEvent>>,
    time_expiry: BTreeSet<(Instant, ArrivalKey)>,
    time_anchor: Option<Instant>,
    late_replies: LateReplyMode,
}

impl PartialEq for RollingEvents {
    fn eq(&self, other: &Self) -> bool {
        // Arrival keys are bookkeeping, not retained events.
        let same_time_events = match (&self.time_events, &other.time_events) {
            (Some(left), Some(right)) => left.values().eq(right.values()),
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
            time_events: config.rolling_time.map(|_| BTreeMap::new()),
            time_expiry: BTreeSet::new(),
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
                    let (_, arrival) = self.time_expiry.pop_first().unwrap();
                    window.remove(&arrival);
                }
                if event.at() < cutoff {
                    return;
                }
            }
            // Within one anchor, no accepted event expires, so length orders
            // arrivals. A newer anchor sorts after all earlier arrivals even
            // when expiry reduces the length. No lifetime counter can wrap.
            let arrival = (anchor, window.len());
            self.time_expiry.insert((event.at(), arrival));
            window.insert(arrival, event);
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
            .map(|events| snapshot_window(events.values(), self.late_replies))
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

    // Snapshots alone cannot reveal storage or traversal over expired events.
    #[test]
    fn expired_interior_batches_are_reclaimed_while_the_front_stays_live() {
        let base = Instant::now();
        let warning = |seconds| StatsEvent::Warning {
            at: base + Duration::from_secs(seconds),
        };
        let mut rolling = RollingEvents::new(StatsConfig {
            rolling_time: Some(Duration::from_secs(100)),
            ..StatsConfig::continuous()
        });
        rolling.push(warning(100));
        for second in 0..64 {
            // A large batch sits exactly on the cutoff behind the live front.
            for _ in 0..2_048 {
                rolling.push(warning(second));
            }
            assert_eq!(
                rolling.time_snapshot().unwrap().events.warning_events,
                second + 1 + 2_048
            );
            rolling.push(warning(101 + second));
            // This now-expired arrival must not allocate another retained slot.
            rolling.push(warning(second));
            let live = usize::try_from(second + 2).unwrap();
            assert_eq!(rolling.time_events.as_ref().unwrap().len(), live);
            assert_eq!(rolling.time_expiry.len(), live);
            assert_eq!(
                rolling.time_snapshot().unwrap().events.warning_events,
                u64::try_from(live).unwrap()
            );
        }
        rolling.push(warning(1_000));
        assert_eq!(rolling.time_events.as_ref().unwrap().len(), 1);
        assert_eq!(rolling.time_expiry.len(), 1);
    }

    #[test]
    fn chronological_stream_retains_only_the_current_window() {
        let base = Instant::now();
        let window_ms = 16_384;
        let mut rolling = RollingEvents::new(StatsConfig {
            rolling_time: Some(Duration::from_millis(window_ms)),
            ..StatsConfig::continuous()
        });
        // Fill a substantial uncapped window, then run through several expiries.
        for at_ms in 0..4 * window_ms {
            rolling.push(StatsEvent::Warning {
                at: base + Duration::from_millis(at_ms),
            });
            let live = usize::try_from(at_ms.min(window_ms) + 1).unwrap();
            assert_eq!(rolling.time_events.as_ref().unwrap().len(), live);
            assert_eq!(rolling.time_expiry.len(), live);
        }
        assert_eq!(
            rolling.time_snapshot().unwrap().events.warning_events,
            window_ms + 1
        );
    }
}
