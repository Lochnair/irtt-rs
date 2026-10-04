use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

use measurement_stats::CountWindow;

use crate::{
    core::CoreStats, loss::loss_stats, normalization::StatsEvent, LateReplyMode, SampleMode,
    Snapshot, StatsConfig,
};

type ArrivalKey = (Instant, usize);

// Capture the observed cumulative position before eviction can discard it.
// Packet ordinals also reveal holes left by time filtering backdated events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct DirectionalPosition {
    packet_events: u128,
    server_received: Option<u64>,
    server_observed_at: u128,
    counter_reset_at: u128,
}

impl DirectionalPosition {
    fn advance(&mut self, event: &StatsEvent) -> bool {
        match event {
            StatsEvent::UniqueReply { sample, .. } => {
                if let Some(count) = sample.received_count {
                    let count = u64::from(count);
                    // A jump across half the 32-bit range can be a wrap or an
                    // old reply from the other side of a wrap. Start a new
                    // observation segment without guessing an epoch. Windows
                    // spanning the discontinuity have no directional estimate.
                    if self
                        .server_received
                        .is_some_and(|current| current.abs_diff(count) >= (1_u64 << 31))
                    {
                        self.server_received = None;
                        self.counter_reset_at = self.packet_events + 1;
                    }
                    if self.server_received.is_none_or(|current| count >= current) {
                        self.server_received = Some(count);
                        self.server_observed_at = self.packet_events + 1;
                    }
                }
            }
            StatsEvent::Sent { .. }
            | StatsEvent::DuplicateReply { .. }
            | StatsEvent::UntrackedLate { .. } => {}
            StatsEvent::Loss { .. } | StatsEvent::Warning { .. } => return false,
        }
        self.packet_events += 1;
        true
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RollingEvent {
    event: StatsEvent,
    before: DirectionalPosition,
}

#[derive(Debug, Clone)]
pub(crate) struct RollingEvents {
    time_limit: Option<Duration>,
    count_events: Option<CountWindow<RollingEvent>>,
    // Both indexes contain exactly the live events; expiry removes from both.
    time_events: Option<BTreeMap<ArrivalKey, RollingEvent>>,
    time_expiry: BTreeSet<(Instant, ArrivalKey)>,
    time_anchor: Option<Instant>,
    late_replies: LateReplyMode,
    directional_position: DirectionalPosition,
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
            && self.directional_position == other.directional_position
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
            directional_position: DirectionalPosition::default(),
        }
    }

    pub(crate) fn push(&mut self, event: StatsEvent) {
        if self.count_events.is_none() && self.time_events.is_none() {
            return;
        }
        let at = event.at();
        let before = self.directional_position;
        self.directional_position.advance(&event);
        let event = RollingEvent { event, before };
        if let Some(window) = self.count_events.as_mut() {
            window.push(event.clone());
        }

        if let (Some(duration), Some(window)) = (self.time_limit, self.time_events.as_mut()) {
            let anchor = self.time_anchor.map_or(at, |previous| previous.max(at));
            self.time_anchor = Some(anchor);
            let cutoff = anchor.checked_sub(duration);
            if let Some(cutoff) = cutoff {
                // Expiry uses timestamp order; replay keeps arrival order.
                while self.time_expiry.first().is_some_and(|(at, _)| *at < cutoff) {
                    let (_, arrival) = self.time_expiry.pop_first().unwrap();
                    window.remove(&arrival);
                }
                if at < cutoff {
                    return;
                }
            }
            // Within one anchor, no accepted event expires, so length orders
            // arrivals. A newer anchor sorts after all earlier arrivals even
            // when expiry reduces the length. No lifetime counter can wrap.
            let arrival = (anchor, window.len());
            self.time_expiry.insert((at, arrival));
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
/// cumulative collector, except directional loss uses an interval-relative
/// server count. Raw server counters and all other metrics still replay normally.
fn snapshot_window<'a>(
    events: impl Iterator<Item = &'a RollingEvent>,
    late_replies: LateReplyMode,
) -> Snapshot {
    let mut core = CoreStats::new(SampleMode::RunningOnly, late_replies);
    let mut start = None;
    let mut end: Option<DirectionalPosition> = None;
    let mut contiguous = true;
    for event in events {
        let mut after = event.before;
        if after.advance(&event.event) {
            start.get_or_insert(event.before);
            if end.is_some_and(|previous| previous.packet_events != event.before.packet_events) {
                contiguous = false;
            }
            end = Some(after);
        }
        core.apply(event.event.clone());
    }
    let server_delta = start.zip(end).and_then(|(start, end)| {
        if !contiguous || start.counter_reset_at != end.counter_reset_at {
            return None;
        }
        let baseline = if start.packet_events == 0 {
            0
        } else if start.server_observed_at == start.packet_events {
            start.server_received?
        } else {
            // Packet events after the observation but outside the window can
            // have advanced the server count by an unknown amount.
            return None;
        };
        end.server_received.map(|count| count - baseline)
    });
    let mut snapshot = core.snapshot();
    snapshot.loss = loss_stats(snapshot.packets, server_delta);
    snapshot
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
