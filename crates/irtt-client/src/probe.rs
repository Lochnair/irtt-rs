use std::{
    collections::{HashMap, HashSet, TryReserveError, VecDeque},
    time::{Instant, SystemTime},
};

use crate::{error::ClientError, timing::ClientTimestamp};

#[derive(Debug, Clone)]
pub(crate) struct PendingProbe {
    pub wire_seq: u32,
    pub sent_at: ClientTimestamp,
    pub timeout_at: Instant,
    /// Observed Linux kernel TX_SOFTWARE wall timestamp for this probe's
    /// send, when the socket has TX timestamping enabled and a matching
    /// `MSG_ERRQUEUE` record has been drained. Dormant metadata only: no
    /// measurement in this change reads it. `sent_at` remains authoritative
    /// for RTT, timeout, and current one-way delay.
    pub kernel_tx_timestamp: Option<SystemTime>,
}

#[derive(Debug)]
struct PendingEntry {
    probe: PendingProbe,
    previous: Option<u32>,
    next: Option<u32>,
}

#[derive(Debug)]
pub(crate) struct ExpiredBatch {
    pub probes: Vec<PendingProbe>,
    pub more_due: bool,
}

#[derive(Debug)]
pub(crate) struct PendingMap {
    map: HashMap<u32, PendingEntry>,
    first: Option<u32>,
    last: Option<u32>,
    max_capacity: usize,
}

impl PendingMap {
    pub fn new(max_capacity: usize) -> Self {
        Self {
            map: HashMap::new(),
            first: None,
            last: None,
            max_capacity,
        }
    }

    pub fn preflight_insert(&mut self, wire_seq: u32) -> Result<(), ClientError> {
        if self.map.contains_key(&wire_seq) {
            return Err(ClientError::PendingSequenceCollision { seq: wire_seq });
        }
        if self.map.len() >= self.max_capacity {
            return Err(ClientError::PendingLimitExceeded {
                limit: self.max_capacity,
            });
        }
        if self.map.len() == self.map.capacity() {
            self.map.try_reserve(1).map_err(pending_allocation_failed)?;
        }
        Ok(())
    }

    pub fn commit_insert(&mut self, probe: PendingProbe) {
        let wire_seq = probe.wire_seq;
        let previous = self.last;
        if let Some(previous) = previous {
            let previous = self
                .map
                .get_mut(&previous)
                .expect("pending list tail remains present");
            // probe_timeout is fixed for an open session and committed sends
            // have nondecreasing monotonic timestamps, so deadlines append.
            debug_assert!(previous.probe.timeout_at <= probe.timeout_at);
            debug_assert!(
                previous.next.is_none(),
                "pending list tail has no successor"
            );
            previous.next = Some(wire_seq);
        } else {
            debug_assert!(self.first.is_none(), "empty pending list has no head");
            self.first = Some(wire_seq);
        }

        let replaced = self.map.insert(
            wire_seq,
            PendingEntry {
                probe,
                previous,
                next: None,
            },
        );
        debug_assert!(replaced.is_none(), "preflight rejected pending collision");
        self.last = Some(wire_seq);
        self.assert_links_consistent();
    }

    pub fn remove(&mut self, wire_seq: u32) -> Option<PendingProbe> {
        let PendingEntry {
            probe,
            previous,
            next,
        } = self.map.remove(&wire_seq)?;

        if let Some(previous) = previous {
            let previous = self
                .map
                .get_mut(&previous)
                .expect("pending list predecessor remains present");
            debug_assert_eq!(previous.next, Some(wire_seq));
            previous.next = next;
        } else {
            debug_assert_eq!(self.first, Some(wire_seq));
            self.first = next;
        }
        if let Some(next) = next {
            let next = self
                .map
                .get_mut(&next)
                .expect("pending list successor remains present");
            debug_assert_eq!(next.previous, Some(wire_seq));
            next.previous = previous;
        } else {
            debug_assert_eq!(self.last, Some(wire_seq));
            self.last = previous;
        }

        self.assert_links_consistent();
        Some(probe)
    }

    pub(crate) fn drain_expired_bounded(&mut self, now: Instant, limit: usize) -> ExpiredBatch {
        let mut probes = Vec::with_capacity(limit.min(self.map.len()));
        while probes.len() < limit {
            let Some(wire_seq) = self.first else {
                break;
            };
            let timeout_at = self
                .map
                .get(&wire_seq)
                .expect("pending list head remains present")
                .probe
                .timeout_at;
            if timeout_at > now {
                break;
            }

            let probe = self
                .remove(wire_seq)
                .expect("pending list head remains present");
            probes.push(probe);
        }
        let more_due = self
            .first
            .and_then(|wire_seq| self.map.get(&wire_seq))
            .is_some_and(|entry| entry.probe.timeout_at <= now);
        ExpiredBatch { probes, more_due }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Mutable access to a still-pending probe by wire sequence, without
    /// disturbing its position in the timeout order. Used to attach an
    /// observed kernel TX timestamp to a probe that has not yet completed or
    /// timed out.
    pub fn get_mut(&mut self, wire_seq: u32) -> Option<&mut PendingProbe> {
        self.map.get_mut(&wire_seq).map(|entry| &mut entry.probe)
    }

    #[cfg(any(feature = "tokio", test))]
    pub fn next_timeout_deadline(&self) -> Option<Instant> {
        self.first
            .and_then(|wire_seq| self.map.get(&wire_seq))
            .map(|entry| entry.probe.timeout_at)
    }

    #[cfg(any(feature = "tokio", test))]
    pub fn latest_timeout_deadline(&self) -> Option<Instant> {
        self.last
            .and_then(|wire_seq| self.map.get(&wire_seq))
            .map(|entry| entry.probe.timeout_at)
    }

    fn assert_links_consistent(&self) {
        debug_assert_eq!(self.first.is_none(), self.map.is_empty());
        debug_assert_eq!(self.last.is_none(), self.map.is_empty());
        if let Some(first) = self.first {
            debug_assert_eq!(
                self.map
                    .get(&first)
                    .expect("pending list head remains present")
                    .previous,
                None
            );
        }
        if let Some(last) = self.last {
            debug_assert_eq!(
                self.map
                    .get(&last)
                    .expect("pending list tail remains present")
                    .next,
                None
            );
        }
    }

    #[cfg(test)]
    pub fn contains(&self, wire_seq: u32) -> bool {
        self.map.contains_key(&wire_seq)
    }

    #[cfg(test)]
    pub fn capacity(&self) -> usize {
        self.map.capacity()
    }

    #[cfg(test)]
    pub fn linkage(&self) -> (Option<u32>, Option<u32>) {
        (self.first, self.last)
    }

    #[cfg(test)]
    pub fn entry_links(&self, wire_seq: u32) -> Option<(Option<u32>, Option<u32>)> {
        self.map
            .get(&wire_seq)
            .map(|entry| (entry.previous, entry.next))
    }
}

fn pending_allocation_failed(source: TryReserveError) -> ClientError {
    ClientError::AllocationFailed {
        operation: "pending probe storage",
        source,
    }
}

#[derive(Debug)]
pub(crate) struct TimedOutMap {
    map: HashMap<u32, PendingProbe>,
    insertion_order: VecDeque<u32>,
    max_capacity: usize,
}

impl TimedOutMap {
    pub fn new(max_capacity: usize) -> Self {
        Self {
            map: HashMap::new(),
            insertion_order: VecDeque::new(),
            max_capacity,
        }
    }

    pub fn insert(&mut self, probe: PendingProbe) {
        if self.max_capacity == 0 {
            return;
        }
        if let std::collections::hash_map::Entry::Occupied(mut entry) =
            self.map.entry(probe.wire_seq)
        {
            entry.insert(probe);
            return;
        }
        while self.map.len() >= self.max_capacity {
            self.evict_oldest();
        }
        self.insertion_order.push_back(probe.wire_seq);
        self.map.insert(probe.wire_seq, probe);
    }

    pub fn remove(&mut self, wire_seq: u32) -> Option<PendingProbe> {
        let removed = self.map.remove(&wire_seq);
        if removed.is_some() {
            self.insertion_order.retain(|seq| *seq != wire_seq);
        }
        removed
    }

    /// Mutable access to a still-retained timed-out probe by wire sequence,
    /// without disturbing eviction order. Used to attach an observed kernel
    /// TX timestamp that arrives after the probe has already timed out.
    pub fn get_mut(&mut self, wire_seq: u32) -> Option<&mut PendingProbe> {
        self.map.get_mut(&wire_seq)
    }

    pub fn clear(&mut self) {
        self.map.clear();
        self.insertion_order.clear();
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    #[cfg(feature = "tokio")]
    pub fn latest_timeout_deadline(&self) -> Option<Instant> {
        self.map.values().map(|probe| probe.timeout_at).max()
    }

    #[cfg(test)]
    pub fn contains(&self, wire_seq: u32) -> bool {
        self.map.contains_key(&wire_seq)
    }

    #[cfg(test)]
    fn insertion_order_len(&self) -> usize {
        self.insertion_order.len()
    }

    fn evict_oldest(&mut self) {
        while let Some(oldest_key) = self.insertion_order.pop_front() {
            if self.map.remove(&oldest_key).is_some() {
                break;
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct CompletedSet {
    set: HashSet<u32>,
    insertion_order: VecDeque<u32>,
    max_capacity: usize,
}

impl CompletedSet {
    pub fn new(max_capacity: usize) -> Self {
        Self {
            set: HashSet::new(),
            insertion_order: VecDeque::new(),
            max_capacity,
        }
    }

    pub fn insert(&mut self, seq: u32) {
        if self.set.contains(&seq) {
            return;
        }

        if self.set.len() >= self.max_capacity {
            self.evict_oldest();
        }

        self.insertion_order.push_back(seq);
        self.set.insert(seq);
    }

    pub fn contains(&self, seq: u32) -> bool {
        self.set.contains(&seq)
    }

    pub fn remove(&mut self, seq: u32) -> bool {
        let removed = self.set.remove(&seq);
        if removed {
            self.insertion_order.retain(|entry| *entry != seq);
        }
        removed
    }

    fn evict_oldest(&mut self) {
        while let Some(oldest_seq) = self.insertion_order.pop_front() {
            if self.set.remove(&oldest_seq) {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime};

    fn ts(mono: Instant) -> ClientTimestamp {
        ClientTimestamp {
            mono,
            wall: SystemTime::now(),
        }
    }

    fn pending(seq: u32, timeout_at: Instant) -> PendingProbe {
        PendingProbe {
            wire_seq: seq,
            sent_at: ts(timeout_at - Duration::from_secs(1)),
            timeout_at,
            kernel_tx_timestamp: None,
        }
    }

    fn insert(map: &mut PendingMap, probe: PendingProbe) {
        map.preflight_insert(probe.wire_seq).unwrap();
        map.commit_insert(probe);
    }

    #[test]
    fn pending_map_rejects_over_capacity() {
        let mut map = PendingMap::new(2);
        let now = Instant::now();
        map.preflight_insert(0).unwrap();
        map.commit_insert(pending(0, now + Duration::from_secs(4)));
        map.preflight_insert(1).unwrap();
        map.commit_insert(pending(1, now + Duration::from_secs(4)));
        assert!(matches!(
            map.preflight_insert(2),
            Err(ClientError::PendingLimitExceeded { limit: 2 })
        ));
    }

    #[test]
    fn pending_map_preflight_reserves_without_changing_contents() {
        let mut map = PendingMap::new(2);
        let initial_capacity = map.capacity();

        map.preflight_insert(7).unwrap();
        let reserved_capacity = map.capacity();
        assert!(reserved_capacity >= initial_capacity);
        assert_eq!(map.len(), 0);
        assert!(!map.contains(7));

        map.preflight_insert(7).unwrap();
        assert_eq!(map.capacity(), reserved_capacity);
        assert_eq!(map.len(), 0);
        assert!(!map.contains(7));
    }

    #[test]
    fn pending_map_preflight_rejects_sequence_collision() {
        let mut map = PendingMap::new(2);
        let now = Instant::now();
        map.preflight_insert(9).unwrap();
        map.commit_insert(pending(9, now));

        assert!(matches!(
            map.preflight_insert(9),
            Err(ClientError::PendingSequenceCollision { seq: 9 })
        ));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn pending_map_allocation_failure_maps_to_dedicated_error() {
        let mut map = HashMap::<u32, PendingProbe>::new();
        let source = map.try_reserve(usize::MAX).unwrap_err();

        assert!(matches!(
            pending_allocation_failed(source),
            ClientError::AllocationFailed {
                operation: "pending probe storage",
                ..
            }
        ));
    }

    #[test]
    fn pending_map_unlinks_head_middle_and_tail_and_updates_extrema() {
        let mut map = PendingMap::new(4);
        let now = Instant::now();
        insert(&mut map, pending(1, now + Duration::from_secs(1)));
        insert(&mut map, pending(2, now + Duration::from_secs(2)));
        insert(&mut map, pending(3, now + Duration::from_secs(3)));

        assert_eq!(
            map.next_timeout_deadline(),
            Some(now + Duration::from_secs(1))
        );
        assert_eq!(
            map.latest_timeout_deadline(),
            Some(now + Duration::from_secs(3))
        );
        assert_eq!(map.linkage(), (Some(1), Some(3)));
        assert_eq!(map.entry_links(1), Some((None, Some(2))));
        assert_eq!(map.entry_links(2), Some((Some(1), Some(3))));
        assert_eq!(map.entry_links(3), Some((Some(2), None)));

        assert!(map.remove(2).is_some());
        assert_eq!(
            map.next_timeout_deadline(),
            Some(now + Duration::from_secs(1))
        );
        assert_eq!(
            map.latest_timeout_deadline(),
            Some(now + Duration::from_secs(3))
        );
        assert_eq!(map.linkage(), (Some(1), Some(3)));
        assert_eq!(map.entry_links(1), Some((None, Some(3))));
        assert_eq!(map.entry_links(3), Some((Some(1), None)));

        assert!(map.remove(1).is_some());
        assert_eq!(
            map.next_timeout_deadline(),
            Some(now + Duration::from_secs(3))
        );
        assert_eq!(
            map.latest_timeout_deadline(),
            Some(now + Duration::from_secs(3))
        );
        assert_eq!(map.linkage(), (Some(3), Some(3)));
        assert_eq!(map.entry_links(3), Some((None, None)));

        assert!(map.remove(3).is_some());
        assert_eq!(map.next_timeout_deadline(), None);
        assert_eq!(map.latest_timeout_deadline(), None);
        assert_eq!(map.linkage(), (None, None));
    }

    #[test]
    fn pending_map_bounded_expiration_matches_exhaustive_order() {
        let now = Instant::now();
        let probes = [
            pending(1, now + Duration::from_secs(1)),
            pending(2, now + Duration::from_secs(2)),
            pending(3, now + Duration::from_secs(3)),
            pending(4, now + Duration::from_secs(4)),
        ];
        let mut bounded = PendingMap::new(probes.len());
        let mut exhaustive = PendingMap::new(probes.len());
        for probe in probes.iter().cloned() {
            insert(&mut bounded, probe.clone());
            insert(&mut exhaustive, probe);
        }

        let empty = bounded.drain_expired_bounded(now + Duration::from_secs(4), 0);
        assert!(empty.probes.is_empty());
        assert!(empty.more_due);
        assert_eq!(bounded.linkage(), (Some(1), Some(4)));

        let mut actual = Vec::new();
        loop {
            let batch = bounded.drain_expired_bounded(now + Duration::from_secs(4), 2);
            actual.extend(batch.probes.into_iter().map(|probe| probe.wire_seq));
            if !batch.more_due {
                break;
            }
        }
        let expected = exhaustive
            .drain_expired_bounded(now + Duration::from_secs(4), usize::MAX)
            .probes
            .into_iter()
            .map(|probe| probe.wire_seq)
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
        assert_eq!(actual, vec![1, 2, 3, 4]);
        assert_eq!(bounded.linkage(), (None, None));
    }

    #[test]
    fn pending_map_sequence_reuse_leaves_no_stale_link() {
        let mut map = PendingMap::new(1);
        let now = Instant::now();
        insert(&mut map, pending(7, now));
        assert!(map.remove(7).is_some());
        insert(&mut map, pending(7, now + Duration::from_secs(1)));

        assert!(map.drain_expired_bounded(now, usize::MAX).probes.is_empty());
        assert_eq!(map.linkage(), (Some(7), Some(7)));
        assert_eq!(map.entry_links(7), Some((None, None)));
        assert_eq!(
            map.drain_expired_bounded(now + Duration::from_secs(1), usize::MAX)
                .probes
                .into_iter()
                .map(|probe| probe.wire_seq)
                .collect::<Vec<_>>(),
            vec![7]
        );
        assert_eq!(map.linkage(), (None, None));
    }

    #[test]
    fn pending_map_commit_uses_only_preflighted_map_capacity() {
        let mut map = PendingMap::new(2);
        let now = Instant::now();
        map.preflight_insert(7).unwrap();
        let capacity = map.capacity();

        map.commit_insert(pending(7, now));

        assert_eq!(map.capacity(), capacity);
        assert_eq!(map.len(), 1);
        assert_eq!(map.linkage(), (Some(7), Some(7)));
    }

    #[test]
    fn pending_map_mixed_removals_preserve_links() {
        let mut map = PendingMap::new(4);
        let now = Instant::now();
        for seq in 1..=4 {
            insert(
                &mut map,
                pending(seq, now + Duration::from_secs(u64::from(seq))),
            );
        }

        assert!(map.remove(2).is_some());
        assert_eq!(map.entry_links(1), Some((None, Some(3))));
        assert_eq!(map.entry_links(3), Some((Some(1), Some(4))));
        let expired = map.drain_expired_bounded(now + Duration::from_secs(3), usize::MAX);
        assert_eq!(
            expired
                .probes
                .into_iter()
                .map(|probe| probe.wire_seq)
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
        assert_eq!(map.linkage(), (Some(4), Some(4)));
        assert!(map.remove(4).is_some());
        assert_eq!(map.linkage(), (None, None));
    }

    #[test]
    fn bounded_probe_tracking_evicts_oldest_entries() {
        let mut set = CompletedSet::new(3);
        set.insert(0);
        set.insert(1);
        set.insert(2);
        assert!(set.contains(0));
        assert!(set.contains(1));
        assert!(set.contains(2));
        set.insert(3);
        assert_eq!(set.set.len(), 3);
        assert!(!set.contains(0));
        assert!(set.contains(3));

        let mut map = TimedOutMap::new(2);
        let now = Instant::now();
        map.insert(pending(0, now));
        map.insert(pending(1, now));
        map.insert(pending(2, now));

        assert_eq!(map.len(), 2);
        assert!(map.remove(0).is_none());
        assert!(map.remove(1).is_some());
        assert!(map.remove(2).is_some());
    }

    #[test]
    fn timed_out_map_remove_prunes_insertion_order() {
        let mut map = TimedOutMap::new(4);
        let now = Instant::now();

        for i in 0..20 {
            map.insert(pending(i, now));
            assert!(map.remove(i).is_some());
            assert_eq!(map.len(), 0);
            assert_eq!(map.insertion_order_len(), 0);
        }
    }
}
