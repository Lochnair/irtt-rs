use std::{
    collections::{BTreeSet, HashMap, HashSet, TryReserveError, VecDeque},
    time::Instant,
};

use crate::{error::ClientError, timing::ClientTimestamp};

#[derive(Debug, Clone)]
pub(crate) struct PendingProbe {
    pub wire_seq: u32,
    pub sent_at: ClientTimestamp,
    pub timeout_at: Instant,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DeadlineKey {
    timeout_at: Instant,
    sent_at: Instant,
    wire_seq: u32,
}

impl From<&PendingProbe> for DeadlineKey {
    fn from(probe: &PendingProbe) -> Self {
        Self {
            timeout_at: probe.timeout_at,
            sent_at: probe.sent_at.mono,
            wire_seq: probe.wire_seq,
        }
    }
}

#[derive(Debug)]
pub(crate) struct ExpiredBatch {
    pub probes: Vec<PendingProbe>,
    pub more_due: bool,
}

#[derive(Debug)]
pub(crate) struct PendingMap {
    map: HashMap<u32, PendingProbe>,
    deadlines: BTreeSet<DeadlineKey>,
    max_capacity: usize,
}

impl PendingMap {
    pub fn new(max_capacity: usize) -> Self {
        Self {
            map: HashMap::new(),
            deadlines: BTreeSet::new(),
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
        let inserted = self.deadlines.insert(DeadlineKey::from(&probe));
        debug_assert!(inserted, "pending deadline index already contained probe");
        let replaced = self.map.insert(probe.wire_seq, probe);
        debug_assert!(replaced.is_none(), "preflight rejected pending collision");
        self.assert_index_consistent();
    }

    pub fn remove(&mut self, wire_seq: u32) -> Option<PendingProbe> {
        let probe = self.map.remove(&wire_seq)?;
        let removed = self.deadlines.remove(&DeadlineKey::from(&probe));
        debug_assert!(removed, "pending deadline index lost removed probe");
        self.assert_index_consistent();
        Some(probe)
    }

    pub(crate) fn drain_expired_bounded(&mut self, now: Instant, limit: usize) -> ExpiredBatch {
        let mut probes = Vec::with_capacity(limit.min(self.map.len()));
        while probes.len() < limit {
            let Some(key) = self.deadlines.first().copied() else {
                break;
            };
            if key.timeout_at > now {
                break;
            }

            let removed_key = self
                .deadlines
                .take(&key)
                .expect("first pending deadline remains present");
            debug_assert_eq!(removed_key, key);
            let probe = self
                .map
                .remove(&key.wire_seq)
                .expect("pending deadline always has a pending probe");
            debug_assert_eq!(DeadlineKey::from(&probe), key);
            probes.push(probe);
        }
        self.assert_index_consistent();
        let more_due = self
            .deadlines
            .first()
            .is_some_and(|key| key.timeout_at <= now);
        ExpiredBatch { probes, more_due }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn next_timeout_deadline(&self) -> Option<Instant> {
        self.deadlines.first().map(|key| key.timeout_at)
    }

    pub fn latest_timeout_deadline(&self) -> Option<Instant> {
        self.deadlines.last().map(|key| key.timeout_at)
    }

    fn assert_index_consistent(&self) {
        debug_assert_eq!(
            self.deadlines.len(),
            self.map.len(),
            "pending deadline index cardinality diverged"
        );
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
    pub fn deadline_index_len(&self) -> usize {
        self.deadlines.len()
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
    fn pending_map_indexes_deadline_extrema_and_removals() {
        let mut map = PendingMap::new(4);
        let now = Instant::now();
        insert(&mut map, pending(1, now + Duration::from_secs(3)));
        insert(&mut map, pending(2, now + Duration::from_secs(1)));
        insert(&mut map, pending(3, now + Duration::from_secs(2)));

        assert_eq!(
            map.next_timeout_deadline(),
            Some(now + Duration::from_secs(1))
        );
        assert_eq!(
            map.latest_timeout_deadline(),
            Some(now + Duration::from_secs(3))
        );
        assert_eq!(map.deadline_index_len(), map.len());

        assert!(map.remove(3).is_some());
        assert_eq!(
            map.next_timeout_deadline(),
            Some(now + Duration::from_secs(1))
        );
        assert_eq!(
            map.latest_timeout_deadline(),
            Some(now + Duration::from_secs(3))
        );
        assert_eq!(map.deadline_index_len(), map.len());

        assert!(map.remove(2).is_some());
        assert_eq!(
            map.next_timeout_deadline(),
            Some(now + Duration::from_secs(3))
        );
        assert_eq!(
            map.latest_timeout_deadline(),
            Some(now + Duration::from_secs(3))
        );
        assert_eq!(map.deadline_index_len(), map.len());

        assert!(map.remove(1).is_some());
        assert_eq!(map.next_timeout_deadline(), None);
        assert_eq!(map.latest_timeout_deadline(), None);
        assert_eq!(map.deadline_index_len(), map.len());
    }

    #[test]
    fn pending_map_bounded_expiration_matches_exhaustive_order() {
        let now = Instant::now();
        let probes = [
            pending(4, now + Duration::from_secs(4)),
            pending(2, now + Duration::from_secs(2)),
            pending(1, now + Duration::from_secs(1)),
            pending(3, now + Duration::from_secs(3)),
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
        assert_eq!(bounded.deadline_index_len(), bounded.len());

        let mut actual = Vec::new();
        loop {
            let batch = bounded.drain_expired_bounded(now + Duration::from_secs(4), 2);
            actual.extend(batch.probes.into_iter().map(|probe| probe.wire_seq));
            assert_eq!(bounded.deadline_index_len(), bounded.len());
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
        assert_eq!(bounded.deadline_index_len(), 0);
    }

    #[test]
    fn pending_map_sequence_reuse_leaves_no_stale_deadline() {
        let mut map = PendingMap::new(1);
        let now = Instant::now();
        insert(&mut map, pending(7, now));
        assert!(map.remove(7).is_some());
        insert(&mut map, pending(7, now + Duration::from_secs(1)));

        assert!(map.drain_expired_bounded(now, usize::MAX).probes.is_empty());
        assert_eq!(map.deadline_index_len(), map.len());
        assert_eq!(
            map.drain_expired_bounded(now + Duration::from_secs(1), usize::MAX)
                .probes
                .into_iter()
                .map(|probe| probe.wire_seq)
                .collect::<Vec<_>>(),
            vec![7]
        );
        assert_eq!(map.deadline_index_len(), map.len());
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
