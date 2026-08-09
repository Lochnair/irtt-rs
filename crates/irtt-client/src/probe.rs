use std::{
    collections::{HashMap, HashSet, TryReserveError, VecDeque},
    time::Instant,
};

use crate::{error::ClientError, timing::ClientTimestamp};

#[derive(Debug, Clone)]
pub(crate) struct PendingProbe {
    pub wire_seq: u32,
    pub sent_at: ClientTimestamp,
    pub timeout_at: Instant,
}

#[derive(Debug)]
pub(crate) struct PendingMap {
    map: HashMap<u32, PendingProbe>,
    max_capacity: usize,
}

impl PendingMap {
    pub fn new(max_capacity: usize) -> Self {
        Self {
            map: HashMap::new(),
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
        let replaced = self.map.insert(probe.wire_seq, probe);
        debug_assert!(replaced.is_none(), "preflight rejected pending collision");
    }

    pub fn remove(&mut self, wire_seq: u32) -> Option<PendingProbe> {
        self.map.remove(&wire_seq)
    }

    pub fn drain_expired(&mut self, now: Instant) -> Vec<PendingProbe> {
        let expired_keys: Vec<u32> = self
            .map
            .iter()
            .filter(|(_, probe)| probe.timeout_at <= now)
            .map(|(key, _)| *key)
            .collect();
        let mut expired = Vec::with_capacity(expired_keys.len());
        for key in expired_keys {
            if let Some(probe) = self.map.remove(&key) {
                expired.push(probe);
            }
        }
        expired.sort_by_key(|p| p.sent_at.mono);
        expired
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    #[cfg(feature = "tokio")]
    pub fn next_timeout_deadline(&self) -> Option<Instant> {
        self.map.values().map(|probe| probe.timeout_at).min()
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
    pub fn capacity(&self) -> usize {
        self.map.capacity()
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
