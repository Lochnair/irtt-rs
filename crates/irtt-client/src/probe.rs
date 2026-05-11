use std::{
    collections::{HashMap, VecDeque},
    time::Instant,
};

use crate::{error::ClientError, timing::ClientTimestamp};

#[derive(Debug, Clone)]
pub(crate) struct PendingProbe {
    pub logical_seq: u64,
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

    pub fn check_capacity(&self) -> Result<(), ClientError> {
        if self.map.len() >= self.max_capacity {
            return Err(ClientError::PendingLimitExceeded {
                limit: self.max_capacity,
            });
        }
        Ok(())
    }

    pub fn insert(&mut self, probe: PendingProbe) -> Result<(), ClientError> {
        self.check_capacity()?;
        self.map.insert(probe.wire_seq, probe);
        Ok(())
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
        expired.sort_by_key(|p| p.logical_seq);
        expired
    }

    pub fn len(&self) -> usize {
        self.map.len()
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

    pub fn len(&self) -> usize {
        self.map.len()
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
    set: HashMap<u32, u64>,
    insertion_order: VecDeque<u32>,
    max_capacity: usize,
}

impl CompletedSet {
    pub fn new(max_capacity: usize) -> Self {
        Self {
            set: HashMap::new(),
            insertion_order: VecDeque::new(),
            max_capacity,
        }
    }

    pub fn insert(&mut self, wire_seq: u32, logical_seq: u64) {
        if let Some(existing) = self.set.get_mut(&wire_seq) {
            *existing = logical_seq;
            return;
        }
        if self.set.len() >= self.max_capacity {
            self.evict_oldest();
        }
        self.insertion_order.push_back(wire_seq);
        self.set.insert(wire_seq, logical_seq);
    }

    pub fn contains(&self, wire_seq: u32) -> bool {
        self.set.contains_key(&wire_seq)
    }

    fn evict_oldest(&mut self) {
        while let Some(oldest_key) = self.insertion_order.pop_front() {
            if self.set.remove(&oldest_key).is_some() {
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

    fn pending(seq: u32, logical: u64, timeout_at: Instant) -> PendingProbe {
        PendingProbe {
            logical_seq: logical,
            wire_seq: seq,
            sent_at: ts(timeout_at - Duration::from_secs(1)),
            timeout_at,
        }
    }

    #[test]
    fn pending_map_insert_and_remove() {
        let mut map = PendingMap::new(10);
        let now = Instant::now();
        let probe = pending(0, 0, now + Duration::from_secs(4));
        map.insert(probe).unwrap();
        assert_eq!(map.len(), 1);
        assert!(map.remove(0).is_some());
        assert_eq!(map.len(), 0);
        assert!(map.remove(0).is_none());
    }

    #[test]
    fn pending_map_rejects_over_capacity() {
        let mut map = PendingMap::new(2);
        let now = Instant::now();
        map.insert(pending(0, 0, now + Duration::from_secs(4)))
            .unwrap();
        map.insert(pending(1, 1, now + Duration::from_secs(4)))
            .unwrap();
        assert!(matches!(
            map.insert(pending(2, 2, now + Duration::from_secs(4))),
            Err(ClientError::PendingLimitExceeded { limit: 2 })
        ));
    }

    #[test]
    fn drain_expired_returns_in_logical_order() {
        let mut map = PendingMap::new(10);
        let now = Instant::now();
        let past = now - Duration::from_secs(1);
        map.insert(pending(5, 5, past)).unwrap();
        map.insert(pending(2, 2, past)).unwrap();
        map.insert(pending(8, 8, now + Duration::from_secs(10)))
            .unwrap();
        let expired = map.drain_expired(now);
        assert_eq!(expired.len(), 2);
        assert_eq!(expired[0].wire_seq, 2);
        assert_eq!(expired[1].wire_seq, 5);
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn completed_set_tracks_and_evicts() {
        let mut set = CompletedSet::new(3);
        set.insert(0, 0);
        set.insert(1, 1);
        set.insert(2, 2);
        assert!(set.contains(0));
        assert!(set.contains(1));
        assert!(set.contains(2));
        set.insert(3, 3);
        assert_eq!(set.set.len(), 3);
        assert!(!set.contains(0));
        assert!(set.contains(3));
    }

    #[test]
    fn timed_out_map_tracks_and_evicts() {
        let mut map = TimedOutMap::new(2);
        let now = Instant::now();
        map.insert(pending(0, 0, now));
        map.insert(pending(1, 1, now));
        map.insert(pending(2, 2, now));

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
            map.insert(pending(i, u64::from(i), now));
            assert!(map.remove(i).is_some());
            assert_eq!(map.len(), 0);
            assert_eq!(map.insertion_order_len(), 0);
        }
    }
}
