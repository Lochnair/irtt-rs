use std::{collections::HashMap, time::Instant};

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

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.map.len()
    }
}

#[derive(Debug)]
pub(crate) struct CompletedSet {
    set: HashMap<u32, u64>,
    max_capacity: usize,
}

impl CompletedSet {
    pub fn new(max_capacity: usize) -> Self {
        Self {
            set: HashMap::new(),
            max_capacity,
        }
    }

    pub fn insert(&mut self, wire_seq: u32, logical_seq: u64) {
        if self.set.len() >= self.max_capacity {
            self.evict_oldest();
        }
        self.set.insert(wire_seq, logical_seq);
    }

    pub fn contains(&self, wire_seq: u32) -> bool {
        self.set.contains_key(&wire_seq)
    }

    fn evict_oldest(&mut self) {
        if let Some((&oldest_key, _)) = self.set.iter().min_by_key(|(_, &seq)| seq) {
            self.set.remove(&oldest_key);
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
}
