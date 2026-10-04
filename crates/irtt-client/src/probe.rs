use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::{Instant, SystemTime},
};

use crate::{error::ClientError, timing::ClientTimestamp};

#[derive(Debug, Clone)]
pub(crate) struct PendingProbe {
    pub wire_seq: u32,
    /// Paired client wall/monotonic timestamp captured immediately after the
    /// successful UDP send completed. This is the RTT endpoint (via
    /// `compute_rtt`), the userspace fallback endpoint for upstream one-way
    /// delay, `timer_error`'s basis, and the value surfaced on the public
    /// `EchoSent`/`EchoReply`/`EchoLoss`/`LateReply` events. It is *not* the
    /// input to `timeout_at` below — see that field's doc.
    pub sent_at: ClientTimestamp,
    /// Operational timeout deadline, computed from the pre-send send
    /// anchor's monotonic timestamp plus the configured probe timeout
    /// (`SessionMachine::finalize_probe_commit`), not from `sent_at.mono`
    /// above. Timeout semantics deliberately stay anchored to before the
    /// socket send so that fallible deadline arithmetic (and its overflow
    /// check) still runs, and a `PendingProbe` is only ever created, before
    /// the datagram is transmitted.
    pub timeout_at: Instant,
    /// Local wall-clock lower bound for validating an asynchronous Linux
    /// kernel `TX_SOFTWARE` timestamp, sampled at the same pre-send send
    /// anchor as `timeout_at` — i.e. before the socket send, not from
    /// `sent_at.wall` above. A legitimate `TX_SOFTWARE` timestamp can be
    /// generated during the send path and therefore observed earlier than
    /// the post-send `sent_at.wall` sample; using this earlier pre-send
    /// bound instead of `sent_at.wall` avoids rejecting such a timestamp
    /// merely because userspace samples `sent_at` after `send()` returns.
    /// See `compute_one_way`'s `preferred_send_wall`.
    pub tx_not_before_wall: SystemTime,
    /// Observed Linux kernel TX_SOFTWARE wall timestamp for this probe's
    /// send, when the socket has TX timestamping enabled and a matching
    /// `MSG_ERRQUEUE` record has been drained. Optional observed metadata,
    /// eligible only for upstream one-way delay after local plausibility
    /// validation against `tx_not_before_wall` (see `compute_one_way`'s
    /// `preferred_send_wall`). `sent_at` remains the userspace fallback for
    /// upstream one-way delay when this is absent, implausible, or kernel-ID
    /// correlation was invalidated by a failed probe submission.
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
            self.map
                .try_reserve(1)
                .map_err(|source| ClientError::AllocationFailed {
                    operation: "pending probe storage",
                    source,
                })?;
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

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Mutable access to a still-pending probe by wire sequence, without
    /// disturbing its position in the timeout order. Used to attach an
    /// observed kernel TX timestamp to a probe that has not yet completed or
    /// timed out.
    pub fn get_mut(&mut self, wire_seq: u32) -> Option<&mut PendingProbe> {
        self.map.get_mut(&wire_seq).map(|entry| &mut entry.probe)
    }

    pub fn next_timeout_deadline(&self) -> Option<Instant> {
        self.first
            .and_then(|wire_seq| self.map.get(&wire_seq))
            .map(|entry| entry.probe.timeout_at)
    }

    #[cfg(feature = "tokio")]
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
}

#[derive(Debug)]
pub(crate) struct TimedOutMap {
    // Timeout transitions consume PendingMap's head in deadline order. Reuse
    // its linked storage in a separate map: the head owns FIFO eviction and
    // the tail owns the latest retained timeout, even after arbitrary replies.
    probes: PendingMap,
    #[cfg(all(test, feature = "tokio"))]
    deadline_inspections: std::cell::Cell<usize>,
}

impl TimedOutMap {
    pub fn new(max_capacity: usize) -> Self {
        Self {
            probes: PendingMap::new(max_capacity),
            #[cfg(all(test, feature = "tokio"))]
            deadline_inspections: std::cell::Cell::new(0),
        }
    }

    pub fn insert(&mut self, probe: PendingProbe) {
        if self.probes.max_capacity == 0 {
            return;
        }
        // A sequence cannot be both pending and retained: committing its next
        // generation removes the old retention before adding the pending probe.
        debug_assert!(!self.probes.map.contains_key(&probe.wire_seq));
        if self.probes.map.len() == self.probes.max_capacity {
            self.probes
                .remove(self.probes.first.expect("full retention has a head"));
        }
        // Unlike pending admission, retention always makes room by eviction.
        // Allocation here remains infallible as it was for the previous map.
        self.probes.commit_insert(probe);
    }

    pub fn remove(&mut self, wire_seq: u32) -> Option<PendingProbe> {
        self.probes.remove(wire_seq)
    }

    /// Mutable access to a still-retained timed-out probe by wire sequence,
    /// without disturbing eviction order. Used to attach an observed kernel
    /// TX timestamp that arrives after the probe has already timed out.
    pub fn get_mut(&mut self, wire_seq: u32) -> Option<&mut PendingProbe> {
        self.probes.get_mut(wire_seq)
    }

    #[cfg(feature = "tokio")]
    pub fn latest_timeout_deadline(&self) -> Option<Instant> {
        self.probes
            .last
            .and_then(|wire_seq| self.probes.map.get(&wire_seq))
            .map(|entry| {
                #[cfg(test)]
                self.deadline_inspections
                    .set(self.deadline_inspections.get() + 1);
                entry.probe.timeout_at
            })
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
        let sent_at = ts(timeout_at - Duration::from_secs(1));
        PendingProbe {
            wire_seq: seq,
            sent_at,
            timeout_at,
            tx_not_before_wall: sent_at.wall,
            kernel_tx_timestamp: None,
        }
    }

    #[test]
    fn pending_removal_and_sequence_reuse_preserve_expiry_and_capacity() {
        let start = Instant::now();
        // Remove the head, middle or tail, then reuse that sequence at a later
        // deadline. Observe expiry and capacity, not private list links.
        for removed in 0..3 {
            let mut pending_map = PendingMap::new(3);
            for seq in 0..3 {
                pending_map.preflight_insert(seq).unwrap();
                pending_map
                    .commit_insert(pending(seq, start + Duration::from_secs(u64::from(seq))));
            }
            assert!(matches!(
                pending_map.preflight_insert(3),
                Err(ClientError::PendingLimitExceeded { .. })
            ));
            assert_eq!(pending_map.remove(removed).unwrap().wire_seq, removed);
            pending_map.preflight_insert(removed).unwrap();
            let reused_deadline = start + Duration::from_secs(4);
            pending_map.commit_insert(pending(removed, reused_deadline));

            let remaining = (0..3).filter(|seq| *seq != removed).collect::<Vec<_>>();
            for (index, seq) in remaining.iter().enumerate() {
                assert_eq!(
                    pending_map.next_timeout_deadline(),
                    Some(start + Duration::from_secs(u64::from(*seq)))
                );
                let expired = pending_map.drain_expired_bounded(start + Duration::from_secs(3), 1);
                assert_eq!(
                    expired
                        .probes
                        .iter()
                        .map(|probe| probe.wire_seq)
                        .collect::<Vec<_>>(),
                    vec![*seq]
                );
                assert_eq!(expired.more_due, index == 0);
            }
            assert_eq!(pending_map.next_timeout_deadline(), Some(reused_deadline));
            let expired = pending_map.drain_expired_bounded(reused_deadline, 3);
            assert_eq!(expired.probes.len(), 1);
            assert_eq!(expired.probes[0].wire_seq, removed);
            assert!(!expired.more_due);
            assert!(pending_map.is_empty());
            assert_eq!(pending_map.next_timeout_deadline(), None);
            pending_map.preflight_insert(removed).unwrap();
            pending_map.commit_insert(pending(removed, reused_deadline));
            assert_eq!(pending_map.remove(removed).unwrap().wire_seq, removed);
            assert!(pending_map.is_empty());
        }
    }

    // Measure deadline inspection work directly: wall-clock thresholds cannot
    // reliably distinguish a bounded lookup from a retention scan.
    #[cfg(feature = "tokio")]
    #[test]
    fn timeout_retention_deadlines_have_bounded_lookup_work() {
        const COUNT: u32 = 4096;
        let start = Instant::now();
        let deadline = |seq| start + Duration::from_micros(u64::from(seq / 2));
        let mut pending_map = PendingMap::new(COUNT as usize);
        let mut timed_out = TimedOutMap::new(COUNT as usize);
        for seq in 0..COUNT {
            pending_map.preflight_insert(seq).unwrap();
            pending_map.commit_insert(pending(seq, deadline(seq)));
        }
        let last_deadline = deadline(COUNT - 1);
        for seq in 0..COUNT {
            let batch = pending_map.drain_expired_bounded(last_deadline, 1);
            assert_eq!(batch.probes.len(), 1);
            assert_eq!(batch.probes[0].wire_seq, seq);
            assert_eq!(batch.more_due, seq + 1 < COUNT);
            timed_out.insert(batch.probes.into_iter().next().unwrap());
            assert_eq!(timed_out.latest_timeout_deadline(), Some(deadline(seq)));
            assert_eq!(
                pending_map
                    .latest_timeout_deadline()
                    .into_iter()
                    .chain(timed_out.latest_timeout_deadline())
                    .max(),
                Some(last_deadline)
            );
        }
        assert!(
            timed_out.deadline_inspections.replace(0) <= 2 * COUNT as usize,
            "draining must not scan growing timeout retention after each transition"
        );
        // A middle/head removal leaves the latest deadline intact. Removing
        // the newest survivors must walk back through equal deadlines too.
        assert!(timed_out.remove(COUNT / 2).is_some());
        assert!(timed_out.remove(0).is_some());
        for seq in (1..COUNT).rev().filter(|seq| *seq != COUNT / 2) {
            assert_eq!(timed_out.latest_timeout_deadline(), Some(deadline(seq)));
            assert!(timed_out.remove(seq).is_some());
        }
        assert_eq!(timed_out.latest_timeout_deadline(), None);
        assert!(timed_out.deadline_inspections.get() <= COUNT as usize);
    }

    // Public loss/reply events cannot reveal leaked retention bookkeeping.
    #[test]
    fn timed_out_retention_preserves_fifo_eviction_and_stays_bounded() {
        let mut map = TimedOutMap::new(4);
        let now = Instant::now();

        for i in 0..20 {
            map.insert(pending(i, now));
            assert!(map.remove(i).is_some());
            assert!(map.probes.map.len() <= 4);
        }

        // Equal deadlines across wire-sequence wrap must still evict by
        // insertion order. Removal at any position frees capacity immediately.
        let sequences = [u32::MAX - 1, u32::MAX, 0, 1];
        for removed in sequences {
            let mut map = TimedOutMap::new(4);
            for seq in sequences {
                map.insert(pending(seq, now));
            }
            assert!(map.remove(removed).is_some());
            map.insert(pending(2, now));
            let oldest = sequences.into_iter().find(|seq| *seq != removed).unwrap();
            map.insert(pending(3, now));
            assert!(map.remove(oldest).is_none(), "oldest survivor is evicted");
            for seq in sequences
                .into_iter()
                .filter(|seq| *seq != removed && *seq != oldest)
            {
                assert!(map.remove(seq).is_some());
            }
            assert!(map.remove(2).is_some());
            assert!(map.remove(3).is_some());
            assert!(map.probes.map.is_empty());
            #[cfg(feature = "tokio")]
            assert_eq!(map.latest_timeout_deadline(), None);
        }
    }
}
