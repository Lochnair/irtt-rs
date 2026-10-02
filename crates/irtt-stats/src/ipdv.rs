use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::{Duration, Instant},
};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IpdvSample {
    pub(crate) seq: u32,
    pub(crate) rtt_primary_ns: i128,
    pub(crate) client_send_mono: Instant,
    pub(crate) client_receive_mono: Instant,
    pub(crate) client_send_wall_ns: Option<i128>,
    pub(crate) client_receive_wall_ns: Option<i128>,
    pub(crate) server_receive_mono_ns: Option<i64>,
    pub(crate) server_send_mono_ns: Option<i64>,
    pub(crate) server_receive_wall_ns: Option<i64>,
    pub(crate) server_send_wall_ns: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IpdvTracker {
    samples: HashMap<u32, IpdvSample>,
    sample_order: VecDeque<u32>,
    completed_pairs: HashSet<u32>,
    sequence_limit: Option<usize>,
}

impl IpdvTracker {
    pub(crate) fn new(sequence_limit: Option<usize>) -> Self {
        Self {
            samples: HashMap::new(),
            sample_order: VecDeque::new(),
            completed_pairs: HashSet::new(),
            sequence_limit,
        }
    }

    pub(crate) fn insert(&mut self, sample: IpdvSample) -> Vec<CompletedIpdvPair> {
        let seq = sample.seq;
        if self.samples.insert(seq, sample).is_some() {
            return Vec::new();
        }

        self.sample_order.push_back(seq);
        self.enforce_sequence_limit();

        let mut pairs = Vec::with_capacity(2);
        if let Some(pair) = self.try_pair(seq) {
            pairs.push(pair);
        }
        if let Some(pair) = self.try_pair(seq.wrapping_add(1)) {
            pairs.push(pair);
        }
        pairs
    }

    fn enforce_sequence_limit(&mut self) {
        let Some(limit) = self.sequence_limit else {
            return;
        };
        while self.samples.len() > limit {
            let Some(seq) = self.sample_order.pop_front() else {
                break;
            };
            if self.samples.remove(&seq).is_some() {
                self.completed_pairs.remove(&seq);
                self.completed_pairs.remove(&seq.wrapping_add(1));
            }
        }
    }

    fn try_pair(&mut self, current_seq: u32) -> Option<CompletedIpdvPair> {
        let previous_seq = current_seq.wrapping_sub(1);

        if !self.completed_pairs.insert(current_seq) {
            return None;
        }

        let Some(previous) = self.samples.get(&previous_seq) else {
            self.completed_pairs.remove(&current_seq);
            return None;
        };

        let Some(current) = self.samples.get(&current_seq) else {
            self.completed_pairs.remove(&current_seq);
            return None;
        };

        Some(CompletedIpdvPair {
            previous_seq,
            current_seq,
            rtt_ipdv_ns: abs_i128_ns(current.rtt_primary_ns - previous.rtt_primary_ns),
            send_ipdv_ns: send_ipdv_ns(previous, current).map(abs_i128_ns),
            receive_ipdv_ns: receive_ipdv_ns(previous, current).map(abs_i128_ns),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CompletedIpdvPair {
    pub(crate) previous_seq: u32,
    pub(crate) current_seq: u32,
    pub(crate) rtt_ipdv_ns: i128,
    pub(crate) send_ipdv_ns: Option<i128>,
    pub(crate) receive_ipdv_ns: Option<i128>,
}

fn send_ipdv_ns(previous: &IpdvSample, current: &IpdvSample) -> Option<i128> {
    if let (Some(prev_server), Some(cur_server)) = (
        previous.server_receive_mono_ns,
        current.server_receive_mono_ns,
    ) {
        return Some(
            i128::from(cur_server)
                - i128::from(prev_server)
                - instant_diff_ns(current.client_send_mono, previous.client_send_mono),
        );
    }
    if let (Some(prev_server), Some(cur_server), Some(prev_client), Some(cur_client)) = (
        previous.server_receive_wall_ns,
        current.server_receive_wall_ns,
        previous.client_send_wall_ns,
        current.client_send_wall_ns,
    ) {
        return Some(i128::from(cur_server) - i128::from(prev_server) - (cur_client - prev_client));
    }
    None
}

fn receive_ipdv_ns(previous: &IpdvSample, current: &IpdvSample) -> Option<i128> {
    if let (Some(prev_server), Some(cur_server)) =
        (previous.server_send_mono_ns, current.server_send_mono_ns)
    {
        return Some(
            instant_diff_ns(current.client_receive_mono, previous.client_receive_mono)
                - (i128::from(cur_server) - i128::from(prev_server)),
        );
    }
    if let (Some(prev_server), Some(cur_server), Some(prev_client), Some(cur_client)) = (
        previous.server_send_wall_ns,
        current.server_send_wall_ns,
        previous.client_receive_wall_ns,
        current.client_receive_wall_ns,
    ) {
        return Some(
            (cur_client - prev_client) - (i128::from(cur_server) - i128::from(prev_server)),
        );
    }
    None
}

fn instant_diff_ns(current: Instant, previous: Instant) -> i128 {
    if let Some(diff) = current.checked_duration_since(previous) {
        duration_ns_i128(diff)
    } else {
        -duration_ns_i128(previous.duration_since(current))
    }
}

fn duration_ns_i128(duration: Duration) -> i128 {
    i128::try_from(duration.as_nanos()).unwrap_or(i128::MAX)
}

fn abs_i128_ns(value: i128) -> i128 {
    value.saturating_abs()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ipdv_sample(seq: u32, rtt_primary_ns: i128) -> IpdvSample {
        let now = Instant::now();
        IpdvSample {
            seq,
            rtt_primary_ns,
            client_send_mono: now,
            client_receive_mono: now,
            client_send_wall_ns: None,
            client_receive_wall_ns: None,
            server_receive_mono_ns: None,
            server_send_mono_ns: None,
            server_receive_wall_ns: None,
            server_send_wall_ns: None,
        }
    }

    mod properties {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            // Hidden retention state must stay bounded even with duplicates,
            // gaps and reordered replies; snapshots cannot expose a leak here.
            #[test]
            fn bounded_tracker_state_never_exceeds_the_limit(
                limit in 1usize..8,
                seqs in prop::collection::vec(0u32..40, 0..300),
            ) {
                let mut tracker = IpdvTracker::new(Some(limit));
                for seq in seqs {
                    let completed_before = tracker.completed_pairs.clone();
                    let pairs = tracker.insert(ipdv_sample(seq, i128::from(seq)));
                    let emitted: HashSet<_> = pairs.iter().map(|pair| pair.current_seq).collect();
                    prop_assert_eq!(emitted.len(), pairs.len());
                    prop_assert!(emitted.is_disjoint(&completed_before), "a retained pair must not be emitted twice");
                    prop_assert!(tracker.samples.len() <= limit);
                    prop_assert!(tracker.sample_order.len() <= limit);
                    prop_assert!(tracker.completed_pairs.len() <= limit);
                    // Eviction must remove stale pair markers as well as samples;
                    // otherwise retained state can prevent future adjacent pairs.
                    prop_assert_eq!(tracker.sample_order.len(), tracker.samples.len());
                    prop_assert_eq!(tracker.sample_order.iter().collect::<HashSet<_>>().len(), tracker.samples.len());
                    prop_assert!(tracker.sample_order.iter().all(|seq| tracker.samples.contains_key(seq)));
                    prop_assert!(tracker.completed_pairs.iter().all(|seq| {
                        tracker.samples.contains_key(seq)
                            && tracker.samples.contains_key(&seq.wrapping_sub(1))
                    }), "completed pairs must retain both sequence endpoints");
                }
            }

        }
    }
}
