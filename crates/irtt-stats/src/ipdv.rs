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

    #[test]
    fn ipdv_tracker_late_wrapped_previous_completes_pair() {
        let mut tracker = IpdvTracker::new(None);
        assert!(tracker.insert(ipdv_sample(0, 14)).is_empty());

        let pairs = tracker.insert(ipdv_sample(u32::MAX, 10));

        assert_eq!(
            pairs,
            vec![CompletedIpdvPair {
                previous_seq: u32::MAX,
                current_seq: 0,
                rtt_ipdv_ns: 4,
                send_ipdv_ns: None,
                receive_ipdv_ns: None,
            }]
        );
    }

    #[test]
    fn ipdv_tracker_duplicate_sequence_does_not_emit_pair_again() {
        let mut tracker = IpdvTracker::new(None);
        assert!(tracker.insert(ipdv_sample(0, 10)).is_empty());
        assert_eq!(tracker.insert(ipdv_sample(1, 14)).len(), 1);

        assert!(tracker.insert(ipdv_sample(1, 18)).is_empty());
    }

    #[test]
    fn ipdv_tracker_bounded_mode_limits_sequence_state() {
        let limit = 4;
        let mut tracker = IpdvTracker::new(Some(limit));

        // Far more inserts than the limit, so eviction runs repeatedly. Every
        // per-sequence collection must stay bounded by the limit rather than
        // growing with the number of samples seen, and eviction must not stop
        // adjacent sequences from still completing a pair.
        for seq in 0..512_u32 {
            let pairs = tracker.insert(ipdv_sample(seq, i128::from(seq)));

            assert_eq!(pairs.len(), usize::from(seq > 0), "seq {seq}");
            assert!(tracker.samples.len() <= limit, "seq {seq}");
            assert_eq!(
                tracker.sample_order.len(),
                tracker.samples.len(),
                "seq {seq}"
            );
            assert!(tracker.completed_pairs.len() <= limit, "seq {seq}");
        }
    }

    mod properties {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            /// The hand-written `ipdv_tracker_bounded_mode_limits_sequence_state`
            /// test above only exercises a strictly increasing `0..512`
            /// sequence. Real traffic can duplicate, skip, or otherwise not
            /// strictly increase sequence numbers (see the client's
            /// `LateReply`/`DuplicateReply` handling), so this generates
            /// arbitrary, possibly-repeated, possibly-non-monotonic sequence
            /// streams and checks the same bounds still hold after every
            /// insert: `samples` and `sample_order` never exceed the
            /// configured limit and stay in lockstep, and `completed_pairs`
            /// never exceeds `samples.len()` (a successful entry can only
            /// persist once both its endpoint samples are present, see
            /// `try_pair`, so it is always a subset of the live sample keys).
            #[test]
            fn bounded_tracker_state_never_exceeds_the_limit(
                limit in 1usize..8,
                seqs in prop::collection::vec(0u32..40, 0..300),
            ) {
                let mut tracker = IpdvTracker::new(Some(limit));
                for seq in seqs {
                    tracker.insert(ipdv_sample(seq, i128::from(seq)));
                    prop_assert!(tracker.samples.len() <= limit);
                    prop_assert_eq!(tracker.sample_order.len(), tracker.samples.len());
                    prop_assert!(tracker.completed_pairs.len() <= tracker.samples.len());
                }
            }

            /// A pair for a given adjacent `(seq - 1, seq)` boundary is only
            /// ever emitted once, however many times `insert` is called
            /// (including for already-seen or unrelated sequences in
            /// between). This is the unbounded case, so eviction cannot be
            /// the reason a pair fails to repeat.
            #[test]
            fn each_adjacent_pair_is_emitted_at_most_once(
                seqs in prop::collection::vec(0u32..12, 0..200),
            ) {
                let mut tracker = IpdvTracker::new(None);
                let mut seen_pairs = std::collections::HashSet::new();
                for seq in seqs {
                    for pair in tracker.insert(ipdv_sample(seq, i128::from(seq))) {
                        let key = (pair.previous_seq, pair.current_seq);
                        prop_assert!(
                            seen_pairs.insert(key),
                            "pair {key:?} emitted more than once"
                        );
                    }
                }
            }
        }
    }
}
