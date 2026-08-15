//! Approximate estimates of the storage a statistics configuration retains.
//!
//! The estimate is derived from the structures this crate actually keeps, so
//! retention changes and the planning estimate stay in one place. It models
//! live element storage plus a fixed capacity-headroom factor; it is not
//! allocator accounting.

use std::mem::size_of;

use crate::core::CONTINUOUS_SEQUENCE_LIMIT;
use crate::ipdv::IpdvSample;
use crate::normalization::{ReplySample, StatsEvent};
use crate::{SampleMode, StatsConfig};

/// Capacity headroom applied to the live element bytes.
///
/// `Vec`, `VecDeque`, `HashMap`, and `HashSet` all grow by doubling, so a
/// container can hold up to roughly twice the bytes its live elements need.
/// Assuming the worst case keeps the estimate conservative rather than
/// falsely precise.
const GROWTH_FACTOR: u64 = 2;

/// Approximate per-entry control overhead for the hashed containers.
const HASH_ENTRY_OVERHEAD: u64 = 1;

/// Exact-sample vectors that retain one value per measurable unique reply.
///
/// In [`SampleMode::Exact`] these are the primary, raw, and adjusted RTT
/// metrics, the three IPDV metrics, and the two one-way delay metrics.
/// `send_call`, `timer_error`, and `server_processing` do not retain exact
/// samples in any mode.
///
/// The adjusted-RTT, one-way, and send/receive IPDV metrics only receive a
/// sample when the negotiated session supplies the corresponding optional
/// measurement, so counting all eight is the upper bound.
const EXACT_SAMPLE_VECS_PER_REPLY: u64 = 8;

/// Normalized events a probe usually contributes to a rolling window: one send
/// event and one unique reply event.
const ROLLING_EVENTS_PER_PROBE: u64 = 2;

/// Returns the approximate bytes retained after `probe_count` probes.
pub(crate) fn estimated_retained_bytes(config: &StatsConfig, probe_count: u64) -> u64 {
    let cumulative = match config.samples {
        SampleMode::Exact => exact_bytes(probe_count),
        SampleMode::RunningOnly => running_only_bytes(probe_count),
    };

    cumulative
        .saturating_add(rolling_count_bytes(config, probe_count))
        .saturating_mul(GROWTH_FACTOR)
}

/// Exact mode retains one sample per metric and one IPDV tracker entry for
/// every measurable reply, so its storage grows with the probe count.
fn exact_bytes(probe_count: u64) -> u64 {
    let per_reply = EXACT_SAMPLE_VECS_PER_REPLY
        .saturating_mul(size_of::<i128>() as u64)
        .saturating_add(ipdv_tracker_bytes_per_sample());
    probe_count.saturating_mul(per_reply)
}

/// Running-only mode retains no exact samples and bounds the IPDV tracker at a
/// fixed number of sequences, so its storage stops growing once that bound is
/// reached.
fn running_only_bytes(probe_count: u64) -> u64 {
    probe_count
        .min(CONTINUOUS_SEQUENCE_LIMIT as u64)
        .saturating_mul(ipdv_tracker_bytes_per_sample())
}

/// The IPDV tracker keys a sample map by sequence and keeps a sequence order
/// queue and a completed-pair set alongside it.
fn ipdv_tracker_bytes_per_sample() -> u64 {
    let sample_entry = (size_of::<u32>() + size_of::<IpdvSample>()) as u64 + HASH_ENTRY_OVERHEAD;
    let order_entry = size_of::<u32>() as u64;
    let completed_entry = size_of::<u32>() as u64 + HASH_ENTRY_OVERHEAD;
    sample_entry
        .saturating_add(order_entry)
        .saturating_add(completed_entry)
}

/// Count-based rolling windows retain whole normalized events, bounded by the
/// configured event count.
fn rolling_count_bytes(config: &StatsConfig, probe_count: u64) -> u64 {
    let Some(limit) = config.rolling_count else {
        return 0;
    };
    let retained = probe_count
        .saturating_mul(ROLLING_EVENTS_PER_PROBE)
        .min(limit as u64);
    retained.saturating_mul(rolling_bytes_per_event())
}

/// A retained event is the enum itself plus, for a unique reply, the boxed
/// reply sample it owns.
fn rolling_bytes_per_event() -> u64 {
    (size_of::<StatsEvent>() + size_of::<ReplySample>()) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_sample_components_are_non_zero() {
        assert!(ipdv_tracker_bytes_per_sample() > 0);
        assert!(rolling_bytes_per_event() > 0);
    }

    #[test]
    fn running_only_stops_growing_at_the_bounded_sequence_limit() {
        let limit = CONTINUOUS_SEQUENCE_LIMIT as u64;
        assert_eq!(running_only_bytes(limit), running_only_bytes(limit * 100));
        assert!(running_only_bytes(limit - 1) < running_only_bytes(limit));
    }
}
