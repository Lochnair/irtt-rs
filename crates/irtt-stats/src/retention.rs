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

/// Exact timing samples an ordinary successful probe can retain.
///
/// In [`SampleMode::Exact`] every public timing metric retains its samples, so
/// one probe that is sent and answered can add one `i128` to each of them:
/// send call and timer error from the send, and primary, raw, and adjusted
/// RTT, the three IPDV metrics, the two one-way delays, and server processing
/// from the reply. [`SampleMode::RunningOnly`] retains none of them.
///
/// Adjusted RTT, one-way delay, send/receive IPDV, and server processing only
/// receive a sample when the negotiated session supplies the corresponding
/// optional measurement, so counting all eleven is the upper bound.
pub(crate) const EXACT_SAMPLES_PER_PROBE: u64 = 11;

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

/// Exact mode retains one sample per timing metric and one IPDV tracker entry
/// for every ordinary successful probe, so its storage grows with the probe
/// count.
fn exact_bytes(probe_count: u64) -> u64 {
    let per_probe = EXACT_SAMPLES_PER_PROBE
        .saturating_mul(size_of::<i128>() as u64)
        .saturating_add(ipdv_tracker_bytes_per_sample());
    probe_count.saturating_mul(per_probe)
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
