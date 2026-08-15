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
///
/// This count must track the metrics `CoreStats::new` builds with exact
/// retention. `exact_mode_retains_one_sample_per_counted_metric` below counts
/// the metrics a fully measured probe actually retains, so the two cannot
/// drift apart unnoticed.
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

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant, UNIX_EPOCH};

    use irtt_client::{
        ClientEvent, ClientTimestamp, OneWayDelaySample, PacketMeta, ReceivedStatsSample,
        RttSample, ServerTiming, SignedDuration,
    };

    use super::*;
    use crate::{Snapshot, StatsCollector, TimeStats};

    /// Every public timing metric a snapshot exposes.
    fn timing_metrics(snapshot: &Snapshot) -> Vec<&TimeStats> {
        vec![
            &snapshot.send_call,
            &snapshot.timer_error,
            &snapshot.rtt.primary,
            &snapshot.rtt.raw,
            &snapshot.rtt.adjusted,
            &snapshot.ipdv.round_trip,
            &snapshot.ipdv.send,
            &snapshot.ipdv.receive,
            &snapshot.one_way_delay.send_delay,
            &snapshot.one_way_delay.receive_delay,
            &snapshot.server_processing.processing,
        ]
    }

    fn timestamp(offset_ms: u64) -> ClientTimestamp {
        ClientTimestamp {
            mono: Instant::now() + Duration::from_millis(offset_ms),
            wall: UNIX_EPOCH + Duration::from_millis(offset_ms),
        }
    }

    fn sent(seq: u32) -> ClientEvent {
        let sent_at = timestamp(u64::from(seq) * 10);
        ClientEvent::EchoSent {
            seq,
            remote: "127.0.0.1:2112".parse().unwrap(),
            scheduled_at: sent_at.mono,
            sent_at,
            bytes: 32,
            send_call: Duration::from_micros(10 + u64::from(seq)),
            timer_error: Duration::from_micros(2 + u64::from(seq)),
        }
    }

    /// A reply carrying every optional measurement, so one probe exercises the
    /// upper bound the estimate assumes.
    fn fully_measured_reply(seq: u32) -> ClientEvent {
        let sent_at = timestamp(u64::from(seq) * 10);
        let raw = Duration::from_millis(10 + u64::from(seq));
        let received_at = ClientTimestamp {
            mono: sent_at.mono + raw,
            wall: sent_at.wall + raw,
        };
        let base_ns = i64::from(seq) * 10_000_000;
        ClientEvent::EchoReply {
            seq,
            remote: "127.0.0.1:2112".parse().unwrap(),
            sent_at,
            received_at,
            rtt: RttSample {
                raw,
                adjusted: Some(SignedDuration::from_duration(raw)),
                effective: SignedDuration::from_duration(raw),
            },
            server_timing: Some(ServerTiming {
                receive_mono_ns: Some(base_ns + 1_000_000),
                send_mono_ns: Some(base_ns + 2_000_000),
                receive_wall_ns: Some(base_ns + 1_000_000),
                send_wall_ns: Some(base_ns + 2_000_000),
                midpoint_mono_ns: None,
                midpoint_wall_ns: None,
                processing: Some(Duration::from_millis(1 + u64::from(seq))),
            }),
            one_way: Some(OneWayDelaySample {
                client_to_server: Some(SignedDuration::from_nanos(1_000_000 + i128::from(seq))),
                server_to_client: Some(SignedDuration::from_nanos(2_000_000 + i128::from(seq))),
            }),
            received_stats: Some(ReceivedStatsSample {
                count: Some(seq + 1),
                window: Some(0xff),
            }),
            bytes: 64,
            packet_meta: PacketMeta::default(),
        }
    }

    /// Two probes fill every metric, including the IPDV pair metrics that need
    /// adjacent replies.
    fn fully_measured_snapshot(config: StatsConfig) -> Snapshot {
        let mut collector = StatsCollector::new(config);
        for seq in 0..2 {
            collector.process(&sent(seq));
            collector.process(&fully_measured_reply(seq));
        }
        collector.snapshot()
    }

    /// The estimate is only honest while it counts the metrics that actually
    /// retain samples. Adding or removing an exact-retaining public timing
    /// metric without revisiting [`EXACT_SAMPLES_PER_PROBE`] fails here.
    #[test]
    fn exact_mode_retains_one_sample_per_counted_metric() {
        let snapshot = fully_measured_snapshot(StatsConfig::finite());
        let with_medians = timing_metrics(&snapshot)
            .into_iter()
            .filter(|stats| stats.median_ns.is_some())
            .count() as u64;

        assert_eq!(
            with_medians, EXACT_SAMPLES_PER_PROBE,
            "the estimate counts {EXACT_SAMPLES_PER_PROBE} exact sample streams per probe, \
             but {with_medians} public timing metrics retained samples"
        );
    }

    #[test]
    fn running_only_retains_no_exact_samples() {
        let snapshot = fully_measured_snapshot(StatsConfig::continuous());

        for stats in timing_metrics(&snapshot) {
            assert!(stats.count > 0, "the fixture should populate every metric");
            assert_eq!(
                stats.median_ns, None,
                "running-only mode retains no exact samples"
            );
        }
    }

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
