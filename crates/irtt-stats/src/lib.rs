//! Statistics aggregation for `irtt-client` events.
//!
//! The crate consumes `irtt-client` events and produces cumulative or rolling
//! snapshots for reporting and integration code.

//#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]

use std::time::Duration;

use irtt_client::ClientEvent;

mod core;
mod ipdv;
mod loss;
mod normalization;
mod rolling;
mod time_stats;

use core::CoreStats;
pub use loss::LossStats;
use normalization::normalize_event;
use rolling::RollingEvents;
pub use time_stats::TimeStats;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Configuration for statistics collection.
pub struct StatsConfig {
    /// How timing samples are retained for median-capable metrics.
    pub samples: SampleMode,
    /// Number of recent normalized events retained for count-based rolling snapshots.
    ///
    /// A successful probe usually contributes two normalized events: one send event
    /// and one unique reply event.
    pub rolling_count: Option<usize>,
    /// Time span of recent normalized events retained for time-based rolling snapshots.
    pub rolling_time: Option<Duration>,
}

impl StatsConfig {
    /// Returns the default configuration for finite runs.
    ///
    /// Finite mode retains exact samples where needed for medians and keeps
    /// unbounded adjacent-sequence IPDV tracking so late adjacent replies can
    /// still complete IPDV pairs.
    pub fn finite() -> Self {
        Self {
            samples: SampleMode::Exact,
            rolling_count: None,
            rolling_time: None,
        }
    }

    /// Returns a configuration for long-running use.
    ///
    /// Continuous mode uses running statistics, does not retain exact samples
    /// for medians, and bounds adjacent-sequence IPDV tracking for long-running
    /// sessions.
    pub fn continuous() -> Self {
        Self {
            samples: SampleMode::RunningOnly,
            rolling_count: None,
            rolling_time: None,
        }
    }
}

impl Default for StatsConfig {
    fn default() -> Self {
        Self::finite()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Controls whether exact timing samples are retained.
pub enum SampleMode {
    /// Keep only running statistics; exact medians are not available.
    RunningOnly,
    /// Retain exact samples for metrics that report exact medians.
    Exact,
}

#[derive(Debug, Clone, PartialEq)]
/// Stateful statistics collector for `irtt-client` events.
///
/// A collector maintains cumulative statistics and, when configured, rolling
/// windows. Rolling snapshots are recomputed from retained normalized events.
pub struct StatsCollector {
    cumulative: CoreStats,
    rolling: RollingEvents,
}

impl StatsCollector {
    /// Creates a collector with the supplied configuration.
    pub fn new(config: StatsConfig) -> Self {
        Self {
            cumulative: CoreStats::new(config.samples),
            rolling: RollingEvents::new(config),
        }
    }

    /// Processes one client event and returns the per-event statistics update.
    ///
    /// Updates currently report whether the event contributed a unique reply
    /// timing sample and any adjacent-sequence IPDV pairs completed by this
    /// event.
    pub fn process(&mut self, event: &ClientEvent) -> EventStatsUpdate {
        let Some(stats_event) = normalize_event(event) else {
            return EventStatsUpdate::default();
        };

        let update = self.cumulative.apply(stats_event.clone());
        self.rolling.push(stats_event);
        update
    }

    /// Returns a snapshot of all events processed by this collector.
    pub fn snapshot(&self) -> Snapshot {
        self.cumulative.snapshot()
    }

    /// Returns the configured count-based rolling snapshot, if enabled.
    ///
    /// The snapshot is recomputed from the retained rolling events.
    pub fn rolling_count(&self) -> Option<Snapshot> {
        self.rolling.count_snapshot()
    }

    /// Returns the configured time-based rolling snapshot, if enabled.
    ///
    /// The snapshot is recomputed from the retained rolling events.
    pub fn rolling_time(&self) -> Option<Snapshot> {
        self.rolling.time_snapshot()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
/// Per-event statistics produced by [`StatsCollector::process`].
pub struct EventStatsUpdate {
    /// Whether the processed event contributed a unique reply timing sample.
    pub contributed_sample: bool,
    /// Adjacent-sequence IPDV pairs completed by the processed event.
    pub ipdv_pairs: Vec<IpdvPairUpdate>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Adjacent-sequence IPDV pair completed by a processed event.
pub struct IpdvPairUpdate {
    /// Sequence number of the earlier reply in the adjacent pair.
    pub previous_seq: u32,
    /// Sequence number of the later reply in the adjacent pair.
    pub current_seq: u32,
    /// Absolute round-trip IPDV between the adjacent replies.
    pub rtt_ipdv: Duration,
    /// Absolute send-side IPDV when send one-way delay is available for both replies.
    pub send_ipdv: Option<Duration>,
    /// Absolute receive-side IPDV when receive one-way delay is available for both replies.
    pub receive_ipdv: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq)]
/// Point-in-time statistics summary.
pub struct Snapshot {
    /// Event counters grouped by event class.
    pub events: EventCounts,
    /// Packet and byte counters.
    pub packets: PacketCounts,
    /// Packet loss, duplicate, and late-packet percentages.
    pub loss: LossStats,
    /// Duration of send calls, in nanoseconds.
    pub send_call: TimeStats,
    /// Sender scheduling error, in nanoseconds.
    pub timer_error: TimeStats,
    /// Round-trip timing statistics.
    pub rtt: RttStats,
    /// Inter-packet delay variation statistics.
    pub ipdv: IpdvStats,
    /// One-way delay statistics.
    pub one_way_delay: OneWayDelayStats,
    /// Server processing time statistics.
    pub server_processing: ServerProcessingStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
/// Counts of normalized client events.
pub struct EventCounts {
    /// Probe send events processed.
    pub sent_events: u64,
    /// On-time unique echo replies processed.
    pub echo_replies: u64,
    /// Late unique echo replies processed.
    pub late_unique_replies: u64,
    /// Duplicate echo replies processed.
    pub duplicate_replies: u64,
    /// Loss events processed.
    pub loss_events: u64,
    /// Warning events processed.
    pub warning_events: u64,
    /// Late replies that could not be matched to retained in-flight state.
    pub untracked_late_replies: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
/// Packet, byte, and server-reported receive counters.
pub struct PacketCounts {
    /// Probe packets sent by the local client.
    pub packets_sent: u64,
    /// Reply packets received by the local client, including duplicates and late packets.
    pub packets_received: u64,
    /// Unique replies received by the local client.
    pub unique_replies: u64,
    /// Duplicate replies received by the local client.
    pub duplicates: u64,
    /// Late reply packets received by the local client.
    pub late_packets: u64,
    /// Probe bytes sent by the local client.
    pub bytes_sent: u64,
    /// Reply bytes received by the local client.
    pub bytes_received: u64,
    /// Server-reported packets received, when available.
    pub server_packets_received: Option<u64>,
    /// Server-reported receive window, when available.
    pub server_received_window: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
/// Round-trip time statistics.
pub struct RttStats {
    /// Effective RTT used for primary RTT reporting and IPDV input.
    pub primary: TimeStats,
    /// Client-observed raw RTT.
    pub raw: TimeStats,
    /// RTT adjusted for server processing time when available.
    pub adjusted: TimeStats,
}

#[derive(Debug, Clone, PartialEq)]
/// Inter-packet delay variation statistics.
pub struct IpdvStats {
    /// Round-trip IPDV.
    pub round_trip: TimeStats,
    /// Send-side IPDV, when send one-way delay is available.
    pub send: TimeStats,
    /// Receive-side IPDV, when receive one-way delay is available.
    pub receive: TimeStats,
}

#[derive(Debug, Clone, PartialEq)]
/// One-way delay statistics.
pub struct OneWayDelayStats {
    /// Client-to-server delay.
    pub send_delay: TimeStats,
    /// Server-to-client delay.
    pub receive_delay: TimeStats,
}

#[derive(Debug, Clone, PartialEq)]
/// Server processing time statistics.
pub struct ServerProcessingStats {
    /// Time spent processing a probe at the server.
    pub processing: TimeStats,
}

#[cfg(test)]
mod memory_growth_tests {
    use irtt_client::PacketMeta;

    use super::*;
    use std::{
        alloc::{GlobalAlloc, Layout, System},
        net::{IpAddr, SocketAddr},
        str::FromStr,
        sync::atomic::{AtomicUsize, Ordering},
        time::{Duration, Instant, SystemTime},
    };

    struct CountingAlloc;

    static CURRENT_ALLOCATED: AtomicUsize = AtomicUsize::new(0);
    static PEAK_ALLOCATED: AtomicUsize = AtomicUsize::new(0);

    unsafe impl GlobalAlloc for CountingAlloc {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let ptr = unsafe { System.alloc(layout) };
            if !ptr.is_null() {
                let new_current =
                    CURRENT_ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();

                let mut peak = PEAK_ALLOCATED.load(Ordering::Relaxed);
                while new_current > peak {
                    match PEAK_ALLOCATED.compare_exchange_weak(
                        peak,
                        new_current,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => break,
                        Err(observed) => peak = observed,
                    }
                }
            }
            ptr
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { System.dealloc(ptr, layout) };
            CURRENT_ALLOCATED.fetch_sub(layout.size(), Ordering::Relaxed);
        }

        unsafe fn realloc(&self, ptr: *mut u8, old_layout: Layout, new_size: usize) -> *mut u8 {
            let new_ptr = unsafe { System.realloc(ptr, old_layout, new_size) };
            if !new_ptr.is_null() {
                if new_size >= old_layout.size() {
                    let delta = new_size - old_layout.size();
                    let new_current = CURRENT_ALLOCATED.fetch_add(delta, Ordering::Relaxed) + delta;

                    let mut peak = PEAK_ALLOCATED.load(Ordering::Relaxed);
                    while new_current > peak {
                        match PEAK_ALLOCATED.compare_exchange_weak(
                            peak,
                            new_current,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => break,
                            Err(observed) => peak = observed,
                        }
                    }
                } else {
                    CURRENT_ALLOCATED.fetch_sub(old_layout.size() - new_size, Ordering::Relaxed);
                }
            }
            new_ptr
        }
    }

    #[global_allocator]
    static ALLOC: CountingAlloc = CountingAlloc;

    fn reset_alloc_counter() {
        CURRENT_ALLOCATED.store(0, Ordering::Relaxed);
        PEAK_ALLOCATED.store(0, Ordering::Relaxed);
    }

    fn current_allocated() -> usize {
        CURRENT_ALLOCATED.load(Ordering::Relaxed)
    }

    fn peak_allocated() -> usize {
        PEAK_ALLOCATED.load(Ordering::Relaxed)
    }

    fn mib(bytes: usize) -> f64 {
        bytes as f64 / 1024.0 / 1024.0
    }

    fn client_timestamp(mono: Instant) -> irtt_client::ClientTimestamp {
        irtt_client::ClientTimestamp {
            mono,
            wall: SystemTime::UNIX_EPOCH,
        }
    }

    fn sent_event(seq: u32, base: Instant) -> irtt_client::ClientEvent {
        let mono = base + Duration::from_millis(u64::from(seq));

        irtt_client::ClientEvent::EchoSent {
            seq,
            sent_at: client_timestamp(mono),
            bytes: 64,
            remote: SocketAddr::new(IpAddr::from_str("1.1.1.1").unwrap(), 9),
            scheduled_at: Instant::now(),
            send_call: Duration::from_nanos(1_000),
            timer_error: Duration::ZERO,
        }
    }

    fn reply_event(seq: u32, base: Instant) -> irtt_client::ClientEvent {
        let sent_mono = base + Duration::from_millis(u64::from(seq));
        let received_mono = sent_mono + Duration::from_millis(10);

        irtt_client::ClientEvent::EchoReply {
            seq,
            sent_at: client_timestamp(sent_mono),
            received_at: client_timestamp(received_mono),
            remote: SocketAddr::new(IpAddr::from_str("1.1.1.1").unwrap(), 9),
            packet_meta: PacketMeta {
                traffic_class: None,
                dscp: None,
                ecn: None,
                kernel_rx_timestamp: None,
            },
            rtt: irtt_client::RttSample {
                raw: Duration::from_millis(10),
                adjusted: Some(Duration::from_millis(9)),
                effective: Duration::from_millis(9),
                adjusted_signed: Some(irtt_client::SignedDuration {
                    ns: 9_000_000 + i128::from(seq % 100),
                }),
                effective_signed: irtt_client::SignedDuration {
                    ns: 9_000_000 + i128::from(seq % 100),
                },
            },
            server_timing: None,
            one_way: None,
            received_stats: Some(irtt_client::ReceivedStatsSample {
                count: Some(seq + 1),
                window: Some(u64::MAX),
            }),
            bytes: 64,
        }
    }

    fn feed_successful_probes(collector: &mut StatsCollector, count: usize) {
        let base = Instant::now();

        for seq in 0..count {
            let seq = u32::try_from(seq).expect("sample count should fit in u32");
            collector.process(&sent_event(seq, base));
            collector.process(&reply_event(seq, base));
        }
    }

    fn measure_config(label: &str, config: StatsConfig, count: usize) {
        reset_alloc_counter();

        let baseline_current = current_allocated();
        let baseline_peak = peak_allocated();

        let mut collector = StatsCollector::new(config);
        feed_successful_probes(&mut collector, count);

        let snapshot = collector.snapshot();

        let current = current_allocated().saturating_sub(baseline_current);
        let peak = peak_allocated().saturating_sub(baseline_peak);

        println!(
            "{label:<42} probes={count:<8} current={:>12} B ({:>8.2} MiB) peak={:>12} B ({:>8.2} MiB) rtt_count={} median={:?} ipdv_pairs={}",
            current,
            mib(current),
            peak,
            mib(peak),
            snapshot.rtt.primary.count,
            snapshot.rtt.primary.median_ns,
            snapshot.ipdv.round_trip.count,
        );
    }

    #[test]
    #[ignore = "prints approximate heap allocation growth for stats configurations"]
    fn print_storage_growth_allocations() {
        println!();
        println!("irtt-stats heap allocation growth");
        println!("=================================");
        println!(
            "Note: these numbers are approximate allocator-level deltas for this test binary."
        );
        println!(
            "They include collection growth and Box allocations, but also any incidental allocations during the measured section."
        );
        println!();

        for count in [10usize, 1_000, 100_000] {
            measure_config("finite", StatsConfig::finite(), count);

            measure_config("continuous", StatsConfig::continuous(), count);

            measure_config(
                "continuous + rolling_count=1_000",
                StatsConfig {
                    samples: SampleMode::RunningOnly,
                    rolling_count: Some(1_000),
                    rolling_time: None,
                },
                count,
            );

            measure_config(
                "continuous + rolling_count=100_000",
                StatsConfig {
                    samples: SampleMode::RunningOnly,
                    rolling_count: Some(100_000),
                    rolling_time: None,
                },
                count,
            );

            println!();
        }
    }
}
