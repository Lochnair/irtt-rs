//! Statistics aggregation for `irtt-client` events.
//!
//! The crate consumes `irtt-client` events and produces cumulative or rolling
//! snapshots for reporting and integration code.

#![forbid(unsafe_code)]
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
