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
mod retention;
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
    /// How timing samples are retained, and so whether the cumulative snapshot
    /// reports medians.
    pub samples: SampleMode,
    /// Number of recent normalized events retained for count-based rolling snapshots.
    ///
    /// A successful probe usually contributes two normalized events: one send event
    /// and one unique reply event.
    pub rolling_count: Option<usize>,
    /// Time span of recent normalized events retained for time-based rolling snapshots.
    pub rolling_time: Option<Duration>,
    /// Whether matched late replies contribute measurements.
    pub late_replies: LateReplyMode,
}

impl StatsConfig {
    /// Returns the default configuration for finite runs.
    ///
    /// Finite mode retains exact samples for every timing metric, so each of
    /// them reports a median in the cumulative snapshot once it has samples,
    /// and keeps unbounded adjacent-sequence IPDV tracking so late adjacent
    /// replies can still complete IPDV pairs.
    pub fn finite() -> Self {
        Self {
            samples: SampleMode::Exact,
            rolling_count: None,
            rolling_time: None,
            late_replies: LateReplyMode::Measure,
        }
    }

    /// Returns a configuration for long-running use.
    ///
    /// Continuous mode uses running statistics, retains no exact samples so no
    /// timing metric reports a median, and bounds adjacent-sequence IPDV
    /// tracking for long-running sessions.
    pub fn continuous() -> Self {
        Self {
            samples: SampleMode::RunningOnly,
            rolling_count: None,
            rolling_time: None,
            late_replies: LateReplyMode::Measure,
        }
    }

    /// Returns an approximate estimate of the bytes a [`StatsCollector`] with
    /// this configuration retains after processing `probe_count` probes.
    ///
    /// This is a planning aid — it exists so callers can warn about a run
    /// before starting it — and deliberately not allocator accounting. It is
    /// derived from the structures this crate retains and a fixed
    /// capacity-headroom factor for the amortized-doubling containers those
    /// structures use, so it errs toward overestimating.
    ///
    /// # What is included
    ///
    /// - Exact timing samples, for every metric that retains them.
    /// - Adjacent-sequence IPDV tracking state.
    /// - Count-based rolling storage, when [`StatsConfig::rolling_count`] is
    ///   set, assuming a probe contributes one send event and one unique
    ///   reply event.
    ///
    /// # What is excluded
    ///
    /// - Time-based rolling storage. [`StatsConfig::rolling_time`] retains
    ///   whatever arrives inside its window, which cannot be bounded from a
    ///   probe count alone without assuming traffic timing, so it does not
    ///   contribute to this estimate.
    /// - Everything outside this crate: sockets, client session state,
    ///   rendered output, and caller structures.
    ///
    /// # Sample mode
    ///
    /// [`SampleMode::Exact`] retains one value per timing metric per ordinary
    /// successful probe and keeps unbounded IPDV state, so the estimate grows
    /// with `probe_count`. [`SampleMode::RunningOnly`] retains no exact
    /// samples and bounds its IPDV state, so the estimate reaches a ceiling
    /// and stops growing.
    ///
    /// # Optional measurements
    ///
    /// Adjusted RTT, one-way delay, send/receive IPDV, and server processing
    /// are only measured when the negotiated session supplies the necessary
    /// timestamps. The estimate assumes all of them are available, which is
    /// the upper bound.
    ///
    /// # Saturation
    ///
    /// The result is deterministic and computed with saturating arithmetic, so
    /// an enormous `probe_count` saturates at [`u64::MAX`] rather than
    /// wrapping or panicking.
    pub fn estimated_retained_bytes(&self, probe_count: u64) -> u64 {
        retention::estimated_retained_bytes(self, probe_count)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
/// Whether a matched late reply contributes measurements.
///
/// This is a measurement policy, not a classification. A late reply that the
/// client matched to retained send state is a late unique reply in both modes,
/// and a late reply that could not be matched remains an untracked late reply
/// in both modes. Only what a matched late reply is allowed to measure differs,
/// so a caller that treats late replies as diagnostics does not have to degrade
/// the event itself to suppress its timing contribution.
pub enum LateReplyMode {
    /// Matched late replies contribute timing, one-way, server-processing, and
    /// IPDV measurements like any other unique reply.
    #[default]
    Measure,
    /// Matched late replies are counted but measure nothing.
    ///
    /// Packet, byte, late, and server-reported receive counters still update,
    /// so the reply stays visible as a late unique reply. Timing metrics,
    /// one-way and server-processing measurements, and IPDV do not update, and
    /// [`EventStatsUpdate::contributed_sample`] is `false`.
    CountOnly,
}

impl Default for StatsConfig {
    fn default() -> Self {
        Self::finite()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Controls whether exact timing samples are retained.
///
/// This governs the cumulative snapshot from [`StatsCollector::snapshot`],
/// where the choice applies uniformly to every timing metric, so that snapshot
/// never mixes median-capable and median-incapable timing metrics. Rolling
/// snapshots are recomputed with running statistics whatever this is set to;
/// see [`StatsCollector::rolling_count`].
pub enum SampleMode {
    /// Keep only running statistics; medians are not available for any timing
    /// metric, and retention stays bounded for long-running sessions.
    RunningOnly,
    /// Retain exact samples for every timing metric, so each one reports a
    /// median once it has samples in the cumulative snapshot. Retention grows
    /// with the probe count.
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
            cumulative: CoreStats::new(config.samples, config.late_replies),
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
    ///
    /// This is the snapshot [`StatsConfig::samples`] applies to, so under
    /// [`SampleMode::Exact`] every timing metric here reports a median once it
    /// has samples.
    pub fn snapshot(&self) -> Snapshot {
        self.cumulative.snapshot()
    }

    /// Returns the configured count-based rolling snapshot, if enabled.
    ///
    /// The snapshot is recomputed from the retained rolling events using
    /// running statistics only, whatever [`StatsConfig::samples`] is set to,
    /// so its timing metrics report no median. A rolling window is a bounded
    /// recent view rather than the run's retained history.
    pub fn rolling_count(&self) -> Option<Snapshot> {
        self.rolling.count_snapshot()
    }

    /// Returns the configured time-based rolling snapshot, if enabled.
    ///
    /// Like [`StatsCollector::rolling_count`], this is recomputed with running
    /// statistics only and reports no medians.
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
    ///
    /// These replies are observed on the socket, but cannot contribute RTT,
    /// one-way delay, server-processing, or IPDV samples.
    pub untracked_late_replies: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
/// Packet, byte, and server-reported receive counters.
pub struct PacketCounts {
    /// Probe packets sent by the local client.
    pub packets_sent: u64,
    /// Reply packets observed by the local client.
    ///
    /// This includes unique measurable replies, duplicate replies, and late
    /// replies that cannot be matched to retained in-flight state.
    pub packets_received: u64,
    /// Unique replies that can contribute timing measurements.
    pub unique_replies: u64,
    /// Duplicate replies received by the local client.
    pub duplicates: u64,
    /// Late reply packets received by the local client, including untracked-late replies.
    pub late_packets: u64,
    /// Probe bytes sent by the local client.
    pub bytes_sent: u64,
    /// Reply bytes observed by the local client.
    ///
    /// This includes bytes from unique measurable replies, duplicate replies,
    /// and untracked-late replies. Per-category byte counts are not currently
    /// exposed separately.
    pub bytes_received: u64,
    /// Highest cumulative server-reported packets received, when available.
    ///
    /// This is the only server-reported input to the directional loss estimates
    /// in [`LossStats`].
    pub server_packets_received: Option<u64>,
    /// Raw server-reported receive window from the most recently processed
    /// reply that carried one, when available.
    ///
    /// The value is stored exactly as the server reported it and is not
    /// interpreted by this crate. It is a bounded bitmap of recent server
    /// receive history rather than a fixed 64-packet loss mask, so a value of
    /// `0x1` means no earlier history is represented and not that the previous
    /// 63 packets were lost. Replies are aggregated in local arrival order, so
    /// a late reply can leave an older window here than an already-processed
    /// reply carried.
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
