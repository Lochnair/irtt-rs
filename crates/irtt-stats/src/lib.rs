//! Statistics aggregation for `irtt-client` events.

#![forbid(unsafe_code)]

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
pub struct StatsConfig {
    pub samples: SampleMode,
    pub rolling_count: Option<usize>,
    pub rolling_time: Option<Duration>,
}

impl StatsConfig {
    pub fn finite() -> Self {
        Self {
            samples: SampleMode::Exact,
            rolling_count: None,
            rolling_time: None,
        }
    }

    /// Configuration for long-running use.
    ///
    /// This disables exact sample retention and bounds sequence-adjacent IPDV
    /// tracking. Exact arbitrary-late IPDV completion is finite-mode behavior.
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
pub enum SampleMode {
    RunningOnly,
    Exact,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StatsCollector {
    cumulative: CoreStats,
    rolling: RollingEvents,
}

impl StatsCollector {
    pub fn new(config: StatsConfig) -> Self {
        Self {
            cumulative: CoreStats::new(config.samples),
            rolling: RollingEvents::new(config),
        }
    }

    pub fn process(&mut self, event: &ClientEvent) -> EventStatsUpdate {
        let Some(stats_event) = normalize_event(event) else {
            return EventStatsUpdate::default();
        };

        let update = self.cumulative.apply(stats_event.clone());
        self.rolling.push(stats_event);
        update
    }

    pub fn snapshot(&self) -> Snapshot {
        self.cumulative.snapshot()
    }

    pub fn rolling_count(&self) -> Option<Snapshot> {
        self.rolling.count_snapshot()
    }

    pub fn rolling_time(&self) -> Option<Snapshot> {
        self.rolling.time_snapshot()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EventStatsUpdate {
    pub contributed_sample: bool,
    pub ipdv_pairs: Vec<IpdvPairUpdate>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpdvPairUpdate {
    pub previous_seq: u32,
    pub current_seq: u32,
    pub rtt_ipdv: Duration,
    pub send_ipdv: Option<Duration>,
    pub receive_ipdv: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    pub events: EventCounts,
    pub packets: PacketCounts,
    pub loss: LossStats,
    pub send_call: TimeStats,
    pub timer_error: TimeStats,
    pub rtt: RttStats,
    pub ipdv: IpdvStats,
    pub one_way_delay: OneWayDelayStats,
    pub server_processing: ServerProcessingStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EventCounts {
    pub sent_events: u64,
    pub echo_replies: u64,
    pub late_unique_replies: u64,
    pub duplicate_replies: u64,
    pub loss_events: u64,
    pub warning_events: u64,
    pub untracked_late_replies: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PacketCounts {
    pub packets_sent: u64,
    pub packets_received: u64,
    pub unique_replies: u64,
    pub duplicates: u64,
    pub late_packets: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub server_packets_received: Option<u64>,
    pub server_received_window: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RttStats {
    pub primary: TimeStats,
    pub raw: TimeStats,
    pub adjusted: TimeStats,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IpdvStats {
    pub round_trip: TimeStats,
    pub send: TimeStats,
    pub receive: TimeStats,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OneWayDelayStats {
    pub send_delay: TimeStats,
    pub receive_delay: TimeStats,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ServerProcessingStats {
    pub processing: TimeStats,
}
