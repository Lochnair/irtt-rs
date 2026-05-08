//! Statistics aggregation for `irtt-client` events.

#![forbid(unsafe_code)]

use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use irtt_client::{
    ClientEvent, ClientTimestamp, OneWayDelaySample, ReceivedStatsSample, RttSample, ServerTiming,
    SignedDuration,
};

const CONTINUOUS_SEQUENCE_LIMIT: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatsConfig {
    pub median: MedianMode,
    pub rolling_count: Option<usize>,
    pub rolling_time: Option<Duration>,
}

impl StatsConfig {
    pub fn finite() -> Self {
        Self {
            median: MedianMode::ExactFinite,
            rolling_count: None,
            rolling_time: None,
        }
    }

    /// Configuration for long-running use.
    ///
    /// This disables finite median retention and bounds sequence-adjacent IPDV
    /// tracking. Exact arbitrary-late IPDV completion is finite-mode behavior.
    pub fn continuous() -> Self {
        Self {
            median: MedianMode::Disabled,
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
pub enum MedianMode {
    Disabled,
    ExactFinite,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StatsCollector {
    config: StatsConfig,
    cumulative: CoreStats,
    rolling_count: Option<VecDeque<WindowEvent>>,
    rolling_time: Option<VecDeque<WindowEvent>>,
}

impl StatsCollector {
    pub fn new(config: StatsConfig) -> Self {
        Self {
            config,
            cumulative: CoreStats::new(config.median),
            rolling_count: config.rolling_count.map(|_| VecDeque::new()),
            rolling_time: config.rolling_time.map(|_| VecDeque::new()),
        }
    }

    pub fn process(&mut self, event: &ClientEvent) -> EventStatsUpdate {
        self.process_with_update(event)
    }

    pub fn process_with_update(&mut self, event: &ClientEvent) -> EventStatsUpdate {
        let Some(window_event) = WindowEvent::from_client_event(event) else {
            return EventStatsUpdate::default();
        };

        let update = self.cumulative.apply(window_event.clone());

        if let (Some(limit), Some(window)) =
            (self.config.rolling_count, self.rolling_count.as_mut())
        {
            window.push_back(window_event.clone());
            while window.len() > limit {
                window.pop_front();
            }
        }

        if let (Some(duration), Some(window)) =
            (self.config.rolling_time, self.rolling_time.as_mut())
        {
            let cutoff = window_event.at().checked_sub(duration);
            window.push_back(window_event);
            if let Some(cutoff) = cutoff {
                while window.front().is_some_and(|event| event.at() < cutoff) {
                    window.pop_front();
                }
            }
        }

        update
    }

    pub fn cumulative(&self) -> CumulativeSnapshot {
        self.cumulative.snapshot()
    }

    pub fn rolling_count(&self) -> Option<RollingSnapshot> {
        self.rolling_count.as_ref().map(snapshot_window)
    }

    pub fn rolling_time(&self) -> Option<RollingSnapshot> {
        self.rolling_time.as_ref().map(snapshot_window)
    }

    pub fn summary(&self) -> FiniteSummary {
        self.cumulative()
    }

    #[cfg(test)]
    fn retained_median_samples(&self) -> usize {
        self.cumulative.retained_median_samples()
    }

    #[cfg(test)]
    fn retained_sequence_samples(&self) -> usize {
        self.cumulative.retained_sequence_samples()
    }
}

pub type RollingSnapshot = CumulativeSnapshot;
pub type FiniteSummary = CumulativeSnapshot;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EventStatsUpdate {
    pub contributed_sample: bool,
    pub ipdv_pairs: Vec<IpdvPairUpdate>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpdvPairUpdate {
    pub previous_logical_seq: u64,
    pub current_logical_seq: u64,
    pub rtt_ipdv: Duration,
    pub send_ipdv: Option<Duration>,
    pub receive_ipdv: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CumulativeSnapshot {
    pub events: EventCounts,
    pub packets: PacketCounts,
    pub loss: LossStats,
    pub send_call: DurationStats,
    pub timer_error: DurationStats,
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LossStats {
    pub lost_packets: u64,
    pub unknown_loss_packets: u64,
    pub upstream_loss_packets: Option<i128>,
    pub downstream_loss_packets: Option<i128>,
    pub packet_loss_percent: f64,
    pub upstream_loss_percent: f64,
    pub downstream_loss_percent: f64,
    pub duplicate_percent: f64,
    pub late_packets_percent: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RttStats {
    pub primary: SignedDurationStatsWithMedian,
    pub raw: DurationStatsWithMedian,
    pub adjusted: SignedDurationStatsWithMedian,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IpdvStats {
    pub round_trip: DurationStatsWithMedian,
    pub send: DurationStatsWithMedian,
    pub receive: DurationStatsWithMedian,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OneWayDelayStats {
    pub send_delay: DurationStatsWithMedian,
    pub receive_delay: DurationStatsWithMedian,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ServerProcessingStats {
    pub processing: DurationStats,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DurationStats {
    pub count: u64,
    pub total_ns: u128,
    pub min_ns: Option<u64>,
    pub max_ns: Option<u64>,
    pub mean_ns: f64,
    pub variance_ns2: f64,
}

impl DurationStats {
    pub fn stddev_ns(&self) -> f64 {
        self.variance_ns2.sqrt()
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DurationStatsWithMedian {
    pub stats: DurationStats,
    pub median_ns: Option<f64>,
}

impl DurationStatsWithMedian {
    pub fn stddev_ns(&self) -> f64 {
        self.stats.stddev_ns()
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SignedDurationStats {
    pub count: u64,
    pub total_ns: i128,
    pub min_ns: Option<i128>,
    pub max_ns: Option<i128>,
    pub mean_ns: f64,
    pub variance_ns2: f64,
}

impl SignedDurationStats {
    pub fn stddev_ns(&self) -> f64 {
        self.variance_ns2.sqrt()
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SignedDurationStatsWithMedian {
    pub stats: SignedDurationStats,
    pub median_ns: Option<f64>,
}

impl SignedDurationStatsWithMedian {
    pub fn stddev_ns(&self) -> f64 {
        self.stats.stddev_ns()
    }
}

#[derive(Debug, Clone, PartialEq)]
struct CoreStats {
    median: MedianMode,
    sequence_limit: Option<usize>,
    events: EventCounts,
    packets: PacketCounts,
    send_call: MetricU64,
    timer_error: MetricU64,
    rtt_primary: MetricI128,
    rtt_raw: MetricU64,
    rtt_adjusted: MetricI128,
    ipdv_round_trip: MetricU64,
    ipdv_send: MetricU64,
    ipdv_receive: MetricU64,
    send_delay: MetricU64,
    receive_delay: MetricU64,
    server_processing: MetricU64,
    samples: HashMap<u64, UniqueSample>,
    sample_order: VecDeque<u64>,
    ipdv_pairs: HashSet<u64>,
}

impl CoreStats {
    fn new(median: MedianMode) -> Self {
        Self {
            median,
            sequence_limit: if median == MedianMode::ExactFinite {
                None
            } else {
                Some(CONTINUOUS_SEQUENCE_LIMIT)
            },
            events: EventCounts::default(),
            packets: PacketCounts::default(),
            send_call: MetricU64::new(false),
            timer_error: MetricU64::new(false),
            rtt_primary: MetricI128::new(median == MedianMode::ExactFinite),
            rtt_raw: MetricU64::new(median == MedianMode::ExactFinite),
            rtt_adjusted: MetricI128::new(median == MedianMode::ExactFinite),
            ipdv_round_trip: MetricU64::new(median == MedianMode::ExactFinite),
            ipdv_send: MetricU64::new(median == MedianMode::ExactFinite),
            ipdv_receive: MetricU64::new(median == MedianMode::ExactFinite),
            send_delay: MetricU64::new(median == MedianMode::ExactFinite),
            receive_delay: MetricU64::new(median == MedianMode::ExactFinite),
            server_processing: MetricU64::new(false),
            samples: HashMap::new(),
            sample_order: VecDeque::new(),
            ipdv_pairs: HashSet::new(),
        }
    }

    fn apply(&mut self, event: WindowEvent) -> EventStatsUpdate {
        let mut update = EventStatsUpdate::default();
        match event {
            WindowEvent::Sent {
                bytes,
                send_call_ns,
                timer_error_ns,
                ..
            } => {
                self.events.sent_events += 1;
                self.packets.packets_sent += 1;
                self.packets.bytes_sent = self.packets.bytes_sent.saturating_add(bytes as u64);
                self.send_call.push(send_call_ns);
                self.timer_error.push(timer_error_ns);
            }
            WindowEvent::UniqueReply {
                is_late, sample, ..
            } => {
                let sample = *sample;
                self.events.echo_replies += u64::from(!is_late);
                self.events.late_unique_replies += u64::from(is_late);
                self.packets.packets_received += 1;
                self.packets.unique_replies += 1;
                self.packets.late_packets += u64::from(is_late);
                self.packets.bytes_received = self
                    .packets
                    .bytes_received
                    .saturating_add(sample.bytes as u64);
                update.contributed_sample = true;
                if let Some(count) = sample.received_count {
                    self.packets.server_packets_received = Some(u64::from(count));
                }
                if let Some(window) = sample.received_window {
                    self.packets.server_received_window = Some(window);
                }

                self.rtt_primary.push(sample.rtt_primary_ns);
                self.rtt_raw.push(sample.rtt_raw_ns);
                if let Some(adjusted) = sample.rtt_adjusted_ns {
                    self.rtt_adjusted.push(adjusted);
                }
                if let Some(processing) = sample.server_processing_ns {
                    self.server_processing.push(processing);
                }
                if let Some(delay) = sample.send_delay_ns {
                    self.send_delay.push(delay);
                }
                if let Some(delay) = sample.receive_delay_ns {
                    self.receive_delay.push(delay);
                }

                let seq = sample.logical_seq;
                if self.samples.insert(seq, sample).is_none() {
                    self.sample_order.push_back(seq);
                    self.enforce_sequence_limit();
                    if let Some(pair) = self.try_ipdv_pair(seq) {
                        update.ipdv_pairs.push(pair);
                    }
                    if let Some(next) = seq.checked_add(1) {
                        if let Some(pair) = self.try_ipdv_pair(next) {
                            update.ipdv_pairs.push(pair);
                        }
                    }
                }
            }
            WindowEvent::DuplicateReply { .. } => {
                self.events.duplicate_replies += 1;
                self.packets.packets_received += 1;
                self.packets.duplicates += 1;
            }
            WindowEvent::Loss { .. } => {
                self.events.loss_events += 1;
            }
            WindowEvent::Warning { .. } => {
                self.events.warning_events += 1;
            }
            WindowEvent::UntrackedLate { .. } => {
                self.events.untracked_late_replies += 1;
            }
        }
        update
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
                self.ipdv_pairs.remove(&seq);
                if let Some(next) = seq.checked_add(1) {
                    self.ipdv_pairs.remove(&next);
                }
            }
        }
    }

    fn try_ipdv_pair(&mut self, current_seq: u64) -> Option<IpdvPairUpdate> {
        let previous_seq = current_seq.checked_sub(1)?;

        if !self.ipdv_pairs.insert(current_seq) {
            return None;
        }

        let Some(previous) = self.samples.get(&previous_seq) else {
            self.ipdv_pairs.remove(&current_seq);
            return None;
        };

        let Some(current) = self.samples.get(&current_seq) else {
            self.ipdv_pairs.remove(&current_seq);
            return None;
        };

        // Compute everything before mutating metric fields, otherwise the borrow
        // checker may quite reasonably start throwing furniture.
        let rtt_ipdv = abs_i128_to_u64(current.rtt_primary_ns - previous.rtt_primary_ns);
        let send_ipdv = send_ipdv_ns(previous, current).map(abs_i128_to_u64);
        let receive_ipdv = receive_ipdv_ns(previous, current).map(abs_i128_to_u64);

        self.ipdv_round_trip.push(rtt_ipdv);

        if let Some(value) = send_ipdv {
            self.ipdv_send.push(value);
        }

        if let Some(value) = receive_ipdv {
            self.ipdv_receive.push(value);
        }

        Some(IpdvPairUpdate {
            previous_logical_seq: previous_seq,
            current_logical_seq: current_seq,
            rtt_ipdv: Duration::from_nanos(rtt_ipdv),
            send_ipdv: send_ipdv.map(Duration::from_nanos),
            receive_ipdv: receive_ipdv.map(Duration::from_nanos),
        })
    }

    fn snapshot(&self) -> CumulativeSnapshot {
        let packets = self.packets;
        CumulativeSnapshot {
            events: self.events,
            packets,
            loss: loss_stats(packets),
            send_call: self.send_call.stats(),
            timer_error: self.timer_error.stats(),
            rtt: RttStats {
                primary: self.rtt_primary.stats_with_median(),
                raw: self.rtt_raw.stats_with_median(),
                adjusted: self.rtt_adjusted.stats_with_median(),
            },
            ipdv: IpdvStats {
                round_trip: self.ipdv_round_trip.stats_with_median(),
                send: self.ipdv_send.stats_with_median(),
                receive: self.ipdv_receive.stats_with_median(),
            },
            one_way_delay: OneWayDelayStats {
                send_delay: self.send_delay.stats_with_median(),
                receive_delay: self.receive_delay.stats_with_median(),
            },
            server_processing: ServerProcessingStats {
                processing: self.server_processing.stats(),
            },
        }
    }

    #[cfg(test)]
    fn retained_median_samples(&self) -> usize {
        self.rtt_primary.retained_samples()
            + self.rtt_raw.retained_samples()
            + self.rtt_adjusted.retained_samples()
            + self.ipdv_round_trip.retained_samples()
            + self.ipdv_send.retained_samples()
            + self.ipdv_receive.retained_samples()
            + self.send_delay.retained_samples()
            + self.receive_delay.retained_samples()
    }

    #[cfg(test)]
    fn retained_sequence_samples(&self) -> usize {
        self.samples.len()
    }
}

#[derive(Debug, Clone, PartialEq)]
enum WindowEvent {
    Sent {
        at: Instant,
        bytes: usize,
        send_call_ns: u64,
        timer_error_ns: u64,
    },
    UniqueReply {
        at: Instant,
        is_late: bool,
        sample: Box<UniqueSample>,
    },
    DuplicateReply {
        at: Instant,
    },
    Loss {
        at: Instant,
    },
    Warning {
        at: Instant,
    },
    UntrackedLate {
        at: Instant,
    },
}

impl WindowEvent {
    fn from_client_event(event: &ClientEvent) -> Option<Self> {
        match event {
            ClientEvent::EchoSent {
                sent_at,
                bytes,
                send_call,
                timer_error,
                ..
            } => Some(Self::Sent {
                at: sent_at.mono,
                bytes: *bytes,
                send_call_ns: duration_ns_u64(*send_call),
                timer_error_ns: duration_ns_u64(*timer_error),
            }),
            ClientEvent::EchoReply {
                logical_seq,
                sent_at,
                received_at,
                rtt,
                server_timing,
                one_way,
                received_stats,
                bytes,
                ..
            } => Some(Self::UniqueReply {
                at: received_at.mono,
                is_late: false,
                sample: Box::new(UniqueSample::new(
                    *logical_seq,
                    *sent_at,
                    *received_at,
                    *rtt,
                    *server_timing,
                    *one_way,
                    *received_stats,
                    *bytes,
                )),
            }),
            ClientEvent::LateReply {
                logical_seq: Some(logical_seq),
                sent_at: Some(sent_at),
                received_at,
                rtt: Some(rtt),
                server_timing,
                one_way,
                received_stats,
                bytes,
                ..
            } => Some(Self::UniqueReply {
                at: received_at.mono,
                is_late: true,
                sample: Box::new(UniqueSample::new(
                    *logical_seq,
                    *sent_at,
                    *received_at,
                    *rtt,
                    *server_timing,
                    *one_way,
                    *received_stats,
                    *bytes,
                )),
            }),
            ClientEvent::LateReply { received_at, .. } => Some(Self::UntrackedLate {
                at: received_at.mono,
            }),
            ClientEvent::DuplicateReply { received_at, .. } => Some(Self::DuplicateReply {
                at: received_at.mono,
            }),
            ClientEvent::EchoLoss { timeout_at, .. } => Some(Self::Loss { at: *timeout_at }),
            ClientEvent::Warning { .. } => Some(Self::Warning { at: Instant::now() }),
            ClientEvent::SessionStarted { .. }
            | ClientEvent::NoTestCompleted { .. }
            | ClientEvent::SessionClosed { .. } => None,
        }
    }

    fn at(&self) -> Instant {
        match self {
            Self::Sent { at, .. }
            | Self::UniqueReply { at, .. }
            | Self::DuplicateReply { at }
            | Self::Loss { at }
            | Self::Warning { at }
            | Self::UntrackedLate { at } => *at,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct UniqueSample {
    logical_seq: u64,
    bytes: usize,
    rtt_primary_ns: i128,
    rtt_raw_ns: u64,
    rtt_adjusted_ns: Option<i128>,
    send_delay_ns: Option<u64>,
    receive_delay_ns: Option<u64>,
    server_processing_ns: Option<u64>,
    received_count: Option<u32>,
    received_window: Option<u64>,
    client_send_mono: Instant,
    client_receive_mono: Instant,
    client_send_wall_ns: Option<i128>,
    client_receive_wall_ns: Option<i128>,
    server_receive_mono_ns: Option<i64>,
    server_send_mono_ns: Option<i64>,
    server_receive_wall_ns: Option<i64>,
    server_send_wall_ns: Option<i64>,
}

impl UniqueSample {
    #[allow(clippy::too_many_arguments)]
    fn new(
        logical_seq: u64,
        sent_at: ClientTimestamp,
        received_at: ClientTimestamp,
        rtt: RttSample,
        server_timing: Option<ServerTiming>,
        one_way: Option<OneWayDelaySample>,
        received_stats: Option<ReceivedStatsSample>,
        bytes: usize,
    ) -> Self {
        Self {
            logical_seq,
            bytes,
            rtt_primary_ns: signed_duration_ns(rtt.effective_signed),
            rtt_raw_ns: duration_ns_u64(rtt.raw),
            rtt_adjusted_ns: rtt.adjusted_signed.map(signed_duration_ns),
            send_delay_ns: one_way
                .and_then(|sample| sample.client_to_server)
                .map(duration_ns_u64),
            receive_delay_ns: one_way
                .and_then(|sample| sample.server_to_client)
                .map(duration_ns_u64),
            server_processing_ns: server_timing
                .and_then(|timing| timing.processing)
                .map(duration_ns_u64),
            received_count: received_stats.and_then(|stats| stats.count),
            received_window: received_stats.and_then(|stats| stats.window),
            client_send_mono: sent_at.mono,
            client_receive_mono: received_at.mono,
            client_send_wall_ns: system_time_ns(sent_at.wall),
            client_receive_wall_ns: system_time_ns(received_at.wall),
            server_receive_mono_ns: server_timing.and_then(|timing| timing.receive_mono_ns),
            server_send_mono_ns: server_timing.and_then(|timing| timing.send_mono_ns),
            server_receive_wall_ns: server_timing.and_then(|timing| timing.receive_wall_ns),
            server_send_wall_ns: server_timing.and_then(|timing| timing.send_wall_ns),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct MetricU64 {
    running: RunningU64,
    samples: Option<Vec<u64>>,
}

impl MetricU64 {
    fn new(retain_samples: bool) -> Self {
        Self {
            running: RunningU64::default(),
            samples: retain_samples.then(Vec::new),
        }
    }

    fn push(&mut self, value: u64) {
        self.running.push(value);
        if let Some(samples) = self.samples.as_mut() {
            samples.push(value);
        }
    }

    fn stats(&self) -> DurationStats {
        self.running.stats()
    }

    fn stats_with_median(&self) -> DurationStatsWithMedian {
        DurationStatsWithMedian {
            stats: self.stats(),
            median_ns: self
                .samples
                .as_ref()
                .and_then(|samples| median_u64(samples)),
        }
    }

    #[cfg(test)]
    fn retained_samples(&self) -> usize {
        self.samples.as_ref().map_or(0, Vec::len)
    }
}

#[derive(Debug, Clone, PartialEq)]
struct MetricI128 {
    running: RunningI128,
    samples: Option<Vec<i128>>,
}

impl MetricI128 {
    fn new(retain_samples: bool) -> Self {
        Self {
            running: RunningI128::default(),
            samples: retain_samples.then(Vec::new),
        }
    }

    fn push(&mut self, value: i128) {
        self.running.push(value);
        if let Some(samples) = self.samples.as_mut() {
            samples.push(value);
        }
    }

    fn stats(&self) -> SignedDurationStats {
        self.running.stats()
    }

    fn stats_with_median(&self) -> SignedDurationStatsWithMedian {
        SignedDurationStatsWithMedian {
            stats: self.stats(),
            median_ns: self
                .samples
                .as_ref()
                .and_then(|samples| median_i128(samples)),
        }
    }

    #[cfg(test)]
    fn retained_samples(&self) -> usize {
        self.samples.as_ref().map_or(0, Vec::len)
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
struct RunningU64 {
    count: u64,
    total: u128,
    min: Option<u64>,
    max: Option<u64>,
    mean: f64,
    m2: f64,
}

impl RunningU64 {
    fn push(&mut self, value: u64) {
        self.count += 1;
        self.total = self.total.saturating_add(u128::from(value));
        self.min = Some(self.min.map_or(value, |min| min.min(value)));
        self.max = Some(self.max.map_or(value, |max| max.max(value)));
        let x = value as f64;
        let delta = x - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = x - self.mean;
        self.m2 += delta * delta2;
    }

    fn stats(&self) -> DurationStats {
        DurationStats {
            count: self.count,
            total_ns: self.total,
            min_ns: self.min,
            max_ns: self.max,
            mean_ns: if self.count == 0 { 0.0 } else { self.mean },
            variance_ns2: sample_variance(self.count, self.m2),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
struct RunningI128 {
    count: u64,
    total: i128,
    min: Option<i128>,
    max: Option<i128>,
    mean: f64,
    m2: f64,
}

impl RunningI128 {
    fn push(&mut self, value: i128) {
        self.count += 1;
        self.total = self.total.saturating_add(value);
        self.min = Some(self.min.map_or(value, |min| min.min(value)));
        self.max = Some(self.max.map_or(value, |max| max.max(value)));
        let x = value as f64;
        let delta = x - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = x - self.mean;
        self.m2 += delta * delta2;
    }

    fn stats(&self) -> SignedDurationStats {
        SignedDurationStats {
            count: self.count,
            total_ns: self.total,
            min_ns: self.min,
            max_ns: self.max,
            mean_ns: if self.count == 0 { 0.0 } else { self.mean },
            variance_ns2: sample_variance(self.count, self.m2),
        }
    }
}

fn snapshot_window(events: &VecDeque<WindowEvent>) -> CumulativeSnapshot {
    let mut core = CoreStats::new(MedianMode::Disabled);
    for event in events {
        core.apply(event.clone());
    }
    core.snapshot()
}

fn sample_variance(count: u64, m2: f64) -> f64 {
    if count < 2 {
        0.0
    } else {
        m2 / (count - 1) as f64
    }
}

fn median_u64(samples: &[u64]) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    Some(median_sorted_u64(&sorted))
}

fn median_sorted_u64(sorted: &[u64]) -> f64 {
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        sorted[mid] as f64
    } else {
        (sorted[mid - 1] as f64 + sorted[mid] as f64) / 2.0
    }
}

fn median_i128(samples: &[i128]) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let mid = sorted.len() / 2;
    Some(if sorted.len() % 2 == 1 {
        sorted[mid] as f64
    } else {
        (sorted[mid - 1] as f64 + sorted[mid] as f64) / 2.0
    })
}

fn loss_stats(packets: PacketCounts) -> LossStats {
    let lost = packets.packets_sent.saturating_sub(packets.unique_replies);
    let packet_loss_percent = if packets.packets_sent == 0 {
        0.0
    } else if packets.unique_replies == 0 {
        100.0
    } else {
        percent(lost as f64, packets.packets_sent as f64)
    };

    let (
        upstream_loss_packets,
        upstream_loss_percent,
        downstream_loss_packets,
        downstream_loss_percent,
    ) = if let Some(server_received) = packets.server_packets_received {
        let upstream = i128::from(packets.packets_sent) - i128::from(server_received);
        let downstream = i128::from(server_received) - i128::from(packets.packets_received);
        let upstream_percent = if packets.packets_sent == 0 {
            0.0
        } else {
            percent(upstream as f64, packets.packets_sent as f64)
        };
        let downstream_percent = if server_received == 0 {
            0.0
        } else {
            percent(downstream as f64, server_received as f64)
        };
        (
            Some(upstream),
            upstream_percent,
            Some(downstream),
            downstream_percent,
        )
    } else {
        (None, 0.0, None, 0.0)
    };

    LossStats {
        lost_packets: lost,
        unknown_loss_packets: lost,
        upstream_loss_packets,
        downstream_loss_packets,
        packet_loss_percent,
        upstream_loss_percent,
        downstream_loss_percent,
        duplicate_percent: if packets.packets_received == 0 {
            0.0
        } else {
            percent(packets.duplicates as f64, packets.packets_received as f64)
        },
        late_packets_percent: if packets.packets_received == 0 {
            0.0
        } else {
            percent(packets.late_packets as f64, packets.packets_received as f64)
        },
    }
}

fn percent(numerator: f64, denominator: f64) -> f64 {
    100.0 * numerator / denominator
}

fn send_ipdv_ns(previous: &UniqueSample, current: &UniqueSample) -> Option<i128> {
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

fn receive_ipdv_ns(previous: &UniqueSample, current: &UniqueSample) -> Option<i128> {
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

fn system_time_ns(time: SystemTime) -> Option<i128> {
    if let Ok(duration) = time.duration_since(UNIX_EPOCH) {
        return Some(duration_ns_i128(duration));
    }
    UNIX_EPOCH
        .duration_since(time)
        .ok()
        .map(|duration| -duration_ns_i128(duration))
}

fn duration_ns_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn duration_ns_i128(duration: Duration) -> i128 {
    i128::try_from(duration.as_nanos()).unwrap_or(i128::MAX)
}

fn signed_duration_ns(duration: SignedDuration) -> i128 {
    duration.ns
}

fn abs_i128_to_u64(value: i128) -> u64 {
    u64::try_from(value.saturating_abs()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use irtt_client::{
        ClientTimestamp, PacketMeta, ReceivedStatsSample, RttSample, ServerTiming, SignedDuration,
    };
    use std::time::SystemTime;

    fn ts(ms: u64) -> ClientTimestamp {
        ClientTimestamp {
            mono: Instant::now() + Duration::from_millis(ms),
            wall: UNIX_EPOCH + Duration::from_millis(ms),
        }
    }

    fn rtt(raw_ms: u64, effective_ms: i128) -> RttSample {
        RttSample {
            raw: Duration::from_millis(raw_ms),
            adjusted: u64::try_from(effective_ms).ok().map(Duration::from_millis),
            effective: u64::try_from(effective_ms)
                .ok()
                .map(Duration::from_millis)
                .unwrap_or_else(|| Duration::from_millis(raw_ms)),
            adjusted_signed: Some(SignedDuration {
                ns: effective_ms * 1_000_000,
            }),
            effective_signed: SignedDuration {
                ns: effective_ms * 1_000_000,
            },
        }
    }

    fn sent(seq: u32, logical_seq: u64, sent_at: ClientTimestamp) -> ClientEvent {
        ClientEvent::EchoSent {
            seq,
            logical_seq,
            remote: "127.0.0.1:2112".parse().unwrap(),
            scheduled_at: sent_at.mono,
            sent_at,
            bytes: 32,
            send_call: Duration::from_micros(10),
            timer_error: Duration::from_micros(2),
        }
    }

    fn reply(logical_seq: u64, raw_ms: u64, effective_ms: i128) -> ClientEvent {
        let sent_at = ts(logical_seq * 10);
        let received_at = ClientTimestamp {
            mono: sent_at.mono + Duration::from_millis(raw_ms),
            wall: sent_at.wall + Duration::from_millis(raw_ms),
        };
        ClientEvent::EchoReply {
            seq: logical_seq as u32,
            logical_seq,
            remote: "127.0.0.1:2112".parse().unwrap(),
            sent_at,
            received_at,
            rtt: rtt(raw_ms, effective_ms),
            server_timing: Some(ServerTiming {
                receive_wall_ns: Some(system_time_ns(sent_at.wall).unwrap() as i64 + 1_000_000),
                receive_mono_ns: Some(logical_seq as i64 * 10_000_000 + 1_000_000),
                send_wall_ns: Some(system_time_ns(sent_at.wall).unwrap() as i64 + 2_000_000),
                send_mono_ns: Some(logical_seq as i64 * 10_000_000 + 2_000_000),
                midpoint_wall_ns: None,
                midpoint_mono_ns: None,
                processing: Some(Duration::from_millis(1)),
            }),
            one_way: Some(OneWayDelaySample {
                client_to_server: Some(Duration::from_millis(1)),
                server_to_client: Some(Duration::from_millis(2)),
            }),
            received_stats: Some(ReceivedStatsSample {
                count: Some((logical_seq + 1) as u32),
                window: Some(0xff),
            }),
            bytes: 64,
            packet_meta: PacketMeta::default(),
        }
    }

    fn late_reply(logical_seq: u64, raw_ms: u64, effective_ms: i128) -> ClientEvent {
        let ClientEvent::EchoReply {
            seq,
            remote,
            sent_at,
            received_at,
            rtt,
            server_timing,
            one_way,
            received_stats,
            bytes,
            packet_meta,
            ..
        } = reply(logical_seq, raw_ms, effective_ms)
        else {
            unreachable!();
        };
        ClientEvent::LateReply {
            seq,
            logical_seq: Some(logical_seq),
            highest_seen: seq + 1,
            remote,
            sent_at: Some(sent_at),
            received_at,
            rtt: Some(rtt),
            server_timing,
            one_way,
            received_stats,
            bytes,
            packet_meta,
        }
    }

    #[test]
    fn running_duration_stats_use_sample_variance() {
        let mut metric = MetricU64::new(false);
        metric.push(1);
        metric.push(2);
        metric.push(3);
        let stats = metric.stats();
        assert_eq!(stats.count, 3);
        assert_eq!(stats.total_ns, 6);
        assert_eq!(stats.min_ns, Some(1));
        assert_eq!(stats.max_ns, Some(3));
        assert_eq!(stats.mean_ns, 2.0);
        assert_eq!(stats.variance_ns2, 1.0);
        assert_eq!(stats.stddev_ns(), 1.0);
    }

    #[test]
    fn exact_median_handles_odd_and_even_samples() {
        assert_eq!(median_u64(&[3, 1, 2]), Some(2.0));
        assert_eq!(median_u64(&[4, 1, 2, 3]), Some(2.5));
        assert_eq!(median_i128(&[-5, 1, 3]), Some(1.0));
        assert_eq!(median_i128(&[-5, 1, 3, 7]), Some(2.0));
    }

    #[test]
    fn disabled_median_avoids_finite_retention() {
        let mut collector = StatsCollector::new(StatsConfig::continuous());
        collector.process(&reply(0, 10, 9));
        collector.process(&reply(1, 20, 19));
        assert_eq!(collector.retained_median_samples(), 0);
        assert_eq!(collector.cumulative().rtt.primary.median_ns, None);
    }

    #[test]
    fn continuous_mode_bounds_sequence_tracking() {
        let mut collector = StatsCollector::new(StatsConfig::continuous());
        for seq in 0..(CONTINUOUS_SEQUENCE_LIMIT as u64 + 8) {
            collector.process(&reply(seq, 10, 10));
        }

        assert_eq!(collector.retained_median_samples(), 0);
        assert!(collector.retained_sequence_samples() <= CONTINUOUS_SEQUENCE_LIMIT);
        assert_eq!(
            collector.cumulative().rtt.primary.stats.count,
            CONTINUOUS_SEQUENCE_LIMIT as u64 + 8
        );
    }

    #[test]
    fn cumulative_rtt_uses_signed_effective_and_tracks_raw() {
        let mut collector = StatsCollector::new(StatsConfig::finite());
        collector.process(&reply(0, 1, -2));
        collector.process(&reply(1, 10, 8));

        let snapshot = collector.cumulative();
        assert_eq!(snapshot.rtt.primary.stats.count, 2);
        assert_eq!(snapshot.rtt.primary.stats.min_ns, Some(-2_000_000));
        assert_eq!(snapshot.rtt.primary.median_ns, Some(3_000_000.0));
        assert_eq!(snapshot.rtt.raw.stats.total_ns, 11_000_000);
    }

    #[test]
    fn late_unique_counts_and_duplicates_do_not_update_measurements() {
        let mut collector = StatsCollector::new(StatsConfig::finite());
        collector.process(&sent(0, 0, ts(0)));
        collector.process(&sent(1, 1, ts(10)));
        collector.process(&reply(1, 10, 9));
        collector.process(&late_reply(0, 20, 19));
        collector.process(&ClientEvent::DuplicateReply {
            seq: 0,
            remote: "127.0.0.1:2112".parse().unwrap(),
            received_at: ts(50),
            bytes: 64,
        });

        let snapshot = collector.cumulative();
        assert_eq!(snapshot.packets.packets_sent, 2);
        assert_eq!(snapshot.packets.packets_received, 3);
        assert_eq!(snapshot.packets.unique_replies, 2);
        assert_eq!(snapshot.packets.duplicates, 1);
        assert_eq!(snapshot.packets.late_packets, 1);
        assert_eq!(snapshot.rtt.primary.stats.count, 2);
        assert_eq!(snapshot.loss.lost_packets, 0);
        assert_eq!(snapshot.loss.duplicate_percent, 100.0 / 3.0);
    }

    #[test]
    fn final_loss_uses_sent_minus_unique_replies_not_echo_loss_events() {
        let mut collector = StatsCollector::new(StatsConfig::finite());
        collector.process(&sent(0, 0, ts(0)));
        collector.process(&sent(1, 1, ts(10)));
        collector.process(&ClientEvent::EchoLoss {
            seq: 0,
            logical_seq: 0,
            sent_at: ts(0),
            timeout_at: Instant::now(),
        });
        collector.process(&late_reply(0, 10, 9));

        let snapshot = collector.summary();
        assert_eq!(snapshot.events.loss_events, 1);
        assert_eq!(snapshot.packets.unique_replies, 1);
        assert_eq!(snapshot.loss.lost_packets, 1);
        assert_eq!(snapshot.loss.packet_loss_percent, 50.0);
    }

    fn assert_no_ipdv_pairs(update: &EventStatsUpdate) {
        assert!(update.ipdv_pairs.is_empty(), "{update:?}");
    }

    fn assert_one_ipdv_pair(
        update: &EventStatsUpdate,
        previous_logical_seq: u64,
        current_logical_seq: u64,
        rtt_ipdv: Duration,
    ) -> &IpdvPairUpdate {
        assert_eq!(update.ipdv_pairs.len(), 1, "{update:?}");
        let pair = &update.ipdv_pairs[0];
        assert_eq!(pair.previous_logical_seq, previous_logical_seq);
        assert_eq!(pair.current_logical_seq, current_logical_seq);
        assert_eq!(pair.rtt_ipdv, rtt_ipdv);
        pair
    }

    #[test]
    fn ipdv_is_sequence_adjacent_and_gap_preserving() {
        let mut collector = StatsCollector::new(StatsConfig::finite());
        let first = collector.process(&reply(0, 10, 10));
        let gap = collector.process(&reply(2, 15, 15));
        let adjacent = collector.process(&reply(3, 12, 12));

        let snapshot = collector.cumulative();
        assert!(first.contributed_sample);
        assert_no_ipdv_pairs(&first);

        assert!(gap.contributed_sample);
        assert_no_ipdv_pairs(&gap);

        assert!(adjacent.contributed_sample);
        assert_one_ipdv_pair(&adjacent, 2, 3, Duration::from_millis(3));
        assert_eq!(snapshot.ipdv.round_trip.stats.count, 1);
        assert_eq!(snapshot.ipdv.round_trip.stats.total_ns, 3_000_000);
    }

    #[test]
    fn late_reply_can_complete_ipdv_pair() {
        let mut collector = StatsCollector::new(StatsConfig::finite());
        collector.process(&reply(1, 20, 20));
        let update = collector.process(&late_reply(0, 10, 10));

        let snapshot = collector.cumulative();

        assert!(update.contributed_sample);
        assert_one_ipdv_pair(&update, 0, 1, Duration::from_millis(10));

        assert_eq!(snapshot.ipdv.round_trip.stats.count, 1);
        assert_eq!(snapshot.ipdv.round_trip.stats.total_ns, 10_000_000);
    }

    #[test]
    fn update_exposes_directional_ipdv_when_available() {
        let mut collector = StatsCollector::new(StatsConfig::finite());
        collector.process(&reply(0, 10, 10));
        let update = collector.process(&reply(1, 13, 13));

        assert!(update.contributed_sample);

        let pair = assert_one_ipdv_pair(&update, 0, 1, Duration::from_millis(3));
        assert!(pair.send_ipdv.is_some());
        assert!(pair.receive_ipdv.is_some());
    }

    #[test]
    fn gap_fill_update_exposes_both_completed_ipdv_pairs() {
        let mut collector = StatsCollector::new(StatsConfig::finite());

        let first = collector.process(&reply(0, 10, 10));
        let gap = collector.process(&reply(2, 20, 20));
        let fill = collector.process(&reply(1, 13, 13));

        assert!(first.contributed_sample);
        assert!(first.ipdv_pairs.is_empty());

        assert!(gap.contributed_sample);
        assert!(gap.ipdv_pairs.is_empty());

        assert!(fill.contributed_sample);
        assert_eq!(fill.ipdv_pairs.len(), 2);

        assert_eq!(fill.ipdv_pairs[0].previous_logical_seq, 0);
        assert_eq!(fill.ipdv_pairs[0].current_logical_seq, 1);
        assert_eq!(fill.ipdv_pairs[0].rtt_ipdv, Duration::from_millis(3));

        assert_eq!(fill.ipdv_pairs[1].previous_logical_seq, 1);
        assert_eq!(fill.ipdv_pairs[1].current_logical_seq, 2);
        assert_eq!(fill.ipdv_pairs[1].rtt_ipdv, Duration::from_millis(7));

        let snapshot = collector.cumulative();
        assert_eq!(snapshot.ipdv.round_trip.stats.count, 2);
        assert_eq!(snapshot.ipdv.round_trip.stats.total_ns, 10_000_000);
    }

    #[test]
    fn server_processing_and_one_way_require_available_samples() {
        let mut collector = StatsCollector::new(StatsConfig::finite());
        collector.process(&reply(0, 10, 9));
        collector.process(&ClientEvent::LateReply {
            seq: 9,
            logical_seq: None,
            highest_seen: 10,
            remote: "127.0.0.1:2112".parse().unwrap(),
            sent_at: None,
            received_at: ts(100),
            rtt: None,
            server_timing: None,
            one_way: None,
            received_stats: None,
            bytes: 64,
            packet_meta: PacketMeta::default(),
        });

        let snapshot = collector.cumulative();
        assert_eq!(snapshot.server_processing.processing.count, 1);
        assert_eq!(snapshot.one_way_delay.send_delay.stats.count, 1);
        assert_eq!(snapshot.events.untracked_late_replies, 1);
    }

    #[test]
    fn rolling_count_eviction_recomputes_from_bounded_events() {
        let mut collector = StatsCollector::new(StatsConfig {
            median: MedianMode::Disabled,
            rolling_count: Some(2),
            rolling_time: None,
        });
        collector.process(&sent(0, 0, ts(0)));
        collector.process(&reply(0, 10, 10));
        collector.process(&reply(1, 20, 20));

        let rolling = collector.rolling_count().unwrap();
        assert_eq!(rolling.packets.packets_sent, 0);
        assert_eq!(rolling.packets.unique_replies, 2);
        assert_eq!(rolling.rtt.primary.stats.count, 2);
    }

    #[test]
    fn rolling_time_eviction_uses_event_timestamps() {
        let mut collector = StatsCollector::new(StatsConfig {
            median: MedianMode::Disabled,
            rolling_count: None,
            rolling_time: Some(Duration::from_millis(15)),
        });
        collector.process(&sent(0, 0, ts(0)));
        collector.process(&sent(1, 1, ts(10)));
        collector.process(&sent(2, 2, ts(30)));

        let rolling = collector.rolling_time().unwrap();
        assert_eq!(rolling.packets.packets_sent, 1);
    }

    #[test]
    fn empty_and_all_lost_edges_are_defined() {
        let empty = StatsCollector::new(StatsConfig::finite()).summary();
        assert_eq!(empty.loss.packet_loss_percent, 0.0);

        let mut collector = StatsCollector::new(StatsConfig::finite());
        collector.process(&sent(0, 0, ts(0)));
        let all_lost = collector.summary();
        assert_eq!(all_lost.loss.lost_packets, 1);
        assert_eq!(all_lost.loss.packet_loss_percent, 100.0);
    }

    #[test]
    fn directional_loss_uses_server_received_count_when_available() {
        let mut collector = StatsCollector::new(StatsConfig::finite());
        collector.process(&sent(0, 0, ts(0)));
        collector.process(&sent(1, 1, ts(10)));
        collector.process(&reply(0, 10, 10));

        let loss = collector.summary().loss;
        assert_eq!(loss.upstream_loss_packets, Some(1));
        assert_eq!(loss.downstream_loss_packets, Some(0));
        assert_eq!(loss.upstream_loss_percent, 50.0);
    }

    #[test]
    fn single_sample_stddev_is_zero() {
        let mut metric = MetricU64::new(false);
        metric.push(42);
        let stats = metric.stats();
        assert_eq!(stats.variance_ns2, 0.0);
        assert_eq!(stats.stddev_ns(), 0.0);
    }

    #[test]
    fn system_time_before_epoch_is_supported() {
        let before = UNIX_EPOCH - Duration::from_nanos(7);
        assert_eq!(system_time_ns(before), Some(-7));
        let after = UNIX_EPOCH + Duration::from_nanos(7);
        assert_eq!(system_time_ns(after), Some(7));
        let now = SystemTime::now();
        assert!(system_time_ns(now).is_some());
    }
}
