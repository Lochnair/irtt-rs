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
    config: StatsConfig,
    cumulative: CoreStats,
    rolling_count: Option<VecDeque<StatsEvent>>,
    rolling_time: Option<VecDeque<StatsEvent>>,
}

impl StatsCollector {
    pub fn new(config: StatsConfig) -> Self {
        Self {
            config,
            cumulative: CoreStats::new(config.samples),
            rolling_count: config.rolling_count.map(|_| VecDeque::new()),
            rolling_time: config.rolling_time.map(|_| VecDeque::new()),
        }
    }

    pub fn process(&mut self, event: &ClientEvent) -> EventStatsUpdate {
        let Some(stats_event) = normalize_event(event) else {
            return EventStatsUpdate::default();
        };

        let update = self.cumulative.apply(stats_event.clone());

        if let (Some(limit), Some(window)) =
            (self.config.rolling_count, self.rolling_count.as_mut())
        {
            window.push_back(stats_event.clone());
            while window.len() > limit {
                window.pop_front();
            }
        }

        if let (Some(duration), Some(window)) =
            (self.config.rolling_time, self.rolling_time.as_mut())
        {
            let cutoff = stats_event.at().checked_sub(duration);
            window.push_back(stats_event);
            if let Some(cutoff) = cutoff {
                while window.front().is_some_and(|event| event.at() < cutoff) {
                    window.pop_front();
                }
            }
        }

        update
    }

    pub fn snapshot(&self) -> Snapshot {
        self.cumulative.snapshot()
    }

    pub fn rolling_count(&self) -> Option<Snapshot> {
        self.rolling_count.as_ref().map(snapshot_window)
    }

    pub fn rolling_time(&self) -> Option<Snapshot> {
        self.rolling_time.as_ref().map(snapshot_window)
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TimeStats {
    pub count: u64,
    pub total_ns: i128,
    pub min_ns: Option<i128>,
    pub max_ns: Option<i128>,
    pub mean_ns: f64,
    pub median_ns: Option<f64>,
    pub variance_ns2: f64,
}

impl TimeStats {
    pub fn stddev_ns(&self) -> f64 {
        self.variance_ns2.sqrt()
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
}

#[derive(Debug, Clone, PartialEq)]
struct CoreStats {
    events: EventCounts,
    packets: PacketCounts,
    send_call: TimeMetric,
    timer_error: TimeMetric,
    rtt_primary: TimeMetric,
    rtt_raw: TimeMetric,
    rtt_adjusted: TimeMetric,
    ipdv_round_trip: TimeMetric,
    ipdv_send: TimeMetric,
    ipdv_receive: TimeMetric,
    send_delay: TimeMetric,
    receive_delay: TimeMetric,
    server_processing: TimeMetric,
    ipdv_tracker: IpdvTracker,
}

impl CoreStats {
    fn new(sample_mode: SampleMode) -> Self {
        let sequence_limit = if sample_mode == SampleMode::Exact {
            None
        } else {
            Some(CONTINUOUS_SEQUENCE_LIMIT)
        };

        Self {
            events: EventCounts::default(),
            packets: PacketCounts::default(),
            send_call: TimeMetric::new(false),
            timer_error: TimeMetric::new(false),
            rtt_primary: TimeMetric::new(sample_mode == SampleMode::Exact),
            rtt_raw: TimeMetric::new(sample_mode == SampleMode::Exact),
            rtt_adjusted: TimeMetric::new(sample_mode == SampleMode::Exact),
            ipdv_round_trip: TimeMetric::new(sample_mode == SampleMode::Exact),
            ipdv_send: TimeMetric::new(sample_mode == SampleMode::Exact),
            ipdv_receive: TimeMetric::new(sample_mode == SampleMode::Exact),
            send_delay: TimeMetric::new(sample_mode == SampleMode::Exact),
            receive_delay: TimeMetric::new(sample_mode == SampleMode::Exact),
            server_processing: TimeMetric::new(false),
            ipdv_tracker: IpdvTracker::new(sequence_limit),
        }
    }

    fn apply(&mut self, event: StatsEvent) -> EventStatsUpdate {
        let mut update = EventStatsUpdate::default();
        match event {
            StatsEvent::Sent {
                bytes,
                send_call_ns,
                timer_error_ns,
                ..
            } => {
                self.events.sent_events += 1;
                self.packets.packets_sent += 1;
                self.packets.bytes_sent = self.packets.bytes_sent.saturating_add(bytes as u64);
                self.send_call.push_ns(send_call_ns);
                self.timer_error.push_ns(timer_error_ns);
            }
            StatsEvent::UniqueReply {
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

                self.rtt_primary.push_ns(sample.ipdv.rtt_primary_ns);
                self.rtt_raw.push_ns(sample.rtt_raw_ns);
                if let Some(adjusted) = sample.rtt_adjusted_ns {
                    self.rtt_adjusted.push_ns(adjusted);
                }
                if let Some(processing) = sample.server_processing_ns {
                    self.server_processing.push_ns(processing);
                }
                if let Some(delay) = sample.send_delay_ns {
                    self.send_delay.push_ns(delay);
                }
                if let Some(delay) = sample.receive_delay_ns {
                    self.receive_delay.push_ns(delay);
                }

                for pair in self.ipdv_tracker.insert(sample.ipdv) {
                    let Some(rtt_ipdv) = duration_from_non_negative_i128_ns(pair.rtt_ipdv_ns)
                    else {
                        continue;
                    };
                    let send_ipdv = pair
                        .send_ipdv_ns
                        .and_then(duration_from_non_negative_i128_ns);
                    let receive_ipdv = pair
                        .receive_ipdv_ns
                        .and_then(duration_from_non_negative_i128_ns);

                    self.ipdv_round_trip.push_ns(pair.rtt_ipdv_ns);

                    if let Some(value) = pair.send_ipdv_ns {
                        self.ipdv_send.push_ns(value);
                    }

                    if let Some(value) = pair.receive_ipdv_ns {
                        self.ipdv_receive.push_ns(value);
                    }

                    update.ipdv_pairs.push(IpdvPairUpdate {
                        previous_seq: pair.previous_seq,
                        current_seq: pair.current_seq,
                        rtt_ipdv,
                        send_ipdv,
                        receive_ipdv,
                    });
                }
            }
            StatsEvent::DuplicateReply { .. } => {
                self.events.duplicate_replies += 1;
                self.packets.packets_received += 1;
                self.packets.duplicates += 1;
            }
            StatsEvent::Loss { .. } => {
                self.events.loss_events += 1;
            }
            StatsEvent::Warning { .. } => {
                self.events.warning_events += 1;
            }
            StatsEvent::UntrackedLate { .. } => {
                self.events.untracked_late_replies += 1;
            }
        }
        update
    }

    fn snapshot(&self) -> Snapshot {
        let packets = self.packets;
        Snapshot {
            events: self.events,
            packets,
            loss: loss_stats(packets),
            send_call: self.send_call.stats(),
            timer_error: self.timer_error.stats(),
            rtt: RttStats {
                primary: self.rtt_primary.stats(),
                raw: self.rtt_raw.stats(),
                adjusted: self.rtt_adjusted.stats(),
            },
            ipdv: IpdvStats {
                round_trip: self.ipdv_round_trip.stats(),
                send: self.ipdv_send.stats(),
                receive: self.ipdv_receive.stats(),
            },
            one_way_delay: OneWayDelayStats {
                send_delay: self.send_delay.stats(),
                receive_delay: self.receive_delay.stats(),
            },
            server_processing: ServerProcessingStats {
                processing: self.server_processing.stats(),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum StatsEvent {
    Sent {
        at: Instant,
        bytes: usize,
        send_call_ns: i128,
        timer_error_ns: i128,
    },
    UniqueReply {
        at: Instant,
        is_late: bool,
        sample: Box<ReplySample>,
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

fn normalize_event(event: &ClientEvent) -> Option<StatsEvent> {
    match event {
        ClientEvent::EchoSent {
            sent_at,
            bytes,
            send_call,
            timer_error,
            ..
        } => Some(StatsEvent::Sent {
            at: sent_at.mono,
            bytes: *bytes,
            send_call_ns: duration_ns_i128(*send_call),
            timer_error_ns: duration_ns_i128(*timer_error),
        }),
        ClientEvent::EchoReply {
            seq,
            sent_at,
            received_at,
            rtt,
            server_timing,
            one_way,
            received_stats,
            bytes,
            ..
        } => Some(StatsEvent::UniqueReply {
            at: received_at.mono,
            is_late: false,
            sample: Box::new(ReplySample::from_reply_parts(
                *seq,
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
            seq,
            sent_at: Some(sent_at),
            received_at,
            rtt: Some(rtt),
            server_timing,
            one_way,
            received_stats,
            bytes,
            ..
        } => Some(StatsEvent::UniqueReply {
            at: received_at.mono,
            is_late: true,
            sample: Box::new(ReplySample::from_reply_parts(
                *seq,
                *sent_at,
                *received_at,
                *rtt,
                *server_timing,
                *one_way,
                *received_stats,
                *bytes,
            )),
        }),
        ClientEvent::LateReply { received_at, .. } => Some(StatsEvent::UntrackedLate {
            at: received_at.mono,
        }),
        ClientEvent::DuplicateReply { received_at, .. } => Some(StatsEvent::DuplicateReply {
            at: received_at.mono,
        }),
        ClientEvent::EchoLoss { timeout_at, .. } => Some(StatsEvent::Loss { at: *timeout_at }),
        ClientEvent::Warning { at, .. } => Some(StatsEvent::Warning { at: at.mono }),
        ClientEvent::SessionStarted { .. }
        | ClientEvent::NoTestCompleted { .. }
        | ClientEvent::SessionClosed { .. } => None,
    }
}

impl StatsEvent {
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
struct ReplySample {
    bytes: usize,
    rtt_raw_ns: i128,
    rtt_adjusted_ns: Option<i128>,
    send_delay_ns: Option<i128>,
    receive_delay_ns: Option<i128>,
    server_processing_ns: Option<i128>,
    received_count: Option<u32>,
    received_window: Option<u64>,
    ipdv: IpdvSample,
}

#[derive(Debug, Clone, PartialEq)]
struct IpdvSample {
    seq: u32,
    rtt_primary_ns: i128,
    client_send_mono: Instant,
    client_receive_mono: Instant,
    client_send_wall_ns: Option<i128>,
    client_receive_wall_ns: Option<i128>,
    server_receive_mono_ns: Option<i64>,
    server_send_mono_ns: Option<i64>,
    server_receive_wall_ns: Option<i64>,
    server_send_wall_ns: Option<i64>,
}

impl ReplySample {
    #[allow(clippy::too_many_arguments)]
    fn from_reply_parts(
        seq: u32,
        sent_at: ClientTimestamp,
        received_at: ClientTimestamp,
        rtt: RttSample,
        server_timing: Option<ServerTiming>,
        one_way: Option<OneWayDelaySample>,
        received_stats: Option<ReceivedStatsSample>,
        bytes: usize,
    ) -> Self {
        let rtt_primary_ns = signed_duration_ns(rtt.effective_signed);
        Self {
            bytes,
            rtt_raw_ns: duration_ns_i128(rtt.raw),
            rtt_adjusted_ns: rtt.adjusted_signed.map(signed_duration_ns),
            send_delay_ns: one_way
                .and_then(|sample| sample.client_to_server)
                .map(duration_ns_i128),
            receive_delay_ns: one_way
                .and_then(|sample| sample.server_to_client)
                .map(duration_ns_i128),
            server_processing_ns: server_timing
                .and_then(|timing| timing.processing)
                .map(duration_ns_i128),
            received_count: received_stats.and_then(|stats| stats.count),
            received_window: received_stats.and_then(|stats| stats.window),
            ipdv: IpdvSample {
                seq,
                rtt_primary_ns,
                client_send_mono: sent_at.mono,
                client_receive_mono: received_at.mono,
                client_send_wall_ns: system_time_ns(sent_at.wall),
                client_receive_wall_ns: system_time_ns(received_at.wall),
                server_receive_mono_ns: server_timing.and_then(|timing| timing.receive_mono_ns),
                server_send_mono_ns: server_timing.and_then(|timing| timing.send_mono_ns),
                server_receive_wall_ns: server_timing.and_then(|timing| timing.receive_wall_ns),
                server_send_wall_ns: server_timing.and_then(|timing| timing.send_wall_ns),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct IpdvTracker {
    samples: HashMap<u32, IpdvSample>,
    sample_order: VecDeque<u32>,
    completed_pairs: HashSet<u32>,
    sequence_limit: Option<usize>,
}

impl IpdvTracker {
    fn new(sequence_limit: Option<usize>) -> Self {
        Self {
            samples: HashMap::new(),
            sample_order: VecDeque::new(),
            completed_pairs: HashSet::new(),
            sequence_limit,
        }
    }

    fn insert(&mut self, sample: IpdvSample) -> Vec<CompletedIpdvPair> {
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
        if let Some(next) = seq.checked_add(1) {
            if let Some(pair) = self.try_pair(next) {
                pairs.push(pair);
            }
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
                if let Some(next) = seq.checked_add(1) {
                    self.completed_pairs.remove(&next);
                }
            }
        }
    }

    fn try_pair(&mut self, current_seq: u32) -> Option<CompletedIpdvPair> {
        let previous_seq = current_seq.checked_sub(1)?;

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
struct CompletedIpdvPair {
    previous_seq: u32,
    current_seq: u32,
    rtt_ipdv_ns: i128,
    send_ipdv_ns: Option<i128>,
    receive_ipdv_ns: Option<i128>,
}

#[derive(Debug, Clone, PartialEq)]
struct TimeMetric {
    running: RunningStats,
    samples: Option<Vec<i128>>,
}

impl TimeMetric {
    fn new(retain_samples: bool) -> Self {
        Self {
            running: RunningStats::default(),
            samples: retain_samples.then(Vec::new),
        }
    }

    fn push_ns(&mut self, value: i128) {
        self.running.push(value);
        if let Some(samples) = self.samples.as_mut() {
            samples.push(value);
        }
    }

    fn stats(&self) -> TimeStats {
        self.running
            .stats(self.samples.as_ref().and_then(|samples| median_ns(samples)))
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
struct RunningStats {
    count: u64,
    total_ns: i128,
    min_ns: Option<i128>,
    max_ns: Option<i128>,
    mean_ns: f64,
    m2_ns2: f64,
}

impl RunningStats {
    fn push(&mut self, value: i128) {
        self.count += 1;
        self.total_ns = self.total_ns.saturating_add(value);
        self.min_ns = Some(self.min_ns.map_or(value, |min| min.min(value)));
        self.max_ns = Some(self.max_ns.map_or(value, |max| max.max(value)));
        let x = value as f64;
        let delta = x - self.mean_ns;
        self.mean_ns += delta / self.count as f64;
        let delta2 = x - self.mean_ns;
        self.m2_ns2 += delta * delta2;
    }

    fn stats(&self, median_ns: Option<f64>) -> TimeStats {
        TimeStats {
            count: self.count,
            total_ns: self.total_ns,
            min_ns: self.min_ns,
            max_ns: self.max_ns,
            mean_ns: if self.count == 0 { 0.0 } else { self.mean_ns },
            median_ns,
            variance_ns2: sample_variance(self.count, self.m2_ns2),
        }
    }
}

fn snapshot_window(events: &VecDeque<StatsEvent>) -> Snapshot {
    let mut core = CoreStats::new(SampleMode::RunningOnly);
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

fn median_ns(samples: &[i128]) -> Option<f64> {
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

fn system_time_ns(time: SystemTime) -> Option<i128> {
    if let Ok(duration) = time.duration_since(UNIX_EPOCH) {
        return Some(duration_ns_i128(duration));
    }
    UNIX_EPOCH
        .duration_since(time)
        .ok()
        .map(|duration| -duration_ns_i128(duration))
}

fn duration_ns_i128(duration: Duration) -> i128 {
    i128::try_from(duration.as_nanos()).unwrap_or(i128::MAX)
}

fn signed_duration_ns(duration: SignedDuration) -> i128 {
    duration.ns
}

fn abs_i128_ns(value: i128) -> i128 {
    value.saturating_abs()
}

fn duration_from_non_negative_i128_ns(value: i128) -> Option<Duration> {
    u64::try_from(value).ok().map(Duration::from_nanos)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

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
    fn running_duration_stats_use_sample_variance() {
        let mut metric = TimeMetric::new(false);
        metric.push_ns(1);
        metric.push_ns(2);
        metric.push_ns(3);
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
        assert_eq!(median_ns(&[3, 1, 2]), Some(2.0));
        assert_eq!(median_ns(&[4, 1, 2, 3]), Some(2.5));
        assert_eq!(median_ns(&[-5, 1, 3]), Some(1.0));
        assert_eq!(median_ns(&[-5, 1, 3, 7]), Some(2.0));
    }

    #[test]
    fn single_sample_stddev_is_zero() {
        let mut metric = TimeMetric::new(false);
        metric.push_ns(42);
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

    #[test]
    fn ipdv_tracker_completes_adjacent_pair() {
        let mut tracker = IpdvTracker::new(None);
        assert!(tracker.insert(ipdv_sample(0, 10)).is_empty());

        let pairs = tracker.insert(ipdv_sample(1, 14));

        assert_eq!(
            pairs,
            vec![CompletedIpdvPair {
                previous_seq: 0,
                current_seq: 1,
                rtt_ipdv_ns: 4,
                send_ipdv_ns: None,
                receive_ipdv_ns: None,
            }]
        );
    }

    #[test]
    fn ipdv_tracker_gap_fill_completes_both_adjacent_pairs() {
        let mut tracker = IpdvTracker::new(None);
        assert!(tracker.insert(ipdv_sample(0, 10)).is_empty());
        assert!(tracker.insert(ipdv_sample(2, 20)).is_empty());

        let pairs = tracker.insert(ipdv_sample(1, 13));

        assert_eq!(
            pairs,
            vec![
                CompletedIpdvPair {
                    previous_seq: 0,
                    current_seq: 1,
                    rtt_ipdv_ns: 3,
                    send_ipdv_ns: None,
                    receive_ipdv_ns: None,
                },
                CompletedIpdvPair {
                    previous_seq: 1,
                    current_seq: 2,
                    rtt_ipdv_ns: 7,
                    send_ipdv_ns: None,
                    receive_ipdv_ns: None,
                },
            ]
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
    fn ipdv_tracker_bounded_mode_evicts_old_sequence_state() {
        let mut tracker = IpdvTracker::new(Some(2));
        assert!(tracker.insert(ipdv_sample(0, 10)).is_empty());
        assert_eq!(tracker.insert(ipdv_sample(1, 14)).len(), 1);

        let pairs = tracker.insert(ipdv_sample(2, 20));

        assert_eq!(pairs.len(), 1);
        assert!(!tracker.samples.contains_key(&0));
        assert!(!tracker.completed_pairs.contains(&0));
        assert!(!tracker.completed_pairs.contains(&1));
        assert!(tracker.completed_pairs.contains(&2));
    }
}
