//! Statistics aggregation for `irtt-client` events.

#![forbid(unsafe_code)]

use std::{
    collections::VecDeque,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use irtt_client::{
    ClientEvent, ClientTimestamp, OneWayDelaySample, ReceivedStatsSample, RttSample, ServerTiming,
    SignedDuration,
};

mod ipdv;
mod time_stats;

use ipdv::{IpdvSample, IpdvTracker};
use time_stats::TimeMetric;
pub use time_stats::TimeStats;

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

#[derive(Debug, Clone, PartialEq)]
struct RollingEvents {
    count_limit: Option<usize>,
    time_limit: Option<Duration>,
    count_events: Option<VecDeque<StatsEvent>>,
    time_events: Option<VecDeque<StatsEvent>>,
}

impl RollingEvents {
    fn new(config: StatsConfig) -> Self {
        Self {
            count_limit: config.rolling_count,
            time_limit: config.rolling_time,
            count_events: config.rolling_count.map(|_| VecDeque::new()),
            time_events: config.rolling_time.map(|_| VecDeque::new()),
        }
    }

    fn push(&mut self, event: StatsEvent) {
        if let (Some(limit), Some(window)) = (self.count_limit, self.count_events.as_mut()) {
            window.push_back(event.clone());
            while window.len() > limit {
                window.pop_front();
            }
        }

        if let (Some(duration), Some(window)) = (self.time_limit, self.time_events.as_mut()) {
            let cutoff = event.at().checked_sub(duration);
            window.push_back(event);
            if let Some(cutoff) = cutoff {
                while window.front().is_some_and(|event| event.at() < cutoff) {
                    window.pop_front();
                }
            }
        }
    }

    fn count_snapshot(&self) -> Option<Snapshot> {
        self.count_events.as_ref().map(snapshot_window)
    }

    fn time_snapshot(&self) -> Option<Snapshot> {
        self.time_events.as_ref().map(snapshot_window)
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
        match event {
            StatsEvent::Sent {
                bytes,
                send_call_ns,
                timer_error_ns,
                ..
            } => {
                self.apply_sent(bytes, send_call_ns, timer_error_ns);
                EventStatsUpdate::default()
            }
            StatsEvent::UniqueReply {
                is_late, sample, ..
            } => self.apply_unique_reply(is_late, *sample),
            StatsEvent::DuplicateReply { .. } => {
                self.apply_duplicate_reply();
                EventStatsUpdate::default()
            }
            StatsEvent::Loss { .. } => {
                self.apply_loss();
                EventStatsUpdate::default()
            }
            StatsEvent::Warning { .. } => {
                self.apply_warning();
                EventStatsUpdate::default()
            }
            StatsEvent::UntrackedLate { .. } => {
                self.apply_untracked_late();
                EventStatsUpdate::default()
            }
        }
    }

    fn apply_sent(&mut self, bytes: usize, send_call_ns: i128, timer_error_ns: i128) {
        self.events.sent_events += 1;
        self.packets.packets_sent += 1;
        self.packets.bytes_sent = self.packets.bytes_sent.saturating_add(bytes as u64);
        self.send_call.push_ns(send_call_ns);
        self.timer_error.push_ns(timer_error_ns);
    }

    fn apply_unique_reply(&mut self, is_late: bool, sample: ReplySample) -> EventStatsUpdate {
        let mut update = EventStatsUpdate {
            contributed_sample: true,
            ..EventStatsUpdate::default()
        };

        self.account_unique_reply(is_late, &sample);
        self.record_reply_metrics(&sample);
        update.ipdv_pairs = self.apply_ipdv_sample(sample.ipdv);

        update
    }

    fn account_unique_reply(&mut self, is_late: bool, sample: &ReplySample) {
        self.events.echo_replies += u64::from(!is_late);
        self.events.late_unique_replies += u64::from(is_late);
        self.packets.packets_received += 1;
        self.packets.unique_replies += 1;
        self.packets.late_packets += u64::from(is_late);
        self.packets.bytes_received = self
            .packets
            .bytes_received
            .saturating_add(sample.bytes as u64);

        if let Some(count) = sample.received_count {
            self.packets.server_packets_received = Some(u64::from(count));
        }
        if let Some(window) = sample.received_window {
            self.packets.server_received_window = Some(window);
        }
    }

    fn record_reply_metrics(&mut self, sample: &ReplySample) {
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
    }

    fn apply_ipdv_sample(&mut self, sample: IpdvSample) -> Vec<IpdvPairUpdate> {
        let mut updates = Vec::new();

        for pair in self.ipdv_tracker.insert(sample) {
            let Some(rtt_ipdv) = duration_from_non_negative_i128_ns(pair.rtt_ipdv_ns) else {
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

            updates.push(IpdvPairUpdate {
                previous_seq: pair.previous_seq,
                current_seq: pair.current_seq,
                rtt_ipdv,
                send_ipdv,
                receive_ipdv,
            });
        }

        updates
    }

    fn apply_duplicate_reply(&mut self) {
        self.events.duplicate_replies += 1;
        self.packets.packets_received += 1;
        self.packets.duplicates += 1;
    }

    fn apply_loss(&mut self) {
        self.events.loss_events += 1;
    }

    fn apply_warning(&mut self) {
        self.events.warning_events += 1;
    }

    fn apply_untracked_late(&mut self) {
        self.events.untracked_late_replies += 1;
    }

    fn snapshot(&self) -> Snapshot {
        let packets = self.packets;
        Snapshot {
            events: self.events,
            packets,
            loss: loss_stats(packets),
            send_call: self.send_call.stats(),
            timer_error: self.timer_error.stats(),
            rtt: self.rtt_stats(),
            ipdv: self.ipdv_stats(),
            one_way_delay: self.one_way_delay_stats(),
            server_processing: self.server_processing_stats(),
        }
    }

    fn rtt_stats(&self) -> RttStats {
        RttStats {
            primary: self.rtt_primary.stats(),
            raw: self.rtt_raw.stats(),
            adjusted: self.rtt_adjusted.stats(),
        }
    }

    fn ipdv_stats(&self) -> IpdvStats {
        IpdvStats {
            round_trip: self.ipdv_round_trip.stats(),
            send: self.ipdv_send.stats(),
            receive: self.ipdv_receive.stats(),
        }
    }

    fn one_way_delay_stats(&self) -> OneWayDelayStats {
        OneWayDelayStats {
            send_delay: self.send_delay.stats(),
            receive_delay: self.receive_delay.stats(),
        }
    }

    fn server_processing_stats(&self) -> ServerProcessingStats {
        ServerProcessingStats {
            processing: self.server_processing.stats(),
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

fn snapshot_window(events: &VecDeque<StatsEvent>) -> Snapshot {
    let mut core = CoreStats::new(SampleMode::RunningOnly);
    for event in events {
        core.apply(event.clone());
    }
    core.snapshot()
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

fn duration_from_non_negative_i128_ns(value: i128) -> Option<Duration> {
    u64::try_from(value).ok().map(Duration::from_nanos)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

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
