use std::time::Duration;

use crate::ipdv::{IpdvSample, IpdvTracker};
use crate::loss::loss_stats;
use crate::normalization::{ReplySample, StatsEvent};
use crate::time_stats::TimeMetric;
use crate::{
    EventCounts, EventStatsUpdate, IpdvPairUpdate, IpdvStats, OneWayDelayStats, PacketCounts,
    RttStats, SampleMode, ServerProcessingStats, Snapshot,
};

const CONTINUOUS_SEQUENCE_LIMIT: usize = 4096;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CoreStats {
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
    pub(crate) fn new(sample_mode: SampleMode) -> Self {
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

    pub(crate) fn apply(&mut self, event: StatsEvent) -> EventStatsUpdate {
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
            StatsEvent::DuplicateReply { bytes, .. } => {
                self.apply_duplicate_reply(bytes);
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
            StatsEvent::UntrackedLate { bytes, .. } => {
                self.apply_untracked_late(bytes);
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
            let count = u64::from(count);
            self.packets.server_packets_received = Some(
                self.packets
                    .server_packets_received
                    .map_or(count, |current| current.max(count)),
            );
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

    fn apply_duplicate_reply(&mut self, bytes: usize) {
        self.events.duplicate_replies += 1;
        self.packets.packets_received += 1;
        self.packets.duplicates += 1;
        self.packets.bytes_received = self.packets.bytes_received.saturating_add(bytes as u64);
    }

    fn apply_loss(&mut self) {
        self.events.loss_events += 1;
    }

    fn apply_warning(&mut self) {
        self.events.warning_events += 1;
    }

    fn apply_untracked_late(&mut self, bytes: usize) {
        self.events.untracked_late_replies += 1;
        self.packets.packets_received += 1;
        self.packets.late_packets += 1;
        self.packets.bytes_received = self.packets.bytes_received.saturating_add(bytes as u64);
    }

    pub(crate) fn snapshot(&self) -> Snapshot {
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

fn duration_from_non_negative_i128_ns(value: i128) -> Option<Duration> {
    u64::try_from(value).ok().map(Duration::from_nanos)
}
