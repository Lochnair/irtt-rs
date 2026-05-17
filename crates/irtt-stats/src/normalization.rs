use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use irtt_client::{
    ClientEvent, ClientTimestamp, OneWayDelaySample, ReceivedStatsSample, RttSample, ServerTiming,
    SignedDuration,
};

use crate::ipdv::IpdvSample;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum StatsEvent {
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
        bytes: usize,
    },
    Loss {
        at: Instant,
    },
    Warning {
        at: Instant,
    },
    UntrackedLate {
        at: Instant,
        bytes: usize,
    },
}

pub(crate) fn normalize_event(event: &ClientEvent) -> Option<StatsEvent> {
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
        ClientEvent::LateReply {
            received_at, bytes, ..
        } => Some(StatsEvent::UntrackedLate {
            at: received_at.mono,
            bytes: *bytes,
        }),
        ClientEvent::DuplicateReply {
            received_at, bytes, ..
        } => Some(StatsEvent::DuplicateReply {
            at: received_at.mono,
            bytes: *bytes,
        }),
        ClientEvent::EchoLoss { timeout_at, .. } => Some(StatsEvent::Loss { at: *timeout_at }),
        ClientEvent::Warning { at, .. } => Some(StatsEvent::Warning { at: at.mono }),
        ClientEvent::SessionStarted { .. }
        | ClientEvent::NoTestCompleted { .. }
        | ClientEvent::SessionClosed { .. } => None,
    }
}

impl StatsEvent {
    pub(crate) fn at(&self) -> Instant {
        match self {
            Self::Sent { at, .. }
            | Self::UniqueReply { at, .. }
            | Self::DuplicateReply { at, .. }
            | Self::Loss { at }
            | Self::Warning { at }
            | Self::UntrackedLate { at, .. } => *at,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReplySample {
    pub(crate) bytes: usize,
    pub(crate) rtt_raw_ns: i128,
    pub(crate) rtt_adjusted_ns: Option<i128>,
    pub(crate) send_delay_ns: Option<i128>,
    pub(crate) receive_delay_ns: Option<i128>,
    pub(crate) server_processing_ns: Option<i128>,
    pub(crate) received_count: Option<u32>,
    pub(crate) received_window: Option<u64>,
    pub(crate) ipdv: IpdvSample,
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
        let rtt_primary_ns = signed_duration_ns(rtt.effective);
        Self {
            bytes,
            rtt_raw_ns: duration_ns_i128(rtt.raw),
            rtt_adjusted_ns: rtt.adjusted.map(signed_duration_ns),
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

#[cfg(test)]
mod tests {
    use super::*;

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
