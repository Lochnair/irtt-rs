use std::time::{Duration, Instant, UNIX_EPOCH};

use irtt_client::{
    ClientEvent, ClientTimestamp, OneWayDelaySample, PacketMeta, ReceivedStatsSample, RttSample,
    ServerTiming, SignedDuration,
};

pub fn ts(ms: u64) -> ClientTimestamp {
    ClientTimestamp {
        mono: Instant::now() + Duration::from_millis(ms),
        wall: UNIX_EPOCH + Duration::from_millis(ms),
    }
}

pub fn adjusted_rtt(raw_ms: u64, adjusted_ms: i128) -> RttSample {
    let adjusted = SignedDuration::from_nanos(adjusted_ms * 1_000_000);
    RttSample {
        raw: Duration::from_millis(raw_ms),
        adjusted: Some(adjusted),
        effective: adjusted,
    }
}

pub fn unadjusted_rtt(raw_ms: u64) -> RttSample {
    let raw = Duration::from_millis(raw_ms);
    RttSample {
        raw,
        adjusted: None,
        effective: SignedDuration::from_duration(raw),
    }
}

pub fn sent(seq: u32, sent_at: ClientTimestamp) -> ClientEvent {
    ClientEvent::EchoSent {
        seq,
        remote: "127.0.0.1:2112".parse().unwrap(),
        scheduled_at: sent_at.mono,
        sent_at,
        bytes: 32,
        send_call: Duration::from_micros(10),
        timer_error: Duration::from_micros(2),
    }
}

pub fn adjusted_reply(seq: u32, raw_ms: u64, adjusted_ms: i128) -> ClientEvent {
    reply_with_rtt(seq, raw_ms, adjusted_rtt(raw_ms, adjusted_ms))
}

pub fn unadjusted_reply(seq: u32, raw_ms: u64) -> ClientEvent {
    reply_with_rtt(seq, raw_ms, unadjusted_rtt(raw_ms))
}

fn reply_with_rtt(seq: u32, raw_ms: u64, rtt: RttSample) -> ClientEvent {
    let sent_at = ts(seq as u64 * 10);
    let received_at = ClientTimestamp {
        mono: sent_at.mono + Duration::from_millis(raw_ms),
        wall: sent_at.wall + Duration::from_millis(raw_ms),
    };
    ClientEvent::EchoReply {
        seq,
        remote: "127.0.0.1:2112".parse().unwrap(),
        sent_at,
        received_at,
        rtt,
        server_timing: Some(ServerTiming {
            receive_wall_ns: Some(unix_time_ns_after_epoch(sent_at.wall) as i64 + 1_000_000),
            receive_mono_ns: Some(seq as i64 * 10_000_000 + 1_000_000),
            send_wall_ns: Some(unix_time_ns_after_epoch(sent_at.wall) as i64 + 2_000_000),
            send_mono_ns: Some(seq as i64 * 10_000_000 + 2_000_000),
            midpoint_wall_ns: None,
            midpoint_mono_ns: None,
            processing: Some(Duration::from_millis(1)),
        }),
        one_way: Some(OneWayDelaySample {
            client_to_server: Some(SignedDuration::from_nanos(1_000_000)),
            server_to_client: Some(SignedDuration::from_nanos(2_000_000)),
        }),
        received_stats: Some(ReceivedStatsSample {
            count: Some(seq + 1),
            window: Some(0xff),
        }),
        bytes: 64,
        packet_meta: PacketMeta::default(),
    }
}

/// Builds a normal reply carrying exactly the supplied server received stats.
pub fn reply_with_received_stats(
    seq: u32,
    raw_ms: u64,
    count: Option<u32>,
    window: Option<u64>,
) -> ClientEvent {
    let mut event = unadjusted_reply(seq, raw_ms);
    let ClientEvent::EchoReply { received_stats, .. } = &mut event else {
        unreachable!();
    };
    *received_stats =
        (count.is_some() || window.is_some()).then_some(ReceivedStatsSample { count, window });
    event
}

/// Builds a send event with the supplied send-call duration and timer error.
pub fn sent_with_timings(
    seq: u32,
    sent_at: ClientTimestamp,
    send_call_us: u64,
    timer_error_us: u64,
) -> ClientEvent {
    let mut event = sent(seq, sent_at);
    let ClientEvent::EchoSent {
        send_call,
        timer_error,
        ..
    } = &mut event
    else {
        unreachable!();
    };
    *send_call = Duration::from_micros(send_call_us);
    *timer_error = Duration::from_micros(timer_error_us);
    event
}

/// Builds a normal reply reporting exactly the supplied server processing time.
pub fn reply_with_processing(seq: u32, raw_ms: u64, processing_ms: u64) -> ClientEvent {
    let mut event = unadjusted_reply(seq, raw_ms);
    let ClientEvent::EchoReply { server_timing, .. } = &mut event else {
        unreachable!();
    };
    if let Some(timing) = server_timing.as_mut() {
        timing.processing = Some(Duration::from_millis(processing_ms));
    }
    event
}

/// Builds a normal reply from a session that supplied no server timestamps, so
/// no server processing measurement exists.
pub fn reply_without_server_timing(seq: u32, raw_ms: u64) -> ClientEvent {
    let mut event = unadjusted_reply(seq, raw_ms);
    let ClientEvent::EchoReply { server_timing, .. } = &mut event else {
        unreachable!();
    };
    *server_timing = None;
    event
}

pub fn unadjusted_late_reply(seq: u32, raw_ms: u64) -> ClientEvent {
    late_reply_from(unadjusted_reply(seq, raw_ms))
}

fn late_reply_from(reply: ClientEvent) -> ClientEvent {
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
    } = reply
    else {
        unreachable!();
    };
    ClientEvent::LateReply {
        seq,
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

fn unix_time_ns_after_epoch(time: std::time::SystemTime) -> i128 {
    time.duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as i128)
        .unwrap()
}
