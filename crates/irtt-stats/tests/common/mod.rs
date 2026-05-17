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

pub fn rtt(raw_ms: u64, effective_ms: i128) -> RttSample {
    let effective = SignedDuration {
        ns: effective_ms * 1_000_000,
    };
    RttSample {
        raw: Duration::from_millis(raw_ms),
        adjusted: Some(effective),
        effective,
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

pub fn reply(seq: u32, raw_ms: u64, effective_ms: i128) -> ClientEvent {
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
        rtt: rtt(raw_ms, effective_ms),
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
            client_to_server: Some(Duration::from_millis(1)),
            server_to_client: Some(Duration::from_millis(2)),
        }),
        received_stats: Some(ReceivedStatsSample {
            count: Some(seq + 1),
            window: Some(0xff),
        }),
        bytes: 64,
        packet_meta: PacketMeta::default(),
    }
}

pub fn late_reply(seq: u32, raw_ms: u64, effective_ms: i128) -> ClientEvent {
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
    } = reply(seq, raw_ms, effective_ms)
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
