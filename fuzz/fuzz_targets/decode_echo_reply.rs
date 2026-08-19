#![no_main]

use irtt_proto::{
    decode_echo_reply, echo_packet_len, hmac::compute_hmac_in_place, Clock, PacketLayout, Params,
    ReceivedStats, StampAt, FLAG_HMAC, FLAG_REPLY, TIMESTAMP_SIZE,
};
use libfuzzer_sys::fuzz_target;

/// See `decode_request.rs` for the rationale behind this bound.
const MAX_INPUT_LEN: usize = 128 * 1024;

/// Mirrors `irtt-proto`'s crate-private HMAC field offset, the same way the
/// crate's own external integration tests do.
const HMAC_OFFSET: usize = 4;

const FUZZ_HMAC_KEY: &[u8] = b"fuzz-decode-echo-reply-key";

/// Negotiated-length vocabulary around the boundaries that matter for this
/// decoder: zero, tiny, MTU-ish, and modestly larger. Deliberately bounded so
/// we fuzz the decoder rather than benchmark OOM handling.
const LENGTH_BUCKETS: &[i64] = &[
    0, 1, 63, 64, 65, 127, 128, 129, 511, 512, 513, 1199, 1200, 1201, 1471, 1472, 1473, 1499, 1500,
    1501, 4096, 65507,
];

fn pick_received_stats(v: u8) -> ReceivedStats {
    match v % 4 {
        0 => ReceivedStats::None,
        1 => ReceivedStats::Count,
        2 => ReceivedStats::Window,
        _ => ReceivedStats::Both,
    }
}

fn pick_stamp_at(v: u8) -> StampAt {
    match v % 5 {
        0 => StampAt::None,
        1 => StampAt::Send,
        2 => StampAt::Receive,
        3 => StampAt::Both,
        _ => StampAt::Midpoint,
    }
}

fn pick_clock(v: u8) -> Clock {
    match v % 4 {
        0 => Clock::Unspecified,
        1 => Clock::Wall,
        2 => Clock::Monotonic,
        _ => Clock::Both,
    }
}

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_LEN {
        return;
    }
    // Need enough control bytes to derive Params before any packet bytes.
    const CONTROL_LEN: usize = 6;
    if data.len() < CONTROL_LEN {
        return;
    }
    let (control, rest) = data.split_at(CONTROL_LEN);

    let received_stats = pick_received_stats(control[0]);
    let stamp_at = pick_stamp_at(control[1]);
    let clock = pick_clock(control[2]);
    let length_bucket = LENGTH_BUCKETS[control[3] as usize % LENGTH_BUCKETS.len()];
    let hmac_present = control[4] & 1 == 1;
    let use_compat_len = control[5] & 1 == 1;

    let params = Params {
        protocol_version: 1,
        length: length_bucket,
        received_stats,
        stamp_at,
        clock,
        ..Params::default()
    };

    let Ok(normal_len) = echo_packet_len(hmac_present, &params) else {
        return;
    };

    let layout = PacketLayout::echo(hmac_present, &params);
    let is_single_clock_midpoint =
        stamp_at == StampAt::Midpoint && matches!(clock, Clock::Wall | Clock::Monotonic);
    let target_len = if use_compat_len && is_single_clock_midpoint {
        (layout.header_len() + TIMESTAMP_SIZE).max(normal_len)
    } else {
        normal_len
    };

    let flags = FLAG_REPLY | if hmac_present { FLAG_HMAC } else { 0 };
    let mut packet = Vec::with_capacity(target_len);
    packet.extend_from_slice(&irtt_proto::MAGIC);
    packet.push(flags);
    if hmac_present {
        packet.extend_from_slice(&[0u8; 16]);
    }
    // Fill the remainder of the negotiated length from the fuzz-provided
    // tail, truncating or zero-padding to hit the target length exactly so
    // the decoder's field parsing gets real coverage rather than bailing out
    // on a length mismatch every time.
    let body_len = target_len.saturating_sub(packet.len());
    if rest.len() >= body_len {
        packet.extend_from_slice(&rest[..body_len]);
    } else {
        packet.extend_from_slice(rest);
        packet.resize(target_len, 0);
    }
    packet.resize(target_len, 0);

    if hmac_present {
        if compute_hmac_in_place(FUZZ_HMAC_KEY, &mut packet, HMAC_OFFSET).is_err() {
            return;
        }
        let reply = decode_echo_reply(&packet, &params, Some(FUZZ_HMAC_KEY));
        if let Ok(reply) = reply {
            assert!(reply.payload.len() <= packet.len());
        }
    } else {
        let reply = decode_echo_reply(&packet, &params, None);
        if let Ok(reply) = reply {
            assert!(reply.payload.len() <= packet.len());
        }
    }

    // Also exercise the raw fuzz bytes directly against the same negotiated
    // Params, unmodified and un-lengthened, purely for the no-panic
    // invariant on genuinely arbitrary bytes.
    let _ = decode_echo_reply(data, &params, None);
});
