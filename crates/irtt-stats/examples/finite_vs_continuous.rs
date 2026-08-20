//! Contrasts finite and continuous retention modes.
//!
//! Feeds the same synthetic stream of `irtt-client` events into two
//! collectors — one configured with [`StatsConfig::finite`], one with
//! [`StatsConfig::continuous`] — and prints how their cumulative snapshots
//! differ. This uses synthetic events rather than a live session so the
//! example runs deterministically with no network or server involved.
//!
//! Run with:
//!
//! ```text
//! cargo run -p irtt-stats --example finite_vs_continuous
//! ```

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant, SystemTime};

use irtt_client::{ClientEvent, ClientTimestamp, PacketMeta, RttSample, SignedDuration};
use irtt_stats::{StatsCollector, StatsConfig};

fn main() {
    let mut finite = StatsCollector::new(StatsConfig::finite());
    let mut continuous = StatsCollector::new(StatsConfig::continuous());

    for event in synthetic_probe_stream(10) {
        finite.process(&event);
        continuous.process(&event);
    }

    let finite_snapshot = finite.snapshot();
    let continuous_snapshot = continuous.snapshot();

    println!(
        "finite:     replies={}, rtt median_ns={:?}",
        finite_snapshot.events.echo_replies, finite_snapshot.rtt.primary.median_ns
    );
    println!(
        "continuous: replies={}, rtt median_ns={:?}",
        continuous_snapshot.events.echo_replies, continuous_snapshot.rtt.primary.median_ns
    );
    println!(
        "continuous mode keeps no exact samples, so its median is always None, \
         while finite mode's median is available once it has samples."
    );
}

/// Builds a short synthetic stream of `EchoSent`/`EchoReply` pairs, plus one
/// loss, standing in for what a real `irtt-client` session would emit.
fn synthetic_probe_stream(count: u32) -> Vec<ClientEvent> {
    let remote = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 2112);
    let mut events = Vec::new();

    for seq in 0..count {
        let sent_at = ClientTimestamp {
            wall: SystemTime::now(),
            mono: Instant::now(),
        };
        events.push(ClientEvent::EchoSent {
            seq,
            remote,
            scheduled_at: sent_at.mono,
            sent_at,
            bytes: 64,
            send_call: Duration::from_micros(50),
            timer_error: Duration::from_micros(20),
        });

        if seq == count - 1 {
            // The last probe times out instead of receiving a reply.
            events.push(ClientEvent::EchoLoss {
                seq,
                sent_at,
                timeout_at: sent_at.mono + Duration::from_secs(1),
            });
            continue;
        }

        let rtt = Duration::from_millis(10 + u64::from(seq));
        let received_at = ClientTimestamp {
            wall: sent_at.wall + rtt,
            mono: sent_at.mono + rtt,
        };
        events.push(ClientEvent::EchoReply {
            seq,
            remote,
            sent_at,
            received_at,
            rtt: RttSample {
                raw: rtt,
                adjusted: None,
                effective: SignedDuration::from_duration(rtt),
            },
            server_timing: None,
            one_way: None,
            received_stats: None,
            bytes: 64,
            packet_meta: PacketMeta::default(),
        });
    }

    events
}
