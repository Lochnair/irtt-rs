//! Low-level Tokio client example (`tokio` feature).
//!
//! Drives [`AsyncClient`] directly on a caller-owned runtime. This is the
//! adapter to reach for when you already have a Tokio runtime and want to
//! await sends/receives yourself, rather than use the higher-level `managed`
//! driver (see the `managed_client` example).
//!
//! Run a server first, then run this example:
//!
//! ```text
//! cargo run -p irtt-rs --bin irtt-server --features server
//! cargo run -p irtt-client --example tokio_client --features tokio
//! ```
//!
//! Without a reachable server this exits quickly with an open-timeout error
//! instead of hanging, because the example shortens `open.timeouts` for a
//! fast demonstration.

use std::time::{Duration, Instant};

use irtt_client::{
    AsyncClient, ClientConfig, ClientEvent, OpenOutcome, OpenPolicy, SessionRequest,
};

/// Upper bound on how long the receive wait can go without also calling
/// `poll_timeouts()`. `AsyncClient` does not expose the earliest pending
/// probe's own timeout deadline, so this example polls on a short fixed
/// cadence instead of waiting for the full `probe_timeout()` from whenever
/// this loop last ran — that would let poll_timeouts() classify a lost
/// probe (and its dependent statistics/session state) far later than it
/// actually became lost.
const TIMEOUT_POLL_INTERVAL: Duration = Duration::from_millis(50);

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("failed to build Tokio runtime");

    runtime.block_on(run());
}

async fn run() {
    let config = ClientConfig {
        open: OpenPolicy {
            // A single short attempt keeps this example fast when no server is
            // reachable; production callers should generally keep the default.
            timeouts: vec![Duration::from_millis(300)],
            ..Default::default()
        },
        request: SessionRequest {
            duration: Some(Duration::from_secs(2)),
            interval: Duration::from_millis(200),
            ..Default::default()
        },
        ..Default::default()
    };

    let mut client = match AsyncClient::connect("127.0.0.1:2112", config).await {
        Ok(client) => client,
        Err(err) => {
            eprintln!("failed to prepare client socket: {err}");
            return;
        }
    };

    let negotiated = match client.open().await {
        Ok(OpenOutcome::Started {
            event, negotiated, ..
        }) => {
            println!("session opened: {event:?}");
            negotiated
        }
        Ok(OpenOutcome::NoTestCompleted { event, .. }) => {
            println!("{event:?}");
            return;
        }
        Err(err) => {
            eprintln!("open failed (is a server running at 127.0.0.1:2112?): {err}");
            return;
        }
    };

    // This loop owns cadence and duration. The transport sends whenever called.
    let interval = Duration::from_nanos(negotiated.params.interval_ns as u64);
    let start = Instant::now();
    let end = start + Duration::from_nanos(negotiated.params.duration_ns as u64);
    let deadline = end + client.probe_timeout();
    let mut next_send = start;
    while Instant::now() < deadline && !client.is_peer_closed() {
        if Instant::now() >= end && !client.has_pending_probes() {
            break;
        }
        // Bound the receive wait by the next send/timeout deadline, rather
        // than awaiting recv() unconditionally: a lost or rate-limited reply
        // must not stall pacing or timeout classification indefinitely.
        let wake_at = (next_send < end)
            .then_some(next_send)
            .into_iter()
            .chain(client.next_probe_timeout_deadline())
            .chain(std::iter::once(Instant::now() + TIMEOUT_POLL_INTERVAL))
            .min()
            .unwrap();

        let recv_result = tokio::select! {
            events = client.recv() => Some(events),
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(wake_at)) => None,
        };
        if let Some(result) = recv_result {
            match result {
                Ok(events) => events.iter().for_each(print_event),
                Err(err) => {
                    eprintln!("receive failed: {err}");
                    break;
                }
            }
        }

        if Instant::now() < end && Instant::now() >= next_send {
            match client.send_probe().await {
                Ok(events) => {
                    events.iter().for_each(print_event);
                    while next_send <= Instant::now() {
                        next_send += interval;
                    }
                }
                Err(err) => {
                    eprintln!("send failed: {err}");
                    break;
                }
            }
        }
        match client.poll_timeouts() {
            Ok(events) => events.iter().for_each(print_event),
            Err(err) => {
                eprintln!("timeout polling failed: {err}");
                break;
            }
        }
    }

    if client.is_peer_closed() {
        return;
    }
    match client.close().await {
        Ok(events) => events.iter().for_each(print_event),
        Err(err) => eprintln!("close failed: {err}"),
    }
}

fn print_event(event: &ClientEvent) {
    match event {
        ClientEvent::EchoReply { seq, rtt, .. } => {
            println!("seq {seq}: rtt {:?}", rtt.effective);
        }
        ClientEvent::EchoLoss { seq, .. } => println!("seq {seq}: lost"),
        other => println!("{other:?}"),
    }
}
