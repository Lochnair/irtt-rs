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
//! instead of hanging, because the example shortens `open_timeouts` for a
//! fast demonstration.

use std::time::{Duration, Instant};

use irtt_client::{AsyncClient, ClientConfig, ClientEvent};

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
        server_addr: "127.0.0.1:2112".to_owned(),
        duration: Some(Duration::from_secs(2)),
        interval: Duration::from_millis(200),
        // A single short attempt keeps this example fast when no server is
        // reachable; production callers should generally keep the default.
        open_timeouts: vec![Duration::from_millis(300)],
        ..Default::default()
    };

    let mut client = match AsyncClient::connect(config).await {
        Ok(client) => client,
        Err(err) => {
            eprintln!("failed to prepare client socket: {err}");
            return;
        }
    };

    match client.open().await {
        Ok(outcome) => println!("session opened: {outcome:?}"),
        Err(err) => {
            eprintln!("open failed (is a server running at 127.0.0.1:2112?): {err}");
            return;
        }
    }

    while !client.is_run_complete() {
        // Bound the receive wait by the next send/timeout deadline, rather
        // than awaiting recv() unconditionally: a lost or rate-limited reply
        // must not stall pacing or timeout classification indefinitely.
        let wake_at = client
            .next_send_deadline()
            .into_iter()
            .chain(std::iter::once(Instant::now() + TIMEOUT_POLL_INTERVAL))
            .min()
            .expect("the fixed poll-interval deadline is always present");

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

        if client
            .next_send_deadline()
            .is_some_and(|send_at| Instant::now() >= send_at)
        {
            // A failed send retains its prepared probe without advancing the
            // schedule, so a persistent error (e.g. the interface going
            // down) would otherwise make this loop retry forever instead of
            // reporting it.
            match client.send_probe().await {
                Ok(events) => events.iter().for_each(print_event),
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
