//! Runtime-free blocking client example.
//!
//! Opens a session against a local IRTT-compatible server, sends a few
//! probes, and prints the events observed. Uses only [`Client`], which has no
//! Tokio dependency and works in a default (`--no-default-features`) build.
//!
//! Run a server first, then run this example:
//!
//! ```text
//! cargo run -p irtt-rs --bin irtt-server --features server
//! cargo run -p irtt-client --example blocking_client
//! ```
//!
//! Without a reachable server this exits quickly with an open-timeout error
//! instead of hanging, because the example shortens `open_timeouts` for a
//! fast demonstration; a real caller would normally keep the default retry
//! schedule.

use std::time::{Duration, Instant};

use irtt_client::{Client, ClientConfig, ClientEvent};

const RUN_DURATION: Duration = Duration::from_secs(2);

fn main() {
    let config = ClientConfig {
        server_addr: "127.0.0.1:2112".to_owned(),
        duration: Some(RUN_DURATION),
        interval: Duration::from_millis(200),
        // A single short attempt keeps this example fast when no server is
        // reachable; production callers should generally keep the default.
        open_timeouts: vec![Duration::from_millis(300)],
        socket_config: irtt_client::SocketConfig {
            recv_timeout: Some(Duration::from_millis(100)),
            ..Default::default()
        },
        ..Default::default()
    };

    let mut client = match Client::connect(config) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("failed to prepare client socket: {err}");
            return;
        }
    };

    match client.open() {
        Ok(outcome) => println!("session opened: {outcome:?}"),
        Err(err) => {
            eprintln!("open failed (is a server running at 127.0.0.1:2112?): {err}");
            return;
        }
    }

    // The run itself lasts `duration`, but a lost probe isn't classified as
    // `EchoLoss` until `probe_timeout()` after it was sent; wait at least
    // that much past the run's own end so a loss near the end can still be
    // observed and printed before this example gives up and closes.
    let deadline = Instant::now() + RUN_DURATION + client.probe_timeout();
    while Instant::now() < deadline && !client.is_run_complete() {
        // send_probe is caller-paced: only call it once its own schedule
        // says a probe is due, rather than as fast as the loop spins.
        if client
            .next_send_deadline()
            .is_some_and(|send_at| Instant::now() >= send_at)
        {
            // A failed send retains its prepared probe without advancing the
            // schedule, so a persistent error (e.g. the interface going
            // down) would otherwise make this loop retry silently until the
            // safety deadline instead of reporting it.
            match client.send_probe() {
                Ok(events) => events.iter().for_each(print_event),
                Err(err) => {
                    eprintln!("send failed: {err}");
                    break;
                }
            }
        }
        match client.recv_once() {
            Ok(events) => events.iter().for_each(print_event),
            Err(err) => {
                eprintln!("receive failed: {err}");
                break;
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

    match client.close() {
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
