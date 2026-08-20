//! Managed multi-target client example (`tokio` feature).
//!
//! [`BlockingManagedClient`] runs one or more targets under a dedicated
//! current-thread Tokio runtime and exposes a synchronous handle, so a caller
//! that does not want to touch Tokio directly can still get managed
//! multi-target orchestration, retries, and a lossy event stream.
//!
//! Run a server first, then run this example:
//!
//! ```text
//! cargo run -p irtt-rs --bin irtt-server --features server
//! cargo run -p irtt-client --example managed_client --features tokio
//! ```
//!
//! This example does not wait for the managed run to finish naturally: it
//! requests a stop after a fixed short window so it exits promptly even
//! without a reachable server.

use std::time::Duration;

use irtt_client::managed::{
    BlockingManagedClient, ManagedClientConfig, ManagedCompletionPolicy, ManagedEvent,
    ManagedTargetConfig,
};
use irtt_client::ClientConfig;

fn main() {
    let config = ManagedClientConfig {
        client: ClientConfig {
            duration: None,
            interval: Duration::from_millis(200),
            open_timeouts: vec![Duration::from_millis(300)],
            ..Default::default()
        },
        completion: ManagedCompletionPolicy::ExplicitStop,
        ..Default::default()
    };
    let targets = vec![ManagedTargetConfig::new("local", "127.0.0.1:2112")];

    let (owner, mut events) = match BlockingManagedClient::start_with_subscription(config, targets)
    {
        Ok(started) => started,
        Err(err) => {
            eprintln!("failed to start managed client: {err}");
            return;
        }
    };

    let stop_after = Duration::from_secs(1);
    let deadline = std::time::Instant::now() + stop_after;
    while std::time::Instant::now() < deadline {
        match events.try_recv() {
            Ok(event) => print_event(&event),
            Err(_) => std::thread::sleep(Duration::from_millis(50)),
        }
    }

    // `stop()` requests the stop synchronously; the returned receipt is only
    // useful for awaiting durable acknowledgement, which this blocking
    // example does not need.
    std::mem::drop(owner.handle().stop());
    match owner.join() {
        Ok(outcome) => println!("managed run ended: {:?}", outcome.end_reason),
        Err(err) => eprintln!("managed worker did not join cleanly: {err}"),
    }
}

fn print_event(event: &ManagedEvent) {
    match event {
        ManagedEvent::Client { target, event } => println!("{}: {event:?}", target.id),
        other => println!("{other:?}"),
    }
}
