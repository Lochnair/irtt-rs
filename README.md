# irtt-rs

`irtt-rs` is a Rust implementation of an IRTT-compatible client.

It provides both a command-line client and a reusable Rust library for finite or continuous latency probing. It is intended for interactive diagnostics, scripting, monitoring, and integration into applications such as network autorate controllers.

This is not the upstream IRTT project. For the original implementation, protocol background, and broader documentation, see [heistp/irtt](https://github.com/heistp/irtt).

## Features

* Finite and continuous probe sessions
* Concurrent probing of multiple targets
* Reusable client library with a structured event stream
* Human-readable table output
* CSV, TSV, and JSON Lines output for scripts and monitoring systems
* Selectable output columns
* Automatic final summaries for eligible table-output runs
* Optional terminal UI
* Router-friendly multi-applet binary layout

Server support is not implemented.

The installed dispatcher is named `irtt-rs` rather than `irtt` to avoid conflicting with or being mistaken for upstream IRTT.

## Installation

The build requires Rust 1.88 or newer.

From a local checkout:

```sh
git clone https://github.com/Lochnair/irtt-rs.git
cd irtt-rs
cargo install --path crates/irtt-cli
```

This installs:

* `irtt-rs`, the canonical multi-applet dispatcher
* `irtt-cli`, the stream and text client

To also install the optional terminal UI:

```sh
cargo install --path crates/irtt-cli --features tui
```

## Quick start

Probe a server using the default settings:

```sh
irtt-cli netperf-eu.bufferbloat.net:2112
```

Set the test duration and probe interval:

```sh
irtt-cli netperf-eu.bufferbloat.net:2112 \
    --duration 30s \
    --interval 100ms
```

Run continuously until interrupted:

```sh
irtt-cli netperf-eu.bufferbloat.net:2112 --duration 0
```

Use `Ctrl-C` to stop gracefully.

## Multiple targets

Probe several targets concurrently:

```sh
irtt-cli host-a:2112 host-b:2112
```

Assign stable labels to targets:

```sh
irtt-cli \
    --target ams=ams.example.com:2112 \
    --target sg=sg.example.com:2112
```

Multi-target sessions use staggered pacing by default. To send each target's probes together:

```sh
irtt-cli host-a:2112 host-b:2112 --pacing burst
```

Target labels are included in the default multi-target output.

## Output formats

The CLI supports four event-row formats:

* `table`: human-readable terminal output
* `csv`: comma-separated output
* `tsv`: tab-separated output
* `jsonl`: one JSON object per event

Examples:

```sh
irtt-cli <server> --format table
irtt-cli <server> --format jsonl
irtt-cli <server> --format csv \
    --columns event,seq,remote,effective_rtt_us
```

For a stream containing only effective RTT values in microseconds:

```sh
irtt-cli <server> \
    --format tsv \
    --columns effective_rtt_us \
    --header never
```

List all available columns with:

```sh
irtt-cli --list-columns
```

Useful measurement fields include:

* `raw_rtt_us`: client-observed send-to-receive RTT
* `adjusted_rtt_us`: RTT adjusted for server processing when available
* `effective_rtt_us`: adjusted RTT when available, otherwise raw RTT
* `sd_us` and `rd_us`: send and receive one-way delay estimates
* `ipdv_us`: inter-packet delay variation
* `server_processing_us`: time spent processing the packet at the server

Adjusted RTT can be negative when server processing exceeds the measured raw RTT. One-way delay estimates can be negative because of clock skew between the client and server.

Default table output prints a final summary after completed finite runs and interrupted continuous runs when the run policy permits it. CSV, TSV, and JSON Lines output do not print this summary.

A peer closing a finite CLI or TUI run is accepted as a terminal outcome. In
continuous mode, peer closure exits nonzero unless the user requested
interruption, allowing a supervisor to restart the client.

## Terminal UI

When built with the `tui` feature, `irtt-tui` provides a live cumulative dashboard:

```sh
irtt-tui <server>
```

It runs continuously by default. A finite duration can be selected explicitly:

```sh
irtt-tui <server> --duration 30s
```

Multiple targets and pacing options work the same way as in `irtt-cli`:

```sh
irtt-tui host-a:2112 host-b:2112 --pacing burst
```

Quit with `q` or `Ctrl-C`.

## Library usage

`irtt-client` exposes the client session and event layer independently of CLI formatting and statistics.

Add it from a local checkout:

```toml
[dependencies]
irtt-client = { path = "crates/irtt-client" }
```

Minimal managed-client example:

```rust
use std::time::Duration;

use irtt_client::{
    ClientConfig, ClientEvent, ManagedClient, SubscriberConfig,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig {
        server_addr: "netperf-eu.bufferbloat.net:2112".to_owned(),
        duration: Some(Duration::from_secs(10)),
        interval: Duration::from_secs(1),
        ..ClientConfig::default()
    };

    let (session, events) =
        ManagedClient::start_with_subscription(
            config,
            SubscriberConfig::default(),
        )?;

    while let Ok(event) = events.recv() {
        match event {
            ClientEvent::EchoReply { seq, rtt, .. } => {
                println!(
                    "seq={seq} rtt_us={}",
                    rtt.effective.as_micros()
                );
            }
            ClientEvent::SessionClosed { .. } => break,
            _ => {}
        }
    }

    let outcome = session.join()?;
    println!("session ended: {:?}", outcome.end_reason);

    Ok(())
}
```

The client library emits structured events for session lifecycle, sent probes, successful replies, loss, duplicates, late replies, warnings, and shutdown.

For shared-socket multi-target integrations, `ManagedClientGroup` publishes
`ManagedGroupEvent::TargetFinished` exactly once for every terminal target
incarnation, including open/runtime failures, removal, cancellation, no-test
completion, and peer closure. Consumers can use `ManagedTargetEndReason` and
`ManagedTargetFailureKind` for outcome and exit decisions without parsing
diagnostic text.

`ManagedGroupCompletionPolicy::AllTargetsComplete` supports finite static
groups. `ExplicitCancellation` supports long-lived controllers; replacing the
desired set with an empty vector removes current targets but leaves the group
idle and ready for a later `update_targets` call. `join()` reports aggregate
outcome counts and retains only the 256 most recent target outcomes, so
long-running target churn remains bounded. The aggregate
`peer_closed_target_outcomes` count includes peer closures omitted from that
recent snapshot.

Managed event subscriptions remain bounded and nonblocking. When using a
dropping overflow policy, check `EventSubscription::dropped_events()` before
trusting a complete statistical summary.

## Binaries and features

| Build                   | Binaries                          |
| ----------------------- | --------------------------------- |
| `--no-default-features` | `irtt-rs`                         |
| Default features        | `irtt-rs`, `irtt-cli`             |
| `--features tui`        | `irtt-rs`, `irtt-cli`, `irtt-tui` |
| `--all-features`        | `irtt-rs`, `irtt-cli`, `irtt-tui` |

For available command-line options:

```sh
irtt-cli --help
irtt-tui --help
```

## Development

Common verification commands:

```sh
cargo fmt --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo build -p irtt-cli --all-features --release
```

## Project status

The client, event stream, machine-readable output, multi-target execution, local statistics, and optional TUI are implemented.

Server support is not implemented.
