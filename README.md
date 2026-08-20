# irtt-rs

`irtt-rs` is a Rust implementation of an IRTT-compatible client and server.

It provides command-line applets and reusable Rust libraries for finite or continuous latency probing, plus the server side those probes can run against. It is intended for interactive diagnostics, scripting, monitoring, and integration into applications such as network autorate controllers.

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
* Reusable Tokio-native server library
* UDP server applet with explicit bind and session policy options
* Router-friendly multi-applet binary layout

The installed dispatcher is named `irtt-rs` rather than `irtt` to avoid conflicting with or being mistaken for upstream IRTT.

## Installation

The build requires Rust 1.88 or newer.

From a local checkout:

```sh
git clone https://github.com/Lochnair/irtt-rs.git
cd irtt-rs
cargo install --path crates/irtt-app
```

This installs:

* `irtt-rs`, the canonical multi-applet dispatcher
* `irtt-client`, the stream and text client
* `irtt-tui`, the terminal UI
* `irtt-server`, the UDP server

Each applet can also be built in isolation with `--no-default-features` plus
the applet's own feature. For example, a lightweight client-only install:

```sh
cargo install --path crates/irtt-app --no-default-features --features client
```

## Quick start

Probe a server using the default settings:

```sh
irtt-client netperf-eu.bufferbloat.net:2112
```

Set the test duration and probe interval:

```sh
irtt-client netperf-eu.bufferbloat.net:2112 \
    --duration 30s \
    --interval 100ms
```

Run continuously until interrupted:

```sh
irtt-client netperf-eu.bufferbloat.net:2112 --duration 0
```

Use `Ctrl-C` to stop gracefully.

### Continuous-mode memory

`--duration 0` uses continuous statistics. Its retained application state does
not grow without bound with elapsed run time or the number of probes: timing
metrics keep running count/min/max/mean/variance values, not a complete
timing-sample history. Consequently, exact medians are unavailable. The
default continuous statistics configuration also enables no rolling
event/history window; it keeps bounded adjacent-sequence IPDV state (4,096
sequences) so nearby replies can still form IPDV pairs.

Reply classification is bounded per target session. By default, each of the
pending, timed-out, and completed/duplicate sequence stores has its own
4,096-entry limit; this is not one shared 4,096-probe total. A full pending
store makes the managed CLI/TUI fail and drain that target as resource
exhausted; it does not wait for capacity to free. Timed-out and completed state
evict their oldest entries, so a sufficiently old late reply can still be seen
and counted but no longer has retained send state for measurements such as RTT
or applicable one-way delay.

The retained state scales with the number of configured targets and these
fixed per-target limits, rather than total probes. CLI output is written as
events arrive rather than accumulated. Its managed presentation stream is
bounded and lossy (the applets configure 16,384 events), so a slow consumer can
miss events and the CLI/TUI reports that resulting output/statistics may be
incomplete. The TUI additionally keeps up to 100,000 graph samples per target
and 80 recent messages. These are retained-application-state bounds, not a
claim of a fixed process RSS ceiling: allocator behavior, output consumers,
and operating-system resources remain outside them.

For finite runs, the tradeoff is different: exact timing samples (and
adjacent-sequence IPDV state) are retained to calculate exact medians, so
statistics memory grows with probe count.

## Multiple targets

Probe several targets concurrently:

```sh
irtt-client host-a:2112 host-b:2112
```

Every target argument accepts `[LABEL=]TARGET`. An optional `LABEL=` prefix assigns the logical
target name used in output and the TUI; without it, the target string itself is used. Assign
stable labels to targets:

```sh
irtt-client ams=ams.example.com:2112 sg=sg.example.com:2112
```

Labeled and unlabeled targets can be freely mixed in one argument list:

```sh
irtt-client host-a:2112 ams=ams.example.com:2112
```

Multi-target sessions use staggered pacing by default. To send each target's probes together:

```sh
irtt-client host-a:2112 host-b:2112 --pacing burst
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
irtt-client <server> --format table
irtt-client <server> --format jsonl
irtt-client <server> --format csv \
    --columns event,seq,remote,effective_rtt_us
```

For a stream containing only effective RTT values in microseconds:

```sh
irtt-client <server> \
    --format tsv \
    --columns effective_rtt_us \
    --header never
```

List all available columns with:

```sh
irtt-client --list-columns
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

Multiple targets and pacing options work the same way as in `irtt-client`:

```sh
irtt-tui host-a:2112 host-b:2112 --pacing burst
```

Quit with `q` or `Ctrl-C`.

## Server

The server applet requires at least one explicit bind address:

```sh
irtt-server --bind 127.0.0.1:2112
```

The same applet is reachable through the dispatcher:

```sh
irtt-rs server --bind 192.0.2.10:2112
```

Repeat `--bind` to serve several addresses from one process, in the order given:

```sh
irtt-server \
    --bind 0.0.0.0:2112 \
    --bind [::]:2112
```

Every invocation runs through the same multi-listener path, so one address is an
ordinary set of one rather than a separate mode. The policy options below are
applied to every listener, but the listeners are otherwise independent: each has
its own sessions and tokens, so a session belongs to the address it was opened
on and `--max-sessions` bounds each listener rather than the process. Binding is
all or nothing — if any address cannot be bound, none are served — and a port of
`0` selects an unused port per listener. If a listener fails while running, the
others are shut down with it rather than leaving a service configured for two
families answering on one.

An explicit interface address works on every supported system. A wildcard bind
such as `--bind 0.0.0.0:2112` reads each request's local destination from packet
metadata and sends that request's reply from the same address, so a client on a
multi-homed host is answered from the endpoint it contacted. That is implemented
on Linux, macOS and FreeBSD; elsewhere a wildcard bind is refused rather than
served from a routing-table source address a client would discard. Wildcard
IPv4 and IPv6 listeners may share one port, as above.

Each bound endpoint is printed on startup once every listener is up, which also
resolves a port of `0`. `Ctrl-C` stops the server gracefully.

Session policy is set with options that map directly onto the server library's
configuration; anything left unset keeps the library default:

```sh
irtt-server \
    --bind 192.0.2.10:2112 \
    --max-sessions 512 \
    --idle-timeout 30s
```

Two optional controls restrict what the server will provide a session, and both
are off unless asked for:

```sh
irtt-server \
    --bind 192.0.2.10:2112 \
    --timestamp-allowance single \
    --no-dscp
```

`--timestamp-allowance` takes `dual` (the default, honoring every requested
placement), `single`, which provides at most one timestamp instant and answers a
request for both receive and send timestamps with the midpoint, or `none`, which
provides no timestamps. The requested clock source is never changed, so a single
instant is still reported once per requested clock. `--no-dscp` negotiates any
requested DSCP to zero, so clients are told their echo replies will be unmarked,
and they are sent unmarked.

To require authentication, pass the shared key both sides use:

```sh
irtt-server --bind 192.0.2.10:2112 --hmac secret
```

For the full option list:

```sh
irtt-server --help
```

## Library usage

`irtt-client` exposes the client session and event layer independently of CLI formatting and statistics.

Add it from a local checkout:

```toml
[dependencies]
irtt-client = { path = "crates/irtt-client", features = ["tokio"] }
```

Minimal managed-client example for a synchronous frontend:

```rust
use std::time::Duration;

use irtt_client::managed::{
    BlockingManagedClient, ManagedClientConfig, ManagedCompletionPolicy,
    ManagedTargetConfig, TargetId,
};
use irtt_client::ClientConfig;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ManagedClientConfig {
        client: ClientConfig {
            duration: Some(Duration::from_secs(10)),
            interval: Duration::from_secs(1),
            ..ClientConfig::default()
        },
        completion: ManagedCompletionPolicy::FinishWhenQuiescent,
        ..ManagedClientConfig::default()
    };
    let targets = vec![ManagedTargetConfig::new(
        TargetId::from("edge"),
        "netperf-eu.bufferbloat.net:2112",
    )];
    let owner = BlockingManagedClient::start(config, targets)?;
    let outcome = owner.join()?;
    println!("session ended: {:?}", outcome.end_reason);

    Ok(())
}
```

For presentation events, use `BlockingManagedClient::start_with_subscription`,
`ManagedEvent`, and `ManagedEventTryRecvError`, while consulting the durable
`handle.status()` and final outcome for authoritative state. Presentation
events are bounded and lossy, not reliable history. Dynamic desired-target
updates are available through `ManagedClientHandle::update_targets`.

`BlockingManagedClient` owns a dedicated Tokio runtime and thread for
synchronous callers. Callers that already own Tokio can drive
`ManagedClientTask` directly.

## Binaries and features

| Build                                       | Binaries                                            |
| -------------------------------------------- | --------------------------------------------------- |
| `--no-default-features`                     | `irtt-rs`                                           |
| `--no-default-features --features client`   | `irtt-rs`, `irtt-client`                            |
| `--no-default-features --features server`   | `irtt-rs`, `irtt-server`                            |
| `--no-default-features --features tui`      | `irtt-rs`, `irtt-tui`                               |
| Default features (or `--all-features`)      | `irtt-rs`, `irtt-client`, `irtt-tui`, `irtt-server`  |

`irtt-client` requires the `client` feature, `irtt-server` the `server` feature,
and `irtt-tui` the `tui` feature. The `irtt-rs` dispatcher is always built, and
reports which applets the build actually contains.

For available command-line options:

```sh
irtt-client --help
irtt-server --help
irtt-tui --help
```

## Development

Common verification commands:

```sh
cargo fmt --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo build -p irtt-rs --all-features --release
```

## Project status

The client, event stream, machine-readable output, multi-target execution, local statistics, and optional TUI are implemented.

The server library and the server applet are implemented: open negotiation,
session state, echo processing, per-session rate limiting, idle expiry, the
maximum-duration close, HMAC authentication, the negotiated per-session reply
traffic class, which is applied on sockets that support it, server fill,
wildcard reply-source selection on Linux, macOS and FreeBSD, the optional
timestamp and DSCP restriction controls, and serving several listeners from one
process with independent sessions per listener.

Server replies fill their payload with the negotiated ServerFill mode: `none`,
which is zero-filled, `rand`, or `pattern:` with a repeating hexadecimal
pattern. Every valid descriptor is honored; a descriptor the server cannot
parse, and a client that expresses no fill preference at all, get the default
`pattern:69727474`.
