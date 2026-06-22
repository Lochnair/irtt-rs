# irtt-rs

`irtt-rs` is a Rust implementation of an IRTT-compatible client.

It is not the upstream IRTT project. For the original implementation, protocol background, and broader project documentation, see:

<https://github.com/heistp/irtt>

## Status

This repository currently provides:

- `irtt-rs`, the canonical multi-applet binary
- `irtt-cli`, a stream/text IRTT-compatible client applet
- `irtt-tui`, an optional terminal UI applet when built with the `tui` feature
- `irtt-client`, a Rust library for running client sessions and consuming events
- finite and continuous probe runs
- table, CSV, TSV, and JSON Lines event-row output formats with selectable columns
- optional local summary statistics

Server support is not implemented.
The installed binary is intentionally not named `irtt` to avoid conflicting or
confusing overlap with upstream IRTT.

## Install

Requires Rust 1.85 or newer.

From a local checkout:

```sh
git clone https://github.com/Lochnair/irtt-rs.git
cd irtt-rs
cargo install --path crates/irtt-cli
```

The default install provides the normal client-oriented binaries, including
`irtt-rs` and `irtt-cli`. To install the TUI applet as a separate binary target:

```sh
cargo install --path crates/irtt-cli --features tui
```

Or run it directly without installing:

```sh
cargo run -p irtt-cli --bin irtt-cli -- <server>
```

For space-sensitive packaging, such as OpenWrt packages, distributors can ship
only `irtt-rs` and symlink or hardlink enabled applet names such as `irtt-cli`
and, when built with the `tui` feature, `irtt-tui` to it. Applet dispatch is
based on the invoked binary name. Server support is not implemented.

cargo-dist release archives are currently configured with the `full` feature
set, so target archives bundle the enabled applet binaries: `irtt-rs`,
`irtt-cli`, and `irtt-tui`. That is separate from space-sensitive package
layouts, which may still ship one dispatcher binary plus applet-name symlinks
or hardlinks. cargo-dist is not currently configured to create those links.

## Build verification

Useful local release/package sanity checks:

```sh
cargo fmt --check
cargo test -p irtt-cli
cargo clippy --workspace --all-targets -- -D warnings
cargo build -p irtt-cli --release
cargo build -p irtt-cli --no-default-features --release
cargo build -p irtt-cli --features tui --release
cargo build -p irtt-cli --all-features --release
```

The no-default-features build only provides the `irtt-rs` dispatcher binary;
the default build provides `irtt-rs` and `irtt-cli`; `tui`, `full`, and
all-features builds also provide `irtt-tui`.

## CLI usage

Basic test:

```sh
irtt-cli <server>
```

Set duration and interval:

```sh
irtt-cli <server> --duration 30s --interval 100ms
```

Run continuously:

```sh
irtt-cli <server> --duration 0
```

Use a specific output format or column selection:

```sh
irtt-cli <server> --format table
irtt-cli <server> --format jsonl
irtt-cli <server> --format csv --columns event,seq,remote,effective_rtt_us
irtt-cli <server> --format tsv --columns effective_rtt_us --header never
irtt-cli --list-columns
```

With the optional TUI feature, `irtt-tui` opens a live cumulative dashboard for
interactive probing. It defaults to continuous probing, equivalent to
`--duration 0`. Quit with `q` or `Ctrl-C`; the client will drain and close the
session gracefully.

```sh
irtt-tui <server>
irtt-tui <server> --duration 30s
cargo run -p irtt-cli --features tui --bin irtt-tui -- <server>
```

For available options:

```sh
irtt-cli --help
irtt-tui --help   # when built or installed with the tui feature
```

## Event row output

The stream client separates event row rendering from final summaries. Event rows
are controlled by `--format`, `--columns`, and `--header`; final summaries remain
separate table-style text and are printed for table output when the run policy
allows a summary.

Formats:

- `table`: default, terminal-readable columns with a header
- `csv`: comma-separated rows, intended for scripts
- `tsv`: tab-separated rows, useful for one-value streams and shell pipelines
- `jsonl`: one JSON object per event row, suitable for streaming consumers

Headers are controlled with `--header auto|always|never`. In `auto`, table, CSV,
and TSV print one header row; JSON Lines never prints a header.

Columns are selected with `-c, --columns <COLUMNS>`, where `COLUMNS` is a
comma-separated list. Use `--list-columns` to print the available names. Useful
columns include:

```text
event, seq, remote, token, rtt, rtt_us, raw_rtt_us, effective_rtt_us,
adjusted_rtt_us, rd, rd_us, sd, sd_us, ipdv, ipdv_us, proc,
server_processing_us, bytes, send_call_us, timer_error_us, highest_seen,
server_received, server_window, dscp, ecn, traffic_class, kernel_rx_ns,
warning_kind, message, event_wall_ns, client_send_wall_ns,
client_receive_wall_ns
```

Aliases are accepted for readability: `receive_delay` and `receive_delay_us`
for `rd` and `rd_us`, `send_delay` and `send_delay_us` for `sd` and `sd_us`,
`server_processing` for `proc`, `server_received_count` for
`server_received`, and `server_received_window` for `server_window`.

The default table output favors readability with compact columns:
`event,seq,rtt,rd,sd,ipdv,proc,message`. It omits `echo_sent` rows. Passing
`--columns default` is the same as omitting `--columns`, including the default
table row filtering. Custom table column selections include all event rows.
CSV, TSV, and JSONL default to all columns for structured export; use
`--columns all` explicitly to request every column in any format. Missing table
values render as `-`; missing CSV and TSV values render as empty fields;
missing JSONL values render as `null`.

Use JSON Lines for structured event streaming:

Example:

```sh
irtt-cli <server> --duration 0 --interval 250ms --format jsonl
```

Each line is one client event object, for example:

```json
{"event":"echo_reply","seq":4,"remote":"203.0.113.10:2112","token":null,"rtt":"12.1ms","rtt_us":12100,"raw_rtt_us":12400,"effective_rtt_us":12100,"adjusted_rtt_us":12100,"rd":"6.0ms","rd_us":6000,"sd":"6.1ms","sd_us":6100,"ipdv":"300.0µs","ipdv_us":300,"proc":"300.0µs","server_processing_us":300,"bytes":64,"send_call_us":null,"timer_error_us":null,"highest_seen":null,"server_received":5,"server_window":"0x1f","dscp":0,"ecn":0,"traffic_class":0,"kernel_rx_ns":null,"warning_kind":null,"message":null,"event_wall_ns":1760000000012400000,"client_send_wall_ns":1760000000000000000,"client_receive_wall_ns":1760000000012400000}
```

Consumers should match on the `event` property and read the columns they need.
RTT, send-call, and timer-error `*_us` fields are reported in microseconds.
`raw_rtt_us` is the measured client send-to-receive RTT. `effective_rtt_us` and
`adjusted_rtt_us` are signed and may be negative when server processing exceeds
the raw RTT or timing correction produces a negative adjusted value. When
one-way delay fields are present, `sd_us` and `rd_us` are also signed
microseconds and may be negative due to wall-clock skew.

For consumers that only need effective RTT values in microseconds, use:

```sh
irtt-cli <server> --format tsv --columns effective_rtt_us --header never
```

## Library usage

`irtt-client` can be used directly from Rust code.

Example `Cargo.toml` dependency from a local checkout:

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
        ManagedClient::start_with_subscription(config, SubscriberConfig::default())?;

    while let Ok(event) = events.recv() {
        match event {
            ClientEvent::EchoReply { seq, rtt, .. } => {
                println!("seq={seq} rtt_us={}", rtt.effective.as_micros());
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
