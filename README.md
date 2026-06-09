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
- human, simple, machine-readable, and RTT-only stream output modes
- optional local summary statistics

Server support is not implemented.
The installed binary is intentionally not named `irtt` to avoid conflicting or
confusing overlap with upstream IRTT.

## Install

Requires Rust 1.75 or newer.

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
cargo run -p irtt-cli -- <server>
```

For space-sensitive packaging, such as OpenWrt packages, distributors can ship
only `irtt-rs` and symlink or hardlink applet names such as `irtt-cli`,
`irtt-tui`, and future server applet names to it. Applet dispatch is based on
the invoked binary name.

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

Use a specific output mode:

```sh
irtt-cli <server> --output human
irtt-cli <server> --output simple
irtt-cli <server> --output machine
irtt-cli <server> --output rtt-us
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
irtt-tui --help
```

## Machine output

Use `--output machine` for line-oriented `key=value` output intended for scripts, monitoring, and autorate consumers.

Example:

```sh
irtt-cli <server> --duration 0 --interval 250ms --output machine
```

Each line represents one client event and starts with an `event` field, for example:

```text
event=echo_reply seq=4 remote=203.0.113.10:2112 client_send_wall_ns=1760000000000000000 client_receive_wall_ns=1760000000012400000 raw_rtt_us=12400 effective_rtt_us=12100 adjusted_rtt_us=12100 server_receive_wall_ns=1760000000006100000 server_receive_mono_ns=5000006100000 server_send_wall_ns=1760000000006400000 server_send_mono_ns=5000006400000 server_processing_us=300 client_to_server_us=6100 server_to_client_us=6000 server_received_count=5 server_received_window=0x1f dscp=0 ecn=0 kernel_rx_ns=none
```

Consumers should match on `event=...` and read the fields they need. Unknown fields should be ignored.
RTT fields are reported in microseconds. `raw_rtt_us` is the measured client
send-to-receive RTT. `effective_rtt_us` and `adjusted_rtt_us` are signed and may
be negative when server processing exceeds the raw RTT or timing correction
produces a negative adjusted value. When one-way delay fields are present,
`client_to_server_us` and `server_to_client_us` are also signed microseconds and
may be negative due to wall-clock skew.

For consumers that only need RTT values, use:

```sh
irtt-cli <server> --output rtt-us
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
