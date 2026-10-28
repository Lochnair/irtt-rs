# irtt-rs

`irtt-rs` is an IRTT-compatible client and server written in Rust. You can use
it to measure UDP latency for a fixed duration or leave it running
continuously, and run your own server for the probes to talk to.

The original IRTT implementation is [heistp/irtt](https://github.com/heistp/irtt),
which also has protocol background and more documentation. This project is
independent, so the installed dispatcher is named `irtt-rs` to keep it from
conflicting with or being mistaken for `irtt`.

## Features

- Finite and continuous probe sessions
- Concurrent probing of multiple targets, with staggered or burst pacing
- Reusable client library with a structured event stream (blocking, Tokio,
  or managed)
- Human-readable table output, plus CSV, TSV, and JSON Lines for scripts and
  monitoring systems, with selectable output columns
- Optional terminal UI
- Reusable Tokio-native server library
- UDP server applet you can run without arguments, with options for bind
  addresses and session policy when you need them
- A single binary that can run several applets, useful on routers

## Comparison with upstream IRTT

`irtt-rs` is useful if you want to watch several targets at once, compare
them in a terminal dashboard, or feed selected measurements into a script.
You can leave it running with statistics and automatic reconnection, use
its Rust libraries in your own application, or deploy a single binary on
a router.

Upstream IRTT is much more mature, and if you don't need the features
specific to `irtt-rs`, you should probably use it. It also has more tools
for working with saved results and controlling how probes run, including
replaying saved JSON reports, timer and clock diagnostics, and client
payload fill and timing algorithms.

See the [detailed comparison](docs/INTEROP_COMPARISON.md) for individual
features, version coverage, and black-box interoperability testing.

## Installation

Prebuilt binaries for supported platforms are available on the
[GitHub Releases](https://github.com/Lochnair/irtt-rs/releases) page.

Alternatively, install from crates.io using Rust 1.88 or newer:

```sh
cargo install irtt-rs --locked
```

This installs the `irtt-rs` dispatcher along with dedicated `irtt-client`,
`irtt-tui`, and `irtt-server` binaries. If you only need the client, you can
disable the other applets and statistics:

```sh
cargo install irtt-rs --locked --no-default-features --features client
```

This installs `irtt-client` and the `irtt-rs` dispatcher. Add `stats`
(`--features client,stats`) to retain statistics in a client-only build.
Statistics are enabled by default; the TUI always enables them.

For development, or to install directly from a checkout:

```sh
git clone https://github.com/Lochnair/irtt-rs.git
cd irtt-rs
cargo install --path crates/irtt-app --locked
```

## Quick start

Probe a server:

```sh
irtt-client netperf-eu.bufferbloat.net:2112 --duration 30s --interval 100ms

# Negotiate parameters without sending measurement probes
irtt-client --no-test netperf-eu.bufferbloat.net:2112
```

Probe continuously until interrupted (`Ctrl-C`):

```sh
irtt-client netperf-eu.bufferbloat.net:2112 --continuous

# Recover failed, closed, or silent sessions independently per target
irtt-client netperf-eu.bufferbloat.net:2112 --continuous --reconnect \
  --reconnect-timeout 10s --reconnect-initial-delay 250ms --reconnect-max-delay 10s
```

Reconnection resets statistics for that target; summaries cover its latest
session. Add `--no-stats` for measurement events without statistics collection.

Watch the same session live in the terminal UI:

```sh
irtt-tui netperf-eu.bufferbloat.net:2112
```

To run your own server, you can leave out `--bind` and use the defaults:

```sh
irtt-server
```

### Dynamic targets from stdin

If another application decides which servers to probe, `--targets-stdin`
lets it change targets without restarting the client. It works in
continuous mode:

```sh
irtt-client --continuous --targets-stdin --reconnect --no-stats \
  --format csv --columns event,target,seq,rd_us,sd_us
```

Send one comma-separated target set per line to stdin, for example:

```text
ams=ams.example:2112,lhr=lhr.example:2112
ams=ams.example:2112
[]
```

Each line gives the complete desired target set. Unchanged targets keep
running, removed targets stop, and new or changed targets start. Use `[]`
to clear all targets without exiting.

The CSV includes an `event` column so your script can distinguish
`echo_reply` measurements from other events.

Keep stdin open while controlling the client. Closing it sends EOF and stops the run gracefully.

See [Dynamic target sets from standard input](docs/irtt-client.md#dynamic-target-sets-from-standard-input)
for the full syntax and behavior.

## Documentation

- [`docs/irtt-client.md`](docs/irtt-client.md): target syntax, output
  formats and columns, measurement field definitions, finite/continuous
  memory behavior, kernel timestamps.
- [`docs/irtt-tui.md`](docs/irtt-tui.md): controls, continuous default,
  retained history.
- [`docs/irtt-server.md`](docs/irtt-server.md): bind defaults and
  overrides, wildcard listener behavior, session policy, HMAC.
- [`docs/irtt-rs.md`](docs/irtt-rs.md): the multicall dispatcher, applet
  binary names, feature-gated builds.

You can also run any applet with `--help` for its full option list.

## Crates

| Crate | crates.io | What it is |
| --- | --- | --- |
| [`irtt-rs`](crates/irtt-app) | [crates.io](https://crates.io/crates/irtt-rs) | Application package: the `irtt-rs`, `irtt-client`, `irtt-tui`, and `irtt-server` binaries |
| [`irtt-client`](crates/irtt-client) | [crates.io](https://crates.io/crates/irtt-client) | Reusable client/session library ([docs.rs](https://docs.rs/irtt-client)) |
| [`irtt-server`](crates/irtt-server) | [crates.io](https://crates.io/crates/irtt-server) | Reusable Tokio-native server library ([docs.rs](https://docs.rs/irtt-server)) |
| [`irtt-stats`](crates/irtt-stats) | [crates.io](https://crates.io/crates/irtt-stats) | Statistics aggregation over `irtt-client` events ([docs.rs](https://docs.rs/irtt-stats)) |
| [`irtt-proto`](crates/irtt-proto) | [crates.io](https://crates.io/crates/irtt-proto) | Low-level wire protocol encode/decode ([docs.rs](https://docs.rs/irtt-proto)) |

## Binaries and features

| Build | Binaries |
| --- | --- |
| `--no-default-features` | `irtt-rs` |
| `--no-default-features --features client` | `irtt-rs`, `irtt-client` |
| `--no-default-features --features client,stats` | `irtt-rs`, `irtt-client` (with statistics) |
| `--no-default-features --features server` | `irtt-rs`, `irtt-server` |
| `--no-default-features --features tui` | `irtt-rs`, `irtt-tui` |
| Default features (or `--all-features`) | `irtt-rs`, `irtt-client`, `irtt-tui`, `irtt-server` |

## Development

```sh
cargo fmt --all -- --check
cargo test --workspace --locked
cargo test --workspace --doc --all-features --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
```
