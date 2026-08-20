# irtt-rs

`irtt-rs` is a Rust implementation of an IRTT-compatible client and server:
finite or continuous UDP latency probing, plus the server side those probes
run against.

This is not the upstream IRTT project. For the original implementation,
protocol background, and broader documentation, see
[heistp/irtt](https://github.com/heistp/irtt). The installed dispatcher is
named `irtt-rs` rather than `irtt` to avoid conflicting with or being
mistaken for it.

## Features

* Finite and continuous probe sessions
* Concurrent probing of multiple targets, with staggered or burst pacing
* Reusable client library with a structured event stream (blocking, Tokio,
  or managed)
* Human-readable table output, plus CSV, TSV, and JSON Lines for scripts and
  monitoring systems, with selectable output columns
* Optional terminal UI
* Reusable Tokio-native server library
* UDP server applet with sensible zero-argument defaults and explicit bind
  and session-policy options
* Router-friendly multi-applet binary layout

## Comparison with upstream IRTT

`irtt-rs` is an independent, compatible reimplementation, not a successor or
replacement for upstream IRTT — [heistp/irtt](https://github.com/heistp/irtt)
remains the original implementation. Interoperability is a project goal; see
[`docs/INTEROP_COMPARISON.md`](docs/INTEROP_COMPARISON.md) and
[`docs/protocol/`](docs/protocol/) for the black-box interoperability
methodology and protocol baseline this project verifies against. Choose
between them based on the capabilities and deployment model you need.

The table below was checked against upstream release
[v0.9.2](https://github.com/heistp/irtt/releases/tag/v0.9.2) (2026-07-17).
This project's own interop testing and protocol baseline are currently
pinned to the prior release, v0.9.1; the wire protocol version number is
unchanged between the two, but v0.9.2 has not itself been exercised against
this project's interop harness.

| Capability | irtt-rs | upstream IRTT (v0.9.2) |
| --- | --- | --- |
| Wire protocol | Protocol version 1, verified against upstream v0.9.1 | Protocol version 1 (unchanged in v0.9.2) |
| Finite probing | Supported, default 10s | Supported, default 1m |
| Continuous/indefinite operation | Supported (`--duration 0`); still computes running statistics (see below) | Supported via streaming mode (`-s`, added in v0.9.2); computes no statistics at all in that mode |
| Statistics in continuous mode | Running count/min/max/mean/variance per target; exact medians unavailable; bounded IPDV tracking | None in streaming mode — raw/event output only, by design, to avoid unbounded memory |
| Multi-target probing (one invocation) | Supported, with labels and staggered/burst pacing | Not supported — one remote address per invocation |
| Terminal UI | `irtt-tui`, a live graph/dashboard applet | Not present |
| Output formats | table, csv, tsv, jsonl, with selectable columns | Text summary, JSON (optionally gzip to file), and a fixed-field raw mode; no CSV/TSV and no column selection |
| Client/server library API | Separate, independently versioned Rust crates (`irtt-client`, `irtt-server`) | One combined Go package exposing both roles |
| Server: multiple listeners | `ServerSet` supervises several listeners in one process | `ListenAndServe` also serves multiple requested addresses |
| Server: wildcard reply-source | Default behavior on Linux/macOS/FreeBSD; refused elsewhere rather than served incorrectly | Opt-in via `--set-src-ip`, with a documented per-packet allocation cost |
| Server: zero-argument bind | Binds the wildcard address on both families by default, gated by the platform check above | Binds the wildcard address on both families by default; no documented platform gate |
| Prebuilt release binaries | Published for six target triples | None published; install via `go install` or manual build |

Upstream has capabilities this project does not implement: a `report`
subcommand for replaying saved JSON results, benchmarking and
timer/clock-diagnostic subcommands, client-side payload fill and
don't-fragment/wait-strategy/timer-algorithm controls, an operator-configurable
server-side fill policy (this project's server always honors a session's
negotiated fill request, with a fixed fallback pattern, but has no equivalent
to upstream's flags for restricting or overriding it), syslog logging, and
bundled systemd/OpenRC service files.

This project has capabilities upstream does not implement: multi-target
probing with labels, the terminal UI, CSV/TSV output with selectable
columns, a quantified and actively-warned memory-retention model for
long-running client sessions, and best-effort Linux kernel-timestamp
preference for its own send/receive endpoint.

## Installation

The build requires Rust 1.88 or newer.

Install the latest release from crates.io:

```sh
cargo install irtt-rs --locked
```

This installs the `irtt-rs` dispatcher plus dedicated `irtt-client`,
`irtt-tui`, and `irtt-server` binaries. Each applet can also be built in
isolation with `--no-default-features` plus the applet's own feature, for
example a lightweight client-only install:

```sh
cargo install irtt-rs --locked --no-default-features --features client
```

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
```

Probe continuously until interrupted (`Ctrl-C`):

```sh
irtt-client netperf-eu.bufferbloat.net:2112 --duration 0
```

Watch the same session live in the terminal UI:

```sh
irtt-tui netperf-eu.bufferbloat.net:2112
```

Run a server with sensible defaults — no `--bind` needed:

```sh
irtt-server
```

## Documentation

* [`docs/irtt-client.md`](docs/irtt-client.md) — target syntax, output
  formats and columns, measurement field definitions, finite/continuous
  memory behavior, kernel timestamps.
* [`docs/irtt-tui.md`](docs/irtt-tui.md) — controls, continuous default,
  retained history.
* [`docs/irtt-server.md`](docs/irtt-server.md) — bind defaults and
  overrides, wildcard listener behavior, session policy, HMAC.
* [`docs/irtt-rs.md`](docs/irtt-rs.md) — the multicall dispatcher, applet
  binary names, feature-gated builds.
* [docs.rs](https://docs.rs) for the library crates below.
* [`docs/protocol/`](docs/protocol/) and
  [`docs/INTEROP_COMPARISON.md`](docs/INTEROP_COMPARISON.md) for the wire
  protocol specification and interoperability validation against upstream
  `irtt`.

Every applet also documents its full option list with `--help`.

## Crates

| Crate | crates.io | What it is |
| --- | --- | --- |
| [`irtt-rs`](crates/irtt-app) | [crates.io](https://crates.io/crates/irtt-rs) | Application package: the `irtt-rs`, `irtt-client`, `irtt-tui`, and `irtt-server` binaries |
| [`irtt-client`](crates/irtt-client) | [crates.io](https://crates.io/crates/irtt-client) | Reusable client/session library ([docs.rs](https://docs.rs/irtt-client)) |
| [`irtt-server`](crates/irtt-server) | [crates.io](https://crates.io/crates/irtt-server) | Reusable Tokio-native server library ([docs.rs](https://docs.rs/irtt-server)) |
| [`irtt-stats`](crates/irtt-stats) | [crates.io](https://crates.io/crates/irtt-stats) | Statistics aggregation over `irtt-client` events ([docs.rs](https://docs.rs/irtt-stats)) |
| [`irtt-proto`](crates/irtt-proto) | [crates.io](https://crates.io/crates/irtt-proto) | Low-level wire protocol encode/decode ([docs.rs](https://docs.rs/irtt-proto)) |

Each library crate has its own crates.io README describing what it's for and
when to use it instead of `irtt-client`/`irtt-server` directly.

## Binaries and features

| Build | Binaries |
| --- | --- |
| `--no-default-features` | `irtt-rs` |
| `--no-default-features --features client` | `irtt-rs`, `irtt-client` |
| `--no-default-features --features server` | `irtt-rs`, `irtt-server` |
| `--no-default-features --features tui` | `irtt-rs`, `irtt-tui` |
| Default features (or `--all-features`) | `irtt-rs`, `irtt-client`, `irtt-tui`, `irtt-server` |

## Development

```sh
cargo fmt --all -- --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
```

See [`AGENTS.md`](AGENTS.md) for architecture, testing policy, and the
clean-room provenance boundary this project maintains against upstream
`irtt`.
