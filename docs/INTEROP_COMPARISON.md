# Interop comparison

## Feature comparison

This compares `irtt-rs` with the original [heistp/irtt](https://github.com/heistp/irtt)
release [v0.9.2](https://github.com/heistp/irtt/releases/tag/v0.9.2) (2026-07-17).
The aim is for the two implementations to interoperate, though the interop
tests and verified protocol baseline still use upstream v0.9.1. Both
upstream releases use wire protocol version 1, but v0.9.2 hasn't been run
through this project's interop harness. See [`docs/protocol/`](protocol/)
for the verified protocol baseline.

| Capability | irtt-rs | Upstream IRTT (v0.9.2) |
| --- | --- | --- |
| Wire protocol | Version 1, verified against upstream v0.9.1 | Version 1, unchanged from v0.9.1 |
| Finite probing | Default duration of 10s | Default duration of 1m |
| Continuous probing | `--continuous` or `--duration 0`, with optional per-target reconnection | Streaming mode (`-s`, added in v0.9.2) |
| Statistics in continuous mode | Optional count/min/max/mean/variance per session, with bounded IPDV tracking and no exact medians. Disable with `--no-stats` or omit the `stats` feature | No statistics; raw/event output only to avoid unbounded memory |
| Multiple targets | Labels and staggered/burst pacing in one invocation | One remote address per invocation |
| Terminal UI | Live graph and dashboard through `irtt-tui` | None |
| Output formats | table, csv, tsv, jsonl, with selectable columns | Text summary, JSON (optionally gzip to file), and fixed-field raw output; no CSV/TSV or column selection |
| Replay saved results | Not implemented | `report` subcommand for saved JSON results |
| Benchmarking and clock diagnostics | Not implemented | Benchmarking and timer/clock-diagnostic subcommands |
| Client payload and timing controls | No client-side payload fill or don't-fragment/wait-strategy/timer-algorithm controls | Client-side payload fill and controls for don't-fragment, wait strategy, and timer algorithm |
| Kernel timestamps | Best-effort preference on Linux for its own send/receive endpoint | Not implemented |
| Server listeners | Several listeners in one process, supervised by `ServerSet` | Multiple requested addresses through `ListenAndServe` |
| Server wildcard reply-source selection | Default on Linux/macOS/FreeBSD; other platforms refuse wildcard binds | Opt-in through `--set-src-ip`, with a documented per-packet allocation cost |
| Server default bind | Wildcard addresses for both families, subject to the platform restriction above | Wildcard addresses for both families; no documented platform restriction |
| Server payload fill policy | Honors the session's negotiated request, with a fixed fallback pattern; no options to restrict or override it | Options to restrict or override client fill requests |
| Syslog logging | Not implemented | Supported |
| Bundled service files | None | systemd and OpenRC |
| Client/server library API | Separate, independently versioned Rust crates (`irtt-client`, `irtt-server`) | One Go package exposing both roles |
| Prebuilt release binaries | Published for six target triples | None published; install through `go install` or build manually |

## Black-box testing

`scripts/compare_irtt_clients.py` is a black-box comparison harness for running
upstream `irtt` and `irtt-rs` clients concurrently against the same server. It
captures raw stdout/stderr, upstream JSON, parsed RTT samples, per-case
summaries, and comparison JSON under `target/interop-comparisons` by default.

The harness does not inspect upstream source or tests. It only invokes the
upstream executable.

### Remote or Existing Local Server

Build `irtt-rs` and ensure an upstream `irtt` executable is available:

```sh
cargo build -p irtt-rs --bin irtt-client
go install github.com/heistp/irtt/cmd/irtt@v0.9.1
```

Run the default comparison set:

```sh
python3 scripts/compare_irtt_clients.py --upstream-irtt "$HOME/go/bin/irtt"
```

To compare against a specific target:

```sh
python3 scripts/compare_irtt_clients.py \
  --target netperf-eu.bufferbloat.net \
  --duration 15s \
  --interval 250ms \
  --upstream-irtt "$HOME/go/bin/irtt"
```

Remote comparisons are useful for smoke coverage, but Internet paths are noisy
and can make RTT/loss differences hard to interpret.

### Linux Netem Interop

`scripts/run_netem_interop_ci.py` provides deterministic Linux CI coverage using
network namespaces, a veth pair, and `tc netem`. It is intended for GitHub
Actions Ubuntu runners or a Linux host with passwordless `sudo` for `ip` and
`tc`.

The harness creates two namespaces:

- `irtt-client` with `veth-client` at `10.10.0.1/24`
- `irtt-server` with `veth-server` at `10.10.0.2/24`

It runs the upstream server in the server namespace:

```sh
irtt server -b 10.10.0.2:2112 --tstamp=dual
```

Both clients run in the client namespace through the existing comparison
harness:

```sh
sudo ip netns exec irtt-client ...
```

Netem qdiscs are applied on veth egress:

- `veth-client` controls client-to-server delay/loss
- `veth-server` controls server-to-client delay/loss

Cleanup deletes qdiscs, stops the server process, and removes both namespaces in
a `finally` path.

#### Scenarios

The CI harness implements:

| scenario | client-to-server | server-to-client | duration | interval | policy |
| --- | --- | --- | --- | --- | --- |
| `baseline-no-netem` | none | none | `5s` | `100ms` | deterministic |
| `symmetric-20ms` | `delay 20ms` | `delay 20ms` | `5s` | `100ms` | deterministic |
| `symmetric-50ms-jitter` | `delay 50ms 5ms` | `delay 50ms 5ms` | `5s` | `100ms` | jitter |
| `asymmetric-delay` | `delay 40ms` | `delay 10ms` | `5s` | `100ms` | deterministic |
| `packet-loss` | `loss 5%` | `loss 5%` | `10s` | `50ms` | stochastic loss |

Asymmetric one-way delay fields include client/server wall-clock offset. Since
both clients run in the same client namespace against the same server namespace,
relative one-way shape is still useful for diagnostics, but the CI pass/fail
policy is based on RTT and packet behavior.

#### Pass/Fail Policy

Deterministic scenarios fail when:

- either client exits non-zero
- parsed mean RTT or received packet counts are missing
- either client receives no packets
- unexpected loss exceeds `0.5%`
- mean RTT delta exceeds `max(2000us, 5% of expected RTT)`

The jitter scenario fails on crashes, parser failures, no received packets, or
mean RTT delta above `max(8000us, 10% of expected RTT)`.

The packet-loss scenario fails on crashes, parser failures, or no received
packets. Loss-rate deltas above `10%` are warnings because packet loss is
stochastic and exact percentages are not stable enough for CI.

Thresholds can be adjusted with:

```sh
python3 scripts/run_netem_interop_ci.py \
  --mean-abs-tolerance-us 3000 \
  --mean-pct-tolerance 0.08 \
  --jitter-mean-abs-tolerance-us 10000 \
  --loss-rate-warning-pct 12
```

#### Manual Linux Usage

On Linux:

```sh
cargo build -p irtt-rs --bin irtt-client
go install github.com/heistp/irtt/cmd/irtt@v0.9.1
python3 scripts/run_netem_interop_ci.py \
  --all \
  --skip-build \
  --upstream-irtt "$HOME/go/bin/irtt" \
  --irtt-rs-command "$PWD/target/debug/irtt-client"
```

Run one scenario:

```sh
python3 scripts/run_netem_interop_ci.py --scenario symmetric-20ms
```

Artifacts are written under `target/interop-netem/<timestamp>/` by default. Each
scenario contains the existing comparison harness output under
`<scenario>/comparison/<timestamp>/`, plus netem-specific `summary.md` and
`netem-summary.json` files. Server stdout/stderr are captured at the run root as
`server.stdout` and `server.stderr`.

On macOS, `ip netns` and `tc netem` are unavailable; use the Gitea Actions
workflow for netem coverage.
