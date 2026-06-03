# Interop Comparison

`scripts/compare_irtt_clients.py` is a black-box comparison harness for running
upstream `irtt` and `irtt-rs` clients concurrently against the same server. It
captures raw stdout/stderr, upstream JSON, parsed RTT samples, per-case
summaries, and comparison JSON under `target/interop-comparisons` by default.

The harness does not inspect upstream source or tests. It only invokes the
upstream executable.

## Remote or Existing Local Server

Build `irtt-rs` and ensure an upstream `irtt` executable is available:

```sh
cargo build -p irtt-cli
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

## Linux Netem Interop

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

### Scenarios

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

### Pass/Fail Policy

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

### Manual Linux Usage

On Linux:

```sh
cargo build -p irtt-cli
go install github.com/heistp/irtt/cmd/irtt@v0.9.1
python3 scripts/run_netem_interop_ci.py \
  --all \
  --skip-build \
  --upstream-irtt "$HOME/go/bin/irtt" \
  --irtt-rs-command "$PWD/target/debug/irtt-cli"
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

On macOS, `ip netns` and `tc netem` are unavailable; use the GitHub Actions
workflow for netem coverage.
