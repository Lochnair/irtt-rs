# irtt-stats

Statistics aggregation over [`irtt-client`](https://crates.io/crates/irtt-client)
events: loss/duplicate/late accounting, RTT and one-way-delay timing, and
inter-packet delay variation (IPDV), as cumulative and rolling snapshots.

## Relationship to irtt-client

This crate does not open sessions or drive sockets. Feed it the `ClientEvent`
stream produced by an `irtt-client` `Client`, `AsyncClient`, or managed
session via `StatsCollector::process`, and read back a `Snapshot`.

A `StatsCollector` has one sequence/IPDV namespace and one packet count, both
scoped to a single target. A managed session covering multiple targets needs
one collector per target — as the `irtt-rs` CLI does — since a target's
sequence numbers start over at zero and feeding two targets into one
collector would pair up unrelated probes.

## Retention modes

`StatsConfig::finite()` retains exact timing samples for its cumulative
snapshot, so every metric with samples reports an exact median there —
retention grows with the probe count. `StatsConfig::continuous()` keeps
bounded running statistics instead (no exact median) plus a bounded
4096-entry adjacent-sequence IPDV store per target, for long-running or
unbounded sessions. Rolling-window snapshots are always running-only and
report no medians, whichever `StatsConfig` produced them.

See `examples/` in the repository for a runnable comparison of both modes.

## Documentation

Full API documentation is on [docs.rs/irtt-stats](https://docs.rs/irtt-stats).

## Project

Part of [irtt-rs](https://github.com/Lochnair/irtt-rs), an independent Rust
implementation of an IRTT-compatible protocol stack.
