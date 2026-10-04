# irtt-stats

Statistics aggregation over [`irtt-client`](https://crates.io/crates/irtt-client)
events: loss/duplicate/late accounting, RTT and one-way-delay timing, and
inter-packet delay variation (IPDV), as cumulative and rolling snapshots.

## Relationship to irtt-client

This crate does not open sessions or drive sockets. Feed it the `ClientEvent`
stream produced by an `irtt-client` `Client`, `AsyncClient`, or managed
session via `StatsCollector::process`, and read back a `Snapshot`.

Generic timing accumulation and count-window storage come from
`measurement-stats`. `TimeStats` remains available from `irtt_stats` as a
re-export. Event interpretation, IPDV pairing, loss, and snapshot reconstruction
remain here.

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

An enabled time-based rolling window has no event-count cap, including in
continuous mode. It expires only on new events, anchored at the maximum observed
normalized timestamp, with events exactly at the cutoff retained. Retained events
are replayed in arrival order; backdated events cannot move the window backwards.
Reading a snapshot does not advance time. Use count-based rolling storage for a
hard bound on retained events. The memory estimate excludes time-window storage.

Rolling directional loss uses the increase in the highest observed server
receive count within one observation segment across the retained packet events
in arrival order. Upstream loss is window sends minus that increase; downstream
loss is the increase minus all window replies, including duplicates and untracked
late replies, as in cumulative accounting. The baseline must be supplied by the
packet event immediately before
the first retained packet event, or is zero if no earlier packet events existed.
It must also equal the highest count observed in that segment. Intervening sends
or replies without a current count make an older baseline stale: its next increase
may cover packets outside the window. Each retained event keeps the count's observation
ordinal, so eviction cannot erase this distinction. Reordered lower counts do not
refresh the baseline; only matched unique replies supply counter observations,
regardless of the late-reply timing policy.

Both directional packet estimates are `None` if the baseline or endpoint is
unknown or stale, or if time filtering excludes a packet event between retained
packet events. Their percentage fields use the existing unavailable value of `0.0`.
Loss/warning events do not affect this interval. Signed estimates remain possible
when server counts exceed local expectations or extra replies arrive, and
outstanding sends retain the existing provisional-loss interpretation. These are
changes in observed accounting, not exact network transit loss for probes sent
in a wall-clock period.
Raw `server_packets_received`, ordinary packet counts, and cumulative loss retain
their existing meanings.

A counter jump of at least half the 32-bit range is ambiguous between counter
wrap and a very old reordered reply from the other side of a wrap. It starts a
new observation segment; windows crossing the discontinuity have no directional
estimate. Later windows can recover using a fresh baseline within that segment.
The collector does not infer an epoch from incomplete arrival history. Smaller
regressions retain the existing maximum-count interpretation for reordered replies.

Exact snapshots sort a temporary copy of each metric's retained samples, adding
O(n) scratch memory and O(n log n) computation for that metric. The retention
estimate is not peak snapshot or allocator accounting.

See `examples/` in the repository for a runnable comparison of both modes.

## Documentation

Full API documentation is on [docs.rs/irtt-stats](https://docs.rs/irtt-stats).

## Project

Part of [irtt-rs](https://github.com/Lochnair/irtt-rs), an independent Rust
implementation of an IRTT-compatible protocol stack.
