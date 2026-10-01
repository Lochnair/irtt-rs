# irtt-client

Reusable client/session library for IRTT-compatible round-trip-time probing:
socket lifecycle, open negotiation, probe send/receive, loss/duplicate/late
classification, and a typed event stream.

## Looking for the CLI?

If you just want to run probes from the command line, use the
[`irtt-rs`](https://crates.io/crates/irtt-rs) application package instead,
which installs the `irtt-client` binary. This crate is the library the
binaries are built on.

## API tiers

- `Client` — runtime-free blocking adapter. No Tokio dependency; this is the
  default build.
- `AsyncClient` (feature `tokio`) — low-level Tokio adapter for callers that
  own a runtime and drive readiness directly.
- `managed` (feature `tokio`) — a unified managed driver: `ManagedClientTask`
  / `ManagedClientHandle` for multi-target orchestration under a Tokio
  runtime, and `BlockingManagedClient` for synchronous callers, which owns its
  own dedicated current-thread runtime.

`Client` and `AsyncClient` send one probe whenever called. They expose the
negotiated parameters, pending-probe status, and timeout deadlines; callers
own cadence and run duration. The managed driver provides fixed cadence,
missed-slot handling, finite runs, and staggered or burst pacing. Low-level
`EchoSent` events leave `scheduled_at` and `timer_error` absent; the managed
driver supplies those schedule measurements.

Tokio stays optional: the default build has no runtime and no Tokio
dependency at all.

See `examples/` in the repository for runnable examples of each tier.

## Documentation

Full API documentation is on [docs.rs/irtt-client](https://docs.rs/irtt-client).

## Project

Part of [irtt-rs](https://github.com/Lochnair/irtt-rs), an independent Rust
implementation of an IRTT-compatible protocol stack.
