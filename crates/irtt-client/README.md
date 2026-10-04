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

## Reusable configuration

`ClientConfig` contains no endpoint. The low-level APIs take it separately:
`Client::connect(endpoint, config)` and
`AsyncClient::connect(endpoint, config).await`, accepting `String` or `&str`
endpoints. An omitted port defaults to 2112.

Configuration is grouped by ownership:

- `address_family`: `AddressFamily::{Any, Ipv4, Ipv6}`, defaulting to `Any`.
  It filters remote resolution; `Ipv6` also sets `IPV6_V6ONLY`.
- `socket`: `SocketConfig` for local bind, device/routing options, and TTL.
- `request`: `SessionRequest` for duration, interval, length, received stats,
  timestamps, clock, DSCP, server fill, and run mode.
- `open`: `OpenPolicy` for attempt `timeouts` and `negotiation` policy.
- `auth`, `probe_timeout`, and `max_pending_probes`: authentication and local
  probe tracking policy.

Clone one config to connect to multiple endpoints. Managed targets retain their
own ID, endpoint, and authentication inheritance/override; shared
`ManagedClientConfig.client` is reusable without endpoint mutation.

Blocking receive policy belongs to `Client`:
`client.set_recv_timeout(Some(duration))?` sets it before or after opening,
and `client.set_recv_timeout(None)?` restores the default indefinite wait.
Open attempts use only `open.timeouts`, restoring the adapter setting afterward.
Async and managed clients have no blocking receive-timeout setting.

This breaks the former flat config, endpoint-in-config, socket family flags,
and `SocketConfig.recv_timeout` APIs. Use the grouped fields and adapter method
directly; defaults and protocol/runtime behavior are preserved.

## Authentication

`ClientConfig.auth` is concrete: `Authentication::Unauthenticated` or
`Authentication::Hmac(HmacKey::new(bytes))`. Keys share their bytes across
clones and print only `HmacKey([REDACTED])` through `Debug`. Borrowing key
material requires an explicit `as_bytes()` call. An empty key still selects
HMAC authentication; textual key syntax belongs to the application.

For managed targets, `ManagedTargetConfig.auth` defaults to `TargetAuth::Inherit`.
`TargetAuth::Override(Authentication::Unauthenticated)` disables shared
authentication, and `TargetAuth::Override(Authentication::Hmac(key))` replaces
the shared key. Initial construction and update planning resolve inheritance
into concrete session authentication before execution. Switching between
inheritance and an override replaces the target generation even when their
resolved authentication is equal.

This replaces the former `ClientConfig.hmac_key` and `ClientAuthConfig` API;
there are no compatibility aliases.

## Documentation

Full API documentation is on [docs.rs/irtt-client](https://docs.rs/irtt-client).

## Project

Part of [irtt-rs](https://github.com/Lochnair/irtt-rs), an independent Rust
implementation of an IRTT-compatible protocol stack.
