# Changelog

This project's crates are versioned independently. Each release below is
scoped to a single crate and tagged as `<crate>/vX.Y.Z` (see
[Releasing](#releasing) below).

All notable changes to each crate are documented in its own section per
release. The format is loosely based on
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## irtt-proto

### 0.5.0

#### Added

- New server-direction wire codecs: `decode_open_request`, `encode_open_reply`, `decode_echo_request`, `encode_echo_reply`, and `decode_close_request`, so a server implementation (now `irtt-server`) can encode/decode the same wire format the client side already used, instead of duplicating protocol logic outside this crate.
- New `ProtoError` variants `MissingField` and `UnexpectedField` for structural validation of optional fields that must be present/absent together.

#### Changed

- `encode_echo_request` (and the request-side echo codec generally) now takes the negotiated params as a separate argument rather than bundling them into `EchoRequest` — a breaking signature change for any direct caller of the proto-level echo encoder.
- `ProtoError::NegativePacketLength` was renamed to `ProtoError::PacketLengthUnrepresentable { length: i64 }`, reflecting that the check now rejects any length that doesn't fit the wire representation, not only negative ones — a breaking rename for callers matching on this variant.

#### Compatibility

- The echo-reply length codec now accepts upstream `irtt`'s longer midpoint-echo compatibility length instead of rejecting it, fixing an interop gap against real upstream servers/clients that send that length.

## irtt-client

### 0.5.0

#### Added

- New optional `tokio` Cargo feature adding `AsyncClient`, a low-level non-blocking/poll-based Tokio adapter alongside the existing runtime-free blocking `Client`.
- New managed driver stack built on top of `AsyncClient`: `ManagedClientTask` / `ManagedClientHandle` (a unified Tokio-based managed driver and control surface, supporting dynamic per-target updates to a running session group) and `BlockingManagedClient` (a synchronous owner that runs a dedicated current-thread Tokio runtime internally). This replaces the project's earlier managed-client design outright rather than extending it, including changes to the `ClientError` variants it can surface.
- Linux kernel receive-timestamp capture (`SO_TIMESTAMPING`-family ancillary data) feeding one-way-delay (OWD) calculations, and equivalent kernel transmit-timestamp capture on the send side, materially improving OWD accuracy over the previous best-effort user-space send/receive timestamps on Linux. Both require the `ancillary` feature.

#### Fixed

- DSCP wire semantics: the negotiated DSCP value was being shifted twice on the wire path (double-application of the ECN/DSCP bit layout), which produced an incorrect traffic-class byte in some configurations; the client-side DSCP handling was reworked to apply the shift exactly once and is covered by expanded DSCP/negotiation tests.
- Post-send timestamp capture correctness fix so the locally-recorded send timestamp used for OWD/RTT calculations reflects when the probe actually left the socket rather than a timestamp that could be skewed by scheduling between encode and send.
- A batch of managed-session liveness and lifecycle fixes: dynamic target groups no longer spin instead of blocking while idle; empty dynamic groups stay idle correctly; peer-initiated closes are now recorded and surfaced (including counters/provenance) instead of being silently dropped or racing with correlation; authenticated close is honored ahead of reply correlation; terminal outcomes are now published for targets that fail rather than being dropped silently; completed event hubs are sealed and completed-target retention is bounded (preventing unbounded growth in long multi-target runs); missed probe slots are skipped rather than mis-scheduled; dropped-subscription events are now exposed to consumers instead of being swallowed; and a timeout-budget bug in the managed opening phase was corrected.
- Linux receive paths (blocking `Client::open`/`recv_once`/`recv_available`, `AsyncClient`'s non-blocking recv/open polling, and the MSG_ERRQUEUE TX-timestamp drain) now retry transparently on `EINTR` instead of surfacing it as a fatal socket error, while preserving each path's existing timeout/deadline contract (no retransmit, no timeout extension under repeated interruption).
- `ClientError` failure classification was made exhaustive/more precise, giving callers a fuller and more accurate error taxonomy to match on instead of a catch-all in some paths.

#### Compatibility

- The `ancillary` (Linux socket ancillary-data) feature was hardened to only perform safe operations, closing a soundness gap in how ancillary control-message buffers were handled.

## irtt-server

### 0.5.0

#### Added

- `irtt-server` is a new first-class, reusable crate this release, built from scratch: a deterministic `ServerCore` handling OPEN/ECHO/CLOSE packet admission, authentication policy, open negotiation, a bounded session table, echo processing with per-session receive state and timestamps, per-session rate limiting, session lifetime with idle expiry, server-initiated close on maximum duration, and client-initiated close.
- A reusable Tokio UDP `Server` runs one sequential `ServerCore` per listener with caller-controlled shutdown and once-per-second scheduled idle-session maintenance (in addition to exact logical expiry on authenticated, structurally valid requests). `ServerSet` sits above it as the service-level owner of one or more independent `Server`s: it binds them all-or-none, runs each in its own Tokio task, fans one external shutdown signal out to all of them, joins them, and fails the group if any listener fails or stops early — enabling multi-listener deployments from a single `ServerConfig`.
- Each reply now carries the raw traffic class it must be sent with, applied to the listener socket immediately before every send.
- Wildcard listeners recover each request's local destination from packet ancillary metadata and send that request's reply from the same address, on Linux, macOS, and FreeBSD. `Server::from_socket`/`Server::bind` are now fallible specifically because of this: a wildcard bind on a platform without that path is refused at construction (`ServerRuntimeError::WildcardSourceSelectionUnsupported`) rather than silently served from a routing-table-chosen source address. Explicit-address listeners are unaffected on all platforms.
- Configurable echo-reply payload fill policy (`ServerFill`), controlling what bytes a session's echo replies are padded with.
- Two optional negotiation policies, both off by default and settled during open negotiation: a timestamp allowance and a DSCP permission, restricting what a session may ask the server to provide.
- Linux kernel receive-timestamp capture for inbound packets, feeding more accurate server-side timing metadata, mirroring the equivalent client-side capability added in `irtt-client`.

#### Fixed

- The DSCP/traffic-class byte applied to replies was corrected to match the client-side wire-semantics fix (see `irtt-client`), so server-echoed DSCP marking is now consistent with what clients expect.
- Executable-params validation on OPEN requests was tightened so malformed/contradictory negotiated parameters are rejected during open negotiation rather than accepted and mishandled later.
- Packet-length policy corrected so oversized/invalid negotiated lengths are rejected consistently with the updated `irtt-proto` length validation.
- A receive-drop/backpressure policy fix for malformed or unauthenticated inbound packets, preventing them from disrupting the session table or other in-flight sessions.
- A receive-timestamp "wall clock" fix on the kernel RX-capture path, and a 32-bit (i686) `timespec` conversion correctness fix covered by a dedicated regression test.
- The server's ancillary receive loop now retries transparently on `EINTR` instead of terminating the listener, matching the equivalent `irtt-client` fix.

#### Compatibility

- Resource bounds are enforced by design (bounded session table, per-listener `max_sessions`, rate limiting, idle/max-duration expiry) rather than mirroring upstream `irtt`'s effectively unbounded session/per-peer behavior; this is a deliberate divergence, not an oversight, and is documented in `crates/irtt-server/AGENTS.md`.

## irtt-stats

### 0.5.0

#### Fixed

- Exact median calculation for `send_call`, `timer_error`, and `server_processing` event-duration statistics, which previously always reported `None` for these medians instead of a computed value.

#### Added

- `LateReplyMode` (`Measure` / `CountOnly`) lets a consumer choose whether replies that arrive after their probe is considered late are still fully measured into the running statistics or only counted, without affecting the other samples.
- `StatsConfig::estimated_retained_bytes(probe_count)` gives callers an API to estimate the memory a stats configuration will retain for a given probe count, ahead of actually running a session (used by `irtt-cli`'s multi-target memory-usage warning, see below).

## irtt-cli

### 0.5.0

#### Added

- A new `server` applet (the `irtt-server` binary and `irtt-rs server` subcommand), gated behind the `server` Cargo feature which is now part of `irtt-cli`'s default feature set. It is thin orchestration over the new `irtt-server` crate: one current-thread Tokio runtime, one repeatable `--bind`, and `ServerSet` for startup/shutdown/listener-failure handling — including for a single bind.
- Positional `[LABEL=]TARGET` argument syntax for specifying client targets.

#### Changed

- Target specification moved from a `--target LABEL=TARGET` flag to plain positional `[LABEL=]TARGET` arguments — a breaking CLI syntax change for any script invoking targets via the old flag.
- The `Target` column is now always present in default, CSV, TSV, and JSONL output, rather than only appearing once more than one target was specified. This is a breaking output-format change for scripts that parsed single-target output and assumed no `Target` column.
- Cargo feature flags were simplified: the separate `stats`, `full`, and `client-runtime` features were removed, and statistics support is now mandatory whenever the `client` (or `tui`) feature is enabled, rather than optional. Consumers building with `--no-default-features` and selectively re-enabling a `stats` or `client-runtime` feature will need to update their feature selection.
- The stats-related memory-usage warning now scales with the number of configured targets (using `irtt-stats`'s new `estimated_retained_bytes`), instead of assuming a single target's worth of retained memory.

#### Fixed

- Single-target runs are now driven through the same `ManagedClient` path as multi-target runs, instead of a separate single-target code path, fixing behavioral drift between the two (e.g. continuous single-target runs now stop correctly on peer-initiated close and multi-target failures are now reported with a nonzero exit status).
- Continuous (unbounded-duration) runs, both single- and multi-target, now stop and report correctly when the peer closes the session, instead of hanging or exiting silently.
- The TUI's input-event draining is now bounded per tick, fixing a starvation/liveness issue where a burst of input could stall rendering or probe scheduling.
- The TUI's live graph history buffer is now allocated lazily instead of eagerly, avoiding unnecessary upfront allocation for graphs that are never shown.
- A managed-session terminal-state reconciliation fix in the CLI's use of the managed client, correcting a case where a target's final/terminal outcome could be reconciled incorrectly.

#### Compatibility

- MSRV raised to Rust 1.88 (from 1.85), to track `ratatui`'s minimum supported Rust version; this applies to the whole workspace, including `irtt-cli`.
- CI test coverage was expanded to include `musl` and `aarch64` targets, increasing confidence in `irtt-cli` binaries built for those platforms (no code-level behavior change).

## Releasing

Starting with this release, crates are versioned and tagged independently
rather than in lockstep. Each crate's release is tagged as `<crate>/vX.Y.Z`
(e.g. `irtt-proto/v0.5.0`), pointing at the commit its version was released
from. `irtt-proto`, `irtt-client`, `irtt-server`, and `irtt-stats` are
libraries published to [crates.io](https://crates.io); their tags exist to
identify the exact source for a given crates.io release and do not produce a
GitHub Release. `irtt-cli` is the only crate with prebuilt binary artifacts:
pushing an `irtt-cli/vX.Y.Z` tag triggers [cargo-dist](https://opensource.axo.dev/cargo-dist/)
to build and publish a GitHub Release with platform binaries.
