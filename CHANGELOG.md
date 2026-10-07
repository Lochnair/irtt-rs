# Changelog

This project's crates are versioned independently. Each release below is
scoped to a single crate and tagged as `<crate>/vX.Y.Z` (see
[Releasing](#releasing) below).

All notable changes to each crate are documented in its own section per
release. The format is loosely based on
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## irtt-proto

### 0.5.1

#### Added

- Added a crate-specific README and runnable API doctests to make the published crate's protocol types and codecs easier to discover and use.

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

### 0.6.0

#### Added

- Added `ManagedClientHandle::subscribe_status()` and `ManagedStatusSubscription` for observing durable lifecycle and completion state independently of the lossy event stream. The current snapshot is immediately readable, change notifications track subsequent updates, and terminal target status includes its completed outcome.
- Managed targets can override the shared address-family policy while retaining the original hostname for resolution on each new generation.

#### Changed

- `ClientConfig` is now reusable across endpoints: `Client::connect` and `AsyncClient::connect` take the endpoint separately, with configuration grouped into `socket`, `request` (`SessionRequest`), and `open` (`OpenPolicy`). `AddressFamily` replaces the separate socket family flags, and blocking receive timeouts are configured through `Client::set_recv_timeout`.
- Explicit `Authentication`, `HmacKey`, and managed `TargetAuth` replace `ClientConfig.hmac_key` and `ClientAuthConfig`. Key bytes are shared across clones and redacted from debug output. Switching between inherited and explicit authentication or address-family settings starts a new target generation, even when the effective settings are equal.
- `NegotiationResult` replaces `NegotiatedParams`, separating typed `AcceptedSessionParameters` from exact peer-returned wire parameters and accepted `NegotiationChange` records (formerly `NegotiationRestriction`). Open outcomes and lifecycle events now use tuple variants carrying `SessionStarted` or `NoTestCompleted` payloads.
- Low-level clients now leave probe cadence and run duration entirely to the caller. `next_send_deadline` and `is_run_complete` were removed; managed clients continue to provide scheduling and finite-run completion. Low-level callers can inspect `negotiation()`, `has_pending_probes()`, and `next_probe_timeout_deadline()` to drive their own event loop.
- Probe sends now return `Result<SendReceipt, SendProbeError>`, distinguishing failures before socket acceptance from failures after a probe was committed. Receipts convert to `ClientEvent::EchoSent`; its `scheduled_at` and `timer_error` fields are now optional and absent for low-level sends.
- Stop receipts resolve when the stop request is durably observed or the task becomes terminal; callers must still await task completion to observe finished cleanup.

#### Fixed

- Finite managed runs now reach their negotiated end under socket backpressure, even when no pending probe timeout remains to wake the driver. Staggered pacing no longer repeatedly wakes idle or draining targets that have no send deadline.
- Blocking receive operations now return lifecycle errors immediately when no session is open, instead of blocking for a datagram or returning empty success after a timeout.

#### Performance

- Managed drain-deadline lookup no longer scans retained timed-out probes, keeping lookup work bounded as retained probe state grows.

#### Compatibility

- The configuration, authentication, negotiation, lifecycle-event, and send APIs above are breaking changes without compatibility aliases. Low-level callers must enforce their own cadence and duration or use a managed client.
- `AcceptedSessionParameters::dscp` is a six-bit codepoint; `NegotiationResult::peer_params.dscp` remains the raw traffic-class byte. Accepted durations and intervals use Rust duration types rather than wire nanosecond integers.
- Tokio remains optional, and the default client build remains runtime-free. These API changes do not introduce a wire-protocol migration.

### 0.5.3

#### Added

- `SocketConfig` can bind UDP sockets to a network interface and set a packet mark on Linux, Android, and Fuchsia, or select a routing FIB on FreeBSD.

### 0.5.2

#### Fixed

- Kernel transmit-timestamp correlation is now invalidated when a probe send fails, preventing a later timestamp from being matched to the wrong probe.

### 0.5.1

#### Added

- `ManagedCommandReceipt::blocking_wait()` lets synchronous callers wait for a managed target-set update to be applied or rejected without entering an async runtime.

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

### 0.5.3

#### Added

- Added a crate-specific README and usage examples for single- and multi-listener servers, plus runnable API doctests.

### 0.5.2

#### Changed

- `parse_hex_pattern`'s chunking now uses the stable `slice::as_chunks` API instead of `chunks_exact`; no behavior change.

### 0.5.1

#### Added

- New public `address_family_available()` helper (re-exported from `set`), for probing whether an address family can actually bind locally rather than merely constructing a socket for it. Used by the `irtt-rs` server applet to distinguish a genuinely unsupported address family from an ordinary bind failure when falling back from its default dual-family bind.

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

### 0.5.2

#### Changed

- `TimeStats` is now re-exported from `measurement-stats`, retaining the existing `irtt_stats::TimeStats` import path, fields, and methods.
- Timer-error statistics now omit sends without scheduling metadata instead of recording an artificial measurement.

#### Fixed

- Time-based rolling windows now expire backdated events correctly, including expired events inside retained arrival history. Older timestamps cannot move the expiry anchor backwards or restore expired history.
- Rolling upstream and downstream loss now use the server receive-count increase over the retained packet-event interval instead of comparing window counts against a cumulative server count.
- Rolling directional-loss estimates are unavailable when counter observations are missing, stale, discontinuous, or separated by an interior gap in the time window, rather than reporting misleading values.

#### Performance

- Indexed rolling-window expiry avoids full-window scans on insertion and reclaims expired entries without accumulating tombstones.

#### Compatibility

- This release consumes `irtt-client` 0.6 events; applications using both crates must upgrade them together.
- Unavailable rolling directional-loss packet estimates are `None`; their percentage fields remain `0.0`. Valid signed estimates remain possible, and raw server receive counts retain their cumulative meaning.
- Time-based rolling windows expire when new events arrive, retain events exactly at the cutoff, and have no event-count cap. Use `rolling_count` when event storage needs a fixed bound.

### 0.5.1

#### Added

- Added a crate-specific README, a finite-versus-continuous statistics example, and runnable API doctests for the published crate.

### 0.5.0

#### Fixed

- Exact median calculation for `send_call`, `timer_error`, and `server_processing` event-duration statistics, which previously always reported `None` for these medians instead of a computed value.

#### Added

- `LateReplyMode` (`Measure` / `CountOnly`) lets a consumer choose whether replies that arrive after their probe is considered late are still fully measured into the running statistics or only counted, without affecting the other samples.
- `StatsConfig::estimated_retained_bytes(probe_count)` gives callers an API to estimate the memory a stats configuration will retain for a given probe count, ahead of actually running a session (used by `irtt-cli`'s multi-target memory-usage warning, see below).

## irtt-rs

### 0.8.0

#### Added

- Added mutually exclusive `-4` / `--ipv4`, `-6` / `--ipv6`, and `--dual-stack` options to the client and TUI. Dual-stack hostnames expand into independent `/v4` and `/v6` targets with separate sessions, statistics, and failures; explicit IP literals remain single targets.
- Continuous TUI runs now reconnect independently after target failures, peer closure, or server-limited session completion while healthy targets continue uninterrupted. The TUI remains running even when every target fails.
- Added target selection and a scrollable details sheet showing negotiation, counters, timing statistics, warnings, and recent events.
- Graph axes now display local wall-clock timestamps, including fractional seconds or dates where appropriate.

#### Changed

- Replaced separate Graph and Dashboard views with one dashboard comparing all targets. Target rows show effective RTT, cumulative loss, jitter, and the age of the last primary reply. `Tab` / `Shift-Tab` selects a target; `d` opens details, with `g` retained as an alias.
- Reconnecting targets retain their logical row and graph history, reset session statistics for each new generation, and leave gaps between generations.
- Increased graph retention from 100,000 to 500,000 samples per target and extended the maximum graph window from one hour to 24 hours.
- Clearing graph history now clears all targets and returns to live view while preserving latest samples and statistics.
- Client and TUI frontends now react asynchronously to measurements, status changes, input, and shutdown, removing periodic polling delays.
- The application library's `run_stream`, `run_tui`, and `prepare_managed_run` entry points are now asynchronous.

#### Performance

- Graph rendering selects samples within the visible viewport instead of scanning the full retained history, with interpolation at viewport boundaries.
- Reduced graph-history memory usage per sample substantially, allowing retention to increase from 100,000 to 500,000 samples per target without proportional memory growth.
- Increased regular TUI refresh frequency to 100 ms for smoother updates.

#### Compatibility

- Automatic reconnect applies to continuous TUI runs, not ordinary `irtt-client` runs or finite TUI runs. Explicit stops and no-test completion do not trigger retries.
- Dual-stack expansion changes target labels and can double probe traffic per hostname. The stdin limit of 128 desired targets applies after expansion; generated labels must remain unique.
- Address families are discovered when a declaration is added. Each new session resolves the original hostname within its assigned family; newly appearing families are not added automatically. Unchanged stdin declarations retain their discovered families, while removal and re-addition performs fresh discovery.
- New stdin declarations are prepared asynchronously while existing measurements continue. Preparation errors stop the stream gracefully without applying a partial target set.
- The larger graph cap permits higher eventual memory use: approximately 42 MiB per target for retained graph samples at the cap on 64-bit macOS, excluding statistics and rendering buffers.
- Application-library callers must await client/TUI run and preparation functions and supply the new shutdown argument to the run functions.
- `ClientArgs::prepare` and `TuiArgs::prepare` are also asynchronous; `TargetPreparation::prepare` replaces `prepare_managed_targets`, and `parse_stdin_target_set` now returns `TargetSpec` values for preparation.
- The application's former `TargetAuth` type was replaced by `irtt_client::managed::TargetAuth`.
- The public output wrappers `EventRenderStats` and `IpdvPair` were removed in favor of `irtt_stats::EventStatsUpdate` and `IpdvPairUpdate`.
- `DEFAULT_RECV_TIMEOUT`, `is_shutdown_requested`, and `TuiArgs::timestamp_mode` were removed. `install_signal_handler` is now available only with the `server` feature; client/TUI embedding uses `install_async_signal_handler`.
- MSRV remains Rust 1.88. Existing output column names and formats are preserved.

### 0.7.1

#### Fixed

- `IRTT_SERVER_NO_DSCP` now accepts ordinary boolean environment values such as `1` and `0`; falsey values leave DSCP marking enabled, while truthy values disable it. The `--no-dscp` command-line flag remains a flag without a value.

### 0.7.0

#### Added

- Client target arguments now support per-target HMAC configuration: append `@hmac=KEY` to override the global `--hmac` value for that target, or use `@hmac=` to explicitly disable HMAC for it. The target syntax supports context-aware escaping for literal delimiters.
- Continuous client runs can now take complete desired target sets from standard input with `--targets-stdin`. Each non-empty record atomically replaces the current set, `[]` selects an empty set, and EOF requests a graceful stop. The controller retains only the newest unapplied record under transient replacement backpressure, with explicit bounds on record size, desired targets, and live target generations.

#### Changed

- A peer-initiated close is now target-local in stdin-controlled continuous runs: the controller and other desired targets keep running, and a later desired set can start that target again.

### 0.6.2

#### Added

- Every `irtt-server` applet flag can now also be set via an `IRTT_SERVER_*` environment variable (`IRTT_SERVER_BIND`, `IRTT_SERVER_HMAC`, `IRTT_SERVER_MAX_SESSIONS`, `IRTT_SERVER_MAX_PACKET_LENGTH`, `IRTT_SERVER_MIN_INTERVAL`, `IRTT_SERVER_BURST`, `IRTT_SERVER_IDLE_TIMEOUT`, `IRTT_SERVER_MAX_DURATION`, `IRTT_SERVER_TIMESTAMP_ALLOWANCE`, `IRTT_SERVER_NO_DSCP`), for deployments that configure by environment (containers, orchestrators) instead of command-line flags. An explicit flag on the command line still overrides the corresponding variable.
- A `Dockerfile` and Gitea Actions workflow publishing a minimal, statically linked `irtt-server` container image (cross-compiled for `amd64`/`arm64` via `tonistiigi/xx`, no QEMU) to this project's Gitea and GHCR container registries.

#### Changed

- The bundled `ctrlc` dependency now enables its `termination` feature, so the applet also handles `SIGTERM` (not just `SIGINT`) for graceful shutdown.

### 0.6.1

#### Added

- The `irtt-server` applet now has sensible zero-argument defaults: with no `--bind`, it binds the wildcard IRTT port on both address families (`[::]:2112` then `0.0.0.0:2112`) instead of requiring an explicit address, on platforms where wildcard reply-source selection is supported. Any explicit `--bind` fully replaces the default pair rather than adding to it.

#### Changed

- If one of the two default listeners fails because its address family has no local support on the host (e.g. IPv6 administratively disabled on an otherwise IPv4-capable host), the server now falls back to serving just the surviving family instead of failing outright. Any other failure reason (port in use, permission denied, no safe wildcard reply-source path) still fails startup as it would for an explicit `--bind`.

### 0.6.0

#### Changed

- The application Cargo package was renamed from `irtt-cli` to `irtt-rs`, moving to `crates/irtt-app`. This is the application distribution package that produces the `irtt-rs`, `irtt-client`, `irtt-tui`, and `irtt-server` binaries; it is unrelated to the reusable `irtt-client` library crate, which keeps its existing name and version.
- The `irtt-cli` executable was hard-renamed to `irtt-client` to avoid reading as an umbrella CLI now that the application also ships a first-class server. There is no `irtt-cli` compatibility binary or alias.
- Dedicated binaries now have role-specific entry points (`irtt-client`, `irtt-tui`, `irtt-server`) instead of all funneling through the multicall dispatcher's argv0/subcommand logic. Each dedicated binary always runs its own role regardless of the name it is invoked or copied under, and no longer links code for the other applet roles it doesn't implement. The `irtt-rs` binary remains the feature-selectable multicall dispatcher and is the only binary that inspects argv0 or subcommands.
- Future application releases move to the `irtt-rs/vX.Y.Z` tag namespace (previously `irtt-cli/vX.Y.Z`); see [Releasing](#releasing).

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
GitHub Release. The application package (`irtt-cli` through 0.5.0, `irtt-rs`
from 0.6.0 onward) is the only package with prebuilt binary artifacts: pushing
an `irtt-rs/vX.Y.Z` tag triggers [cargo-dist](https://opensource.axo.dev/cargo-dist/)
to build and publish a GitHub Release with platform binaries. The historical
`irtt-cli/v0.5.0` tag remains as released; it is not rewritten or replaced.
