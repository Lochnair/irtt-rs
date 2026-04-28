# IRTT-RS Rust Design Draft

## Status

This document is a Rust-side architecture and implementation design brief for an independent IRTT-compatible client suite.

It is intended to be consumed by a clean implementation-planning or implementation agent together with:

- `IRTT_CLIENT_PROTOCOL_SPEC.md`
- `BLACKBOX_VERIFICATION_REPORT.md`
- clean packet test vectors
- clean-room process notes

This document describes Rust architecture and product goals. It is not a protocol specification and does not copy or depend on upstream source structure.

---

## Core Product Goal

Build a layered, low-memory, event-producing IRTT-compatible Rust client suite.

The project is not primarily an upstream CLI clone. The primary design goal is a reusable client engine suitable for:

- `sqm-autorate-rust`
- `cake-autorate` integration through a CLI
- router-friendly continuous latency probing
- normal user-facing finite IRTT-style tests
- optional aggregate statistics and reports

The central architectural rule is:

```text
irtt-client emits probe events.
irtt-stats aggregates probe events.
irtt-cli formats probe events and/or statistics.
```

Aggregate statistics must not be welded into the client engine.

---

## License

Preferred project license:

```text
Apache-2.0
```

Optional alternative if broader Rust ecosystem convention is desired:

```text
MIT OR Apache-2.0
```

The implementation agent must not inspect upstream GPL source code, upstream tests, upstream comments, contaminated notes, or contaminated transcripts.

---

## Workspace Layout

Use a Cargo workspace with four crates:

```text
irtt-rs/
  Cargo.toml
  crates/
    irtt-proto/
    irtt-client/
    irtt-stats/
    irtt-cli/
  docs/
    IRTT_CLIENT_PROTOCOL_SPEC.md
    BLACKBOX_VERIFICATION_REPORT.md
    CLEANROOM.md
    RUST_DESIGN.md
  test-vectors/
```

---

# Crate Responsibilities

## `irtt-proto`

Pure protocol crate.

### Responsibilities

`irtt-proto` owns only binary/protocol logic:

- packet flags
- magic/version constants
- varint/zigzag parameter encoding and decoding
- parameter model
- packet layout calculation
- open request/reply encoding and decoding
- echo request/reply encoding and decoding
- close request encoding
- HMAC-MD5 calculation and verification
- clean packet vector tests

### Must Not Contain

`irtt-proto` must not contain:

- sockets
- threads
- channels
- timers
- CLI code
- statistics aggregation
- session runtime control
- event fanout
- long-term sample retention
- OS-specific network metadata handling

### Important Boundary

ECN, DSCP, TOS, Traffic Class, and kernel receive timestamps are socket/IP metadata. They are not IRTT payload fields. `irtt-proto` must not model ECN as a protocol field.

---

## `irtt-client`

Client engine crate.

### Responsibilities

`irtt-client` owns:

- connected UDP socket setup
- socket configuration through `socket2`
- session negotiation
- open/no-test/active/close lifecycle
- caller-paced manual client API
- optional managed threaded runner
- probe sending
- reply receiving
- packet validation
- timestamp capture
- RTT sample construction
- duplicate detection
- late detection
- loss timeout detection
- public event types
- optional public event fanout for managed mode

### Must Not Contain

`irtt-client` must not contain:

- aggregate statistics
- rolling windows
- median/stddev/variance
- human final reports
- CLI formatting
- mandatory full sample history
- upstream-style result monoliths

### Retention Boundary

`irtt-client` must not retain samples for reporting or aggregation.

It may retain bounded in-flight operational state required for:

- matching replies to sent probes
- computing RTT for completed probes
- detecting duplicates
- detecting late replies
- detecting loss after timeout
- managing sequence/session state

Historical retention, rolling windows, finite-run summaries, and aggregate statistics belong in `irtt-stats` or in the consuming application.

---

## `irtt-stats`

Optional statistics crate.

### Responsibilities

`irtt-stats` consumes `ClientEvent` values and provides:

- cumulative statistics
- rolling time-window statistics
- rolling sample-count statistics
- finite-run summaries
- RTT summaries
- raw RTT summaries
- adjusted RTT summaries
- IPDV summaries
- server-processing summaries
- loss summaries
- duplicate/late summaries
- min/max/mean/median/stddev/variance where supported

### Must Not Contain

`irtt-stats` must not contain:

- sockets
- packet encoding
- packet decoding
- session negotiation
- thread/runtime ownership
- CLI parsing

It is a consumer of events, not a driver of the client.

---

## `irtt-cli`

User-facing executable crate.

### Long-Term Role

`irtt-cli` should become the full user-facing client over time.

It should eventually support practical feature completeness for client usage:

- finite tests
- continuous streaming
- human output
- machine output
- statistics
- HMAC
- DSCP / Traffic Class
- timestamp modes
- received stats modes
- server fill
- no-test/check mode
- optional JSON output

### Stats Relationship

`irtt-cli` should consume `irtt-stats` by default in normal builds.

The stats dependency is feature-gated for binary size and resource usage, not because stats are second-class.

Default/full CLI builds should include stats.

Minimal/router builds may disable stats to produce a smaller streaming-only binary.

```toml
[features]
default = ["stats"]
stats = ["dep:irtt-stats"]
json = ["dep:serde_json", "irtt-client/serde", "irtt-stats/serde"]
tracing = ["irtt-client/tracing"]
ancillary = ["irtt-client/ancillary"]
interop = []
```

Minimal build example:

```bash
cargo build -p irtt-cli --no-default-features --release
```

Normal build example:

```bash
cargo build -p irtt-cli --release
```

### Layering Rule

`irtt-cli` must compose `irtt-client` and `irtt-stats`. It must not reimplement protocol, session, or statistics logic internally.

---

# Public Client Layers

`irtt-client` should provide two public client layers:

1. `Client`
2. `ManagedClient`

## `Client`

`Client` is the manual, caller-paced client.

It owns:

- connected UDP socket
- socket configuration
- local and remote addresses
- session state
- negotiated parameters
- connection token
- sequence state
- bounded pending-probe state

It does not own:

- threads
- public event hub
- statistics aggregation
- CLI formatting
- long-term sample history

The consumer controls pacing and polling.

### Example Shape

```rust
pub struct Client {
    socket: ClientSocket,
    state: SessionState,
}

impl Client {
    pub fn connect(config: ClientConfig) -> Result<Self, ClientError>;
    pub fn open(&mut self, now: ClientTimestamp) -> Result<Vec<ClientEvent>, ClientError>;
    pub fn next_send_deadline(&self) -> Option<Instant>;
    pub fn send_probe(&mut self, now: ClientTimestamp) -> Result<Vec<ClientEvent>, ClientError>;
    pub fn recv_once(&mut self) -> Result<Option<Vec<ClientEvent>>, ClientError>;
    pub fn recv_available(&mut self, budget: RecvBudget) -> Result<Vec<ClientEvent>, ClientError>;
    pub fn poll_timeouts(&mut self, now: ClientTimestamp) -> Result<Vec<ClientEvent>, ClientError>;
    pub fn close(&mut self, now: ClientTimestamp) -> Result<Vec<ClientEvent>, ClientError>;
}
```

The exact API may be adjusted, but the core principle is stable:

```text
Client owns socket/session.
Caller owns pacing.
```

Manual pacing is required so consumers such as `sqm-autorate-rust` can integrate probing into their own control loop.

---

## `ManagedClient`

`ManagedClient` is a convenience runner built on the same socket/session primitives.

It owns:

- runtime threads
- cancellation
- pacing loop
- receive loop
- coordinator/correlator
- event fanout

It is intended for:

- `irtt-cli`
- simple applications
- integration tests
- users who do not need manual control

### Example Shape

```rust
pub struct ManagedClientSession {
    // owns worker threads and event hub
}

impl ManagedClientSession {
    pub fn subscribe(&self, config: SubscriberConfig) -> EventSubscription;
    pub fn stop(&self);
    pub fn join(self) -> Result<SessionOutcome, ClientError>;
}

pub struct ManagedClient;

impl ManagedClient {
    pub fn start(config: ClientConfig) -> Result<ManagedClientSession, ClientError>;
}
```

---

# Socket Ownership

The IRTT session uses one connected UDP socket for open, echo, and close traffic.

This socket is part of the client/session abstraction and should be owned by `irtt-client`, not by downstream consumers.

Manual pacing does not imply manual socket ownership.

## `ClientSocket`

Use an internal socket wrapper named `ClientSocket`.

Do not use names like `IrtSocket`.

```rust
struct ClientSocket {
    inner: std::net::UdpSocket,
}
```

`ClientSocket` is responsible for:

- creating the socket
- binding local address
- configuring socket options
- connecting UDP socket
- sending packets
- receiving packets
- cloning handles if required by managed mode
- returning optional packet metadata

## `socket2`

Use `socket2` for socket creation and configuration.

The MVP implementation may create/configure with `socket2::Socket`, then convert into `std::net::UdpSocket` internally.

`socket2` is required because the standard library does not expose all socket options needed for DSCP/TOS/Traffic Class/TTL style behavior.

## One Socket, Multiple Handles

Managed mode may clone the underlying socket handle for sender/receiver threads.

This is still one connected UDP socket at the OS/session level.

---

# Received Packet Metadata

ECN and DSCP are IP-header metadata. They are not part of the IRTT payload wire format.

The client socket abstraction should expose optional packet metadata:

```rust
pub struct PacketMeta {
    pub traffic_class: Option<u8>,
    pub dscp: Option<Dscp>,
    pub ecn: Option<Ecn>,
    pub kernel_rx_timestamp: Option<SystemTime>,
}
```

## Important Modeling Rule

The implementation must distinguish:

- raw Traffic Class / TOS byte
- DSCP bits
- ECN bits

It must not collapse these into a single ambiguous `ecn` field.

## MVP

MVP may use normal `UdpSocket::recv` / `recv_from` behavior internally.

In this mode:

```text
traffic_class = None
dscp = None
ecn = None
kernel_rx_timestamp = None
```

## Future `ancillary` Feature

A future `ancillary` feature should use platform control-message APIs, such as `recvmsg`, to populate:

- raw received Traffic Class / TOS byte
- derived DSCP
- derived ECN
- optional kernel receive timestamp

Normal protocol compatibility must not depend on this feature.

## Upstream Server ECN Mode

Upstream server ECN mode should be treated as an advanced compatibility feature. It appears to require a control-message socket path rather than a new IRTT payload field.

Before claiming compatibility with this mode, add dedicated black-box tests using `irtt server --ecn`.

---

# DSCP / Traffic Class / ECN API

The Rust API should expose real DSCP and ECN concepts while also permitting raw compatibility.

```rust
pub struct Dscp(u8); // valid 0..=63
pub struct Ecn(u8);  // valid 0..=3

pub enum TrafficClassConfig {
    Dscp(Dscp),
    DscpEcn { dscp: Dscp, ecn: Ecn },
    Raw(u8),
}
```

CLI should distinguish:

```text
--dscp 46              # real DSCP 46, raw byte 0xb8
--traffic-class 0xb8   # raw byte compatibility
--ecn ect0             # optional/future explicit ECN helper
```

The protocol parameter ultimately negotiates/sends a raw Traffic Class / TOS byte. The public Rust API should avoid misleading users into thinking every value is a pure 6-bit DSCP codepoint.

---

# Runtime Model

## Core Decision

Use a synchronous/threaded core for the managed runner.

Do not make Tokio or any async runtime required by the core crates.

Async integration may be added later as a wrapper or feature.

## Manual `Client`

Manual `Client` owns no threads.

The caller decides when to:

- open
- send probes
- poll receive traffic
- poll timeouts
- close

This is the preferred integration layer for `sqm-autorate-rust` if it wants direct control.

## Managed Runtime Topology

After negotiation, `ManagedClient` should use three runtime roles:

1. coordinator/control thread
2. sender/pacer thread
3. receiver thread

### Sender/Pacer Thread

The sender/pacer thread is timing-sensitive.

It must not perform:

- statistics aggregation
- public event fanout
- CLI formatting
- subscriber delivery
- long-running coordination work
- blocking operations unrelated to sending

It should only:

- maintain absolute send schedule
- prepare outgoing packets before deadline where possible
- send packets
- timestamp immediately before send call
- report compact sent records to coordinator

### Receiver Thread

The receiver thread should:

- block on receive
- timestamp immediately after receive returns, before expensive parsing/validation
- parse/validate reply enough to create a compact reply record
- report compact reply records to coordinator

### Coordinator Thread

The coordinator should:

- correlate sent and received records
- compute RTT samples
- manage pending probes
- handle loss timeouts
- detect duplicates/late replies
- emit public events through the event hub
- handle graceful shutdown and close

---

# Event Distribution

`flume` channels are single-consumer queues, not broadcast channels.

Therefore `ManagedClient` must not expose a single shared `Receiver<ClientEvent>` as the only event API.

Use `flume` internally, but provide a small fanout/broadcast layer for public subscribers.

## Event Hub

```rust
pub struct EventHub {
    // internal list of subscriber queues
}

pub struct EventSubscription {
    receiver: flume::Receiver<ClientEvent>,
    id: SubscriberId,
}
```

Consumers subscribe independently:

```rust
let stats_rx = session.subscribe(SubscriberConfig::stats());
let sqm_rx = session.subscribe(SubscriberConfig::latest_only());
let cli_rx = session.subscribe(SubscriberConfig::cli());
```

Each subscriber receives its own copy of each event.

## Subscriber Configuration

```rust
pub struct SubscriberConfig {
    pub capacity: usize,
    pub overflow: SubscriberOverflow,
    pub event_filter: EventFilter,
}

pub enum SubscriberOverflow {
    DropNewest,
    DropOldest,
    Block,
    Disconnect,
}
```

Suggested profiles:

```text
live control / sqm:
  DropOldest, small bounded queue, keep freshest data

stats:
  Block or Disconnect, because dropped events corrupt statistics

debug logging:
  DropNewest
```

The event hub should track dropped events per subscriber where practical.

---

# Public Events

Events should be compact and cheap to clone.

Do not include large packet buffers in normal public events.

## Core Events

```rust
pub enum ClientEvent {
    SessionStarted {
        session_id: SessionId,
        remote: SocketAddr,
        negotiated: NegotiatedParams,
    },

    EchoReply {
        session_id: SessionId,
        seq: u64,
        wire_seq: u32,
        remote: SocketAddr,
        sent_at: ClientTimestamp,
        received_at: ClientTimestamp,
        rtt: RttSample,
        one_way: Option<OneWayDelaySample>,
        server_timing: Option<ServerTiming>,
        received_stats: Option<ReceivedStatsSample>,
        packet_meta: PacketMeta,
    },

    EchoLoss {
        session_id: SessionId,
        seq: u64,
        wire_seq: u32,
        kind: LossKind,
    },

    DuplicateReply {
        session_id: SessionId,
        seq: u64,
        wire_seq: u32,
    },

    LateReply {
        session_id: SessionId,
        seq: u64,
        wire_seq: u32,
        rtt: Option<RttSample>,
    },

    Warning {
        session_id: Option<SessionId>,
        warning: ClientWarning,
    },

    SessionEnded {
        session_id: SessionId,
        reason: SessionEndReason,
    },

    FatalError {
        session_id: Option<SessionId>,
        error: ClientErrorKind,
    },
}
```

`EchoSent` should not be emitted by default. It may be available as an optional debug event.

---

# Sequence Numbers

The wire protocol sequence number is 32-bit.

Public events should expose a logical `u64` sequence number and optionally also the wire `u32` sequence.

```rust
struct ProbeSeq {
    logical: u64,
    wire: u32,
}
```

This keeps continuous mode sane even if wire sequence wraps in very long sessions.

---

# Run Modes

```rust
pub enum RunMode {
    NoTest,
    Duration(Duration),
    Count(u64),
    Continuous,
}
```

## `NoTest`

Open and immediately close/complete without echo probes.

Used for:

- CLI check mode
- negotiation smoke tests
- interop tests

## `Duration`

Finite duration-limited test.

Finite duration has an exclusive end. Expected packet count for finite duration is:

```text
ceil(duration / interval)
```

## `Count`

Finite packet-count test.

Useful for deterministic tests and simple manual checks.

## `Continuous`

Continuous mode is first-class.

It must:

- emit events until stopped
- avoid full sample retention
- avoid preallocating finite result vectors
- keep memory bounded
- send close packet on graceful stop where possible

If the server permits unbounded or sufficiently long sessions, use a single long-lived session.

If the server negotiates a finite duration or closes the session, behavior is controlled by `ReconnectPolicy`.

```rust
pub enum ReconnectPolicy {
    Never,
    OnServerClose { delay: Duration },
    Always { delay: Duration, max_attempts: Option<u32> },
}
```

Suggested defaults:

```text
library default:
  ReconnectPolicy::Never

CLI continuous default:
  ReconnectPolicy::OnServerClose { delay: 1s }
```

---

# Negotiation Policy

Do not use a confusing strict/loose boolean as the primary API.

Use:

```rust
pub enum NegotiationPolicy {
    AcceptServerRestrictions,
    RequireExact,
    RequireWithin(LocalRequirements),
}
```

Suggested defaults:

```text
irtt-client default:
  AcceptServerRestrictions

interop tests:
  RequireExact where useful

sqm/cake usage:
  RequireWithin(...) when the caller has hard operational limits
```

The server is authoritative unless the caller explicitly requires exact or bounded negotiated values.

---

# Timing Policy

## Send Timestamp

Capture send timestamp immediately before the `send` call. This ensures the
measured RTT includes local send/enqueue overhead, which is the more
conservative direction for latency-control consumers.

## Receive Timestamp

Capture receive timestamp immediately after receive returns, before expensive validation/parsing.

If kernel receive timestamps are available through an optional ancillary feature, expose them separately in `PacketMeta` or timestamp metadata.

## Timer Scheduling

Managed sender uses absolute scheduling:

```text
scheduled_time(seq) = session_start + seq * interval
```

Do not schedule by repeatedly doing:

```text
next = now + interval
```

because that accumulates drift.

## Timer Modes

```rust
pub enum TimerMode {
    Normal,
    Precise { spin_threshold: Duration },
}
```

Default:

```text
TimerMode::Normal
```

Router mode must not busy-spin unless explicitly requested.

---

# RTT Model

Do not collapse RTT into a single ambiguous value.

Expose raw RTT, server processing time, and adjusted RTT separately.

```rust
pub struct RttSample {
    pub raw: Duration,
    pub adjusted: Option<Duration>,
    pub server_processing: Option<Duration>,
}

impl RttSample {
    pub fn effective(&self) -> Duration {
        self.adjusted.unwrap_or(self.raw)
    }
}
```

## Rules

```text
raw RTT:
  client_receive_mono - client_send_mono

server processing:
  server_send - server_receive, if both relevant server timestamps are available

adjusted RTT:
  raw - server_processing, only if server_processing <= raw

if server_processing > raw:
  adjusted = None
  emit warning
  effective RTT falls back to raw
```

Warning type:

```rust
pub enum ClientWarning {
    ServerProcessingExceedsRawRtt {
        seq: u64,
        raw: Duration,
        server_processing: Duration,
    },
}
```

Consumers decide whether to use raw, adjusted, or effective RTT.

Suggested defaults:

```text
sqm-autorate-rust:
  use rtt.effective()

simple CLI/rtt-us format:
  print rtt.effective()

machine CLI format:
  include effective, raw, and server-processing fields

stats:
  provide effective by default, but expose raw/adjusted variants where useful
```

---

# Loss Model

`irtt-client` may emit loss events based on timeout/loss policy.

```rust
pub enum LossDetection {
    Disabled,
    Timeout(Duration),
    MultipleOfInterval(u32),
}
```

Suggested default for continuous mode:

```text
LossDetection::MultipleOfInterval(3)
```

Loss kind:

```rust
pub enum LossKind {
    Unknown,
    Upstream,
    Downstream,
}
```

Direction classification should use received count/window when available. If insufficient data exists, use `Unknown`.

---

# Validation Policy

```rust
pub enum ValidationPolicy {
    Strict,
    DropMalformed,
    DropUnrelatedAbortMalformedSession,
}
```

Suggested default:

```text
Drop unrelated garbage.
Abort or fatal-error on packets that appear to belong to the active session but violate core protocol invariants.
```

Examples:

```text
wrong magic / unrelated packet:
  drop

bad HMAC / bad token / impossible sequence / malformed active-session packet:
  abort or emit FatalError depending on policy
```

---

# Cancellation and Outcome

No signal handling in `irtt-client`.

`irtt-cli` handles Ctrl-C and calls `stop()` / `join()`.

```rust
pub struct CancellationToken {
    // Arc<AtomicBool> internally
}

pub struct SessionOutcome {
    pub end_reason: SessionEndReason,
    pub packets_sent: u64,
    pub replies_received: u64,
    pub duplicates: u64,
    pub late: u64,
    pub malformed: u64,
}
```

For manual `Client`, the caller owns cancellation by simply not calling further send/poll methods and then calling close/drop as appropriate.

---

# CLI Output

CLI output must support router-friendly, awk-friendly, line-oriented formats.

The design is inspired by classic ping-style machine output:

- one result/event per line
- fixed positional fields
- configurable delimiter
- optional timestamp
- separate human and machine output
- no JSON parser required for router usage

## Formats

Support:

```text
human
machine
simple
rtt-us
jsonl   # optional feature
```

## Machine Format

Default delimiter: tab.

Reply line:

```text
reply<TAB>timestamp<TAB>remote<TAB>session_id<TAB>seq<TAB>rtt_us<TAB>raw_rtt_us<TAB>server_processing_us<TAB>down_us<TAB>up_us<TAB>ecn<TAB>status
```

Loss line:

```text
loss<TAB>timestamp<TAB>remote<TAB>session_id<TAB>seq<TAB>loss_kind
```

Duplicate line:

```text
duplicate<TAB>timestamp<TAB>remote<TAB>session_id<TAB>seq
```

Late line:

```text
late<TAB>timestamp<TAB>remote<TAB>session_id<TAB>seq<TAB>rtt_us
```

Session line:

```text
session<TAB>timestamp<TAB>remote<TAB>session_id<TAB>state<TAB>reason
```

Example:

```text
reply	1714320000.123456	1.2.3.4:2112	1	42	8341	8420	79	-	-	-	ok
loss	1714320000.323456	1.2.3.4:2112	1	43	unknown
```

Field meanings:

```text
rtt_us:
  effective RTT = adjusted if valid, else raw

raw_rtt_us:
  raw client-measured RTT

server_processing_us:
  server processing time or "-"

down_us/up_us:
  one-way estimates if available, otherwise "-"

ecn:
  received ECN if available, otherwise "-"
```

## Simple Format

```text
timestamp<TAB>seq<TAB>result
```

Example:

```text
1714320000.123456	42	8341
1714320000.323456	43	loss
1714320000.523456	44	8120
```

## RTT-Only Format

```text
8341
loss
8120
```

This is intended for extremely simple shell consumption.

## Delimiter

Support:

```text
--delimiter tab
--delimiter ','
--delimiter ';'
--delimiter '|'
```

Internally, join fields. Do not mutate format strings.

## Timestamp Format

Default timestamp format:

```text
unix_seconds.microseconds
```

Example:

```text
1714320000.123456
```

Avoid nanosecond epoch integers in awk-oriented output.

## Header

Default: no header.

Optional:

```text
--header
```

Machine-readable output formats must be stable. Fields must not be reordered between releases. New fields should be added only in a new named/versioned format or at the end when explicitly documented.

---

# Dependencies

## `irtt-proto`

```toml
[dependencies]
thiserror = "2"
hmac = "0.12"
md-5 = "0.10"
subtle = "2"
fastrand = "2"
serde = { version = "1", features = ["derive"], optional = true }
```

## `irtt-client`

```toml
[dependencies]
irtt-proto = { path = "../irtt-proto" }
thiserror = "2"
socket2 = "0.5"
flume = "0.11"
fastrand = "2"
serde = { version = "1", features = ["derive"], optional = true }
tracing = { version = "0.1", optional = true }
```

Future optional dependency for ancillary receive metadata:

```toml
rustix = { version = "...", optional = true, features = ["net"] }
```

Exact ancillary dependency can be chosen later.

## `irtt-stats`

```toml
[dependencies]
irtt-client = { path = "../irtt-client" }
thiserror = "2"
serde = { version = "1", features = ["derive"], optional = true }
```

## `irtt-cli`

```toml
[dependencies]
irtt-client = { path = "../irtt-client" }
irtt-stats = { path = "../irtt-stats", optional = true }
clap = { version = "4", features = ["derive"] }
thiserror = "2"
ctrlc = "3"
serde_json = { version = "1", optional = true }
```

---

# Testing Strategy

## Pure Tests

Normal `cargo test` should run:

- varint encode/decode tests
- parameter encode/decode tests
- packet layout calculation tests
- open request encode tests
- open reply decode tests
- echo request encode tests
- echo reply decode tests
- close request encode tests
- HMAC calculation and verification tests
- clean packet vector tests
- stats unit tests
- CLI formatter unit tests

## Interop Tests

Interop tests require `irtt` in `PATH`.

Local default:

```bash
cargo test
```

Interop:

```bash
cargo test --workspace --features interop
```

CI should run interop tests and provide upstream `irtt` in `PATH`.

Local development should not require `irtt` unless the interop feature is explicitly enabled.

## `tshark`

`tshark` is diagnostic/manual only.

Do not make `tshark` a normal test or CI dependency.

## Black-Box ECN Tests

Before claiming support for upstream server ECN mode, run dedicated black-box tests for:

- IPv4 behavior
- IPv6 behavior
- server `--ecn`
- outgoing Traffic Class values with ECN bits set
- received Traffic Class metadata
- logged ECN values
- behavior with and without ancillary receive support

---

# Implementation Milestones

## Milestone 0 — Workspace Skeleton

- create Cargo workspace
- create crates
- add license files
- add clean-room docs
- add design docs
- ensure no upstream source is present

## Milestone 1 — `irtt-proto`

- flags
- constants
- varint/zigzag
- params
- packet layout
- open request/reply
- echo request/reply
- close request
- HMAC
- packet vectors passing

## Milestone 2 — `irtt-client::Client` No-Test / Negotiation

- `ClientSocket` with `socket2`
- connected UDP socket
- open request
- open reply parse
- no-test mode
- close behavior
- basic interop with upstream server

## Milestone 3 — Manual Client Finite Probe Events

- caller-paced send_probe
- recv_once / recv_available
- timeout polling
- EchoReply events
- RTT sample model
- duplicate/late detection
- finite count/duration smoke test

## Milestone 4 — Minimal `irtt-cli` Stream Mode

- use manual or managed layer as appropriate
- machine/simple/rtt-us output
- delimiter support
- timestamp support
- no stats required for minimal build

## Milestone 5 — Managed Runner

- coordinator/control thread
- sender/pacer thread
- receiver thread
- EventHub fanout
- cancellation
- stop/join
- continuous mode

## Milestone 6 — `irtt-stats`

- consumes `ClientEvent`
- cumulative stats
- rolling stats
- finite summary
- min/max/mean/median/stddev/variance
- loss/duplicate/late summaries

## Milestone 7 — Full/default CLI Stats Integration

- default build enables stats
- finite human report
- optional stats lines in stream mode
- minimal `--no-default-features` build still works without stats

## Milestone 8 — Compatibility Features

- HMAC interop
- all timestamp modes
- all received stats modes
- server fill
- DSCP / Traffic Class
- TTL / hop limit where applicable

## Milestone 9 — Advanced Networking

- ancillary receive metadata feature
- incoming Traffic Class / DSCP / ECN
- optional kernel RX timestamps
- upstream server `--ecn` compatibility tests

---

# Known Open / Verification Items

These are not blockers for the initial architecture, but should be tracked:

- exact behavior of upstream server ECN mode
- IPv4 vs IPv6 ECN metadata behavior
- kernel RX timestamp portability
- server close during active test
- rare case where server processing time exceeds raw RTT
- exact handling of very long or unbounded continuous sessions
- ancillary implementation strategy on Linux/OpenWrt/macOS

Design should be defensive around these items and expose data to consumers rather than hiding ambiguity.

---

# Implementation Agent Rules

The implementation agent must follow these rules:

1. Do not inspect upstream source code.
2. Do not inspect upstream tests.
3. Do not inspect contaminated notes or transcripts.
4. Implement from the clean protocol spec, black-box report, packet vectors, and this Rust design brief.
5. Do not build an upstream CLI clone first.
6. Keep `irtt-client` event-first and stats-free.
7. Keep stats in `irtt-stats`.
8. Keep CLI formatting in `irtt-cli`.
9. Keep ECN/DSCP receive metadata out of `irtt-proto`.
10. Keep managed runtime optional; expose manual `Client` for caller-paced integration.

Final architecture mantra:

```text
irtt-proto: wire protocol
irtt-client::Client: socket + session, caller-paced
irtt-client::ManagedClient: threaded convenience runner
irtt-stats: event aggregation
irtt-cli: user-facing formatting and reports
```

