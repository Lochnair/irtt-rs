# IRTT-RS Implementation Plan

## 1. Inputs Reviewed

### RUST_DESIGN.md

**Contribution:** Defines the full Rust architecture: four-crate workspace (`irtt-proto`, `irtt-client`, `irtt-stats`, `irtt-cli`), the `Client` / `ManagedClient` split, event model, CLI output formats, dependency choices, and milestone sequence.

**Sufficiency:** Sufficient for implementation planning. The design is thorough and internally consistent. A few minor amendments are noted in Section 18.

**Inconsistencies found:**
- None that would block implementation. The design is well-aligned with the protocol spec and black-box findings.

### IRTT_CLIENT_PROTOCOL_SPEC.md

**Contribution:** Normative protocol specification. Defines wire format, field ordering, parameter serialization (varint/zigzag), session lifecycle, timing semantics, measurement formulas, validation rules, and error handling. Includes two packet test vectors with HMAC.

**Sufficiency:** Sufficient for implementing a conforming client. All resolved open questions have clear answers. Three open questions remain (19.3, 19.6, 19.12) — none block initial implementation.

**Inconsistencies found:**
- The spec's packet count formula was corrected to `ceil(d/i)` — the spec itself already reflects this correction.
- DSCP parameter range was corrected to 0–255 (TOS byte) — the spec already reflects this.
- Test vector sizes corrected to 92 bytes — the spec already reflects this.
- The flags byte in test vector 18.2 is `0x08` (HMAC only, no Reply flag). The spec explicitly notes this is for HMAC/layout verification only and that real packets require Reply flag `0x02`. This is clear and not a problem.

### BLACKBOX_VERIFICATION_REPORT.md

**Contribution:** Independent black-box verification of protocol behavior against irtt 0.9.1. Resolves 7 open questions, partially resolves 2, confirms spec corrections, and provides packet captures as ground truth.

**Sufficiency:** Sufficient. The remaining unresolved items (19.3, 19.6, 19.12) are edge cases with clear defensive strategies.

**Inconsistencies found:**
- None with the spec (corrections already applied).

### CLEANROOM_NOTES.md

**Contribution:** Documents the clean-room boundary, source materials inspected by the specification agent, post-drafting audit, and second-pass scrub.

**Sufficiency:** Sufficient. Clear separation between contaminated (spec-writing) and clean (implementation) sides.

**Inconsistencies found:**
- None.

### Clean Packet Test Vectors (`docs/test-vectors.md`)

**Contribution:** 12 packet test vectors from actual network captures against irtt 0.9.1. Covers: open request/reply (with and without HMAC), echo request/reply (with and without HMAC, various stats/timestamp configs), close request (with and without HMAC), no-test mode request/reply, minimal echo packet (16 bytes), and midpoint timestamp echo reply.

**Sufficiency:** Excellent. Combined with the two HMAC test vectors in the protocol spec (Section 18.1, 18.2), this provides 14 total test vectors covering every packet type and major field combination. Sufficient for thorough protocol validation.

**Inconsistencies found:**
- None. All vectors are consistent with the protocol spec and black-box report.

---

## 2. Executive Summary

- **Crate structure:** Four crates in a Cargo workspace — `irtt-proto` (wire protocol), `irtt-client` (socket + session + events), `irtt-stats` (event aggregation), `irtt-cli` (user-facing binary).
- **Manual vs managed client:** `Client` is the caller-paced, no-thread, no-stats core. `ManagedClient` is a threaded convenience runner built on the same primitives. Both live in `irtt-client`.
- **Event-first design:** `irtt-client` emits `ClientEvent` values. It never aggregates statistics. Consumers decide what to do with events.
- **Stats separation:** `irtt-stats` consumes `ClientEvent` and provides cumulative, rolling, and summary statistics. It is an optional dependency of `irtt-cli`.
- **CLI output philosophy:** Line-oriented, awk-friendly, delimiter-configurable output. Human, machine, simple, rtt-us, and optional JSONL formats. Default builds include stats; minimal/router builds can omit them.
- **Interop testing strategy:** `cargo test` runs pure unit tests without external dependencies. `cargo test --features interop` requires `irtt` server in PATH. `tshark` is diagnostic-only, never a CI dependency.
- **Clean-room compliance:** Implementation works only from the four clean artifacts. No upstream source, tests, or contaminated notes.
- **Router friendliness:** No mandatory async runtime. No busy-spin by default. Feature-gated optional capabilities. Minimal binary possible with `--no-default-features`.
- **First-class consumers:** `sqm-autorate-rust` (manual `Client` API), `cake-autorate` (CLI machine output), general users (CLI with stats).
- **Staged implementation:** 10 milestones, each independently testable, building from protocol layer up.

---

## 3. Architecture Confirmation

The intended architecture from `RUST_DESIGN.md` is confirmed with no structural changes needed.

### `irtt-proto`

**Public responsibility:**
- Binary wire format encoding and decoding
- Packet constants (magic, flags, protocol version)
- Varint/zigzag encoding and decoding
- Parameter model and serialization
- Packet layout calculation (minimum packet size for given parameters)
- Open request/reply encoding and decoding
- Echo request encoding, echo reply decoding
- Close request encoding
- HMAC-MD5 calculation and verification

**Forbidden responsibility:**
- Sockets, threads, channels, timers
- Statistics aggregation
- Session runtime control
- Event fanout
- ECN/DSCP/TOS socket metadata (these are IP-layer, not IRTT payload)

**Main public types:**
- `Magic`, `Flags`, `ProtocolVersion`
- `Params`, `ParamTag`, `ReceivedStats`, `StampAt`, `Clock`, `ServerFill`
- `OpenRequest`, `OpenReply`
- `EchoRequest`, `EchoReply`
- `CloseRequest`
- `PacketLayout`
- `HmacKey`, HMAC computation functions
- `ProtoError`

**Internal modules:**
- `varint` — uvarint/zigzag encode/decode
- `params` — parameter serialization/deserialization
- `header` — magic/flags/field parsing
- `hmac` — HMAC-MD5 wrapper
- `layout` — packet size calculation

**Feature flags:**
- `serde` — derive Serialize/Deserialize on public types

**Dependencies:**
- `thiserror`, `hmac`, `md-5`, `subtle`, `fastrand`
- `serde` (optional)

### `irtt-client`

**Public responsibility:**
- Connected UDP socket setup and configuration via `socket2`
- Session negotiation (open/close lifecycle)
- Caller-paced manual client API (`Client`)
- Optional managed threaded runner (`ManagedClient`)
- Probe sending and reply receiving
- Packet validation
- Timestamp capture
- RTT sample construction
- Duplicate/late/loss detection
- Public event types (`ClientEvent`)
- Event fanout for managed mode

**Forbidden responsibility:**
- Aggregate statistics (min/max/mean/median/stddev/variance)
- Rolling windows
- Human-readable reports
- CLI formatting
- Mandatory full sample history

**Main public types:**
- `ClientConfig`, `SocketConfig`
- `Client`, `OpenOutcome`
- `ManagedClient`, `ManagedClientSession`
- `EventHub`, `EventSubscription`, `SubscriberConfig`, `SubscriberOverflow`
- `ClientEvent`, `RttSample`, `ServerTiming`
- `ReceivedStatsSample`, `PacketMeta`, `LossKind`
- `ClientWarning`, `SessionOutcome`, `SessionEndReason`
- `NegotiatedParams`, `NegotiationPolicy`, `ValidationPolicy`
- `RunMode`, `LossDetection`, `TimerMode`
- `ReconnectPolicy`, `CancellationToken`
- `ClientError`
- `ClientTimestamp`

**Internal modules:**
- `socket` — `ClientSocket` wrapper
- `session` — session state machine
- `probe` — pending probe tracking
- `validate` — packet validation logic
- `timing` — timestamp capture and RTT computation
- `managed` — managed runner threads and event hub

**Feature flags:**
- `serde` — derive Serialize/Deserialize on public event types
- `tracing` — structured logging
- `ancillary` — platform control-message APIs for ECN/DSCP/kernel RX timestamps

**Dependencies:**
- `irtt-proto`, `thiserror`, `socket2`, `flume`, `fastrand`
- `serde`, `tracing` (optional)
- `rustix` or similar (optional, for ancillary)

### `irtt-stats`

**Public responsibility:**
- Consume `ClientEvent` values
- Cumulative statistics
- Rolling time-window statistics
- Rolling sample-count statistics
- Finite-run summaries
- RTT/IPDV/OWD/server-processing/loss statistics

**Forbidden responsibility:**
- Sockets, packet encoding/decoding
- Session negotiation
- Thread/runtime ownership
- CLI parsing

**Main public types:**
- `StatsCollector` (or `StatsEngine`)
- `CumulativeStats`
- `RollingStats`
- `FiniteSummary`
- `RttStats`, `IpdvStats`, `LossStats`
- `StatsConfig`

**Internal modules:**
- `running` — online mean/variance computation
- `rolling` — windowed statistics
- `summary` — finite-run summary builder
- `median` — median computation strategy

**Feature flags:**
- `serde` — derive Serialize/Deserialize on stats types

**Dependencies:**
- `irtt-client` (for `ClientEvent` types)
- `thiserror`
- `serde` (optional)

### `irtt-cli`

**Public responsibility:**
- User-facing executable
- Command-line argument parsing
- Output formatting (human, machine, simple, rtt-us, jsonl)
- Stats display (when feature-enabled)
- Signal handling (Ctrl-C)
- Exit codes

**Forbidden responsibility:**
- Protocol reimplementation
- Session logic reimplementation
- Statistics reimplementation

**Main public types:**
- (binary crate — minimal public API)
- Internal: `CliConfig`, `OutputFormatter`, `MachineFormatter`, `SimpleFormatter`, `RttUsFormatter`, `HumanFormatter`

**Internal modules:**
- `args` — clap argument parsing
- `format` — output formatting
- `run` — test execution orchestration
- `signal` — Ctrl-C handling

**Feature flags:**
- `default = ["stats"]`
- `stats = ["dep:irtt-stats"]`
- `json = ["dep:serde_json", "irtt-client/serde", "irtt-stats?/serde"]`
- `tracing = ["irtt-client/tracing"]`
- `ancillary = ["irtt-client/ancillary"]`
- `interop = []`

**Dependencies:**
- `irtt-client`, `irtt-stats` (optional), `clap`, `thiserror`, `ctrlc`
- `serde_json` (optional)

---

## 4. Protocol Implementation Plan

### Constants

```
MAGIC: [0x14, 0xA7, 0x5B]
PROTOCOL_VERSION: 1
FLAG_OPEN:  0x01
FLAG_REPLY: 0x02
FLAG_CLOSE: 0x04
FLAG_HMAC:  0x08
RESERVED_FLAGS_MASK: 0xF0
HMAC_SIZE: 16
TOKEN_SIZE: 8
SEQ_SIZE: 4
RECV_COUNT_SIZE: 4
RECV_WINDOW_SIZE: 8
TIMESTAMP_SIZE: 8
```

### Flags

Implement `Flags` as a newtype over `u8` with named bit accessors:
- `is_open()`, `is_reply()`, `is_close()`, `is_hmac()`
- `has_reserved_bits()` — returns true if bits 4–7 are set
- Builder pattern for constructing outgoing flags

### Field Layout

Implement `PacketLayout` that computes field offsets and total minimum size given:
- Whether HMAC is present
- Whether this is an open request (no token), echo (token + seq + optional fields), or close (token only)
- `ReceivedStats` mode
- `StampAt` mode
- `Clock` mode

The layout must match the field ordering verified in BLACKBOX_VERIFICATION_REPORT Section 19.11:
1. HMAC (if flag set)
2. Connection Token (except open request from client)
3. Sequence Number (echo only)
4. Received Count (if count or both)
5. Received Window (if window or both)
6. Receive Wall (if receive or both, and wall or both clock)
7. Receive Mono (if receive or both, and mono or both clock)
8. Midpoint Wall (if midpoint, and wall or both clock) — mutually exclusive with recv/send
9. Midpoint Mono (if midpoint, and mono or both clock)
10. Send Wall (if send or both, and wall or both clock)
11. Send Mono (if send or both, and mono or both clock)

### Endianness

All multi-byte integers are little-endian. Use `u32::from_le_bytes()`, `u64::from_le_bytes()`, `i64::from_le_bytes()` for reading. Use `.to_le_bytes()` for writing.

### Varint / Zigzag

Implement two functions:
- `encode_uvarint(value: u64, buf: &mut [u8]) -> usize` — standard unsigned LEB128
- `decode_uvarint(buf: &[u8]) -> Result<(u64, usize), ProtoError>` — returns (value, bytes_consumed)

And for signed:
- `encode_varint(value: i64, buf: &mut [u8]) -> usize` — zigzag then LEB128
- `decode_varint(buf: &[u8]) -> Result<(i64, usize), ProtoError>` — LEB128 then unzigzag

Zigzag transform:
- Encode: `((v << 1) ^ (v >> 63)) as u64`
- Decode: `((uv >> 1) as i64) ^ -((uv & 1) as i64)`

### Parameter Encoding/Decoding

Parameters are tag-value pairs:
- Tag: uvarint
- Value: varint (signed zigzag), except for string-typed values
- String values: uvarint length prefix + raw UTF-8 bytes

Define `Params` struct:
```
protocol_version: i64
duration_ns: i64
interval_ns: i64
length: i64
received_stats: ReceivedStats
stamp_at: StampAt
clock: Clock
dscp: i64
server_fill: Option<String>
```

Serialize: iterate fields, skip zero values (except protocol_version which is always 1), write tag + value.

Deserialize: read tag-value pairs in a loop, match known tags, silently ignore unknown tags.

Verified encoding examples from BLACKBOX_VERIFICATION_REPORT:
- ProtocolVersion=1: tag `01`, value zigzag(1)=2 → `02`
- Duration=3s: tag `02`, value zigzag(3000000000)=6000000000 → `80 f8 82 ad 16`
- DSCP=0xb8: tag `08`, value zigzag(184)=368 → `f0 02`

### Open Request/Reply

**Open Request encoding:**
1. Write magic (3 bytes)
2. Write flags (1 byte): Open | optional HMAC | optional Close (no-test)
3. If HMAC: write 16 zero bytes (placeholder)
4. No connection token
5. Write serialized params as payload
6. If HMAC: compute HMAC-MD5 over entire buffer with HMAC field zeroed, write result into HMAC field

**Open Reply decoding:**
1. Validate magic
2. Read flags — must have Open | Reply. Check for Close flag and reserved bits.
3. If HMAC flag: extract HMAC, verify
4. Read connection token (8 bytes)
5. Parse params from remaining payload
6. Determine outcome: normal open, no-test ack, or rejection

### Echo Request/Reply

**Echo Request encoding:**
1. Write magic + flags (0x00 or 0x08 for HMAC)
2. If HMAC: write 16 zero bytes
3. Write connection token (8 bytes)
4. Write sequence number (4 bytes LE)
5. Write zeroed received stats fields (if negotiated)
6. Write zeroed timestamp fields (if negotiated)
7. Write payload to reach negotiated length
8. If HMAC: compute and write

**Echo Reply decoding:**
1. Validate magic
2. Read flags — must have Reply. Check HMAC, Close, reserved bits.
3. If HMAC: extract and verify
4. Read connection token
5. Read sequence number
6. Read received count (if negotiated)
7. Read received window (if negotiated)
8. Read timestamp fields (per StampAt + Clock)
9. Remaining bytes are payload (ignore for measurement purposes)

### Close Request

**Encoding:**
1. Write magic + flags (Close | optional HMAC)
2. If HMAC: write 16 zero bytes
3. Write connection token
4. If HMAC: compute and write

Total: 12 bytes without HMAC, 28 bytes with HMAC.

### HMAC-MD5 Handling

1. Use `hmac` and `md-5` crates
2. Compute over entire packet buffer with HMAC field (bytes 4–19) zeroed
3. Use `subtle::ConstantTimeEq` for verification
4. HMAC key is raw bytes (from user-provided string, typically UTF-8)

### Packet Vector Tests

Implement tests for:
1. Test vector 18.1 (echo request with HMAC, 92 bytes) — verify HMAC, field layout, field values
2. Test vector 18.2 (echo reply with HMAC, 92 bytes) — verify HMAC, field layout, field values
3. Varint encoding/decoding round-trips for all verified values from BLACKBOX_VERIFICATION_REPORT
4. Parameter encoding/decoding round-trips
5. Layout calculation for all 6 verified field combinations from Section 19.11
6. Flags construction and parsing
7. Open request encoding (verify no token field present)
8. Close request encoding (verify 12/28 byte sizes)

---

## 5. Client Manual API Plan

### `ClientConfig`

```
server_addr: String          // hostname:port or IP:port
duration: Option<Duration>   // None for continuous
interval: Duration           // default 1s
length: u32                  // 0 = minimum
received_stats: ReceivedStats // default Both
stamp_at: StampAt            // default Both
clock: Clock                 // default Both
dscp: u8                     // default 0
hmac_key: Option<Vec<u8>>
server_fill: Option<String>
open_timeouts: Vec<Duration> // default [1s, 2s, 4s, 8s]
run_mode: RunMode
negotiation_policy: NegotiationPolicy
validation_policy: ValidationPolicy
loss_detection: LossDetection
timer_mode: TimerMode
socket_config: SocketConfig
```

### `SocketConfig`

```
bind_addr: Option<SocketAddr>
ttl: Option<u32>
ipv4_only: bool
ipv6_only: bool
recv_timeout: Option<Duration>  // for blocking recv in manual mode
```

### `ClientSocket`

Internal struct wrapping `std::net::UdpSocket` (created via `socket2::Socket`).

Responsibilities:
- Create socket (IPv4 or IPv6 based on resolved address)
- Bind to local address (or ephemeral)
- Set DSCP/TOS via `socket2` (`set_tos` for IPv4, `set_traffic_class_v6` for IPv6) — applied only for echo phase per Finding C
- Set TTL/hop limit if configured
- Connect to resolved server address
- `send(&self, buf: &[u8]) -> io::Result<usize>`
- `recv(&self, buf: &mut [u8]) -> io::Result<usize>`
- `set_read_timeout(&self, timeout: Option<Duration>)`
- `try_clone(&self) -> io::Result<ClientSocket>` — for managed mode

MVP `PacketMeta` returns all `None` fields. Future `ancillary` feature fills them.

### `SessionState`

Internal struct tracking:
- `connection_token: u64`
- `negotiated_params: NegotiatedParams`
- `next_seq: u32` (wire sequence)
- `logical_seq: u64`
- `packets_sent: u64`
- `replies_received: u64`
- `duplicates: u64`
- `late_count: u64`
- `malformed: u64`
- `max_received_seq: Option<u32>` (for late detection)
- `pending_probes: BoundedPendingMap` (seq → PendingProbe)
- `start_time: Option<Instant>`
- `end_time: Option<Instant>` (computed from start + duration)
- `phase: SessionPhase` (Opening, Active, Draining, Closing, Completed)

### `PendingProbe`

```
logical_seq: u64
wire_seq: u32
sent_at_mono: Instant
sent_at_wall: Option<SystemTime>
timeout_at: Instant
```

### `BoundedPendingMap`

A bounded map from `u32` (wire seq) to `PendingProbe`. Capacity limited to prevent unbounded growth in continuous mode. Oldest entries evicted when capacity reached (these become loss events).

### `Client::connect`

1. Resolve server address (DNS lookup if hostname)
2. Create `ClientSocket` (socket2 → std)
3. Bind local address
4. Connect UDP socket
5. Return `Client` in `Opening` phase

### `Client::open`

Returns `Result<OpenOutcome, ClientError>` (not `Vec<ClientEvent>`).

```rust
pub enum OpenOutcome {
    Started {
        session_id: SessionId,
        remote: SocketAddr,
        negotiated: NegotiatedParams,
        event: ClientEvent,
    },
    NoTestCompleted {
        remote: SocketAddr,
        event: ClientEvent,
    },
}
```

Steps:
1. Encode open request with proposed params
2. Send open request
3. Set read timeout to first timeout value
4. Loop: recv → validate magic/flags/HMAC → parse open reply
5. On timeout: retransmit with next timeout value
6. On all timeouts exhausted: return `ClientError::OpenTimeout`
7. On valid reply:
   - If Close flag and client didn't request close: return `ClientError::ServerRejected`
   - If Close flag and client requested close (no-test): return `OpenOutcome::NoTestCompleted`
   - Extract connection token, negotiated params
   - Apply negotiation policy (reject or accept restrictions)
   - Compute minimum packet length from negotiated params
   - Transition to Active phase
   - Set DSCP on socket (only now, for echo phase)
   - Return `OpenOutcome::Started`

### `Client::send_probe`

1. Assert Active phase
2. Check if duration exceeded (if finite mode) → if so, transition to Draining
3. Encode echo request with current sequence number, connection token
4. Capture send timestamp (Instant + SystemTime) immediately before send call
5. Call `socket.send()`
6. Insert PendingProbe into pending map
7. Increment sequence counters
8. Return any events (e.g., eviction-triggered loss events from bounded map)

### `Client::recv_once`

1. Attempt one `socket.recv()` (may block up to read timeout, or nonblocking)
2. Capture receive timestamp immediately after recv returns
3. Validate packet (magic, flags, reserved bits, HMAC, connection token, sequence)
4. Match to pending probe
5. If duplicate: emit `DuplicateReply`, do not update original
6. If late (seq < max_received_seq): emit `LateReply`
7. Compute `RttSample` (raw, server_processing, adjusted)
8. Compute `OneWayDelaySample` if wall timestamps available
9. Extract `ReceivedStatsSample` if negotiated
10. Emit `EchoReply` event
11. Return `Ok(events)` — empty Vec on timeout/would-block

### `Client::recv_available`

Loop calling `recv_once` up to a budget limit (max packets or max duration). Collect all events.

### `Client::poll_timeouts`

1. Check all pending probes against loss detection policy
2. For probes exceeding timeout: emit `EchoLoss` event, remove from pending map
3. Use received window from most recent reply to classify loss direction where possible
4. Return collected events

### `Client::close`

1. Reset DSCP to 0 on socket (per Finding C)
2. Encode and send close request
3. Transition to Completed phase
4. Return `SessionEnded` event with outcome

### No-Test Mode

`RunMode::NoTest`: `Client::open` sets both Open and Close flags. On valid reply with Close flag and zero token, immediately returns `SessionEnded`. No echo phase.

### Continuous Mode

`RunMode::Continuous`: No duration limit. `send_probe` never transitions to Draining on its own. Caller is responsible for stopping (by calling `close` or dropping). Bounded pending map prevents unbounded memory growth.

### Finite Duration/Count Modes

`RunMode::Duration(d)`: `send_probe` checks `Instant::now() >= start + d` before sending. `send_probe` returns a "test complete" indication when done.

`RunMode::Count(n)`: `send_probe` checks `logical_seq >= n` before sending.

### Negotiation Policy

- `Strict` (default): reject if any param was changed by server
- `Loose`: accept server-returned params, emit warnings if changed

### Validation Policy

- `Strict`: abort session on any malformed packet
- `DropMalformed`: silently drop malformed packets, continue
- `DropUnrelatedAbortMalformedSession`: drop unrelated garbage, abort on malformed session packets

### Loss Detection Policy

- `Disabled`: never emit loss events from timeout
- `Timeout(Duration)`: emit loss if no reply within fixed duration
- `MultipleOfInterval(u32)`: emit loss if no reply within N × interval

### Cancellation for Manual Mode

No signal handling in `Client`. Caller controls lifecycle by:
- Not calling `send_probe` after deciding to stop
- Calling `close` for graceful shutdown
- Dropping `Client` for ungraceful shutdown

---

## 6. Managed Runner Plan

### Overview

`ManagedClient` wraps the same socket and session primitives as `Client`, but runs them on dedicated threads with event fanout.

### Thread Roles

**Coordinator thread:**
- Owns session state (equivalent of `SessionState`)
- Receives compact `SentRecord` from sender via internal `flume` channel
- Receives compact `RecvRecord` from receiver via internal `flume` channel
- Correlates sent/received records
- Computes RTT samples
- Manages pending probe map
- Handles loss timeouts
- Detects duplicates and late replies
- Emits `ClientEvent` through `EventHub`
- Handles graceful shutdown and close packet

**Sender/pacer thread:**
- Owns a cloned socket handle (write half)
- Maintains absolute send schedule: `scheduled_time(seq) = session_start + seq * interval`
- Prepares echo request packets
- Sends packets at scheduled times
- Captures send timestamps immediately before send call
- Sends compact `SentRecord { wire_seq, logical_seq, sent_at_mono, sent_at_wall }` to coordinator
- Must NOT perform: event fanout, stats, subscriber delivery, blocking coordination

**Receiver thread:**
- Owns a cloned socket handle (read half)
- Blocks on `socket.recv()`
- Captures receive timestamp immediately after recv returns, before parsing
- Parses/validates reply minimally
- Sends compact `RecvRecord { wire_seq, received_at_mono, received_at_wall, server_timestamps, received_stats, packet_meta, raw_packet_info }` to coordinator
- Must NOT perform: RTT computation, event fanout, stats

### Internal Channels

- Sender → Coordinator: `flume::bounded<SentRecord>(capacity)` — capacity sized to avoid dropped sends (e.g., 256)
- Receiver → Coordinator: `flume::bounded<RecvRecord>(capacity)` — similar
- Control → All: `CancellationToken` (Arc<AtomicBool>) checked by all threads

Internal channels must never silently drop records. If sender or receiver channel is full, this indicates coordinator is stuck — treat as fatal.

### Event Hub / Fanout

```
EventHub {
    subscribers: Vec<SubscriberSlot>
}

SubscriberSlot {
    sender: flume::Sender<ClientEvent>
    config: SubscriberConfig
    drops: AtomicU64
}
```

When coordinator emits an event:
1. Clone event for each subscriber
2. Attempt send according to subscriber's overflow policy:
   - `Block`: blocking send (only if subscriber is trusted to keep up)
   - `DropNewest`: try_send, increment drops on failure
   - `Disconnect`: try_send, on failure remove subscriber

`DropOldest` is deferred from MVP. It is useful for SQM/live-control freshness, but `flume` does not provide it natively and it is not worth blocking the managed runner. A future implementation may use a drain-one-and-retry workaround or a ring-buffer channel.

**Critical invariant:** Public subscriber backpressure must NOT delay the sender/pacer thread. The sender never touches the event hub. Only the coordinator touches the event hub, and the coordinator is not on the timing-critical path.

### Subscriber Configuration

```
SubscriberConfig {
    capacity: usize          // channel buffer size
    overflow: SubscriberOverflow
    event_filter: EventFilter // optional: subscribe only to specific event types
}
```

Preset profiles:
- `SubscriberConfig::stats()`: Block, large capacity, all events
- `SubscriberConfig::live()`: DropNewest, small capacity (e.g., 4), EchoReply/EchoLoss only (future: DropOldest when available)
- `SubscriberConfig::cli()`: DropNewest, moderate capacity (e.g., 1024), all events
- `SubscriberConfig::debug()`: DropNewest, small capacity, all events

### Shutdown / Stop / Join

1. `session.stop()`: sets `CancellationToken`, non-blocking
2. Sender thread: detects cancellation, exits loop
3. Receiver thread: detects cancellation (via recv timeout + check), exits
4. Coordinator: detects cancellation, drains remaining records, sends close packet, emits `SessionEnded`, exits
5. `session.join(self) -> Result<SessionOutcome, ClientError>`: joins all threads, returns outcome

### Thread Lifecycle

1. `ManagedClient::start(config)`:
   - Create socket, perform open handshake (on calling thread)
   - On success: clone socket handles
   - Spawn sender, receiver, coordinator threads
   - Return `ManagedClientSession` (owns join handles + event hub + cancellation token)
2. Subscribers can be added before or after start (before start is preferred; adding after requires the hub to be shared)
3. On thread panic: coordinator detects via channel disconnect, emits FatalError, shuts down remaining threads

### Timer Strategy

Sender uses absolute scheduling. For `TimerMode::Normal`:
- Compute `wake_time = session_start + seq * interval`
- Sleep until `wake_time` using `std::thread::sleep` or `std::thread::park_timeout`
- After waking, immediately send

For `TimerMode::Precise { spin_threshold }`:
- Sleep until `wake_time - spin_threshold`
- Busy-spin for remaining time
- Only for users who explicitly opt in (not default, not router-safe)

### Reuse of Manual/Session Primitives

The managed runner reuses:
- `irtt-proto` for all encoding/decoding
- `ClientSocket` for socket operations
- `PacketLayout` for buffer sizing
- Validation logic from the session module
- RTT/timestamp computation logic

It does NOT reuse `Client` directly (the managed runner has different ownership semantics), but the logic is shared at the module level.

---

## 7. Event Model Plan

### `ClientEvent`

Do **not** define or emit `EchoSent` in MVP. It is debug/instrumentation noise that can be added later behind a tracing or debug event option.

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
        error: ClientError,
    },
}
```

**Change from initial design:** `FatalError` uses `ClientError` directly instead of a separate `ClientErrorKind` enum.

### `RttSample`

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

**Rules:**
- `raw = client_receive_mono - client_send_mono`
- `server_processing = server_send_mono - server_receive_mono` (prefer mono, fallback wall)
- `adjusted = raw - server_processing` ONLY IF `server_processing <= raw`
- If `server_processing > raw`: `adjusted = None`, emit `ClientWarning::ServerProcessingExceedsRawRtt`

### `ClientTimestamp`

```rust
pub struct ClientTimestamp {
    pub mono: Instant,
    pub wall: SystemTime,
}
```

Both captured together, as close to the syscall as possible.

### `PacketMeta`

```rust
pub struct PacketMeta {
    pub traffic_class: Option<u8>,
    pub dscp: Option<Dscp>,
    pub ecn: Option<Ecn>,
    pub kernel_rx_timestamp: Option<SystemTime>,
}
```

All `None` in MVP. Populated by `ancillary` feature.

### `ReceivedStatsSample`

```rust
pub struct ReceivedStatsSample {
    pub count: Option<u32>,
    pub window: Option<u64>,
}
```

### `ServerTiming`

```rust
pub struct ServerTiming {
    pub receive_wall: Option<i64>,   // ns since epoch
    pub receive_mono: Option<i64>,   // ns duration
    pub send_wall: Option<i64>,
    pub send_mono: Option<i64>,
    pub midpoint_wall: Option<i64>,
    pub midpoint_mono: Option<i64>,
}
```

### `OneWayDelaySample`

```rust
pub struct OneWayDelaySample {
    pub send_delay: Duration,    // server_best_receive_wall - client_send_wall
    pub receive_delay: Duration, // client_receive_wall - server_best_send_wall
}
```

Only computable with wall clock timestamps and assumes synchronized clocks.

### `LossKind`

```rust
pub enum LossKind {
    Unknown,
    Upstream,
    Downstream,
}
```

Classified using received window when available.

### `ClientWarning`

```rust
pub enum ClientWarning {
    ServerProcessingExceedsRawRtt {
        seq: u64,
        raw: Duration,
        server_processing: Duration,
    },
    SubscriberDroppedEvents {
        subscriber_id: SubscriberId,
        count: u64,
    },
    ServerRestrictedParams {
        original: Params,
        negotiated: Params,
    },
}
```

### `SessionOutcome`

```rust
pub struct SessionOutcome {
    pub end_reason: SessionEndReason,
    pub packets_sent: u64,
    pub replies_received: u64,
    pub duplicates: u64,
    pub late: u64,
    pub malformed: u64,
}
```

### `SessionEndReason`

```rust
pub enum SessionEndReason {
    TestComplete,
    Cancelled,
    ServerClosed,
    Error(ClientError),
}
```

### Sequence Number Modeling

```rust
pub struct ProbeSeq {
    pub logical: u64,
    pub wire: u32,
}
```

Wire sequence is the 32-bit value on the wire. Logical sequence is a monotonic u64 that survives wire wraps in very long continuous sessions. For practical purposes in protocol v1, `wire = (logical as u32)`.

---

## 8. Socket and Networking Plan

### socket2 Usage

Use `socket2::Socket` for creation and configuration:
1. Create `Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))`
2. Domain: `Domain::IPV4` or `Domain::IPV6` based on resolved address
3. Bind: `socket.bind(&local_addr.into())`
4. Configure options (TOS/TC, TTL, etc.)
5. Connect: `socket.connect(&server_addr.into())`
6. Convert: `std::net::UdpSocket::from(socket)`

### Bind/Connect Flow

1. Resolve server address (may involve DNS)
2. Determine IP version from resolved address
3. Create socket2 socket for that IP version
4. Bind to configured local address or `0.0.0.0:0` / `[::]:0`
5. Set socket options
6. Connect to server address
7. Convert to std `UdpSocket`

### IPv4/IPv6 Handling

- If user specifies `-4` or `-6`, use only that version
- If user specifies an IP literal, use matching version
- If user specifies a hostname, resolve and prefer based on what succeeds (implementation-defined preference)
- Do NOT create dual-stack sockets; create one socket for one IP version per session

### Connected UDP Semantics

The socket is connected via `connect()`. This means:
- `send()` instead of `send_to()` — no per-packet address overhead
- `recv()` instead of `recv_from()` — kernel filters by connected address
- ICMP errors may be delivered (port unreachable on localhost)

### DSCP / Traffic Class Setting

- IPv4: `socket.set_tos(dscp_byte)` via socket2
- IPv6: `socket2` does not directly expose `IPV6_TCLASS`; may need raw `setsockopt` via `libc` or `socket2`'s `set_traffic_class_v6` (check availability)
- Applied only before echo phase (after open, before first send_probe)
- Reset to 0 before close packet (per Finding C)
- Set from the negotiated DSCP value, which is the full TOS/TC byte (0–255)

### TTL / Hop Limit

Optional. If configured:
- IPv4: `socket.set_ttl(ttl)` via socket2
- IPv6: `socket.set_unicast_hops_v6(hops)` via socket2

### Socket Cloning for Managed Mode

`socket.try_clone()` returns a new handle to the same underlying socket. Sender and receiver threads each get a clone. This is safe for connected UDP: one thread sends, one thread receives.

### Nonblocking / Read-Timeout Behavior

- Manual mode: caller sets read timeout via `Client` API or uses nonblocking mode
- Managed mode receiver: uses blocking recv with a moderate timeout (e.g., 100ms) to periodically check cancellation token
- Managed mode sender: uses `thread::sleep` / `thread::park_timeout` for timing

### Future Ancillary Feature

Feature-gated (`ancillary`):
- Use `recvmsg` syscall to receive control messages
- Extract `IP_TOS` / `IPV6_TCLASS` from ancillary data → populate `PacketMeta`
- Extract `SO_TIMESTAMPNS` / `SCM_TIMESTAMPNS` → populate kernel RX timestamp
- Platform-specific: Linux via `rustix` or `libc`, macOS may differ
- NOT MVP — defer entirely

### MVP vs Future

| Feature | MVP | Future |
|---------|-----|--------|
| socket2 create/bind/connect | Yes | - |
| IPv4 TOS setting | Yes | - |
| IPv6 Traffic Class setting | Yes (best effort) | - |
| Connected UDP send/recv | Yes | - |
| Socket cloning | Yes | - |
| Read timeout | Yes | - |
| Incoming TC/DSCP/ECN | No (all None) | ancillary feature |
| Kernel RX timestamp | No (None) | ancillary feature |
| DF bit | No | Maybe |

---

## 9. Statistics Plan

### Architecture

`StatsCollector` consumes `ClientEvent` values and maintains running statistics. It does not own threads or channels — it is a passive processor.

```rust
pub struct StatsCollector {
    config: StatsConfig,
    cumulative: CumulativeStats,
    rolling_count: Option<RollingCountWindow>,
    rolling_time: Option<RollingTimeWindow>,
}

impl StatsCollector {
    pub fn new(config: StatsConfig) -> Self;
    pub fn process(&mut self, event: &ClientEvent);
    pub fn cumulative(&self) -> &CumulativeStats;
    pub fn rolling_count(&self) -> Option<&RollingCountWindow>;
    pub fn rolling_time(&self) -> Option<&RollingTimeWindow>;
    pub fn summary(&self) -> FiniteSummary;
}
```

### Cumulative Stats

Maintained incrementally as events arrive:
- `packets_sent: u64`
- `replies_received: u64`
- `duplicates: u64`
- `late: u64`
- `lost: u64`
- `rtt_stats: RunningStats` (min/max/mean/variance online)
- `raw_rtt_stats: RunningStats`
- `adjusted_rtt_stats: RunningStats` (only from samples where adjusted is Some)
- `ipdv_stats: RunningStats`
- `server_processing_stats: RunningStats`
- `send_delay_stats: Option<RunningStats>`
- `receive_delay_stats: Option<RunningStats>`
- `upstream_lost: u64`
- `downstream_lost: u64`

### Rolling Count Window

A fixed-size ring buffer of the last N samples. Recomputes stats over the window on demand.

### Rolling Time Window

A time-bounded deque of samples. Evicts samples older than the window duration. Recomputes stats over remaining samples.

### Finite Summary

Built from cumulative stats at session end:
- All cumulative fields
- Median RTT (requires storing all RTT values — only for finite mode)
- Standard deviation / variance (from online algorithm)

### RTT Stats Breakdown

- **Effective RTT stats:** uses `rtt.effective()` for each sample
- **Raw RTT stats:** uses `rtt.raw`
- **Adjusted RTT stats:** uses `rtt.adjusted` when Some; skip when None

### IPDV Stats

IPDV computed as `rtt_effective[N] - rtt_effective[N-1]` for successive received replies. Only computable when both N and N-1 have replies.

### Server Processing Stats

From `rtt.server_processing` when Some.

### Loss/Duplicate/Late Stats

Simple counters. Loss percentage = `lost / (sent)`. Direction breakdown using `LossKind`.

### Median / Stddev / Variance Strategy

- **Online mean/variance:** Welford's algorithm (standard approach, no attribution needed)
- **Median:** Requires all values. For finite mode: store all effective RTT values in a Vec, sort at summary time. For continuous/rolling mode: median is either not available or approximate.
- **Stddev:** `sqrt(variance)` from online computation

### Handling Edge Cases

- **Missing adjusted RTT:** Skip in adjusted stats; effective falls back to raw, so effective stats always have data.
- **Server processing > raw RTT:** Warning already emitted by client. Stats treats adjusted as None for that sample. Effective = raw.
- **Loss direction unknown:** Counted in total loss and unknown-direction loss.
- **Duplicate replies:** Counted but not included in RTT stats (per spec: duplicates MUST NOT update original data).
- **Late replies:** Counted. `LateReply` event may include RTT if it can be computed — if so, include in late-RTT stats but NOT in primary RTT stats.
- **Events missing due to subscriber drops:** Stats will be inaccurate. `StatsCollector` should track a `missed_events` counter if the subscriber reports drops. Stats output should note that results are approximate.

---

## 10. CLI Plan

### Command Structure

```
irtt-cli [OPTIONS] <server>
irtt-cli --no-test <server>
irtt-cli --continuous <server>
```

Single binary, single command (no subcommands in MVP). Options control mode and output.

### Run Modes

- **Finite test (default):** `-d 60s -i 1s` — run for duration, print summary
- **No-test / check:** `-n` or `--no-test` — open, negotiate, close immediately
- **Continuous:** `--continuous` or `-C` — run until Ctrl-C
- **Count:** `-c 100` — send exactly N probes

### Output Formats

- **human** (`--format=human`): Human-readable per-line output + summary
- **machine** (`--format=machine`): Tab-delimited, all fields, stable format
- **simple** (`--format=simple`): `timestamp\tseq\tresult`
- **rtt-us** (`--format=rtt-us`): One value per line: RTT in microseconds or "loss"
- **jsonl** (`--format=jsonl`, requires `json` feature): One JSON object per line

Default: `human` for finite tests, `machine` for continuous

### Machine Format Examples

```
reply	1714320000.123456	1.2.3.4:2112	1	42	8341	8420	79	-	-	-	ok
loss	1714320000.323456	1.2.3.4:2112	1	43	unknown
duplicate	1714320000.423456	1.2.3.4:2112	1	42
late	1714320000.523456	1.2.3.4:2112	1	40	8500
```

### Simple Format Examples

```
1714320000.123456	42	8341
1714320000.323456	43	loss
```

### RTT-US Format Examples

```
8341
loss
8120
```

### Awk-Friendly Usage

```bash
# Extract RTT values only (rtt-us format)
irtt-cli --format=rtt-us server | grep -v loss

# Average RTT from machine format
irtt-cli --format=machine -d 10s server | awk -F'\t' '/^reply/ { sum+=$6; n++ } END { print sum/n }'

# Filter high-latency replies
irtt-cli --format=simple server | awk -F'\t' '$3 > 10000 { print }'
```

### Delimiter Support

`--delimiter=<char>`: tab (default), comma, semicolon, pipe, space

### Timestamp Support

`--timestamp-format=<fmt>`: `unix` (default: `1714320000.123456`), `iso8601`, `none`

### Header Support

`--header`: Print column header as first line (off by default)

### Stats Integration

Default build (`stats` feature):
- Finite test: print per-probe lines during test, print summary at end
- Continuous: print per-probe lines, optional periodic summary lines

Minimal build (no `stats` feature):
- Per-probe lines only
- No summary
- Smaller binary

### Ctrl-C Handling

Use `ctrlc` crate:
1. Register handler that sets a shared `AtomicBool`
2. On Ctrl-C: call `session.stop()`
3. Wait for `session.join()`
4. Print partial summary if stats available
5. Exit with appropriate code

### Exit Codes

- 0: Success
- 1: Error (open timeout, server rejection, etc.)
- 2: Partial results (cancelled mid-test)

---

## 11. Feature Flags

### `irtt-proto`

| Feature | Default | MVP | Description |
|---------|---------|-----|-------------|
| `serde` | No | Deferred | Serialize/Deserialize on protocol types |

### `irtt-client`

| Feature | Default | MVP | Description |
|---------|---------|-----|-------------|
| `serde` | No | Deferred | Serialize/Deserialize on event types |
| `tracing` | No | Deferred | Structured logging |
| `ancillary` | No | Deferred | Platform control-message APIs |

### `irtt-stats`

| Feature | Default | MVP | Description |
|---------|---------|-----|-------------|
| `serde` | No | Deferred | Serialize/Deserialize on stats types |

### `irtt-cli`

| Feature | Default | MVP | Description |
|---------|---------|-----|-------------|
| `stats` | Yes | Milestone 7–8 | Include irtt-stats |
| `json` | No | Deferred | JSONL output |
| `tracing` | No | Deferred | Pass-through to irtt-client |
| `ancillary` | No | Deferred | Pass-through to irtt-client |
| `interop` | No | MVP | Enable interop test compilation |

### MVP Features

Milestone 1–5: No optional features needed. Core functionality only.
Milestone 6–8: `stats` feature implemented and default-enabled.
Post-MVP: `serde`, `json`, `tracing`, `ancillary`, `interop`.

`interop` is special: it gates `#[cfg(feature = "interop")]` test modules but adds no runtime code.

---

## 12. Error Model

### `irtt-proto::ProtoError`

```rust
#[derive(Debug, thiserror::Error)]
pub enum ProtoError {
    #[error("invalid magic bytes")]
    InvalidMagic,
    #[error("reserved flag bits set: {0:#04x}")]
    ReservedFlagBits(u8),
    #[error("buffer too short: need {need}, have {have}")]
    BufferTooShort { need: usize, have: usize },
    #[error("varint overflow")]
    VarintOverflow,
    #[error("varint truncated")]
    VarintTruncated,
    #[error("unknown received stats value: {0}")]
    UnknownReceivedStats(i64),
    #[error("unknown stamp_at value: {0}")]
    UnknownStampAt(i64),
    #[error("unknown clock value: {0}")]
    UnknownClock(i64),
    #[error("invalid HMAC")]
    InvalidHmac,
    #[error("midpoint and receive/send timestamps mutually exclusive")]
    TimestampConflict,
    #[error("server fill too long: {0} bytes")]
    ServerFillTooLong(usize),
    #[error("invalid parameter tag: {0}")]
    InvalidParamTag(u64),
}
```

### `irtt-client::ClientError`

```rust
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    // Connection errors
    #[error("DNS resolution failed: {0}")]
    ResolutionFailed(String),
    #[error("socket creation failed: {0}")]
    SocketCreation(#[source] io::Error),
    #[error("socket bind failed: {0}")]
    SocketBind(#[source] io::Error),
    #[error("socket connect failed: {0}")]
    SocketConnect(#[source] io::Error),
    #[error("DSCP/TOS not supported: {0}")]
    DscpNotSupported(#[source] io::Error),

    // Open phase errors
    #[error("open timeout: no reply after {attempts} attempts")]
    OpenTimeout { attempts: usize },
    #[error("server rejected session")]
    ServerRejected,
    #[error("protocol version mismatch: expected {expected}, got {got}")]
    ProtocolVersionMismatch { expected: i64, got: i64 },
    #[error("server restricted parameters")]
    ServerRestriction { original: Params, negotiated: Params },

    // Protocol errors
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtoError),

    // Runtime errors
    #[error("send failed: {0}")]
    SendFailed(#[source] io::Error),
    #[error("receive failed: {0}")]
    RecvFailed(#[source] io::Error),
    #[error("malformed packet from active session")]
    MalformedSessionPacket,
    #[error("unexpected sequence number: {0}")]
    UnexpectedSequence(u32),
    #[error("short reply: expected {expected}, got {got}")]
    ShortReply { expected: usize, got: usize },
    #[error("stamp_at mismatch in reply")]
    StampAtMismatch,
    #[error("clock mismatch in reply")]
    ClockMismatch,

    // Thread errors (managed mode)
    #[error("worker thread panicked")]
    ThreadPanic,
    #[error("internal channel disconnected")]
    ChannelDisconnected,

    // Partial outcome
    #[error("session ended with error: {0}")]
    SessionError(Box<ClientError>),
}
```

`ClientError` is used directly in `ClientEvent::FatalError` and `SessionEndReason::Error`. No separate `ClientErrorKind` is needed — `ClientError` should derive `Clone` where possible, or the event variants should use `Arc<ClientError>` for cheap cloning if `io::Error` prevents `Clone`.

---

## 13. Testing Plan

### Pure Protocol Unit Tests (`irtt-proto`)

- Varint encode/decode: round-trip for 0, 1, -1, 127, 128, -128, i64::MAX, i64::MIN, all verified values from black-box report
- Uvarint encode/decode: round-trip for 0, 1, 127, 128, 16383, u64::MAX
- Parameter encoding: encode known params, compare bytes to black-box captures
- Parameter decoding: decode known bytes, compare to expected params
- Parameter round-trip: encode → decode → compare
- Flags construction and parsing
- Layout calculation: verify minimum sizes for all 6 verified configurations (16, 28, 32, 44, 60, 76 bytes)
- Open request encoding: verify no token field, correct magic/flags
- Close request encoding: verify 12 bytes (no HMAC), 28 bytes (with HMAC)
- HMAC calculation: verify test vector 18.1 (echo request)
- HMAC verification: verify test vector 18.2 (echo reply)
- Field ordering: encode echo request with various field configs, verify byte offsets
- Zigzag edge cases: 0, -1, 1, i64::MAX, i64::MIN

### Packet Vector Tests

From `docs/test-vectors.md` (12 vectors) and protocol spec Section 18 (2 vectors):

- Vector 1: Open request (no HMAC, 24 bytes) — verify magic, flags, param encoding
- Vector 2: Open reply (no HMAC, 32 bytes) — verify token extraction, param decoding
- Vector 3: Echo request (no HMAC, 60 bytes) — verify field layout, zeroed placeholders
- Vector 4: Echo reply (no HMAC, 60 bytes) — verify all field extraction (seq, count, window, timestamps)
- Vector 5: Close request (no HMAC, 12 bytes) — verify minimal close format
- Vector 6: HMAC open request (40 bytes) — verify HMAC computation
- Vector 7: HMAC echo request (76 bytes) — verify HMAC with full header
- Vector 8: HMAC close request (28 bytes) — verify HMAC close format
- Vector 9: No-test open+close request (25 bytes) — verify Open|Close flags, param encoding
- Vector 10: No-test open+close reply (33 bytes) — verify zero token, Open|Reply|Close flags
- Vector 11: Minimal echo packet (16 bytes) — verify absolute minimum size
- Vector 12: Midpoint timestamp echo reply (44 bytes) — verify midpoint field layout
- Spec 18.1: Echo request with HMAC (92 bytes, pattern fill) — verify HMAC, full field layout
- Spec 18.2: Echo reply with HMAC (92 bytes, pattern fill) — verify HMAC, all field values

### Client State Tests (`irtt-client`)

- Session phase transitions: Opening → Active → Draining → Closing → Completed
- Pending probe map: insert, match reply, detect duplicate, detect late, eviction at capacity
- Loss detection: timeout-based loss, multiple-of-interval loss
- Sequence numbering: wire/logical mapping, wrap behavior
- Negotiation policy: accept restrictions, reject restrictions, require-within bounds
- RTT computation: raw, server_processing, adjusted, effective, warning on processing > raw
- Loss direction classification from received window
- Run mode: count completion, duration completion, continuous no-stop

### Socket Tests

- (Minimal: socket creation is hard to unit test without a real network stack)
- Verify DSCP setter works on available platform (integration-level)
- Verify socket clone produces working handle

### Managed Runner Tests

- Start/stop lifecycle
- Event delivery to subscriber
- Multiple subscribers receive all events
- Subscriber overflow policies (DropNewest, DropOldest)
- Cancellation token stops all threads
- Thread panic is detected and reported

### Stats Tests (`irtt-stats`)

- Process sequence of EchoReply events, verify cumulative min/max/mean
- Process with losses, verify loss counts and percentages
- Process with duplicates, verify duplicate counting (not included in RTT stats)
- Rolling count window: verify correct window behavior
- Rolling time window: verify eviction
- IPDV computation from successive replies
- Edge case: server_processing > raw (adjusted=None, effective=raw)
- Edge case: all packets lost (no RTT stats)
- Edge case: single packet (no IPDV)
- Median computation for finite summary

### CLI Formatter Tests

- Machine format: verify tab-delimited output for EchoReply, EchoLoss, DuplicateReply, LateReply
- Simple format: verify timestamp + seq + result
- RTT-us format: verify single value or "loss"
- Delimiter customization
- Timestamp formatting
- Header line generation
- Edge cases: missing fields (no server timestamps → "-")

### Interop Tests (feature = "interop")

Require `irtt` server binary in PATH. Gated behind `--features interop`.

- Basic connectivity: open, send probes, receive replies, close
- No-test mode: open+close with zero token
- Parameter negotiation (strict and loose)
- HMAC authentication (correct key)
- HMAC mismatch (expect timeout)
- DSCP setting
- All timestamp modes (none, send, receive, both, midpoint)
- All clock modes (wall, mono, both)
- All received stats modes (none, count, window, both)
- Large packet (1472 bytes)
- Server fill
- IPv6 (if available)
- Finite duration count verification (ceil(d/i))
- Continuous mode start/stop

### Black-Box Regression Tests

- Run interop tests and compare packet counts, RTT ranges, error behavior to known-good baseline
- Verify close packet is sent at end of session

### Tests Intentionally Deferred

- Ancillary metadata tests (requires ancillary feature)
- ECN-specific tests (requires server --ecn)
- Server close during test (hard to trigger)
- Kernel RX timestamp tests (requires ancillary feature)
- Tokio/async integration tests (not planned for MVP)
- Performance/benchmark tests (post-MVP)

### Test Execution Requirements

- `cargo test`: Must pass without `irtt` in PATH. Pure unit tests only.
- `cargo test --workspace --features interop`: Requires `irtt` in PATH. CI must provide it.
- `tshark`: Diagnostic/manual only. Never a CI or test dependency.

---

## 14. Milestone Plan

### Milestone 0 — Workspace Skeleton

**Goal:** Create the Cargo workspace with all four crates, license files, and documentation.

**Files/modules:**
- `Cargo.toml` (workspace)
- `crates/irtt-proto/Cargo.toml`, `crates/irtt-proto/src/lib.rs`
- `crates/irtt-client/Cargo.toml`, `crates/irtt-client/src/lib.rs`
- `crates/irtt-stats/Cargo.toml`, `crates/irtt-stats/src/lib.rs`
- `crates/irtt-cli/Cargo.toml`, `crates/irtt-cli/src/main.rs`
- `LICENSE-APACHE`, `LICENSE-MIT`

**Tasks:**
1. Create workspace `Cargo.toml` with members
2. Create each crate with minimal `Cargo.toml` and empty lib/main
3. Add license files
4. Add `.gitignore`
5. Verify `cargo check --workspace` passes
6. Verify `cargo test --workspace` passes (trivially)

**Tests:** `cargo check --workspace`

**Success criteria:** Workspace compiles. All four crates exist. No upstream source present.

**Risks:** None.

### Milestone 1 — `irtt-proto`

**Goal:** Complete wire protocol implementation with all packet types and passing test vectors.

**Files/modules:**
- `crates/irtt-proto/src/lib.rs`
- `crates/irtt-proto/src/varint.rs`
- `crates/irtt-proto/src/params.rs`
- `crates/irtt-proto/src/header.rs`
- `crates/irtt-proto/src/layout.rs`
- `crates/irtt-proto/src/hmac.rs`
- `crates/irtt-proto/src/error.rs`
- `crates/irtt-proto/src/open.rs`
- `crates/irtt-proto/src/echo.rs`
- `crates/irtt-proto/src/close.rs`

**Tasks:**
1. Implement varint/zigzag encode/decode
2. Implement parameter model and serialization
3. Implement flags and magic constants
4. Implement packet layout calculation
5. Implement open request encoding
6. Implement open reply decoding
7. Implement echo request encoding
8. Implement echo reply decoding
9. Implement close request encoding
10. Implement HMAC-MD5 calculation and verification
11. Write all unit tests
12. Verify test vectors 18.1 and 18.2

**Tests:**
- All varint round-trips
- All parameter encode/decode tests
- Layout calculations for all 6 verified configurations
- All 14 packet test vectors (12 from `docs/test-vectors.md` + 2 from spec Section 18)
- HMAC verification for vectors 6, 7, 8, spec 18.1, spec 18.2
- Edge cases (empty params, unknown tags, max values)

**Success criteria:** All proto tests pass. All 14 test vectors verified. `cargo test -p irtt-proto` green.

**Risks:** Varint encoding mismatch (mitigated by verified test values).

### Milestone 2 — Manual Client No-Test / Open

**Goal:** `Client` can connect to an irtt server, perform open handshake, and run no-test mode.

**Files/modules:**
- `crates/irtt-client/src/lib.rs`
- `crates/irtt-client/src/socket.rs`
- `crates/irtt-client/src/config.rs`
- `crates/irtt-client/src/session.rs`
- `crates/irtt-client/src/client.rs`
- `crates/irtt-client/src/event.rs`
- `crates/irtt-client/src/error.rs`
- `crates/irtt-client/src/timing.rs`

**Tasks:**
1. Implement `ClientSocket` with socket2
2. Implement `ClientConfig` and `SocketConfig`
3. Implement DNS resolution
4. Implement `Client::connect`
5. Implement open request sending with retransmission
6. Implement open reply parsing and validation
7. Implement negotiation policy
8. Implement no-test mode (Open+Close flags)
9. Implement `Client::close`
10. Define `ClientEvent` enum (full definition, even if only SessionStarted/SessionEnded used here)
11. Define error types

**Tests:**
- Unit: config validation, negotiation policy logic
- Interop (gated): no-test mode against real server
- Interop (gated): basic open/close cycle

**Success criteria:** `Client::connect` + `Client::open` + no-test mode works against irtt server. Interop test passes.

**Risks:** Socket option availability on target platforms. IPv6 TOS/TC setting may need platform-specific handling.

### Milestone 3 — Manual Finite Probe Events

**Goal:** `Client` can send probes, receive replies, and emit events for a finite test.

**Files/modules:**
- `crates/irtt-client/src/probe.rs` (pending probe map)
- `crates/irtt-client/src/validate.rs`
- Updates to `client.rs`, `session.rs`, `event.rs`

**Tasks:**
1. Implement `Client::send_probe`
2. Implement `Client::recv_once`
3. Implement `Client::recv_available`
4. Implement `Client::poll_timeouts`
5. Implement pending probe map (bounded)
6. Implement reply matching and validation
7. Implement RTT sample computation
8. Implement duplicate detection
9. Implement late detection
10. Implement loss timeout detection
11. Implement `RunMode::Duration` and `RunMode::Count` completion logic
12. Implement `OneWayDelaySample` computation
13. Implement `ReceivedStatsSample` extraction
14. Implement `ServerTiming` extraction
15. Implement `ClientWarning::ServerProcessingExceedsRawRtt`

**Tests:**
- Unit: RTT computation (raw, adjusted, effective)
- Unit: pending probe map operations
- Unit: duplicate/late detection
- Unit: loss timeout detection
- Unit: received window → loss direction classification
- Interop (gated): finite 5s test, verify packet counts, RTT values present
- Interop (gated): verify `ceil(d/i)` packet count formula

**Success criteria:** Manual client can run a complete finite test and produce correct events. Interop tests pass.

**Risks:** Timing accuracy. Potential edge cases in recv_once blocking behavior.

### Milestone 4 — Minimal CLI Stream Mode

**Goal:** Working CLI binary that can run a test and print results in machine/simple/rtt-us format.

**Files/modules:**
- `crates/irtt-cli/src/main.rs`
- `crates/irtt-cli/src/args.rs`
- `crates/irtt-cli/src/format.rs`
- `crates/irtt-cli/src/run.rs`
- `crates/irtt-cli/src/signal.rs`

**Tasks:**
1. Implement CLI argument parsing with clap
2. Implement machine output formatter
3. Implement simple output formatter
4. Implement rtt-us output formatter
5. Implement basic run loop (using manual Client initially, or managed if M5 is done)
6. Implement Ctrl-C handling
7. Implement delimiter support
8. Implement timestamp formatting
9. Implement header option

**Tests:**
- Unit: formatter tests for each format
- Unit: delimiter substitution
- Unit: timestamp formatting
- Integration: run CLI against irtt server, verify output format
- Verify awk-friendly examples from design doc work

**Success criteria:** `irtt-cli <server>` runs a test and prints machine-format output. Ctrl-C works.

**Risks:** None significant. Straightforward formatting work.

### Milestone 5 — Managed Runner

**Goal:** `ManagedClient` with coordinator/sender/receiver threads and event fanout.

**Files/modules:**
- `crates/irtt-client/src/managed/mod.rs`
- `crates/irtt-client/src/managed/coordinator.rs`
- `crates/irtt-client/src/managed/sender.rs`
- `crates/irtt-client/src/managed/receiver.rs`
- `crates/irtt-client/src/managed/hub.rs`
- `crates/irtt-client/src/managed/cancellation.rs`

**Tasks:**
1. Implement `CancellationToken`
2. Implement `EventHub` with subscriber management
3. Implement `SubscriberConfig` and overflow policies
4. Implement sender/pacer thread with absolute scheduling
5. Implement receiver thread
6. Implement coordinator thread (correlate, compute RTT, emit events)
7. Implement `ManagedClient::start` (open on calling thread, then spawn)
8. Implement `ManagedClientSession::subscribe`
9. Implement `ManagedClientSession::stop` and `join`
10. Implement continuous mode
11. Implement thread panic detection
12. Implement graceful shutdown (close packet)

**Tests:**
- Unit: EventHub fanout to multiple subscribers
- Unit: SubscriberOverflow policies
- Unit: CancellationToken
- Integration: ManagedClient finite test, verify events arrive at subscriber
- Integration: ManagedClient continuous mode, stop, verify clean shutdown
- Interop (gated): managed client against real server

**Success criteria:** ManagedClient runs tests, delivers events to subscribers, shuts down cleanly.

**Risks:** Thread coordination complexity. Potential deadlocks if channels are misused. Careful testing required.

### Milestone 6 — `irtt-stats`

**Goal:** Statistics crate that consumes events and produces cumulative/rolling/summary stats.

**Files/modules:**
- `crates/irtt-stats/src/lib.rs`
- `crates/irtt-stats/src/collector.rs`
- `crates/irtt-stats/src/running.rs`
- `crates/irtt-stats/src/rolling.rs`
- `crates/irtt-stats/src/summary.rs`
- `crates/irtt-stats/src/median.rs`

**Tasks:**
1. Implement `RunningStats` (online mean/variance)
2. Implement `StatsCollector` event processing
3. Implement cumulative stats
4. Implement rolling count window
5. Implement rolling time window
6. Implement IPDV computation
7. Implement loss/duplicate/late counting
8. Implement finite summary with median
9. Handle edge cases (missing adjusted, processing > raw, all lost)

**Tests:**
- Unit: RunningStats with known sequences
- Unit: process events, verify min/max/mean/variance
- Unit: rolling window behavior
- Unit: IPDV from successive RTTs
- Unit: median computation
- Unit: edge cases (empty, single, all lost)

**Success criteria:** StatsCollector produces correct statistics from event streams.

**Risks:** Floating-point precision. Median storage for long tests (acceptable for finite mode).

### Milestone 7 — Full/Default CLI Stats Integration

**Goal:** Default CLI build includes stats. Finite tests show summary. Minimal build still works.

**Files/modules:**
- Updates to `crates/irtt-cli/src/run.rs`
- `crates/irtt-cli/src/format.rs` (summary formatting)
- `crates/irtt-cli/Cargo.toml` (feature flags)

**Tasks:**
1. Wire StatsCollector into CLI run loop as event subscriber
2. Implement human summary formatting
3. Implement stats display for finite tests
4. Optional periodic stats in continuous mode
5. Verify `--no-default-features` build works without stats
6. Implement human output format (with summary)

**Tests:**
- Integration: finite test with summary output
- Integration: minimal build compiles and runs without stats
- Unit: human summary formatting

**Success criteria:** `cargo build -p irtt-cli` includes stats. `cargo build -p irtt-cli --no-default-features` works without.

**Risks:** None significant.

### Milestone 8 — Compatibility Features

**Goal:** Full protocol compatibility: HMAC, all timestamp modes, all received stats modes, server fill, DSCP.

**Files/modules:**
- Updates across irtt-client and irtt-cli

**Tasks:**
1. HMAC interop testing (correct key, wrong key, no key)
2. All StampAt modes (none, send, receive, both, midpoint)
3. All Clock modes (wall, mono, both)
4. All ReceivedStats modes (none, count, window, both)
5. Server fill
6. DSCP / Traffic Class setting
7. TTL / hop limit (if desired)
8. Large packet sizes
9. Comprehensive interop test suite

**Tests:**
- Interop: all timestamp × clock × stats combinations
- Interop: HMAC authentication
- Interop: DSCP capture verification
- Interop: server fill
- Interop: large packets

**Success criteria:** Full compatibility with irtt 0.9.1 server across all parameter combinations.

**Risks:** Platform-specific socket option issues (IPv6 TC, DF bit).

### Milestone 9 — Advanced Networking

**Goal:** Ancillary receive metadata for ECN/DSCP/kernel RX timestamps.

**Files/modules:**
- `crates/irtt-client/src/ancillary.rs` (feature-gated)
- Platform-specific code

**Tasks:**
1. Implement recvmsg wrapper (Linux)
2. Extract IP_TOS / IPV6_TCLASS from ancillary data
3. Extract kernel RX timestamp
4. Populate PacketMeta
5. macOS support (if feasible)
6. Test with `irtt server --ecn`

**Tests:**
- Integration: verify ancillary data extraction on Linux
- Interop: verify ECN behavior with server --ecn
- Unit: ancillary data parsing

**Success criteria:** PacketMeta populated with real values on supported platforms.

**Risks:** High platform variability. macOS ancillary API differences. ECN behavior not fully verified.

---

## 15. Open Questions

### Q1: Server Close During Active Test (Section 19.3)

**Why it matters:** If the server can set Close flag on echo replies mid-test, the client must detect and handle this gracefully.

**Blocks implementation:** No. Defensive handling is straightforward.

**Suggested behavior:** Check for Close flag in every echo reply. If set, process the reply normally, emit SessionEnded with reason ServerClosed. This is safe regardless of whether the behavior actually occurs.

**Black-box test suggestion:** Run a very long test exceeding the server's max duration. If the server closes the session, observe the packet.

### Q2: Send Timestamp Capture Timing (Section 19.6)

**Why it matters:** Affects RTT accuracy.

**Blocks implementation:** No. This is an implementation choice.

**Recommended behavior:** Capture `Instant::now()` + `SystemTime::now()` immediately before `send()`. This ensures the measured RTT includes local send/enqueue overhead, which is more conservative for latency-control consumers like sqm-autorate-rust. The receive timestamp remains captured immediately after `recv()` returns.

### Q3: RTT When Server Processing > Raw RTT (Section 19.12)

**Why it matters:** Negative adjusted RTT is nonsensical.

**Blocks implementation:** No. The Rust design already specifies the correct handling.

**Recommended behavior:** Set `adjusted = None`, emit `ClientWarning::ServerProcessingExceedsRawRtt`. `effective()` falls back to raw. This is already in the design.

### Q4: Upstream Server `--ecn`

**Why it matters:** Required for full ECN compatibility testing.

**Blocks implementation:** No. ECN is a deferred ancillary feature (Milestone 9).

**Suggested black-box test:** Run `irtt server --ecn` and capture packets with tshark. Observe whether the server sets ECN bits in IP header. Test with `--dscp` values that include ECN bits.

### Q5: Continuous Session Duration Semantics

**Why it matters:** If server negotiates a finite duration for a continuous client, the client must handle session expiry.

**Blocks implementation:** No. `ReconnectPolicy` covers this.

**Recommended behavior:** Default `ReconnectPolicy::Never` for library. CLI continuous mode uses `ReconnectPolicy::OnServerClose { delay: 1s }`. If the server closes, reconnect after delay.

### Q6: Exact Feature-Gating of Stats/JSON/Ancillary

**Why it matters:** Build size and dependency management.

**Blocks implementation:** No. Feature flags are well-defined in the design.

**Recommended behavior:** Follow the design document exactly. `stats` is default on for irtt-cli. All others are opt-in.

### Q7: DropOldest Subscriber Overflow with Flume — RESOLVED

**Decision:** Deferred from MVP. Initial overflow policies are `DropNewest`, `Block`, and `Disconnect`. `DropOldest` may be added post-MVP using a drain-one-and-retry approach or a ring-buffer channel.

---

## 16. Risk Register

| Risk | Impact | Likelihood | Mitigation | Owner/Artifact |
|------|--------|------------|------------|----------------|
| Clean-room contamination | Critical — legal/licensing | Low | Implementation agent reads only clean artifacts. No upstream source in repo. Documented boundary in CLEANROOM_NOTES.md | Process / all crates |
| Protocol wire format mismatch | High — interop failure | Low | Test vectors verified. Black-box captures available. Interop tests in CI. | irtt-proto |
| Varint/zigzag encoding mismatch | High — open/negotiation fails | Very Low | Independently verified with multiple value samples. Standard LEB128/zigzag. | irtt-proto |
| HMAC computation mismatch | High — authentication fails | Very Low | Two test vectors verified. Four packet types verified against captures. | irtt-proto |
| Timing accuracy insufficient | Medium — RTT measurements off | Low | Use monotonic clock. Absolute scheduling. Timestamp close to syscall. TimerMode::Precise available as opt-in. | irtt-client |
| Router resource exhaustion | Medium — OOM on OpenWrt | Low | Bounded pending map. No mandatory sample retention. Feature-gated stats. Minimal build path. | irtt-client, irtt-cli |
| Event subscriber backpressure delays sender | High — timing disruption | Low | Sender thread never touches event hub. Only coordinator fans out. Coordinator is not timing-critical. | irtt-client (managed) |
| Stats/event loss from subscriber drops | Medium — inaccurate stats | Medium | Document limitation. Track drop count. Stats output marks approximate results. | irtt-stats |
| Managed runner deadlock | High — hang | Low | Careful channel sizing. CancellationToken as escape hatch. Thread panic detection. Thorough testing. | irtt-client (managed) |
| ECN/ancillary portability | Medium — feature unavailable | Medium | Feature-gated. Deferred to Milestone 9. Platform-specific code isolated. Graceful fallback to None. | irtt-client (ancillary) |
| CLI format instability | Medium — downstream breakage | Low | Machine format fields are positional and documented. New fields added at end only. | irtt-cli |
| IPv6 Traffic Class socket option | Low — IPv6 DSCP may not work | Medium | Use socket2 if available. Fall back to libc setsockopt. Test on target platforms. | irtt-client |
| Server behavior divergence across versions | Low — untested server behavior | Low | Target irtt 0.9.1 only. Black-box tests as regression suite. | all |
| flume DropOldest not native | Low — subscriber policy gap | Medium | Deferred from MVP. DropNewest/Block/Disconnect available. | irtt-client (managed) |

---

## 17. Implementation Agent Instructions

### What to implement first

1. **Milestone 0:** Workspace skeleton. Get `cargo check --workspace` passing with empty crates.
2. **Milestone 1:** `irtt-proto`. All wire format logic. This is pure, testable, and has no external dependencies beyond crypto crates. Verify test vectors before moving on.
3. **Milestone 2:** Manual `Client` open/close/no-test. First real interop with an irtt server.
4. **Milestone 3:** Manual `Client` probe sending/receiving. Full event model.

### What NOT to implement first

- Do NOT implement `ManagedClient` before the manual `Client` works (milestones 2–3 come before 5).
- Do NOT implement `irtt-stats` before the client produces correct events (milestone 6 comes after 5).
- Do NOT implement ancillary networking (Milestone 9) during initial implementation.
- Do NOT implement serde/json/tracing features during initial milestones.
- Do NOT implement human output format before machine format works.
- Do NOT implement `ReconnectPolicy` logic before basic continuous mode works.

### What files to read

- `docs/IRTT_CLIENT_PROTOCOL_SPEC.md` — normative protocol reference
- `docs/BLACKBOX_VERIFICATION_REPORT.md` — verified protocol details and packet captures
- `docs/RUST_DESIGN.md` — architecture and product goals
- `docs/test-vectors.md` — 12 packet test vectors from real captures
- `docs/CLEANROOM_NOTES.md` — clean-room boundary (read once for awareness)
- `IMPLEMENTATION_PLAN.md` (this file) — detailed implementation guidance

### What files NOT to read

- Any upstream IRTT source code (Go files, Go tests, Go modules)
- Any files in `captures/` directory (these are diagnostic artifacts from the spec agent, not implementation inputs — use the verified findings in the black-box report instead)
- Any contaminated notes or transcripts
- Any GPL-licensed material

### Clean-room boundaries to maintain

1. Never search for, open, or read upstream IRTT source files
2. Never search online for IRTT implementation details
3. Never ask for contaminated notes or transcripts
4. Never copy or adapt upstream type names, function names, or module layout
5. Design all internal names independently based on Rust conventions
6. Use only the clean protocol spec and black-box report for protocol truth
7. When uncertain about protocol behavior, mark as open question and suggest a black-box test — do NOT guess from upstream patterns

### How to report uncertainties

- If a protocol detail is unclear: add a `// OPEN: <description>` comment and note it in test output
- If a behavior differs from spec expectation during interop testing: document the discrepancy, do not silently adapt
- If a platform-specific feature is unavailable: use `cfg` attributes, provide graceful fallback, and document

### How to structure commits/milestones

- One commit per logical unit of work (e.g., "implement varint encoding" or "add open request encoding")
- Tag milestone completion with a clear commit message (e.g., "milestone 1: irtt-proto complete")
- At every commit, ensure:
  - `cargo fmt --check --workspace`
  - `cargo clippy --workspace --all-targets`
  - `cargo test --workspace`
- Run `cargo test --workspace --features interop` at milestone boundaries (2, 3, 4, 5, 8)

### First work order

The first implementation agent task should be:

Implement Milestone 0 and Milestone 1 only. Create the Cargo workspace and complete `irtt-proto`. Do not implement sockets, client runtime, stats, CLI behavior, managed runner, serde, tracing, ancillary networking, or interop tests yet.

Success means:
- `cargo check --workspace` passes
- `cargo test -p irtt-proto` passes
- All 14 packet vectors pass
- No upstream source or tests are present

---

## 18. Design Brief Amendments

The following amendments to `RUST_DESIGN.md` have been incorporated into this plan. They are listed here for traceability.

### Incorporated: `Client::open` return type

Changed from `Result<Vec<ClientEvent>, ClientError>` to `Result<OpenOutcome, ClientError>`. See Section 5.

### Incorporated: `recv_once` return type

Changed from `Result<Option<Vec<ClientEvent>>, ClientError>` to `Result<Vec<ClientEvent>, ClientError>` with empty Vec meaning "no data available." See Section 5.

### Incorporated: `EchoSent` event omitted

`EchoSent` is not defined in MVP. Can be added later behind a debug/tracing option.

### Incorporated: `DropOldest` deferred

`DropOldest` subscriber overflow is deferred from MVP. Initial overflow policies: `DropNewest`, `Block`, `Disconnect`. See Section 6.

### Incorporated: `NegotiationPolicy` simplified

Changed from three-variant (`AcceptServerRestrictions`/`RequireExact`/`RequireWithin`) to two-variant (`Strict`/`Loose`). See Section 5.

### Incorporated: `ClientErrorKind` removed

`FatalError` and `SessionEndReason::Error` use `ClientError` directly. No separate `ClientErrorKind` enum.

### Remaining: `irtt-stats` dependency on `irtt-client`

Keep as-is. The dependency is on the types, not the runtime. Rust's dead-code elimination handles this. If binary size becomes an issue, extract types later.

### Remaining: Clock value 0

The spec defines Clock enum values as 1=Wall, 2=Monotonic, 3=Both. Value 0 is not defined. If StampAt is None, ignore Clock value. If StampAt is non-None and Clock is 0, treat as an error or default to Wall. Document the decision.

---

## 19. Final Recommendation

**Proceed.**

The clean artifacts are thorough and internally consistent. The protocol spec has been verified against a real irtt 0.9.1 server. The Rust design is well-structured and addresses all major architectural concerns. The three remaining open questions (server close mid-test, timestamp capture timing, processing > raw RTT) do not block implementation — defensive handling is specified for all three.

No major blockers exist. The recommended minor amendments (Section 18) are quality-of-life improvements, not structural issues.

**First milestone to implement:** Milestone 0 (workspace skeleton), immediately followed by Milestone 1 (`irtt-proto`). The protocol crate is pure, heavily testable, and establishes the foundation for everything else.
