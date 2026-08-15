# `irtt-client` guidance

Repository-wide guidance in the root `AGENTS.md` also applies here.

## Linux kernel TX timestamp capture

On Linux, with the `ancillary` feature, the client best-effort upgrades its
socket from RX-only `SO_TIMESTAMPING` to RX+TX immediately after a successful
Open, before the first Echo probe can be sent:

```
socket created
    -> RX timestamping configured (RX_SOFTWARE | SOFTWARE)
    -> Open attempts/retries (untimestamped)
    -> successful Open
    -> best-effort enable TX timestamping (+ TX_SOFTWARE | OPT_ID | OPT_TSONLY)
    -> first Echo probe
```

`TX_SOFTWARE` asks the kernel for a software send timestamp; `OPT_ID` asks it
to tag each notification with an automatically incrementing per-socket ID;
`OPT_TSONLY` means the notification never needs to carry the original
payload, so a zero-length receive buffer is enough. The upgrade is entirely
best-effort: if it fails, the client keeps operating on the original RX-only
configuration (`try_enable_tx_timestamping` restores it explicitly) and
timing falls back to userspace `sent_at` as it always has. TX capability
failure is never a public error and is never logged from the library.

### Correlation invariant: kernel ID == wire sequence

The client uses the kernel's automatic `OPT_ID` directly as the probe's wire
sequence number — no separate correlation map. This is safe because:

- **A failed/nonblocking send never consumes an ID.** Linux increments the
  per-socket ID counter (`sk->sk_tskey`) speculatively while building a
  datagram and rolls the increment back (`atomic_dec`) on every error path
  out of `__ip_append_data`/`__ip6_append_data`
  (`net/ipv4/ip_output.c`, `net/ipv6/ip6_output.c`), guarded by a local
  `hold_tskey` flag. A `WouldBlock` `try_send` therefore can never leave a
  gap in, or otherwise advance, the ID the kernel will assign to the next
  *successful* submission. (`include/net/sock.h`'s `_sock_tx_timestamp`
  performs the same increment for the non-corked path.) Client-side, the
  existing send transaction only calls `commit_probe_sent` — which is the
  only thing that advances `next_wire_seq` — after a successful socket send;
  a `WouldBlock` result never reaches it.
- **The two counters start together and advance together.** `wire_seq`
  starts at 0 on every successful Open (`SessionMachine::commit_open` builds
  a fresh `ActiveSession` with `next_wire_seq: 0`) and advances by exactly 1
  only via `commit_probe_sent`, which only runs after a confirmed successful
  send — the same condition under which the kernel's counter advances by 1.
  TX timestamping is enabled only after that same successful Open and before
  any probe is sent, so both counters' zero points line up exactly.
- **One socket, one session, no reopen.** `SessionMachine`'s state machine
  only reaches `Open` from `Connected`, and `commit_open`/`commit_local_close`
  never route back to `Connected`. A `Client`/`AsyncClient` cannot reopen a
  second session on the same socket, so there is no second epoch of wire
  sequence numbers to collide with a stale kernel ID.
- **The one extra send (Close) is harmless.** `Close` sends one datagram
  after TX timestamping is already enabled, generating one TX timestamp
  record with no corresponding `wire_seq` (`Close` does not advance
  `next_wire_seq`). Because the session is terminal immediately after,
  nothing will ever look up that ID; `record_kernel_tx_timestamp`'s
  "unknown ID -> ignore" rule discards it safely.

## MSG_ERRQUEUE drains

`receive::linux::error_queue` decodes one `MSG_ERRQUEUE` record at a time
(`try_recv_error_queue_record`) and classifies it: a genuine send-timestamp
completion (`TxTimestamp`), a timestamp-origin record that is not a usable
`SND` completion (`MalformedOrUnsupportedTimestamp` — never treated as
fatal, since this client only ever requests `TX_SOFTWARE`/`SND`, so anything
else on that origin is unexpected metadata, not a network error), a genuine
non-timestamp socket/network error (`SocketError`), or nothing (`Ignored`).
Origin (`SO_EE_ORIGIN_TIMESTAMPING` or not) is the only thing that decides
between the last two categories.

`receive::linux::drain_tx_timestamps` reads up to
`MAX_TX_TIMESTAMP_RECORDS_PER_DRAIN` (32) records per call: nonblocking, one
record at a time, no heap allocation, stopping early once the queue is
empty. It is a starvation guard, not a queue-capacity promise — one TX
timestamp is generated per successful probe, and drains happen at several
natural choke points, so a modest constant comfortably covers realistic
backlog. `Client`/`AsyncClient` call it after a successful probe send,
before processing an inbound reply, and before evaluating timeouts; it never
waits for a timestamp; an absent timestamp after all of those opportunities
simply stays absent. A genuine `SocketError` record stops the drain and is
surfaced through the ordinary `ClientError::Socket` path.

The Tokio adapter uses the same raw nonblocking `recvmsg(MSG_ERRQUEUE |
MSG_DONTWAIT)` rather than `try_io`/`poll_recv_ready`: error-queue readiness
is not ordinary read readiness, and driving it through the Tokio reactor
risks clearing readiness state that the normal Echo receive path depends on.
There is no background task or second `AsyncFd` around the Tokio-owned
socket.

## Retained state

`PendingProbe` carries an optional `kernel_tx_timestamp: Option<SystemTime>`
alongside the userspace `sent_at`. It survives pending -> timed-out retention
(`SessionMachine::record_kernel_tx_timestamp` updates whichever of
`PendingMap`/`TimedOutMap` still holds the probe) and is otherwise bounded by
the same limits those maps already enforce — no new unbounded state. First
valid observation wins on duplicates.

### Pre-send anchor vs. post-send `sent_at` (F-04)

The probe send transaction splits into two timestamps around the socket
send, not one:

- A **private pre-send anchor**, sampled immediately before the fallible
  commit preflight (`SessionMachine::finalize_probe_commit`) that must
  happen before the socket send: timeout deadline `checked_add` arithmetic
  (so `DurationOverflow` still guarantees nothing was transmitted) and the
  kernel TX lower-plausibility bound. Its monotonic half becomes
  `PendingProbe::timeout_at`'s basis; its wall half becomes
  `PendingProbe::tx_not_before_wall`. It is never exposed publicly.
- The **public `sent_at`**, a paired wall/monotonic `ClientTimestamp`
  captured immediately after the socket send returns successfully
  (`SessionMachine::commit_probe_sent`, which is infallible — all fallible
  work already happened against the pre-send anchor). This is the RTT
  endpoint, the userspace upstream-OWD fallback endpoint, `timer_error`'s
  basis, and the value surfaced on `EchoSent`/`EchoReply`/`EchoLoss`/
  `LateReply`.

So `timeout_at` is deliberately **not** derived from `sent_at.mono` — it
remains anchored to before the send, on purpose, so the existing pre-send
timeout/overflow/no-send contract is unchanged. Only the *measurement*
`sent_at` moved to after the send. A `WouldBlock` `try_send` (Tokio) never
samples a `sent_at`, never calls `finalize_probe_commit`'s matching
`commit_probe_sent`, and never advances the machine or schedule; each retry
gets its own fresh pre-send anchor, and only a successful attempt's anchor
and `sent_at` are ever retained.

**The kernel TX timestamp is eligible for exactly one measurement: the
upstream (`client_to_server`) one-way delay send endpoint.**
`compute_one_way`'s private `preferred_send_wall` helper prefers it over the
userspace `sent_at.wall` sample when

    tx_not_before_wall <= kernel_tx_timestamp <= reply_received_at.wall

(inclusive, compared only against the client's own wall clock — never
against the remote server's wall timestamp). The lower bound is the
pre-send `tx_not_before_wall`, not the post-send `sent_at.wall`: a
legitimate `TX_SOFTWARE` timestamp can be generated while the send syscall
is still in flight, so it can legitimately precede the post-send `sent_at`
sample without being implausible. Otherwise, including when no kernel
timestamp was observed, the upstream endpoint falls back to `sent_at.wall`
(the post-send sample); a rejected observation is never erased from
`PendingProbe`, only skipped for that measurement. There is no
maximum-lag bound: unlike the kernel receive side, legitimate client
send-to-reply time can exceed a second.

`sent_at` remains the sole input to RTT and the downstream
(`server_to_client`) one-way delay, exactly as before this capability
existed; `TX_SOFTWARE` is still a software kernel timestamp near the driver
handoff, not a NIC hardware transmit time. IPDV is likewise unaffected — it
is computed from the existing userspace timestamp fields only. A normal
reply and a measurable late reply (one whose `PendingProbe` is still
retained in `TimedOutMap`) apply the identical selection policy; an
untracked late reply has no retained probe and stays unmeasurable, and a
kernel timestamp that arrives after a reply has already been processed is
simply never observed by that reply.
