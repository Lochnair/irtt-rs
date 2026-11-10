# `irtt-client` guidance

Repository-wide guidance in the root `AGENTS.md` also applies here.

## Probe-send commitment API

`Client::send_probe` and `AsyncClient::send_probe` return
`Result<SendReceipt, SendProbeError>`; the Tokio poll driver returns the same
result inside `Poll`. A receipt represents an Echo datagram accepted by the
socket and already committed in session state: sequence and sent count advanced,
and the probe entered pending tracking. It contains `seq`, `remote`, the public
post-send `sent_at`, accepted `bytes`, and `send_call`, without schedule metadata.
`ClientEvent::from(receipt)` constructs `EchoSent` with no schedule annotations.

Classify every failure explicitly: `NotCommitted(ClientError)` means no datagram
was accepted and no probe committed; `AfterCommit { receipt, source }` preserves
the authoritative accepted send when length validation or TX error-queue
processing fails. There is no blanket `From<ClientError>` conversion. Do not
roll back a committed probe or classify a post-send error with an indiscriminate
`?`.

All fallible state/capacity/counter/allocation and timeout preparation precedes
submission. Immediately after `send`/`try_send` returns `Ok(bytes)`, capture the
public post-send timestamp, commit infallibly, and construct the receipt before
fallible post-send processing. No suspension, readiness wait, fallible allocation,
or fallible state operation may intervene between acceptance and commitment;
the Tokio poll must never return `Pending` after acceptance.

The managed driver consumes receipts for cadence/stagger acceptance and constructs
`EchoSent` with its scheduled timestamp and timer error. A post-commit failure
advances cadence and publishes that send before failure cleanup; the underlying
cause remains a sending failure. `NotCommitted` and `Pending` advance neither
cadence nor stagger gating. Counters are cumulative accounting, never evidence
of one operation's commitment.

Tokio retains one prepared probe across cancellation/readiness false positives
and `WouldBlock`, samples a fresh private anchor on each retry, and clears the
prepared probe after commitment even when post-send processing fails. Failed implicit-ID submissions invalidate correlation as documented below.
Explicit-ID sends consume a distinct attempt ID even on failure.

## Linux kernel TX timestamp capture

RX and TX use separate `TimestampMode` policies. Both default to auto, which
falls back when initialization fails. On requires initialization and off
skips that direction. Per-packet timing is optional. Keep IP_TOS/IPV6_TCLASS
capture separate from both policies. Gate software RX metadata on successful
RX initialization even if TX software reporting produces an RX cmsg.

Configure software TX with OPT_ID after trusted Open acceptance, before committing the session. Earlier Open traffic has no TX generation. A failed strict activation restores local preparation and attempts a token-bound cleanup Close.

Keep `OPT_TSONLY` to avoid queuing the original datagram payload. An
implicit-ID Echo send error or anomalous accepted length can desynchronize
kernel IDs and wire sequences. Quarantine later associations. A Close that
may generate a timestamp and fails to submit also invalidates implicit
correlation, because the session remains open. Keep already attached
timestamps, send receipts and pending/retained accounting. Never correlate
a reused timestamp ID.

## MSG_ERRQUEUE drains

`receive::linux::error_queue` decodes one `MSG_ERRQUEUE` record at a time
(`try_recv_error_queue_record`) and classifies it: a genuine send-timestamp
completion (`TxTimestamp`), a timestamp-origin record that is not a usable
`SND` completion (`MalformedOrUnsupportedTimestamp` — never treated as
fatal, since this client only ever requests `TX_SOFTWARE`/`SND`, so anything
else on that origin is unexpected metadata, not a network error), a genuine
non-timestamp socket/network error (`SocketError`), or nothing (`Ignored`).
A timestamp origin cannot become a socket error. Truncated or ambiguous
control records are optional metadata and are discarded.

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
surfaced as `ClientError::Socket`. During post-send processing it is wrapped in
`SendProbeError::AfterCommit` with the accepted send's receipt; receive/timeout
operations retain the ordinary `ClientError` path.

The Tokio adapter uses the same raw nonblocking `recvmsg(MSG_ERRQUEUE |
MSG_DONTWAIT)` rather than `try_io`/`poll_recv_ready`: error-queue readiness
is not ordinary read readiness, and driving it through the Tokio reactor
risks clearing readiness state that the normal Echo receive path depends on.
There is no background task or second `AsyncFd` around the Tokio-owned
socket.

### Testing kernel timestamp behavior

The requirements above describe production behavior, not a required unit-test
architecture.

Prefer Linux tests that exercise the real socket and kernel timestamp path
through production APIs. A test intended to prove TX timestamp support must
actually require and observe a kernel TX timestamp when running on a supported
Linux environment; accepting the userspace fallback does not prove that path.

Likewise, prefer observable socket/client behavior for genuine error-queue
errors and bounded draining where it can reasonably be induced.

Do not restore synthetic `ControlMessageOwned` permutations, classifier-unit
suites, raw ancillary fixtures, test-only socket hooks, or other internal seams
solely to cover every `MSG_ERRQUEUE` branch. `nix` owns ancillary-message
decoding and ABI safety; `irtt-client` tests should primarily prove its own
observable interpretation and behavior through the real path.

If some invariant cannot reasonably be induced through a production interface,
a small focused internal test may be retained, but the burden is on that test to
justify why integration coverage is insufficient.

## Retained state

`PendingProbe` carries an optional `kernel_tx_timestamp: Option<SystemTime>`
alongside the userspace `sent_at`. It survives pending -> timed-out retention
(`SessionMachine::record_kernel_tx_timestamp` updates whichever of
`PendingMap`/`TimedOutMap` still holds the probe) and is otherwise bounded by
the same limits those maps already enforce — no new unbounded state. The earliest software observation at or after the send anchor is retained
when duplicate notifications disagree. Check the reply-time upper bound when
selecting the endpoint.

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
`commit_probe_sent`, and never advances the machine; the managed runner advances cadence only
  after an accepted send; each retry
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

`sent_at` remains the RTT endpoint and the userspace fallback for the
upstream (`client_to_server`) one-way delay, exactly as before this
capability existed. It is not an input to the downstream
(`server_to_client`) one-way delay, which is derived solely from
`ReceiveMeta::preferred_receive_wall` and the negotiated server send
timestamp — unaffected by this capability or by F-04. `TX_SOFTWARE` is
still a software kernel timestamp near the driver handoff, not a NIC
hardware transmit time. `irtt-stats`'s `ReplySample`
copies public `sent_at` straight into `client_send_mono`/`client_send_wall_ns`,
which `send_ipdv_ns` compares across probes for the upstream IPDV component —
so that component shifts endpoint along with `sent_at` itself (pre-send vs.
post-send) exactly as RTT and the upstream OWD fallback do; it is not a
separate invariant. A normal
reply and a measurable late reply (one whose `PendingProbe` is still
retained in `TimedOutMap`) apply the identical selection policy; an
untracked late reply has no retained probe and stays unmeasurable, and a
kernel timestamp that arrives after a reply has already been processed is
simply never observed by that reply.
