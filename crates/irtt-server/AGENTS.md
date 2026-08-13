# `irtt-server` guidance

Repository-wide guidance in the root `AGENTS.md` also applies here.

## Where server behavior comes from

Server wire behavior comes from the sanitized clean server documents, not from
upstream implementation material:

- `docs/protocol/IRTT_SERVER_PROTOCOL_SPEC.md`
- `docs/protocol/test-vectors/SERVER_BEHAVIORAL_VECTORS.md`
- `docs/protocol/BLACKBOX_VERIFICATION_REPORT.md`
- `docs/protocol/IRTT_CLIENT_PROTOCOL_SPEC.md` for the shared wire format

Never derive behavior from upstream `heistp/irtt` source or tests. If the clean
material does not establish a semantic, state the ambiguity instead of guessing.

## Boundaries

- Protocol behavior comes from `irtt-proto`. `irtt-server` must **not** depend
  on `irtt-client` to reuse wire logic; a needed primitive belongs in
  `irtt-proto`, where both sides get it.
- Do not reimplement magic validation, flag rules, HMAC placement, request
  classification, token/header offsets or parameter parsing here.
- No statistics, CLI or presentation concerns.
- `SocketAddr` belongs here: endpoint identity is session state. It does not
  belong in `irtt-proto`.
- Endpoint identity is compared field by field, not with `SocketAddr` equality.
  Family, address and port are established components. The IPv6 flow label is
  excluded because it identifies no endpoint; the IPv6 zone is included as a
  deliberate project policy, because the specification records multi-zone
  behavior as untested and two peers sharing a link-local address in different
  zones are genuinely different peers. Reuse that one comparison rather than
  writing a second identity rule, and keep both choices labelled as policy
  rather than as verified compatibility.

## Core versus runtime

- Deterministic protocol and session logic — admission, negotiation, the session
  table, resource decisions, packet construction — belongs in the core.
- Socket and runtime orchestration — `UdpSocket`, IPv4/IPv6 listeners,
  `recv_from`/`send_to`, timers, expiry, shutdown, `select!`, socket options —
  belongs around the core.
- The server is intentionally **Tokio-native**. Tokio does not need to be
  optional for this crate.
- `Server` owns exactly one Tokio `UdpSocket` and one `ServerCore`. Run protocol
  processing sequentially in that one task: do not spawn per datagram, put the
  core behind a lock, or start an internal detached task.
- The runtime must remain a thin `recv_from` -> `handle_datagram` -> optional
  `send_to` loop. Do not duplicate protocol parsing or encoding around the core.
- A receive failure or core failure may terminate `Server::run`; a per-packet
  send failure (including a short send) drops only that reply and must not stop
  the listener. Core state is not rolled back after a transport send failure.
- Runtime maintenance calls the core's expiry hook. It must not inspect session
  internals or grow a second expiry rule.
- Explicit-address binds are the source-address-safe supported path. A wildcard
  bind on a multi-homed host remains subject to kernel source-address selection
  until packet-info handling is implemented.
- Do not add a blocking server, an alternate-runtime variant, a transport trait,
  a runtime abstraction layer, or a mirrored client-style API family. There is
  no product requirement for any of them, and `ServerCore` being public is not a
  promise of runtime independence.

## Rejection is silence

Malformed, unauthenticated, unsupported and policy-refused input is discarded
without a reply and without disturbing any live session. The protocol defines no
error reply, no reset and no NACK. Reserve the error type for internal failures
on the server's own side; an inbound `ProtoError` must never surface as a server
failure. Hostile UDP must leave the server running.

Do not let reply behavior reveal which stage rejected a datagram — in
particular, an authentication failure must be indistinguishable from an unknown
token.

## An acknowledged open must be executable

Validate the **final, restricted** parameters before acknowledging an open, not
just the requested ones. Once an open passes admission and negotiation, its
effective parameters must be safe for the server to run.

- Effective non-none `StampAt` with `Clock::Unspecified` is silently rejected.
  Timestamps from no clock lay out no timestamp field, so the session could not
  be run. Do **not** synthesize a clock and do **not** rewrite `StampAt` to
  none: either answers with a session the client did not ask for. This is
  `irtt-rs` policy for nonconforming input, chosen because the clean evidence
  records the reference server accepting this combination and then failing on
  the first echo. A conforming client always sends a clock when it selects
  timestamps, so conforming traffic is unaffected.
- `Clock::Unspecified` alone is valid — it is just an absent tag. Only the
  combination is refused, and only an absent tag reaches it, since an explicit
  zero Clock is already out of range for the decoder.
- Run the check after negotiation, so that a later policy restricting `StampAt`
  to none makes an omitted clock safe without this rule changing.
- Negative `Length` stays an accepted negotiated value, returned unchanged in
  the open reply and stored verbatim in the session. It needs no clamping to be
  executable: `irtt-proto` floors actual echo packet sizing at the required
  protocol field block, so a negative length behaves as zero does.
- A session's echo datagram must fit `ServerConfig::max_packet_length`, whose
  default is 65,507 bytes. That is `irtt-rs` resource policy, not an
  interoperability requirement: it is the largest IPv4 UDP payload, a
  conservative cross-family ceiling that bounds one session's echo buffer to
  roughly 64 KiB and that no normal client exceeds. It is **not** an MTU, not an
  upstream default, and says nothing about what a path will carry. Upstream's
  unlimited default is not a compatibility target.
- A requested positive `Length` above the configured maximum is reduced to it
  during negotiation, so the reply is honest about what the server will emit.
  Zero and negative values are left exactly as requested.
- Capping the parameter is not sufficient. After negotiation, the mandatory echo
  field block must itself fit the configured maximum or the open is silently
  refused — statistics and timestamp fields, and authentication's 16 bytes, can
  exceed a small maximum on their own. Ask `irtt_proto::echo_packet_len` with
  this server's actual HMAC mode rather than growing a second packet-size rule
  here; an unrepresentable length from it is a request rejection, not a
  `ServerError`.
- Zero is a valid maximum and is not a synonym for unlimited: it refuses every
  session, and no-test opens with it, since both are validated against the same
  effective session.
- Echo request admission reuses this same configured maximum, comparing it
  against the received datagram length before any receive state is mutated.

## Echo processing

- An accepted echo owns per-session `ReceiveState` — count, window and the
  previous accepted sequence number. Admission order is authentication, then
  received datagram length against the configured maximum, then token, then
  endpoint; a request failing any of them mutates nothing, in this session or
  any other.
- The count and window transitions follow the clean specification's Section 10
  and the Section 1 vectors, including the parts that read as defects: a
  duplicate advances the count and leaves the window alone, and a late,
  reordered or far-gap request resets the window to `0x1` rather than setting a
  historical bit, because the distance is unsigned. Do not replace this with a
  conventional selective-acknowledgement bitmap. Reference the vectors rather
  than restating the tables here.
- Only the token and sequence number mean anything in a request. The tail is
  opaque, and the request's length never sizes the reply — layout and length
  come from the session's negotiated params through `irtt-proto` alone.
- The transition is pure and is committed only after the reply has encoded, so
  an internal encoding failure cannot leave a session claiming to have answered
  a request it never did.
- Clock sampling is a private injected seam, like token generation, not a
  runtime abstraction: the receive instant is taken as soon as a datagram is
  classified as an echo and the send instant just before the reply is built, so
  they bracket the server's own handling. The monotonic origin belongs to the
  clock source and is stable for its life.
- A reply's receive instant is held back to its send instant where the wall
  clock stepped backwards between the two readings, so the required ordering
  holds. That correction is per reply and stateless on purpose: latching a
  pre-step wall value across packets would keep reporting a time the host has
  corrected away, which is the smoothing the specification forbids, and
  anchoring the wall clock to the monotonic origin would drift and never pick up
  a legitimate correction. The server reports its own wall clock honestly.
- For a single-clock midpoint, `irtt-rs` emits the one negotiated field. It does
  not reproduce upstream 0.9.1's dual-field midpoint; `irtt-proto` still decodes
  that form from a peer.
- Reply payload bytes are zero, which is the initial safe fill policy: payload
  carries no protocol meaning, and a server must never emit residue from another
  request or client. Request payload bytes never reach a reply.
- The full `ServerFill` policy is a later slice. Do not add placeholder session
  fields for it.

## Reply traffic class

The core hands each reply out as an `OutboundDatagram`: the bytes plus the
transport policy sending them requires. The runtime must not rediscover that
policy by decoding the outgoing packet, inspecting the inbound token, reaching
into a `Session` or keeping its own token-to-marking map — the core is where
session policy lives.

- `traffic_class` is the **raw 8-bit IPv4 TOS / IPv6 Traffic Class byte**, which
  is what the DSCP protocol parameter carries. The six-bit codepoint is
  `traffic_class >> 2` and the low two bits are ECN. Never shift, mask or round
  it inside the server.
- Open and no-test replies always request class zero, whatever the session
  negotiates. That is observed behavior, not a simplification.
- An echo reply requests the session's negotiated byte, and a server-close echo
  reply keeps it: the close is an ordinary reply with a flag added, not a
  special packet.
- A negotiated integer outside `0..=255` stays in `Params` — returned and stored
  exactly as requested — but is transported unmarked. This is **`irtt-rs`
  policy**: the clean evidence records the reference host's handling of such
  values as platform-specific and explicitly not a compatibility requirement, so
  there is nothing to reproduce, and a malformed-but-accepted session should
  stay usable rather than be given a marking nobody asked for. Do not add
  negotiation-time rejection, clamping or wrapping for it.
- The runtime applies a class before **every** send. Zero must be applied as
  explicitly as a nonzero value: skipping the call would send an open reply, or
  an unmarked session's reply, under whichever marking the previous reply left
  on the shared socket. There is no restore-to-zero-after-send step, and no
  cached "current value" to elide the call — the next reply is authoritative.
- **Sequential send ownership is what makes a socket-wide setting race-free.**
  One task owns the socket and no other sends from it, including while a send is
  suspended. Do not introduce concurrent sends without replacing this with
  per-packet control messages.
- Applying the class and sending are both per-packet transport steps: a failure
  of either drops that one reply and the listener keeps serving. A marking that
  could not be applied means the reply is not sent at all, because sending it
  would put it on the wire under the previous reply's marking.
- **One exception, and it is what keeps the server usable on hosts without the
  option**: an *unmarked* reply may still be sent when this server has never
  successfully applied a nonzero class to the socket, because then there is no
  marking of ours for it to inherit. Some Windows builds do not support
  `IP_TOS`, and a few targets have no safe setter at all; without this, such a
  host would drop every open reply and no session could ever start. The flag
  behind it is consulted only on failure — it never elides a call.
- The platform helper must report failure honestly, including for zero. "The
  socket was cleared" and "nothing was written" are different statements, and a
  socket passed to `Server::from_socket` may have been marked by its creator.
  Whether an unappliable zero is nevertheless safe is the runtime's decision,
  made from what this server itself applied.
- The option itself comes from `socket2`'s safe API, chosen by the listener's
  bound address family rather than the peer's. The crate forbids unsafe code;
  there is no raw `setsockopt` and no `sendmsg`/`cmsg` machinery. Keep the
  target lists identical to the pinned `socket2` version's own gates — check
  its source rather than copying another crate's older matrix.

## Rate, lifetime and server-initiated close

Defaults: minimum send interval 10 ms, burst allowance 5, idle timeout 60 s, no
maximum test duration. All four are per-server configuration, and the burst
allowance is per session.

Negotiation applies the interval floor first and the (idle timeout ÷ 4) cap
second, so a configured minimum above a quarter of the timeout ends with the cap
winning and the returned interval below that minimum. A configured maximum test
duration reduces a longer requested Duration and also replaces a *continuous*
request (an absent Duration), because restricting continuous mode to a finite
test is what configuring a maximum means. A zero configured maximum is stored as
no maximum: Duration zero on the wire means continuous, so a finite maximum of
zero could only be expressed by making a client's finite test endless.

Several choices below deliberately differ from the reference server's observed
behavior. Each is **`irtt-rs` project policy**, taken where the clean evidence
records upstream behavior as policy rather than an interoperability requirement.
Do not rewrite the protocol evidence to match them.

- **The refill interval is the shorter of the configured minimum and the
  negotiated interval.** The specification records upstream replenishing on the
  configured minimum regardless, so its own idle cap can hand a client a 2 s
  interval while the limiter enforces 5 s and rate-limits a fully conforming
  client. A server must not enforce a cadence it did not advertise. Ordinary
  configurations are unaffected: a 10 ms minimum against a 1 s negotiated
  interval still refills every 10 ms.
- **Zero means two different things and neither is "unlimited".** A zero minimum
  send interval is no time-based throttling; a zero burst allowance is no
  allowance at all, so every echo is rate-limited.
- **A rate-limited echo refreshes the idle deadline and advances no statistic.**
  This one *is* observed behavior — the only tested drop class that refreshes —
  and is kept deliberately.
- **The idle deadline runs from the open**, not from the first echo request. A
  session that never carries one still ages out; upstream's never-expiring
  unused session is a resource leak the specification recommends against.
- **Expiry is immediate and silent at the configured deadline.** No five-second
  additive grace, no final lazy-release reply to the first echo that finds an
  expired session, and no request-class-dependent "who consumed it" behavior.
  The only interoperability constraint is negative — expiry must never be
  signalled — and silence satisfies it. A zero idle timeout expires a session at
  the next evaluation; it is not "never expire".
- **Logical expiry is request-exact; physical reclamation also runs on the
  runtime's fixed maintenance cadence.** Every authenticated, structurally valid
  request runs the global sweep before dispatch, so a request at the deadline
  finds the session expired and a stale session cannot deny an open. The Tokio
  runtime invokes that same core sweep once per second so dead sessions do not
  remain resident when there is no further traffic. This cadence is internal
  housekeeping, not protocol policy or public configuration, and the runtime
  must not inspect session internals or duplicate the expiry rule.
- **The maximum-duration deadline is `first served echo + maximum + 2 s`.** The
  origin is measured behavior: neither the open, nor a rejected first echo, nor a
  rate-limited one starts it. The two-second grace matches the measured upstream
  margin and is a fixed internal constant, not a configuration knob.
- **Rate allowance is judged before the maximum-duration close.** A
  deadline-crossing echo with no allowance is dropped silently and the close is
  carried by the next echo that is served. Other rejected classes likewise do not
  trigger or defer it — the specification records that case as untested, so this
  is the deterministic reading rather than an observed one.
- **A server close is an ordinary echo reply with `FLAG_CLOSE` added**, carrying
  the triggering request's sequence number, statistics, timestamps and
  authentication. There is no standalone close packet in protocol version 1. The
  session is released once that reply has encoded, so every later request is an
  unknown token.

Every state transition — receive, rate and lifetime — is computed purely and
committed only after the reply has encoded, so an encoding failure leaves the
session exactly as it was and releases nothing.

## Resource policy

Total session and resource state must stay bounded. A single unauthenticated
datagram creates a session and opens are never deduplicated, so neither an
unbounded table nor a remotely chosen packet buffer is defensible. Upstream's
observed absence of session and per-peer bounds, and its unlimited default
packet length, are explicitly **not** compatibility targets and must not be
reproduced.

Configured bounds are trusted local policy: an operator may deliberately set an
absurd `max_sessions` or `max_packet_length`, and that is their choice. The
defaults are what must stay bounded, and no bound gets an unlimited setting.

Keep fallible preparation before irreversible state mutation: a reply is
prepared before a session is inserted, so an internal failure cannot leave a
half-created session.

## Testing

- Drive the core with real packets built by the production encoders. Do not
  maintain shadow encoders for well-formed wire data.
- Hand-build packets only where the point is a payload a compliant encoder
  cannot produce — truncated varints, out-of-range enums, corrupted MACs.
- Token generation and clock sampling are the only nondeterministic parts; tests
  inject a scripted source for each, so identity, collisions, allocation failure
  and timestamp values are all assertable. Keep both seams private, and keep
  timestamp, rate and lifetime tests free of sleeps, tolerances and real
  wall-clock assertions.
- Two clock fakes, for two different questions. A scripted clock returns a fixed
  list of samples and is for tests that assert individual timestamp *readings*; a
  hand-moved clock stands still until the test moves it, and is for rate and
  lifetime tests, which care when a datagram arrived and not how many times the
  core read the clock on the way. Prefer the latter for anything about deadlines.
- Assert rate and lifetime behavior from replies: which datagrams came back, the
  count and window they reported, and `session_count`. The token bucket is a
  black-box inference about the reference server, not a protocol requirement, so
  do not add accessors for allowance, activity instants or the maximum-duration
  origin.
- As the server gains behavior, prefer it over fake peers for normal compliant
  behavior in other crates' tests. Keep small raw/adversarial peers for
  intentionally malformed or non-compliant wire behavior.
