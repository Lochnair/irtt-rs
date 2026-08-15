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
- Do not add a blocking server, an alternate-runtime variant, a transport trait,
  a runtime abstraction layer, or a mirrored client-style API family. There is
  no product requirement for any of them, and `ServerCore` being public is not a
  promise of runtime independence.

## One listener and several

`Server` is the packet/runtime primitive and stays that: one socket, one core,
one sequential loop. `ServerSet` is the service around it and owns no protocol
or session state of its own — it binds listeners, supervises them and shuts them
down. Nothing it does is visible on the wire.

- **A set of one is an ordinary set.** Singleton sets are normal and supported,
  and the server CLI runs *every* invocation through one, single bind included.
  There is no bind-count branch to reintroduce: one orchestration path is what
  keeps startup, shutdown and failure semantics identical for one listener and
  for five, and what keeps the singleton case exercised in normal use.
- **Nothing is shared between listeners but configuration.** Each gets its own
  socket, `ServerCore`, session table, tokens, receive/rate/lifetime state,
  clock origin and traffic-class socket state. Do not add an
  `Arc<Mutex<ServerCore>>`, a process-global token allocator or session
  registry, or a shared clock: a token from one listener is an unknown token at
  the others, and that is the point rather than a limitation.
- **`ServerConfig` is cloned per listener, so every bound in it is per
  listener.** `max_sessions` of 100 across two listeners admits 100 each. Say so
  in the rustdoc, the CLI help and the README; do not add a process-wide session
  cap here.
- **Construction is all-or-nothing and completes before anything runs.** Bind
  every listener first, spawn nothing while iterating, and drop what was bound
  if a later one fails. Startup diagnostics belong after the whole set exists,
  so a set that never started cannot print that it is listening.
- **One task per listener, and the listener stays sequential inside it.** Do not
  spawn per datagram or add a second sender: one sender per socket is what makes
  the socket-wide traffic class race-free, and that invariant is unchanged by
  there being several sockets.
- **One external shutdown, fanned out internally.** The library knows nothing
  about Ctrl-C, signals or atomics. Every listener task is joined before `run`
  returns — no detached tasks, no `abort_all()` for ordinary shutdown, and a
  `JoinError` is never discarded.
- **A listener failure fails the set.** A listener that errors, or that returns
  before shutdown was requested, shuts its siblings down; the group drains and
  returns the first meaningful failure. A service configured for IPv4 and IPv6
  must not continue silently as IPv4 only. There is no error aggregation.
- **Same-port coexistence is settled before the bind, and only there.**
  `IPV6_V6ONLY` can only be set before binding, so a genuine IPv6 listener
  sharing an explicit port with an IPv4 sibling is created through a small
  `socket2` helper and handed to `Server::from_socket`. That helper owns nothing
  else: wildcard destination metadata, the core and traffic-class state stay
  with `Server`. No `SO_REUSEPORT`, no bind-order dependence, no unsafe code,
  and standalone `Server::bind` semantics are unchanged.
- **An IPv4-mapped address is an IPv4 listener here too**, exactly as it is for
  wildcard handling and the traffic-class option. It is never given
  `IPV6_V6ONLY` and never counted as the IPv6 half of a same-port pair.
- Listener sets are fixed at construction. Do not add dynamic add/remove,
  hostname or interface expansion, or per-listener configuration without
  agreement.

## Reply source address

A reply must leave from the address its request was sent to, or a client reading
from a connected socket discards it. An explicit-address listener satisfies that
by construction and asks for no destination metadata — do not give it any merely
because the helper exists. It keeps the plain `recv_from`/`send_to` path
everywhere except Linux, where it receives through `recvmsg` for the unrelated
reason below and still sends with plain `send_to`.

A wildcard listener does not, so it recovers each request's local destination
and sends that reply from it. The supported targets are Linux, macOS and
FreeBSD; a wildcard bind anywhere else fails at construction. Correct refusal is
the point: a listener that starts and then answers from a routing-table source
address fails invisibly, as loss.

- **Destination metadata is runtime transport state.** It never enters
  `ServerCore`, `Session` or `OutboundDatagram`. The core knows peers and
  sessions; which of the host's addresses a request arrived on is not session
  policy, and adding it there would make the core platform-shaped.
- **A wildcard listener must never process a datagram whose local destination it
  could not recover.** Missing destination metadata, `MSG_CTRUNC` and `MSG_TRUNC`
  all drop the datagram *before* the core, so no receive, rate or lifetime state
  moves for a request that cannot be answered correctly. There is no fall back
  to kernel source selection: the listener promised correct sources when it
  accepted the bind.
- Wildcard-ness is decided once, from the bound address, and never inferred from
  a peer.
- Source selection is **per-packet send metadata**; the traffic class stays the
  existing socket-wide setting applied before every send. Two transport
  mechanisms, deliberately: the marking policy is already implemented and tested
  across a wider target matrix, and sequential send ownership is what keeps it
  race-free. Do not migrate it into control messages merely because `sendmsg` is
  now in reach.
- `recvmsg` and `sendmsg` integrate through Tokio readiness — `readable()` /
  `writable()` with `try_io` — following the client's ancillary receive path.
  No blocking call, no `spawn_blocking`, no dedicated receive thread, and still
  one socket and one task.
- The crate forbids unsafe code and that is not negotiable for this feature. Use
  `nix`'s safe `recvmsg`/`sendmsg`, `ControlMessage`, `ControlMessageOwned` and
  `sockopt` wrappers; no raw syscall, no `CMSG_*` walking, no hand-written ABI
  layer. A target that cannot be served this way is refused, not excepted.
- The receive and send structures are not interchangeable, and the BSD IPv4
  options are not Linux's under other names. Build each direction's control
  message explicitly rather than echoing a received one back.
- **Pin the source address, not the route.** What the protocol requires is the
  address a reply leaves from; which interface carries it is the routing table's
  business. Naming the arrival interface on a send would lose replies on a host
  whose path back to the peer leaves by another one. The single exception is a
  link-local source, which is not unique across interfaces and must name its
  own — every other source, IPv4 included, goes out with no interface set.
- A send failure is per-packet loss, exactly as an ordinary `send_to` failure
  is. It never terminates the listener and never rolls back core state.

## Kernel receive timestamps

A Linux listener asks the kernel for a software receive timestamp
(`SO_TIMESTAMPNS`) on every datagram and reports it as transport metadata. It is
the mirror image of destination metadata, and the contrast is the point.

- **Optional, not required.** Enabling it is best-effort and must never fail
  construction: a listener without it answers every request exactly as it
  otherwise would. Destination metadata failing a wildcard bind is correct;
  timestamp setup failing anything is not.
- **Its absence is never packet loss.** No timestamp, a timestamp truncated away
  on an explicit listener, or one that describes no representable instant, all
  mean the datagram is served with `kernel_rx_timestamp: None`. Only the
  destination rules a datagram out, and only for the listener that needs one.
- **Nothing consumes it yet.** It stops at the runtime boundary. Do not thread it
  into `ServerCore`, add a timestamp argument or variant to `handle_datagram`,
  stash it in a "next receive" side channel, or wrap the core's clock. How a
  kernel-observed arrival time should enter the core is an open design question,
  to be settled deliberately and not as a side effect of some other change.
- **Do not map wall to monotonic.** It is a realtime reading with no monotonic
  counterpart, and `ClockSample` is a paired userspace instant. Synthesizing the
  missing half would invent precision the kernel never reported.
- **Do not judge plausibility here.** Conversion is structural only. Whether a
  representable reading is close enough to the server's own sample to measure
  against belongs with whoever measures.
- Linux only, and receive only. No macOS, FreeBSD or Windows implementation, and
  no transmit timestamping, without a reason and agreement.
- Control-buffer capacity is derived from the cmsg payload types and asserted at
  compile time. Adding a control message means extending that derivation — an
  undersized buffer sets `MSG_CTRUNC`, which is silent packet loss for a wildcard
  listener. Never parse cmsgs positionally or stop at the first match: the kernel
  orders them as it likes, and each kind is collected independently.

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
- Run the check after negotiation. That is what makes a configured timestamp
  allowance of none accept this same request: its effective `StampAt` is none, so
  the omitted clock selects nothing and the session is executable. See
  "Capability restrictions" below.
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
- Reply payload bytes come from the session's fill; see "Server fill" below.
  Request payload bytes never reach a reply under any mode.

## Server fill

The ServerFill *string* is wire data `irtt-proto` carries verbatim, and it
already enforces the 32-byte, UTF-8 and truncation rules. What a descriptor
*means* is server policy and lives in `fill`. Do not move descriptor parsing
into `irtt-proto`, and do not duplicate the decoder's checks here.

- **All three valid descriptor families are accepted**: `none`, `rand` and
  `pattern:HH…`. Mode names are case-sensitive; only a pattern's hexadecimal
  body is not. This is deliberately more permissive than the reference server's
  configurable glob allow-list, which is upstream policy rather than an
  interoperability requirement. Do **not** add glob, regex or allow-list
  configuration, and do not add server fill CLI knobs.
- **Absent or empty means no preference.** The negotiated params keep their
  representation exactly — absent stays absent, empty stays empty — while the
  session uses the server default internally. Writing the default descriptor
  into the reply would manufacture a restriction, which a strict client rightly
  rejects.
- **The default and fallback descriptor is `pattern:69727474`**, the bytes
  `irtt`. The descriptor and the bytes are separate constants; neither is
  derived from the other at run time.
- **An unknown or malformed explicit descriptor is replaced** in the negotiated
  params by that default descriptor, because the server really did change what
  was asked for. The client models it as `ServerFillChanged`; do not weaken
  client negotiation to hide it.
- **`none` zero-fills.** The clean evidence records the reference server leaving
  the region as residual buffer content, returning other clients' bytes. Zero is
  produced by handing the encoder an empty payload, not by copying zeroes over
  an already-zeroed packet.
- **Pattern phase resets for every reply.** Upstream's observed cross-packet and
  cross-listener continuity is explicitly not required of a conforming server,
  and per-packet reset keeps the server free of global mutable fill state and
  makes replies deterministic. There is no shared phase between sessions or
  listeners, and adding one would be a regression.
- **`FillMode` belongs to the `Session`**, parsed once at open. Never re-derive
  it from `params` later: that would lose the absent-descriptor-with-default-
  behavior distinction. Keep the type crate-private.
- **Fill is generated before the reply is encoded**, like every other part of an
  echo, so the packet's HMAC covers the fill bytes through the ordinary encoder
  path and no session state commits before encoding succeeds. Never mutate a
  payload after encoding.
- **A random-source failure zero-fills that one payload and is not fatal.** It
  must never become a `ServerError`, terminate `Server::run` or drop the
  session. The bytes exist to vary the payload and are not security state;
  session tokens keep their own source.
- Payload length comes from `irtt-proto`'s layout and `echo_packet_len`, never
  from `params.length`, and needs no second packet-size bound of its own.

## Capability restrictions

Two optional negotiation policies restrict what a session may ask this server to
*provide*. Both default to off, so a configuration that sets neither negotiates
exactly what it did before they existed. Both live in `negotiate`, with the rest
of open negotiation, and neither reaches the runtime.

- **`TimestampAllowance` is server policy, and deliberately not `StampAt`.** The
  wire enum describes what a session requests and negotiates; the allowance
  describes what this server is willing to hand out. Keep the two types apart,
  and keep the allowance out of `irtt-proto`.
- The default is `Dual`, which honors every requested placement. `None` maps every
  non-none placement to none. `Single` maps `Both` to **`Midpoint`** and leaves
  every already-single placement alone — that one substitution is the only
  interesting row; the rest is in the clean spec's Section 11.4.
- **`Clock` is never rewritten.** The evidence describes an allowance on timestamp
  *placement*, not on clock domains, and the echo layout already carries no
  timestamp field once the placement is none. So `Single` bounds reported
  *instants*, not field count: on `Clock::Both` a negotiated midpoint still emits
  a wall and a monotonic field. Do not document it as one field. Rewriting the clock would answer
  with a session the client did not ask for.
- **Restriction runs before the effective-session executability check**, and that
  ordering is load-bearing: it is what makes a request selecting timestamps with
  an omitted Clock *safe* under a none allowance, where the same request is
  refused under the default. Do not move the check onto the requested params.
- **DSCP permission defaults to allowed.** When disallowed, negotiation sets
  `Params.dscp` to zero before the reply is encoded and before the `Session`
  exists, for every requested value including ones outside `0..=255`. The open is
  never refused over DSCP, and nothing is clamped or wrapped.
- **No second policy bit anywhere.** The negotiated params are the single source
  of truth: the session stores zero, `OutboundDatagram`'s traffic class derives
  from it as for any other zero-DSCP session, and the runtime never learns that
  `ServerConfig::dscp_allowed` exists. `Params::encode` omits a zero integer, so
  the reply simply carries no DSCP tag.
- **This is operator policy, not socket capability detection.** What a given host
  can apply to a socket is settled per send by the runtime (see "Reply traffic
  class"). Do not couple `ServerCore` negotiation to `socket2` or to runtime
  capability state.

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
  bound address family rather than the peer's. **On Linux an IPv4-mapped bound
  address is an IPv4 listener**, whatever shape it wears: the socket is
  `AF_INET6`, but the kernel binds it to the IPv4 side and `IPV6_TCLASS` does not
  reach the packets it emits — it leaves their TOS at zero and the negotiated
  marking is lost, while `IP_TOS` on that same socket marks them. macOS rejects
  `IP_TOS` on an `AF_INET6` socket outright and marks nothing either way, so it
  keeps the IPv6 option: reading a mapped bind as IPv4 there would turn an
  unmarked reply into a *dropped* one, because an unappliable marking stops the
  send. Verify a platform before extending that list.
- A genuinely dual-stack `[::]` listener still emits **unmarked** IPv4-mapped
  replies, and this is known. It is not fixed by choosing the option per reply,
  for the macOS reason above, and it predates wildcard source selection. Leave it
  until it is worth its own slice, and do not "fix" it by weakening the rule that
  an unappliable marking stops the send. The crate forbids unsafe code;
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
