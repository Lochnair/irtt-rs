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
- For a single-clock midpoint, `irtt-rs` emits the one negotiated field. It does
  not reproduce upstream 0.9.1's dual-field midpoint; `irtt-proto` still decodes
  that form from a peer.
- Reply payload bytes are zero, which is the initial safe fill policy: payload
  carries no protocol meaning, and a server must never emit residue from another
  request or client. Request payload bytes never reach a reply.
- Rate limiting, idle expiry, maximum duration, server-initiated close, DSCP
  socket application and the full `ServerFill` policy are later slices. Do not
  add placeholder session fields for them.

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
  timestamp tests free of sleeps, tolerances and real wall-clock assertions.
- As the server gains behavior, prefer it over fake peers for normal compliant
  behavior in other crates' tests. Keep small raw/adversarial peers for
  intentionally malformed or non-compliant wire behavior.
