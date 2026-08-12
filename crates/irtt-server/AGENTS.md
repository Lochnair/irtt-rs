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

## Resource policy

Total session and resource state must stay bounded. A single unauthenticated
datagram creates a session and opens are never deduplicated, so an unbounded
table is a liability. Upstream's observed absence of session and per-peer bounds
is explicitly **not** a compatibility target and must not be reproduced.

Keep fallible preparation before irreversible state mutation: a reply is
prepared before a session is inserted, so an internal failure cannot leave a
half-created session.

## Testing

- Drive the core with real packets built by the production encoders. Do not
  maintain shadow encoders for well-formed wire data.
- Hand-build packets only where the point is a payload a compliant encoder
  cannot produce — truncated varints, out-of-range enums, corrupted MACs.
- Token generation is the only nondeterministic part; tests inject a scripted
  source so identity, collisions and allocation failure are assertable. Keep
  that seam private.
- As the server gains behavior, prefer it over fake peers for normal compliant
  behavior in other crates' tests. Keep small raw/adversarial peers for
  intentionally malformed or non-compliant wire behavior.
