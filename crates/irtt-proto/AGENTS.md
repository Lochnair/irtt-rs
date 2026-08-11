# `irtt-proto` guidance

Repository-wide guidance in the root `AGENTS.md` also applies here.

`irtt-proto` is the pure protocol boundary. Keep it free of sockets, runtimes, client/server lifecycle, session tables, statistics, and presentation concerns.

As `irtt-server` is built, it may need server-facing wire-inspection
primitives here, such as request decoding, reply encoding, packet/header
validation, HMAC-presence inspection, and token/header inspection before
Params-dependent decoding.

For protocol behavior, consult before changing semantics:

- `docs/protocol/IRTT_CLIENT_PROTOCOL_SPEC.md`
- `docs/protocol/IRTT_SERVER_PROTOCOL_SPEC.md`
- `docs/protocol/BLACKBOX_VERIFICATION_REPORT.md`
- `docs/protocol/test-vectors/README.md`
- `docs/protocol/test-vectors/SERVER_BEHAVIORAL_VECTORS.md`

Protocol changes that affect server-side parsing, encoding, or session-facing wire semantics must consult the server spec and behavioral vectors as well as the client-side material above.

Focused unit and table-driven tests are explicitly encouraged in this crate. Exact wire vectors, layout rules, flag combinations, malformed inputs, boundary values, HMAC behavior, and encode/decode round trips are durable compatibility coverage rather than test-count inflation.

Prefer extending an existing table/vector family when it naturally covers the new case instead of creating many one-off tests. Do not duplicate an exact vector or invariant solely for branch coverage.

Never derive protocol behavior from upstream `heistp/irtt` source code or upstream tests. If the local verified specification is insufficient, identify the ambiguity rather than guessing or consulting upstream implementation code.
