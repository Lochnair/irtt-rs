# `irtt-proto` guidance

Repository-wide guidance in the root `AGENTS.md` also applies here.

`irtt-proto` is the pure protocol boundary. Keep it free of sockets, runtimes, client/server lifecycle, statistics, and presentation concerns.

For protocol behavior, consult before changing semantics:

- `docs/protocol/IRTT_CLIENT_PROTOCOL_SPEC.md`
- `docs/protocol/BLACKBOX_VERIFICATION_REPORT.md`
- `docs/protocol/test-vectors/README.md`

Focused unit and table-driven tests are explicitly encouraged in this crate. Exact wire vectors, layout rules, flag combinations, malformed inputs, boundary values, HMAC behavior, and encode/decode round trips are durable compatibility coverage rather than test-count inflation.

Prefer extending an existing table/vector family when it naturally covers the new case instead of creating many one-off tests. Do not duplicate an exact vector or invariant solely for branch coverage.

Never derive protocol behavior from upstream `heistp/irtt` source code or upstream tests. If the local verified specification is insufficient, identify the ambiguity rather than guessing or consulting upstream implementation code.
