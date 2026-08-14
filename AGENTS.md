# Repository guidance

Repository-wide guidance for contributors and coding agents working on `irtt-rs`.
Before editing, also check for a more specific `AGENTS.md` in the target subtree.

## Clean-room boundary

`irtt-rs` is an independent Rust implementation of an IRTT-compatible protocol stack. It began as a clean-room client reimplementation and now contains project-specific architecture and behavior beyond that original compatibility baseline.

- Do not inspect, quote, copy, translate, or derive implementation details from upstream `heistp/irtt` source code or upstream tests.
- Do not use contaminated notes or source-derived pseudocode as implementation input.
- Use the local clean-room specification, black-box verification artifacts, current `irtt-rs` code, and current tests.
- Black-box interoperability testing against an installed upstream `irtt` binary is allowed when relevant. Treat it as an external implementation, not source material.
- If behavior remains ambiguous, state the ambiguity instead of guessing or consulting upstream implementation code.

Compatibility and interoperability references:

- `docs/protocol/IRTT_CLIENT_PROTOCOL_SPEC.md` — verified client-side
  compatibility baseline.
- `docs/protocol/IRTT_SERVER_PROTOCOL_SPEC.md` — verified server-side
  compatibility baseline.
- `docs/protocol/BLACKBOX_VERIFICATION_REPORT.md` — black-box evidence and
  corrections supporting both the client and server baselines.
- `docs/protocol/test-vectors/README.md` — captured client-facing packet
  vectors.
- `docs/protocol/test-vectors/SERVER_BEHAVIORAL_VECTORS.md` — black-box
  server behavioral vectors, edge cases, and session behavior.
- `docs/protocol/captures/` — packet captures referenced by the verification
  report and behavioral vectors. Behavioral vectors and captures are
  evidence/artifacts, not architecture prescriptions.
- `docs/INTEROP_COMPARISON.md` — procedures for black-box comparison against
  the upstream `irtt` executable.
- `docs/clean-room/CLEANROOM_NOTES.md` — provenance only; not a normative
  protocol specification.

The verified client spec predates later `irtt-rs` architecture and project-specific features. Its scope statements, including server implementation being out of scope, describe that document's scope and are not permanent repository restrictions.

For compatibility questions, the verified spec and black-box observations define the baseline. For `irtt-rs`-specific APIs, runtime architecture, resource bounds, and later features, current code and current project documentation are authoritative.

## Architecture

Main boundary:

> `irtt-proto` — wire semantics shared by client/server. `irtt-client` — client/session behavior. `irtt-server` — server/session behavior and Tokio runtime/orchestration. `irtt-stats` — statistics over client-facing events/results as currently designed. `irtt-cli` — presentation/orchestration.

- `irtt-proto`: pure protocol encoding/decoding, flags, layouts, params, varints/zigzag, HMAC-MD5, validation. No sockets, runtimes, lifecycle, stats, or presentation.
- `irtt-client`: socket/session lifecycle, negotiation, probe send/receive, timing metadata, classification, bounded state, and events.
  - `Client`: runtime-free blocking/manual adapter.
  - `AsyncClient`: optional Tokio low-level adapter.
  - `ManagedClientTask` / `ManagedClientHandle`: unified Tokio managed driver/control surface.
  - `BlockingManagedClient`: synchronous owner using a dedicated current-thread Tokio runtime.
- `irtt-server`: exists; see "Server architecture (`irtt-server`)" below.
- `irtt-stats`: cumulative/rolling/finite statistics over client events. No wire/session state machine logic.
- `irtt-cli`: argument parsing, managed orchestration, output, summaries, TUI, server applet orchestration, diagnostics, and exit behavior. Do not duplicate protocol/session state machines here.

Tokio must remain optional for `irtt-client`; default builds must remain runtime-free and Tokio-free unless an explicit architectural change says otherwise. This rule is specific to `irtt-client` — do not generalize it to `irtt-server`, which has the opposite policy below.

### Server architecture (`irtt-server`)

`irtt-server` is a first-class reusable server crate. It contains the deterministic OPEN/ECHO/session core — packet admission, authentication policy, open negotiation, the bounded session table, open reply construction, echo processing with its per-session receive state and timestamps, per-session rate limiting, session lifetime with idle expiry, the maximum-duration server-initiated close, and client-initiated close — plus a reusable Tokio UDP `Server` that runs one sequential `ServerCore` per listener with caller-controlled shutdown and scheduled idle maintenance. Logical expiry remains exact on authenticated, structurally valid requests, while the runtime also reclaims expired sessions once per second when no traffic arrives. Each reply carries the raw traffic class it must be sent with, which the runtime applies to the listener before every send. A wildcard listener additionally recovers each request's local destination from packet metadata and sends that request's reply from it, on Linux, macOS and FreeBSD; a wildcard bind on a target without that path is refused at construction rather than served from a routing-table source address. Explicit-address listeners are unaffected everywhere. Echo replies fill their payload from the session's negotiated server fill; the policy and its deliberate divergences are in `crates/irtt-server/AGENTS.md`. Two optional negotiation policies restrict what a session may ask the server to provide — a timestamp allowance and DSCP permission — both off by default and both settled during open negotiation rather than in the runtime. Serving multiple listeners in one process is not implemented; that, and ordinary process-level polish and observability decisions, is what remains of the server architecture.

- It shares wire semantics with the client through `irtt-proto` and follows the same clean-room boundary.
- It must **not** depend on `irtt-client` merely to reuse protocol behavior.
- Unlike `irtt-client`, the server has no product requirement to remain runtime-free: `irtt-server` is intentionally **Tokio-native**, and Tokio does not need to be optional for it.
- There is currently no requirement for a blocking server API, alternate async runtimes, a runtime abstraction layer, or a runtime-free server feature. Do not build blocking/runtime variants merely for symmetry with the client, and do not invent a mirrored client-style API family (`BlockingServer`, `AsyncServer`, `ManagedServer`, `BlockingManagedServer`, or similar) ahead of agreement.
- Protocol/session logic should be separated from socket/runtime orchestration where that materially improves deterministic testing, ownership clarity, state-machine reasoning, or maintainability. A `ServerCore`-like internal boundary is therefore desirable, but its exact name/API is not prescribed, it need not be public, it is not a promise of runtime independence, and simple code should not be tortured merely to make the core artificially pure.
- Runtime/server orchestration belongs around that core and may freely use Tokio for UDP sockets, `recv_from`/`send_to`, IPv4/IPv6 listeners, timers, session expiry, max-duration deadlines, shutdown, bounded control/config channels where useful, and `select!`-style orchestration.
- The CLI server applet exists behind the `irtt-cli` `server` feature, which is part of that crate's default build. It is thin orchestration/configuration over reusable `irtt-server` — one current-thread Tokio runtime, one explicitly bound listener, one `Server` — and must not grow a second server state machine. Multi-listener orchestration, hostname resolution, daemonization, and configuration files are deliberately absent.
- Resource policy: the server should use bounded session/resource policy. Upstream's observed lack of useful default session/per-peer bounds, and its never expiring a session that has not carried an echo request (see `IRTT_SERVER_PROTOCOL_SPEC.md`), are not interoperability requirements and must not be reproduced merely for compatibility.
- Testing: prefer `irtt-server` for normal compliant fake-peer behavior where practical, as far as its implemented behavior reaches. Keep narrow adversarial/raw peers for intentionally malformed, ambiguous, or non-compliant wire behavior.

## Engineering rules

- Make the smallest coherent change that solves the requested problem.
- Avoid unrelated cleanup and speculative abstractions.
- Preserve public API and wire behavior unless the task explicitly changes them.
- Treat resource bounds, wake behavior, blocking, cancellation, cleanup, and lifecycle termination as correctness concerns in long-running code.
- Keep fallible preparation before irreversible side effects where transaction semantics require it.
- Do not suppress compiler or Clippy warnings merely to make checks green.
- If an apparent defect may encode an intentional design decision, inspect surrounding code/docs/history and ask rather than silently redesigning it.

## Testing policy

Tests protect durable behavior, compatibility, and important invariants. They do not exist to maximize test count or coverage percentage.

### Prefer behavior over internals

For client, managed-runtime, blocking, CLI, and TUI behavior, prefer normal-interface component/integration tests: real UDP/session exchange, emitted events/outcomes, negotiated params, multi-target lifecycle, process exit status, and user-visible diagnostics.

One strong behavioral test is usually better than several tests of private helpers that reconstruct the same behavior.

Do not add persistent tests merely because implementation code changed. Avoid tests that primarily:

- assert private fields, cursors, queue shapes, or exact internal representation;
- mirror implementation steps line-for-line;
- exercise branches/helpers solely for coverage;
- duplicate stronger coverage at another layer;
- pin incidental scheduling/ordering that is not contractual;
- test standard-library/dependency behavior;
- require production test-only observability for a fact naturally visible through packets/events/outcomes.

A harmless private refactor should not require widespread test rewrites unless a deliberately protected invariant changed.

### Unit tests where they earn it

Focused unit/table-driven tests are encouraged for stable pure or algorithmic behavior, especially:

- `irtt-proto` layouts, codecs, vectors, HMAC, parsing, malformed input, and boundaries;
- `irtt-stats` formulas, loss accounting, IPDV, rolling windows, and bounded state;
- hand-rolled data structures and subtle pure invariants;
- resource/liveness guarantees that cannot be observed reliably from a higher layer.

If a unit test and a normal-interface test prove the same durable behavior, prefer the normal-interface test unless the unit test materially improves precision or diagnosis.

### Temporary implementation tests

Temporary narrow tests are welcome while implementing/debugging a change to prove a hypothesis, reproduce a bug, or exercise a difficult state transition.

Before finalizing, remove or consolidate them unless they independently protect a durable regression or invariant. A test being useful during development does not by itself justify permanent repository code.

### Resource/liveness tests and hooks

Direct low-level tests and test-only counters are justified when the invariant is otherwise not observable, e.g. bounded work per poll, no starvation/eventual progress, avoiding an otherwise O(n) hot-path operation, or exact cancellation/stop/update linearization.

Assert the invariant rather than the current mechanism used to achieve it. Prefer “bounded work and eventual progress” over a specific private cursor value when another fair implementation would also be valid.

`#[cfg(test)]` fault injection is acceptable for conditions that cannot be produced reliably through normal interfaces, such as socket-option failures, short sends, false readiness, exact cancellation points, sequence wrap, or race linearization. Do not add test-only production hooks solely for convenient observability.

### Fake peers and interoperability

Use production protocol encoders/decoders for compliant fake peers. Do not maintain shadow packet encoders in tests unless intentionally constructing malformed wire data.

Prefer a real `irtt-server` for normal compliant behavior once available; keep small adversarial peers for behavior a compliant server should not produce.

Cross-implementation tests are especially valuable because two `irtt-rs` endpoints can otherwise agree on the same mistake.

When compatibility is in question, prefer black-box interoperability testing
against an upstream `irtt` executable where practical. Follow
`docs/INTEROP_COMPARISON.md` and treat upstream only as an external executable;
do not inspect its source or tests.

### Synchronization and timing

Prefer synchronization on actual events/state over sleeps or tiny wall-clock windows.

- Use channels, condvars, packet gates, or equivalent deterministic signals with bounded failure timeouts.
- Sleeps are fine when oversleeping cannot invalidate correctness; do not sleep merely hoping another thread/process finished work.
- Avoid wall-clock assertions that include process spawn or unrelated setup time.
- Condition-wait loops must be bounded and fail with a useful diagnostic rather than hang.

When a test fails after a legitimate implementation change, determine whether production regressed or the harness encoded an invalid assumption. Do not change correct production behavior merely to satisfy a brittle test.

## Workflow and verification

For non-trivial work:

1. Inspect relevant code, tests, and local protocol/design references.
2. Identify the externally meaningful contract or internal invariant.
3. Make a short plan for multi-file or subtle lifecycle/state changes.
4. Implement the smallest coherent patch.
5. Run focused verification while iterating.
6. Review the final diff for accidental scope, brittle tests, and unnecessary test hooks.
7. Run broader verification appropriate to the risk.

Skip planning ceremony for tiny obvious edits. Do not add unrelated tests, abstractions, docs, or refactors merely because they are nearby.

Use the narrowest useful checks first. For substantive workspace changes, final verification should normally include:

```sh
cargo fmt --all -- --check
cargo check --workspace --all-targets --all-features --locked
cargo test --workspace --all-targets --all-features --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
```

Also verify `--no-default-features` when changing feature gates, optional dependencies, runtime boundaries, or code intended to build without Tokio.

Interop tests require an external `irtt` binary and should only run when the task concerns interoperability or explicitly requests them. Do not run repetition loops, fuzzing, Miri, Loom, mutation testing, or similar expensive checks as routine ceremony.

Never claim a command passed unless it actually ran successfully.

## Repository hygiene

- Do not use destructive Git operations unless explicitly requested.
- Do not push, force-push, create/update a PR, or rewrite unrelated history unless explicitly requested.
- Keep commits scoped to the requested work and preserve unrelated local changes.

## Completion

Report concisely:

1. what changed;
2. verification actually run and its result;
3. meaningful tests added/updated/consolidated/removed;
4. remaining risks, ambiguities, or follow-ups.

Do not produce a giant changelog of incidental edits.
