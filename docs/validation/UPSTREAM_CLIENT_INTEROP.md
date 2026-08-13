# Upstream IRTT 0.9.1 Client Interoperability

## Scope

The released upstream IRTT 0.9.1 client was run as a **black box** against the
Tokio `irtt_server::Server` runtime. Only the executable's reported version, its
command-line help, its exit status and its normal stdout/stderr were observed.
Upstream implementation source, tests and repository history were **not**
inspected, and no upstream behavior was inferred beyond what the running program
printed.

- Tested against `irtt-rs` base commit `ace3defe22e71980f18ccdce1ef783d630e070cf`.
  No implementation change was required, so the validated behavior is that of the
  unmodified baseline.
- Listeners were **explicit loopback binds only** (`127.0.0.1:0`, `[::1]:0`).
  Wildcard binds were not exercised.
- DSCP marking, the full `ServerFill` policy and wildcard/multi-homed
  source-address (`pktinfo`) handling are unimplemented and out of scope here.
- This document records implementation validation. It creates no protocol
  requirements and does not amend the airlocked protocol evidence.

The server was exposed through a small throwaway Cargo harness kept outside this
repository. It only built a `ServerConfig`, called `Server::bind`, printed the
bound `SocketAddr` and called `Server::run`; it contains no protocol handling and
is deliberately not committed.

## Environment

| Item | Value |
| --- | --- |
| Date | 2026-08-13 |
| OS | macOS 26.5.2 (Darwin 25.5.0) |
| Architecture | arm64 (Apple silicon) |
| Rust toolchain | rustc 1.97.1, cargo 1.97.1 |
| Upstream reported version | `irtt version: 0.9.1`, protocol version 1, JSON format version 1 |
| Upstream executable SHA-256 | `35df8b2b2c10fe81a222eaac2fa7461891fa2354f74c1e9b379c3c31f43c61cc` |
| How obtained | Pre-existing installation already on `PATH`; version confirmed with `irtt version`. No isolated build was needed and no upstream source was fetched. |
| `irtt-rs` base SHA | `ace3defe22e71980f18ccdce1ef783d630e070cf` |

Flags used were taken from the executable's own `irtt help client` output:
`-d`, `-i`, `-l`, `-n`, `-4`, `-6`, `--hmac`, `--loose`, `--timeouts`,
`--tstamp`, `--clock`.

`--loose` is upstream's documented client-side switch for accepting
server-restricted test parameters instead of exiting non-zero. Restriction
scenarios were therefore run **both** ways: without it to confirm the client
detects and reports the restriction, and with it to confirm the restricted
session actually runs.

## Matrix

Server policy is `ServerConfig::default()` unless stated. All runs used numeric
loopback addresses and no DNS.

| Scenario | Server policy | Client request | Result | Notes |
| --- | --- | --- | --- | --- |
| IPv4 ordinary | default | `-4 -d 1s -i 100ms -l 128` | **PASS** | 10/10 packets, 0.00% loss, exit 0 |
| HMAC | `with_hmac_key("interop-secret")` | `--hmac=interop-secret -d 500ms -i 100ms -l 128` | **PASS** | 5/5 packets, 0.00% loss, exit 0 |
| HMAC, wrong key (negative control) | `with_hmac_key("interop-secret")` | `--hmac=wrong-secret --timeouts=200ms,200ms` | **PASS** | Silently refused; client reports `[OpenTimeout] no reply from server`, exit 1; server unaffected |
| IPv6 | default, bound `[::1]:0` | `-6 -d 1s -i 100ms -l 128` | **PASS** | 10/10 packets, 0.00% loss, exit 0 |
| Packet-length restriction | `with_max_packet_length(96)` | `-l 128` (strict) | **PASS** | `[ServerRestriction] server reduced length from 128 to 96`, exit 1 — upstream's documented strict-mode policy |
| Packet-length restriction | `with_max_packet_length(96)` | `-l 128 --loose` | **PASS** | Runs at 96 bytes, 10/10 packets, 0.00% loss, exit 0 |
| Interval restriction | default (10 ms floor) | `-i 1ms -d 500ms --loose` | **PASS** | `server increased interval from 1ms to 10ms`; 50/50 packets, 0.00% loss — the rate limiter never penalised a client sending at the interval it was given |
| Maximum-duration Close | `with_max_test_duration(500ms)` | `-d 5s -i 100ms --loose` | **PASS** (restriction) | `server reduced duration from 5s to 500ms`; 5/5 packets, exit 0. The Close-flagged reply itself is **not reachable** by a conforming client — see Findings. |
| Continuous request | `with_max_test_duration(500ms)` | `-d 0` | **SKIPPED** | Upstream rejects it client-side (`[DurationNonPositive] duration (0s) must be > 0`); a continuous request never reaches the wire from this CLI |
| No-test (optional) | default | `-n -l 128` | **PASS** | `[NoTest] skipping test at user request`, exit 0, no measurement session |
| Midpoint, single clock (optional) | default | `--tstamp=midpoint --clock=monotonic` | **PASS** | 5/5 packets, exit 0; RTT and IPDV computed from the single midpoint field |
| Midpoint, wall only (optional) | default | `--tstamp=midpoint --clock=wall` | **PASS** | 3/3 packets, exit 0 |
| Midpoint, both clocks (optional) | default | `--tstamp=midpoint --clock=both` | **PASS** | 3/3 packets, exit 0 |

In every scenario the harness process was still alive after the client exited
and shut down cleanly on `SIGTERM` (`Server::run` returned `Ok`).

## Findings

No implementation incompatibility was found in the tested matrix, and no code
change was made.

Two observations are worth recording.

**The single-field midpoint reply is accepted.** `irtt-rs` emits one timestamp
field per negotiated clock for `StampAt::Midpoint`, deliberately not reproducing
upstream 0.9.1's dual-field midpoint form (see `crates/irtt-server/AGENTS.md`).
The released upstream client parsed that reply without complaint and derived RTT
and send/receive IPDV from it, in the monotonic-only, wall-only and both-clocks
cases. This was the tested path most likely to expose a reply-shape
incompatibility, and it did not.

**The server-initiated maximum-duration Close is not reachable by a conforming
client.** The deadline is `first served echo + configured maximum + 2 s`, while
negotiation reduces a session's Duration to at most that same configured maximum.
A client that honours the Duration it was given therefore always stops sending at
least two seconds before the deadline, so no echo ever arrives to carry the
Close flag. That was confirmed empirically: with a 500 ms maximum against a
requested 5 s test, the upstream client accepted the restriction, sent for
~400 ms and exited 0 without a Close ever being due.

This is not a defect and no fix is authorised for it. The two-second grace is
recorded in the airlocked evidence as the measured upstream margin and is a fixed
internal constant; the implementation matches that evidence rather than violating
it. The consequence is simply that the Close is a defence against a client that
overruns its negotiated Duration, not part of an ordinary session. Upstream's
CLI offers no way to build such a client — `--loose` accepts and uses the
restricted parameters, and a continuous request is rejected before it reaches the
wire. The Close path itself remains covered deterministically in-repo by
`crates/irtt-server/src/tests/lifecycle.rs`, notably
`the_first_echo_past_the_maximum_duration_carries_close_and_ends_the_session`.

## Scope remaining

This validation does **not** establish:

- DSCP response marking;
- full `ServerFill` behavior (echo payloads remain zero-filled);
- wildcard or multi-homed source-address handling (`pktinfo`);
- behavior under pathological or exhaustive parameter combinations;
- behavior on operating systems other than the macOS/arm64 host recorded above;
- behavior across a real network path, MTU limits or fragmentation.

No claim of universal compatibility is made. No permanent test in this repository
depends on the upstream executable, the Go toolchain or network access; this
validation is intentionally manual and reproducible from the commands above.
