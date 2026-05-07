# Clean-Room Implementation Notes

## Purpose

This document records the clean-room boundary for the IRTT client
reimplementation project. The goal is to ensure that the implementation
is developed from a behavioral/protocol specification only, without exposure
to the GPL-licensed upstream source code.

## Clean-Room Boundary

- **Contaminated side:** The specification agent (this side) inspected the
  upstream source code, documentation, tests, CLI behavior, and release
  notes to understand the IRTT protocol.

- **Clean side:** The implementation agent will receive only the
  `IRTT_CLIENT_PROTOCOL_SPEC.md` document. The implementation agent MUST NOT
  see the upstream source code.

## Source Materials Inspected

The following materials from the upstream IRTT repository (heistp/irtt,
GPLv2) were inspected to produce the protocol specification:

1. **Documentation files:**
   - `README.md` — project overview, FAQ, features, limitations
   - `doc/irtt.md` — main man page
   - `doc/irtt-client.md` — client man page with CLI options and JSON output format
   - `doc/irtt-server.md` — server man page (referenced, not fully read)
   - `CHANGES.md` — changelog

2. **Source files (inspected for protocol behavior only):**
   - Protocol version and constants
   - Packet format, field layout, magic bytes, flags
   - Parameter serialization format (tag-value pairs, varint encoding)
   - Connection lifecycle (open, echo, close handshakes)
   - Client configuration and defaults
   - Server connection handling and parameter restriction logic
   - HMAC computation
   - Timestamp handling and clock types
   - Received stats (count and window) format
   - Round-trip data recording and measurement calculations
   - Result computation (RTT, OWD, IPDV, loss)
   - Error codes and error handling
   - Packet test vectors (serialized bytes for known inputs)
   - Network layer (IP version, DSCP, socket options)
   - Wait/drain behavior

3. **Test files:**
   - Packet serialization tests (used to derive test vectors only; test
     code itself was not copied)

## Statement of Non-Copying

No upstream source code, comments, or pseudocode were copied into the
specification document (`IRTT_CLIENT_PROTOCOL_SPEC.md`). The specification
describes only externally observable protocol behavior, wire format, and
interoperability requirements.

Test vectors in the specification were derived from the known inputs and
expected byte sequences found in the upstream test suite. Only the input
parameters and resulting byte arrays were used; no test logic or code was
copied.

Executing a locally installed `irtt` binary is allowed for black-box
compatibility testing, including optional real-backend tests selected with
`IRTT_TEST_BACKEND=real`. The binary must be treated only as an executable
black box; upstream source code, tests, comments, or implementation notes must
not be inspected or copied.

## Post-Drafting Audit

After the initial specification was drafted, a clean-room audit was
performed. The following issues were identified and corrected:

1. **Upstream private type names leaked:** "ctoken" and "seqno" appeared
   as parenthetical abbreviations in the terminology table. Removed.

2. **Upstream implementation preference disclosed:** A note about IPv4
   preference in dual-stack resolution revealed an upstream implementation
   choice. Removed.

3. **Go-specific function references:** References to `binary.PutUvarint`,
   `binary.PutVarint`, and `encoding/binary` package named the upstream
   language's standard library. Replaced with language-neutral descriptions
   (LEB128, protobuf-style zigzag).

4. **Internal scheduling algorithm described:** The send scheduling section
   described a specific "snap-to-interval" drift-correction algorithm that
   mirrored upstream control flow. Replaced with observable behavior
   description (packets sent at approximately ideal times, drift
   compensation is implementation-defined).

5. **Upstream timer algorithm named:** "The upstream uses exponential
   averaging of timer errors" revealed an internal implementation choice.
   Removed.

6. **RTT edge case speculated:** The spec asserted that server processing
   time exceeding raw RTT should cause subtraction to be skipped — a guard
   the upstream does not actually implement. Moved to Open Questions.

7. **Received window validity asserted normatively:** The behavior of
   window=0 as an invalidity sentinel was stated as fact when it should be
   verification-required. Softened with reference to Open Questions.

8. **Go concurrency term leaked:** "goroutine" appeared in the result model
   section. Replaced with "operation."

9. **Upstream test design referenced:** Section 18.2 described what "the
   upstream test" was testing internally. Rewritten to describe the test
   vector neutrally.

10. **Upstream constants asserted normatively:** The minimum restricted
    interval (1 second) and maximum parameter buffer size (128 bytes) are
    internal constants that were stated as protocol requirements. Moved to
    Open Questions with verification suggestions.

11. **Timestamp capture order stated as MUST:** Implementation-level timing
    advice was stated as a protocol requirement. Changed to SHOULD with
    measurement-accuracy rationale.

12. **Internal recording structure described:** Section 19.6 described a
    "pre-send timestamp" used for send-call-time measurement, which is an
    internal recording detail. Rewritten to focus on observable behavior.

13. **Self-contradictory correction note:** Section 8.2 contained an inline
    "Correction:" note that was confusing and revealed the drafting process.
    Cleaned up.

14. **Field ordering lacked verification note:** The critical field order
    in Section 8.1.3 was derived from source and stated without any
    verification caveat. Added verification note and new Open Question 19.11.

## Clean-Room Compliance Checklist

- [x] No upstream source code included.
- [x] No upstream comments quoted.
- [x] No upstream private function names included.
- [x] No upstream private type names included.
- [x] No upstream file/module layout described.
- [x] No pseudocode derived from upstream implementation.
- [x] No implementation-language API design included.
- [x] No Go-specific language references remain.
- [x] No upstream implementation preferences or algorithm choices disclosed.
- [x] Protocol behavior described independently.
- [x] Source-derived constants marked for verification.
- [x] Ambiguous behavior marked for verification (Section 19).
- [x] Black-box tests proposed where needed (Section 17).
- [x] Post-drafting audit completed and documented (this section).
- [x] Second-pass scrub completed (see below).

## Second-Pass Clean-Room Scrub

A second independent review was performed on 2026-04-28 by an agent that
had not seen the upstream source. The following issues were identified and
corrected:

1. **Implementation architecture directive in scope:** Section 2 stated
   "Library-first implementation target," which prescribes internal
   architecture. Removed; the spec now says only that CLI design is not
   prescribed.

2. **Concurrency architecture leaked:** Section 6.3 described "a separate
   receive path processes incoming reply packets concurrently," which
   prescribes an internal threading/task model. Rewritten to state the
   observable requirement (send and receive occur concurrently) without
   prescribing how.

3. **Source-derived design rationale in echo request:** Section 8.4
   explained that zeroed fields exist to "use the same field layout that
   appears in echo replies," which reveals internal design reasoning.
   Simplified to state only the observable purpose (reaching the
   negotiated packet length).

4. **Source-exhaustive knowledge parenthetical:** Section 8.6 said
   "(currently only ServerFill)" for string-typed values, revealing
   knowledge of the complete parameter set from source inspection.
   Removed the parenthetical.

5. **Upstream CLI syntax leaked:** Section 10.10 used `NxD`/`NrD`/`D`
   notation for wait variants, which mirrors upstream CLI option syntax.
   Replaced with plain-language descriptions.

6. **Algorithm name disclosed:** Section 12.9 named "Welford's online
   algorithm" as a MAY recommendation. This is implementation guidance.
   Replaced with "the method of computing running statistics is
   implementation-defined."

7. **Implementation-specific error category:** Section 14 listed
   "Allocate results buffer failure," implying a pre-allocated buffer
   strategy. Generalized to "Insufficient resources for test parameters."

8. **Language-specific references in CLEANROOM_NOTES.md:** The Purpose
   section named "the Rust implementation" and the checklist mentioned
   "No Rust API design." Both replaced with language-neutral phrasing.
