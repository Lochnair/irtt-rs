# Clean-Room Implementation Notes

## Purpose

This document records the clean-room boundary for the IRTT reimplementation
project. The goal is to ensure that the implementation is developed from a
behavioral/protocol specification only, without exposure to the GPL-licensed
upstream source code.

It covers three passes and a final sanitization:

- the **client** pass (2026-04-28), producing
  `../protocol/IRTT_CLIENT_PROTOCOL_SPEC.md`;
- the **server** pass (2026-08-11), producing
  `../protocol/IRTT_SERVER_PROTOCOL_SPEC.md` and its behavioral vectors, and
  applying corrections back to the client specification;
- the **midpoint timestamp follow-up** (2026-08-11);
- the **final sanitization pass** (2026-08-11), recorded at the end of this file.

## How to Read This File

This is a provenance record. It states *what class of material was excluded* and
*what discipline was applied*; it deliberately does **not** restate the excluded
material. A note that says "an upstream identifier was removed" and then quotes
the identifier has removed nothing. Earlier revisions of this file made exactly
that mistake, and the audit sections below have been rewritten to describe
categories rather than instances.

Two things that are *not* contamination, recorded here so they are not
re-flagged:

- **Parameter names.** ProtocolVersion, Duration, Interval, Length,
  ReceivedStats, StampAt, Clock, DSCP and ServerFill appear throughout the
  specifications. They come from upstream's **published** documentation of its
  client's machine-readable output, not from source inspection.
- **Quoted diagnostic text.** Where a document quotes a message printed by the
  upstream client or server, that is transcribed program output observed during
  testing — the same thing any black-box tester sees — and is cited as evidence
  of observed behavior.

## Clean-Room Boundary

- **Contaminated side:** The specification agent (this side) inspected the
  upstream source code, documentation, tests, CLI behavior, and release
  notes to understand the IRTT protocol.

- **Clean side:** The implementation agent will receive only the documents
  under `docs/protocol/` and this notes file. The implementation agent MUST NOT
  see the upstream source code.

## Source Materials Inspected

The following materials from the upstream IRTT project (heistp/irtt, GPLv2) were
inspected to produce the protocol specification:

1. **Published end-user documentation:** the project overview and FAQ, the
   general and per-command manual pages, and the changelog. This material is
   public and may be relied on directly; it is cited in the specifications where
   used.

2. **Source (inspected for protocol behavior only):**
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

## Post-Drafting Audit

After the initial specification was drafted, a clean-room audit was performed.
The categories of problem found and corrected were:

1. **Upstream private abbreviations in the terminology table.** Removed and
   replaced with descriptive names.

2. **An upstream implementation preference disclosed** in the address-resolution
   discussion. Removed; the spec now leaves the choice to the implementer.

3. **Implementation-language standard-library references** in the parameter
   encoding section. Replaced with language-neutral descriptions (LEB128,
   protobuf-style zigzag).

4. **An internal scheduling algorithm described** in the send-scheduling section,
   mirroring upstream control flow. Replaced with observable behavior: packets
   are sent at approximately their ideal times and drift compensation is
   implementation-defined.

5. **An upstream timer-smoothing strategy named.** Removed.

6. **An RTT edge-case guard asserted** that upstream does not in fact implement.
   Moved to Open Questions.

7. **Received-window validity asserted normatively** when it was
   verification-required. Softened with a reference to Open Questions.

8. **An implementation-language concurrency term** in the result-model section.
   Replaced with a neutral word.

9. **Upstream test internals referenced** in the test-vector section. Rewritten
   to describe the vector neutrally.

10. **Internal constants asserted as protocol requirements** — a minimum
    restricted interval and a maximum parameter buffer size. Moved to Open
    Questions with verification suggestions. Both were subsequently settled by
    measurement (Sections 19.9 and 19.10 of the client specification).

11. **Timestamp capture order stated as MUST** when it was implementation-level
    timing advice. Downgraded, and later rewritten again in the final
    sanitization pass to describe only the externally relevant relationship.

12. **An internal recording structure described** in the send-timing discussion.
    Rewritten to focus on observable behavior.

13. **A self-contradictory inline correction note** that revealed the drafting
    process. Cleaned up.

14. **Field ordering stated without a verification caveat** despite being
    source-derived at the time. Verification note added, and the ordering was
    subsequently confirmed by capture across six field combinations
    (Section 19.11).

No instance of the removed material is reproduced here. Restating a private
identifier in order to record that it was removed would defeat the removal.

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

A second review was performed on 2026-04-28 by an agent that had not seen the
upstream source. The categories of problem found and corrected were:

1. **An implementation-architecture directive in the scope section**, which
   prescribed internal structure. Removed; the specification now constrains only
   observable behavior.

2. **A concurrency architecture prescribed** in the active-test section,
   implying an internal threading or task model. Rewritten to state the
   observable requirement — sending and receiving proceed concurrently — without
   prescribing how.

3. **Source-derived design rationale** offered for the zeroed placeholder fields
   in an echo request. Simplified to the observable purpose: reaching the
   negotiated packet length.

4. **A parenthetical revealing source-exhaustive knowledge** of the complete
   parameter set. Removed.

5. **Upstream CLI option syntax mirrored** in the wait-variant notation.
   Replaced with plain-language descriptions.

6. **A named statistics algorithm recommended.** Replaced with a statement that
   the method of computing running statistics is implementation-defined.

7. **An implementation-specific error category** that implied a particular
   allocation strategy. Generalized.

8. **Implementation-language references in this notes file itself.** Replaced
   with language-neutral phrasing.

As above, the removed instances are described by category and not reproduced.

---

## Server Pass (2026-08-11)

### Scope

A second contaminated-side pass characterised the upstream **server** and
produced `../protocol/IRTT_SERVER_PROTOCOL_SPEC.md`, together with
`../protocol/test-vectors/SERVER_BEHAVIORAL_VECTORS.md` and four new captures.
Corrections and clarifications arising from it were applied to the client
specification.

### Upstream Material Inspected

Three upstream builds are involved in this work, and the sanitized output names
whichever one it means everywhere it matters:

| Build | Role |
|-------|------|
| **0.9.1 release** | the behavioral baseline; every unqualified statement refers to it |
| **0.9.0 release** | comparison only, for the version difference in the midpoint follow-up |
| **development tree, six upstream commits past the 0.9.1 tag** | comparison only; post-release work, **not** part of 0.9.1, and never allowed to redefine the baseline |

*Provenance correction (final sanitization pass):* an earlier revision described
that development tree as "eight commits past the 0.9.1 tag". The count was wrong
— it included two commits belonging to this clean-room project itself, which were
present in the same working tree but are not upstream work. The upstream count is
**six**, and the outgoing documents now say so consistently.

Inspection covered, at a high level:

1. **Published documentation:** the server manual page, the changelog, and the
   project overview's security, FAQ and roadmap sections.
2. **Source (for behavior only):** request admission and validation, session
   creation and lookup, parameter restriction, echo handling, reception
   statistics, timestamp selection, payload filling, close handling, session
   expiry, and server configuration defaults.
3. **Release history:** tags and commits between 0.9.1 and the checked-out
   development head, to classify version differences.

Source inspection was used only to decide **what to test**. Every normative
statement in the sanitized output rests on an observed experiment, a capture, or
published end-user documentation — not on the source.

### Method, and What Each Part of It Establishes

The distinction below is load-bearing and is preserved in the outgoing documents.

- **Black-box observation — a raw UDP harness.** It drove a real upstream server
  directly, so that requests no conforming client would emit could be tested
  (out-of-order sequence numbers, truncated packets, invalid flag combinations,
  foreign-endpoint packets, corrupted authentication, and so on). It shares no
  code with any implementation. Its measurements are stated as observed wire
  behavior.
- **Black-box observation — the real upstream client.** Driving the unmodified
  upstream client and observing what it recorded, reported and exited with is an
  observation of a genuinely independent second implementation.
- **Measurement, not constant-reading.** Timing boundaries (idle-expiry grace,
  maximum-duration grace, interval caps) were established by measuring the
  server's responses, not by reading values out of the source.
- **Contaminated-side consistency validation — a server written on this side.**
  A server was built from the behavioral model and driven by the real upstream
  client across eleven configurations. Because it was written on the contaminated
  side it is **not** an independent implementation, and nothing in the outgoing
  documents rests on it. What it establishes is that the specification as written
  is sufficient to build something the upstream client accepts; the client's
  acceptance is the black-box part. Earlier revisions called this an "independent
  server"; that description was inaccurate and has been corrected throughout.
- All research scripts, logs, probe sources and notes were kept outside `docs/`
  and are not part of the outgoing material.

### Statement of Non-Copying (Server Pass)

No upstream source code, comments, pseudocode, tests, or file/module names were
copied into any document under `docs/`. The sanitized output describes
externally observable behavior only.

Where upstream behavior differs from what a naive reading of the wire format
would suggest — the received-window reset on reordering, the unenforced protocol
version, the midpoint dual-field emission — that difference is documented as an
**observation**, with the input that produces it and the output it yields. No
explanation of *why* the implementation behaves that way appears in the
sanitized output, because any such explanation would necessarily be
source-derived.

*Amended 2026-08-11:* this remains true of the received-window and
protocol-version items. For the dual-field midpoint emission, one narrowly scoped
**source-assisted historical conclusion** was admitted and labelled — a judgement
that the measured 0.9.0-to-0.9.1 difference was unintended. See "Midpoint
Timestamp Follow-Up" below.

Two upstream robustness defects are recorded in the server specification
(`../protocol/IRTT_SERVER_PROTOCOL_SPEC.md` Section 22) because both are
reachable from ordinary client traffic and a clean implementation has to decide
what to do instead. They are stated as observed outcomes plus **robustness
recommendations for the clean implementation** — explicitly not as
interoperability requirements. A conforming client cannot trigger the
absent-Clock defect through conforming negotiation, but can observe a
process-wide outage if another peer triggers it; no internal cause is described.

### Server-Pass Sanitization Audit

The following were checked for and are absent from all files under `docs/`:

1. Upstream private function, type, field and variable names.
2. Upstream source file names, module names and package names.
3. Implementation-language identifiers, standard-library references and
   concurrency vocabulary.
4. Copied or paraphrased upstream comments.
5. Source-shaped pseudocode and control-flow transcription.
6. Internal constants that are not externally observable. Timing values that
   *are* observable (the idle-expiry grace, the maximum-duration grace, the
   interval cap ratio) are stated with the measurement that establishes them and
   are labelled as server policy, not protocol law.
7. Internal data-structure descriptions. Session lookup is described purely in
   terms of which packets are accepted and which are dropped.
8. Token generation strategy. The specification states only the uniqueness
   requirement and explicitly leaves generation to the implementer.

The following items were considered and deliberately **excluded** from the
sanitized output as implementation-internal:

- how session state is stored, indexed, or reclaimed;
- how packet buffers are managed or reused;
- the mechanism by which expired sessions are noticed;
- any description of why the received-window behavior takes the form it does.

### Clean-Room Compliance Checklist (Server Pass)

- [x] No upstream source code included.
- [x] No upstream comments quoted.
- [x] No upstream private function or type names included.
- [x] No upstream file/module layout described.
- [x] No implementation-language references remain.
- [x] No pseudocode derived from upstream implementation.
- [x] No internal data structures or algorithms described.
- [x] Observable behavior separated from server policy throughout.
- [x] Every normative statement backed by an experiment, capture, or public
      documentation.
- [x] Version differences classified explicitly, with each build named (server
      specification Section 21).
- [x] Unresolved questions recorded rather than inferred (server
      specification Section 23).
- [x] Contaminated-side consistency validation performed against the real
      upstream client, and labelled as such rather than as independent
      verification.
- [x] Contaminated research notes, scripts and probe sources kept outside
      `docs/`.

---

## Midpoint Timestamp Follow-Up (2026-08-11)

### Scope

A focused contaminated-side investigation of one behavior: the timestamp layout
upstream 0.9.1 emits for StampAt = Midpoint. It produced changes to the server
specification (Sections 9.2, 11.3.1, 21.1, 21.2, 23.4, 24), the client
specification (Sections 8.5, 19.14), the verification report (Part III,
Finding S-18, plus a caveat on Section 19.2) and the behavioral vectors
(Section 5.2 and its new subsections).

### A Different Provenance Mix From the Earlier Passes

The earlier passes used source inspection **only to decide what to test**. This
pass went further: contaminated-side analysis was used to form expectations,
which were then tested. The sanitized output therefore contains one class of
statement the earlier passes did not, and it is labelled wherever it appears.
What crosses the boundary falls into four kinds:

- **Black-box observed** — the wire order (wall field then monotonic field), the
  affected StampAt/Clock combinations, the dependence of the reply length on the
  negotiated length, the value an ordinary positional decoder reads in the
  monotonic-only case, and the 0.9.0 / 0.9.1 / tested-post-0.9.1 comparison. All
  measured with a raw UDP harness against three separate server builds, and
  cross-checked at the application layer with the real upstream client.
- **Source-assisted historical conclusion** — one judgement only: that the
  measured difference between 0.9.0 and 0.9.1 is an unintended regression rather
  than a designed feature. It is labelled where it appears, it is not normative,
  and the measured version difference is stated separately and stands on its own.
- **Interoperability requirement** — narrowly, what a clean client must tolerate
  for this one verified case.
- **Robustness / implementation recommendation** — what a clean server should
  emit.

The specification wording keeps these four apart explicitly, so a clean-side
reader can tell which statements they may rely on as observed fact.

Note in particular what is **not** in the list. An earlier revision also carried
a characterisation of how upstream conceptually represents a midpoint value.
That was an internal model, it was source-derived, and the useful wire behavior
is fully established without it, so it was removed from all implementation-facing
documentation in the final sanitization pass.

### What Was Deliberately Not Transferred

Detailed upstream implementation structure, internal representation, control
flow, change-level source evidence, and the source-derived reasoning supporting
the regression diagnosis were retained only on the contaminated side.

That sentence is the whole of the disclosure, and deliberately so. An earlier
revision of this file enumerated the withheld findings in enough detail that
reading the list conveyed much of what the list claimed to be withholding. Naming
the categories is what a provenance record needs; restating the contents defeats
the exclusion it is recording.

What crossed the boundary from that body of work is one sentence of conclusion —
labelled source-assisted, non-normative, and separable from every requirement in
the outgoing documents.

### Method

- A raw UDP harness performed the open negotiation, sent a single echo request,
  and reported the exact reply datagram length and the bytes at each timestamp
  offset. It shares no code with any implementation.
- Three server builds were exercised, each named wherever its results are used:
  the **0.9.1 release**, the **0.9.0 release**, and a build of the upstream
  development tree **six upstream commits past the 0.9.1 tag**. Builds and
  harness live outside this tree.
- The real upstream client was used as a second, genuinely independent observer
  for the application-layer effects.

### Clean-Room Compliance Checklist (Midpoint Follow-Up)

- [x] No upstream source code, comments or pseudocode included.
- [x] No upstream private function, type or field names included.
- [x] No upstream control flow described.
- [x] No upstream file or module layout described.
- [x] No internal conceptual model of upstream's timestamp representation
      included.
- [x] The single source-assisted statement labelled as such, confined to a
      historical judgement, and never presented as black-box verified.
- [x] Black-box observation, source-assisted conclusion, interoperability
      requirement and robustness recommendation kept distinct.
- [x] Excluded contaminated-side material described by category only, without
      restating its content.
- [x] Earlier over-general claim ("always 8 bytes longer") corrected rather than
      left standing.

---

## Final Sanitization Pass (2026-08-11)

A last audit of the entire outgoing `docs/` tree before it crosses the airlock.
No new protocol research was performed; this pass only corrected, reclassified
and removed.

### Provenance

- Every statement now names the build it came from. The **0.9.1 release** is the
  baseline; the **0.9.0 release** and the **development tree six upstream commits
  past the 0.9.1 tag** appear only where explicitly labelled, and neither is
  allowed to stand in for 0.9.1.
- The "eight commits past the 0.9.1 tag" figure was corrected to **six**. The
  earlier count wrongly included two commits belonging to this clean-room project
  itself.
- The server written on this side is now described everywhere as **contaminated-
  side consistency validation**, never as an independent implementation. Only the
  raw UDP harness and the real upstream client are treated as black-box
  observers.

### Statement classification

The outgoing documents now separate six kinds of statement and never conflate
them: interoperability requirement; black-box observed upstream behavior;
black-box inference; upstream policy or default; robustness recommendation for
the clean implementation; and source-assisted historical conclusion.

Specific reclassifications made in this pass:

- Surviving a failed reply send is a **robustness recommendation**, not a
  protocol MUST. A process-wide failure can be visible to unrelated conforming
  clients, but that does not make a particular clean resilience policy an
  interoperability requirement.
- Upstream's absence of session and per-peer bounds is **observed policy**, and
  the documents now say explicitly that it is not something a compatible server
  should reproduce. The clean server is expected to be bounded; the shape of that
  policy is left to the clean project.
- The fill-validation, DSCP-clamping and length-clamping items in the hazards
  section were likewise moved from "requirement" to robustness recommendation.
- The server specification's conformance summary is split: interoperability
  MUST / MUST NOT lists, and a separate list of robustness recommendations.

### Architecture neutrality

The outgoing documents prescribe no runtime or architecture for the clean server
— no blocking-versus-async choice, no runtime-free API, no core-type shape, no
socket ownership arrangement. Those are clean-project decisions and belong in the
clean repository after the airlock, not in protocol documentation. The
architecture-neutrality statement in the server specification's scope section was
extended to say so directly.

### Wording removed or rewritten

- **Source-derived timestamp placement.** Statements locating a timestamp
  immediately before or after a particular system call were replaced with the
  externally relevant property: the pair must bracket the server's handling of
  the request, receive never later than send. Where an implementation takes its
  readings is not observable and is no longer described.
- **Validation-order narrative.** The packet-admission section was rewritten from
  a numbered processing sequence into a table of discard conditions plus the two
  ordering relationships that were actually verified externally (Open-versus-Close
  and Open-versus-echo-shaped-body interpretation precedence for an otherwise
  admissible datagram, which overrides no independent rejection condition; and the
  indistinguishability of an authentication failure from an unknown token).
  Behavioral wording replaced control-flow wording throughout.
- **Internal midpoint model.** The characterisation of upstream's internal
  representation of a midpoint timestamp was removed from all
  implementation-facing documentation. What remains is the wire fact: a
  dual-field midpoint representation, wall field followed by monotonic field.
- **DSCP.** The wire parameter is now stated plainly as the raw IP TOS /
  Traffic-Class byte, range 0–255, with Expedited Forwarding at 0xb8 / 184. The
  future user-facing notation for the clean project is marked out of scope rather
  than baked into the protocol documents.

### Corrected claims

- **Midpoint reply length.** Every "negotiated length + 8" and "normal packet
  length + 8" claim was removed. The documents now state the model uniformly:
  `upstream_header` is `normal_header` plus one 8-byte timestamp field;
  `compatible_reply_len = max(negotiated_length, normal_header)`;
  `upstream_0_9_1_reply_len = max(negotiated_length, upstream_header)`; and the
  observable excess is +8, then +7…+1, then 0 as the negotiated length rises. The
  measured sweeps demonstrating this were preserved.
- **Overlong replies.** The blanket requirement that a client tolerate any reply
  longer than the negotiated length was narrowed to the one verified case:
  single-clock midpoint at **exactly** `upstream_0_9_1_reply_len`. That is one
  additional accepted length per negotiation, not a `+1..+8` window — where
  `upstream_0_9_1_reply_len > compatible_reply_len` the two values differ by
  between 1 and 8 bytes, but a length strictly between them is rejected like any
  other unexpected length. The documents say explicitly that this does not
  generalise to arbitrary overlong echo replies and that strict validation of
  other malformed or oversized packets is retained.
- **Normal minimum reply size.** Reply-length validation is stated against
  `compatible_reply_len`, the normal compatible reply length, rather than against
  the negotiated `Length` value alone; the two differ whenever the negotiated
  Length is below the mandatory field block.
- **Monotonic-only midpoint.** The suggestion that a client may simply read the
  second 8-byte region as the monotonic value was replaced with a conservative
  rule. In the equal-length regime the conforming and upstream forms are
  indistinguishable, so the second region may be ordinary payload; a correction
  is permitted only where the dual-field form is otherwise identifiable, the
  ambiguity is recorded rather than resolved by heuristic, and the packet must be
  accepted either way.
- **0.9.0 → 0.9.1.** Any claim of no protocol-visible change between the two was
  corrected. Published release-note content and measured wire behavior are now
  stated separately, and they disagree on this point.
- **Protocol version.** Stale text using "protocol version mismatch" as an
  example of a normal server rejection was removed. Upstream 0.9.1 accepts
  version 0, 2, negative and absent, and returns 1; enforcement is client-side.
- **Received window.** The documents now state that `0x1` means no useful prior
  received-history is represented, **not** that the previous 63 packets were
  lost. Downstream/upstream loss classification is flagged as a separate question
  scheduled for its own clean-side audit.
- **Close semantics.** No production standalone close-reply codec is specified.
  Such a datagram was observed to be tolerated by upstream clients but has no
  demonstrated purpose in protocol version 1, and the documents say so.
- **Parameter payload size.** The "believed to be 128 bytes" limit was replaced
  with the measured result: no protocol limit on a received payload; 128 bytes is
  a local allocation choice.

### Contamination scan

The whole outgoing `docs/` tree was scanned against a private-token list derived
on the contaminated side from the upstream tree. That list is **not** reproduced
here, and neither is any token from it.

Scanned for and now absent: upstream private function, type, field and variable
identifiers; implementation-language identifiers and standard-library references;
upstream source filenames, module and package names; source-derived commit
identifiers; local filesystem paths; scratchpad paths; and temporary build paths.

The residual hits found and fixed in this pass were all in the audit prose of
this file, which had been quoting the identifiers it claimed to have removed, and
one tooling-environment block in the verification report that carried a local
installation path and an implementation-language version string.

Retained deliberately, and not contamination: the public upstream project
identity (`heistp/irtt`) where provenance requires it; the parameter names, which
come from upstream's published documentation; and quoted upstream program output,
which is observed black-box evidence.

### Captures

Every `.pcapng` intended to cross was metadata-reviewed. Four had already been
container-normalized during the server pass; the remaining ten still carried
capture-machine hardware and operating-system identification, the local capture
interface name and description, and the capture filter string. All ten were
regenerated to strip that metadata.

No capture contained a hostname, a username, a filesystem path, a command string
or any reference to an upstream checkout.

**Packet bytes were not altered.** For each regenerated file the full frame
hexdump and the per-frame epoch timestamps were compared before and after and
were identical. No capture had to be excluded from the outgoing set.

### Outgoing tree hygiene

The outgoing `docs/` tree contains only specifications, the verification report,
behavioral and packet vectors, and captures. No research scripts, probe sources,
upstream source archives, build outputs, temporary logs, dirty source notes or
private-identifier lists are present, and the contaminated-side material for the
midpoint work was left where it already lives, outside `docs/`.

One incidental local file sits inside `docs/` and is **not** outgoing material: a
local tool-permissions file under a dotted directory. It is ignored by version
control, it is not tracked, and it may not be copied across the airlock. The
macOS Finder metadata file previously listed here is no longer present; the
exclusion still stands, since Finder can recreate one at any time. Re-verified
2026-08-11: the dotted directory is the only ignored entry under `docs/`, and no
Finder metadata file exists anywhere in the outgoing tree.

---

## Server Deep-Dive Airlock (2026-08-12)

This section records **provenance and boundary discipline** for the material added
on 2026-08-12. It is not implementation archaeology, and it deliberately contains
no detail about what the contaminated-side work looked at.

### What happened outside this repository

A further source-assisted research phase on the upstream server took place in a
separate contaminated workspace, outside this repository and outside `docs/`. As
in the earlier passes, that workspace held upstream source, upstream tests,
upstream history, source-assisted analysis, purpose-built harnesses, build trees,
binaries, raw result files, server logs, and black-box packet evidence. The
overwhelming majority of it is not admissible here and did not cross.

### What was admitted, and what the airlock agent was allowed to see

The transfer was performed by an agent working in this clean repository under an
explicit airlock restriction. Its **entire** permitted view of the contaminated
workspace was:

1. a single sanitizer-candidate report of **independently black-box-verified**
   findings — written to be transferable without upstream source access, and
   containing no source snippets, filenames, function or type names, or internal
   structure; and
2. the black-box packet captures that report referenced, for the purposes of
   confirming the packet-level evidence, checking container metadata, and copying
   suitable files into the clean evidence set.

The agent was explicitly **prohibited** from inspecting, and did not inspect:

- the source-assisted findings report;
- the unresolved/source question matrix;
- the contaminated workspace's own README;
- upstream source, upstream tests, or upstream git history;
- any upstream checkout;
- the research build trees and harnesses;
- raw result files;
- server logs.

It was further prohibited from searching the contaminated workspace at large. No
recursive search, and no history or blame inspection, was performed there; the
only accesses were direct reads of the two authorized items above. No raw result
file was opened at all — the authorized report's own text proved sufficient for
every statement transferred, so the narrowest available boundary was kept.

### What the boundary cost, deliberately

Several items in the authorized report were **not** transferred, and the reasons
are worth recording because they are the boundary doing its job:

- An account of the order in which the server reports rejection reasons, taken
  from the server's own diagnostic output. Even though that output is externally
  emitted, it describes internal check ordering, and the clean specification had
  already deliberately replaced its admission narrative with a table of discard
  conditions plus the two precedence relationships that were verified on the
  wire. Re-importing an ordering narrative would have undone that work.
- A statement that a session is removed before its close-flagged reply is sent.
  That is internal ordering; only its externally visible consequence crossed, and
  that consequence was already recorded.
- A generalisation about which flag combinations are dropped on a data packet
  carrying a live token. The clean documents already carry a **more precise**
  account of the same behavior, and the broader statement would have weakened it.
- An inference that the wire format offers no general extension channel. It
  crossed only as an explicitly labelled inference, alongside the byte encodings
  actually tested, rather than as a property of the format.

Where the authorized report was insufficient to support a clean statement, the
statement was not made and the ambiguity was left standing. Nothing was resolved
by consulting anything else.

### Classification discipline

Every new statement was classified before it was written, and the classification
appears in the text: observed upstream behavior, black-box inference, upstream
policy, protocol requirement, robustness recommendation for the clean
implementation, or remaining unknown. The server specification's statement-level
table was extended with the inference level and with the host-specific marking
that several of these results require.

Two boundaries were held particularly deliberately:

- **An upstream defect was not promoted to a protocol requirement.** The most
  significant finding of this pass is an open request that upstream accepts and
  then faults on. It is recorded as an observed robustness defect with a
  must-not-crash recommendation for the clean server, and the documents state
  explicitly that reproducing the fault is not an interoperability requirement.
- **No implementation policy was chosen.** The documents record what upstream
  does about an open that selects timestamps without a clock, about a negative
  Length, about out-of-range DSCP values, and about payload fill — and in
  each case say that the clean project's choice is a decision for the
  implementation work, not one this evidence settles. Those decisions remain open.

Host-specific figures — interface MTUs, the tested host's maximum outbound
datagram size, interface-MTU effective-length decision knees, and platform
handling of out-of-range DSCP values — are labelled as host-specific wherever
they appear, so that none is mistaken for a protocol constant or compiled into an
implementation.

### Captures admitted

Three captures crossed, each because it materially supports a finding that is new
to the clean documents:

| File | Packets | What it proves |
|------|---------|----------------|
| `../protocol/captures/server-clock-absent.pcapng` | 3 | An open selecting timestamps with no Clock tag is answered with a token and no Clock parameter, and the first echo is the last packet in the exchange |
| `../protocol/captures/server-expiry-consumption.pcapng` | 14 | Control and foreign-endpoint-first test: whether the first tested packet to reach an expired session emits a reply |
| `../protocol/captures/server-dscp-interleaved.pcapng` | 32 | Four interleaved sessions with distinct DSCP values over three rounds, no cross-session leakage, unmarked open replies |

Each was reviewed before copying and rewritten into the container convention
already used here: packet records and the rewriting tool's own version string
only, with no capture comment, no section or interface description, no host, user
or machine identification, no capture filter, and no filesystem path. Packet
bytes and per-frame timestamps were **not** altered — for each file the full
frame hexdump and the per-frame epoch timestamps were verified identical before
and after, and the packet counts above are unchanged from the originals. The
copied files were re-inspected afterwards and contain no string other than the
rewriting tool's version.

Two further captures were reviewed and **not** admitted: one materially redundant
with a capture already present, and one that on inspection did not demonstrate
the behavior it was associated with. In both cases the textual result was kept
instead, which is the preferred trade — a textual black-box result is better than
an unnecessary addition to the evidence set. The reasoning is recorded in the
verification report rather than only here.

### Review corrections applied before merge

Review of the airlock commit found several places where the wording claimed more
than the admitted evidence supports. Each was narrowed to the observation, using
the clean evidence set only — no contaminated material was consulted to resolve
any of them:

- The reply-length cap is stated as byte-exact only for the two interfaces where
  that was measurable. The loopback row shows lengths up to 8000 emitted
  unclamped and nothing about a 16384-byte boundary, which this host cannot reach
  because a reply beyond roughly 9300 bytes ends the server first.
- The interleaved DSCP capture is described as the three rounds it contains, and
  the separate observation that an open reply is unmarked on a listener that has
  already sent marked replies is attributed to the capture that actually shows it
  — an existing capture from the first pass — rather than to the new one, whose
  open exchanges all precede its first marked reply.
- The fill-phase result is scoped to continuity across the sessions tested and
  across the tested IPv4 and IPv6 listeners, rather than asserting one stream
  shared by every session of a process. The recommendation previously drawn from
  it was **removed**: the default fill is a fixed public pattern, so its phase
  carrying across sessions discloses nothing about another peer, and the
  observation does not justify prescribing per-session fill state. The genuine
  no-cross-peer-data invariant was already stated for the no-fill mode and is
  untouched.
- The must-not-crash advice no longer uses RFC 2119 keywords, which this document
  reserves for interoperability requirements. It is a robustness recommendation,
  and how strictly the implementing project binds itself to it belongs in that
  project's own guidance.
- The presence-versus-zero rule is scoped to the reply direction. Requests do
  carry present zeros — `Length = 0` and `DSCP = 0` are accepted, and `Clock = 0`
  is rejected precisely because it is present.
- The admission-table row for an open with no Clock now says the **open** is
  answered normally, not the session, which is what the evidence shows.
- The maximum-duration origin refinement was carried into the client
  specification as well, since both documents are compatibility baselines and
  would otherwise state the lifecycle rule differently.

A second review round found five further places where the first correction pass
had left an inconsistency behind, and these were fixed the same way:

- The must-not-crash advice is called a **recommendation** everywhere, including
  in the paragraph that follows it and in the behavioral vector, so that dropping
  the RFC 2119 keywords was not undone two sentences later.
- The remaining-unknown entry on reply-length capping was brought into line with
  the corrected §9.2: exact only for the Ethernet and tunnel interfaces, with the
  loopback trial recorded as a lower bound.
- The maximum-duration origin is stated for the **two** drop classes actually
  tested in the first-request position — oversized and foreign-endpoint — instead
  of generalising to any dropped request. Corrected in the specification, the
  vector and the verification report together.
- The "first post-deadline request is still answered" rule is qualified to a
  request that would otherwise be served, since the same subsection then shows a
  foreign-endpoint packet being dropped while still consuming the release.
- "Close packets are unmarked" is scoped to standalone datagrams. A
  server-initiated close is an echo reply carrying the session's marking, and
  both specifications now say so rather than leaving the two statements to
  contradict each other.

A final documentation-only review correction restricted the maximum-duration
dropped-trigger result to the directly tested rate-limit case and corrected
fill alternatives as observable but interoperability-equivalent. It used no new
evidence or contaminated material.

None of these corrections required new evidence, and none changed a
classification from observation to requirement or the reverse.

No binaries, source, harnesses, build trees, server logs, result files or
research directories crossed.

### Clean-Room Compliance Checklist (Deep-Dive Airlock)

- [x] Source-assisted findings report never opened.
- [x] Unresolved/source question matrix never opened.
- [x] Upstream source, tests and history never opened.
- [x] Research build trees and harnesses never opened.
- [x] Raw result files never opened.
- [x] Server logs never opened.
- [x] No recursive search, log or blame inspection performed in the contaminated
      workspace.
- [x] Only independently black-box-verified evidence transferred.
- [x] Every transferred statement classified, with observation separated from
      inference and from recommendation.
- [x] Upstream defects recorded as defects, not as protocol requirements.
- [x] Host-specific figures labelled as host-specific.
- [x] Clean implementation policy decisions left open for the implementation work.
- [x] Captures metadata-reviewed, container-rewritten, and verified
      byte-identical in packet data and timestamps.
- [x] No production code changed by the airlock work.

### 2026-08-12 — PR #76 final review wording cleanup

An interrupted prior agent's local maximum-duration and rate-limit corrections
were reviewed rather than discarded: the maximum-duration trigger clarification
was retained, while the rate-limit interaction was kept as a black-box inference
rather than promoted to unsupported RFC 2119 language. Using clean evidence only,
this review also removed the false request-length invariant, scoped normal DSCP
marking to `0..=255`, and clarified that a dropped first post-expiry packet can
release the session before any final reply is emitted. No contaminated material
was consulted.

### 2026-08-12 — PR #76 black-box claim review correction

The latest review found a blanket discard-state claim that conflicted with the
observed rate-limit idle refresh, stale compact maximum-duration and idle-expiry
summaries, and an over-prescriptive parameter-presence statement. It also found
two black-box observations expressed as internal mechanism claims: pre-policy
receive truncation and a rejected OPEN leaving no internal state; configured and
negotiated limiter intervals had likewise been conflated. The wording now states
observable results or explicit inference only. No contaminated material was
consulted, no new evidence was added, and no implementation policy was chosen.
