# IRTT-Compatible Server Protocol Specification

**Version:** 1.1-verified
**Date:** 2026-08-11
**Compatibility baseline:** IRTT 0.9.1, protocol version 1
**Verification platform:** macOS Darwin 25.5.0 arm64

---

## 1. Status

This document describes the externally observable behavior required for an
independent **server** implementation that interoperates with existing IRTT
(Isochronous Round-Trip Tester) clients speaking protocol version 1.

It is the server-side counterpart to `IRTT_CLIENT_PROTOCOL_SPEC.md`. The two
documents share a wire format; this one adds only what a server must do that a
client does not. Where a rule is already stated in the client specification
(magic bytes, flag bits, field ordering, parameter encoding, HMAC computation),
this document references it rather than restating it, and states only the
server-specific reading of it.

This document does **not** describe upstream source structure, internal
algorithms, module layout, or implementation organization. It is intended for a
clean-room implementation that has never seen the upstream source code.

The key words "MUST", "MUST NOT", "REQUIRED", "SHOULD", "SHOULD NOT",
"RECOMMENDED", "MAY", and "OPTIONAL" are to be interpreted as interoperability
requirements, consistent with RFC 2119.

Five distinct levels of statement are used throughout, and they are not
interchangeable. Nothing in one level implies anything at another.

| Marker | Meaning |
|--------|---------|
| **Protocol requirement** | Required for interoperability with existing clients. Stated with MUST / MUST NOT / SHOULD. Every one rests on observed behavior. |
| **Upstream 0.9.1 behavior** | What the reference server was observed to do on the wire. A compatible implementation is **not** obliged to reproduce it unless a separate protocol requirement says so. |
| **Server policy** | A configurable or arbitrary choice of the reference server, including its defaults. A compatible implementation may choose differently, and in several cases should. |
| **Robustness recommendation** | Advice for the clean implementation's own resilience or safety. It is **not** an interoperability requirement and no existing client can observe whether it was followed. |
| **Source-assisted historical conclusion** | A judgement about upstream's release history reached with contaminated-side access to upstream material. Never load-bearing: no requirement in this document depends on one. Used once, in Section 11.3.1. |

Where the reference server's own behavior would be a poor choice for a new
implementation — an unbounded resource policy, a fatal reaction to a single
failed send — this document records the observation and the recommendation
separately, and does not turn either into a protocol rule.

---

## 2. Scope and Compatibility Baseline

- **Server-only.** Client behavior appears only where a server's obligations
  depend on it.
- **Protocol version 1**, as implemented by IRTT 0.9.0 and 0.9.1.
- The baseline for every unqualified statement is the **upstream 0.9.1
  release**.
- Two other upstream builds were exercised, and each is named explicitly
  wherever its results are used:
  - the **0.9.0 release**, for the version comparison in Sections 11.3.1 and
    21.1;
  - a build of the **upstream development tree as it stood six commits past the
    0.9.1 tag**, covered separately in Section 21.2. Those six commits are
    *not* part of the 0.9.1 release, and no result measured against them
    redefines the 0.9.1 baseline.
- Development-era (0.1.x) protocol variants are out of scope and are not
  interoperable.
- CLI design, logging, output formats, configuration file syntax, and process
  supervision are out of scope.

Implementations are not required to use any specific internal architecture,
data structure, naming convention, concurrency model, threading or task model,
socket ownership arrangement, or source organization. This document deliberately
prescribes none of those: they are decisions for the implementing project, not
protocol facts. Only externally observable behavior is normative.

---

## 3. Clean-Room Provenance

This specification was produced on the contaminated side of the clean-room
boundary described in `../clean-room/CLEANROOM_NOTES.md`. It records only
externally observable behavior:

- packet bytes on the wire, in both directions;
- accept / drop / reply decisions;
- session state transitions inferred from subsequent packet exchanges;
- negotiated parameter values returned to the client;
- timing boundaries measured by observation;
- documented command-line policy knobs and their observable effects.

Every normative statement in this document is supported by a black-box
experiment against a running reference server, by a packet capture in
`captures/`, or by published end-user documentation. Statements that could only
be derived from internal structure have been excluded.

**One labelled exception.** Section 11.3.1 carries a single sentence of
*historical* judgement — that a particular measured difference between 0.9.0 and
0.9.1 is an unintended regression rather than a designed feature — which was
reached with contaminated-side access to upstream material. It is labelled where
it appears, it is not normative, and no requirement anywhere in this document
depends on it. The measured 0.9.0-versus-0.9.1 wire difference is stated
separately and stands on its own. The contaminated-side evidence behind the
judgement was retained on the contaminated side and deliberately not carried
into this document; see `../clean-room/CLEANROOM_NOTES.md`.

**Naming.** The parameter names used throughout (ProtocolVersion, Duration,
Interval, Length, ReceivedStats, StampAt, Clock, DSCP, ServerFill) are taken
from upstream's **published** documentation of its client's machine-readable
output, not from source inspection. No other upstream identifier appears in this
document.

---

## 4. Roles and Directionality

A server:

1. listens on one or more UDP endpoints;
2. accepts session-open requests and assigns session tokens;
3. echoes test packets, annotating them with reception statistics and
   timestamps as negotiated;
4. releases session state on client request, on policy limits, or on idle
   expiry.

Every packet a server emits **MUST** have the Reply flag (0x02) set. Every
packet a server accepts **MUST** have the Reply flag clear.

**Protocol requirement.** A reply without the Reply flag is not merely ignored
by existing clients — upstream 0.9.1 clients abort the run and terminate
abnormally when a reply arrives with the flag clear. This was confirmed by
driving an upstream client against a deliberately non-conforming server.

---

## 5. Transport and Packet Envelope

### 5.1 Transport

UDP only. There is no TCP fallback, no connection setup at the transport layer,
and no fragmentation logic in the protocol itself.

The conventional server port is **2112** (server policy: the reference server's
default bind is all addresses on port 2112).

### 5.2 Address Families

A server MUST support IPv4. IPv6 support is RECOMMENDED and is provided by the
reference server.

**Upstream 0.9.1 behavior.** Binding to an unspecified address creates two
independent listeners, one per address family, on the same port. Tokens are
scoped to the listener that issued them: a token issued by the IPv4 listener is
not recognized by the IPv6 listener on the same port, and a request bearing it
is dropped as an unknown token. See `captures/server-session-identity.pcapng` and
the vectors file for the equivalent same-family case.

**Protocol requirement.** Session tokens need only be unique within the scope in
which they are looked up. A compatible implementation MAY use one global table
or one per listener; clients never observe the difference except through the
drop behavior above.

### 5.3 Reply Addressing

**Protocol requirement.** A reply MUST be sent to the exact source address and
port of the request, and MUST be sent **from** the address and port to which the
request was addressed. Clients use connected UDP sockets and silently discard
datagrams whose source does not match the address they sent to.

**Server policy.** On a multi-homed host with a listener bound to an unspecified
address, choosing the reply source address by routing table rather than by the
request's destination address will break clients on the non-preferred address.
The reference server offers an opt-in policy to pin the reply source address to
the request's destination address, and its documentation recommends binding to
explicit addresses instead. On single-homed and loopback configurations both
modes were observed to produce correct source addresses.

### 5.4 Packet Admission

This section states which datagrams a server answers and which it discards. It
describes outcomes, not an internal processing pipeline: any implementation that
produces these accept/discard decisions is conformant, whatever order it
evaluates them in.

**Discard conditions.** Each of the following was independently observed to
cause the datagram to be **silently discarded**, with no reply of any kind and
no change to the state of any live session:

| Condition | Notes |
|-----------|-------|
| Datagram too short to carry magic + flags | With an HMAC key configured, the authentication field is part of the floor, so the floor rises by 16 bytes |
| First three bytes are not the protocol magic | |
| Any flag bit above the four defined bits is set (flags > 0x0F) | |
| The Reply flag is set | A server never answers a datagram carrying it |
| Authentication mismatch of any kind | Section 14; includes the flag/key mismatch in either direction |
| A non-open datagram too short to carry the token | |
| A non-open datagram whose token matches no live session | Section 8 |
| A non-open datagram whose token matches a session bound to a different source endpoint | Section 8; the session itself is unaffected |
| An echo datagram too short to carry the sequence number | |
| An echo datagram exceeding a configured maximum packet length | Section 9.4, server policy |
| An echo datagram arriving with no rate allowance left | Section 9.3, server policy |
| An open datagram whose parameter payload is malformed or out of range | Section 7.2 |

**Protocol requirement.** Every rejection is a **silent discard**. The protocol
defines no error reply, no reset, and no NACK. A client distinguishes "rejected"
from "lost" only by timing out. A compatible server MUST NOT answer any of the
conditions above.

**Observable precedence.** Only two ordering relationships are externally
visible, and both were verified directly. Both describe how an *admissible*
datagram is interpreted; neither weakens any discard condition in the table
above.

1. **For an otherwise admissible datagram with Open set, Open interpretation
   takes precedence over Close interpretation and over an echo-shaped body.** A
   datagram with both Open and Close set is a *no-test* open (Section 16), not a
   close; and an echo-shaped datagram that also carries the Open flag is answered
   as an open, with the bytes that would have been the token and sequence number
   parsed as Open parameter data rather than as those fields. This precedence is
   about interpretation only. It does **not** override the independent rejection
   conditions above: a datagram carrying the Open flag is still silently
   discarded if the Reply flag is also set, if any undefined flag bit is set, if
   authentication fails, or if its Open parameter payload is malformed or out of
   range. A compatible server SHOULD behave the same way, so that a client's
   retransmitted open is never mistaken for an echo.
2. **An authentication failure is indistinguishable from an unknown token.** A
   datagram carrying a *valid* token with a bad MAC produces exactly the same
   silence as one carrying a token that was never issued (Section 14.2). A
   compatible server MUST preserve this: no reply behavior may reveal whether a
   token exists, for a datagram that failed authentication.

**Request bytes beyond the sequence number carry no meaning to a server.** The
midpoint/receive-send exclusivity and clock-consistency rules that govern
timestamp fields apply to *replies*; they have no effect on inbound traffic. An
echo request carrying arbitrary bytes in the region a reply would use for
timestamps is accepted and answered normally.

### 5.5 Minimum and Maximum Datagram Sizes

Observed minimum accepted request sizes (no authentication):

| Request kind | Minimum accepted datagram |
|--------------|---------------------------|
| Open (with empty parameter payload) | 4 bytes |
| Close | 12 bytes |
| Echo | 16 bytes |

With authentication enabled, add 16 bytes to each figure.

**Upstream 0.9.1 behavior.** Trailing bytes beyond the fields a request kind
requires are ignored:

- an echo request longer than the negotiated length is accepted and answered
  with a reply of the *negotiated* length, not the request length;
- a close request with trailing bytes is accepted and closes the session.

The only length ceiling applied to requests is the configured maximum packet
length (Section 19).

---

## 6. Server Lifecycle

### 6.1 Startup

**Server policy.** The reference server resolves its bind specifications,
creates one listener per resolved address/family, and serves until interrupted.
A failure on any single listener causes the whole server to stop.

### 6.2 Shutdown

**Upstream 0.9.1 behavior.** Termination is abrupt from the client's
perspective. No close, reset, or notification packet of any kind is emitted for
live sessions when the server shuts down; subsequent client packets simply go
unanswered. Verified by signalling a running server mid-session and observing
that nothing arrived at the client before the socket went silent.

**Protocol requirement.** None. Clients MUST already tolerate an unresponsive
server. A compatible implementation MAY emit close replies to live sessions on
shutdown; upstream clients ignore unsolicited close replies harmlessly
(Section 15.1).

### 6.3 Reply Send Failures

**Upstream 0.9.1 behavior.** If sending a reply fails with a non-transient
socket error, the listener terminates and the whole server process exits. This
is reachable in ordinary use: a client may negotiate a packet length larger than
the host's maximum outbound datagram size, and the first echo reply then ends the
server. See Section 22.

**Protocol requirement:** none. A reply that is never sent is
indistinguishable, to the client, from a reply that was lost in the network. No
client can observe how the server reacted internally.

**Robustness recommendation (clean implementation).** Because the outcome above
is reachable from ordinary client traffic and takes every other session down
with it, a clean server should treat a per-packet send failure as a per-packet
event: drop that reply, keep the listener and the process running, and continue
serving other sessions. This is a resilience choice for the implementing
project, not something compatibility requires.

---

## 7. OPEN Negotiation

### 7.1 Open Request

**Direction:** client → server.

**Flags:** Open (0x01) set, Reply (0x02) clear, HMAC (0x08) set iff the client
is authenticating, Close (0x04) set iff the client requests *no-test*
(Section 16).

**Layout:** magic (3) + flags (1) + [authentication (16)] + parameter payload.

**Protocol requirement.** There is **no** token field in an open request. The
parameter payload begins immediately after the fixed header (and the
authentication field, when present).

The parameter payload uses the tag/value encoding defined in the client
specification, Section 8.6. A server MUST decode it with the same rules.

### 7.2 Parameter Acceptance and Rejection

A server parses the whole parameter payload. Observed acceptance rules:

| Condition | Observed result |
|-----------|-----------------|
| Empty parameter payload | Accepted; all parameters default to zero, then restriction applies |
| Unknown tag with a well-formed value | Silently ignored; the rest of the payload is still parsed |
| Truncated tag or truncated value varint | **Dropped, no reply** |
| Varint overflow | **Dropped, no reply** |
| Duration present and ≤ 0 | **Dropped, no reply** |
| Interval present and ≤ 0 | **Dropped, no reply** |
| Duration or Interval absent (implicitly 0) | Accepted |
| ReceivedStats outside 0–3 | **Dropped, no reply** |
| StampAt outside 0–4 | **Dropped, no reply** |
| Clock outside 1–3 | **Dropped, no reply** |
| Length any value, including negative | Accepted (see Section 7.3) |
| DSCP any value, including negative or > 255 | Accepted (see Section 7.3) |
| ServerFill string longer than 32 bytes | **Dropped, no reply** |
| ServerFill declared length exceeding the remaining payload | **Dropped, no reply** |
| Parameter payload far larger than any real request (200+ bytes of unknown tags tested) | Accepted |

**Protocol requirement.** Unknown tags MUST be silently ignored. A malformed
parameter payload MUST NOT produce a reply — a rejection reply would let an
unauthenticated peer distinguish a live server from a firewall drop, which is
contrary to the design of the authenticated mode.

**Clarification.** Upstream servers impose no 128-byte ceiling on *incoming*
parameter payloads. A payload of over 200 bytes composed of unknown tags was
accepted and answered normally. Any such limit in an implementation is a local
buffer-sizing choice, not a protocol rule.

### 7.3 Server Restriction of Parameters

A server MAY reduce or replace requested parameters. The restricted values are
what it returns in the open reply and what it enforces thereafter.

Observed restriction rules of upstream 0.9.1 (each is **server policy**, gated
on the corresponding configuration knob):

| Parameter | Restriction applied |
|-----------|--------------------|
| ProtocolVersion | Always rewritten to 1 (see Section 7.5) |
| Duration | Clamped down to the configured maximum test duration, when one is set |
| Interval | Raised to the configured minimum interval, when one is set |
| Interval | Then clamped down to (idle timeout ÷ 4), when an idle timeout is set |
| Length | Clamped down to the configured maximum packet length, when one is set |
| StampAt | Reduced per the configured timestamp allowance (Section 11.4) |
| DSCP | Forced to 0 when DSCP is disallowed or unsupported on the socket |
| ServerFill | Replaced with the server's own default fill descriptor when the requested fill does not match the allow-list |

Measured confirmations of the interval cap: idle timeout 4 s → maximum interval
1 s; 8 s → 2 s; 20 s → 5 s; 40 s → 10 s; 60 s → 15 s.

**Interoperability note.** The interval cap can *reduce* an interval below what
the client asked for. Upstream clients enforce a hard floor of **1 second** on a
server-reduced interval and abort the session — in both strict and loose mode —
when the returned interval is below it. A reduction to exactly 1 s is accepted
in loose mode. A server whose idle timeout is below 4 s will therefore be unable
to serve clients requesting a 1 s interval. Measured: idle timeout 2 s reduces
1 s → 500 ms and the client aborts; idle timeout 3 s reduces to 750 ms and the
client aborts; idle timeout 4 s produces no reduction at all for a 1 s request.

**Server-policy hazard.** If the configured minimum interval is larger than
(idle timeout ÷ 4), the negotiated interval is the smaller of the two while the
enforced minimum remains the larger. A fully conforming client then sends at the
negotiated interval and is rate-limited (Section 9.3). Observed with a 5 s
minimum interval and an 8 s idle timeout: negotiated interval 2 s, but only the
first request in each 5 s window was answered.

### 7.4 Open Reply

**Direction:** server → client.

**Flags:** Open (0x01) set, Reply (0x02) set, HMAC (0x08) set iff the server is
authenticating, Close (0x04) set iff the request had it set (no-test) — see
Section 7.5 for the one case where upstream does *not* set it.

**Layout:** magic (3) + flags (1) + [authentication (16)] + token (8) +
restricted parameter payload.

**Protocol requirement.**

- The token field MUST be present in an open reply.
- For a session-creating open, the token MUST be non-zero. Clients treat a zero
  token without the Close flag as an error.
- The parameter payload MUST contain the **restricted** values — the values the
  server will actually enforce. Clients compare them field-by-field against
  what they requested and, by default, abort on any difference.
- Parameters whose restricted value is zero are omitted from the payload
  entirely; the client reconstructs them as zero.
- The open reply is **not** padded to the negotiated length. Its size is
  header + token + parameter payload.

**Upstream 0.9.1 behavior.** The open reply preserves whatever flags the request
carried, adding Reply. There is no separate "reject" reply shape.

### 7.5 Protocol Version — Important Deviation

**Upstream 0.9.1 behavior.** The server does **not** reject a mismatched
protocol version. Any value in the ProtocolVersion parameter — 0, 2, −1, or the
tag omitted entirely — is silently rewritten to 1, a normal session is created,
and the open reply carries ProtocolVersion = 1 with the Close flag **clear**.
Verified by black-box test: requests with ProtocolVersion 0, 2 and −1 each
produced a session-creating open reply.

Consequence: version mismatch is detected **client-side only**, by the client
comparing the returned version against its own. A client speaking a future
version would send its own version, receive 1, and abort locally.

**Protocol requirement.** A compatible server MUST return ProtocolVersion = 1 in
the open reply so that version-1 clients accept the session. A compatible server
MAY additionally reject unknown versions by replying with the Close flag set and
a zero token; existing clients treat such a reply as a rejection and abort. Both
behaviors are interoperable with version-1 clients; only the second is
interoperable with a hypothetical future client that relies on server-side
detection.

**Do not assume** that an upstream server will tell you your version is wrong.

### 7.6 Repeated and Retransmitted Opens

**Upstream 0.9.1 behavior.** Every open request creates a new, independent
session with a fresh token. There is no deduplication of retransmitted opens and
no per-endpoint session limit:

- two opens from the *same* source port yield two distinct tokens, and **both**
  remain usable simultaneously;
- an open request sent twice (as a client retransmission would) yields two
  distinct tokens; the client keeps only the token from the reply it processes,
  and the other session is orphaned;
- 1000 sequential opens from a single source endpoint all succeeded, all tokens
  were distinct, and the first, middle and last were all still usable
  afterwards.

**Resource consequence (upstream policy).** An orphaned session created by a
retransmitted open never carries an echo request, and — under upstream's idle
policy — a session that has never carried an echo request **never expires**
(Section 18.2). On a lossy path, open retransmissions therefore accumulate
permanent state.

**Protocol requirement:** none. Clients cannot observe orphaned sessions, and
nothing about the behavior above is required for interoperability.

**Robustness recommendation (clean implementation).** Bound the lifetime of
sessions that have never carried an echo request, so that open retransmissions
cannot accumulate. See Section 18.3.

---

## 8. Session Identity and Tokens

### 8.1 What Identifies a Session

**Protocol requirement.** A post-open request is bound to a session by the
**token**, and the server MUST additionally verify that it arrived from the
same source endpoint the session was opened from.

Observed identity components, all of which must match:

| Component | Must match |
|-----------|-----------|
| Token (64-bit, little-endian) | Yes |
| Source IP address | Yes |
| Source UDP port | Yes |
| IPv6 zone/scope, when present | Yes |
| Address family / listener | Yes — tokens are not shared between the IPv4 and IPv6 listeners of the same port |

Observed outcomes:

| Case | Result |
|------|--------|
| Correct token, original endpoint | Served normally |
| Correct token, **different source port**, same IP | **Dropped, no reply.** Session left intact and still usable from the original port |
| Correct token, different address family | **Dropped, no reply** (different listener, unknown token there) |
| Unknown token | **Dropped, no reply** |
| Zero token | **Dropped, no reply** (treated as any other unknown token) |
| Token of a session already closed | **Dropped, no reply** |
| **Close** request bearing a valid token from a foreign source port | **Dropped**; the session is **not** closed |

The last row matters: endpoint binding protects the close path as well as the
echo path. A capture of these cases is in
`captures/server-session-identity.pcapng`.

**Practical consequence.** Any NAT rebinding, source-port change, or address
change during a session terminates it from the client's point of view — all
subsequent packets go unanswered until the idle timeout reclaims the state.
There is no rebinding or migration mechanism in protocol version 1.

### 8.2 Token Semantics

| Property | Value |
|----------|-------|
| Width | 64 bits |
| Encoding | Little-endian in the token field |
| Assigned by | Server, during open |
| Zero token | Reserved in practice: clients reject a zero token in a session-creating open reply, and a zero token is used in no-test replies (Section 16) |
| Uniqueness | MUST be unique among the server's live sessions in the scope where lookups occur |
| Lifetime | From open until close, policy limit, or idle expiry |
| Reuse | A token that has been released MAY be issued again later; no client-visible barrier exists |

**Protocol requirement.** The server MUST echo the session token back in every
reply it sends for that session.

**Upstream 0.9.1 client behavior (informative).** Upstream clients do **not**
validate the token in echo replies — a reply carrying a wrong token was
accepted and recorded. Do not rely on this: other implementations may validate,
and matching the token is required by this specification.

**Token generation is deliberately unspecified.** A compatible implementation
MAY choose any generation strategy that satisfies the uniqueness requirement.
Because the token is the only thing standing between an off-path attacker and a
usable session on a non-authenticated server, generating tokens from a
cryptographically strong source is RECOMMENDED.

---

## 9. ECHO Processing

### 9.1 Echo Request

**Direction:** client → server.

**Flags:** Open clear, Reply clear, Close clear, HMAC set iff authenticating.

**Layout:** magic (3) + flags (1) + [authentication (16)] + token (8) +
sequence number (4) + client padding to the negotiated length.

**Protocol requirement.** A server MUST read only the token and the 32-bit
little-endian sequence number from a request. Everything after the sequence
number is opaque to the server: clients place zeroed placeholders there so the
request reaches the negotiated length, and a server MUST NOT interpret them.

**Upstream 0.9.1 behavior.** The datagram length of a request is not compared
against the negotiated length. A 16-byte request on a session that negotiated
64 bytes is accepted and answered with a 64-byte reply. A request *longer* than
the negotiated length is likewise accepted, and the reply is still the
negotiated length.

### 9.2 Echo Reply

**Direction:** server → client.

**Flags:** Reply (0x02) set; HMAC (0x08) set iff authenticating; Close (0x04)
set only in the server-initiated close case (Section 15.2). Open MUST be clear —
upstream clients abort if an echo reply has the Open flag set.

**Layout (fields in the order defined in the client specification,
Section 8.1.3):**

```
magic (3) | flags (1) | [auth (16)] | token (8) | sequence (4)
          | [received count (4)]      -- iff negotiated stats include "count"
          | [received window (8)]     -- iff negotiated stats include "window"
          | [timestamp fields]        -- per negotiated StampAt and Clock
          | payload padding to the negotiated length
```

**Protocol requirement.**

- The sequence number MUST be copied from the request unchanged.
- The token MUST be copied from the request unchanged.
- The reply length MUST be at least the length of the fields it contains, and
  SHOULD be exactly the negotiated length when that is larger.
- When the negotiated length is smaller than the field block (including the
  common case of a negotiated length of 0), the reply is the field block's
  length. Measured: a session negotiating length 0 or 20 with full statistics
  and dual timestamps on both clocks produced 60-byte replies.

Writing `normal_header` for the block of fields the negotiated parameters imply:

```
compatible_reply_len = max(negotiated_length, normal_header)
```

For StampAt = Midpoint with a single negotiated clock, upstream 0.9.1 emits one
8-byte timestamp field more than the negotiated parameters imply, which changes
the reply length in exactly the cases where the field block wins. See
Section 11.3.1.

**Upstream 0.9.1 behavior — reply length is capped by the local interface MTU.**
The negotiated length is echoed back to the client unchanged, but the actual
reply datagram is truncated to the detected MTU of the listener's interface.
Measured on a 1500-byte-MTU interface: negotiated 1501, 2000, 3000 and 8000 all
produced 1500-byte replies, while the open reply still reported the requested
value. Upstream clients treat a reply shorter than the negotiated length as a
fatal error, so this configuration fails on the first echo. See Section 22.

### 9.3 Rate Limiting

**Server policy.** The reference server enforces the negotiated minimum send
interval with a token-bucket allowance:

- a burst allowance of *N* requests is available immediately (default N = 5);
- allowance is replenished at one request per configured minimum interval and
  is capped at *N*;
- a request arriving with no allowance is **dropped without a reply**.

Measured with a 100 ms minimum interval and a burst of 5: a blast of 12 requests
produced exactly 5 replies; after a 600 ms pause, the next 3 spaced requests
were all answered. With a burst of 1, a blast of 4 produced 1 reply.

**Protocol requirement.** A rate-limited request MUST NOT be answered and MUST
NOT affect the reception statistics (Section 10). Measured: the received count
advanced 1, 2, 3, 4, 5 across the accepted burst and continued 6, 7, 8 for the
later requests — dropped requests were not counted.

### 9.4 Maximum Request Length

**Server policy.** When a maximum packet length is configured, a request whose
**datagram length** exceeds it is dropped without a reply. Measured with a
maximum of 64: 16-, 32- and 64-byte requests were answered with 64-byte replies;
65- and 100-byte requests were dropped.

The maximum applies to echo requests only. An open request larger than the
maximum was still accepted and answered.

If the configured maximum is below the minimum echo field block, **every** echo
request is dropped while opens still succeed. Measured with a maximum of 10: the
session opened, negotiated a length of 10, and every 16-byte echo request was
dropped.

---

## 10. Sequence Handling and Received Statistics

This section is the most behaviorally distinctive part of the server and is
specified strictly as input → observed output. The reference implementation's
observable semantics differ from a naive reading of "a 64-packet reception
bitmap", and clients depend on the observed behavior.

### 10.1 Received Count

| Property | Value |
|----------|-------|
| Field width | 32 bits, little-endian |
| Present when | Negotiated ReceivedStats includes "count" (values 1 or 3) |
| Initial value | 0 at session creation; the first accepted request is answered with 1 |
| Scope | Per session. A new session starts at 0 again |
| Counts | Every echo request that reaches the reply stage |
| Does **not** count | Requests dropped for bad magic, bad flags, bad authentication, unknown token, endpoint mismatch, oversize, or rate limiting |
| Duplicates | **Counted.** A repeated sequence number increments the count |
| Reordered / late requests | **Counted** |
| Close and open requests | Not counted |

**Protocol requirement.** The count reported in a reply MUST include the request
being answered.

**Wraparound.** The field is 32 bits. Behavior at 2^32 requests in one session
was not observed and is listed in Section 23.

### 10.2 Received Window

| Property | Value |
|----------|-------|
| Field width | 64 bits, little-endian |
| Present when | Negotiated ReceivedStats includes "window" (values 2 or 3) |
| Bit 0 (LSB) | The request being answered |
| Bit *k* | The request with sequence number (current − *k*), for 1 ≤ *k* ≤ 63 |
| Set bit | That sequence number was received by the server in this session |
| Initial value | 0 at session creation |

**Observed upstream 0.9.1 semantics.** The table below is a summary of the
measured input → output vectors in
`test-vectors/SERVER_BEHAVIORAL_VECTORS.md` Section 1, expressed as a relation
between two values a client can see on the wire: the window in the previous
reply and the window in the next one. It is a description of observed output,
not a prescription of how to compute it — any method producing these values is
conformant.

Let `S` be the sequence number of the incoming request, `L` the sequence number
carried by the previous accepted request of the session, `W` the window value
carried by the previous reply (`W = 0` before the session's first request), and

```
Δ = (S − L) mod 2^32          -- unsigned 32-bit difference
```

Three cases account for every observed vector:

| Condition | Resulting window |
|-----------|------------------|
| Δ = 0 | `W \| 1` |
| 1 ≤ Δ ≤ 63 | `(W << Δ) \| 1` |
| Δ ≥ 64 | `0x1` |

Read through the cases a client actually encounters:

| Situation | Δ | Resulting window |
|-----------|---|------------------|
| First request of a session | any | `0x1` (because `W` is 0) |
| In order, no gap | 1 | `(W << 1) \| 1` |
| Duplicate of the most recent sequence number | 0 | `W`, unchanged |
| Gap ahead, within range | 2 … 63 | `(W << Δ) \| 1` |
| Gap ahead of 64 or more | ≥ 64 | **`0x1`** |
| **Out of order / late arrival** | large, because the unsigned difference wraps | **`0x1`** |

**The last two rows are the critical ones.** Because the difference is unsigned,
a sequence number *lower* than the previous one produces a very large Δ and
therefore falls into the third case. A late or reordered request does **not**
set a historical bit; it resets the window to `0x1`. The information that
earlier packets were received is lost, and it is not recovered by later packets
— the window rebuilds from scratch.

**Protocol requirement.** Bit 0 is always set in a window a server emits. A
compatible server MUST set bit 0. Consequently a window value of 0 is never
produced by upstream 0.9.1 when the window field is negotiated at all; clients
MAY continue to treat 0 defensively as "no information".

**Client-facing consequence, and why this must be stated.** A window of `0x1` is
a *valid* window that carries no information about earlier packets. It does
**not** mean the previous 63 sequence numbers were lost; it means no useful
prior received-history is represented in the window after the transition that
produced it. A client that reads `0x1` literally will classify up to 63 earlier
packets as lost upstream after a single reordering event or after any gap of 64
or more. Implementations of either side SHOULD treat `0x1` on a non-first packet
as "no historical information available" rather than "nothing earlier was
received".

This is a statement about what the window *represents*. How a client turns
window bits into an upstream/downstream loss classification is a separate
question, out of scope here, and is scheduled for its own audit on the clean
side; nothing in this section should be read as settling it.

**Sequence wraparound.** The comparison is modular over 32 bits, so a session
that crosses the u32 boundary continues normally. Measured with sequence numbers
4294967293, 4294967294, 4294967295, 0, 1: windows advanced
`0x1, 0x3, 0x7, 0xf, 0x1f` with counts 1…5, exactly as for a non-wrapping run.

**Sequence numbers need not start at 0.** A session whose first request carries
sequence 100 behaves identically to one starting at 0: window `0x1`, then `0x3`,
then `0x7`.

### 10.3 Behavioral Vectors

See `test-vectors/SERVER_BEHAVIORAL_VECTORS.md` for the full input → output
tables, and `captures/server-recv-window.pcapng` for a capture of the
gap / duplicate / reorder / far-gap sequence.

---

## 11. Timestamp Semantics

### 11.1 What Each Field Means

| Negotiated StampAt | Fields emitted | Instant represented |
|--------------------|----------------|---------------------|
| None (0) | none | — |
| Send (1) | send wall and/or send monotonic | The server's departure instant for this reply |
| Receive (2) | receive wall and/or receive monotonic | The server's arrival instant for this request |
| Both (3) | receive and send fields | Both of the above |
| Midpoint (4) | midpoint fields | The arithmetic mean of the receive and send instants |

The externally relevant property is the *relationship* between the two instants,
not where in a server's internals they are taken. The pair must bracket the
server's own handling of the request as tightly as the implementation can manage,
so that the difference between them is a usable estimate of server processing
time and the client's round-trip correction is not systematically wrong. How
closely a given implementation can bracket that interval is not externally
observable; see Section 23.7.

**Protocol requirement.** The receive instant MUST NOT be later than the send
instant for the same reply. Measured server-side processing durations on
loopback were on the order of a few microseconds.

### 11.2 Clock Encodings

| Clock | Field | Encoding |
|-------|-------|----------|
| Wall | wall fields | Signed 64-bit little-endian, **nanoseconds since the Unix epoch** |
| Monotonic | monotonic fields | Signed 64-bit little-endian, **nanoseconds since an arbitrary server-chosen origin** |

**Observed monotonic origin.** The origin is fixed for the life of the server
process and is shared by every session and every listener of that process. Two
sessions opened seconds apart on different address families reported monotonic
values differing by exactly the elapsed wall time between them, and values
tracked the server process's uptime. A client therefore MUST NOT assume the
origin is per-session, and MUST NOT compare monotonic values against its own
clock — only differences between two server monotonic values from the same
server are meaningful.

**Protocol requirement.** A compatible server MAY choose any monotonic origin,
provided it is stable for as long as any session it reports into may live.

### 11.3 Absent Fields

A timestamp field that is not negotiated is simply not present in the reply; it
is not zero-filled. Field presence is positional — receivers derive offsets from
the negotiated parameters, not from the packet.

### 11.3.1 Upstream 0.9.1 — Dual-Field Midpoint Wire Representation

**Black-box observed.** For StampAt = Midpoint, upstream 0.9.1 emits the midpoint
as **two** 8-byte timestamp fields — the midpoint **wall** field followed by the
midpoint **monotonic** field, in that order — **even when the negotiated Clock
selects only one clock**. The midpoint occupies that fixed two-field, 16-byte
region whenever it is present at all, so a single-clock midpoint reply carries
one 8-byte field more than the negotiated parameters imply.

This is a statement about bytes on the wire. It says nothing about how upstream
represents a midpoint internally, and nothing in this section depends on any
such model.

**Which negotiations are affected.**

| StampAt | Clock | Midpoint region on the wire |
|---------|-------|-----------------------------|
| Midpoint (4) | Wall (1) | wall + monotonic — **one field more than negotiated** |
| Midpoint (4) | Monotonic (2) | wall + monotonic — **one field more than negotiated** |
| Midpoint (4) | Both (3) | wall + monotonic — matches the negotiated layout |

Receive-only, send-only and both-timestamp modes are **not** affected: each of
those emits exactly the fields the negotiated Clock implies, for all three Clock
values. Measured for all 15 StampAt × Clock combinations; only the two
single-clock midpoint rows deviate.

A client can reach this configuration without asking for midpoint timestamps: a
server restricted to a single timestamp answers a request for both receive and
send with Midpoint (Section 11.4), so a Wall-only client that requested StampAt =
Both also receives the dual-field form.

**Effect on reply length.** The extra field enlarges the *minimum* reply length;
it does **not** add 8 bytes to every reply. Reply length follows the ordinary
rule of Section 9.2 — the larger of the negotiated length and the field block —
applied to a field block one timestamp field larger. Writing `normal_header` for
the block of fields the negotiated parameters imply:

```
normal_header   = negotiated field block
upstream_header = normal_header + one 8-byte timestamp field

compatible_reply_len    = max(negotiated_length, normal_header)
upstream_0_9_1_reply_len = max(negotiated_length, upstream_header)
```

The observable excess is therefore `upstream_0_9_1_reply_len −
compatible_reply_len`, which takes one of three forms depending on the
negotiated length:

| Negotiated length | Upstream 0.9.1 reply | Excess over a compatible server |
|-------------------|----------------------|---------------------------------|
| ≤ `normal_header` | `upstream_header` | **+8** |
| between `normal_header` and `upstream_header` | `upstream_header` | **+7 … +1**, shrinking as the negotiated length rises |
| ≥ `upstream_header` | exactly the negotiated length | **0** — the extra field displaces 8 bytes of payload |

Read the excess column across rows, not within one session: for any *single* set
of negotiated Params the upstream reply has exactly one length,
`upstream_0_9_1_reply_len`. The +8, +7…+1 and 0 values are what that one length
works out to under different negotiations. No negotiation admits a range of
lengths.

Measured with ReceivedStats = Both, StampAt = Midpoint, Clock = Wall
(`normal_header` 36, `upstream_header` 44):

| Negotiated length | 0 | 16 | 32 | 36 | 37 | 40 | 43 | 44 | 45 | 64 | 1024 | 4096 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Reply datagram | 44 | 44 | 44 | 44 | 44 | 44 | 44 | 44 | 45 | 64 | 1024 | 4096 |
| Excess | +8 | +8 | +8 | +8 | +7 | +4 | +1 | 0 | 0 | 0 | 0 | 0 |

The same pattern was measured for ReceivedStats = None, Count and Window, with
the two minima shifting accordingly (24/32, 28/36 and 32/40 respectively). At a
negotiated length of 4096 the midpoint region still contains both fields,
followed by fill — so the dual-field emission is unconditional, and only its
effect on the *total datagram length* depends on the negotiated length.

**Correction to earlier material.** Any statement that an upstream reply is "the
negotiated length plus 8 bytes", or "the normal packet length plus 8", is wrong
as a general claim. It holds only in the first regime above. For sufficiently
large negotiated packets the datagram length is exactly the negotiated length,
and the excess field is visible only as displaced payload.

**Effect on the values a positional decoder reads.** Field offsets are derived
from the negotiated parameters, and the extra field is the last field before the
payload. The two forms compare as follows:

```
Clock = Wall
  conforming single-clock form:   [midpoint_wall][payload...]
  upstream 0.9.1 form:            [midpoint_wall][midpoint_mono][payload...]

Clock = Monotonic
  conforming single-clock form:   [midpoint_mono][payload...]
  upstream 0.9.1 form:            [midpoint_wall][midpoint_mono][payload...]
```

- **Clock = Wall:** the negotiated field comes first in both forms, so an
  ordinary positional decoder reads the correct value.
- **Clock = Monotonic:** an ordinary positional decoder reads the first 8-byte
  region — which upstream fills with the **wall-domain** value — as the
  negotiated monotonic timestamp. Black-box verified with the upstream client: a
  0.9.1 client requesting monotonic-only midpoint reported server monotonic
  timestamps of approximately 1.79 × 10¹⁸ ns — a nanoseconds-since-epoch
  magnitude, not a process-relative one — and completed the test without error.

**The two forms are not generally distinguishable.** Once the negotiated length
reaches `upstream_header`, the equality

```
upstream_0_9_1_reply_len == compatible_reply_len
```

holds, and the conforming and upstream forms produce datagrams of **identical
length**:

```
conforming:   [mono][payload...]
upstream:     [wall][mono][payload...]
```

In that regime nothing in the datagram identifies which form was used. The eight
bytes following the first midpoint region are a monotonic timestamp under the
upstream form and ordinary payload under a conforming one. This ambiguity is
recorded here as a limitation, not resolved: no heuristic for telling the two
apart in the equal-length regime has been established, and this document does not
invent one.

**Version behavior.** Black-box measured: upstream 0.9.0 emits only the
negotiated midpoint field for a single clock (a monotonic-only midpoint reply
carries a process-relative value at the negotiated length). The dual-field form
appears in 0.9.1 and is **unchanged** in the tested post-0.9.1 development tree
(Section 21).

**Source-assisted historical conclusion (non-normative).** Contaminated-side
investigation of upstream's release history indicates that the difference between
0.9.0 and 0.9.1 recorded above is an **unintended regression** rather than a
designed protocol feature. This is a judgement reached with access to upstream
material on the contaminated side; it is **not** an independent observation, and
the measured 0.9.0-versus-0.9.1 difference stated above does not by itself
establish intent. Nothing in upstream's published release notes or documentation
refers to the behavior. No requirement below depends on this paragraph, and a
reader who discards it entirely loses nothing normative.

**Protocol requirement.**

- A compatible server SHOULD emit exactly the fields implied by the negotiated
  StampAt and Clock, including for midpoint. Emitting the conforming form is known
  to interoperate: a contaminated-side server built to emit only the negotiated
  midpoint field was driven by the upstream 0.9.1 client for Wall-only,
  Monotonic-only and dual-clock midpoint configurations, and the client — an
  independent implementation — completed every run cleanly.
- A compatible **client** MUST tolerate the verified upstream single-clock
  midpoint dual-field form. Specifically, when the negotiated parameters are
  StampAt = Midpoint with a single Clock, it MUST accept a reply whose length is
  **exactly** `upstream_0_9_1_reply_len`. It MUST NOT require the extra field to
  be present, and it MUST NOT treat its presence as an error.
- That tolerance is a **single additional accepted length, not a range**. For any
  one set of negotiated Params, `compatible_reply_len` and
  `upstream_0_9_1_reply_len` are two fixed numbers, and those are the only two
  lengths an echo reply may have. When
  `upstream_0_9_1_reply_len > compatible_reply_len` the upstream form is
  identifiable by length alone, and the difference between the two values is
  between 1 and 8 bytes. The +8, +7…+1 and 0 figures in the excess table earlier
  in this section are differences **across different negotiations**; they are not
  a range of acceptable lengths within one negotiation. A client MUST NOT apply a
  rule of the form "accept anything up to 8 bytes over", and MUST reject a length
  strictly between `compatible_reply_len` and `upstream_0_9_1_reply_len` exactly
  as it rejects any other unexpected length.
- The tolerance is otherwise **narrow and specific**. It is not a general rule
  that replies longer than expected are valid, and it does not license accepting
  arbitrary overlong echo replies. A compatible client MUST continue to reject
  replies *shorter* than `compatible_reply_len` — the normal compatible reply
  length, which is the larger of the negotiated Length and the mandatory field
  block, not the negotiated Length on its own — and MUST continue to apply its
  ordinary strict validation to every other malformed or unexpectedly sized
  datagram.
- A client that negotiates monotonic-only midpoint MAY read its monotonic value
  from the second 8-byte region of the midpoint area **only when the dual-field
  form is otherwise identifiable** — that is, when
  `upstream_0_9_1_reply_len > compatible_reply_len` for the negotiated Params and
  the datagram length is exactly `upstream_0_9_1_reply_len`, which happens only
  while the negotiated length is below `upstream_header`. A client MUST NOT apply
  that correction unconditionally
  whenever midpoint and monotonic are negotiated: in the equal-length regime it
  would interpret ordinary payload bytes from a conforming server as a timestamp.
- In the equal-length regime a client SHOULD record the value as ambiguous rather
  than guess. Whatever it decides, it MUST still accept the packet: a reply is
  not to be rejected merely because upstream might have produced the ambiguous
  form, and a client that applies no correction MUST NOT fail on the resulting
  implausible value.
- All of the above constrains a *client*. None of it is a licence, or a
  requirement, for a clean implementation's **server** to emit the extra field.

**Accepted echo reply lengths, in full.** For a given set of negotiated Params,
compute `compatible_reply_len` and `upstream_0_9_1_reply_len` once. A conforming
client then accepts exactly these lengths and no others:

| Received `packet_len` | Condition | Outcome |
|---|---|---|
| `== compatible_reply_len` | always | **accepted** — the normal exact form |
| `== upstream_0_9_1_reply_len` | single-clock Midpoint **and** `upstream_0_9_1_reply_len > compatible_reply_len` | **accepted** — the identifiable upstream midpoint compatibility form |
| any other value | — | **rejected** |

When `upstream_0_9_1_reply_len == compatible_reply_len` the two rows collapse:
the packet is accepted at the normal exact length, the wire form is ambiguous
between `[mono][payload…]` and `[wall][mono][payload…]`, and no generic monotonic
correction is applied. This is behavioral wording; it prescribes no particular
decoder structure.

### 11.4 Timestamp Allowance (Server Policy)

The reference server can restrict which timestamp modes it will provide. The
observed mapping from requested StampAt to negotiated StampAt:

| Requested | Allowance = dual | Allowance = single | Allowance = none |
|-----------|------------------|--------------------|------------------|
| None (0) | 0 | 0 | 0 |
| Send (1) | 1 | 1 | 0 |
| Receive (2) | 2 | 2 | 0 |
| Both (3) | 3 | **4 (midpoint)** | 0 |
| Midpoint (4) | 4 | 4 | 0 |

Note the substitution: under a single-timestamp allowance, a request for both
receive and send is answered with **midpoint**, not with one of the two. Clients
comparing negotiated against requested parameters will see this as a server
restriction.

---

## 12. Server Timing Relationships

A server's timestamps exist so a client can separate server processing time from
network time. The relationships a compatible server MUST make derivable:

```
server_processing = server_send − server_receive          (both from the same clock)
adjusted_rtt      = raw_client_rtt − server_processing
send_delay        = server_best_receive_wall − client_send_wall
receive_delay     = client_receive_wall − server_best_send_wall
```

where *best receive* is the receive timestamp when present, otherwise the send
timestamp, and symmetrically for *best send*. For midpoint timestamps both
resolve to the midpoint value, which makes `server_processing` identically zero
— measured: an upstream client using midpoint timestamps reports a server
processing time of 0 s in all four statistics.

**Protocol requirement.**

- When both receive and send timestamps are negotiated, they MUST be taken from
  the same clock domain, and MUST bracket the server's own handling of that
  request.
- One-way delays are only meaningful with externally synchronized wall clocks;
  a server has no obligation beyond reporting its own wall clock honestly.
- A server MUST NOT rewrite or smooth timestamps between packets.

Client-side statistics (IPDV, loss classification, aggregate reporting) are out
of scope here; see the client specification, Section 12.

---

## 13. Server Fill and Payload

### 13.1 What the Payload Is

Everything after the last present field in a reply, up to the reply's length, is
payload. Its content carries no protocol meaning; it exists to reach the
negotiated packet length.

### 13.2 Fill Negotiation

The ServerFill parameter (tag 9) is a length-prefixed UTF-8 string, maximum 32
bytes. Observed descriptor forms:

| Descriptor | Meaning |
|------------|---------|
| absent / empty | Client expresses no preference; the server uses its own configured fill |
| `none` | Do not fill; see Section 13.4 |
| `rand` | Fill with random bytes |
| `pattern:HH…` | Fill with the repeating byte pattern given as hex |

**Server policy.** The reference server matches the requested descriptor against
an allow-list of glob patterns (default: `rand` only). A descriptor that does
not match is replaced with the server's **default fill descriptor**, which is
returned to the client in the open reply.

Measured on a default-configuration server:

| Requested | Negotiated value returned | Actual reply payload |
|-----------|---------------------------|----------------------|
| `rand` | `rand` | random bytes |
| `none` | `pattern:69727474` | repeating `69 72 74 74` |
| `pattern:aabb` | `pattern:69727474` | repeating `69 72 74 74` |
| `bogus` | `pattern:69727474` | repeating `69 72 74 74` |
| 32-byte string | `pattern:69727474` | repeating `69 72 74 74` |
| 33-byte string | — | **open dropped, no reply** |

**Note.** The descriptor returned on refusal is the server's *default* fill
descriptor and does not necessarily describe the bytes the server will actually
send — a server configured with a different fill still returns the default
descriptor while filling with its configured fill. Clients MUST NOT rely on the
returned descriptor to predict payload bytes.

With a permissive allow-list, requested descriptors are honored and returned
verbatim: `pattern:aabb` produced `aa bb aa bb …`, `pattern:00` produced zeros,
`pattern:ff00` produced `ff 00 ff 00 …`. An unparseable hex body
(`pattern:zz`) falls back to the server default and returns
`pattern:69727474`.

### 13.3 Default Fill Bytes and Pattern Phase

**Upstream 0.9.1 behavior.** The default fill is the repeating four-byte pattern
`69 72 74 74`. The pattern's **phase is not reset per packet or per session**;
it advances continuously as bytes are consumed. Observed across four consecutive
replies with a 7-byte payload on one session:

```
seq 0 payload: 72 74 74 69 72 74 74
seq 1 payload: 69 72 74 74 69 72 74
seq 2 payload: 74 69 72 74 74 69 72
seq 3 payload: 74 74 69 72 74 74 69
```

and across separate sessions on the same listener, the phase carried over.

**Protocol requirement.** A client MUST NOT assume a reply payload begins at the
start of the fill pattern, and MUST NOT use payload bytes for any protocol
purpose. A compatible server MAY reset the phase per packet.

### 13.4 No Fill

**Upstream 0.9.1 behavior.** When the effective fill is `none`, no bytes are
written into the payload region. The bytes that appear there are unspecified
residual buffer content. In practice:

- when the request was at least as long as the reply, the client's own request
  payload is returned;
- when the request was shorter, the trailing bytes are residue from earlier
  traffic on that listener — observed to include fragments of a previous
  packet's parameter payload.

The reference server's own documentation describes `none` as insecure on public
servers, and its default allow-list excludes it.

**Protocol requirement.** A compatible server that supports a no-fill mode MUST
NOT emit residual bytes from other clients' traffic. Zero-filling or echoing
only the bytes actually supplied by the requester are both acceptable.

### 13.5 Interaction with Length and Authentication

- The payload region is what remains after all present fields. Enabling
  authentication consumes 16 bytes of the packet, reducing the payload for the
  same negotiated length.
- If the negotiated length is smaller than the field block, there is no payload.
- The fill is applied after all fields are written, so it never overwrites
  protocol data.

---

## 14. Authentication (HMAC)

### 14.1 Mechanism

When a server is configured with a key, an authentication field of 16 bytes
immediately follows the flags byte in **every** packet in both directions, and
the HMAC flag (0x08) is set.

The MAC is HMAC-MD5 over the **entire datagram** with the 16-byte
authentication field zeroed, exactly as specified in the client specification,
Section 13.4. A server computes and verifies it identically.

**Protocol requirement.** A server that is authenticating MUST set the HMAC flag
**and** write a valid MAC in every reply it sends, including echo replies. This
was verified the hard way: a server that authenticated open replies correctly
but omitted the HMAC flag from echo replies caused every echo reply to be
discarded by the upstream client, which then terminated abnormally with no
packets received.

### 14.2 Observed Behavior on Authentication Failure

Every case below results in a **silent drop**: no reply of any kind, and no
change to any session's state.

| Condition (server has a key) | Result |
|------------------------------|--------|
| Request has no HMAC flag and no field | Dropped |
| Request has the HMAC flag, MAC computed with a different key | Dropped |
| Request has the HMAC flag, one bit flipped in the MAC | Dropped |
| Request has the HMAC flag, MAC field all zeros | Dropped |
| Request too short to contain the MAC field (< 20 bytes) | Dropped |
| MAC field truncated (field shortened, rest shifted) | Dropped |
| MAC computed correctly but the HMAC flag cleared | Dropped |
| Valid open, valid echo, valid close | Served normally |

| Condition (server has **no** key) | Result |
|-----------------------------------|--------|
| Request has the HMAC flag set | Dropped |

**Protocol requirement.** Authentication failure MUST NOT produce a reply. A
server MUST reject a packet whose HMAC flag disagrees with its own
configuration, in **both** directions of mismatch. MAC comparison SHOULD be
constant-time.

Applies to all three request kinds: open, echo, and close. An unauthenticated
close request cannot tear down an authenticated session.

**Indistinguishability.** An authentication failure is externally
indistinguishable from every other drop: a packet bearing a *valid* token with a
bad MAC produces exactly the same silence as one bearing an unknown token.

**Protocol requirement.** A compatible server MUST NOT allow the result of a
session lookup to leak through its reply behavior for a packet that failed
authentication. Otherwise an unauthenticated peer could probe for live tokens.

### 14.3 Session Effect

An authentication failure leaves the session completely untouched. Measured: a
sequence of eight malformed or wrongly-keyed packets against a live session was
followed by a correctly authenticated echo that was served normally, with the
received count advancing only for the accepted packets.

---

## 15. CLOSE Lifecycle

### 15.1 Client-Initiated Close

**Request.** Direction client → server.

**Flags:** Close (0x04) set, Reply clear, Open clear, HMAC set iff
authenticating.

**Layout:** magic (3) + flags (1) + [authentication (16)] + token (8). Total
12 bytes unauthenticated, 28 bytes authenticated. There is no sequence number
and no payload.

**Observed server behavior:**

| Event | Result |
|-------|--------|
| Valid close from the session's endpoint | Session removed. **No reply of any kind is sent.** |
| Trailing bytes after the token | Accepted; session removed |
| Repeated close with the same token | Dropped as an unknown token; no reply |
| Echo after close | Dropped as an unknown token; no reply |
| Close from a foreign source port | Dropped; session **not** removed |
| Close bearing an unknown or zero token | Dropped; no reply |

**Protocol requirement.** A server MUST accept the close request and release the
session. It MUST NOT be expected to reply, and a compatible server that sends
nothing is fully conformant.

- **Upstream 0.9.1 sends no reply at all** to a normal client close. Verified in
  `captures/server-close-lifecycle.pcapng`, where the close request is the last
  packet in the exchange.
- Once the session is gone, a repeated close or a post-close echo is handled the
  same way as any unknown or stale token: silently discarded, no reply.
- A standalone Reply|Close datagram is *tolerated* by upstream clients but has no
  demonstrated purpose in protocol version 1. As a one-off experiment, an
  upstream client was driven against a server that answered close with
  magic + flags(Reply|Close) + [auth] + token; the run completed normally with
  correct results and nothing in the client's output changed, showing no sign the
  reply had been used. That establishes only that such a datagram is harmless.

**No production standalone close-reply codec is specified here.** Because no
version-1 client has been shown to do anything with such a datagram, this
document does not define one as required — or even as recommended — behavior. An
implementation that wants to emit one is not prohibited from doing so, but it
gains nothing observable and should not treat the shape above as a normative
encoding.

**The exact point at which the session becomes unavailable** is the moment the
close request is processed. The client has no acknowledgement and therefore no
way to distinguish "close delivered" from "close lost"; a lost close leaves the
session to be reclaimed by idle expiry (Section 18).

### 15.2 Server-Initiated Close

**When it happens.** A server signals close by setting the Close flag (0x04) on
an otherwise ordinary **echo reply**. There is no standalone server-initiated
close packet in protocol version 1.

**Upstream 0.9.1 behavior.** The single observed trigger is the maximum test
duration hard limit:

- when a maximum test duration is configured, the server clamps the negotiated
  duration to it during negotiation;
- it additionally enforces a hard deadline of **maximum duration + 2 seconds**,
  measured from the **first echo request** of the session;
- the first echo request arriving after that deadline is answered with a normal,
  complete echo reply — correct sequence number, token, statistics and
  timestamps — with the Close flag additionally set;
- the session is removed at the same time, so every later request is dropped as
  an unknown token.

Measured: maximum duration 1 s → close flag on the reply at 3.01 s after the
first echo; maximum duration 2 s → close flag at 4.06 s and again at 4.1 s in a
separate run. Captured in `captures/server-close.pcapng` (frame 36 carries
flags `0x06`).

A conforming client never reaches this deadline, because it stops sending at the
negotiated duration. The mechanism exists to bound sessions from clients that
ignore the negotiated duration.

**Observed client reaction.** An upstream 0.9.1 client that receives an echo
reply with the Close flag set:

1. **does not record** that reply's measurement;
2. terminates the run immediately, reporting a "server closed connection"
   condition;
3. prints the partial results gathered up to that point and exits with a success
   status;
4. sends its own close request for the session.

**Protocol requirement.**

- A server that terminates a session mid-test MUST do so by setting the Close
  flag on an echo reply; there is no other in-band mechanism.
- The close-flagged reply MUST otherwise be a well-formed echo reply for the
  request that triggered it — clients parse it before noticing the flag.
- A server MUST NOT expect an acknowledgement. The client's subsequent close
  request will be dropped as an unknown token, which is normal and harmless.
- The close-flagged reply MUST be authenticated when the session is
  authenticated.
- The session token MUST be released at the same time; it MUST NOT remain usable
  after a close-flagged reply.

---

## 16. No-Test Mode

A client may open and immediately close a session without running a test, in
order to validate parameters against a server.

**Request:** an open request with **both** Open (0x01) and Close (0x04) set.

**Reply:** Open | Reply | Close (0x07, or 0x0F with authentication), a **zero
token**, and the fully restricted parameter payload.

**Observed properties:**

| Property | Result |
|----------|--------|
| Session state created | **None.** No token is issued and nothing is retained |
| Token in the reply | All zeros |
| Parameters in the reply | The same restricted values a real open would return |
| Subsequent echo with the zero token | Dropped, no reply |
| Subsequent close with the zero token | Dropped, no reply |
| With authentication | Reply is authenticated normally; flags `0x0F` |
| Cleanup required | None |

**Protocol requirement.** A server MUST NOT create a usable session for a
no-test open, and MUST return a zero token. The Close flag in the reply here
means "acknowledged and finished", not "rejected"; the distinction is made by
the client from whether *it* set Close in the request. See the client
specification, Sections 6.2 and 8.3.

---

## 17. Malformed, Unexpected and Adversarial Packets

Compact reference table. "Session effect: none" means the referenced session (if
any) is left fully usable.

| Request condition | Session state | Auth state | Server response | Session effect |
|---|---|---|---|---|
| Datagram shorter than the fixed header | any | any | drop | none |
| Bad magic | any | any | drop | none |
| Any reserved flag bit set (flags > 0x0F) | any | any | drop | none |
| Reply flag set | any | any | drop | none |
| HMAC flag set, server has no key | any | n/a | drop | none |
| No HMAC flag, server has a key | any | fail | drop | none |
| Wrong / corrupted / zeroed / truncated MAC | any | fail | drop | none |
| Open, truncated parameter varint | n/a | ok | drop | none |
| Open, duration ≤ 0 or interval ≤ 0 | n/a | ok | drop | none |
| Open, out-of-range stats / stamp-at / clock enum | n/a | ok | drop | none |
| Open, server-fill string > 32 bytes or short buffer | n/a | ok | drop | none |
| Open, unknown parameter tags | n/a | ok | **open reply**, tags ignored | new session |
| Open, empty parameter payload | n/a | ok | **open reply** | new session |
| Open, protocol version ≠ 1 | n/a | ok | **open reply with version 1** | new session |
| Open with Close flag (no-test) | n/a | ok | **reply, zero token** | none created |
| Non-open, too short for the token | any | ok | drop | none |
| Non-open, unknown token | — | ok | drop | none |
| Non-open, zero token | — | ok | drop | none |
| Echo, correct token, foreign source port | live | ok | drop | none |
| Close, correct token, foreign source port | live | ok | drop | **none — stays open** |
| Echo, too short for the sequence number | live | ok | drop | none |
| Echo exceeding the configured maximum length | live | ok | drop | none |
| Echo arriving faster than the rate allowance | live | ok | drop | none |
| Echo with trailing bytes past the negotiated length | live | ok | **reply at negotiated length** | normal |
| Echo shorter than the negotiated length | live | ok | **reply at negotiated length** | normal |
| Echo with a duplicate sequence number | live | ok | **reply** | count +1, window unchanged |
| Echo with a lower sequence number than the last | live | ok | **reply** | count +1, window reset to `0x1` |
| Echo after the maximum-duration deadline | live | ok | **reply with Close flag** | session removed |
| Echo after idle expiry deadline | expired | ok | **reply** (served once) | session removed |
| Any request on a closed / expired session | gone | ok | drop | none |
| Close with trailing bytes | live | ok | **accepted**; no reply, as for any close | session removed |
| Echo-shaped datagram with the Open flag set, otherwise admissible | — | ok | treated as an **open**; the token bytes are parsed as parameters | new session if those bytes parse |
| Open flag set together with the Reply flag, or with an undefined flag bit | any | any | drop | none |

The last row restates the observable precedence of Section 5.4 in its narrow
form: for an otherwise admissible datagram, Open interpretation wins over Close
interpretation and over an echo-shaped body, so an echo-shaped datagram with the
Open flag set produces an open reply rather than an echo reply. That precedence
does not override the drop rows above — a datagram carrying the Open flag is
still dropped when the Reply flag is set, when an undefined flag bit is set, on
an authentication failure, or when its Open parameter payload is malformed or out
of range. A compatible server SHOULD behave the same way, so that a client's
retransmitted open is never mistaken for an echo.

---

## 18. Session Lifetime and Resource Policy

### 18.1 Lifetime Events

| Event | Effect |
|-------|--------|
| Open (non-no-test) | Session created, token issued |
| Open with Close (no-test) | No session created |
| Client close from the bound endpoint | Session removed immediately, no reply |
| Maximum test duration exceeded | Close-flagged echo reply, session removed (Section 15.2) |
| Idle expiry | Session removed (Section 18.2) |
| Server shutdown | All sessions gone, no notification |
| Client disappears | Nothing until idle expiry |

### 18.2 Idle Expiry — Observed Semantics

**Server policy.** The reference server has a configurable idle timeout,
default **1 minute**, with `0` meaning "never expire".

**Observed behavior**, measured with a 2-second idle timeout:

| Gap since the previous accepted echo | First request after the gap | The request after that |
|---|---|---|
| 5.0 s | answered | answered |
| 6.5 s | answered | answered |
| 6.9 s | answered | answered |
| 7.1 s | answered | **dropped** |
| 7.5 s | answered | **dropped** |
| 9.0 s | answered | **dropped** |

Two things follow, and both are externally observable:

1. **The effective expiry deadline is the configured idle timeout plus a
   5-second grace period.** Measured boundary between 6.9 s and 7.1 s for a
   2-second timeout.
2. **The first request that arrives after the deadline is still answered**, and
   the session is torn down as part of handling it. Only the *next* request is
   dropped. A compatible implementation MAY drop the first post-deadline request
   instead; clients cannot depend on either behavior, because from their side
   the deadline coincides with an unresponsive server.

**Upstream 0.9.1 behavior — a session that has never carried an echo request
never expires.** The idle clock starts at the first echo request, not at open.
Measured with a 2-second idle timeout: a session opened and left idle for 9
seconds with no echo request was still served afterwards. See Section 7.6 for
why this matters (open retransmissions).

**Protocol requirement.** Expiry itself is entirely server policy, and a
compatible server MAY choose any deadline or none. The one interoperability
constraint is negative: a server MUST NOT signal expiry to the client — the
client observes it only as silence.

**Robustness recommendation (clean implementation).** Apply an expiry deadline to
sessions that have never carried an echo request as well, rather than leaving
them resident indefinitely as upstream does.

Requests dropped for rate limiting still refresh the idle clock, so a client
sending faster than the allowance keeps its session alive without receiving
replies.

### 18.3 Resource Limits

**Observed upstream 0.9.1 policy.** The table below records what the reference
server was seen to do with its default configuration. Every row is an
observation about upstream's *policy*, not a protocol rule, and none of it is
behavior a compatible server is expected to reproduce.

| Limit | Observed |
|-------|----------|
| Maximum concurrent sessions | **None observed.** 1000 sessions from one source endpoint all succeeded and all remained usable |
| Per-peer session limit | **None** |
| Per-peer or global request rate limit | **None** beyond the per-session minimum-interval allowance |
| Maximum request length | Only if configured; unlimited by default |
| Maximum test duration | Only if configured; unlimited by default |
| Server-fill descriptor length | 32 bytes (hard) |

**Protocol requirement.** None. Nothing here is observable to a conforming
client, and a compatible server MAY impose session caps, per-peer caps and
admission limits freely. Because the protocol has no rejection reply, the only
way to refuse is to drop — a client hitting a cap sees an open timeout, which it
already has to tolerate.

**Explicitly not an interoperability requirement.** Upstream's absence of session
and per-peer bounds is an observed policy choice. It is **not** something a
compatible irtt-rs server should reproduce, and reproducing it would not improve
interoperability by any measure.

**Robustness recommendation (clean implementation).** A session is created by a
single unauthenticated datagram, opens are never deduplicated, and upstream never
expires a session that has not carried an echo request. The clean irtt-rs server
is therefore expected to operate under a **bounded** resource policy: bounded
total session state, and an expiry deadline that also covers never-used sessions.
The specific limits, the eviction strategy and the shape of that policy are a
clean-project design decision and are deliberately not prescribed here.

---

## 19. Protocol-Visible Limits and Defaults

Values marked *(protocol)* are fixed by the wire format. Values marked *(policy)*
are the reference server's defaults and are freely changeable by a compatible
implementation.

| Item | Value | Kind |
|------|-------|------|
| Magic | `0x14 0xA7 0x5B` | protocol |
| Flag bits defined | 0x01 Open, 0x02 Reply, 0x04 Close, 0x08 HMAC | protocol |
| Reserved flag bits | 0x10–0x80, must be zero | protocol |
| Token width | 64 bits, little-endian | protocol |
| Sequence width | 32 bits, little-endian, wraps modularly | protocol |
| Received count width | 32 bits, little-endian | protocol |
| Received window width | 64 bits, little-endian, bit 0 = current | protocol |
| Timestamp width | 64 bits signed, little-endian, nanoseconds | protocol |
| Authentication field | 16 bytes, HMAC-MD5, packet-wide with the field zeroed | protocol |
| Server-fill descriptor maximum | 32 bytes | protocol |
| Protocol version | 1 | protocol |
| Default port | 2112 | policy |
| Default minimum send interval | 10 ms | policy |
| Default burst allowance | 5 requests | policy |
| Default idle timeout | 1 minute | policy |
| Idle-expiry grace beyond the timeout | 5 seconds (measured) | policy |
| Maximum negotiable interval | idle timeout ÷ 4 (measured) | policy |
| Maximum-duration grace | 2 seconds (measured) | policy |
| Default maximum test duration | unlimited | policy |
| Default maximum packet length | unlimited | policy |
| Default fill pattern | repeating `69 72 74 74` | policy |
| Default fill allow-list | random fills only | policy |
| Default timestamp allowance | dual | policy |
| DSCP allowed by default | yes | policy |

**Client-side floor worth knowing.** Upstream clients reject any server-reduced
interval below **1 second**, in strict and loose mode alike. A server policy that
would produce a smaller interval makes the server unusable by those clients.

---

## 20. DSCP

**Protocol requirement — what the wire parameter is.** The DSCP parameter
(tag 8) carries the **raw IP TOS / Traffic Class byte**, with a useful range of
**0–255**. It is not a 6-bit codepoint. A server applies the negotiated value
directly as the TOS / Traffic Class byte of its echo replies, and a compatible
implementation MUST encode and interpret it the same way. The common Expedited
Forwarding marking is therefore **0xb8 (184)** on the wire, not 46. Verified by
capture; see `BLACKBOX_VERIFICATION_REPORT.md` Finding B and
`captures/dscp-session.pcapng`.

> **Out of scope: user-facing DSCP notation.** Whether an implementation's
> command line or configuration accepts a 6-bit codepoint (0–63) and shifts it
> left by two to produce the wire byte, or accepts the raw byte directly, is a
> presentation decision for that project. The clean irtt-rs project will decide
> and implement its own convention separately. Nothing about that choice is
> upstream wire behavior, and it does not belong in this specification.

**Server policy.** When DSCP is permitted and supported on the socket, the server
applies the negotiated value to its echo replies. When DSCP is disallowed, the
negotiated value is forced to 0 and returned as absent from the parameter
payload.

**Observed range handling.** The negotiated value is **not** range-checked
during negotiation. Values 0–255 are applied normally. A value above 255 is
accepted and echoed back in the open reply, but then every echo reply for that
session fails to send and is dropped — the client sees a session that opens and
then answers nothing.

**Robustness recommendation (clean implementation).** Reject or clamp DSCP
values outside 0–255 during negotiation rather than accepting a value that cannot
be applied, so that the value returned in the open reply is one the server will
honour.

DSCP is applied to echo replies only; open and close packets are unmarked (see
the client specification, Section 10.8).

---

## 21. Version Differences

### 21.1 0.9.0 → 0.9.1

Two independent things are recorded here and must not be conflated.

**What upstream's published release notes say.** They cover platform time
precision, server logging transport, a probe integration, build changes and bug
fixes. None of it is presented as a protocol change, and the protocol version
remained 1. Taken alone, the release notes would suggest no wire-visible
difference.

**What was measured on the wire.** One wire-visible difference exists that the
release notes do not mention: the single-clock midpoint layout. A 0.9.0 server
emits only the negotiated midpoint field; a 0.9.1 server emits both midpoint
fields. Measured side by side with identical scripted sessions:

| Server | StampAt = 4, Clock = Wall | StampAt = 4, Clock = Monotonic | StampAt = 4, Clock = Both |
|--------|---------------------------|--------------------------------|---------------------------|
| 0.9.0 | 36 bytes, one field | 36 bytes, one field, process-relative value | 44 bytes, two fields |
| 0.9.1 | **44 bytes, two fields** | **44 bytes, two fields** | 44 bytes, two fields |

(Received stats = both, negotiated length 0.) See Section 11.3.1.

**Correction to earlier material.** Any earlier statement that there were no
protocol-visible changes between 0.9.0 and 0.9.1 is wrong. That claim followed
the release notes rather than measurement; the measured difference above stands.

**Classification: one wire-visible difference in 0.9.1, alongside changes that
are not wire-visible.** Section 11.3.1 additionally records a source-assisted,
non-normative judgement that the wire-visible difference is unintended. That
judgement is separate from, and not required by, the measurement above.

### 21.2 0.9.1 → the tested post-0.9.1 development tree

**Provenance.** The development tree exercised for this study is the 0.9.1
release plus **six** subsequent upstream commits on the development branch: an
ECN change and its merge, a roadmap note, a documentation correction, and a
dependency update. Only one of them is a functional change. These six commits are
**not** part of the 0.9.1 release; everything in this subsection was measured
against a build of that development tree, and nothing here describes 0.9.1.

**Change: an opt-in server ECN option.**

- Adds a server command-line option that, per its documentation, ships ECN bits
  for the client to log, and states that it is IPv6-only, that it forces the
  source-IP-pinning option, and that it "disables UDP replies from server".
- **Observed behavior contradicts the documentation on the last point.** With
  the option enabled, sessions completed normally over both IPv4 and IPv6:
  packets sent/received 4/4, no loss, ordinary results. Replies are **not**
  disabled.
- Enabling it forces the source-IP-pinning behavior described in Section 5.3.
- ECN observation is a receive-side property read from the IP header; it does
  not change the IRTT payload in either direction.

**Timestamp layout is unchanged.** The full StampAt × Clock matrix and the
midpoint length sweep of Section 11.3.1 were re-measured against a build of this
development tree and produced results identical to 0.9.1, byte-position for
byte-position. The tested post-0.9.1 tree therefore behaves like 0.9.1 in this
respect, and none of the six commits changes the dual-field midpoint emission.

**Wire comparison.** A side-by-side run of the 0.9.1 release build and the
post-0.9.1 build, using an identical scripted request sequence (open with full
statistics and dual timestamps on both clocks; echo sequence 0, 1, 4, 3; close),
produced byte-identical structure: same open-reply layout, same reply lengths
(60 bytes), same flags, same counts (1, 2, 3, 4) and the same windows
(`0x1`, `0x3`, `0x19`, `0x1`), and no close reply in either case.

**Classification: CLI / server-policy difference. Not wire-visible.**

Nothing in this document changes for the tested post-0.9.1 tree.

### 21.3 0.1.x

Incompatible predecessor protocol. Out of scope.

---

## 22. Implementation Hazards Observed Upstream

These are behaviors a clean implementation should deliberately **not**
reproduce. They are listed because each is reachable from ordinary client
traffic, so a clean server has to decide what to do instead.

Everything in this section is an **observation plus a robustness
recommendation**. None of it is an interoperability requirement: no conforming
client can tell which choice a server made, because in every case the visible
effect is either silence or an ordinary reply.

### 22.1 Oversized Reply Terminates the Server

If the negotiated packet length exceeds the host's maximum outbound UDP
datagram size, the first echo reply fails to send with a non-transient error,
and upstream 0.9.1 treats that as fatal: the listener stops and the server
process exits. Observed on a host whose maximum outbound datagram size was
9216 bytes: a session negotiating 15000 bytes killed the server on its first
echo reply, and all subsequent clients received nothing.

The default configuration places no upper bound on the negotiated length, so
this is reachable without any malformed packet.

**Robustness recommendation (restated from Section 6.3).** Treat a per-packet
send failure as a per-packet event rather than a fatal one, and refuse to
negotiate a length the server cannot transmit.

### 22.2 Zero-Length Fill Pattern Terminates the Server

A server-fill descriptor of the form `pattern:` with an empty hex body
terminates upstream 0.9.1 during open handling, when the fill allow-list permits
pattern fills. The default allow-list does not, so a default-configured server
is not exposed; a server configured with a permissive allow-list is.

**Robustness recommendation.** Validate a fill descriptor before using it and
reject a zero-length pattern. Falling back to the server default — the behavior
already observed for an unparseable hex body such as `pattern:zz` — is the
natural response, and is what upstream already does for that neighbouring case.

### 22.3 Negotiated Length Not Bounded by Path or Interface

The negotiated length reported to the client is not clamped to the interface
MTU, but the reply is (Section 9.2). Clients treat a short reply as fatal, so
this produces a session that opens successfully and then fails immediately.

**Robustness recommendation.** Clamp the negotiated length to what the server can
actually emit, so that the value returned in the open reply is honest. (The
resulting reply length is then covered by the ordinary rule of Section 9.2, which
*is* an interoperability requirement — a reply shorter than the negotiated length
is fatal to upstream clients.)

### 22.4 Residual Payload Disclosure Under No-Fill

See Section 13.4. A no-fill mode that emits uninitialized buffer content can
leak bytes from other clients' traffic. The requirement not to emit another
peer's data is stated in Section 13.4; it is a security property of the
implementation rather than a wire-compatibility rule.

---

## 23. Remaining Unknowns

Each entry records the question, why compatibility might depend on it, what was
inspected, what was attempted, and what evidence would settle it.

### 23.1 Received-count wraparound

- **Question:** What does the 32-bit received count do at 2^32 requests in a
  single session — wrap, saturate, or something else?
- **Why it matters:** A client computing upstream loss from the count would see
  a large negative or nonsensical delta at the wrap point. Only reachable in
  extremely long, high-rate sessions.
- **Evidence inspected:** Field width and encoding confirmed from captures;
  counter observed to advance by exactly one per accepted request.
- **Attempted:** Not attempted. Reaching 2^32 requests is not feasible in a
  targeted experiment.
- **Would resolve it:** A server-side test harness able to preload a session
  counter, or a very long-running instrumented session.

### 23.2 Behavior under genuine resource exhaustion

- **Question:** What does a server do when session state or buffers can no
  longer be allocated?
- **Why it matters:** Determines whether a client sees an open timeout, a
  partial session, or a dead server.
- **Evidence inspected:** 1000 concurrent sessions from one endpoint were
  created with no refusals and no degradation; no admission control was
  observed.
- **Attempted:** Scaling to exhaustion was deliberately not attempted — it is a
  resource-starvation test, not a protocol test.
- **Would resolve it:** A memory-capped container running the reference server
  while a controlled harness opens sessions until allocation fails.

### 23.3 IPv6 zone / link-local session identity

- **Question:** Is the IPv6 zone index part of session identity in practice, and
  how are scoped addresses compared?
- **Why it matters:** A server bound to a link-local address on multiple
  interfaces could confuse two peers with the same address in different zones.
- **Evidence inspected:** IPv6 sessions over loopback work identically to IPv4;
  the IPv4 and IPv6 listeners of one port were confirmed to have separate token
  scopes.
- **Attempted:** Link-local multi-interface testing was not possible on the
  single-host test platform.
- **Would resolve it:** Two interfaces with the same link-local address in
  different zones, with clients on each.

### 23.4 Midpoint dual-field emission and non-upstream clients

- **Question:** Do client implementations other than upstream 0.9.1 tolerate the
  extra midpoint field, and do any require it?
- **Why it matters:** Section 11.3.1 recommends emitting only the negotiated
  fields; a client that computes an exact expected reply length and rejects
  longer replies would break against upstream servers, not against a compatible
  one.
- **Evidence inspected:** All 60 combinations of ReceivedStats × StampAt × Clock
  measured against the reference server; a negotiated-length sweep across the two
  minima for four received-stats settings; upstream client verified to accept
  both forms. The 0.9.0 build and the tested post-0.9.1 build were measured for
  comparison.
- **Note:** the question is narrower than it was. Because the length difference
  disappears once the negotiated length reaches `upstream_header`, a third-party
  client is only exposed to the length discrepancy at small negotiated lengths;
  at larger ones it is exposed only to the field *contents* in the monotonic-only
  case — which is also the regime in which the two forms cannot be told apart
  (Section 11.3.1).
- **Attempted:** Only upstream 0.9.1 was available as a second implementation.
- **Would resolve it:** Testing against a third-party client, or a survey of
  other implementations.

### 23.5 Reply-length capping on other platforms

- **Question:** Is the reply-length cap always the listener interface's MTU, or
  does it vary with how the platform reports interface MTUs?
- **Why it matters:** It determines the largest length a server can honestly
  negotiate.
- **Evidence inspected:** Measured on one host: replies capped at exactly the
  bind interface's MTU (1500 on an Ethernet interface); loopback with a
  16384-byte MTU was masked by a smaller maximum outbound datagram size.
- **Attempted:** Two interfaces with different MTUs on one host.
- **Would resolve it:** The same measurement on Linux and Windows hosts with
  jumbo-frame and tunnel interfaces.

### 23.6 Whether any trigger other than the duration limit sets Close on a reply

- **Question:** Are there conditions besides the maximum-duration hard limit
  that cause upstream to set the Close flag on an echo reply?
- **Why it matters:** A client must handle the flag whenever it appears; a
  server implementer needs to know which conditions warrant it.
- **Evidence inspected:** The duration limit was reproduced and captured. Idle
  expiry, shutdown, rate limiting, oversize requests and authentication failure
  were each exercised and none of them produced a close-flagged reply.
- **Attempted:** All the above, plus repeated close and foreign-endpoint close.
- **Would resolve it:** Exhaustive exercise of every server policy knob against
  a rogue client that keeps sending regardless.

### 23.7 How tightly server timestamps bracket the request

- **Question:** How much of a server's own send and receive path falls outside
  the interval its two timestamps describe?
- **Why it matters:** It bounds the accuracy of the client's server-processing
  correction.
- **Evidence inspected:** Receive-to-send deltas of a few microseconds on
  loopback; ordering (receive ≤ send) always held.
- **Attempted:** Cannot be separated from scheduling noise externally.
- **Would resolve it:** Hardware timestamping on both endpoints. Nothing about
  where an implementation takes its timestamps is externally observable, so this
  is likely to remain permanently open, and no requirement in this document
  depends on it. Section 11.1 states the relationship a compatible server should
  aim to make derivable.

---

## 24. Conformance Summary

The two lists below are **interoperability** requirements only: each is
observable by a conforming client, and each rests on a black-box experiment.
Robustness recommendations for the clean implementation are collected separately
in Section 24.3 and are deliberately not mixed in.

### 24.1 A conforming server MUST:

1. Speak UDP, validate magic and flag bits, and reject any packet with the Reply
   flag set.
2. Set the Reply flag on every packet it emits, and never the Open flag on an
   echo reply.
3. Silently drop every rejected packet — no error replies, ever.
4. Accept open requests without a token field, decode the parameter payload per
   the shared encoding rules, and return the **restricted** parameters together
   with a non-zero token.
5. Return ProtocolVersion = 1 in the open reply.
6. Return a zero token and the Close flag for a no-test open, and create no
   session for it.
7. Bind each session to its token **and** its source endpoint, and drop packets
   that fail either check without disturbing the session.
8. Echo the token and sequence number unchanged in every echo reply.
9. Emit the negotiated fields in the specified order, and pad to the negotiated
   length. For StampAt = Midpoint this means the fields the negotiated Clock
   implies and no others — a conforming server does not reproduce the upstream
   dual-field emission of Section 11.3.1.
10. Report a received count that includes the current request and excludes every
    dropped one.
11. Report a received window with bit 0 set, bit *k* meaning "sequence
    (current − *k*) was received".
12. Emit receive and send timestamps from a consistent clock domain, such that
    the pair brackets the server's own handling of that request and the receive
    instant is never later than the send instant.
13. Release the session on a valid close request. Sending nothing in response is
    correct and is what upstream does; no close-reply datagram is required, and
    none is specified here (Section 15.1).
14. Signal a server-initiated close only by setting the Close flag on an
    otherwise complete echo reply, and release the token at that point.
15. When authenticating: set the HMAC flag and a valid MAC on **every** reply,
    and drop silently on any authentication failure, with no reply behavior that
    could distinguish a known token from an unknown one.

### 24.2 A conforming server MUST NOT:

1. Reply to any malformed, unauthenticated, unknown-token or foreign-endpoint
   packet.
2. Interpret any request bytes beyond the token and sequence number.
3. Emit another peer's data, or uninitialized memory, as payload.
4. Return a negotiated packet length in the open reply that it will not
   actually emit.

### 24.3 Robustness recommendations for the clean implementation

Not interoperability requirements. No conforming client can observe whether these
were followed; they are here because each addresses a behavior observed upstream
that a new implementation should not copy.

1. Treat a per-packet send failure as a per-packet event, not a fatal one
   (Sections 6.3, 22.1).
2. Operate under a bounded session-resource policy, and expire sessions that have
   never carried an echo request (Sections 7.6, 18.2, 18.3). The specific limits
   and eviction policy are a clean-project decision.
3. Validate a fill descriptor before use, and reject a zero-length pattern
   (Section 22.2).
4. Reject or clamp DSCP values the socket cannot carry, during negotiation
   (Section 20).
5. Clamp the negotiated length to what the server can actually transmit
   (Sections 22.1, 22.3).

---

## 25. Related Documents

- `IRTT_CLIENT_PROTOCOL_SPEC.md` — wire format, parameter encoding, HMAC
  computation, client-side measurement semantics.
- `BLACKBOX_VERIFICATION_REPORT.md` — verification evidence, including the
  server-side section.
- `test-vectors/SERVER_BEHAVIORAL_VECTORS.md` — input → output vectors for this
  document.
- `test-vectors/README.md` — packet-level test vectors.
- `captures/` — packet captures referenced throughout.
- `../clean-room/CLEANROOM_NOTES.md` — clean-room boundary and audit record.
