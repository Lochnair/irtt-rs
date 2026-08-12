# IRTT Black-Box Verification Report

**Date:** 2026-04-28  
**irtt version:** 0.9.1 (protocol version 1, json format version 1)  
**OS:** macOS Darwin 25.3.0 arm64 (Apple Silicon)  
**tshark version:** TShark (Wireshark) 4.6.4  
**Loopback interface:** lo0

---

## Environment Check

Tooling used for Part I. Installation paths are omitted deliberately; only the
versions matter for reproducing these results.

```
$ irtt version
irtt version: 0.9.1
protocol version: 1
json format version: 1

$ tshark --version
TShark (Wireshark) 4.6.4
```

Reference server and client: the upstream **0.9.1 release**, unmodified.
Capture platform: macOS Darwin 25.3.0 arm64, loopback interface.

---

## Open Question Resolution

### 19.1 Open Request Field Layout — RESOLVED

**Question:** Does the open request include a connection token field (zeroed)?

**Test setup:** Captured a full session including open exchange.

**Command:**
```
irtt server -b 127.0.0.1:2112
irtt client -d 3s -i 1s 127.0.0.1:2112 -q
```

**Capture:** `captures/full-session.pcapng`

**Observed open request (frame 1):**
```
14 a7 5b 01 01 02 02 80 f8 82 ad 16 03 80 a8 d6
b9 07 05 06 06 06 07 06
```
- Bytes 0-2: Magic `14 a7 5b`
- Byte 3: Flags `01` (Open)
- Bytes 4+: Parameter payload (no connection token)

**Observed open reply (frame 2):**
```
14 a7 5b 03 13 52 71 87 ab b6 96 78 01 02 02 80
f8 82 ad 16 03 80 a8 d6 b9 07 05 06 06 06 07 06
```
- Bytes 0-2: Magic
- Byte 3: Flags `03` (Open | Reply)
- Bytes 4-11: Connection token `13 52 71 87 ab b6 96 78`
- Bytes 12+: Parameter payload

**Conclusion:** The open request does NOT include a connection token field.
The layout is: magic (3) + flags (1) + [HMAC (16) if applicable] + params.
This is a **normative requirement** — the connection token is absent in open
requests because it has not yet been assigned.

---

### 19.2 Minimum Packet Length — RESOLVED

**Question:** What is the minimum echo packet length for various
configurations?

**Test setup:** Multiple sessions with different --stats and --tstamp
settings, all with -l 0 (minimum length).

**Commands:**
```
irtt client -d 1s -i 1s --tstamp=none --stats=none 127.0.0.1
irtt client -d 1s -i 1s --tstamp=send --clock=wall --stats=count 127.0.0.1
irtt client -d 1s -i 1s --tstamp=receive --clock=monotonic --stats=window 127.0.0.1
irtt client -d 1s -i 1s --tstamp=midpoint --clock=both --stats=both 127.0.0.1
irtt client -d 1s -i 1s --tstamp=both --clock=both --stats=both 127.0.0.1
```

**Capture:** `captures/tstamp-none.pcapng`

**Observed minimum packet lengths (echo request/reply):**

| Stats | StampAt | Clock | Packet Length | Breakdown |
|-------|---------|-------|---------------|-----------|
| none | none | - | 16 bytes | magic(3)+flags(1)+token(8)+seq(4) |
| count | send | wall | 28 bytes | +recv_count(4)+send_wall(8) |
| window | receive | mono | 32 bytes | +recv_window(8)+recv_mono(8) |
| both | midpoint | both | 44 bytes | +recv_count(4)+recv_window(8)+mid_wall(8)+mid_mono(8) |
| both | both | both | 60 bytes | +recv_count(4)+recv_window(8)+recv_wall(8)+recv_mono(8)+send_wall(8)+send_mono(8) |

With HMAC, add 16 bytes (observed: 76 bytes for default+HMAC).

**Conclusion:** The minimum packet length formula is:
```
min_length = 4 (header) + 8 (token) + 4 (seq)
           + [16 if HMAC]
           + [4 if recv_count (stats=count or both)]
           + [8 if recv_window (stats=window or both)]
           + [8 per timestamp field present]
```

Timestamp field count depends on StampAt and Clock:
- none: 0 fields
- send/wall or receive/wall: 1 field (8 bytes)
- send/mono or receive/mono: 1 field (8 bytes)
- send/both or receive/both: 2 fields (16 bytes)
- both/wall: 2 fields (16 bytes)
- both/mono: 2 fields (16 bytes)
- both/both: 4 fields (32 bytes)
- midpoint/wall: 1 field (8 bytes)
- midpoint/mono: 1 field (8 bytes)
- midpoint/both: 2 fields (16 bytes)

This is a **normative requirement** — implementations must compute the
correct minimum to avoid malformed packets.

**Caveat added 2026-08-11:** the two single-clock midpoint rows describe the
*negotiated* layout, which is what a compatible implementation emits. Upstream
0.9.1 emits 2 fields (16 bytes) for midpoint with any clock; see Findings S-7 and
S-18.

---

### 19.4 Varint Encoding — RESOLVED

**Question:** Is the varint encoding byte-compatible with protobuf-style
LEB128/zigzag?

**Test setup:** Decoded parameters from captured open request packets and
verified against known values.

**Observed parameter encodings:**

| Parameter | Value | Zigzag Encoded | LEB128 Bytes |
|-----------|-------|---------------|--------------|
| ProtocolVersion=1 | 1 | 2 | `02` |
| Duration=3s | 3000000000 | 6000000000 | `80 f8 82 ad 16` |
| Duration=1s | 1000000000 | 2000000000 | `80 a8 d6 b9 07` |
| Interval=1s | 1000000000 | 2000000000 | `80 a8 d6 b9 07` |
| Length=1472 | 1472 | 2944 | `80 17` |
| ReceivedStats=Both | 3 | 6 | `06` |
| StampAt=Both | 3 | 6 | `06` |
| Clock=Both | 3 | 6 | `06` |
| DSCP=0xb8 | 184 | 368 | `f0 02` |
| ServerFill (tag) | 9 | n/a (uvarint) | `09` |
| ServerFill (length) | 24 | n/a (uvarint) | `18` |

All decoded correctly using standard protobuf-style zigzag + LEB128.

**Cross-checked with a separately written Python decoder**, implemented from the
wire-format description alone.

**Conclusion:** The varint encoding is standard protobuf-compatible. This
is a **normative requirement** for interoperability.

---

### 19.5 HMAC Computation Scope — RESOLVED

**Question:** Is the HMAC computed over the entire packet (including payload)
with the HMAC field zeroed?

**Test setup:** Captured HMAC session, extracted packet bytes, and recomputed
HMAC-MD5 with a separately written Python script.

**Commands:**
```
irtt server -b 127.0.0.1:2114 --hmac=testkey
irtt client -d 2s -i 1s --hmac=testkey 127.0.0.1:2114 -q
```

**Capture:** `captures/hmac-session.pcapng`

**Verification (Python):**
```python
import hmac, hashlib
key = b"testkey"
# Zero HMAC field (bytes 4-19), compute over entire packet
computed = hmac.new(key, packet_with_zeroed_hmac, hashlib.md5).digest()
```

**Results:**
| Packet Type | Captured HMAC | Computed HMAC | Match |
|-------------|--------------|--------------|-------|
| Open request | `ff9016a7aa537816...` | `ff9016a7aa537816...` | YES |
| Echo request (seq 0) | `d9874f82b3131031...` | `d9874f82b3131031...` | YES |
| Echo reply (seq 0) | `eb30c07d6375a30b...` | `eb30c07d6375a30b...` | YES |
| Close request | `f5cd0fa9de9d7d66...` | `f5cd0fa9de9d7d66...` | YES |

Also verified the spec's test vectors (18.1 and 18.2) — both match when
using the correct 92-byte packet size.

**Conclusion:** HMAC-MD5 is computed over the entire packet buffer with the
16-byte HMAC field zeroed. This is a **normative requirement**.

**Additional finding:** The test vectors in Section 18 are described as
"target length 256 bytes (shown truncated)" but the HMACs were actually
computed over 92-byte packets (76-byte header + 16-byte payload). The
"256 bytes" description is incorrect in the spec.

---

### 19.7 Server Received Window Validity — RESOLVED

**Question:** Is bit 0 always set for valid windows, making 0 a sentinel for
invalid?

**Observed window values from `captures/full-session.pcapng`:**

| Seq | Received Count | Received Window (hex LE) | Window (binary, low bits) |
|-----|---------------|-------------------------|--------------------------|
| 0 | 1 | `01 00 00 00 00 00 00 00` | ...0001 |
| 1 | 2 | `03 00 00 00 00 00 00 00` | ...0011 |
| 2 | 3 | `07 00 00 00 00 00 00 00` | ...0111 |

**Analysis:** Bit 0 (LSB) represents the current packet, which was
obviously received (it's being replied to). This bit is always 1 for
valid windows. A window value of 0 would indicate either an invalid window
or a bug.

**Conclusion:** Bit 0 is always set for valid windows. A window value of 0
can be used as an invalidity sentinel. This is a **compatibility
recommendation** — implementations SHOULD treat window=0 as invalid.

**Superseded in part by Finding S-2 (Part II).** Upstream 0.9.1 was later
confirmed never to emit 0 at all when the window is negotiated. The value that
actually needs care is `0x1`, which is a *valid* window carrying no history and
must not be read as "nothing earlier was received".

---

### 19.8 HMAC Mismatch Behavior — RESOLVED

**Question:** What happens when HMAC keys don't match?

**Commands:**
```
irtt server -b 127.0.0.1:2120 --hmac=correctkey
irtt client --hmac=wrongkey --timeouts=500ms 127.0.0.1:2120
irtt client --timeouts=500ms 127.0.0.1:2120  # no HMAC
```

**Observed behavior:**

| Scenario | Server Log | Client Result |
|----------|-----------|--------------|
| Wrong HMAC key | `[Drop] [BadHMAC] invalid HMAC: ...` | `[OpenTimeout] no reply from server` (exit 1) |
| No HMAC to HMAC server | `[Drop] [NoHMAC] no HMAC present` | `[OpenTimeout] no reply from server` (exit 1) |

**Conclusion:** The server silently drops packets with missing or invalid
HMAC. The client times out. This is **observed behavior** — the protocol does
not define an explicit HMAC error response.

---

### 19.9 Minimum Restricted Interval Safety Floor — PARTIALLY RESOLVED

**Question:** What safety floor does the client enforce for server-restricted
intervals?

**Test setup:** Server with -i 500ms (minimum interval), client requests
200ms.

**Command:**
```
irtt server -b 127.0.0.1:2123 -i 500ms
irtt client -d 2s -i 200ms 127.0.0.1:2123
```

**Observed:** `[ServerRestriction] server increased interval from 200ms to
500ms` — client exits with code 1 in strict mode. In loose mode, client
accepts 500ms interval.

**Limitation:** This test only confirms that the client rejects server-
increased intervals in strict mode. It does not confirm the exact safety
floor value for server-decreased intervals (which would require a server
that decreases interval below the client's request). The safety floor for
decreased intervals cannot be easily tested without a cooperative server
that sends deliberately small intervals.

**Conclusion:** Strict mode rejects any server parameter change by default.
The specific safety floor for decreased intervals remains an **open
question** that requires further testing.

---

### 19.10 Maximum Parameter Buffer Size — PARTIALLY RESOLVED

**Question:** Is there a maximum serialized parameter payload size?

**Test setup:** Open request with all parameters including a 24-character
ServerFill string.

**Observed:** Largest serialized parameter payload was 52 bytes (with
ProtocolVersion, Duration, Interval, Length=1472, ReceivedStats=both,
StampAt=both, Clock=both, DSCP=0xb8, ServerFill="pattern:aabbccddeeff0011").

**Analysis:** Even with maximum-value parameters and the longest reasonable
ServerFill string (32 bytes max), the parameter buffer stays well under
128 bytes. A rough upper bound:
- Tag+value overhead per param: ~6 bytes for large values
- 8 params × 6 bytes = 48 bytes
- Plus ServerFill: tag(1) + length(1) + string(32) = 34 bytes
- Total: ~82 bytes maximum

**Conclusion:** The 128-byte limit stated in the spec appears safe.
Reclassified as **compatibility recommendation** — implementations should
allocate at least 128 bytes for the parameter buffer.

**Superseded by Part II.** Upstream servers were later shown to accept an open
request carrying over 200 bytes of parameters. There is no protocol limit on a
*received* payload; 128 bytes is a local allocation choice only, and a receiver
should tolerate more rather than reject.

---

### 19.11 Field Ordering Verification — RESOLVED

**Question:** Is the field ordering in Section 8.1.3 correct?

**Test setup:** Multiple captures with different field combinations.

**Verified orderings:**

1. **No optional fields (stats=none, tstamp=none):**
   Token → Seq. Packet = 16 bytes. ✓

2. **Count + send wall:**
   Token → Seq → Recv Count → Send Wall. Packet = 28 bytes. ✓

3. **Window + receive mono:**
   Token → Seq → Recv Window → Recv Mono. Packet = 32 bytes. ✓

4. **Both + midpoint both:**
   Token → Seq → Recv Count → Recv Window → Midpoint Wall → Midpoint Mono.
   Packet = 44 bytes. ✓

5. **Both + both both (default):**
   Token → Seq → Recv Count → Recv Window → Recv Wall → Recv Mono →
   Send Wall → Send Mono. Packet = 60 bytes. ✓

6. **HMAC + both + both both:**
   HMAC → Token → Seq → Recv Count → Recv Window → Recv Wall → Recv Mono →
   Send Wall → Send Mono. Packet = 76 bytes. ✓

**Conclusion:** The field ordering in Section 8.1.3 is correct. This is a
**normative requirement** — incorrect ordering breaks interoperability.

---

### 19.3 Server Close During Test — NOT DIRECTLY TESTED (superseded)

**Limitation at the time:** Triggering a server-initiated close mid-test requires
specific server conditions that were difficult to set up reliably in a
black-box test. The spec's description (server MAY set Close flag on echo
reply) had not been verified externally, and was reclassified as **open /
verification required**.

**Resolved in Part II by Finding S-4.** The maximum-test-duration hard limit was
found to trigger it reliably, and the behavior was captured. This entry is
retained only as a record of the earlier state.

---

### 19.6 Send Timestamp Capture Timing — NOT TESTABLE

**Limitation:** How closely a client's own timestamp capture sits to its send
and receive operations is internal to that client and cannot be verified through
black-box testing, in either direction. Remains an implementation choice, and
nothing normative depends on it.

---

### 19.12 RTT When Server Processing > Raw Round-Trip — NOT DIRECTLY TESTED

**Limitation:** This edge case requires server processing time to exceed
the raw client-measured RTT, which is extremely unlikely on localhost. Would
require a heavily loaded server or deliberate delays. Remains an
**open question**.

---

## Additional Findings

### Finding A: Packet Count Formula Correction

**Spec claim:** Expected packet count = `floor(d / i) + 1`

**Test setup:** Multiple sessions with various duration/interval combinations.

**Command:** `irtt client -d <D> -i <I> 127.0.0.1 -q`

**Results:**

| Duration | Interval | Observed Packets | floor(d/i)+1 | ceil(d/i) |
|----------|----------|-----------------|-------------|-----------|
| 10s | 200ms | 50 | 51 | 50 |
| 1s | 200ms | 5 | 6 | 5 |
| 1s | 333ms | 4 | 4 | 4 |
| 1s | 500ms | 2 | 3 | 2 |
| 1s | 1s | 1 | 2 | 1 |
| 2s | 1s | 2 | 3 | 2 |
| 500ms | 100ms | 5 | 6 | 5 |
| 100ms | 10ms | 10 | 11 | 10 |
| 999ms | 1s | 1 | 1 | 1 |
| 1001ms | 1s | 2 | 2 | 2 |

**Analysis:** When duration is an exact multiple of interval, the packet at
exactly `start + duration` is NOT sent (exclusive end). The correct formula
is:

```
expected_packets = ceil(duration / interval)
```

Or equivalently:
```
if duration % interval == 0:
    expected_packets = duration / interval
else:
    expected_packets = floor(duration / interval) + 1
```

**Conclusion:** The spec's formula `floor(d / i) + 1` is incorrect. The
correct formula is `ceil(d / i)`. The spec MUST be updated.

---

### Finding B: DSCP Parameter Encodes TOS Byte Value

**Spec claim:** DSCP valid values are "0-63 (6-bit DSCP field)"

**Test setup:** Captured packets with various --dscp values.

**Results:**

| CLI Option | TOS Byte in IP Header | DSCP (6-bit) | Param Value Encoded |
|------------|----------------------|-------------|-------------------|
| --dscp=0xb8 | 0xb8 | 46 (EF) | 184 |
| --dscp=0x2e | 0x2e | 11 | 46 |
| --dscp=46 | 0x2e | 11 | 46 |
| --dscp=8 | 0x08 | 2 | 8 |
| --dscp=0x20 | 0x20 | 8 (CS1) | 32 |

**Conclusion:** The DSCP parameter encodes the raw value as specified on the
CLI, which is used directly as the TOS/Traffic Class byte in the IP header.
The range is 0-255, not 0-63. The spec MUST be updated to reflect that
the parameter is the TOS byte value, not the 6-bit DSCP field.

---

### Finding C: DSCP Not Applied to Open/Close Packets

**Observation:** In all captures, the IP TOS field for open request, open
reply, and close request packets is always 0x00, regardless of the
negotiated DSCP value. Only echo request and echo reply packets carry the
negotiated DSCP/TOS value.

**Verified with:** `captures/dscp-session.pcapng`

**Conclusion:** This is **observed behavior**. It may not be a strict
protocol requirement, but implementations should be aware that DSCP is only
applied during the active test phase.

---

### Finding D: No-Test Mode Packet Exchange

**Test setup:** Captured a no-test session.

**Command:** `irtt client -n 127.0.0.1:2113`

**Capture:** `captures/no-test.pcapng`

**Observed:**

Open request (frame 1):
```
14 a7 5b 05 [params...]
```
- Flags: `05` = Open (0x01) | Close (0x04)
- No connection token

Open reply (frame 2):
```
14 a7 5b 07 00 00 00 00 00 00 00 00 [params...]
```
- Flags: `07` = Open (0x01) | Reply (0x02) | Close (0x04)
- Connection token: `00 00 00 00 00 00 00 00` (zero)

**Conclusion:** In no-test mode:
- Client sets both Open and Close flags in the open request.
- Server replies with Open|Reply|Close and a **zero** connection token.
- The client treats this as successful completion, not rejection.
- The Close flag in the open reply is **not** a rejection when the client
  originally requested Close.

The spec's description of "An open reply with the close flag set: the
server rejected the session" needs clarification. The Close flag in the
reply indicates rejection ONLY when the client did not request Close in its
open request.

---

### Finding E: Test Vector Size Correction

The test vectors in Section 18 are described as "target length 256 bytes
(shown truncated)." Recomputing the HMAC from the captured bytes confirms that
both vectors (18.1 and 18.2) were computed over 92-byte packets (76-byte header
+ 16-byte payload), not 256-byte packets. The "256 bytes" description is
incorrect.

---

### Finding F: Default Server Fill Pattern

**Observation:** When no server fill is requested, the server fills reply
payloads with the repeating hex pattern `69 72 74 74` (ASCII "irtt"). This
is the default server fill as documented in server help:
`--fill=fill  payload fill if not requested (default pattern:69727474)`.

---

### Finding G: Close Packet Minimal Format

**Observed close packet:**
```
14 a7 5b 04 [8-byte connection token]
```
Total: 12 bytes. Contains only magic (3) + flags (1) + connection token (8).
No sequence number, no payload.

With HMAC:
```
14 a7 5b 0c [16-byte HMAC] [8-byte connection token]
```
Total: 28 bytes.

---

### Finding H: Unreachable Server Behavior

**Command:** `irtt client --timeouts=200ms,200ms 127.0.0.1:2121` (no server)

**Observed:** `Error: read udp4 ... connection refused` (exit 1)

On localhost, the OS delivers ICMP port unreachable immediately. On remote
hosts, the behavior would be a timeout after all retransmissions are
exhausted.

---

### Finding I: Parameter Restriction Behavior

**Commands:**
```
irtt server -b 127.0.0.1 -l 100    # max length 100
irtt client -l 200 127.0.0.1       # request length 200
```

**Strict mode (default):** `[ServerRestriction] server reduced length from
200 to 100` — exit code 1.

**Loose mode (--loose):** `[ServerRestriction] server reduced length from
200 to 100` — continues with length 100, exit code 0.

---

## Captures Created

All capture files referenced in this report were metadata-reviewed and their
containers rewritten to strip capture comments, section and interface
descriptions, host/user/machine identification, capture filter strings and
filesystem paths. Packet bytes and timestamps were not altered — full frame
hexdumps and per-frame epoch timestamps were verified identical before and after.

| File | Contents |
|------|----------|
| `captures/full-session.pcapng` | Complete session: open, 3 echoes, close |
| `captures/basic-session.pcapng` | Basic session (partial, missed open) |
| `captures/no-test.pcapng` | No-test mode: open+close only |
| `captures/hmac-session.pcapng` | HMAC authenticated session |
| `captures/tstamp-none.pcapng` | Multiple sessions with different timestamp modes |
| `captures/dscp-session.pcapng` | DSCP=0xb8 session |
| `captures/dscp-46.pcapng` | DSCP=46 and DSCP=0x2e sessions |
| `captures/dscp-values.pcapng` | DSCP=8 and DSCP=0x20 sessions |
| `captures/large-packet.pcapng` | 1472-byte packet session |
| `captures/long-sfill.pcapng` | Session with long server fill string |

## Test Vectors Created

See `test-vectors/` directory.

## Summary

### Resolved Open Questions
- **19.1** Open request field layout (no token)
- **19.2** Minimum packet length calculation
- **19.4** Varint encoding compatibility
- **19.5** HMAC computation scope
- **19.7** Received window validity (bit 0 sentinel)
- **19.8** HMAC mismatch behavior
- **19.11** Field ordering verification

### Partially Resolved
- **19.9** Minimum restricted interval safety floor (strict mode rejects,
  but exact floor for decreased intervals untested)
- **19.10** Maximum parameter buffer size (observed well under 128 bytes)

### Still Unresolved
- **19.3** Server close during test (hard to trigger)
- **19.6** Send timestamp capture timing (implementation detail, not testable)
- **19.12** RTT when server processing exceeds raw RTT (hard to trigger)

### Spec Corrections Applied
1. Packet count formula: `ceil(d/i)` not `floor(d/i)+1`
2. DSCP parameter: TOS byte value (0-255) not 6-bit DSCP (0-63)
3. Test vector size: 92 bytes not 256 bytes
4. No-test mode close flag: clarified in Sections 6.2, 8.3, and 14
5. Server close during test (19.3): reclassified as open/verification
   required; removed source-derived claims; spec now recommends
   defensive handling without asserting the behavior occurs
6. Received window late-packet behavior: removed unverified claim about
   late packets producing window=0; retained verified bit-0 sentinel

---
---

# Part II — Server-Side Verification

**Date:** 2026-08-11
**Baseline:** the upstream **0.9.1 release** (protocol version 1)
**OS:** macOS Darwin 25.5.0 arm64 (Apple Silicon)

**Method, and what each part of it establishes:**

- **Black-box observation.** A raw UDP harness drove a real upstream server
  directly, so that requests no conforming client would emit could be tested. It
  shares no code with any implementation. Everything it measured is stated here
  as observed wire behavior.
- **Black-box observation at the application layer.** The real upstream 0.9.1
  *client* was driven against various servers, and its externally visible
  reaction — what it recorded, what it reported, how it exited — was observed.
  The client is an independent second implementation for this purpose.
- **Contaminated-side consistency validation.** A server was also written on the
  contaminated side from the behavioral model and driven by the real upstream
  client. Because it was produced on this side of the boundary it is **not** an
  independent implementation, and no statement in this report rests on it. What
  it provides is a consistency check: it demonstrates that the model as written
  is sufficient to drive a real upstream client. The *client's* reaction to it is
  black-box observation; the server itself is not.

Findings feed `IRTT_SERVER_PROTOCOL_SPEC.md`. Input → output tables are in
`test-vectors/SERVER_BEHAVIORAL_VECTORS.md`.

---

## Part II Environment

```
$ irtt version
irtt version: 0.9.1
protocol version: 1
json format version: 1
```

**Provenance of the second binary.** A second server binary was built from the
upstream development tree as it stood **six upstream commits past the 0.9.1
tag**, for the version-difference comparison in Finding S-13. Those six commits
are post-release work and are **not** part of 0.9.1; every result attributed to
that build is labelled as such and never folded into the 0.9.1 baseline.

*(An earlier revision of this report said "8 commits past the 0.9.1 tag". That
count was wrong: it included two commits belonging to this clean-room project
itself, which were present in the same working tree but are not upstream work.
The upstream count is six.)*

---

## Finding S-1: Session identity is token + source endpoint

A post-open request is bound to a session by its token **and** by an exact match
on the source address, source port, and address family.

The identity trials below used live, non-expired sessions. Finding S-21 records
the separately observed expired-session release case.

| Case | Result |
|------|--------|
| Correct token, original endpoint | Served |
| Correct token, different source port | **Dropped, no reply; the live session remained usable from its original endpoint** |
| Correct token, other address family's listener on the same port | Dropped |
| Unknown token | Dropped |
| Zero token | Dropped |
| Token of a closed session | Dropped |
| **Close** bearing a valid token from a foreign source port | **Dropped; does not close an otherwise live session** |

Endpoint binding protects the close path as well as the echo path. A capture is
in `captures/server-session-identity.pcapng`.

**Classification:** SERVER-ONLY NEW INFORMATION.

## Finding S-2: Received window resets to `0x1` on reordering and large gaps

The single most consequential server behavior found. A request arriving with a
sequence number lower than the highest already seen, or 64 or more ahead of the
previous one, causes the reported window to become `0x1` — history is discarded,
not merely left unshifted, and it is never recovered.

```
seq 0 -> count 1 window 0x0000000000000001
seq 1 -> count 2 window 0x0000000000000003
seq 5 -> count 3 window 0x0000000000000031
seq 3 -> count 4 window 0x0000000000000001    <- gap fill discards history
```

Duplicates leave the window unchanged and advance the count. 32-bit sequence
wraparound is handled as ordinary modular arithmetic. Window value 0 was never
observed. Full tables in `test-vectors/SERVER_BEHAVIORAL_VECTORS.md` Section 1;
capture in `captures/server-recv-window.pcapng`.

The three-case rule stated in `IRTT_SERVER_PROTOCOL_SPEC.md` Section 10.2 was
cross-checked against a live server over 12 sessions of 40 requests each — 480
replies — with sequence numbers drawn from a mix of in-order steps, duplicates,
forward gaps of 2 to 80 and backward steps of 1 to 5, and starting sequence
numbers of 0, 1, 7, 100 and 4294967294. **480 replies checked, 0 mismatches** in
either window or count.

**Classification:** SERVER-ONLY NEW INFORMATION **and** CLIENT SPEC
CLARIFICATION (client spec Sections 12.8 and 19.15).

## Finding S-3: Received count semantics

The count advances by one for every request that reaches the reply stage,
including duplicates and out-of-order arrivals, and never for requests dropped
for bad magic, bad flags, bad authentication, unknown token, endpoint mismatch,
oversize, or rate limiting. It is per session and includes the request being
answered. Confirmed against a rate-limited server: 12 back-to-back requests
produced 5 replies with counts 1–5, and three later requests continued 6–8.

**Classification:** SERVER-ONLY NEW INFORMATION.

## Finding S-4: Server-initiated close exists and is triggered by the duration limit

Resolves client-spec open question 19.3. A server with a maximum test duration
answers the first otherwise-serviceable echo request past its hard deadline with
a complete echo reply carrying flags `0x06` (Reply|Close), and removes the
session at that moment.

| Configured maximum duration | Close flag observed at |
|---|---|
| 1 s | 3.01 s after the first echo |
| 2 s | 4.06 s / 4.10 s after the first echo |

The deadline is the maximum duration plus a 2-second grace, measured from the
session's first **served** echo — not from open. A deadline-crossing echo that is
itself rate-limited is dropped; the next served echo carries Close. Capture:
`captures/server-close.pcapng`, frame 36.

Driving an upstream 0.9.1 client against a server that sets the flag mid-test:
the client discards that reply's measurement, terminates with a "server closed
connection" condition, prints partial results, and exits successfully.

No other condition — idle expiry, shutdown, rate limiting, oversize, auth
failure — was observed to set the flag.

**Classification:** SERVER-ONLY NEW INFORMATION **and** CLIENT SPEC
CLARIFICATION (client spec Sections 13.7 and 19.3).

## Finding S-5: Client-initiated close is never acknowledged, and a close reply is harmless

Upstream 0.9.1 sends **nothing** in response to a close request. Repeated closes
and post-close echoes are dropped as unknown-token packets. Capture:
`captures/server-close-lifecycle.pcapng` ends with three unanswered client
packets.

A contaminated-side server that *does* reply to close — magic + flags(Reply|Close)
+ [MAC] + token — was driven against a real 0.9.1 client and the run completed
normally with correct results, with nothing in the client's output changing.
Sending such a datagram is therefore harmless, but nothing was observed to make
use of it: it has no demonstrated purpose in protocol version 1, and the server
specification does not define it as required behavior.

**Classification:** SERVER-ONLY NEW INFORMATION.

## Finding S-6: Protocol version is not enforced by the server

Open requests with `ProtocolVersion` of 0, 2, −1, or the tag omitted entirely,
each produced a session-creating open reply (flags `0x03`, non-zero token) with
`ProtocolVersion = 1` returned. The client spec's claim that "the server will
also set the Close flag if there is a version mismatch" is not true of upstream
0.9.1.

**Classification:** CLIENT SPEC CORRECTION (client spec Sections 6.2, 9.1,
19.13).

## Finding S-7: Dual-field midpoint wire representation

With StampAt = midpoint and a single negotiated clock, the reply carries both
midpoint fields — wall then monotonic — which is one 8-byte field more than the
negotiated parameters imply, and makes the datagram longer at small negotiated
lengths. Measured across every combination of received-stats and clock; confirmed
at the application layer by an upstream client reporting 36-byte packets while
receiving 44-byte datagrams and completing normally.

A contaminated-side server emitting only the negotiated midpoint field was driven
by the 0.9.1 client for wall-only, monotonic-only and dual-clock configurations,
and the client completed every run cleanly. This is consistency validation, not
an independent observation; what it shows is that the *client* accepts the
conforming form.

**Extended 2026-08-11 — see Finding S-18** for the negotiated-length dependence,
the version comparison, and the correction to the "always +8" characterisation.

**Classification:** SERVER-ONLY NEW INFORMATION **and** CLIENT SPEC
CLARIFICATION (client spec Sections 8.5 and 19.14).

## Finding S-8: Interval restriction resolves the client's safety floor

A server restricts the negotiated interval to at most one quarter of its idle
timeout (measured: 4 s → 1 s, 8 s → 2 s, 20 s → 5 s, 40 s → 10 s, 60 s → 15 s).
This gives an ordinary, unmodified server that reduces a client's interval, which
resolves client-spec open question 19.9.

| Server idle timeout | Returned interval | Client outcome |
|---|---|---|
| 2 s | 500 ms | Aborts — strict **and** loose |
| 3 s | 750 ms | Aborts — strict **and** loose |
| 4 s | 1 s (no reduction) | Proceeds |

Reduction to exactly 1 s is an ordinary restriction: rejected in strict mode,
accepted in loose mode. The safety floor is **1 second**.

**Classification:** CLIENT SPEC CLARIFICATION (client spec Sections 6.2, 19.9).

## Finding S-9: Every rejection is a silent drop

No error, reset, or NACK exists anywhere in the protocol. Verified for: short
datagrams, bad magic, reserved flag bits, the Reply flag on an inbound packet,
every authentication failure mode, malformed parameters, invalid parameter enum
values, unknown tokens, endpoint mismatch, oversize requests and rate limiting.
Full table in `IRTT_SERVER_PROTOCOL_SPEC.md` Section 17.

Minimum accepted request sizes without authentication: open 4 bytes, close
12 bytes, echo 16 bytes; add 16 bytes with authentication.

**Classification:** SERVER-ONLY NEW INFORMATION, CONSISTENT with the client
spec's HMAC-drop finding (19.8).

## Finding S-10: Authentication must be applied to replies, including echo replies

A contaminated-side server that authenticated its open reply correctly but
omitted the HMAC flag from echo replies caused the 0.9.1 client to discard every
echo reply and terminate abnormally with zero packets received. Fixing the flag
made all configurations pass. The load-bearing observation here is the **client's**
reaction, which is black-box; the server was merely the stimulus.

All authentication failures — missing flag, missing field, wrong key, flipped
bit, zeroed MAC, truncated MAC, flag/MAC disagreement in either direction —
produce a silent drop with no session state change. A session survived eight
consecutive failing probes and served the next valid request normally.

**Classification:** SERVER-ONLY NEW INFORMATION.

## Finding S-11: Idle expiry has a 5-second grace, and never-used sessions do not expire

With a 2-second idle timeout, the boundary was measured between 6.9 s and 7.1 s,
i.e. timeout + 5 s. A post-deadline echo that would otherwise be served is
answered and releases the session while handling it; only the next request is
dropped.

A session that has never carried an echo request does not expire at all: opened
and left idle for 9 seconds under a 2-second timeout, it was served normally.
Combined with the fact that every open — including a retransmitted one — creates
a new session, this means open retransmissions accumulate permanent state.

**Classification:** SERVER-ONLY NEW INFORMATION.

## Finding S-12: No session or per-peer limits by default

1000 sequential opens from a single source endpoint all succeeded with distinct
tokens, and the first, middle and last were all still usable afterwards. Two
opens from the same source port yield two independently usable sessions.

**Classification:** SERVER-ONLY NEW INFORMATION.

## Finding S-13: Tested post-0.9.1 tree — CLI-only difference

The development tree built for this study is **six upstream commits** past the
0.9.1 tag. Those commits are not part of the 0.9.1 release, and nothing in this
finding describes 0.9.1. The only functional change among them is an opt-in
server ECN option; the remainder are documentation, roadmap and dependency
updates.

- The option's documentation states that it "disables UDP replies from server."
  **Observed behavior contradicts this**: with the option enabled, sessions
  completed normally over both IPv4 and IPv6 (4/4 packets, no loss).
- Enabling it forces the source-IP-pinning policy.
- ECN observation is a receive-side property of the IP header and does not
  change the IRTT payload in either direction.

A side-by-side run of both builds with an identical scripted request sequence
(open with full statistics and dual timestamps on both clocks; echoes 0, 1, 4,
3; close) produced identical structure: same open-reply layout, 60-byte replies,
same flags, counts 1–4, windows `0x1`, `0x3`, `0x19`, `0x1`, and no close reply
in either case.

**Classification:** VERSION DIFFERENCE — CLI / server-policy. Not wire-visible.

## Finding S-14: Reply length is capped by the interface MTU but the negotiated value is not

On a 1500-byte-MTU interface, negotiated lengths of 1501, 2000, 3000 and 8000
each produced 1500-byte replies while the open reply returned the requested
value unchanged. Clients treat a short reply as fatal, so such a session opens
and then fails on the first echo.

**Classification:** SERVER-ONLY NEW INFORMATION.

## Finding S-15: Robustness hazards reachable from ordinary traffic

Two conditions terminate the upstream server process. Both are recorded in
`IRTT_SERVER_PROTOCOL_SPEC.md` Section 22 so that a clean implementation does
not reproduce them.

1. **Oversized reply.** If the negotiated length exceeds the host's maximum
   outbound datagram size, the send fails with a non-transient error, which is
   treated as fatal: the listener stops and the process exits. Observed on a
   host with a 9216-byte maximum: a session negotiating 15000 bytes killed the
   server on its first echo reply. The default configuration places no upper
   bound on the negotiated length.
2. **Zero-length fill pattern.** A server-fill descriptor of the form
   `pattern:` with an empty hex body terminates the server during open handling,
   when the fill allow-list permits pattern fills. The default allow-list does
   not, so a default-configured server is not exposed. An unparseable hex body
   such as `pattern:zz` is handled correctly by falling back to the server
   default.

**Classification:** SERVER-ONLY NEW INFORMATION.

## Finding S-16: Server fill is a continuous stream

The default fill is the repeating pattern `69 72 74 74`, and its phase is not
reset per packet or per session — it advances continuously as bytes are consumed
and carries over between sessions on the same listener.

```
seq 0 payload: 72 74 74 69 72 74 74
seq 1 payload: 69 72 74 74 69 72 74
seq 2 payload: 74 69 72 74 74 69 72
seq 3 payload: 74 74 69 72 74 74 69
```

Under a no-fill policy the payload contains unspecified residual bytes; one
observed reply's trailing bytes were a fragment of a previous packet's parameter
payload.

**Classification:** SERVER-ONLY NEW INFORMATION; also noted as a client-side
caution in client spec Section 8.5.

## Finding S-17: Contaminated-side consistency validation of the model

A server built from the behavioral model in `IRTT_SERVER_PROTOCOL_SPEC.md` — with
no upstream code — was driven by the real 0.9.1 client across: default
parameters; no statistics and no timestamps; midpoint with wall, monotonic and
both clocks; send-only with wall; receive-only with monotonic; a 200-byte packet
length; no-test mode; HMAC with a matching key; HMAC with a mismatched key; and
IPv6. Every configuration produced a clean, lossless run with correct results,
and the mismatched-key case correctly timed out.

**This is contaminated-side consistency validation, not independent
verification.** The server was written on this side of the clean-room boundary,
so it cannot corroborate anything about upstream. What it does show is that the
specification as written is sufficient to build a server the real upstream client
accepts — the client's acceptance being the black-box part. No statement anywhere
in the outgoing documents rests on this exercise.

**Classification:** Consistency validation, not a finding.

---

## Part II Captures Created

| File | Contents |
|------|----------|
| `captures/server-recv-window.pcapng` | Sequential, gap, duplicate, reorder and far-gap sequence numbers on one session |
| `captures/server-close-lifecycle.pcapng` | Client close, repeated close, post-close echo — none answered |
| `captures/server-close.pcapng` | Server-initiated close via the duration limit; frame 36 carries flags `0x06` |
| `captures/server-session-identity.pcapng` | Foreign-source-port echo and close, unknown token, original endpoint still served |

## Part II Vectors Created

`test-vectors/SERVER_BEHAVIORAL_VECTORS.md`.

## Part II Client Spec Changes Applied

| Section | Change | Class |
|---------|--------|-------|
| 2 | Pointer to the companion server specification | Editorial |
| 6.2, 9.1, 19.13 | Server does not enforce the protocol version | **CORRECTION** |
| 6.2, 19.9 | Interval safety floor pinned to 1 second, enforced in loose mode too | **CLARIFICATION** |
| 8.5, 19.14 | The one verified case in which a reply exceeds `compatible_reply_len`, scoped to the single extra length `upstream_0_9_1_reply_len`; `compatible_reply_len` rather than the negotiated Length is the normal minimum; payload content is not predictable | **CLARIFICATION** |
| 12.8, 19.15 | A window of `0x1` carries no history; window 0 is never emitted | **CLARIFICATION** |
| 13.7, 19.3 | Server-initiated close confirmed; observed client reaction | **CLARIFICATION** |
| 19.10 | No protocol limit on received parameter payload size | **CLARIFICATION** |

## Part II Summary

- Resolved client-spec open questions: **19.3**, **19.9**, **19.10**.
- New client-spec open questions resolved on discovery: **19.13**, **19.14**,
  **19.15**.
- Still unresolved on the client side: **19.6** (not externally testable),
  **19.12** (hard to trigger).
- Server-side unknowns are enumerated in `IRTT_SERVER_PROTOCOL_SPEC.md`
  Section 23.

---

# Part III — Midpoint Timestamp Follow-Up (2026-08-11)

A focused follow-up on Finding S-7. Three server builds were exercised with a raw
UDP harness that performs the open negotiation, sends one echo request, and
reports the exact reply datagram length and the bytes at the timestamp offsets:

| Build | Provenance |
|-------|-----------|
| **0.9.1** | the upstream release; the baseline for this study |
| **0.9.0** | the preceding upstream release, for comparison |
| **post-0.9.1 tree** | the upstream development tree six upstream commits past the 0.9.1 tag — post-release work, not part of any release |

All on macOS arm64, loopback.

**Provenance note.** The measurements below are black-box observations. Part III
additionally records one **source-assisted historical conclusion** — a judgement
about whether the measured 0.9.0/0.9.1 difference was intended — which is
labelled where it appears and is **not** presented as independently verified.
Nothing normative in the specifications depends on it, and the detailed
contaminated-side evidence behind it was retained on the contaminated side and
does not appear here.

## Finding S-18: Dual-field midpoint emission is length-dependent in effect

### The wire representation

For StampAt = midpoint, upstream 0.9.1 emits two 8-byte timestamp fields — wall
first, monotonic second — for every negotiated Clock, occupying a fixed 16-byte
region. Full StampAt × Clock matrix at negotiated length 0, received-stats =
both, against 0.9.1:

| StampAt | Clock = wall | Clock = monotonic | Clock = both |
|---------|--------------|-------------------|--------------|
| none | 28 | 28 | 28 |
| send | 36 | 36 | 44 |
| receive | 36 | 36 | 44 |
| both | 44 | 44 | 60 |
| **midpoint** | **44** (expected 36) | **44** (expected 36) | 44 |

Every non-midpoint row matches the negotiated layout exactly. Only midpoint
deviates, and only when a single clock is negotiated.

### The length dependence — correction to "always +8"

Reply length is the larger of the negotiated length and the field block; the
extra field enlarges only the field block. Writing `normal_header` for the block
the negotiated parameters imply and `upstream_header` for that block plus one
8-byte timestamp field:

```
compatible_reply_len     = max(negotiated_length, normal_header)
upstream_0_9_1_reply_len = max(negotiated_length, upstream_header)
```

Negotiated-length sweep, 0.9.1, midpoint + wall + received-stats = both
(`normal_header` 36, `upstream_header` 44):

| Negotiated length | 0 | 16 | 32 | 36 | 37 | 40 | 43 | 44 | 45 | 48 | 64 | 128 | 1024 | 4096 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Reply datagram | 44 | 44 | 44 | 44 | 44 | 44 | 44 | 44 | 45 | 48 | 64 | 128 | 1024 | 4096 |
| Excess over a compatible server | +8 | +8 | +8 | +8 | +7 | +4 | +1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

Repeated for received-stats = none, count and window; the two minima shift to
24/32, 28/36 and 32/40 and the pattern is identical. The dual-clock control case
(midpoint + both clocks) showed zero excess at every length.

At negotiated length 4096 the midpoint region still contains both fields,
followed by fill bytes — so the emission itself is unconditional; only its effect
on the datagram length depends on the negotiated length.

**This corrects the earlier characterisation.** "Upstream replies are the
negotiated length plus 8 bytes", or "the normal packet length plus 8", is true
only while the negotiated length is at or below `normal_header`. Between the two
minima the excess falls through +7…+1, and once the negotiated length reaches
`upstream_header` the excess is **0** — the reply is exactly the negotiated
length and the extra field displaces payload. Any surviving "+8 always" wording
elsewhere is stale and wrong.

**And it is a per-negotiation figure.** Each column of the sweep is a distinct
negotiation whose upstream reply has exactly one length,
`upstream_0_9_1_reply_len`. The +8 / +7…+1 / 0 values are what the difference
works out to across those different negotiations. No negotiation produced a range
of lengths, and no intermediate length between `compatible_reply_len` and
`upstream_0_9_1_reply_len` was ever observed. Wording that reads as "any reply up
to 8 bytes over is acceptable" is equally stale and wrong.

### Reachable without requesting midpoint

Against a 0.9.1 server restricted to a single timestamp, a request for StampAt =
both is answered with StampAt = midpoint (already recorded in the timestamp
allowance table). Measured: requested `3/wall` → negotiated `4/wall`, reply 44
where 36 was implied; requested `3/monotonic` → same; requested `3/both` →
negotiated `4/both`, reply 44, no excess.

### Value consequences for a positional decoder

With Clock = monotonic the first 8 bytes of the midpoint region hold the wall
value, so a decoder that expects one midpoint field reads a
nanoseconds-since-epoch magnitude into its monotonic slot. Measured with the
upstream 0.9.1 client itself (`--tstamp=midpoint --clock=monotonic`): reported
server monotonic timestamps of 1786437872024631833 and 1786437872225745750 ns —
about 56.6 years, not process uptime — with the run completing normally and
reporting 72 bytes sent against 88 bytes received for two packets.

**Limit of what this supports.** The misread is verified; a general correction is
not. Once the negotiated length reaches `upstream_header` the two forms are the
same size:

```
conforming single-clock:   [mono][payload...]
upstream 0.9.1:            [wall][mono][payload...]
```

and nothing in the datagram tells them apart. A client that unconditionally read
the second 8-byte region as monotonic would, against a conforming server, be
reading ordinary payload. The specifications therefore permit a correction only
where the dual-field form is otherwise identifiable, and record the equal-length
case as ambiguous rather than inventing a heuristic for it.

### Version comparison — black-box measured

Same harness, same parameters, negotiated length 0, received-stats = both:

| Build | midpoint + wall | midpoint + monotonic | midpoint + both |
|-------|-----------------|----------------------|-----------------|
| 0.9.0 | 36 bytes, one field | 36 bytes, one field, process-relative value | 44 bytes, two fields |
| 0.9.1 | 44 bytes, two fields | 44 bytes, two fields | 44 bytes, two fields |
| tested post-0.9.1 tree | 44 bytes, two fields | 44 bytes, two fields | 44 bytes, two fields |

0.9.0 emits exactly the negotiated fields; 0.9.1 emits the dual-field form; the
tested post-0.9.1 tree behaves like 0.9.1. The full matrix and the length sweep
were re-run against the post-0.9.1 build and matched 0.9.1 in every cell.

This is a measurement. It says which bytes each build put on the wire, and
nothing about why.

### Deliberate or defect — source-assisted historical conclusion

**Labelled contaminated-side judgement. Not an independent observation, and not
load-bearing.** Contaminated-side investigation of upstream's release history
indicates that the 0.9.0-to-0.9.1 difference measured above is an **unintended
regression** rather than a designed protocol feature. Nothing in upstream's
published release notes or documentation refers to the behavior, and none of the
six post-0.9.1 upstream commits changes it.

Three things about this paragraph:

1. The measured 0.9.0-versus-0.9.1 difference is stated separately, above, and
   stands entirely on its own.
2. That measurement is *consistent with* this judgement but does not establish
   it. Intent is not observable from the wire.
3. The contaminated-side evidence supporting the judgement was retained on the
   contaminated side and deliberately did not cross the boundary. Only the
   conclusion appears here.

No requirement in either specification depends on this paragraph. A reader who
disregards it loses nothing normative.

**Classification:** SERVER SPEC CORRECTION (server spec Sections 9.2, 11.3.1,
21.1, 21.2, 23.4, 24) **and** CLIENT SPEC CORRECTION (client spec Sections 8.5,
19.14).

---

# Part IV — Server Deep-Dive Black-Box Pass (2026-08-12)

A further black-box pass over the server, aimed at behavior that only a
non-conforming peer can reach: parameter presence, the lifetime boundaries, the
drop classes, reply sizing, and IP marking. It resolves several statements that
earlier parts left thin, and it records one previously undocumented upstream
robustness defect that is reachable from two unauthenticated datagrams.

## Part IV Method and Environment

**Builds exercised**, all unmodified, all run as ordinary server processes with
documented command-line options only:

| Build | Role in this part |
|-------|-------------------|
| **0.9.1 release** | the baseline; every unqualified statement refers to it |
| **0.9.0 release** | reproduction check |
| **development tree, six upstream commits past the 0.9.1 tag** | reproduction check; post-release work, not part of any release |

**Platform:** macOS Darwin 25.5.0 arm64. Loopback except where an Ethernet or
tunnel interface is named.

**Instrument.** An independent raw-UDP harness sharing no code with any IRTT
implementation. It performs the open negotiation itself, emits exact request
bytes, and records exact reply bytes and monotonic arrival times. For IP marking
it reads the received TOS / traffic-class byte from the kernel.

**Server provenance.** Each server under test is an unmodified build of the
revision named in the table above — the 0.9.0 release, the 0.9.1 release, or the
development tree six upstream commits past the 0.9.1 tag. No source change,
patch, instrumentation or test hook was applied to any of them, and each was run
as an ordinary server process configured only through documented command-line
options.

**What counts as evidence here.** Request bytes, reply bytes, the presence or
absence of a reply, the reply's IP header marking, arrival times, and whether a
server process was still serving afterwards. Timing boundaries were established
by sweeping probe offsets, not by reading any value out of anything. Where a
result depends on the tested host — interface MTUs, the maximum outbound datagram
size, socket handling of out-of-range values — it is labelled host-specific below
and in the specification.

**Repeatability.** Timing boundaries were swept at 50 ms spacing with three
repetitions per configured value. Behavioral results are single-shot unless a
repetition count is given.

## Finding S-19: An open whose restricted StampAt remains non-none without a Clock is accepted, then fatal

The most serious result of this pass.

When timestamp restriction leaves the restricted (effective) `StampAt` non-none,
an open request carrying `StampAt` = 1, 2, 3 or 4 and **no Clock tag at all** is
accepted: the server returns an ordinary session-creating reply with a valid
non-zero token, and the reply carries no Clock parameter — none is synthesized.
The **first** echo request on that session receives no reply and the server
process terminates. On a wildcard bind, the listener for the other address family
dies with it, so unrelated sessions stop being served at the same moment.

```
C -> S  14a75b01 05 06 06 06 04 00
        open; ReceivedStats=3, StampAt=3 (both), Length=0, no Clock tag
S -> C  14a75b03 7ca44127b2cd7eab 01 02 05 06 06 06
        open reply; non-zero token, ProtocolVersion 1, ReceivedStats 3,
        StampAt 3 — no Clock parameter
C -> S  14a75b00 7ca44127b2cd7eab 00000000
        first echo, sequence 0  ->  no reply; server no longer serving
```

| Input | Outcome |
|-------|---------|
| Requested StampAt 1 / 2 / 3 / 4, Clock tag absent; restricted StampAt remains non-none | open accepted, first echo fatal |
| Requested StampAt 0, Clock tag absent | open accepted, serves normally, no timestamps |
| StampAt 3, **explicit `Clock = 0`** | **open dropped** — no reply, no session |
| Requested StampAt 1 / 2 / 3 / 4, Clock tag absent; no-timestamp policy restricts StampAt to none | open accepted, serves normally, no timestamps |
| Requested StampAt 1 / 2 / 3 / 4, Clock tag absent; single-timestamp policy leaves restricted StampAt non-none | open accepted, first echo still fatal |

Reproduced for all four non-none StampAt values on **0.9.0, 0.9.1 and the
development build**. Capture: `captures/server-clock-absent.pcapng` (3 packets).

The absent-versus-explicit-zero contrast is the load-bearing part: the two inputs
differ by one encoded tag and produce opposite outcomes, so parameter *presence*
is externally meaningful in this upstream behavior. An implementation that
chooses to reproduce those distinct outcomes needs request validation that can
distinguish the two forms; this observation does not prescribe a clean server's
representation or policy for adversarial inputs.

Nothing here describes why the server stops. That is not observable from the
wire, it is not stated, and no conclusion in this report depends on it.

**Classification:** SERVER-ONLY NEW INFORMATION — upstream robustness defect
(server spec Sections 7.2, 22.5, 24.3; vectors Section 4.7).

## Finding S-20: A rate-limited request is the only tested drop class that keeps a session alive

For each drop class: open a session, send one echo, inject the packet under test,
send another echo, and read the second reply's count and window. Separate timing
trials established whether the injected packet moved the expiry boundary.

Every class tested advanced neither the received count nor the received window.
They divide on the second question:

| Injected packet | Extends the observed idle lifetime |
|-----------------|------------------------------------|
| Bad magic, invalid flag bits, Reply/Open flag on a data packet | no |
| Unknown token, zero token | no |
| Valid token from a foreign source endpoint | no (but see S-21) |
| Truncated echo, oversized echo | no |
| Bad, missing, zeroed or truncated MAC | no |
| **Request arriving with no rate allowance left** | **yes** |

The rate-limit result was separated from its alternatives three ways rather than
asserted from one trial: a session fed *only* too-fast requests outlived its idle
deadline; a session fed the same cadence of oversized requests expired on
schedule; and a silent session expired on schedule. A two-probe method was used
at the boundary so that "still alive" is distinguished from "expired but not yet
touched" — the distinction S-21 turns on. Reproduced on all three builds.

This is a statement about how long the session kept being served. What a server
records when it declines a request is not observable and is not claimed.

**Classification:** SERVER-ONLY NEW INFORMATION; strengthens the previously
unelaborated statement in server spec Section 18.2 (now Sections 9.3, 18.2;
vectors Section 8.2).

## Finding S-21: The first packet to release an expired session determines whether a final reply is emitted

Part II established that a post-deadline echo that would otherwise be served is
answered and releases the session while handling it. This pass establishes that
a packet which would normally be dropped can instead release the session without
receiving a reply, so no later request receives a final reply.

Both trials are in one capture, `captures/server-expiry-consumption.pcapng`
(14 packets), with a 3-second idle timeout — an 8-second effective boundary:

| Trial | Sequence | Client's first post-deadline echo |
|-------|----------|-----------------------------------|
| Control | nothing between the deadline and the client's request | **answered** (count 2); the next one dropped |
| Test | an echo bearing the correct token from a **foreign source port** arrived 0.3 s earlier, just past the same deadline | **no reply**, and none for the one after it |

The foreign-endpoint packet was dropped, as such a packet is at any time, and it
released the session without receiving a reply. Whether it advanced the session's
received count before the release is **not** observable here: no reply was ever
emitted for that session again, so no statistic carries the answer. Finding S-1's
result that a foreign-endpoint packet is not counted was measured on a **live**
session, where a later reply exposed the count, and it does not transfer to this
expired-session path. The same
release-without-a-final-reply outcome was produced by a truncated echo, an
oversized echo, a close from a foreign endpoint, and a burst of unrelated
ordinary opens.

Two scope limits are stated deliberately. These are the classes that were tested,
and the effect is not claimed for every possible packet. And nothing here
describes the order in which a server performs any internal step: what was
observed is that the session was gone when the legitimate request arrived, and
that in the control, with no intervening packet, it was not.

Separately, a burst of unrelated **ordinary opens** released an expired session
with no packet bearing its token involved at all, while a burst of **no-test**
opens did not — consistent with a no-test open creating no session.

**Classification:** SERVER-ONLY NEW INFORMATION (server spec Sections 16, 18.2;
vectors Section 8.3).

## Finding S-22: Both lifetime margins are fixed additive constants

Part II measured the idle grace at one timeout and the maximum-duration grace at
two maxima. Sweeping both across several configured values shows each margin is a
fixed addition, not a proportion, and not an artifact of when the session began.

Idle session-release sweep, with gaps measured from the last accepted echo. Each
probe used a first otherwise-serviceable echo after the gap and an immediate
follow-up; the boundary is inferred from whether that follow-up was dropped.

| Configured idle timeout | Longest gap with follow-up served | Shortest gap with follow-up dropped |
|-------------------------|----------------------------------|------------------------------------|
| 500 ms | 5.45 s | 5.50 s |
| 2 s | 6.95 s | 7.00 s |
| 4 s | 8.95 s | 9.00 s |

At and beyond the observed release boundary, the first otherwise-serviceable
post-gap echo may still receive a final lazy-release reply; the immediate
follow-up is then dropped. These values are not first-request rejection
thresholds.

Maximum duration, offsets measured from the first **served** echo:

| Configured maximum duration | Last offset answered normally | First offset carrying Close |
|-----------------------------|-------------------------------|-----------------------------|
| 500 ms | 2.45 s | 2.50 s |
| 1 s | 2.95 s | 3.00 s |
| 3 s | 4.95 s | 5.00 s |

Across the settings swept — idle timeouts of 500 ms, 2 s and 4 s, and maximum
durations of 500 ms, 1 s and 3 s — both margins held at five seconds and two
seconds respectively, to within the 50 ms probe spacing, and reproduced on all
three builds. The idle boundary did not shift with when the session was opened.

Each margin rests on three measured points. That is enough to rule out a margin
proportional to the configured value over the range tested, and it is what the
"additive" description means here. It does not establish the margin for every
configurable value: a threshold, a cap, or different behavior at much larger or
much smaller settings would not have shown up in these sweeps and remains
untested.

These are measured properties of the tested builds, stated with their tolerance.
They are upstream policy, not protocol constants, and no requirement in either
specification depends on either figure.

**Classification:** SERVER-ONLY NEW INFORMATION; strengthens Findings S-4 and
S-11 (server spec Sections 15.2, 18.2, 19; vectors Sections 3.2, 8.1).

## Finding S-23: Maximum duration starts at the first served echo and closes on the first later served echo

The boundary sweep establishes two distinct points. The **origin** is the first
echo request that is actually **served**. The **trigger** is the first echo after
the resulting deadline that would otherwise be served.

| Case | Result |
|------|--------|
| Session opened 5 s before its first echo | deadline unchanged — the open does not start the clock |
| First request oversized, dropped by the maximum-length policy | clock not started; a later served echo starts it |
| First request from a foreign source endpoint, dropped | clock not started; a later served echo starts it |
| Echo that crosses the deadline | full normal reply with Close set, and **counted** in the statistics it reports |
| Deadline-crossing echo that is itself rate-limited | dropped; the **next served** echo carries the Close flag |
| Client's next echo, and its close, after the flag | both unanswered — the token is released |

Two drop classes were tested in the first-request position — oversized and
foreign-endpoint — and neither started the clock. That is the extent of the
result: the remaining drop classes were not exercised in that position, and no
general "any dropped request" rule is claimed. The rate-limit row is a different
question, about a request crossing an already-running deadline rather than one
that would start it.

The close-flagged reply itself is in `captures/server-close.pcapng`, frame 36,
from Part II; the boundary sweep is a timing measurement with no capture of its
own.

**Classification:** SERVER-ONLY NEW INFORMATION; refines Finding S-4 (server spec
Section 15.2; vectors Section 3.2).

## Finding S-24: Repeated tags — last valid wins; invalid cases are limited

A tag may appear more than once in one open payload. Among individually valid
occurrences the **last** takes effect; this was confirmed for ProtocolVersion,
Duration, Interval, Length, ReceivedStats, StampAt, Clock, DSCP and ServerFill.

| Payload | Result |
|---------|--------|
| Duration 1 s, then 2 s | Duration 2 s |
| Duration 2 s, then 1 s | Duration 1 s |
| Clock 1, then 3 | Clock 3 |
| Clock 3, then 4 (out of range) | **no reply** |
| Duration 1 s, then 0 (invalid) | **no reply** |
| Duration 0 (invalid), then 1 s | **no reply** |
| ServerFill `rand`, then a 33-character string | **no reply** |

**Invalid repeated values were tested more narrowly.** An invalid Duration zero
occurrence was dropped in both tested positions: valid then invalid, and invalid
then valid. The Clock and ServerFill rows each tested an invalid second occurrence
and were also dropped. The opposite ordering was not tested for Clock or
ServerFill, and invalid duplicate occurrences were not tested for
ProtocolVersion, Interval, Length, ReceivedStats, StampAt or DSCP. These results
therefore do not establish a parser-wide, position-independent invalid-duplicate
rule for every parameter.

No conforming client emits a repeated tag; the result is recorded because a
decoder that stops at the first occurrence, or validates only the occurrence it
keeps, can diverge on the measured adversarial inputs.

Unknown tags were re-tested at 0, 10, 11, 100, 127, 128, 1000 and 2³². Each was
ignored with the rest of the payload still parsed, and an open carrying only
unknown tags created an ordinary default session. The value of an unknown tag is
consumed as a varint; no length-prefixed or string-shaped value form was accepted
for one.

Finally, a rejected open did not poison the next open: after each malformed
payload — incomplete trailing varint, tag with no value, length-prefixed string
running past the buffer, varint overflow — a well-formed open from the same
endpoint immediately afterwards was served normally. This establishes no
persistent externally visible effect on the following request; it does not
establish whether hidden accounting or temporary state exists.

**Classification:** SERVER-ONLY NEW INFORMATION (server spec Section 7.2; vectors
Section 4.6).

## Finding S-25: Reply sizing — negative lengths, the MTU cap, and the fatal ceiling

Three separate results that are easy to conflate.

**A negative Length is accepted and produces a minimum-size reply.** It is
returned in the open reply as sent. Echo replies for such a session are the size
of the field block the negotiated parameters imply — the ordinary rule of server
spec Section 9.2 applied to a negotiated length below that block. There is no
negative-size concept on the wire.

**On the two interfaces where the cap was reachable, the emitted reply is capped
at the bind interface's MTU.** Re-measured across three interfaces of one host:

| Bind interface (MTU) | negotiate 1500 | 1501 | 2000 | 8000 |
|----------------------|----------------|------|------|------|
| loopback (16384) | 1500 | 1501 | 2000 | 8000 |
| ethernet (1500) | 1500 | **1500** | **1500** | **1500** |
| tunnel (1280) | **1280** | **1280** | **1280** | **1280** |

On the 1500-byte and 1280-byte interfaces the cap is byte-exact — a negotiated
1281 produces 1280 — and the open reply still returns the requested value
unclamped, which is the Part II Finding S-14 result confirmed on one further
interface.

The loopback row is weaker evidence and is **not** an observation of a
16384-byte cap. It shows only that lengths up to 8000 were emitted unclamped. The
loopback cap cannot be approached on this host, because a reply beyond roughly
9300 bytes ends the server process before any such length is reached — see the
next result. **Host-specific:** these are this host's interface MTUs.

**A length the host cannot transmit terminates the process.** On the same host,
whose maximum outbound datagram size is 9216 bytes, replies up to 9216 bytes were
sent normally and negotiated lengths producing replies from roughly 9300 bytes
upward failed on the first echo and ended the server. With a wildcard bind, a
fatal send on the IPv4 listener took the healthy IPv6 listener down with it —
sessions on a different address family, unrelated to the offending one, stopped
being served at the same instant. **Host-specific:** 9216 is this host's outbound
datagram limit, not a protocol figure.

On the tested host, this failure was reachable in configurations whose effective
reply cap exceeded the host's outbound datagram ceiling, including the tested
loopback and wildcard cases. The tested 1500- and 1280-byte interface-bound
listeners could not reach it because their reply caps were lower. Jumbo interfaces
and other platforms were not tested.

**Classification:** SERVER-ONLY NEW INFORMATION; strengthens Findings S-14 and
S-15 (server spec Sections 7.3, 9.2, 22.1, 23.5).

## Finding S-26: Over-MTU requests show an interface-MTU effective length boundary

Bound to an interface with a 1500-byte MTU, a 3000-byte request was dropped with
a configured maximum of 1499 and accepted with a maximum of 1500. On a 1280-byte
tunnel the same decision knee was at 1280. The observed maximum-length decision
therefore behaved as though its comparison length was capped at the tested
interface boundary. Separately, the policy was strict at the ordinary tested
sizes: with a maximum of 1000, a 1000-byte request was answered and a 1001-byte
request was dropped.

> **Black-box inference.** Receive-path truncation before the maximum-length
> decision is one explanation consistent with the 1499/1500 and 1279/1280
> boundaries, but the experiments cannot distinguish it from another mechanism
> that yields the same effective comparison length.

**Host-specific and environment-specific.** This is an observation on the
tested host and interfaces, not a wire rule. It does not prescribe a clean
server's receive strategy or maximum-length policy. Negotiated Length is not by
itself a valid receive-buffer upper bound, because mandatory protocol structure
can require a larger datagram.

**No capture accompanies this finding.** The capture taken during the experiment
was recorded on loopback with no maximum-length policy configured, and there all
three oversized requests were answered normally at the negotiated length; it
demonstrates neither the effective boundary nor a policy drop, and the behavior it does
show is already covered by server spec Section 5.5 and vectors Section 5.4. It
was reviewed and not admitted. The textual result above is the evidence.

**Classification:** SERVER-ONLY NEW INFORMATION, environment-specific (server spec
Section 9.4).

## Finding S-27: DSCP marking is per session, with unmarked opens and marked closes

Four sessions were opened on one listener negotiating 46, 8, 0 and 184, then
driven in three interleaved rounds. Every reply's IP header was read from the
kernel. Capture: `captures/server-dscp-interleaved.pcapng` (32 packets).

| Session | Requested | Open reply TOS | Echo reply TOS, rounds 1–3 |
|---------|-----------|----------------|-----------------------------|
| 1 | 46 | **0x00** | 0x2e, 0x2e, 0x2e |
| 2 | 8 | **0x00** | 0x08, 0x08, 0x08 |
| 3 | not requested | **0x00** | 0x00, 0x00, 0x00 |
| 4 | 184 | **0x00** | 0xb8, 0xb8, 0xb8 |

For each of these sessions, whose negotiated values are in `0..=255`, every echo
reply carried its own session's value verbatim; no reply carried another session's
value in any round, and every open reply was unmarked. Negative and out-of-range
values are addressed separately below.

**Server-initiated close replies: measured, but not shown by any admitted
capture.** The DSCP experiment recorded that a close-flagged echo reply carries
the same in-range session mark as an ordinary one. Neither admitted capture
demonstrates it, and a reader checking them will find nothing that does: the
interleaved capture above contains no close-flagged reply at all, and
`captures/server-close.pcapng` — which does contain one, at frame 36 — is TOS 0
throughout, because that session negotiated no DSCP. The result therefore rests
on the measurement, not on capture evidence, and the earlier wording that
explained it as "an ordinary marked echo reply that also sets the Close flag" has
been dropped: that phrasing argued from the flag rather than reporting what was
observed. A capture of a close-flagged reply on a session with a nonzero
negotiated DSCP would settle it directly and was not taken.

**On a listener that has already sent marked replies, an open reply is still
unmarked — evidenced separately.** This capture does not show that ordering: its
four open exchanges are frames 1–8 and its first marked echo reply is frame 10,
so no open reply in it follows a marked one. The ordering is instead established
by `captures/dscp-values.pcapng` from Part I, where one listener serves two
sessions in sequence: frame 4 is a marked echo reply (TOS `0x08`) and frame 7 is
the next session's open reply, unmarked, on the same port. The two pieces of
evidence are cited separately rather than merged.

Observed negative and out-of-range DSCP handling is host-specific, not protocol
behavior: a requested −1 appeared as TOS 255 on IPv4 and as traffic class 0 on
IPv6. A value of 256 or above was negotiated and echoed back, but no echo reply
was observed for that session while the server continued serving its other
sessions. The black-box result does not identify whether the absence arose while
applying the marking, during transmission, or at another stage.

**Classification:** SERVER-ONLY NEW INFORMATION; extends Findings B and C of
Part I (server spec Section 20; vectors Sections 10.1–10.3).

## Finding S-28: The default fill phase is continuous across the tested listeners

Finding S-16 recorded that the default fill pattern's phase is not reset per
packet or per session and carries over between sessions on one listener. It also
carries over between **listeners**: in the tested configuration — one server
bound to both an IPv4 and an IPv6 listener — successive echo replies drawn from a
session on each continued the same repeating pattern without resetting.

That is the scope of the result: phase continuity across the sessions tested and
across those two listeners. It is not established that a single stream is shared
by every session of a process in every configuration, and nothing here describes
how the bytes are produced.

**No recommendation is drawn from it.** The default fill is a fixed, public
pattern, so phase continuity across sessions discloses nothing about another
peer's traffic — unlike the no-fill mode of Finding S-16's second paragraph and
server spec Section 13.4, which is a real disclosure risk and carries its own
requirement there. A compatible server may reset the phase, continue it, or
derive the payload some other way. Those choices can produce different payload
bytes, making the phase policy observable in a packet capture, but they are
interoperability-equivalent because a conforming client cannot depend on payload
phase. No fill-state arrangement is prescribed.

**Classification:** SERVER-ONLY NEW INFORMATION; extends Finding S-16 (server spec
Section 13.3).

## Part IV Version Comparison

Every result in this part was checked against the 0.9.0 release and the tested
development build wherever a reproduction check was meaningful. All of the
following were **identical** on all three builds:

| Behavior | 0.9.0 | 0.9.1 | development build |
|----------|-------|-------|-------------------|
| Open whose restricted StampAt remains non-none and has no Clock is accepted, first echo fatal (S-19) | yes | yes | yes |
| Idle boundary at timeout + 5 s (S-22) | yes | yes | yes |
| Maximum-duration close at maximum + 2 s (S-23) | yes | yes | yes |
| A rate-limited request keeps the session alive (S-20) | yes | yes | yes |
| Interface-MTU effective length boundary (S-26) | yes | yes | yes |
| Burst allowance behavior | yes | yes | yes |
| Fill phase continuous across the tested listeners (S-28) | yes | yes | yes |

No new version difference was found. The single known wire-visible difference
between these builds remains the single-clock midpoint layout of Findings S-7 and
S-18, which this pass re-observed unchanged and which is not restated here.

## Part IV Captures Created

| File | Packets | Contents |
|------|---------|----------|
| `captures/server-clock-absent.pcapng` | 3 | An open selecting timestamps with no Clock tag is answered with a token and no Clock parameter; the first echo is the last packet in the exchange |
| `captures/server-expiry-consumption.pcapng` | 14 | Control versus foreign-endpoint-first, in one file: whether the packet that releases an expired session emits a final reply |
| `captures/server-dscp-interleaved.pcapng` | 32 | Four interleaved sessions with distinct DSCP values over three rounds; no cross-session leakage; unmarked open replies |

Each was container-rewritten to the convention already used for the captures of
Parts I–III — packet records and the rewriting tool's version string only, with
no capture comment, host, user, interface or filter metadata and no filesystem
path. Packet bytes and per-frame timestamps were verified identical before and
after, and the packet counts above are unchanged from the originals.

Two further captures from this pass were **not** admitted. One was materially
redundant with `captures/server-close.pcapng`, which already shows the
close-flagged reply, the trigger being counted, and the following packet
unanswered; a single-maximum capture cannot evidence the additive-margin result,
which is the new part. The other is described under Finding S-26. In both cases
the textual result is the evidence, which is the better trade.

## Part IV Client Spec Change Applied

| Section | Change | Class |
|---------|--------|-------|
| 19.3 | Maximum-duration deadline origin is the first echo request that is **served**; the close trigger is the first later echo that would otherwise be served. The two tested first-request drops do not start the clock, and a deadline-crossing rate-limited echo does not carry Close. The 2-second margin is additive across configured maxima | **CLARIFICATION** |

The client specification and the server specification are both compatibility
baselines, so the lifecycle rule is stated the same way in each. The earlier
client-side measurements are unaffected — the first request was served in those
runs — and no client behavior depends on the distinction, since a conforming
client stops sending at the negotiated duration and never reaches the deadline.

## Part IV Summary

- New upstream robustness defect documented: **S-19**, an accepted open that is
  fatal on its first echo, reachable in the default configuration from two
  unauthenticated datagrams, present in all three builds.
- Statements strengthened from thin or single-point evidence: the idle grace and
  maximum-duration grace (**S-22**), the maximum-duration origin (**S-23**), the
  rate-limit/idle interaction (**S-20**), the reply-length cap (**S-25**), and the
  fill-phase scope (**S-28**).
- New parser-edge results: repeated tags and unknown-tag value encoding
  (**S-24**), and the presence-versus-explicit-zero distinction that **S-19**
  turns on.
- No result contradicted any statement in Parts I–III. **S-23** narrows the
  origin recorded in Finding S-4 from "the first echo request" to "the first
  echo request that is served"; the earlier measurement is unaffected, because
  the first request was served in that experiment.
- Host-specific figures — interface MTUs, the 9216-byte outbound datagram limit,
  the effective-length decision knees, and the socket handling of out-of-range DSCP — are
  labelled as such wherever they appear and are not protocol constants.
- Nothing in this part rests on anything but observed request bytes, reply bytes,
  reply presence, IP header markings, timings, and whether the server was still
  serving afterwards.
