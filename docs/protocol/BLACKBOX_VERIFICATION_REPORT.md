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

| Case | Result |
|------|--------|
| Correct token, original endpoint | Served |
| Correct token, different source port | **Dropped, no reply; session intact** |
| Correct token, other address family's listener on the same port | Dropped |
| Unknown token | Dropped |
| Zero token | Dropped |
| Token of a closed session | Dropped |
| **Close** bearing a valid token from a foreign source port | **Dropped; session not closed** |

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
answers the first echo request past its hard deadline with a complete echo reply
carrying flags `0x06` (Reply|Close), and removes the session at that moment.

| Configured maximum duration | Close flag observed at |
|---|---|
| 1 s | 3.01 s after the first echo |
| 2 s | 4.06 s / 4.10 s after the first echo |

The deadline is the maximum duration plus a 2-second grace, measured from the
session's first echo request — not from open. Capture:
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
i.e. timeout + 5 s. The first request past the deadline is still answered and
the session is released while handling it; only the next request is dropped.

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
