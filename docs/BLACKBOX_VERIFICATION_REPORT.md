# IRTT Black-Box Verification Report

**Date:** 2026-04-28  
**irtt version:** 0.9.1 (protocol version 1, json format version 1)  
**OS:** macOS Darwin 25.3.0 arm64 (Apple Silicon)  
**tshark version:** TShark (Wireshark) 4.6.4  
**Loopback interface:** lo0

---

## Environment Check

```
$ command -v irtt
/Users/lochnair/.local/bin/irtt

$ irtt version
irtt version: 0.9.1
protocol version: 1
json format version: 1
go version: go1.26.2 on darwin/arm64

$ command -v tshark
/opt/homebrew/bin/tshark

$ tshark --version
TShark (Wireshark) 4.6.4 (Git commit f7c4a74874d9).
```

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

**Verified independently with Python decoder.**

**Conclusion:** The varint encoding is standard protobuf-compatible. This
is a **normative requirement** for interoperability.

---

### 19.5 HMAC Computation Scope — RESOLVED

**Question:** Is the HMAC computed over the entire packet (including payload)
with the HMAC field zeroed?

**Test setup:** Captured HMAC session, extracted packet bytes, independently
computed HMAC-MD5 with Python.

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

### 19.3 Server Close During Test — NOT DIRECTLY TESTED

**Limitation:** Triggering a server-initiated close mid-test requires
specific server conditions that are difficult to set up reliably in a
black-box test. The spec's description (server MAY set Close flag on echo
reply) has not been verified externally. Reclassified as **open /
verification required** — the spec now recommends defensive handling
without asserting that this behavior occurs.

---

### 19.6 Send Timestamp Capture Timing — NOT TESTABLE

**Limitation:** The exact timing of client-side timestamp capture relative
to the send syscall is an implementation-internal detail that cannot be
verified through black-box testing. Remains an implementation choice.

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
(shown truncated)." Independent HMAC verification confirms that both test
vectors (18.1 and 18.2) were computed over 92-byte packets (76-byte header
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
