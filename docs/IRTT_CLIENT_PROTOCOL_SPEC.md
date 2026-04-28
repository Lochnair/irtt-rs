# IRTT-Compatible Client Protocol Specification

**Version:** 1.1-verified  
**Date:** 2026-04-28  
**Verified against:** irtt 0.9.1, protocol version 1, macOS Darwin 25.3.0 arm64

---

## 1. Status

This document describes the externally observable behavior required for an
independent client implementation compatible with existing IRTT (Isochronous
Round-Trip Tester) servers.

This document does **not** describe upstream source structure, internal
algorithms, or implementation organization. It is intended for a clean-room
implementation that has never seen the upstream source code.

The key words "MUST", "MUST NOT", "REQUIRED", "SHOULD", "SHOULD NOT",
"RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted
as interoperability requirements, consistent with RFC 2119.

---

## 2. Scope

- **Client-only implementation.** Server implementation is out of scope unless
  server behavior is needed to understand client interoperability.
- **Compatible with existing IRTT servers** running protocol version 1.
- This specification describes protocol and measurement behavior; CLI design
  is not prescribed.
- Server implementation is out of scope.

---

## 3. Terminology

| Term | Definition |
|------|-----------|
| **Client** | The endpoint that initiates a session, sends test packets, and receives replies. |
| **Server** | The endpoint that accepts sessions and echoes test packets back to the client. |
| **Session** | A stateful association between a client and server, identified by a connection token. |
| **Open / Negotiation phase** | The initial handshake in which the client proposes test parameters and the server responds with (possibly restricted) parameters and a connection token. |
| **Test packet** | A UDP datagram sent by the client during the active test phase. Sometimes called an "echo request." |
| **Reply packet** | A UDP datagram sent by the server in response to a test packet. Sometimes called an "echo reply." |
| **Close / Finalization phase** | The exchange in which the client notifies the server that the session is ending. |
| **Connection token** | A 64-bit value assigned by the server during negotiation, used to identify the session in all subsequent packets. |
| **Round-trip time (RTT)** | The time between sending a test packet and receiving its reply, optionally minus server processing time. |
| **Send interval** | The fixed time period between successive test packet transmissions. |
| **Sequence number** | A 32-bit unsigned integer, starting at 0, assigned to each test packet in order of transmission. |
| **Timeout** | The duration the client waits for a server reply during the open phase before retransmitting. |
| **Packet loss** | A test packet for which no reply was received. |
| **Duplicate** | A reply received with a sequence number for which a reply was already recorded. |
| **Late packet** | A reply received with a sequence number lower than the most recently received sequence number (indicates out-of-order delivery). |
| **IPDV (Instantaneous Packet Delay Variation)** | The difference in delay between two successive successfully returned replies. Commonly called "jitter." |
| **Server processing delay** | The time the server spent between receiving a request and sending its reply, measured by server timestamps. |
| **Received count** | A running count of all packets the server has received for this session. |
| **Received window** | A 64-bit bitmap indicating the receipt status of the most recent 64 packets at the server. |
| **Wall clock** | System real-time clock (nanoseconds since Unix epoch). Subject to NTP adjustments. |
| **Monotonic clock** | A clock that only moves forward and is not subject to system time adjustments. Used for duration calculations. |
| **HMAC** | Hash-based Message Authentication Code (MD5), used for packet authentication. |
| **DSCP** | Differentiated Services Code Point, set in the IP header TOS/Traffic Class field. |
| **Drain period** | The period after the last test packet is sent during which the client waits for outstanding replies. |

---

## 4. Conformance

A conforming client MUST implement:

1. Session lifecycle: open, test, close.
2. The binary wire format defined in this document.
3. Parameter negotiation.
4. Connection token handling.
5. Sequence number assignment and reply matching.
6. RTT measurement.
7. Packet loss detection.
8. Tolerance for UDP reordering and duplication.

A conforming client SHOULD implement:

1. HMAC authentication.
2. Server timestamp handling (one-way delay, IPDV).
3. Received stats (count and window) processing.
4. Configurable DSCP.

Implementations are not required to use any specific internal architecture,
data structure, function layout, naming convention, concurrency model, or
source organization. Only the externally observable behavior described in
this document is normative.

---

## 5. Transport Requirements

### 5.1 Transport Protocol

IRTT uses **UDP** exclusively. There is no TCP fallback.

### 5.2 Default Port

The default server port is **2112**. The default client local port is
ephemeral (OS-assigned, port 0).

### 5.3 IPv4 and IPv6

A conforming client MUST support IPv4. IPv6 support is RECOMMENDED. When the
user does not specify an IP version preference, the client SHOULD attempt
address resolution for both IPv4 and IPv6, and use whichever resolves
successfully. If both resolve, the selection behavior is
implementation-defined.

The client MUST use a single connected UDP socket for the entire session. The
client calls the equivalent of `connect()` on the socket, binding it to the
resolved server address before the open phase.

### 5.4 Socket Options

- **DSCP/TOS:** A client SHOULD support setting the DSCP value on outgoing
  packets. For IPv4, this is the TOS field. For IPv6, this is the Traffic
  Class field. The DSCP value is negotiated as a parameter; if the server
  disallows DSCP, the negotiated value will be 0.
- **TTL/Hop Limit:** A client MAY support setting the IP TTL (IPv4) or Hop
  Limit (IPv6).
- **Don't Fragment (DF):** A client MAY support setting the DF bit.
- **ECN:** ECN bit handling is OPTIONAL and experimental.

### 5.5 Connectionless Behavior

Although the client uses a connected UDP socket, the protocol is
connectionless at the network layer. The client MUST tolerate:

- Packet reordering.
- Packet duplication.
- Packet loss.
- Receiving unrelated UDP traffic on the socket (which MUST be discarded
  based on validation rules).

### 5.6 MTU Considerations

Common maximum unfragmented UDP payload sizes for a 1500-byte MTU:
- IPv4: 1472 bytes (1500 - 20 IPv4 header - 8 UDP header)
- IPv6: 1452 bytes (1500 - 40 IPv6 header - 8 UDP header)

The client does not need to perform path MTU discovery. Packet length is a
user-configured parameter.

---

## 6. Session Lifecycle

A client session progresses through the following externally observable states:

```
  Resolving ──► Opening ──► Active Test ──► Draining ──► Closing ──► Completed
                  │                                         │
                  ▼                                         ▼
               Failed                                   Completed
```

### 6.1 Resolving

- **Entry:** Client is given a server address (hostname or IP) and port.
- **Behavior:** The client resolves the address to a UDP endpoint. The
  default port (2112) MUST be appended if no port is specified.
- **Exit:** Successful resolution transitions to Opening. Failure transitions
  to Failed.

### 6.2 Opening / Negotiating

- **Entry:** The client has a resolved server address and a connected UDP socket.
- **Behavior:**
  1. The client constructs an open request packet (see Section 8.2) containing
     the proposed test parameters serialized in the payload.
  2. The client sends the open request and waits for a reply.
  3. If no reply is received within the current timeout, the client
     retransmits the open request with the next timeout value from its
     timeout list.
  4. The default timeout sequence is: 1s, 2s, 4s, 8s (total maximum wait:
     15s, maximum 4 packets). The minimum timeout value MUST be at least
     200ms.
  5. If all timeouts are exhausted with no reply, the session transitions to
     Failed.

- **Valid server responses:**
  - An open reply with a non-zero connection token: session parameters are
    extracted from the reply payload. The client stores the connection token.
    Transitions to Active Test (or Completed if the no-test flag was set).
  - An open reply with the close flag set, **when the client did NOT set
    Close in its request**: the server rejected the session (e.g., protocol
    version mismatch). Transition to Failed.
  - An open reply with the close flag set, **when the client DID set Close
    in its request** (no-test mode): the server acknowledged the open+close.
    The connection token is zero. Transition to Completed.
    [**Verified 2026-04-28** — see BLACKBOX_VERIFICATION_REPORT.md Finding D.]

- **Parameter negotiation:** The server MAY restrict parameters. The client
  MUST compare returned parameters against its requested parameters. A
  conforming client MUST, by default, reject the session if parameters were
  restricted (transition to Failed with an appropriate error). A "loose" mode
  MAY be provided that accepts server restrictions with a warning.

  The client MUST reject (as an error) the following server parameter changes:
  - Duration increased beyond what was requested.
  - Length increased beyond what was requested.
  - Interval reduced below a safety floor (see Section 19.9 for verification).

- **Protocol version check:** The client MUST verify that the server's
  protocol version matches exactly. Protocol version 1 is the current
  version. Mismatch MUST cause the session to fail.

### 6.3 Active Test

- **Entry:** Open reply received and parameters accepted.
- **Behavior:**
  1. The client sends test packets at fixed intervals for the configured
     duration.
  2. The client MUST be able to receive reply packets while continuing
     to send test packets (i.e., sending and receiving occur concurrently).
  3. Test packets are numbered with sequence numbers starting at 0.
  4. The test duration is **exclusive**: the client MUST NOT send a packet at
     or after the end time. The end time is calculated as start time + duration.
     The next scheduled send time is computed, and if it would be at or past
     the end time, sending stops.
  5. The expected number of packets for a given duration `d` and interval `i`
     is: `ceil(d / i)`. [**Verified 2026-04-28** by black-box testing with
     irtt 0.9.1. See BLACKBOX_VERIFICATION_REPORT.md Finding A.]

- **Exit:** All scheduled packets have been sent. Transition to Draining.

### 6.4 Draining Replies

- **Entry:** All test packets have been sent.
- **Behavior:** The client waits for outstanding replies. The wait duration
  is configurable:
  - Default: 3 times the maximum observed RTT, falling back to 4 seconds if
    no RTT has been measured.
  - Alternatives: a factor of mean RTT, or a fixed duration.
  - If all replies have already been received, no wait is needed.

- **Exit:** Wait period expires, or all replies received. Transition to Closing.

### 6.5 Closing

- **Entry:** Test and drain are complete.
- **Behavior:** The client sends a single close packet (see Section 8.4) to
  the server. The close packet includes the connection token and the close
  flag. The client does NOT wait for a close acknowledgement (this is a
  known limitation of protocol version 1).

- **Exit:** Close packet sent. Transition to Completed.

### 6.6 Completed

- **Entry:** Session has finished normally.
- **Behavior:** Results are computed and made available.

### 6.7 Failed

- **Entry:** An unrecoverable error occurred.
- **Behavior:** Partial results MAY be available if the failure occurred
  during or after the Active Test phase.

---

## 7. Packet Format Overview

All IRTT packets share a common header structure. The presence or absence of
optional fields is determined by context (packet type and negotiated
parameters).

| Packet Type | Direction | When Sent | Required | Purpose |
|------------|-----------|-----------|----------|---------|
| Open Request | Client → Server | Opening phase | Yes | Propose session parameters |
| Open Reply | Server → Client | Opening phase | Yes | Return connection token and (possibly restricted) parameters |
| Echo Request (Test Packet) | Client → Server | Active test | Yes | Carry test payload with sequence number |
| Echo Reply | Server → Client | Active test | Yes | Return test payload with timestamps and stats |
| Close Request | Client → Server | Closing phase | Yes | Signal session end |
| Close Reply | Server → Client | Closing phase | No | Acknowledge close (server MAY send this) |

---

## 8. Binary Wire Format

### 8.1 General Header Structure

All multi-byte integer fields use **little-endian** byte order.

The packet format uses a flexible field layout. Fields are placed sequentially
in the packet buffer. A field is either present at its full capacity or absent
(zero length). The set of present fields is determined by the packet type and
negotiated parameters.

#### 8.1.1 Fixed Header Fields (always present)

| Offset | Size (bytes) | Field | Encoding | Description |
|--------|-------------|-------|----------|-------------|
| 0 | 3 | Magic | Raw bytes | Fixed value: `0x14 0xA7 0x5B` |
| 3 | 1 | Flags | Unsigned byte | Bit flags (see below) |

**Total minimum header: 4 bytes.**

#### 8.1.2 Flag Bits

| Bit | Mask | Name | Meaning |
|-----|------|------|---------|
| 0 | 0x01 | Open | Set on open request and open reply packets |
| 1 | 0x02 | Reply | Set on all packets from server to client; clear on all packets from client to server |
| 2 | 0x04 | Close | Set on close request and close reply packets |
| 3 | 0x08 | HMAC | Set if an HMAC field is present in the packet |

Bits 4-7 are reserved and MUST be zero. A client MUST reject packets where
reserved flag bits are set.

#### 8.1.3 Optional Fields

After the fixed header, optional fields appear **in the following order** when
present. A field is either present at full size or entirely absent. There are
no length-prefix or presence-indicator bytes; the receiver knows which fields
are present based on the packet type and negotiated session parameters.

**Verification note:** The field ordering below is critical for
interoperability. [**Verified 2026-04-28** by packet capture with multiple
field combinations — see BLACKBOX_VERIFICATION_REPORT.md Section 19.11.]

| Order | Field | Size (bytes) | Encoding | When Present |
|-------|-------|-------------|----------|-------------|
| 1 | HMAC | 16 | MD5 HMAC | When HMAC flag (0x08) is set |
| 2 | Connection Token | 8 | Little-endian uint64 | All packets except initial open request sent from client before token is known. Present in open reply and all subsequent packets. |
| 3 | Sequence Number | 4 | Little-endian uint32 | Echo request and echo reply packets |
| 4 | Received Count | 4 | Little-endian uint32 | Echo reply, when "count" stats negotiated |
| 5 | Received Window | 8 | Little-endian uint64 | Echo reply, when "window" stats negotiated |
| 6 | Receive Wall Timestamp | 8 | Little-endian int64 (ns since Unix epoch) | Echo reply, when receive timestamps negotiated with wall clock |
| 7 | Receive Mono Timestamp | 8 | Little-endian int64 (ns, duration) | Echo reply, when receive timestamps negotiated with monotonic clock |
| 8 | Midpoint Wall Timestamp | 8 | Little-endian int64 (ns since Unix epoch) | Echo reply, when midpoint timestamp negotiated with wall clock |
| 9 | Midpoint Mono Timestamp | 8 | Little-endian int64 (ns, duration) | Echo reply, when midpoint timestamp negotiated with monotonic clock |
| 10 | Send Wall Timestamp | 8 | Little-endian int64 (ns since Unix epoch) | Echo reply, when send timestamps negotiated with wall clock |
| 11 | Send Mono Timestamp | 8 | Little-endian int64 (ns, duration) | Echo reply, when send timestamps negotiated with monotonic clock |

**Important:** Midpoint timestamps are **mutually exclusive** with
receive/send timestamps. If a midpoint timestamp is present, receive and send
timestamp fields MUST NOT be present, and vice versa. A packet containing
both midpoint and receive/send timestamps is malformed.

When both receive and send timestamps are present, they MUST use the same
clock mode (both wall, both monotonic, or both wall+monotonic).

#### 8.1.4 Payload

Everything after the last present header field is **payload**. The total
packet length is the negotiated packet length. If the negotiated length is
larger than the header, the remaining bytes are payload. If the negotiated
length is 0 or unspecified, the packet length equals the header length (no
payload).

The client fills the payload with zeros, random data, or a repeating
pattern, as configured. The server fills reply payloads according to its
configuration and the negotiated server fill parameter.

### 8.2 Open Request Packet

**Direction:** Client → Server  
**Flags:** Open (0x01) set. Reply (0x02) clear. HMAC (0x08) set if HMAC key
configured.

**Header fields present:**
- Magic (3 bytes)
- Flags (1 byte)
- HMAC (16 bytes) — if HMAC configured

The connection token field is NOT present in the open request (the token has
not yet been assigned). [**Verified 2026-04-28** by packet capture — see
BLACKBOX_VERIFICATION_REPORT.md Section 19.1.]

**Payload:** Serialized parameters (see Section 8.6).

If the client wants to open and immediately close (no-test mode), both the
Open (0x01) and Close (0x04) flags are set.

### 8.3 Open Reply Packet

**Direction:** Server → Client  
**Flags:** Open (0x01) set. Reply (0x02) set. Close (0x04) set if the server
is rejecting the connection. HMAC (0x08) set if applicable.

**Header fields present:**
- Magic (3 bytes)
- Flags (1 byte)
- HMAC (16 bytes) — if applicable
- Connection Token (8 bytes)

**Payload:** Serialized parameters (see Section 8.6), reflecting any server
restrictions.

**Validation:**
- If the Close flag is NOT set, the connection token MUST be non-zero. A
  zero token without the close flag is an error.
- If the Close flag IS set **and the client did NOT set Close in its open
  request**, the server is rejecting the session (transition to Failed).
- If the Close flag IS set **and the client DID set Close in its open
  request** (no-test mode), the server acknowledged the open+close. The
  connection token is zero. Transition to Completed.
  [**Verified 2026-04-28** — see BLACKBOX_VERIFICATION_REPORT.md Finding D.]

### 8.4 Echo Request Packet (Test Packet)

**Direction:** Client → Server  
**Flags:** Reply (0x02) clear. HMAC (0x08) if applicable. Open (0x01) and
Close (0x04) clear.

**Header fields present:**
- Magic (3 bytes)
- Flags (1 byte)
- HMAC (16 bytes) — if applicable
- Connection Token (8 bytes)
- Sequence Number (4 bytes)
- Received Count (4 bytes, zeroed) — if "count" stats negotiated
- Received Window (8 bytes, zeroed) — if "window" stats negotiated
- Timestamp fields (zeroed) — according to negotiated stamp_at and clock

The echo request includes zeroed placeholder fields for received stats and
timestamps so that the total packet reaches the negotiated length.

**Payload:** Filled according to client configuration (zeros, random, or
pattern).

**Total packet length:** The negotiated `length` parameter. If the header
(including zeroed optional fields) is larger than the negotiated length, the
actual packet length is the header length. The final negotiated length
reflects this minimum.

### 8.5 Echo Reply Packet

**Direction:** Server → Client  
**Flags:** Reply (0x02) set. HMAC (0x08) if applicable. Close (0x04) MAY be
set (see Section 19.3 — not yet verified by black-box testing).

**Header fields present:**
- Magic (3 bytes)
- Flags (1 byte)
- HMAC (16 bytes) — if applicable
- Connection Token (8 bytes)
- Sequence Number (4 bytes)
- Received Count (4 bytes) — if negotiated, contains server's running count
- Received Window (8 bytes) — if negotiated, contains 64-bit received bitmap
- Timestamp fields — according to negotiated stamp_at and clock settings

**Payload:** Server-filled according to negotiation.

### 8.5.1 Close Request Packet

**Direction:** Client → Server  
**Flags:** Close (0x04) set. Reply (0x02) clear. HMAC (0x08) if applicable.

**Header fields present:**
- Magic (3 bytes)
- Flags (1 byte)
- HMAC (16 bytes) — if applicable
- Connection Token (8 bytes)

No sequence number, no payload.

### 8.6 Parameter Serialization Format

Parameters are serialized as a sequence of tag-value pairs in the packet
payload during the open request and open reply.

Each parameter is encoded as:
1. **Tag:** Unsigned varint (LEB128 encoding).
2. **Value:** Signed varint (zigzag + LEB128 encoding), EXCEPT for
   string-typed values.

For string-typed values, the value is:
1. Length as unsigned varint.
2. Raw UTF-8 bytes.

**Varint encoding:** This uses the same variable-length integer encoding as
Google Protocol Buffers:
- **Unsigned varint (uvarint):** Standard unsigned LEB128. Each byte uses
  the high bit as a continuation flag. The remaining 7 bits contribute to
  the value, least significant group first.
- **Signed varint (varint):** Uses zigzag encoding: `(v << 1) ^ (v >> 63)`
  for encoding, decoded as `(uv >> 1) ^ -(uv & 1)`. This maps signed
  integers to unsigned integers so that small-magnitude values (positive and
  negative) have compact encodings.

See Section 19.4 for cross-language verification notes.

**Parameter tags:**

| Tag | Name | Value Type | Unit / Encoding | Description |
|-----|------|-----------|-----------------|-------------|
| 1 | ProtocolVersion | Signed varint | Integer | Protocol version (currently 1) |
| 2 | Duration | Signed varint | Nanoseconds | Test duration |
| 3 | Interval | Signed varint | Nanoseconds | Send interval |
| 4 | Length | Signed varint | Bytes | Packet length |
| 5 | ReceivedStats | Signed varint | Enum (see below) | Server received packet statistics mode |
| 6 | StampAt | Signed varint | Enum (see below) | Server timestamp mode |
| 7 | Clock | Signed varint | Enum (see below) | Clock selection for timestamps |
| 8 | DSCP | Signed varint | Integer (TOS byte value) | IP TOS/Traffic Class byte value (see Section 10.8) |
| 9 | ServerFill | String (length-prefixed) | UTF-8, max 32 bytes | Server payload fill mode |

Parameters with a zero value MAY be omitted from serialization (they will
default to zero on the receiving end).

Unknown parameter tags MUST be silently ignored by the receiver.

The maximum serialized parameter buffer size is believed to be 128 bytes
(see Section 19.10 for verification).

**ReceivedStats enum values:**

| Value | Name | Meaning |
|-------|------|---------|
| 0 | None | No received packet statistics |
| 1 | Count | Include received packet count |
| 2 | Window | Include 64-bit received window |
| 3 | Both | Include both count and window |

**StampAt enum values:**

| Value | Name | Meaning |
|-------|------|---------|
| 0 | None | No server timestamps |
| 1 | Send | Timestamp at server send |
| 2 | Receive | Timestamp at server receive |
| 3 | Both | Timestamps at both server receive and send |
| 4 | Midpoint | Midpoint timestamp (average of receive and send) |

**Clock enum values:**

| Value | Name | Meaning |
|-------|------|---------|
| 1 | Wall | Wall clock only |
| 2 | Monotonic | Monotonic clock only |
| 3 | Both | Both wall and monotonic clocks |

---

## 9. Versioning and Compatibility

### 9.1 Protocol Version

The protocol version is negotiated during the open phase. The current (and
only) protocol version is **1**.

A conforming client MUST set `ProtocolVersion` to 1 in its open request.

A conforming client MUST verify that the server's returned protocol version
matches exactly. If there is a mismatch, the client MUST abort the session.
The server will also set the Close flag if there is a version mismatch.

### 9.2 Unknown Fields and Extensions

Unknown parameter tags in the negotiation payload MUST be silently ignored.

Reserved flag bits (4-7) MUST NOT be set. Packets with unknown flag bits set
MUST be rejected.

### 9.3 Server Versions

This specification targets interoperability with servers implementing
protocol version 1, as found in IRTT versions 0.9.0 and later. Earlier
development versions (0.1.x) used an incompatible protocol and are not
supported.

---

## 10. Client Configuration Parameters

The following configuration parameters affect protocol behavior:

### 10.1 Server Address

- **Effect:** Determines the destination for all packets.
- **Default port:** 2112.
- **Valid values:** Hostname, IPv4 address, IPv6 address (in brackets),
  optionally with port.

### 10.2 Duration

- **Effect:** How long the client sends test packets.
- **Default:** 60 seconds (1 minute).
- **Valid values:** Positive duration.
- **Protocol:** Sent as nanoseconds in the Duration parameter. The server
  MAY reduce this value.

### 10.3 Interval

- **Effect:** Time between successive test packets.
- **Default:** 1 second.
- **Valid values:** Positive duration.
- **Protocol:** Sent as nanoseconds in the Interval parameter. The server
  MAY increase this value (enforce minimum interval). The server MAY also
  decrease it to stay within its timeout constraints.

### 10.4 Length

- **Effect:** Total UDP payload length of each test packet.
- **Default:** 0, which means the packet is the minimum size needed for the
  header fields.
- **Valid values:** 0 or positive integer. The actual minimum is determined
  by the header fields present.
- **Protocol:** Sent in the Length parameter. The server MAY reduce this.

### 10.5 Received Stats

- **Effect:** What statistics the server includes about received packets.
- **Default:** Both (count + window).
- **Valid values:** None (0), Count (1), Window (2), Both (3).

### 10.6 Timestamp Mode (StampAt)

- **Effect:** When the server records timestamps.
- **Default:** Both (send + receive).
- **Valid values:** None (0), Send (1), Receive (2), Both (3), Midpoint (4).
- **Note:** The server MAY restrict timestamps (e.g., allow only single or
  no timestamps).

### 10.7 Clock Selection

- **Effect:** Which clock(s) are used for server timestamps.
- **Default:** Both (wall + monotonic).
- **Valid values:** Wall (1), Monotonic (2), Both (3).

### 10.8 DSCP

- **Effect:** Sets the TOS/Traffic Class byte in the IP header for both
  client and server packets during the active test phase.
- **Default:** 0 (best effort).
- **Valid values:** 0-255 (full TOS byte value). The DSCP field occupies
  the upper 6 bits; the lower 2 bits (ECN) are typically zero.
  Common values: 0 (best effort), 0xb8 (EF), 0xa0 (CS5), 0x20 (CS1).
  [**Verified 2026-04-28** — see BLACKBOX_VERIFICATION_REPORT.md Finding B.]
- **Protocol:** The server echoes packets with the negotiated DSCP value.
  The server MAY disallow DSCP and reset it to 0.
- **Observed behavior:** DSCP/TOS is applied only to echo request and echo
  reply packets. Open and close packets use TOS=0 regardless of the
  negotiated DSCP value. [**Verified 2026-04-28** — Finding C.]

### 10.9 Open Timeouts

- **Effect:** Controls retransmission during the open phase.
- **Default:** 1s, 2s, 4s, 8s.
- **Valid values:** List of positive durations, each >= 200ms.

### 10.10 Wait (Drain Period)

- **Effect:** How long to wait after the last send for outstanding replies.
- **Default:** 3 times the maximum observed RTT, falling back to 4 seconds
  if no RTT has been measured.
- **Variants:**
  - A factor of maximum observed RTT, with a fallback duration if no RTT
    is available.
  - A factor of mean observed RTT, with a fallback duration if no RTT is
    available.
  - A fixed duration.

### 10.11 HMAC Key

- **Effect:** When set, an MD5-based HMAC is computed over each packet and
  included in the HMAC field. The server MUST also have the same key.
- **Default:** None (no HMAC).
- **Protocol:** When an HMAC key is configured, the HMAC flag (0x08) is set
  in every packet.

### 10.12 Server Fill

- **Effect:** Requests the server to fill reply payloads with specific data.
- **Default:** Not specified (server uses its default fill).
- **Valid values:** String, max 32 characters. Common values: "none", "rand",
  "pattern:XXXX" (hex pattern).
- **Protocol:** The server MAY restrict this to allowed fill types.

### 10.13 No-Test Mode

- **Effect:** Opens a connection and immediately closes it without running a
  test. Useful for parameter validation.
- **Protocol:** Both the Open and Close flags are set in the open request.

### 10.14 Loose Mode

- **Effect:** When enabled, the client accepts server-restricted parameters
  with a warning instead of aborting.
- **Default:** Disabled (strict mode).

---

## 11. Timing Semantics

### 11.1 Timestamp Capture Points

- **Client send timestamp:** SHOULD be captured as close as possible to the
  send system call. For best RTT accuracy, the timestamp should be taken
  immediately before the send call, so that the measured RTT includes any
  local send/enqueue overhead.
- **Client receive timestamp:** SHOULD be captured as close as possible to
  the receive system call return, before any packet processing.
- **Server timestamps:** Determined by the negotiated StampAt parameter:
  - Receive: captured when the server receives the packet.
  - Send: captured just before the server sends the reply.
  - Midpoint: the average of receive and send times.

### 11.2 Clock Types

A conforming client MUST track both wall clock and monotonic clock times
for client-side timestamps. The monotonic clock SHOULD be used for all
duration calculations (RTT, IPDV) because it is immune to system time
adjustments. The wall clock is needed for one-way delay calculations.

### 11.3 Send Scheduling

The client sends packets on a **fixed isochronous schedule**:

1. Record the start time.
2. The ideal send time for packet N is: `start + N * interval`.
3. The client SHOULD send each packet as close to its ideal send time as
   the system timer allows.
4. The client SHOULD compensate for cumulative timer drift so that packets
   remain aligned to integer multiples of the interval from the start time.
   The specific drift-compensation strategy is implementation-defined.
5. Before each send, the client checks whether the next scheduled time
   would be at or past `start + duration`. If so, sending stops.

The observable effect is that packets are sent at approximately
`start + 0`, `start + interval`, `start + 2*interval`, etc., with the
last packet sent strictly before `start + duration`.

### 11.4 Duration and Count Interaction

- The expected packet count is `ceil(duration / interval)`.
  [**Verified 2026-04-28** — see BLACKBOX_VERIFICATION_REPORT.md Finding A.]
- The duration is **exclusive**: the last packet is sent at approximately
  `start + (ceil(duration / interval) - 1) * interval`, which is strictly
  before `start + duration`.
- Example: duration=1s, interval=200ms → packets at 0ms, 200ms, 400ms,
  600ms, 800ms = 5 packets = ceil(1000/200).
- Example: duration=1s, interval=1s → packet at 0ms only = 1 packet =
  ceil(1000/1000). The packet at 1000ms is NOT sent (exclusive end).

### 11.5 Timer Compensation

The client SHOULD use timer error compensation to improve send accuracy.
The specific compensation algorithm is implementation-defined. Any approach
that produces accurate isochronous sending is acceptable.

### 11.6 Drain Period

After the last packet is sent, if not all replies have been received, the
client MUST wait for outstanding replies for the configured wait duration
before closing the session.

---

## 12. Measurement Semantics

### 12.1 Round-Trip Time (RTT)

RTT MUST be calculated using client-side monotonic clock values:

```
RTT = client_receive_mono - client_send_mono
```

If both server send and receive timestamps are available (monotonic
preferred, wall clock as fallback), the server processing time SHOULD be
subtracted:

```
server_processing_time = server_send - server_receive
RTT = (client_receive_mono - client_send_mono) - server_processing_time
```

This subtraction SHOULD only be applied when both server timestamps are
available. See Section 19.12 for edge-case behavior when server processing
time appears to exceed the raw round-trip time.

### 12.2 One-Way Delay

One-way delay requires wall clock timestamps and externally synchronized
clocks (e.g., NTP, PTP). Values are only meaningful when clocks are
synchronized.

```
send_delay = server_best_receive_wall - client_send_wall
receive_delay = client_receive_wall - server_best_send_wall
```

Where "best receive" prefers the actual receive timestamp, falling back to
the send timestamp if receive is not available (and vice versa for "best
send"). For midpoint timestamps, both best_receive and best_send are the
midpoint value.

### 12.3 Packet Loss

```
packet_loss_percent = 100 * (packets_sent - unique_replies_received) / packets_sent
```

If the received count is available from the server:

```
upstream_loss_percent = 100 * (packets_sent - server_packets_received) / packets_sent
downstream_loss_percent = 100 * (server_packets_received - unique_replies_received) / server_packets_received
```

### 12.4 Duplicate Packets

A duplicate is a reply with a sequence number for which a reply has already
been recorded. Duplicates MUST be counted but MUST NOT update the original
round-trip data for that sequence number.

```
duplicate_percent = 100 * duplicates / total_packets_received
```

### 12.5 Late Packets (Out-of-Order)

A late packet is one whose sequence number is lower than the most recently
received sequence number.

```
late_packets_percent = 100 * late_packets / total_packets_received
```

### 12.6 IPDV (Jitter)

IPDV is the difference in delay between two successive successfully received
replies. It is only calculable for sequence number N when both packet N and
packet N-1 have received replies.

```
round_trip_ipdv[N] = RTT[N] - RTT[N-1]
send_ipdv[N] = send_delay[N] - send_delay[N-1]         (if timestamps available)
receive_ipdv[N] = receive_delay[N] - receive_delay[N-1] (if timestamps available)
```

For send and receive IPDV, monotonic clock differences are RECOMMENDED
over wall clock differences when both are available, as the monotonic clock
is not subject to NTP adjustments. Note that IPDV does NOT require
synchronized clocks — only consistent clocks within each endpoint.

### 12.7 Server Processing Time

When both server send and receive timestamps are available:

```
server_processing_time = server_send - server_receive
```

Prefer monotonic clock values if available; fall back to wall clock.

### 12.8 Per-Packet Upstream/Downstream Loss (Received Window)

When the 64-bit received window is present in a reply:
- Bit 0 (LSB) represents the current packet (the one being replied to).
- Bit 1 represents the packet with sequence number one less than current, etc.
- A set bit means the server received that packet; a clear bit means it
  did not.

The window may not be valid for all replies. Bit 0 (LSB) is always set
for valid windows, since it represents the current packet which was
received. [**Verified 2026-04-28** — see Section 19.7.] Implementations
SHOULD treat a window value of 0 as potentially invalid and avoid using
it for loss classification.

The client processes the received window to classify previous packets:
- If a packet was not received by the client but the window says the server
  received it → lost downstream.
- If a packet was not received by the client and the window says the server
  did not receive it → lost upstream.
- If the window cannot confirm (e.g., more than 64 packets ago, or window
  was invalid) → generic loss.

The window has a maximum lookback of 63 packets (bits 1-63).

### 12.9 Statistics

For RTT, send delay, receive delay, and IPDV, a conforming client SHOULD
calculate:
- Minimum, Maximum, Mean, Median, Standard Deviation, Variance.
- Total count (N).

Median requires storing all values. The method of computing running
statistics is implementation-defined.

---

## 13. Validation Rules

### 13.1 Magic Bytes

Every received packet MUST begin with the magic bytes `0x14 0xA7 0x5B`.
Packets with incorrect magic MUST be discarded.

### 13.2 Flag Validation

Reserved flag bits (4-7) MUST be zero. Packets with invalid flag bits MUST
be discarded.

### 13.3 Reply Flag

All packets received by the client MUST have the Reply flag (0x02) set.
Packets without the Reply flag MUST be discarded.

### 13.4 HMAC Validation

If an HMAC key is configured:
- Received packets MUST have the HMAC flag set.
- The HMAC MUST be validated:
  1. Extract the HMAC value from the packet.
  2. Zero the HMAC field in the packet buffer.
  3. Compute HMAC-MD5 over the entire packet buffer.
  4. Compare the computed HMAC with the extracted value using a
     constant-time comparison.
- Packets with missing or invalid HMAC MUST be discarded.

If no HMAC key is configured:
- Received packets MUST NOT have the HMAC flag set. Packets with an
  unexpected HMAC MUST be discarded.

### 13.5 Sequence Number Validation

Echo reply sequence numbers MUST correspond to a previously sent packet.
Replies with sequence numbers outside the range [0, packets_sent) MUST be
rejected.

### 13.6 Timestamp Consistency

If both receive and send timestamps are present in a reply, they MUST use
the same clock mode (both wall, both monotonic, or both sets). Inconsistent
clock modes MUST cause the packet to be rejected.

Midpoint timestamps MUST be exclusive — if a midpoint timestamp is present,
no receive or send timestamp fields may also be present.

### 13.7 Close Flag in Echo Reply

If the server sets the Close flag in an echo reply, the client SHOULD
process the reply normally (record measurements) and then close the
connection. This is defensive handling for the possibility that the
server can forcibly end a session mid-test.

**Note:** This behavior has not been directly verified by black-box
testing (see Section 19.3). Implementations SHOULD handle it defensively.

---

## 14. Error Handling

| Error Category | Client Behavior |
|---------------|----------------|
| Name resolution failure | MUST abort. |
| Socket creation / bind failure | MUST abort. |
| Server unreachable (all open timeouts exhausted) | MUST abort. No partial results. |
| Protocol version mismatch | MUST abort. |
| Server restriction rejected (strict mode) | MUST abort. |
| Server restriction accepted (loose mode) | SHOULD warn, continue. |
| DSCP not supported by OS | MUST abort if DSCP was requested non-zero. |
| DF bit not supported by OS | MUST abort if DF was requested non-default. |
| Open reply with Close flag (not no-test) | MUST abort (server rejected session). |
| Zero connection token (without close flag) | MUST abort. |
| Send error during test | SHOULD abort, report partial results. |
| Receive error during test | SHOULD abort, report partial results. |
| Individual malformed packet during test | SHOULD abort. |
| Short reply (shorter than negotiated length) | SHOULD abort. |
| Stamp-at mismatch in reply | SHOULD abort. |
| Clock mismatch in reply | SHOULD abort. |
| Unexpected sequence number | SHOULD abort. |
| Context cancellation (user interrupt) | SHOULD stop gracefully, close connection, report partial results. |
| Insufficient resources for test parameters | MUST abort. |

---

## 15. Result Model

After a test, the client implementation SHOULD expose the following
information:

### 15.1 Per-Packet Data (Round Trips)

For each sent packet (indexed by sequence number):
- Sequence number.
- Lost status: not lost, lost (generic), lost upstream, lost downstream.
- Client send timestamp (wall + monotonic).
- Client receive timestamp (wall + monotonic) — empty if lost.
- Server receive timestamp (wall and/or monotonic) — as negotiated.
- Server send timestamp (wall and/or monotonic) — as negotiated.
- Computed RTT.
- Computed send delay.
- Computed receive delay.
- Computed IPDV (round-trip, send, receive) relative to previous packet.

### 15.2 Aggregate Statistics

- Test start time (wall + monotonic).
- Actual test duration.
- Packets sent.
- Packets received (unique).
- Packet loss percentage.
- Upstream loss percentage (if server count available).
- Downstream loss percentage (if server count available).
- Duplicate count and percentage.
- Late packet count and percentage.
- RTT statistics: min, max, mean, median, stddev, variance.
- Send delay statistics (if timestamps available).
- Receive delay statistics (if timestamps available).
- IPDV statistics (round-trip, send, receive).
- Server processing time statistics.
- Bytes sent and received.
- Send and receive bitrate.
- Timer error statistics.
- Timer miss count and percentage.
- Wait duration actually used.

### 15.3 Configuration

- Local and remote addresses (resolved).
- Negotiated parameters.
- Originally supplied parameters.
- IP version used.

### 15.4 Errors

- Send error (if the send operation failed).
- Receive error (if the receive operation failed).

---

## 16. Interoperability Requirements

1. A conforming client MUST be able to open a session with a compatible IRTT
   server running protocol version 1.

2. A conforming client MUST send open requests with correctly serialized
   parameters using the varint-encoded tag-value format.

3. A conforming client MUST correctly parse server open replies, including
   restricted parameters.

4. A conforming client MUST include the connection token in all post-open
   packets.

5. A conforming client MUST send test packets in the negotiated format, with
   the correct field layout and packet length.

6. A conforming client MUST correctly match replies to sent packets using
   sequence numbers.

7. A conforming client MUST tolerate UDP reordering, duplication, and loss.

8. A conforming client MUST validate magic bytes, flags, and HMAC on all
   received packets.

9. A conforming client SHOULD produce RTT measurements consistent with
   upstream implementations within the precision of the system clocks.

10. A conforming client MUST send a close packet at the end of the session.

11. A conforming client MUST correctly handle the HMAC calculation:
    compute HMAC-MD5 over the full packet with the HMAC field zeroed.

12. When computing an outgoing HMAC, the client MUST:
    a. Set the HMAC flag bit.
    b. Zero the HMAC field.
    c. Compute HMAC-MD5 over the entire packet buffer.
    d. Write the resulting MAC into the HMAC field.

---

## 17. Black-Box Test Plan

### Test 1: Basic Connectivity

- **Setup:** Start an upstream IRTT server with default settings.
- **Client config:** Default parameters (1s interval, 1m duration), reduce
  to 5s duration for testing. Server address: localhost.
- **Expected:** Session opens, packets are exchanged, results include RTT
  measurements, session closes cleanly.
- **Pass criteria:** Non-zero packets_sent and packets_received. RTT values
  present. No errors.

### Test 2: Parameter Negotiation

- **Setup:** Start a server with `--max-length=100`.
- **Client config:** Request length 200.
- **Expected (strict mode):** Client aborts with server restriction error.
- **Expected (loose mode):** Client continues with length 100.
- **Pass criteria (strict):** Client exits with error indicating server
  reduced length.
- **Pass criteria (loose):** Successful test with negotiated length 100.

### Test 3: HMAC Authentication

- **Setup:** Start server with `--hmac=testkey`.
- **Client config:** `--hmac=testkey`.
- **Expected:** Session succeeds.
- **Pass criteria:** Successful test with no errors.
- **Variant:** Client uses wrong key. Expected: open timeout (server drops
  packets with bad HMAC silently).

### Test 4: Packet Capture Verification

- **Setup:** Start server on localhost. Capture with tcpdump on the loopback
  interface, port 2112.
- **Client config:** Short test (5s, 1s interval).
- **Expected:** Capture shows:
  - Open request: magic bytes 14 a7 5b, flags=0x01, followed by varint
    params in payload.
  - Open reply: magic bytes, flags=0x03 (open+reply), 8-byte token, params.
  - Echo requests: magic bytes, flags=0x00, 8-byte token, 4-byte sequence number.
  - Echo replies: magic bytes, flags=0x02 (reply), 8-byte token, 4-byte
    sequence number, optional stats and timestamps.
  - Close request: magic bytes, flags=0x04.
- **Pass criteria:** All packets have correct magic, flags, and field layout.

### Test 5: Sequence Number Handling

- **Setup:** Server with default settings.
- **Client config:** 10s duration, 200ms interval (expect ~51 packets).
- **Expected:** Reply sequence numbers are in range [0, packets_sent).
  Results show correct per-packet data.
- **Pass criteria:** All received replies have valid sequence numbers. No
  unexpected sequence number errors.

### Test 6: Timestamp Modes

- **Setup:** Server with default settings.
- **Variants:**
  - `--tstamp=none --clock=wall`
  - `--tstamp=send --clock=wall`
  - `--tstamp=receive --clock=monotonic`
  - `--tstamp=both --clock=both`
  - `--tstamp=midpoint --clock=both`
- **Expected:** Each variant produces the correct timestamp fields in replies.
  RTT is always available. One-way delay is only available with wall clock
  timestamps.
- **Pass criteria:** Results match expected availability of delay measurements.

### Test 7: Loss Detection

- **Setup:** Use a network impairment tool (e.g., `tc netem loss 10%`) or a
  server on a lossy link.
- **Client config:** 30s duration, 100ms interval.
- **Expected:** packet_loss_percent is non-zero. With `--stats=both`,
  upstream and downstream loss are differentiated.
- **Pass criteria:** Loss percentages are reported. With received window,
  per-packet loss direction is classified.

### Test 8: IPv6

- **Setup:** Server listening on IPv6 (::1).
- **Client config:** `-6 ::1`.
- **Expected:** Successful test over IPv6.
- **Pass criteria:** IP version in results is IPv6.

### Test 9: No-Test Mode

- **Setup:** Server with default settings.
- **Client config:** `-n localhost`.
- **Expected:** Connection opens, parameters are negotiated, connection
  closes immediately. No test packets sent.
- **Pass criteria:** No round-trip data. No errors.

### Test 10: Large Packet Size

- **Setup:** Server with default settings.
- **Client config:** `-l 1472 -d 5s localhost`.
- **Expected:** Successful test with 1472-byte packets.
- **Pass criteria:** Non-zero results.

### Test 11: Server Fill

- **Setup:** Server with `--allow-fills=rand`.
- **Client config:** `--sfill=rand -l 172 -d 5s localhost`.
- **Expected:** Successful test. Server fills reply payloads with random data.
- **Pass criteria:** Test completes without error.

### Test 12: DSCP

- **Setup:** Server with default settings (DSCP allowed).
- **Client config:** `--dscp=46 -d 5s localhost`.
- **Expected:** Packets are sent with EF DSCP marking. Capture verification
  shows TOS/TC field set.
- **Pass criteria:** Test completes. Packet capture confirms DSCP value.

---

## 18. Packet Test Vectors

### 18.1 Echo Request with HMAC

**Scenario:** Echo request with HMAC, received stats (both), timestamps
(both, both clocks), pattern fill `0xFF 0xFE 0xFD 0xFC`, 92-byte packet
(76-byte header + 16-byte payload).
[**Size corrected 2026-04-28** — originally described as 256 bytes, but
HMAC verification confirms the test vector is 92 bytes. See
BLACKBOX_VERIFICATION_REPORT.md Finding E.]

**Connection token:** `0x886bc9a722b33eea` (little-endian)  
**Sequence number:** `0x6fe2a1bb` (little-endian)  
**HMAC key:** `0x3c 0x68 0x1d 0x39 0x41 0x1d 0x72 0x43`

**Fields present (in order):**
1. Magic: 3 bytes
2. Flags: 1 byte (0x08 = HMAC)
3. HMAC: 16 bytes
4. Connection Token: 8 bytes
5. Sequence Number: 4 bytes
6. Received Count: 4 bytes (zeroed)
7. Received Window: 8 bytes (zeroed)
8. Receive Wall: 8 bytes (zeroed)
9. Receive Mono: 8 bytes (zeroed)
10. Send Wall: 8 bytes (zeroed)
11. Send Mono: 8 bytes (zeroed)

**Header total:** 3+1+16+8+4+4+8+8+8+8+8 = 76 bytes  
**Payload:** 92 - 76 = 16 bytes of repeating pattern

**Hex (92 bytes):**
```
14 a7 5b 08 e7 03 41 e7 d4 08 cf 69 41 f3 f4 78
5a 56 0c 4c ea 3e b3 22 a7 c9 6b 88 bb a1 e2 6f
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
00 00 00 00 00 00 00 00 00 00 00 00 ff fe fd fc
ff fe fd fc ff fe fd fc ff fe fd fc
```

**Field breakdown:**
- Bytes 0-2: Magic `14 a7 5b`
- Byte 3: Flags `08` (HMAC set)
- Bytes 4-19: HMAC `e7 03 41 e7 d4 08 cf 69 41 f3 f4 78 5a 56 0c 4c`
- Bytes 20-27: Connection Token `ea 3e b3 22 a7 c9 6b 88` (LE: 0x886bc9a722b33eea)
- Bytes 28-31: Sequence Number `bb a1 e2 6f` (LE: 0x6fe2a1bb)
- Bytes 32-35: Received Count (zeroed)
- Bytes 36-43: Received Window (zeroed)
- Bytes 44-51: Receive Wall Timestamp (zeroed)
- Bytes 52-59: Receive Mono Timestamp (zeroed)
- Bytes 60-67: Send Wall Timestamp (zeroed)
- Bytes 68-75: Send Mono Timestamp (zeroed)
- Bytes 76+: Payload (pattern fill)

**Client behavior:** This packet is sent to the server during the active test phase.

### 18.2 Echo Reply with HMAC and Full Timestamps

**Scenario:** Echo reply with HMAC, received stats (both), timestamps (both,
both clocks), pattern fill, 92-byte packet (76-byte header + 16-byte payload).
[**Size corrected 2026-04-28** — see Finding E.]

**Connection token:** `0xe666ceb6766fcbc3` (little-endian)  
**Sequence number:** `0x1d3b0706` (little-endian)  
**HMAC key:** `0xda 0xb3 0xe9 0x04 0xa6 0x87 0x92 0x49`  
**Received count:** `0xa3bc9f19` (little-endian)  
**Received window:** `0xd7e939e586f83b9b` (little-endian)  

**Timestamps (little-endian int64):**
- Receive wall: `0x525af13dee2a75a1`
- Receive mono: `0x2d5562223e4ac69a`
- Send wall: `0x589705d446293f69`
- Send mono: `0x461df12fdd2c5066`

**Fields present (in order):**
1. Magic: 3 bytes
2. Flags: 1 byte (0x08 = HMAC, plus 0x02 = Reply → but let's verify from the test vector)
3. HMAC: 16 bytes
4. Connection Token: 8 bytes
5. Sequence Number: 4 bytes
6. Received Count: 4 bytes
7. Received Window: 8 bytes
8. Receive Wall: 8 bytes
9. Receive Mono: 8 bytes
10. Send Wall: 8 bytes
11. Send Mono: 8 bytes

**Note on flags:** The flags byte in this test vector is `0x08` (only HMAC
set). This vector is provided for verifying field layout and HMAC
computation only. **In actual protocol operation, the Reply flag (0x02)
MUST also be set on all server-to-client packets**, making the expected
flags byte `0x0A` for a normal HMAC echo reply. Implementers MUST
set the Reply flag in actual protocol exchanges; the HMAC computation
itself does not depend on the Reply flag's value.

**Hex (first 92 bytes):**
```
14 a7 5b 08 d2 98 a3 4a 6a 13 41 02 68 b2 67 a8
d6 7e 28 25 c3 cb 6f 76 b6 ce 66 e6 06 07 3b 1d
19 9f bc a3 9b 3b f8 86 e5 39 e9 d7 a1 75 2a ee
3d f1 5a 52 9a c6 4a 3e 22 62 55 2d 69 3f 29 46
d4 05 97 58 66 50 2c dd 2f f1 1d 46 ff fe fd fc
ff fe fd fc ff fe fd fc ff fe fd fc
```

**Field breakdown:**
- Bytes 0-2: Magic `14 a7 5b`
- Byte 3: Flags `08` (HMAC; see note above about Reply flag)
- Bytes 4-19: HMAC `d2 98 a3 4a 6a 13 41 02 68 b2 67 a8 d6 7e 28 25`
- Bytes 20-27: Connection Token `c3 cb 6f 76 b6 ce 66 e6` (LE: 0xe666ceb6766fcbc3)
- Bytes 28-31: Sequence Number `06 07 3b 1d` (LE: 0x1d3b0706)
- Bytes 32-35: Received Count `19 9f bc a3` (LE: 0xa3bc9f19)
- Bytes 36-43: Received Window `9b 3b f8 86 e5 39 e9 d7` (LE: 0xd7e939e586f83b9b)
- Bytes 44-51: Receive Wall `a1 75 2a ee 3d f1 5a 52` (LE: 0x525af13dee2a75a1)
- Bytes 52-59: Receive Mono `9a c6 4a 3e 22 62 55 2d` (LE: 0x2d5562223e4ac69a)
- Bytes 60-67: Send Wall `69 3f 29 46 d4 05 97 58` (LE: 0x589705d446293f69)
- Bytes 68-75: Send Mono `66 50 2c dd 2f f1 1d 46` (LE: 0x461df12fdd2c5066)
- Bytes 76+: Payload (pattern fill)

**Expected client behavior:** Validate magic, validate HMAC, extract
connection token, extract sequence number, extract received stats, extract timestamps,
record round-trip data.

---

## 19. Open Questions / Verification Required

### 19.1 Open Request Field Layout — ✅ RESOLVED

**Status:** Verified 2026-04-28, irtt 0.9.1, macOS arm64.

**Result:** The open request does NOT include a connection token field. The
layout is: magic (3) + flags (1) + [HMAC (16) if applicable] + params.
Verified by packet capture (`captures/full-session.pcapng`,
`captures/hmac-session.pcapng`).

### 19.2 Minimum Packet Length Calculation — ✅ RESOLVED

**Status:** Verified 2026-04-28, irtt 0.9.1, macOS arm64.

**Result:** Minimum echo packet lengths verified for all stats/tstamp/clock
combinations. See BLACKBOX_VERIFICATION_REPORT.md Section 19.2 for the
complete table. The minimum is 16 bytes (no stats, no timestamps) and the
maximum default is 76 bytes (both+both+both+HMAC).

### 19.3 Server Close During Test — OPEN / Verification Required

**Status:** Not tested. Hard to trigger reliably with black-box testing.

**Question:** Can the server set the Close flag on an echo reply mid-test
(e.g., due to exceeding a duration limit)? If so, how should the client
respond?

**Compatibility recommendation:** The client SHOULD check for the Close
flag in echo replies and, if set, process the reply normally (record
measurements) and then terminate the session. This is defensive handling
for a scenario that has not been directly observed in black-box testing.

### 19.4 Varint Encoding Compatibility — ✅ RESOLVED

**Status:** Verified 2026-04-28, irtt 0.9.1, macOS arm64.

**Result:** Parameters decoded from captured packets match expected values
using standard protobuf-style zigzag + LEB128. All parameter tags and values
verified. See BLACKBOX_VERIFICATION_REPORT.md Section 19.4.

### 19.5 HMAC Computation Scope — ✅ RESOLVED

**Status:** Verified 2026-04-28, irtt 0.9.1, macOS arm64.

**Result:** HMAC-MD5 is computed over the entire packet buffer (including
payload) with the 16-byte HMAC field zeroed. Independently verified with
Python for all four packet types (open request, echo request, echo reply,
close request). The spec's test vectors (18.1 and 18.2) also verify
correctly at 92-byte packet size.

### 19.6 Send Timestamp Capture Timing — OPEN (not externally testable)

This is an implementation-internal detail that cannot be verified via
black-box testing. The client SHOULD capture timestamps as close to the
send/receive system calls as possible (see Section 11.1), but the exact
timing is an implementation choice.

### 19.7 Server Received Window Validity — ✅ RESOLVED

**Status:** Verified 2026-04-28, irtt 0.9.1, macOS arm64.

**Result:** Bit 0 (LSB) of the received window is always set for valid
windows (it represents the current packet which was received). Observed
window values: seq 0 → 0x01, seq 1 → 0x03, seq 2 → 0x07. A window
value of 0 can be used as the invalid sentinel. See
BLACKBOX_VERIFICATION_REPORT.md Section 19.7.

### 19.8 Open Reply Without HMAC When Client Expects HMAC — ✅ RESOLVED

**Status:** Verified 2026-04-28, irtt 0.9.1, macOS arm64.

**Result:** The server silently drops packets with missing or invalid HMAC.
The client times out with `[OpenTimeout] no reply from server`. Same
behavior for wrong key and no key. See BLACKBOX_VERIFICATION_REPORT.md
Section 19.8.

### 19.9 Minimum Restricted Interval Safety Floor — PARTIALLY RESOLVED

**Status:** Partially tested 2026-04-28.

**Result:** In strict mode, the client rejects ANY server parameter change
with a non-zero exit code. In loose mode, changes are accepted with a
warning. The exact safety floor for server-decreased intervals could not
be tested without a cooperative malicious server. See
BLACKBOX_VERIFICATION_REPORT.md Section 19.9.

### 19.10 Maximum Parameter Buffer Size — PARTIALLY RESOLVED

**Status:** Partially tested 2026-04-28.

**Result:** Largest observed parameter payload was 52 bytes (all params
including 24-character ServerFill). Upper bound estimate is ~82 bytes. The
128-byte limit appears safe. See BLACKBOX_VERIFICATION_REPORT.md
Section 19.10.

### 19.11 Field Ordering Verification — ✅ RESOLVED

**Status:** Verified 2026-04-28, irtt 0.9.1, macOS arm64.

**Result:** The field ordering in Section 8.1.3 is correct. Verified with
six different field combinations including no optional fields, various
stats/tstamp combinations, midpoint timestamps, and HMAC. See
BLACKBOX_VERIFICATION_REPORT.md Section 19.11.

### 19.12 RTT When Server Processing Time Exceeds Raw Round-Trip — OPEN / Verification Required

**Status:** Not tested. Hard to trigger on localhost.

**Question:** When server processing time (as reported by server
timestamps) exceeds the client-measured round-trip time — possible due to
clock granularity or drift — the subtraction would produce a negative RTT.
Should the client skip the subtraction, clamp to zero, or report the
negative value?

This edge case requires server processing time to exceed the raw
client-measured RTT, which is extremely unlikely under normal conditions.

---

## 20. Non-Goals

This specification does **not** require:

- Matching upstream source layout, file organization, or module structure.
- Matching upstream private names (functions, types, variables).
- Matching upstream internal algorithms where equivalent behavior can be
  achieved independently.
- Matching exact CLI text output.
- Implementing the server.
- Implementing every optional upstream feature in the first milestone.
- Implementing timer compensation with any specific algorithm (only accurate
  send timing is required).
- Implementing any specific concurrency model.
- Using any specific programming language features or libraries.
- Supporting IRTT protocol versions other than 1.
- Supporting the development-era (0.1.x) protocol.
