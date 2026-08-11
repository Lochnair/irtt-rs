# IRTT Server Behavioral Vectors

**Source:** Black-box observation of an upstream IRTT **0.9.1 release** server on
macOS Darwin 25.5.0 arm64, driven by a raw UDP harness.
**Date:** 2026-08-11
**Companion to:** `../IRTT_SERVER_PROTOCOL_SPEC.md`

These vectors are stated as **input → observed output**. They describe what a
server was seen to emit for a given request sequence. They do not describe how
any implementation produces those bytes.

Unless a row says otherwise, the server under test is the unmodified upstream
0.9.1 release. Two other builds appear only in Section 5.2.3, each named
explicitly: the 0.9.0 release, and a build of the upstream development tree six
upstream commits past the 0.9.1 tag. Results from those builds never stand in for
0.9.1.

Addresses in the accompanying captures are loopback only. Session tokens are
server-assigned random values and differ per run; where a token appears below it
is the value from that specific run and carries no meaning.

---

## 1. Received Window and Received Count

All vectors in this section were produced on a single session negotiated with
ReceivedStats = both (3), StampAt = none (0). Each row is one echo request and
the reply it produced. `count` is the 32-bit received count field; `window` is
the 64-bit received window field.

### 1.1 Sequential, no loss

| # | request seq | reply count | reply window |
|---|-------------|-------------|--------------|
| 1 | 0 | 1 | `0x0000000000000001` |
| 2 | 1 | 2 | `0x0000000000000003` |
| 3 | 2 | 3 | `0x0000000000000007` |
| 4 | 3 | 4 | `0x000000000000000f` |
| 5 | 4 | 5 | `0x000000000000001f` |
| 6 | 5 | 6 | `0x000000000000003f` |

### 1.2 One gap

| # | request seq | reply count | reply window | note |
|---|-------------|-------------|--------------|------|
| 1 | 0 | 1 | `0x0000000000000001` | |
| 2 | 1 | 2 | `0x0000000000000003` | |
| 3 | 3 | 3 | `0x000000000000000d` | bit 1 clear — sequence 2 missing |
| 4 | 4 | 4 | `0x000000000000001b` | bit 2 clear — sequence 2 missing |

### 1.3 Multiple gaps

| # | request seq | reply count | reply window |
|---|-------------|-------------|--------------|
| 1 | 0 | 1 | `0x0000000000000001` |
| 2 | 3 | 2 | `0x0000000000000009` |
| 3 | 7 | 3 | `0x0000000000000091` |
| 4 | 8 | 4 | `0x0000000000000123` |

### 1.4 Later gap fill — window collapses

| # | request seq | reply count | reply window | note |
|---|-------------|-------------|--------------|------|
| 1 | 0 | 1 | `0x0000000000000001` | |
| 2 | 1 | 2 | `0x0000000000000003` | |
| 3 | 5 | 3 | `0x0000000000000031` | gap over 2, 3, 4 |
| 4 | 3 | 4 | **`0x0000000000000001`** | filling the gap **discards** history |

A late arrival does not set the corresponding historical bit. The window is
reset to "only the current packet is known".

### 1.5 Duplicate sequence number

| # | request seq | reply count | reply window | note |
|---|-------------|-------------|--------------|------|
| 1 | 0 | 1 | `0x0000000000000001` | |
| 2 | 1 | 2 | `0x0000000000000003` | |
| 3 | 1 | 3 | `0x0000000000000003` | count advances, window unchanged |
| 4 | 2 | 4 | `0x0000000000000007` | |

### 1.6 Reordering

| # | request seq | reply count | reply window | note |
|---|-------------|-------------|--------------|------|
| 1 | 0 | 1 | `0x0000000000000001` | |
| 2 | 1 | 2 | `0x0000000000000003` | |
| 3 | 2 | 3 | `0x0000000000000007` | |
| 4 | 1 | 4 | **`0x0000000000000001`** | out-of-order arrival resets the window |
| 5 | 3 | 5 | `0x0000000000000005` | rebuilds from the reset state |

Note row 5: the window says sequence 1 was received (bit 2) and sequence 3 was
received (bit 0), but bit 1 is clear even though sequence 2 *was* received
earlier. History before the reset is unrecoverable.

### 1.7 Strictly descending sequence numbers

| # | request seq | reply count | reply window |
|---|-------------|-------------|--------------|
| 1 | 5 | 1 | `0x0000000000000001` |
| 2 | 4 | 2 | `0x0000000000000001` |
| 3 | 3 | 3 | `0x0000000000000001` |
| 4 | 2 | 4 | `0x0000000000000001` |
| 5 | 1 | 5 | `0x0000000000000001` |
| 6 | 0 | 6 | `0x0000000000000001` |

Every reply is answered; only the count carries information.

### 1.8 Gap at and beyond the representable window

| first seq | second seq | gap | second reply window |
|-----------|------------|-----|---------------------|
| 0 | 63 | 63 | `0x8000000000000001` — highest bit set |
| 0 | 64 | 64 | `0x0000000000000001` |
| 0 | 65 | 65 | `0x0000000000000001` |
| 0 | 1000 | 1000 | `0x0000000000000001` |

The window represents the current packet plus 63 predecessors. A gap of exactly
63 still records the older packet in the top bit; a gap of 64 or more discards
all history.

### 1.9 Non-zero starting sequence number

| # | request seq | reply count | reply window |
|---|-------------|-------------|--------------|
| 1 | 100 | 1 | `0x0000000000000001` |
| 2 | 101 | 2 | `0x0000000000000003` |
| 3 | 102 | 3 | `0x0000000000000007` |

The first request of a session always yields window `0x1`, whatever its
sequence number.

### 1.10 32-bit sequence wraparound

| # | request seq | reply count | reply window |
|---|-------------|-------------|--------------|
| 1 | 4294967293 | 1 | `0x0000000000000001` |
| 2 | 4294967294 | 2 | `0x0000000000000003` |
| 3 | 4294967295 | 3 | `0x0000000000000007` |
| 4 | 0 | 4 | `0x000000000000000f` |
| 5 | 1 | 5 | `0x000000000000001f` |

Wraparound is handled as ordinary modular arithmetic: crossing 2^32 − 1 → 0 is
treated as a step of one, not as a gap of 2^32 − 1 and not as reordering.

Starting a session at 4294967295 and following with 0 gives `0x1` then `0x3`.

### 1.11 Randomized cross-check

The window and count rules in `../IRTT_SERVER_PROTOCOL_SPEC.md` Section 10 were
checked against a live server over 12 sessions of 40 requests each — 480 replies
in total — with sequence numbers drawn from a mix of in-order steps, duplicates,
forward gaps of 2 to 80, and backward steps of 1 to 5, and with starting
sequence numbers of 0, 1, 7, 100 and 4294967294.

**Result: 480 replies checked, 0 mismatches** in either the window value or the
count value.

### 1.12 Count excludes dropped requests

Server configured with a minimum interval of 100 ms and a burst allowance of 5.
Twelve requests sent back to back, then three more after a 600 ms pause.

| requests sent | replies received | counts in replies |
|---------------|------------------|-------------------|
| 12 back to back | 5 | 1, 2, 3, 4, 5 |
| 3 after a pause | 3 | 6, 7, 8 |

Rate-limited requests produce no reply and do not advance the count.

---

## 2. Session Identity

Capture: `../captures/server-session-identity.pcapng`.

Session opened from source port A; token `88a76d2fd0cd5f32`.

| # | source | packet | observed |
|---|--------|--------|----------|
| 1 | A | open request | open reply, token assigned |
| 2 | A | echo seq 0 | reply, count 1, window `0x1` |
| 3 | **B** | echo seq 1, correct token | **no reply** |
| 4 | **B** | close, correct token | **no reply**, session **not** closed |
| 5 | A | echo seq 2 | reply, count 2, window `0x5` |
| 6 | **C** | echo seq 0, token `1111111111111111` | **no reply** |
| 7 | A | close | **no reply**, session removed |

Row 5 is the important one: the count advanced from 1 to 2, not to 3 — the
foreign-endpoint request in row 3 was not counted. The window `0x5` reflects
sequence 2 (bit 0) and sequence 0 (bit 2), with bit 1 clear because sequence 1
never reached the session.

Row 4 confirms that a close from a foreign endpoint does not tear down the
session.

Additional cases from the same class of experiment:

| Case | Result |
|------|--------|
| Two opens from the same source port | Two distinct tokens, both simultaneously usable |
| Same token replayed to the other address family's listener on the same port | No reply |
| Echo with an all-zero token | No reply |
| Echo with the token of an already-closed session | No reply |
| Repeated close with the same token | No reply |
| 1000 opens from one source endpoint | 1000 distinct tokens, all usable |

---

## 3. Close Lifecycle

### 3.1 Client-initiated close

Capture: `../captures/server-close-lifecycle.pcapng`. Session token
`eef1e3fb1ab2eb66`, negotiated ReceivedStats = both, StampAt = none.

| frame | direction | bytes |
|-------|-----------|-------|
| 1 | C → S | `14a75b01 01020280e0ba84bf030380a8d6b9070506` |
| 2 | S → C | `14a75b03 eef1e3fb1ab2eb66 01020280e0ba84bf030380a8d6b9070506` |
| 3 | C → S | `14a75b00 eef1e3fb1ab2eb66 00000000 00000000 0000000000000000` |
| 4 | S → C | `14a75b02 eef1e3fb1ab2eb66 00000000 01000000 0100000000000000` |
| 5 | C → S | `14a75b00 eef1e3fb1ab2eb66 01000000 00000000 0000000000000000` |
| 6 | S → C | `14a75b02 eef1e3fb1ab2eb66 01000000 02000000 0300000000000000` |
| 7 | C → S | `14a75b00 eef1e3fb1ab2eb66 02000000 00000000 0000000000000000` |
| 8 | S → C | `14a75b02 eef1e3fb1ab2eb66 02000000 03000000 0700000000000000` |
| 9 | C → S | `14a75b04 eef1e3fb1ab2eb66` — close request |
| 10 | C → S | `14a75b04 eef1e3fb1ab2eb66` — repeated close |
| 11 | C → S | `14a75b00 eef1e3fb1ab2eb66 09000000 …` — echo after close |

Frames 9, 10 and 11 receive **no response**. Frame 9 removes the session; 10 and
11 are dropped as unknown-token packets. The capture ends with three unanswered
client packets, which is the complete and correct close exchange for
protocol version 1.

Close request layout: magic (3) + flags `0x04` (1) + token (8) = 12 bytes. With
authentication: magic (3) + flags `0x0c` (1) + MAC (16) + token (8) = 28 bytes.

### 3.2 Server-initiated close

Capture: `../captures/server-close.pcapng`. Server configured with a maximum
test duration of 2 s; client requested 600 s and was restricted to 2 s. Requests
sent every ~250 ms.

| frame | direction | bytes (abridged) | note |
|-------|-----------|------------------|------|
| 2 | S → C | `14a75b03 f0592d9fb129558c 010202 80d0acf30e 03…` | duration restricted to 2 s |
| 34 | S → C | `14a75b02 … 0f000000 10000000 ffff000000000000` | ordinary reply, seq 15 |
| 35 | C → S | `14a75b00 … 10000000 …` | echo seq 16 |
| **36** | S → C | `14a75b06 f0592d9fb129558c 10000000 11000000 ffff010000000000` | **flags `0x06` = Reply\|Close** |
| 37 | C → S | `14a75b00 … 63000000 …` | echo seq 99 — **no reply** |

Frame 36 is a complete, ordinary echo reply — correct sequence number (16),
correct token, count 17, window `0x1ffff` — with the Close flag additionally
set. The session is gone from that moment; frame 37 is unanswered.

Timing measurements of the deadline, from the **first** echo request of the
session:

| configured maximum duration | close flag observed at |
|-----------------------------|------------------------|
| 1 s | 3.01 s |
| 2 s | 4.06 s |
| 2 s (second run) | 4.10 s |

The deadline is the configured maximum duration plus a 2-second grace period.

### 3.3 Observed upstream client reaction to a close-flagged reply

Driving an upstream 0.9.1 client against a server that sets the Close flag from
sequence 2 onward:

```
seq=0  recorded
seq=1  recorded
seq=2  reply carries Close  -> run terminates
```

The client reports a "server closed connection" condition, prints the statistics
for sequences 0 and 1 only — the close-flagged reply is **not** recorded — and
exits with a success status.

---

## 4. Open Negotiation

### 4.1 Protocol version is not rejected

Server default configuration. Each row is an open request carrying only the
listed ProtocolVersion value.

| requested ProtocolVersion | server response | reply parameter payload |
|---------------------------|-----------------|-------------------------|
| 1 | open reply, session created | `0102` (version 1) |
| 0 | open reply, session created | `0102` (version 1) |
| 2 | open reply, session created | `0102` (version 1) |
| −1 | open reply, session created | `0102` (version 1) |
| tag absent entirely | open reply, session created | `0102` (version 1) |

In every case the reply flags were `0x03` (Open\|Reply) with the Close flag
**clear** and a non-zero token. Reply length 14 bytes: magic (3) + flags (1) +
token (8) + `0102`.

### 4.2 Parameter payload acceptance

| open request parameter payload | response |
|-------------------------------|----------|
| empty | open reply, 14 bytes |
| tag 2 only, no value bytes | no reply |
| tag 2 with an incomplete multi-byte varint | no reply |
| version 1 + duration 0 | no reply |
| version 1 + duration −1 | no reply |
| version 1 + interval −5 | no reply |
| version 1 + ReceivedStats 4 | no reply |
| version 1 + ReceivedStats −1 | no reply |
| version 1 + StampAt 5 | no reply |
| version 1 + StampAt 4 + Clock 3 | open reply |
| version 1 + Clock 0 | no reply |
| version 1 + Clock 4 | no reply |
| version 1 + unknown tag 42 | open reply, tag ignored |
| version 1 + unknown tag 200 | open reply, tag ignored |
| version 1 + 80 repetitions of unknown tag 42 (~200 bytes) | open reply, all ignored |
| version 1 + ServerFill declared 40 bytes | no reply |
| version 1 + ServerFill declaring 10 bytes with only 2 present | no reply |
| 50 bytes of arbitrary data | no reply |

### 4.3 No-test open

| direction | bytes |
|-----------|-------|
| C → S | `14a75b05` + parameter payload — flags Open\|Close |
| S → C | `14a75b07 0000000000000000` + restricted parameters — flags Open\|Reply\|Close, **zero token** |

With authentication the reply flags are `0x0f` and the reply carries a valid MAC.
The zero token is not usable: subsequent echo and close requests bearing it get
no reply.

### 4.4 Interval restriction against the idle timeout

Client requests an interval of one hour; server has no configured minimum
interval.

| server idle timeout | negotiated interval |
|---------------------|---------------------|
| 4 s | 1 s |
| 8 s | 2 s |
| 20 s | 5 s |
| 40 s | 10 s |
| 60 s | 15 s |

Client-side floor, measured with an upstream 0.9.1 client requesting a 1 s
interval:

| server idle timeout | server-returned interval | client outcome |
|---------------------|--------------------------|----------------|
| 2 s | 500 ms | aborts — reduction below 1 s, in strict **and** loose mode |
| 3 s | 750 ms | aborts — reduction below 1 s, in strict **and** loose mode |
| 4 s | 1 s (no reduction) | proceeds |

Requesting a 2 s interval against a 4 s idle timeout produces a reduction to
exactly 1 s: rejected in strict mode as an ordinary restriction, accepted in
loose mode.

### 4.5 Timestamp allowance

Requested StampAt → negotiated StampAt, by server timestamp allowance:

| requested | dual | single | none |
|-----------|------|--------|------|
| 0 (none) | 0 | 0 | 0 |
| 1 (send) | 1 | 1 | 0 |
| 2 (receive) | 2 | 2 | 0 |
| 3 (both) | 3 | **4** | 0 |
| 4 (midpoint) | 4 | 4 | 0 |

---

## 5. Reply Length

### 5.1 Length as a function of negotiated fields

Reply lengths, unauthenticated, measured across every combination. `S` is
ReceivedStats, `A` is StampAt, `C` is Clock.

| S | A | C | reply length |
|---|---|---|--------------|
| 0 | 0 | any | 16 |
| 1 | 0 | any | 20 |
| 2 | 0 | any | 24 |
| 3 | 0 | any | 28 |
| 3 | 1 or 2 | 1 or 2 | 36 |
| 3 | 1 or 2 | 3 | 44 |
| 3 | 3 | 1 or 2 | 44 |
| 3 | 3 | 3 | 60 |
| 3 | 4 | 1 or 2 | **44** — see below |
| 3 | 4 | 3 | 44 |

Add 16 bytes to every figure when authentication is in use.

### 5.2 Dual-field midpoint wire representation

For StampAt = 4 (midpoint), upstream 0.9.1 emits **two** 8-byte timestamp fields
— the midpoint wall field followed by the midpoint monotonic field, in that order
— for every negotiated Clock, occupying a fixed 16-byte region. With a single
negotiated clock this is one field more than the negotiated parameters imply.
These rows record bytes observed on the wire and nothing else. All vectors below
at negotiated length 0.

| S | A | C | expected from negotiation | observed |
|---|---|---|---------------------------|----------|
| 0 | 4 | 1 | 24 | **32** |
| 0 | 4 | 2 | 24 | **32** |
| 0 | 4 | 3 | 32 | 32 |
| 1 | 4 | 1 | 28 | **36** |
| 1 | 4 | 2 | 28 | **36** |
| 2 | 4 | 1 | 32 | **40** |
| 2 | 4 | 2 | 32 | **40** |
| 3 | 4 | 1 | 36 | **44** |
| 3 | 4 | 2 | 36 | **44** |
| 3 | 4 | 3 | 44 | 44 |

Raw reply for S=0, A=4, C=1 (wall only), 32 bytes:

```
14 a7 5b 02  <token 8>  00 00 00 00
a4 c1 74 90 47 83 ca 18      <- midpoint wall,      1786384545595310500
da 45 a2 3b 00 00 00 00      <- midpoint monotonic, 1000490458
```

The second field was not negotiated. An upstream 0.9.1 client run in this
configuration reports "packet length: 36 bytes" while receiving 44-byte
datagrams, and completes normally.

No other StampAt value deviates. Measured at negotiated length 0, S=3: `A=1` and
`A=2` give 36 bytes for C=1 or C=2 and 44 for C=3; `A=3` gives 44 for C=1 or C=2
and 60 for C=3 — all exactly as the negotiated layout implies.

### 5.2.1 The excess is bounded by the negotiated length

The extra field enlarges the field block, not every reply. Reply length remains
the larger of the negotiated length and the field block (Section 5.3):

```
normal_header   = negotiated field block
upstream_header = normal_header + one 8-byte timestamp field

compatible_reply_len     = max(negotiated_length, normal_header)
upstream_0_9_1_reply_len = max(negotiated_length, upstream_header)
```

Sweep for S=3, A=4, C=1 — `normal_header` 36, `upstream_header` 44:

| negotiated length | 0 | 16 | 32 | 36 | 37 | 40 | 43 | 44 | 45 | 48 | 64 | 128 | 1024 | 4096 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| reply datagram | 44 | 44 | 44 | 44 | 44 | 44 | 44 | 44 | 45 | 48 | 64 | 128 | 1024 | 4096 |
| excess | +8 | +8 | +8 | +8 | +7 | +4 | +1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

Same pattern for the other received-stats settings, with the two minima at 24/32
(S=0), 28/36 (S=1) and 32/40 (S=2). The C=3 control case shows zero excess at
every negotiated length.

At negotiated length 4096 the midpoint region still holds both fields, followed
by fill — the extra field displaces payload rather than extending the datagram.
The excess is therefore **+8**, then **+7…+1**, then **0**, depending on the
negotiated length. There is no negotiated length at which the reply is
universally "negotiated length + 8".

Read that row across columns, not within a column. Each column is a separate
negotiation whose upstream reply has exactly one length,
`upstream_0_9_1_reply_len`. The +8 / +7…+1 / 0 figures are the values the
difference takes across different negotiations; no single negotiation produces a
range of lengths, and nothing here says an intermediate length between
`compatible_reply_len` and `upstream_0_9_1_reply_len` was ever emitted or should
be accepted.

**Consequence for decoding.** In the zero-excess regime
(`upstream_0_9_1_reply_len == compatible_reply_len`) the upstream form
`[wall][mono][payload…]` and a conforming single-clock form `[mono][payload…]`
produce datagrams of identical length, and the vectors above provide no way to
tell them apart. That ambiguity is recorded, not resolved.

### 5.2.2 Reached without requesting midpoint

Server restricted to a single timestamp, S=3, negotiated length 0:

| requested A/C | negotiated A/C | expected from negotiation | observed |
|---------------|----------------|---------------------------|----------|
| 3 / 1 | 4 / 1 | 36 | **44** |
| 3 / 2 | 4 / 2 | 36 | **44** |
| 3 / 3 | 4 / 3 | 44 | 44 |
| 4 / 1 | 4 / 1 | 36 | **44** |

### 5.2.3 Version comparison

Negotiated length 0, S=3:

| build | A=4, C=1 | A=4, C=2 | A=4, C=3 |
|-------|----------|----------|----------|
| 0.9.0 release | 36, one field | 36, one field (process-relative value) | 44, two fields |
| 0.9.1 release | **44, two fields** | **44, two fields** | 44, two fields |
| tested post-0.9.1 tree (six upstream commits past the 0.9.1 tag) | **44, two fields** | **44, two fields** | 44, two fields |

The 0.9.0 release emits exactly the negotiated fields; 0.9.1 emits the dual-field
form; the tested post-0.9.1 tree behaves like 0.9.1. The six post-0.9.1 commits
are post-release work and are not part of the 0.9.1 release.

A contaminated-side server emitting **only** the negotiated midpoint field was
driven by an upstream 0.9.1 client for wall-only, monotonic-only and dual-clock
midpoint configurations, and the client completed every run cleanly. This is
contaminated-side consistency validation; the black-box part of it is the
client's acceptance of the conforming form.

### 5.3 Negotiated length versus actual reply length

Server bound to an interface with a 1500-byte MTU, ReceivedStats = none,
StampAt = none (16-byte field block).

| requested length | negotiated length returned | reply datagram |
|------------------|----------------------------|----------------|
| 0 | 0 | 16 |
| 10 | 10 | 16 |
| 16 | 16 | 16 |
| 17 | 17 | 17 |
| 1000 | 1000 | 1000 |
| 1400 | 1400 | 1400 |
| 1500 | 1500 | 1500 |
| 1501 | 1501 | **1500** |
| 2000 | 2000 | **1500** |
| 3000 | 3000 | **1500** |
| 8000 | 8000 | **1500** |

The negotiated value returned to the client is not clamped; the datagram is.

A session negotiating a length of 0 or 20 with full statistics and dual
timestamps on both clocks produced 60-byte replies — the field block wins when
it is larger than the negotiated length.

### 5.4 Request length versus reply length

| negotiated length | request datagram | reply datagram |
|-------------------|------------------|----------------|
| 16 | 216 (200 trailing bytes) | 16 |
| 64 | 16 | 64 |
| 64 | 32 | 64 |
| 64 | 64 | 64 |

The reply length depends only on the negotiation, never on the request.

### 5.5 Maximum request length policy

Server configured with a maximum packet length of 64; client requested 200 and
was restricted to 64.

| request datagram | result |
|------------------|--------|
| 16 | reply, 64 bytes |
| 32 | reply, 64 bytes |
| 64 | reply, 64 bytes |
| 65 | **no reply** |
| 100 | **no reply** |
| open request larger than 64 | **accepted**, open reply sent |

Server configured with a maximum packet length of 10 (below the minimum field
block): the session opens and negotiates a length of 10, and every 16-byte echo
request is dropped.

---

## 6. Server Fill

### 6.1 Default fill bytes and pattern phase

Default-configuration server, ReceivedStats = none, StampAt = none, so the
payload begins at offset 16.

| negotiated length | reply payload |
|-------------------|---------------|
| 17 | `69` |
| 20 | `72 74 74 69` |
| 24 | `72 74 74 69 72 74 74 69` |
| 48 | `72 74 74 69` repeated 8 times |

Four consecutive replies on one session with a 7-byte payload:

```
seq 0: 72 74 74 69 72 74 74
seq 1: 69 72 74 74 69 72 74
seq 2: 74 69 72 74 74 69 72
seq 3: 74 74 69 72 74 74 69
```

The four-byte pattern `69 72 74 74` is emitted as a continuous stream; its phase
is not reset per packet, and it carries over between sessions on the same
listener.

### 6.2 Fill descriptor negotiation

Default allow-list (random fills only), negotiated length 32 so the payload is
16 bytes:

| requested descriptor | negotiated descriptor returned | payload |
|----------------------|-------------------------------|---------|
| `rand` | `rand` | random, differs per packet |
| `none` | `pattern:69727474` | `72747469…` |
| `pattern:aabb` | `pattern:69727474` | `72747469…` |
| `pattern:69727474` | `pattern:69727474` | `72747469…` |
| `bogus` | `pattern:69727474` | `72747469…` |
| 32-character string | `pattern:69727474` | `72747469…` |
| 33-character string | — | **open dropped** |

Permissive allow-list:

| requested descriptor | negotiated descriptor returned | payload |
|----------------------|-------------------------------|---------|
| `rand` | `rand` | random |
| `pattern:aabb` | `pattern:aabb` | `aabbaabbaabbaabb…` |
| `pattern:00` | `pattern:00` | `0000000000000000…` |
| `pattern:ff00` | `pattern:ff00` | `ff00ff00ff00ff00…` |
| `pattern:zz` | `pattern:69727474` | `69727474…` (invalid hex, falls back) |
| `none` | `none` | **unspecified residual bytes** |
| `pattern:` | — | **server terminated** |

Empty allow-list: every requested descriptor, including `rand`, is answered with
`pattern:69727474`.

### 6.3 The refused descriptor does not describe the payload

Two servers with different configured fills, default allow-list (`rand` only),
negotiated length 32 so the payload is 16 bytes:

| Server fill | Requested descriptor | Negotiated descriptor returned | Payload |
|-------------|----------------------|-------------------------------|---------|
| random | none | absent | `25ae387a787d641d82eb514f2bfca15f` (random) |
| random | `pattern:aabb` | `pattern:69727474` | `29ce866b6d192c74376e7161dd71975d` (random) |
| random | `rand` | `rand` | random |
| `pattern:abcd` | none | absent | `abcdabcdabcdabcdabcdabcdabcdabcd` |
| `pattern:abcd` | `pattern:aabb` | `pattern:69727474` | `abcdabcdabcdabcdabcdabcdabcdabcd` |
| `pattern:abcd` | `rand` | `rand` | random |

On refusal, the descriptor returned to the client is a **fixed default string**,
not a description of the bytes the server actually sends. A client must not
predict payload content from it.

### 6.4 No-fill payload content

Server configured with no fill and a permissive allow-list, negotiated length 32
(16-byte payload):

| request payload | reply payload |
|-----------------|---------------|
| `000102…0f` (16 bytes) | `000102…0f` — the request's own bytes |
| none (16-byte request) | `000102…0f` — **residue from the previous packet** |

On a `none` fill negotiated through the descriptor, one observed 16-byte payload
was `c0cbacf6220380897a044009 046e6f6e`, whose trailing bytes are a fragment of
the preceding open reply's parameter payload. Payload content under no-fill is
unspecified and may contain data from unrelated traffic.

---

## 7. Authentication

Server configured with key `correctkey`.

| request | result |
|---------|--------|
| Open, flags `0x09`, valid MAC | Open reply, flags `0x0b`, valid MAC, 43 bytes |
| Echo, flags `0x08`, valid MAC | Echo reply, flags `0x0a`, valid MAC, 44 bytes |
| Close, flags `0x0c`, valid MAC | Session removed, **no reply** |
| No-test open, flags `0x0d`, valid MAC | Reply flags `0x0f`, zero token, valid MAC |
| Open with no HMAC flag and no MAC field | no reply |
| Echo with no HMAC flag and no MAC field | no reply |
| Close with no HMAC flag and no MAC field | no reply, **session survives** |
| Open with MAC computed under a different key | no reply |
| Echo with MAC computed under a different key | no reply |
| Echo with one bit flipped in the MAC | no reply |
| Echo with the MAC field all zeros | no reply |
| Echo with the MAC field truncated to 8 bytes | no reply |
| 19-byte datagram (too short for the MAC field) | no reply |
| 20-byte datagram, flags `0x08`, valid MAC, nothing else | no reply |
| Valid MAC but the HMAC flag cleared in flags | no reply |
| MAC recomputed with the flag cleared, flag cleared | no reply |

Against a server with **no** key, any request with the HMAC flag set gets no
reply.

**Session integrity across failures.** After the eight failing cases above were
sent to a live session, the next correctly authenticated echo was served
normally and the received count had advanced only for the accepted packets.

**Reply authentication is mandatory.** A contaminated-side server that
authenticated its open reply correctly but omitted the HMAC flag from echo
replies caused an upstream 0.9.1 client to discard every echo reply and terminate
abnormally with zero packets received. The observation that matters is the
*client's* reaction, which is black-box; the server was only the stimulus.

---

## 8. Idle Expiry

Server configured with a 2-second idle timeout. Each trial: open, one echo to
start the idle clock, wait, then two echoes back to back.

| gap after the last accepted echo | first request after the gap | the request after that |
|---|---|---|
| 5.0 s | answered | answered |
| 6.5 s | answered | answered |
| 6.9 s | answered | answered |
| 7.1 s | answered | **dropped** |
| 7.5 s | answered | **dropped** |
| 9.0 s | answered | **dropped** |

Deadline = idle timeout + 5 s. The first request past the deadline is still
answered, and the session is released while handling it.

**A session that has never carried an echo request does not expire.** With the
same 2-second timeout, a session left completely idle for 9 seconds after open
was served normally on its first echo request.

With the idle timeout disabled, a session idle for 9 seconds was served normally
and remained usable.

---

## 9. Malformed Packet Admission

Session established; probes sent on the same socket. Prefix used:
`14a75b00` + token + `00000000`.

| probe | result |
|-------|--------|
| 0–3 byte datagram | no reply |
| 4–11 byte datagram (flags `0x00`) | no reply |
| 12–15 byte datagram (flags `0x00`) | no reply |
| 16-byte datagram (flags `0x00`) | **reply, 28 bytes** |
| magic `15a75b` | no reply |
| magic `000000` | no reply |
| flags `0x10`, `0x20`, `0x40`, `0x80`, `0xf0`, `0xff`, `0x1f` | no reply |
| flags `0x02`, `0x03`, `0x06`, `0x07`, `0x0a` (Reply set) | no reply |
| flags `0x09` (Open\|HMAC) to a non-authenticating server | no reply |
| flags `0x01` (Open) | open reply, new session |
| flags `0x05` (Open\|Close) | open reply, zero token |
| echo-shaped datagram with flags `0x01` | treated as an open; the token bytes are parsed as parameters |
| close with 20 trailing bytes | no reply, session removed |

A 12-byte datagram with flags `0x04` is a valid close request; a 12-byte
datagram with flags `0x00` is too short to be an echo request and is dropped.

The Open flag decides *interpretation*, not admission. For an otherwise
admissible datagram it wins over Close (`0x05` is the no-test open form) and over
an echo-shaped body (the token/sequence bytes are read as Open parameter data).
It does not rescue a datagram from an independent rejection: `0x03` (Open|Reply)
is dropped for the Reply flag, `0x09` (Open|HMAC) to a non-authenticating server
is dropped for the authentication mismatch, `0x1f` is dropped for the undefined
flag bits, and an Open whose parameter payload is malformed or out of range is
dropped as well (Section 4.2).

---

## 10. DSCP Negotiation and Application

The values below are **raw IP TOS / Traffic Class byte** values, which is what
the wire parameter carries — not 6-bit codepoints. Expedited Forwarding is
therefore 184 (0xb8) in this table, not 46.

| requested DSCP | negotiated value returned | echo replies |
|----------------|---------------------------|--------------|
| 0 | absent (0) | delivered |
| 46 | 46 | delivered |
| 184 | 184 | delivered |
| 255 | 255 | delivered |
| 256 | 256 | **all dropped** |
| 300 | 300 | **all dropped** |
| −5 | −5 | delivered |

With DSCP disallowed by server policy, every requested value is returned as
absent (0) and replies are delivered normally.

A value the server cannot apply to its socket is still accepted during
negotiation; the session then opens successfully and answers nothing.

---

## 11. Captures

| File | Contents |
|------|----------|
| `../captures/server-recv-window.pcapng` | One session exercising sequential, gap, duplicate, reorder and far-gap sequence numbers |
| `../captures/server-close-lifecycle.pcapng` | Client-initiated close, repeated close, and an echo after close — none answered |
| `../captures/server-close.pcapng` | Server-initiated close via the maximum-duration limit; frame 36 carries flags `0x06` |
| `../captures/server-session-identity.pcapng` | Foreign-source-port echo and close, unknown token, and the original endpoint continuing to be served |

All captures are loopback traffic with the server on a fixed port and the client
on an ephemeral port.

**Capture container metadata.** Every `.pcapng` under `../captures/` was reviewed
and its container rewritten so that it carries no capture comment, no
section/interface description, no host, user or capture-machine identification,
no capture filter string, and no filesystem path — only the packet records and
the rewriting tool's own version string. Packet bytes and timestamps were **not**
altered: for each file, the full frame hexdump and the per-frame epoch timestamps
were verified byte-identical before and after the rewrite.
