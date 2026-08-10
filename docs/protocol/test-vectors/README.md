# IRTT Packet Test Vectors

**Source:** Black-box captures from irtt 0.9.1 on macOS Darwin 25.3.0 arm64  
**Date:** 2026-04-28

All vectors are from actual network captures (not constructed).

---

## Vector 1: Open Request (no HMAC)

**Scenario:** Client opens session with default parameters (duration=3s,
interval=1s, stats=both, tstamp=both, clock=both).  
**Direction:** Client → Server  
**irtt version:** 0.9.1  
**Command:** `irtt client -d 3s -i 1s 127.0.0.1:2112`

**Packet (24 bytes):**
```
14 a7 5b 01 01 02 02 80 f8 82 ad 16 03 80 a8 d6
b9 07 05 06 06 06 07 06
```

**Field breakdown:**
- Bytes 0-2: Magic `14 a7 5b`
- Byte 3: Flags `01` (Open)
- Bytes 4-23: Parameter payload:
  - Tag 1 (ProtocolVersion): value `02` → zigzag → 1
  - Tag 2 (Duration): value `80 f8 82 ad 16` → zigzag → 3000000000 ns (3s)
  - Tag 3 (Interval): value `80 a8 d6 b9 07` → zigzag → 1000000000 ns (1s)
  - Tag 5 (ReceivedStats): value `06` → zigzag → 3 (Both)
  - Tag 6 (StampAt): value `06` → zigzag → 3 (Both)
  - Tag 7 (Clock): value `06` → zigzag → 3 (Both)

**Notes:**
- No connection token field (token not yet assigned)
- Tags 4 (Length), 8 (DSCP) omitted because values are 0
- Tag 9 (ServerFill) omitted because not specified

---

## Vector 2: Open Reply (no HMAC)

**Scenario:** Server accepts session, returns token and echoed parameters.  
**Direction:** Server → Client  
**Token:** `0x7896b6ab87715213`

**Packet (32 bytes):**
```
14 a7 5b 03 13 52 71 87 ab b6 96 78 01 02 02 80
f8 82 ad 16 03 80 a8 d6 b9 07 05 06 06 06 07 06
```

**Field breakdown:**
- Bytes 0-2: Magic `14 a7 5b`
- Byte 3: Flags `03` (Open | Reply)
- Bytes 4-11: Connection token `13 52 71 87 ab b6 96 78` (LE: 0x7896b6ab87715213)
- Bytes 12-31: Parameter payload (same as request — no restrictions)

---

## Vector 3: Echo Request (no HMAC, default stats+timestamps)

**Scenario:** First test packet, sequence 0, default settings.  
**Direction:** Client → Server  
**Token:** `0x7896b6ab87715213`, Seq: 0

**Packet (60 bytes):**
```
14 a7 5b 00 13 52 71 87 ab b6 96 78 00 00 00 00
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
00 00 00 00 00 00 00 00 00 00 00 00
```

**Field breakdown:**
- Bytes 0-2: Magic `14 a7 5b`
- Byte 3: Flags `00` (no flags — echo request from client)
- Bytes 4-11: Connection token
- Bytes 12-15: Sequence number `00 00 00 00` (LE: 0)
- Bytes 16-19: Received count (zeroed placeholder)
- Bytes 20-27: Received window (zeroed placeholder)
- Bytes 28-35: Receive wall timestamp (zeroed placeholder)
- Bytes 36-43: Receive mono timestamp (zeroed placeholder)
- Bytes 44-51: Send wall timestamp (zeroed placeholder)
- Bytes 52-59: Send mono timestamp (zeroed placeholder)

**Notes:** All stats and timestamp fields are zeroed in echo requests.
They serve as placeholders to reach the negotiated packet length.

---

## Vector 4: Echo Reply (no HMAC, default stats+timestamps)

**Scenario:** Server reply to seq 2, third packet in session.  
**Direction:** Server → Client  
**Token:** `0x7896b6ab87715213`, Seq: 2, Recv Count: 3, Window: 0x07

**Packet (60 bytes):**
```
14 a7 5b 02 13 52 71 87 ab b6 96 78 02 00 00 00
03 00 00 00 07 00 00 00 00 00 00 00 b8 1a 33 0c
86 6d aa 18 de 26 35 95 00 00 00 00 80 4d 33 0c
86 6d aa 18 b2 57 35 95 00 00 00 00
```

**Field breakdown:**
- Bytes 0-2: Magic `14 a7 5b`
- Byte 3: Flags `02` (Reply)
- Bytes 4-11: Connection token
- Bytes 12-15: Sequence number `02 00 00 00` (LE: 2)
- Bytes 16-19: Received count `03 00 00 00` (LE: 3)
- Bytes 20-27: Received window `07 00 00 00 00 00 00 00` (LE: 0x07, bits 0-2 set)
- Bytes 28-35: Receive wall timestamp `b8 1a 33 0c 86 6d aa 18` (LE int64)
- Bytes 36-43: Receive mono timestamp `de 26 35 95 00 00 00 00` (LE int64)
- Bytes 44-51: Send wall timestamp `80 4d 33 0c 86 6d aa 18` (LE int64)
- Bytes 52-59: Send mono timestamp `b2 57 35 95 00 00 00 00` (LE int64)

**Window interpretation (for seq 2):**
- Bit 0 (0x01): Seq 2 received ✓
- Bit 1 (0x02): Seq 1 received ✓
- Bit 2 (0x04): Seq 0 received ✓

---

## Vector 5: Close Request (no HMAC)

**Scenario:** Client sends close after test.  
**Direction:** Client → Server  
**Token:** `0x7896b6ab87715213`

**Packet (12 bytes):**
```
14 a7 5b 04 13 52 71 87 ab b6 96 78
```

**Field breakdown:**
- Bytes 0-2: Magic `14 a7 5b`
- Byte 3: Flags `04` (Close)
- Bytes 4-11: Connection token

---

## Vector 6: HMAC Open Request

**Scenario:** Client opens session with HMAC key "testkey".  
**Direction:** Client → Server  
**HMAC key:** ASCII "testkey" (7 bytes)

**Packet (40 bytes):**
```
14 a7 5b 09 ff 90 16 a7 aa 53 78 16 9e c3 a2 d5
54 dc 30 36 01 02 02 80 d0 ac f3 0e 03 80 a8 d6
b9 07 05 06 06 06 07 06
```

**Field breakdown:**
- Bytes 0-2: Magic `14 a7 5b`
- Byte 3: Flags `09` (Open | HMAC)
- Bytes 4-19: HMAC `ff 90 16 a7 aa 53 78 16 9e c3 a2 d5 54 dc 30 36`
- Bytes 20-39: Parameter payload

**HMAC verified independently with Python (HMAC-MD5 over entire packet
with HMAC field zeroed).**

---

## Vector 7: HMAC Echo Request

**Scenario:** Echo request with HMAC, seq 0.  
**Direction:** Client → Server  
**Token:** `0x4387e9eb5d3fca59`, HMAC key: "testkey"

**Packet (76 bytes):**
```
14 a7 5b 08 d9 87 4f 82 b3 13 10 31 59 ad 2c 8f
6b da ef ff 59 ca 3f 5d eb e9 87 43 00 00 00 00
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
00 00 00 00 00 00 00 00 00 00 00 00
```

**Field breakdown:**
- Bytes 0-2: Magic
- Byte 3: Flags `08` (HMAC)
- Bytes 4-19: HMAC
- Bytes 20-27: Connection token `59 ca 3f 5d eb e9 87 43`
- Bytes 28-31: Sequence number `00 00 00 00`
- Bytes 32-75: Stats + timestamp placeholders (zeroed)

---

## Vector 8: HMAC Close Request

**Scenario:** Close with HMAC.  
**Direction:** Client → Server  
**Token:** `0x4387e9eb5d3fca59`, HMAC key: "testkey"

**Packet (28 bytes):**
```
14 a7 5b 0c f5 cd 0f a9 de 9d 7d 66 8d 91 a5 32
48 0e 42 e0 59 ca 3f 5d eb e9 87 43
```

**Field breakdown:**
- Bytes 0-2: Magic
- Byte 3: Flags `0c` (Close | HMAC)
- Bytes 4-19: HMAC
- Bytes 20-27: Connection token

---

## Vector 9: No-Test Mode (Open+Close Request)

**Scenario:** Client opens and immediately closes.  
**Direction:** Client → Server

**Packet (25 bytes):**
```
14 a7 5b 05 01 02 02 80 e0 ba 84 bf 03 03 80 a8
d6 b9 07 05 06 06 06 07 06
```

**Field breakdown:**
- Bytes 0-2: Magic
- Byte 3: Flags `05` (Open | Close)
- Bytes 4-24: Parameter payload
  - Tag 1: ProtocolVersion = 1
  - Tag 2: Duration = 60000000000 ns (60s, default)
  - Tag 3: Interval = 1000000000 ns (1s, default)
  - Tag 5: ReceivedStats = 3 (Both)
  - Tag 6: StampAt = 3 (Both)
  - Tag 7: Clock = 3 (Both)

---

## Vector 10: No-Test Mode (Open+Close Reply)

**Scenario:** Server acknowledges no-test open+close.  
**Direction:** Server → Client

**Packet (33 bytes):**
```
14 a7 5b 07 00 00 00 00 00 00 00 00 01 02 02 80
e0 ba 84 bf 03 03 80 a8 d6 b9 07 05 06 06 06 07
06
```

**Field breakdown:**
- Bytes 0-2: Magic
- Byte 3: Flags `07` (Open | Reply | Close)
- Bytes 4-11: Connection token `00 00 00 00 00 00 00 00` (ZERO)
- Bytes 12-32: Parameter payload

**Notes:** In no-test mode, the server returns a zero connection token.
The Close flag in this context means "acknowledged close" not "rejected."

---

## Vector 11: Minimal Echo Packet (no stats, no timestamps)

**Scenario:** Echo request with stats=none, tstamp=none.  
**Direction:** Client → Server  
**Token:** `0xa0316fa25c61154e`, Seq: 0

**Packet (16 bytes):**
```
14 a7 5b 00 4e 15 61 5c a2 6f 31 a0 00 00 00 00
```

**Field breakdown:**
- Bytes 0-2: Magic
- Byte 3: Flags `00`
- Bytes 4-11: Connection token
- Bytes 12-15: Sequence number

This is the absolute minimum echo packet size.

---

## Vector 12: Midpoint Timestamps (both clocks)

**Scenario:** Echo reply with stats=both, tstamp=midpoint, clock=both.  
**Direction:** Server → Client

**Packet (44 bytes):**
```
14 a7 5b 02 91 a4 1e 7a f0 14 f6 62 00 00 00 00
01 00 00 00 01 00 00 00 00 00 00 00 f7 0e 2c 93
a2 6d aa 18 5a 5f 14 47 03 00 00 00
```

**Field breakdown:**
- Bytes 0-2: Magic
- Byte 3: Flags `02` (Reply)
- Bytes 4-11: Token
- Bytes 12-15: Seq `00 00 00 00` (0)
- Bytes 16-19: Recv count `01 00 00 00` (1)
- Bytes 20-27: Recv window `01 00 00 00 00 00 00 00`
- Bytes 28-35: Midpoint wall timestamp
- Bytes 36-43: Midpoint mono timestamp

**Notes:** Only midpoint timestamps present. No receive/send timestamp
fields. Confirms midpoint is mutually exclusive with receive+send.
