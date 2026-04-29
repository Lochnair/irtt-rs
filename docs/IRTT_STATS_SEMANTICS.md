# IRTT stats semantics

## Executive summary

This document is a clean-room behavioral specification for upstream-compatible
IRTT client result statistics in `irtt-rs`. It describes externally observable
client result behavior only.

Key compatibility rules:

- The first accepted reply for a sequence is the only reply that can populate
  that sequence's result sample.
- A late unique reply is still counted as received, is stats-eligible, and can
  reduce final loss.
- A duplicate reply is counted as received and increments duplicate counters,
  but it does not replace the first result for that sequence and does not
  contribute to RTT, median, IPDV, one-way delay, or server-processing stats.
- A late duplicate is categorized as a duplicate, not as a late unique packet.
- `packets_received` can exceed `packets_sent` when duplicate replies occur.
- Loss percentage is based on sent packets minus unique RTT/stat samples, not
  raw received datagrams.
- Summary RTT uses adjusted RTT when server-processing time is available.
  Server-processing time is also reported separately when timestamp data allows
  it.
- IPDV excludes losses and duplicates. Round-trip IPDV is sequence-adjacent,
  not arrival-adjacent.
- Per-packet IPDV values are signed. Summary IPDV stats use absolute values.
- One-way delay requires wall-clock server timestamp data. Send/receive IPDV
  can use compatible monotonic timestamp data.
- Medians are exact finite-run summary values. Upstream-compatible output does
  not expose rolling medians or percentiles beyond median.

## Definitions

### Sent packet

A packet for which the client completed a send attempt successfully enough to
include it in sent-packet accounting.

Evidence: `confirmed by source-side behavioral analysis`.

### Unique reply

The first accepted reply observed by the client for a sequence number.

Only a unique reply populates that sequence's measurement record.

Evidence: `confirmed by source-side behavioral analysis`.

### Normal reply

A unique reply for which either no previous unique reply has been accepted, or
whose sequence number is greater than or equal to the immediately previous
unique reply's sequence number.

Evidence: `confirmed by source-side behavioral analysis`.

### Late unique reply

A unique reply whose sequence number is lower than the immediately previous
unique reply's sequence number. The first accepted unique reply is never late.

Lateness is an arrival-order comparison against the previous unique reply, not a
comparison against the highest sequence number ever seen. For example, if unique
replies arrive as `5, 3, 4`, sequence `3` is late and sequence `4` is not late.

Evidence: `confirmed by source-side behavioral analysis`.

### Duplicate reply

An accepted reply for a sequence number that already has a unique reply.

Duplicate replies are included in `packets_received` and `duplicates`, but they
do not modify the stored per-sequence measurement.

Evidence: `confirmed by source-side behavioral analysis`.

### Late duplicate reply

A duplicate reply whose sequence number is lower than the immediately previous
unique reply's sequence number.

The observable category is duplicate. It increments duplicate accounting, not
late-packet accounting.

Evidence: `confirmed by source-side behavioral analysis`.

### Lost packet

A sent packet for which no unique reply was accepted before client result
finalization.

Evidence: `confirmed by source-side behavioral analysis`.

### Stats-eligible reply

A normal or late unique reply. Stats eligibility can still be limited by metric
availability, such as missing timestamp data.

Evidence: `confirmed by source-side behavioral analysis`.

## Evidence / Verification Status Note

Evidence labels used in this document:

- `verified from public CLI/JSON/docs`: confirmed from public CLI help,
  documented behavior, public output shape, or JSON-facing field names.
- `confirmed by source-side behavioral analysis`: confirmed by inspecting
  upstream behavior while keeping this document free of upstream source code,
  private names, pseudocode, or implementation structure.
- `confirmed by optional local smoke test`: confirmed by a cheap local
  black-box run.
- `unresolved`: not established strongly enough to require compatibility.
- `intentionally implementation-defined for irtt-rs`: `irtt-rs` may choose a
  behavior.

No optional local smoke tests were run for this revision. The workspace did not
contain a runnable upstream CLI entry point, and this task intentionally avoided
building a network impairment harness.

## Behavior Table For Event Categories

| Event category | Classification rule | Received count | Late count | Duplicate count | Updates per-sequence measurement | Updates bytes/rate timing | Stats contribution |
|---|---|---:|---:|---:|---:|---:|---:|
| Normal reply | First accepted reply for sequence; not late | Yes | No | No | Yes | Yes | Yes, where metric data exists |
| Late unique reply | First accepted reply for sequence; lower than immediately previous unique reply | Yes | Yes | No | Yes | Yes | Yes, where metric data exists |
| Duplicate reply | Reply for sequence already measured; not late by arrival-order rule | Yes | No | Yes | No | No | No |
| Late duplicate reply | Reply for sequence already measured; lower than immediately previous unique reply | Yes | No | Yes | No | No | No |
| Loss | No unique reply before finalization | No | No | No | No | No | No |

Evidence: `confirmed by source-side behavioral analysis`.

## Stats Inclusion Matrix

In this table, "received count" means summary `packets_received`, not RTT sample
count.

| Sample category | Received count | RTT stats | Median RTT | IPDV | Server processing | One-way delay | Loss percentage |
|---|---:|---:|---:|---:|---:|---:|---:|
| Normal reply | Included | Included | Included | Included when adjacent previous sequence also has a unique reply | Included when timestamp data provides server receive and send endpoints | Included when wall-clock server timestamp data exists | Reduces loss |
| Late unique reply | Included | Included | Included | Included when adjacent previous sequence also has a unique reply | Included when timestamp data provides server receive and send endpoints | Included when wall-clock server timestamp data exists | Reduces loss |
| Duplicate reply | Included | Excluded | Excluded | Excluded | Excluded | Excluded | Does not reduce loss |
| Late duplicate reply | Included | Excluded | Excluded | Excluded | Excluded | Excluded | Does not reduce loss |
| Loss | Excluded | Excluded | Excluded | Excluded | Excluded | Excluded | Increases loss |

Evidence: `confirmed by source-side behavioral analysis`.

## Reply Categories

### Normal and late unique replies

Normal and late unique replies are both stats-eligible. They differ only in late
accounting and human per-packet labeling.

A late unique reply:

- increments `late_packets`;
- is included in `packets_received`;
- populates the per-sequence result;
- contributes to RTT stats and RTT median;
- contributes to IPDV, one-way delay, and server-processing stats when those
  metrics are otherwise available;
- reduces final loss for its sequence.

Evidence: `confirmed by source-side behavioral analysis`.

### Duplicate replies

A duplicate reply:

- increments `duplicates`;
- is included in `packets_received`;
- does not update the stored result for its sequence;
- does not contribute to RTT, median, IPDV, one-way delay, or
  server-processing stats;
- does not reduce loss, because loss uses unique RTT/stat samples rather than
  raw received datagrams.

Evidence: `confirmed by source-side behavioral analysis`.

### Late duplicates

If a reply is both duplicate and late, duplicate treatment wins. It is counted
as a duplicate and as received, but it does not increment `late_packets` and is
not stats-eligible.

Evidence: `confirmed by source-side behavioral analysis`.

## Late Replies

Late unique replies are included in the same primary sample sets as normal
unique replies.

| Question | Behavior | Evidence |
|---|---|---|
| Is a late unique reply counted as received? | Yes. | `confirmed by source-side behavioral analysis` |
| Included in primary RTT stats? | Yes. | `confirmed by source-side behavioral analysis` |
| Included in median RTT? | Yes. | `confirmed by source-side behavioral analysis` |
| Included in RTT min/max/mean/stddev? | Yes. | `confirmed by source-side behavioral analysis` |
| Included in round-trip IPDV? | Yes, when the adjacent previous sequence also has a unique reply. | `confirmed by source-side behavioral analysis` |
| Included in send/receive IPDV? | Yes, with the same adjacency rule and timestamp availability requirements. | `confirmed by source-side behavioral analysis` |
| Included in one-way delay? | Yes, when wall-clock server timestamp data is available. | `confirmed by source-side behavioral analysis` |
| Included in server-processing stats? | Yes, when server receive and send timestamp endpoints are available. | `confirmed by source-side behavioral analysis` |
| Can it reduce final loss after the packet was previously outstanding? | Yes, if it arrives before result finalization. | `confirmed by source-side behavioral analysis` |
| Are finalized results revised after completion? | No externally observable revision occurs after the client result is complete. | `confirmed by source-side behavioral analysis` |

## Duplicate Replies

Duplicate replies are accounting events, not measurement samples.

| Question | Behavior | Evidence |
|---|---|---|
| Included in `packets_received`? | Yes. | `confirmed by source-side behavioral analysis` |
| Included in RTT stats? | No. | `confirmed by source-side behavioral analysis` |
| Included in median RTT? | No. | `confirmed by source-side behavioral analysis` |
| Included in IPDV? | No. | `confirmed by source-side behavioral analysis` |
| Included in one-way delay? | No. | `confirmed by source-side behavioral analysis` |
| Included in server-processing? | No. | `confirmed by source-side behavioral analysis` |
| Counted separately only, or also in received totals? | Both: `duplicates` and `packets_received`. | `confirmed by source-side behavioral analysis` |
| Affect loss percentage? | No, except indirectly in downstream-loss estimates that use raw received totals. | `confirmed by source-side behavioral analysis` |
| Can `packets_received` exceed `packets_sent`? | Yes. | `confirmed by source-side behavioral analysis` |

## Loss

### Total loss count

The effective total loss count is:

```text
packets_sent - stats.rtt.n
```

Equivalently, it is sent packets minus unique replies with RTT samples.
Duplicates do not reduce this count.

There is no separate top-level JSON field for total packet loss count.

Evidence: `confirmed by source-side behavioral analysis`.

### Loss percentage

When at least one RTT sample exists:

```text
packet_loss_percent = 100 * (packets_sent - stats.rtt.n) / packets_sent
```

When there are no RTT samples, `packet_loss_percent` is reported as `100`.

If no packets were sent, the percentage is not a meaningful network-loss
measurement. `irtt-rs` should avoid division by zero and treat zero-send
percentage behavior as `intentionally implementation-defined for irtt-rs`
unless upstream-compatible behavior is explicitly verified for that case.

Evidence: `confirmed by source-side behavioral analysis`.

### Upstream and downstream loss

The client can request server-side receive statistics with `--stats`:

| `--stats` mode | Observable meaning |
|---|---|
| `none` | Do not request server receive statistics. |
| `count` | Request the server's total received count. |
| `window` | Request a recent receive-status window. |
| `both` | Request both count and window. This is the default. |

Mode names and public descriptions are `verified from public CLI/JSON/docs`.
The accounting behavior below is `confirmed by source-side behavioral analysis`.

When a server received-count value has been observed:

```text
upstream_loss_percent = 100 * (packets_sent - server_packets_received) / packets_sent
downstream_loss_percent = 100 * (server_packets_received - packets_received) / server_packets_received
```

If no server received-count value has been observed, upstream and downstream
loss percentages remain zero in JSON-compatible summary output.

Important compatibility point: `packets_received` includes duplicates.
Therefore duplicates can reduce the downstream-loss estimate and can make it
negative in artificial cases.

### Per-packet JSON loss values

`round_trips[].lost` has these observable values:

| Value | Meaning |
|---|---|
| `false` | The client accepted a unique reply for this sequence. |
| `true` | No unique reply was accepted and no server receive-window evidence classified the direction. |
| `true_up` | No unique reply was accepted and received server-window evidence indicates the server had not received that request. |
| `true_down` | No unique reply was accepted and received server-window evidence indicates the server had received that request. |

Receive-window classification is limited to packets covered by later replies
that reach the client. Tail losses and packets outside the available window can
remain `true`.

Evidence: `confirmed by source-side behavioral analysis`.

### Unavailable server-side loss data

If server-side count/window data is unavailable, `irtt-rs` should preserve the
observable distinction:

- compute total `packet_loss_percent` from `packets_sent` and unique RTT samples;
- leave upstream/downstream summary percentages at zero unless a server
  received count is actually available;
- use `true`/`false` per-packet loss values unless receive-window evidence is
  available for directional refinement.

Evidence: `confirmed by source-side behavioral analysis`.

## RTT

### Sample set

RTT stats include one sample for each unique reply, including late unique
replies. They exclude duplicates, late duplicates, and losses.

Evidence: `confirmed by source-side behavioral analysis`.

### Raw and adjusted RTT

The per-packet `delay.rtt` and summary `stats.rtt` values use adjusted RTT:

```text
adjusted RTT = client-observed round trip - server processing time
```

The subtraction is applied when server-processing time is available. There is no
separate top-level raw RTT statistic in upstream-compatible JSON.

Evidence: `confirmed by source-side behavioral analysis`.

### Timestamp-mode effect on RTT adjustment

| Timestamp mode | RTT sample available for unique replies? | Server-processing adjustment |
|---|---:|---|
| `none` | Yes | No |
| `send` | Yes | No |
| `receive` | Yes | No |
| `both` | Yes | Yes, when compatible clock data exists |
| `midpoint` | Yes | Available as zero-duration server processing |

Timestamp mode names are `verified from public CLI/JSON/docs`. Adjustment
behavior is `confirmed by source-side behavioral analysis`.

### Server processing greater than raw RTT

Upstream-compatible behavior does not clamp adjusted RTT to zero. If accepted
timestamp data implies server-processing time greater than client-observed raw
RTT, the adjusted RTT may be negative and may participate in RTT stats.

Evidence: `confirmed by source-side behavioral analysis`.

## IPDV

IPDV is instantaneous packet delay variation between adjacent sequence numbers.

### Round-trip IPDV

For sequence `N`, round-trip IPDV exists only when both sequence `N` and
sequence `N-1` have unique replies. The adjacency rule is by sequence number,
not by arrival order.

Missing sequence numbers create gaps. Upstream-compatible behavior does not
bridge over losses for IPDV.

Examples:

| Unique replies present | Round-trip IPDV samples |
|---|---|
| `0, 1, 2` | samples for `1` and `2` |
| `0, 2, 3` | no sample for `2`; sample for `3` |
| `5, 3, 4` | sample eligibility is based on final sequence adjacency, not arrival order |

Evidence: `confirmed by source-side behavioral analysis`.

### Signed and absolute values

Per-packet JSON IPDV values are signed:

```text
current sequence delay - previous sequence delay
```

Summary IPDV stats and human output use absolute values.

Evidence: `confirmed by source-side behavioral analysis`.

### Inclusion rules

| Category | IPDV behavior |
|---|---|
| Normal unique reply | Eligible if adjacent previous sequence also has a unique reply. |
| Late unique reply | Eligible by the same sequence-adjacency rule. |
| Duplicate reply | Excluded. |
| Late duplicate reply | Excluded. |
| Loss | Excluded and creates a gap for the following sequence. |

Evidence: `confirmed by source-side behavioral analysis`.

### Send and receive IPDV

Send and receive IPDV use the same sequence-adjacency, duplicate-exclusion, and
gap rules as round-trip IPDV. They require compatible server timestamp data for
both adjacent unique replies.

When monotonic server timestamp data is available for adjacent samples, it can
be used for send/receive IPDV. Otherwise, compatible wall-clock timestamp data
can be used. Unlike one-way delay, send/receive IPDV does not require externally
synchronized wall clocks when monotonic timestamp differences are available.

Evidence: `confirmed by source-side behavioral analysis`.

## One-Way Delay

One-way delay fields are `delay.send`, `delay.receive`, `stats.send_delay`, and
`stats.receive_delay`.

### Clock requirement

One-way delay requires wall-clock server timestamp data. The values are only
meaningful as absolute one-way delay when client and server wall clocks are
externally synchronized.

The public documentation describes the clock-synchronization limitation.

Evidence: `verified from public CLI/JSON/docs` for the limitation;
`confirmed by source-side behavioral analysis` for field availability.

### Timestamp-mode availability

| Timestamp mode | With wall-clock server timestamps | Without wall-clock server timestamps |
|---|---:|---:|
| `none` | Not available | Not available |
| `send` | Send and receive delay can be present, using the server send timestamp as the available endpoint | Not available |
| `receive` | Send and receive delay can be present, using the server receive timestamp as the available endpoint | Not available |
| `both` | Send and receive delay available | Not available |
| `midpoint` | Send and receive delay available, using the midpoint timestamp as both endpoints | Not available |

In single-endpoint timestamp modes, upstream-compatible output can expose both
one-way delay fields by using the available server endpoint for the missing
side. This is compatibility behavior, not a statement that both physical
one-way paths were independently measured.

Evidence: `confirmed by source-side behavioral analysis`.

### Sample inclusion

One-way delay stats include normal unique replies and late unique replies when
wall-clock timestamp data is available. They exclude duplicate replies, late
duplicates, and losses.

When required timestamp data is unavailable, per-packet delay fields are omitted
from `round_trips[].delay`, and the corresponding summary duration-stat object
has zero samples.

Evidence: `confirmed by source-side behavioral analysis`.

## Server Processing

Server-processing time is the duration between the server-side receive timestamp
and server-side send timestamp for a reply.

### Availability

| Timestamp mode | Server-processing behavior |
|---|---|
| `none` | Not available |
| `send` | Not available |
| `receive` | Not available |
| `both` | Available when compatible clock data exists for both endpoints |
| `midpoint` | Available as zero-duration samples because the same midpoint timestamp is used for both endpoints |

Evidence: `confirmed by source-side behavioral analysis`.

### Sample inclusion

Server-processing stats include normal unique replies and late unique replies
when server-processing time is available. They exclude duplicates, late
duplicates, and losses.

Evidence: `confirmed by source-side behavioral analysis`.

### Relation to RTT

Server-processing time is both:

- reported separately in `stats.server_processing_time`; and
- subtracted from per-packet and summary RTT when available.

Evidence: `confirmed by source-side behavioral analysis`.

## Medians

### Metrics with median values

These summary duration-stat families expose median when they have samples:

- `stats.rtt`
- `stats.send_delay`
- `stats.receive_delay`
- `stats.ipdv_round_trip`
- `stats.ipdv_send`
- `stats.ipdv_receive`

Evidence: `confirmed by source-side behavioral analysis`.

### Metrics without median values

These summary duration-stat families expose min/mean/max/stddev but do not
expose median in upstream-compatible JSON:

- `stats.send_call`
- `stats.timer_error`
- `stats.server_processing_time`

Human summary output leaves the median column blank for rows without median.

Evidence: `confirmed by source-side behavioral analysis`.

### Exactness and percentiles

Medians are exact finite-run medians over the eligible sample set for that
metric. For an even number of samples, the median is the arithmetic midpoint of
the two center values after sorting.

No rolling medians are exposed. No percentile fields beyond median are exposed.

Evidence: `confirmed by source-side behavioral analysis`.

## JSON / Human Output

JSON duration values are numeric nanoseconds.

### Top-level JSON shape

| Field | Meaning | Evidence |
|---|---|---|
| `version` | Version/build metadata. | `confirmed by source-side behavioral analysis` |
| `system_info` | Runtime/system metadata. | `confirmed by source-side behavioral analysis` |
| `config` | Effective client configuration. | `confirmed by source-side behavioral analysis` |
| `send_err` | Send-side termination error when present. | `confirmed by source-side behavioral analysis` |
| `receive_err` | Receive-side termination error when present. | `confirmed by source-side behavioral analysis` |
| `stats` | Summary stats object. | `confirmed by source-side behavioral analysis` |
| `round_trips` | Per-sequence result array. | `confirmed by source-side behavioral analysis` |

### Summary `stats` fields

| Field | Behavioral meaning | Evidence |
|---|---|---|
| `start_time` | Client start timestamp. | `confirmed by source-side behavioral analysis` |
| `send_call` | Send-call duration stats. No median. | `confirmed by source-side behavioral analysis` |
| `timer_error` | Absolute send timer-error duration stats. No median. | `confirmed by source-side behavioral analysis` |
| `rtt` | Adjusted RTT stats over unique replies. Has median. | `confirmed by source-side behavioral analysis` |
| `send_delay` | Send-side one-way delay stats when wall-clock timestamp data exists. Has median. | `confirmed by source-side behavioral analysis` |
| `receive_delay` | Receive-side one-way delay stats when wall-clock timestamp data exists. Has median. | `confirmed by source-side behavioral analysis` |
| `server_packets_received` | Most recent server received-count value observed by the client. | `confirmed by source-side behavioral analysis` |
| `bytes_sent` | Bytes sent by successful client send attempts. | `confirmed by source-side behavioral analysis` |
| `bytes_received` | Bytes from unique replies only. Duplicates do not add bytes. | `confirmed by source-side behavioral analysis` |
| `duplicates` | Count of accepted duplicate replies. | `confirmed by source-side behavioral analysis` |
| `late_packets` | Count of late unique replies. Late duplicates are excluded. | `confirmed by source-side behavioral analysis` |
| `wait` | Final wait duration for outstanding replies. | `confirmed by source-side behavioral analysis` |
| `duration` | Total client run duration. | `confirmed by source-side behavioral analysis` |
| `packets_sent` | Count of successful client test-packet sends. | `confirmed by source-side behavioral analysis` |
| `packets_received` | Unique replies plus duplicates. | `confirmed by source-side behavioral analysis` |
| `packet_loss_percent` | Loss percent based on `packets_sent - stats.rtt.n`. | `confirmed by source-side behavioral analysis` |
| `upstream_loss_percent` | Upstream loss estimate from observed server received count. | `confirmed by source-side behavioral analysis` |
| `downstream_loss_percent` | Downstream loss estimate from observed server count and client `packets_received`. | `confirmed by source-side behavioral analysis` |
| `duplicate_percent` | `100 * duplicates / packets_received`, or zero when no packets were received. | `confirmed by source-side behavioral analysis` |
| `late_packets_percent` | `100 * late_packets / packets_received`, or zero when no packets were received. | `confirmed by source-side behavioral analysis` |
| `ipdv_send` | Absolute send-IPDV summary stats. Has median. | `confirmed by source-side behavioral analysis` |
| `ipdv_receive` | Absolute receive-IPDV summary stats. Has median. | `confirmed by source-side behavioral analysis` |
| `ipdv_round_trip` | Absolute round-trip-IPDV summary stats. Has median. | `confirmed by source-side behavioral analysis` |
| `server_processing_time` | Server-processing duration stats. No median. | `confirmed by source-side behavioral analysis` |
| `timer_err_percent` | Mean timer error as a percentage of configured interval. | `confirmed by source-side behavioral analysis` |
| `timer_misses` | Expected sends not made according to interval-derived schedule. | `confirmed by source-side behavioral analysis` |
| `timer_miss_percent` | Missed-send percentage against expected sends. | `confirmed by source-side behavioral analysis` |
| `send_rate` | Send bitrate over the send interval. | `confirmed by source-side behavioral analysis` |
| `receive_rate` | Receive bitrate over the unique-reply receive interval. Duplicates do not add bytes or timing. | `confirmed by source-side behavioral analysis` |

### Duration-stat object fields

| Field | Meaning | Evidence |
|---|---|---|
| `total` | Sum of included samples. | `confirmed by source-side behavioral analysis` |
| `n` | Number of included samples. | `confirmed by source-side behavioral analysis` |
| `min` | Minimum included sample. | `confirmed by source-side behavioral analysis` |
| `max` | Maximum included sample. | `confirmed by source-side behavioral analysis` |
| `mean` | Arithmetic mean. | `confirmed by source-side behavioral analysis` |
| `median` | Present only for metric families that compute median and only when at least one sample exists. | `confirmed by source-side behavioral analysis` |
| `stddev` | Sample standard deviation; zero when fewer than two samples exist. | `confirmed by source-side behavioral analysis` |
| `variance` | Variance corresponding to `stddev`, represented in duration-like numeric units for compatibility. | `confirmed by source-side behavioral analysis` |

### `round_trips[]` fields

| Field | Behavioral meaning | Evidence |
|---|---|---|
| `seqno` | Sequence number. | `confirmed by source-side behavioral analysis` |
| `lost` | `false`, `true`, `true_up`, or `true_down`. | `confirmed by source-side behavioral analysis` |
| `timestamps.client.send` | Client send timestamp. | `confirmed by source-side behavioral analysis` |
| `timestamps.client.receive` | Client receive timestamp for unique replies. | `confirmed by source-side behavioral analysis` |
| `timestamps.server.receive` | Server receive timestamp according to timestamp mode. | `confirmed by source-side behavioral analysis` |
| `timestamps.server.send` | Server send timestamp according to timestamp mode. | `confirmed by source-side behavioral analysis` |
| `delay.rtt` | Adjusted RTT for unique replies. Omitted for losses. | `confirmed by source-side behavioral analysis` |
| `delay.send` | Send-side one-way delay when available. | `confirmed by source-side behavioral analysis` |
| `delay.receive` | Receive-side one-way delay when available. | `confirmed by source-side behavioral analysis` |
| `ipdv.rtt` | Signed round-trip IPDV when sequence-adjacent data exists. | `confirmed by source-side behavioral analysis` |
| `ipdv.send` | Signed send IPDV when sequence-adjacent timestamp data exists. | `confirmed by source-side behavioral analysis` |
| `ipdv.receive` | Signed receive IPDV when sequence-adjacent timestamp data exists. | `confirmed by source-side behavioral analysis` |

Duplicate replies do not create additional `round_trips[]` entries and do not
modify an existing entry.

Evidence: `confirmed by source-side behavioral analysis`.

### Human per-packet output

Normal and late unique replies are printed with sequence, RTT, optional
receive/send delay, IPDV or `n/a`, and a late marker for late unique replies.

Duplicate replies are printed as duplicate events with the sequence number.

Quiet mode suppresses normal per-packet output but still prints the final
summary. Really-quiet mode suppresses normal output except errors.

Evidence: `confirmed by source-side behavioral analysis`.

### Human summary output

The human summary includes duration-stat rows when samples exist:

- `RTT`
- `send delay`
- `receive delay`
- `IPDV (jitter)`
- `send IPDV`
- `receive IPDV`
- `send call time`
- `timer error`
- `server proc. time`

The duration table columns are min, mean, median, max, and standard deviation.
Rows without medians leave the median column blank.

The summary counters include:

| Human label | Behavioral meaning |
|---|---|
| `duration` with `wait` | Total run duration and final wait duration. |
| `packets sent/received` | `packets_sent` and `packets_received`; displayed loss uses unique RTT samples. |
| `server packets received` | Server received-count estimate and up/down loss estimates when available. |
| `*** DUPLICATES` | Printed only when duplicates exist. |
| `late (out-of-order) pkts` | Printed only when late unique replies exist. |
| `bytes sent/received` | Sent bytes and unique-reply received bytes. |
| `send/receive rate` | Send bitrate and unique-reply receive bitrate. |
| `packet length` | Negotiated packet length. |
| `timer stats` | Missed sends and timer-error percentage. |

Evidence: `confirmed by source-side behavioral analysis`.

## Notes On Unresolved Or Implementation-Defined Behavior

No core statistics behavior currently required for Milestone 6 is marked
`unresolved` in this document.

The following is `intentionally implementation-defined for irtt-rs`:

- Internal data structures and algorithms used to compute the documented
  behavior.
- Zero-send percentage behavior, unless upstream-compatible behavior is
  explicitly verified for that case.
- Behavior for malformed local state that cannot arise from accepted upstream
  client/server packet exchanges.
- Formatting details outside the documented JSON fields and human summary
  semantics, unless `irtt-rs` explicitly targets byte-for-byte text output.

## Clean-Room Compliance Note

This document describes observable client result behavior, public field names,
public mode names, and public output semantics. It intentionally avoids
upstream source code, source-derived pseudocode, private type or function names,
copied comments, and implementation structure.

Future implementation agents should use this document as the compatibility
target and should not need to read upstream source to implement Milestone 6.

## Optional Local Validation Appendix

No optional local smoke tests were run for this revision.
