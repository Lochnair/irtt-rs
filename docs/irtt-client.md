# irtt-client

## NAME

irtt-client - IRTT-compatible stream client

## SYNOPSIS

`irtt-client` [*OPTIONS*] [*[LABEL=]TARGET[@hmac=KEY]*]...

`irtt-client` `--list-columns`

## DESCRIPTION

`irtt-client` probes one or more IRTT-compatible servers over UDP and prints
per-probe measurement events as they arrive, plus a final summary for
eligible runs.

## TARGETS

Every target argument accepts `[LABEL=]TARGET[@hmac=KEY]`. An optional
`LABEL=` prefix assigns the logical target name used in output; without it,
the target string itself is used as the label. Labeled and unlabeled targets
can be freely mixed in one argument list. Prefer explicit labels for
long-running dynamic target sets: the label is the stable target identity
across replacements.

```sh
irtt-client host-a:2112
irtt-client eu=host-a:2112 us=host-b:2112
irtt-client host-a:2112 eu=host-b:2112
irtt-client --hmac default-secret eu=host-a:2112@hmac=eu-secret public=host-b:2112@hmac=
```

Targets without `@hmac=` inherit the global `--hmac` value. `@hmac=KEY`
overrides it for that target, and `@hmac=` explicitly disables HMAC for that
target. The modifier uses `@`, not a shell command separator; quote an argument
if its key needs shell quoting. A literal `@` immediately before `hmac=` in
the target portion is written as `\@`.

Multiple targets are probed concurrently. By default, a multi-target session
uses **staggered** pacing, spacing active targets across the probe interval;
pass `--pacing burst` to send one probe to every active target back-to-back
once per interval instead. Final per-target summaries print in the order
targets were supplied on the command line, not the order in which they
finish or their labels sort alphabetically.

## DYNAMIC TARGET SETS FROM STANDARD INPUT

`--targets-stdin` is available only with continuous mode (`--duration 0`).
Positional targets remain an initial desired set, and may be omitted to
start empty:

~~~sh
irtt-client --duration 0 --targets-stdin
irtt-client --duration 0 --targets-stdin ams=ams.example:2112
~~~

Each non-empty stdin record declares the complete desired target set, rather
than a delta. A later record replaces an earlier set: targets absent from it
are retired, unchanged target configurations retain their generation, and a
changed address or HMAC setting creates a fresh generation. Under transient
live-generation backpressure, only the latest unapplied desired set is kept.

Commas frame targets in stdin records; escape a literal comma within one
target as `\,`. Target parsing inside each element is otherwise the same as
for positional arguments, including `@hmac=KEY` and `@hmac=`:

~~~sh
printf '%s\n' \
  'ams=ams.example:2112,sg=sg.example:2112@hmac=sg-secret' \
  'sg=sg.example:2112@hmac=sg-secret,nyc=nyc.example:2112' |
  irtt-client --duration 0 --targets-stdin --hmac default-secret
~~~

The exact record `[]` selects an empty desired set. Empty records are
ignored; whitespace is otherwise payload, not trimming. EOF requests a
graceful stop. The stdin-controlled mode bounds each desired set to 128
targets and retains at most 256 live target generations while replacements
drain.

## FINITE VERSUS CONTINUOUS OPERATION

`--duration` controls the run length:

- Any nonzero duration (default `10s`) is a **finite** run: probing stops
  after that long, and a final summary is always printed for table output
  (see [OUTPUT](#output) below — CSV, TSV, and JSON Lines never print one).
- `--duration 0` is **continuous**: the client runs until interrupted
  (`Ctrl-C`). For table output, a final summary is printed only if the run
  was interrupted; an uninterrupted continuous run (one that exits for
  another reason) does not get one.

See [MEASUREMENTS AND MEMORY](#measurements-and-memory) below for why this
distinction also determines what statistics can be computed and how much
memory a long run retains.

## OUTPUT

### `--format FORMAT`

One of four event-row formats, default `table`:

- `table`: human-readable terminal output; the only format that prints a
  final summary.
- `csv`: comma-separated output.
- `tsv`: tab-separated output.
- `jsonl`: one JSON object per event.

CSV, TSV, and JSON Lines default to **all** columns for structured export;
`table` defaults to a compact column set and hides `echo_sent` rows. An
explicit `--columns` selection other than `default` shows all event rows,
including `echo_sent`; `--columns default` is equivalent to omitting the
option and keeps `echo_sent` hidden for table output.

### `--columns COLUMNS` / `-c COLUMNS`

Comma-separated event row columns, or the special values `default` (the
format's default set) or `all` (every column). List every available column,
its meaning, and its aliases with:

```sh
irtt-client --list-columns
```

(`--list-columns` does not require a target.)

### `--header auto|always|never`

Header policy for `table`, `csv`, and `tsv` output (default `auto`). JSON
Lines never prints a header line.

### `--verbose`

Includes extra fields in table output and final summaries. Has no effect on
CSV, TSV, or JSON Lines output either way — those formats show their default
all-column set or an explicit `--columns` selection exactly as given,
regardless of `--verbose`.

### Examples

```sh
irtt-client <server> --format table
irtt-client <server> --format jsonl
irtt-client <server> --format csv --columns event,seq,remote,effective_rtt_us
```

A stream containing only effective RTT values in microseconds:

```sh
irtt-client <server> --format tsv --columns effective_rtt_us --header never
```

## MEASUREMENT FIELDS

Useful measurement columns:

- `raw_rtt_us`: client-observed send-to-receive RTT. Always userspace-timed
  and unaffected by kernel timestamp preference (below).
- `adjusted_rtt_us`: RTT adjusted for server processing when available. Can
  be negative when server processing exceeds the measured raw RTT.
- `effective_rtt_us`: `adjusted_rtt_us` when available, otherwise
  `raw_rtt_us`.
- `rd_us` / `sd_us` (selectable with the longer aliases `receive_delay_us` /
  `send_delay_us`, but always emitted under the short names): one-way delay
  estimates. Can be negative because of clock skew between client and
  server.
- `ipdv_us`: inter-packet delay variation.
- `server_processing_us`: time spent processing the packet at the server.
- `kernel_rx_ns`: kernel receive timestamp, in Unix nanoseconds, when
  captured (see below).

### Kernel timestamps (Linux)

On Linux, one-way delay estimates prefer a kernel-captured timestamp over
userspace timing for this client's own send/receive endpoint, where the
platform and socket support it, reducing the effect of scheduling and
queuing delay. The peer's reported endpoint gets the same preference when it
is the `irtt-server` applet running on Linux; against another
IRTT-compatible server, whatever wall-clock value it reports is used as-is.
This is best-effort: an unsupported or implausible kernel timestamp falls
back to userspace timing. `raw_rtt_us` is unaffected either way;
`adjusted_rtt_us`/`effective_rtt_us` can still shift slightly, since server
processing time is derived from the same server receive endpoint.

## MEASUREMENTS AND MEMORY

Finite and continuous modes make a different tradeoff between exact
statistics and bounded memory.

**Finite mode** retains exact timing samples (and adjacent-sequence IPDV
state) so the final summary can report exact medians. Retained memory
therefore grows with the number of probes sent, at a small constant cost per
probe (on the order of a few hundred bytes of retained sample state per
probe per target, before allocator overhead) — a very long finite run at a
short interval is the case to plan capacity for. The client warns on stderr
when its own estimate of this retained state crosses roughly 128 MiB, and
again more strongly past roughly 512 MiB and 1 GiB; treat those numbers as
practical guidance, not a promised RSS ceiling, since allocator and OS
overhead sit outside the estimate.

**Continuous mode** (`--duration 0`) does not retain a growing sample
history: running statistics keep count/min/max/mean/variance only, so exact
medians are unavailable. It still keeps a bounded adjacent-sequence IPDV
store, capped at 4,096 sequences per target, so nearby replies can still
form IPDV pairs. Retained statistics state therefore approaches a fixed
per-target bound rather than growing with elapsed run time.

Reply classification (independent of the statistics choice above) is also
bounded per target: each of the pending, timed-out, and completed/duplicate
sequence stores has its own 4,096-entry limit — not one shared total across
the three. A full pending store makes the client fail and drain that target
as resource-exhausted rather than wait for capacity to free. Timed-out and
completed state evict their oldest entries, so a sufficiently old late reply
can still be seen and counted, but no longer has retained send state for
measurements such as RTT.

Overall, retained state scales with the **number of configured targets**
and these fixed per-target limits, not with total probes sent in continuous
mode. Output is written as events arrive rather than accumulated; the
managed event stream feeding it is itself bounded and lossy (16,384 events),
so a sufficiently slow consumer can miss events, in which case the client
reports that resulting output/statistics may be incomplete.

For capacity planning on constrained devices (for example an OpenWrt
router): a finite run's statistics memory is roughly proportional to
`targets × probes_sent`, while a continuous run's statistics memory
approaches a fixed multiple of `targets` regardless of how long it runs.

## PEER CLOSE AND EXIT BEHAVIOR

A peer closing a finite run is accepted as a terminal, successful outcome.
In continuous mode, an uninterrupted peer closure is treated as an error —
the client exits nonzero — so a process supervisor can restart it; an
interrupted run (`Ctrl-C`) never reports peer closure as an error, whichever
mode it was in.

With `--targets-stdin`, peer closure is target-local: other desired targets
and the stdin controller continue running. Including that target in a later
complete set starts a fresh generation.

## EXIT STATUS

`0` on success (including a finite run's ordinary completion, and an
interrupted run of either mode). Nonzero if the managed driver failed, if an
uninterrupted continuous run ended by peer closure, or if the run was not
interrupted and no target completed successfully. An interrupted run never
fails on the last of these conditions — for example, interrupting a
continuous run whose only target never opened still exits `0`.

## SERVER FILL REQUEST

`--sfill DESCRIPTOR` requests the server's echo-reply payload fill: `none`
(zero-filled), `rand` (random bytes), or `pattern:HEX` (a repeating
hexadecimal pattern). See `irtt-server(1)` SERVER FILL for what the server
does with an unset or unparseable request.

## EXAMPLES

```sh
irtt-client netperf-eu.bufferbloat.net:2112
irtt-client netperf-eu.bufferbloat.net:2112 --duration 30s --interval 100ms
irtt-client netperf-eu.bufferbloat.net:2112 --duration 0
irtt-client host-a:2112 host-b:2112 --pacing burst
```

## SEE ALSO

`irtt-tui(1)`, `irtt-server(1)`, `irtt-rs(1)`, and `irtt-client --help` for
the full option list, including HMAC, clock/timestamp negotiation, DSCP, and
TTL flags.
