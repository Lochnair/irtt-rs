# irtt-client

## Name

irtt-client - IRTT-compatible stream client

## Synopsis

`irtt-client` [*OPTIONS*] [*[LABEL=]TARGET[@hmac=KEY]*]...

`irtt-client` `--list-columns`

## Description

Use `irtt-client` to probe one or more IRTT-compatible servers over UDP.
It prints measurement events as they arrive, and a final summary when the
run qualifies for one.

## Targets

Write targets as `[LABEL=]TARGET[@hmac=KEY]`. You can add a `LABEL=` prefix
to choose the name shown in output. If you leave it out, the client uses
the target string as its label. You can mix labeled and unlabeled targets
in the same command.

If you're changing targets during a long run, give them explicit labels.
The label identifies the same logical target when you replace its
configuration.

```sh
irtt-client host-a:2112
irtt-client eu=host-a:2112 us=host-b:2112
irtt-client host-a:2112 eu=host-b:2112
irtt-client --hmac default-secret eu=host-a:2112@hmac=eu-secret public=host-b:2112@hmac=
```

Targets without `@hmac=` inherit the global `--hmac` value. `@hmac=KEY`
overrides it for that target, and `@hmac=` explicitly disables HMAC for that
target. The modifier uses `@`, not a shell command separator; quote an argument
if its key needs shell quoting.

Both the global `--hmac KEY` and per-target `@hmac=KEY` accept plain-text
keys or hexadecimal keys prefixed with `0x`. For example, `secret` and
`0x736563726574` are equivalent.

Target syntax supports these backslash escapes in labels, addresses, and HMAC
values: `\\` for a literal backslash, `\@` for a literal `@`, `\=` for a
literal `=`, and `\;` for a literal semicolon. Escaping `@` is necessary in a
label or address when it would otherwise begin `@hmac=`; escaping the first
`=` lets an unlabeled target or a label contain that character. An `=` in
an HMAC value does not need escaping. Positional target arguments may
contain a raw comma; stdin target sets have the additional `\,` escape
described below.

Shells process unquoted backslashes and characters such as semicolons before
the client sees them. Single-quote a target when it contains parser escapes,
for example `'west\=edge=host\@hmac=literal@hmac=a\;b\\c'`, so the
backslashes reach `irtt-client` unchanged.

The client probes multiple targets concurrently. By default it staggers
them across the probe interval. If you want to send one probe to every
active target back-to-back once per interval, use `--pacing burst`.
Final summaries follow the command-line target order, regardless of which
target finished first or how their labels sort.

## Address family

By default, each target uses the first address returned by the system resolver,
whether IPv4 or IPv6. Use `-4` / `--ipv4` to select IPv4 only, or
`-6` / `--ipv6` to select IPv6 only. Hostnames are resolved when each session
connects; these flags do not perform an extra discovery lookup. An explicit
IP literal from the other family is rejected during preparation.

Use `--dual-stack` to compare IPv4 and IPv6 paths as separate measurements:

```sh
irtt-client -6 host.example
irtt-client --dual-stack cov=uk-cov1.irtt.lochnair.net
```

For each hostname, the client discovers which address families the system
resolver currently returns. IPv4 results create `cov/v4`, IPv6 results create
`cov/v6`, and both create both targets, in that order. Without an alias,
`host.example` becomes `host.example/v4` and/or `host.example/v6`. Multiple
addresses within one family still produce only one target. Explicit IP literals
keep their original label and remain single targets.

Each target has its own session, statistics, and failures. Its original hostname
is retained: every new session resolves it again and selects the first address
matching its family. Families are discovered only when a declaration is added,
so a newly appearing family does not create another target automatically.
Discovery failure or a successful lookup returning no addresses rejects the
whole target set. A missing family is normal and simply creates no target for
that family. Final labels must be unique, including generated `/v4` and `/v6`
labels; choose another alias if one collides with an explicit label.

`-4`, `-6`, and `--dual-stack` are mutually exclusive. Expanded targets use
ordinary multi-target pacing and each sends its own probes, so dual-stack can
double the number of probes per interval compared with one target per hostname.

## Local socket configuration

If you need to control where probes leave the machine, use these socket
options:

- `--bind-addr IP[:PORT]` binds sockets to a local IP address and optional
  port. Leave out the port or use `0` to let the OS assign an ephemeral port.
  IPv6 addresses with a port need `[ADDR]:PORT` syntax. The address family
  must match the target's resolved address family.
- `--bind-device DEVICE` (Linux, Android, Fuchsia) binds sockets to a
  network interface using `SO_BINDTODEVICE`.
- `--mark MARK` (Linux, Android, Fuchsia) sets the socket's firewall mark
  using `SO_MARK`, so policy routing or firewall rules can select the
  traffic. It accepts a decimal or 0x-prefixed hexadecimal unsigned 32-bit
  integer.
- `--fib FIB` (FreeBSD) selects a routing table using `SO_SETFIB`. Supply
  the FIB as a decimal unsigned 32-bit integer.

For example, to bind IPv4 probes to a specific source address and interface
with a firewall mark:

```sh
irtt-client -4 --bind-addr 192.0.2.10 --bind-device wan0 --mark 1234 host.example
```

Setting a firewall mark or binding to a network device may require elevated
privileges or appropriate capabilities. If the OS rejects a socket option,
socket setup fails.

A fixed local port can cause bind conflicts when the client creates sockets
for multiple targets. Unless you need a particular source port, let the OS
assign one. A bind address for one family also cannot be used with targets
in the other family.

## Dynamic target sets from standard input

`--targets-stdin` only works in continuous mode (`--continuous` or
`--duration 0`). Use it to change targets while the client is running.
Positional targets form the initial set. You can leave them out to start
with none:

~~~sh
irtt-client --duration 0 --targets-stdin
irtt-client --duration 0 --targets-stdin ams=ams.example:2112
~~~

Each non-empty stdin record gives the complete set of targets you want.
A new record replaces the previous set: the client retires targets you leave
out, keeps the generation for unchanged configurations, and starts a fresh
generation when an address or HMAC setting changes. If live-generation
capacity temporarily prevents an update, only the latest unapplied set is
kept.

The same address-family preparation applies to initial and stdin targets.
With `--dual-stack`, unchanged labels and endpoints retain their discovered
families across records, including HMAC-only changes. Removing a declaration
and later adding it again performs fresh discovery. New declarations are
prepared asynchronously while existing measurements continue. A preparation
error stops the stream gracefully without applying a partial set.

Separate targets in stdin records with commas, and escape a comma inside a
target as `\,`. Each target otherwise uses the same syntax as positional
arguments, including the escapes above, `@hmac=KEY`, and `@hmac=`.

Keep the producer or pipe/FIFO writer open while controlling the client.
For example, run this in one terminal:

~~~sh
mkfifo /tmp/irtt-targets
irtt-client --duration 0 --targets-stdin --hmac default-secret \
  < /tmp/irtt-targets
~~~

Then, in another terminal, open the FIFO once and keep that descriptor open
while sending complete target sets:

~~~sh
exec 3> /tmp/irtt-targets
printf '%s\n' 'ams=ams.example:2112,sg=sg.example:2112@hmac=sg-secret' >&3
printf '%s\n' 'sg=sg.example:2112@hmac=sg-secret,nyc=nyc.example:2112' >&3
# Publish more sets as needed, then send EOF and stop the client:
exec 3>&-
~~~

Send exactly `[]` to select an empty target set. The client ignores empty
records, but otherwise treats whitespace as part of the input without
trimming it.

Closing stdin sends EOF and asks the client to stop gracefully. An update
that has been read but not yet acknowledged as applied may be discarded
at that point. EOF does not wait for an update blocked by capacity to
become applicable.

The maximum stdin record payload is 64 KiB. Stdin-controlled mode bounds each
desired set to 128 targets and retains at most 256 live target generations
while replacements drain. The 128-target limit applies after dual-stack
expansion: a hostname producing both families consumes two target slots.

## Negotiation only

`-n` / `--no-test` connects to each target, negotiates the requested parameters,
and exits without sending measurement probes. For example:

```sh
irtt-client --no-test host.example:2112 --duration 30s --interval 100ms
```

On successful negotiation, the `no_test_completed` event includes the
negotiated duration, interval, and packet length. Output format and column
selection work as usual. The client prints no measurement summary or
statistics-memory warning. Negotiation failures use the normal target
diagnostics and exit policy: an uninterrupted run exits nonzero if no
target succeeds.

Duration options still specify the duration to negotiate; even with
`--continuous` or `--duration 0`, the client exits after negotiation.
`--no-test` cannot be combined with `--targets-stdin`.

## Finite versus continuous operation

`--duration` and `--interval` accept fractional and composite durations, such as
`2.5s`, `1m30s`, and `1h`, as well as `ms`, `us` (or `µs`), and `ns` units
(e.g. `500us` or `100ns`). A nonzero value requires a unit. The interval must
be greater than zero; a zero test duration, including `0s`, selects continuous
mode.

`--timeouts 1s,2s,4s,8s` sets the session-opening retry schedule. Each
comma-separated duration is the receive timeout for one open attempt; attempts
stop when a valid open reply arrives. The list must be nonempty, with each
value at least `200ms`, and uses the same duration syntax as `--interval`.
Omitting the option preserves the library default schedule.

`--duration` controls the run length:

- A nonzero duration (default `10s`) gives you a finite run. Probing stops
  after that long. If statistics are enabled and you're using table
  [output](#output), the client prints a final summary. CSV, TSV, and
  JSON Lines never include a summary.
- With `--continuous` (or `--duration 0`), the client runs until interrupted
  (`Ctrl-C`). With statistics and table output enabled, interrupting the run
  prints a final summary. In stdin-controlled mode, EOF also stops the run
  gracefully and prints the retained summary. A continuous run that exits
  for another reason without being interrupted does not print one.

The choice also affects which statistics are available and how much memory
a long run retains. See [Measurements and memory](#measurements-and-memory).

### Optional recovery and event-only runs

If you only need individual measurements, use `--no-stats` to disable
statistics collection and final summaries. Columns that need statistics,
such as IPDV, are unavailable. The runtime flag still leaves the
`irtt-stats` dependency in the build.

To omit statistics at compile time, build with:

```sh
cargo build --release -p irtt-rs --bin irtt-client --no-default-features --features client
```

Use `--features client,stats` to include statistics in a client-only build.
Default builds include statistics, and `tui` always enables them. Without
compiled statistics, `--no-stats` remains accepted, summaries and statistics
warnings are disabled, and IPDV columns retain their names with absent values
(`-` in tables, empty CSV/TSV fields, and JSON `null`). Measurement events and
protocol-level received statistics remain available.

Use `--reconnect` to recover individual targets in continuous mode
(`--continuous` or `--duration 0`). The client retries failed connections,
closed sessions, and unresponsive servers while the other targets keep
running. By default, the delay doubles after each failed attempt, from
250 ms up to 10 seconds. A valid reply resets it to the initial delay.
Each target backs off independently.

You can change the delays with `--reconnect-initial-delay DURATION`
(default: `250ms`) and `--reconnect-max-delay DURATION` (default: `10s`).
Both require `--reconnect` and must be positive and schedulable. The maximum
must be at least twice the initial delay. For example,
`--reconnect-initial-delay 1s --reconnect-max-delay 5s` retries after `1s`,
`2s`, `4s`, `5s`, then `5s` from there on.

`--reconnect-timeout DURATION` sets how long a session can go without valid
replies before the client tries to reconnect (default: `10s`). It only
applies while actively sending probes, so allow for your probe interval and
expected packet loss.

Reconnecting resets statistics for that target, so its final summary only
covers the latest session. The client warns about this when reconnection
and statistics collection are both enabled. Use `--no-stats` to disable
collection and suppress the warning.

For continuous CSV output from multiple targets without statistics:

```sh
irtt-client --continuous --reconnect --no-stats \
  --reconnect-timeout 10s \
  --reconnect-initial-delay 250ms --reconnect-max-delay 10s \
  --format csv --header never \
  --columns client_receive_wall_ns,target,seq,rd_us,sd_us \
  reflector1 reflector2
```

## Output

### `--format FORMAT`

Choose one of four event-row formats. The default is `table`:

- `table`: human-readable terminal output; the only format that prints a
  final summary.
- `csv`: comma-separated output.
- `tsv`: tab-separated output.
- `jsonl`: one JSON object per event.

CSV, TSV, and JSON Lines include all columns by default for structured export;
`table` defaults to a compact column set and hides `echo_sent` rows. An
explicit `--columns` selection other than `default` shows all event rows,
including `echo_sent`; `--columns default` is equivalent to omitting the
option and keeps `echo_sent` hidden for table output.

### `--columns COLUMNS` / `-c COLUMNS`

Choose event-row columns with a comma-separated list, or use `default`
for the format's default set and `all` for every column. To see the
available columns, their meanings, and aliases, run:

```sh
irtt-client --list-columns
```

You don't need a target for `--list-columns`.

### `--header auto|always|never`

Header policy for `table`, `csv`, and `tsv` output (default `auto`). JSON
Lines never prints a header line.

### `--verbose`

Include extra fields in table output and final summaries. CSV, TSV, and
JSON Lines keep their default all-column set or your explicit `--columns`
selection regardless of `--verbose`.

### Examples

```sh
irtt-client <server> --format table
irtt-client <server> --format jsonl
irtt-client <server> --format csv --columns event,seq,remote,effective_rtt_us
```

To print only effective RTT values in microseconds:

```sh
irtt-client <server> --format tsv --columns effective_rtt_us --header never
```

## Measurement fields

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

On Linux, the client prefers kernel timestamps for its own send/receive
endpoint when calculating one-way delays, where the platform and socket
support them. This reduces the effect of scheduling and queuing delay.
The `irtt-server` applet makes the same choice for its endpoint on Linux.
With another IRTT-compatible server, the client uses the wall-clock value
that server reports as-is.

If a kernel timestamp is unsupported or implausible, timing falls back to
userspace. This leaves `raw_rtt_us` unchanged. The
`adjusted_rtt_us`/`effective_rtt_us` values can still shift slightly, since
server processing time uses the same server receive endpoint.

## Measurements and memory

If the `stats` feature is compiled in and you haven't used `--no-stats`,
the client collects statistics as described here. Disabling collection
allocates no statistics collectors, though the client still keeps its
bounded reply-classification state.

In finite mode, the client keeps exact timing samples and adjacent-sequence
IPDV state so it can report exact medians in the final summary. Memory
grows with the number of probes sent, at roughly a few hundred bytes per
probe per target before allocator overhead. So if you're planning a long
finite run with a short interval, allow for that growth.

The client prints a warning on stderr when its estimate of retained state
crosses roughly 128 MiB, with stronger warnings past roughly 512 MiB and
1 GiB. These are useful planning figures, but they don't promise an RSS
ceiling: allocator and OS overhead are outside the estimate.

In continuous mode (`--duration 0`), running statistics keep only
count/min/max/mean/variance, so exact medians are unavailable. The client
still keeps up to 4,096 sequences per target for adjacent-sequence IPDV
pairs. Statistics memory approaches a fixed limit per target as the run
continues.

Reply classification has its own bounds, regardless of whether you collect
statistics. Each target has separate pending, timed-out, and
completed/duplicate sequence stores, each with a 4,096-entry limit. If the
pending store fills, the client fails and drains that target as
resource-exhausted. It does not wait for space to free up.

The timed-out and completed stores evict their oldest entries. A very late
reply can still be seen and counted after eviction, but without its retained
send state the client cannot calculate measurements such as RTT.

In continuous mode, retained state scales with the number of configured
targets and these fixed limits per target. The client writes output as
events arrive. The managed event stream has a 16,384-event capacity and can
drop events if the consumer falls behind. If that happens, the client
reports that output and statistics may be incomplete.

For capacity planning on constrained devices (for example an OpenWrt
router): a finite run's statistics memory is roughly proportional to
`targets × probes_sent`, while a continuous run's statistics memory
approaches a fixed multiple of `targets` regardless of how long it runs.

## Peer close and exit behavior

If a peer closes a finite run, the client treats that as successful
completion. In continuous mode without `--reconnect`, a peer closure ends
an uninterrupted run with a nonzero exit status so a process supervisor
can restart it. If you interrupted the run (`Ctrl-C`), peer closure is
never reported as an error in either mode.

With `--reconnect`, a desired target reopens automatically after peer closure.
Without recovery, `--targets-stdin` makes peer closure target-local: other
desired targets and the stdin controller continue running. Including that
target in a later complete set starts a fresh generation.

## Exit status

The client exits `0` on success, including normal finite completion and
interrupted runs in either mode. It exits nonzero if the managed driver
fails, an uninterrupted continuous run ends by peer closure, or no target
completes successfully in an uninterrupted run.

Interrupting the run prevents the last condition from counting as a failure.
For example, if you interrupt a continuous run whose only target never
opened, it still exits `0`.

## Server fill request

`--sfill DESCRIPTOR` requests the server's echo-reply payload fill: `none`
(zero-filled), `rand` (random bytes), or `pattern:HEX` (a repeating
hexadecimal pattern). See the Server fill section in `irtt-server(1)` for
what the server does with an unset or unparseable request.

## Examples

```sh
irtt-client netperf-eu.bufferbloat.net:2112
irtt-client netperf-eu.bufferbloat.net:2112 --duration 30s --interval 100ms
irtt-client netperf-eu.bufferbloat.net:2112 --duration 0
irtt-client host-a:2112 host-b:2112 --pacing burst
```

## See also

`irtt-tui(1)`, `irtt-server(1)`, `irtt-rs(1)`, and `irtt-client --help` for
the full option list, including HMAC, clock/timestamp negotiation, DSCP, and
TTL flags.
