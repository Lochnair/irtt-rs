# irtt-tui

## Name

irtt-tui - IRTT-compatible terminal UI client

## Synopsis

`irtt-tui` [*OPTIONS*] *[LABEL=]TARGET*...

## Description

`irtt-tui` gives you a live graph and status dashboard using the same probing
engine as `irtt-client(1)`. You need at least one target to start it.

Targets and pacing work the same as in `irtt-client(1)`. See that manual
for `[LABEL=]TARGET` syntax, multiple targets, and the
`--pacing staggered|burst` option. The configuration flags
(`--interval`, `--timeouts`, `--length`, `--hmac`, `--clock`, `--tstamp`,
`--stats`, `--sfill`, `--dscp`, `--ttl`, `--loose`) are also shared with
`irtt-client`, so the same descriptions apply.

Address-family controls are shared too: `-4` / `--ipv4` uses IPv4 only,
`-6` / `--ipv6` uses IPv6 only, and `--dual-stack` gives each available family
of a hostname its own measurement row and graph series. These switches are
mutually exclusive. Explicit IP literals remain single targets; a literal
conflicting with `-4` or `-6` is rejected before the dashboard starts.

```sh
irtt-tui -4 uk-cov1.irtt.lochnair.net
irtt-tui --dual-stack cov=uk-cov1.irtt.lochnair.net
```

The second command creates `cov/v4` and/or `cov/v6` according to the system
resolver's initial results, letting you compare the two paths directly.
See the client manual's address-family section for discovery errors and labels.

## Continuous default

The TUI defaults to continuous mode (`--duration 0`) and runs until you
quit. If you're used to `irtt-client`, which defaults to a finite run,
keep that difference in mind. To set a finite run, pass an explicit
`--duration`. Both `--duration` and `--interval` use the
[client duration syntax](irtt-client.md#finite-versus-continuous-operation),
including fractional values (`2.5s`), composite values (`1m30s`), and units
from nanoseconds (`ns`) through hours (`h`):

```sh
irtt-tui <server> --duration 30s
```

In continuous mode, the TUI reconnects after a target fails, the peer closes
the session, or the session ends because the server limited its duration.
After a 1.5-second delay, it groups completed targets into one retry while
healthy sessions keep running. Even if every target fails, the TUI stays
open until you quit.

Explicit stops, removed or replaced targets, and no-test completion do not
trigger retries. Quitting cancels any pending retry work.

With `--dual-stack`, a failed family reconnects independently while its healthy
sibling continues. Reconnect resolves the original hostname again within the
target's family. It does not discover additional families or pin the address
returned by initial discovery.

Reconnecting keeps the target in the same row. Statistics reset when a new
session generation is accepted, including attempts that fail before opening.
The TUI keeps graph history and the age of the last actual reply, with a
gap in the graph between generations.

## Dashboard

The dashboard uses the same layout for any target count and terminal size.
It shows:

- Run status, elapsed time, and finite duration or continuous mode.
- The selected target's label and remote address, plus latest signed
  client-to-server and server-to-client delays (`-` means unavailable).
- Target rows with latest effective RTT, cumulative loss percentage, jitter
  (standard deviation of round-trip IPDV), and age of the last primary reply.
- A graph overlaying all targets, with matching target colors and a visible
  live/history indicator and window size.

An active session shows `waiting` until it gets a primary reply. Check the
reply age to see how fresh the displayed RTT is. The loss estimate comes
from the shared statistics collector, so outstanding sends can temporarily
count towards cumulative loss. Jitter stays `-` until an IPDV pair is
available.

Use `Tab` / `Shift-Tab` to select a target and bring its row into view.
The dashboard shows up to six target rows, with fewer on smaller terminals.
Selecting a target changes the identification, one-way summary, and details.
The graph still compares every target.

Press `d` to open details in the graph's space. You'll find the selected
target's session and negotiation, packet counters, progress, latest timing
sample, full timing statistics, and warnings, along with recent events
from all targets.

Scroll with the arrows or page keys. You can scroll horizontally to read
long lines and timing columns, including labels and remote addresses that
don't fit in the header. Press `d` or `Esc` to return to the graph with its
metric and viewport preserved. You can also use `g` as an alias for `d`.

You need a terminal at least 56 columns wide and 18 rows tall. Below that
size, the TUI asks you to resize while continuing to collect measurements.
Resizing and important status redraws still work when the display is paused.

## Controls

| Key | Action |
| --- | --- |
| `q`, `Ctrl-C` | Quit |
| `p` | Toggle display pause (probing and acquisition continue) |
| `Tab` / `Shift-Tab` | Select next / previous target |
| `d`, `g` | Toggle target details |
| `Esc` | Close target details |
| `r` | Clear graph history for all targets; keep latest samples and statistics |
| `m` | Cycle graph metric (graph visible) |
| `←` / `→` | Pan graph; scroll columns in details |
| `↑` / `↓` | Scroll details |
| `PageUp` / `PageDown` | Page-pan graph; page-scroll details |
| `Home` / `End` | Oldest / live graph; top / bottom of details |
| `+` / `=` | Zoom graph in (graph visible) |
| `-` | Zoom graph out (graph visible) |
| `0` | Reset graph window and zoom, keeping live/history position (graph visible) |

The six graph metrics are effective RTT, raw RTT, adjusted RTT,
client-to-server delay, server-to-client delay, and server processing time.
Signed effective/adjusted RTT and one-way values are preserved. Graph windows
range from 5 seconds to 24 hours and default to one minute. The footer shows
controls appropriate to the currently open graph or details sheet.

## Retained history

Along with the statistics described in `irtt-client(1)`, the TUI keeps
some history for the display:

- Up to 500,000 graph samples per target.
- Up to 80 recent status/log messages.

At 500,000 replies per target, you get approximately 13h53m20s of history
with 100ms intervals, 27h46m40s at 200ms, or 5d18h53m20s at 1s. Each graph
sample stores both clocks, six values ready for graphing, and a flag marking
session boundaries. The latest details keep the full typed timing values
and sequence number.

On 64-bit macOS, each retained sample takes 88 bytes, giving about 42 MiB
per target at the cap. Statistics and temporary rendering buffers use
additional memory. History grows as needed and reserves exactly to the
cap on its final growth step, so you don't pay for the full history of
every target at startup.

Press `r` to clear graph history for all targets and return to the live
view. This leaves the 500,000-sample cap unchanged. Display history is kept
in addition to the client's statistics (see Measurements and memory in
`irtt-client(1)`). During a long continuous TUI session, whichever uses more
memory for your target count will dominate the total.

If you set a finite `--duration`, the TUI retains exact statistics samples
just as `irtt-client(1)` does in finite mode. Memory then grows with the
probe count. The TUI does not print the memory warnings you get from
`irtt-client`, since a stderr warning on the alternate screen could be
invisible or corrupt the display. For a long finite run, plan memory using
the figures in `irtt-client(1)` under Measurements and memory, or use the
continuous default.

## Exit behavior

Quitting with `q` or `Ctrl-C` counts as an interrupted, successful exit.
In continuous mode, the TUI reconnects after peer closure and target failures.
A managed driver failure or an unexpected reconnect-update failure still
ends the run with an error.

Finite runs do not reconnect. They complete after their target sessions end;
a peer closure is a normal session ending, while a run whose targets all fail
exits with an error.

## Examples

```sh
irtt-tui host.example
irtt-tui eu=host.example
irtt-tui eu=host-a.example us=host-b.example --pacing burst
irtt-tui host.example --duration 30s
```

## See also

`irtt-client(1)` for target syntax, pacing, negotiation flags, and
finite/continuous memory behavior; `irtt-server(1)`; `irtt-rs(1)`.
