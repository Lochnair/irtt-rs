# irtt-tui

## NAME

irtt-tui - IRTT-compatible terminal UI client

## SYNOPSIS

`irtt-tui` [*OPTIONS*] *[LABEL=]TARGET*...

## DESCRIPTION

`irtt-tui` is a live dashboard over the same probing engine as
`irtt-client(1)`: a graph and status view instead of a printed event stream.
At least one target is required.

Target syntax, multiple targets, and pacing are identical to
`irtt-client(1)` — see that manual for `[LABEL=]TARGET` syntax and the
`--pacing staggered|burst` option. The full set of negotiation flags
(`--interval`, `--length`, `--hmac`, `--clock`, `--tstamp`, `--stats`,
`--sfill`, `--dscp`, `--ttl`, `--loose`) is shared with `irtt-client` as
well; consult that manual for their meaning.

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

## CONTINUOUS DEFAULT

Unlike `irtt-client`, **the TUI defaults to continuous mode**
(`--duration 0`): it runs until you quit. Pass an explicit `--duration` for
a finite run:

```sh
irtt-tui <server> --duration 30s
```

Continuous runs reconnect after target-local failures, peer closure, or a
session completing because the server restricted its duration. After a
1.5-second delay, one coalesced retry reopens completed targets while healthy
sessions continue undisturbed. Even if every target fails, the TUI stays alive
until you quit. Explicit stops, removed or replaced targets, and no-test
completion do not trigger retries. Quitting cancels pending retry work.

With `--dual-stack`, a failed family reconnects independently while its healthy
sibling continues. Reconnect resolves the original hostname again within the
target's family. It does not discover additional families or pin the address
returned by initial discovery.

Each reconnect keeps the same logical target row. Statistics reset when a new
session generation is accepted, including attempts that fail before opening.
Retained graph history and the age of the last actual reply remain; the graph
leaves a gap between generations instead of connecting their samples.

## DASHBOARD

One dashboard is used for every target count and terminal size. It shows:

- Run status, elapsed time, and finite duration or continuous mode.
- The selected target's label and remote address, plus latest signed
  client-to-server and server-to-client delays (`-` means unavailable).
- Target rows with latest effective RTT, cumulative loss percentage, jitter
  (standard deviation of round-trip IPDV), and age of the last primary reply.
- A graph overlaying all targets, with matching target colors and a visible
  live/history indicator and window size.

An active session with no primary replies shows `waiting`. Reply age makes a
stale RTT distinguishable from a fresh measurement. Loss follows the shared
statistics collector: outstanding sends can temporarily contribute to the
cumulative loss estimate. Jitter is `-` until an IPDV pair is available.

`Tab` / `Shift-Tab` selects a target, bringing its row into view when necessary.
Up to six target rows are shown; smaller terminals show fewer. Selecting a target
changes the identification, one-way summary and details; the graph continues to
compare every target.

`d` opens a details sheet in the graph's space. It contains the selected target's
session and negotiation, packet counters, progress, latest timing sample, full
timing statistics, warnings, and recent events from all targets. Scroll it with
the arrows or page keys. Long lines and timing columns can be read with horizontal
scrolling, including full labels and remote addresses that do not fit in the
header. `d` or `Esc` returns to the graph, preserving its metric and viewport.
The former large/compact Dashboard screens have been replaced by this sheet;
`g` remains an alias for `d`.

The minimum terminal size is 56 columns by 18 rows. Below that size the UI asks
for a resize and continues acquiring measurements. Resize and important status
redraws still work while the display is paused.

## CONTROLS

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

## RETAINED HISTORY

The TUI keeps its own bounded presentation state on top of the statistics
retention described in `irtt-client(1)`:

- Up to 100,000 graph samples per target.
- Up to 80 recent status/log messages.

`r` clears graph history for all targets and returns the viewport to
live; it does not change the 100,000-sample cap itself. These are
presentation bounds on top of the client's own statistics retention (see
`irtt-client(1)` MEASUREMENTS AND MEMORY) — a long continuous TUI session's
total memory is dominated by whichever of the two is larger for your target
count.

A **finite** `--duration` TUI run retains exact statistics samples exactly as
`irtt-client(1)` does in finite mode, so its retained state grows with the
probe count rather than approaching a bound. Unlike `irtt-client`, the TUI
prints **no** memory warning when that estimate gets large: it owns the
alternate screen, and a stderr warning there would be invisible or would
corrupt the display. Size a long finite TUI run from the figures in
`irtt-client(1)` MEASUREMENTS AND MEMORY, or use the continuous default.

## EXIT BEHAVIOR

Quitting with `q` or `Ctrl-C` is an interrupted, successful exit. In continuous
mode, peer closure and target failures trigger reconnect rather than ending the
run. A managed driver failure or an unexpected reconnect-update failure still
ends the TUI with an error.

Finite runs do not reconnect. They complete after their target sessions end;
a peer closure is a normal session ending, while a run whose targets all fail
exits with an error.

## EXAMPLES

```sh
irtt-tui host.example
irtt-tui eu=host.example
irtt-tui eu=host-a.example us=host-b.example --pacing burst
irtt-tui host.example --duration 30s
```

## SEE ALSO

`irtt-client(1)` for target syntax, pacing, negotiation flags, and
finite/continuous memory behavior; `irtt-server(1)`; `irtt-rs(1)`.
