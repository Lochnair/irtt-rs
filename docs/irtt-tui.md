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

## CONTINUOUS DEFAULT

Unlike `irtt-client`, **the TUI defaults to continuous mode**
(`--duration 0`): it runs until you quit. Pass an explicit `--duration` for
a finite run:

```sh
irtt-tui <server> --duration 30s
```

## CONTROLS

| Key | Action |
| --- | --- |
| `q`, `Ctrl-C` | Quit |
| `p` | Toggle display pause (probing continues; only redraws stop) |
| `g` | Toggle Graph / Dashboard view |
| `m` | Cycle graph metric |
| `r` | Clear visible graph history |
| `←` / `→` | Pan graph |
| `PageUp` / `PageDown` | Page-pan graph |
| `Home` / `End` | Jump to oldest / jump to live |
| `+` / `=` | Zoom graph in |
| `-` | Zoom graph out |
| `0` | Reset graph window and zoom |

## RETAINED HISTORY

The TUI keeps its own bounded presentation state on top of the statistics
retention described in `irtt-client(1)`:

- Up to 100,000 graph samples per target.
- Up to 80 recent status/log messages.

`r` clears the currently visible graph history and returns the viewport to
live; it does not change the 100,000-sample cap itself. These are
presentation bounds on top of the client's own statistics retention (see
`irtt-client(1)` MEASUREMENTS AND MEMORY) — a long continuous TUI session's
total memory is dominated by whichever of the two is larger for your target
count.

## EXIT BEHAVIOR

Quitting with `q` or `Ctrl-C` is an interrupted, successful exit. Otherwise,
exit status follows the same peer-close and driver-failure rules as
`irtt-client(1)`.

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
