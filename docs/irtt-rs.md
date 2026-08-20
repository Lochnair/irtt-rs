# irtt-rs

## NAME

irtt-rs - IRTT-compatible multi-applet dispatcher

## SYNOPSIS

`irtt-rs` `client` [*OPTIONS*] [*[LABEL=]TARGET*]...

`irtt-rs` `tui` [*OPTIONS*] *[LABEL=]TARGET*...

`irtt-rs` `server` [*OPTIONS*]

## DESCRIPTION

`irtt-rs` is a multicall dispatcher: one binary that runs the client, TUI,
or server applet depending on the subcommand given, or on the name it was
invoked/copied as. It exists for router-friendly single-binary deployment;
it does not add behavior beyond what `irtt-client(1)`, `irtt-tui(1)`, and
`irtt-server(1)` already provide.

Each subcommand behaves exactly like its dedicated binary — `irtt-rs client
...` is equivalent to running `irtt-client ...` directly. See those manuals
for target syntax, options, output, and exit behavior.

## APPLET BINARY NAMES

The build also produces dedicated binaries: `irtt-client`, `irtt-tui`, and
`irtt-server`. A dedicated binary always runs its own role, regardless of
what it is invoked or copied as — it does not consult argv0 dispatch logic.

`irtt-rs` itself inspects argv0 first: if invoked (or symlinked/copied) under
one of the recognized applet names — `irtt-client`, `irtt-tui`, `irttd`, or
`irtt-server` — it runs that applet directly, without needing a subcommand.
Any other name starting with `irtt-` (other than `irtt-rs` itself) is
rejected as an unknown applet name before subcommand parsing is even
attempted — `irtt-custom server` is a hard error, not a fallthrough. Only
`irtt-rs` itself, or a name that doesn't start with `irtt-` at all, falls
through to ordinary `client`/`tui`/`server` subcommand parsing.

## FEATURE-DEPENDENT APPLET AVAILABILITY

Each applet is gated by a Cargo feature on the `irtt-rs` package: `client`,
`tui`, `server`. All three are in the default feature set. A build with a
feature disabled still produces the `irtt-rs` binary, but that applet is
unavailable at runtime — running it (by subcommand or by binary name)
reports which feature to rebuild with, and `irtt-rs --help` lists which
applets the current build actually contains.

```sh
cargo install irtt-rs --locked --no-default-features --features client
```

## RELATIONSHIP TO irtt-client, irtt-tui, irtt-server

`irtt-rs` is packaging and dispatch only. All probing, display, and server
behavior lives in `irtt-client(1)`, `irtt-tui(1)`, and `irtt-server(1)` —
consult those manuals for anything observable about running a probe or
serving one.

## SEE ALSO

`irtt-client(1)`, `irtt-tui(1)`, `irtt-server(1)`.
