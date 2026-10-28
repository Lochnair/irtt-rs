# irtt-rs

## Name

irtt-rs - IRTT-compatible multi-applet dispatcher

## Synopsis

`irtt-rs` `client` [*OPTIONS*] [*[LABEL=]TARGET*]...

`irtt-rs` `tui` [*OPTIONS*] *[LABEL=]TARGET*...

`irtt-rs` `server` [*OPTIONS*]

## Description

`irtt-rs` lets you run the client, TUI, or server from one binary. You choose
the applet with a subcommand, or by the name you invoke or copy the binary
as. This is useful on a router where you want a single binary to deploy.
The applets behave the same as `irtt-client(1)`, `irtt-tui(1)`, and
`irtt-server(1)`.

For example, `irtt-rs client ...` does the same thing as running `irtt-client ...` directly. See the
individual manuals for target syntax, options, output, and exit behavior.

## Applet binary names

The build also produces dedicated `irtt-client`, `irtt-tui`, and
`irtt-server` binaries. These always run their own applet, even if you copy
or invoke them under a different name. They do not use argv0 to choose an
applet.

The `irtt-rs` dispatcher checks argv0 first. If you invoke it through a
symlink or copy named `irtt-client`, `irtt-tui`, or `irtt-server`, it runs
that applet without a subcommand.

Other names starting with `irtt-`, except `irtt-rs` itself, are rejected
before subcommands are parsed. So `irtt-custom server` fails with an unknown
applet error. If the name is `irtt-rs`, or doesn't start with `irtt-`, the
dispatcher parses the usual `client`/`tui`/`server` subcommands.

## Feature-dependent applet availability

Each applet has a Cargo feature on the `irtt-rs` package: `client`,
`tui`, `server`. All three are enabled by default. If you disable one, the
build still produces `irtt-rs`, but you cannot run that applet. Trying it
through either a subcommand or a binary name tells you which feature to
rebuild with. Use `irtt-rs --help` to see which applets your build includes.

```sh
cargo install irtt-rs --locked --no-default-features --features client,stats
```

Use `--features client` to omit statistics. The default build includes the
`stats` feature, and `tui` always enables it.

## Relationship to irtt-client, irtt-tui, irtt-server

The `irtt-rs` dispatcher handles packaging and applet selection. For how to run
probes, use the dashboard, or serve requests, see `irtt-client(1)`,
`irtt-tui(1)`, and `irtt-server(1)`.

## See also

`irtt-client(1)`, `irtt-tui(1)`, `irtt-server(1)`.
