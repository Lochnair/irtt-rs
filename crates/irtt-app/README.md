# irtt-rs

The IRTT-compatible client, TUI, and server applications, packaged as Cargo
package `irtt-rs`.

Installing this crate builds:

- `irtt-rs` — a multicall dispatcher binary that runs the client, TUI, or
  server applet by subcommand.
- `irtt-client` — the dedicated client binary (feature `client`).
- `irtt-tui` — the interactive terminal UI (feature `tui`).
- `irtt-server` — the dedicated server binary (feature `server`).

The `client`, `server`, `tui`, and `stats` features are enabled by default, so a
plain `cargo install irtt-rs` installs everything. The TUI requires statistics
and enables `stats` automatically.

For a client without statistics, use
`cargo install irtt-rs --locked --no-default-features --features client --bin irtt-client`.
Use `--features client,stats` for a client-only build with statistics.

## This is an application, not a library

This package's library target is undocumented (`doc = false`) and exists only
to share code between its binaries. If you're looking for the client/session
or server API to embed in your own Rust program, use
[`irtt-client`](https://crates.io/crates/irtt-client) or
[`irtt-server`](https://crates.io/crates/irtt-server) instead.

## Usage

See the [project repository](https://github.com/Lochnair/irtt-rs) for
installation instructions, command-line usage, and configuration.

## Project

Part of [irtt-rs](https://github.com/Lochnair/irtt-rs), an independent Rust
implementation of an IRTT-compatible protocol stack.
