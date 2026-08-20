# irtt-server

Reusable, Tokio-native library for running an IRTT-compatible UDP server:
open negotiation, the bounded session table, echo processing, per-session
rate limiting, idle and max-duration expiry, and reply construction.

## Looking for the CLI?

If you just want to run a server from the command line, use the
[`irtt-rs`](https://crates.io/crates/irtt-rs) application package instead,
which installs the `irtt-server` binary. This crate is the library that
binary is built on.

## Structure

- `Server` — the one-listener primitive: one UDP socket, one deterministic
  `ServerCore`, one sequential receive/reply loop.
- `ServerSet` — supervises one or more `Server`s as a single service: binds
  them all or none, runs each in its own task, and fans one shutdown out to
  all of them.

Unlike `irtt-client`, Tokio is not optional here — this crate is intentionally
Tokio-native, with no blocking or alternate-runtime variant.

A wildcard listener (`0.0.0.0` / `[::]`) recovers each request's local
destination and replies from it, so a client on a second local address is
answered from the address it actually contacted. That path is implemented
for Linux, macOS, and FreeBSD; a wildcard bind is refused at construction on
other targets rather than served incorrectly. Explicit-address listeners are
unaffected everywhere.

See `examples/` in the repository for runnable examples.

## Documentation

Full API documentation is on [docs.rs/irtt-server](https://docs.rs/irtt-server).

## Project

Part of [irtt-rs](https://github.com/Lochnair/irtt-rs), an independent Rust
implementation of an IRTT-compatible protocol stack.
