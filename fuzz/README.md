# `irtt-proto` fuzzing

Coverage-guided fuzzing of `irtt-proto`'s wire decoders via
[`cargo-fuzz`](https://github.com/rust-fuzz/cargo-fuzz)/libFuzzer.

This project is intentionally isolated from the normal stable/MSRV
workspace: it lives in its own `[workspace]` under `fuzz/`, is never a member
of the root workspace, requires nightly Rust, and is not built by
`cargo check`/`test`/`clippy`/`doc` at the workspace root.

## Prerequisites

- nightly Rust (`rustup toolchain install nightly`)
- `cargo-fuzz` (`cargo install cargo-fuzz --locked --version 0.13.1`)

## Targets

| Target              | Exercises                                                        |
| ------------------- | ----------------------------------------------------------------- |
| `decode_request`    | `decode_request`, `Params::decode` on a decoded Open, `verify_packet_hmac` |
| `decode_open_reply` | `decode_open_reply`, unauthenticated and HMAC-authenticated       |
| `decode_echo_reply` | `decode_echo_reply` against a deterministic, bounded `Params` matrix, unauthenticated and HMAC-authenticated |
| `decode_params`     | `Params::decode` and `Params::decode_with_presence`, plus an encode/decode round trip |
| `decode_varint`     | `varint::decode_uvarint`/`decode_varint` and their round trip through `encode_uvarint`/`encode_varint` |

The invariant every target asserts is: **arbitrary bytes must never panic**.
Returning `Err` is normal and expected; a successful decode is also normal.

Every target bounds its input to 128 KiB (`MAX_INPUT_LEN` in each harness) so
libFuzzer spends its time exploring protocol structure rather than
allocating/copying megabytes — production IRTT datagrams are UDP-sized, well
under this bound.

## Running

```sh
cargo +nightly fuzz run decode_request
```

Bounded smoke run, e.g. 30 seconds:

```sh
cargo +nightly fuzz run decode_request -- -max_total_time=30
```

Build every target without running:

```sh
cargo +nightly fuzz build
```

## Corpus

Seed corpora live under `fuzz/corpus/<target>/`. They were generated once
from `irtt-proto`'s own public encoders (`encode_request`, `encode_open_reply`,
`encode_echo_reply`, `Params::encode`) by a throwaway local script, not copied
from any external source, and not derived from upstream `heistp/irtt`. Each
file is a small, representative wire packet (minimal Open, Close, Echo, HMAC
variants, ordinary/recv-count/recv-window/midpoint Echo replies, and a few
`Params` payloads including an unknown tag and `server_fill`).

## Crash artifacts

A failing case is written under `fuzz/artifacts/<target>/`. To replay it:

```sh
cargo +nightly fuzz run decode_request fuzz/artifacts/decode_request/<crash-file>
```

## Isolation notes

- `fuzz/Cargo.toml` declares its own `[workspace]`, so it is never picked up
  by the root `[workspace]` in `../Cargo.toml`.
- `libfuzzer-sys` and `arbitrary` are fuzz-only dependencies; they never
  appear in any published crate's dependency graph.
- `fuzz/Cargo.lock` is committed for reproducible fuzzing builds, independent
  of the root `Cargo.lock`.
