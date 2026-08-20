# irtt-proto

Low-level wire encoding and decoding for the IRTT-compatible protocol: request
and reply structs, flags, negotiable parameters, varint/zigzag primitives, and
HMAC-MD5 packet authentication. No sockets, no runtime, no session state.

## Who should use this

Most applications should not depend on this crate directly. Use
[`irtt-client`](https://docs.rs/irtt-client) to run client sessions or
[`irtt-server`](https://docs.rs/irtt-server) to run a server; both build on
`irtt-proto` and provide session lifecycle, negotiation, and validated
configuration on top of it.

Reach for `irtt-proto` directly only if you are implementing another
IRTT-compatible client or server from scratch and need the raw packet layer.

Field values mirror the wire format closely rather than expressing policy:
`Params::decode` rejects malformed input, but direct construction plus
`Params::encode` performs no additional validation — callers are responsible
for validating values before sending them.

## Documentation

Full API documentation is on [docs.rs/irtt-proto](https://docs.rs/irtt-proto).

## Project

Part of [irtt-rs](https://github.com/Lochnair/irtt-rs), an independent Rust
implementation of an IRTT-compatible protocol stack.
