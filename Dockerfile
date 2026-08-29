# syntax=docker/dockerfile:1

# Builds the irtt-server applet only, statically linked against musl, and
# ships it in a scratch image with no shell or libc.
#
# Cross-compiles for TARGETPLATFORM using tonistiigi/xx: the builder stage
# always runs natively on BUILDPLATFORM (no QEMU emulation) and xx-cargo
# picks the right musl target triple, sysroot, and linker for whatever
# platform buildx asked for.

FROM --platform=$BUILDPLATFORM tonistiigi/xx AS xx

FROM --platform=$BUILDPLATFORM rust:1.98-alpine AS builder

COPY --from=xx / /

RUN apk add --no-cache clang lld file

ARG TARGETPLATFORM
RUN xx-apk add --no-cache musl-dev

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN xx-cargo build \
    --release \
    --locked \
    -p irtt-rs \
    --bin irtt-server \
    --no-default-features \
    --features server \
    --target-dir /build/target \
    && xx-verify /build/target/$(xx-cargo --print-target-triple)/release/irtt-server \
    && cp /build/target/$(xx-cargo --print-target-triple)/release/irtt-server /irtt-server

FROM scratch

COPY --from=builder /irtt-server /irtt-server

ENTRYPOINT ["/irtt-server"]
