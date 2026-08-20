# irtt-server

## NAME

irtt-server - IRTT-compatible UDP server

## SYNOPSIS

`irtt-server` [`--bind` *ADDR*]... [*OPTIONS*]

`irtt-rs server` [`--bind` *ADDR*]... [*OPTIONS*]

## DESCRIPTION

`irtt-server` answers IRTT echo requests over UDP. It is thin orchestration
over the reusable `irtt-server` library: one current-thread Tokio runtime,
one or more listeners, and the library's own session/policy logic.

Run with no arguments for the ordinary case:

```sh
irtt-server
```

With no `--bind`, the server listens on the standard IRTT port (2112) on the
wildcard address for both address families, in this fixed order:

1. `[::]:2112`
2. `0.0.0.0:2112`

This is available wherever the platform supports safe wildcard reply-source
selection (see [Wildcard binds](#wildcard-binds) below). Any explicit
`--bind` **replaces** this default pair entirely rather than adding to it —
`irtt-server --bind 127.0.0.1:2112` binds only that one address.

If one of the two default addresses cannot be bound because its address
family itself has no local support at all — IPv6 disabled on an otherwise
IPv4-capable host, for example — the server falls back to serving just the
other one instead of failing outright, and says so on startup. Any other
reason a default listener fails — the port already in use, permission
denied, no safe wildcard reply-source path — is not a family-support
problem and is not silently worked around; the invocation fails as it would
for an explicit `--bind`. This fallback applies only to the implicit default
pair: an explicit `--bind` list always keeps the ordinary all-or-nothing
behavior described below.

## OPTIONS

### `--bind ADDR`

Local UDP address to bind, as `ADDR:PORT`, for example `127.0.0.1:2112` or
`[::1]:2112`. Host names are not resolved. Repeatable.

Repeat the option to serve several addresses from one process, in the order
given. Every listener applies the same policy options, but each keeps its
own sessions and tokens, so a session belongs to the address it was opened
on. Binding is all-or-nothing: if any address cannot be bound, none are
served.

A port of `0` selects an unused port per listener, so two such binds get two
different ports.

With no `--bind` at all, see [Default bind](#default-bind) above.

### `--hmac KEY`

HMAC key, taken as the UTF-8 bytes of this argument. With a key configured,
every request must carry a valid MAC and every reply is authenticated;
without one, authenticated requests are dropped. The key is visible in the
process arguments.

### `--max-sessions COUNT`

Maximum number of simultaneously live sessions **per listener**, not per
process. Once a listener's table is full, a session-creating open to it is
dropped silently; nothing is evicted. Zero refuses every session-creating
open.

### `--max-packet-length BYTES`

Maximum echo datagram size a session may negotiate, in bytes. A longer
requested length is reduced during negotiation, an open whose mandatory
field block would not fit is refused, and inbound echo requests are admitted
against the same limit. This is a resource bound, not an MTU.

### `--min-interval DURATION`

Floor on the send interval a session may negotiate. The negotiated interval
is still capped at a quarter of the idle timeout afterwards, so a
`--min-interval` above that cap is not the value actually negotiated.

A session's reply allowance refills at the shorter of this configured floor
and the interval it actually negotiated. Ordinarily that is the configured
floor itself, since the negotiated interval is normally at least as long —
a 10ms `--min-interval` against a 1s negotiated interval refills every
10ms. The two differ only when the idle-timeout cap above pulled the
negotiated interval below the configured floor, in which case the shorter,
negotiated value is what the allowance actually refills at: a server must
not enforce a cadence it did not advertise to the client. Use `0` for no
time-based throttling.

### `--burst COUNT`

Echo replies one session may have answered before its allowance has to
refill. Use `0` for no allowance at all, which rate-limits every echo
request whatever the interval is.

### `--idle-timeout DURATION`

Release a session after this long without a served or rate-limited echo
request. The deadline runs from the open, and release is silent. Use `0` to
expire a session at the next evaluation; it is not a way to disable expiry.

### `--max-duration DURATION`

Maximum test duration a session may negotiate. A longer request is reduced
to it, and a continuous request is answered with it. Omit this flag for no
maximum; a maximum of zero cannot be expressed, because a negotiated
duration of zero means continuous.

### `--timestamp-allowance MODE`

How many timestamps this server will provide: `dual` (default, honors a
request for send, receive, both, or midpoint timestamps), `single`
(provides at most one timestamp instant; a request for both is negotiated to
midpoint), or `none` (no timestamps at all). The requested clock source is
never changed, only which instants are reported, so a single instant is
still reported once per requested clock.

### `--no-dscp`

Refuse to provide requested traffic-class marking. Any requested DSCP is
negotiated to zero, so the client is told its echo replies will be unmarked,
and they are sent unmarked. The session is not refused.

Durations everywhere in this command accept an integer followed by a unit:
`ms`, `s`, or `m`.

## OUTPUT

Each bound endpoint is printed on startup, once every listener is up, which
also resolves a port of `0`:

```
irtt-server: listening on [::]:2112
irtt-server: listening on 0.0.0.0:2112
```

`Ctrl-C` stops the server gracefully.

## MULTIPLE LISTENERS

Every invocation runs through the same multi-listener path, so one address —
whether from the default pair or a single explicit `--bind` — is an ordinary
set of one rather than a separate mode. The policy options above apply to
every listener, but listeners are otherwise independent: each has its own
sessions and tokens, and `--max-sessions` bounds each listener rather than
the process. If any listener fails while running, the others are shut down
with it rather than leaving a service configured for two families answering
on one.

Wildcard IPv4 and IPv6 listeners may share one port, as the default pair
does.

## SERVER FILL

Echo replies fill their payload according to the negotiated `ServerFill`
mode, which a client requests with `irtt-client`'s `--sfill` (see
`irtt-client(1)`):

- `none`: zero-filled.
- `rand`: random bytes.
- `pattern:HEX`: the given repeating hexadecimal pattern.

Every valid descriptor is honored. A descriptor the server cannot parse, and
a client that expresses no fill preference at all, both get the default
`pattern:69727474`, i.e. the repeating bytes `69 72 74 74` (`irtt`).

## WILDCARD BINDS

An explicit interface address works on every supported system. A wildcard
bind such as `0.0.0.0:2112` or `[::]:2112` reads each request's local
destination from packet metadata and sends that request's reply from the
same address, so a client on a multi-homed host is answered from the
endpoint it actually contacted.

That reply-source selection is implemented on **Linux, macOS, and FreeBSD**.
Elsewhere, a wildcard bind is refused at construction rather than served
from a routing-table-selected source address a client would discard — this
also applies to the zero-argument default, which will fail with a clear
error asking for an explicit `--bind` on such a platform. This correctness
rule is deliberate and is not weakened by the zero-argument default.

## EXAMPLES

Run with the default wildcard listeners on the standard port:

```sh
irtt-server
```

Bind a single explicit address:

```sh
irtt-server --bind 127.0.0.1:2112
```

Through the `irtt-rs` dispatcher:

```sh
irtt-rs server --bind 192.0.2.10:2112
```

Serve two explicit addresses from one process:

```sh
irtt-server \
    --bind 0.0.0.0:2112 \
    --bind [::]:2112
```

Apply session policy:

```sh
irtt-server \
    --bind 192.0.2.10:2112 \
    --max-sessions 512 \
    --idle-timeout 30s
```

Restrict optional capabilities:

```sh
irtt-server \
    --bind 192.0.2.10:2112 \
    --timestamp-allowance single \
    --no-dscp
```

Require authentication:

```sh
irtt-server --bind 192.0.2.10:2112 --hmac secret
```

## EXIT STATUS

`0` on a clean shutdown (`Ctrl-C`), nonzero if binding fails or a listener
fails while running.

## LIMITATIONS

- No hostname resolution, interface expansion, daemonization, or
  configuration file — binds are explicit addresses only.
- Wildcard reply-source selection is unavailable outside Linux, macOS, and
  FreeBSD; such platforms must bind explicit addresses.

## SEE ALSO

`irtt-client(1)`, `irtt-tui(1)`, `irtt-rs(1)`, and `irtt-server --help` for
the full option list.
