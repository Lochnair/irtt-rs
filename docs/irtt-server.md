# irtt-server

## Name

irtt-server - IRTT-compatible UDP server

## Synopsis

`irtt-server` [`--bind` *ADDR*]... [*OPTIONS*]

`irtt-rs server` [`--bind` *ADDR*]... [*OPTIONS*]

## Description

`irtt-server` answers IRTT echo requests over UDP. It uses the reusable
`irtt-server` library for sessions and policy, with one current-thread
Tokio runtime running one or more listeners.

To start a server with the defaults, run:

```sh
irtt-server
```

With no `--bind`, the server listens on the standard IRTT port (2112) on the
wildcard address for both address families, in this fixed order:

1. `[::]:2112`
2. `0.0.0.0:2112`

The defaults work on platforms that support wildcard reply-source
selection (see [Wildcard binds](#wildcard-binds)). If you supply any
`--bind`, it replaces both defaults. For example,
`irtt-server --bind 127.0.0.1:2112` listens only on that address.

If the host has no support for one address family, for example because IPv6
is disabled, the server uses the other default listener and tells you on
startup. Other bind failures still stop startup. This includes a port
already in use, permission denied, or a platform without safe wildcard
reply-source selection.

The address-family fallback only applies to the default pair. An explicit
`--bind` list always uses the all-or-nothing behavior described below.

## Options

### `--bind ADDR`

Use `ADDR:PORT` to choose a local UDP address, for example `127.0.0.1:2112`
or `[::1]:2112`. Hostnames are not resolved.

Repeat `--bind` to serve several addresses from one process, in the order
given. Every listener uses the same policy options, but keeps its own
sessions and tokens. A session belongs to the address it was opened on.
Binding is all-or-nothing: if any address cannot be bound, none are served.

A port of `0` selects an unused port per listener, so two such binds get two
different ports.

With no `--bind` at all, see [Description](#description) above for the
default wildcard pair and the address-family fallback it allows.

If you need DSCP/traffic-class marking on replies to IPv4 peers, use care
with a lone dual-stack `[::]:PORT` listener. On Linux, those peers use
IPv4-mapped addresses and their replies cannot retain the negotiated
marking. Use paired `[::]:PORT` and `0.0.0.0:PORT` listeners on the same
port. The server makes the IPv6 listener IPv6-only for that pair, so each
family gets the correct socket option. A lone IPv6 listener keeps its usual
behavior; the server does not force `IPV6_V6ONLY` on it.

### `--hmac KEY`

You can use a plain-text key or a hexadecimal key prefixed with `0x`.
For example, `secret` and `0x736563726574` are equivalent. The
`IRTT_SERVER_HMAC` environment variable accepts the same syntax.

With a key configured, every request must carry a valid MAC and the server
authenticates every reply. Without a key, the server drops authenticated
requests. A key passed on the command line is visible in the process
arguments.

### `--max-sessions COUNT`

Set the maximum number of live sessions per listener. This limit applies
to each listener separately, so the process can have more sessions in total.
Once a listener's table is full, it silently drops opens that would create
a session and keeps the existing sessions. Zero refuses every open that
would create a session.

### `--max-packet-length BYTES`

Set the maximum echo datagram size a session may negotiate, in bytes. The
server reduces longer requests during negotiation and refuses an open if
its mandatory fields would not fit. Incoming echo requests must fit the
same limit. Use this to bound resource use; it does not set the network MTU.

### `--min-interval DURATION`

Set the minimum send interval a session may negotiate. The server then caps
the negotiated interval at a quarter of the idle timeout, so a
`--min-interval` above that cap will be reduced.

The session's reply allowance refills at the shorter of the configured
minimum and the negotiated interval. Usually this is the configured
minimum: a 10ms `--min-interval` with a 1s negotiated interval refills every
10ms.

If the idle-timeout cap reduces the negotiated interval below the configured
minimum, the allowance refills at that shorter interval too. This keeps the
server from enforcing a slower cadence than it told the client to use.
Use `0` for no time-based throttling.

### `--burst COUNT`

Set how many echo replies a session can send before its allowance needs to
refill. Use `0` for no allowance, which rate-limits every
echo request regardless of the interval.

### `--idle-timeout DURATION`

Release a session after this long without a served or rate-limited echo
request. The timeout starts at open, and the server releases the session
without sending a close. If you use `0`, the session expires at the next
evaluation; zero does not disable expiry.

### `--max-duration DURATION`

Set the maximum test duration a session may negotiate. The server reduces
longer requests to this value and uses it for continuous requests too.
Leave the flag out if you don't want a maximum. You cannot set a maximum of
zero, since a negotiated duration of zero means continuous.

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

## Environment

You can also set each option through an environment variable, which is
useful for containers, orchestrators, and unit files. A command-line flag
overrides the corresponding variable entirely. If neither is set, the
server uses its built-in default.

| Variable | Option |
| --- | --- |
| `IRTT_SERVER_BIND` | `--bind`, one address only |
| `IRTT_SERVER_HMAC` | `--hmac` |
| `IRTT_SERVER_MAX_SESSIONS` | `--max-sessions` |
| `IRTT_SERVER_MAX_PACKET_LENGTH` | `--max-packet-length` |
| `IRTT_SERVER_MIN_INTERVAL` | `--min-interval` |
| `IRTT_SERVER_BURST` | `--burst` |
| `IRTT_SERVER_IDLE_TIMEOUT` | `--idle-timeout` |
| `IRTT_SERVER_MAX_DURATION` | `--max-duration` |
| `IRTT_SERVER_TIMESTAMP_ALLOWANCE` | `--timestamp-allowance` |
| `IRTT_SERVER_NO_DSCP` | `--no-dscp` |

`IRTT_SERVER_BIND` takes exactly one address, the same as a single `--bind`;
there is no way to request several listeners from the environment. Serving
more than one address needs repeated `--bind` options.

`IRTT_SERVER_NO_DSCP` is the only boolean. `0`, `false`, `no`, `n`, `off` and
an empty value leave traffic-class marking enabled; any other value disables
it, and an unset variable leaves it enabled.

## Output

Once every listener is up, the server prints each bound endpoint. If you
requested port `0`, this shows which port was assigned:

```
irtt-server: listening on [::]:2112
irtt-server: listening on 0.0.0.0:2112
```

`Ctrl-C` stops the server gracefully.

## Multiple listeners

The server manages listeners the same way whether you have one or several,
including when only one address from the default pair is available or you
supply a single `--bind`. Each listener uses the same policy options, but
keeps its own sessions and tokens. The `--max-sessions` limit applies to
each listener separately.

If any listener fails while running, the server shuts down the others too.
So a service configured for both address families won't carry on answering
on just one after a listener failure.

Wildcard IPv4 and IPv6 listeners may share one port, as the default pair
does.

## Server fill

Echo replies fill their payload according to the negotiated `ServerFill`
mode, which a client requests with `irtt-client`'s `--sfill` (see
`irtt-client(1)`):

- `none`: zero-filled.
- `rand`: random bytes.
- `pattern:HEX`: the given repeating hexadecimal pattern.

The server honors every valid descriptor. If it cannot parse the descriptor,
or the client sends no fill preference, it uses `pattern:69727474`: the
repeating bytes `69 72 74 74` (`irtt`).

## Wildcard binds

An explicit interface address works on every supported system. A wildcard
bind such as `0.0.0.0:2112` or `[::]:2112` reads each request's local
destination from packet metadata and sends that request's reply from the
same address, so a client on a multi-homed host is answered from the
endpoint it actually contacted.

Reply-source selection is supported on Linux, macOS, and FreeBSD. On other
platforms, the server refuses wildcard binds because the routing table
could select a source address the client would discard. This also means
running without arguments fails on those platforms, with an error asking
you to supply an explicit `--bind`.

## Examples

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

## Exit status

`0` on a clean shutdown (`Ctrl-C`), nonzero if binding fails or a listener
fails while running.

## Limitations

- No hostname resolution, interface expansion, daemonization, or
  configuration file. Binds must use explicit addresses.
- Wildcard reply-source selection is unavailable outside Linux, macOS, and
  FreeBSD; such platforms must bind explicit addresses.

## See also

`irtt-client(1)`, `irtt-tui(1)`, `irtt-rs(1)`, and `irtt-server --help` for
the full option list.
