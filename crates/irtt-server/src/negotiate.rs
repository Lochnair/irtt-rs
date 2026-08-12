use irtt_proto::{Params, PROTOCOL_VERSION};

use crate::{clock::saturating_ns, config::ServerConfig};

/// Produces the parameters the server returns in an open reply and will enforce
/// for the session.
///
/// **Protocol version is rewritten to [`PROTOCOL_VERSION`].** A server must
/// return version 1 so that version-1 clients accept the session; any requested
/// value, including an absent, zero, negative or future one, is accepted and
/// answered with 1. Detecting a version mismatch is therefore client-side only.
///
/// **A positive Length above the configured maximum is reduced to it.** That
/// bounds the echo datagram — and so the buffer — a remote peer can make this
/// server negotiate, which is why it happens here rather than at send time: the
/// value returned in the open reply has to be the one the server will actually
/// honor. A negative Length is left exactly as requested and a zero stays zero;
/// neither asks for space beyond the mandatory field block, which `irtt-proto`
/// floors echo sizing at, so there is nothing to reduce and rewriting them would
/// answer with a session the client did not ask for.
///
/// **Duration is reduced to the configured maximum test duration**, when one is
/// configured. A *continuous* request — Duration absent, which is the wire
/// default of zero — is answered with the maximum as well, because restricting
/// continuous mode to a finite test is exactly what configuring a maximum asks
/// for; the client models that as an ordinary duration reduction. An explicit
/// zero or negative Duration never reaches here, being refused during request
/// admission, and a configured maximum is never zero, so a finite request can
/// never be rewritten into a continuous session.
///
/// **Interval is raised to the configured minimum and then capped at a quarter
/// of the idle timeout.** The floor bounds how fast a session may ask to be
/// answered; the cap keeps a well-behaved client from idling itself out by
/// sending at the very interval it was given. An absent Interval takes the floor
/// like any other value below it, since absence means the wire default of zero
/// rather than a refusal — an explicit zero is refused during request admission.
///
/// The two can disagree: a configured minimum above a quarter of the idle
/// timeout ends with the cap winning, and the returned interval below the
/// configured minimum. That is deliberate, and `rate` is where the consequence
/// is settled — the session's allowance then replenishes at the *negotiated*
/// interval, so this server never enforces a cadence slower than the one it just
/// handed the client.
///
/// Later policy slices will add the timestamp allowance, DSCP policy and the
/// server-fill allow-list.
///
/// Negotiation is not by itself an acknowledgement: the core validates what this
/// function returns and silently discards an open whose effective parameters it
/// could not execute. That is a separate step on purpose — this function decides
/// what the session *would* be, and the core decides whether that session can
/// exist. Capping Length here is not enough on its own, because the mandatory
/// echo field block can exceed the configured maximum by itself.
pub(crate) fn negotiate_params(mut requested: Params, config: &ServerConfig) -> Params {
    requested.protocol_version = PROTOCOL_VERSION;

    // A configured maximum wider than `i64` cannot restrict anything: the wire
    // field cannot carry more than `i64::MAX` in the first place. Saturating
    // there is therefore exact, not an approximation — unlike converting the
    // requested length the other way, which would turn a value this platform
    // cannot represent into a plausible buffer size.
    let max_length = i64::try_from(config.max_packet_length()).unwrap_or(i64::MAX);
    if requested.length > max_length {
        requested.length = max_length;
    }

    if let Some(maximum) = config.max_test_duration() {
        // Never zero — `with_max_test_duration` stores a zero maximum as no
        // maximum — so this cannot turn a finite request into a continuous one.
        let maximum = saturating_ns(maximum);
        if requested.duration_ns == 0 || requested.duration_ns > maximum {
            requested.duration_ns = maximum;
        }
    }

    // Floor first, cap second. The order is what lets the cap produce a
    // negotiated interval below the configured minimum, which is the case the
    // rate limiter is written to honor rather than contradict.
    let minimum = saturating_ns(config.min_send_interval());
    if requested.interval_ns < minimum {
        requested.interval_ns = minimum;
    }
    // Divided as a `Duration`, before the conversion rather than after it. The
    // two agree for every timeout `i64` nanoseconds can hold, but saturating
    // first would cap an absurdly long timeout's quarter at `i64::MAX / 4` —
    // reducing intervals the configured cap never meant to reach — where
    // dividing first leaves every representable interval alone, which is what a
    // timeout beyond the wire's range should do.
    let idle_cap = saturating_ns(config.idle_timeout() / 4);
    if idle_cap > 0 && requested.interval_ns > idle_cap {
        requested.interval_ns = idle_cap;
    }

    requested
}
