use irtt_proto::{Params, StampAt, PROTOCOL_VERSION};

use crate::{
    clock::saturating_ns,
    config::{ServerConfig, TimestampAllowance},
};

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
/// **ServerFill is settled separately**, by
/// [`negotiate_server_fill`](crate::fill::negotiate_server_fill), because it
/// produces a second result this function has nowhere to put: the effective
/// fill the session will use, which is not always what the descriptor in
/// `Params` says. The two run back to back on the same open.
///
/// **StampAt is reduced to the configured timestamp allowance**, by
/// [`restrict_stamp_at`]. The Clock is deliberately untouched: the allowance is
/// a policy about which timestamps this server provides, and the echo layout
/// carries no timestamp field once the placement is [`StampAt::None`], so there
/// is no clock to restrict as well. Rewriting it would answer with a session the
/// client did not ask for.
///
/// **DSCP is forced to zero when the server disallows it**, and otherwise
/// returned exactly as requested — including a value outside `0..=255`, which
/// stays in the negotiated parameters and is transported unmarked. Zero is a
/// negotiated value like any other and `Params::encode` simply omits it, so a
/// restricted reply carries no DSCP tag and the session's stored parameters
/// carry zero. That one value is what the runtime derives an unmarked reply
/// from; no session or transport state records the policy separately.
///
/// Both restrictions run here, before the core validates the effective session,
/// which is what makes a timestamp-disallowing server accept an open that
/// selected timestamps without sending a Clock.
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

    requested.stamp_at = restrict_stamp_at(requested.stamp_at, config.timestamp_allowance());
    if !config.dscp_allowed() {
        requested.dscp = 0;
    }

    requested
}

/// The timestamp placement a server offering `allowance` provides for a session
/// that requested `requested`.
///
/// The mapping is the observed one, from the clean specification's Section 11.4.
/// Only one row substitutes rather than removes: under [`Single`], a request for
/// both instants is answered with [`StampAt::Midpoint`] — one instant that still
/// describes both — and not with whichever of Send or Receive an implementation
/// happened to prefer. Every already-single placement passes through, and
/// [`None`] removes all of them.
///
/// The allowance bounds reported *instants*, not fields: the clock is untouched,
/// so a negotiated midpoint on `Clock::Both` still reports one field per domain.
///
/// [`Single`]: TimestampAllowance::Single
/// [`None`]: TimestampAllowance::None
fn restrict_stamp_at(requested: StampAt, allowance: TimestampAllowance) -> StampAt {
    match allowance {
        TimestampAllowance::Dual => requested,
        TimestampAllowance::Single => match requested {
            StampAt::Both => StampAt::Midpoint,
            other => other,
        },
        TimestampAllowance::None => StampAt::None,
    }
}
