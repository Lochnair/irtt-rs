use irtt_proto::{Params, PROTOCOL_VERSION};

use crate::config::ServerConfig;

/// Produces the parameters the server returns in an open reply and will enforce
/// for the session.
///
/// Two restrictions apply so far.
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
/// Those two are the whole of the current policy. A server *may* reduce or
/// replace requested parameters, and later policy slices will add the maximum
/// test duration clamp,
/// minimum interval, idle-timeout interval cap, timestamp allowance, DSCP policy
/// and server-fill allow-list.
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

    requested
}
