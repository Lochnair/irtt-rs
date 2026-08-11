use irtt_proto::{Params, PROTOCOL_VERSION};

use crate::config::ServerConfig;

/// Produces the parameters the server returns in an open reply and will enforce
/// for the session.
///
/// This slice applies the minimum the specification requires: the requested
/// known parameters, with the protocol version rewritten to
/// [`PROTOCOL_VERSION`]. A server must return version 1 so that version-1
/// clients accept the session; any requested value, including an absent,
/// zero, negative or future one, is accepted and answered with 1. Detecting a
/// version mismatch is therefore client-side only.
///
/// Nothing else is restricted yet. A server *may* reduce or replace requested
/// parameters, and later policy slices will add the maximum test duration
/// clamp, minimum interval, idle-timeout interval cap, maximum packet length,
/// timestamp allowance, DSCP policy and server-fill allow-list. This function
/// is where they belong, which is why the configuration is threaded through
/// even though no knob reads it yet.
pub(crate) fn negotiate_params(mut requested: Params, _config: &ServerConfig) -> Params {
    requested.protocol_version = PROTOCOL_VERSION;
    requested
}
