use std::time::Duration;

use irtt_client::{
    Authentication, ClientConfig, HmacKey, NegotiationPolicy, OpenPolicy, SessionRequest,
    SocketConfig,
};

use super::args::CommonClientArgs;

impl CommonClientArgs {
    /// Build the shared client configuration template for a managed run.
    ///
    /// The config is endpoint-independent; managed targets own their addresses.
    pub fn to_client_config(&self, duration: Duration) -> ClientConfig {
        ClientConfig {
            open: OpenPolicy {
                negotiation: if self.loose {
                    NegotiationPolicy::Loose
                } else {
                    NegotiationPolicy::Strict
                },
                ..Default::default()
            },
            request: SessionRequest {
                duration: (!duration.is_zero()).then_some(duration),
                interval: self.interval,
                length: self.length,
                received_stats: self.stats.into(),
                stamp_at: self.tstamp.into(),
                clock: self.clock.into(),
                dscp: self.dscp,
                server_fill: self.server_fill.clone(),
                ..Default::default()
            },
            auth: self
                .hmac
                .as_ref()
                .map_or(Authentication::Unauthenticated, |key| {
                    Authentication::Hmac(HmacKey::new(key.as_bytes()))
                }),
            socket: SocketConfig {
                ttl: self.ttl,
                ..SocketConfig::default()
            },
            ..ClientConfig::default()
        }
    }
}

pub fn expected_probe_count(duration: Duration, interval: Duration) -> u64 {
    let interval_nanos = interval.as_nanos();
    if interval_nanos == 0 {
        return u64::MAX;
    }

    let expected = duration
        .as_nanos()
        .saturating_add(interval_nanos.saturating_sub(1))
        / interval_nanos;
    expected.min(u128::from(u64::MAX)) as u64
}
