use std::time::Duration;

use irtt_client::{ClientConfig, NegotiationPolicy, SocketConfig};

use super::args::CommonClientArgs;

pub const DEFAULT_RECV_TIMEOUT: Duration = Duration::from_millis(20);

impl CommonClientArgs {
    /// Build the shared client configuration template for a managed run.
    ///
    /// The template carries no server address: the managed driver replaces it
    /// per target from that target's configuration, so deriving one here from
    /// an arbitrary target would be dead data.
    pub fn to_client_config(&self, duration: Duration) -> ClientConfig {
        ClientConfig {
            duration: (!duration.is_zero()).then_some(duration),
            interval: self.interval,
            length: self.length,
            received_stats: self.stats.into(),
            stamp_at: self.tstamp.into(),
            clock: self.clock.into(),
            dscp: self.dscp,
            hmac_key: self.hmac.as_ref().map(|key| key.as_bytes().to_vec()),
            server_fill: self.server_fill.clone(),
            negotiation_policy: if self.loose {
                NegotiationPolicy::Loose
            } else {
                NegotiationPolicy::Strict
            },
            socket_config: SocketConfig {
                ttl: self.ttl,
                recv_timeout: Some(DEFAULT_RECV_TIMEOUT),
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
