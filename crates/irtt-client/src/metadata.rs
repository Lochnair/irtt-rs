use std::time::{Duration, SystemTime};

use crate::event::PacketMeta;

/// Largest lag behind the paired userspace receive sample that a kernel
/// receive timestamp may show and still be used as a measurement endpoint.
///
/// This is a sanity guard, not an expected kernel-to-userspace wakeup
/// latency: ordinary wakeup latency is orders of magnitude smaller. A kernel
/// timestamp this far behind the userspace sample most likely reflects a
/// realtime clock discontinuity or otherwise unusable metadata, and falling
/// back is preferable to injecting a bad cross-host timing sample.
const MAX_KERNEL_RX_LAG: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct ReceiveMeta {
    pub(crate) traffic_class: Option<u8>,
    pub(crate) kernel_rx_timestamp: Option<SystemTime>,
}

impl ReceiveMeta {
    /// Receive wall-clock endpoint to use for downstream one-way delay.
    ///
    /// Prefers an observed kernel receive timestamp, which is sampled earlier
    /// than the userspace receive instant and therefore excludes socket wakeup
    /// latency from the measured server-to-client delay. The kernel value is
    /// only used when it is plausible for the datagram that produced
    /// `userspace_wall`: it cannot be later than the userspace sample that
    /// observed the datagram, and it may not lag it by more than
    /// [`MAX_KERNEL_RX_LAG`]. Anything else falls back to `userspace_wall`.
    ///
    /// Rejecting a kernel timestamp here never discards it as observed
    /// metadata: [`PacketMeta::kernel_rx_timestamp`] still reports the raw
    /// value.
    pub(crate) fn preferred_receive_wall(&self, userspace_wall: SystemTime) -> SystemTime {
        let Some(kernel_wall) = self.kernel_rx_timestamp else {
            return userspace_wall;
        };
        // `duration_since` fails exactly when the kernel timestamp is later
        // than the userspace sample, which cannot happen for a datagram that
        // sample observed.
        match userspace_wall.duration_since(kernel_wall) {
            Ok(lag) if lag <= MAX_KERNEL_RX_LAG => kernel_wall,
            _ => userspace_wall,
        }
    }
}

impl From<ReceiveMeta> for PacketMeta {
    fn from(meta: ReceiveMeta) -> Self {
        Self {
            traffic_class: meta.traffic_class,
            dscp: meta.traffic_class.map(|traffic_class| traffic_class >> 2),
            ecn: meta.traffic_class.map(|traffic_class| traffic_class & 0b11),
            kernel_rx_timestamp: meta.kernel_rx_timestamp,
        }
    }
}
