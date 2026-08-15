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

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use super::{ReceiveMeta, MAX_KERNEL_RX_LAG};
    use crate::event::PacketMeta;

    fn userspace_wall() -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000)
    }

    fn meta_with_kernel_rx(kernel_rx_timestamp: SystemTime) -> ReceiveMeta {
        ReceiveMeta {
            traffic_class: None,
            kernel_rx_timestamp: Some(kernel_rx_timestamp),
        }
    }

    #[test]
    fn preferred_receive_wall_without_kernel_timestamp_uses_userspace_sample() {
        let userspace = userspace_wall();

        assert_eq!(
            ReceiveMeta::default().preferred_receive_wall(userspace),
            userspace
        );
    }

    #[test]
    fn preferred_receive_wall_uses_plausible_earlier_kernel_timestamp() {
        let userspace = userspace_wall();
        let kernel = userspace - Duration::from_micros(80);

        assert_eq!(
            meta_with_kernel_rx(kernel).preferred_receive_wall(userspace),
            kernel
        );
    }

    #[test]
    fn preferred_receive_wall_accepts_kernel_timestamp_equal_to_userspace_sample() {
        let userspace = userspace_wall();

        assert_eq!(
            meta_with_kernel_rx(userspace).preferred_receive_wall(userspace),
            userspace
        );
    }

    #[test]
    fn preferred_receive_wall_rejects_kernel_timestamp_later_than_userspace_sample() {
        let userspace = userspace_wall();
        let kernel = userspace + Duration::from_nanos(1);

        assert_eq!(
            meta_with_kernel_rx(kernel).preferred_receive_wall(userspace),
            userspace
        );
    }

    #[test]
    fn preferred_receive_wall_accepts_kernel_timestamp_exactly_at_the_lag_bound() {
        // The bound is inclusive: a lag of exactly MAX_KERNEL_RX_LAG is
        // still accepted, one nanosecond more is not.
        let userspace = userspace_wall();
        let at_bound = userspace - MAX_KERNEL_RX_LAG;
        let past_bound = userspace - MAX_KERNEL_RX_LAG - Duration::from_nanos(1);

        assert_eq!(
            meta_with_kernel_rx(at_bound).preferred_receive_wall(userspace),
            at_bound
        );
        assert_eq!(
            meta_with_kernel_rx(past_bound).preferred_receive_wall(userspace),
            userspace
        );
    }

    #[test]
    fn preferred_receive_wall_rejects_implausibly_old_kernel_timestamp() {
        let userspace = userspace_wall();
        let kernel = SystemTime::UNIX_EPOCH;

        assert_eq!(
            meta_with_kernel_rx(kernel).preferred_receive_wall(userspace),
            userspace
        );
    }

    #[test]
    fn preferred_receive_wall_handles_extreme_timestamps_without_panicking() {
        let userspace = userspace_wall();
        let far_future = SystemTime::UNIX_EPOCH + Duration::from_secs(u64::from(u32::MAX)) * 4;
        let before_epoch = SystemTime::UNIX_EPOCH - Duration::from_secs(u64::from(u32::MAX));

        assert_eq!(
            meta_with_kernel_rx(far_future).preferred_receive_wall(userspace),
            userspace
        );
        assert_eq!(
            meta_with_kernel_rx(before_epoch).preferred_receive_wall(userspace),
            userspace
        );
        assert_eq!(
            ReceiveMeta::default().preferred_receive_wall(before_epoch),
            before_epoch
        );
    }

    #[test]
    fn metadata_preserves_kernel_timestamp_rejected_for_measurement() {
        let userspace = userspace_wall();
        let rejected = userspace + Duration::from_secs(30);
        let meta = meta_with_kernel_rx(rejected);

        assert_eq!(meta.preferred_receive_wall(userspace), userspace);
        assert_eq!(
            PacketMeta::from(meta).kernel_rx_timestamp,
            Some(rejected),
            "observed metadata must survive rejection as a measurement endpoint"
        );
    }

    #[test]
    fn metadata_observed_traffic_class_zero_preserves_observed_zero() {
        let packet_meta = PacketMeta::from(ReceiveMeta {
            traffic_class: Some(0),
            kernel_rx_timestamp: None,
        });

        assert_eq!(packet_meta.traffic_class, Some(0));
        assert_eq!(packet_meta.dscp, Some(0));
        assert_eq!(packet_meta.ecn, Some(0));
        assert_eq!(packet_meta.kernel_rx_timestamp, None);
    }

    #[test]
    fn metadata_observed_traffic_class_derives_dscp_and_ecn() {
        let packet_meta = PacketMeta::from(ReceiveMeta {
            traffic_class: Some(184),
            kernel_rx_timestamp: None,
        });

        assert_eq!(packet_meta.traffic_class, Some(184));
        assert_eq!(packet_meta.dscp, Some(46));
        assert_eq!(packet_meta.ecn, Some(0));
        assert_eq!(packet_meta.kernel_rx_timestamp, None);

        let packet_meta = PacketMeta::from(ReceiveMeta {
            traffic_class: Some(186),
            kernel_rx_timestamp: None,
        });

        assert_eq!(packet_meta.traffic_class, Some(186));
        assert_eq!(packet_meta.dscp, Some(46));
        assert_eq!(packet_meta.ecn, Some(2));
        assert_eq!(packet_meta.kernel_rx_timestamp, None);
    }

    #[test]
    fn metadata_mapping_preserves_none_vs_observed_zero() {
        let unavailable = PacketMeta::from(ReceiveMeta::default());
        let observed_zero = PacketMeta::from(ReceiveMeta {
            traffic_class: Some(0),
            kernel_rx_timestamp: None,
        });

        assert_eq!(unavailable.traffic_class, None);
        assert_eq!(unavailable.dscp, None);
        assert_eq!(unavailable.ecn, None);

        assert_eq!(observed_zero.traffic_class, Some(0));
        assert_eq!(observed_zero.dscp, Some(0));
        assert_eq!(observed_zero.ecn, Some(0));
    }
}
