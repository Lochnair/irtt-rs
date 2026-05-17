use std::time::SystemTime;

use crate::event::PacketMeta;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct ReceiveMeta {
    pub(crate) traffic_class: Option<u8>,
    pub(crate) kernel_rx_timestamp: Option<SystemTime>,
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
    use super::ReceiveMeta;
    use crate::event::PacketMeta;

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
