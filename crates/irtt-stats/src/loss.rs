use crate::PacketCounts;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LossStats {
    pub lost_packets: u64,
    pub unknown_loss_packets: u64,
    pub upstream_loss_packets: Option<i128>,
    pub downstream_loss_packets: Option<i128>,
    pub packet_loss_percent: f64,
    pub upstream_loss_percent: f64,
    pub downstream_loss_percent: f64,
    pub duplicate_percent: f64,
    pub late_packets_percent: f64,
}

pub(crate) fn loss_stats(packets: PacketCounts) -> LossStats {
    let lost = packets.packets_sent.saturating_sub(packets.unique_replies);
    let packet_loss_percent = if packets.packets_sent == 0 {
        0.0
    } else if packets.unique_replies == 0 {
        100.0
    } else {
        percent(lost as f64, packets.packets_sent as f64)
    };

    let (
        upstream_loss_packets,
        upstream_loss_percent,
        downstream_loss_packets,
        downstream_loss_percent,
    ) = if let Some(server_received) = packets.server_packets_received {
        let upstream = i128::from(packets.packets_sent) - i128::from(server_received);
        let downstream = i128::from(server_received) - i128::from(packets.packets_received);
        let upstream_percent = if packets.packets_sent == 0 {
            0.0
        } else {
            percent(upstream as f64, packets.packets_sent as f64)
        };
        let downstream_percent = if server_received == 0 {
            0.0
        } else {
            percent(downstream as f64, server_received as f64)
        };
        (
            Some(upstream),
            upstream_percent,
            Some(downstream),
            downstream_percent,
        )
    } else {
        (None, 0.0, None, 0.0)
    };

    LossStats {
        lost_packets: lost,
        unknown_loss_packets: lost,
        upstream_loss_packets,
        downstream_loss_packets,
        packet_loss_percent,
        upstream_loss_percent,
        downstream_loss_percent,
        duplicate_percent: if packets.packets_received == 0 {
            0.0
        } else {
            percent(packets.duplicates as f64, packets.packets_received as f64)
        },
        late_packets_percent: if packets.packets_received == 0 {
            0.0
        } else {
            percent(packets.late_packets as f64, packets.packets_received as f64)
        },
    }
}

fn percent(numerator: f64, denominator: f64) -> f64 {
    100.0 * numerator / denominator
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_and_late_percentages_use_packets_received_denominator() {
        let loss = loss_stats(PacketCounts {
            packets_received: 4,
            duplicates: 1,
            late_packets: 2,
            ..PacketCounts::default()
        });

        assert_eq!(loss.duplicate_percent, 25.0);
        assert_eq!(loss.late_packets_percent, 50.0);
    }
}
