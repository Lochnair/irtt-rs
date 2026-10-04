use crate::PacketCounts;

#[derive(Debug, Clone, Copy, PartialEq)]
/// Packet loss, duplicate, and late-packet statistics.
///
/// Percentage fields are percentages from `0.0` to `100.0` in normal cases.
/// Directional upstream and downstream loss can be negative when
/// server-reported counts exceed local expectations.
///
/// The directional fields are derived from
/// [`PacketCounts::server_packets_received`] for cumulative snapshots. Rolling
/// snapshots use the increase in the highest observed count over the retained
/// packet-event arrival interval, with the preceding observation as baseline
/// (zero at the start of the collector's history). They are `None` without a
/// known baseline/endpoint or when time filtering leaves an interior packet gap.
/// The percentage fields are `0.0` when the corresponding estimate is unavailable.
/// These remain estimates: outstanding sends and replies crossing a window
/// boundary can produce signed values. [`PacketCounts::server_received_window`]
/// is never used to derive loss.
pub struct LossStats {
    /// Locally inferred total lost packets.
    pub lost_packets: u64,
    /// Server-assisted upstream loss estimate, when server counts are available.
    pub upstream_loss_packets: Option<i128>,
    /// Server-assisted downstream loss estimate, when server counts are available.
    pub downstream_loss_packets: Option<i128>,
    /// Total packet loss percentage.
    pub packet_loss_percent: f64,
    /// Server-assisted upstream loss percentage.
    pub upstream_loss_percent: f64,
    /// Server-assisted downstream loss percentage.
    pub downstream_loss_percent: f64,
    /// Duplicate reply percentage.
    pub duplicate_percent: f64,
    /// Late reply packet percentage.
    pub late_packets_percent: f64,
}

pub(crate) fn loss_stats(packets: PacketCounts, server_received: Option<u64>) -> LossStats {
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
    ) = if let Some(server_received) = server_received {
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
