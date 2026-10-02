use super::*;
use support::echo_packet_len;

#[cfg(not(all(target_os = "linux", feature = "ancillary")))]
fn assert_packet_meta_unavailable(packet_meta: &irtt_client::PacketMeta) {
    assert_eq!(packet_meta.traffic_class, None);
    assert_eq!(packet_meta.dscp, None);
    assert_eq!(packet_meta.ecn, None);
    assert_eq!(packet_meta.kernel_rx_timestamp, None);
}

#[cfg(all(target_os = "linux", feature = "ancillary"))]
fn metadata_unavailable_skip(test_name: &str) {
    eprintln!("{test_name}: skipping metadata assertion because kernel did not provide traffic class metadata");
}

#[cfg(all(target_os = "linux", feature = "ancillary"))]
fn kernel_rx_timestamp_unavailable_skip(test_name: &str) {
    eprintln!("{test_name}: skipping kernel timestamp assertion because kernel did not provide SCM_TIMESTAMPNS");
}

mod burst_recv;
mod packet_length;
mod reply_classification;
mod reply_decode;
mod reply_metadata;
mod timeouts;
