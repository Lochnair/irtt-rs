#![no_main]

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use arbitrary::Unstructured;
use irtt_server::{ServerConfig, ServerCore};
use libfuzzer_sys::fuzz_target;

/// Bounds one libFuzzer input as well as the work it can ask the core to do.
/// These are deliberately much smaller than a production UDP payload: the
/// target is stateful parser/session exploration, not packet-size throughput.
const MAX_INPUT_LEN: usize = 32 * 1024;
const MAX_DATAGRAMS: usize = 16;
const MAX_DATAGRAM_LEN: usize = 2 * 1024;
const MAX_SESSIONS: u8 = 8;
const FUZZ_HMAC_KEY: &[u8] = b"fuzz-server-core-key";

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_LEN {
        return;
    }

    let mut input = Unstructured::new(data);
    let Ok(mut config) = config(&mut input) else {
        return;
    };
    if input.arbitrary::<bool>().unwrap_or(false) {
        config = config.with_hmac_key(FUZZ_HMAC_KEY);
    }
    let mut core = ServerCore::new(config);

    for _ in 0..MAX_DATAGRAMS {
        let Ok(peer) = peer(&mut input) else {
            break;
        };
        let Ok(length) = input.int_in_range(0..=MAX_DATAGRAM_LEN) else {
            break;
        };
        let Ok(packet) = input.bytes(length) else {
            break;
        };

        // Arbitrary network input is expected to be rejected or to produce an
        // internal error in exceptional host conditions. It must never panic,
        // and it may never grow the bounded table past its configured cap.
        let _ = core.handle_datagram(peer, packet);
        assert!(core.session_count() <= core.config().max_sessions());
    }
});

fn config(input: &mut Unstructured<'_>) -> arbitrary::Result<ServerConfig> {
    let max_sessions = usize::from(input.int_in_range(0..=MAX_SESSIONS)?);
    Ok(ServerConfig::default()
        .with_max_sessions(max_sessions)
        .with_max_packet_length(MAX_DATAGRAM_LEN))
}

fn peer(input: &mut Unstructured<'_>) -> arbitrary::Result<SocketAddr> {
    let port = input.arbitrary()?;
    let address = if input.arbitrary()? {
        IpAddr::V4(Ipv4Addr::from(input.arbitrary::<[u8; 4]>()?))
    } else {
        IpAddr::V6(Ipv6Addr::from(input.arbitrary::<[u8; 16]>()?))
    };
    Ok(SocketAddr::new(address, port))
}
