#![no_main]

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    time::Duration,
};

use arbitrary::Unstructured;
use irtt_proto::{decode_open_reply, encode_request, Params, RequestToEncode};
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

#[derive(Clone)]
struct FuzzSession {
    token: u64,
    peer: SocketAddr,
    params: Params,
}

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT_LEN {
        return;
    }

    let mut input = Unstructured::new(data);
    let Ok(mut config) = config(&mut input) else {
        return;
    };
    let hmac_key = input
        .arbitrary::<bool>()
        .ok()
        .filter(|enabled| *enabled)
        .map(|_| FUZZ_HMAC_KEY);
    if let Some(key) = hmac_key {
        config = config.with_hmac_key(key);
    }
    let mut core = ServerCore::new(config);
    let mut sessions = Vec::new();

    for _ in 0..MAX_DATAGRAMS {
        let Ok(operation) = input.arbitrary::<u8>() else {
            break;
        };

        match operation % 4 {
            // Keep arbitrary-byte admission coverage alongside the stateful
            // production-encoded requests below.
            0 => raw_datagram(&mut core, &mut input),
            1 => open_session(&mut core, &mut input, hmac_key, &mut sessions),
            2 => echo_session(&mut core, &mut input, hmac_key, &sessions),
            _ => close_session(&mut core, &mut input, hmac_key, &mut sessions),
        }

        // Arbitrary network input is expected to be rejected or to produce an
        // internal error in exceptional host conditions. It must never panic,
        // and it may never grow the bounded table past its configured cap.
        assert!(core.session_count() <= core.config().max_sessions());
    }
});

fn config(input: &mut Unstructured<'_>) -> arbitrary::Result<ServerConfig> {
    let max_sessions = usize::from(input.int_in_range(0..=MAX_SESSIONS)?);
    Ok(ServerConfig::default()
        .with_max_sessions(max_sessions)
        .with_max_packet_length(MAX_DATAGRAM_LEN)
        .with_min_send_interval(Duration::ZERO))
}

fn raw_datagram(core: &mut ServerCore, input: &mut Unstructured<'_>) {
    let (Ok(peer), Ok(length)) = (peer(input), input.int_in_range(0..=MAX_DATAGRAM_LEN)) else {
        return;
    };
    let Ok(packet) = input.bytes(length) else {
        return;
    };
    let _ = core.handle_datagram(peer, packet);
}

fn open_session(
    core: &mut ServerCore,
    input: &mut Unstructured<'_>,
    hmac_key: Option<&[u8]>,
    sessions: &mut Vec<FuzzSession>,
) {
    let Ok(peer) = peer(input) else {
        return;
    };
    let params = session_params();
    let request = encode_request(
        RequestToEncode::Open {
            params: &params,
            no_test: false,
        },
        hmac_key,
    )
    .expect("fixed valid fuzz open must encode");
    let Ok(Some(reply)) = core.handle_datagram(peer, &request) else {
        return;
    };
    let Ok(reply) = decode_open_reply(reply.bytes(), hmac_key) else {
        return;
    };
    sessions.push(FuzzSession {
        token: reply.token,
        peer,
        params: reply.params,
    });
}

fn echo_session(
    core: &mut ServerCore,
    input: &mut Unstructured<'_>,
    hmac_key: Option<&[u8]>,
    sessions: &[FuzzSession],
) {
    let Some(session) =
        sessions.get(usize::from(input.arbitrary::<u8>().unwrap_or(0)) % sessions.len().max(1))
    else {
        return;
    };
    let sequence = input.arbitrary().unwrap_or_default();
    let request = encode_request(
        RequestToEncode::Echo {
            token: session.token,
            sequence,
            params: &session.params,
            payload: &[],
        },
        hmac_key,
    )
    .expect("fixed valid fuzz echo must encode");
    let _ = core.handle_datagram(session.peer, &request);
}

fn close_session(
    core: &mut ServerCore,
    input: &mut Unstructured<'_>,
    hmac_key: Option<&[u8]>,
    sessions: &mut Vec<FuzzSession>,
) {
    let index = usize::from(input.arbitrary::<u8>().unwrap_or(0)) % sessions.len().max(1);
    let Some(session) = sessions.get(index) else {
        return;
    };
    let request = encode_request(
        RequestToEncode::Close {
            token: session.token,
        },
        hmac_key,
    )
    .expect("fixed valid fuzz close must encode");
    let _ = core.handle_datagram(session.peer, &request);
    sessions.swap_remove(index);
}

fn session_params() -> Params {
    Params {
        protocol_version: 1,
        length: 256,
        ..Params::default()
    }
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
