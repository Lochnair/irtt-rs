use irtt_client::{Client, ClientEvent};
use irtt_proto::{Clock, ReceivedStats, StampAt};

use crate::support::{config_for_params, default_params, params_for_modes, BackendPeer};

#[test]
fn backend_basic_open_echo_close() {
    let params = default_params();
    let peer = BackendPeer::start_open_echo(params.clone(), None);
    let mut client = Client::connect(config_for_params(peer.addr(), &params)).unwrap();

    let outcome = client.open().unwrap();
    assert!(matches!(outcome, irtt_client::OpenOutcome::Started { .. }));

    let sent = client.send_probe().unwrap();
    assert_eq!(sent.len(), 1);

    let events = client.recv_once().unwrap();
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], ClientEvent::EchoReply { .. }));

    client.close().unwrap();
}

#[test]
fn backend_ttl_smoke() {
    let params = default_params();
    let peer = BackendPeer::start_open_echo(params.clone(), None);
    let mut config = config_for_params(peer.addr(), &params);
    config.socket_config.ttl = Some(64);
    let mut client = Client::connect(config).unwrap();

    let outcome = client.open().unwrap();
    assert!(matches!(outcome, irtt_client::OpenOutcome::Started { .. }));

    let sent = client.send_probe().unwrap();
    assert_eq!(sent.len(), 1);

    let events = client.recv_once().unwrap();
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], ClientEvent::EchoReply { .. }));

    client.close().unwrap();
}

#[test]
fn backend_received_stats_smoke() {
    for mode in [
        ReceivedStats::None,
        ReceivedStats::Count,
        ReceivedStats::Window,
        ReceivedStats::Both,
    ] {
        let params = params_for_modes(mode, StampAt::None, Clock::Both);
        let peer = BackendPeer::start_open_echo(params.clone(), None);
        let mut client = Client::connect(config_for_params(peer.addr(), &params)).unwrap();

        client.open().unwrap();
        client.send_probe().unwrap();
        let events = client.recv_once().unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], ClientEvent::EchoReply { .. }));

        client.close().unwrap();
    }
}

#[test]
fn backend_timestamp_smoke() {
    for mode in [
        StampAt::None,
        StampAt::Send,
        StampAt::Receive,
        StampAt::Both,
        StampAt::Midpoint,
    ] {
        let params = params_for_modes(ReceivedStats::None, mode, Clock::Both);
        let peer = BackendPeer::start_open_echo(params.clone(), None);
        let mut client = Client::connect(config_for_params(peer.addr(), &params)).unwrap();

        client.open().unwrap();
        client.send_probe().unwrap();
        let events = client.recv_once().unwrap();
        assert_eq!(events.len(), 1);

        match &events[0] {
            ClientEvent::EchoReply {
                server_timing,
                one_way,
                ..
            } => match mode {
                StampAt::None => {
                    assert!(server_timing.is_none());
                    assert!(one_way.is_none());
                }
                StampAt::Send => {
                    let st = server_timing.as_ref().unwrap();
                    assert!(st.send_wall_ns.is_some() || st.send_mono_ns.is_some());
                }
                StampAt::Receive => {
                    let st = server_timing.as_ref().unwrap();
                    assert!(st.receive_wall_ns.is_some() || st.receive_mono_ns.is_some());
                }
                StampAt::Both => {
                    let st = server_timing.as_ref().unwrap();
                    assert!(st.receive_wall_ns.is_some() || st.receive_mono_ns.is_some());
                    assert!(st.send_wall_ns.is_some() || st.send_mono_ns.is_some());
                }
                StampAt::Midpoint => {
                    let st = server_timing.as_ref().unwrap();
                    assert!(st.midpoint_wall_ns.is_some() || st.midpoint_mono_ns.is_some());
                }
            },
            other => panic!("expected EchoReply, got {other:?}"),
        }

        client.close().unwrap();
    }
}

#[test]
fn backend_clock_smoke() {
    for clock in [Clock::Wall, Clock::Monotonic, Clock::Both] {
        let params = params_for_modes(ReceivedStats::None, StampAt::Both, clock);
        let peer = BackendPeer::start_open_echo(params.clone(), None);
        let mut client = Client::connect(config_for_params(peer.addr(), &params)).unwrap();

        client.open().unwrap();
        client.send_probe().unwrap();
        let events = client.recv_once().unwrap();
        assert_eq!(events.len(), 1);

        match &events[0] {
            ClientEvent::EchoReply { server_timing, .. } => {
                let st = server_timing.as_ref().unwrap();
                match clock {
                    Clock::Wall => {
                        assert!(st.receive_wall_ns.is_some());
                        assert!(st.send_wall_ns.is_some());
                    }
                    Clock::Monotonic => {
                        assert!(st.receive_mono_ns.is_some());
                        assert!(st.send_mono_ns.is_some());
                    }
                    Clock::Both => {
                        assert!(st.receive_wall_ns.is_some());
                        assert!(st.receive_mono_ns.is_some());
                        assert!(st.send_wall_ns.is_some());
                        assert!(st.send_mono_ns.is_some());
                    }
                }
            }
            other => panic!("expected EchoReply, got {other:?}"),
        }

        client.close().unwrap();
    }
}
