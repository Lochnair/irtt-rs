use std::{
    future::Future,
    net::UdpSocket,
    pin::Pin,
    sync::mpsc,
    task::{Context, Poll, Waker},
    thread::{self, JoinHandle},
    time::SystemTime,
};

use irtt_proto::{
    decode_request, encode_echo_reply, encode_open_reply, flags, verify_packet_hmac,
    DecodedRequestKind, EchoReply, OpenReply, Params, ReceivedStats, StampAt, TimestampFields,
};
use tokio::runtime::{Builder, Runtime};

#[path = "in_tree_server_support.rs"]
mod in_tree_server;

use super::*;
use crate::{
    probe::PendingProbe, socket_options::tokio_socket_traffic_class, Client, ClientTimestamp,
    RunMode, SocketConfig,
};
use in_tree_server::InTreeServer;
use irtt_server::ServerConfig;

const TOKEN: u64 = 0x1234_5678_90ab_cdef;

struct TestServer {
    addr: SocketAddr,
    packets: mpsc::Receiver<Vec<u8>>,
    done: JoinHandle<()>,
}

impl TestServer {
    fn finish(self) -> Vec<Vec<u8>> {
        self.done.join().unwrap();
        self.packets.try_iter().collect()
    }
}

fn runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .unwrap()
}

fn config(addr: SocketAddr, key: Option<Vec<u8>>, dscp: u8) -> ClientConfig {
    ClientConfig {
        server_addr: addr.to_string(),
        received_stats: ReceivedStats::None,
        stamp_at: StampAt::None,
        dscp,
        hmac_key: key,
        open_timeouts: vec![Duration::from_millis(200)],
        socket_config: SocketConfig {
            recv_timeout: Some(Duration::from_millis(200)),
            ..SocketConfig::default()
        },
        ..ClientConfig::default()
    }
}

async fn opened_client(addr: SocketAddr, dscp: u8) -> AsyncClient {
    let mut client = AsyncClient::connect(config(addr, None, dscp))
        .await
        .unwrap();
    client.open().await.unwrap();
    client
}

fn inject_send(client: &AsyncClient, result: InjectedSend) {
    client.test_hooks.sends.borrow_mut().push_back(result);
}

fn socket_dscp(client: &AsyncClient) -> u32 {
    tokio_socket_traffic_class(&client.socket, client.remote).unwrap() >> 2
}

fn start_server<F>(handler: F) -> TestServer
where
    F: FnOnce(UdpSocket, mpsc::Sender<Vec<u8>>) + Send + 'static,
{
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let addr = socket.local_addr().unwrap();
    let (tx, packets) = mpsc::channel();
    let done = thread::spawn(move || handler(socket, tx));
    TestServer {
        addr,
        packets,
        done,
    }
}

fn recv_packet(socket: &UdpSocket, tx: &mpsc::Sender<Vec<u8>>) -> (Vec<u8>, SocketAddr) {
    let mut buffer = [0_u8; 8192];
    let (len, peer) = socket.recv_from(&mut buffer).unwrap();
    let packet = buffer[..len].to_vec();
    tx.send(packet.clone()).unwrap();
    (packet, peer)
}

/// Admits a request the way a compliant server does: structural decode first,
/// then HMAC presence policy, then authentication. Returns `None` for anything
/// a server would silently discard.
fn decode_server_request<'a>(
    packet: &'a [u8],
    key: Option<&[u8]>,
) -> Option<DecodedRequestKind<'a>> {
    let request = decode_request(packet).ok()?;
    if request.hmac_present != key.is_some() {
        return None;
    }
    if let Some(key) = key {
        verify_packet_hmac(key, packet).ok()?;
    }
    Some(request.kind)
}

/// Returns the requested parameters and whether this was a no-test open.
fn open_request(packet: &[u8], key: Option<&[u8]>) -> (Params, bool) {
    match decode_server_request(packet, key) {
        Some(DecodedRequestKind::Open { params, no_test }) => {
            (Params::decode(params).unwrap(), no_test)
        }
        other => panic!("expected an authenticated open request, got {other:?}"),
    }
}

fn echo_request_sequence(packet: &[u8], key: Option<&[u8]>) -> u32 {
    match decode_server_request(packet, key) {
        Some(DecodedRequestKind::Echo { sequence, .. }) => sequence,
        other => panic!("expected an authenticated echo request, got {other:?}"),
    }
}

fn close_request_token(packet: &[u8], key: Option<&[u8]>) -> u64 {
    match decode_server_request(packet, key) {
        Some(DecodedRequestKind::Close { token }) => token,
        other => panic!("expected an authenticated close request, got {other:?}"),
    }
}

fn send_open_reply(
    socket: &UdpSocket,
    peer: SocketAddr,
    params: Params,
    key: Option<&[u8]>,
    reply_flags: u8,
    token: u64,
) {
    let reply = encode_open_reply(
        &OpenReply {
            flags: reply_flags,
            token,
            params,
        },
        key,
    )
    .unwrap();
    socket.send_to(&reply, peer).unwrap();
}

fn open_session(
    socket: &UdpSocket,
    tx: &mpsc::Sender<Vec<u8>>,
    key: Option<&[u8]>,
) -> (Params, SocketAddr) {
    let (packet, peer) = recv_packet(socket, tx);
    let (params, _) = open_request(&packet, key);
    send_open_reply(
        socket,
        peer,
        params.clone(),
        key,
        flags::FLAG_OPEN | flags::FLAG_REPLY,
        TOKEN,
    );
    (params, peer)
}

fn echo_reply(
    params: &Params,
    sequence: u32,
    token: u64,
    reply_flags: u8,
    key: Option<&[u8]>,
) -> Vec<u8> {
    encode_echo_reply(
        &EchoReply {
            flags: reply_flags,
            token,
            sequence,
            recv_count: None,
            recv_window: None,
            timestamps: TimestampFields::default(),
            payload: Vec::new(),
        },
        params,
        key,
    )
    .unwrap()
}

fn start_open_server(key: Option<Vec<u8>>, packets_after_open: usize) -> TestServer {
    start_server(move |socket, tx| {
        open_session(&socket, &tx, key.as_deref());
        for _ in 0..packets_after_open {
            let _ = recv_packet(&socket, &tx);
        }
    })
}

fn start_no_test_server() -> TestServer {
    start_server(move |socket, tx| {
        let (open_packet, peer) = recv_packet(&socket, &tx);
        let (params, no_test) = open_request(&open_packet, None);
        assert!(no_test);
        send_open_reply(
            &socket,
            peer,
            params,
            None,
            flags::FLAG_OPEN | flags::FLAG_REPLY | flags::FLAG_CLOSE,
            0,
        );
    })
}

fn start_peer_close_server() -> TestServer {
    start_server(move |socket, tx| {
        let (params, peer) = open_session(&socket, &tx, None);
        let (probe_packet, _) = recv_packet(&socket, &tx);
        let sequence = echo_request_sequence(&probe_packet, None);
        socket
            .send_to(
                &echo_reply(
                    &params,
                    sequence,
                    TOKEN,
                    flags::FLAG_REPLY | flags::FLAG_CLOSE,
                    None,
                ),
                peer,
            )
            .unwrap();
    })
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    let mut context = Context::from_waker(Waker::noop());
    Future::poll(future, &mut context)
}

fn poll_recv_once(client: &mut AsyncClient) -> Poll<Result<Vec<ClientEvent>, ClientError>> {
    let mut future = Box::pin(client.recv());
    poll_once(Pin::as_mut(&mut future))
}

#[test]
fn connect_requires_current_runtime_when_polled() {
    let mut future = Box::pin(AsyncClient::connect(ClientConfig::default()));
    assert!(matches!(
        poll_once(Pin::as_mut(&mut future)),
        Poll::Ready(Err(ClientError::NoTokioRuntime))
    ));
}

#[test]
fn recv_before_open_fails_on_first_poll_without_socket_readiness() {
    runtime().block_on(async {
        let remote = SocketAddr::from(([127, 0, 0, 1], 2112));
        let mut client = AsyncClient::connect(config(remote, None, 0)).await.unwrap();

        assert!(matches!(
            poll_recv_once(&mut client),
            Poll::Ready(Err(ClientError::NotOpen))
        ));
    });
}

#[test]
fn open_ignores_malformed_and_bad_hmac_without_resending() {
    let key = b"open-filter-key".to_vec();
    let server_key = key.clone();
    let server = start_server(move |socket, tx| {
        let (open_packet, peer) = recv_packet(&socket, &tx);
        let (params, _) = open_request(&open_packet, Some(&server_key));
        socket.send_to(&[0_u8], peer).unwrap();
        let mut bad_hmac = encode_open_reply(
            &OpenReply {
                flags: flags::FLAG_OPEN | flags::FLAG_REPLY,
                token: TOKEN,
                params: params.clone(),
            },
            Some(&server_key),
        )
        .unwrap();
        bad_hmac[4] ^= 0xff;
        socket.send_to(&bad_hmac, peer).unwrap();
        send_open_reply(
            &socket,
            peer,
            params,
            Some(&server_key),
            flags::FLAG_OPEN | flags::FLAG_REPLY,
            TOKEN,
        );
    });

    runtime().block_on(async {
        let mut client = AsyncClient::connect(config(server.addr, Some(key), 0))
            .await
            .unwrap();
        inject_send(&client, InjectedSend::WouldBlock);
        let attempts = client.test_hooks.send_attempts.get();
        assert!(matches!(
            client.open().await.unwrap(),
            OpenOutcome::Started { .. }
        ));
        assert_eq!(client.test_hooks.send_attempts.get(), attempts + 2);
        assert!(client.prepared_open.is_none());
        assert!(client.schedule.is_some());
    });
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn ignored_open_traffic_uses_one_request_and_deadline_per_attempt() {
    let server = start_server(move |socket, tx| {
        for _ in 0..2 {
            let (_, peer) = recv_packet(&socket, &tx);
            for _ in 0..3 {
                socket.send_to(&[0_u8], peer).unwrap();
            }
        }
    });
    let mut client_config = config(server.addr, None, 0);
    client_config.open_timeouts = vec![Duration::from_millis(200), Duration::from_millis(200)];

    runtime().block_on(async {
        let mut client = AsyncClient::connect(client_config).await.unwrap();
        assert!(matches!(client.open().await, Err(ClientError::OpenTimeout)));
        assert!(client.machine.prepare_open_request().is_ok());
        assert!(client.schedule.is_none());
    });
    assert_eq!(server.finish().len(), 2);
}

#[test]
fn opening_poll_budget_yields_then_public_open_completes() {
    let (queued, queue_ready) = mpsc::channel();
    let server = start_server(move |socket, tx| {
        let (open_packet, peer) = recv_packet(&socket, &tx);
        let (params, _) = open_request(&open_packet, None);
        for _ in 0..=OPEN_POLL_WORK_BUDGET {
            socket.send_to(&[0_u8], peer).unwrap();
        }
        send_open_reply(
            &socket,
            peer,
            params,
            None,
            flags::FLAG_OPEN | flags::FLAG_REPLY,
            TOKEN,
        );
        queued.send(()).unwrap();
    });

    runtime().block_on(async {
        let mut client = AsyncClient::connect(config(server.addr, None, 0))
            .await
            .unwrap();
        client.socket.writable().await.unwrap();
        client.test_hooks.pause_open_before_readable.set(true);
        let mut opening = Box::pin(client.open());

        // Submit the one opening request, then let the server queue all noise
        // before performing the explicit bounded poll below.
        assert!(matches!(
            poll_once(Pin::as_mut(&mut opening)),
            Poll::Pending
        ));
        queue_ready.recv_timeout(Duration::from_secs(2)).unwrap();

        // The valid reply is behind more ignored datagrams than one opening
        // poll may consume, so this cannot complete without the budget yield.
        assert!(matches!(
            poll_once(Pin::as_mut(&mut opening)),
            Poll::Pending
        ));
        assert!(matches!(
            opening.await.unwrap(),
            OpenOutcome::Started { .. }
        ));
    });
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn ignored_open_traffic_cannot_extend_the_absolute_attempt_deadline() {
    let server = start_server(move |socket, tx| {
        let (first_packet, peer) = recv_packet(&socket, &tx);
        let (params, _) = open_request(&first_packet, None);
        socket
            .set_read_timeout(Some(Duration::from_millis(10)))
            .unwrap();
        let started = Instant::now();
        let mut next_noise = started;
        let mut buffer = [0_u8; 512];

        while started.elapsed() < Duration::from_millis(350) {
            let now = Instant::now();
            if now >= next_noise {
                socket.send_to(&[0_u8], peer).unwrap();
                next_noise = now + Duration::from_millis(25);
            }
            match socket.recv_from(&mut buffer) {
                Ok((len, second_peer)) => {
                    tx.send(buffer[..len].to_vec()).unwrap();
                    send_open_reply(
                        &socket,
                        second_peer,
                        params,
                        None,
                        flags::FLAG_OPEN | flags::FLAG_REPLY,
                        TOKEN,
                    );
                    return;
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                    ) => {}
                Err(error) => panic!("{error}"),
            }
        }
        panic!("second open request did not arrive after the first absolute deadline");
    });
    let mut client_config = config(server.addr, None, 0);
    client_config.open_timeouts = vec![Duration::from_millis(200), Duration::from_millis(200)];

    runtime().block_on(async {
        let mut client = AsyncClient::connect(client_config).await.unwrap();
        assert!(matches!(
            client.open().await.unwrap(),
            OpenOutcome::Started { .. }
        ));
    });
    assert_eq!(server.finish().len(), 2);
}

#[test]
fn authenticated_rejection_is_terminal_without_retry() {
    let server = start_server(move |socket, tx| {
        let (open_packet, peer) = recv_packet(&socket, &tx);
        let (params, _) = open_request(&open_packet, None);
        send_open_reply(
            &socket,
            peer,
            params,
            None,
            flags::FLAG_OPEN | flags::FLAG_REPLY | flags::FLAG_CLOSE,
            0,
        );
    });
    let mut client_config = config(server.addr, None, 0);
    client_config.open_timeouts = vec![Duration::from_millis(200), Duration::from_millis(200)];

    runtime().block_on(async {
        let mut client = AsyncClient::connect(client_config).await.unwrap();
        assert!(matches!(
            client.open().await,
            Err(ClientError::ServerRejected)
        ));
        assert!(client.machine.prepare_open_request().is_ok());
    });
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn opening_cancellation_before_submission_is_transactional() {
    let server = start_server(move |socket, tx| {
        socket
            .set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        let mut buffer = [0_u8; 512];
        if let Ok((len, _)) = socket.recv_from(&mut buffer) {
            tx.send(buffer[..len].to_vec()).unwrap();
        }
    });

    runtime().block_on(async {
        let mut client = AsyncClient::connect(config(server.addr, None, 0))
            .await
            .unwrap();
        client.test_hooks.pause_open_before_writable.set(true);
        let mut open = Box::pin(client.open());
        assert!(matches!(poll_once(Pin::as_mut(&mut open)), Poll::Pending));
        drop(open);

        assert!(client.machine.prepare_open_request().is_ok());
        assert!(client.prepared_open.is_some());
        assert!(client.schedule.is_none());
        assert_eq!(client.applied_traffic_class, None);
    });
    assert!(server.finish().is_empty());
}

#[test]
fn opening_cancellation_after_submission_can_retry() {
    let server = start_server(move |socket, tx| {
        let (first, _) = recv_packet(&socket, &tx);
        let (first_params, _) = open_request(&first, None);
        let (_, peer) = recv_packet(&socket, &tx);
        send_open_reply(
            &socket,
            peer,
            first_params,
            None,
            flags::FLAG_OPEN | flags::FLAG_REPLY,
            TOKEN,
        );
    });

    runtime().block_on(async {
        let mut client = AsyncClient::connect(config(server.addr, None, 0))
            .await
            .unwrap();
        client.socket.writable().await.unwrap();
        client.test_hooks.pause_open_before_readable.set(true);
        let mut open = Box::pin(client.open());
        assert!(matches!(poll_once(Pin::as_mut(&mut open)), Poll::Pending));
        drop(open);

        assert!(client.machine.prepare_open_request().is_ok());
        assert!(client.prepared_open.is_some());
        assert!(client.schedule.is_none());
        assert_eq!(client.applied_traffic_class, None);
        assert!(matches!(
            client.open().await.unwrap(),
            OpenOutcome::Started { .. }
        ));
    });
    assert_eq!(server.finish().len(), 2);
}

#[test]
fn adapter_failure_sends_preencoded_cleanup_without_committing_open() {
    let server = start_server(move |socket, tx| {
        open_session(&socket, &tx, None);
        let (close, _) = recv_packet(&socket, &tx);
        assert_eq!(close_request_token(&close, None), TOKEN);
    });

    runtime().block_on(async {
        let mut client = AsyncClient::connect(config(server.addr, None, 46))
            .await
            .unwrap();
        client.test_hooks.fail_open_dscp.set(true);
        assert!(matches!(
            client.open().await,
            Err(ClientError::SocketOption { .. })
        ));
        assert!(client.machine.prepare_open_request().is_ok());
        assert!(client.schedule.is_none());
        assert_eq!(client.applied_traffic_class, None);
    });
    assert_eq!(server.finish().len(), 2);
}

#[test]
fn cleanup_failure_preserves_primary_open_error() {
    let server = start_server(move |socket, tx| {
        open_session(&socket, &tx, None);
    });

    runtime().block_on(async {
        let mut client = AsyncClient::connect(config(server.addr, None, 46))
            .await
            .unwrap();
        client.test_hooks.fail_open_dscp.set(true);
        client.test_hooks.fail_cleanup_send.set(true);
        assert!(matches!(
            client.open().await,
            Err(ClientError::SocketOption {
                operation: "set negotiated DSCP",
                ..
            })
        ));
        assert!(client.machine.prepare_open_request().is_ok());
    });
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn no_test_open_has_no_schedule_or_dscp_state() {
    let server = start_no_test_server();
    runtime().block_on(async {
        let mut client_config = config(server.addr, None, 46);
        client_config.run_mode = RunMode::NoTest;
        let mut client = AsyncClient::connect(client_config).await.unwrap();

        assert!(matches!(
            client.open().await.unwrap(),
            OpenOutcome::NoTestCompleted { .. }
        ));
        assert!(client.prepared_open.is_none());
        assert!(client.schedule.is_none());
        assert_eq!(client.applied_traffic_class, None);
        assert!(client.is_run_complete());
        assert!(matches!(
            client.close().await,
            Err(ClientError::AlreadyCompleted)
        ));
    });
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn probe_cancellation_would_block_and_error_retain_one_preparation() {
    let server = start_open_server(None, 1);
    runtime().block_on(async {
        let mut client = opened_client(server.addr, 0).await;
        let deadline = client.next_send_deadline();
        let attempts = client.test_hooks.send_attempts.get();

        client.test_hooks.pause_probe_before_writable.set(true);
        let mut send = Box::pin(client.send_probe());
        assert!(matches!(poll_once(Pin::as_mut(&mut send)), Poll::Pending));
        drop(send);

        let retained = client.prepared_probe.as_ref().unwrap().bytes.clone();
        let retained_ptr = client.prepared_probe.as_ref().unwrap().bytes.as_ptr();
        assert_eq!(client.machine.packets_sent(), 0);
        assert!(client.machine.pending_is_empty());
        assert_eq!(client.next_send_deadline(), deadline);
        assert_eq!(client.test_hooks.send_attempts.get(), attempts);

        client
            .test_hooks
            .sends
            .borrow_mut()
            .extend([InjectedSend::WouldBlock, InjectedSend::Error]);
        assert!(matches!(
            client.send_probe().await,
            Err(ClientError::Socket(_))
        ));
        assert_eq!(client.prepared_probe.as_ref().unwrap().bytes, retained);
        assert_eq!(
            client.prepared_probe.as_ref().unwrap().bytes.as_ptr(),
            retained_ptr
        );
        assert_eq!(client.machine.packets_sent(), 0);
        assert!(client.machine.pending_is_empty());
        assert_eq!(client.next_send_deadline(), deadline);

        assert!(matches!(
            client.send_probe().await.unwrap().as_slice(),
            [ClientEvent::EchoSent { seq: 0, .. }]
        ));
        assert_eq!(client.machine.packets_sent(), 1);
        assert!(client.prepared_probe.is_none());
    });
    assert_eq!(server.finish().len(), 2);
}

#[test]
fn probe_false_readiness_resamples_all_timestamps() {
    let server = start_open_server(None, 1);
    runtime().block_on(async {
        let mut client = opened_client(server.addr, 0).await;
        let scheduled_at = client.next_send_deadline().unwrap();
        let first_sent = ClientTimestamp {
            wall: SystemTime::UNIX_EPOCH + Duration::from_secs(1),
            mono: scheduled_at + Duration::from_millis(3),
        };
        let second_sent = ClientTimestamp {
            wall: SystemTime::UNIX_EPOCH + Duration::from_secs(2),
            mono: scheduled_at + Duration::from_millis(11),
        };
        client.test_hooks.probe_timestamps.borrow_mut().extend([
            ProbeSendTimestamps {
                permission_at: scheduled_at + Duration::from_millis(1),
                send_anchor: first_sent,
                sent_at: first_sent,
                send_call_start: scheduled_at + Duration::from_millis(4),
                send_finished_at: scheduled_at + Duration::from_millis(6),
            },
            ProbeSendTimestamps {
                permission_at: scheduled_at + Duration::from_millis(8),
                send_anchor: second_sent,
                sent_at: second_sent,
                send_call_start: scheduled_at + Duration::from_millis(12),
                send_finished_at: scheduled_at + Duration::from_millis(17),
            },
        ]);
        inject_send(&client, InjectedSend::WouldBlock);

        let events = client.send_probe().await.unwrap();
        assert!(matches!(
            events.as_slice(),
            [ClientEvent::EchoSent {
                scheduled_at: event_scheduled,
                sent_at,
                send_call,
                timer_error,
                ..
            }] if *event_scheduled == scheduled_at
                && *sent_at == second_sent
                && *send_call == Duration::from_millis(5)
                && *timer_error == Duration::from_millis(11)
        ));
        assert!(matches!(
            client
                .poll_timeouts_at(second_sent.mono + client.probe_timeout())
                .unwrap()
                .as_slice(),
            [ClientEvent::EchoLoss { sent_at, .. }] if *sent_at == second_sent
        ));
    });
    assert_eq!(server.finish().len(), 2);
}

#[test]
fn public_timeout_polling_remains_exhaustive() {
    let server = start_open_server(None, 0);
    runtime().block_on(async {
        let mut client = opened_client(server.addr, 0).await;
        let now = Instant::now();
        for seq in 0..3 {
            client.replace_pending_for_test(PendingProbe {
                wire_seq: seq,
                sent_at: ClientTimestamp {
                    mono: now - Duration::from_secs(2),
                    wall: SystemTime::UNIX_EPOCH,
                },
                timeout_at: now - Duration::from_secs(1),
                tx_not_before_wall: SystemTime::UNIX_EPOCH,
                kernel_tx_timestamp: None,
            });
        }

        assert!(matches!(
            client.poll_timeouts_at(now).unwrap().as_slice(),
            [
                ClientEvent::EchoLoss { seq: 0, .. },
                ClientEvent::EchoLoss { seq: 1, .. },
                ClientEvent::EchoLoss { seq: 2, .. },
            ]
        ));
    });
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn successful_short_probe_commits_before_length_error() {
    let server = start_open_server(None, 0);
    runtime().block_on(async {
        let mut client = opened_client(server.addr, 0).await;
        client.test_hooks.pause_probe_before_writable.set(true);
        let mut send = Box::pin(client.send_probe());
        assert!(matches!(poll_once(Pin::as_mut(&mut send)), Poll::Pending));
        drop(send);
        let probe_len = client.prepared_probe.as_ref().unwrap().bytes.len();
        let deadline = client.next_send_deadline().unwrap();
        inject_send(&client, InjectedSend::ReportedLength(probe_len - 1));

        assert!(matches!(
            client.send_probe().await,
            Err(ClientError::DatagramLengthMismatch { .. })
        ));
        assert_eq!(client.machine.packets_sent(), 1);
        assert!(!client.machine.pending_is_empty());
        assert!(client.prepared_probe.is_none());
        assert_eq!(
            client.next_send_deadline(),
            Some(deadline + client.machine.config().interval)
        );
    });
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn stale_retained_probe_is_rejected_before_send() {
    let server = start_open_server(None, 0);
    runtime().block_on(async {
        let mut client = opened_client(server.addr, 0).await;
        client.test_hooks.pause_probe_before_writable.set(true);
        let mut send = Box::pin(client.send_probe());
        assert!(matches!(poll_once(Pin::as_mut(&mut send)), Poll::Pending));
        drop(send);

        let advanced = client.machine.prepare_probe().unwrap().unwrap();
        let preflight = client.machine.preflight_probe_commit(&advanced).unwrap();
        let commit = client
            .machine
            .finalize_probe_commit(preflight, ClientTimestamp::now())
            .unwrap();
        client
            .machine
            .commit_probe_sent(commit, ClientTimestamp::now(), advanced.bytes.len());
        let attempts = client.test_hooks.send_attempts.get();

        assert!(matches!(
            client.send_probe().await,
            Err(ClientError::StalePreparedProbe { .. })
        ));
        assert_eq!(client.test_hooks.send_attempts.get(), attempts);
        assert!(client.prepared_probe.is_some());
    });
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn finite_duration_boundary_clears_preparation_without_send() {
    let server = start_open_server(None, 0);
    runtime().block_on(async {
        let mut client_config = config(server.addr, None, 0);
        client_config.duration = Some(Duration::from_millis(20));
        let mut client = AsyncClient::connect(client_config).await.unwrap();
        client.open().await.unwrap();
        let start = client.next_send_deadline().unwrap();
        let attempts = client.test_hooks.send_attempts.get();
        client
            .test_hooks
            .probe_timestamps
            .borrow_mut()
            .push_back(ProbeSendTimestamps {
                permission_at: start + Duration::from_millis(20),
                send_anchor: ClientTimestamp {
                    wall: SystemTime::UNIX_EPOCH,
                    mono: start + Duration::from_millis(21),
                },
                sent_at: ClientTimestamp {
                    wall: SystemTime::UNIX_EPOCH,
                    mono: start + Duration::from_millis(21),
                },
                send_call_start: start + Duration::from_millis(22),
                send_finished_at: start + Duration::from_millis(23),
            });

        assert!(client.send_probe().await.unwrap().is_empty());
        assert!(client.prepared_probe.is_none());
        assert_eq!(client.machine.packets_sent(), 0);
        assert_eq!(client.test_hooks.send_attempts.get(), attempts);
        assert!(client.is_run_complete());
    });
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn receive_retries_false_readiness_and_shares_packet_classification() {
    let server = start_server(move |socket, tx| {
        let (params, peer) = open_session(&socket, &tx, None);
        let (probe_packet, _) = recv_packet(&socket, &tx);
        let sequence = echo_request_sequence(&probe_packet, None);
        socket
            .send_to(
                &echo_reply(&params, sequence, TOKEN, flags::FLAG_REPLY, None),
                peer,
            )
            .unwrap();
    });

    runtime().block_on(async {
        let mut client = opened_client(server.addr, 0).await;
        client.send_probe().await.unwrap();
        client.test_hooks.receive_would_block.set(2);

        assert!(matches!(
            client.recv().await.unwrap().as_slice(),
            [ClientEvent::EchoReply { seq: 0, .. }]
        ));
    });
    assert_eq!(server.finish().len(), 2);
}

#[test]
fn receive_retries_interrupted_syscall_and_shares_packet_classification() {
    let server = start_server(move |socket, tx| {
        let (params, peer) = open_session(&socket, &tx, None);
        let (probe_packet, _) = recv_packet(&socket, &tx);
        let sequence = echo_request_sequence(&probe_packet, None);
        socket
            .send_to(
                &echo_reply(&params, sequence, TOKEN, flags::FLAG_REPLY, None),
                peer,
            )
            .unwrap();
    });

    runtime().block_on(async {
        let mut client = opened_client(server.addr, 0).await;
        client.send_probe().await.unwrap();
        client.test_hooks.inject_recv_interrupted.set(2);

        assert!(matches!(
            client.recv().await.unwrap().as_slice(),
            [ClientEvent::EchoReply { seq: 0, .. }]
        ));
    });
    assert_eq!(server.finish().len(), 2);
}

#[test]
fn open_retries_interrupted_syscall_within_the_same_attempt() {
    let server = start_open_server(None, 0);
    runtime().block_on(async {
        let mut client = AsyncClient::connect(config(server.addr, None, 0))
            .await
            .unwrap();
        client.test_hooks.inject_recv_interrupted.set(3);

        assert!(matches!(
            client.open().await,
            Ok(OpenOutcome::Started { .. })
        ));
        // Only one open request reached the server: interruptions were
        // absorbed inside the single open attempt, with no retransmit.
        assert_eq!(client.test_hooks.send_attempts.get(), 1);
    });
    server.finish();
}

#[test]
fn peer_close_dscp_failure_preserves_events_and_logical_ownership() {
    let server = start_peer_close_server();
    runtime().block_on(async {
        let mut client = opened_client(server.addr, 46).await;
        client.send_probe().await.unwrap();
        client.prepared_probe = client.machine.prepare_probe().unwrap();
        client.test_hooks.fail_peer_close_dscp.set(true);

        assert!(matches!(
            client.recv().await.unwrap().as_slice(),
            [
                ClientEvent::EchoReply { .. },
                ClientEvent::SessionClosed { .. }
            ]
        ));
        assert!(client.is_peer_closed());
        assert!(client.is_run_complete());
        assert!(client.schedule.is_none());
        assert!(client.prepared_probe.is_none());
        assert_eq!(client.applied_traffic_class, Some(184));
        assert_eq!(client.next_send_deadline(), None);
        assert_eq!(socket_dscp(&client), 46);
    });
    assert_eq!(server.finish().len(), 2);
}

#[test]
fn recv_after_local_close_and_no_test_fails_on_first_poll() {
    let close_server = start_server(move |socket, tx| {
        open_session(&socket, &tx, None);
        let (close_packet, _) = recv_packet(&socket, &tx);
        assert_eq!(close_request_token(&close_packet, None), TOKEN);
    });
    runtime().block_on(async {
        let mut client = opened_client(close_server.addr, 0).await;
        client.close().await.unwrap();
        assert!(matches!(
            poll_recv_once(&mut client),
            Poll::Ready(Err(ClientError::AlreadyClosed))
        ));
    });
    assert_eq!(close_server.finish().len(), 2);

    let no_test_server = start_no_test_server();
    runtime().block_on(async {
        let mut client_config = config(no_test_server.addr, None, 0);
        client_config.run_mode = RunMode::NoTest;
        let mut client = AsyncClient::connect(client_config).await.unwrap();
        client.open().await.unwrap();
        assert!(matches!(
            poll_recv_once(&mut client),
            Poll::Ready(Err(ClientError::AlreadyCompleted))
        ));
    });
    assert_eq!(no_test_server.finish().len(), 1);
}

#[test]
fn close_cancellation_and_would_block_restore_dscp_before_suspend() {
    let server = start_open_server(None, 1);
    runtime().block_on(async {
        let mut client = opened_client(server.addr, 46).await;
        let deadline = client.next_send_deadline();
        let attempts = client.test_hooks.send_attempts.get();
        assert_eq!(socket_dscp(&client), 46);

        client.test_hooks.pause_close_before_writable.set(true);
        let mut close = Box::pin(client.close());
        assert!(matches!(poll_once(Pin::as_mut(&mut close)), Poll::Pending));
        drop(close);
        assert!(client.machine.is_open());
        assert!(client.schedule.is_some());
        assert_eq!(client.next_send_deadline(), deadline);
        assert_eq!(client.applied_traffic_class, Some(184));
        assert_eq!(client.test_hooks.send_attempts.get(), attempts);
        assert_eq!(socket_dscp(&client), 46);

        inject_send(&client, InjectedSend::WouldBlock);
        client.test_hooks.pause_close_after_would_block.set(true);
        let mut close = Box::pin(client.close());
        assert!(matches!(poll_once(Pin::as_mut(&mut close)), Poll::Pending));
        drop(close);
        assert!(client.machine.is_open());
        assert!(client.schedule.is_some());
        assert_eq!(client.applied_traffic_class, Some(184));
        assert_eq!(socket_dscp(&client), 46);

        assert!(matches!(
            client.close().await.unwrap().as_slice(),
            [ClientEvent::SessionClosed { .. }]
        ));
        assert!(client.schedule.is_none());
        assert_eq!(client.applied_traffic_class, None);
    });
    assert_eq!(server.finish().len(), 2);
}

#[test]
fn close_errors_preserve_open_state_and_primary_send_error() {
    let server = start_open_server(None, 0);
    runtime().block_on(async {
        let mut client = opened_client(server.addr, 46).await;
        let deadline = client.next_send_deadline();

        inject_send(&client, InjectedSend::Error);
        assert!(matches!(client.close().await, Err(ClientError::Socket(_))));
        assert!(client.machine.is_open());
        assert!(client.schedule.is_some());
        assert_eq!(client.next_send_deadline(), deadline);
        assert_eq!(client.applied_traffic_class, Some(184));
        assert_eq!(socket_dscp(&client), 46);

        client.test_hooks.fail_dscp_restore.set(true);
        inject_send(&client, InjectedSend::Error);
        assert!(matches!(client.close().await, Err(ClientError::Socket(_))));
        assert!(client.machine.is_open());
        assert!(client.schedule.is_some());
    });
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn close_would_block_restore_failure_stops_without_committing() {
    let server = start_open_server(None, 0);
    runtime().block_on(async {
        let mut client = opened_client(server.addr, 46).await;
        client.test_hooks.fail_dscp_restore.set(true);
        inject_send(&client, InjectedSend::WouldBlock);

        assert!(matches!(
            client.close().await,
            Err(ClientError::SocketOption {
                operation: "restore negotiated DSCP",
                ..
            })
        ));
        assert!(client.machine.is_open());
        assert!(client.schedule.is_some());
        assert_eq!(client.applied_traffic_class, Some(184));
    });
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn successful_short_close_commits_before_length_error() {
    let server = start_open_server(None, 0);
    runtime().block_on(async {
        let mut client = opened_client(server.addr, 0).await;
        client.test_hooks.pause_probe_before_writable.set(true);
        let mut send = Box::pin(client.send_probe());
        assert!(matches!(poll_once(Pin::as_mut(&mut send)), Poll::Pending));
        drop(send);
        assert!(client.prepared_probe.is_some());

        let close_len = client.machine.prepare_close().unwrap().bytes.len();
        inject_send(&client, InjectedSend::ReportedLength(close_len - 1));

        assert!(matches!(
            client.close().await,
            Err(ClientError::DatagramLengthMismatch { .. })
        ));
        assert!(!client.machine.is_open());
        assert!(client.schedule.is_none());
        assert!(client.prepared_probe.is_none());
        assert_eq!(client.applied_traffic_class, None);
        assert!(matches!(
            client.machine.prepare_close(),
            Err(ClientError::AlreadyClosed)
        ));
    });
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn successful_close_uses_post_acceptance_timestamp() {
    let server = start_open_server(None, 1);
    runtime().block_on(async {
        let mut client = opened_client(server.addr, 0).await;
        let close_sent_at = ClientTimestamp {
            wall: SystemTime::UNIX_EPOCH + Duration::from_secs(44),
            mono: Instant::now() + Duration::from_secs(1),
        };
        client.test_hooks.close_sent_at.set(Some(close_sent_at));

        assert!(matches!(
            client.close().await.unwrap().as_slice(),
            [ClientEvent::SessionClosed { at, .. }] if *at == close_sent_at
        ));
    });
    assert_eq!(server.finish().len(), 2);
}

#[test]
fn blocking_and_async_hmac_dscp_lifecycle_are_semantically_equivalent() {
    let key = b"async-equivalence-key".to_vec();
    let blocking_server = InTreeServer::start(ServerConfig::default().with_hmac_key(key.clone()));
    let mut blocking =
        Client::connect(config(blocking_server.addr, Some(key.clone()), 46)).unwrap();
    let blocking_open = blocking.open().unwrap();
    let blocking_sent = blocking.send_probe().unwrap();
    let blocking_reply = blocking.recv_once().unwrap();
    let blocking_close = blocking.close().unwrap();
    drop(blocking_server);

    let async_server = InTreeServer::start(ServerConfig::default().with_hmac_key(key.clone()));
    let (async_open, async_sent, async_reply, async_close) = runtime().block_on(async {
        let mut client = AsyncClient::connect(config(async_server.addr, Some(key), 46))
            .await
            .unwrap();
        let opened = client.open().await.unwrap();
        assert_eq!(socket_dscp(&client), 46);
        let sent = client.send_probe().await.unwrap();
        let reply = client.recv().await.unwrap();
        let closed = client.close().await.unwrap();
        (opened, sent, reply, closed)
    });
    drop(async_server);

    assert_eq!(
        open_negotiated(&blocking_open),
        open_negotiated(&async_open)
    );
    assert_eq!(
        open_negotiated(&async_open).params.dscp,
        184,
        "negotiated Params::dscp is the raw wire byte for codepoint 46"
    );
    assert_matching_event_shape(&blocking_sent[0], &async_sent[0]);
    assert_matching_event_shape(&blocking_reply[0], &async_reply[0]);
    assert!(matches!(
        blocking_close.as_slice(),
        [ClientEvent::SessionClosed { token, .. }] if *token == open_token(&blocking_open)
    ));
    assert!(matches!(
        async_close.as_slice(),
        [ClientEvent::SessionClosed { token, .. }] if *token == open_token(&async_open)
    ));
    assert_matching_event_shape(&blocking_close[0], &async_close[0]);
}

#[test]
fn blocking_and_async_no_test_are_semantically_equivalent() {
    let blocking_no_test_server = InTreeServer::start(ServerConfig::default());
    let mut blocking_no_test_config = config(blocking_no_test_server.addr, None, 0);
    blocking_no_test_config.run_mode = RunMode::NoTest;
    let mut blocking_no_test = Client::connect(blocking_no_test_config).unwrap();
    let blocking_no_test_open = blocking_no_test.open().unwrap();
    assert!(blocking_no_test.is_run_complete());
    drop(blocking_no_test_server);

    let async_no_test_server = InTreeServer::start(ServerConfig::default());
    let mut async_no_test_config = config(async_no_test_server.addr, None, 0);
    async_no_test_config.run_mode = RunMode::NoTest;
    let async_no_test_open = runtime().block_on(async {
        let mut client = AsyncClient::connect(async_no_test_config).await.unwrap();
        let opened = client.open().await.unwrap();
        assert!(client.is_run_complete());
        opened
    });
    drop(async_no_test_server);
    assert!(matches!(
        blocking_no_test_open,
        OpenOutcome::NoTestCompleted { .. }
    ));
    assert!(matches!(
        async_no_test_open,
        OpenOutcome::NoTestCompleted { .. }
    ));
    assert_eq!(
        open_negotiated(&blocking_no_test_open),
        open_negotiated(&async_no_test_open)
    );
}

#[test]
fn blocking_and_async_peer_close_are_semantically_equivalent() {
    let blocking_peer_close_server = start_peer_close_server();
    let mut blocking_peer_close =
        Client::connect(config(blocking_peer_close_server.addr, None, 0)).unwrap();
    blocking_peer_close.open().unwrap();
    blocking_peer_close.send_probe().unwrap();
    let blocking_peer_close_events = blocking_peer_close.recv_once().unwrap();
    assert!(blocking_peer_close.is_peer_closed());
    assert_eq!(blocking_peer_close_server.finish().len(), 2);

    let async_peer_close_server = start_peer_close_server();
    let async_peer_close_events = runtime().block_on(async {
        let mut client = AsyncClient::connect(config(async_peer_close_server.addr, None, 0))
            .await
            .unwrap();
        client.open().await.unwrap();
        client.send_probe().await.unwrap();
        let events = client.recv().await.unwrap();
        assert!(client.is_peer_closed());
        events
    });
    assert_eq!(async_peer_close_server.finish().len(), 2);
    assert_eq!(
        blocking_peer_close_events.len(),
        async_peer_close_events.len()
    );
    for (blocking, asynchronous) in blocking_peer_close_events
        .iter()
        .zip(&async_peer_close_events)
    {
        assert_matching_event_shape(blocking, asynchronous);
    }
}

fn open_negotiated(outcome: &OpenOutcome) -> &crate::NegotiatedParams {
    match outcome {
        OpenOutcome::Started { negotiated, .. }
        | OpenOutcome::NoTestCompleted { negotiated, .. } => negotiated,
    }
}

fn open_token(outcome: &OpenOutcome) -> u64 {
    match outcome {
        OpenOutcome::Started { token, .. } => *token,
        OpenOutcome::NoTestCompleted { .. } => panic!("no-test open has no session token"),
    }
}

fn assert_matching_event_shape(blocking: &ClientEvent, asynchronous: &ClientEvent) {
    assert_eq!(event_shape(blocking), event_shape(asynchronous));
}

fn event_shape(event: &ClientEvent) -> (&'static str, u64, usize) {
    match event {
        ClientEvent::EchoSent { seq, bytes, .. } => ("sent", u64::from(*seq), *bytes),
        ClientEvent::EchoReply { seq, bytes, .. } => ("reply", u64::from(*seq), *bytes),
        ClientEvent::SessionClosed { .. } => ("closed", 0, 0),
        other => panic!("unexpected equivalence event: {other:?}"),
    }
}

/// A peer that ignores the first `ignored_requests` datagrams before serving a
/// compliant open, one echo, and one close.
///
/// Silence is the point here, so this stays a narrow fake peer rather than a
/// real `irtt-server`: a compliant server has no way to be deliberately mute
/// for exactly one open attempt.
fn start_silent_then_open_server(ignored_requests: usize) -> TestServer {
    start_server(move |socket, tx| {
        for _ in 0..ignored_requests {
            let _ = recv_packet(&socket, &tx);
        }
        let (params, peer) = open_session(&socket, &tx, None);
        let (probe_packet, _) = recv_packet(&socket, &tx);
        let sequence = echo_request_sequence(&probe_packet, None);
        socket
            .send_to(
                &echo_reply(&params, sequence, TOKEN, flags::FLAG_REPLY, None),
                peer,
            )
            .unwrap();
        let (close_packet, _) = recv_packet(&socket, &tx);
        assert_eq!(close_request_token(&close_packet, None), TOKEN);
    })
}

/// Stable name for a `ClientError` variant, for comparing what each driver
/// promises rather than how it produced it.
fn error_name(error: &ClientError) -> &'static str {
    match error {
        ClientError::OpenTimeout => "open timeout",
        ClientError::NotOpen => "not open",
        ClientError::AlreadyOpen => "already open",
        ClientError::AlreadyClosed => "already closed",
        ClientError::AlreadyCompleted => "already completed",
        ClientError::ServerRejected => "server rejected",
        other => panic!("unexpected conformance error: {other:?}"),
    }
}

fn probe_sequences(events: &[ClientEvent]) -> Vec<u32> {
    events
        .iter()
        .filter_map(|event| match event {
            ClientEvent::EchoSent { seq, .. } => Some(*seq),
            _ => None,
        })
        .collect()
}

fn reply_sequences(events: &[ClientEvent]) -> Vec<u32> {
    events
        .iter()
        .filter_map(|event| match event {
            ClientEvent::EchoReply { seq, .. } => Some(*seq),
            _ => None,
        })
        .collect()
}

#[test]
fn blocking_and_async_roll_back_a_failed_open_and_accept_a_retry() {
    // One open attempt is ignored, so the first open times out and the second
    // must be able to open the same connected client.
    let blocking_server = start_silent_then_open_server(1);
    let mut blocking = Client::connect(config(blocking_server.addr, None, 0)).unwrap();
    let blocking_first = blocking.open().unwrap_err();
    let blocking_second = blocking.open().unwrap();
    let blocking_sent = blocking.send_probe().unwrap();
    let blocking_reply = blocking.recv_once().unwrap();
    let blocking_close = blocking.close().unwrap();
    assert_eq!(blocking_server.finish().len(), 4);

    let async_server = start_silent_then_open_server(1);
    let (async_first, async_second, async_sent, async_reply, async_close) =
        runtime().block_on(async {
            let mut client = AsyncClient::connect(config(async_server.addr, None, 0))
                .await
                .unwrap();
            let first = client.open().await.unwrap_err();
            let second = client.open().await.unwrap();
            let sent = client.send_probe().await.unwrap();
            let reply = client.recv().await.unwrap();
            let closed = client.close().await.unwrap();
            (first, second, sent, reply, closed)
        });
    assert_eq!(async_server.finish().len(), 4);

    // Both drivers promise the same failure, and neither leaves the client in a
    // state that refuses the retry.
    assert_eq!(error_name(&blocking_first), "open timeout");
    assert_eq!(error_name(&blocking_first), error_name(&async_first));
    assert!(matches!(blocking_second, OpenOutcome::Started { .. }));
    assert!(matches!(async_second, OpenOutcome::Started { .. }));
    assert_eq!(
        open_negotiated(&blocking_second),
        open_negotiated(&async_second)
    );
    assert_eq!(open_token(&blocking_second), open_token(&async_second));

    // The retried session starts a fresh probe sequence in both drivers.
    assert_eq!(probe_sequences(&blocking_sent), [0]);
    assert_eq!(probe_sequences(&async_sent), [0]);
    assert_eq!(reply_sequences(&blocking_reply), [0]);
    assert_eq!(reply_sequences(&async_reply), [0]);
    assert_eq!(blocking_sent.len(), async_sent.len());
    assert_eq!(blocking_reply.len(), async_reply.len());
    assert_eq!(blocking_close.len(), async_close.len());
    assert_matching_event_shape(&blocking_close[0], &async_close[0]);
}

#[test]
fn blocking_and_async_complete_every_caller_paced_probe() {
    const PROBES: u32 = 3;

    let blocking_server = InTreeServer::start(ServerConfig::default());
    let mut blocking = Client::connect(config(blocking_server.addr, None, 0)).unwrap();
    blocking.open().unwrap();
    let mut blocking_sent = Vec::new();
    let mut blocking_replies = Vec::new();
    for _ in 0..PROBES {
        blocking_sent.extend(blocking.send_probe().unwrap());
        blocking_replies.extend(blocking.recv_once().unwrap());
    }
    blocking.close().unwrap();
    drop(blocking_server);

    let async_server = InTreeServer::start(ServerConfig::default());
    let (async_sent, async_replies) = runtime().block_on(async {
        let mut client = AsyncClient::connect(config(async_server.addr, None, 0))
            .await
            .unwrap();
        client.open().await.unwrap();
        let mut sent = Vec::new();
        let mut replies = Vec::new();
        for _ in 0..PROBES {
            sent.extend(client.send_probe().await.unwrap());
            replies.extend(client.recv().await.unwrap());
        }
        client.close().await.unwrap();
        (sent, replies)
    });
    drop(async_server);

    let expected: Vec<u32> = (0..PROBES).collect();
    assert_eq!(probe_sequences(&blocking_sent), expected);
    assert_eq!(probe_sequences(&async_sent), expected);
    assert_eq!(reply_sequences(&blocking_replies), expected);
    assert_eq!(reply_sequences(&async_replies), expected);
    // Compare the whole emitted streams, not just the probe and reply events
    // the sequence helpers keep: an extra event on either side is a difference.
    assert_eq!(blocking_sent.len(), async_sent.len());
    assert_eq!(blocking_replies.len(), async_replies.len());
    for (blocking, asynchronous) in blocking_sent.iter().zip(&async_sent) {
        assert_matching_event_shape(blocking, asynchronous);
    }
    for (blocking, asynchronous) in blocking_replies.iter().zip(&async_replies) {
        assert_matching_event_shape(blocking, asynchronous);
    }
}

#[test]
fn blocking_and_async_reject_probes_before_open_and_after_close() {
    let blocking_server = InTreeServer::start(ServerConfig::default());
    let mut blocking = Client::connect(config(blocking_server.addr, None, 0)).unwrap();
    let blocking_before = blocking.send_probe().unwrap_err();
    blocking.open().unwrap();
    let blocking_reopen = blocking.open().unwrap_err();
    blocking.close().unwrap();
    let blocking_after_send = blocking.send_probe().unwrap_err();
    let blocking_after_close = blocking.close().unwrap_err();
    drop(blocking_server);

    let async_server = InTreeServer::start(ServerConfig::default());
    let (async_before, async_reopen, async_after_send, async_after_close) =
        runtime().block_on(async {
            let mut client = AsyncClient::connect(config(async_server.addr, None, 0))
                .await
                .unwrap();
            let before = client.send_probe().await.unwrap_err();
            client.open().await.unwrap();
            let reopen = client.open().await.unwrap_err();
            client.close().await.unwrap();
            let after_send = client.send_probe().await.unwrap_err();
            let after_close = client.close().await.unwrap_err();
            (before, reopen, after_send, after_close)
        });
    drop(async_server);

    assert_eq!(error_name(&blocking_before), "not open");
    assert_eq!(error_name(&blocking_reopen), "already open");
    assert_eq!(error_name(&blocking_after_send), "already closed");
    assert_eq!(error_name(&blocking_after_close), "already closed");
    assert_eq!(error_name(&blocking_before), error_name(&async_before));
    assert_eq!(error_name(&blocking_reopen), error_name(&async_reopen));
    assert_eq!(
        error_name(&blocking_after_send),
        error_name(&async_after_send)
    );
    assert_eq!(
        error_name(&blocking_after_close),
        error_name(&async_after_close)
    );
}
