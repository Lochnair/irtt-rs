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
    decode_close_request, decode_echo_request, decode_open_request, encode_echo_reply,
    encode_open_reply, flags, EchoReply, OpenReply, Params, ReceivedStats, StampAt,
    TimestampFields,
};
use tokio::runtime::{Builder, Runtime};

use super::*;
use crate::{
    socket::resolution_call_counts, socket_options::tokio_socket_traffic_class, Client, RunMode,
    SocketConfig, WarningKind,
};

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
        let (open_packet, peer) = recv_packet(&socket, &tx);
        let request = decode_open_request(&open_packet, key.as_deref()).unwrap();
        send_open_reply(
            &socket,
            peer,
            request.params,
            key.as_deref(),
            flags::FLAG_OPEN | flags::FLAG_REPLY,
            TOKEN,
        );
        for _ in 0..packets_after_open {
            let _ = recv_packet(&socket, &tx);
        }
    })
}

fn start_no_test_server() -> TestServer {
    start_server(move |socket, tx| {
        let (open_packet, peer) = recv_packet(&socket, &tx);
        let request = decode_open_request(&open_packet, None).unwrap();
        assert!(request.close);
        send_open_reply(
            &socket,
            peer,
            request.params,
            None,
            flags::FLAG_OPEN | flags::FLAG_REPLY | flags::FLAG_CLOSE,
            0,
        );
    })
}

fn start_peer_close_server() -> TestServer {
    start_server(move |socket, tx| {
        let (open_packet, peer) = recv_packet(&socket, &tx);
        let request = decode_open_request(&open_packet, None).unwrap();
        send_open_reply(
            &socket,
            peer,
            request.params.clone(),
            None,
            flags::FLAG_OPEN | flags::FLAG_REPLY,
            TOKEN,
        );
        let (probe_packet, _) = recv_packet(&socket, &tx);
        let probe = decode_echo_request(&probe_packet, &request.params, None).unwrap();
        socket
            .send_to(
                &echo_reply(
                    &request.params,
                    probe.sequence,
                    TOKEN,
                    flags::FLAG_REPLY | flags::FLAG_CLOSE,
                    None,
                ),
                peer,
            )
            .unwrap();
    })
}

fn start_echo_close_server(key: Option<Vec<u8>>) -> TestServer {
    start_server(move |socket, tx| {
        let (open_packet, peer) = recv_packet(&socket, &tx);
        let request = decode_open_request(&open_packet, key.as_deref()).unwrap();
        send_open_reply(
            &socket,
            peer,
            request.params.clone(),
            key.as_deref(),
            flags::FLAG_OPEN | flags::FLAG_REPLY,
            TOKEN,
        );

        let (probe_packet, _) = recv_packet(&socket, &tx);
        let probe = decode_echo_request(&probe_packet, &request.params, key.as_deref()).unwrap();
        socket
            .send_to(
                &echo_reply(
                    &request.params,
                    probe.sequence,
                    TOKEN,
                    flags::FLAG_REPLY,
                    key.as_deref(),
                ),
                peer,
            )
            .unwrap();

        let (close_packet, _) = recv_packet(&socket, &tx);
        assert_eq!(
            decode_close_request(&close_packet, key.as_deref())
                .unwrap()
                .token,
            TOKEN
        );
    })
}

fn start_timeout_server() -> TestServer {
    start_server(move |socket, tx| {
        let (open_packet, peer) = recv_packet(&socket, &tx);
        let request = decode_open_request(&open_packet, None).unwrap();
        send_open_reply(
            &socket,
            peer,
            request.params,
            None,
            flags::FLAG_OPEN | flags::FLAG_REPLY,
            TOKEN,
        );
        let _ = recv_packet(&socket, &tx);
    })
}

fn start_filtered_open_server(key: Vec<u8>) -> TestServer {
    start_server(move |socket, tx| {
        let (open_packet, peer) = recv_packet(&socket, &tx);
        let request = decode_open_request(&open_packet, Some(&key)).unwrap();
        socket.send_to(&[0_u8], peer).unwrap();
        let mut bad_hmac = encode_open_reply(
            &OpenReply {
                flags: flags::FLAG_OPEN | flags::FLAG_REPLY,
                token: TOKEN,
                params: request.params.clone(),
            },
            Some(&key),
        )
        .unwrap();
        bad_hmac[4] ^= 0xff;
        socket.send_to(&bad_hmac, peer).unwrap();
        send_open_reply(
            &socket,
            peer,
            request.params,
            Some(&key),
            flags::FLAG_OPEN | flags::FLAG_REPLY,
            TOKEN,
        );
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
fn hostname_connect_uses_tokio_lookup_without_synchronous_resolution() {
    runtime().block_on(async {
        let mut client_config = config(SocketAddr::from(([127, 0, 0, 1], 2112)), None, 0);
        client_config.server_addr = "localhost:2112".to_owned();
        client_config.socket_config.ipv4_only = true;
        let before = resolution_call_counts();

        let client = AsyncClient::connect(client_config).await.unwrap();

        let after = resolution_call_counts();
        assert!(client.remote.is_ipv4());
        assert_eq!(after.0, before.0);
        assert_eq!(after.1, before.1 + 1);
    });
}

#[test]
fn literal_connect_bypasses_all_name_resolution() {
    runtime().block_on(async {
        let remote = SocketAddr::from(([127, 0, 0, 1], 2112));
        let before = resolution_call_counts();

        let client = AsyncClient::connect(config(remote, None, 0)).await.unwrap();

        assert_eq!(client.remote, remote);
        assert_eq!(resolution_call_counts(), before);
    });
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
        let request = decode_open_request(&open_packet, Some(&server_key)).unwrap();
        socket.send_to(&[0_u8], peer).unwrap();
        let mut bad_hmac = encode_open_reply(
            &OpenReply {
                flags: flags::FLAG_OPEN | flags::FLAG_REPLY,
                token: TOKEN,
                params: request.params.clone(),
            },
            Some(&server_key),
        )
        .unwrap();
        bad_hmac[4] ^= 0xff;
        socket.send_to(&bad_hmac, peer).unwrap();
        send_open_reply(
            &socket,
            peer,
            request.params,
            Some(&server_key),
            flags::FLAG_OPEN | flags::FLAG_REPLY,
            TOKEN,
        );
    });

    runtime().block_on(async {
        let mut client = AsyncClient::connect(config(server.addr, Some(key), 0))
            .await
            .unwrap();
        client
            .test_hooks
            .sends
            .borrow_mut()
            .push_back(InjectedSend::WouldBlock);
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
fn ignored_open_traffic_cannot_extend_the_absolute_attempt_deadline() {
    let server = start_server(move |socket, tx| {
        let (first_packet, peer) = recv_packet(&socket, &tx);
        let request = decode_open_request(&first_packet, None).unwrap();
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
                        request.params,
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
        let request = decode_open_request(&open_packet, None).unwrap();
        send_open_reply(
            &socket,
            peer,
            request.params,
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
        assert_eq!(client.applied_dscp, None);
    });
    assert!(server.finish().is_empty());
}

#[test]
fn opening_cancellation_after_submission_can_retry() {
    let server = start_server(move |socket, tx| {
        let (first, _) = recv_packet(&socket, &tx);
        let first = decode_open_request(&first, None).unwrap();
        let (_, peer) = recv_packet(&socket, &tx);
        send_open_reply(
            &socket,
            peer,
            first.params,
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
        assert_eq!(client.applied_dscp, None);
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
        let (open_packet, peer) = recv_packet(&socket, &tx);
        let request = decode_open_request(&open_packet, None).unwrap();
        send_open_reply(
            &socket,
            peer,
            request.params,
            None,
            flags::FLAG_OPEN | flags::FLAG_REPLY,
            TOKEN,
        );
        let (close, _) = recv_packet(&socket, &tx);
        assert_eq!(decode_close_request(&close, None).unwrap().token, TOKEN);
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
        assert_eq!(client.applied_dscp, None);
    });
    assert_eq!(server.finish().len(), 2);
}

#[test]
fn cleanup_failure_preserves_primary_open_error() {
    let server = start_server(move |socket, tx| {
        let (open_packet, peer) = recv_packet(&socket, &tx);
        let request = decode_open_request(&open_packet, None).unwrap();
        send_open_reply(
            &socket,
            peer,
            request.params,
            None,
            flags::FLAG_OPEN | flags::FLAG_REPLY,
            TOKEN,
        );
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
        assert_eq!(client.applied_dscp, None);
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
        let mut client = AsyncClient::connect(config(server.addr, None, 0))
            .await
            .unwrap();
        client.open().await.unwrap();
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
        let mut client = AsyncClient::connect(config(server.addr, None, 0))
            .await
            .unwrap();
        client.open().await.unwrap();
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
                sent_at: first_sent,
                send_call_start: scheduled_at + Duration::from_millis(4),
                send_finished_at: scheduled_at + Duration::from_millis(6),
            },
            ProbeSendTimestamps {
                permission_at: scheduled_at + Duration::from_millis(8),
                sent_at: second_sent,
                send_call_start: scheduled_at + Duration::from_millis(12),
                send_finished_at: scheduled_at + Duration::from_millis(17),
            },
        ]);
        client
            .test_hooks
            .sends
            .borrow_mut()
            .push_back(InjectedSend::WouldBlock);

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
fn successful_short_probe_commits_before_length_error() {
    let server = start_open_server(None, 0);
    runtime().block_on(async {
        let mut client = AsyncClient::connect(config(server.addr, None, 0))
            .await
            .unwrap();
        client.open().await.unwrap();
        client.test_hooks.pause_probe_before_writable.set(true);
        let mut send = Box::pin(client.send_probe());
        assert!(matches!(poll_once(Pin::as_mut(&mut send)), Poll::Pending));
        drop(send);
        let probe_len = client.prepared_probe.as_ref().unwrap().bytes.len();
        let deadline = client.next_send_deadline().unwrap();
        client
            .test_hooks
            .sends
            .borrow_mut()
            .push_back(InjectedSend::ReportedLength(probe_len - 1));

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
        let mut client = AsyncClient::connect(config(server.addr, None, 0))
            .await
            .unwrap();
        client.open().await.unwrap();
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
            .commit_probe_sent(commit, advanced.bytes.len());
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
        let (open_packet, peer) = recv_packet(&socket, &tx);
        let request = decode_open_request(&open_packet, None).unwrap();
        send_open_reply(
            &socket,
            peer,
            request.params.clone(),
            None,
            flags::FLAG_OPEN | flags::FLAG_REPLY,
            TOKEN,
        );
        let (probe_packet, _) = recv_packet(&socket, &tx);
        let probe = decode_echo_request(&probe_packet, &request.params, None).unwrap();
        socket.send_to(&[0_u8], peer).unwrap();
        socket
            .send_to(
                &echo_reply(
                    &request.params,
                    probe.sequence,
                    TOKEN + 1,
                    flags::FLAG_REPLY,
                    None,
                ),
                peer,
            )
            .unwrap();
        let normal = echo_reply(
            &request.params,
            probe.sequence,
            TOKEN,
            flags::FLAG_REPLY,
            None,
        );
        socket.send_to(&normal, peer).unwrap();
        socket.send_to(&normal, peer).unwrap();
        socket
            .send_to(
                &echo_reply(
                    &request.params,
                    probe.sequence,
                    TOKEN,
                    flags::FLAG_REPLY | flags::FLAG_CLOSE,
                    None,
                ),
                peer,
            )
            .unwrap();
    });

    runtime().block_on(async {
        let mut client = AsyncClient::connect(config(server.addr, None, 46))
            .await
            .unwrap();
        client.open().await.unwrap();
        client.send_probe().await.unwrap();
        client.test_hooks.receive_would_block.set(2);

        assert!(matches!(
            client.recv().await.unwrap().as_slice(),
            [ClientEvent::Warning {
                kind: WarningKind::MalformedOrUnrelatedPacket,
                ..
            }]
        ));
        assert!(matches!(
            client.recv().await.unwrap().as_slice(),
            [ClientEvent::Warning {
                kind: WarningKind::WrongToken,
                ..
            }]
        ));
        assert!(matches!(
            client.recv().await.unwrap().as_slice(),
            [ClientEvent::EchoReply { packet_meta, .. }]
                if *packet_meta == crate::PacketMeta::default()
        ));
        assert!(matches!(
            client.recv().await.unwrap().as_slice(),
            [ClientEvent::DuplicateReply { .. }]
        ));
        assert!(matches!(
            client.recv().await.unwrap().as_slice(),
            [
                ClientEvent::DuplicateReply { .. },
                ClientEvent::SessionClosed { .. }
            ]
        ));
        assert!(client.is_peer_closed());
        assert!(client.schedule.is_none());
        assert!(client.prepared_probe.is_none());
        assert_eq!(client.applied_dscp, None);
        assert_eq!(
            tokio_socket_traffic_class(&client.socket, client.remote).unwrap() & 0xfc,
            0
        );
    });
    assert_eq!(server.finish().len(), 2);
}

#[test]
fn timed_out_probe_reply_is_classified_late() {
    let server = start_server(move |socket, tx| {
        let (open_packet, peer) = recv_packet(&socket, &tx);
        let request = decode_open_request(&open_packet, None).unwrap();
        send_open_reply(
            &socket,
            peer,
            request.params.clone(),
            None,
            flags::FLAG_OPEN | flags::FLAG_REPLY,
            TOKEN,
        );
        let (probe_packet, _) = recv_packet(&socket, &tx);
        let probe = decode_echo_request(&probe_packet, &request.params, None).unwrap();
        thread::sleep(Duration::from_millis(30));
        socket
            .send_to(
                &echo_reply(
                    &request.params,
                    probe.sequence,
                    TOKEN,
                    flags::FLAG_REPLY,
                    None,
                ),
                peer,
            )
            .unwrap();
    });

    runtime().block_on(async {
        let mut client_config = config(server.addr, None, 0);
        client_config.probe_timeout = Duration::from_millis(5);
        let mut client = AsyncClient::connect(client_config).await.unwrap();
        client.open().await.unwrap();
        client.send_probe().await.unwrap();
        time::sleep(Duration::from_millis(10)).await;
        assert!(matches!(
            client.poll_timeouts().unwrap().as_slice(),
            [ClientEvent::EchoLoss { seq: 0, .. }]
        ));
        assert!(matches!(
            client.recv().await.unwrap().as_slice(),
            [ClientEvent::LateReply { seq: 0, .. }]
        ));
    });
    assert_eq!(server.finish().len(), 2);
}

#[test]
fn peer_close_dscp_failure_preserves_events_and_logical_ownership() {
    let server = start_peer_close_server();
    runtime().block_on(async {
        let mut client = AsyncClient::connect(config(server.addr, None, 46))
            .await
            .unwrap();
        client.open().await.unwrap();
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
        assert_eq!(client.applied_dscp, Some(46));
        assert_eq!(client.next_send_deadline(), None);
        assert_eq!(
            tokio_socket_traffic_class(&client.socket, client.remote).unwrap() & 0xfc,
            46 << 2
        );
    });
    assert_eq!(server.finish().len(), 2);
}

#[test]
fn recv_after_local_close_and_no_test_fails_on_first_poll() {
    let close_server = start_server(move |socket, tx| {
        let (open_packet, peer) = recv_packet(&socket, &tx);
        let request = decode_open_request(&open_packet, None).unwrap();
        send_open_reply(
            &socket,
            peer,
            request.params,
            None,
            flags::FLAG_OPEN | flags::FLAG_REPLY,
            TOKEN,
        );
        let (close_packet, _) = recv_packet(&socket, &tx);
        assert_eq!(
            decode_close_request(&close_packet, None).unwrap().token,
            TOKEN
        );
    });
    runtime().block_on(async {
        let mut client = AsyncClient::connect(config(close_server.addr, None, 0))
            .await
            .unwrap();
        client.open().await.unwrap();
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
        let mut client = AsyncClient::connect(config(server.addr, None, 46))
            .await
            .unwrap();
        client.open().await.unwrap();
        let deadline = client.next_send_deadline();
        let attempts = client.test_hooks.send_attempts.get();
        assert_eq!(
            tokio_socket_traffic_class(&client.socket, client.remote).unwrap() & 0xfc,
            46 << 2
        );

        client.test_hooks.pause_close_before_writable.set(true);
        let mut close = Box::pin(client.close());
        assert!(matches!(poll_once(Pin::as_mut(&mut close)), Poll::Pending));
        drop(close);
        assert!(client.machine.is_open());
        assert!(client.schedule.is_some());
        assert_eq!(client.next_send_deadline(), deadline);
        assert_eq!(client.applied_dscp, Some(46));
        assert_eq!(client.test_hooks.send_attempts.get(), attempts);
        assert_eq!(
            tokio_socket_traffic_class(&client.socket, client.remote).unwrap() & 0xfc,
            46 << 2
        );

        client
            .test_hooks
            .sends
            .borrow_mut()
            .push_back(InjectedSend::WouldBlock);
        client.test_hooks.pause_close_after_would_block.set(true);
        let mut close = Box::pin(client.close());
        assert!(matches!(poll_once(Pin::as_mut(&mut close)), Poll::Pending));
        drop(close);
        assert!(client.machine.is_open());
        assert!(client.schedule.is_some());
        assert_eq!(client.applied_dscp, Some(46));
        assert_eq!(
            tokio_socket_traffic_class(&client.socket, client.remote).unwrap() & 0xfc,
            46 << 2
        );

        assert!(matches!(
            client.close().await.unwrap().as_slice(),
            [ClientEvent::SessionClosed { .. }]
        ));
        assert!(client.schedule.is_none());
        assert_eq!(client.applied_dscp, None);
    });
    assert_eq!(server.finish().len(), 2);
}

#[test]
fn close_errors_preserve_open_state_and_primary_send_error() {
    let server = start_open_server(None, 0);
    runtime().block_on(async {
        let mut client = AsyncClient::connect(config(server.addr, None, 46))
            .await
            .unwrap();
        client.open().await.unwrap();
        let deadline = client.next_send_deadline();

        client
            .test_hooks
            .sends
            .borrow_mut()
            .push_back(InjectedSend::Error);
        assert!(matches!(client.close().await, Err(ClientError::Socket(_))));
        assert!(client.machine.is_open());
        assert!(client.schedule.is_some());
        assert_eq!(client.next_send_deadline(), deadline);
        assert_eq!(client.applied_dscp, Some(46));
        assert_eq!(
            tokio_socket_traffic_class(&client.socket, client.remote).unwrap() & 0xfc,
            46 << 2
        );

        client.test_hooks.fail_dscp_restore.set(true);
        client
            .test_hooks
            .sends
            .borrow_mut()
            .push_back(InjectedSend::Error);
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
        let mut client = AsyncClient::connect(config(server.addr, None, 46))
            .await
            .unwrap();
        client.open().await.unwrap();
        client.test_hooks.fail_dscp_restore.set(true);
        client
            .test_hooks
            .sends
            .borrow_mut()
            .push_back(InjectedSend::WouldBlock);

        assert!(matches!(
            client.close().await,
            Err(ClientError::SocketOption {
                operation: "restore negotiated DSCP",
                ..
            })
        ));
        assert!(client.machine.is_open());
        assert!(client.schedule.is_some());
        assert_eq!(client.applied_dscp, Some(46));
    });
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn close_timestamp_is_sampled_after_success_and_short_send_commits() {
    let server = start_open_server(None, 0);
    runtime().block_on(async {
        let mut client = AsyncClient::connect(config(server.addr, None, 0))
            .await
            .unwrap();
        client.open().await.unwrap();
        client.test_hooks.pause_probe_before_writable.set(true);
        let mut send = Box::pin(client.send_probe());
        assert!(matches!(poll_once(Pin::as_mut(&mut send)), Poll::Pending));
        drop(send);
        assert!(client.prepared_probe.is_some());

        let close_len = client.machine.prepare_close().unwrap().bytes.len();
        let close_sent_at = ClientTimestamp {
            wall: SystemTime::UNIX_EPOCH + Duration::from_secs(44),
            mono: Instant::now() + Duration::from_secs(1),
        };
        client.test_hooks.close_sent_at.set(Some(close_sent_at));
        client
            .test_hooks
            .sends
            .borrow_mut()
            .push_back(InjectedSend::ReportedLength(close_len - 1));

        assert!(matches!(
            client.close().await,
            Err(ClientError::DatagramLengthMismatch { .. })
        ));
        assert!(!client.machine.is_open());
        assert!(client.schedule.is_none());
        assert!(client.prepared_probe.is_none());
        assert_eq!(client.applied_dscp, None);
        assert!(matches!(
            client.machine.prepare_close(),
            Err(ClientError::AlreadyClosed)
        ));

        let server = start_open_server(None, 1);
        let mut exact = AsyncClient::connect(config(server.addr, None, 0))
            .await
            .unwrap();
        exact.open().await.unwrap();
        exact.test_hooks.close_sent_at.set(Some(close_sent_at));
        let events = exact.close().await.unwrap();
        assert!(matches!(
            events.as_slice(),
            [ClientEvent::SessionClosed { at, .. }] if *at == close_sent_at
        ));
        assert!(matches!(
            exact.close().await,
            Err(ClientError::AlreadyClosed)
        ));
        assert_eq!(server.finish().len(), 2);
    });
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn blocking_and_async_hmac_dscp_lifecycle_are_semantically_equivalent() {
    let key = b"async-equivalence-key".to_vec();
    let blocking_server = start_echo_close_server(Some(key.clone()));
    let mut blocking =
        Client::connect(config(blocking_server.addr, Some(key.clone()), 46)).unwrap();
    let blocking_open = blocking.open().unwrap();
    let blocking_sent = blocking.send_probe().unwrap();
    let blocking_reply = blocking.recv_once().unwrap();
    let blocking_close = blocking.close().unwrap();
    assert_eq!(blocking_server.finish().len(), 3);

    let async_server = start_echo_close_server(Some(key.clone()));
    let (async_open, async_sent, async_reply, async_close) = runtime().block_on(async {
        let mut client = AsyncClient::connect(config(async_server.addr, Some(key), 46))
            .await
            .unwrap();
        let opened = client.open().await.unwrap();
        assert_eq!(
            tokio_socket_traffic_class(&client.socket, client.remote).unwrap() & 0xfc,
            46 << 2
        );
        let sent = client.send_probe().await.unwrap();
        let reply = client.recv().await.unwrap();
        let closed = client.close().await.unwrap();
        (opened, sent, reply, closed)
    });
    assert_eq!(async_server.finish().len(), 3);

    assert_eq!(
        open_negotiated(&blocking_open),
        open_negotiated(&async_open)
    );
    assert_eq!(open_negotiated(&async_open).params.dscp, 46);
    assert_matching_event_shape(&blocking_sent[0], &async_sent[0]);
    assert_matching_event_shape(&blocking_reply[0], &async_reply[0]);
    assert_matching_event_shape(&blocking_close[0], &async_close[0]);
}

#[test]
fn blocking_and_async_no_test_timeout_and_peer_close_are_equivalent() {
    let blocking_no_test_server = start_no_test_server();
    let mut blocking_no_test_config = config(blocking_no_test_server.addr, None, 0);
    blocking_no_test_config.run_mode = RunMode::NoTest;
    let mut blocking_no_test = Client::connect(blocking_no_test_config).unwrap();
    let blocking_no_test_open = blocking_no_test.open().unwrap();
    assert!(blocking_no_test.is_run_complete());
    assert_eq!(blocking_no_test_server.finish().len(), 1);

    let async_no_test_server = start_no_test_server();
    let mut async_no_test_config = config(async_no_test_server.addr, None, 0);
    async_no_test_config.run_mode = RunMode::NoTest;
    let async_no_test_open = runtime().block_on(async {
        let mut client = AsyncClient::connect(async_no_test_config).await.unwrap();
        let opened = client.open().await.unwrap();
        assert!(client.is_run_complete());
        opened
    });
    assert_eq!(async_no_test_server.finish().len(), 1);
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

    let blocking_timeout_server = start_timeout_server();
    let mut blocking_timeout_config = config(blocking_timeout_server.addr, None, 0);
    blocking_timeout_config.probe_timeout = Duration::from_millis(5);
    let mut blocking_timeout = Client::connect(blocking_timeout_config).unwrap();
    blocking_timeout.open().unwrap();
    blocking_timeout.send_probe().unwrap();
    thread::sleep(Duration::from_millis(10));
    let blocking_loss = blocking_timeout.poll_timeouts().unwrap();
    assert_eq!(blocking_timeout_server.finish().len(), 2);

    let async_timeout_server = start_timeout_server();
    let mut async_timeout_config = config(async_timeout_server.addr, None, 0);
    async_timeout_config.probe_timeout = Duration::from_millis(5);
    let async_loss = runtime().block_on(async {
        let mut client = AsyncClient::connect(async_timeout_config).await.unwrap();
        client.open().await.unwrap();
        client.send_probe().await.unwrap();
        time::sleep(Duration::from_millis(10)).await;
        client.poll_timeouts().unwrap()
    });
    assert_eq!(async_timeout_server.finish().len(), 2);
    assert_matching_event_shape(&blocking_loss[0], &async_loss[0]);

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

#[test]
fn blocking_and_async_filtered_open_and_finite_completion_are_equivalent() {
    let key = b"filtered-open-equivalence".to_vec();
    let blocking_server = start_filtered_open_server(key.clone());
    let mut blocking = Client::connect(config(blocking_server.addr, Some(key.clone()), 0)).unwrap();
    let blocking_open = blocking.open().unwrap();
    assert_eq!(blocking_server.finish().len(), 1);

    let async_server = start_filtered_open_server(key.clone());
    let async_open = runtime().block_on(async {
        let mut client = AsyncClient::connect(config(async_server.addr, Some(key), 0))
            .await
            .unwrap();
        client.open().await.unwrap()
    });
    assert_eq!(async_server.finish().len(), 1);
    assert_eq!(
        open_negotiated(&blocking_open),
        open_negotiated(&async_open)
    );

    let blocking_finite_server = start_open_server(None, 0);
    let mut blocking_config = config(blocking_finite_server.addr, None, 0);
    blocking_config.interval = Duration::from_millis(5);
    blocking_config.duration = Some(Duration::from_millis(20));
    let mut blocking = Client::connect(blocking_config).unwrap();
    blocking.open().unwrap();
    thread::sleep(Duration::from_millis(25));
    assert!(blocking.send_probe().unwrap().is_empty());
    assert!(blocking.is_run_complete());
    assert_eq!(blocking_finite_server.finish().len(), 1);

    let async_finite_server = start_open_server(None, 0);
    let mut async_config = config(async_finite_server.addr, None, 0);
    async_config.interval = Duration::from_millis(5);
    async_config.duration = Some(Duration::from_millis(20));
    runtime().block_on(async {
        let mut client = AsyncClient::connect(async_config).await.unwrap();
        client.open().await.unwrap();
        time::sleep(Duration::from_millis(25)).await;
        assert!(client.send_probe().await.unwrap().is_empty());
        assert!(client.is_run_complete());
    });
    assert_eq!(async_finite_server.finish().len(), 1);
}

fn open_negotiated(outcome: &OpenOutcome) -> &crate::NegotiatedParams {
    match outcome {
        OpenOutcome::Started { negotiated, .. }
        | OpenOutcome::NoTestCompleted { negotiated, .. } => negotiated,
    }
}

fn assert_matching_event_shape(blocking: &ClientEvent, asynchronous: &ClientEvent) {
    match (blocking, asynchronous) {
        (
            ClientEvent::EchoSent {
                seq: left_seq,
                bytes: left_bytes,
                ..
            },
            ClientEvent::EchoSent {
                seq: right_seq,
                bytes: right_bytes,
                ..
            },
        ) => {
            assert_eq!(left_seq, right_seq);
            assert_eq!(left_bytes, right_bytes);
        }
        (
            ClientEvent::EchoReply {
                seq: left_seq,
                bytes: left_bytes,
                ..
            },
            ClientEvent::EchoReply {
                seq: right_seq,
                bytes: right_bytes,
                ..
            },
        ) => {
            assert_eq!(left_seq, right_seq);
            assert_eq!(left_bytes, right_bytes);
        }
        (
            ClientEvent::SessionClosed {
                token: left_token, ..
            },
            ClientEvent::SessionClosed {
                token: right_token, ..
            },
        ) => assert_eq!(left_token, right_token),
        (
            ClientEvent::EchoLoss { seq: left_seq, .. },
            ClientEvent::EchoLoss { seq: right_seq, .. },
        ) => assert_eq!(left_seq, right_seq),
        pair => panic!("event shapes differ: {pair:?}"),
    }
}
