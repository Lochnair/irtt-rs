use std::{
    future::Future,
    net::UdpSocket,
    pin::Pin,
    sync::mpsc,
    task::{Context, Poll, Waker},
    thread::{self, JoinHandle},
};

use irtt_proto::{
    decode_request, encode_echo_reply, encode_open_reply, flags, verify_packet_hmac,
    DecodedRequestKind, EchoReply, OpenReply, Params, ReceivedStats, StampAt, TimestampFields,
};
use tokio::runtime::{Builder, Runtime};

#[path = "in_tree_server_support.rs"]
mod in_tree_server;

use super::*;
use crate::{socket_options::tokio_socket_traffic_class, Client, RunMode, SocketConfig};
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
    assert!(blocking_no_test.negotiated_params().is_none());
    drop(blocking_no_test_server);

    let async_no_test_server = InTreeServer::start(ServerConfig::default());
    let mut async_no_test_config = config(async_no_test_server.addr, None, 0);
    async_no_test_config.run_mode = RunMode::NoTest;
    let async_no_test_open = runtime().block_on(async {
        let mut client = AsyncClient::connect(async_no_test_config).await.unwrap();
        let opened = client.open().await.unwrap();
        assert!(client.negotiated_params().is_none());
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
    assert!(!blocking.has_pending_probes());
    let blocking_first = blocking.open().unwrap_err();
    assert!(!blocking.has_pending_probes());
    let blocking_second = blocking.open().unwrap();
    assert!(!blocking.has_pending_probes());
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
            assert!(!client.has_pending_probes());
            let first = client.open().await.unwrap_err();
            assert!(!client.has_pending_probes());
            let second = client.open().await.unwrap();
            assert!(!client.has_pending_probes());
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
    fn caller_config(addr: SocketAddr) -> ClientConfig {
        ClientConfig {
            duration: Some(Duration::from_millis(1)),
            interval: Duration::from_secs(1),
            ..config(addr, None, 0)
        }
    }

    let blocking_server = InTreeServer::start(ServerConfig::default());
    let mut blocking = Client::connect(caller_config(blocking_server.addr)).unwrap();
    let outcome = blocking.open().unwrap();
    assert_eq!(
        blocking.negotiated_params(),
        Some(open_negotiated(&outcome))
    );
    assert!(!blocking.has_pending_probes());
    // Oversleeping cannot invalidate this: low-level sends remain caller-owned
    // after the negotiated duration. The compliant server has no duration cap.
    thread::sleep(Duration::from_millis(2));
    let mut blocking_sent = Vec::new();
    let mut blocking_replies = Vec::new();
    for _ in 0..PROBES {
        blocking_sent.extend(blocking.send_probe().unwrap());
        assert!(blocking.has_pending_probes());
        assert!(blocking.next_probe_timeout_deadline().is_some());
        blocking_replies.extend(blocking.recv_once().unwrap());
        assert!(!blocking.has_pending_probes());
    }
    blocking.close().unwrap();
    drop(blocking_server);

    let async_server = InTreeServer::start(ServerConfig::default());
    let (async_sent, async_replies) = runtime().block_on(async {
        let mut client = AsyncClient::connect(caller_config(async_server.addr))
            .await
            .unwrap();
        let outcome = client.open().await.unwrap();
        assert_eq!(client.negotiated_params(), Some(open_negotiated(&outcome)));
        tokio::time::sleep(Duration::from_millis(2)).await;
        let mut sent = Vec::new();
        let mut replies = Vec::new();
        for _ in 0..PROBES {
            sent.extend(client.send_probe().await.unwrap());
            assert!(client.has_pending_probes());
            assert!(client.next_probe_timeout_deadline().is_some());
            replies.extend(client.recv().await.unwrap());
            assert!(!client.has_pending_probes());
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
    for event in blocking_sent.iter().chain(&async_sent) {
        assert!(matches!(
            event,
            ClientEvent::EchoSent {
                scheduled_at: None,
                timer_error: None,
                ..
            }
        ));
    }
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
    });
    assert_eq!(server.finish().len(), 2);
}
