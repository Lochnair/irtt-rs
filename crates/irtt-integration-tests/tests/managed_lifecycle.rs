use irtt_client::{Authentication, HmacKey, OpenPolicy, SessionRequest};
use std::{
    future::Future,
    net::{SocketAddr, UdpSocket},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use irtt_proto::{
    decode_request, encode_echo_reply, encode_open_reply, flags, verify_packet_hmac,
    DecodedRequestKind, EchoReply, OpenReply, Params, ReceivedStats, StampAt, TimestampFields,
};
use tokio::runtime::{Builder, Runtime};

use irtt_client::RunMode;
use irtt_client::{managed::*, ClientConfig};
use tokio::sync::broadcast;

const TOKEN: u64 = 0x1234_5678_90ab_cdef;

#[derive(Clone, Copy)]
enum ServerBehavior {
    Echo,
    NoTest,
    PeerClose,
    DelayedEcho(Duration),
}

#[derive(Clone, Debug)]
enum PacketKind {
    Open,
    Probe,
    Close,
}

#[derive(Clone, Debug)]
struct PacketRecord {
    kind: PacketKind,
    at: Instant,
}

struct TestServer {
    addr: SocketAddr,
    records: Arc<Mutex<Vec<PacketRecord>>>,
    thread: JoinHandle<()>,
}

impl TestServer {
    fn finish(self) -> Vec<PacketRecord> {
        self.thread.join().unwrap();
        self.records.lock().unwrap().clone()
    }
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

fn decode_open_params(packet: &[u8], key: Option<&[u8]>) -> Params {
    match decode_server_request(packet, key) {
        Some(DecodedRequestKind::Open { params, .. }) => Params::decode(params).unwrap(),
        other => panic!("expected an authenticated open request, got {other:?}"),
    }
}

fn start_server(behavior: ServerBehavior, key: Option<Vec<u8>>) -> TestServer {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let addr = socket.local_addr().unwrap();
    let records = Arc::new(Mutex::new(Vec::new()));
    let thread_records = Arc::clone(&records);
    let thread = thread::spawn(move || {
        let mut buffer = [0_u8; 8192];
        let (open_len, peer) = socket.recv_from(&mut buffer).unwrap();
        thread_records.lock().unwrap().push(PacketRecord {
            kind: PacketKind::Open,
            at: Instant::now(),
        });
        let negotiated = decode_open_params(&buffer[..open_len], key.as_deref());
        let no_test = matches!(behavior, ServerBehavior::NoTest);
        let open_reply = encode_open_reply(
            &OpenReply {
                flags: flags::FLAG_OPEN
                    | flags::FLAG_REPLY
                    | if no_test { flags::FLAG_CLOSE } else { 0 },
                token: if no_test { 0 } else { TOKEN },
                params: negotiated.clone(),
            },
            key.as_deref(),
        )
        .unwrap();
        socket.send_to(&open_reply, peer).unwrap();
        if no_test {
            return;
        }

        loop {
            let Ok((len, packet_peer)) = socket.recv_from(&mut buffer) else {
                return;
            };
            let packet = &buffer[..len];
            match decode_server_request(packet, key.as_deref()) {
                Some(DecodedRequestKind::Echo { sequence, .. }) => {
                    thread_records.lock().unwrap().push(PacketRecord {
                        kind: PacketKind::Probe,
                        at: Instant::now(),
                    });
                    if let ServerBehavior::DelayedEcho(delay) = behavior {
                        thread::sleep(delay);
                    }
                    let peer_close = matches!(behavior, ServerBehavior::PeerClose);
                    let reply = encode_echo_reply(
                        &EchoReply {
                            flags: flags::FLAG_REPLY
                                | if peer_close { flags::FLAG_CLOSE } else { 0 },
                            token: TOKEN,
                            sequence,
                            recv_count: None,
                            recv_window: None,
                            timestamps: TimestampFields::default(),
                            payload: Vec::new(),
                        },
                        &negotiated,
                        key.as_deref(),
                    )
                    .unwrap();
                    socket.send_to(&reply, packet_peer).unwrap();
                    if peer_close {
                        return;
                    }
                }
                Some(DecodedRequestKind::Close { .. }) => {
                    thread_records.lock().unwrap().push(PacketRecord {
                        kind: PacketKind::Close,
                        at: Instant::now(),
                    });
                    return;
                }
                Some(DecodedRequestKind::Open { .. }) | None => {}
            }
        }
    });
    TestServer {
        addr,
        records,
        thread,
    }
}

fn runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .unwrap()
}

fn config(pacing: ManagedPacing) -> ManagedClientConfig {
    ManagedClientConfig {
        client: ClientConfig {
            open: OpenPolicy {
                timeouts: vec![Duration::from_millis(200)],
                ..Default::default()
            },
            request: SessionRequest {
                duration: Some(Duration::from_millis(130)),
                interval: Duration::from_millis(60),
                received_stats: ReceivedStats::None,
                stamp_at: StampAt::None,
                ..Default::default()
            },
            probe_timeout: Duration::from_millis(35),
            ..ClientConfig::default()
        },
        pacing,
        final_drain: Duration::from_millis(5),
        ..ManagedClientConfig::default()
    }
}

fn target(id: &str, addr: SocketAddr) -> ManagedTargetConfig {
    ManagedTargetConfig::new(id, addr.to_string())
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    Future::poll(future, &mut Context::from_waker(Waker::noop()))
}

fn probes(records: &[PacketRecord]) -> Vec<Instant> {
    records
        .iter()
        .filter_map(|record| matches!(record.kind, PacketKind::Probe).then_some(record.at))
        .collect()
}

fn has_close(records: &[PacketRecord]) -> bool {
    records
        .iter()
        .any(|record| matches!(record.kind, PacketKind::Close))
}

#[test]
fn construction_is_runtime_and_io_free() {
    let (task, handle) = ManagedClient::task(
        config(ManagedPacing::Staggered),
        vec![ManagedTargetConfig::new("dns", "no-such-host.invalid")],
    )
    .unwrap();
    assert_eq!(handle.status().lifecycle, ManagedLifecycle::NotStarted);
    drop(task);
}

#[test]
fn quiescent_rejects_empty_initial_targets() {
    assert!(matches!(
        ManagedClient::task(config(ManagedPacing::Staggered), vec![]),
        Err(ManagedConfigError::EmptyInitialTargets)
    ));
}

#[test]
fn explicit_empty_waits_for_stop() {
    let server = start_server(ServerBehavior::Echo, None);
    let mut config = config(ManagedPacing::Staggered);
    config.completion = ManagedCompletionPolicy::ExplicitStop;
    config.client.request.duration = None;
    let (task, handle) = ManagedClient::task(config, vec![]).unwrap();
    let mut events = handle.subscribe().unwrap();
    let outcome = runtime().block_on(async {
        tokio::time::timeout(Duration::from_secs(2), async {
            let mut task = Box::pin(task);
            assert!(poll_once(task.as_mut()).is_pending());
            assert_eq!(handle.status().lifecycle, ManagedLifecycle::Running);
            let update = handle
                .update_targets(vec![target("one", server.addr)])
                .unwrap();
            loop {
                tokio::select! {
                    _ = task.as_mut() => panic!("explicit-stop task completed before stop"),
                    event = events.recv() => {
                        if matches!(event.unwrap(), ManagedEvent::Client {
                            event: irtt_client::ClientEvent::EchoReply { .. }, ..
                        }) {
                            break;
                        }
                    }
                }
            }
            update.await.unwrap();
            let mut receipt = Box::pin(handle.stop());
            assert!(poll_once(receipt.as_mut()).is_pending());
            assert!(poll_once(task.as_mut()).is_pending());
            let status = handle.status();
            assert_eq!(status.lifecycle, ManagedLifecycle::Stopping);
            assert!(status.stop_requested);
            assert!(status.final_outcome.is_none());
            // The driver is paused during graceful cleanup: receipt readiness must
            // come from durable stop observation, not eventual task completion.
            assert!(poll_once(receipt.as_mut()).is_ready());
            task.await
        })
        .await
        .expect("stop observation and cleanup must complete")
    });
    assert_eq!(outcome.end_reason, ManagedEndReason::StopRequested);
    assert!(has_close(&server.finish()));
}

#[test]
fn extreme_event_capacity_is_rejected_without_panic() {
    let mut managed = config(ManagedPacing::Staggered);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    managed.event_capacity = usize::MAX;
    assert!(matches!(
        ManagedClient::task(managed, vec![]),
        Err(ManagedConfigError::EventCapacityTooLarge {
            configured: usize::MAX,
            ..
        })
    ));
}

#[test]
fn extreme_command_capacity_is_rejected_without_panic() {
    let mut managed = config(ManagedPacing::Staggered);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    managed.command_capacity = usize::MAX;
    assert!(matches!(
        ManagedClient::task(managed, vec![]),
        Err(ManagedConfigError::CommandCapacityTooLarge {
            configured: usize::MAX,
            ..
        })
    ));
}

#[test]
fn first_poll_without_runtime_fails_durably() {
    let (task, handle) = ManagedClient::task(
        config(ManagedPacing::Staggered),
        vec![target("one", "127.0.0.1:9".parse().unwrap())],
    )
    .unwrap();
    let mut task = Box::pin(task);
    let Poll::Ready(outcome) = poll_once(task.as_mut()) else {
        panic!("task did not fail on its first poll")
    };
    assert_eq!(
        outcome.end_reason,
        ManagedEndReason::DriverFailed(ManagedDriverFailure::NoTokioRuntime)
    );
    assert_eq!(handle.status().lifecycle, ManagedLifecycle::Failed);
}

#[test]
fn pre_poll_stop() {
    let (task, handle) = ManagedClient::task(
        config(ManagedPacing::Staggered),
        vec![ManagedTargetConfig::new("one", "no-such-host.invalid")],
    )
    .unwrap();
    let mut receipt = Box::pin(handle.stop());
    assert!(poll_once(receipt.as_mut()).is_pending());
    assert!(!handle.status().stop_requested);
    let mut task = Box::pin(task);
    let Poll::Ready(outcome) = poll_once(task.as_mut()) else {
        panic!("pre-stopped task did not complete")
    };
    assert!(poll_once(receipt.as_mut()).is_ready());
    assert_eq!(outcome.end_reason, ManagedEndReason::StopRequested);
}

#[test]
fn finite_immediate_replies_use_retained_drain_deadline() {
    let server = start_server(ServerBehavior::Echo, None);
    let mut managed = config(ManagedPacing::Staggered);
    managed.client.probe_timeout = Duration::from_secs(3);
    managed.final_drain = Duration::from_millis(30);
    let (task, _) = ManagedClient::task(managed, vec![target("one", server.addr)]).unwrap();
    let started_at = Instant::now();
    let outcome = runtime().block_on(async {
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("replied probes must not hold the drain until their obsolete timeout")
    });
    assert!(started_at.elapsed() < Duration::from_secs(1));
    let records = server.finish();
    assert_eq!(outcome.end_reason, ManagedEndReason::TargetsComplete);
    assert!(matches!(
        outcome.recent_target_outcomes[0].end_reason,
        ManagedTargetEndReason::TestComplete
    ));
    assert!(!probes(&records).is_empty() && has_close(&records));
}

#[test]
fn no_test_target_completion() {
    let server = start_server(ServerBehavior::NoTest, None);
    let mut config = config(ManagedPacing::Staggered);
    config.client.request.run_mode = RunMode::NoTest;
    let (task, _) = ManagedClient::task(config, vec![target("no-test", server.addr)]).unwrap();
    let outcome = runtime().block_on(task);
    let records = server.finish();
    assert!(matches!(
        outcome.recent_target_outcomes[0].end_reason,
        ManagedTargetEndReason::NoTestComplete
    ));
    assert!(probes(&records).is_empty() && !has_close(&records));
}

#[test]
fn authenticated_peer_close_outcome() {
    let key = b"managed-peer-close".to_vec();
    let server = start_server(ServerBehavior::PeerClose, Some(key.clone()));
    let mut configured = target("peer", server.addr);
    configured.auth = TargetAuth::Override(Authentication::Hmac(HmacKey::new(key)));
    let (task, _) =
        ManagedClient::task(config(ManagedPacing::Staggered), vec![configured]).unwrap();
    let outcome = runtime().block_on(task);
    let records = server.finish();
    assert_eq!(outcome.peer_closed_target_outcomes, 1);
    assert!(matches!(
        outcome.recent_target_outcomes[0].end_reason,
        ManagedTargetEndReason::PeerClosed
    ));
    assert!(!has_close(&records));
}

#[test]
fn static_multi_target_completion() {
    let first = start_server(ServerBehavior::Echo, None);
    let second = start_server(ServerBehavior::Echo, None);
    let (task, _) = ManagedClient::task(
        config(ManagedPacing::Burst),
        vec![target("first", first.addr), target("second", second.addr)],
    )
    .unwrap();
    let outcome = runtime().block_on(task);
    assert_eq!(outcome.total_target_outcomes, 2);
    assert_eq!(outcome.successful_target_outcomes, 2);
    assert!(!probes(&first.finish()).is_empty());
    assert!(!probes(&second.finish()).is_empty());
}

#[test]
fn sibling_failure_isolation() {
    let good = start_server(ServerBehavior::Echo, None);
    let unused = UdpSocket::bind("127.0.0.1:0").unwrap();
    let bad_addr = unused.local_addr().unwrap();
    drop(unused);
    let (task, _) = ManagedClient::task(
        config(ManagedPacing::Staggered),
        vec![target("good", good.addr), target("bad", bad_addr)],
    )
    .unwrap();
    let outcome = runtime().block_on(task);
    assert_eq!(outcome.total_target_outcomes, 2);
    assert_eq!(outcome.successful_target_outcomes, 1);
    assert_eq!(outcome.failed_target_outcomes, 1);
    assert!(!probes(&good.finish()).is_empty());
}

#[test]
fn pending_limit_failure_drains_reply_and_closes_session() {
    let key = b"managed-send-failure-cleanup".to_vec();
    let server = start_server(
        ServerBehavior::DelayedEcho(Duration::from_millis(80)),
        Some(key.clone()),
    );
    let mut managed = config(ManagedPacing::Staggered);
    managed.client.request.duration = Some(Duration::from_millis(250));
    managed.client.request.interval = Duration::from_millis(20);
    managed.client.probe_timeout = Duration::from_secs(2);
    managed.client.max_pending_probes = 1;
    managed.final_drain = Duration::from_millis(50);
    let mut configured = target("one", server.addr);
    configured.auth = TargetAuth::Override(Authentication::Hmac(HmacKey::new(key)));
    let (task, _) = ManagedClient::task(managed, vec![configured]).unwrap();
    let outcome = runtime().block_on(async {
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("send failure cleanup must shorten after draining the committed reply")
    });
    let records = server.finish();
    let target = &outcome.recent_target_outcomes[0];
    assert_eq!(outcome.failed_target_outcomes, 1);
    assert!(matches!(
        target.end_reason,
        ManagedTargetEndReason::Failed(ManagedTargetFailure {
            phase: ManagedTargetFailurePhase::Sending,
            kind: ManagedTargetFailureKind::ResourceExhausted,
            ..
        })
    ));
    assert_eq!(target.cleanup_failure, None);
    assert_eq!(target.packets_sent, 1);
    assert_eq!(target.replies_received, 1);
    assert_eq!(probes(&records).len(), 1);
    assert!(has_close(&records));
}

#[test]
fn event_loss_independence() {
    let server = start_server(ServerBehavior::Echo, None);
    let mut config = config(ManagedPacing::Staggered);
    config.event_capacity = 1;
    let (task, handle) = ManagedClient::task(config, vec![target("one", server.addr)]).unwrap();
    let initial = handle.status();
    assert_eq!(
        initial.targets[0].lifecycle,
        ManagedTargetLifecycle::Pending
    );
    assert!(initial.targets[0].outcome.is_none());
    let mut events = handle.subscribe().unwrap();
    let outcome = runtime().block_on(task);
    assert!(matches!(
        events.try_recv(),
        Err(broadcast::error::TryRecvError::Lagged(_))
    ));
    assert_eq!(outcome.successful_target_outcomes, 1);
    let status = handle.status();
    assert_eq!(status.lifecycle, ManagedLifecycle::Completed);
    assert!(!status.stop_requested);
    let mut receipt = Box::pin(handle.stop());
    assert!(poll_once(receipt.as_mut()).is_ready());
    assert!(Arc::ptr_eq(&status, &handle.status()));
    assert_eq!(status.targets.len(), 1);
    let target = &status.targets[0];
    assert!(target.desired);
    assert_eq!(target.lifecycle, ManagedTargetLifecycle::Terminal);
    let terminal = target
        .outcome
        .as_ref()
        .expect("terminal details are durable");
    assert_eq!(terminal.target, target.target);
    assert_eq!(status.terminal_target_count, 1);
    assert_eq!(status.total_target_outcomes, 1);
    assert_eq!(status.successful_target_outcomes, 1);
    assert_eq!(status.failed_target_outcomes, 0);
    assert_eq!(status.peer_closed_target_outcomes, 0);
    assert_eq!(status.discarded_target_outcomes, 0);
    assert_eq!(
        status.recent_target_outcomes.as_ref(),
        &[terminal.as_ref().clone()]
    );
    assert_eq!(
        status.recent_target_outcomes,
        outcome.recent_target_outcomes
    );
    server.finish();
}

#[test]
fn dropping_all_handles_does_not_stop() {
    let server = start_server(ServerBehavior::Echo, None);
    let (task, handle) = ManagedClient::task(
        config(ManagedPacing::Staggered),
        vec![target("one", server.addr)],
    )
    .unwrap();
    drop(handle);
    let outcome = runtime().block_on(task);
    assert_eq!(outcome.end_reason, ManagedEndReason::TargetsComplete);
    server.finish();
}

#[test]
fn task_drop_abandonment() {
    // A latched but unobserved stop must not turn abandonment into completion.
    for request_stop in [false, true] {
        let (task, handle) = ManagedClient::task(
            config(ManagedPacing::Staggered),
            vec![target("one", "127.0.0.1:9".parse().unwrap())],
        )
        .unwrap();
        let mut events = handle.subscribe().unwrap();
        let mut receipt = request_stop.then(|| Box::pin(handle.stop()));
        if let Some(receipt) = &mut receipt {
            assert!(poll_once(receipt.as_mut()).is_pending());
        }
        drop(task);
        assert_eq!(handle.status().lifecycle, ManagedLifecycle::Abandoned);
        assert_eq!(handle.status().total_target_outcomes, 0);
        assert!(!handle.status().stop_requested);
        assert!(handle.status().final_outcome.is_none());
        assert!(matches!(events.try_recv(), Ok(ManagedEvent::Abandoned)));
        if let Some(receipt) = &mut receipt {
            assert!(poll_once(receipt.as_mut()).is_ready());
        }
        let mut late_stop = Box::pin(handle.stop());
        assert!(poll_once(late_stop.as_mut()).is_ready());
        assert_eq!(handle.status().lifecycle, ManagedLifecycle::Abandoned);
        assert!(!handle.status().stop_requested);
    }
}

#[test]
fn terminal_subscription_is_closed() {
    let (task, handle) = ManagedClient::task(
        config(ManagedPacing::Staggered),
        vec![target("one", "127.0.0.1:9".parse().unwrap())],
    )
    .unwrap();
    let mut task = Box::pin(task);
    assert!(poll_once(task.as_mut()).is_ready());
    assert!(matches!(
        handle.subscribe(),
        Err(ManagedSubscribeError::Closed)
    ));
}
