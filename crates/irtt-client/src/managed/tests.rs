use std::{
    future::{poll_fn, Future},
    mem,
    net::{SocketAddr, UdpSocket},
    pin::Pin,
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll, Waker},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use irtt_proto::{
    decode_close_request, decode_echo_request, decode_open_request, encode_echo_reply,
    encode_open_reply, flags, EchoReply, OpenReply, ReceivedStats, StampAt, TimestampFields,
};
use tokio::runtime::{Builder, Runtime};

use super::*;
use crate::{socket::resolution_call_counts, ClientAuthConfig, NegotiationPolicy, RunMode};

const TOKEN: u64 = 0x1234_5678_90ab_cdef;

#[derive(Clone, Copy)]
enum ServerBehavior {
    Echo,
    NoTest,
    PeerClose,
    DelayedEcho(Duration),
    DeferredBurst(usize),
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
    probe_seen: Arc<(Mutex<bool>, Condvar)>,
    reply_sent: Arc<(Mutex<bool>, Condvar)>,
    thread: JoinHandle<()>,
}

impl TestServer {
    fn finish(self) -> Vec<PacketRecord> {
        self.thread.join().unwrap();
        self.records.lock().unwrap().clone()
    }
}

#[derive(Default)]
struct PacketGate {
    seen: Mutex<bool>,
    seen_ready: Condvar,
    released: Mutex<bool>,
    release_ready: Condvar,
}

impl PacketGate {
    fn arrive_and_wait(&self) {
        *self.seen.lock().unwrap() = true;
        self.seen_ready.notify_all();
        let released = self.released.lock().unwrap();
        let (released, timeout) = self
            .release_ready
            .wait_timeout_while(released, Duration::from_secs(2), |released| !*released)
            .unwrap();
        assert!(*released && !timeout.timed_out());
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.release_ready.notify_all();
    }
}
fn start_server(behavior: ServerBehavior, key: Option<Vec<u8>>) -> TestServer {
    start_server_negotiating(behavior, key, None)
}
fn start_server_negotiating(
    behavior: ServerBehavior,
    key: Option<Vec<u8>>,
    interval: Option<Duration>,
) -> TestServer {
    start_server_with_gates(behavior, key, interval, None, None)
}

fn start_server_with_gates(
    behavior: ServerBehavior,
    key: Option<Vec<u8>>,
    interval: Option<Duration>,
    open_gate: Option<Arc<PacketGate>>,
    reply_gate: Option<Arc<PacketGate>>,
) -> TestServer {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let addr = socket.local_addr().unwrap();
    let records = Arc::new(Mutex::new(Vec::new()));
    let thread_records = Arc::clone(&records);
    let probe_seen = Arc::new((Mutex::new(false), Condvar::new()));
    let thread_probe_seen = Arc::clone(&probe_seen);
    let reply_sent = Arc::new((Mutex::new(false), Condvar::new()));
    let thread_reply_sent = Arc::clone(&reply_sent);
    let thread = thread::spawn(move || {
        let mut buffer = [0_u8; 8192];
        let (open_len, peer) = socket.recv_from(&mut buffer).unwrap();
        thread_records.lock().unwrap().push(PacketRecord {
            kind: PacketKind::Open,
            at: Instant::now(),
        });
        let request = decode_open_request(&buffer[..open_len], key.as_deref()).unwrap();
        if let Some(gate) = &open_gate {
            gate.arrive_and_wait();
        }
        let mut negotiated = request.params.clone();
        if let Some(interval) = interval {
            negotiated.interval_ns = i64::try_from(interval.as_nanos()).unwrap();
        }
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

        let mut deferred_burst_sent = false;
        loop {
            let Ok((len, packet_peer)) = socket.recv_from(&mut buffer) else {
                return;
            };
            let packet = &buffer[..len];
            if let Ok(probe) = decode_echo_request(packet, &negotiated, key.as_deref()) {
                thread_records.lock().unwrap().push(PacketRecord {
                    kind: PacketKind::Probe,
                    at: Instant::now(),
                });
                let (seen, ready) = &*thread_probe_seen;
                *seen.lock().unwrap() = true;
                ready.notify_all();
                if let ServerBehavior::DelayedEcho(delay) = behavior {
                    thread::sleep(delay);
                }
                if let ServerBehavior::DeferredBurst(count) = behavior {
                    if deferred_burst_sent {
                        continue;
                    }
                    deferred_burst_sent = true;
                    thread::sleep(Duration::from_millis(30));
                    for reply_index in 0..count {
                        let reply = encode_echo_reply(
                            &EchoReply {
                                flags: flags::FLAG_REPLY,
                                token: TOKEN,
                                sequence: if reply_index < 2 {
                                    probe.sequence
                                } else {
                                    u32::MAX
                                },
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
                    }
                    continue;
                }
                if let Some(gate) = &reply_gate {
                    gate.arrive_and_wait();
                }
                let peer_close = matches!(behavior, ServerBehavior::PeerClose);
                let reply = encode_echo_reply(
                    &EchoReply {
                        flags: flags::FLAG_REPLY | if peer_close { flags::FLAG_CLOSE } else { 0 },
                        token: TOKEN,
                        sequence: probe.sequence,
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
                let (sent, ready) = &*thread_reply_sent;
                *sent.lock().unwrap() = true;
                ready.notify_all();
                if peer_close {
                    return;
                }
            } else if decode_close_request(packet, key.as_deref()).is_ok() {
                thread_records.lock().unwrap().push(PacketRecord {
                    kind: PacketKind::Close,
                    at: Instant::now(),
                });
                return;
            }
        }
    });
    TestServer {
        addr,
        records,
        probe_seen,
        reply_sent,
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
            duration: Some(Duration::from_millis(130)),
            interval: Duration::from_millis(60),
            probe_timeout: Duration::from_millis(35),
            open_timeouts: vec![Duration::from_millis(200)],
            received_stats: ReceivedStats::None,
            stamp_at: StampAt::None,
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
fn drive_task_until(
    runtime: &Runtime,
    task: &mut Pin<Box<ManagedClientTask>>,
    condition: impl Fn(&ManagedClientTask) -> bool,
) {
    runtime.block_on(poll_fn(|cx| {
        assert!(
            task.as_mut().poll(cx).is_pending(),
            "task completed before condition"
        );
        if condition(task.as_ref().get_ref()) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }));
}

fn wait_flag(flag: &Arc<(Mutex<bool>, Condvar)>) {
    let (flag, ready) = &**flag;
    let value = flag.lock().unwrap();
    let (value, timeout) = ready
        .wait_timeout_while(value, Duration::from_secs(2), |value| !*value)
        .unwrap();
    assert!(*value && !timeout.timed_out());
}

fn client_events(
    observations: &Mutex<Vec<(ManagedEvent, Arc<ManagedStatus>)>>,
) -> Vec<ClientEvent> {
    observations
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(event, _)| match event {
            ManagedEvent::Client { event, .. } => Some(event.clone()),
            _ => None,
        })
        .collect()
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

fn echo_sends(
    observations: &Mutex<Vec<(ManagedEvent, Arc<ManagedStatus>)>>,
) -> Vec<(TargetInstance, u32, Instant, Instant)> {
    observations
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(event, _)| match event {
            ManagedEvent::Client {
                target,
                event:
                    ClientEvent::EchoSent {
                        seq,
                        scheduled_at,
                        sent_at,
                        ..
                    },
            } => Some((target.clone(), *seq, *scheduled_at, sent_at.mono)),
            _ => None,
        })
        .collect()
}

fn assert_rotated_without_catchup(sends: &[(TargetInstance, u32, Instant, Instant)]) {
    assert!(sends.len() >= 4);
    assert_ne!(sends[0].0, sends[1].0);
    for target in [&sends[0].0, &sends[1].0] {
        let target_sends = sends
            .iter()
            .filter(|(instance, ..)| instance == target)
            .collect::<Vec<_>>();
        assert!(target_sends.len() >= 2);
        assert!(target_sends[1].2 > target_sends[0].3);
    }
}

fn run_negotiated_stagger_case(
    requested: Duration,
    negotiated: &[Duration],
) -> (Vec<(usize, Duration)>, Vec<u64>) {
    let servers = negotiated
        .iter()
        .map(|interval| start_server_negotiating(ServerBehavior::Echo, None, Some(*interval)))
        .collect::<Vec<_>>();
    let mut managed = config(ManagedPacing::Staggered);
    managed.client.duration = Some(Duration::from_millis(190));
    managed.client.interval = requested;
    managed.client.negotiation_policy = NegotiationPolicy::Loose;
    let targets = servers
        .iter()
        .enumerate()
        .map(|(index, server)| target(&format!("target-{index}"), server.addr))
        .collect();
    let (mut task, _) = ManagedClient::task(managed, targets).unwrap();
    let observations = Arc::new(Mutex::new(Vec::new()));
    task.stagger_observations = Some(Arc::clone(&observations));
    let outcome = runtime().block_on(task);
    let packets_sent = (0..servers.len())
        .map(|index| {
            outcome
                .recent_target_outcomes
                .iter()
                .find(|target| target.target.id.as_ref() == format!("target-{index}"))
                .unwrap()
                .packets_sent
        })
        .collect();
    for server in servers {
        server.finish();
    }
    let spacings = observations.lock().unwrap().clone();
    (spacings, packets_sent)
}

fn run_deferred_drain_burst(packet_count: usize) -> ManagedOutcome {
    let server = start_server(ServerBehavior::DeferredBurst(packet_count), None);
    let mut managed = config(ManagedPacing::Staggered);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    managed.client.duration = None;
    managed.client.interval = Duration::from_millis(100);
    managed.client.probe_timeout = Duration::from_millis(60);
    managed.final_drain = Duration::from_millis(20);
    let (mut task, handle) =
        ManagedClient::task(managed, vec![target("one", server.addr)]).unwrap();
    task.drain_test_hook.defer_work_until_deadline = true;
    let seen = Arc::clone(&server.probe_seen);
    let stopper = thread::spawn(move || {
        let (flag, ready) = &*seen;
        let guard = flag.lock().unwrap();
        let (guard, timeout) = ready
            .wait_timeout_while(guard, Duration::from_secs(2), |seen| !*seen)
            .unwrap();
        assert!(*guard && !timeout.timed_out());
        drop(handle.stop());
    });
    let outcome = runtime().block_on(async {
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
    });
    stopper.join().unwrap();
    server.finish();
    outcome
}

#[test]
fn construction_is_runtime_and_io_free() {
    let before = resolution_call_counts();
    let (task, handle) = ManagedClient::task(
        config(ManagedPacing::Staggered),
        vec![ManagedTargetConfig::new("dns", "no-such-host.invalid")],
    )
    .unwrap();
    assert_eq!(resolution_call_counts(), before);
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
    let mut config = config(ManagedPacing::Staggered);
    config.completion = ManagedCompletionPolicy::ExplicitStop;
    let (task, handle) = ManagedClient::task(config, vec![]).unwrap();
    let outcome = runtime().block_on(async move {
        let mut task = Box::pin(task);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), task.as_mut())
                .await
                .is_err()
        );
        assert_eq!(handle.status().lifecycle, ManagedLifecycle::Running);
        let receipt = handle.stop();
        let outcome = task.await;
        receipt.await;
        outcome
    });
    assert_eq!(outcome.end_reason, ManagedEndReason::StopRequested);
}

#[test]
fn duplicate_and_limit_rejections_are_transactional() {
    let mut limited = config(ManagedPacing::Staggered);
    limited.max_live_target_generations = 1;
    assert!(matches!(
        ManagedClient::task(
            limited,
            vec![
                target("a", "127.0.0.1:1".parse().unwrap()),
                target("b", "127.0.0.1:2".parse().unwrap())
            ]
        ),
        Err(ManagedConfigError::TooManyTargets { .. })
    ));
    let duplicate = vec![
        target("same", "127.0.0.1:1".parse().unwrap()),
        target("same", "127.0.0.1:2".parse().unwrap()),
    ];
    assert!(matches!(
        ManagedClient::task(config(ManagedPacing::Staggered), duplicate),
        Err(ManagedConfigError::DuplicateTargetId { .. })
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
    let before = resolution_call_counts();
    let (task, handle) = ManagedClient::task(
        config(ManagedPacing::Staggered),
        vec![ManagedTargetConfig::new("one", "no-such-host.invalid")],
    )
    .unwrap();
    let mut receipt = Box::pin(handle.stop());
    let mut task = Box::pin(task);
    let Poll::Ready(outcome) = poll_once(task.as_mut()) else {
        panic!("pre-stopped task did not complete")
    };
    assert!(poll_once(receipt.as_mut()).is_ready());
    assert_eq!(outcome.end_reason, ManagedEndReason::StopRequested);
    assert_eq!(resolution_call_counts(), before);
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
    config.client.run_mode = RunMode::NoTest;
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
    configured.auth = Some(ClientAuthConfig {
        hmac_key: Some(key),
    });
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
fn peer_close_during_stop_drain_remains_authoritative() {
    let key = b"managed-drain-peer-close".to_vec();
    let server = start_server(ServerBehavior::PeerClose, Some(key.clone()));
    let mut configured = target("peer", server.addr);
    configured.auth = Some(ClientAuthConfig {
        hmac_key: Some(key),
    });
    let mut managed = config(ManagedPacing::Staggered);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    managed.client.duration = None;
    managed.client.interval = Duration::from_secs(1);
    let (task, handle) = ManagedClient::task(managed, vec![configured]).unwrap();
    let runtime = runtime();
    let mut task = Box::pin(task);
    drive_task_until(&runtime, &mut task, |task| {
        task.targets[0].counters.packets_sent == 1
    });
    drop(handle.stop());
    wait_flag(&server.reply_sent);
    let outcome = runtime.block_on(task);
    let records = server.finish();
    assert_eq!(outcome.end_reason, ManagedEndReason::StopRequested);
    assert_eq!(outcome.peer_closed_target_outcomes, 1);
    assert!(matches!(
        outcome.recent_target_outcomes[0].end_reason,
        ManagedTargetEndReason::PeerClosed
    ));
    assert!(!has_close(&records));
}

#[test]
fn stop_during_opening_finishes_in_flight_work() {
    let open_gate = Arc::new(PacketGate::default());
    let server = start_server_with_gates(
        ServerBehavior::Echo,
        None,
        None,
        Some(Arc::clone(&open_gate)),
        None,
    );
    let mut managed = config(ManagedPacing::Staggered);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    managed.client.duration = None;
    managed.client.open_timeouts = vec![Duration::from_secs(1), Duration::from_secs(1)];
    let (task, handle) =
        ManagedClient::task(managed, vec![target("success", server.addr)]).unwrap();
    let runtime = runtime();
    let mut task = Box::pin(task);
    drive_task_until(&runtime, &mut task, |task| match &task.targets[0].state {
        TargetState::Opening { open, .. } => open.has_in_flight_work(),
        _ => false,
    });
    drop(handle.stop());
    open_gate.release();
    let outcome = runtime.block_on(task);
    let records = server.finish();
    assert_eq!(outcome.end_reason, ManagedEndReason::StopRequested);
    assert!(matches!(
        outcome.recent_target_outcomes[0].end_reason,
        ManagedTargetEndReason::Stopped
    ));
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record.kind, PacketKind::Open))
            .count(),
        1
    );
    assert!(probes(&records).is_empty() && has_close(&records));
    let key = b"managed-retained-open-cleanup".to_vec();
    let cleanup_server = start_server(ServerBehavior::Echo, Some(key.clone()));
    let mut configured = target("cleanup", cleanup_server.addr);
    configured.auth = Some(ClientAuthConfig {
        hmac_key: Some(key),
    });
    let mut managed = config(ManagedPacing::Staggered);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    managed.client.duration = None;
    let (task, handle) = ManagedClient::task(managed, vec![configured]).unwrap();
    let mut task = Box::pin(task);
    drive_task_until(&runtime, &mut task, |task| {
        matches!(task.targets[0].state, TargetState::Opening { .. })
    });
    let TargetState::Opening { client, .. } = &task.as_ref().get_ref().targets[0].state else {
        unreachable!()
    };
    client.retain_open_cleanup_once();
    drive_task_until(&runtime, &mut task, |task| match &task.targets[0].state {
        TargetState::Opening { open, .. } => open.has_retained_cleanup(),
        _ => false,
    });
    drop(handle.stop());
    let outcome = runtime.block_on(task);
    let records = cleanup_server.finish();
    let target = &outcome.recent_target_outcomes[0];
    assert!(matches!(target.end_reason, ManagedTargetEndReason::Stopped));
    assert_eq!(outcome.failed_target_outcomes, 0);
    assert!(matches!(
        target.cleanup_failure,
        Some(ManagedTargetFailure {
            phase: ManagedTargetFailurePhase::Opening,
            ..
        })
    ));
    assert!(probes(&records).is_empty() && has_close(&records));
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
fn burst_pacing_fairness() {
    let first = start_server(ServerBehavior::Echo, None);
    let second = start_server(ServerBehavior::Echo, None);
    let (mut task, _) = ManagedClient::task(
        config(ManagedPacing::Burst),
        vec![target("first", first.addr), target("second", second.addr)],
    )
    .unwrap();
    let observations = Arc::new(Mutex::new(Vec::new()));
    task.event_observations = Some(Arc::clone(&observations));
    runtime().block_on(task);
    assert_rotated_without_catchup(&echo_sends(&observations));
    first.finish();
    second.finish();
}

#[test]
fn staggered_pacing_fairness() {
    let first = start_server(ServerBehavior::Echo, None);
    let second = start_server(ServerBehavior::Echo, None);
    let (mut task, _) = ManagedClient::task(
        config(ManagedPacing::Staggered),
        vec![target("first", first.addr), target("second", second.addr)],
    )
    .unwrap();
    let observations = Arc::new(Mutex::new(Vec::new()));
    task.event_observations = Some(Arc::clone(&observations));
    runtime().block_on(task);
    assert_rotated_without_catchup(&echo_sends(&observations));
    first.finish();
    second.finish();
}

#[test]
fn staggered_pacing_uses_all_active_negotiated_intervals() {
    let requested = Duration::from_millis(120);

    let (single, single_packets) =
        run_negotiated_stagger_case(requested, &[Duration::from_millis(40)]);
    assert!(single
        .iter()
        .all(|entry| *entry == (1, Duration::from_millis(40))));
    assert!(single_packets[0] >= 4);

    let (different, different_packets) = run_negotiated_stagger_case(
        requested,
        &[Duration::from_millis(30), Duration::from_millis(90)],
    );
    let while_both_active = different
        .iter()
        .filter(|(active, _)| *active == 2)
        .collect::<Vec<_>>();
    assert!(!while_both_active.is_empty());
    assert!(while_both_active
        .iter()
        .all(|(_, spacing)| *spacing == Duration::from_millis(15)));
    assert!(different_packets[0] > different_packets[1]);

    let (equal, _) = run_negotiated_stagger_case(
        requested,
        &[Duration::from_millis(60), Duration::from_millis(60)],
    );
    let while_both_active = equal
        .iter()
        .filter(|(active, _)| *active == 2)
        .collect::<Vec<_>>();
    assert!(!while_both_active.is_empty());
    assert!(while_both_active
        .iter()
        .all(|(_, spacing)| *spacing == Duration::from_millis(30)));
}

#[test]
fn stagger_gate_tracks_active_membership() {
    let first =
        start_server_negotiating(ServerBehavior::Echo, None, Some(Duration::from_millis(100)));
    let second_gate = Arc::new(PacketGate::default());
    let second = start_server_with_gates(
        ServerBehavior::Echo,
        None,
        Some(Duration::from_secs(1)),
        Some(Arc::clone(&second_gate)),
        None,
    );
    let mut managed = config(ManagedPacing::Staggered);
    managed.client.duration = Some(Duration::from_millis(1_250));
    managed.client.interval = Duration::from_secs(1);
    managed.client.negotiation_policy = NegotiationPolicy::Loose;
    managed.client.open_timeouts = vec![Duration::from_secs(2)];
    let (task, handle) = ManagedClient::task(
        managed,
        vec![target("first", first.addr), target("second", second.addr)],
    )
    .unwrap();
    let runtime = runtime();
    let mut task = Box::pin(task);
    drive_task_until(&runtime, &mut task, |task| {
        task.targets[0].counters.packets_sent == 1
    });
    let one_target_last = task.last_stagger_send.unwrap();
    let one_target_gate = task.send_gate.unwrap();
    assert_eq!(
        task.send_gate,
        one_target_last.checked_add(Duration::from_millis(100))
    );
    second_gate.release();
    drive_task_until(&runtime, &mut task, |task| task.active_count() == 2);
    let last = task.last_stagger_send.unwrap();
    let gate = task.send_gate.unwrap();
    assert_eq!(gate, last.checked_add(Duration::from_millis(50)).unwrap());
    assert!(gate <= one_target_gate);
    assert!(gate > last && gate < last.checked_add(Duration::from_millis(100)).unwrap());
    drive_task_until(&runtime, &mut task, |task| {
        task.targets[1].counters.packets_sent == 1
    });

    let accepted_at = Instant::now();
    task.record_stagger_acceptance(
        SendResult::Failed { accepted: true },
        Some((2, Duration::from_millis(50))),
        accepted_at,
    );
    assert_eq!(
        task.send_gate,
        accepted_at.checked_add(Duration::from_millis(50))
    );
    let preserved_gate = task.send_gate;
    let TargetState::Active { client } =
        mem::replace(&mut task.targets[0].state, TargetState::Terminal)
    else {
        panic!("fast target was not active before removal");
    };
    assert!(task.begin_drain(0, client, ManagedTargetEndReason::Stopped, Instant::now()));
    assert_eq!(task.send_gate, preserved_gate);
    drive_task_until(&runtime, &mut task, |task| {
        task.targets[1].counters.packets_sent == 2
    });

    drop(handle.stop());
    let outcome = runtime.block_on(task);
    assert_eq!(outcome.failed_target_outcomes, 0);
    first.finish();
    second.finish();
}

#[test]
fn overdue_queued_reply_is_lost_then_late() {
    let reply_gate = Arc::new(PacketGate::default());
    let server = start_server_with_gates(
        ServerBehavior::Echo,
        None,
        None,
        None,
        Some(Arc::clone(&reply_gate)),
    );
    let mut managed = config(ManagedPacing::Staggered);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    managed.client.duration = None;
    managed.client.interval = Duration::from_secs(1);
    managed.client.probe_timeout = Duration::from_millis(30);
    let (mut task, handle) =
        ManagedClient::task(managed, vec![target("one", server.addr)]).unwrap();
    let observations = Arc::new(Mutex::new(Vec::new()));
    task.event_observations = Some(Arc::clone(&observations));
    let runtime = runtime();
    let mut task = Box::pin(task);
    drive_task_until(&runtime, &mut task, |task| {
        task.targets[0].counters.packets_sent == 1
    });
    let timeout = match &task.targets[0].state {
        TargetState::Active { client } => client.next_probe_timeout_deadline().unwrap(),
        _ => panic!("probe sender did not remain active"),
    };
    thread::sleep(timeout.saturating_duration_since(Instant::now()));
    while Instant::now() <= timeout {
        thread::yield_now();
    }
    reply_gate.release();
    wait_flag(&server.reply_sent);
    runtime.block_on(poll_fn(|cx| {
        let ready = match &task.as_ref().get_ref().targets[0].state {
            TargetState::Active { client } => client.poll_recv_ready_for_test(cx),
            _ => panic!("probe sender did not remain active"),
        };
        ready.map(|result| result.unwrap())
    }));
    runtime.block_on(poll_fn(|cx| {
        assert!(task.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    }));
    let classified = client_events(&observations)
        .into_iter()
        .filter(|event| {
            matches!(
                event,
                ClientEvent::EchoLoss { .. }
                    | ClientEvent::LateReply { .. }
                    | ClientEvent::EchoReply { .. }
            )
        })
        .collect::<Vec<_>>();
    assert!(matches!(
        classified.as_slice(),
        [ClientEvent::EchoLoss { .. }, ClientEvent::LateReply { .. }]
    ));
    drop(handle.stop());
    let outcome = runtime.block_on(task);
    let target = &outcome.recent_target_outcomes[0];
    assert_eq!(target.replies_received, 0);
    assert_eq!(target.late, 1);
    server.finish();
}

#[test]
fn retained_probe_deadline_shortens_after_late_reply() {
    let reply_gate = Arc::new(PacketGate::default());
    let server = start_server_with_gates(
        ServerBehavior::Echo,
        None,
        None,
        None,
        Some(Arc::clone(&reply_gate)),
    );
    let mut managed = config(ManagedPacing::Staggered);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    managed.client.duration = None;
    managed.client.interval = Duration::from_secs(1);
    managed.client.probe_timeout = Duration::from_millis(100);
    managed.final_drain = Duration::from_millis(200);
    let final_drain = managed.final_drain;
    let (task, handle) = ManagedClient::task(managed, vec![target("one", server.addr)]).unwrap();
    let runtime = runtime();
    let mut task = Box::pin(task);
    drive_task_until(&runtime, &mut task, |task| {
        task.targets[0].counters.packets_sent == 1
    });
    drop(handle.stop());
    drive_task_until(&runtime, &mut task, |task| {
        matches!(task.targets[0].state, TargetState::Draining { .. })
    });
    let (drain_started_at, initial_deadline, timeout_at) = match &task.targets[0].state {
        TargetState::Draining {
            client,
            drain_started_at,
            deadline,
            ..
        } => (
            *drain_started_at,
            *deadline,
            client.next_probe_timeout_deadline().unwrap(),
        ),
        _ => unreachable!(),
    };
    assert_eq!(
        initial_deadline,
        timeout_at.checked_add(final_drain).unwrap()
    );
    thread::sleep(timeout_at.saturating_duration_since(Instant::now()));
    while Instant::now() <= timeout_at {
        thread::yield_now();
    }
    drive_task_until(&runtime, &mut task, |task| match &task.targets[0].state {
        TargetState::Draining { client, .. } => {
            client.next_probe_timeout_deadline().is_none()
                && client.latest_probe_timeout_deadline() == Some(timeout_at)
        }
        _ => false,
    });
    let retained_deadline = match &task.targets[0].state {
        TargetState::Draining { deadline, .. } => *deadline,
        _ => unreachable!(),
    };
    assert_eq!(retained_deadline, initial_deadline);
    reply_gate.release();
    wait_flag(&server.reply_sent);
    let shortened_deadline = drain_started_at.checked_add(final_drain).unwrap();
    drive_task_until(&runtime, &mut task, |task| {
        matches!(
            task.targets[0].state,
            TargetState::Draining { deadline, .. } if deadline == shortened_deadline
        )
    });
    assert!(shortened_deadline < initial_deadline);
    let outcome = runtime.block_on(task);
    let records = server.finish();
    let target = &outcome.recent_target_outcomes[0];
    assert_eq!(outcome.end_reason, ManagedEndReason::StopRequested);
    assert!(matches!(target.end_reason, ManagedTargetEndReason::Stopped));
    assert_eq!(target.packets_sent, 1);
    assert_eq!(target.replies_received, 0);
    assert_eq!(target.late, 1);
    assert_eq!(probes(&records).len(), 1);
}

#[test]
fn pending_limit_failure_drains_reply_and_closes_session() {
    let key = b"managed-send-failure-cleanup".to_vec();
    let server = start_server(
        ServerBehavior::DelayedEcho(Duration::from_millis(80)),
        Some(key.clone()),
    );
    let mut managed = config(ManagedPacing::Staggered);
    managed.client.duration = Some(Duration::from_millis(250));
    managed.client.interval = Duration::from_millis(20);
    managed.client.probe_timeout = Duration::from_secs(2);
    managed.client.max_pending_probes = 1;
    managed.final_drain = Duration::from_millis(50);
    let mut configured = target("one", server.addr);
    configured.auth = Some(ClientAuthConfig {
        hmac_key: Some(key),
    });
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
fn drain_failures_preserve_primary_outcome_and_first_cleanup_failure() {
    let completed_server = start_server(ServerBehavior::Echo, None);
    let (mut completed_task, _) = ManagedClient::task(
        config(ManagedPacing::Staggered),
        vec![target("completed", completed_server.addr)],
    )
    .unwrap();
    completed_task.drain_test_hook.fail_receive = true;
    completed_task.drain_test_hook.fail_close = true;
    let completed = runtime().block_on(completed_task);
    let completed_target = &completed.recent_target_outcomes[0];
    assert!(matches!(
        completed_target.end_reason,
        ManagedTargetEndReason::TestComplete
    ));
    assert_eq!(completed.failed_target_outcomes, 0);
    assert!(matches!(
        completed_target.cleanup_failure,
        Some(ManagedTargetFailure {
            phase: ManagedTargetFailurePhase::Receiving,
            kind: ManagedTargetFailureKind::Socket,
            ..
        })
    ));
    completed_server.finish();

    let stopped_server = start_server(ServerBehavior::Echo, None);
    let mut managed = config(ManagedPacing::Staggered);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    managed.client.duration = None;
    let (mut stopped_task, handle) =
        ManagedClient::task(managed, vec![target("stopped", stopped_server.addr)]).unwrap();
    stopped_task.drain_test_hook.fail_receive = true;
    let seen = Arc::clone(&stopped_server.probe_seen);
    let stopper = thread::spawn(move || {
        let (flag, ready) = &*seen;
        let guard = flag.lock().unwrap();
        let (guard, timeout) = ready
            .wait_timeout_while(guard, Duration::from_secs(2), |seen| !*seen)
            .unwrap();
        assert!(*guard && !timeout.timed_out());
        drop(handle.stop());
    });
    let stopped = runtime().block_on(stopped_task);
    stopper.join().unwrap();
    let stopped_target = &stopped.recent_target_outcomes[0];
    assert!(matches!(
        stopped_target.end_reason,
        ManagedTargetEndReason::Stopped
    ));
    assert_eq!(stopped.failed_target_outcomes, 0);
    assert!(matches!(
        stopped_target.cleanup_failure,
        Some(ManagedTargetFailure {
            phase: ManagedTargetFailurePhase::Receiving,
            kind: ManagedTargetFailureKind::Socket,
            ..
        })
    ));
    stopped_server.finish();
}

#[test]
fn final_drain_overflow_is_rejected_and_preserves_success() {
    let mut invalid = config(ManagedPacing::Staggered);
    invalid.final_drain = Duration::MAX;
    assert!(matches!(
        ManagedClient::task(
            invalid,
            vec![target("invalid", "127.0.0.1:9".parse().unwrap())]
        ),
        Err(ManagedConfigError::UnschedulableFinalDrain {
            duration: Duration::MAX
        })
    ));

    let server = start_server(ServerBehavior::Echo, None);
    let (mut task, _) = ManagedClient::task(
        config(ManagedPacing::Staggered),
        vec![target("completed", server.addr)],
    )
    .unwrap();
    task.config.final_drain = Duration::MAX;
    let outcome = runtime().block_on(task);
    let target = &outcome.recent_target_outcomes[0];
    assert!(matches!(
        target.end_reason,
        ManagedTargetEndReason::TestComplete
    ));
    assert_eq!(outcome.successful_target_outcomes, 1);
    assert_eq!(outcome.failed_target_outcomes, 0);
    assert!(matches!(
        target.cleanup_failure,
        Some(ManagedTargetFailure {
            phase: ManagedTargetFailurePhase::Timing,
            kind: ManagedTargetFailureKind::ResourceExhausted,
            ..
        })
    ));
    assert!(has_close(&server.finish()));
}

#[test]
fn post_deadline_receive_sweep_drains_queued_packets_and_is_bounded() {
    let drained = run_deferred_drain_burst(3);
    let drained_target = &drained.recent_target_outcomes[0];
    assert!(matches!(
        drained_target.end_reason,
        ManagedTargetEndReason::Stopped
    ));
    assert_eq!(drained_target.replies_received, 0);
    assert_eq!(drained_target.duplicates, 1);
    assert_eq!(drained_target.late, 2);

    let bounded = run_deferred_drain_burst(POST_DEADLINE_RECEIVE_BUDGET + 64);
    let bounded_target = &bounded.recent_target_outcomes[0];
    assert!(matches!(
        bounded_target.end_reason,
        ManagedTargetEndReason::Stopped
    ));
    assert_eq!(
        bounded_target.replies_received + bounded_target.duplicates + bounded_target.late,
        u64::try_from(POST_DEADLINE_RECEIVE_BUDGET).unwrap()
    );
    assert_eq!(bounded.failed_target_outcomes, 0);
}

#[test]
fn event_loss_independence() {
    let server = start_server(ServerBehavior::Echo, None);
    let mut config = config(ManagedPacing::Staggered);
    config.event_capacity = 1;
    let (task, handle) = ManagedClient::task(config, vec![target("one", server.addr)]).unwrap();
    let mut events = handle.subscribe().unwrap();
    let outcome = runtime().block_on(task);
    assert!(matches!(
        events.try_recv(),
        Err(broadcast::error::TryRecvError::Lagged(_))
    ));
    assert_eq!(outcome.successful_target_outcomes, 1);
    assert_eq!(handle.status().lifecycle, ManagedLifecycle::Completed);
    server.finish();
}

#[test]
fn outcome_history_eviction_preserves_aggregates() {
    let servers = [
        start_server(ServerBehavior::NoTest, None),
        start_server(ServerBehavior::NoTest, None),
        start_server(ServerBehavior::NoTest, None),
    ];
    let mut config = config(ManagedPacing::Staggered);
    config.client.run_mode = RunMode::NoTest;
    config.outcome_history_limit = 1;
    let targets = servers
        .iter()
        .enumerate()
        .map(|(index, server)| target(&format!("target-{index}"), server.addr))
        .collect();
    let (mut task, _) = ManagedClient::task(config, targets).unwrap();
    let observations = Arc::new(Mutex::new(Vec::new()));
    task.event_observations = Some(Arc::clone(&observations));
    let outcome = runtime().block_on(task);
    assert_eq!(outcome.total_target_outcomes, 3);
    assert_eq!(outcome.successful_target_outcomes, 3);
    assert_eq!(outcome.discarded_target_outcomes, 2);
    assert_eq!(outcome.recent_target_outcomes.len(), 1);
    let last_finished = observations
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(event, _)| match event {
            ManagedEvent::TargetFinished { outcome } => Some(outcome.target.clone()),
            _ => None,
        })
        .next_back()
        .unwrap();
    assert_eq!(outcome.recent_target_outcomes[0].target, last_finished);
    for server in servers {
        server.finish();
    }
}

#[test]
fn status_precedes_corresponding_events() {
    let server = start_server(ServerBehavior::NoTest, None);
    let mut config = config(ManagedPacing::Staggered);
    config.client.run_mode = RunMode::NoTest;
    let (mut task, _) = ManagedClient::task(config, vec![target("one", server.addr)]).unwrap();
    let observations = Arc::new(Mutex::new(Vec::new()));
    task.event_observations = Some(Arc::clone(&observations));
    runtime().block_on(task);
    let mut saw_started = false;
    let mut saw_state = false;
    let mut saw_finished = false;
    let mut saw_stopping = false;
    let mut saw_completed = false;
    for (event, status) in observations.lock().unwrap().iter() {
        match event {
            ManagedEvent::Started => {
                saw_started = true;
                assert_eq!(status.lifecycle, ManagedLifecycle::Running);
            }
            ManagedEvent::TargetStateChanged { target, lifecycle } => {
                saw_state = true;
                assert!(status
                    .targets
                    .iter()
                    .any(|entry| entry.target == *target && entry.lifecycle == *lifecycle));
            }
            ManagedEvent::TargetFinished { .. } => {
                saw_finished = true;
                assert!(status.total_target_outcomes >= 1);
            }
            ManagedEvent::Stopping => {
                saw_stopping = true;
                assert_eq!(status.lifecycle, ManagedLifecycle::Stopping);
            }
            ManagedEvent::Completed { .. } => {
                saw_completed = true;
                assert_eq!(status.lifecycle, ManagedLifecycle::Completed);
            }
            _ => {}
        }
    }
    assert!(saw_started && saw_state && saw_finished && saw_stopping && saw_completed);
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
    let (task, handle) = ManagedClient::task(
        config(ManagedPacing::Staggered),
        vec![target("one", "127.0.0.1:9".parse().unwrap())],
    )
    .unwrap();
    let mut events = handle.subscribe().unwrap();
    drop(task);
    assert_eq!(handle.status().lifecycle, ManagedLifecycle::Abandoned);
    assert_eq!(handle.status().total_target_outcomes, 0);
    assert!(matches!(events.try_recv(), Ok(ManagedEvent::Abandoned)));
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
