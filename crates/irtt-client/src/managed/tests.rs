use std::{
    future::{poll_fn, Future},
    mem,
    net::{SocketAddr, UdpSocket},
    pin::Pin,
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll, Waker},
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime},
};

use irtt_proto::{
    decode_request, encode_echo_reply, encode_open_reply, flags, verify_packet_hmac,
    DecodedRequestKind, EchoReply, OpenReply, Params, ReceivedStats, StampAt, TimestampFields,
};
use tokio::runtime::{Builder, Runtime};

use super::*;
use crate::{
    probe::PendingProbe, socket::resolution_call_counts, ClientAuthConfig, ClientTimestamp,
    NegotiationPolicy, RunMode,
};

const TOKEN: u64 = 0x1234_5678_90ab_cdef;

#[derive(Clone, Copy)]
enum ServerBehavior {
    Echo,
    NoTest,
    PeerClose,
    DelayedEcho(Duration),
    DeferredBurst(usize),
    NoisyOpen(usize),
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
    open_reply_sent: Arc<(Mutex<bool>, Condvar)>,
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
    let open_reply_sent = Arc::new((Mutex::new(false), Condvar::new()));
    let thread_open_reply_sent = Arc::clone(&open_reply_sent);
    let thread = thread::spawn(move || {
        let mut buffer = [0_u8; 8192];
        let (open_len, peer) = socket.recv_from(&mut buffer).unwrap();
        thread_records.lock().unwrap().push(PacketRecord {
            kind: PacketKind::Open,
            at: Instant::now(),
        });
        let mut negotiated = decode_open_params(&buffer[..open_len], key.as_deref());
        if let Some(gate) = &open_gate {
            gate.arrive_and_wait();
        }
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
        let noisy_open = matches!(behavior, ServerBehavior::NoisyOpen(_));
        if let ServerBehavior::NoisyOpen(count) = behavior {
            for _ in 0..count {
                socket.send_to(&[0_u8], peer).unwrap();
            }
        }
        socket.send_to(&open_reply, peer).unwrap();
        let (sent, ready) = &*thread_open_reply_sent;
        *sent.lock().unwrap() = true;
        ready.notify_all();
        if no_test || noisy_open {
            return;
        }

        let mut deferred_burst_sent = false;
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
                                    sequence: if reply_index < 2 { sequence } else { u32::MAX },
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
                    let (sent, ready) = &*thread_reply_sent;
                    *sent.lock().unwrap() = true;
                    ready.notify_all();
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
        probe_seen,
        reply_sent,
        open_reply_sent,
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

fn timeout_probe(seq: u32, sent_at: Instant, timeout_at: Instant) -> PendingProbe {
    PendingProbe {
        wire_seq: seq,
        sent_at: ClientTimestamp {
            mono: sent_at,
            wall: SystemTime::UNIX_EPOCH,
        },
        timeout_at,
        tx_not_before_wall: SystemTime::UNIX_EPOCH,
        kernel_tx_timestamp: None,
    }
}

fn open_target_for_timeout_test(runtime: &Runtime, task: &mut ManagedClientTask, index: usize) {
    let TargetState::Pending { client_config } =
        mem::replace(&mut task.targets[index].state, TargetState::Terminal)
    else {
        panic!("timeout test target must start pending");
    };
    let client = runtime.block_on(async {
        let mut client = AsyncClient::connect(client_config).await.unwrap();
        client.open().await.unwrap();
        client
    });
    task.targets[index].desired = false;
    task.install_target_state(index, TargetState::Active { client });
}

fn replace_pending_for_timeout_test(
    task: &mut ManagedClientTask,
    index: usize,
    probe: PendingProbe,
) {
    let client = match &mut task.targets[index].state {
        TargetState::Active { client } | TargetState::Draining { client, .. } => client,
        _ => panic!("timeout test target must be active or draining"),
    };
    client.replace_pending_for_test(probe);
}

fn remove_pending_for_timeout_test(
    task: &mut ManagedClientTask,
    index: usize,
    wire_seq: u32,
) -> PendingProbe {
    let client = match &mut task.targets[index].state {
        TargetState::Active { client } | TargetState::Draining { client, .. } => client,
        _ => panic!("timeout test target must be active or draining"),
    };
    client
        .remove_pending_for_test(wire_seq)
        .expect("timeout test pending probe must exist")
}

fn close_timeout_test_target(runtime: &Runtime, task: &mut ManagedClientTask, index: usize) {
    let (TargetState::Active { mut client } | TargetState::Draining { mut client, .. }) =
        mem::replace(&mut task.targets[index].state, TargetState::Terminal)
    else {
        panic!("timeout test target did not remain active or draining");
    };
    runtime.block_on(client.close()).unwrap();
}

fn poll_task_once(runtime: &Runtime, task: &mut Pin<Box<ManagedClientTask>>) {
    runtime.block_on(poll_fn(|cx| {
        assert!(task.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    }));
}

fn timeout_losses_for(
    observations: &Mutex<Vec<(ManagedEvent, Arc<ManagedStatus>)>>,
    id: &str,
) -> usize {
    observations
        .lock()
        .unwrap()
        .iter()
        .filter(|(event, _)| {
            matches!(
                event,
                ManagedEvent::Client {
                    target,
                    event: ClientEvent::EchoLoss { .. },
                } if target.id.as_ref() == id
            )
        })
        .count()
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
fn dynamic_addition_opens_and_operates() {
    let server = start_server(ServerBehavior::Echo, None);
    let mut managed = config(ManagedPacing::Staggered);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    managed.client.duration = None;
    let (task, handle) = ManagedClient::task(managed, vec![]).unwrap();
    let receipt = handle
        .update_targets(vec![target("added", server.addr)])
        .unwrap();
    let runtime = runtime();
    let mut task = Box::pin(task);
    drive_task_until(&runtime, &mut task, |task| {
        task.targets[0].counters.packets_sent != 0
    });
    let acknowledgement = runtime.block_on(receipt).unwrap();
    assert_eq!(acknowledgement.sequence, 1);
    assert_eq!(acknowledgement.status.targets[0].target.generation, 1);
    assert!(acknowledgement.status.targets[0].desired);
    let stop = handle.stop();
    let outcome = runtime.block_on(task);
    runtime.block_on(stop);
    assert_eq!(outcome.applied_command_sequence, 1);
    assert!(!probes(&server.finish()).is_empty());
}

#[test]
fn dynamic_identical_open_target_preserves_session_without_pending_event() {
    let server = start_server(ServerBehavior::Echo, None);
    let mut managed = config(ManagedPacing::Staggered);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    managed.client.duration = None;
    let configured = target("same", server.addr);
    let (mut task, handle) = ManagedClient::task(managed, vec![configured.clone()]).unwrap();
    let observations = Arc::new(Mutex::new(Vec::new()));
    task.event_observations = Some(Arc::clone(&observations));
    let runtime = runtime();
    let mut task = Box::pin(task);
    drive_task_until(&runtime, &mut task, |task| {
        task.targets[0].counters.packets_sent >= 2
    });
    let instance = task.targets[0].instance.clone();
    let sent = task.targets[0].counters.packets_sent;
    let receipt = handle.update_targets(vec![configured]).unwrap();
    drive_task_until(&runtime, &mut task, |task| {
        task.applied_command_sequence == 1
    });
    let acknowledgement = runtime.block_on(receipt).unwrap();
    assert_eq!(acknowledgement.status.targets[0].target, instance);
    drive_task_until(&runtime, &mut task, |task| {
        task.targets[0].counters.packets_sent > sent
    });
    assert!(!observations.lock().unwrap().iter().any(|(event, _)| {
        matches!(event, ManagedEvent::TargetStateChanged { target, lifecycle: ManagedTargetLifecycle::Pending } if *target == instance)
    }));
    let stop = handle.stop();
    runtime.block_on(task);
    runtime.block_on(stop);
    assert_eq!(
        server
            .finish()
            .iter()
            .filter(|record| matches!(record.kind, PacketKind::Open))
            .count(),
        1
    );
}

#[test]
fn dynamic_retiring_active_is_ineligible_before_target_cleanup() {
    let server = start_server(ServerBehavior::Echo, None);
    let mut managed = config(ManagedPacing::Staggered);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    managed.client.duration = None;
    let (task, handle) = ManagedClient::task(managed, vec![target("old", server.addr)]).unwrap();
    let runtime = runtime();
    let mut task = Box::pin(task);
    drive_task_until(&runtime, &mut task, |task| {
        task.targets[0].counters.packets_sent != 0
    });
    let old = task.targets[0].instance.clone();
    let sent = task.targets[0].counters.packets_sent;
    let acknowledgement = task
        .as_mut()
        .get_mut()
        .apply_targets(vec![ManagedTargetConfig::new("old", "127.0.0.1:9")])
        .unwrap();
    assert!(acknowledgement
        .status
        .targets
        .iter()
        .any(|target| target.target == old && !target.desired));
    assert!(matches!(task.targets[0].state, TargetState::Active { .. }));
    assert_eq!(
        task.targets[0].retirement,
        Some(ManagedTargetEndReason::Replaced)
    );
    let result = runtime.block_on(poll_fn(|cx| {
        Poll::Ready(
            task.as_mut()
                .get_mut()
                .poll_one_send(0, cx, Instant::now(), None),
        )
    }));
    assert!(matches!(result, SendResult::NotAttempted));
    assert_eq!(task.targets[0].counters.packets_sent, sent);
    let stop = handle.stop();
    runtime.block_on(task);
    runtime.block_on(stop);
    server.finish();
}

#[test]
fn dynamic_replacement_counts_sync_and_async_live_generations() {
    let mut managed = config(ManagedPacing::Staggered);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    managed.max_live_target_generations = 1;
    let (mut pending, _) = ManagedClient::task(
        managed.clone(),
        vec![ManagedTargetConfig::new("same", "127.0.0.1:9")],
    )
    .unwrap();
    let acknowledgement = pending
        .apply_targets(vec![ManagedTargetConfig::new("same", "127.0.0.1:10")])
        .unwrap();
    assert_eq!(acknowledgement.status.targets.len(), 1);
    assert_eq!(acknowledgement.status.targets[0].target.generation, 2);
    assert!(matches!(
        acknowledgement.status.recent_target_outcomes[0].end_reason,
        ManagedTargetEndReason::Replaced
    ));

    let server = start_server(ServerBehavior::Echo, None);
    let replacement = start_server(ServerBehavior::Echo, None);
    managed.client.duration = None;
    let (task, handle) = ManagedClient::task(managed, vec![target("same", server.addr)]).unwrap();
    let runtime = runtime();
    let mut task = Box::pin(task);
    drive_task_until(&runtime, &mut task, |task| {
        task.targets[0].counters.packets_sent != 0
    });
    let before = task.targets[0].instance.clone();
    let before_status = task.snapshot();
    let before_generation = task.next_generation;
    let before_sequence = task.applied_command_sequence;
    assert!(matches!(
        task.as_mut()
            .get_mut()
            .apply_targets(vec![target("same", replacement.addr)]),
        Err(ManagedCommandApplyError::LiveGenerationLimitExceeded {
            required: 2,
            limit: 1
        })
    ));
    assert_eq!(task.snapshot(), before_status);
    assert_eq!(task.next_generation, before_generation);
    assert_eq!(task.applied_command_sequence, before_sequence);
    assert_eq!(task.targets.len(), 1);
    task.config.max_live_target_generations = 2;
    let acknowledgement = task
        .as_mut()
        .get_mut()
        .apply_targets(vec![target("same", replacement.addr)])
        .unwrap();
    assert!(acknowledgement
        .status
        .targets
        .iter()
        .any(|target| target.target == before && !target.desired));
    assert_eq!(acknowledgement.status.targets.len(), 2);
    drive_task_until(&runtime, &mut task, |task| {
        task.targets
            .iter()
            .any(|target| target.desired && target.counters.packets_sent != 0)
    });
    let stop = handle.stop();
    let outcome = runtime.block_on(task);
    runtime.block_on(stop);
    assert!(outcome.recent_target_outcomes.iter().any(|outcome| {
        outcome.target == before && matches!(outcome.end_reason, ManagedTargetEndReason::Replaced)
    }));
    server.finish();
    replacement.finish();
}

#[test]
fn terminal_targets_count_toward_retained_generation_limit() {
    let mut managed = config(ManagedPacing::Staggered);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    managed.max_live_target_generations = 1;

    let (mut task, _) = ManagedClient::task(
        managed.clone(),
        vec![ManagedTargetConfig::new("same", "127.0.0.1:9")],
    )
    .unwrap();
    task.targets[0].state = TargetState::Terminal;
    assert!(matches!(
        task.apply_targets(vec![
            ManagedTargetConfig::new("same", "127.0.0.1:9"),
            ManagedTargetConfig::new("other", "127.0.0.1:10"),
        ]),
        Err(ManagedCommandApplyError::LiveGenerationLimitExceeded {
            required: 2,
            limit: 1,
        })
    ));

    let (mut task, _) = ManagedClient::task(
        managed.clone(),
        vec![ManagedTargetConfig::new("same", "127.0.0.1:9")],
    )
    .unwrap();
    task.targets[0].state = TargetState::Terminal;
    let acknowledgement = task
        .apply_targets(vec![ManagedTargetConfig::new("same", "127.0.0.1:10")])
        .unwrap();
    assert_eq!(task.targets.len(), 1);
    assert_eq!(acknowledgement.status.targets.len(), 1);
    assert_eq!(acknowledgement.status.targets[0].target.generation, 2);

    let (mut task, _) = ManagedClient::task(
        managed,
        vec![ManagedTargetConfig::new("same", "127.0.0.1:9")],
    )
    .unwrap();
    task.targets[0].state = TargetState::Terminal;
    task.apply_targets(Vec::new()).unwrap();
    assert!(task.targets.is_empty());
    assert!(task.snapshot().targets.is_empty());
}

#[test]
fn pruning_leaves_no_stale_target_work() {
    let (mut task, _) = ManagedClient::task(
        config(ManagedPacing::Staggered),
        vec![
            ManagedTargetConfig::new("one", "127.0.0.1:9"),
            ManagedTargetConfig::new("two", "127.0.0.1:10"),
            ManagedTargetConfig::new("three", "127.0.0.1:11"),
        ],
    )
    .unwrap();
    task.cursor = 2;
    task.timeout_cursor = 2;
    task.send_cursor = 2;
    task.scan_remaining = 3;
    task.burst_remaining = 3;
    task.stagger_remaining = 3;

    task.targets[2].desired = false;
    task.targets[2].state = TargetState::Terminal;
    task.prune_undesired_terminal();

    // Timeout discovery indexes `targets` with `timeout_cursor` directly, so a
    // prune must leave it safe to use; the target and send passes modulo their
    // own cursors. Pending per-pass work must not outlive the targets it was
    // scheduled for either.
    assert_eq!(task.targets.len(), 2);
    assert!(task.timeout_cursor < task.targets.len());
    assert!(task.scan_remaining <= task.targets.len());
    assert!(task.burst_remaining <= task.targets.len());
    assert!(task.stagger_remaining <= task.targets.len());
    assert!(!task.poll_timeout_pass(Instant::now()));

    for target in &mut task.targets {
        target.desired = false;
        target.state = TargetState::Terminal;
    }
    task.prune_undesired_terminal();

    assert!(task.targets.is_empty());
    assert_eq!(task.scan_remaining, 0);
    assert_eq!(task.burst_remaining, 0);
    assert_eq!(task.stagger_remaining, 0);
    assert!(!task.poll_timeout_pass(Instant::now()));
}

#[test]
fn dynamic_rejections_leave_transaction_state_unchanged() {
    let mut managed = config(ManagedPacing::Staggered);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    let (mut task, _) = ManagedClient::task(
        managed,
        vec![ManagedTargetConfig::new("same", "127.0.0.1:9")],
    )
    .unwrap();
    let status = task.snapshot();
    let generation = task.next_generation;
    let sequence = task.applied_command_sequence;
    let instances = task
        .targets
        .iter()
        .map(|target| (target.instance.clone(), target.desired))
        .collect::<Vec<_>>();
    let duplicate = vec![
        ManagedTargetConfig::new("same", "127.0.0.1:10"),
        ManagedTargetConfig::new("same", "127.0.0.1:11"),
    ];
    assert!(task.apply_targets(duplicate).is_err());
    task.config.client.open_timeouts.clear();
    assert!(task
        .apply_targets(vec![ManagedTargetConfig::new("same", "127.0.0.1:10")])
        .is_err());
    assert_eq!(task.next_generation, generation);
    assert_eq!(task.applied_command_sequence, sequence);
    assert_eq!(task.snapshot(), status);
    assert_eq!(
        task.targets
            .iter()
            .map(|target| (target.instance.clone(), target.desired))
            .collect::<Vec<_>>(),
        instances
    );
}

#[test]
fn dynamic_empty_update_orders_stopping_before_acknowledgement() {
    let mut managed = config(ManagedPacing::Staggered);
    managed.command_capacity = 2;
    let (mut task, handle) = ManagedClient::task(
        managed,
        vec![ManagedTargetConfig::new("gone", "127.0.0.1:9")],
    )
    .unwrap();
    let observations = Arc::new(Mutex::new(Vec::new()));
    task.event_observations = Some(Arc::clone(&observations));
    let removal = handle.update_targets(vec![]).unwrap();
    let later = handle
        .update_targets(vec![ManagedTargetConfig::new("later", "127.0.0.1:9")])
        .unwrap();
    let outcome = runtime().block_on(task);
    let acknowledgement = runtime().block_on(removal).unwrap();
    assert_eq!(acknowledgement.status.lifecycle, ManagedLifecycle::Stopping);
    assert_eq!(acknowledgement.status.desired_target_count, 0);
    assert!(matches!(
        runtime().block_on(later),
        Err(ManagedCommandApplyError::Stopping)
    ));
    assert_eq!(outcome.end_reason, ManagedEndReason::TargetsComplete);
    let events = observations.lock().unwrap();
    let stopping = events
        .iter()
        .position(|(event, _)| matches!(event, ManagedEvent::Stopping))
        .unwrap();
    let finished = events
        .iter()
        .position(|(event, _)| matches!(event, ManagedEvent::TargetFinished { .. }))
        .unwrap();
    assert!(stopping < finished);
    assert_eq!(events[stopping].1.lifecycle, ManagedLifecycle::Stopping);
    assert!(Arc::ptr_eq(&acknowledgement.status, &events[stopping].1));
}

#[test]
fn noisy_opening_yields_before_stop_observation() {
    let open_gate = Arc::new(PacketGate::default());
    let server = start_server_with_gates(
        ServerBehavior::NoisyOpen(256),
        None,
        None,
        Some(Arc::clone(&open_gate)),
        None,
    );
    let mut managed = config(ManagedPacing::Staggered);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    managed.client.duration = None;
    let (task, handle) = ManagedClient::task(managed, vec![target("noisy", server.addr)]).unwrap();
    let runtime = runtime();
    let mut task = Box::pin(task);

    drive_task_until(&runtime, &mut task, |task| {
        matches!(task.targets[0].state, TargetState::Opening { .. })
    });
    drive_task_until(&runtime, &mut task, |task| {
        matches!(
            task.targets[0].state,
            TargetState::Opening { ref open, .. } if open.has_in_flight_work()
        )
    });
    let seen = open_gate.seen.lock().unwrap();
    let (seen, timeout) = open_gate
        .seen_ready
        .wait_timeout_while(seen, Duration::from_secs(2), |seen| !*seen)
        .unwrap();
    assert!(*seen && !timeout.timed_out());
    drop(seen);
    open_gate.release();
    wait_flag(&server.open_reply_sent);

    // One managed task poll must leave a valid open reply behind the noisy
    // queue, rather than letting this target consume all immediate work.
    let poll = runtime.block_on(poll_fn(|cx| Poll::Ready(task.as_mut().poll(cx))));
    assert!(poll.is_pending());
    assert!(matches!(task.targets[0].state, TargetState::Opening { .. }));

    let receipt = handle.stop();
    let poll = runtime.block_on(poll_fn(|cx| Poll::Ready(task.as_mut().poll(cx))));
    assert!(poll.is_pending());
    assert!(handle.status().stop_requested);
    assert_eq!(handle.status().lifecycle, ManagedLifecycle::Stopping);
    let outcome = runtime.block_on(task);
    runtime.block_on(receipt);
    assert_eq!(outcome.end_reason, ManagedEndReason::StopRequested);
    server.finish();
}

#[test]
fn stop_during_quiescent_stopping_is_durable_without_duplicate_event() {
    let server = start_server(ServerBehavior::Echo, None);
    let mut managed = config(ManagedPacing::Staggered);
    managed.client.duration = None;
    managed.final_drain = Duration::from_millis(80);
    let (mut task, handle) =
        ManagedClient::task(managed, vec![target("old", server.addr)]).unwrap();
    let observations = Arc::new(Mutex::new(Vec::new()));
    task.event_observations = Some(Arc::clone(&observations));
    let runtime = runtime();
    let mut task = Box::pin(task);
    drive_task_until(&runtime, &mut task, |task| task.active_count() == 1);

    let removal = handle.update_targets(vec![]).unwrap();
    drive_task_until(&runtime, &mut task, |task| {
        task.state == DriverState::Stopping
            && !task.stop_observed
            && task.targets.iter().any(|target| !target.desired)
    });
    let acknowledgement = runtime.block_on(removal).unwrap();
    assert_eq!(acknowledgement.status.lifecycle, ManagedLifecycle::Stopping);

    let stop = handle.stop();
    let outcome = runtime.block_on(task);
    runtime.block_on(stop);
    assert_eq!(outcome.end_reason, ManagedEndReason::StopRequested);
    assert!(handle.status().stop_requested);
    assert!(outcome
        .recent_target_outcomes
        .iter()
        .any(|outcome| { matches!(outcome.end_reason, ManagedTargetEndReason::Removed) }));
    assert_eq!(
        observations
            .lock()
            .unwrap()
            .iter()
            .filter(|(event, _)| matches!(event, ManagedEvent::Stopping))
            .count(),
        1
    );
    server.finish();
}

#[test]
fn stop_before_enqueue_rejects_without_allocating_a_generation() {
    let mut managed = config(ManagedPacing::Staggered);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    let (task, handle) = ManagedClient::task(managed, vec![]).unwrap();
    let initial = handle.status();
    let stop = handle.stop();
    assert!(matches!(
        handle.update_targets(vec![ManagedTargetConfig::new("later", "127.0.0.1:9")]),
        Err(ManagedCommandError::Stopping)
    ));
    assert_eq!(task.next_generation, 1);
    assert_eq!(task.applied_command_sequence, 0);
    assert_eq!(handle.status(), initial);
    let outcome = runtime().block_on(task);
    runtime().block_on(stop);
    assert_eq!(outcome.end_reason, ManagedEndReason::StopRequested);
}

#[test]
fn paused_submission_cannot_outlive_terminal_seal() {
    let mut managed = config(ManagedPacing::Staggered);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    let (task, mut handle) = ManagedClient::task(managed, vec![]).unwrap();
    let hook = Arc::new(UpdateSubmissionTestHook::default());
    hook.arm();
    handle.update_submission_hook = Some(Arc::clone(&hook));
    let submitter = handle.clone();
    let sender = thread::spawn(move || {
        submitter.update_targets(vec![ManagedTargetConfig::new("late", "127.0.0.1:9")])
    });
    hook.wait_until_arrived();

    let stop = handle.stop();
    let runtime = runtime();
    let mut task = Box::pin(task);
    let Poll::Ready(outcome) = runtime.block_on(poll_fn(|cx| Poll::Ready(task.as_mut().poll(cx))))
    else {
        panic!("empty stopped task did not seal")
    };
    runtime.block_on(stop);
    assert_eq!(outcome.end_reason, ManagedEndReason::StopRequested);

    hook.release();
    assert!(matches!(
        sender.join().unwrap(),
        Err(ManagedCommandError::DriverClosed)
    ));
    drop(task);
}

#[test]
fn paused_submission_can_enqueue_during_stopping() {
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
    managed.client.probe_timeout = Duration::from_secs(1);
    let (task, mut handle) =
        ManagedClient::task(managed, vec![target("old", server.addr)]).unwrap();
    let hook = Arc::new(UpdateSubmissionTestHook::default());
    hook.arm();
    handle.update_submission_hook = Some(Arc::clone(&hook));
    let runtime = runtime();
    let mut task = Box::pin(task);
    drive_task_until(&runtime, &mut task, |task| {
        task.targets[0].counters.packets_sent == 1
    });
    wait_flag(&server.probe_seen);

    let next_generation = task.next_generation;
    let applied_command_sequence = task.applied_command_sequence;
    let desired_membership = task
        .targets
        .iter()
        .map(|target| (target.instance.clone(), target.desired))
        .collect::<Vec<_>>();
    let submitter = handle.clone();
    let sender = thread::spawn(move || {
        submitter.update_targets(vec![ManagedTargetConfig::new("new", "127.0.0.1:9")])
    });
    hook.wait_until_arrived();

    let stop = handle.stop();
    drive_task_until(&runtime, &mut task, |task| {
        task.state == DriverState::Stopping
            && task.stop_observed
            && task.resources().stop.update_admission() == UpdateAdmission::Stopping
            && matches!(task.targets[0].state, TargetState::Draining { .. })
    });
    let stopping_status = handle.status();
    assert_eq!(stopping_status.lifecycle, ManagedLifecycle::Stopping);

    hook.release();
    let receipt = sender
        .join()
        .unwrap()
        .expect("send must succeed while the receiver remains open during stopping");

    runtime.block_on(poll_fn(|cx| {
        assert!(task.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    }));
    let result = runtime.block_on(receipt);
    assert!(!matches!(
        &result,
        Err(ManagedCommandApplyError::AcknowledgementDisconnected)
    ));
    assert!(matches!(result, Err(ManagedCommandApplyError::Stopping)));
    assert_eq!(task.applied_command_sequence, applied_command_sequence);
    assert_eq!(task.next_generation, next_generation);
    assert_eq!(
        task.targets
            .iter()
            .map(|target| (target.instance.clone(), target.desired))
            .collect::<Vec<_>>(),
        desired_membership
    );
    assert_eq!(handle.status(), stopping_status);

    reply_gate.release();
    wait_flag(&server.reply_sent);
    let outcome = runtime.block_on(task);
    runtime.block_on(stop);
    assert_eq!(outcome.end_reason, ManagedEndReason::StopRequested);
    server.finish();
}

#[test]
fn accepted_update_before_stop_resolves_stopping() {
    let mut managed = config(ManagedPacing::Staggered);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    let (task, handle) = ManagedClient::task(managed, vec![]).unwrap();
    let receipt = handle
        .update_targets(vec![ManagedTargetConfig::new("queued", "127.0.0.1:9")])
        .unwrap();
    let stop = handle.stop();
    let outcome = runtime().block_on(task);
    runtime().block_on(stop);
    assert_eq!(outcome.applied_command_sequence, 0);
    assert!(matches!(
        runtime().block_on(receipt),
        Err(ManagedCommandApplyError::Stopping)
    ));
}

#[test]
fn accepted_update_before_driver_failure_resolves_driver_failed() {
    let mut managed = config(ManagedPacing::Staggered);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    let (task, handle) = ManagedClient::task(managed, vec![]).unwrap();
    let receipt = handle
        .update_targets(vec![ManagedTargetConfig::new("queued", "127.0.0.1:9")])
        .unwrap();
    let mut task = Box::pin(task);
    let Poll::Ready(outcome) = poll_once(task.as_mut()) else {
        panic!("first poll outside Tokio did not fail")
    };
    assert_eq!(
        outcome.end_reason,
        ManagedEndReason::DriverFailed(ManagedDriverFailure::NoTokioRuntime)
    );
    let mut receipt = Box::pin(receipt);
    assert!(matches!(
        poll_once(Pin::as_mut(&mut receipt)),
        Poll::Ready(Err(ManagedCommandApplyError::DriverFailed(
            ManagedDriverFailure::NoTokioRuntime
        )))
    ));
    assert!(matches!(
        handle.update_targets(vec![]),
        Err(ManagedCommandError::DriverClosed)
    ));
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
fn oversized_desired_set_is_rejected_before_enqueue() {
    let mut managed = config(ManagedPacing::Staggered);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    managed.max_live_target_generations = 1;
    let (task, handle) = ManagedClient::task(managed, vec![]).unwrap();
    let initial = handle.status();
    let capacity = handle.commands.capacity();
    assert!(matches!(
        handle.update_targets(vec![
            ManagedTargetConfig::new("one", "127.0.0.1:9"),
            ManagedTargetConfig::new("two", "127.0.0.1:10"),
        ]),
        Err(ManagedCommandError::TooManyTargets {
            configured: 2,
            limit: 1
        })
    ));
    assert_eq!(handle.commands.capacity(), capacity);
    assert_eq!(task.next_generation, 1);
    assert_eq!(task.applied_command_sequence, 0);
    assert_eq!(handle.status(), initial);
}

#[test]
fn legal_desired_set_can_still_fail_at_overlap_application() {
    let server = start_server(ServerBehavior::Echo, None);
    let mut managed = config(ManagedPacing::Staggered);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    managed.client.duration = None;
    managed.max_live_target_generations = 1;
    let (task, handle) = ManagedClient::task(managed, vec![target("same", server.addr)]).unwrap();
    let runtime = runtime();
    let mut task = Box::pin(task);
    drive_task_until(&runtime, &mut task, |task| task.active_count() == 1);

    let receipt = handle
        .update_targets(vec![ManagedTargetConfig::new("same", "127.0.0.1:10")])
        .unwrap();
    let mut receipt = Box::pin(receipt);
    let result = runtime.block_on(poll_fn(|cx| {
        assert!(task.as_mut().poll(cx).is_pending());
        match receipt.as_mut().poll(cx) {
            Poll::Ready(result) => Poll::Ready(result),
            Poll::Pending => Poll::Pending,
        }
    }));
    assert!(matches!(
        result,
        Err(ManagedCommandApplyError::LiveGenerationLimitExceeded {
            required: 2,
            limit: 1
        })
    ));
    let stop = handle.stop();
    runtime.block_on(task);
    runtime.block_on(stop);
    server.finish();
}

#[test]
fn dynamic_explicit_remove_and_readd_uses_new_generation() {
    let first = start_server(ServerBehavior::Echo, None);
    let second = start_server(ServerBehavior::Echo, None);
    let mut managed = config(ManagedPacing::Staggered);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    managed.client.duration = None;
    let (task, handle) = ManagedClient::task(managed, vec![target("same", first.addr)]).unwrap();
    let runtime = runtime();
    let mut task = Box::pin(task);
    drive_task_until(&runtime, &mut task, |task| task.active_count() == 1);
    let first_instance = task.targets[0].instance.clone();
    let removal = handle.update_targets(vec![]).unwrap();
    drive_task_until(&runtime, &mut task, |task| task.targets.is_empty());
    let acknowledgement = runtime.block_on(removal).unwrap();
    assert_eq!(acknowledgement.status.lifecycle, ManagedLifecycle::Running);
    assert!(acknowledgement
        .status
        .targets
        .iter()
        .any(|target| target.target == first_instance && !target.desired));
    assert!(matches!(
        task.history.recent.back().unwrap().end_reason,
        ManagedTargetEndReason::Removed
    ));
    let addition = handle
        .update_targets(vec![target("same", second.addr)])
        .unwrap();
    drive_task_until(&runtime, &mut task, |task| {
        task.targets[0].counters.packets_sent != 0
    });
    let acknowledgement = runtime.block_on(addition).unwrap();
    assert!(acknowledgement.status.targets[0].target.generation > first_instance.generation);
    let stop = handle.stop();
    runtime.block_on(task);
    runtime.block_on(stop);
    assert!(has_close(&first.finish()));
    second.finish();
}

#[test]
fn dynamic_queue_stop_and_dropped_receipt_are_linearized() {
    let mut managed = config(ManagedPacing::Staggered);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    managed.command_capacity = 1;
    let (task, handle) = ManagedClient::task(managed.clone(), vec![]).unwrap();
    let receipt = handle
        .update_targets(vec![ManagedTargetConfig::new("queued", "127.0.0.1:9")])
        .unwrap();
    assert!(matches!(
        handle.update_targets(vec![]),
        Err(ManagedCommandError::QueueFull)
    ));
    let stop = handle.stop();
    let outcome = runtime().block_on(task);
    runtime().block_on(stop);
    assert_eq!(outcome.applied_command_sequence, 0);
    assert!(matches!(
        runtime().block_on(receipt),
        Err(ManagedCommandApplyError::Stopping)
    ));

    let server = start_server(ServerBehavior::Echo, None);
    managed.command_capacity = 2;
    managed.client.duration = None;
    let (task, handle) = ManagedClient::task(managed, vec![]).unwrap();
    drop(
        handle
            .update_targets(vec![target("dropped", server.addr)])
            .unwrap(),
    );
    let runtime = runtime();
    let mut task = Box::pin(task);
    drive_task_until(&runtime, &mut task, |task| {
        task.targets[0].counters.packets_sent != 0
    });
    assert_eq!(handle.status().applied_command_sequence, 1);
    assert_eq!(handle.status().targets[0].target.generation, 1);
    let stop = handle.stop();
    runtime.block_on(task);
    runtime.block_on(stop);
    assert!(!probes(&server.finish()).is_empty());
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
fn timeout_discovery_inspects_at_most_one_budget_of_targets() {
    let target_count = TIMEOUT_WORK_BUDGET + 1;
    let mut managed = config(ManagedPacing::Burst);
    managed.max_live_target_generations = target_count;
    let targets = (0..target_count)
        .map(|index| ManagedTargetConfig::new(format!("target-{index}"), "127.0.0.1:9"))
        .collect();
    let (mut task, _) = ManagedClient::task(managed, targets).unwrap();

    task.timeout_inspections = 0;
    assert!(!task.poll_timeout_pass(Instant::now()));

    assert_eq!(task.timeout_inspections, TIMEOUT_WORK_BUDGET);
}

#[test]
fn sparse_timeout_backlog_persists_across_polls() {
    let server = start_server(ServerBehavior::Echo, None);
    let target_count = (2 * TIMEOUT_WORK_BUDGET) + 1;
    let mut managed = config(ManagedPacing::Burst);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    managed.client.duration = None;
    managed.max_live_target_generations = target_count;
    let targets = std::iter::once(target("sparse", server.addr))
        .chain(
            (1..target_count)
                .map(|index| ManagedTargetConfig::new(format!("target-{index}"), "127.0.0.1:9")),
        )
        .collect();
    let (mut task, _) = ManagedClient::task(managed, targets).unwrap();
    for target in &mut task.targets[1..] {
        target.state = TargetState::Terminal;
    }
    let observations = Arc::new(Mutex::new(Vec::new()));
    task.event_observations = Some(Arc::clone(&observations));
    let runtime = runtime();
    open_target_for_timeout_test(&runtime, &mut task, 0);
    let now = Instant::now();
    for seq in 0..u32::try_from((2 * TIMEOUT_WORK_BUDGET) + 64).unwrap() {
        replace_pending_for_timeout_test(
            &mut task,
            0,
            timeout_probe(
                seq,
                now - Duration::from_secs(2),
                now - Duration::from_secs(1),
            ),
        );
    }

    assert!(task.poll_timeout_pass(now));
    assert_eq!(
        timeout_losses_for(&observations, "sparse"),
        TIMEOUT_WORK_BUDGET
    );
    assert_eq!(task.timeout_backlog.len(), 1);

    assert!(task.poll_timeout_pass(now));
    assert_eq!(
        timeout_losses_for(&observations, "sparse"),
        2 * TIMEOUT_WORK_BUDGET
    );
    assert_eq!(task.timeout_backlog.len(), 1);
    close_timeout_test_target(&runtime, &mut task, 0);
    drop(task);
    server.finish();
}

#[test]
fn timeout_backlog_does_not_starve_discovery() {
    let first = start_server(ServerBehavior::Echo, None);
    let second = start_server(ServerBehavior::Echo, None);
    let target_count = (2 * TIMEOUT_WORK_BUDGET) + 1;
    let late_index = target_count - 1;
    let mut managed = config(ManagedPacing::Burst);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    managed.client.duration = None;
    managed.max_live_target_generations = target_count;
    let targets = (0..target_count)
        .map(|index| match index {
            0 => target("backlog", first.addr),
            index if index == late_index => target("late", second.addr),
            _ => ManagedTargetConfig::new(format!("target-{index}"), "127.0.0.1:9"),
        })
        .collect();
    let (mut task, _) = ManagedClient::task(managed, targets).unwrap();
    for target in &mut task.targets[1..late_index] {
        target.state = TargetState::Terminal;
    }
    let observations = Arc::new(Mutex::new(Vec::new()));
    task.event_observations = Some(Arc::clone(&observations));
    let runtime = runtime();
    open_target_for_timeout_test(&runtime, &mut task, 0);
    open_target_for_timeout_test(&runtime, &mut task, late_index);
    let now = Instant::now();
    for seq in 0..u32::try_from(3 * TIMEOUT_WORK_BUDGET).unwrap() {
        replace_pending_for_timeout_test(
            &mut task,
            0,
            timeout_probe(
                seq,
                now - Duration::from_secs(2),
                now - Duration::from_secs(1),
            ),
        );
    }
    replace_pending_for_timeout_test(
        &mut task,
        late_index,
        timeout_probe(
            0,
            now - Duration::from_secs(2),
            now - Duration::from_secs(1),
        ),
    );
    task.timeout_backlog.push_back(TimeoutBacklogEntry {
        index: 0,
        generation: task.targets[0].instance.generation,
    });
    task.timeout_cursor = 1;

    assert!(task.poll_timeout_pass(now));
    assert_eq!(timeout_losses_for(&observations, "late"), 0);

    assert!(task.poll_timeout_pass(now));
    assert_eq!(timeout_losses_for(&observations, "late"), 1);
    assert_eq!(task.timeout_backlog.len(), 1);
    close_timeout_test_target(&runtime, &mut task, 0);
    close_timeout_test_target(&runtime, &mut task, late_index);
    drop(task);
    first.finish();
    second.finish();
}

#[test]
fn stale_timeout_backlog_entry_is_dropped_after_pruning() {
    let server = start_server(ServerBehavior::Echo, None);
    let target_count = TIMEOUT_WORK_BUDGET + 2;
    let mut managed = config(ManagedPacing::Burst);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    managed.client.duration = None;
    managed.max_live_target_generations = target_count;
    let targets = vec![
        ManagedTargetConfig::new("retired", "127.0.0.1:9"),
        target("survivor", server.addr),
    ]
    .into_iter()
    .chain(
        (2..target_count)
            .map(|index| ManagedTargetConfig::new(format!("target-{index}"), "127.0.0.1:9")),
    )
    .collect();
    let (mut task, _) = ManagedClient::task(managed, targets).unwrap();
    let observations = Arc::new(Mutex::new(Vec::new()));
    task.event_observations = Some(Arc::clone(&observations));
    let runtime = runtime();
    open_target_for_timeout_test(&runtime, &mut task, 1);
    let now = Instant::now();
    replace_pending_for_timeout_test(
        &mut task,
        1,
        timeout_probe(
            0,
            now - Duration::from_secs(2),
            now - Duration::from_secs(1),
        ),
    );
    task.timeout_backlog.push_back(TimeoutBacklogEntry {
        index: 0,
        generation: task.targets[0].instance.generation,
    });
    task.targets[0].desired = false;
    task.targets[0].state = TargetState::Terminal;
    task.prune_undesired_terminal();
    task.timeout_cursor = 1;

    assert!(!task.poll_timeout_pass(now));
    assert!(task.timeout_backlog.is_empty());
    assert_eq!(timeout_losses_for(&observations, "survivor"), 0);

    task.timeout_cursor = 0;
    assert!(!task.poll_timeout_pass(now));
    assert_eq!(timeout_losses_for(&observations, "survivor"), 1);
    close_timeout_test_target(&runtime, &mut task, 0);
    drop(task);
    server.finish();
}

#[test]
fn immediate_timeout_backlog_skips_global_deadline_scan() {
    let server = start_server(ServerBehavior::Echo, None);
    let mut managed = config(ManagedPacing::Burst);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    managed.client.duration = None;
    let (mut task, _) = ManagedClient::task(managed, vec![target("one", server.addr)]).unwrap();
    let runtime = runtime();
    open_target_for_timeout_test(&runtime, &mut task, 0);
    let now = Instant::now();
    for seq in 0..u32::try_from(TIMEOUT_WORK_BUDGET + 1).unwrap() {
        replace_pending_for_timeout_test(
            &mut task,
            0,
            timeout_probe(
                seq,
                now - Duration::from_secs(2),
                now - Duration::from_secs(1),
            ),
        );
    }
    task.timeout_backlog.push_back(TimeoutBacklogEntry {
        index: 0,
        generation: task.targets[0].instance.generation,
    });
    task.deadline_inspections = 0;

    let mut task = Box::pin(task);
    poll_task_once(&runtime, &mut task);
    assert_eq!(task.deadline_inspections, 0);
    assert_eq!(task.timeout_backlog.len(), 1);

    assert!(!task.poll_timeout_pass(now));
    assert!(task.timeout_backlog.is_empty());
    task.deadline_inspections = 0;
    poll_task_once(&runtime, &mut task);
    assert_eq!(task.deadline_inspections, 1);
    close_timeout_test_target(&runtime, &mut task, 0);
    drop(task);
    server.finish();
}

#[test]
fn timeout_discovery_reaches_later_target_after_budget_slice() {
    let server = start_server(ServerBehavior::Echo, None);
    let target_count = TIMEOUT_WORK_BUDGET + 1;
    let late_index = target_count - 1;
    let mut managed = config(ManagedPacing::Burst);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    managed.client.duration = None;
    managed.max_live_target_generations = target_count;
    let targets = (0..target_count)
        .map(|index| {
            if index == late_index {
                target("late", server.addr)
            } else {
                ManagedTargetConfig::new(format!("target-{index}"), "127.0.0.1:9")
            }
        })
        .collect();
    let (mut task, _) = ManagedClient::task(managed, targets).unwrap();
    for target in &mut task.targets[..late_index] {
        target.state = TargetState::Terminal;
    }
    let observations = Arc::new(Mutex::new(Vec::new()));
    task.event_observations = Some(Arc::clone(&observations));
    let runtime = runtime();
    open_target_for_timeout_test(&runtime, &mut task, late_index);
    let now = Instant::now();
    replace_pending_for_timeout_test(
        &mut task,
        late_index,
        timeout_probe(0, now - Duration::from_secs(1), now),
    );

    task.timeout_inspections = 0;
    assert!(!task.poll_timeout_pass(now));
    assert_eq!(task.timeout_inspections, TIMEOUT_WORK_BUDGET);
    assert_eq!(timeout_losses_for(&observations, "late"), 0);

    task.timeout_inspections = 0;
    assert!(!task.poll_timeout_pass(now));
    assert_eq!(task.timeout_inspections, TIMEOUT_WORK_BUDGET);
    assert_eq!(timeout_losses_for(&observations, "late"), 1);

    let TargetState::Active { mut client } =
        mem::replace(&mut task.targets[late_index].state, TargetState::Terminal)
    else {
        panic!("later timeout target did not remain active");
    };
    runtime.block_on(client.close()).unwrap();
    drop(task);
    server.finish();
}

#[test]
fn timeout_work_is_bounded_per_task_poll_and_eventually_drains() {
    let server = start_server(ServerBehavior::Echo, None);
    let mut managed = config(ManagedPacing::Staggered);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    managed.client.duration = None;
    let (mut task, handle) =
        ManagedClient::task(managed, vec![target("one", server.addr)]).unwrap();
    let observations = Arc::new(Mutex::new(Vec::new()));
    task.event_observations = Some(Arc::clone(&observations));
    let runtime = runtime();
    open_target_for_timeout_test(&runtime, &mut task, 0);
    let now = Instant::now();
    for seq in 0..u32::try_from(TIMEOUT_WORK_BUDGET + 1).unwrap() {
        replace_pending_for_timeout_test(
            &mut task,
            0,
            timeout_probe(
                seq,
                now - Duration::from_secs(2),
                now - Duration::from_secs(1),
            ),
        );
    }

    let mut task = Box::pin(task);
    poll_task_once(&runtime, &mut task);
    assert_eq!(
        timeout_losses_for(&observations, "one"),
        TIMEOUT_WORK_BUDGET
    );
    assert!(matches!(
        &task.targets[0].state,
        TargetState::Active { client }
            if client.next_probe_timeout_deadline().is_some()
    ));

    drive_task_until(&runtime, &mut task, |task| {
        matches!(
            &task.targets[0].state,
            TargetState::Active { client }
                if client.next_probe_timeout_deadline().is_none()
        )
    });
    assert_eq!(
        timeout_losses_for(&observations, "one"),
        TIMEOUT_WORK_BUDGET + 1
    );

    drop(handle.stop());
    runtime.block_on(task);
    server.finish();
}

#[test]
fn timeout_work_round_robins_simultaneous_target_backlogs() {
    let first = start_server(ServerBehavior::Echo, None);
    let second = start_server(ServerBehavior::Echo, None);
    let mut managed = config(ManagedPacing::Staggered);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    managed.client.duration = None;
    let (mut task, handle) = ManagedClient::task(
        managed,
        vec![target("first", first.addr), target("second", second.addr)],
    )
    .unwrap();
    let observations = Arc::new(Mutex::new(Vec::new()));
    task.event_observations = Some(Arc::clone(&observations));
    let runtime = runtime();
    open_target_for_timeout_test(&runtime, &mut task, 0);
    open_target_for_timeout_test(&runtime, &mut task, 1);
    let now = Instant::now();
    let per_target = TIMEOUT_WORK_BUDGET + 16;
    for index in 0..2 {
        for seq in 0..u32::try_from(per_target).unwrap() {
            replace_pending_for_timeout_test(
                &mut task,
                index,
                timeout_probe(
                    seq,
                    now - Duration::from_secs(2),
                    now - Duration::from_secs(1),
                ),
            );
        }
    }

    let mut task = Box::pin(task);
    poll_task_once(&runtime, &mut task);
    assert_eq!(
        timeout_losses_for(&observations, "first"),
        TIMEOUT_WORK_BUDGET / 2
    );
    assert_eq!(
        timeout_losses_for(&observations, "second"),
        TIMEOUT_WORK_BUDGET / 2
    );
    assert!(matches!(
        &task.targets[0].state,
        TargetState::Active { client }
            if client.next_probe_timeout_deadline().is_some()
    ));
    assert!(matches!(
        &task.targets[1].state,
        TargetState::Active { client }
            if client.next_probe_timeout_deadline().is_some()
    ));

    drive_task_until(&runtime, &mut task, |task| {
        task.targets.iter().all(|target| {
            matches!(
                &target.state,
                TargetState::Active { client }
                    if client.next_probe_timeout_deadline().is_none()
            )
        })
    });
    assert_eq!(timeout_losses_for(&observations, "first"), per_target);
    assert_eq!(timeout_losses_for(&observations, "second"), per_target);

    drop(handle.stop());
    runtime.block_on(task);
    first.finish();
    second.finish();
}

#[test]
fn budget_deferred_timeout_blocks_send_until_timeout_processing() {
    let reply_gate = Arc::new(PacketGate::default());
    let server = start_server_with_gates(
        ServerBehavior::Echo,
        None,
        None,
        None,
        Some(Arc::clone(&reply_gate)),
    );
    let target_count = TIMEOUT_WORK_BUDGET + 1;
    let late_index = target_count - 1;
    let mut managed = config(ManagedPacing::Burst);
    managed.completion = ManagedCompletionPolicy::ExplicitStop;
    managed.client.duration = None;
    managed.client.interval = Duration::from_nanos(1);
    managed.client.probe_timeout = Duration::from_secs(1);
    managed.client.max_pending_probes = 1;
    managed.max_live_target_generations = target_count;
    let targets = (0..target_count)
        .map(|index| {
            if index == late_index {
                target("late", server.addr)
            } else {
                ManagedTargetConfig::new(format!("target-{index}"), "127.0.0.1:9")
            }
        })
        .collect();
    let (mut task, _) = ManagedClient::task(managed, targets).unwrap();
    for target in &mut task.targets[..late_index] {
        target.state = TargetState::Terminal;
    }
    let observations = Arc::new(Mutex::new(Vec::new()));
    task.event_observations = Some(Arc::clone(&observations));
    let runtime = runtime();
    open_target_for_timeout_test(&runtime, &mut task, late_index);
    task.targets[late_index].desired = true;
    let sent = match &mut task.targets[late_index].state {
        TargetState::Active { client } => runtime.block_on(client.send_probe()).unwrap(),
        _ => panic!("deferred-send target did not open"),
    };
    assert!(matches!(
        sent.as_slice(),
        [ClientEvent::EchoSent { seq: 0, .. }]
    ));
    wait_flag(&server.probe_seen);

    let overdue = Instant::now();
    replace_pending_for_timeout_test(
        &mut task,
        late_index,
        timeout_probe(0, overdue - Duration::from_secs(1), overdue),
    );
    task.state = DriverState::Running;
    task.lifecycle = ManagedLifecycle::Running;
    task.timeout_cursor = 0;
    task.send_cursor = late_index;

    let mut task = Box::pin(task);
    poll_task_once(&runtime, &mut task);
    assert_eq!(task.timeout_inspections, TIMEOUT_WORK_BUDGET);
    assert_eq!(timeout_losses_for(&observations, "late"), 0);
    assert!(matches!(
        &task.targets[late_index].state,
        TargetState::Active { client }
            if client.packets_sent() == 1
                && client.next_probe_timeout_deadline() == Some(overdue)
    ));
    assert!(!task.targets[late_index].send_waiting);

    poll_task_once(&runtime, &mut task);
    assert_eq!(timeout_losses_for(&observations, "late"), 1);
    assert!(matches!(
        &task.targets[late_index].state,
        TargetState::Active { client }
            if client.next_probe_timeout_deadline().is_none()
    ));

    poll_task_once(&runtime, &mut task);
    assert!(matches!(
        &task.targets[late_index].state,
        TargetState::Active { client } if client.packets_sent() == 2
    ));

    reply_gate.release();
    let TargetState::Active { mut client } =
        mem::replace(&mut task.targets[late_index].state, TargetState::Terminal)
    else {
        panic!("deferred-send target did not remain active");
    };
    runtime.block_on(client.close()).unwrap();
    drop(task);
    server.finish();
}

#[test]
fn partial_timeout_batch_blocks_active_receive_until_loss_commits() {
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
    let (mut task, handle) =
        ManagedClient::task(managed, vec![target("one", server.addr)]).unwrap();
    let observations = Arc::new(Mutex::new(Vec::new()));
    task.event_observations = Some(Arc::clone(&observations));
    let runtime = runtime();
    open_target_for_timeout_test(&runtime, &mut task, 0);
    let sent = match &mut task.targets[0].state {
        TargetState::Active { client } => runtime.block_on(client.send_probe()).unwrap(),
        _ => panic!("timeout test target did not open"),
    };
    assert!(matches!(
        sent.as_slice(),
        [ClientEvent::EchoSent { seq: 0, .. }]
    ));
    wait_flag(&server.probe_seen);

    let now = Instant::now();
    let _ = remove_pending_for_timeout_test(&mut task, 0, 0);
    for seq in 1..=u32::try_from(TIMEOUT_WORK_BUDGET).unwrap() {
        replace_pending_for_timeout_test(
            &mut task,
            0,
            timeout_probe(
                seq,
                now - Duration::from_secs(2),
                now - Duration::from_secs(1),
            ),
        );
    }
    replace_pending_for_timeout_test(
        &mut task,
        0,
        timeout_probe(0, now - Duration::from_secs(1), now),
    );
    reply_gate.release();
    wait_flag(&server.reply_sent);

    let mut task = Box::pin(task);
    poll_task_once(&runtime, &mut task);
    assert_eq!(
        timeout_losses_for(&observations, "one"),
        TIMEOUT_WORK_BUDGET
    );
    assert!(!client_events(&observations).iter().any(|event| {
        matches!(
            event,
            ClientEvent::EchoLoss { seq: 0, .. }
                | ClientEvent::LateReply { seq: 0, .. }
                | ClientEvent::EchoReply { seq: 0, .. }
        )
    }));

    drive_task_until(&runtime, &mut task, |_| {
        client_events(&observations)
            .iter()
            .any(|event| matches!(event, ClientEvent::LateReply { seq: 0, .. }))
    });
    let classified = client_events(&observations)
        .into_iter()
        .filter(|event| {
            matches!(
                event,
                ClientEvent::EchoLoss { seq: 0, .. }
                    | ClientEvent::LateReply { seq: 0, .. }
                    | ClientEvent::EchoReply { seq: 0, .. }
            )
        })
        .collect::<Vec<_>>();
    assert!(matches!(
        classified.as_slice(),
        [
            ClientEvent::EchoLoss { seq: 0, .. },
            ClientEvent::LateReply { seq: 0, .. }
        ]
    ));

    drop(handle.stop());
    runtime.block_on(task);
    server.finish();
}

#[test]
fn partial_timeout_batch_blocks_draining_receive_until_loss_commits() {
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
    let (mut task, handle) =
        ManagedClient::task(managed, vec![target("one", server.addr)]).unwrap();
    let observations = Arc::new(Mutex::new(Vec::new()));
    task.event_observations = Some(Arc::clone(&observations));
    let runtime = runtime();
    open_target_for_timeout_test(&runtime, &mut task, 0);
    let sent = match &mut task.targets[0].state {
        TargetState::Active { client } => runtime.block_on(client.send_probe()).unwrap(),
        _ => panic!("timeout test target did not open"),
    };
    assert!(matches!(
        sent.as_slice(),
        [ClientEvent::EchoSent { seq: 0, .. }]
    ));
    wait_flag(&server.probe_seen);

    let now = Instant::now();
    let _ = remove_pending_for_timeout_test(&mut task, 0, 0);
    for seq in 1..=u32::try_from(TIMEOUT_WORK_BUDGET).unwrap() {
        replace_pending_for_timeout_test(
            &mut task,
            0,
            timeout_probe(
                seq,
                now - Duration::from_secs(2),
                now - Duration::from_secs(1),
            ),
        );
    }
    replace_pending_for_timeout_test(
        &mut task,
        0,
        timeout_probe(0, now - Duration::from_secs(1), now),
    );
    let TargetState::Active { client } =
        mem::replace(&mut task.targets[0].state, TargetState::Terminal)
    else {
        panic!("timeout test target did not remain active");
    };
    assert!(task.begin_drain(0, client, ManagedTargetEndReason::Stopped, now));
    reply_gate.release();
    wait_flag(&server.reply_sent);

    let mut task = Box::pin(task);
    poll_task_once(&runtime, &mut task);
    assert_eq!(
        timeout_losses_for(&observations, "one"),
        TIMEOUT_WORK_BUDGET
    );
    assert!(matches!(
        &task.targets[0].state,
        TargetState::Draining {
            primary_end: ManagedTargetEndReason::Stopped,
            cleanup_failure: None,
            ..
        }
    ));
    assert!(!client_events(&observations).iter().any(|event| {
        matches!(
            event,
            ClientEvent::EchoLoss { seq: 0, .. }
                | ClientEvent::LateReply { seq: 0, .. }
                | ClientEvent::EchoReply { seq: 0, .. }
        )
    }));

    drive_task_until(&runtime, &mut task, |_| {
        client_events(&observations)
            .iter()
            .any(|event| matches!(event, ClientEvent::LateReply { seq: 0, .. }))
    });
    let classified = client_events(&observations)
        .into_iter()
        .filter(|event| {
            matches!(
                event,
                ClientEvent::EchoLoss { seq: 0, .. }
                    | ClientEvent::LateReply { seq: 0, .. }
                    | ClientEvent::EchoReply { seq: 0, .. }
            )
        })
        .collect::<Vec<_>>();
    assert!(matches!(
        classified.as_slice(),
        [
            ClientEvent::EchoLoss { seq: 0, .. },
            ClientEvent::LateReply { seq: 0, .. }
        ]
    ));

    drop(handle.stop());
    let outcome = runtime.block_on(task);
    let target = &outcome.recent_target_outcomes[0];
    assert!(matches!(target.end_reason, ManagedTargetEndReason::Stopped));
    assert!(target.cleanup_failure.is_none());
    server.finish();
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
