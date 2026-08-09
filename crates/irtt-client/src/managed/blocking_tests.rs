use std::{
    net::{SocketAddr, UdpSocket},
    sync::{mpsc::sync_channel, Arc, Condvar, Mutex},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use irtt_proto::{
    decode_close_request, decode_echo_request, decode_open_request, encode_echo_reply,
    encode_open_reply, flags, EchoReply, OpenReply, ReceivedStats, StampAt, TimestampFields,
};
use tokio::{runtime::Builder, time::timeout};

use super::{
    blocking::{join_worker, WorkerRuntime},
    BlockingManagedClient, BlockingManagedJoinError, ManagedClientConfig, ManagedCompletionPolicy,
    ManagedEndReason, ManagedEvent, ManagedLifecycle, ManagedPacing, ManagedTargetConfig,
    ManagedTargetEndReason,
};
use crate::ClientConfig;

const TOKEN: u64 = 0x1234_5678_90ab_cdef;

struct Signal {
    value: Mutex<bool>,
    ready: Condvar,
}

impl Signal {
    fn set(&self) {
        *self.value.lock().unwrap() = true;
        self.ready.notify_all();
    }

    fn wait(&self) {
        let value = self.value.lock().unwrap();
        let (value, timeout) = self
            .ready
            .wait_timeout_while(value, Duration::from_secs(2), |value| !*value)
            .unwrap();
        assert!(*value && !timeout.timed_out());
    }
}

struct EchoServer {
    addr: SocketAddr,
    probe: Arc<Signal>,
    close: Arc<Signal>,
    worker: JoinHandle<()>,
}

impl EchoServer {
    fn start() -> Self {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let addr = socket.local_addr().unwrap();
        let probe = Arc::new(Signal {
            value: Mutex::new(false),
            ready: Condvar::new(),
        });
        let close = Arc::new(Signal {
            value: Mutex::new(false),
            ready: Condvar::new(),
        });
        let worker_probe = Arc::clone(&probe);
        let worker_close = Arc::clone(&close);
        let worker = thread::spawn(move || {
            let mut buffer = [0_u8; 8192];
            let (open_len, peer) = socket.recv_from(&mut buffer).unwrap();
            let request = decode_open_request(&buffer[..open_len], None).unwrap();
            let negotiated = request.params;
            let reply = encode_open_reply(
                &OpenReply {
                    flags: flags::FLAG_OPEN | flags::FLAG_REPLY,
                    token: TOKEN,
                    params: negotiated.clone(),
                },
                None,
            )
            .unwrap();
            socket.send_to(&reply, peer).unwrap();

            loop {
                let (len, packet_peer) = socket.recv_from(&mut buffer).unwrap();
                let packet = &buffer[..len];
                if let Ok(probe) = decode_echo_request(packet, &negotiated, None) {
                    worker_probe.set();
                    let reply = encode_echo_reply(
                        &EchoReply {
                            flags: flags::FLAG_REPLY,
                            token: TOKEN,
                            sequence: probe.sequence,
                            recv_count: None,
                            recv_window: None,
                            timestamps: TimestampFields::default(),
                            payload: Vec::new(),
                        },
                        &negotiated,
                        None,
                    )
                    .unwrap();
                    socket.send_to(&reply, packet_peer).unwrap();
                } else if decode_close_request(packet, None).is_ok() {
                    worker_close.set();
                    return;
                }
            }
        });
        Self {
            addr,
            probe,
            close,
            worker,
        }
    }

    fn wait_probe(&self) {
        self.probe.wait();
    }

    fn wait_close(&self) {
        self.close.wait();
    }

    fn finish(self) {
        self.worker.join().unwrap();
    }
}

fn test_runtime() -> tokio::runtime::Runtime {
    Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .unwrap()
}

fn config(completion: ManagedCompletionPolicy) -> ManagedClientConfig {
    ManagedClientConfig {
        client: ClientConfig {
            duration: Some(Duration::from_millis(120)),
            interval: Duration::from_millis(20),
            probe_timeout: Duration::from_millis(40),
            open_timeouts: vec![Duration::from_millis(200)],
            received_stats: ReceivedStats::None,
            stamp_at: StampAt::None,
            ..ClientConfig::default()
        },
        pacing: ManagedPacing::Burst,
        completion,
        final_drain: Duration::from_millis(5),
        ..ManagedClientConfig::default()
    }
}

fn target(server: &EchoServer) -> ManagedTargetConfig {
    ManagedTargetConfig::new("target", server.addr.to_string())
}

fn assert_stopped(outcome: &super::ManagedOutcome) {
    assert_eq!(outcome.end_reason, ManagedEndReason::StopRequested);
}

#[test]
fn starts_without_caller_runtime_and_finishes_naturally() {
    let server = EchoServer::start();
    let owner = BlockingManagedClient::start(
        config(ManagedCompletionPolicy::FinishWhenQuiescent),
        vec![target(&server)],
    )
    .unwrap();

    server.wait_probe();
    let outcome = owner.join().unwrap();
    assert_eq!(outcome.end_reason, ManagedEndReason::TargetsComplete);
    assert_eq!(outcome.recent_target_outcomes.len(), 1);
    assert_eq!(
        outcome.recent_target_outcomes[0].end_reason,
        ManagedTargetEndReason::TestComplete
    );
    server.wait_close();
    server.finish();
}

#[test]
fn subscribed_start_captures_events_from_a_short_run() {
    let server = EchoServer::start();
    let (owner, mut events) = BlockingManagedClient::start_with_subscription(
        config(ManagedCompletionPolicy::FinishWhenQuiescent),
        vec![target(&server)],
    )
    .unwrap();

    server.wait_probe();
    let outcome = owner.join().unwrap();
    assert_eq!(outcome.end_reason, ManagedEndReason::TargetsComplete);

    let mut saw_started = false;
    let mut saw_finished = false;
    let mut saw_completed = false;
    while let Ok(event) = events.try_recv() {
        match event {
            ManagedEvent::Started => saw_started = true,
            ManagedEvent::TargetFinished { .. } => saw_finished = true,
            ManagedEvent::Completed { .. } => saw_completed = true,
            _ => {}
        }
    }
    assert!(saw_started && saw_finished && saw_completed);
    server.wait_close();
    server.finish();
}

#[test]
fn external_handle_stop_ends_with_graceful_close() {
    let server = EchoServer::start();
    let owner = BlockingManagedClient::start(
        config(ManagedCompletionPolicy::ExplicitStop),
        vec![target(&server)],
    )
    .unwrap();
    let handle = owner.handle();

    server.wait_probe();
    test_runtime().block_on(async {
        timeout(Duration::from_secs(2), handle.stop())
            .await
            .unwrap();
    });
    let outcome = owner.join().unwrap();
    assert_stopped(&outcome);
    server.wait_close();
    server.finish();
}

#[test]
fn dynamic_update_reaches_background_worker() {
    let server = EchoServer::start();
    let owner = BlockingManagedClient::start(config(ManagedCompletionPolicy::ExplicitStop), vec![])
        .unwrap();
    let handle = owner.handle();

    let acknowledgement = test_runtime().block_on(async {
        timeout(
            Duration::from_secs(2),
            handle.update_targets(vec![target(&server)]).unwrap(),
        )
        .await
        .unwrap()
        .unwrap()
    });
    assert_eq!(acknowledgement.sequence, 1);
    server.wait_probe();
    test_runtime().block_on(async {
        timeout(Duration::from_secs(2), owner.handle().stop())
            .await
            .unwrap();
    });
    assert_stopped(&owner.join().unwrap());
    server.wait_close();
    server.finish();
}

#[test]
fn dropping_external_handles_does_not_stop_owner() {
    let server = EchoServer::start();
    let owner = BlockingManagedClient::start(
        config(ManagedCompletionPolicy::FinishWhenQuiescent),
        vec![target(&server)],
    )
    .unwrap();
    drop(owner.handle());
    drop(owner.handle());

    server.wait_probe();
    assert_eq!(
        owner.join().unwrap().end_reason,
        ManagedEndReason::TargetsComplete
    );
    server.wait_close();
    server.finish();
}

#[test]
fn dropping_owner_stops_and_joins_worker() {
    let server = EchoServer::start();
    let owner = BlockingManagedClient::start(
        config(ManagedCompletionPolicy::ExplicitStop),
        vec![target(&server)],
    )
    .unwrap();
    let handle = owner.handle();

    server.wait_probe();
    drop(owner);
    let status = handle.status();
    assert_eq!(status.lifecycle, ManagedLifecycle::Completed);
    assert_eq!(
        status.final_outcome.as_ref().unwrap().end_reason,
        ManagedEndReason::StopRequested
    );
    server.wait_close();
    server.finish();
}

#[test]
fn worker_panic_is_a_join_error() {
    let worker = thread::spawn(|| -> super::ManagedOutcome { panic!("test worker panic") });
    assert_eq!(
        join_worker(worker),
        Err(BlockingManagedJoinError::WorkerPanicked)
    );
}

#[test]
fn dropping_worker_runtime_inside_async_context_does_not_panic() {
    test_runtime().block_on(async {
        let runtime = Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .unwrap();
        drop(WorkerRuntime::new(runtime));
    });
}

#[test]
fn worker_runtime_shutdown_is_bounded_with_blocking_work() {
    let runtime = Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .unwrap();
    let (started_send, started_receive) = sync_channel(0);
    let (release_send, release_receive) = sync_channel(0);

    runtime.block_on(async move {
        tokio::task::spawn_blocking(move || {
            started_send.send(()).unwrap();
            release_receive.recv().unwrap();
        });
    });
    started_receive
        .recv_timeout(Duration::from_secs(2))
        .unwrap();

    let started = Instant::now();
    WorkerRuntime::new(runtime).shutdown();
    assert!(started.elapsed() < Duration::from_secs(2));
    release_send.send(()).unwrap();
}

#[test]
fn drop_after_natural_completion_preserves_outcome() {
    let server = EchoServer::start();
    let owner = BlockingManagedClient::start(
        config(ManagedCompletionPolicy::FinishWhenQuiescent),
        vec![target(&server)],
    )
    .unwrap();
    let handle = owner.handle();
    server.wait_probe();
    server.wait_close();
    let deadline = Instant::now() + Duration::from_secs(2);
    let status = loop {
        let status = handle.status();
        if status.lifecycle == ManagedLifecycle::Completed && status.final_outcome.is_some() {
            break status;
        }
        assert!(Instant::now() < deadline, "managed worker did not complete");
        thread::sleep(Duration::from_millis(5));
    };
    assert_eq!(
        status.final_outcome.as_ref().unwrap().end_reason,
        ManagedEndReason::TargetsComplete
    );
    drop(owner);
    assert_eq!(
        handle.status().final_outcome.as_ref().unwrap().end_reason,
        ManagedEndReason::TargetsComplete
    );
    server.finish();
}
