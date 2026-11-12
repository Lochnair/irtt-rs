use std::time::{Duration, Instant};

use irtt_client::{
    managed::*, ClientConfig, ClientEvent, NegotiationPolicy, OpenPolicy, SessionRequest,
};
use irtt_server::{Server, ServerConfig};
use tokio::{sync::oneshot, time::timeout};

async fn server(
    config: ServerConfig,
) -> (String, oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let mut server = Server::bind("127.0.0.1:0".parse().unwrap(), config)
        .await
        .unwrap();
    let address = server.local_addr().unwrap().to_string();
    let (stop, stopped) = oneshot::channel();
    let run = tokio::spawn(async move {
        server
            .run(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    (address, stop, run)
}

fn config(epoch: Instant) -> ManagedClientConfig {
    ManagedClientConfig {
        client: ClientConfig {
            request: SessionRequest {
                duration: None,
                interval: Duration::from_millis(20),
                ..Default::default()
            },
            open: OpenPolicy {
                negotiation: NegotiationPolicy::Loose,
                timeouts: vec![Duration::from_secs(1)],
            },
            ..Default::default()
        },
        pacing: ManagedPacing::Explicit { epoch },
        completion: ManagedCompletionPolicy::ExplicitStop,
        ..Default::default()
    }
}

fn target(id: &str, address: &str, offset: u64) -> ManagedTargetConfig {
    let mut target = ManagedTargetConfig::new(id, address);
    target.send_offset = Some(Duration::from_millis(offset));
    target
}

// A live offset change must not masquerade as replacement. A server-limited
// session then exercises automatic recovery on the same externally assigned grid.
#[tokio::test(flavor = "current_thread")]
async fn explicit_rephasing_preserves_sessions_and_recovery_rejoins_the_grid() {
    timeout(Duration::from_secs(8), async {
        let interval = Duration::from_millis(80);
        let server_config = ServerConfig::default().with_min_send_interval(interval);
        let (address, stop, service) = server(
            server_config
                .clone()
                .with_max_test_duration(Duration::from_millis(800)),
        )
        .await;
        let (healthy_address, healthy_stop, healthy_service) = server(server_config).await;
        let epoch = Instant::now() - Duration::from_millis(37);
        let mut config = config(epoch);
        config.reconnect_timeout = Some(Duration::from_secs(3));
        config.reconnect_initial_delay = Duration::from_millis(10);
        config.reconnect_max_delay = Duration::from_millis(20);
        let mut targets = vec![
            target("slot", &address, 0),
            target("healthy", &healthy_address, 40),
        ];
        let (task, handle) = ManagedClient::task(config, targets.clone()).unwrap();
        let original: Vec<_> = handle
            .status()
            .targets
            .iter()
            .map(|status| status.target.clone())
            .collect();
        let mut events = handle.subscribe().unwrap();
        let run = tokio::spawn(task);
        let mut starts = [0; 2];
        let mut tokens = [None; 2];
        let mut last_seq = [None; 2];
        let mut replies = [false; 2];
        let mut last_slot_send = None;
        let mut updated = false;
        let mut rephased = false;
        let mut recovered = false;
        while !recovered {
            if let ManagedEvent::Client { target, event } = events.recv().await.unwrap() {
                let index = usize::from(target.id.as_str() == "healthy");
                assert_eq!(target, original[index]);
                match event {
                    ClientEvent::SessionStarted(started) => {
                        starts[index] += 1;
                        assert_eq!(started.negotiation.accepted.interval, interval);
                        if index == 0 && starts[0] > 1 {
                            assert!(rephased, "offset change must not start a session");
                            assert_ne!(tokens[0], Some(started.token));
                        }
                        tokens[index] = Some(started.token);
                        last_seq[index] = None;
                    }
                    ClientEvent::EchoSent {
                        seq,
                        scheduled_at,
                        sent_at,
                        timer_error,
                        ..
                    } => {
                        assert_eq!(seq, last_seq[index].map_or(0, |previous| previous + 1));
                        last_seq[index] = Some(seq);
                        let scheduled = scheduled_at.unwrap();
                        assert!(sent_at.mono >= scheduled);
                        assert_eq!(timer_error, Some(sent_at.mono.duration_since(scheduled)));
                        let phase =
                            scheduled.duration_since(epoch).as_nanos() % interval.as_nanos();
                        if index == 1 {
                            assert_eq!(starts[1], 1);
                            assert_eq!(phase, Duration::from_millis(40).as_nanos());
                        } else {
                            if phase == Duration::from_millis(25).as_nanos() {
                                assert!(updated);
                                if !rephased {
                                    assert_eq!(starts[0], 1);
                                    assert!(
                                        scheduled.duration_since(last_slot_send.unwrap())
                                            >= interval
                                    );
                                    rephased = true;
                                }
                                recovered = starts[0] >= 2;
                            } else {
                                assert!(!rephased);
                                assert_eq!(phase, 0);
                            }
                            last_slot_send = Some(sent_at.mono);
                        }
                    }
                    ClientEvent::EchoReply { .. } => replies[index] = true,
                    _ => {}
                }
            }
            if replies == [true; 2] && !updated {
                targets[0].send_offset = Some(Duration::from_millis(25));
                let applied = handle
                    .update_targets(targets.clone())
                    .unwrap()
                    .await
                    .unwrap();
                assert_eq!(applied.status.total_target_outcomes, 0);
                assert_eq!(
                    applied
                        .status
                        .targets
                        .iter()
                        .map(|status| status.target.clone())
                        .collect::<Vec<_>>(),
                    original
                );
                updated = true;
            }
        }
        // Endpoint replacement creates a new session but retains the assigned
        // slot; the healthy session must still continue on its original grid.
        targets[0].server_addr = healthy_address.clone();
        let applied = handle.update_targets(targets).unwrap().await.unwrap();
        let replacement = applied
            .status
            .targets
            .iter()
            .find(|status| status.desired && status.target.id.as_str() == "slot")
            .unwrap()
            .target
            .clone();
        assert_ne!(replacement, original[0]);
        let mut replacement_sent = false;
        loop {
            if let ManagedEvent::Client { target, event } = events.recv().await.unwrap() {
                if target == replacement {
                    match event {
                        ClientEvent::EchoSent {
                            seq, scheduled_at, ..
                        } => {
                            assert_eq!(seq, 0);
                            assert_eq!(
                                scheduled_at.unwrap().duration_since(epoch).as_nanos()
                                    % interval.as_nanos(),
                                Duration::from_millis(25).as_nanos()
                            );
                            replacement_sent = true;
                        }
                        ClientEvent::EchoReply { .. } => {
                            assert!(replacement_sent);
                            break;
                        }
                        _ => {}
                    }
                } else if target.id.as_str() == "healthy" {
                    assert_eq!(target, original[1]);
                    assert!(!matches!(event, ClientEvent::SessionStarted(_)));
                }
            }
        }
        handle.stop().await;
        let outcome = run.await.unwrap();
        assert_eq!(outcome.failed_target_outcomes, 0);
        assert!(outcome
            .recent_target_outcomes
            .iter()
            .all(|outcome| outcome.packets_sent >= 1 && outcome.replies_received >= 1));
        stop.send(()).unwrap();
        healthy_stop.send(()).unwrap();
        service.await.unwrap();
        healthy_service.await.unwrap();
    })
    .await
    .expect("explicit rephasing/recovery made no progress");
}

// Invalid desired schedules reject the entire transaction, including otherwise
// valid in-place phase changes and newly added targets.
#[tokio::test(flavor = "current_thread")]
async fn explicit_schedule_validation_and_updates_are_atomic() {
    timeout(Duration::from_secs(5), async {
        let epoch = Instant::now();
        for (pacing, offset) in [
            (ManagedPacing::Explicit { epoch }, None),
            (ManagedPacing::Explicit { epoch }, Some(Duration::MAX)),
            (ManagedPacing::Burst, Some(Duration::ZERO)),
            (ManagedPacing::Staggered, Some(Duration::ZERO)),
        ] {
            let mut invalid = target("invalid", "127.0.0.1:9", 0);
            invalid.send_offset = offset;
            let mut config = config(epoch);
            config.pacing = pacing;
            assert!(matches!(
                ManagedClient::task(config, vec![invalid]),
                Err(ManagedConfigError::InvalidTarget { .. })
            ));
        }
        let (address, stop, service) = server(ServerConfig::default()).await;
        let original = target("stable", &address, 0);
        let (task, handle) = ManagedClient::task(config(epoch), vec![original.clone()]).unwrap();
        let instance = handle.status().targets[0].target.clone();
        let mut events = handle.subscribe().unwrap();
        let run = tokio::spawn(task);
        loop {
            if matches!(
                events.recv().await.unwrap(),
                ManagedEvent::Client {
                    event: ClientEvent::SessionStarted(_),
                    ..
                }
            ) {
                break;
            }
        }
        for invalid_offset in [None, Some(Duration::MAX)] {
            let mut changed = original.clone();
            changed.send_offset = Some(Duration::from_millis(7));
            let mut invalid = target("invalid", &address, 0);
            invalid.send_offset = invalid_offset;
            assert!(matches!(
                handle.update_targets(vec![changed, invalid]).unwrap().await,
                Err(ManagedCommandApplyError::InvalidTarget { .. })
            ));
            let status = handle.status();
            assert_eq!(status.applied_command_sequence, 0);
            assert_eq!(status.desired_target_count, 1);
            assert_eq!(status.targets[0].target, instance);
            assert_eq!(status.total_target_outcomes, 0);
        }
        let rejected_at = Instant::now();
        loop {
            if let ManagedEvent::Client {
                target,
                event:
                    ClientEvent::EchoSent {
                        scheduled_at,
                        sent_at,
                        ..
                    },
            } = events.recv().await.unwrap()
            {
                if sent_at.mono < rejected_at {
                    continue;
                }
                assert_eq!(target, instance);
                assert_eq!(
                    scheduled_at.unwrap().duration_since(epoch).as_nanos()
                        % Duration::from_millis(20).as_nanos(),
                    0
                );
                break;
            }
        }
        let applied = handle
            .update_targets(vec![original])
            .unwrap()
            .await
            .unwrap();
        assert_eq!(applied.sequence, 1);
        assert_eq!(applied.status.targets[0].target, instance);
        handle.stop().await;
        run.await.unwrap();
        stop.send(()).unwrap();
        service.await.unwrap();
    })
    .await
    .expect("transactional scheduling test made no progress");
}
