mod support;

use std::{collections::HashMap, time::Duration};

use irtt_client::{managed::*, ClientConfig, ClientEvent};
use irtt_server::ServerConfig;
use support::InTreeServer;
use tokio::time::timeout;

// A real service and public events protect cadence/fairness without depending
// on a scheduler, a send cursor, or a private event-publication hook.
#[tokio::test(flavor = "current_thread")]
async fn both_pacing_modes_serve_every_target_on_its_negotiated_grid() {
    timeout(Duration::from_secs(10), async {
        let server = InTreeServer::start(ServerConfig::default());
        for pacing in [ManagedPacing::Burst, ManagedPacing::Staggered] {
            let mut observed_pacing = false;
            for _ in 0..3 {
                let config = ManagedClientConfig {
                    client: ClientConfig {
                        duration: Some(Duration::from_millis(600)),
                        interval: Duration::from_millis(100),
                        probe_timeout: Duration::from_millis(100),
                        open_timeouts: vec![Duration::from_millis(200)],
                        ..ClientConfig::default()
                    },
                    pacing,
                    ..ManagedClientConfig::default()
                };
                let targets = ["first", "second"]
                    .map(|id| ManagedTargetConfig::new(id, server.addr.to_string()));
                let (task, handle) = ManagedClient::task(config, targets.into()).unwrap();
                let mut events = handle.subscribe().unwrap();
                let run = tokio::spawn(task);
                let mut sends: HashMap<TargetInstance, Vec<_>> = HashMap::new();
                let mut actual_sends = Vec::new();
                while let Ok(event) = events.recv().await {
                    if let ManagedEvent::Client {
                        target,
                        event:
                            ClientEvent::EchoSent {
                                scheduled_at,
                                sent_at,
                                ..
                            },
                    } = event
                    {
                        let scheduled = scheduled_at.expect("managed probes expose their cadence");
                        assert!(sent_at.mono >= scheduled);
                        actual_sends.push((target.clone(), sent_at.mono));
                        let previous = sends.entry(target).or_default();
                        if let Some(last) = previous.last() {
                            let elapsed: Duration = scheduled.duration_since(*last);
                            assert!(!elapsed.is_zero());
                            assert_eq!(
                                elapsed.as_nanos() % Duration::from_millis(100).as_nanos(),
                                0
                            );
                        }
                        previous.push(scheduled);
                    }
                }
                let outcome = run.await.unwrap();
                assert_eq!(outcome.end_reason, ManagedEndReason::TargetsComplete);
                assert_eq!(outcome.successful_target_outcomes, 2);
                assert_eq!(sends.len(), 2);
                for target in outcome.recent_target_outcomes.iter() {
                    assert!(sends[&target.target].len() >= 2);
                    assert!(target.replies_received >= 2);
                }
                // Use only the initial pair: later stagger gates can legitimately
                // compress after a delayed wake. Startup may instead serve one
                // target twice before the other opens, so retry a fresh run then.
                if actual_sends.len() < 2 || actual_sends[0].0 == actual_sends[1].0 {
                    continue;
                }
                let gap = actual_sends[1].1.duration_since(actual_sends[0].1);
                match pacing {
                    ManagedPacing::Staggered => {
                        assert!(
                            gap >= Duration::from_millis(50),
                            "first stagger gate was bypassed: {gap:?}"
                        );
                        observed_pacing = true;
                    }
                    ManagedPacing::Burst => {
                        // A pause between burst sends makes this observation
                        // inconclusive; it must not fail an otherwise valid run.
                        observed_pacing = gap < Duration::from_millis(25);
                    }
                }
                if observed_pacing {
                    break;
                }
            }
            assert!(
                observed_pacing,
                "could not observe {pacing:?} startup pacing in three fresh runs"
            );
        }
    })
    .await
    .expect("managed pacing did not complete");
}

// Updates, receipts, generations and cleanup are observable through the control
// handle and event subscription; no task internals or artificial races are needed.
#[tokio::test(flavor = "current_thread")]
async fn target_updates_preserve_identical_sessions_and_retire_removed_generations() {
    timeout(Duration::from_secs(5), async {
        let server = InTreeServer::start(ServerConfig::default());
        let config = ManagedClientConfig {
            client: ClientConfig {
                duration: None,
                interval: Duration::from_millis(20),
                probe_timeout: Duration::from_millis(100),
                open_timeouts: vec![Duration::from_millis(200)],
                ..ClientConfig::default()
            },
            completion: ManagedCompletionPolicy::ExplicitStop,
            outcome_history_limit: 1,
            ..ManagedClientConfig::default()
        };
        let configured = ManagedTargetConfig::new("one", server.addr.to_string());
        let (task, handle) = ManagedClient::task(config, vec![]).unwrap();
        let mut events = handle.subscribe().unwrap();
        let run = tokio::spawn(task);
        let added = handle
            .update_targets(vec![configured.clone()])
            .unwrap()
            .await
            .unwrap();
        let first = added.status.targets[0].target.clone();
        loop {
            if matches!(events.recv().await.unwrap(), ManagedEvent::Client {
                target, event: ClientEvent::EchoReply { .. }
            } if target == first)
            {
                break;
            }
        }
        let identical = handle
            .update_targets(vec![configured.clone()])
            .unwrap()
            .await
            .unwrap();
        assert_eq!(identical.status.targets.len(), 1);
        assert_eq!(identical.status.targets[0].target, first);
        loop {
            match events.recv().await.unwrap() {
                ManagedEvent::Client {
                    target,
                    event: ClientEvent::SessionStarted { .. },
                } if target == first => panic!("identical update reopened the session"),
                ManagedEvent::Client {
                    target,
                    event: ClientEvent::EchoReply { .. },
                } if target == first => break,
                _ => {}
            }
        }
        handle.update_targets(vec![]).unwrap().await.unwrap();
        loop {
            if let ManagedEvent::TargetFinished { outcome } = events.recv().await.unwrap() {
                let status = handle.status();
                assert_eq!(outcome.target, first);
                assert_eq!(outcome.end_reason, ManagedTargetEndReason::Removed);
                assert_eq!(outcome.cleanup_failure, None);
                assert_eq!(status.total_target_outcomes, 1);
                assert_eq!(status.successful_target_outcomes, 1);
                assert_eq!(
                    status.recent_target_outcomes.as_ref(),
                    &[(*outcome).clone()]
                );
                assert!(status.targets.iter().all(|target| target.target != first
                    || target.lifecycle == ManagedTargetLifecycle::Terminal));
                break;
            }
        }
        let readded = handle
            .update_targets(vec![configured])
            .unwrap()
            .await
            .unwrap();
        let second = readded
            .status
            .targets
            .iter()
            .find(|target| target.desired)
            .unwrap()
            .target
            .clone();
        assert!(second.generation > first.generation);
        loop {
            if matches!(events.recv().await.unwrap(), ManagedEvent::Client {
                target, event: ClientEvent::EchoReply { .. }
            } if target == second)
            {
                break;
            }
        }
        let stop = handle.stop();
        loop {
            match events.recv().await.unwrap() {
                ManagedEvent::TargetFinished { outcome } => {
                    let status = handle.status();
                    assert_eq!(outcome.target, second);
                    assert_eq!(status.total_target_outcomes, 2);
                    assert_eq!(
                        status.recent_target_outcomes.as_ref(),
                        &[(*outcome).clone()]
                    );
                    assert!(status.targets.iter().all(|target| target.target != second
                        || target.lifecycle == ManagedTargetLifecycle::Terminal));
                }
                ManagedEvent::Completed { outcome } => {
                    let status = handle.status();
                    assert_eq!(status.lifecycle, ManagedLifecycle::Completed);
                    assert_eq!(status.final_outcome.as_ref(), Some(&outcome));
                    break;
                }
                _ => {}
            }
        }
        let outcome = run.await.unwrap();
        stop.await;
        assert_eq!(outcome.end_reason, ManagedEndReason::StopRequested);
        assert_eq!(outcome.total_target_outcomes, 2);
        assert_eq!(outcome.successful_target_outcomes, 2);
        assert_eq!(outcome.failed_target_outcomes, 0);
        assert_eq!(outcome.discarded_target_outcomes, 1);
        assert_eq!(outcome.recent_target_outcomes.len(), 1);
        let status = handle.status();
        assert_eq!(status.total_target_outcomes, 2);
        assert_eq!(status.successful_target_outcomes, 2);
        assert_eq!(status.failed_target_outcomes, 0);
        assert_eq!(status.discarded_target_outcomes, 1);
        assert_eq!(status.recent_target_outcomes.len(), 1);
        assert_eq!(status.recent_target_outcomes[0].target, second);
        assert!(outcome.recent_target_outcomes.iter().any(|target| {
            target.target == second
                && target.end_reason == ManagedTargetEndReason::Stopped
                && target.cleanup_failure.is_none()
        }));
    })
    .await
    .expect("managed target updates did not complete");
}
