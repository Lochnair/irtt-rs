#![cfg(feature = "tokio")]

use std::{
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use irtt_client::managed::*;

fn config() -> ManagedClientConfig {
    ManagedClientConfig {
        completion: ManagedCompletionPolicy::ExplicitStop,
        command_capacity: 3,
        ..ManagedClientConfig::default()
    }
}

#[test]
fn stop_resolves_every_accepted_update_and_closes_admission() {
    let (task, handle) = ManagedClient::task(config(), vec![]).unwrap();
    let mut subscription: ManagedStatusSubscription = handle.subscribe_status();
    assert_eq!(
        subscription.borrow().lifecycle,
        ManagedLifecycle::NotStarted
    );
    assert!(!subscription.borrow().stop_requested);
    assert!(subscription.borrow().final_outcome.is_none());
    let receipts: Vec<_> = (0..3)
        .map(|index| {
            handle
                .update_targets(vec![ManagedTargetConfig::new(
                    format!("queued-{index}"),
                    "127.0.0.1:9",
                )])
                .unwrap()
        })
        .collect();
    assert!(matches!(
        handle.update_targets(vec![]),
        Err(ManagedCommandError::QueueFull)
    ));

    let mut stop = pin!(handle.stop());
    assert!(stop
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    assert!(!handle.status().stop_requested);
    assert!(matches!(
        handle.update_targets(vec![]),
        Err(ManagedCommandError::Stopping)
    ));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(2), async {
            let outcome = task.await;
            stop.await;
            for receipt in receipts {
                assert!(matches!(
                    receipt.await,
                    Err(ManagedCommandApplyError::Stopping)
                ));
            }
            assert_eq!(outcome.end_reason, ManagedEndReason::StopRequested);
            assert_eq!(outcome.applied_command_sequence, 0);
            assert_eq!(outcome.total_target_outcomes, 0);
            let status = handle.status();
            assert_eq!(status.lifecycle, ManagedLifecycle::Completed);
            assert!(status.stop_requested);
            assert_eq!(status.final_outcome.as_deref(), Some(&outcome));
            subscription.changed().await.unwrap();
            assert!(std::sync::Arc::ptr_eq(
                &subscription.borrow_and_update(),
                &status
            ));
            assert!(subscription.changed().await.is_err());
            assert_eq!(
                subscription.borrow().final_outcome.as_deref(),
                Some(&outcome)
            );
            assert!(matches!(
                handle.update_targets(vec![]),
                Err(ManagedCommandError::DriverClosed)
            ));
            handle.stop().await;
        })
        .await
        .expect("stop must resolve every buffered update and repeated stop receipt");
    });
}

#[test]
fn failed_driver_resolves_every_accepted_update_with_its_terminal_failure() {
    let (task, handle) = ManagedClient::task(config(), vec![]).unwrap();
    let receipts: Vec<_> = (0..3)
        .map(|index| {
            handle
                .update_targets(vec![ManagedTargetConfig::new(
                    format!("queued-{index}"),
                    "127.0.0.1:9",
                )])
                .unwrap()
        })
        .collect();
    let mut context = Context::from_waker(Waker::noop());
    let Poll::Ready(outcome) = pin!(task).as_mut().poll(&mut context) else {
        panic!("polling without Tokio must fail immediately");
    };
    assert_eq!(
        outcome.end_reason,
        ManagedEndReason::DriverFailed(ManagedDriverFailure::NoTokioRuntime)
    );
    for receipt in receipts {
        assert!(matches!(
            pin!(receipt).as_mut().poll(&mut context),
            Poll::Ready(Err(ManagedCommandApplyError::DriverFailed(
                ManagedDriverFailure::NoTokioRuntime
            )))
        ));
    }
    let status = handle.status();
    assert_eq!(status.lifecycle, ManagedLifecycle::Failed);
    assert!(!status.stop_requested);
    assert_eq!(status.final_outcome.as_deref(), Some(&outcome));
    assert!(matches!(
        handle.update_targets(vec![]),
        Err(ManagedCommandError::DriverClosed)
    ));
    assert!(pin!(handle.stop()).as_mut().poll(&mut context).is_ready());
    assert!(!handle.status().stop_requested);
}

#[test]
fn concurrent_submission_and_stop_or_seal_never_lose_an_accepted_receipt() {
    let (task, handle) = ManagedClient::task(config(), vec![]).unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let submit_barrier = std::sync::Arc::clone(&barrier);
    let submit_handle = handle.clone();
    let (sender, receiver) = std::sync::mpsc::channel();
    let submit = std::thread::spawn(move || {
        submit_barrier.wait();
        sender.send(submit_handle.update_targets(vec![])).unwrap();
    });
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    // Stop is latched before the driver starts, while submission may cross
    // admission before stop, after stop, or after terminal sealing.
    barrier.wait();
    let stop = handle.stop();
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(2), async {
            let outcome = task.await;
            stop.await;
            assert_eq!(outcome.end_reason, ManagedEndReason::StopRequested);
            assert_eq!(outcome.applied_command_sequence, 0);
            assert_eq!(outcome.total_target_outcomes, 0);
            let result = receiver
                .recv_timeout(Duration::from_secs(2))
                .expect("concurrent submission must return");
            submit.join().unwrap();
            match result {
                Ok(receipt) => assert!(matches!(
                    receipt.await,
                    Err(ManagedCommandApplyError::Stopping)
                )),
                Err(ManagedCommandError::Stopping | ManagedCommandError::DriverClosed) => {}
                Err(error) => panic!("unexpected concurrent submission result: {error:?}"),
            }
            assert!(matches!(
                handle.update_targets(vec![]),
                Err(ManagedCommandError::DriverClosed)
            ));
        })
        .await
        .expect("every accepted concurrent submission must receive a terminal disposition");
    });
}
