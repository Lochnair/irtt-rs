use std::{io, thread, time::Duration};

use irtt_client::managed::{ManagedClientHandle, ManagedClientTask, ManagedOutcome};
use tokio::sync::oneshot;

/// Keeps measurement scheduling separate from frontend output and statistics.
/// Drop also stops and joins on frontend errors or cancellation.
pub(super) struct ManagedWorker {
    handle: ManagedClientHandle,
    thread: Option<thread::JoinHandle<()>>,
    completion: oneshot::Receiver<io::Result<ManagedOutcome>>,
}

impl ManagedWorker {
    pub(super) fn start(task: ManagedClientTask, handle: ManagedClientHandle) -> io::Result<Self> {
        let (completed, completion) = oneshot::channel();
        let thread = thread::Builder::new()
            .name("irtt-managed".to_owned())
            .spawn(move || {
                // Construct and destroy the runtime on its owning thread, including
                // startup failures; never drop a runtime inside the frontend executor.
                let outcome = (|| {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()?;
                    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        runtime.block_on(task)
                    }));
                    runtime.shutdown_timeout(Duration::from_millis(100));
                    match outcome {
                        Ok(outcome) => Ok(outcome),
                        Err(panic) => std::panic::resume_unwind(panic),
                    }
                })();
                let _ = completed.send(outcome);
            })?;
        Ok(Self {
            handle,
            thread: Some(thread),
            completion,
        })
    }

    pub(super) async fn join(mut self) -> Result<ManagedOutcome, Box<dyn std::error::Error>> {
        let outcome = (&mut self.completion).await;
        self.join_thread()?;
        Ok(outcome
            .map_err(|_| io::Error::other("managed worker completed without an outcome"))??)
    }

    fn join_thread(&mut self) -> io::Result<()> {
        self.thread
            .take()
            .expect("managed worker is joined once")
            .join()
            .map_err(|_| io::Error::other("managed worker thread panicked"))
    }
}

impl Drop for ManagedWorker {
    fn drop(&mut self) {
        if self.thread.is_some() {
            drop(self.handle.stop());
            let _ = self.join_thread();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use irtt_client::managed::{
        ManagedClient, ManagedClientConfig, ManagedCompletionPolicy, ManagedEndReason,
    };

    // The frontend executor must be able to stop a worker while awaiting its
    // completion; a blocking join here would deadlock this current-thread runtime.
    #[test]
    fn completion_leaves_frontend_free_to_stop_and_preserves_outcome() {
        let (task, handle) = ManagedClient::task(
            ManagedClientConfig {
                completion: ManagedCompletionPolicy::ExplicitStop,
                ..Default::default()
            },
            vec![],
        )
        .unwrap();
        let worker = ManagedWorker::start(task, handle.clone()).unwrap();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let (outcome, ()) = tokio::time::timeout(Duration::from_secs(2), async {
                    tokio::join!(worker.join(), handle.stop())
                })
                .await
                .expect("worker completion must allow frontend stop");
                let outcome = outcome.unwrap();
                assert_eq!(outcome.end_reason, ManagedEndReason::StopRequested);
                assert_eq!(handle.status().final_outcome.as_deref(), Some(&outcome));
            });
    }

    // Cancelling the frontend's completion wait must stop and join the worker,
    // rather than detaching an ongoing measurement task.
    #[test]
    fn cancelling_completion_stops_and_joins_worker() {
        use std::{
            future::Future,
            task::{Context, Waker},
        };
        let (task, handle) = ManagedClient::task(
            ManagedClientConfig {
                completion: ManagedCompletionPolicy::ExplicitStop,
                ..Default::default()
            },
            vec![],
        )
        .unwrap();
        let worker = ManagedWorker::start(task, handle.clone()).unwrap();
        let mut completion = Box::pin(worker.join());
        assert!(completion
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        drop(completion);
        assert_eq!(
            handle.status().final_outcome.as_ref().unwrap().end_reason,
            ManagedEndReason::StopRequested
        );
    }
}
