use std::{io, thread, time::Duration};

use irtt_client::managed::{ManagedClientHandle, ManagedClientTask, ManagedOutcome};
use tokio::sync::oneshot;

/// Keeps measurement scheduling separate from frontend output and statistics.
/// Drop also stops and joins on frontend errors or cancellation.
pub(crate) struct ManagedWorker {
    handle: ManagedClientHandle,
    thread: Option<thread::JoinHandle<()>>,
    completion: oneshot::Receiver<io::Result<ManagedOutcome>>,
}

impl ManagedWorker {
    pub(crate) fn start(task: ManagedClientTask, handle: ManagedClientHandle) -> io::Result<Self> {
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

    pub(crate) async fn join(mut self) -> Result<ManagedOutcome, Box<dyn std::error::Error>> {
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
