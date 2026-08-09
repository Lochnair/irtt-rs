use std::thread::{self, JoinHandle};

use thiserror::Error;
use tokio::runtime::Builder;

use super::{
    ManagedClient, ManagedClientConfig, ManagedClientHandle, ManagedConfigError, ManagedOutcome,
    ManagedTargetConfig,
};

/// Failure to start a blocking managed client owner.
#[derive(Debug, Error)]
pub enum BlockingManagedStartError {
    /// The managed task configuration was invalid.
    #[error(transparent)]
    Config(#[from] ManagedConfigError),

    /// The dedicated Tokio runtime could not be created.
    #[error("failed to create blocking managed Tokio runtime: {source}")]
    Runtime {
        #[source]
        source: std::io::Error,
    },

    /// The dedicated worker thread could not be spawned.
    #[error("failed to spawn blocking managed worker thread: {source}")]
    ThreadSpawn {
        #[source]
        source: std::io::Error,
    },
}

/// Failure to obtain completion from a blocking managed client owner.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum BlockingManagedJoinError {
    /// The dedicated managed worker thread panicked.
    #[error("blocking managed worker thread panicked")]
    WorkerPanicked,
}

/// Blocking completion owner for one unified managed Tokio task.
///
/// This owner runs exactly one [`ManagedClient`] task on one dedicated OS thread
/// with its own current-thread Tokio runtime. Dropping a live owner requests a
/// graceful stop and synchronously joins that worker, so drop may block while
/// bounded managed cleanup completes.
#[must_use = "dropping the blocking managed owner requests stop and joins its worker"]
pub struct BlockingManagedClient {
    handle: ManagedClientHandle,
    worker: Option<JoinHandle<ManagedOutcome>>,
}

impl BlockingManagedClient {
    /// Validate configuration, create a dedicated runtime, and start its worker.
    pub fn start(
        config: ManagedClientConfig,
        targets: Vec<ManagedTargetConfig>,
    ) -> Result<Self, BlockingManagedStartError> {
        let (task, handle) = ManagedClient::task(config, targets)?;
        let runtime = Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .map_err(|source| BlockingManagedStartError::Runtime { source })?;
        let worker = thread::Builder::new()
            .name("irtt-managed".to_owned())
            .spawn(move || runtime.block_on(task))
            .map_err(|source| BlockingManagedStartError::ThreadSpawn { source })?;

        Ok(Self {
            handle,
            worker: Some(worker),
        })
    }

    /// Return a cloned managed control and observation capability.
    pub fn handle(&self) -> ManagedClientHandle {
        self.handle.clone()
    }

    /// Wait synchronously for the managed task's authoritative outcome.
    pub fn join(mut self) -> Result<ManagedOutcome, BlockingManagedJoinError> {
        self.join_worker()
    }

    fn join_worker(&mut self) -> Result<ManagedOutcome, BlockingManagedJoinError> {
        self.worker
            .take()
            .expect("blocking managed worker is joined at most once")
            .join()
            .map_err(|_| BlockingManagedJoinError::WorkerPanicked)
    }
}

impl Drop for BlockingManagedClient {
    fn drop(&mut self) {
        if self.worker.is_some() {
            let _receipt = self.handle.stop();
            let _ = self.join_worker();
        }
    }
}
