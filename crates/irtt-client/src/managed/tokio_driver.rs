use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
    thread::{self, JoinHandle},
};

use thiserror::Error;
use tokio::{
    runtime,
    sync::{broadcast, mpsc, oneshot, watch},
};

const COMMAND_CAPACITY: usize = 1;
const EVENT_CAPACITY: usize = 16;

/// Result produced by either the async managed driver or its blocking owner.
pub type ManagedTaskResult = Result<ManagedOutcome, ManagedRunError>;

/// Durable lifecycle outcome produced by graceful skeleton completion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedOutcome {
    /// Last control revision durably applied before completion.
    pub control_revision: u64,
}

/// Authoritative latest lifecycle state.
///
/// Handles retain the final immutable snapshot after the driver has finished
/// and dropped the watch sender.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ManagedStatus {
    /// Construction completed, but the task has not yet been polled.
    NotStarted,
    /// The driver has entered its running state.
    Running {
        /// Last durably applied control revision.
        control_revision: u64,
    },
    /// Admission is closed and the driver is sealing its outcome.
    Stopping {
        /// Last durably applied control revision.
        control_revision: u64,
    },
    /// The driver completed graceful shutdown.
    Completed(ManagedOutcome),
    /// The driver failed without producing a complete outcome.
    Failed {
        /// Durable failure reported by the driver.
        error: ManagedRunError,
    },
    /// The task was dropped before it completed.
    Abandoned,
}

/// Lossy presentation event emitted by the managed skeleton.
///
/// Lifecycle correctness never depends on delivery of these events.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ManagedEvent {
    /// The first poll entered a Tokio runtime and started the driver.
    Started,
    /// A control barrier was durably applied.
    ControlApplied {
        /// Monotonic revision assigned to the applied barrier.
        revision: u64,
    },
    /// Graceful shutdown started.
    Stopping,
    /// Graceful shutdown completed.
    Completed(ManagedOutcome),
    /// The driver failed.
    Failed {
        /// Failure reported by the driver.
        error: ManagedRunError,
    },
    /// The task was dropped before completion.
    Abandoned,
}

/// Receiving half of the lossy presentation event channel.
pub type ManagedEventSubscription = broadcast::Receiver<ManagedEvent>;

/// Failure returned by lazy event subscription.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ManagedSubscribeError {
    /// The driver has dropped its only strong event sender.
    #[error("managed event stream is closed")]
    Closed,
}

/// Failure returned while starting or controlling a managed client.
#[derive(Debug, Error)]
pub enum ManagedStartError {
    /// The dedicated blocking worker could not be spawned.
    #[error("failed to spawn managed client worker: {0}")]
    ThreadSpawn(#[source] io::Error),
}

/// Driver or blocking-worker failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ManagedRunError {
    /// The task's first poll did not occur inside a Tokio runtime.
    #[error("ManagedClientTask was first polled without a current Tokio runtime")]
    NoRuntime,
    /// Building the dedicated current-thread Tokio runtime failed.
    #[error("failed to build managed client Tokio runtime: {message}")]
    RuntimeBuild {
        /// Runtime builder error text.
        message: String,
    },
    /// The dedicated worker thread panicked.
    #[error("managed client worker thread panicked")]
    WorkerPanicked,
}

/// Failure returned while submitting or acknowledging a control command.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ManagedCommandError {
    /// Stop admission has closed.
    #[error("managed client is stopping")]
    Stopping,
    /// The bounded control queue has no available capacity.
    #[error("managed client command queue is full")]
    QueueFull,
    /// The driver has closed its command receiver.
    #[error("managed client command channel is closed")]
    DriverClosed,
    /// The task ended before acknowledging an accepted command.
    #[error("managed client command acknowledgement was disconnected")]
    AcknowledgementDisconnected,
}

/// Acknowledgement for an applied control barrier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManagedCommandAck {
    /// Monotonic revision assigned after durable mutation.
    pub revision: u64,
}

/// Future receipt for one accepted control barrier.
#[must_use = "poll the receipt to observe command acknowledgement"]
pub struct ManagedCommandReceipt {
    receiver: oneshot::Receiver<Result<ManagedCommandAck, ManagedCommandError>>,
}

impl Future for ManagedCommandReceipt {
    type Output = Result<ManagedCommandAck, ManagedCommandError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(_)) => {
                Poll::Ready(Err(ManagedCommandError::AcknowledgementDisconnected))
            }
        }
    }
}

#[derive(Debug)]
struct StopLatch {
    admission_open: AtomicBool,
}

impl StopLatch {
    fn new() -> Self {
        Self {
            admission_open: AtomicBool::new(true),
        }
    }

    fn close(&self) -> bool {
        self.admission_open.swap(false, Ordering::AcqRel)
    }

    fn is_open(&self) -> bool {
        self.admission_open.load(Ordering::Acquire)
    }
}

enum ManagedCommand {
    Barrier {
        ack: oneshot::Sender<Result<ManagedCommandAck, ManagedCommandError>>,
    },
    Wakeup,
}

struct ControlLease {
    sender: mpsc::Sender<ManagedCommand>,
    stop_latch: Arc<StopLatch>,
}

impl Drop for ControlLease {
    fn drop(&mut self) {
        self.stop_latch.close();
        // The sender field drops immediately after this method. No synthetic
        // wake is needed: closing Tokio's final MPSC sender wakes the receiver.
    }
}

/// Cloneable control, status, and subscription capability.
///
/// A handle never owns execution and cannot join either the async task or the
/// blocking worker.
#[derive(Clone)]
pub struct ManagedClientHandle {
    control: Arc<ControlLease>,
    events: broadcast::WeakSender<ManagedEvent>,
    status: watch::Receiver<Arc<ManagedStatus>>,
    stop_latch: Arc<StopLatch>,
}

impl ManagedClientHandle {
    /// Return the latest authoritative immutable status snapshot.
    pub fn status(&self) -> Arc<ManagedStatus> {
        Arc::clone(&self.status.borrow())
    }

    /// Subscribe to future lossy presentation events.
    ///
    /// This fails after the driver drops its only strong event sender.
    pub fn subscribe(&self) -> Result<ManagedEventSubscription, ManagedSubscribeError> {
        self.events
            .upgrade()
            .map(|sender| sender.subscribe())
            .ok_or(ManagedSubscribeError::Closed)
    }

    /// Submit an acknowledged ordering barrier to the bounded control queue.
    ///
    /// The acknowledgement is sent only after the driver's authoritative
    /// status contains the returned revision. Dropping the receipt does not
    /// cancel an already accepted command.
    pub fn barrier(&self) -> Result<ManagedCommandReceipt, ManagedCommandError> {
        if !self.stop_latch.is_open() {
            return Err(ManagedCommandError::Stopping);
        }

        let (ack, receiver) = oneshot::channel();
        let command = ManagedCommand::Barrier { ack };
        match self.control.sender.try_send(command) {
            Ok(()) => Ok(ManagedCommandReceipt { receiver }),
            Err(mpsc::error::TrySendError::Full(_)) => Err(ManagedCommandError::QueueFull),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(ManagedCommandError::DriverClosed),
        }
    }

    /// Atomically close command admission and nonblockingly wake the driver.
    ///
    /// Stop does not need queue capacity for correctness. If the queue is full,
    /// the queued command already keeps the receiver runnable and the shared
    /// latch remains the authoritative stop request.
    pub fn stop(&self) {
        if self.stop_latch.close() {
            let _ = self.control.sender.try_send(ManagedCommand::Wakeup);
        }
    }
}

struct DriverResources {
    commands: mpsc::Receiver<ManagedCommand>,
    events: Option<broadcast::Sender<ManagedEvent>>,
    status: watch::Sender<Arc<ManagedStatus>>,
    stop_latch: Arc<StopLatch>,
    control_revision: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DriverState {
    NotStarted,
    Running,
    Sealing,
    Finished,
}

/// Unique async driver for the unified managed client.
///
/// Construction is runtime-independent. The first poll checks for a current
/// Tokio runtime before entering the running state or creating future Tokio I/O
/// and timer resources.
#[must_use = "ManagedClientTask must be awaited or explicitly dropped"]
pub struct ManagedClientTask {
    state: DriverState,
    resources: Option<DriverResources>,
    abandonment_guard_armed: bool,
    blocking_worker: bool,
    #[cfg(test)]
    panic_on_poll: bool,
}

impl ManagedClientTask {
    fn resources(&self) -> &DriverResources {
        self.resources
            .as_ref()
            .expect("managed task resources missing before completion")
    }

    fn resources_mut(&mut self) -> &mut DriverResources {
        self.resources
            .as_mut()
            .expect("managed task resources missing before completion")
    }

    fn publish_status(&self, status: ManagedStatus) {
        let _ = self.resources().status.send_replace(Arc::new(status));
    }

    fn publish_event(&self, event: ManagedEvent) {
        if let Some(events) = &self.resources().events {
            let _ = events.send(event);
        }
    }

    fn apply_command(&mut self, command: ManagedCommand) {
        match command {
            ManagedCommand::Barrier { ack } => {
                let revision = self.resources().control_revision.saturating_add(1);
                self.resources_mut().control_revision = revision;
                self.publish_status(ManagedStatus::Running {
                    control_revision: revision,
                });
                self.publish_event(ManagedEvent::ControlApplied { revision });
                let _ = ack.send(Ok(ManagedCommandAck { revision }));
            }
            ManagedCommand::Wakeup => {}
        }
    }

    fn reject_queued_commands(&mut self) {
        let resources = self.resources_mut();
        resources.commands.close();
        while let Ok(command) = resources.commands.try_recv() {
            if let ManagedCommand::Barrier { ack } = command {
                let _ = ack.send(Err(ManagedCommandError::Stopping));
            }
        }
    }

    fn disconnect_queued_commands(&mut self) {
        let resources = self.resources_mut();
        resources.commands.close();
        while resources.commands.try_recv().is_ok() {}
    }

    fn finish_gracefully(&mut self) -> Poll<ManagedTaskResult> {
        self.state = DriverState::Sealing;
        self.resources().stop_latch.close();
        let revision = self.resources().control_revision;
        self.publish_status(ManagedStatus::Stopping {
            control_revision: revision,
        });
        self.publish_event(ManagedEvent::Stopping);
        self.reject_queued_commands();

        let outcome = ManagedOutcome {
            control_revision: revision,
        };
        self.publish_event(ManagedEvent::Completed(outcome.clone()));
        self.publish_status(ManagedStatus::Completed(outcome.clone()));
        self.resources_mut().events.take();
        self.resources.take();
        self.state = DriverState::Finished;
        self.abandonment_guard_armed = false;
        Poll::Ready(Ok(outcome))
    }

    fn seal_failure(&mut self, error: ManagedRunError) {
        self.state = DriverState::Sealing;
        self.resources().stop_latch.close();
        self.disconnect_queued_commands();
        self.publish_event(ManagedEvent::Failed {
            error: error.clone(),
        });
        self.publish_status(ManagedStatus::Failed { error });
        self.resources_mut().events.take();
        self.resources.take();
        self.state = DriverState::Finished;
        self.abandonment_guard_armed = false;
    }

    fn finish_failure(&mut self, error: ManagedRunError) -> Poll<ManagedTaskResult> {
        self.seal_failure(error.clone());
        Poll::Ready(Err(error))
    }

    fn fail_before_poll(mut self, error: ManagedRunError) -> ManagedTaskResult {
        self.seal_failure(error.clone());
        Err(error)
    }

    fn seal_abandoned(&mut self) {
        self.state = DriverState::Sealing;
        self.resources().stop_latch.close();
        self.disconnect_queued_commands();
        self.publish_event(ManagedEvent::Abandoned);
        self.publish_status(ManagedStatus::Abandoned);
        self.resources_mut().events.take();
        self.resources.take();
        self.state = DriverState::Finished;
        self.abandonment_guard_armed = false;
    }
}

impl Future for ManagedClientTask {
    type Output = ManagedTaskResult;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let task = self.get_mut();

        #[cfg(test)]
        if task.panic_on_poll {
            panic!("injected managed worker panic");
        }

        match task.state {
            DriverState::NotStarted => {
                if runtime::Handle::try_current().is_err() {
                    return task.finish_failure(ManagedRunError::NoRuntime);
                }
                task.state = DriverState::Running;
                task.publish_status(ManagedStatus::Running {
                    control_revision: 0,
                });
                task.publish_event(ManagedEvent::Started);
            }
            DriverState::Running => {}
            DriverState::Sealing => {
                unreachable!("managed task cannot yield while sealing");
            }
            DriverState::Finished => {
                panic!("ManagedClientTask polled after completion");
            }
        }

        loop {
            if !task.resources().stop_latch.is_open() {
                return task.finish_gracefully();
            }

            let command = match Pin::new(&mut task.resources_mut().commands).poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(command)) => command,
                Poll::Ready(None) => {
                    task.resources().stop_latch.close();
                    return task.finish_gracefully();
                }
            };

            if !task.resources().stop_latch.is_open() {
                if let ManagedCommand::Barrier { ack } = command {
                    let _ = ack.send(Err(ManagedCommandError::Stopping));
                }
                return task.finish_gracefully();
            }
            task.apply_command(command);
        }
    }
}

impl Drop for ManagedClientTask {
    fn drop(&mut self) {
        if !self.abandonment_guard_armed {
            return;
        }
        if self.blocking_worker && thread::panicking() {
            self.seal_failure(ManagedRunError::WorkerPanicked);
        } else {
            self.seal_abandoned();
        }
    }
}

/// Unique owner of the dedicated blocking worker thread.
///
/// [`join`](Self::join) only waits; callers must request stop through a handle
/// when the driver would otherwise keep running. Dropping the owner requests
/// graceful stop and joins the worker. This destructor can block while bounded
/// managed cleanup completes once real protocol cleanup is added.
#[must_use = "dropping the owner requests stop and joins the managed worker"]
pub struct ManagedClient {
    control: ManagedClientHandle,
    worker: Option<JoinHandle<ManagedTaskResult>>,
}

impl ManagedClient {
    /// Construct the lazy async driver, its first handle, and an initial event
    /// subscription without requiring a current Tokio runtime.
    pub fn start_async() -> Result<
        (
            ManagedClientTask,
            ManagedClientHandle,
            ManagedEventSubscription,
        ),
        ManagedStartError,
    > {
        let (command_sender, commands) = mpsc::channel(COMMAND_CAPACITY);
        let (event_sender, initial_events) = broadcast::channel(EVENT_CAPACITY);
        let (status_sender, status) = watch::channel(Arc::new(ManagedStatus::NotStarted));
        let stop_latch = Arc::new(StopLatch::new());

        let control = Arc::new(ControlLease {
            sender: command_sender,
            stop_latch: Arc::clone(&stop_latch),
        });
        let handle = ManagedClientHandle {
            control,
            events: event_sender.downgrade(),
            status,
            stop_latch: Arc::clone(&stop_latch),
        };
        let task = ManagedClientTask {
            state: DriverState::NotStarted,
            resources: Some(DriverResources {
                commands,
                events: Some(event_sender),
                status: status_sender,
                stop_latch,
                control_revision: 0,
            }),
            abandonment_guard_armed: true,
            blocking_worker: false,
            #[cfg(test)]
            panic_on_poll: false,
        };
        Ok((task, handle, initial_events))
    }

    /// Start the same managed task on a dedicated current-thread Tokio runtime.
    ///
    /// Thread-spawn failure is returned synchronously. Runtime construction and
    /// later task failures are returned by [`join`](Self::join).
    pub fn start() -> Result<(Self, ManagedEventSubscription), ManagedStartError> {
        let (task, handle, events) = Self::start_async()?;
        let owner = Self::spawn_task(task, handle)?;
        Ok((owner, events))
    }

    fn spawn_task(
        mut task: ManagedClientTask,
        control: ManagedClientHandle,
    ) -> Result<Self, ManagedStartError> {
        task.blocking_worker = true;
        let worker = thread::Builder::new()
            .name("irtt-managed".to_owned())
            .spawn(move || run_blocking_task(task))
            .map_err(ManagedStartError::ThreadSpawn)?;
        Ok(Self {
            control,
            worker: Some(worker),
        })
    }

    /// Clone a control/status/subscription capability.
    pub fn handle(&self) -> ManagedClientHandle {
        self.control.clone()
    }

    /// Wait for worker completion without implicitly requesting stop.
    pub fn join(mut self) -> ManagedTaskResult {
        let worker = self
            .worker
            .take()
            .expect("managed owner worker missing before join");
        match worker.join() {
            Ok(result) => result,
            Err(_) => Err(ManagedRunError::WorkerPanicked),
        }
    }
}

impl Drop for ManagedClient {
    fn drop(&mut self) {
        let Some(worker) = self.worker.take() else {
            return;
        };
        self.control.stop();
        let _ = worker.join();
    }
}

fn run_blocking_task(task: ManagedClientTask) -> ManagedTaskResult {
    match runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
    {
        Ok(runtime) => runtime.block_on(task),
        Err(error) => task.fail_before_poll(ManagedRunError::RuntimeBuild {
            message: error.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::{poll_fn, Future},
        pin::pin,
        sync::mpsc as std_mpsc,
        task::{Context, Poll, Waker},
        thread,
        time::Duration,
    };

    use tokio::{runtime::Builder, sync::broadcast::error::TryRecvError};

    use super::{
        ManagedClient, ManagedCommandError, ManagedEvent, ManagedOutcome, ManagedRunError,
        ManagedStatus, ManagedSubscribeError,
    };

    fn runtime() -> tokio::runtime::Runtime {
        Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .unwrap()
    }

    #[test]
    fn start_async_constructs_outside_tokio() {
        assert!(tokio::runtime::Handle::try_current().is_err());
        let (task, handle, _events) = ManagedClient::start_async().unwrap();
        assert_eq!(*handle.status(), ManagedStatus::NotStarted);
        drop(task);
        assert_eq!(*handle.status(), ManagedStatus::Abandoned);
    }

    #[test]
    fn task_constructed_outside_tokio_runs_inside_current_thread_runtime() {
        let (task, handle, _events) = ManagedClient::start_async().unwrap();
        handle.stop();
        let outcome = runtime().block_on(task).unwrap();
        assert_eq!(outcome.control_revision, 0);
        assert_eq!(*handle.status(), ManagedStatus::Completed(outcome));
    }

    #[test]
    fn first_poll_without_tokio_is_a_durable_startup_failure() {
        let (task, handle, _events) = ManagedClient::start_async().unwrap();
        let mut task = pin!(task);
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);

        assert_eq!(
            task.as_mut().poll(&mut context),
            Poll::Ready(Err(ManagedRunError::NoRuntime))
        );
        assert_eq!(
            *handle.status(),
            ManagedStatus::Failed {
                error: ManagedRunError::NoRuntime
            }
        );
    }

    #[test]
    fn never_polled_task_drop_records_abandoned() {
        let (task, handle, mut events) = ManagedClient::start_async().unwrap();
        drop(task);

        assert_eq!(*handle.status(), ManagedStatus::Abandoned);
        assert_eq!(events.try_recv(), Ok(ManagedEvent::Abandoned));
        assert_eq!(events.try_recv(), Err(TryRecvError::Closed));
    }

    #[test]
    fn dropping_running_task_records_abandoned_and_disconnects_ack() {
        let (task, handle, _events) = ManagedClient::start_async().unwrap();
        let mut task = Box::pin(task);
        runtime().block_on(poll_fn(|cx| {
            assert!(task.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        }));
        let receipt = handle.barrier().unwrap();

        drop(task);
        assert_eq!(*handle.status(), ManagedStatus::Abandoned);
        assert_eq!(
            runtime().block_on(receipt),
            Err(ManagedCommandError::AcknowledgementDisconnected)
        );
    }

    #[test]
    fn completed_task_drop_does_not_overwrite_terminal_status() {
        let (task, handle, _events) = ManagedClient::start_async().unwrap();
        handle.stop();
        let mut task = Box::pin(task);
        let outcome = runtime().block_on(&mut task).unwrap();
        assert_eq!(*handle.status(), ManagedStatus::Completed(outcome.clone()));

        drop(task);
        assert_eq!(*handle.status(), ManagedStatus::Completed(outcome));
    }

    #[test]
    fn handle_requests_but_cannot_drive_completion() {
        let (task, handle, _events) = ManagedClient::start_async().unwrap();
        handle.stop();
        assert_eq!(*handle.status(), ManagedStatus::NotStarted);

        let outcome = runtime().block_on(task).unwrap();
        assert_eq!(*handle.status(), ManagedStatus::Completed(outcome));
    }

    #[test]
    fn final_handle_drop_requests_graceful_stop() {
        let (task, handle, _events) = ManagedClient::start_async().unwrap();
        let status = handle.status.clone();
        let running_status = status.clone();
        let outcome = runtime().block_on(async move {
            let task = tokio::spawn(task);
            tokio::task::yield_now().await;
            assert_eq!(
                **running_status.borrow(),
                ManagedStatus::Running {
                    control_revision: 0
                }
            );
            drop(handle);
            task.await.unwrap().unwrap()
        });
        assert_eq!(**status.borrow(), ManagedStatus::Completed(outcome));
    }

    #[test]
    fn dropping_subscription_has_no_lifecycle_effect() {
        let (task, handle, events) = ManagedClient::start_async().unwrap();
        drop(events);
        let receipt = handle.barrier().unwrap();

        let mut task = pin!(task);
        let mut receipt = pin!(receipt);
        let ack = runtime().block_on(poll_fn(|cx| {
            assert!(task.as_mut().poll(cx).is_pending());
            receipt.as_mut().poll(cx)
        }));
        assert_eq!(ack.unwrap().revision, 1);
        assert_eq!(
            *handle.status(),
            ManagedStatus::Running {
                control_revision: 1
            }
        );
        handle.stop();
        assert!(runtime().block_on(task).is_ok());
    }

    #[test]
    fn stop_completes_when_bounded_command_queue_is_full() {
        let (task, handle, _events) = ManagedClient::start_async().unwrap();
        let receipt = handle.barrier().unwrap();
        assert_eq!(handle.barrier().err(), Some(ManagedCommandError::QueueFull));

        handle.stop();
        let outcome = runtime().block_on(task).unwrap();
        assert_eq!(outcome.control_revision, 0);
        assert_eq!(
            runtime().block_on(receipt),
            Err(ManagedCommandError::Stopping)
        );
    }

    #[test]
    fn acknowledgement_follows_durable_status_mutation() {
        let (task, handle, _events) = ManagedClient::start_async().unwrap();
        let receipt = handle.barrier().unwrap();
        let mut task = pin!(task);
        let mut receipt = pin!(receipt);
        let ack = runtime()
            .block_on(poll_fn(|cx| {
                assert!(task.as_mut().poll(cx).is_pending());
                receipt.as_mut().poll(cx)
            }))
            .unwrap();

        assert_eq!(ack.revision, 1);
        assert_eq!(
            *handle.status(),
            ManagedStatus::Running {
                control_revision: ack.revision
            }
        );
        handle.stop();
        assert!(runtime().block_on(task).is_ok());
    }

    #[test]
    fn accepted_command_applies_after_receipt_is_dropped() {
        let (task, handle, _events) = ManagedClient::start_async().unwrap();
        drop(handle.barrier().unwrap());
        let mut task = pin!(task);

        runtime().block_on(poll_fn(|cx| {
            assert!(task.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        }));
        assert_eq!(
            *handle.status(),
            ManagedStatus::Running {
                control_revision: 1
            }
        );
        handle.stop();
        assert!(runtime().block_on(task).is_ok());
    }

    #[test]
    fn existing_events_drain_after_terminal_sender_drop() {
        let (task, handle, mut events) = ManagedClient::start_async().unwrap();
        handle.stop();
        let outcome = runtime().block_on(task).unwrap();

        assert_eq!(events.try_recv(), Ok(ManagedEvent::Started));
        assert_eq!(events.try_recv(), Ok(ManagedEvent::Stopping));
        assert_eq!(events.try_recv(), Ok(ManagedEvent::Completed(outcome)));
        assert_eq!(events.try_recv(), Err(TryRecvError::Closed));
    }

    #[test]
    fn new_subscription_fails_after_completion() {
        let (task, handle, _events) = ManagedClient::start_async().unwrap();
        handle.stop();
        assert!(runtime().block_on(task).is_ok());
        assert_eq!(
            handle.subscribe().err(),
            Some(ManagedSubscribeError::Closed)
        );
    }

    #[test]
    fn final_status_remains_readable_after_completion() {
        let (task, handle, _events) = ManagedClient::start_async().unwrap();
        handle.stop();
        let outcome = runtime().block_on(task).unwrap();
        assert_eq!(*handle.status(), ManagedStatus::Completed(outcome));
    }

    #[test]
    fn blocking_join_waits_without_requesting_stop() {
        let (owner, _events) = ManagedClient::start().unwrap();
        let handle = owner.handle();
        let (result_sender, result_receiver) = std_mpsc::channel();
        let (entered_sender, entered_receiver) = std_mpsc::channel();
        let joiner = thread::spawn(move || {
            entered_sender.send(()).unwrap();
            result_sender.send(owner.join()).unwrap();
        });

        entered_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        assert!(result_receiver
            .recv_timeout(Duration::from_millis(100))
            .is_err());
        handle.stop();
        assert!(result_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .is_ok());
        joiner.join().unwrap();
    }

    #[test]
    fn blocking_owner_drop_requests_stop_and_joins() {
        let (owner, _events) = ManagedClient::start().unwrap();
        let handle = owner.handle();

        drop(owner);

        assert!(matches!(
            &*handle.status(),
            ManagedStatus::Completed(ManagedOutcome { .. })
        ));
    }

    #[test]
    fn worker_panic_maps_to_failure_not_abandonment() {
        let (mut task, handle, _events) = ManagedClient::start_async().unwrap();
        task.panic_on_poll = true;
        let owner = ManagedClient::spawn_task(task, handle.clone()).unwrap();

        assert_eq!(owner.join(), Err(ManagedRunError::WorkerPanicked));
        assert_eq!(
            *handle.status(),
            ManagedStatus::Failed {
                error: ManagedRunError::WorkerPanicked
            }
        );
    }
}
