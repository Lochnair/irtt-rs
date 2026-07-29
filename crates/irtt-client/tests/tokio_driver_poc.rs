#![cfg(feature = "tokio")]

use std::{
    collections::BTreeSet,
    error::Error,
    future::{poll_fn, Future},
    io,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use tokio::{
    net::UdpSocket,
    runtime::{Builder, Handle},
    sync::{mpsc, oneshot},
    task,
    time::{self, Instant, Sleep},
};

const COMMAND_BUDGET: usize = 2;
const DEADLINE_BUDGET: usize = 2;
const TARGET_BUDGET: usize = 2;
const PENDING_CAPACITY: usize = 4;
const OUTER_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug)]
struct PreparedProbe {
    bytes: Box<[u8]>,
    seq: u32,
}

#[derive(Debug)]
struct ProbeCommit {
    // Every fallible value and storage choice is validated before try_send.
    pending_slot: usize,
    pending: PendingProbe,
    next_seq: u32,
    sent_count: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingProbe {
    seq: u32,
    bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PocTargetId(u8);

#[derive(Default)]
struct PocDropCounters {
    targets: AtomicUsize,
    prepared: AtomicUsize,
}

struct PocTarget {
    id: PocTargetId,
    socket: UdpSocket,
    deadline: Option<Instant>,
    deadline_after_receive: Option<Duration>,
    outbound: Option<PreparedProbe>,
    next_seq: u32,
    sent_count: u64,
    pending: [Option<PendingProbe>; PENDING_CAPACITY],
    received_count: u64,
    deadline_count: u64,
    write_ready_polls: u64,
    injected_try_send: Option<InjectedTrySend>,
    drops: Arc<PocDropCounters>,
}

impl PocTarget {
    fn new(
        id: PocTargetId,
        socket: UdpSocket,
        deadline: Option<Instant>,
        drops: Arc<PocDropCounters>,
    ) -> Self {
        Self {
            id,
            socket,
            deadline,
            deadline_after_receive: None,
            outbound: None,
            next_seq: 0,
            sent_count: 0,
            pending: [None; PENDING_CAPACITY],
            received_count: 0,
            deadline_count: 0,
            write_ready_polls: 0,
            injected_try_send: None,
            drops,
        }
    }

    fn set_deadline_after_receive(&mut self, delay: Duration) {
        self.deadline_after_receive = Some(delay);
    }

    fn prepare_send(&mut self, bytes: Box<[u8]>) {
        assert!(self.outbound.is_none(), "only one PoC datagram is prepared");
        self.outbound = Some(PreparedProbe {
            bytes,
            seq: self.next_seq,
        });
    }

    fn drop_prepared(&mut self) {
        self.outbound = None;
    }

    fn inject_next_try_send(&mut self, result: InjectedTrySend) {
        self.injected_try_send = Some(result);
    }

    fn on_deadline(&mut self, now: Instant) -> PocTargetProgress {
        let old = self.deadline.take();
        self.deadline_count += 1;
        PocTargetProgress {
            observation: Some(PocObservation {
                id: self.id,
                kind: PocObservationKind::Deadline { at: now },
            }),
            deadline_change: Some(DeadlineChange {
                id: self.id,
                old,
                new: None,
            }),
        }
    }

    fn poll_io(&mut self, cx: &mut Context<'_>) -> io::Result<PocTargetVisit> {
        let receive = self.poll_receive(cx)?;
        let outbound = if self.outbound.is_some() {
            match self.poll_outbound(cx) {
                Poll::Pending => PocTargetProgress::default(),
                Poll::Ready(result) => result?,
            }
        } else {
            PocTargetProgress::default()
        };
        Ok(PocTargetVisit { receive, outbound })
    }

    fn poll_receive(&mut self, cx: &mut Context<'_>) -> io::Result<PocTargetProgress> {
        match self.socket.poll_recv_ready(cx) {
            Poll::Pending => Ok(PocTargetProgress::default()),
            Poll::Ready(Err(error)) => Err(error),
            Poll::Ready(Ok(())) => {
                let mut bytes = [0_u8; 2048];
                let result = self.socket.try_recv(&mut bytes);
                self.consume_receive(result, &bytes, Instant::now())
            }
        }
    }

    fn consume_receive(
        &mut self,
        result: io::Result<usize>,
        bytes: &[u8],
        now: Instant,
    ) -> io::Result<PocTargetProgress> {
        let received = match result {
            Ok(received) => received,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                return Ok(PocTargetProgress::default());
            }
            Err(error) => return Err(error),
        };

        self.received_count += 1;
        let deadline_change = self.deadline_after_receive.take().map(|delay| {
            let old = self.deadline.replace(now + delay);
            DeadlineChange {
                id: self.id,
                old,
                new: self.deadline,
            }
        });
        Ok(PocTargetProgress {
            observation: Some(PocObservation {
                id: self.id,
                kind: PocObservationKind::Received(bytes[..received].to_vec()),
            }),
            deadline_change,
        })
    }

    fn poll_outbound(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<PocTargetProgress>> {
        if self.outbound.is_none() {
            return Poll::Ready(Ok(PocTargetProgress::default()));
        }

        self.write_ready_polls += 1;
        match self.socket.poll_send_ready(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => Poll::Ready(self.try_send_prepared()),
        }
    }

    fn try_send_prepared(&mut self) -> io::Result<PocTargetProgress> {
        let prepared = self
            .outbound
            .as_ref()
            .expect("outbound readiness is conditional on a prepared probe");
        let seq = prepared.seq;
        let bytes = prepared.bytes.len();

        // Preflight owns every fallible operation. ProbeCommit is deliberately
        // non-Clone and short-lived.
        let commit = self.preflight_commit(seq, bytes)?;
        let send_result = match self.injected_try_send.take() {
            Some(InjectedTrySend::WouldBlock) => Err(io::Error::from(io::ErrorKind::WouldBlock)),
            Some(InjectedTrySend::Success) => Ok(bytes),
            None => self
                .socket
                .try_send(&self.outbound.as_ref().expect("still prepared").bytes),
        };

        match send_result {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                // Only the ephemeral commit is dropped. The prepared packet
                // was never taken, so there is no take/put-back path.
                Ok(PocTargetProgress::default())
            }
            Err(error) => Err(error),
            Ok(_accepted) => {
                // There is no fallible work between acceptance and this
                // consuming, infallible commit.
                self.commit_sent(commit);
                self.outbound = None;
                Ok(PocTargetProgress {
                    observation: Some(PocObservation {
                        id: self.id,
                        kind: PocObservationKind::Sent { seq },
                    }),
                    deadline_change: None,
                })
            }
        }
    }

    fn preflight_commit(&self, seq: u32, bytes: usize) -> io::Result<ProbeCommit> {
        if seq != self.next_seq {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "prepared sequence is stale",
            ));
        }
        let pending_slot = self
            .pending
            .iter()
            .position(Option::is_none)
            .ok_or_else(|| io::Error::other("PoC pending storage is full"))?;
        let next_seq = seq
            .checked_add(1)
            .ok_or_else(|| io::Error::other("PoC sequence overflow"))?;
        let sent_count = self
            .sent_count
            .checked_add(1)
            .ok_or_else(|| io::Error::other("PoC send counter overflow"))?;

        Ok(ProbeCommit {
            pending_slot,
            pending: PendingProbe { seq, bytes },
            next_seq,
            sent_count,
        })
    }

    fn commit_sent(&mut self, commit: ProbeCommit) {
        self.pending[commit.pending_slot] = Some(commit.pending);
        self.next_seq = commit.next_seq;
        self.sent_count = commit.sent_count;
    }

    fn snapshot(&self) -> PocTargetSnapshot {
        PocTargetSnapshot {
            id: self.id,
            deadline: self.deadline,
            prepared_seq: self.outbound.as_ref().map(|prepared| prepared.seq),
            next_seq: self.next_seq,
            sent_count: self.sent_count,
            pending_count: self.pending.iter().flatten().count(),
            received_count: self.received_count,
            deadline_count: self.deadline_count,
            write_ready_polls: self.write_ready_polls,
        }
    }
}

impl Drop for PocTarget {
    fn drop(&mut self) {
        self.drops.targets.fetch_add(1, Ordering::SeqCst);
        if self.outbound.is_some() {
            self.drops.prepared.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum InjectedTrySend {
    WouldBlock,
    Success,
}

#[derive(Default)]
struct PocTargetProgress {
    observation: Option<PocObservation>,
    deadline_change: Option<DeadlineChange>,
}

struct PocTargetVisit {
    receive: PocTargetProgress,
    outbound: PocTargetProgress,
}

#[derive(Clone, Copy)]
struct DeadlineChange {
    id: PocTargetId,
    old: Option<Instant>,
    new: Option<Instant>,
}

#[derive(Debug, Eq, PartialEq)]
struct PocObservation {
    id: PocTargetId,
    kind: PocObservationKind,
}

#[derive(Debug, Eq, PartialEq)]
enum PocObservationKind {
    Received(Vec<u8>),
    Sent { seq: u32 },
    Deadline { at: Instant },
}

enum PocCommand {
    Insert {
        target: Box<PocTarget>,
        ack: oneshot::Sender<usize>,
    },
    Remove {
        id: PocTargetId,
        ack: oneshot::Sender<Option<PocRemoved>>,
    },
    Snapshot {
        ack: oneshot::Sender<PocSnapshot>,
    },
    Shutdown {
        ack: oneshot::Sender<()>,
    },
}

#[derive(Debug)]
struct PocRemoved {
    id: PocTargetId,
    received_count: u64,
    sent_count: u64,
    pending_count: usize,
}

#[derive(Debug)]
struct PocSnapshot {
    targets: Vec<PocTargetSnapshot>,
    armed_deadline: Option<Instant>,
}

impl PocSnapshot {
    fn target(&self, id: PocTargetId) -> &PocTargetSnapshot {
        self.targets
            .iter()
            .find(|target| target.id == id)
            .expect("target must be present in snapshot")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PocTargetSnapshot {
    id: PocTargetId,
    deadline: Option<Instant>,
    prepared_seq: Option<u32>,
    next_seq: u32,
    sent_count: u64,
    pending_count: usize,
    received_count: u64,
    deadline_count: u64,
    write_ready_polls: u64,
}

#[derive(Debug)]
struct PocDriverOutput {
    observations: Vec<PocObservation>,
    metrics: PocMetrics,
    remaining_targets: usize,
}

#[derive(Debug, Default)]
struct PocMetrics {
    cycles: Vec<PocPollCycle>,
    visits: Vec<PocVisit>,
    timer_rearms: Vec<Option<Instant>>,
    incomplete_sweep_wakes: usize,
    progress_wakes: usize,
}

#[derive(Debug, Default)]
struct PocPollCycle {
    commands: usize,
    deadlines: usize,
    targets: usize,
    cursor_after: usize,
    sweep_remaining_after: usize,
    target_len_after: usize,
    self_woke: bool,
    made_progress: bool,
    command_budget_exhausted: bool,
    deadline_budget_exhausted: bool,
}

#[derive(Debug)]
struct PocVisit {
    id: PocTargetId,
    cursor_before: usize,
    cursor_after: usize,
    target_len: usize,
}

#[derive(Debug, Eq, PartialEq)]
enum PocStartError {
    NoRuntime,
}

impl std::fmt::Display for PocStartError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoRuntime => formatter.write_str("no current Tokio runtime"),
        }
    }
}

impl Error for PocStartError {}

struct PocDriver {
    control_rx: mpsc::Receiver<PocCommand>,
    control_open: bool,
    targets: Vec<PocTarget>,
    deadlines: BTreeSet<(Instant, PocTargetId)>,
    timer: Pin<Box<Sleep>>,
    armed_deadline: Option<Instant>,
    cursor: usize,
    sweep_remaining: usize,
    observations: Vec<PocObservation>,
    metrics: PocMetrics,
}

impl PocDriver {
    fn new(
        control_rx: mpsc::Receiver<PocCommand>,
        targets: Vec<PocTarget>,
    ) -> Result<Self, PocStartError> {
        Handle::try_current().map_err(|_| PocStartError::NoRuntime)?;
        let deadlines = targets
            .iter()
            .filter_map(|target| target.deadline.map(|deadline| (deadline, target.id)))
            .collect();
        let sweep_remaining = targets.len();

        // This checks only that a Tokio context exists. A runtime without I/O
        // or time drivers remains caller misconfiguration and may panic here
        // or when a socket is constructed; the PoC does not catch that panic.
        let timer = Box::pin(time::sleep_until(Instant::now()));
        Ok(Self {
            control_rx,
            control_open: true,
            targets,
            deadlines,
            timer,
            armed_deadline: None,
            cursor: 0,
            sweep_remaining,
            observations: Vec::new(),
            metrics: PocMetrics::default(),
        })
    }

    fn poll_commands(&mut self, cx: &mut Context<'_>, cycle: &mut PocPollCycle) -> bool {
        if !self.control_open {
            return false;
        }

        for _ in 0..COMMAND_BUDGET {
            let command = match Pin::new(&mut self.control_rx).poll_recv(cx) {
                Poll::Pending => return false,
                Poll::Ready(None) => {
                    self.control_open = false;
                    return false;
                }
                Poll::Ready(Some(command)) => command,
            };
            cycle.commands += 1;
            cycle.made_progress = true;
            if self.apply_command(command) {
                return true;
            }
        }

        cycle.command_budget_exhausted = true;
        false
    }

    fn apply_command(&mut self, command: PocCommand) -> bool {
        match command {
            PocCommand::Insert { target, ack } => {
                let target = *target;
                assert!(
                    self.targets.iter().all(|existing| existing.id != target.id),
                    "PoC target IDs are unique"
                );
                if let Some(deadline) = target.deadline {
                    self.deadlines.insert((deadline, target.id));
                }
                self.targets.push(target);
                self.reset_sweep_after_structure_change();
                let _ = ack.send(self.targets.len());
                false
            }
            PocCommand::Remove { id, ack } => {
                let removed = self.remove(id);
                let _ = ack.send(removed);
                false
            }
            PocCommand::Snapshot { ack } => {
                let _ = ack.send(PocSnapshot {
                    targets: self.targets.iter().map(PocTarget::snapshot).collect(),
                    armed_deadline: self.armed_deadline,
                });
                false
            }
            PocCommand::Shutdown { ack } => {
                self.deadlines.clear();
                self.targets.clear();
                self.cursor = 0;
                self.sweep_remaining = 0;
                let _ = ack.send(());
                true
            }
        }
    }

    fn remove(&mut self, id: PocTargetId) -> Option<PocRemoved> {
        let position = self.targets.iter().position(|target| target.id == id)?;
        let target = self.targets.remove(position);
        if let Some(deadline) = target.deadline {
            self.deadlines.remove(&(deadline, id));
        }
        if position < self.cursor {
            self.cursor -= 1;
        }
        if self.targets.is_empty() || self.cursor == self.targets.len() {
            self.cursor = 0;
        }
        self.reset_sweep_after_structure_change();

        let removed = PocRemoved {
            id,
            received_count: target.received_count,
            sent_count: target.sent_count,
            pending_count: target.pending.iter().flatten().count(),
        };
        // The socket and any prepared packet are gone before the acknowledgement.
        drop(target);
        Some(removed)
    }

    fn reset_sweep_after_structure_change(&mut self) {
        self.sweep_remaining = self.targets.len();
        if self.targets.is_empty() {
            self.cursor = 0;
        } else {
            self.cursor %= self.targets.len();
        }
    }

    fn poll_due_deadlines(&mut self, cx: &mut Context<'_>, cycle: &mut PocPollCycle) {
        self.rearm_timer();
        if self.armed_deadline.is_none() || self.timer.as_mut().poll(cx).is_pending() {
            return;
        }

        let now = Instant::now();
        for _ in 0..DEADLINE_BUDGET {
            let Some((deadline, id)) = self.deadlines.first().copied() else {
                break;
            };
            if deadline > now {
                break;
            }
            self.deadlines.remove(&(deadline, id));
            let Some(position) = self.targets.iter().position(|target| target.id == id) else {
                continue;
            };
            let progress = self.targets[position].on_deadline(now);
            self.record_progress(progress, cycle);
            cycle.deadlines += 1;
        }

        cycle.deadline_budget_exhausted = self
            .deadlines
            .first()
            .is_some_and(|(deadline, _)| *deadline <= now);
    }

    fn poll_targets(&mut self, cx: &mut Context<'_>, cycle: &mut PocPollCycle) -> io::Result<()> {
        if self.targets.is_empty() {
            self.sweep_remaining = 0;
            return Ok(());
        }
        if self.sweep_remaining == 0 {
            self.sweep_remaining = self.targets.len();
        }

        let visits = TARGET_BUDGET.min(self.sweep_remaining);
        for _ in 0..visits {
            let cursor_before = self.cursor;
            let id = self.targets[cursor_before].id;
            let visit = self.targets[cursor_before].poll_io(cx)?;
            self.cursor = (self.cursor + 1) % self.targets.len();
            self.sweep_remaining -= 1;
            cycle.targets += 1;
            self.metrics.visits.push(PocVisit {
                id,
                cursor_before,
                cursor_after: self.cursor,
                target_len: self.targets.len(),
            });
            self.record_progress(visit.receive, cycle);
            self.record_progress(visit.outbound, cycle);
        }
        Ok(())
    }

    fn record_progress(&mut self, progress: PocTargetProgress, cycle: &mut PocPollCycle) {
        if let Some(change) = progress.deadline_change {
            if let Some(old) = change.old {
                self.deadlines.remove(&(old, change.id));
            }
            if let Some(new) = change.new {
                self.deadlines.insert((new, change.id));
            }
        }
        if let Some(observation) = progress.observation {
            self.observations.push(observation);
            cycle.made_progress = true;
        }
    }

    fn finish_or_rearm(&mut self, cx: &mut Context<'_>, cycle: &mut PocPollCycle) {
        self.rearm_timer();
        let incomplete_sweep = self.sweep_remaining > 0;
        let should_wake = incomplete_sweep
            || cycle.made_progress
            || cycle.command_budget_exhausted
            || cycle.deadline_budget_exhausted;
        if should_wake {
            cx.waker().wake_by_ref();
            cycle.self_woke = true;
            if incomplete_sweep {
                self.metrics.incomplete_sweep_wakes += 1;
            } else {
                self.metrics.progress_wakes += 1;
            }
        }
        cycle.cursor_after = self.cursor;
        cycle.sweep_remaining_after = self.sweep_remaining;
        cycle.target_len_after = self.targets.len();
    }

    fn rearm_timer(&mut self) {
        let earliest = self.deadlines.first().map(|(deadline, _)| *deadline);
        if earliest == self.armed_deadline {
            return;
        }
        self.armed_deadline = earliest;
        self.metrics.timer_rearms.push(earliest);
        if let Some(deadline) = earliest {
            self.timer.as_mut().reset(deadline);
        }
    }

    fn complete(&mut self) -> PocDriverOutput {
        PocDriverOutput {
            observations: std::mem::take(&mut self.observations),
            metrics: std::mem::take(&mut self.metrics),
            remaining_targets: self.targets.len(),
        }
    }
}

impl Future for PocDriver {
    type Output = io::Result<PocDriverOutput>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let driver = self.get_mut();
        let mut cycle = PocPollCycle::default();

        if driver.poll_commands(cx, &mut cycle) {
            driver.finish_or_rearm(cx, &mut cycle);
            driver.metrics.cycles.push(cycle);
            return Poll::Ready(Ok(driver.complete()));
        }
        driver.poll_due_deadlines(cx, &mut cycle);
        if let Err(error) = driver.poll_targets(cx, &mut cycle) {
            return Poll::Ready(Err(error));
        }
        driver.finish_or_rearm(cx, &mut cycle);
        driver.metrics.cycles.push(cycle);
        Poll::Pending
    }
}

#[derive(Default)]
struct WakeCounter {
    wakes: AtomicUsize,
}

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn directly_polled_driver_multiplexes_real_sockets_fairly() -> Result<(), Box<dyn Error>> {
    let runtime = Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()?;
    runtime.block_on(real_socket_scenario())
}

async fn real_socket_scenario() -> Result<(), Box<dyn Error>> {
    let drops = Arc::new(PocDropCounters::default());
    let (client_one, peer_one) = connected_pair().await?;
    let (client_two, peer_two) = connected_pair().await?;
    let (client_three, peer_three) = connected_pair().await?;
    let (client_four, peer_four) = connected_pair().await?;
    let base_deadline = Instant::now() + Duration::from_millis(500);

    let target_one = PocTarget::new(
        PocTargetId(1),
        client_one,
        Some(base_deadline),
        Arc::clone(&drops),
    );
    let mut target_two = PocTarget::new(
        PocTargetId(2),
        client_two,
        Some(base_deadline),
        Arc::clone(&drops),
    );
    target_two.set_deadline_after_receive(Duration::from_millis(30));
    let mut target_three = PocTarget::new(
        PocTargetId(3),
        client_three,
        Some(base_deadline),
        Arc::clone(&drops),
    );
    target_three.prepare_send(Box::from(&b"prepared-three"[..]));
    let target_four = PocTarget::new(
        PocTargetId(4),
        client_four,
        Some(base_deadline),
        Arc::clone(&drops),
    );

    let (command_tx, command_rx) = mpsc::channel(16);
    let driver = PocDriver::new(
        command_rx,
        vec![target_one, target_two, target_three, target_four],
    )?;

    // Queue more work than one command budget before the first poll.
    let mut queued_snapshots = Vec::new();
    for _ in 0..5 {
        let (ack, received) = oneshot::channel();
        command_tx.try_send(PocCommand::Snapshot { ack })?;
        queued_snapshots.push(received);
    }
    let driver_task = task::spawn(driver);
    for received in queued_snapshots {
        time::timeout(OUTER_TIMEOUT, received).await??;
    }

    // One harness task keeps a single socket hot across both the progress
    // deadline and the shared base deadline. There is no task per target.
    let hot_until = base_deadline + Duration::from_millis(100);
    let hot_peer = task::spawn(async move {
        while Instant::now() < hot_until {
            peer_one.send(b"hot-one").await?;
            task::yield_now().await;
        }
        Ok::<(), io::Error>(())
    });

    let (dynamic_client, dynamic_peer) = connected_pair().await?;
    let dynamic_addr = dynamic_client.local_addr()?;
    let dynamic_deadline = base_deadline - Duration::from_millis(100);
    let dynamic = PocTarget::new(
        PocTargetId(5),
        dynamic_client,
        Some(dynamic_deadline),
        Arc::clone(&drops),
    );

    // A cloned sender inserts while the hot socket is active. The ack cannot
    // arrive until the registry contains the target.
    let insert_tx = command_tx.clone();
    let (insert_ack, inserted) = oneshot::channel();
    insert_tx
        .send(PocCommand::Insert {
            target: Box::new(dynamic),
            ack: insert_ack,
        })
        .await?;
    assert_eq!(time::timeout(OUTER_TIMEOUT, inserted).await??, 5);

    // A different clone removes it. The target and socket are dropped before
    // the ack is sent, so later traffic cannot produce an observation.
    let remove_tx = command_tx.clone();
    let (remove_ack, removed) = oneshot::channel();
    remove_tx
        .send(PocCommand::Remove {
            id: PocTargetId(5),
            ack: remove_ack,
        })
        .await?;
    let removed = time::timeout(OUTER_TIMEOUT, removed)
        .await??
        .expect("dynamic target must be removed");
    assert_eq!(removed.id, PocTargetId(5));
    assert_eq!(removed.received_count, 0);
    assert_eq!(removed.sent_count, 0);
    assert_eq!(removed.pending_count, 0);
    dynamic_peer.send(b"after-remove").await?;
    peer_two.send(b"quiet-two").await?;
    peer_three.send(b"quiet-three").await?;
    peer_four.send(b"quiet-four").await?;

    // The removed client socket is already closed when its acknowledgement is
    // observed.
    let rebound = std::net::UdpSocket::bind(dynamic_addr)?;
    drop(rebound);

    let early_snapshot = time::timeout(OUTER_TIMEOUT, async {
        loop {
            let snapshot = request_snapshot(&command_tx).await?;
            let all_received = (1..=4)
                .map(PocTargetId)
                .all(|id| snapshot.target(id).received_count >= 1);
            if all_received && snapshot.target(PocTargetId(2)).deadline_count >= 1 {
                return Ok::<PocSnapshot, Box<dyn Error>>(snapshot);
            }
            task::yield_now().await;
        }
    })
    .await??;

    // Write readiness was conditional: only the target with prepared bytes
    // polled it, and real kernel acceptance committed exactly once.
    assert_eq!(early_snapshot.target(PocTargetId(1)).write_ready_polls, 0);
    assert_eq!(early_snapshot.target(PocTargetId(2)).write_ready_polls, 0);
    assert!(early_snapshot.target(PocTargetId(3)).write_ready_polls >= 1);
    assert_eq!(early_snapshot.target(PocTargetId(4)).write_ready_polls, 0);
    assert_eq!(early_snapshot.target(PocTargetId(3)).prepared_seq, None);
    assert_eq!(early_snapshot.target(PocTargetId(3)).next_seq, 1);
    assert_eq!(early_snapshot.target(PocTargetId(3)).sent_count, 1);
    assert_eq!(early_snapshot.target(PocTargetId(3)).pending_count, 1);

    let mut sent = [0_u8; 64];
    let sent_len = time::timeout(OUTER_TIMEOUT, peer_three.recv(&mut sent)).await??;
    assert_eq!(&sent[..sent_len], b"prepared-three");

    // Three base deadlines expire together after the target-two progress
    // deadline, forcing the deadline budget to split them across polls.
    let final_snapshot = time::timeout(OUTER_TIMEOUT, async {
        loop {
            let snapshot = request_snapshot(&command_tx).await?;
            let all_base_deadlines = [PocTargetId(1), PocTargetId(3), PocTargetId(4)]
                .into_iter()
                .all(|id| snapshot.target(id).deadline_count >= 1);
            if all_base_deadlines {
                return Ok::<PocSnapshot, Box<dyn Error>>(snapshot);
            }
            task::yield_now().await;
        }
    })
    .await??;
    assert_eq!(final_snapshot.target(PocTargetId(2)).deadline_count, 1);
    time::timeout(OUTER_TIMEOUT, hot_peer).await???;

    let (shutdown_ack, shutdown_received) = oneshot::channel();
    command_tx
        .send(PocCommand::Shutdown { ack: shutdown_ack })
        .await?;
    time::timeout(OUTER_TIMEOUT, shutdown_received).await??;
    let output = time::timeout(OUTER_TIMEOUT, driver_task).await???;

    assert_eq!(output.remaining_targets, 0);
    assert_eq!(drops.targets.load(Ordering::SeqCst), 5);
    assert_eq!(drops.prepared.load(Ordering::SeqCst), 0);
    assert!(output.observations.iter().all(|observation| {
        observation.id != PocTargetId(5)
            && !matches!(
                &observation.kind,
                PocObservationKind::Received(bytes) if bytes == b"after-remove"
            )
    }));
    for id in (1..=4).map(PocTargetId) {
        assert!(output.observations.iter().any(|observation| {
            observation.id == id && matches!(observation.kind, PocObservationKind::Received(_))
        }));
    }
    assert!(output.observations.iter().any(|observation| {
        observation.id == PocTargetId(2)
            && matches!(observation.kind, PocObservationKind::Deadline { .. })
    }));

    assert_driver_budgets_and_cursor(&output.metrics);
    assert!(output
        .metrics
        .cycles
        .iter()
        .any(|cycle| cycle.commands == COMMAND_BUDGET));
    assert!(output
        .metrics
        .cycles
        .iter()
        .any(|cycle| cycle.deadlines == DEADLINE_BUDGET));
    assert!(output
        .metrics
        .cycles
        .iter()
        .any(|cycle| cycle.targets == TARGET_BUDGET));
    assert!(output.metrics.incomplete_sweep_wakes > 0);

    // Timer history shows initial arming, insertion moving the earliest
    // deadline earlier, removal restoring it, receive progress moving it near,
    // and deadline progress restoring the shared base deadline.
    assert!(subsequence(
        &output.metrics.timer_rearms,
        &[
            Some(base_deadline),
            Some(dynamic_deadline),
            Some(base_deadline)
        ]
    ));
    assert!(output
        .metrics
        .timer_rearms
        .iter()
        .any(|deadline| { deadline.is_some_and(|deadline| deadline < dynamic_deadline) }));
    assert_eq!(early_snapshot.armed_deadline, Some(base_deadline));

    Ok(())
}

#[test]
fn transactional_send_keeps_prepared_state_until_acceptance() -> Result<(), Box<dyn Error>> {
    let runtime = Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()?;
    runtime.block_on(async {
        let drops = Arc::new(PocDropCounters::default());
        let (client, _peer) = connected_pair().await?;
        let mut target = PocTarget::new(PocTargetId(1), client, None, drops);
        let initial = target.snapshot();

        target.prepare_send(Box::from(&b"drop-before-send"[..]));
        target.drop_prepared();
        assert_transaction_state_eq(&target.snapshot(), &initial);

        target.prepare_send(Box::from(&b"transactional"[..]));
        target.inject_next_try_send(InjectedTrySend::WouldBlock);
        let blocked =
            time::timeout(OUTER_TIMEOUT, poll_fn(|cx| target.poll_outbound(cx))).await??;
        assert!(blocked.observation.is_none());
        assert_transaction_state_eq(&target.snapshot(), &initial);
        assert_eq!(target.snapshot().prepared_seq, Some(0));

        // The same PreparedProbe survives another readiness poll and another
        // ephemeral preflight commit.
        target.inject_next_try_send(InjectedTrySend::WouldBlock);
        let blocked_again =
            time::timeout(OUTER_TIMEOUT, poll_fn(|cx| target.poll_outbound(cx))).await??;
        assert!(blocked_again.observation.is_none());
        assert_transaction_state_eq(&target.snapshot(), &initial);
        assert_eq!(target.snapshot().prepared_seq, Some(0));

        target.inject_next_try_send(InjectedTrySend::Success);
        let sent = time::timeout(OUTER_TIMEOUT, poll_fn(|cx| target.poll_outbound(cx))).await??;
        assert!(matches!(
            sent.observation,
            Some(PocObservation {
                kind: PocObservationKind::Sent { seq: 0 },
                ..
            })
        ));
        let committed = target.snapshot();
        assert_eq!(committed.prepared_seq, None);
        assert_eq!(committed.next_seq, 1);
        assert_eq!(committed.sent_count, 1);
        assert_eq!(committed.pending_count, 1);

        // ProbeCommit is private, non-Clone, and consumed by commit_sent.
        // With no prepared slot, there is structurally no second commit path.
        let Poll::Ready(no_second_send) =
            target.poll_outbound(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("a target without prepared work cannot be pending");
        };
        let no_second_send = no_second_send?;
        assert!(no_second_send.observation.is_none());
        assert_eq!(target.snapshot(), committed);

        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn receive_would_block_is_a_state_preserving_false_positive() -> Result<(), Box<dyn Error>> {
    let runtime = Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()?;
    runtime.block_on(async {
        let drops = Arc::new(PocDropCounters::default());
        let (client, _peer) = connected_pair().await?;
        let mut target = PocTarget::new(PocTargetId(1), client, None, drops);
        let before = target.snapshot();
        let progress = target.consume_receive(
            Err(io::Error::from(io::ErrorKind::WouldBlock)),
            &[],
            Instant::now(),
        )?;
        assert!(progress.observation.is_none());
        assert_eq!(target.snapshot(), before);
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn readiness_sweep_self_wakes_only_while_incomplete() -> Result<(), Box<dyn Error>> {
    let runtime = Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()?;
    runtime.block_on(async {
        let drops = Arc::new(PocDropCounters::default());
        let (client_one, _peer_one) = connected_pair().await?;
        let (client_two, _peer_two) = connected_pair().await?;
        let (client_three, _peer_three) = connected_pair().await?;
        let targets = vec![
            PocTarget::new(PocTargetId(1), client_one, None, Arc::clone(&drops)),
            PocTarget::new(PocTargetId(2), client_two, None, Arc::clone(&drops)),
            PocTarget::new(PocTargetId(3), client_three, None, Arc::clone(&drops)),
        ];
        let (_command_tx, command_rx) = mpsc::channel(1);
        let mut driver = Box::pin(PocDriver::new(command_rx, targets)?);
        let counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(Arc::clone(&counter));
        let mut cx = Context::from_waker(&waker);

        assert!(driver.as_mut().poll(&mut cx).is_pending());
        assert_eq!(counter.wakes.load(Ordering::SeqCst), 1);
        assert_eq!(driver.as_ref().get_ref().sweep_remaining, 1);

        assert!(driver.as_mut().poll(&mut cx).is_pending());
        assert_eq!(driver.as_ref().get_ref().sweep_remaining, 0);
        let wakes_after_complete = counter.wakes.load(Ordering::SeqCst);
        task::yield_now().await;
        task::yield_now().await;
        assert_eq!(counter.wakes.load(Ordering::SeqCst), wakes_after_complete);
        assert!(!driver.as_ref().get_ref().metrics.cycles[1].self_woke);
        Ok::<(), Box<dyn Error>>(())
    })
}

#[test]
fn dropping_driver_releases_sockets_and_prepared_values() -> Result<(), Box<dyn Error>> {
    let runtime = Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()?;
    let (drops, client_addr) = runtime.block_on(async {
        let drops = Arc::new(PocDropCounters::default());
        let (client, _peer) = connected_pair().await?;
        let client_addr = client.local_addr()?;
        let mut target = PocTarget::new(PocTargetId(1), client, None, Arc::clone(&drops));
        target.prepare_send(Box::from(&b"drop-with-driver"[..]));
        let (_command_tx, command_rx) = mpsc::channel(1);
        let driver = PocDriver::new(command_rx, vec![target])?;
        drop(driver);
        Ok::<_, Box<dyn Error>>((drops, client_addr))
    })?;

    assert_eq!(drops.targets.load(Ordering::SeqCst), 1);
    assert_eq!(drops.prepared.load(Ordering::SeqCst), 1);
    let rebound = std::net::UdpSocket::bind(client_addr)?;
    drop(rebound);
    Ok(())
}

#[test]
fn no_current_runtime_is_a_normal_start_error() {
    let (_command_tx, command_rx) = mpsc::channel(1);
    assert_eq!(
        PocDriver::new(command_rx, Vec::new()).err(),
        Some(PocStartError::NoRuntime)
    );
}

async fn connected_pair() -> io::Result<(UdpSocket, UdpSocket)> {
    let loopback = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));
    let client = UdpSocket::bind(loopback).await?;
    let peer = UdpSocket::bind(loopback).await?;
    client.connect(peer.local_addr()?).await?;
    peer.connect(client.local_addr()?).await?;
    Ok((client, peer))
}

async fn request_snapshot(
    command_tx: &mpsc::Sender<PocCommand>,
) -> Result<PocSnapshot, Box<dyn Error>> {
    let (ack, received) = oneshot::channel();
    command_tx.send(PocCommand::Snapshot { ack }).await?;
    Ok(received.await?)
}

fn assert_transaction_state_eq(actual: &PocTargetSnapshot, expected: &PocTargetSnapshot) {
    assert_eq!(actual.next_seq, expected.next_seq);
    assert_eq!(actual.sent_count, expected.sent_count);
    assert_eq!(actual.pending_count, expected.pending_count);
}

fn assert_driver_budgets_and_cursor(metrics: &PocMetrics) {
    assert!(!metrics.cycles.is_empty());
    assert!(!metrics.visits.is_empty());
    for cycle in &metrics.cycles {
        assert!(cycle.commands <= COMMAND_BUDGET);
        assert!(cycle.deadlines <= DEADLINE_BUDGET);
        assert!(cycle.targets <= TARGET_BUDGET);
        assert!(cycle.sweep_remaining_after <= cycle.target_len_after);
        if cycle.target_len_after == 0 {
            assert_eq!(cycle.cursor_after, 0);
        } else {
            assert!(cycle.cursor_after < cycle.target_len_after);
        }
    }
    for visit in &metrics.visits {
        assert_eq!(
            visit.cursor_after,
            (visit.cursor_before + 1) % visit.target_len,
            "cursor did not advance after visiting {:?}",
            visit.id
        );
    }
}

fn subsequence<T: PartialEq>(haystack: &[T], needle: &[T]) -> bool {
    let mut remaining = needle.iter();
    let mut next = remaining.next();
    for item in haystack {
        if next.is_some_and(|expected| item == expected) {
            next = remaining.next();
        }
    }
    next.is_none()
}
