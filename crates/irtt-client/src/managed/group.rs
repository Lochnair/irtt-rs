use std::{
    collections::{HashMap, HashSet},
    fmt,
    hash::{Hash, Hasher},
    net::{SocketAddr, UdpSocket},
    sync::{mpsc, Arc, Mutex},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crate::{
    config::{ClientAuthConfig, ClientConfig},
    error::ClientError,
    event::{ClientEvent, OpenOutcome},
    receive::recv_datagram_from,
    runtime::{params_from_config, SendProbeResult, SessionRuntime},
    socket::{bind_unconnected_udp_socket, validate_open_timeouts},
    socket_options::apply_dscp_to_socket,
    timing::ClientTimestamp,
};

use super::{
    cancellation::CancellationToken,
    hub::{EventHub, EventSubscription, SubscriberConfig},
};

const GROUP_RECV_TIMEOUT: Duration = Duration::from_millis(20);
const GROUP_FINAL_DRAIN: Duration = Duration::from_millis(100);
const IDLE_SLEEP: Duration = Duration::from_millis(1);
const MAX_SLEEP: Duration = Duration::from_millis(20);
const RECV_BUFFER_SIZE: usize = 65_536;

/// Caller-owned target identity for managed multi-target probing.
#[derive(Clone, Eq)]
pub struct TargetId(Arc<str>);

impl TargetId {
    /// Borrow the identifier as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for TargetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("TargetId").field(&self.0).finish()
    }
}

impl fmt::Display for TargetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl PartialEq for TargetId {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Hash for TargetId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl From<&str> for TargetId {
    fn from(value: &str) -> Self {
        Self(Arc::from(value))
    }
}

impl From<String> for TargetId {
    fn from(value: String) -> Self {
        Self(Arc::from(value))
    }
}

impl AsRef<str> for TargetId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// Per-target configuration for [`ManagedClientGroup`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedTargetConfig {
    /// Caller-owned stable target identity.
    pub id: TargetId,
    /// Resolved UDP remote address. Group v1 does not perform DNS resolution.
    pub remote: SocketAddr,
    /// Optional auth override for this target.
    pub auth: Option<ClientAuthConfig>,
}

/// Group send pacing strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ManagedGroupPacing {
    /// Send active targets one at a time, spaced approximately interval / N.
    #[default]
    Staggered,
    /// Send one probe to every active target back-to-back once per interval.
    Burst,
}

/// Configuration for a managed multi-target client group.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ManagedClientGroupConfig {
    /// Shared client configuration template.
    ///
    /// Most protocol, socket, timing, and negotiation settings are group-wide.
    /// `server_addr` is ignored because targets already carry resolved
    /// [`SocketAddr`] values. `hmac_key` is the default auth unless a target
    /// supplies [`ManagedTargetConfig::auth`].
    pub client: ClientConfig,
    /// Coordinated group pacing mode.
    pub pacing: ManagedGroupPacing,
}

/// Target-scoped managed event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetEvent {
    /// Target that produced the event.
    pub target: TargetId,
    /// Client event produced by that target's session runtime.
    pub event: ClientEvent,
}

/// Subscription type for target-scoped group events.
pub type TargetEventSubscription = EventSubscription<TargetEvent>;

/// Entry point for running a shared-socket multi-target managed client group.
#[derive(Debug)]
pub struct ManagedClientGroup;

/// Running managed client group.
///
/// Dropping the session requests cooperative cancellation. Use
/// [`join`](Self::join) to wait for scheduler and receive threads and obtain
/// the final [`ManagedGroupOutcome`].
#[must_use = "dropping the session cancels the managed client group; call join() to wait for completion"]
#[derive(Debug)]
pub struct ManagedClientGroupSession {
    hub: EventHub<TargetEvent>,
    control_tx: mpsc::Sender<ControlMessage>,
    cancellation: CancellationToken,
    scheduler: Option<JoinHandle<Result<ManagedGroupOutcome, ClientError>>>,
    receiver: Option<JoinHandle<Result<(), ClientError>>>,
}

/// Outcome returned by a completed managed client group.
#[must_use = "managed group outcomes contain completion status and per-target lifecycle counters"]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedGroupOutcome {
    /// Why the group scheduler stopped.
    pub end_reason: ManagedGroupEndReason,
    /// Per-target lifecycle records. These are not RTT/loss summaries.
    pub targets: Vec<ManagedTargetOutcome>,
}

/// Reason the managed group stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedGroupEndReason {
    /// All currently desired targets reached a terminal state.
    AllTargetsComplete,
    /// Cancellation was requested through stop, drop, or a worker failure.
    Cancelled,
}

/// Per-target lifecycle outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedTargetOutcome {
    /// Target identity.
    pub id: TargetId,
    /// Resolved remote address used for the target.
    pub remote: SocketAddr,
    /// Why this target stopped.
    pub end_reason: ManagedTargetEndReason,
    /// Number of echo requests sent to this target.
    pub packets_sent: u64,
    /// Number of first in-window echo replies received from this target.
    pub replies_received: u64,
    /// Number of duplicate reply events emitted for this target.
    pub duplicates: u64,
    /// Number of late reply events emitted for this target.
    pub late: u64,
    /// Number of warning events emitted for this target.
    pub warning_events: u64,
}

/// Reason an individual target stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManagedTargetEndReason {
    /// The negotiated finite test duration completed.
    TestComplete,
    /// Cancellation stopped the target.
    Cancelled,
    /// The target was removed by `update_targets`.
    Removed,
    /// The target completed a no-test open exchange.
    NoTestComplete,
    /// The server closed the target session.
    PeerClosed,
    /// Opening failed before a `ClientEvent` lifecycle event existed.
    OpenFailed { message: String },
    /// Runtime send/receive/close handling failed for this target.
    Failed { message: String },
}

impl ManagedClientGroup {
    /// Start a managed group without creating an initial event subscription.
    pub fn start(
        config: ManagedClientGroupConfig,
        targets: Vec<ManagedTargetConfig>,
    ) -> Result<ManagedClientGroupSession, ClientError> {
        Self::start_inner(config, targets, None).map(|(session, _)| session)
    }

    /// Start a managed group and subscribe before worker threads run.
    pub fn start_with_subscription(
        config: ManagedClientGroupConfig,
        targets: Vec<ManagedTargetConfig>,
        subscriber_config: SubscriberConfig,
    ) -> Result<(ManagedClientGroupSession, TargetEventSubscription), ClientError> {
        let (session, subscription) = Self::start_inner(config, targets, Some(subscriber_config))?;
        Ok((
            session,
            subscription.expect("initial subscription must be present"),
        ))
    }

    fn start_inner(
        mut config: ManagedClientGroupConfig,
        targets: Vec<ManagedTargetConfig>,
        subscriber_config: Option<SubscriberConfig>,
    ) -> Result<(ManagedClientGroupSession, Option<TargetEventSubscription>), ClientError> {
        if targets.is_empty() {
            return Err(ClientError::InvalidConfig {
                reason: "managed client group requires at least one target".to_owned(),
            });
        }
        if config.client.socket_config.recv_timeout.is_none()
            || config.client.socket_config.recv_timeout > Some(GROUP_RECV_TIMEOUT)
        {
            config.client.socket_config.recv_timeout = Some(GROUP_RECV_TIMEOUT);
        }

        validate_open_timeouts(&config.client.open_timeouts)?;
        let requested = params_from_config(&config.client)?;
        let family_remote = validate_target_configs(&targets, None)?;
        validate_group_family_flags(&config.client, family_remote)?;

        let now = Instant::now();
        let mut next_order = 0_u64;
        let mut registry = TargetRegistry::default();
        for target in targets {
            let state = Arc::new(Mutex::new(TargetState::new(
                &config.client,
                target,
                next_order,
                now,
            )?));
            next_order = next_order
                .checked_add(1)
                .ok_or(ClientError::CounterOverflow {
                    counter: "target_order",
                })?;
            let (id, remote) = {
                let target = state.lock().expect("target mutex poisoned");
                (target.id.clone(), target.remote)
            };
            registry.remotes.insert(remote, id.clone());
            registry.targets.insert(id, state);
        }

        let socket = bind_unconnected_udp_socket(&config.client.socket_config, family_remote)?;
        apply_dscp_to_socket(&socket, family_remote, config.client.dscp)?;

        let hub = EventHub::new();
        let initial_subscription = subscriber_config
            .map(|config| hub.subscribe(config))
            .transpose()?;

        let cancellation = CancellationToken::new();
        let (control_tx, control_rx) = mpsc::channel();
        let shared = Arc::new(GroupShared {
            registry: Mutex::new(registry),
            hub: hub.clone(),
            cancellation: cancellation.clone(),
            control_tx: control_tx.clone(),
            requested_interval_ns: requested.interval_ns,
            requested_dscp: requested.dscp,
            family_remote,
        });

        let send_socket = socket;
        let recv_socket = send_socket.try_clone()?;
        let scheduler_shared = shared.clone();
        let scheduler_config = config.clone();
        let scheduler = thread::spawn(move || {
            run_group_scheduler(
                scheduler_config,
                send_socket,
                scheduler_shared,
                control_rx,
                next_order,
            )
        });

        let receiver_shared = shared;
        let receiver = thread::spawn(move || run_group_receiver(recv_socket, receiver_shared));

        Ok((
            ManagedClientGroupSession {
                hub,
                control_tx,
                cancellation,
                scheduler: Some(scheduler),
                receiver: Some(receiver),
            },
            initial_subscription,
        ))
    }
}

impl ManagedClientGroupSession {
    /// Add another target-event subscriber to this running group.
    pub fn subscribe(
        &self,
        config: SubscriberConfig,
    ) -> Result<TargetEventSubscription, ClientError> {
        self.hub.subscribe(config)
    }

    /// Replace the desired target set.
    ///
    /// The update is authoritative. Existing targets with the same
    /// [`TargetId`], remote address, and auth setting remain running. In v1,
    /// changing the remote address or auth for an existing target id is
    /// rejected instead of mutating the active session in place.
    pub fn update_targets(&self, targets: Vec<ManagedTargetConfig>) -> Result<(), ClientError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.control_tx
            .send(ControlMessage::Update {
                targets,
                reply: reply_tx,
            })
            .map_err(|_| ClientError::AlreadyClosed)?;
        reply_rx.recv().unwrap_or(Err(ClientError::AlreadyClosed))
    }

    /// Request cooperative cancellation of the group.
    pub fn stop(&self) {
        self.cancellation.cancel();
        let _ = self.control_tx.send(ControlMessage::Wake);
    }

    /// Wait for the scheduler and receive threads to finish.
    pub fn join(mut self) -> Result<ManagedGroupOutcome, ClientError> {
        let scheduler = self.scheduler.take().expect(
            "ManagedClientGroupSession invariant violated: scheduler handle missing before join",
        );
        let scheduler_result = match scheduler.join() {
            Ok(outcome) => outcome,
            Err(_) => {
                self.cancellation.cancel();
                self.hub.disconnect_all();
                return Err(ClientError::WorkerPanicked);
            }
        };

        self.cancellation.cancel();
        let _ = self.control_tx.send(ControlMessage::Wake);

        let receiver = self.receiver.take().expect(
            "ManagedClientGroupSession invariant violated: receiver handle missing before join",
        );
        let receiver_result = match receiver.join() {
            Ok(outcome) => outcome,
            Err(_) => {
                self.hub.disconnect_all();
                return Err(ClientError::WorkerPanicked);
            }
        };

        self.hub.disconnect_all();
        match (scheduler_result, receiver_result) {
            (Ok(outcome), Ok(())) => Ok(outcome),
            (Err(err), _) => Err(err),
            (Ok(_), Err(err)) => Err(err),
        }
    }
}

impl Drop for ManagedClientGroupSession {
    fn drop(&mut self) {
        self.cancellation.cancel();
        let _ = self.control_tx.send(ControlMessage::Wake);
    }
}

#[derive(Debug)]
enum ControlMessage {
    Update {
        targets: Vec<ManagedTargetConfig>,
        reply: mpsc::Sender<Result<(), ClientError>>,
    },
    Wake,
}

#[derive(Debug)]
struct GroupShared {
    registry: Mutex<TargetRegistry>,
    hub: EventHub<TargetEvent>,
    cancellation: CancellationToken,
    control_tx: mpsc::Sender<ControlMessage>,
    requested_interval_ns: i64,
    requested_dscp: i64,
    family_remote: SocketAddr,
}

#[derive(Debug, Default)]
struct TargetRegistry {
    targets: HashMap<TargetId, Arc<Mutex<TargetState>>>,
    remotes: HashMap<SocketAddr, TargetId>,
}

#[derive(Debug)]
struct TargetState {
    id: TargetId,
    remote: SocketAddr,
    configured_auth: Option<ClientAuthConfig>,
    runtime: SessionRuntime,
    status: TargetStatus,
    open_packet: Vec<u8>,
    counters: TargetCounters,
    order: u64,
    final_reason: Option<ManagedTargetEndReason>,
}

#[derive(Debug)]
enum TargetStatus {
    Opening {
        attempt: usize,
        next_send_at: Instant,
    },
    Active,
    Draining {
        deadline: Instant,
    },
    Finished,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct TargetCounters {
    replies_received: u64,
    duplicates: u64,
    late: u64,
    warning_events: u64,
}

impl TargetState {
    fn new(
        group_config: &ClientConfig,
        target: ManagedTargetConfig,
        order: u64,
        now: Instant,
    ) -> Result<Self, ClientError> {
        let mut config = group_config.clone();
        if let Some(auth) = &target.auth {
            config.hmac_key = auth.hmac_key.clone();
        }
        let runtime = SessionRuntime::new(config, target.remote)?;
        let open_packet = runtime.open_packet()?;
        Ok(Self {
            id: target.id,
            remote: target.remote,
            configured_auth: target.auth,
            runtime,
            status: TargetStatus::Opening {
                attempt: 0,
                next_send_at: now,
            },
            open_packet,
            counters: TargetCounters::default(),
            order,
            final_reason: None,
        })
    }

    fn same_config(&self, config: &ManagedTargetConfig) -> bool {
        self.remote == config.remote && self.configured_auth == config.auth
    }

    fn observe(&mut self, event: &ClientEvent) {
        match event {
            ClientEvent::EchoReply { .. } => self.counters.replies_received += 1,
            ClientEvent::DuplicateReply { .. } => self.counters.duplicates += 1,
            ClientEvent::LateReply { .. } => self.counters.late += 1,
            ClientEvent::Warning { .. } => self.counters.warning_events += 1,
            _ => {}
        }
    }

    fn mark_finished(&mut self, reason: ManagedTargetEndReason) {
        self.status = TargetStatus::Finished;
        self.final_reason = Some(reason);
    }

    fn outcome(&self, reason: ManagedTargetEndReason) -> ManagedTargetOutcome {
        ManagedTargetOutcome {
            id: self.id.clone(),
            remote: self.remote,
            end_reason: reason,
            packets_sent: self.runtime.packets_sent(),
            replies_received: self.counters.replies_received,
            duplicates: self.counters.duplicates,
            late: self.counters.late,
            warning_events: self.counters.warning_events,
        }
    }
}

fn run_group_scheduler(
    config: ManagedClientGroupConfig,
    socket: UdpSocket,
    shared: Arc<GroupShared>,
    control_rx: mpsc::Receiver<ControlMessage>,
    mut next_order: u64,
) -> Result<ManagedGroupOutcome, ClientError> {
    let mut records = Vec::new();
    let mut pacing = PacingRuntime::new(config.pacing);
    let mut pending_control = None;

    loop {
        drain_control_messages(
            &config.client,
            &socket,
            &shared,
            &control_rx,
            pending_control.take(),
            &mut next_order,
            &mut records,
        );

        if shared.cancellation.is_cancelled() {
            cancel_remaining_targets(&socket, &shared, &mut records);
            return Ok(ManagedGroupOutcome {
                end_reason: ManagedGroupEndReason::Cancelled,
                targets: records,
            });
        }

        let now = Instant::now();
        drive_open_attempts(&config.client, &socket, &shared, now);
        poll_active_timeouts(&shared, now);
        finish_completed_targets(&socket, &shared, now);
        collect_finished_targets(&shared, &mut records);

        if registry_is_empty(&shared) {
            return Ok(ManagedGroupOutcome {
                end_reason: ManagedGroupEndReason::AllTargetsComplete,
                targets: records,
            });
        }

        let active = active_targets(&shared);
        pacing.reconcile(&active, now);
        if pacing.send_due(&active, now, config.client.interval) {
            match config.pacing {
                ManagedGroupPacing::Staggered => {
                    if let Some((target, scheduled_at)) =
                        pacing.next_staggered(&active, config.client.interval)
                    {
                        send_echo_to_target(&socket, &shared, target, scheduled_at);
                    }
                }
                ManagedGroupPacing::Burst => {
                    if let Some(scheduled_at) = pacing.next_burst(config.client.interval) {
                        for target in active {
                            send_echo_to_target(&socket, &shared, target, scheduled_at);
                        }
                    }
                }
            }
            continue;
        }

        pending_control = wait_for_next_scheduler_wakeup(&control_rx, &shared, &pacing, &active);
    }
}

fn run_group_receiver(socket: UdpSocket, shared: Arc<GroupShared>) -> Result<(), ClientError> {
    let mut buf = vec![0_u8; RECV_BUFFER_SIZE];
    while !shared.cancellation.is_cancelled() {
        let datagram = match recv_datagram_from(&socket, &mut buf) {
            Ok(datagram) => datagram,
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                continue;
            }
            Err(err) => {
                shared.cancellation.cancel();
                let _ = shared.control_tx.send(ControlMessage::Wake);
                return Err(ClientError::Socket(err));
            }
        };

        let target = {
            let registry = shared
                .registry
                .lock()
                .expect("target registry mutex poisoned");
            let Some(id) = registry.remotes.get(&datagram.source) else {
                continue;
            };
            registry.targets.get(id).cloned()
        };
        let Some(target) = target else {
            continue;
        };

        let packet = &buf[..datagram.len];
        let mut wake_scheduler = false;
        {
            let mut target = target.lock().expect("target mutex poisoned");
            match target.status {
                TargetStatus::Opening { .. } => {
                    let result = target.runtime.decode_open_reply(packet).and_then(|reply| {
                        target.runtime.accept_open_reply(
                            reply,
                            datagram.received_at,
                            |negotiated| validate_group_negotiation(&shared, negotiated),
                        )
                    });

                    match result {
                        Ok(outcome) => {
                            publish_open_outcome(&shared.hub, &mut target, outcome);
                            wake_scheduler = true;
                        }
                        Err(err) => {
                            target.mark_finished(ManagedTargetEndReason::OpenFailed {
                                message: err.to_string(),
                            });
                            wake_scheduler = true;
                        }
                    }
                }
                TargetStatus::Active | TargetStatus::Draining { .. } => {
                    match target.runtime.process_received_echo_packet(
                        packet,
                        datagram.received_at,
                        datagram.meta,
                    ) {
                        Ok(events) => {
                            publish_events(&shared.hub, &mut target, events);
                            if target.runtime.is_peer_closed() {
                                wake_scheduler = true;
                            }
                        }
                        Err(err) => {
                            target.mark_finished(ManagedTargetEndReason::Failed {
                                message: err.to_string(),
                            });
                            wake_scheduler = true;
                        }
                    }
                }
                TargetStatus::Finished => {}
            }
        }

        if wake_scheduler {
            let _ = shared.control_tx.send(ControlMessage::Wake);
        }
    }
    Ok(())
}

fn validate_group_negotiation(
    shared: &GroupShared,
    negotiated: &crate::NegotiatedParams,
) -> Result<(), ClientError> {
    if negotiated.params.interval_ns != shared.requested_interval_ns {
        return Err(ClientError::NegotiationRejected {
            reason: "managed group v1 requires the negotiated interval to match the group interval"
                .to_owned(),
        });
    }
    if negotiated.params.dscp != shared.requested_dscp {
        return Err(ClientError::NegotiationRejected {
            reason: "managed group v1 requires the negotiated DSCP to match the group DSCP"
                .to_owned(),
        });
    }
    Ok(())
}

fn publish_open_outcome(
    hub: &EventHub<TargetEvent>,
    target: &mut TargetState,
    outcome: OpenOutcome,
) {
    match outcome {
        OpenOutcome::Started { event, .. } => {
            target.status = TargetStatus::Active;
            hub.publish(TargetEvent {
                target: target.id.clone(),
                event,
            });
        }
        OpenOutcome::NoTestCompleted { event, .. } => {
            target.observe(&event);
            hub.publish(TargetEvent {
                target: target.id.clone(),
                event,
            });
            target.mark_finished(ManagedTargetEndReason::NoTestComplete);
        }
    }
}

fn publish_events(hub: &EventHub<TargetEvent>, target: &mut TargetState, events: Vec<ClientEvent>) {
    for event in events {
        target.observe(&event);
        hub.publish(TargetEvent {
            target: target.id.clone(),
            event,
        });
    }
}

fn drain_control_messages(
    config: &ClientConfig,
    socket: &UdpSocket,
    shared: &GroupShared,
    control_rx: &mpsc::Receiver<ControlMessage>,
    first_message: Option<ControlMessage>,
    next_order: &mut u64,
    records: &mut Vec<ManagedTargetOutcome>,
) {
    if let Some(message) = first_message {
        handle_control_message(config, socket, shared, next_order, records, message);
    }
    while let Ok(message) = control_rx.try_recv() {
        handle_control_message(config, socket, shared, next_order, records, message);
    }
}

fn handle_control_message(
    config: &ClientConfig,
    socket: &UdpSocket,
    shared: &GroupShared,
    next_order: &mut u64,
    records: &mut Vec<ManagedTargetOutcome>,
    message: ControlMessage,
) {
    match message {
        ControlMessage::Update { targets, reply } => {
            let result = apply_target_update(config, socket, shared, targets, next_order, records);
            let _ = reply.send(result);
        }
        ControlMessage::Wake => {}
    }
}

fn apply_target_update(
    config: &ClientConfig,
    socket: &UdpSocket,
    shared: &GroupShared,
    targets: Vec<ManagedTargetConfig>,
    next_order: &mut u64,
    records: &mut Vec<ManagedTargetOutcome>,
) -> Result<(), ClientError> {
    validate_open_timeouts(&config.open_timeouts)?;
    if targets.is_empty() {
        remove_all_targets(socket, shared, records, ManagedTargetEndReason::Removed);
        return Ok(());
    }
    validate_target_configs(&targets, Some(shared.family_remote))?;

    let desired_ids: HashSet<TargetId> = targets.iter().map(|target| target.id.clone()).collect();
    let mut additions = Vec::new();

    {
        let registry = shared
            .registry
            .lock()
            .expect("target registry mutex poisoned");
        for target in &targets {
            if let Some(existing) = registry.targets.get(&target.id) {
                let existing = existing.lock().expect("target mutex poisoned");
                if !existing.same_config(target) {
                    return Err(ClientError::InvalidConfig {
                        reason: format!(
                            "managed group v1 rejects changed remote/auth for existing target {}",
                            target.id
                        ),
                    });
                }
            } else {
                additions.push(target.clone());
            }
        }
    }

    let mut prepared = Vec::new();
    let now = Instant::now();
    for target in additions {
        let order = *next_order;
        *next_order = next_order
            .checked_add(1)
            .ok_or(ClientError::CounterOverflow {
                counter: "target_order",
            })?;
        prepared.push(Arc::new(Mutex::new(TargetState::new(
            config, target, order, now,
        )?)));
    }

    let removed = {
        let mut registry = shared
            .registry
            .lock()
            .expect("target registry mutex poisoned");
        let removed_ids: Vec<TargetId> = registry
            .targets
            .keys()
            .filter(|id| !desired_ids.contains(*id))
            .cloned()
            .collect();
        let mut removed = Vec::with_capacity(removed_ids.len());
        for id in removed_ids {
            if let Some(target) = registry.targets.remove(&id) {
                let remote = target.lock().expect("target mutex poisoned").remote;
                registry.remotes.remove(&remote);
                removed.push(target);
            }
        }
        for target in prepared {
            let (id, remote) = {
                let target = target.lock().expect("target mutex poisoned");
                (target.id.clone(), target.remote)
            };
            registry.remotes.insert(remote, id.clone());
            registry.targets.insert(id, target);
        }
        removed
    };

    for target in removed {
        records.push(close_target(
            socket,
            &shared.hub,
            target,
            ManagedTargetEndReason::Removed,
        ));
    }

    Ok(())
}

fn validate_target_configs(
    targets: &[ManagedTargetConfig],
    expected_family: Option<SocketAddr>,
) -> Result<SocketAddr, ClientError> {
    let mut ids = HashSet::new();
    let mut remotes = HashSet::new();
    let mut family_remote = expected_family;

    for target in targets {
        if !ids.insert(target.id.clone()) {
            return Err(ClientError::InvalidConfig {
                reason: format!("duplicate managed target id {}", target.id),
            });
        }
        if !remotes.insert(target.remote) {
            return Err(ClientError::InvalidConfig {
                reason: format!("duplicate managed target remote {}", target.remote),
            });
        }
        if let Some(family) = family_remote {
            if family.is_ipv4() != target.remote.is_ipv4() {
                return Err(ClientError::InvalidConfig {
                    reason: "managed client group targets must use one address family".to_owned(),
                });
            }
        } else {
            family_remote = Some(target.remote);
        }
    }

    family_remote.ok_or_else(|| ClientError::InvalidConfig {
        reason: "managed client group requires at least one target".to_owned(),
    })
}

fn validate_group_family_flags(
    config: &ClientConfig,
    remote: SocketAddr,
) -> Result<(), ClientError> {
    if config.socket_config.ipv4_only && config.socket_config.ipv6_only {
        return Err(ClientError::InvalidConfig {
            reason: "ipv4_only and ipv6_only cannot both be true".to_owned(),
        });
    }
    if config.socket_config.ipv4_only && remote.is_ipv6() {
        return Err(ClientError::InvalidConfig {
            reason: "ipv4_only cannot be used with IPv6 group targets".to_owned(),
        });
    }
    if config.socket_config.ipv6_only && remote.is_ipv4() {
        return Err(ClientError::InvalidConfig {
            reason: "ipv6_only cannot be used with IPv4 group targets".to_owned(),
        });
    }
    Ok(())
}

fn drive_open_attempts(
    config: &ClientConfig,
    socket: &UdpSocket,
    shared: &GroupShared,
    now: Instant,
) {
    for target in all_targets(shared) {
        let mut target = target.lock().expect("target mutex poisoned");
        let TargetStatus::Opening {
            attempt,
            next_send_at,
        } = target.status
        else {
            continue;
        };

        if next_send_at > now {
            continue;
        }

        if attempt >= config.open_timeouts.len() {
            target.mark_finished(ManagedTargetEndReason::OpenFailed {
                message: ClientError::OpenTimeout.to_string(),
            });
            continue;
        }

        match socket.send_to(&target.open_packet, target.remote) {
            Ok(_) => {
                target.status = TargetStatus::Opening {
                    attempt: attempt + 1,
                    next_send_at: now + config.open_timeouts[attempt],
                };
            }
            Err(err) => {
                target.mark_finished(ManagedTargetEndReason::Failed {
                    message: ClientError::Socket(err).to_string(),
                });
            }
        }
    }
}

fn poll_active_timeouts(shared: &GroupShared, now: Instant) {
    for target in all_targets(shared) {
        let mut target = target.lock().expect("target mutex poisoned");
        if !matches!(
            target.status,
            TargetStatus::Active | TargetStatus::Draining { .. }
        ) {
            continue;
        }
        match target.runtime.poll_timeouts_at(now) {
            Ok(events) => publish_events(&shared.hub, &mut target, events),
            Err(err) => target.mark_finished(ManagedTargetEndReason::Failed {
                message: err.to_string(),
            }),
        }
    }
}

fn finish_completed_targets(socket: &UdpSocket, shared: &GroupShared, now: Instant) {
    for target in all_targets(shared) {
        let mut target = target.lock().expect("target mutex poisoned");
        match target.status {
            TargetStatus::Active if target.runtime.is_run_complete() => {
                if target.runtime.is_peer_closed() {
                    target.mark_finished(ManagedTargetEndReason::PeerClosed);
                } else if target.runtime.has_timed_out_metadata() {
                    target.status = TargetStatus::Draining {
                        deadline: now + GROUP_FINAL_DRAIN,
                    };
                } else {
                    close_locked_target(
                        socket,
                        &shared.hub,
                        &mut target,
                        ManagedTargetEndReason::TestComplete,
                    );
                }
            }
            TargetStatus::Draining { deadline } => {
                if target.runtime.is_peer_closed() {
                    target.mark_finished(ManagedTargetEndReason::PeerClosed);
                } else if now >= deadline || !target.runtime.has_timed_out_metadata() {
                    close_locked_target(
                        socket,
                        &shared.hub,
                        &mut target,
                        ManagedTargetEndReason::TestComplete,
                    );
                }
            }
            _ => {}
        }
    }
}

fn send_echo_to_target(
    socket: &UdpSocket,
    shared: &GroupShared,
    target: Arc<Mutex<TargetState>>,
    scheduled_at: Instant,
) {
    let mut target = target.lock().expect("target mutex poisoned");
    if !matches!(target.status, TargetStatus::Active) {
        return;
    }
    let remote = target.remote;
    let result = target
        .runtime
        .send_probe_for_deadline(scheduled_at, |packet| {
            let sent_at = ClientTimestamp::now();
            let send_call_start = Instant::now();
            let bytes = socket.send_to(packet, remote)?;
            let send_call = send_call_start.elapsed();
            Ok(SendProbeResult {
                sent_at,
                bytes,
                send_call,
            })
        });

    match result {
        Ok(events) => {
            // Publish while the target lock is still held. EventHub publish is
            // bounded and nonblocking, and this preserves per-target
            // EchoSent-before-EchoReply ordering for immediately returned UDP
            // replies.
            publish_events(&shared.hub, &mut target, events);
        }
        Err(err) => target.mark_finished(ManagedTargetEndReason::Failed {
            message: err.to_string(),
        }),
    }
}

fn collect_finished_targets(shared: &GroupShared, records: &mut Vec<ManagedTargetOutcome>) {
    let finished = {
        let mut registry = shared
            .registry
            .lock()
            .expect("target registry mutex poisoned");
        let ids: Vec<TargetId> = registry
            .targets
            .iter()
            .filter_map(|(id, target)| {
                let target = target.lock().expect("target mutex poisoned");
                matches!(target.status, TargetStatus::Finished).then(|| id.clone())
            })
            .collect();
        let mut finished = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(target) = registry.targets.remove(&id) {
                let remote = target.lock().expect("target mutex poisoned").remote;
                registry.remotes.remove(&remote);
                finished.push(target);
            }
        }
        finished
    };

    for target in finished {
        let target = target.lock().expect("target mutex poisoned");
        let reason =
            target
                .final_reason
                .clone()
                .unwrap_or_else(|| ManagedTargetEndReason::Failed {
                    message: "target finished without a reason".to_owned(),
                });
        records.push(target.outcome(reason));
    }
}

fn cancel_remaining_targets(
    socket: &UdpSocket,
    shared: &GroupShared,
    records: &mut Vec<ManagedTargetOutcome>,
) {
    let targets = drain_registry(shared);
    for target in targets {
        records.push(close_target(
            socket,
            &shared.hub,
            target,
            ManagedTargetEndReason::Cancelled,
        ));
    }
}

fn remove_all_targets(
    socket: &UdpSocket,
    shared: &GroupShared,
    records: &mut Vec<ManagedTargetOutcome>,
    reason: ManagedTargetEndReason,
) {
    let targets = drain_registry(shared);
    for target in targets {
        records.push(close_target(socket, &shared.hub, target, reason.clone()));
    }
}

fn drain_registry(shared: &GroupShared) -> Vec<Arc<Mutex<TargetState>>> {
    let mut registry = shared
        .registry
        .lock()
        .expect("target registry mutex poisoned");
    registry.remotes.clear();
    registry.targets.drain().map(|(_, target)| target).collect()
}

fn close_target(
    socket: &UdpSocket,
    hub: &EventHub<TargetEvent>,
    target: Arc<Mutex<TargetState>>,
    reason: ManagedTargetEndReason,
) -> ManagedTargetOutcome {
    let mut target = target.lock().expect("target mutex poisoned");
    close_locked_target(socket, hub, &mut target, reason.clone());
    let reason = target.final_reason.clone().unwrap_or(reason);
    target.outcome(reason)
}

fn close_locked_target(
    socket: &UdpSocket,
    hub: &EventHub<TargetEvent>,
    target: &mut TargetState,
    reason: ManagedTargetEndReason,
) {
    if target.runtime.is_open() && !target.runtime.is_peer_closed() {
        let remote = target.remote;
        match target.runtime.close_with(|packet| {
            socket.send_to(packet, remote)?;
            Ok(())
        }) {
            Ok(events) => publish_events(hub, target, events),
            Err(err) => {
                target.mark_finished(ManagedTargetEndReason::Failed {
                    message: err.to_string(),
                });
                return;
            }
        }
    }
    target.mark_finished(reason);
}

fn active_targets(shared: &GroupShared) -> Vec<Arc<Mutex<TargetState>>> {
    let mut targets: Vec<_> = all_targets(shared)
        .into_iter()
        .filter(|target| {
            let target = target.lock().expect("target mutex poisoned");
            matches!(target.status, TargetStatus::Active)
        })
        .collect();
    targets.sort_by_key(|target| target.lock().expect("target mutex poisoned").order);
    targets
}

fn all_targets(shared: &GroupShared) -> Vec<Arc<Mutex<TargetState>>> {
    shared
        .registry
        .lock()
        .expect("target registry mutex poisoned")
        .targets
        .values()
        .cloned()
        .collect()
}

fn registry_is_empty(shared: &GroupShared) -> bool {
    shared
        .registry
        .lock()
        .expect("target registry mutex poisoned")
        .targets
        .is_empty()
}

#[derive(Debug)]
struct PacingRuntime {
    mode: ManagedGroupPacing,
    signature: Vec<TargetId>,
    next_slot_at: Option<Instant>,
    slot_index: usize,
    next_burst_at: Option<Instant>,
}

impl PacingRuntime {
    fn new(mode: ManagedGroupPacing) -> Self {
        Self {
            mode,
            signature: Vec::new(),
            next_slot_at: None,
            slot_index: 0,
            next_burst_at: None,
        }
    }

    fn reconcile(&mut self, active: &[Arc<Mutex<TargetState>>], now: Instant) {
        let signature: Vec<TargetId> = active
            .iter()
            .map(|target| target.lock().expect("target mutex poisoned").id.clone())
            .collect();
        if signature == self.signature {
            return;
        }
        self.signature = signature;
        self.slot_index = 0;
        if active.is_empty() {
            self.next_slot_at = None;
            self.next_burst_at = None;
        } else {
            match self.mode {
                ManagedGroupPacing::Staggered => self.next_slot_at = Some(now),
                ManagedGroupPacing::Burst => {
                    self.next_burst_at.get_or_insert(now);
                }
            }
        }
    }

    fn send_due(
        &self,
        active: &[Arc<Mutex<TargetState>>],
        now: Instant,
        _interval: Duration,
    ) -> bool {
        if active.is_empty() {
            return false;
        }
        match self.mode {
            ManagedGroupPacing::Staggered => {
                self.next_slot_at.is_some_and(|deadline| deadline <= now)
            }
            ManagedGroupPacing::Burst => self.next_burst_at.is_some_and(|deadline| deadline <= now),
        }
    }

    fn next_staggered(
        &mut self,
        active: &[Arc<Mutex<TargetState>>],
        interval: Duration,
    ) -> Option<(Arc<Mutex<TargetState>>, Instant)> {
        let scheduled_at = self.next_slot_at?;
        if active.is_empty() {
            self.next_slot_at = None;
            return None;
        }
        let target = active[self.slot_index % active.len()].clone();
        self.slot_index = (self.slot_index + 1) % active.len();
        self.next_slot_at = Some(scheduled_at + divide_duration(interval, active.len()));
        Some((target, scheduled_at))
    }

    fn next_burst(&mut self, interval: Duration) -> Option<Instant> {
        let scheduled_at = self.next_burst_at?;
        self.next_burst_at = Some(scheduled_at + interval);
        Some(scheduled_at)
    }

    fn next_wakeup(&self) -> Option<Instant> {
        match self.mode {
            ManagedGroupPacing::Staggered => self.next_slot_at,
            ManagedGroupPacing::Burst => self.next_burst_at,
        }
    }
}

fn wait_for_next_scheduler_wakeup(
    control_rx: &mpsc::Receiver<ControlMessage>,
    shared: &GroupShared,
    pacing: &PacingRuntime,
    active: &[Arc<Mutex<TargetState>>],
) -> Option<ControlMessage> {
    let open_deadline = next_open_deadline(shared);
    let pacing_deadline = (!active.is_empty()).then(|| pacing.next_wakeup()).flatten();
    let deadline = [open_deadline, pacing_deadline].into_iter().flatten().min();
    let sleep_for = deadline
        .and_then(|deadline| deadline.checked_duration_since(Instant::now()))
        .map(|duration| duration.min(MAX_SLEEP))
        .unwrap_or(IDLE_SLEEP);

    match control_rx.recv_timeout(sleep_for) {
        Ok(message) => Some(message),
        Err(mpsc::RecvTimeoutError::Timeout) => None,
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            shared.cancellation.cancel();
            None
        }
    }
}

fn next_open_deadline(shared: &GroupShared) -> Option<Instant> {
    all_targets(shared)
        .into_iter()
        .filter_map(|target| {
            let target = target.lock().expect("target mutex poisoned");
            match target.status {
                TargetStatus::Opening { next_send_at, .. } => Some(next_send_at),
                _ => None,
            }
        })
        .min()
}

fn divide_duration(duration: Duration, divisor: usize) -> Duration {
    if divisor == 0 {
        return duration;
    }
    let nanos = (duration.as_nanos() / divisor as u128).max(1);
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::NegotiationPolicy, managed::SubscriberOverflow, EventSubscriptionError, WarningKind,
    };
    use irtt_proto::{
        echo_packet_len,
        flags::{self, FLAG_OPEN, FLAG_REPLY},
        layout::PacketLayout,
        Clock, Params, ReceivedStats, StampAt, TimestampFields, MAGIC, PROTOCOL_VERSION,
    };
    use std::sync::mpsc;

    const TOKEN: u64 = 0x1234_5678_90ab_cdef;

    #[derive(Debug)]
    struct FakeServer {
        addr: SocketAddr,
        _observations: mpsc::Receiver<ServerObservation>,
        done: JoinHandle<()>,
    }

    impl FakeServer {
        fn join(self) {
            self.done.join().unwrap();
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ServerObservation {
        Open { at: Instant },
        Echo { seq: u32, at: Instant },
        Close { at: Instant },
    }

    fn test_params(duration: Option<Duration>, interval: Duration) -> Params {
        Params {
            protocol_version: PROTOCOL_VERSION,
            duration_ns: duration.map_or(0, duration_ns_i64),
            interval_ns: duration_ns_i64(interval),
            length: 0,
            received_stats: ReceivedStats::Both,
            stamp_at: StampAt::Both,
            clock: Clock::Both,
            dscp: 0,
            server_fill: None,
        }
    }

    fn duration_ns_i64(duration: Duration) -> i64 {
        i64::try_from(duration.as_nanos()).expect("test duration fits i64 nanoseconds")
    }

    fn group_config(
        duration: Option<Duration>,
        interval: Duration,
        pacing: ManagedGroupPacing,
    ) -> ManagedClientGroupConfig {
        ManagedClientGroupConfig {
            client: ClientConfig {
                server_addr: "127.0.0.1:1".to_owned(),
                duration,
                interval,
                negotiation_policy: NegotiationPolicy::Strict,
                open_timeouts: vec![Duration::from_millis(200)],
                probe_timeout: Duration::from_millis(80),
                socket_config: crate::SocketConfig {
                    recv_timeout: Some(Duration::from_millis(20)),
                    ..Default::default()
                },
                ..ClientConfig::default()
            },
            pacing,
        }
    }

    fn target(id: &str, remote: SocketAddr) -> ManagedTargetConfig {
        ManagedTargetConfig {
            id: TargetId::from(id),
            remote,
            auth: None,
        }
    }

    fn start_echo_server(params: Params, open_delay: Duration) -> FakeServer {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = socket.local_addr().unwrap();
        let (tx, rx) = mpsc::channel();
        let done = thread::spawn(move || {
            socket
                .set_read_timeout(Some(Duration::from_millis(800)))
                .unwrap();
            let mut opened = false;
            while let Some((packet, peer)) = recv_request_timeout(&socket) {
                if packet[3] & FLAG_OPEN != 0 {
                    if !opened {
                        opened = true;
                        tx.send(ServerObservation::Open { at: Instant::now() })
                            .unwrap();
                        if open_delay > Duration::ZERO {
                            thread::sleep(open_delay);
                        }
                    }
                    socket
                        .send_to(&open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params), peer)
                        .unwrap();
                    continue;
                }

                if packet[3] & flags::FLAG_CLOSE != 0 {
                    tx.send(ServerObservation::Close { at: Instant::now() })
                        .unwrap();
                    break;
                }

                let seq = u32::from_le_bytes(packet[12..16].try_into().unwrap());
                tx.send(ServerObservation::Echo {
                    seq,
                    at: Instant::now(),
                })
                .unwrap();
                let ts = TimestampFields {
                    recv_wall: Some(1_000_000_000),
                    recv_mono: Some(100_000),
                    send_wall: Some(1_000_000_000),
                    send_mono: Some(100_000),
                    ..Default::default()
                };
                socket
                    .send_to(&echo_reply_packet(TOKEN, seq, &params, &ts), peer)
                    .unwrap();
            }
        });
        FakeServer {
            addr,
            _observations: rx,
            done,
        }
    }

    fn recv_request_timeout(socket: &UdpSocket) -> Option<(Vec<u8>, SocketAddr)> {
        let mut buf = [0_u8; 2048];
        socket
            .recv_from(&mut buf)
            .ok()
            .map(|(size, peer)| (buf[..size].to_vec(), peer))
    }

    fn open_reply(flags: u8, token: u64, params: &Params) -> Vec<u8> {
        let mut packet = Vec::new();
        packet.extend_from_slice(&MAGIC);
        packet.push(flags);
        packet.extend_from_slice(&token.to_le_bytes());
        packet.extend_from_slice(&params.encode());
        packet
    }

    fn echo_reply_packet(
        token: u64,
        seq: u32,
        params: &Params,
        timestamps: &TimestampFields,
    ) -> Vec<u8> {
        let layout = PacketLayout::echo(false, params);
        let packet_len = echo_packet_len(false, params).unwrap();
        let mut packet = Vec::with_capacity(packet_len);

        packet.extend_from_slice(&MAGIC);
        packet.push(FLAG_REPLY);
        packet.extend_from_slice(&token.to_le_bytes());
        packet.extend_from_slice(&seq.to_le_bytes());

        if layout.recv_count {
            packet.extend_from_slice(&42_u32.to_le_bytes());
        }
        if layout.recv_window {
            packet.extend_from_slice(&0x07_u64.to_le_bytes());
        }
        if layout.recv_wall {
            packet.extend_from_slice(&timestamps.recv_wall.unwrap_or(0).to_le_bytes());
        }
        if layout.recv_mono {
            packet.extend_from_slice(&timestamps.recv_mono.unwrap_or(0).to_le_bytes());
        }
        if layout.midpoint_wall {
            packet.extend_from_slice(&timestamps.midpoint_wall.unwrap_or(0).to_le_bytes());
        }
        if layout.midpoint_mono {
            packet.extend_from_slice(&timestamps.midpoint_mono.unwrap_or(0).to_le_bytes());
        }
        if layout.send_wall {
            packet.extend_from_slice(&timestamps.send_wall.unwrap_or(0).to_le_bytes());
        }
        if layout.send_mono {
            packet.extend_from_slice(&timestamps.send_mono.unwrap_or(0).to_le_bytes());
        }

        packet.resize(packet_len, 0);
        packet
    }

    fn recv_event_with_timeout(sub: &TargetEventSubscription) -> TargetEvent {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match sub.try_recv() {
                Ok(Some(event)) => return event,
                Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(1)),
                Ok(None) => panic!("timed out waiting for group event"),
                Err(err) => panic!("subscription ended while waiting for event: {err}"),
            }
        }
    }

    fn drain_after_join(sub: &TargetEventSubscription) -> Vec<TargetEvent> {
        let mut events = Vec::new();
        loop {
            match sub.try_recv() {
                Ok(Some(event)) => events.push(event),
                Ok(None) => thread::sleep(Duration::from_millis(1)),
                Err(EventSubscriptionError::Disconnected) => return events,
            }
        }
    }

    fn sent_events_after_start(events: &[TargetEvent]) -> Vec<(&TargetId, Instant)> {
        let both_started_at = events
            .iter()
            .filter_map(|event| match &event.event {
                ClientEvent::SessionStarted { at, .. } => Some(at.mono),
                _ => None,
            })
            .max()
            .expect("expected at least one SessionStarted event");

        let mut sent: Vec<_> = events
            .iter()
            .filter_map(|event| match &event.event {
                ClientEvent::EchoSent { sent_at, .. } if sent_at.mono >= both_started_at => {
                    Some((&event.target, sent_at.mono))
                }
                _ => None,
            })
            .collect();
        sent.sort_by_key(|(_, at)| *at);
        sent
    }

    #[test]
    fn target_event_hub_uses_generic_overflow_behavior() {
        let hub = EventHub::<TargetEvent>::new();
        let sub = hub
            .subscribe(SubscriberConfig {
                capacity: 1,
                overflow: SubscriberOverflow::DropOldest,
            })
            .unwrap();

        hub.publish(TargetEvent {
            target: TargetId::from("a"),
            event: ClientEvent::Warning {
                kind: WarningKind::UntrackedReply,
                message: "old".to_owned(),
                at: ClientTimestamp::now(),
            },
        });
        hub.publish(TargetEvent {
            target: TargetId::from("b"),
            event: ClientEvent::Warning {
                kind: WarningKind::UntrackedReply,
                message: "new".to_owned(),
                at: ClientTimestamp::now(),
            },
        });

        let event = sub.try_recv().unwrap().unwrap();
        assert_eq!(event.target, TargetId::from("b"));
        assert!(sub.try_recv().unwrap().is_none());
    }

    #[test]
    fn rejects_duplicate_resolved_remote_targets() {
        let remote: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        let err = ManagedClientGroup::start(
            group_config(
                Some(Duration::from_millis(10)),
                Duration::from_millis(10),
                ManagedGroupPacing::Staggered,
            ),
            vec![target("a", remote), target("b", remote)],
        )
        .unwrap_err();

        assert!(matches!(err, ClientError::InvalidConfig { .. }));
        assert!(err.to_string().contains("duplicate managed target remote"));
    }

    #[test]
    fn rejects_mixed_address_families() {
        let err = ManagedClientGroup::start(
            group_config(
                Some(Duration::from_millis(10)),
                Duration::from_millis(10),
                ManagedGroupPacing::Staggered,
            ),
            vec![
                target("a", "127.0.0.1:12345".parse().unwrap()),
                target("b", "[::1]:12345".parse().unwrap()),
            ],
        )
        .unwrap_err();

        assert!(matches!(err, ClientError::InvalidConfig { .. }));
        assert!(err.to_string().contains("one address family"));
    }

    #[test]
    fn shared_socket_smoke_two_targets_emit_scoped_events() {
        let duration = Duration::from_millis(70);
        let interval = Duration::from_millis(20);
        let params = test_params(Some(duration), interval);
        let a = start_echo_server(params.clone(), Duration::ZERO);
        let b = start_echo_server(params, Duration::ZERO);

        let (session, sub) = ManagedClientGroup::start_with_subscription(
            group_config(Some(duration), interval, ManagedGroupPacing::Staggered),
            vec![target("a", a.addr), target("b", b.addr)],
            SubscriberConfig {
                capacity: 256,
                overflow: SubscriberOverflow::DropNewest,
            },
        )
        .unwrap();

        let outcome = session.join().unwrap();
        let events = drain_after_join(&sub);
        a.join();
        b.join();

        assert_eq!(
            outcome.end_reason,
            ManagedGroupEndReason::AllTargetsComplete
        );
        assert_eq!(outcome.targets.len(), 2);
        for id in [TargetId::from("a"), TargetId::from("b")] {
            assert!(events.iter().any(|event| {
                event.target == id && matches!(event.event, ClientEvent::SessionStarted { .. })
            }));
            assert!(events.iter().any(|event| {
                event.target == id && matches!(event.event, ClientEvent::EchoSent { .. })
            }));
            assert!(events.iter().any(|event| {
                event.target == id && matches!(event.event, ClientEvent::EchoReply { .. })
            }));
        }
    }

    #[test]
    fn staggered_pacing_does_not_burst_active_targets() {
        let duration = Duration::from_millis(170);
        let interval = Duration::from_millis(80);
        let params = test_params(Some(duration), interval);
        let a = start_echo_server(params.clone(), Duration::ZERO);
        let b = start_echo_server(params, Duration::ZERO);

        let (session, sub) = ManagedClientGroup::start_with_subscription(
            group_config(Some(duration), interval, ManagedGroupPacing::Staggered),
            vec![target("a", a.addr), target("b", b.addr)],
            SubscriberConfig {
                capacity: 256,
                overflow: SubscriberOverflow::DropNewest,
            },
        )
        .unwrap();

        let _ = session.join().unwrap();
        let events = drain_after_join(&sub);
        a.join();
        b.join();

        let sent = sent_events_after_start(&events);
        let pair = sent
            .windows(2)
            .find(|window| window[0].0 != window[1].0)
            .expect("expected sends for two targets");
        let delta = pair[1].1.duration_since(pair[0].1);
        assert!(
            delta >= Duration::from_millis(20),
            "staggered sends were too close: {delta:?}"
        );
    }

    #[test]
    fn burst_pacing_sends_active_targets_back_to_back() {
        let duration = Duration::from_millis(190);
        let interval = Duration::from_millis(80);
        let params = test_params(Some(duration), interval);
        let a = start_echo_server(params.clone(), Duration::ZERO);
        let b = start_echo_server(params, Duration::ZERO);

        let (session, sub) = ManagedClientGroup::start_with_subscription(
            group_config(Some(duration), interval, ManagedGroupPacing::Burst),
            vec![target("a", a.addr), target("b", b.addr)],
            SubscriberConfig {
                capacity: 256,
                overflow: SubscriberOverflow::DropNewest,
            },
        )
        .unwrap();

        let _ = session.join().unwrap();
        let events = drain_after_join(&sub);
        a.join();
        b.join();

        let sent = sent_events_after_start(&events);
        let pair = sent
            .windows(2)
            .find(|window| window[0].0 != window[1].0)
            .expect("expected sends for two targets");
        let delta = pair[1].1.duration_since(pair[0].1);
        assert!(
            delta <= Duration::from_millis(20),
            "burst sends were too far apart: {delta:?}"
        );
    }

    #[test]
    fn delayed_open_target_joins_after_active_target_is_already_sending() {
        let duration = Duration::from_millis(240);
        let interval = Duration::from_millis(50);
        let params = test_params(Some(duration), interval);
        let a = start_echo_server(params.clone(), Duration::ZERO);
        let b = start_echo_server(params, Duration::from_millis(120));

        let (session, sub) = ManagedClientGroup::start_with_subscription(
            group_config(Some(duration), interval, ManagedGroupPacing::Staggered),
            vec![target("a", a.addr), target("b", b.addr)],
            SubscriberConfig {
                capacity: 512,
                overflow: SubscriberOverflow::DropNewest,
            },
        )
        .unwrap();

        let _ = session.join().unwrap();
        let events = drain_after_join(&sub);
        a.join();
        b.join();

        let b_started_at = events
            .iter()
            .find_map(|event| match (&event.target, &event.event) {
                (id, ClientEvent::SessionStarted { at, .. }) if id.as_str() == "b" => Some(at.mono),
                _ => None,
            })
            .expect("target b should eventually open");
        let a_sent_before_b = events
            .iter()
            .filter(|event| {
                event.target.as_str() == "a"
                    && matches!(
                        &event.event,
                        ClientEvent::EchoSent { sent_at, .. } if sent_at.mono < b_started_at
                    )
            })
            .count();

        assert!(
            a_sent_before_b >= 2,
            "active target did not continue sending while b was opening"
        );
        assert!(events.iter().any(|event| {
            event.target.as_str() == "b" && matches!(event.event, ClientEvent::EchoReply { .. })
        }));
    }

    #[test]
    fn update_targets_removes_and_adds_without_restarting_unchanged_target() {
        let interval = Duration::from_millis(30);
        let params = test_params(None, interval);
        let a = start_echo_server(params.clone(), Duration::ZERO);
        let b = start_echo_server(params.clone(), Duration::ZERO);
        let c = start_echo_server(params, Duration::ZERO);

        let (session, sub) = ManagedClientGroup::start_with_subscription(
            group_config(None, interval, ManagedGroupPacing::Burst),
            vec![target("a", a.addr), target("b", b.addr)],
            SubscriberConfig {
                capacity: 512,
                overflow: SubscriberOverflow::DropNewest,
            },
        )
        .unwrap();

        let mut saw_a_reply = false;
        let mut saw_b_reply = false;
        while !(saw_a_reply && saw_b_reply) {
            let event = recv_event_with_timeout(&sub);
            saw_a_reply |= event.target.as_str() == "a"
                && matches!(event.event, ClientEvent::EchoReply { .. });
            saw_b_reply |= event.target.as_str() == "b"
                && matches!(event.event, ClientEvent::EchoReply { .. });
        }

        session
            .update_targets(vec![target("a", a.addr), target("c", c.addr)])
            .unwrap();

        let deadline = Instant::now() + Duration::from_millis(220);
        let mut saw_a_after_update = false;
        let mut saw_c_after_update = false;
        let mut saw_b_echo_after_update = false;
        while Instant::now() < deadline && !(saw_a_after_update && saw_c_after_update) {
            match sub.try_recv() {
                Ok(Some(event)) => {
                    saw_a_after_update |= event.target.as_str() == "a"
                        && matches!(event.event, ClientEvent::EchoReply { .. });
                    saw_c_after_update |= event.target.as_str() == "c"
                        && matches!(event.event, ClientEvent::EchoReply { .. });
                    saw_b_echo_after_update |= event.target.as_str() == "b"
                        && matches!(
                            event.event,
                            ClientEvent::EchoSent { .. } | ClientEvent::EchoReply { .. }
                        );
                }
                Ok(None) => thread::sleep(Duration::from_millis(1)),
                Err(err) => panic!("subscription ended unexpectedly: {err}"),
            }
        }

        session.stop();
        let outcome = session.join().unwrap();
        a.join();
        b.join();
        c.join();

        assert_eq!(outcome.end_reason, ManagedGroupEndReason::Cancelled);
        assert!(saw_a_after_update);
        assert!(saw_c_after_update);
        assert!(!saw_b_echo_after_update);
        assert!(outcome
            .targets
            .iter()
            .any(|target| target.id.as_str() == "b"
                && target.end_reason == ManagedTargetEndReason::Removed));
    }

    #[test]
    fn stop_join_cleans_up_group_threads() {
        let interval = Duration::from_millis(20);
        let params = test_params(None, interval);
        let a = start_echo_server(params.clone(), Duration::ZERO);
        let b = start_echo_server(params, Duration::ZERO);

        let session = ManagedClientGroup::start(
            group_config(None, interval, ManagedGroupPacing::Staggered),
            vec![target("a", a.addr), target("b", b.addr)],
        )
        .unwrap();

        thread::sleep(Duration::from_millis(60));
        session.stop();
        session.stop();
        let outcome = session.join().unwrap();
        a.join();
        b.join();

        assert_eq!(outcome.end_reason, ManagedGroupEndReason::Cancelled);
        assert_eq!(outcome.targets.len(), 2);
    }
}
