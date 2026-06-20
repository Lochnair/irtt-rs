use std::{
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crate::{
    config::{ClientConfig, RecvBudget},
    error::ClientError,
    event::{ClientEvent, OpenOutcome},
    Client,
};

use super::{
    cancellation::CancellationToken,
    hub::{EventHub, EventSubscription, SubscriberConfig},
};

const MANAGED_RECV_TIMEOUT: Duration = Duration::from_millis(20);
const MANAGED_RECV_BUDGET: RecvBudget = RecvBudget { max_packets: 64 };
const MANAGED_FINAL_DRAIN: Duration = Duration::from_millis(100);
const IDLE_SLEEP: Duration = Duration::from_millis(1);
const MAX_SLEEP: Duration = Duration::from_millis(20);

/// Entry point for running a client session on a worker thread.
///
/// The managed API owns the lower-level [`Client`](crate::Client) loop: it
/// opens the session, sends probes at the negotiated interval, receives
/// datagrams, publishes [`ClientEvent`] values, and closes the session when the
/// run completes or is cancelled.
#[derive(Debug)]
pub struct ManagedClient;

/// Running managed client session.
///
/// [`ManagedClientSession::join`] waits for the worker and returns the session
/// outcome or client error. Dropping the session requests cooperative
/// cancellation; callers that need the outcome should explicitly join instead
/// of relying on drop.
#[must_use = "dropping the session cancels the managed client; call join() to wait for completion"]
#[derive(Debug)]
pub struct ManagedClientSession {
    hub: EventHub,
    cancellation: CancellationToken,
    worker: Option<JoinHandle<Result<SessionOutcome, ClientError>>>,
}

/// Outcome returned by a completed managed client session.
///
/// These are runner-level lifecycle counters, not statistical summaries. Use
/// `irtt-stats` with emitted `ClientEvent` values for RTT, loss, IPDV, and
/// related summaries.
#[must_use = "managed session outcomes contain completion status and counters"]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionOutcome {
    /// Why the managed loop stopped.
    pub end_reason: SessionEndReason,
    /// Number of echo requests sent by the client.
    pub packets_sent: u64,
    /// Number of first in-window echo replies received.
    pub replies_received: u64,
    /// Number of duplicate reply events emitted.
    pub duplicates: u64,
    /// Number of late reply events emitted.
    pub late: u64,
    /// Number of warning events emitted.
    pub warning_events: u64,
}

/// Reason a managed client session ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionEndReason {
    /// The negotiated finite test duration completed and the client closed the
    /// session normally.
    TestComplete,
    /// Cancellation was requested through [`ManagedClientSession::stop`] or by
    /// dropping the session handle.
    Cancelled,
    /// The session was opened in [`RunMode::NoTest`](crate::RunMode) and
    /// completed after negotiation.
    NoTestComplete,
}

impl ManagedClient {
    /// Start a managed session without creating an initial event subscription.
    ///
    /// The returned [`ManagedClientSession`] can still create subscriptions via
    /// [`ManagedClientSession::subscribe`], but events emitted before a
    /// subscriber is registered are not replayed.
    pub fn start(config: ClientConfig) -> Result<ManagedClientSession, ClientError> {
        Self::start_inner(config, None).map(|(session, _)| session)
    }

    /// Start a managed session and subscribe to events before the worker runs.
    ///
    /// The initial subscription receives the open lifecycle event and
    /// subsequent session events, subject to its queue capacity and overflow
    /// policy.
    pub fn start_with_subscription(
        config: ClientConfig,
        subscriber_config: SubscriberConfig,
    ) -> Result<(ManagedClientSession, EventSubscription), ClientError> {
        let (session, subscription) = Self::start_inner(config, Some(subscriber_config))?;
        Ok((
            session,
            subscription.expect("initial subscription must be present"),
        ))
    }

    fn start_inner(
        mut config: ClientConfig,
        subscriber_config: Option<SubscriberConfig>,
    ) -> Result<(ManagedClientSession, Option<EventSubscription>), ClientError> {
        if config.socket_config.recv_timeout.is_none()
            || config.socket_config.recv_timeout > Some(MANAGED_RECV_TIMEOUT)
        {
            config.socket_config.recv_timeout = Some(MANAGED_RECV_TIMEOUT);
        }

        let hub = EventHub::new();
        let initial_subscription = subscriber_config
            .map(|config| hub.subscribe(config))
            .transpose()?;

        let mut client = Client::connect(config)?;
        let outcome = client.open()?;
        publish_open_outcome(&hub, &outcome);

        let cancellation = CancellationToken::new();
        let worker_hub = hub.clone();
        let worker_cancellation = cancellation.clone();
        let worker =
            thread::spawn(move || run_client_with_cleanup(client, worker_hub, worker_cancellation));

        Ok((
            ManagedClientSession {
                hub,
                cancellation,
                worker: Some(worker),
            },
            initial_subscription,
        ))
    }
}

impl ManagedClientSession {
    /// Add another event subscriber to this running managed session.
    ///
    /// The subscription receives only events published after it is registered.
    pub fn subscribe(&self, config: SubscriberConfig) -> Result<EventSubscription, ClientError> {
        self.hub.subscribe(config)
    }

    /// Request cooperative cancellation of the managed session.
    ///
    /// Cancellation is observed by the worker loop between socket receive,
    /// timeout polling, and send scheduling steps. Call [`join`](Self::join) to
    /// wait for the worker and obtain the final [`SessionOutcome`].
    pub fn stop(&self) {
        self.cancellation.cancel();
    }

    /// Wait for the managed worker thread to finish.
    ///
    /// On success, returns lifecycle counters for the completed session. If the
    /// worker panicked, this returns [`ClientError::WorkerPanicked`]. Joining
    /// also disconnects all event subscriptions while leaving already queued
    /// events available to drain.
    pub fn join(mut self) -> Result<SessionOutcome, ClientError> {
        let worker = self
            .worker
            .take()
            .expect("ManagedClientSession invariant violated: worker handle missing before join");
        match worker.join() {
            Ok(outcome) => {
                self.hub.disconnect_all();
                outcome
            }
            Err(_) => {
                self.hub.disconnect_all();
                Err(ClientError::WorkerPanicked)
            }
        }
    }
}

impl Drop for ManagedClientSession {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

fn publish_open_outcome(hub: &EventHub, outcome: &OpenOutcome) {
    match outcome {
        OpenOutcome::Started { event, .. } | OpenOutcome::NoTestCompleted { event, .. } => {
            hub.publish(event.clone());
        }
    }
}

fn run_client(
    mut client: Client,
    hub: EventHub,
    cancellation: CancellationToken,
) -> Result<SessionOutcome, ClientError> {
    if client.is_run_complete() {
        return Ok(SessionOutcome {
            end_reason: SessionEndReason::NoTestComplete,
            packets_sent: 0,
            replies_received: 0,
            duplicates: 0,
            late: 0,
            warning_events: 0,
        });
    }

    let mut counters = OutcomeCounters::default();
    let mut cancelled = false;

    loop {
        if cancellation.is_cancelled() {
            cancelled = true;
            publish_events(
                &hub,
                &mut counters,
                client.recv_available(MANAGED_RECV_BUDGET)?,
            );
            if !client.is_peer_closed() {
                publish_events(&hub, &mut counters, client.poll_timeouts()?);
            }
            break;
        }

        let now = Instant::now();
        if client
            .next_send_deadline()
            .is_some_and(|deadline| deadline <= now)
        {
            let events = client.send_probe()?;
            publish_events(&hub, &mut counters, events);
            continue;
        }

        publish_events(
            &hub,
            &mut counters,
            client.recv_available(MANAGED_RECV_BUDGET)?,
        );
        if client.is_peer_closed() {
            break;
        }
        publish_events(&hub, &mut counters, client.poll_timeouts()?);

        if client.is_run_complete() {
            break;
        }

        sleep_until_next_wakeup(client.next_send_deadline());
    }

    if !cancelled {
        drain_final_late_replies(&mut client, &hub, &mut counters)?;
    }

    let packets_sent = client.packets_sent();
    if !client.is_peer_closed() {
        let close_events = client.close()?;
        publish_events(&hub, &mut counters, close_events);
    }

    Ok(SessionOutcome {
        end_reason: if cancelled {
            SessionEndReason::Cancelled
        } else {
            SessionEndReason::TestComplete
        },
        packets_sent,
        replies_received: counters.replies_received,
        duplicates: counters.duplicates,
        late: counters.late,
        warning_events: counters.warning_events,
    })
}

fn run_client_with_cleanup(
    client: Client,
    hub: EventHub,
    cancellation: CancellationToken,
) -> Result<SessionOutcome, ClientError> {
    let outcome = run_client(client, hub.clone(), cancellation);
    hub.disconnect_all();
    outcome
}

fn drain_final_late_replies(
    client: &mut Client,
    hub: &EventHub,
    counters: &mut OutcomeCounters,
) -> Result<(), ClientError> {
    if !client.has_timed_out_metadata() {
        return Ok(());
    }

    let deadline = Instant::now() + MANAGED_FINAL_DRAIN;
    while Instant::now() < deadline && client.has_timed_out_metadata() {
        if client.is_peer_closed() {
            break;
        }

        let mut published = false;

        let events = client.recv_available(MANAGED_RECV_BUDGET)?;
        published |= !events.is_empty();
        publish_events(hub, counters, events);

        if client.is_peer_closed() {
            break;
        }

        let events = client.poll_timeouts()?;
        published |= !events.is_empty();
        publish_events(hub, counters, events);

        if !published {
            thread::sleep(IDLE_SLEEP);
        }
    }
    Ok(())
}

fn publish_events(hub: &EventHub, counters: &mut OutcomeCounters, events: Vec<ClientEvent>) {
    for event in events {
        counters.observe(&event);
        hub.publish(event);
    }
}

fn sleep_until_next_wakeup(deadline: Option<Instant>) {
    let sleep_for = deadline
        .and_then(|deadline| deadline.checked_duration_since(Instant::now()))
        .map(|duration| duration.min(MAX_SLEEP))
        .unwrap_or(IDLE_SLEEP);
    if sleep_for > Duration::ZERO {
        thread::sleep(sleep_for);
    }
}

#[derive(Debug, Default)]
struct OutcomeCounters {
    replies_received: u64,
    duplicates: u64,
    late: u64,
    warning_events: u64,
}

impl OutcomeCounters {
    fn observe(&mut self, event: &ClientEvent) {
        match event {
            ClientEvent::EchoReply { .. } => self.replies_received += 1,
            ClientEvent::DuplicateReply { .. } => self.duplicates += 1,
            ClientEvent::LateReply { .. } => self.late += 1,
            ClientEvent::Warning { .. } => self.warning_events += 1,
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::NegotiationPolicy, SubscriberOverflow};
    use irtt_proto::{
        echo_packet_len,
        flags::{self, FLAG_OPEN, FLAG_REPLY},
        layout::PacketLayout,
        Clock, Params, ReceivedStats, StampAt, TimestampFields, MAGIC, PROTOCOL_VERSION,
    };
    use std::{
        net::{SocketAddr, UdpSocket},
        sync::mpsc,
    };

    const TOKEN: u64 = 0x1234_5678_90ab_cdef;
    struct FakeServer {
        addr: SocketAddr,
        done: JoinHandle<()>,
    }

    impl FakeServer {
        fn join(self) {
            self.done.join().unwrap();
        }
    }

    fn test_echo_packet_len(hmac: bool, params: &Params) -> usize {
        echo_packet_len(hmac, params)
            .expect("managed runner test params must have a non-negative packet length")
    }

    fn test_params(duration: Option<Duration>, interval: Duration) -> Params {
        Params {
            protocol_version: PROTOCOL_VERSION,
            duration_ns: duration.map_or(0, test_duration_ns_i64),
            interval_ns: test_duration_ns_i64(interval),
            length: 0,
            received_stats: ReceivedStats::Both,
            stamp_at: StampAt::Both,
            clock: Clock::Both,
            dscp: 0,
            server_fill: None,
        }
    }

    fn test_duration_ns_i64(duration: Duration) -> i64 {
        i64::try_from(duration.as_nanos()).expect("test duration fits i64 nanoseconds")
    }

    fn config(addr: SocketAddr, duration: Option<Duration>) -> ClientConfig {
        ClientConfig {
            server_addr: addr.to_string(),
            duration,
            interval: Duration::from_millis(10),
            negotiation_policy: NegotiationPolicy::Strict,
            open_timeouts: vec![Duration::from_millis(200)],
            probe_timeout: Duration::from_millis(40),
            ..ClientConfig::default()
        }
    }

    fn start_echo_server(params: Params) -> FakeServer {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = socket.local_addr().unwrap();
        let done = thread::spawn(move || {
            let (_, peer) = recv_request(&socket);
            socket
                .send_to(&open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params), peer)
                .unwrap();
            socket
                .set_read_timeout(Some(Duration::from_millis(500)))
                .unwrap();

            while let Some((packet, peer)) = recv_request_timeout(&socket) {
                if packet[3] & flags::FLAG_CLOSE != 0 {
                    break;
                }

                let seq = u32::from_le_bytes(packet[12..16].try_into().unwrap());
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
        FakeServer { addr, done }
    }

    fn start_delayed_reply_server(params: Params, delay: Duration) -> FakeServer {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = socket.local_addr().unwrap();
        let done = thread::spawn(move || {
            let (_, peer) = recv_request(&socket);
            socket
                .send_to(&open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params), peer)
                .unwrap();
            socket
                .set_read_timeout(Some(Duration::from_millis(500)))
                .unwrap();

            let Some((packet, peer)) = recv_request_timeout(&socket) else {
                return;
            };
            let seq = u32::from_le_bytes(packet[12..16].try_into().unwrap());
            thread::sleep(delay);
            let ts = TimestampFields {
                recv_wall: Some(1_000_000_000),
                recv_mono: Some(100_000),
                send_wall: Some(1_001_000_000),
                send_mono: Some(1_100_000),
                ..Default::default()
            };
            socket
                .send_to(&echo_reply_packet(TOKEN, seq, &params, &ts), peer)
                .unwrap();

            while let Some((packet, _)) = recv_request_timeout(&socket) {
                if packet[3] & flags::FLAG_CLOSE != 0 {
                    break;
                }
            }
        });
        FakeServer { addr, done }
    }

    fn start_peer_close_server(params: Params) -> FakeServer {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = socket.local_addr().unwrap();
        let done = thread::spawn(move || {
            let (_, peer) = recv_request(&socket);
            socket
                .send_to(&open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params), peer)
                .unwrap();
            socket
                .set_read_timeout(Some(Duration::from_millis(500)))
                .unwrap();

            let Some((packet, peer)) = recv_request_timeout(&socket) else {
                return;
            };
            let seq = u32::from_le_bytes(packet[12..16].try_into().unwrap());
            let ts = TimestampFields {
                recv_wall: Some(1_000_000_000),
                recv_mono: Some(100_000),
                send_wall: Some(1_000_000_000),
                send_mono: Some(100_000),
                ..Default::default()
            };
            socket
                .send_to(
                    &echo_reply_packet_with_flags(
                        TOKEN,
                        seq,
                        &params,
                        &ts,
                        FLAG_REPLY | flags::FLAG_CLOSE,
                    ),
                    peer,
                )
                .unwrap();

            if let Some((packet, _)) = recv_request_timeout(&socket) {
                panic!(
                    "managed cleanup must not send any packet after peer close; flags={:?} len={}",
                    packet.get(3).copied(),
                    packet.len()
                );
            }
        });
        FakeServer { addr, done }
    }

    fn recv_request(socket: &UdpSocket) -> (Vec<u8>, SocketAddr) {
        let mut buf = [0_u8; 2048];
        let (size, peer) = socket.recv_from(&mut buf).unwrap();
        (buf[..size].to_vec(), peer)
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
        echo_reply_packet_with_flags(token, seq, params, timestamps, FLAG_REPLY)
    }

    fn echo_reply_packet_with_flags(
        token: u64,
        seq: u32,
        params: &Params,
        timestamps: &TimestampFields,
        flags: u8,
    ) -> Vec<u8> {
        let layout = PacketLayout::echo(false, params);
        let packet_len = test_echo_packet_len(false, params);
        let mut packet = Vec::with_capacity(packet_len);

        packet.extend_from_slice(&MAGIC);
        packet.push(flags);
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

    fn recv_event_with_timeout(sub: &EventSubscription) -> ClientEvent {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match sub.try_recv() {
                Ok(Some(event)) => return event,
                Ok(None) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(1));
                }
                Ok(None) => panic!("timed out waiting for managed client event"),
                Err(err) => panic!("subscription ended while waiting for event: {err}"),
            }
        }
    }

    fn collect_until_closed(sub: &EventSubscription) -> Vec<ClientEvent> {
        let mut events = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            match sub.try_recv() {
                Ok(Some(event)) => {
                    let closed = matches!(event, ClientEvent::SessionClosed { .. });
                    events.push(event);
                    if closed {
                        return events;
                    }
                }
                Ok(None) => thread::sleep(Duration::from_millis(1)),
                Err(_) => return events,
            }
        }
        panic!("timed out waiting for session close event");
    }

    #[test]
    fn stop_is_idempotent() {
        let server = start_echo_server(test_params(None, Duration::from_millis(10)));
        let session = ManagedClient::start(config(server.addr, None)).unwrap();
        session.stop();
        session.stop();
        let outcome = session.join().unwrap();
        server.join();

        assert_eq!(outcome.end_reason, SessionEndReason::Cancelled);
    }

    #[test]
    fn finite_managed_run_emits_session_probe_and_close_events() {
        let duration = Duration::from_millis(35);
        let server = start_echo_server(test_params(Some(duration), Duration::from_millis(10)));
        let (session, sub) = ManagedClient::start_with_subscription(
            config(server.addr, Some(duration)),
            SubscriberConfig {
                capacity: 16,
                overflow: SubscriberOverflow::DropNewest,
            },
        )
        .unwrap();

        let events = collect_until_closed(&sub);
        let outcome = session.join().unwrap();
        server.join();

        assert_eq!(outcome.end_reason, SessionEndReason::TestComplete);
        assert!(events
            .iter()
            .any(|event| matches!(event, ClientEvent::SessionStarted { .. })));
        assert!(events
            .iter()
            .any(|event| matches!(event, ClientEvent::EchoReply { .. })));
        assert!(events
            .iter()
            .any(|event| matches!(event, ClientEvent::SessionClosed { .. })));
    }

    #[test]
    fn finite_managed_run_drains_late_reply_after_timeout_before_close() {
        let duration = Duration::from_millis(1);
        let params = test_params(Some(duration), Duration::from_millis(10));
        let server = start_delayed_reply_server(params, Duration::from_millis(60));
        let mut cfg = config(server.addr, Some(duration));
        cfg.probe_timeout = Duration::from_millis(20);

        let (session, sub) = ManagedClient::start_with_subscription(
            cfg,
            SubscriberConfig {
                capacity: 16,
                overflow: SubscriberOverflow::DropNewest,
            },
        )
        .unwrap();

        let events = collect_until_closed(&sub);
        let outcome = session.join().unwrap();
        server.join();

        assert_eq!(outcome.end_reason, SessionEndReason::TestComplete);
        assert!(events
            .iter()
            .any(|event| matches!(event, ClientEvent::EchoLoss { seq: 0, .. })));
        assert!(events.iter().any(|event| matches!(
            event,
            ClientEvent::LateReply {
                seq: 0,
                sent_at: Some(_),
                rtt: Some(_),
                ..
            }
        )));
        let late_before_close = events
            .iter()
            .position(|event| matches!(event, ClientEvent::LateReply { rtt: Some(_), .. }));
        let close = events
            .iter()
            .position(|event| matches!(event, ClientEvent::SessionClosed { .. }));
        let late_before_close = late_before_close.expect("missing stats-eligible LateReply");
        let close = close.expect("missing SessionClosed");
        assert!(late_before_close < close);
    }

    #[test]
    fn peer_close_during_managed_run_is_successful() {
        let server = start_peer_close_server(test_params(None, Duration::from_millis(10)));
        let (session, sub) = ManagedClient::start_with_subscription(
            config(server.addr, None),
            SubscriberConfig {
                capacity: 16,
                overflow: SubscriberOverflow::DropNewest,
            },
        )
        .unwrap();

        let events = collect_until_closed(&sub);
        let outcome = session.join().unwrap();
        server.join();

        assert_eq!(outcome.end_reason, SessionEndReason::TestComplete);
        assert!(events
            .iter()
            .any(|event| matches!(event, ClientEvent::EchoReply { .. })));
        assert!(events
            .iter()
            .any(|event| matches!(event, ClientEvent::SessionClosed { .. })));
    }

    #[test]
    fn continuous_managed_run_can_be_stopped_cleanly() {
        let server = start_echo_server(test_params(None, Duration::from_millis(10)));
        let session = ManagedClient::start(config(server.addr, None)).unwrap();
        let sub = session
            .subscribe(SubscriberConfig {
                capacity: 16,
                overflow: SubscriberOverflow::DropNewest,
            })
            .unwrap();

        loop {
            if matches!(recv_event_with_timeout(&sub), ClientEvent::EchoReply { .. }) {
                break;
            }
        }
        session.stop();
        let events = collect_until_closed(&sub);
        let outcome = session.join().unwrap();
        server.join();

        assert_eq!(outcome.end_reason, SessionEndReason::Cancelled);
        assert!(events
            .iter()
            .any(|event| matches!(event, ClientEvent::SessionClosed { .. })));
    }

    #[test]
    fn join_reports_worker_panic() {
        let hub = EventHub::new();
        let cancellation = CancellationToken::new();
        let worker = thread::spawn(|| -> Result<SessionOutcome, ClientError> {
            panic!("intentional managed worker panic")
        });
        let session = ManagedClientSession {
            hub,
            cancellation,
            worker: Some(worker),
        };

        assert!(matches!(session.join(), Err(ClientError::WorkerPanicked)));
    }

    #[test]
    fn no_test_managed_run_returns_no_test_outcome() {
        use crate::RunMode;

        let params = test_params(Some(Duration::from_millis(10)), Duration::from_millis(10));
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = socket.local_addr().unwrap();
        let (tx, rx) = mpsc::channel();
        let done = thread::spawn(move || {
            let (request, peer) = recv_request(&socket);
            tx.send(request[3] & flags::FLAG_CLOSE != 0).unwrap();
            socket
                .send_to(
                    &open_reply(FLAG_OPEN | FLAG_REPLY | flags::FLAG_CLOSE, 0, &params),
                    peer,
                )
                .unwrap();
        });

        let mut cfg = config(addr, Some(Duration::from_millis(10)));
        cfg.run_mode = RunMode::NoTest;
        let session = ManagedClient::start(cfg).unwrap();
        assert!(rx.recv_timeout(Duration::from_secs(2)).unwrap());
        let outcome = session.join().unwrap();
        done.join().unwrap();

        assert_eq!(outcome.end_reason, SessionEndReason::NoTestComplete);
    }
}
