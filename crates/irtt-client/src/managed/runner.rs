use std::{
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crate::{
    config::{ClientConfig, RecvBudget},
    error::ClientError,
    event::{ClientEvent, OpenOutcome},
    timing::ClientTimestamp,
    Client,
};

use super::{
    cancellation::CancellationToken,
    hub::{EventHub, EventSubscription, SubscriberConfig},
};

const MANAGED_RECV_TIMEOUT: Duration = Duration::from_millis(20);
const MANAGED_RECV_BUDGET: RecvBudget = RecvBudget { max_packets: 64 };
const IDLE_SLEEP: Duration = Duration::from_millis(1);
const MAX_SLEEP: Duration = Duration::from_millis(20);

#[derive(Debug)]
pub struct ManagedClient;

#[derive(Debug)]
pub struct ManagedClientSession {
    hub: EventHub,
    cancellation: CancellationToken,
    worker: Option<JoinHandle<Result<SessionOutcome, ClientError>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionOutcome {
    pub end_reason: SessionEndReason,
    pub packets_sent: u64,
    pub replies_received: u64,
    pub duplicates: u64,
    pub late: u64,
    pub warning_events: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionEndReason {
    TestComplete,
    Cancelled,
    NoTestComplete,
}

impl ManagedClient {
    pub fn start(config: ClientConfig) -> Result<ManagedClientSession, ClientError> {
        Self::start_inner(config, None).map(|(session, _)| session)
    }

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
        let outcome = client.open(ClientTimestamp::now())?;
        publish_open_outcome(&hub, &outcome);

        let cancellation = CancellationToken::new();
        let worker_hub = hub.clone();
        let worker_cancellation = cancellation.clone();
        let worker = thread::spawn(move || run_client(client, worker_hub, worker_cancellation));

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
    pub fn subscribe(&self, config: SubscriberConfig) -> Result<EventSubscription, ClientError> {
        self.hub.subscribe(config)
    }

    pub fn stop(&self) {
        self.cancellation.cancel();
    }

    pub fn join(mut self) -> Result<SessionOutcome, ClientError> {
        let worker = self.worker.take().expect("worker handle must be present");
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
        hub.disconnect_all();
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
            publish_events(
                &hub,
                &mut counters,
                client.poll_timeouts(ClientTimestamp::now())?,
            );
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
        publish_events(
            &hub,
            &mut counters,
            client.poll_timeouts(ClientTimestamp::now())?,
        );

        if client.is_run_complete() {
            break;
        }

        sleep_until_next_wakeup(client.next_send_deadline());
    }

    let packets_sent = client.packets_sent();
    let close_events = client.close(ClientTimestamp::now())?;
    publish_events(&hub, &mut counters, close_events);
    hub.disconnect_all();

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
        echo_packet_len, flags, flags::FLAG_OPEN, flags::FLAG_REPLY, layout::PacketLayout, Clock,
        Params, ReceivedStats, StampAt, TimestampFields, MAGIC, PROTOCOL_VERSION,
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

    fn test_params(duration: Option<Duration>, interval: Duration) -> Params {
        Params {
            protocol_version: PROTOCOL_VERSION,
            duration_ns: duration.map_or(0, |d| d.as_nanos() as i64),
            interval_ns: interval.as_nanos() as i64,
            length: 0,
            received_stats: ReceivedStats::Both,
            stamp_at: StampAt::Both,
            clock: Clock::Both,
            dscp: 0,
            server_fill: None,
        }
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
        let layout = PacketLayout::echo(false, params);
        let packet_len = echo_packet_len(false, params);
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
