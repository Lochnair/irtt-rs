use std::{
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::{Duration, Instant},
};

use irtt_client::{Client, ClientConfig, ClientEvent, OpenOutcome, RecvBudget};

pub const RECV_BUDGET: RecvBudget = RecvBudget { max_packets: 16 };
const MAX_FINAL_DRAIN: Duration = Duration::from_secs(30);
const IDLE_SLEEP: Duration = Duration::from_millis(5);
const MAX_SLEEP: Duration = Duration::from_millis(20);

pub struct ClientSession {
    client: Client,
    continuous: bool,
}

impl ClientSession {
    pub fn connect(
        config: ClientConfig,
        continuous: bool,
    ) -> Result<Self, irtt_client::ClientError> {
        Ok(Self {
            client: Client::connect(config)?,
            continuous,
        })
    }

    pub fn open(&mut self) -> Result<Vec<ClientEvent>, irtt_client::ClientError> {
        Ok(vec![open_event(&self.client.open()?).clone()])
    }

    pub fn step(
        &mut self,
        shutdown_requested: &AtomicBool,
    ) -> Result<Vec<ClientEvent>, irtt_client::ClientError> {
        let mut events = Vec::new();
        if should_send_probe(self.client.next_send_deadline(), shutdown_requested) {
            events.extend(self.client.send_probe()?);
        }
        events.extend(self.client.recv_available(RECV_BUDGET)?);
        if self.client.is_peer_closed() {
            return Ok(events);
        }
        events.extend(self.client.poll_timeouts()?);
        Ok(events)
    }

    pub fn should_continue(&self, shutdown_requested: &AtomicBool) -> bool {
        !is_shutdown_requested(shutdown_requested)
            && !self.client.is_peer_closed()
            && (self.continuous || self.client.next_send_deadline().is_some())
    }

    pub fn should_drain_final(&self, interrupted: bool) -> bool {
        should_drain_final(self.continuous, interrupted)
    }

    pub fn drain_final<F>(&mut self, mut on_events: F) -> Result<(), irtt_client::ClientError>
    where
        F: FnMut(&[ClientEvent]),
    {
        let deadline = Instant::now() + final_drain_duration(self.client.probe_timeout());
        loop {
            if self.client.is_peer_closed() {
                break;
            }

            let mut received = false;

            let events = self.client.recv_available(RECV_BUDGET)?;
            received |= !events.is_empty();
            on_events(&events);

            if self.client.is_peer_closed() {
                break;
            }

            let events = self.client.poll_timeouts()?;
            received |= !events.is_empty();
            on_events(&events);

            if self.client.is_run_complete() || Instant::now() >= deadline {
                break;
            }

            if !received {
                thread::sleep(IDLE_SLEEP);
            }
        }
        Ok(())
    }

    pub fn poll_timeouts(&mut self) -> Result<Vec<ClientEvent>, irtt_client::ClientError> {
        if self.client.is_peer_closed() {
            return Ok(vec![]);
        }
        self.client.poll_timeouts()
    }

    pub fn close(&mut self) -> Result<Vec<ClientEvent>, irtt_client::ClientError> {
        if self.client.is_peer_closed() {
            return Ok(vec![]);
        }
        self.client.close()
    }

    pub fn next_send_deadline(&self) -> Option<Instant> {
        self.client.next_send_deadline()
    }

    pub fn probe_timeout(&self) -> Duration {
        self.client.probe_timeout()
    }

    pub fn sleep_until_next_send(&self) {
        let sleep_for = match self.next_send_deadline() {
            Some(deadline) => deadline
                .saturating_duration_since(Instant::now())
                .min(MAX_SLEEP),
            None => Duration::from_millis(1),
        };
        if !sleep_for.is_zero() {
            thread::sleep(sleep_for);
        }
    }
}

pub fn is_shutdown_requested(shutdown_requested: &AtomicBool) -> bool {
    shutdown_requested.load(Ordering::Relaxed)
}

pub fn should_drain_final(continuous: bool, interrupted: bool) -> bool {
    !continuous || interrupted
}

pub fn should_print_final_summary(continuous: bool, interrupted: bool) -> bool {
    !continuous || interrupted
}

pub fn final_drain_duration(probe_timeout: Duration) -> Duration {
    probe_timeout.min(MAX_FINAL_DRAIN)
}

fn should_send_probe(next_send_deadline: Option<Instant>, shutdown_requested: &AtomicBool) -> bool {
    !is_shutdown_requested(shutdown_requested)
        && next_send_deadline.is_some_and(|deadline| deadline <= Instant::now())
}

fn open_event(outcome: &OpenOutcome) -> &ClientEvent {
    match outcome {
        OpenOutcome::Started { event, .. } | OpenOutcome::NoTestCompleted { event, .. } => event,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use irtt_proto::{
        echo_packet_len,
        flags::{self, FLAG_OPEN, FLAG_REPLY},
        layout::PacketLayout,
        Clock, Params, ReceivedStats, StampAt, TimestampFields, MAGIC, PROTOCOL_VERSION,
    };
    use std::{
        net::{SocketAddr, UdpSocket},
        thread::JoinHandle,
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

    #[test]
    fn final_drain_uses_capped_probe_timeout() {
        assert_eq!(
            final_drain_duration(Duration::from_secs(4)),
            Duration::from_secs(4)
        );
        assert_eq!(
            final_drain_duration(Duration::from_secs(60)),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn should_drain_final_for_finite_or_interrupted_runs() {
        assert!(should_drain_final(false, false));
        assert!(should_drain_final(false, true));
        assert!(should_drain_final(true, true));
        assert!(!should_drain_final(true, false));
    }

    #[test]
    fn peer_close_followed_by_session_cleanup_is_successful() {
        let params = test_params(None, Duration::from_millis(10));
        let server = start_peer_close_server(params);
        let mut session = ClientSession::connect(config(server.addr, None), true).unwrap();
        let shutdown_requested = AtomicBool::new(false);

        assert!(matches!(
            session.open().unwrap().as_slice(),
            [ClientEvent::SessionStarted { .. }]
        ));

        let events = session.step(&shutdown_requested).unwrap();
        assert!(events
            .iter()
            .any(|event| matches!(event, ClientEvent::EchoReply { .. })));
        assert!(events
            .iter()
            .any(|event| matches!(event, ClientEvent::SessionClosed { .. })));
        assert!(!session.should_continue(&shutdown_requested));

        session.drain_final(|_| {}).unwrap();
        assert!(session.poll_timeouts().unwrap().is_empty());
        assert!(session.close().unwrap().is_empty());
        server.join();
    }

    fn config(addr: SocketAddr, duration: Option<Duration>) -> ClientConfig {
        ClientConfig {
            server_addr: addr.to_string(),
            duration,
            interval: Duration::from_millis(10),
            open_timeouts: vec![Duration::from_millis(200)],
            probe_timeout: Duration::from_millis(20),
            socket_config: irtt_client::SocketConfig {
                recv_timeout: Some(Duration::from_millis(200)),
                ..Default::default()
            },
            ..ClientConfig::default()
        }
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

    fn start_peer_close_server(params: Params) -> FakeServer {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = socket.local_addr().unwrap();
        let done = thread::spawn(move || {
            let (_, peer) = recv_request(&socket);
            socket
                .send_to(&open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params), peer)
                .unwrap();

            let (packet, peer) = recv_request(&socket);
            let seq = u32::from_le_bytes(packet[12..16].try_into().unwrap());
            socket
                .send_to(
                    &echo_reply_packet(
                        TOKEN,
                        seq,
                        &params,
                        &TimestampFields::default(),
                        FLAG_REPLY | flags::FLAG_CLOSE,
                    ),
                    peer,
                )
                .unwrap();

            socket
                .set_read_timeout(Some(Duration::from_millis(250)))
                .unwrap();
            while let Some((packet, _)) = recv_request_timeout(&socket) {
                assert_eq!(
                    packet[3] & flags::FLAG_CLOSE,
                    0,
                    "session cleanup must not send a close after peer close"
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
        flags: u8,
    ) -> Vec<u8> {
        let layout = PacketLayout::echo(false, params);
        let packet_len = echo_packet_len(false, params)
            .expect("session test params must have a non-negative packet length");
        let mut packet = Vec::with_capacity(packet_len);

        packet.extend_from_slice(&MAGIC);
        packet.push(flags);
        packet.extend_from_slice(&token.to_le_bytes());
        packet.extend_from_slice(&seq.to_le_bytes());

        if layout.recv_count {
            packet.extend_from_slice(&42_u32.to_le_bytes());
        }
        if layout.recv_window {
            packet.extend_from_slice(&0_u64.to_le_bytes());
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
}
