use std::{
    io,
    net::{SocketAddr, UdpSocket},
    time::{Duration, Instant},
};

use irtt_proto::{
    close::CloseRequest, decode_echo_reply, encode_close_request, encode_echo_request,
    encode_open_request, flags, EchoReply, EchoRequest, OpenReply, OpenRequest, Params, ServerFill,
    TimestampFields, PROTOCOL_VERSION,
};

use crate::{
    config::{ClientConfig, RecvBudget, RunMode},
    error::ClientError,
    event::{
        ClientEvent, OneWayDelaySample, OpenOutcome, PacketMeta, ReceivedStatsSample, RttSample,
        ServerTiming,
    },
    probe::{CompletedSet, PendingMap, PendingProbe},
    session::{validate_negotiated_params, ActiveSession, ClientPhase, NegotiatedParams},
    socket::{connect_udp_socket, resolve_remote, validate_open_timeouts},
    timing::ClientTimestamp,
};

const MAX_OPEN_PACKET_SIZE: usize = 512;
const MAX_RECV_PACKET_SIZE: usize = 2048;

#[derive(Debug)]
pub struct Client {
    config: ClientConfig,
    socket: UdpSocket,
    remote: SocketAddr,
    requested: Params,
    negotiated: Option<NegotiatedParams>,
    phase: ClientPhase,
    session: Option<ActiveSession>,
}

impl Client {
    pub fn connect(config: ClientConfig) -> Result<Self, ClientError> {
        validate_open_timeouts(&config.open_timeouts)?;
        let remote = resolve_remote(&config)?;
        let socket = connect_udp_socket(&config.socket_config, remote)?;
        let requested = params_from_config(&config)?;

        Ok(Self {
            config,
            socket,
            remote,
            requested,
            negotiated: None,
            phase: ClientPhase::Connected,
            session: None,
        })
    }

    pub fn open(&mut self, now: ClientTimestamp) -> Result<OpenOutcome, ClientError> {
        match self.phase {
            ClientPhase::Connected => {}
            ClientPhase::Open { .. } => return Err(ClientError::AlreadyOpen),
            ClientPhase::Closed => return Err(ClientError::AlreadyClosed),
            ClientPhase::NoTestCompleted => return Err(ClientError::AlreadyCompleted),
        }

        let outcome = self.open_inner(now);
        let restore = self
            .socket
            .set_read_timeout(self.config.socket_config.recv_timeout);
        match (outcome, restore) {
            (Ok(outcome), Ok(())) => Ok(outcome),
            (Ok(outcome), Err(_)) => Ok(outcome),
            (Err(err), Ok(())) => Err(err),
            (Err(err), Err(_)) => Err(err),
        }
    }

    fn open_inner(&mut self, now: ClientTimestamp) -> Result<OpenOutcome, ClientError> {
        let request = OpenRequest {
            params: self.requested.clone(),
            close: self.config.run_mode == RunMode::NoTest,
        };
        let packet = encode_open_request(&request, self.config.hmac_key.as_deref())?;
        let mut buf = [0_u8; MAX_OPEN_PACKET_SIZE];

        for timeout in &self.config.open_timeouts {
            self.socket.set_read_timeout(Some(*timeout))?;
            self.socket.send(&packet)?;

            match self.socket.recv(&mut buf) {
                Ok(size) => {
                    let reply = irtt_proto::decode_open_reply(
                        &buf[..size],
                        self.config.hmac_key.as_deref(),
                    )?;
                    return self.accept_open_reply(reply, now);
                }
                Err(err)
                    if matches!(
                        err.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                    ) => {}
                Err(err) => return Err(ClientError::Socket(err)),
            }
        }

        Err(ClientError::OpenTimeout)
    }

    pub fn close(&mut self, now: ClientTimestamp) -> Result<Vec<ClientEvent>, ClientError> {
        let token = match self.phase {
            ClientPhase::Open { token } => token,
            ClientPhase::Closed => return Err(ClientError::AlreadyClosed),
            ClientPhase::Connected | ClientPhase::NoTestCompleted => {
                return Err(ClientError::NotOpen)
            }
        };

        let packet =
            encode_close_request(&CloseRequest { token }, self.config.hmac_key.as_deref())?;
        self.socket.send(&packet)?;
        self.phase = ClientPhase::Closed;
        self.session = None;

        Ok(vec![ClientEvent::SessionClosed {
            remote: self.remote,
            token,
            at: now,
        }])
    }

    pub fn next_send_deadline(&self) -> Option<Instant> {
        let session = self.session.as_ref()?;
        if session.sending_done {
            return None;
        }
        Some(session.next_send_at)
    }

    pub fn send_probe(&mut self, now: ClientTimestamp) -> Result<Vec<ClientEvent>, ClientError> {
        let token = match self.phase {
            ClientPhase::Open { token } => token,
            ClientPhase::Closed => return Err(ClientError::AlreadyClosed),
            ClientPhase::Connected => return Err(ClientError::NotOpen),
            ClientPhase::NoTestCompleted => return Err(ClientError::AlreadyCompleted),
        };

        let session = self
            .session
            .as_mut()
            .expect("session must exist when phase is Open");

        if session.sending_done {
            return Ok(vec![]);
        }

        if let Some(end) = session.end_mono {
            if now.mono >= end {
                session.sending_done = true;
                return Ok(vec![]);
            }
        }

        let negotiated = self
            .negotiated
            .as_ref()
            .expect("negotiated must exist when Open");

        let wire_seq = session.next_wire_seq;
        let logical_seq = session.next_logical_seq;

        let request = EchoRequest {
            token,
            sequence: wire_seq,
            params: negotiated.params.clone(),
            payload: vec![],
        };
        let packet = encode_echo_request(&request, self.config.hmac_key.as_deref())?;
        self.socket.send(&packet)?;

        let pending = PendingProbe {
            logical_seq,
            wire_seq,
            sent_at: now,
            timeout_at: now.mono + self.config.probe_timeout,
        };
        session.pending.insert(pending)?;

        session.next_wire_seq = session.next_wire_seq.wrapping_add(1);
        session.next_logical_seq += 1;
        session.packets_sent += 1;

        let interval_ns = negotiated.params.interval_ns as u64;
        session.next_send_at =
            session.start_mono + Duration::from_nanos(interval_ns * session.packets_sent);

        if let Some(end) = session.end_mono {
            if session.next_send_at >= end {
                session.sending_done = true;
            }
        }

        Ok(vec![])
    }

    pub fn recv_once(&mut self, now: ClientTimestamp) -> Result<Vec<ClientEvent>, ClientError> {
        match self.phase {
            ClientPhase::Open { .. } => {}
            ClientPhase::Closed => return Err(ClientError::AlreadyClosed),
            ClientPhase::Connected => return Err(ClientError::NotOpen),
            ClientPhase::NoTestCompleted => return Err(ClientError::AlreadyCompleted),
        }

        let mut buf = [0_u8; MAX_RECV_PACKET_SIZE];
        let size = match self.socket.recv(&mut buf) {
            Ok(size) => size,
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                return Ok(vec![]);
            }
            Err(err) => return Err(ClientError::Socket(err)),
        };

        self.process_received_packet(&buf[..size], now)
    }

    pub fn recv_available(
        &mut self,
        now: ClientTimestamp,
        budget: RecvBudget,
    ) -> Result<Vec<ClientEvent>, ClientError> {
        let mut all_events = Vec::new();
        for _ in 0..budget.max_packets {
            let events = self.recv_once(now)?;
            if events.is_empty() {
                break;
            }
            all_events.extend(events);
        }
        Ok(all_events)
    }

    pub fn poll_timeouts(&mut self, now: ClientTimestamp) -> Result<Vec<ClientEvent>, ClientError> {
        match self.phase {
            ClientPhase::Open { .. } => {}
            ClientPhase::Closed => return Err(ClientError::AlreadyClosed),
            ClientPhase::Connected => return Err(ClientError::NotOpen),
            ClientPhase::NoTestCompleted => return Err(ClientError::AlreadyCompleted),
        }

        let session = self
            .session
            .as_mut()
            .expect("session must exist when phase is Open");

        let expired = session.pending.drain_expired(now.mono);
        let events: Vec<ClientEvent> = expired
            .into_iter()
            .map(|probe| ClientEvent::EchoLoss {
                seq: probe.wire_seq,
                logical_seq: probe.logical_seq,
                sent_at: probe.sent_at,
                timeout_at: probe.timeout_at,
            })
            .collect();

        Ok(events)
    }

    fn process_received_packet(
        &mut self,
        packet: &[u8],
        now: ClientTimestamp,
    ) -> Result<Vec<ClientEvent>, ClientError> {
        let negotiated = self
            .negotiated
            .as_ref()
            .expect("negotiated must exist when Open");

        let reply =
            match decode_echo_reply(packet, &negotiated.params, self.config.hmac_key.as_deref()) {
                Ok(reply) => reply,
                Err(_) => {
                    return Ok(vec![ClientEvent::Warning {
                        message: "dropped malformed or unrelated packet".to_owned(),
                    }]);
                }
            };

        let token = match self.phase {
            ClientPhase::Open { token } => token,
            _ => unreachable!(),
        };
        if reply.token != token {
            return Ok(vec![ClientEvent::Warning {
                message: format!(
                    "dropped reply with wrong token: expected {token:#x}, got {:#x}",
                    reply.token
                ),
            }]);
        }

        let session = self.session.as_mut().expect("session must exist when Open");

        let wire_seq = reply.sequence;

        if let Some(pending) = session.pending.remove(wire_seq) {
            let rtt = compute_rtt(&pending.sent_at, &now, &reply.timestamps);
            let server_timing = build_server_timing(&reply.timestamps);
            let one_way = compute_one_way(&pending.sent_at, &now, &reply.timestamps);
            let received_stats = build_received_stats(&reply);
            let is_late = session.highest_received_seq.is_some_and(|h| wire_seq < h);
            let highest_seen = session.highest_received_seq.unwrap_or(wire_seq);

            update_highest_received(session, wire_seq);
            session.completed.insert(wire_seq, pending.logical_seq);

            let mut events = Vec::new();
            if let Some(rtt_sample) = rtt {
                if is_late {
                    events.push(ClientEvent::LateReply {
                        seq: wire_seq,
                        logical_seq: Some(pending.logical_seq),
                        highest_seen,
                        remote: self.remote,
                        sent_at: Some(pending.sent_at),
                        received_at: now,
                        rtt: Some(rtt_sample),
                        server_timing,
                        one_way,
                        received_stats,
                        packet_meta: PacketMeta::default(),
                    });
                } else {
                    events.push(ClientEvent::EchoReply {
                        seq: wire_seq,
                        logical_seq: pending.logical_seq,
                        remote: self.remote,
                        sent_at: pending.sent_at,
                        received_at: now,
                        rtt: rtt_sample,
                        server_timing,
                        one_way,
                        received_stats,
                        packet_meta: PacketMeta::default(),
                    });
                }
            }
            Ok(events)
        } else if session.completed.contains(wire_seq) {
            update_highest_received(session, wire_seq);
            Ok(vec![ClientEvent::DuplicateReply {
                seq: wire_seq,
                remote: self.remote,
                received_at: now,
            }])
        } else {
            let highest_seen = session.highest_received_seq.unwrap_or(wire_seq);
            update_highest_received(session, wire_seq);
            Ok(vec![ClientEvent::LateReply {
                seq: wire_seq,
                logical_seq: None,
                highest_seen,
                remote: self.remote,
                sent_at: None,
                received_at: now,
                rtt: None,
                server_timing: build_server_timing(&reply.timestamps),
                one_way: None,
                received_stats: build_received_stats(&reply),
                packet_meta: PacketMeta::default(),
            }])
        }
    }

    fn accept_open_reply(
        &mut self,
        reply: OpenReply,
        now: ClientTimestamp,
    ) -> Result<OpenOutcome, ClientError> {
        if reply.params.protocol_version != PROTOCOL_VERSION {
            return Err(ClientError::ProtocolVersionMismatch {
                requested: PROTOCOL_VERSION,
                received: reply.params.protocol_version,
            });
        }

        let reply_is_close = flags::has(reply.flags, flags::FLAG_CLOSE);
        match self.config.run_mode {
            RunMode::Normal if reply_is_close => Err(ClientError::ServerRejected),
            RunMode::Normal if reply.token == 0 => Err(ClientError::ZeroToken),
            RunMode::Normal => self.accept_normal_open(reply, now),
            RunMode::NoTest if !reply_is_close => Err(ClientError::UnexpectedNoTestReply),
            RunMode::NoTest if reply.token != 0 => {
                Err(ClientError::NonZeroNoTestToken { token: reply.token })
            }
            RunMode::NoTest => self.accept_no_test_open(reply, now),
        }
    }

    fn accept_normal_open(
        &mut self,
        reply: OpenReply,
        now: ClientTimestamp,
    ) -> Result<OpenOutcome, ClientError> {
        validate_negotiated_params(
            &self.requested,
            &reply.params,
            self.config.negotiation_policy,
        )?;
        let negotiated = NegotiatedParams {
            params: reply.params.clone(),
        };
        self.negotiated = Some(negotiated.clone());
        self.phase = ClientPhase::Open { token: reply.token };

        let duration_ns = negotiated.params.duration_ns;
        let end_mono = if duration_ns > 0 {
            Some(now.mono + Duration::from_nanos(duration_ns as u64))
        } else {
            None
        };

        self.session = Some(ActiveSession {
            next_wire_seq: 0,
            next_logical_seq: 0,
            highest_received_seq: None,
            packets_sent: 0,
            start_mono: now.mono,
            end_mono,
            next_send_at: now.mono,
            pending: PendingMap::new(self.config.max_pending_probes),
            completed: CompletedSet::new(self.config.max_pending_probes),
            sending_done: false,
        });

        let event = ClientEvent::SessionStarted {
            remote: self.remote,
            token: reply.token,
            negotiated: negotiated.clone(),
            at: now,
        };

        Ok(OpenOutcome::Started {
            remote: self.remote,
            token: reply.token,
            negotiated,
            event,
        })
    }

    fn accept_no_test_open(
        &mut self,
        reply: OpenReply,
        now: ClientTimestamp,
    ) -> Result<OpenOutcome, ClientError> {
        validate_negotiated_params(
            &self.requested,
            &reply.params,
            self.config.negotiation_policy,
        )?;
        let negotiated = NegotiatedParams {
            params: reply.params.clone(),
        };
        self.negotiated = Some(negotiated.clone());
        self.phase = ClientPhase::NoTestCompleted;
        let event = ClientEvent::NoTestCompleted {
            remote: self.remote,
            negotiated: negotiated.clone(),
            at: now,
        };
        Ok(OpenOutcome::NoTestCompleted {
            remote: self.remote,
            negotiated,
            event,
        })
    }
}

fn update_highest_received(session: &mut ActiveSession, wire_seq: u32) {
    session.highest_received_seq = Some(
        session
            .highest_received_seq
            .map_or(wire_seq, |h| h.max(wire_seq)),
    );
}

fn compute_rtt(
    sent_at: &ClientTimestamp,
    received_at: &ClientTimestamp,
    ts: &TimestampFields,
) -> Option<RttSample> {
    let raw = received_at.mono.checked_duration_since(sent_at.mono)?;

    let server_processing = compute_server_processing(ts);

    let adjusted = server_processing.and_then(|sp| raw.checked_sub(sp));

    let effective = adjusted.unwrap_or(raw);

    Some(RttSample {
        raw,
        adjusted,
        effective,
    })
}

fn compute_server_processing(ts: &TimestampFields) -> Option<Duration> {
    if let (Some(recv_mono), Some(send_mono)) = (ts.recv_mono, ts.send_mono) {
        let diff = send_mono.checked_sub(recv_mono)?;
        return Some(Duration::from_nanos(u64::try_from(diff).ok()?));
    }
    if let (Some(recv_wall), Some(send_wall)) = (ts.recv_wall, ts.send_wall) {
        let diff = send_wall.checked_sub(recv_wall)?;
        return Some(Duration::from_nanos(u64::try_from(diff).ok()?));
    }
    None
}

fn build_server_timing(ts: &TimestampFields) -> Option<ServerTiming> {
    if ts.recv_wall.is_none()
        && ts.recv_mono.is_none()
        && ts.send_wall.is_none()
        && ts.send_mono.is_none()
        && ts.midpoint_wall.is_none()
        && ts.midpoint_mono.is_none()
    {
        return None;
    }
    Some(ServerTiming {
        receive_wall_ns: ts.recv_wall,
        receive_mono_ns: ts.recv_mono,
        send_wall_ns: ts.send_wall,
        send_mono_ns: ts.send_mono,
        midpoint_wall_ns: ts.midpoint_wall,
        midpoint_mono_ns: ts.midpoint_mono,
        processing: compute_server_processing(ts),
    })
}

fn compute_one_way(
    sent_at: &ClientTimestamp,
    received_at: &ClientTimestamp,
    ts: &TimestampFields,
) -> Option<OneWayDelaySample> {
    let server_recv_wall = ts.recv_wall.or(ts.midpoint_wall)?;
    let server_send_wall = ts.send_wall.or(ts.midpoint_wall)?;

    let client_send_ns = sent_at
        .wall
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_nanos() as i64;
    let client_recv_ns = received_at
        .wall
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_nanos() as i64;

    let c2s = server_recv_wall
        .checked_sub(client_send_ns)
        .and_then(|d| u64::try_from(d).ok().map(Duration::from_nanos));
    let s2c = client_recv_ns
        .checked_sub(server_send_wall)
        .and_then(|d| u64::try_from(d).ok().map(Duration::from_nanos));

    Some(OneWayDelaySample {
        client_to_server: c2s,
        server_to_client: s2c,
    })
}

fn build_received_stats(reply: &EchoReply) -> Option<ReceivedStatsSample> {
    if reply.recv_count.is_none() && reply.recv_window.is_none() {
        return None;
    }
    Some(ReceivedStatsSample {
        count: reply.recv_count,
        window: reply.recv_window,
    })
}

fn params_from_config(config: &ClientConfig) -> Result<Params, ClientError> {
    Ok(Params {
        protocol_version: PROTOCOL_VERSION,
        duration_ns: duration_to_ns(config.duration.unwrap_or_default())?,
        interval_ns: duration_to_ns(config.interval)?,
        length: i64::from(config.length),
        received_stats: config.received_stats,
        stamp_at: config.stamp_at,
        clock: config.clock,
        dscp: i64::from(config.dscp),
        server_fill: config.server_fill.clone().map(|value| ServerFill { value }),
    })
}

fn duration_to_ns(duration: Duration) -> Result<i64, ClientError> {
    i64::try_from(duration.as_nanos()).map_err(|_| ClientError::DurationOverflow)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{NegotiationPolicy, DEFAULT_OPEN_TIMEOUTS};
    use irtt_proto::{
        compute_hmac_in_place, echo_packet_len, flags::FLAG_HMAC, flags::FLAG_OPEN,
        flags::FLAG_REPLY, layout::PacketLayout, verify_hmac, Clock, ReceivedStats, StampAt,
        HMAC_SIZE, MAGIC,
    };
    use std::{
        net::UdpSocket,
        sync::mpsc,
        thread::{self, JoinHandle},
        time::SystemTime,
    };

    const TOKEN: u64 = 0x1234_5678_90ab_cdef;
    const HMAC_OFFSET: usize = 4;

    struct FakeServer {
        addr: SocketAddr,
        rx: mpsc::Receiver<Vec<u8>>,
        done: JoinHandle<()>,
    }

    impl FakeServer {
        fn join(self) {
            self.done.join().unwrap();
        }
    }

    fn default_test_config(addr: SocketAddr) -> ClientConfig {
        ClientConfig {
            server_addr: addr.to_string(),
            open_timeouts: vec![Duration::from_millis(200), Duration::from_millis(200)],
            ..ClientConfig::default()
        }
    }

    fn start_fake_server<F>(handler: F) -> FakeServer
    where
        F: FnOnce(UdpSocket, mpsc::Sender<Vec<u8>>) + Send + 'static,
    {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = socket.local_addr().unwrap();
        let (tx, rx) = mpsc::channel();
        let done = thread::spawn(move || handler(socket, tx));
        FakeServer { addr, rx, done }
    }

    fn recv_request(socket: &UdpSocket, tx: &mpsc::Sender<Vec<u8>>) -> (Vec<u8>, SocketAddr) {
        let mut buf = [0_u8; 512];
        let (size, peer) = socket.recv_from(&mut buf).unwrap();
        let packet = buf[..size].to_vec();
        tx.send(packet.clone()).unwrap();
        (packet, peer)
    }

    fn open_reply(flags: u8, token: u64, params: &Params, hmac_key: Option<&[u8]>) -> Vec<u8> {
        let mut packet = Vec::new();
        packet.extend_from_slice(&MAGIC);
        packet.push(flags | hmac_key.map_or(0, |_| FLAG_HMAC));
        if hmac_key.is_some() {
            packet.extend_from_slice(&[0_u8; HMAC_SIZE]);
        }
        packet.extend_from_slice(&token.to_le_bytes());
        packet.extend_from_slice(&params.encode());
        if let Some(key) = hmac_key {
            compute_hmac_in_place(key, &mut packet, HMAC_OFFSET).unwrap();
        }
        packet
    }

    fn echo_reply_packet(
        token: u64,
        seq: u32,
        params: &Params,
        timestamps: &TimestampFields,
        hmac_key: Option<&[u8]>,
    ) -> Vec<u8> {
        let has_hmac = hmac_key.is_some();
        let layout = PacketLayout::echo(has_hmac, params);
        let packet_len = echo_packet_len(has_hmac, params);
        let mut packet = Vec::with_capacity(packet_len);

        let mut flags = FLAG_REPLY;
        if has_hmac {
            flags |= FLAG_HMAC;
        }
        packet.extend_from_slice(&MAGIC);
        packet.push(flags);
        if has_hmac {
            packet.extend_from_slice(&[0_u8; HMAC_SIZE]);
        }
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

        if let Some(key) = hmac_key {
            compute_hmac_in_place(key, &mut packet, HMAC_OFFSET).unwrap();
        }
        packet
    }

    fn open_success_server(params: Params) -> FakeServer {
        start_fake_server(move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
            socket.send_to(&reply, peer).unwrap();
        })
    }

    fn no_test_server(params: Params, token: u64) -> FakeServer {
        start_fake_server(move |socket, tx| {
            let (request, peer) = recv_request(&socket, &tx);
            assert_eq!(request[3] & flags::FLAG_CLOSE, flags::FLAG_CLOSE);
            let reply = open_reply(
                FLAG_OPEN | FLAG_REPLY | flags::FLAG_CLOSE,
                token,
                &params,
                None,
            );
            socket.send_to(&reply, peer).unwrap();
        })
    }

    fn timeout_server(wait: Duration) -> FakeServer {
        start_fake_server(move |socket, tx| {
            socket.set_read_timeout(Some(wait)).unwrap();
            while recv_request_timeout(&socket, &tx).is_some() {}
        })
    }

    fn recv_request_timeout(
        socket: &UdpSocket,
        tx: &mpsc::Sender<Vec<u8>>,
    ) -> Option<(Vec<u8>, SocketAddr)> {
        let mut buf = [0_u8; 512];
        match socket.recv_from(&mut buf) {
            Ok((size, peer)) => {
                let packet = buf[..size].to_vec();
                tx.send(packet.clone()).unwrap();
                Some((packet, peer))
            }
            Err(_) => None,
        }
    }

    fn echo_server(params: Params) -> FakeServer {
        start_fake_server(move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
            socket.send_to(&reply, peer).unwrap();

            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            loop {
                let mut buf = [0_u8; 2048];
                let size = match socket.recv_from(&mut buf) {
                    Ok((size, _)) => size,
                    Err(_) => break,
                };
                let packet = buf[..size].to_vec();
                tx.send(packet.clone()).unwrap();

                if buf[3] & flags::FLAG_CLOSE != 0 {
                    break;
                }

                let seq = u32::from_le_bytes(buf[12..16].try_into().unwrap());
                let ts = TimestampFields {
                    recv_wall: Some(1_000_000_000),
                    recv_mono: Some(100_000),
                    send_wall: Some(1_000_100_000),
                    send_mono: Some(200_000),
                    ..Default::default()
                };
                let reply_packet = echo_reply_packet(TOKEN, seq, &params, &ts, None);
                socket.send_to(&reply_packet, peer).unwrap();
            }
        })
    }

    fn assert_open_started(outcome: OpenOutcome) -> NegotiatedParams {
        match outcome {
            OpenOutcome::Started {
                token, negotiated, ..
            } => {
                assert_eq!(token, TOKEN);
                negotiated
            }
            OpenOutcome::NoTestCompleted { .. } => panic!("unexpected no-test outcome"),
        }
    }

    fn assert_no_test_completed(outcome: OpenOutcome) -> NegotiatedParams {
        match outcome {
            OpenOutcome::NoTestCompleted {
                negotiated, event, ..
            } => {
                assert!(matches!(
                    event,
                    ClientEvent::NoTestCompleted {
                        negotiated: ref event_params,
                        ..
                    } if *event_params == negotiated
                ));
                negotiated
            }
            OpenOutcome::Started { .. } => panic!("unexpected started outcome"),
        }
    }

    fn open_client_with_echo_server(params: &Params) -> (Client, FakeServer) {
        let server = echo_server(params.clone());
        let config = ClientConfig {
            socket_config: crate::SocketConfig {
                recv_timeout: Some(Duration::from_millis(200)),
                ..Default::default()
            },
            ..default_test_config(server.addr)
        };
        let mut client = Client::connect(config).unwrap();
        assert_open_started(client.open(ClientTimestamp::now()).unwrap());
        (client, server)
    }

    fn default_params() -> Params {
        Params {
            protocol_version: 1,
            duration_ns: 3_000_000_000,
            interval_ns: 1_000_000_000,
            received_stats: ReceivedStats::Both,
            stamp_at: StampAt::Both,
            clock: Clock::Both,
            ..Params::default()
        }
    }

    // ---------- Existing Milestone 2 tests ----------

    #[test]
    fn client_config_default() {
        let config = ClientConfig::default();
        assert_eq!(config.duration, Some(Duration::from_secs(3)));
        assert_eq!(config.interval, Duration::from_secs(1));
        assert_eq!(config.length, 0);
        assert_eq!(config.received_stats, ReceivedStats::Both);
        assert_eq!(config.stamp_at, StampAt::Both);
        assert_eq!(config.clock, Clock::Both);
        assert_eq!(config.dscp, 0);
        assert_eq!(config.hmac_key, None);
        assert_eq!(config.server_fill, None);
        assert_eq!(config.open_timeouts, DEFAULT_OPEN_TIMEOUTS);
        assert_eq!(config.run_mode, RunMode::Normal);
        assert_eq!(config.negotiation_policy, NegotiationPolicy::Strict);
        assert_eq!(config.probe_timeout, Duration::from_secs(4));
        assert_eq!(config.max_pending_probes, 4096);
    }

    #[test]
    fn address_resolution_connects_to_local_fake_server() {
        let server = start_fake_server(|socket, tx| {
            let _ = recv_request(&socket, &tx);
        });
        let client = Client::connect(default_test_config(server.addr)).unwrap();
        client.socket.send(b"ping").unwrap();
        assert_eq!(server.rx.recv().unwrap(), b"ping");
        server.join();
    }

    #[test]
    fn successful_open_handshake() {
        let config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
        let params = params_from_config(&config).unwrap();
        let server = open_success_server(params.clone());
        let mut client = Client::connect(default_test_config(server.addr)).unwrap();

        let negotiated = assert_open_started(client.open(ClientTimestamp::now()).unwrap());
        assert_eq!(negotiated.params, params);
        assert!(matches!(client.phase, ClientPhase::Open { token: TOKEN }));
        server.join();
    }

    #[test]
    fn open_fails_when_already_open() {
        let config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
        let params = params_from_config(&config).unwrap();
        let server = open_success_server(params);
        let mut client = Client::connect(default_test_config(server.addr)).unwrap();
        assert_open_started(client.open(ClientTimestamp::now()).unwrap());
        assert!(matches!(
            client.open(ClientTimestamp::now()),
            Err(ClientError::AlreadyOpen)
        ));
        server.join();
    }

    #[test]
    fn open_fails_after_close() {
        let config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
        let params = params_from_config(&config).unwrap();
        let server = start_fake_server(move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
            socket.send_to(&reply, peer).unwrap();
            let _ = recv_request(&socket, &tx);
        });
        let mut client = Client::connect(default_test_config(server.addr)).unwrap();
        assert_open_started(client.open(ClientTimestamp::now()).unwrap());
        client.close(ClientTimestamp::now()).unwrap();
        assert!(matches!(
            client.open(ClientTimestamp::now()),
            Err(ClientError::AlreadyClosed)
        ));
        server.join();
    }

    #[test]
    fn open_fails_after_no_test_completed() {
        let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
        config.run_mode = RunMode::NoTest;
        let params = params_from_config(&config).unwrap();
        let server = no_test_server(params, 0);
        config.server_addr = server.addr.to_string();
        let mut client = Client::connect(config).unwrap();
        assert_no_test_completed(client.open(ClientTimestamp::now()).unwrap());
        assert!(matches!(
            client.open(ClientTimestamp::now()),
            Err(ClientError::AlreadyCompleted)
        ));
        server.join();
    }

    #[test]
    fn open_retries_after_first_timeout() {
        let server = start_fake_server(|socket, tx| {
            let (first, _) = recv_request(&socket, &tx);
            let (_, peer) = recv_request(&socket, &tx);
            let params = Params::decode(&first[4..]).unwrap();
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
            assert_eq!(first[3] & FLAG_OPEN, FLAG_OPEN);
            socket.send_to(&reply, peer).unwrap();
        });
        let config = ClientConfig {
            open_timeouts: vec![Duration::from_millis(200), Duration::from_millis(500)],
            ..default_test_config(server.addr)
        };
        let mut client = Client::connect(config).unwrap();
        let outcome = client.open(ClientTimestamp::now()).unwrap();
        assert_open_started(outcome);
        assert_eq!(server.rx.iter().take(2).count(), 2);
        server.join();
    }

    #[test]
    fn open_timeout_after_all_timeouts() {
        let server = timeout_server(Duration::from_millis(700));
        let config = ClientConfig {
            open_timeouts: vec![Duration::from_millis(200), Duration::from_millis(200)],
            ..default_test_config(server.addr)
        };
        let mut client = Client::connect(config).unwrap();
        assert!(matches!(
            client.open(ClientTimestamp::now()),
            Err(ClientError::OpenTimeout)
        ));
        assert_eq!(server.rx.iter().take(2).count(), 2);
        server.join();
    }

    #[test]
    fn open_restores_configured_read_timeout_after_timeout() {
        let server = timeout_server(Duration::from_millis(700));
        let config = ClientConfig {
            open_timeouts: vec![Duration::from_millis(200)],
            socket_config: crate::SocketConfig {
                recv_timeout: Some(Duration::from_millis(450)),
                ..crate::SocketConfig::default()
            },
            ..default_test_config(server.addr)
        };
        let mut client = Client::connect(config).unwrap();
        assert!(matches!(
            client.open(ClientTimestamp::now()),
            Err(ClientError::OpenTimeout)
        ));

        let start = std::time::Instant::now();
        let mut buf = [0_u8; 1];
        assert!(client.socket.recv(&mut buf).is_err());
        assert!(start.elapsed() >= Duration::from_millis(350));
        server.join();
    }

    #[test]
    fn strict_negotiation_accepts_identical_params() {
        let config = ClientConfig::default();
        let params = params_from_config(&config).unwrap();
        assert!(validate_negotiated_params(&params, &params, NegotiationPolicy::Strict).is_ok());
    }

    #[test]
    fn strict_negotiation_rejects_changed_params() {
        let config = ClientConfig::default();
        let requested = params_from_config(&config).unwrap();
        let mut returned = requested.clone();
        returned.dscp = 1;
        assert!(matches!(
            validate_negotiated_params(&requested, &returned, NegotiationPolicy::Strict),
            Err(ClientError::NegotiationRejected { .. })
        ));
    }

    #[test]
    fn loose_negotiation_accepts_server_restricted_params() {
        let config = ClientConfig::default();
        let requested = params_from_config(&config).unwrap();
        let mut returned = requested.clone();
        returned.duration_ns /= 2;
        returned.length = 0;
        assert!(
            validate_negotiated_params(&requested, &returned, NegotiationPolicy::Loose).is_ok()
        );
    }

    #[test]
    fn protocol_version_mismatch_fails() {
        let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
        config.negotiation_policy = NegotiationPolicy::Loose;
        let mut params = params_from_config(&config).unwrap();
        params.protocol_version = 2;
        let server = open_success_server(params);
        let mut client = Client::connect(default_test_config(server.addr)).unwrap();
        assert!(matches!(
            client.open(ClientTimestamp::now()),
            Err(ClientError::ProtocolVersionMismatch { received: 2, .. })
        ));
        server.join();
    }

    #[test]
    fn server_rejection_fails_in_normal_mode() {
        let config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
        let params = params_from_config(&config).unwrap();
        let server = start_fake_server(move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY | flags::FLAG_CLOSE, 0, &params, None);
            socket.send_to(&reply, peer).unwrap();
        });
        let mut client = Client::connect(default_test_config(server.addr)).unwrap();
        assert!(matches!(
            client.open(ClientTimestamp::now()),
            Err(ClientError::ServerRejected)
        ));
        server.join();
    }

    #[test]
    fn no_test_open_close_succeeds_on_open_reply_close() {
        let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
        config.run_mode = RunMode::NoTest;
        let params = params_from_config(&config).unwrap();
        let server = no_test_server(params.clone(), 0);
        config.server_addr = server.addr.to_string();
        let mut client = Client::connect(config).unwrap();
        let negotiated = assert_no_test_completed(client.open(ClientTimestamp::now()).unwrap());
        assert_eq!(negotiated.params, params);
        assert_eq!(client.negotiated.as_ref(), Some(&negotiated));
        assert!(matches!(
            client.close(ClientTimestamp::now()),
            Err(ClientError::NotOpen)
        ));
        server.join();
    }

    #[test]
    fn no_test_success_validates_params() {
        let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
        config.run_mode = RunMode::NoTest;
        let params = params_from_config(&config).unwrap();
        let server = no_test_server(params.clone(), 0);
        config.server_addr = server.addr.to_string();
        let mut client = Client::connect(config).unwrap();
        let negotiated = assert_no_test_completed(client.open(ClientTimestamp::now()).unwrap());
        assert_eq!(negotiated.params, params);
        server.join();
    }

    #[test]
    fn no_test_rejects_non_close_open_reply() {
        let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
        config.run_mode = RunMode::NoTest;
        let params = params_from_config(&config).unwrap();
        let server = open_success_server(params);
        config.server_addr = server.addr.to_string();
        let mut client = Client::connect(config).unwrap();
        assert!(matches!(
            client.open(ClientTimestamp::now()),
            Err(ClientError::UnexpectedNoTestReply)
        ));
        server.join();
    }

    #[test]
    fn no_test_rejects_non_zero_token_with_close_reply() {
        let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
        config.run_mode = RunMode::NoTest;
        let params = params_from_config(&config).unwrap();
        let server = no_test_server(params, TOKEN);
        config.server_addr = server.addr.to_string();
        let mut client = Client::connect(config).unwrap();
        assert!(matches!(
            client.open(ClientTimestamp::now()),
            Err(ClientError::NonZeroNoTestToken { token: TOKEN })
        ));
        server.join();
    }

    #[test]
    fn no_test_strict_negotiation_rejects_changed_params() {
        let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
        config.run_mode = RunMode::NoTest;
        let mut params = params_from_config(&config).unwrap();
        params.dscp = 1;
        let server = no_test_server(params, 0);
        config.server_addr = server.addr.to_string();
        let mut client = Client::connect(config).unwrap();
        assert!(matches!(
            client.open(ClientTimestamp::now()),
            Err(ClientError::NegotiationRejected { .. })
        ));
        server.join();
    }

    #[test]
    fn no_test_loose_negotiation_accepts_restricted_params() {
        let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
        config.run_mode = RunMode::NoTest;
        config.negotiation_policy = NegotiationPolicy::Loose;
        let mut params = params_from_config(&config).unwrap();
        params.duration_ns /= 2;
        let server = no_test_server(params.clone(), 0);
        config.server_addr = server.addr.to_string();
        let mut client = Client::connect(config).unwrap();
        let negotiated = assert_no_test_completed(client.open(ClientTimestamp::now()).unwrap());
        assert_eq!(negotiated.params, params);
        server.join();
    }

    #[test]
    fn close_sends_one_close_packet_with_negotiated_token() {
        let config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
        let params = params_from_config(&config).unwrap();
        let server = start_fake_server(move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
            socket.send_to(&reply, peer).unwrap();
            let _ = recv_request(&socket, &tx);
        });
        let mut client = Client::connect(default_test_config(server.addr)).unwrap();
        assert_open_started(client.open(ClientTimestamp::now()).unwrap());
        let events = client.close(ClientTimestamp::now()).unwrap();
        assert_eq!(events.len(), 1);
        let packets: Vec<_> = server.rx.iter().take(2).collect();
        let close = &packets[1];
        assert_eq!(close[3], flags::FLAG_CLOSE);
        assert_eq!(u64::from_le_bytes(close[4..12].try_into().unwrap()), TOKEN);
        server.join();
    }

    #[test]
    fn hmac_open_success() {
        let key = b"secret".to_vec();
        let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
        config.hmac_key = Some(key.clone());
        let params = params_from_config(&config).unwrap();
        let server = start_fake_server(move |socket, tx| {
            let (request, peer) = recv_request(&socket, &tx);
            verify_hmac(&key, &request, HMAC_OFFSET).unwrap();
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, Some(&key));
            socket.send_to(&reply, peer).unwrap();
        });
        config.server_addr = server.addr.to_string();
        let mut client = Client::connect(config).unwrap();
        assert_open_started(client.open(ClientTimestamp::now()).unwrap());
        server.join();
    }

    #[test]
    fn hmac_open_rejects_missing_hmac() {
        let key = b"secret".to_vec();
        let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
        config.hmac_key = Some(key);
        let params = params_from_config(&config).unwrap();
        let server = start_fake_server(move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
            socket.send_to(&reply, peer).unwrap();
        });
        config.server_addr = server.addr.to_string();
        let mut client = Client::connect(config).unwrap();
        assert!(matches!(
            client.open(ClientTimestamp::now()),
            Err(ClientError::Protocol(
                irtt_proto::ProtoError::HmacPresenceMismatch
            ))
        ));
        server.join();
    }

    #[test]
    fn hmac_open_rejects_bad_hmac() {
        let key = b"secret".to_vec();
        let wrong_key = b"wrong".to_vec();
        let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
        config.hmac_key = Some(key);
        let params = params_from_config(&config).unwrap();
        let server = start_fake_server(move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, Some(&wrong_key));
            socket.send_to(&reply, peer).unwrap();
        });
        config.server_addr = server.addr.to_string();
        let mut client = Client::connect(config).unwrap();
        assert!(matches!(
            client.open(ClientTimestamp::now()),
            Err(ClientError::Protocol(irtt_proto::ProtoError::BadHmac))
        ));
        server.join();
    }

    #[test]
    fn hmac_close_packet_includes_valid_hmac() {
        let key = b"secret".to_vec();
        let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
        config.hmac_key = Some(key.clone());
        let params = params_from_config(&config).unwrap();
        let server_key = key.clone();
        let server = start_fake_server(move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, Some(&server_key));
            socket.send_to(&reply, peer).unwrap();
            let _ = recv_request(&socket, &tx);
        });
        config.server_addr = server.addr.to_string();
        let mut client = Client::connect(config).unwrap();
        assert_open_started(client.open(ClientTimestamp::now()).unwrap());
        client.close(ClientTimestamp::now()).unwrap();
        let packets: Vec<_> = server.rx.iter().take(2).collect();
        let close = &packets[1];
        assert_eq!(close[3], flags::FLAG_CLOSE | FLAG_HMAC);
        verify_hmac(&key, close, HMAC_OFFSET).unwrap();
        assert_eq!(
            u64::from_le_bytes(close[4 + HMAC_SIZE..12 + HMAC_SIZE].try_into().unwrap()),
            TOKEN
        );
        server.join();
    }

    #[test]
    fn minimum_open_timeout_under_200ms_is_rejected() {
        let config = ClientConfig {
            open_timeouts: vec![Duration::from_millis(199)],
            ..ClientConfig::default()
        };
        assert!(matches!(
            Client::connect(config),
            Err(ClientError::OpenTimeoutTooSmall { .. })
        ));
    }

    #[test]
    fn empty_open_timeouts_is_rejected() {
        let config = ClientConfig {
            open_timeouts: vec![],
            ..ClientConfig::default()
        };
        assert!(matches!(
            Client::connect(config),
            Err(ClientError::NoOpenTimeouts)
        ));
    }

    // ---------- Milestone 3 probe tests ----------

    #[test]
    fn send_probe_fails_before_open() {
        let server = start_fake_server(|_socket, _tx| {});
        let mut client = Client::connect(default_test_config(server.addr)).unwrap();
        assert!(matches!(
            client.send_probe(ClientTimestamp::now()),
            Err(ClientError::NotOpen)
        ));
        server.join();
    }

    #[test]
    fn send_probe_fails_after_no_test_completed() {
        let mut config = default_test_config(SocketAddr::from(([127, 0, 0, 1], 1)));
        config.run_mode = RunMode::NoTest;
        let params = params_from_config(&config).unwrap();
        let server = no_test_server(params, 0);
        config.server_addr = server.addr.to_string();
        let mut client = Client::connect(config).unwrap();
        assert_no_test_completed(client.open(ClientTimestamp::now()).unwrap());
        assert!(matches!(
            client.send_probe(ClientTimestamp::now()),
            Err(ClientError::AlreadyCompleted)
        ));
        server.join();
    }

    #[test]
    fn send_probe_fails_after_close() {
        let params = default_params();
        let server = start_fake_server(move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
            socket.send_to(&reply, peer).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            loop {
                let mut buf = [0_u8; 512];
                match socket.recv_from(&mut buf) {
                    Ok((size, _)) => {
                        tx.send(buf[..size].to_vec()).unwrap();
                    }
                    Err(_) => break,
                }
            }
        });
        let mut client = Client::connect(default_test_config(server.addr)).unwrap();
        assert_open_started(client.open(ClientTimestamp::now()).unwrap());
        client.close(ClientTimestamp::now()).unwrap();
        assert!(matches!(
            client.send_probe(ClientTimestamp::now()),
            Err(ClientError::AlreadyClosed)
        ));
        server.join();
    }

    fn silent_open_server(params: Params) -> FakeServer {
        start_fake_server(move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
            socket.send_to(&reply, peer).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            loop {
                let mut buf = [0_u8; 2048];
                match socket.recv_from(&mut buf) {
                    Ok((size, _)) => {
                        tx.send(buf[..size].to_vec()).unwrap();
                    }
                    Err(_) => break,
                }
            }
        })
    }

    #[test]
    fn send_probe_sends_valid_echo_request() {
        let params = default_params();
        let server = silent_open_server(params);
        let config = ClientConfig {
            socket_config: crate::SocketConfig {
                recv_timeout: Some(Duration::from_millis(200)),
                ..Default::default()
            },
            ..default_test_config(server.addr)
        };
        let mut client = Client::connect(config).unwrap();
        assert_open_started(client.open(ClientTimestamp::now()).unwrap());
        client.send_probe(ClientTimestamp::now()).unwrap();
        thread::sleep(Duration::from_millis(30));
        let packets: Vec<_> = server.rx.try_iter().collect();
        let echo_reqs: Vec<_> = packets
            .iter()
            .filter(|p| p.len() >= 4 && p[3] & FLAG_OPEN == 0)
            .collect();
        let echo_req = echo_reqs.first().unwrap();
        assert_eq!(&echo_req[..3], &MAGIC);
        assert_eq!(echo_req[3], 0x00);
        let req_token = u64::from_le_bytes(echo_req[4..12].try_into().unwrap());
        assert_eq!(req_token, TOKEN);
        let seq = u32::from_le_bytes(echo_req[12..16].try_into().unwrap());
        assert_eq!(seq, 0);
        client.close(ClientTimestamp::now()).unwrap();
        server.join();
    }

    #[test]
    fn send_probe_starts_seq_at_zero_and_increments() {
        let params = default_params();
        let server = silent_open_server(params);
        let config = ClientConfig {
            socket_config: crate::SocketConfig {
                recv_timeout: Some(Duration::from_millis(200)),
                ..Default::default()
            },
            ..default_test_config(server.addr)
        };
        let mut client = Client::connect(config).unwrap();
        assert_open_started(client.open(ClientTimestamp::now()).unwrap());
        client.send_probe(ClientTimestamp::now()).unwrap();
        client.send_probe(ClientTimestamp::now()).unwrap();
        client.send_probe(ClientTimestamp::now()).unwrap();
        thread::sleep(Duration::from_millis(30));
        let packets: Vec<_> = server.rx.try_iter().collect();
        let echo_reqs: Vec<_> = packets
            .iter()
            .filter(|p| p.len() >= 4 && p[3] & FLAG_OPEN == 0 && p[3] & flags::FLAG_CLOSE == 0)
            .collect();
        assert_eq!(echo_reqs.len(), 3);
        for (i, pkt) in echo_reqs.iter().enumerate() {
            let seq = u32::from_le_bytes(pkt[12..16].try_into().unwrap());
            assert_eq!(seq, i as u32);
        }
        client.close(ClientTimestamp::now()).unwrap();
        server.join();
    }

    #[test]
    fn send_probe_respects_finite_duration_exclusive_end() {
        let params = Params {
            protocol_version: 1,
            duration_ns: 1_000_000_000,
            interval_ns: 500_000_000,
            received_stats: ReceivedStats::Both,
            stamp_at: StampAt::Both,
            clock: Clock::Both,
            ..Params::default()
        };
        let server = silent_open_server(params.clone());
        let config = ClientConfig {
            duration: Some(Duration::from_secs(1)),
            interval: Duration::from_millis(500),
            socket_config: crate::SocketConfig {
                recv_timeout: Some(Duration::from_millis(200)),
                ..Default::default()
            },
            ..default_test_config(server.addr)
        };
        let mut client = Client::connect(config).unwrap();
        assert_open_started(client.open(ClientTimestamp::now()).unwrap());

        let session = client.session.as_ref().unwrap();
        let start = session.start_mono;
        let interval = Duration::from_millis(500);

        let now0 = ClientTimestamp {
            mono: start,
            wall: SystemTime::now(),
        };
        assert!(client.send_probe(now0).is_ok());

        let now1 = ClientTimestamp {
            mono: start + interval,
            wall: SystemTime::now(),
        };
        assert!(client.send_probe(now1).is_ok());

        let now2 = ClientTimestamp {
            mono: start + Duration::from_secs(1),
            wall: SystemTime::now(),
        };
        let events = client.send_probe(now2).unwrap();
        assert!(events.is_empty());
        assert!(client.session.as_ref().unwrap().sending_done);
        assert!(client.next_send_deadline().is_none());

        client.close(ClientTimestamp::now()).unwrap();
        server.join();
    }

    #[test]
    fn next_send_deadline_advances_by_interval() {
        let params = default_params();
        let (mut client, server) = open_client_with_echo_server(&params);

        let session = client.session.as_ref().unwrap();
        let start = session.start_mono;
        let deadline0 = client.next_send_deadline().unwrap();
        assert_eq!(deadline0, start);

        client.send_probe(ClientTimestamp::now()).unwrap();
        let deadline1 = client.next_send_deadline().unwrap();
        assert_eq!(deadline1, start + Duration::from_secs(1));

        client.send_probe(ClientTimestamp::now()).unwrap();
        let deadline2 = client.next_send_deadline().unwrap();
        assert_eq!(deadline2, start + Duration::from_secs(2));

        client.close(ClientTimestamp::now()).unwrap();
        server.join();
    }

    #[test]
    fn recv_once_returns_empty_on_timeout() {
        let params = default_params();
        let server = open_success_server(params);
        let config = ClientConfig {
            socket_config: crate::SocketConfig {
                recv_timeout: Some(Duration::from_millis(50)),
                ..Default::default()
            },
            ..default_test_config(server.addr)
        };
        let mut client = Client::connect(config).unwrap();
        assert_open_started(client.open(ClientTimestamp::now()).unwrap());
        let events = client.recv_once(ClientTimestamp::now()).unwrap();
        assert!(events.is_empty());
        server.join();
    }

    #[test]
    fn recv_once_decodes_echo_reply_and_emits_event() {
        let params = default_params();
        let (mut client, server) = open_client_with_echo_server(&params);

        let send_ts = ClientTimestamp::now();
        client.send_probe(send_ts).unwrap();
        thread::sleep(Duration::from_millis(50));

        let recv_ts = ClientTimestamp::now();
        let events = client.recv_once(recv_ts).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            ClientEvent::EchoReply {
                seq,
                logical_seq,
                rtt,
                received_stats,
                server_timing,
                ..
            } => {
                assert_eq!(*seq, 0);
                assert_eq!(*logical_seq, 0);
                assert!(rtt.raw > Duration::ZERO);
                assert_eq!(rtt.effective, rtt.adjusted.unwrap_or(rtt.raw));
                assert!(received_stats.is_some());
                let stats = received_stats.as_ref().unwrap();
                assert_eq!(stats.count, Some(42));
                assert_eq!(stats.window, Some(0x07));
                assert!(server_timing.is_some());
                let st = server_timing.as_ref().unwrap();
                assert!(st.processing.is_some());
            }
            other => panic!("expected EchoReply, got {other:?}"),
        }
        client.close(ClientTimestamp::now()).unwrap();
        server.join();
    }

    #[test]
    fn echo_reply_rtt_uses_client_monotonic() {
        let params = default_params();
        let (mut client, server) = open_client_with_echo_server(&params);

        let send_ts = ClientTimestamp::now();
        client.send_probe(send_ts).unwrap();
        thread::sleep(Duration::from_millis(20));

        let recv_ts = ClientTimestamp::now();
        let events = client.recv_once(recv_ts).unwrap();
        if let ClientEvent::EchoReply { rtt, .. } = &events[0] {
            assert!(rtt.raw >= Duration::from_millis(15));
        } else {
            panic!("expected EchoReply");
        }
        client.close(ClientTimestamp::now()).unwrap();
        server.join();
    }

    #[test]
    fn server_processing_subtracted_when_valid() {
        let params = default_params();
        let (mut client, server) = open_client_with_echo_server(&params);
        client.send_probe(ClientTimestamp::now()).unwrap();
        thread::sleep(Duration::from_millis(20));
        let events = client.recv_once(ClientTimestamp::now()).unwrap();
        if let ClientEvent::EchoReply {
            rtt, server_timing, ..
        } = &events[0]
        {
            let st = server_timing.as_ref().unwrap();
            let processing = st.processing.unwrap();
            assert!(processing > Duration::ZERO);
            if let Some(adj) = rtt.adjusted {
                assert!(adj < rtt.raw);
                assert_eq!(rtt.effective, adj);
            }
        } else {
            panic!("expected EchoReply");
        }
        client.close(ClientTimestamp::now()).unwrap();
        server.join();
    }

    #[test]
    fn server_processing_greater_than_raw_does_not_underflow() {
        let rtt = compute_rtt(
            &ClientTimestamp {
                mono: Instant::now(),
                wall: SystemTime::now(),
            },
            &ClientTimestamp {
                mono: Instant::now() + Duration::from_nanos(1),
                wall: SystemTime::now(),
            },
            &TimestampFields {
                recv_mono: Some(0),
                send_mono: Some(1_000_000_000),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(rtt.adjusted.is_none());
        assert_eq!(rtt.effective, rtt.raw);
    }

    #[test]
    fn received_stats_parsed_into_sample() {
        let params = default_params();
        let (mut client, server) = open_client_with_echo_server(&params);
        client.send_probe(ClientTimestamp::now()).unwrap();
        thread::sleep(Duration::from_millis(30));
        let events = client.recv_once(ClientTimestamp::now()).unwrap();
        if let ClientEvent::EchoReply { received_stats, .. } = &events[0] {
            let rs = received_stats.as_ref().unwrap();
            assert_eq!(rs.count, Some(42));
            assert_eq!(rs.window, Some(0x07));
        } else {
            panic!("expected EchoReply");
        }
        client.close(ClientTimestamp::now()).unwrap();
        server.join();
    }

    #[test]
    fn wrong_token_reply_is_dropped() {
        let params = default_params();
        let wrong_token: u64 = 0xDEAD_BEEF_CAFE_BABE;
        let server = start_fake_server(move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
            socket.send_to(&reply, peer).unwrap();

            socket
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut buf = [0_u8; 2048];
            if let Ok((size, _)) = socket.recv_from(&mut buf) {
                tx.send(buf[..size].to_vec()).unwrap();
                let seq = u32::from_le_bytes(buf[12..16].try_into().unwrap());
                let ts = TimestampFields::default();
                let reply_packet = echo_reply_packet(wrong_token, seq, &params, &ts, None);
                socket.send_to(&reply_packet, peer).unwrap();
            }
        });
        let config = ClientConfig {
            socket_config: crate::SocketConfig {
                recv_timeout: Some(Duration::from_millis(200)),
                ..Default::default()
            },
            ..default_test_config(server.addr)
        };
        let mut client = Client::connect(config).unwrap();
        assert_open_started(client.open(ClientTimestamp::now()).unwrap());
        client.send_probe(ClientTimestamp::now()).unwrap();
        thread::sleep(Duration::from_millis(30));
        let events = client.recv_once(ClientTimestamp::now()).unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], ClientEvent::Warning { .. }));
        server.join();
    }

    #[test]
    fn bad_hmac_reply_is_dropped() {
        let key = b"secret".to_vec();
        let wrong_key = b"wrong".to_vec();
        let params = default_params();
        let server_key = key.clone();
        let server = start_fake_server(move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, Some(&server_key));
            socket.send_to(&reply, peer).unwrap();

            socket
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut buf = [0_u8; 2048];
            if let Ok((size, _)) = socket.recv_from(&mut buf) {
                tx.send(buf[..size].to_vec()).unwrap();
                let seq = u32::from_le_bytes(
                    buf[4 + HMAC_SIZE + 8..4 + HMAC_SIZE + 12]
                        .try_into()
                        .unwrap(),
                );
                let ts = TimestampFields::default();
                let reply_packet = echo_reply_packet(TOKEN, seq, &params, &ts, Some(&wrong_key));
                socket.send_to(&reply_packet, peer).unwrap();
            }
        });
        let config = ClientConfig {
            hmac_key: Some(key),
            socket_config: crate::SocketConfig {
                recv_timeout: Some(Duration::from_millis(200)),
                ..Default::default()
            },
            ..default_test_config(server.addr)
        };
        let mut client = Client::connect(config).unwrap();
        assert_open_started(client.open(ClientTimestamp::now()).unwrap());
        client.send_probe(ClientTimestamp::now()).unwrap();
        thread::sleep(Duration::from_millis(30));
        let events = client.recv_once(ClientTimestamp::now()).unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], ClientEvent::Warning { .. }));
        server.join();
    }

    #[test]
    fn duplicate_reply_emits_duplicate_event() {
        let params = default_params();
        let server = start_fake_server(move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
            socket.send_to(&reply, peer).unwrap();

            socket
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut buf = [0_u8; 2048];
            if let Ok((size, _)) = socket.recv_from(&mut buf) {
                tx.send(buf[..size].to_vec()).unwrap();
                let seq = u32::from_le_bytes(buf[12..16].try_into().unwrap());
                let ts = TimestampFields::default();
                let reply_packet = echo_reply_packet(TOKEN, seq, &params, &ts, None);
                socket.send_to(&reply_packet, peer).unwrap();
                thread::sleep(Duration::from_millis(10));
                socket.send_to(&reply_packet, peer).unwrap();
            }
        });
        let config = ClientConfig {
            socket_config: crate::SocketConfig {
                recv_timeout: Some(Duration::from_millis(200)),
                ..Default::default()
            },
            ..default_test_config(server.addr)
        };
        let mut client = Client::connect(config).unwrap();
        assert_open_started(client.open(ClientTimestamp::now()).unwrap());
        client.send_probe(ClientTimestamp::now()).unwrap();
        thread::sleep(Duration::from_millis(50));

        let events1 = client.recv_once(ClientTimestamp::now()).unwrap();
        assert_eq!(events1.len(), 1);
        assert!(matches!(&events1[0], ClientEvent::EchoReply { .. }));

        thread::sleep(Duration::from_millis(30));
        let events2 = client.recv_once(ClientTimestamp::now()).unwrap();
        assert_eq!(events2.len(), 1);
        assert!(matches!(
            &events2[0],
            ClientEvent::DuplicateReply { seq: 0, .. }
        ));
        server.join();
    }

    #[test]
    fn out_of_order_reply_emits_late_event() {
        let params = default_params();
        let server = start_fake_server(move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
            socket.send_to(&reply, peer).unwrap();

            socket
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();

            let mut seqs = Vec::new();
            for _ in 0..2 {
                let mut buf = [0_u8; 2048];
                if let Ok((size, _)) = socket.recv_from(&mut buf) {
                    tx.send(buf[..size].to_vec()).unwrap();
                    let seq = u32::from_le_bytes(buf[12..16].try_into().unwrap());
                    seqs.push(seq);
                }
            }
            let ts = TimestampFields::default();
            let reply1 = echo_reply_packet(TOKEN, seqs[1], &params, &ts, None);
            socket.send_to(&reply1, peer).unwrap();
            thread::sleep(Duration::from_millis(10));
            let reply0 = echo_reply_packet(TOKEN, seqs[0], &params, &ts, None);
            socket.send_to(&reply0, peer).unwrap();
        });
        let config = ClientConfig {
            socket_config: crate::SocketConfig {
                recv_timeout: Some(Duration::from_millis(200)),
                ..Default::default()
            },
            ..default_test_config(server.addr)
        };
        let mut client = Client::connect(config).unwrap();
        assert_open_started(client.open(ClientTimestamp::now()).unwrap());
        client.send_probe(ClientTimestamp::now()).unwrap();
        client.send_probe(ClientTimestamp::now()).unwrap();
        thread::sleep(Duration::from_millis(50));

        let ev1 = client.recv_once(ClientTimestamp::now()).unwrap();
        assert_eq!(ev1.len(), 1);
        assert!(matches!(&ev1[0], ClientEvent::EchoReply { seq: 1, .. }));

        thread::sleep(Duration::from_millis(30));
        let ev2 = client.recv_once(ClientTimestamp::now()).unwrap();
        assert_eq!(ev2.len(), 1);
        match &ev2[0] {
            ClientEvent::LateReply {
                seq,
                highest_seen,
                rtt,
                ..
            } => {
                assert_eq!(*seq, 0);
                assert_eq!(*highest_seen, 1);
                assert!(rtt.is_some());
            }
            other => panic!("expected LateReply, got {other:?}"),
        }
        server.join();
    }

    #[test]
    fn poll_timeouts_emits_echo_loss() {
        let params = default_params();
        let server = silent_open_server(params);
        let config = ClientConfig {
            probe_timeout: Duration::from_millis(100),
            socket_config: crate::SocketConfig {
                recv_timeout: Some(Duration::from_millis(50)),
                ..Default::default()
            },
            ..default_test_config(server.addr)
        };
        let mut client = Client::connect(config).unwrap();
        assert_open_started(client.open(ClientTimestamp::now()).unwrap());

        client.send_probe(ClientTimestamp::now()).unwrap();
        client.send_probe(ClientTimestamp::now()).unwrap();

        let no_loss = client.poll_timeouts(ClientTimestamp::now()).unwrap();
        assert!(no_loss.is_empty());

        thread::sleep(Duration::from_millis(150));
        let events = client.poll_timeouts(ClientTimestamp::now()).unwrap();
        assert_eq!(events.len(), 2);
        for event in &events {
            assert!(matches!(event, ClientEvent::EchoLoss { .. }));
        }
        server.join();
    }

    #[test]
    fn poll_timeouts_removes_expired_pending() {
        let params = default_params();
        let server = silent_open_server(params);
        let config = ClientConfig {
            probe_timeout: Duration::from_millis(100),
            socket_config: crate::SocketConfig {
                recv_timeout: Some(Duration::from_millis(50)),
                ..Default::default()
            },
            ..default_test_config(server.addr)
        };
        let mut client = Client::connect(config).unwrap();
        assert_open_started(client.open(ClientTimestamp::now()).unwrap());

        client.send_probe(ClientTimestamp::now()).unwrap();
        thread::sleep(Duration::from_millis(150));
        client.poll_timeouts(ClientTimestamp::now()).unwrap();

        let session = client.session.as_ref().unwrap();
        assert_eq!(session.pending.len(), 0);
        server.join();
    }

    #[test]
    fn pending_map_bounded() {
        let params = Params {
            duration_ns: 60_000_000_000,
            ..default_params()
        };
        let server = silent_open_server(params);
        let config = ClientConfig {
            duration: Some(Duration::from_secs(60)),
            max_pending_probes: 3,
            socket_config: crate::SocketConfig {
                recv_timeout: Some(Duration::from_millis(50)),
                ..Default::default()
            },
            ..default_test_config(server.addr)
        };
        let mut client = Client::connect(config).unwrap();
        assert_open_started(client.open(ClientTimestamp::now()).unwrap());
        client.send_probe(ClientTimestamp::now()).unwrap();
        client.send_probe(ClientTimestamp::now()).unwrap();
        client.send_probe(ClientTimestamp::now()).unwrap();
        assert!(matches!(
            client.send_probe(ClientTimestamp::now()),
            Err(ClientError::PendingLimitExceeded { limit: 3 })
        ));
        client.close(ClientTimestamp::now()).unwrap();
        server.join();
    }

    #[test]
    fn minimal_negotiated_layout_works() {
        let params = Params {
            protocol_version: 1,
            duration_ns: 3_000_000_000,
            interval_ns: 1_000_000_000,
            received_stats: ReceivedStats::None,
            stamp_at: StampAt::None,
            clock: Clock::Both,
            ..Params::default()
        };
        let server = start_fake_server(move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
            socket.send_to(&reply, peer).unwrap();

            socket
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut buf = [0_u8; 2048];
            if let Ok((size, _)) = socket.recv_from(&mut buf) {
                tx.send(buf[..size].to_vec()).unwrap();
                let seq = u32::from_le_bytes(buf[12..16].try_into().unwrap());
                let ts = TimestampFields::default();
                let reply_packet = echo_reply_packet(TOKEN, seq, &params, &ts, None);
                socket.send_to(&reply_packet, peer).unwrap();
            }
        });
        let config = ClientConfig {
            received_stats: ReceivedStats::None,
            stamp_at: StampAt::None,
            socket_config: crate::SocketConfig {
                recv_timeout: Some(Duration::from_millis(200)),
                ..Default::default()
            },
            ..default_test_config(server.addr)
        };
        let mut client = Client::connect(config).unwrap();
        assert_open_started(client.open(ClientTimestamp::now()).unwrap());
        client.send_probe(ClientTimestamp::now()).unwrap();
        thread::sleep(Duration::from_millis(30));
        let events = client.recv_once(ClientTimestamp::now()).unwrap();
        assert_eq!(events.len(), 1);
        if let ClientEvent::EchoReply {
            received_stats,
            server_timing,
            ..
        } = &events[0]
        {
            assert!(received_stats.is_none());
            assert!(server_timing.is_none());
        } else {
            panic!("expected EchoReply");
        }
        server.join();
    }

    #[test]
    fn hmac_echo_request_reply_works() {
        let key = b"testkey".to_vec();
        let params = default_params();
        let server_key = key.clone();
        let server = start_fake_server(move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, Some(&server_key));
            socket.send_to(&reply, peer).unwrap();

            socket
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut buf = [0_u8; 2048];
            if let Ok((size, _)) = socket.recv_from(&mut buf) {
                tx.send(buf[..size].to_vec()).unwrap();
                verify_hmac(&server_key, &buf[..size], HMAC_OFFSET).unwrap();
                let seq = u32::from_le_bytes(
                    buf[4 + HMAC_SIZE + 8..4 + HMAC_SIZE + 12]
                        .try_into()
                        .unwrap(),
                );
                let ts = TimestampFields {
                    recv_mono: Some(100),
                    send_mono: Some(200),
                    ..Default::default()
                };
                let reply_packet = echo_reply_packet(TOKEN, seq, &params, &ts, Some(&server_key));
                socket.send_to(&reply_packet, peer).unwrap();
            }
        });
        let config = ClientConfig {
            hmac_key: Some(key),
            socket_config: crate::SocketConfig {
                recv_timeout: Some(Duration::from_millis(200)),
                ..Default::default()
            },
            ..default_test_config(server.addr)
        };
        let mut client = Client::connect(config).unwrap();
        assert_open_started(client.open(ClientTimestamp::now()).unwrap());
        client.send_probe(ClientTimestamp::now()).unwrap();
        thread::sleep(Duration::from_millis(50));
        let events = client.recv_once(ClientTimestamp::now()).unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], ClientEvent::EchoReply { .. }));
        server.join();
    }

    #[test]
    fn late_reply_with_pending_preserves_rtt() {
        let params = default_params();
        let server = start_fake_server(move |socket, tx| {
            let (_, peer) = recv_request(&socket, &tx);
            let reply = open_reply(FLAG_OPEN | FLAG_REPLY, TOKEN, &params, None);
            socket.send_to(&reply, peer).unwrap();

            socket
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut seqs = Vec::new();
            for _ in 0..3 {
                let mut buf = [0_u8; 2048];
                if let Ok((size, _)) = socket.recv_from(&mut buf) {
                    tx.send(buf[..size].to_vec()).unwrap();
                    let seq = u32::from_le_bytes(buf[12..16].try_into().unwrap());
                    seqs.push(seq);
                }
            }
            let ts = TimestampFields::default();
            let reply2 = echo_reply_packet(TOKEN, seqs[2], &params, &ts, None);
            socket.send_to(&reply2, peer).unwrap();
            thread::sleep(Duration::from_millis(10));
            let reply0 = echo_reply_packet(TOKEN, seqs[0], &params, &ts, None);
            socket.send_to(&reply0, peer).unwrap();
        });
        let config = ClientConfig {
            socket_config: crate::SocketConfig {
                recv_timeout: Some(Duration::from_millis(200)),
                ..Default::default()
            },
            ..default_test_config(server.addr)
        };
        let mut client = Client::connect(config).unwrap();
        assert_open_started(client.open(ClientTimestamp::now()).unwrap());
        client.send_probe(ClientTimestamp::now()).unwrap();
        client.send_probe(ClientTimestamp::now()).unwrap();
        client.send_probe(ClientTimestamp::now()).unwrap();
        thread::sleep(Duration::from_millis(50));

        let ev1 = client.recv_once(ClientTimestamp::now()).unwrap();
        assert!(matches!(&ev1[0], ClientEvent::EchoReply { seq: 2, .. }));

        thread::sleep(Duration::from_millis(30));
        let ev2 = client.recv_once(ClientTimestamp::now()).unwrap();
        match &ev2[0] {
            ClientEvent::LateReply {
                seq, rtt, sent_at, ..
            } => {
                assert_eq!(*seq, 0);
                assert!(rtt.is_some());
                assert!(sent_at.is_some());
            }
            other => panic!("expected LateReply, got {other:?}"),
        }
        server.join();
    }
}
