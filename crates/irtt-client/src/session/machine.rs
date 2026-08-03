use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use irtt_proto::{
    close::CloseRequest, decode_echo_reply, echo_packet_len, encode_close_request,
    encode_echo_request, encode_open_request, flags, EchoReply, EchoRequest, OpenReply,
    OpenRequest, Params, ServerFill, TimestampFields, PROTOCOL_VERSION,
};

use crate::{
    client::schedule::ProbeSchedule,
    config::{
        ClientConfig, RunMode, MAX_DSCP_CODEPOINT, MAX_SERVER_FILL_BYTES, MAX_UDP_PAYLOAD_LENGTH,
    },
    error::ClientError,
    event::{
        ClientEvent, OneWayDelaySample, OpenOutcome, ReceivedStatsSample, RttSample, ServerTiming,
        SignedDuration, WarningKind,
    },
    metadata::ReceiveMeta,
    probe::{CompletedSet, PendingMap, PendingProbe, TimedOutMap},
    session::{negotiate_params, NegotiatedParams},
    timing::ClientTimestamp,
};

pub(crate) const MAX_OPEN_PACKET_SIZE: usize = 512;
const MIN_RECV_BUFFER_SIZE: usize = 2048;

#[derive(Debug)]
pub(crate) struct SessionMachine {
    config: ClientConfig,
    remote: std::net::SocketAddr,
    requested: Params,
    state: MachineState,
}

#[derive(Debug)]
enum MachineState {
    Connected,
    Open(Box<ActiveSession>),
    NoTestCompleted,
    Closed {
        source: CloseSource,
        packets_sent: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CloseSource {
    Local,
    Peer,
}

#[derive(Debug)]
struct ActiveSession {
    token: u64,
    negotiated: NegotiatedParams,
    next_wire_seq: u32,
    highest_received_seq: Option<u32>,
    packets_sent: u64,
    pending: PendingMap,
    timed_out: TimedOutMap,
    completed: CompletedSet,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SendProbeResult {
    pub(crate) sent_at: ClientTimestamp,
    pub(crate) bytes: usize,
    pub(crate) send_call: Duration,
}

impl SessionMachine {
    pub(crate) fn new(
        config: ClientConfig,
        remote: std::net::SocketAddr,
    ) -> Result<Self, ClientError> {
        if config.max_pending_probes == 0 {
            return Err(ClientError::InvalidConfig {
                reason: "max_pending_probes must be greater than zero".to_owned(),
            });
        }
        if config.probe_timeout == Duration::ZERO {
            return Err(ClientError::InvalidConfig {
                reason: "probe_timeout must be greater than zero".to_owned(),
            });
        }
        let requested = params_from_config(&config)?;

        Ok(Self {
            config,
            remote,
            requested,
            state: MachineState::Connected,
        })
    }

    pub(crate) fn config(&self) -> &ClientConfig {
        &self.config
    }

    pub(crate) fn has_hmac(&self) -> bool {
        self.config.hmac_key.is_some()
    }

    pub(crate) fn open_packet(&self) -> Result<Vec<u8>, ClientError> {
        match self.state {
            MachineState::Connected => {}
            MachineState::Open(_) => return Err(ClientError::AlreadyOpen),
            MachineState::Closed { .. } => return Err(ClientError::AlreadyClosed),
            MachineState::NoTestCompleted => return Err(ClientError::AlreadyCompleted),
        }

        let request = OpenRequest {
            params: self.requested.clone(),
            close: self.config.run_mode == RunMode::NoTest,
        };
        Ok(encode_open_request(
            &request,
            self.config.hmac_key.as_deref(),
        )?)
    }

    pub(crate) fn decode_open_reply(&self, packet: &[u8]) -> Result<OpenReply, ClientError> {
        Ok(irtt_proto::decode_open_reply(
            packet,
            self.config.hmac_key.as_deref(),
        )?)
    }

    pub(crate) fn accept_open_reply<F>(
        &mut self,
        reply: OpenReply,
        now: ClientTimestamp,
        before_normal_open: F,
    ) -> Result<OpenOutcome, ClientError>
    where
        F: FnOnce(&NegotiatedParams) -> Result<(), ClientError>,
    {
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
            RunMode::Normal => self.accept_normal_open(reply, now, before_normal_open),
            RunMode::NoTest if !reply_is_close => Err(ClientError::UnexpectedNoTestReply),
            RunMode::NoTest if reply.token != 0 => {
                Err(ClientError::NonZeroNoTestToken { token: reply.token })
            }
            RunMode::NoTest => self.accept_no_test_open(reply, now),
        }
    }

    pub(crate) fn probe_timeout(&self) -> Duration {
        self.config.probe_timeout
    }

    pub(crate) fn send_probe_with<F>(
        &mut self,
        schedule: &mut ProbeSchedule,
        override_ts: Option<ClientTimestamp>,
        send: F,
    ) -> Result<Vec<ClientEvent>, ClientError>
    where
        F: FnOnce(&[u8]) -> Result<SendProbeResult, ClientError>,
    {
        self.send_probe_inner(schedule, override_ts, None, false, send)
    }

    pub(crate) fn send_probe_for_deadline<F>(
        &mut self,
        schedule: &mut ProbeSchedule,
        override_ts: Option<ClientTimestamp>,
        scheduled_at: Instant,
        send: F,
    ) -> Result<Vec<ClientEvent>, ClientError>
    where
        F: FnOnce(&[u8]) -> Result<SendProbeResult, ClientError>,
    {
        self.send_probe_inner(schedule, override_ts, Some(scheduled_at), true, send)
    }

    fn send_probe_inner<F>(
        &mut self,
        schedule: &mut ProbeSchedule,
        override_ts: Option<ClientTimestamp>,
        scheduled_at_override: Option<Instant>,
        skip_missed_slots: bool,
        send: F,
    ) -> Result<Vec<ClientEvent>, ClientError>
    where
        F: FnOnce(&[u8]) -> Result<SendProbeResult, ClientError>,
    {
        let now = override_ts.unwrap_or_else(ClientTimestamp::now);
        if !schedule.permit_probe_at(now.mono) {
            return Ok(vec![]);
        }

        let (config, state) = (&self.config, &mut self.state);
        let probe_timeout = config.probe_timeout;
        let hmac_key = config.hmac_key.as_deref();
        let session = match state {
            MachineState::Open(session) => session,
            MachineState::Closed { .. } => return Err(ClientError::AlreadyClosed),
            MachineState::Connected => return Err(ClientError::NotOpen),
            MachineState::NoTestCompleted => return Err(ClientError::AlreadyCompleted),
        };

        session.pending.preflight_insert(session.next_wire_seq)?;

        let wire_seq = session.next_wire_seq;

        let request = EchoRequest {
            token: session.token,
            sequence: wire_seq,
            payload: vec![],
        };
        let packet = encode_echo_request(&request, &session.negotiated.params, hmac_key)?;
        let send_result = send(&packet)?;

        let pending = PendingProbe {
            wire_seq,
            sent_at: send_result.sent_at,
            timeout_at: send_result
                .sent_at
                .mono
                .checked_add(probe_timeout)
                .ok_or(ClientError::DurationOverflow)?,
        };
        session.pending.commit_insert(pending);

        session.next_wire_seq = session.next_wire_seq.wrapping_add(1);
        session.packets_sent =
            session
                .packets_sent
                .checked_add(1)
                .ok_or(ClientError::CounterOverflow {
                    counter: "packets_sent",
                })?;

        let schedule_commit = if skip_missed_slots {
            schedule.preflight_managed_commit(
                scheduled_at_override.expect("managed sends include a scheduled deadline"),
                send_result.sent_at.mono,
            )?
        } else {
            schedule.preflight_caller_commit(send_result.sent_at.mono, session.packets_sent)?
        };
        let scheduled_at = schedule_commit.scheduled_at;
        let timer_error = schedule_commit.timer_error;
        schedule.commit(schedule_commit);

        Ok(vec![ClientEvent::EchoSent {
            seq: wire_seq,
            remote: self.remote,
            scheduled_at,
            sent_at: send_result.sent_at,
            bytes: send_result.bytes,
            send_call: send_result.send_call,
            timer_error,
        }])
    }

    pub(crate) fn process_received_echo_packet(
        &mut self,
        packet: &[u8],
        now: ClientTimestamp,
        meta: ReceiveMeta,
    ) -> Result<Vec<ClientEvent>, ClientError> {
        self.open_session()?;

        let Some(reply) = self.decode_received_packet(packet) else {
            return Ok(vec![ClientEvent::Warning {
                kind: WarningKind::MalformedOrUnrelatedPacket,
                message: "dropped malformed or unrelated packet".to_owned(),
                at: now,
            }]);
        };
        self.process_echo_reply(reply, packet.len(), now, meta)
    }

    pub(crate) fn poll_timeouts_at(
        &mut self,
        now: Instant,
    ) -> Result<Vec<ClientEvent>, ClientError> {
        let session = self.open_session_mut()?;

        let expired = session.pending.drain_expired(now);
        let mut events = Vec::with_capacity(expired.len());
        for probe in expired {
            events.push(ClientEvent::EchoLoss {
                seq: probe.wire_seq,
                sent_at: probe.sent_at,
                timeout_at: probe.timeout_at,
            });
            session.timed_out.insert(probe);
        }

        Ok(events)
    }

    pub(crate) fn close_with<F>(&mut self, send: F) -> Result<Vec<ClientEvent>, ClientError>
    where
        F: FnOnce(&[u8]) -> Result<(), ClientError>,
    {
        let token = match &self.state {
            MachineState::Open(session) => session.token,
            MachineState::Closed { .. } => return Err(ClientError::AlreadyClosed),
            MachineState::Connected | MachineState::NoTestCompleted => {
                return Err(ClientError::NotOpen)
            }
        };

        let packet =
            encode_close_request(&CloseRequest { token }, self.config.hmac_key.as_deref())?;
        send(&packet)?;
        self.transition_to_closed(CloseSource::Local);

        Ok(vec![ClientEvent::SessionClosed {
            remote: self.remote,
            token,
            at: ClientTimestamp::now(),
        }])
    }

    pub(crate) fn pending_is_empty(&self) -> bool {
        match &self.state {
            MachineState::Open(session) => session.pending.len() == 0,
            MachineState::NoTestCompleted | MachineState::Closed { .. } => true,
            MachineState::Connected => false,
        }
    }

    pub(crate) fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            MachineState::NoTestCompleted | MachineState::Closed { .. }
        )
    }

    pub(crate) fn is_peer_closed(&self) -> bool {
        matches!(
            self.state,
            MachineState::Closed {
                source: CloseSource::Peer,
                ..
            }
        )
    }

    pub(crate) fn has_timed_out_metadata(&self) -> bool {
        matches!(
            &self.state,
            MachineState::Open(session) if session.timed_out.len() > 0
        )
    }

    pub(crate) fn packets_sent(&self) -> u64 {
        match &self.state {
            MachineState::Open(session) => session.packets_sent,
            MachineState::Closed { packets_sent, .. } => *packets_sent,
            MachineState::Connected | MachineState::NoTestCompleted => 0,
        }
    }

    pub(crate) fn is_open(&self) -> bool {
        matches!(self.state, MachineState::Open(_))
    }

    pub(crate) fn ensure_open(&self) -> Result<(), ClientError> {
        self.open_session().map(|_| ())
    }

    fn accept_normal_open<F>(
        &mut self,
        reply: OpenReply,
        now: ClientTimestamp,
        before_normal_open: F,
    ) -> Result<OpenOutcome, ClientError>
    where
        F: FnOnce(&NegotiatedParams) -> Result<(), ClientError>,
    {
        let token = reply.token;
        let negotiated = negotiate_params(
            &self.requested,
            reply.params,
            self.config.negotiation_policy,
        )?;
        before_normal_open(&negotiated)?;

        self.state = MachineState::Open(Box::new(ActiveSession {
            token,
            negotiated: negotiated.clone(),
            next_wire_seq: 0,
            highest_received_seq: None,
            packets_sent: 0,
            pending: PendingMap::new(self.config.max_pending_probes),
            timed_out: TimedOutMap::new(self.config.max_pending_probes),
            completed: CompletedSet::new(self.config.max_pending_probes),
        }));

        let event = ClientEvent::SessionStarted {
            remote: self.remote,
            token,
            negotiated: negotiated.clone(),
            at: now,
        };

        Ok(OpenOutcome::Started {
            remote: self.remote,
            token,
            negotiated,
            event,
        })
    }

    fn accept_no_test_open(
        &mut self,
        reply: OpenReply,
        now: ClientTimestamp,
    ) -> Result<OpenOutcome, ClientError> {
        let negotiated = negotiate_params(
            &self.requested,
            reply.params,
            self.config.negotiation_policy,
        )?;
        self.state = MachineState::NoTestCompleted;
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

    fn decode_received_packet(&self, packet: &[u8]) -> Option<EchoReply> {
        let session = self
            .open_session()
            .expect("decode_received_packet is only called for an open session");

        decode_echo_reply(
            packet,
            &session.negotiated.params,
            self.config.hmac_key.as_deref(),
        )
        .ok()
    }

    fn process_echo_reply(
        &mut self,
        reply: EchoReply,
        packet_len: usize,
        now: ClientTimestamp,
        meta: ReceiveMeta,
    ) -> Result<Vec<ClientEvent>, ClientError> {
        let token = self
            .open_session()
            .expect("process_echo_reply is only called for an open session")
            .token;
        if reply.token != token {
            return Ok(vec![ClientEvent::Warning {
                kind: WarningKind::WrongToken,
                message: format!(
                    "dropped reply with wrong token: expected {token:#x}, got {:#x}",
                    reply.token
                ),
                at: now,
            }]);
        }

        let wire_seq = reply.sequence;
        let should_close = flags::has(reply.flags, flags::FLAG_CLOSE);
        let mut events = {
            let session = self
                .open_session_mut()
                .expect("process_echo_reply is only called for an open session");

            if let Some(pending) = session.pending.remove(wire_seq) {
                let rtt = compute_rtt(&pending.sent_at, &now, &reply.timestamps);
                let server_timing = build_server_timing(&reply.timestamps);
                let one_way = compute_one_way(&pending.sent_at, &now, &reply.timestamps);
                let received_stats = build_received_stats(&reply);
                let is_late = session
                    .highest_received_seq
                    .is_some_and(|h| sequence_is_before(wire_seq, h));
                let highest_seen = session.highest_received_seq.unwrap_or(wire_seq);

                update_highest_received(&mut session.highest_received_seq, wire_seq);
                session.completed.insert(wire_seq);

                if is_late {
                    vec![ClientEvent::LateReply {
                        seq: wire_seq,
                        highest_seen,
                        remote: self.remote,
                        sent_at: Some(pending.sent_at),
                        received_at: now,
                        rtt: Some(rtt),
                        server_timing,
                        one_way,
                        received_stats,
                        bytes: packet_len,
                        packet_meta: meta.into(),
                    }]
                } else {
                    vec![ClientEvent::EchoReply {
                        seq: wire_seq,
                        remote: self.remote,
                        sent_at: pending.sent_at,
                        received_at: now,
                        rtt,
                        server_timing,
                        one_way,
                        received_stats,
                        bytes: packet_len,
                        packet_meta: meta.into(),
                    }]
                }
            } else if session.completed.contains(wire_seq) {
                update_highest_received(&mut session.highest_received_seq, wire_seq);
                vec![ClientEvent::DuplicateReply {
                    seq: wire_seq,
                    remote: self.remote,
                    received_at: now,
                    bytes: packet_len,
                }]
            } else if let Some(timed_out) = session.timed_out.remove(wire_seq) {
                let rtt = compute_rtt(&timed_out.sent_at, &now, &reply.timestamps);
                let server_timing = build_server_timing(&reply.timestamps);
                let one_way = compute_one_way(&timed_out.sent_at, &now, &reply.timestamps);
                let received_stats = build_received_stats(&reply);
                let highest_seen = session.highest_received_seq.unwrap_or(wire_seq);
                update_highest_received(&mut session.highest_received_seq, wire_seq);
                session.completed.insert(wire_seq);

                vec![ClientEvent::LateReply {
                    seq: wire_seq,
                    highest_seen,
                    remote: self.remote,
                    sent_at: Some(timed_out.sent_at),
                    received_at: now,
                    rtt: Some(rtt),
                    server_timing,
                    one_way,
                    received_stats,
                    bytes: packet_len,
                    packet_meta: meta.into(),
                }]
            } else if session
                .highest_received_seq
                .is_some_and(|h| sequence_is_before(wire_seq, h))
            {
                vec![ClientEvent::LateReply {
                    seq: wire_seq,
                    highest_seen: session.highest_received_seq.unwrap(),
                    remote: self.remote,
                    sent_at: None,
                    received_at: now,
                    rtt: None,
                    server_timing: build_server_timing(&reply.timestamps),
                    one_way: None,
                    received_stats: build_received_stats(&reply),
                    bytes: packet_len,
                    packet_meta: meta.into(),
                }]
            } else {
                vec![ClientEvent::Warning {
                    kind: WarningKind::UntrackedReply,
                    message: format!(
                        "dropped reply with untracked seq {wire_seq} (no pending or completed entry)"
                    ),
                    at: now,
                }]
            }
        };

        if should_close {
            self.close_from_peer(token, now, &mut events);
        }
        Ok(events)
    }

    fn close_from_peer(&mut self, token: u64, now: ClientTimestamp, events: &mut Vec<ClientEvent>) {
        self.transition_to_closed(CloseSource::Peer);
        events.push(ClientEvent::SessionClosed {
            remote: self.remote,
            token,
            at: now,
        });
    }

    fn transition_to_closed(&mut self, source: CloseSource) {
        let packets_sent = match &mut self.state {
            MachineState::Open(session) => {
                session.timed_out.clear();
                session.packets_sent
            }
            MachineState::Closed { packets_sent, .. } => *packets_sent,
            MachineState::Connected | MachineState::NoTestCompleted => 0,
        };
        self.state = MachineState::Closed {
            source,
            packets_sent,
        };
    }

    fn open_session(&self) -> Result<&ActiveSession, ClientError> {
        match &self.state {
            MachineState::Open(session) => Ok(session),
            MachineState::Closed { .. } => Err(ClientError::AlreadyClosed),
            MachineState::Connected => Err(ClientError::NotOpen),
            MachineState::NoTestCompleted => Err(ClientError::AlreadyCompleted),
        }
    }

    fn open_session_mut(&mut self) -> Result<&mut ActiveSession, ClientError> {
        match &mut self.state {
            MachineState::Open(session) => Ok(session),
            MachineState::Closed { .. } => Err(ClientError::AlreadyClosed),
            MachineState::Connected => Err(ClientError::NotOpen),
            MachineState::NoTestCompleted => Err(ClientError::AlreadyCompleted),
        }
    }
}

pub(crate) fn recv_buffer_size(
    has_hmac: bool,
    negotiated: Option<&NegotiatedParams>,
) -> Result<usize, ClientError> {
    Ok(match negotiated {
        Some(negotiated) => echo_packet_len(has_hmac, &negotiated.params)?
            .saturating_add(1)
            .max(MIN_RECV_BUFFER_SIZE),
        None => MIN_RECV_BUFFER_SIZE,
    })
}

pub(crate) fn params_from_config(config: &ClientConfig) -> Result<Params, ClientError> {
    validate_protocol_config(config)?;
    Ok(Params {
        protocol_version: PROTOCOL_VERSION,
        duration_ns: match config.duration {
            Some(duration) => config_duration_to_ns("duration", duration)?,
            None => 0,
        },
        interval_ns: config_duration_to_ns("interval", config.interval)?,
        length: i64::from(config.length),
        received_stats: config.received_stats,
        stamp_at: config.stamp_at,
        clock: config.clock,
        dscp: i64::from(config.dscp),
        server_fill: config.server_fill.clone().map(|value| ServerFill { value }),
    })
}

pub(crate) fn update_highest_received(highest_received_seq: &mut Option<u32>, wire_seq: u32) {
    *highest_received_seq = Some(highest_received_seq.map_or(wire_seq, |h| {
        if sequence_is_after(wire_seq, h) {
            wire_seq
        } else {
            h
        }
    }));
}

pub(crate) fn sequence_is_after(candidate: u32, current: u32) -> bool {
    candidate != current && candidate.wrapping_sub(current) < (1 << 31)
}

pub(crate) fn sequence_is_before(candidate: u32, current: u32) -> bool {
    current != candidate && current.wrapping_sub(candidate) < (1 << 31)
}

pub(crate) fn compute_rtt(
    sent_at: &ClientTimestamp,
    received_at: &ClientTimestamp,
    ts: &TimestampFields,
) -> RttSample {
    let raw = received_at
        .mono
        .checked_duration_since(sent_at.mono)
        .unwrap_or(Duration::ZERO);

    let server_processing = compute_server_processing(ts);

    let adjusted = server_processing
        .map(|sp| SignedDuration::from_nanos(duration_ns_i128(raw) - duration_ns_i128(sp)));
    let effective = adjusted.unwrap_or_else(|| SignedDuration::from_duration(raw));

    RttSample {
        raw,
        adjusted,
        effective,
    }
}

fn duration_ns_i128(duration: Duration) -> i128 {
    i128::try_from(duration.as_nanos()).unwrap_or(i128::MAX)
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

pub(crate) fn compute_one_way(
    sent_at: &ClientTimestamp,
    received_at: &ClientTimestamp,
    ts: &TimestampFields,
) -> Option<OneWayDelaySample> {
    let server_recv_wall = ts.recv_wall.or(ts.midpoint_wall);
    let server_send_wall = ts.send_wall.or(ts.midpoint_wall);

    let client_send_ns = unix_epoch_ns_i64(sent_at.wall);
    let client_recv_ns = unix_epoch_ns_i64(received_at.wall);

    let c2s = server_recv_wall
        .zip(client_send_ns)
        .and_then(|(srv, cli)| srv.checked_sub(cli))
        .map(|d| SignedDuration::from_nanos(i128::from(d)));
    let s2c = client_recv_ns
        .zip(server_send_wall)
        .and_then(|(cli, srv)| cli.checked_sub(srv))
        .map(|d| SignedDuration::from_nanos(i128::from(d)));

    if c2s.is_none() && s2c.is_none() {
        return None;
    }

    Some(OneWayDelaySample {
        client_to_server: c2s,
        server_to_client: s2c,
    })
}

pub(crate) fn unix_epoch_ns_i64(time: SystemTime) -> Option<i64> {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_nanos()).ok())
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

fn validate_protocol_config(config: &ClientConfig) -> Result<(), ClientError> {
    if config.duration == Some(Duration::ZERO) {
        return Err(ClientError::InvalidConfig {
            reason: "duration must be greater than zero; use None for continuous mode".to_owned(),
        });
    }
    if config.interval == Duration::ZERO {
        return Err(ClientError::InvalidConfig {
            reason: "interval must be greater than zero".to_owned(),
        });
    }
    if config.dscp > MAX_DSCP_CODEPOINT {
        return Err(ClientError::InvalidConfig {
            reason: format!("dscp must be <= {MAX_DSCP_CODEPOINT}"),
        });
    }
    if config.length > MAX_UDP_PAYLOAD_LENGTH {
        return Err(ClientError::InvalidConfig {
            reason: format!("packet length must be <= {MAX_UDP_PAYLOAD_LENGTH}"),
        });
    }

    if let Some(fill) = &config.server_fill {
        let len = fill.len();
        if len == 0 {
            return Err(ClientError::InvalidConfig {
                reason: "server_fill must not be empty".to_owned(),
            });
        }
        if len > MAX_SERVER_FILL_BYTES {
            return Err(ClientError::InvalidConfig {
                reason: format!("server_fill must be <= {MAX_SERVER_FILL_BYTES} bytes, got {len}"),
            });
        }
    }

    Ok(())
}

fn config_duration_to_ns(field: &str, duration: Duration) -> Result<i64, ClientError> {
    i64::try_from(duration.as_nanos()).map_err(|_| ClientError::InvalidConfig {
        reason: format!("{field} is too large to encode as nanoseconds"),
    })
}
