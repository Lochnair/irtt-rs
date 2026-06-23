use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use irtt_proto::{
    close::CloseRequest, decode_echo_reply, echo_packet_len, encode_close_request,
    encode_echo_request, encode_open_request, flags, EchoReply, EchoRequest, OpenReply,
    OpenRequest, Params, ServerFill, TimestampFields, PROTOCOL_VERSION,
};

use crate::{
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
    session::{negotiate_params, ActiveSession, ClientPhase, CloseSource, NegotiatedParams},
    timing::ClientTimestamp,
};

pub(crate) const MAX_OPEN_PACKET_SIZE: usize = 512;
const MIN_RECV_BUFFER_SIZE: usize = 2048;

#[derive(Debug)]
pub(crate) struct SessionRuntime {
    config: ClientConfig,
    remote: std::net::SocketAddr,
    requested: Params,
    negotiated: Option<NegotiatedParams>,
    phase: ClientPhase,
    session: Option<ActiveSession>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SendProbeResult {
    pub(crate) sent_at: ClientTimestamp,
    pub(crate) bytes: usize,
    pub(crate) send_call: Duration,
}

impl SessionRuntime {
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
            negotiated: None,
            phase: ClientPhase::Connected,
            session: None,
        })
    }

    pub(crate) fn config(&self) -> &ClientConfig {
        &self.config
    }

    pub(crate) fn has_hmac(&self) -> bool {
        self.config.hmac_key.is_some()
    }

    pub(crate) fn open_packet(&self) -> Result<Vec<u8>, ClientError> {
        match self.phase {
            ClientPhase::Connected => {}
            ClientPhase::Open { .. } => return Err(ClientError::AlreadyOpen),
            ClientPhase::Closed { .. } => return Err(ClientError::AlreadyClosed),
            ClientPhase::NoTestCompleted => return Err(ClientError::AlreadyCompleted),
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

    pub(crate) fn next_send_deadline(&self) -> Option<Instant> {
        let session = self.session.as_ref()?;
        if session.sending_done {
            return None;
        }
        Some(session.next_send_at)
    }

    pub(crate) fn probe_timeout(&self) -> Duration {
        self.config.probe_timeout
    }

    pub(crate) fn send_probe_with<F>(
        &mut self,
        override_ts: Option<ClientTimestamp>,
        send: F,
    ) -> Result<Vec<ClientEvent>, ClientError>
    where
        F: FnOnce(&[u8]) -> Result<SendProbeResult, ClientError>,
    {
        self.send_probe_inner(override_ts, None, send)
    }

    pub(crate) fn send_probe_for_deadline<F>(
        &mut self,
        scheduled_at: Instant,
        send: F,
    ) -> Result<Vec<ClientEvent>, ClientError>
    where
        F: FnOnce(&[u8]) -> Result<SendProbeResult, ClientError>,
    {
        self.send_probe_inner(None, Some(scheduled_at), send)
    }

    fn send_probe_inner<F>(
        &mut self,
        override_ts: Option<ClientTimestamp>,
        scheduled_at_override: Option<Instant>,
        send: F,
    ) -> Result<Vec<ClientEvent>, ClientError>
    where
        F: FnOnce(&[u8]) -> Result<SendProbeResult, ClientError>,
    {
        let token = match self.phase {
            ClientPhase::Open { token } => token,
            ClientPhase::Closed { .. } => return Err(ClientError::AlreadyClosed),
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

        let now = override_ts.unwrap_or_else(ClientTimestamp::now);

        if let Some(end) = session.end_mono {
            if now.mono >= end {
                session.sending_done = true;
                return Ok(vec![]);
            }
        }

        session.pending.check_capacity()?;

        let negotiated = self
            .negotiated
            .as_ref()
            .expect("negotiated must exist when Open");

        let wire_seq = session.next_wire_seq;
        let scheduled_at = scheduled_at_override.unwrap_or(session.next_send_at);

        let request = EchoRequest {
            token,
            sequence: wire_seq,
            params: negotiated.params.clone(),
            payload: vec![],
        };
        let packet = encode_echo_request(&request, self.config.hmac_key.as_deref())?;
        let send_result = send(&packet)?;
        let timer_error = instant_abs_diff(send_result.sent_at.mono, scheduled_at);

        let session = self
            .session
            .as_mut()
            .expect("session must exist when phase is Open");

        let pending = PendingProbe {
            wire_seq,
            sent_at: send_result.sent_at,
            timeout_at: send_result
                .sent_at
                .mono
                .checked_add(self.config.probe_timeout)
                .ok_or(ClientError::DurationOverflow)?,
        };
        session.pending.insert(pending)?;

        session.next_wire_seq = session.next_wire_seq.wrapping_add(1);
        session.packets_sent =
            session
                .packets_sent
                .checked_add(1)
                .ok_or(ClientError::CounterOverflow {
                    counter: "packets_sent",
                })?;

        let negotiated = self
            .negotiated
            .as_ref()
            .expect("negotiated must exist when Open");
        let interval_ns = u64::try_from(negotiated.params.interval_ns)
            .expect("validated positive negotiated interval");
        let session = self
            .session
            .as_mut()
            .expect("session must exist when phase is Open");
        session.next_send_at = if let Some(scheduled_at) = scheduled_at_override {
            scheduled_at
                .checked_add(Duration::from_nanos(interval_ns))
                .ok_or(ClientError::DurationOverflow)?
        } else {
            next_probe_deadline(session.start_mono, interval_ns, session.packets_sent)?
        };

        if let Some(end) = session.end_mono {
            if session.next_send_at >= end {
                session.sending_done = true;
            }
        }

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
        match self.phase {
            ClientPhase::Open { .. } => {}
            ClientPhase::Closed { .. } => return Err(ClientError::AlreadyClosed),
            ClientPhase::Connected => return Err(ClientError::NotOpen),
            ClientPhase::NoTestCompleted => return Err(ClientError::AlreadyCompleted),
        }

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
        match self.phase {
            ClientPhase::Open { .. } => {}
            ClientPhase::Closed { .. } => return Err(ClientError::AlreadyClosed),
            ClientPhase::Connected => return Err(ClientError::NotOpen),
            ClientPhase::NoTestCompleted => return Err(ClientError::AlreadyCompleted),
        }

        let session = self
            .session
            .as_mut()
            .expect("session must exist when phase is Open");

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
        let token = match self.phase {
            ClientPhase::Open { token } => token,
            ClientPhase::Closed { .. } => return Err(ClientError::AlreadyClosed),
            ClientPhase::Connected | ClientPhase::NoTestCompleted => {
                return Err(ClientError::NotOpen)
            }
        };

        let packet =
            encode_close_request(&CloseRequest { token }, self.config.hmac_key.as_deref())?;
        send(&packet)?;
        self.phase = ClientPhase::Closed {
            source: CloseSource::Local,
        };
        if let Some(session) = self.session.as_mut() {
            session.timed_out.clear();
        }
        self.session = None;

        Ok(vec![ClientEvent::SessionClosed {
            remote: self.remote,
            token,
            at: ClientTimestamp::now(),
        }])
    }

    pub(crate) fn is_run_complete(&self) -> bool {
        let Some(session) = self.session.as_ref() else {
            return matches!(
                self.phase,
                ClientPhase::Closed { .. } | ClientPhase::NoTestCompleted
            );
        };
        session.sending_done && session.pending.len() == 0
    }

    pub(crate) fn is_peer_closed(&self) -> bool {
        matches!(
            self.phase,
            ClientPhase::Closed {
                source: CloseSource::Peer
            }
        )
    }

    pub(crate) fn has_timed_out_metadata(&self) -> bool {
        self.session
            .as_ref()
            .is_some_and(|session| session.timed_out.len() > 0)
    }

    pub(crate) fn packets_sent(&self) -> u64 {
        self.session
            .as_ref()
            .map_or(0, |session| session.packets_sent)
    }

    pub(crate) fn is_open(&self) -> bool {
        matches!(self.phase, ClientPhase::Open { .. })
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
        self.negotiated = Some(negotiated.clone());
        self.phase = ClientPhase::Open { token };

        let end_mono = if negotiated.params.duration_ns > 0 {
            Some(negotiated_end_mono(
                now.mono,
                negotiated.params.duration_ns,
            )?)
        } else {
            None
        };

        self.session = Some(ActiveSession {
            next_wire_seq: 0,
            highest_received_seq: None,
            packets_sent: 0,
            start_mono: now.mono,
            end_mono,
            next_send_at: now.mono,
            pending: PendingMap::new(self.config.max_pending_probes),
            timed_out: TimedOutMap::new(self.config.max_pending_probes),
            completed: CompletedSet::new(self.config.max_pending_probes),
            sending_done: false,
        });

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

    fn decode_received_packet(&self, packet: &[u8]) -> Option<EchoReply> {
        let negotiated = self
            .negotiated
            .as_ref()
            .expect("negotiated must exist when Open");

        decode_echo_reply(packet, &negotiated.params, self.config.hmac_key.as_deref()).ok()
    }

    fn process_echo_reply(
        &mut self,
        reply: EchoReply,
        packet_len: usize,
        now: ClientTimestamp,
        meta: ReceiveMeta,
    ) -> Result<Vec<ClientEvent>, ClientError> {
        let token = match self.phase {
            ClientPhase::Open { token } => token,
            _ => unreachable!(),
        };
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

        let session = self.session.as_mut().expect("session must exist when Open");

        let wire_seq = reply.sequence;

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

            let mut events = Vec::new();
            if is_late {
                events.push(ClientEvent::LateReply {
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
                });
            } else {
                events.push(ClientEvent::EchoReply {
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
                });
            }
            if flags::has(reply.flags, flags::FLAG_CLOSE) {
                self.close_from_peer(token, now, &mut events);
            }
            Ok(events)
        } else if session.completed.contains(wire_seq) {
            update_highest_received(&mut session.highest_received_seq, wire_seq);
            Ok(vec![ClientEvent::DuplicateReply {
                seq: wire_seq,
                remote: self.remote,
                received_at: now,
                bytes: packet_len,
            }])
        } else if let Some(timed_out) = session.timed_out.remove(wire_seq) {
            let rtt = compute_rtt(&timed_out.sent_at, &now, &reply.timestamps);
            let server_timing = build_server_timing(&reply.timestamps);
            let one_way = compute_one_way(&timed_out.sent_at, &now, &reply.timestamps);
            let received_stats = build_received_stats(&reply);
            let highest_seen = session.highest_received_seq.unwrap_or(wire_seq);
            update_highest_received(&mut session.highest_received_seq, wire_seq);
            session.completed.insert(wire_seq);

            let mut events = vec![ClientEvent::LateReply {
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
            }];
            if flags::has(reply.flags, flags::FLAG_CLOSE) {
                self.close_from_peer(token, now, &mut events);
            }
            Ok(events)
        } else if session
            .highest_received_seq
            .is_some_and(|h| sequence_is_before(wire_seq, h))
        {
            Ok(vec![ClientEvent::LateReply {
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
            }])
        } else {
            Ok(vec![ClientEvent::Warning {
                kind: WarningKind::UntrackedReply,
                message: format!(
                    "dropped reply with untracked seq {wire_seq} (no pending or completed entry)"
                ),
                at: now,
            }])
        }
    }

    fn close_from_peer(&mut self, token: u64, now: ClientTimestamp, events: &mut Vec<ClientEvent>) {
        self.phase = ClientPhase::Closed {
            source: CloseSource::Peer,
        };
        if let Some(session) = self.session.as_mut() {
            session.timed_out.clear();
        }
        self.session = None;
        events.push(ClientEvent::SessionClosed {
            remote: self.remote,
            token,
            at: now,
        });
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

fn next_probe_deadline(
    start: Instant,
    interval_ns: u64,
    packets_sent: u64,
) -> Result<Instant, ClientError> {
    let offset_ns = interval_ns
        .checked_mul(packets_sent)
        .ok_or(ClientError::DurationOverflow)?;
    start
        .checked_add(Duration::from_nanos(offset_ns))
        .ok_or(ClientError::DurationOverflow)
}

pub(crate) fn sequence_is_after(candidate: u32, current: u32) -> bool {
    candidate != current && candidate.wrapping_sub(current) < (1 << 31)
}

pub(crate) fn sequence_is_before(candidate: u32, current: u32) -> bool {
    current != candidate && current.wrapping_sub(candidate) < (1 << 31)
}

fn instant_abs_diff(left: Instant, right: Instant) -> Duration {
    left.checked_duration_since(right)
        .or_else(|| right.checked_duration_since(left))
        .unwrap_or(Duration::ZERO)
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

fn negotiated_end_mono(start: Instant, duration_ns: i64) -> Result<Instant, ClientError> {
    debug_assert!(duration_ns > 0);
    let duration_ns = u64::try_from(duration_ns).expect("validated positive negotiated duration");
    start
        .checked_add(Duration::from_nanos(duration_ns))
        .ok_or_else(|| ClientError::NegotiationRejected {
            reason: "duration is too large to schedule".to_owned(),
        })
}
