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
    local_close_packet: Box<[u8]>,
    next_wire_seq: u32,
    highest_received_seq: Option<u32>,
    packets_sent: u64,
    pending: PendingMap,
    timed_out: TimedOutMap,
    completed: CompletedSet,
}

#[derive(Debug)]
pub(crate) struct PreparedOpenRequest {
    pub(crate) bytes: Box<[u8]>,
}

#[derive(Debug)]
pub(crate) enum OpenDatagramDisposition {
    Ignore,
    Trusted(OpenReply),
}

#[derive(Debug)]
pub(crate) struct PreparedOpenAcceptance {
    next_state: MachineState,
    outcome: OpenOutcome,
}

impl PreparedOpenAcceptance {
    pub(crate) fn normal_negotiated(&self) -> Option<&NegotiatedParams> {
        match &self.next_state {
            MachineState::Open(session) => Some(&session.negotiated),
            MachineState::NoTestCompleted => None,
            MachineState::Connected | MachineState::Closed { .. } => {
                unreachable!("open acceptance only prepares open or no-test state")
            }
        }
    }

    pub(crate) fn cleanup_close_packet(&self) -> Option<&[u8]> {
        match &self.next_state {
            MachineState::Open(session) => Some(&session.local_close_packet),
            MachineState::NoTestCompleted => None,
            MachineState::Connected | MachineState::Closed { .. } => {
                unreachable!("open acceptance only prepares open or no-test state")
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn has_prepared_active_session(&self) -> bool {
        matches!(self.next_state, MachineState::Open(_))
    }
}

#[derive(Debug)]
pub(crate) struct PreparedClose<'a> {
    pub(crate) bytes: &'a [u8],
    pub(crate) commit: CloseCommit,
}

#[derive(Debug)]
pub(crate) struct CloseCommit {
    packets_sent: u64,
    token: u64,
}

#[derive(Debug)]
pub(crate) struct OpenAcceptanceFailure {
    pub(crate) primary: ClientError,
    pub(crate) cleanup_close: Option<Box<[u8]>>,
}

impl OpenAcceptanceFailure {
    fn new(primary: ClientError, cleanup_close: Option<Box<[u8]>>) -> Self {
        Self {
            primary,
            cleanup_close,
        }
    }

    fn without_cleanup(primary: ClientError) -> Self {
        Self::new(primary, None)
    }
}

#[derive(Debug)]
pub(crate) struct PreparedProbe {
    pub(crate) bytes: Box<[u8]>,
    pub(crate) seq: u32,
}

#[derive(Debug)]
pub(crate) struct ProbeCommitPreflight {
    seq: u32,
    next_wire_seq: u32,
    pub(crate) next_packets_sent: u64,
}

#[derive(Debug)]
pub(crate) struct ProbeCommit {
    pending: PendingProbe,
    next_wire_seq: u32,
    next_packets_sent: u64,
}

#[derive(Debug)]
pub(crate) struct TimeoutBatch {
    pub events: Vec<ClientEvent>,
    pub more_due: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ProbeSent {
    pub(crate) seq: u32,
    pub(crate) sent_at: ClientTimestamp,
    pub(crate) bytes: usize,
}

impl SessionMachine {
    pub(crate) fn new(
        config: ClientConfig,
        remote: std::net::SocketAddr,
    ) -> Result<Self, ClientError> {
        Self::validate_config(&config)?;
        let requested = params_from_config(&config)?;

        Ok(Self {
            config,
            remote,
            requested,
            state: MachineState::Connected,
        })
    }

    pub(crate) fn validate_config(config: &ClientConfig) -> Result<(), ClientError> {
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
        params_from_config(config).map(|_| ())
    }

    pub(crate) fn config(&self) -> &ClientConfig {
        &self.config
    }

    pub(crate) fn has_hmac(&self) -> bool {
        self.config.hmac_key.is_some()
    }

    pub(crate) fn prepare_open_request(&self) -> Result<PreparedOpenRequest, ClientError> {
        self.ensure_connected()?;
        let request = OpenRequest {
            params: self.requested.clone(),
            close: self.config.run_mode == RunMode::NoTest,
        };
        let bytes = encode_open_request(&request, self.config.hmac_key.as_deref())?;
        Ok(PreparedOpenRequest {
            bytes: bytes.into_boxed_slice(),
        })
    }

    pub(crate) fn inspect_open_datagram(
        &self,
        packet: &[u8],
    ) -> Result<OpenDatagramDisposition, ClientError> {
        self.ensure_connected()?;
        match irtt_proto::decode_open_reply(packet, self.config.hmac_key.as_deref()) {
            Ok(reply) => Ok(OpenDatagramDisposition::Trusted(reply)),
            Err(error @ irtt_proto::ProtoError::ZeroToken) => Err(ClientError::Protocol(error)),
            Err(
                error @ (irtt_proto::ProtoError::TruncatedVarint
                | irtt_proto::ProtoError::VarintOverflow
                | irtt_proto::ProtoError::InvalidUtf8
                | irtt_proto::ProtoError::InvalidEnum { .. }
                | irtt_proto::ProtoError::NegativePacketLength { .. }
                | irtt_proto::ProtoError::ParameterLengthTooLarge { .. }
                | irtt_proto::ProtoError::MalformedParams),
            ) => Err(ClientError::Protocol(error)),
            Err(_) => Ok(OpenDatagramDisposition::Ignore),
        }
    }

    pub(crate) fn prepare_open_acceptance(
        &self,
        reply: OpenReply,
        now: ClientTimestamp,
    ) -> Result<PreparedOpenAcceptance, OpenAcceptanceFailure> {
        self.ensure_connected()
            .map_err(OpenAcceptanceFailure::without_cleanup)?;

        let reply_is_close = flags::has(reply.flags, flags::FLAG_CLOSE);
        let cleanup_close = if !reply_is_close && reply.token != 0 {
            let bytes = encode_close_request(
                &CloseRequest { token: reply.token },
                self.config.hmac_key.as_deref(),
            )
            .map_err(ClientError::from)
            .map_err(OpenAcceptanceFailure::without_cleanup)?;
            Some(bytes.into_boxed_slice())
        } else {
            None
        };

        if reply.params.protocol_version != PROTOCOL_VERSION {
            return Err(OpenAcceptanceFailure::new(
                ClientError::ProtocolVersionMismatch {
                    requested: PROTOCOL_VERSION,
                    received: reply.params.protocol_version,
                },
                cleanup_close,
            ));
        }

        match self.config.run_mode {
            RunMode::Normal if reply_is_close => Err(OpenAcceptanceFailure::new(
                ClientError::ServerRejected,
                cleanup_close,
            )),
            RunMode::Normal if reply.token == 0 => Err(OpenAcceptanceFailure::new(
                ClientError::ZeroToken,
                cleanup_close,
            )),
            RunMode::Normal => self.prepare_normal_open(reply, now, cleanup_close),
            RunMode::NoTest if !reply_is_close => Err(OpenAcceptanceFailure::new(
                ClientError::UnexpectedNoTestReply,
                cleanup_close,
            )),
            RunMode::NoTest if reply.token != 0 => Err(OpenAcceptanceFailure::new(
                ClientError::NonZeroNoTestToken { token: reply.token },
                cleanup_close,
            )),
            RunMode::NoTest => self.prepare_no_test_open(reply, now),
        }
    }

    pub(crate) fn commit_open(&mut self, prepared: PreparedOpenAcceptance) -> OpenOutcome {
        debug_assert!(
            matches!(self.state, MachineState::Connected),
            "open acceptance commits only from connected state"
        );
        self.state = prepared.next_state;
        prepared.outcome
    }

    pub(crate) fn probe_timeout(&self) -> Duration {
        self.config.probe_timeout
    }

    pub(crate) fn prepare_probe(&self) -> Result<Option<PreparedProbe>, ClientError> {
        let session = self.open_session()?;
        let request = EchoRequest {
            token: session.token,
            sequence: session.next_wire_seq,
            payload: vec![],
        };
        let bytes = encode_echo_request(
            &request,
            &session.negotiated.params,
            self.config.hmac_key.as_deref(),
        )?;
        Ok(Some(PreparedProbe {
            bytes: bytes.into_boxed_slice(),
            seq: session.next_wire_seq,
        }))
    }

    pub(crate) fn preflight_probe_commit(
        &mut self,
        prepared: &PreparedProbe,
    ) -> Result<ProbeCommitPreflight, ClientError> {
        let session = self.open_session_mut()?;
        if prepared.seq != session.next_wire_seq {
            return Err(ClientError::StalePreparedProbe {
                prepared_seq: prepared.seq,
                next_wire_seq: session.next_wire_seq,
            });
        }
        session.pending.preflight_insert(prepared.seq)?;
        let next_packets_sent =
            session
                .packets_sent
                .checked_add(1)
                .ok_or(ClientError::CounterOverflow {
                    counter: "packets_sent",
                })?;
        Ok(ProbeCommitPreflight {
            seq: prepared.seq,
            next_wire_seq: prepared.seq.wrapping_add(1),
            next_packets_sent,
        })
    }

    pub(crate) fn finalize_probe_commit(
        &self,
        preflight: ProbeCommitPreflight,
        sent_at: ClientTimestamp,
    ) -> Result<ProbeCommit, ClientError> {
        let timeout_at = sent_at
            .mono
            .checked_add(self.config.probe_timeout)
            .ok_or(ClientError::DurationOverflow)?;

        Ok(ProbeCommit {
            pending: PendingProbe {
                wire_seq: preflight.seq,
                sent_at,
                timeout_at,
            },
            next_wire_seq: preflight.next_wire_seq,
            next_packets_sent: preflight.next_packets_sent,
        })
    }

    pub(crate) fn commit_probe_sent(&mut self, commit: ProbeCommit, bytes: usize) -> ProbeSent {
        let session = match &mut self.state {
            MachineState::Open(session) => session,
            _ => unreachable!("probe commits are only created for an open session"),
        };
        let seq = commit.pending.wire_seq;
        let sent_at = commit.pending.sent_at;
        session.timed_out.remove(seq);
        session.completed.remove(seq);
        session.pending.commit_insert(commit.pending);
        session.next_wire_seq = commit.next_wire_seq;
        session.packets_sent = commit.next_packets_sent;

        ProbeSent {
            seq,
            sent_at,
            bytes,
        }
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
        let batch = self.poll_timeouts_bounded_at(now, usize::MAX)?;
        debug_assert!(!batch.more_due);
        Ok(batch.events)
    }

    pub(crate) fn poll_timeouts_bounded_at(
        &mut self,
        now: Instant,
        limit: usize,
    ) -> Result<TimeoutBatch, ClientError> {
        let session = self.open_session_mut()?;

        let expired = session.pending.drain_expired_bounded(now, limit);
        let mut events = Vec::with_capacity(expired.probes.len());
        for probe in expired.probes {
            events.push(ClientEvent::EchoLoss {
                seq: probe.wire_seq,
                sent_at: probe.sent_at,
                timeout_at: probe.timeout_at,
            });
            session.timed_out.insert(probe);
        }

        Ok(TimeoutBatch {
            events,
            more_due: expired.more_due,
        })
    }

    pub(crate) fn prepare_close(&self) -> Result<PreparedClose<'_>, ClientError> {
        let session = self.open_session()?;
        Ok(PreparedClose {
            bytes: &session.local_close_packet,
            commit: CloseCommit {
                packets_sent: session.packets_sent,
                token: session.token,
            },
        })
    }

    pub(crate) fn commit_local_close(
        &mut self,
        commit: CloseCommit,
        sent_at: ClientTimestamp,
    ) -> ClientEvent {
        debug_assert!(
            matches!(self.state, MachineState::Open(_)),
            "local close commits only from open state"
        );
        self.state = MachineState::Closed {
            source: CloseSource::Local,
            packets_sent: commit.packets_sent,
        };
        ClientEvent::SessionClosed {
            remote: self.remote,
            token: commit.token,
            at: sent_at,
        }
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

    #[cfg(test)]
    pub(crate) fn has_timed_out_metadata(&self) -> bool {
        matches!(
            &self.state,
            MachineState::Open(session) if session.timed_out.len() > 0
        )
    }

    #[cfg(any(feature = "tokio", test))]
    pub(crate) fn packets_sent(&self) -> u64 {
        match &self.state {
            MachineState::Open(session) => session.packets_sent,
            MachineState::Closed { packets_sent, .. } => *packets_sent,
            MachineState::Connected | MachineState::NoTestCompleted => 0,
        }
    }

    #[cfg(feature = "tokio")]
    pub(crate) fn next_probe_timeout_deadline(&self) -> Option<Instant> {
        match &self.state {
            MachineState::Open(session) => session.pending.next_timeout_deadline(),
            MachineState::Connected
            | MachineState::NoTestCompleted
            | MachineState::Closed { .. } => None,
        }
    }

    #[cfg(feature = "tokio")]
    pub(crate) fn latest_probe_timeout_deadline(&self) -> Option<Instant> {
        match &self.state {
            MachineState::Open(session) => session
                .pending
                .latest_timeout_deadline()
                .into_iter()
                .chain(session.timed_out.latest_timeout_deadline())
                .max(),
            MachineState::Connected
            | MachineState::NoTestCompleted
            | MachineState::Closed { .. } => None,
        }
    }

    pub(crate) fn is_open(&self) -> bool {
        matches!(self.state, MachineState::Open(_))
    }

    pub(crate) fn ensure_open(&self) -> Result<(), ClientError> {
        self.open_session().map(|_| ())
    }

    fn prepare_normal_open(
        &self,
        reply: OpenReply,
        now: ClientTimestamp,
        cleanup_close: Option<Box<[u8]>>,
    ) -> Result<PreparedOpenAcceptance, OpenAcceptanceFailure> {
        let token = reply.token;
        let negotiated = match negotiate_params(
            &self.requested,
            reply.params,
            self.config.negotiation_policy,
        ) {
            Ok(negotiated) => negotiated,
            Err(primary) => return Err(OpenAcceptanceFailure::new(primary, cleanup_close)),
        };
        let local_close_packet =
            cleanup_close.expect("normal non-zero-token replies prepare a cleanup close");
        let next_state = MachineState::Open(Box::new(ActiveSession {
            token,
            negotiated: negotiated.clone(),
            local_close_packet,
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

        let outcome = OpenOutcome::Started {
            remote: self.remote,
            token,
            negotiated,
            event,
        };
        Ok(PreparedOpenAcceptance {
            next_state,
            outcome,
        })
    }

    fn prepare_no_test_open(
        &self,
        reply: OpenReply,
        now: ClientTimestamp,
    ) -> Result<PreparedOpenAcceptance, OpenAcceptanceFailure> {
        let negotiated = negotiate_params(
            &self.requested,
            reply.params,
            self.config.negotiation_policy,
        )
        .map_err(OpenAcceptanceFailure::without_cleanup)?;
        let event = ClientEvent::NoTestCompleted {
            remote: self.remote,
            negotiated: negotiated.clone(),
            at: now,
        };
        let outcome = OpenOutcome::NoTestCompleted {
            remote: self.remote,
            negotiated,
            event,
        };
        Ok(PreparedOpenAcceptance {
            next_state: MachineState::NoTestCompleted,
            outcome,
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

    fn ensure_connected(&self) -> Result<(), ClientError> {
        match self.state {
            MachineState::Connected => Ok(()),
            MachineState::Open(_) => Err(ClientError::AlreadyOpen),
            MachineState::Closed { .. } => Err(ClientError::AlreadyClosed),
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

#[cfg(test)]
impl SessionMachine {
    pub(crate) fn seed_wrapped_probe_history_for_test(&mut self, now: ClientTimestamp) {
        let session = self
            .open_session_mut()
            .expect("wrapped probe history requires an open session");
        session.next_wire_seq = 0;
        session.timed_out.insert(PendingProbe {
            wire_seq: 0,
            sent_at: now,
            timeout_at: now.mono,
        });
        session.completed.insert(0);
    }
}

#[cfg(all(test, feature = "tokio"))]
impl SessionMachine {
    pub(crate) fn replace_pending_for_test(&mut self, probe: PendingProbe) {
        let session = self
            .open_session_mut()
            .expect("test pending probes require an open session");
        session.pending.remove(probe.wire_seq);
        session.pending.preflight_insert(probe.wire_seq).unwrap();
        session.pending.commit_insert(probe);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{schedule::ProbeSchedule, validate_datagram_length};

    fn open_machine(max_pending_probes: usize, probe_timeout: Duration) -> SessionMachine {
        let config = ClientConfig {
            max_pending_probes,
            probe_timeout,
            ..ClientConfig::default()
        };
        let remote = "127.0.0.1:2112".parse().unwrap();
        let mut machine = SessionMachine::new(config, remote).unwrap();
        let negotiated = NegotiatedParams {
            params: machine.requested.clone(),
            restrictions: Vec::new(),
        };
        machine.state = MachineState::Open(Box::new(ActiveSession {
            token: 0x0102_0304_0506_0708,
            negotiated,
            local_close_packet: encode_close_request(
                &CloseRequest {
                    token: 0x0102_0304_0506_0708,
                },
                None,
            )
            .unwrap()
            .into_boxed_slice(),
            next_wire_seq: 0,
            highest_received_seq: None,
            packets_sent: 0,
            pending: PendingMap::new(max_pending_probes),
            timed_out: TimedOutMap::new(max_pending_probes),
            completed: CompletedSet::new(max_pending_probes),
        }));
        machine
    }

    fn active(machine: &SessionMachine) -> &ActiveSession {
        match &machine.state {
            MachineState::Open(session) => session,
            _ => panic!("test machine must be open"),
        }
    }

    fn active_mut(machine: &mut SessionMachine) -> &mut ActiveSession {
        match &mut machine.state {
            MachineState::Open(session) => session,
            _ => panic!("test machine must be open"),
        }
    }

    fn timestamp(mono: Instant) -> ClientTimestamp {
        ClientTimestamp {
            mono,
            wall: SystemTime::now(),
        }
    }

    fn connected_machine(config: ClientConfig) -> SessionMachine {
        SessionMachine::new(config, "127.0.0.1:2112".parse().unwrap()).unwrap()
    }

    fn normal_open_reply(machine: &SessionMachine, token: u64) -> OpenReply {
        OpenReply {
            flags: flags::FLAG_OPEN | flags::FLAG_REPLY,
            token,
            params: machine.requested.clone(),
        }
    }

    fn encoded_open_reply(
        machine: &SessionMachine,
        reply: &OpenReply,
    ) -> Result<Vec<u8>, irtt_proto::ProtoError> {
        irtt_proto::encode_open_reply(reply, machine.config.hmac_key.as_deref())
    }

    #[test]
    fn prepared_open_request_is_exact_and_inert_when_dropped() {
        let machine = connected_machine(ClientConfig::default());
        let first = machine.prepare_open_request().unwrap();
        let second = machine.prepare_open_request().unwrap();

        assert_eq!(first.bytes, second.bytes);
        assert!(!first.bytes.is_empty());
        drop(first);
        drop(second);
        assert!(matches!(machine.state, MachineState::Connected));
    }

    #[test]
    fn malformed_wrong_direction_and_invalid_flags_are_ignored() {
        let machine = connected_machine(ClientConfig::default());
        assert!(matches!(
            machine.inspect_open_datagram(&[0_u8]),
            Ok(OpenDatagramDisposition::Ignore)
        ));

        let request = machine.prepare_open_request().unwrap();
        assert!(matches!(
            machine.inspect_open_datagram(&request.bytes),
            Ok(OpenDatagramDisposition::Ignore)
        ));

        let reply = normal_open_reply(&machine, 0x1020_3040_5060_7080);
        let mut reserved = encoded_open_reply(&machine, &reply).unwrap();
        reserved[3] |= 0x10;
        assert!(matches!(
            machine.inspect_open_datagram(&reserved),
            Ok(OpenDatagramDisposition::Ignore)
        ));
        assert!(matches!(machine.state, MachineState::Connected));
    }

    #[test]
    fn missing_unexpected_and_bad_hmac_are_ignored() {
        let key = b"open-key".to_vec();
        let authenticated = connected_machine(ClientConfig {
            hmac_key: Some(key.clone()),
            ..ClientConfig::default()
        });
        let reply = normal_open_reply(&authenticated, 0x1020_3040_5060_7080);
        let plain = irtt_proto::encode_open_reply(&reply, None).unwrap();
        assert!(matches!(
            authenticated.inspect_open_datagram(&plain),
            Ok(OpenDatagramDisposition::Ignore)
        ));

        let mut bad = encoded_open_reply(&authenticated, &reply).unwrap();
        *bad.last_mut().unwrap() ^= 0x80;
        assert!(matches!(
            authenticated.inspect_open_datagram(&bad),
            Ok(OpenDatagramDisposition::Ignore)
        ));

        let plain_machine = connected_machine(ClientConfig::default());
        let unexpected = irtt_proto::encode_open_reply(&reply, Some(&key)).unwrap();
        assert!(matches!(
            plain_machine.inspect_open_datagram(&unexpected),
            Ok(OpenDatagramDisposition::Ignore)
        ));
    }

    #[test]
    fn authenticated_zero_token_is_trusted_and_terminal() {
        let key = b"open-key".to_vec();
        let machine = connected_machine(ClientConfig {
            hmac_key: Some(key.clone()),
            ..ClientConfig::default()
        });
        let reply = normal_open_reply(&machine, 0x1020_3040_5060_7080);
        let mut packet = encoded_open_reply(&machine, &reply).unwrap();
        let token_offset = 4 + irtt_proto::HMAC_SIZE;
        packet[token_offset..token_offset + 8].fill(0);
        irtt_proto::compute_hmac_in_place(&key, &mut packet, 4).unwrap();

        assert!(matches!(
            machine.inspect_open_datagram(&packet),
            Err(ClientError::Protocol(irtt_proto::ProtoError::ZeroToken))
        ));
        assert!(matches!(machine.state, MachineState::Connected));
    }

    #[test]
    fn trusted_invalid_parameter_encoding_is_terminal() {
        let machine = connected_machine(ClientConfig::default());
        let mut packet = irtt_proto::MAGIC.to_vec();
        packet.push(flags::FLAG_OPEN | flags::FLAG_REPLY);
        packet.extend_from_slice(&0x1020_3040_5060_7080_u64.to_le_bytes());
        packet.push(0x80);

        assert!(matches!(
            machine.inspect_open_datagram(&packet),
            Err(ClientError::Protocol(
                irtt_proto::ProtoError::TruncatedVarint
            ))
        ));
        assert!(matches!(machine.state, MachineState::Connected));
    }

    #[test]
    fn trusted_rejection_version_and_run_mode_failures_do_not_open() {
        let machine = connected_machine(ClientConfig::default());
        let rejection = OpenReply {
            flags: flags::FLAG_OPEN | flags::FLAG_REPLY | flags::FLAG_CLOSE,
            token: 0,
            params: machine.requested.clone(),
        };
        assert!(matches!(
            machine
                .prepare_open_acceptance(rejection, timestamp(Instant::now()))
                .unwrap_err()
                .primary,
            ClientError::ServerRejected
        ));

        let mut version = normal_open_reply(&machine, 0x1020_3040_5060_7080);
        version.params.protocol_version += 1;
        assert!(matches!(
            machine
                .prepare_open_acceptance(version, timestamp(Instant::now()))
                .unwrap_err()
                .primary,
            ClientError::ProtocolVersionMismatch { .. }
        ));

        let no_test = connected_machine(ClientConfig {
            run_mode: RunMode::NoTest,
            ..ClientConfig::default()
        });
        assert!(matches!(
            no_test
                .prepare_open_acceptance(
                    normal_open_reply(&no_test, 0x1020_3040_5060_7080),
                    timestamp(Instant::now()),
                )
                .unwrap_err()
                .primary,
            ClientError::UnexpectedNoTestReply
        ));
        assert!(matches!(machine.state, MachineState::Connected));
        assert!(matches!(no_test.state, MachineState::Connected));
    }

    #[test]
    fn no_test_non_close_reply_prepares_authenticated_cleanup_without_state_change() {
        let key = b"cleanup-key".to_vec();
        let machine = connected_machine(ClientConfig {
            run_mode: RunMode::NoTest,
            hmac_key: Some(key.clone()),
            ..ClientConfig::default()
        });
        let token = 0x1020_3040_5060_7080;

        let failure = machine
            .prepare_open_acceptance(
                normal_open_reply(&machine, token),
                timestamp(Instant::now()),
            )
            .unwrap_err();

        assert!(matches!(
            failure.primary,
            ClientError::UnexpectedNoTestReply
        ));
        let cleanup = failure.cleanup_close.as_deref().unwrap();
        assert_eq!(cleanup[3], flags::FLAG_CLOSE | flags::FLAG_HMAC);
        irtt_proto::verify_hmac(&key, cleanup, 4).unwrap();
        assert_eq!(
            u64::from_le_bytes(
                cleanup[4 + irtt_proto::HMAC_SIZE..12 + irtt_proto::HMAC_SIZE]
                    .try_into()
                    .unwrap()
            ),
            token
        );
        drop(failure);
        assert!(matches!(machine.state, MachineState::Connected));
    }

    #[test]
    fn no_test_close_replies_do_not_prepare_cleanup() {
        let machine = connected_machine(ClientConfig {
            run_mode: RunMode::NoTest,
            ..ClientConfig::default()
        });
        let reply = |token| OpenReply {
            flags: flags::FLAG_OPEN | flags::FLAG_REPLY | flags::FLAG_CLOSE,
            token,
            params: machine.requested.clone(),
        };

        let prepared = machine
            .prepare_open_acceptance(reply(0), timestamp(Instant::now()))
            .unwrap();
        assert!(prepared.cleanup_close_packet().is_none());

        let failure = machine
            .prepare_open_acceptance(reply(0x1020_3040_5060_7080), timestamp(Instant::now()))
            .unwrap_err();
        assert!(matches!(
            failure.primary,
            ClientError::NonZeroNoTestToken { .. }
        ));
        assert!(failure.cleanup_close.is_none());
    }

    #[test]
    fn dropping_prepared_acceptance_leaves_connected_state() {
        let machine = connected_machine(ClientConfig::default());
        let prepared = machine
            .prepare_open_acceptance(
                normal_open_reply(&machine, 0x1020_3040_5060_7080),
                timestamp(Instant::now()),
            )
            .unwrap();

        assert!(prepared.has_prepared_active_session());
        assert!(prepared.cleanup_close_packet().is_some());
        drop(prepared);
        assert!(matches!(machine.state, MachineState::Connected));
    }

    #[test]
    fn commit_open_assigns_prebuilt_state_once() {
        let mut machine = connected_machine(ClientConfig::default());
        let token = 0x1020_3040_5060_7080;
        let prepared = machine
            .prepare_open_acceptance(
                normal_open_reply(&machine, token),
                timestamp(Instant::now()),
            )
            .unwrap();

        let expected_close = prepared.cleanup_close_packet().unwrap().to_vec();
        let outcome = machine.commit_open(prepared);

        assert!(matches!(
            outcome,
            OpenOutcome::Started {
                token: outcome_token,
                ..
            } if outcome_token == token
        ));
        assert!(machine.is_open());
        assert_eq!(active(&machine).local_close_packet.as_ref(), expected_close);
        assert!(matches!(
            machine.prepare_open_request(),
            Err(ClientError::AlreadyOpen)
        ));
    }

    #[test]
    fn no_test_preparation_completes_without_active_session() {
        let config = ClientConfig {
            run_mode: RunMode::NoTest,
            ..ClientConfig::default()
        };
        let mut machine = connected_machine(config);
        let reply = OpenReply {
            flags: flags::FLAG_OPEN | flags::FLAG_REPLY | flags::FLAG_CLOSE,
            token: 0,
            params: machine.requested.clone(),
        };
        let prepared = machine
            .prepare_open_acceptance(reply, timestamp(Instant::now()))
            .unwrap();

        assert!(prepared.normal_negotiated().is_none());
        assert!(prepared.cleanup_close_packet().is_none());
        assert!(matches!(machine.state, MachineState::Connected));
        let outcome = machine.commit_open(prepared);
        assert!(matches!(outcome, OpenOutcome::NoTestCompleted { .. }));
        assert!(matches!(machine.state, MachineState::NoTestCompleted));
    }

    #[test]
    fn cleanup_close_is_preencoded_with_token_and_hmac() {
        let key = b"cleanup-key".to_vec();
        let machine = connected_machine(ClientConfig {
            hmac_key: Some(key.clone()),
            ..ClientConfig::default()
        });
        let token = 0x1020_3040_5060_7080;
        let prepared = machine
            .prepare_open_acceptance(
                normal_open_reply(&machine, token),
                timestamp(Instant::now()),
            )
            .unwrap();
        let cleanup = prepared.cleanup_close_packet().unwrap();

        assert_eq!(cleanup[3], flags::FLAG_CLOSE | flags::FLAG_HMAC);
        irtt_proto::verify_hmac(&key, cleanup, 4).unwrap();
        assert_eq!(
            u64::from_le_bytes(
                cleanup[4 + irtt_proto::HMAC_SIZE..12 + irtt_proto::HMAC_SIZE]
                    .try_into()
                    .unwrap()
            ),
            token
        );
        assert!(matches!(machine.state, MachineState::Connected));
    }

    #[test]
    fn prepare_close_is_stable_inert_and_does_not_determine_event_timestamp() {
        let mut machine = open_machine(4, Duration::from_secs(1));
        active_mut(&mut machine).packets_sent = 7;

        let first_bytes = {
            let prepared = machine.prepare_close().unwrap();
            assert_eq!(
                prepared.bytes,
                encode_close_request(
                    &CloseRequest {
                        token: 0x0102_0304_0506_0708,
                    },
                    None,
                )
                .unwrap()
            );
            assert_eq!(prepared.commit.packets_sent, 7);
            assert_eq!(prepared.commit.token, 0x0102_0304_0506_0708);
            prepared.bytes.to_vec()
        };
        assert!(machine.is_open());
        assert_eq!(active(&machine).packets_sent, 7);

        let prepared = machine.prepare_close().unwrap();
        assert_eq!(prepared.bytes, first_bytes);
        assert_eq!(prepared.commit.packets_sent, 7);
        assert_eq!(prepared.commit.token, 0x0102_0304_0506_0708);
        assert!(machine.is_open());
    }

    #[test]
    fn local_close_commit_uses_supplied_exact_timestamp() {
        let mut machine = open_machine(4, Duration::from_secs(1));
        let remote = machine.remote;
        active_mut(&mut machine).packets_sent = 7;
        let prepared = machine.prepare_close().unwrap();
        let sent_at = ClientTimestamp {
            wall: UNIX_EPOCH + Duration::from_secs(1_234),
            mono: Instant::now() + Duration::from_secs(5),
        };
        let event = machine.commit_local_close(prepared.commit, sent_at);

        assert!(matches!(
            event,
            ClientEvent::SessionClosed {
                remote: event_remote,
                token: 0x0102_0304_0506_0708,
                at,
            } if event_remote == remote
                && at == sent_at
        ));
        assert!(matches!(
            machine.state,
            MachineState::Closed {
                source: CloseSource::Local,
                packets_sent: 7,
            }
        ));
        assert!(matches!(
            machine.prepare_close(),
            Err(ClientError::AlreadyClosed)
        ));
    }

    #[test]
    fn prepared_close_reuses_hmac_packet_built_during_open_acceptance() {
        let key = b"close-key".to_vec();
        let mut machine = connected_machine(ClientConfig {
            hmac_key: Some(key.clone()),
            ..ClientConfig::default()
        });
        let token = 0x1020_3040_5060_7080;
        let prepared_open = machine
            .prepare_open_acceptance(
                normal_open_reply(&machine, token),
                timestamp(Instant::now()),
            )
            .unwrap();
        let opening_close = prepared_open.cleanup_close_packet().unwrap().to_vec();
        machine.commit_open(prepared_open);

        let prepared_close = machine.prepare_close().unwrap();
        assert_eq!(prepared_close.bytes, opening_close);
        assert_eq!(
            prepared_close.bytes[3],
            flags::FLAG_CLOSE | flags::FLAG_HMAC
        );
        irtt_proto::verify_hmac(&key, prepared_close.bytes, 4).unwrap();
        assert_eq!(
            u64::from_le_bytes(
                prepared_close.bytes[4 + irtt_proto::HMAC_SIZE..12 + irtt_proto::HMAC_SIZE]
                    .try_into()
                    .unwrap()
            ),
            token
        );
    }

    #[test]
    fn close_preparation_preserves_non_open_errors_and_peer_source() {
        let connected = connected_machine(ClientConfig::default());
        assert!(matches!(
            connected.prepare_close(),
            Err(ClientError::NotOpen)
        ));

        let mut no_test = connected_machine(ClientConfig {
            run_mode: RunMode::NoTest,
            ..ClientConfig::default()
        });
        let reply = OpenReply {
            flags: flags::FLAG_OPEN | flags::FLAG_REPLY | flags::FLAG_CLOSE,
            token: 0,
            params: no_test.requested.clone(),
        };
        let prepared = no_test
            .prepare_open_acceptance(reply, timestamp(Instant::now()))
            .unwrap();
        no_test.commit_open(prepared);
        assert!(matches!(
            no_test.prepare_close(),
            Err(ClientError::AlreadyCompleted)
        ));

        let mut peer_closed = open_machine(4, Duration::from_secs(1));
        peer_closed.transition_to_closed(CloseSource::Peer);
        assert!(matches!(
            peer_closed.state,
            MachineState::Closed {
                source: CloseSource::Peer,
                ..
            }
        ));
        assert!(matches!(
            peer_closed.prepare_close(),
            Err(ClientError::AlreadyClosed)
        ));
    }

    #[test]
    fn dropping_prepared_probe_changes_nothing() {
        let machine = open_machine(4, Duration::from_secs(1));
        let prepared = machine.prepare_probe().unwrap().unwrap();
        assert_eq!(prepared.seq, 0);
        drop(prepared);

        let session = active(&machine);
        assert_eq!(session.next_wire_seq, 0);
        assert_eq!(session.packets_sent, 0);
        assert_eq!(session.pending.len(), 0);
    }

    #[test]
    fn repeated_uncommitted_preflight_changes_no_logical_state() {
        let mut machine = open_machine(4, Duration::from_secs(1));
        let prepared = machine.prepare_probe().unwrap().unwrap();

        let _first_preflight = machine.preflight_probe_commit(&prepared).unwrap();
        let _second_preflight = machine.preflight_probe_commit(&prepared).unwrap();

        let session = active(&machine);
        assert_eq!(session.next_wire_seq, 0);
        assert_eq!(session.packets_sent, 0);
        assert_eq!(session.pending.len(), 0);
    }

    #[test]
    fn stale_prepared_probe_is_rejected_without_changing_state() {
        let mut machine = open_machine(4, Duration::from_secs(1));
        let stale = machine.prepare_probe().unwrap().unwrap();
        let accepted = machine.prepare_probe().unwrap().unwrap();
        let sent_at = timestamp(Instant::now());
        let preflight = machine.preflight_probe_commit(&accepted).unwrap();
        let commit = machine.finalize_probe_commit(preflight, sent_at).unwrap();
        machine.commit_probe_sent(commit, accepted.bytes.len());

        let capacity = active(&machine).pending.capacity();
        assert!(matches!(
            machine.preflight_probe_commit(&stale),
            Err(ClientError::StalePreparedProbe {
                prepared_seq: 0,
                next_wire_seq: 1,
            })
        ));

        let session = active(&machine);
        assert_eq!(session.next_wire_seq, 1);
        assert_eq!(session.packets_sent, 1);
        assert_eq!(session.pending.len(), 1);
        assert_eq!(session.pending.capacity(), capacity);
    }

    #[test]
    fn simulated_send_error_leaves_machine_and_schedule_unchanged() {
        let mut machine = open_machine(4, Duration::from_secs(1));
        let start = Instant::now();
        let mut schedule = ProbeSchedule::new(start, &active(&machine).negotiated).unwrap();
        assert!(schedule.permit_probe_at(start));
        let prepared = machine.prepare_probe().unwrap().unwrap();
        let sent_at = timestamp(start);
        let machine_preflight = machine.preflight_probe_commit(&prepared).unwrap();
        let schedule_commit = schedule
            .preflight_caller_commit(machine_preflight.next_packets_sent)
            .unwrap();
        let machine_commit = machine
            .finalize_probe_commit(machine_preflight, sent_at)
            .unwrap();

        let _uncommitted_machine = machine_commit;
        let _uncommitted_schedule = schedule_commit;

        let session = active(&machine);
        assert_eq!(session.next_wire_seq, 0);
        assert_eq!(session.packets_sent, 0);
        assert_eq!(session.pending.len(), 0);
        assert_eq!(schedule.next_send_deadline(), Some(start));
    }

    #[test]
    fn short_success_commits_before_reporting_transport_invariant() {
        let mut machine = open_machine(4, Duration::from_secs(1));
        let start = Instant::now();
        let mut schedule = ProbeSchedule::new(start, &active(&machine).negotiated).unwrap();
        assert!(schedule.permit_probe_at(start));
        let prepared = machine.prepare_probe().unwrap().unwrap();
        let machine_preflight = machine.preflight_probe_commit(&prepared).unwrap();
        let machine_commit = machine
            .finalize_probe_commit(machine_preflight, timestamp(start))
            .unwrap();
        let schedule_commit = schedule.preflight_managed_commit(start, start).unwrap();
        let expected = prepared.bytes.len();
        let actual = expected - 1;

        machine.commit_probe_sent(machine_commit, actual);
        schedule.commit(schedule_commit);
        let result = validate_datagram_length(expected, actual);

        assert!(matches!(
            result,
            Err(ClientError::DatagramLengthMismatch { expected, actual })
                if actual + 1 == expected
        ));
        let session = active(&machine);
        assert_eq!(session.next_wire_seq, 1);
        assert_eq!(session.packets_sent, 1);
        assert_eq!(session.pending.len(), 1);
        assert_eq!(
            schedule.next_send_deadline(),
            Some(start + machine.config.interval)
        );
    }

    #[test]
    fn repeated_would_block_style_preflight_commits_once() {
        let mut machine = open_machine(4, Duration::from_secs(1));
        let prepared = machine.prepare_probe().unwrap().unwrap();
        let sent_at = timestamp(Instant::now());

        for _ in 0..3 {
            let preflight = machine.preflight_probe_commit(&prepared).unwrap();
            let _would_block_commit = machine.finalize_probe_commit(preflight, sent_at).unwrap();
        }
        let preflight = machine.preflight_probe_commit(&prepared).unwrap();
        let commit = machine.finalize_probe_commit(preflight, sent_at).unwrap();
        let sent = machine.commit_probe_sent(commit, prepared.bytes.len());

        assert_eq!(sent.seq, 0);
        let session = active(&machine);
        assert_eq!(session.next_wire_seq, 1);
        assert_eq!(session.packets_sent, 1);
        assert_eq!(session.pending.len(), 1);
    }

    #[test]
    fn probe_commit_does_not_require_presentation_timing() {
        let mut machine = open_machine(4, Duration::from_secs(1));
        let prepared = machine.prepare_probe().unwrap().unwrap();
        let sent_at = timestamp(Instant::now());
        let preflight = machine.preflight_probe_commit(&prepared).unwrap();
        let commit = machine.finalize_probe_commit(preflight, sent_at).unwrap();
        assert_eq!(commit.pending.sent_at, sent_at);
        assert_eq!(
            commit.pending.timeout_at,
            sent_at.mono + Duration::from_secs(1)
        );

        let sent = machine.commit_probe_sent(commit, prepared.bytes.len());

        assert_eq!(sent.seq, prepared.seq);
        assert_eq!(sent.sent_at, sent_at);
        assert_eq!(sent.bytes, prepared.bytes.len());
    }

    #[test]
    fn dropping_preflight_or_finalized_commit_changes_no_authoritative_state() {
        let mut machine = open_machine(4, Duration::from_secs(1));
        let prepared = machine.prepare_probe().unwrap().unwrap();
        {
            let _preflight = machine.preflight_probe_commit(&prepared).unwrap();
        }
        {
            let preflight = machine.preflight_probe_commit(&prepared).unwrap();
            let _commit = machine
                .finalize_probe_commit(preflight, timestamp(Instant::now()))
                .unwrap();
        }

        let session = active(&machine);
        assert_eq!(session.next_wire_seq, 0);
        assert_eq!(session.packets_sent, 0);
        assert_eq!(session.pending.len(), 0);
    }

    #[test]
    fn commit_uses_reserved_capacity_and_prevalidated_counter() {
        let mut machine = open_machine(2, Duration::from_secs(1));
        active_mut(&mut machine).packets_sent = u64::MAX - 1;
        let prepared = machine.prepare_probe().unwrap().unwrap();
        let preflight = machine.preflight_probe_commit(&prepared).unwrap();
        let reserved_capacity = active(&machine).pending.capacity();
        assert_eq!(preflight.next_packets_sent, u64::MAX);
        let commit = machine
            .finalize_probe_commit(preflight, timestamp(Instant::now()))
            .unwrap();

        machine.commit_probe_sent(commit, prepared.bytes.len());

        let session = active(&machine);
        assert_eq!(session.pending.capacity(), reserved_capacity);
        assert_eq!(session.pending.len(), 1);
        assert_eq!(session.packets_sent, u64::MAX);
        assert_eq!(session.next_wire_seq, 1);
    }

    #[test]
    fn pending_capacity_exhaustion_is_detected_before_commit() {
        let mut machine = open_machine(1, Duration::from_secs(1));
        let first = machine.prepare_probe().unwrap().unwrap();
        let sent_at = timestamp(Instant::now());
        let preflight = machine.preflight_probe_commit(&first).unwrap();
        let commit = machine.finalize_probe_commit(preflight, sent_at).unwrap();
        machine.commit_probe_sent(commit, first.bytes.len());

        let second = machine.prepare_probe().unwrap().unwrap();
        assert!(matches!(
            machine.preflight_probe_commit(&second),
            Err(ClientError::PendingLimitExceeded { limit: 1 })
        ));
        assert_eq!(active(&machine).packets_sent, 1);
    }

    #[test]
    fn pending_sequence_collision_is_detected_before_commit() {
        let mut machine = open_machine(2, Duration::from_secs(1));
        let first = machine.prepare_probe().unwrap().unwrap();
        let sent_at = timestamp(Instant::now());
        let preflight = machine.preflight_probe_commit(&first).unwrap();
        let commit = machine.finalize_probe_commit(preflight, sent_at).unwrap();
        machine.commit_probe_sent(commit, first.bytes.len());
        active_mut(&mut machine).next_wire_seq = 0;

        let reused = machine.prepare_probe().unwrap().unwrap();
        assert!(matches!(
            machine.preflight_probe_commit(&reused),
            Err(ClientError::PendingSequenceCollision { seq: 0 })
        ));
        assert_eq!(active(&machine).packets_sent, 1);
    }

    #[test]
    fn counter_overflow_is_detected_before_commit() {
        let mut machine = open_machine(2, Duration::from_secs(1));
        active_mut(&mut machine).packets_sent = u64::MAX;
        let prepared = machine.prepare_probe().unwrap().unwrap();

        assert!(matches!(
            machine.preflight_probe_commit(&prepared),
            Err(ClientError::CounterOverflow {
                counter: "packets_sent"
            })
        ));
        assert_eq!(active(&machine).pending.len(), 0);
    }

    #[test]
    fn timeout_overflow_is_detected_before_commit() {
        let mut machine = open_machine(2, Duration::MAX);
        let prepared = machine.prepare_probe().unwrap().unwrap();
        let preflight = machine.preflight_probe_commit(&prepared).unwrap();

        assert!(matches!(
            machine.finalize_probe_commit(preflight, timestamp(Instant::now())),
            Err(ClientError::DurationOverflow)
        ));
        assert_eq!(active(&machine).pending.len(), 0);
    }

    #[test]
    fn exhaustive_timeout_polling_returns_every_due_loss() {
        let mut machine = open_machine(4, Duration::from_secs(1));
        let now = Instant::now();
        for seq in [2, 0, 1] {
            let sent_at = timestamp(now - Duration::from_secs(u64::from(seq) + 1));
            let session = active_mut(&mut machine);
            session.pending.preflight_insert(seq).unwrap();
            session.pending.commit_insert(PendingProbe {
                wire_seq: seq,
                sent_at,
                timeout_at: now,
            });
        }

        let events = machine.poll_timeouts_at(now).unwrap();
        assert!(matches!(
            events.as_slice(),
            [
                ClientEvent::EchoLoss { seq: 2, .. },
                ClientEvent::EchoLoss { seq: 1, .. },
                ClientEvent::EchoLoss { seq: 0, .. },
            ]
        ));
        assert_eq!(active(&machine).pending.len(), 0);
        assert_eq!(active(&machine).timed_out.len(), 3);
    }

    #[test]
    fn wrapping_sequence_from_max_to_zero_remains_valid() {
        let mut machine = open_machine(2, Duration::from_secs(1));
        active_mut(&mut machine).next_wire_seq = u32::MAX;
        let prepared = machine.prepare_probe().unwrap().unwrap();
        let preflight = machine.preflight_probe_commit(&prepared).unwrap();
        let commit = machine
            .finalize_probe_commit(preflight, timestamp(Instant::now()))
            .unwrap();
        machine.commit_probe_sent(commit, prepared.bytes.len());

        assert_eq!(active(&machine).next_wire_seq, 0);
        assert_eq!(machine.prepare_probe().unwrap().unwrap().seq, 0);
    }

    #[test]
    fn successful_wrapped_reuse_purges_obsolete_history_only_on_commit() {
        let mut machine = open_machine(3, Duration::from_secs(1));
        let now = Instant::now();
        let obsolete = PendingProbe {
            wire_seq: 0,
            sent_at: timestamp(now - Duration::from_secs(1)),
            timeout_at: now,
        };
        let session = active_mut(&mut machine);
        session.next_wire_seq = 0;
        session.timed_out.insert(obsolete);
        session.completed.insert(0);

        let prepared = machine.prepare_probe().unwrap().unwrap();
        let preflight = machine.preflight_probe_commit(&prepared).unwrap();
        let commit = machine
            .finalize_probe_commit(preflight, timestamp(now))
            .unwrap();
        assert!(active(&machine).timed_out.contains(0));
        assert!(active(&machine).completed.contains(0));

        machine.commit_probe_sent(commit, prepared.bytes.len());
        let session = active(&machine);
        assert!(!session.timed_out.contains(0));
        assert!(!session.completed.contains(0));
        assert!(session.pending.contains(0));
        assert_eq!(session.next_wire_seq, 1);
        assert_eq!(session.packets_sent, 1);
    }
}
