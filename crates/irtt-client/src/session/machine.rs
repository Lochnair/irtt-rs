use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use irtt_proto::{
    decode_echo_reply, echo_packet_len, encode_request, flags, Clock, EchoReply, OpenReply, Params,
    RequestToEncode, ServerFill, TimestampFields, PROTOCOL_VERSION,
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
    socket_options::dscp_codepoint_to_traffic_class,
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
    wire_seq: u32,
    /// Operational timeout deadline, derived from the pre-send send anchor.
    /// See [`PendingProbe::timeout_at`].
    timeout_at: Instant,
    /// Pre-send wall-clock lower bound for kernel TX plausibility. See
    /// [`PendingProbe::tx_not_before_wall`].
    tx_not_before_wall: SystemTime,
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
        let bytes = encode_request(
            RequestToEncode::Open {
                params: &self.requested,
                no_test: self.config.run_mode == RunMode::NoTest,
            },
            self.config.hmac_key.as_deref(),
        )?;
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
            let bytes = encode_request(
                RequestToEncode::Close { token: reply.token },
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
        let bytes = encode_request(
            RequestToEncode::Echo {
                token: session.token,
                sequence: session.next_wire_seq,
                params: &session.negotiated.params,
                payload: &[],
            },
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

    /// Finalizes all fallible probe-commit preflight work that must happen
    /// before the socket send: the timeout deadline arithmetic (which can
    /// overflow) and capturing the pre-send wall-clock lower bound used to
    /// validate a later asynchronous kernel TX timestamp.
    ///
    /// `send_anchor` is a *private* pre-send timestamp, not the public
    /// measurement `sent_at` — it is sampled immediately before this call,
    /// before the socket send. Its `mono` field anchors the operational
    /// timeout deadline and its `wall` field becomes
    /// [`PendingProbe::tx_not_before_wall`]. See the client crate's
    /// `AGENTS.md` for why timeout semantics deliberately stay pre-send
    /// anchored while the public `sent_at` measurement moves after the send.
    pub(crate) fn finalize_probe_commit(
        &self,
        preflight: ProbeCommitPreflight,
        send_anchor: ClientTimestamp,
    ) -> Result<ProbeCommit, ClientError> {
        let timeout_at = send_anchor
            .mono
            .checked_add(self.config.probe_timeout)
            .ok_or(ClientError::DurationOverflow)?;

        Ok(ProbeCommit {
            wire_seq: preflight.seq,
            timeout_at,
            tx_not_before_wall: send_anchor.wall,
            next_wire_seq: preflight.next_wire_seq,
            next_packets_sent: preflight.next_packets_sent,
        })
    }

    /// Infallibly commits a probe as sent, using the post-send measurement
    /// `sent_at` captured immediately after the successful socket send
    /// returned. All fallible work already happened in
    /// [`Self::finalize_probe_commit`]; nothing here can fail.
    pub(crate) fn commit_probe_sent(
        &mut self,
        commit: ProbeCommit,
        sent_at: ClientTimestamp,
        bytes: usize,
    ) -> ProbeSent {
        let session = match &mut self.state {
            MachineState::Open(session) => session,
            _ => unreachable!("probe commits are only created for an open session"),
        };
        let seq = commit.wire_seq;
        let pending = PendingProbe {
            wire_seq: seq,
            sent_at,
            timeout_at: commit.timeout_at,
            tx_not_before_wall: commit.tx_not_before_wall,
            kernel_tx_timestamp: None,
        };
        session.timed_out.remove(seq);
        session.completed.remove(seq);
        session.pending.commit_insert(pending);
        session.next_wire_seq = commit.next_wire_seq;
        session.packets_sent = commit.next_packets_sent;

        ProbeSent {
            seq,
            sent_at,
            bytes,
        }
    }

    /// Record an observed Linux kernel TX timestamp for `wire_seq`, the
    /// automatic `SOF_TIMESTAMPING_OPT_ID` the kernel assigned when the
    /// datagram was submitted.
    ///
    /// `wire_seq` doubles as that correlation ID: both counters start at
    /// zero for a session's first timestamped send and advance by exactly
    /// one only on a confirmed successful submission, so they stay
    /// identical for the life of one open session (see the client crate's
    /// `AGENTS.md` for the full invariant).
    ///
    /// Updates a still-pending or already-timed-out probe in place. Never
    /// resurrects a completed, evicted, or unknown probe, and never touches
    /// `sent_at`, timeout/loss/reply state, or any counter — this is purely
    /// dormant metadata attachment. A probe that already has a timestamp
    /// keeps it: first usable observation wins, so a later duplicate cannot
    /// silently overwrite it.
    pub(crate) fn record_kernel_tx_timestamp(&mut self, wire_seq: u32, timestamp: SystemTime) {
        let MachineState::Open(session) = &mut self.state else {
            return;
        };
        if let Some(probe) = session.pending.get_mut(wire_seq) {
            probe.kernel_tx_timestamp.get_or_insert(timestamp);
            return;
        }
        if let Some(probe) = session.timed_out.get_mut(wire_seq) {
            probe.kernel_tx_timestamp.get_or_insert(timestamp);
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
                let one_way = compute_one_way(
                    &pending.sent_at,
                    pending.tx_not_before_wall,
                    &now,
                    pending.kernel_tx_timestamp,
                    &meta,
                    &reply.timestamps,
                );
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
                let one_way = compute_one_way(
                    &timed_out.sent_at,
                    timed_out.tx_not_before_wall,
                    &now,
                    timed_out.kernel_tx_timestamp,
                    &meta,
                    &reply.timestamps,
                );
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

/// Sizes the receive buffer for the negotiated layout.
///
/// Fallible only because a negotiated length wider than `usize` cannot name a
/// buffer at all. Negotiation already rejects a returned length that is
/// negative or larger than this client requested, and the requested length is a
/// `u32`, so the error is unreachable here in practice — it is propagated
/// rather than asserted away so that no 64-bit assumption is baked in.
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
        dscp: i64::from(dscp_codepoint_to_traffic_class(config.dscp)?),
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

/// Selects the client send wall-clock endpoint for upstream one-way delay.
///
/// Prefers the correlated Linux kernel `TX_SOFTWARE` timestamp — a software
/// timestamp near the driver handoff, not physical NIC departure — when it
/// is locally plausible: it must not precede `tx_not_before_wall` and must
/// not follow `received_at`, both on the client's own wall clock.
/// `tx_not_before_wall` is the pre-send send-anchor sample, not the
/// post-send `sent_at_wall` fallback below — a legitimate `TX_SOFTWARE`
/// timestamp can be generated while the send syscall is still in flight and
/// therefore legitimately precede the post-send `sent_at` sample, so the
/// lower bound must stay anchored before the send rather than after it.
/// Comparing only these two same-client anchors means the check detects
/// local causal-ordering inconsistency (e.g. a backward wall-clock step),
/// not every possible wall-clock discontinuity, and it deliberately does
/// not compare against the remote server's wall clock: cross-host clock
/// offset is a synchronization property of the resulting delay, not
/// evidence that this endpoint is invalid. There is no maximum lag bound —
/// unlike scheduler wakeup delay on the receive side, legitimate
/// send-to-receive time can exceed a second. Falls back to `sent_at_wall`
/// (the post-send measurement) when the kernel timestamp is absent or fails
/// this check; the raw observation on `PendingProbe` is never mutated by a
/// fallback here.
fn preferred_send_wall(
    tx_not_before_wall: SystemTime,
    sent_at_wall: SystemTime,
    kernel_tx_timestamp: Option<SystemTime>,
    received_at: SystemTime,
) -> SystemTime {
    match kernel_tx_timestamp {
        Some(kernel_tx) if tx_not_before_wall <= kernel_tx && kernel_tx <= received_at => kernel_tx,
        _ => sent_at_wall,
    }
}

/// Computes both one-way delay directions from wall-clock endpoints.
///
/// The upstream direction prefers a locally plausible correlated kernel
/// `TX_SOFTWARE` send timestamp over the userspace post-send `sent_at`
/// sample; see [`preferred_send_wall`]. `tx_not_before_wall` is the private
/// pre-send lower bound used only for that plausibility check — it is never
/// itself a candidate endpoint. The downstream direction uses
/// [`ReceiveMeta::preferred_receive_wall`], which prefers a plausible kernel
/// receive timestamp over the userspace receive wall sample. Neither kernel
/// timestamp is ever used for RTT.
pub(crate) fn compute_one_way(
    sent_at: &ClientTimestamp,
    tx_not_before_wall: SystemTime,
    received_at: &ClientTimestamp,
    kernel_tx_timestamp: Option<SystemTime>,
    meta: &ReceiveMeta,
    ts: &TimestampFields,
) -> Option<OneWayDelaySample> {
    let server_recv_wall = ts.recv_wall.or(ts.midpoint_wall);
    let server_send_wall = ts.send_wall.or(ts.midpoint_wall);

    let client_send_wall = preferred_send_wall(
        tx_not_before_wall,
        sent_at.wall,
        kernel_tx_timestamp,
        received_at.wall,
    );
    let client_send_ns = unix_epoch_ns_i64(client_send_wall);
    let client_recv_ns = unix_epoch_ns_i64(meta.preferred_receive_wall(received_at.wall));

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
    if config.clock == Clock::Unspecified {
        return Err(ClientError::InvalidConfig {
            reason: "clock must be wall, monotonic, or both".to_owned(),
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
            tx_not_before_wall: now.wall,
            kernel_tx_timestamp: None,
        });
        session.completed.insert(0);
    }
}

#[cfg(all(test, feature = "tokio"))]
impl SessionMachine {
    pub(crate) fn remove_pending_for_test(&mut self, wire_seq: u32) -> Option<PendingProbe> {
        self.open_session_mut()
            .expect("test pending probes require an open session")
            .pending
            .remove(wire_seq)
    }

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
            local_close_packet: encode_request(
                RequestToEncode::Close {
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
                encode_request(
                    RequestToEncode::Close {
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
    fn uncommitted_probe_preparation_changes_no_authoritative_state() {
        let mut machine = open_machine(4, Duration::from_secs(1));
        let prepared = machine.prepare_probe().unwrap().unwrap();
        assert_eq!(prepared.seq, 0);

        // Repeating preflight, and finalizing a commit that is never applied,
        // must both leave the session exactly as it was.
        {
            let _discarded_preflight = machine.preflight_probe_commit(&prepared).unwrap();
        }
        {
            let preflight = machine.preflight_probe_commit(&prepared).unwrap();
            let _discarded_commit = machine
                .finalize_probe_commit(preflight, timestamp(Instant::now()))
                .unwrap();
        }

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
        machine.commit_probe_sent(commit, sent_at, accepted.bytes.len());

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
        let sent = machine.commit_probe_sent(commit, sent_at, prepared.bytes.len());

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
        let send_anchor = timestamp(Instant::now());
        let preflight = machine.preflight_probe_commit(&prepared).unwrap();
        let commit = machine
            .finalize_probe_commit(preflight, send_anchor)
            .unwrap();
        // Fallible work (timeout deadline arithmetic, tx lower bound) is
        // finalized from the pre-send anchor, not any post-send sample.
        assert_eq!(commit.timeout_at, send_anchor.mono + Duration::from_secs(1));
        assert_eq!(commit.tx_not_before_wall, send_anchor.wall);

        // The post-send measurement `sent_at` is supplied afterward and can
        // legitimately differ from the pre-send anchor.
        let sent_at = timestamp(Instant::now());
        let sent = machine.commit_probe_sent(commit, sent_at, prepared.bytes.len());

        assert_eq!(sent.seq, prepared.seq);
        assert_eq!(sent.sent_at, sent_at);
        assert_eq!(sent.bytes, prepared.bytes.len());
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

        machine.commit_probe_sent(commit, timestamp(Instant::now()), prepared.bytes.len());

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
        machine.commit_probe_sent(commit, sent_at, first.bytes.len());

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
        machine.commit_probe_sent(commit, sent_at, first.bytes.len());
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
        for seq in [2, 1, 0] {
            let sent_at = timestamp(now - Duration::from_secs(u64::from(seq) + 1));
            let session = active_mut(&mut machine);
            session.pending.preflight_insert(seq).unwrap();
            session.pending.commit_insert(PendingProbe {
                wire_seq: seq,
                sent_at,
                timeout_at: sent_at.mono + Duration::from_secs(1),
                tx_not_before_wall: sent_at.wall,
                kernel_tx_timestamp: None,
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
        machine.commit_probe_sent(commit, timestamp(Instant::now()), prepared.bytes.len());

        assert_eq!(active(&machine).next_wire_seq, 0);
        assert_eq!(machine.prepare_probe().unwrap().unwrap().seq, 0);
    }

    #[test]
    fn successful_wrapped_reuse_purges_obsolete_history_only_on_commit() {
        let mut machine = open_machine(3, Duration::from_secs(1));
        let now = Instant::now();
        let obsolete_sent_at = timestamp(now - Duration::from_secs(1));
        let obsolete = PendingProbe {
            wire_seq: 0,
            sent_at: obsolete_sent_at,
            timeout_at: now,
            tx_not_before_wall: obsolete_sent_at.wall,
            kernel_tx_timestamp: None,
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

        machine.commit_probe_sent(commit, timestamp(now), prepared.bytes.len());
        let session = active(&machine);
        assert!(!session.timed_out.contains(0));
        assert!(!session.completed.contains(0));
        assert!(session.pending.contains(0));
        assert_eq!(session.next_wire_seq, 1);
        assert_eq!(session.packets_sent, 1);
    }

    // Downstream one-way delay endpoint selection.
    //
    // All values are anchored at a fixed wall-clock base so the expected
    // delays are exact: the client sends at the base, the server receives 5 ms
    // later and sends 10 ms later, userspace observes the reply at 30 ms
    // (20 ms downstream) and the kernel observed it at 25 ms (15 ms
    // downstream).
    const OWD_BASE_WALL_NS: i64 = 10_000_000_000;
    const OWD_SERVER_RECV_WALL_NS: i64 = OWD_BASE_WALL_NS + 5_000_000;
    const OWD_SERVER_SEND_WALL_NS: i64 = OWD_BASE_WALL_NS + 10_000_000;

    fn owd_wall(offset_ns: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_nanos(u64::try_from(OWD_BASE_WALL_NS).unwrap() + offset_ns)
    }

    fn owd_sent_at(mono: Instant) -> ClientTimestamp {
        ClientTimestamp {
            mono,
            wall: owd_wall(0),
        }
    }

    fn owd_received_at(mono: Instant) -> ClientTimestamp {
        ClientTimestamp {
            mono: mono + Duration::from_millis(40),
            wall: owd_wall(30_000_000),
        }
    }

    fn owd_timestamps() -> TimestampFields {
        TimestampFields {
            recv_wall: Some(OWD_SERVER_RECV_WALL_NS),
            send_wall: Some(OWD_SERVER_SEND_WALL_NS),
            ..Default::default()
        }
    }

    fn owd_reply(timestamps: TimestampFields) -> EchoReply {
        EchoReply {
            flags: flags::FLAG_REPLY,
            token: 0x0102_0304_0506_0708,
            sequence: 0,
            recv_count: None,
            recv_window: None,
            timestamps,
            payload: Vec::new(),
        }
    }

    fn kernel_rx_meta(offset_ns: u64) -> ReceiveMeta {
        ReceiveMeta {
            traffic_class: None,
            kernel_rx_timestamp: Some(owd_wall(offset_ns)),
        }
    }

    /// Machine with one outstanding probe for sequence 0. The pre-send
    /// `tx_not_before_wall` bound defaults to `sent_at.wall`, i.e. this
    /// helper does not exercise the pre-/post-send distinction; use
    /// [`machine_with_pending_probe_anchored`] where that distinction
    /// matters.
    fn machine_with_pending_probe(sent_at: ClientTimestamp) -> SessionMachine {
        machine_with_pending_probe_anchored(sent_at.wall, sent_at)
    }

    /// Machine with one outstanding probe for sequence 0, with an explicit
    /// pre-send `tx_not_before_wall` bound independent of `sent_at.wall`.
    fn machine_with_pending_probe_anchored(
        tx_not_before_wall: SystemTime,
        sent_at: ClientTimestamp,
    ) -> SessionMachine {
        let mut machine = open_machine(4, Duration::from_secs(1));
        let session = active_mut(&mut machine);
        session.pending.preflight_insert(0).unwrap();
        session.pending.commit_insert(PendingProbe {
            wire_seq: 0,
            sent_at,
            timeout_at: sent_at.mono + Duration::from_secs(1),
            tx_not_before_wall,
            kernel_tx_timestamp: None,
        });
        session.next_wire_seq = 1;
        machine
    }

    /// Machine whose probe for sequence 0 already timed out, so a reply for it
    /// is a measurable late reply.
    fn machine_with_timed_out_probe(sent_at: ClientTimestamp) -> SessionMachine {
        let mut machine = open_machine(4, Duration::from_secs(1));
        let session = active_mut(&mut machine);
        session.timed_out.insert(PendingProbe {
            wire_seq: 0,
            sent_at,
            timeout_at: sent_at.mono + Duration::from_secs(1),
            tx_not_before_wall: sent_at.wall,
            kernel_tx_timestamp: None,
        });
        session.next_wire_seq = 1;
        machine
    }

    fn reply_one_way(events: &[ClientEvent]) -> Option<OneWayDelaySample> {
        match events {
            [ClientEvent::EchoReply { one_way, .. } | ClientEvent::LateReply { one_way, .. }] => {
                *one_way
            }
            other => panic!("expected a single measurable reply event, got {other:?}"),
        }
    }

    fn reply_rtt(events: &[ClientEvent]) -> Option<RttSample> {
        match events {
            [ClientEvent::EchoReply { rtt, .. }] => Some(*rtt),
            [ClientEvent::LateReply { rtt, .. }] => *rtt,
            other => panic!("expected a single measurable reply event, got {other:?}"),
        }
    }

    fn process_owd_reply(
        machine: &mut SessionMachine,
        timestamps: TimestampFields,
        meta: ReceiveMeta,
        received_at: ClientTimestamp,
    ) -> Vec<ClientEvent> {
        machine
            .process_echo_reply(owd_reply(timestamps), 64, received_at, meta)
            .unwrap()
    }

    #[test]
    fn downstream_one_way_delay_prefers_valid_kernel_receive_time() {
        let mono = Instant::now();
        let mut machine = machine_with_pending_probe(owd_sent_at(mono));

        let events = process_owd_reply(
            &mut machine,
            owd_timestamps(),
            kernel_rx_meta(25_000_000),
            owd_received_at(mono),
        );

        let one_way = reply_one_way(&events).unwrap();
        assert_eq!(
            one_way.server_to_client,
            Some(SignedDuration::from_nanos(15_000_000))
        );
        assert_eq!(
            one_way.client_to_server,
            Some(SignedDuration::from_nanos(5_000_000))
        );
    }

    #[test]
    fn downstream_one_way_delay_uses_userspace_receive_time_without_kernel_metadata() {
        let mono = Instant::now();
        let mut machine = machine_with_pending_probe(owd_sent_at(mono));

        let events = process_owd_reply(
            &mut machine,
            owd_timestamps(),
            ReceiveMeta::default(),
            owd_received_at(mono),
        );

        let one_way = reply_one_way(&events).unwrap();
        assert_eq!(
            one_way.server_to_client,
            Some(SignedDuration::from_nanos(20_000_000))
        );
        assert_eq!(
            one_way.client_to_server,
            Some(SignedDuration::from_nanos(5_000_000))
        );
    }

    #[test]
    fn downstream_one_way_delay_falls_back_for_implausible_kernel_receive_time() {
        let mono = Instant::now();
        // Later than the userspace sample that observed the datagram.
        let mut future = machine_with_pending_probe(owd_sent_at(mono));
        let future_events = process_owd_reply(
            &mut future,
            owd_timestamps(),
            kernel_rx_meta(35_000_000),
            owd_received_at(mono),
        );

        // Lagging the userspace sample by far more than MAX_KERNEL_RX_LAG.
        let mut stale = machine_with_pending_probe(owd_sent_at(mono));
        let stale_meta = ReceiveMeta {
            traffic_class: None,
            kernel_rx_timestamp: Some(UNIX_EPOCH),
        };
        let stale_events = process_owd_reply(
            &mut stale,
            owd_timestamps(),
            stale_meta,
            owd_received_at(mono),
        );

        for events in [&future_events, &stale_events] {
            assert_eq!(
                reply_one_way(events).unwrap().server_to_client,
                Some(SignedDuration::from_nanos(20_000_000))
            );
        }
    }

    #[test]
    fn measurable_late_reply_uses_the_same_receive_wall_selection() {
        let mono = Instant::now();
        let mut kernel = machine_with_timed_out_probe(owd_sent_at(mono));
        let kernel_events = process_owd_reply(
            &mut kernel,
            owd_timestamps(),
            kernel_rx_meta(25_000_000),
            owd_received_at(mono),
        );

        let mut userspace = machine_with_timed_out_probe(owd_sent_at(mono));
        let userspace_events = process_owd_reply(
            &mut userspace,
            owd_timestamps(),
            ReceiveMeta::default(),
            owd_received_at(mono),
        );

        assert!(matches!(
            kernel_events.as_slice(),
            [ClientEvent::LateReply { .. }]
        ));
        assert_eq!(
            reply_one_way(&kernel_events).unwrap().server_to_client,
            Some(SignedDuration::from_nanos(15_000_000))
        );
        assert_eq!(
            reply_one_way(&userspace_events).unwrap().server_to_client,
            Some(SignedDuration::from_nanos(20_000_000))
        );
    }

    #[test]
    fn untracked_late_reply_reports_no_one_way_delay_with_kernel_metadata() {
        let mono = Instant::now();
        let mut machine = open_machine(4, Duration::from_secs(1));
        active_mut(&mut machine).highest_received_seq = Some(5);

        let events = process_owd_reply(
            &mut machine,
            owd_timestamps(),
            kernel_rx_meta(25_000_000),
            owd_received_at(mono),
        );

        match events.as_slice() {
            [ClientEvent::LateReply {
                sent_at,
                rtt,
                one_way,
                ..
            }] => {
                assert!(sent_at.is_none());
                assert!(rtt.is_none());
                assert!(one_way.is_none());
            }
            other => panic!("expected an untracked LateReply, got {other:?}"),
        }
    }

    #[test]
    fn kernel_receive_time_changes_no_measurement_other_than_downstream_delay() {
        let mono = Instant::now();
        let timestamps = TimestampFields {
            recv_mono: Some(1_000_000),
            send_mono: Some(3_000_000),
            ..owd_timestamps()
        };

        let mut kernel = machine_with_pending_probe(owd_sent_at(mono));
        let kernel_events = process_owd_reply(
            &mut kernel,
            timestamps.clone(),
            kernel_rx_meta(25_000_000),
            owd_received_at(mono),
        );

        let mut userspace = machine_with_pending_probe(owd_sent_at(mono));
        let userspace_events = process_owd_reply(
            &mut userspace,
            timestamps,
            ReceiveMeta::default(),
            owd_received_at(mono),
        );

        // RTT stays a purely monotonic userspace measurement.
        assert_eq!(
            reply_rtt(&kernel_events),
            reply_rtt(&userspace_events),
            "kernel receive metadata must not affect RTT"
        );
        assert_eq!(
            reply_rtt(&kernel_events).unwrap().raw,
            Duration::from_millis(40)
        );

        // Upstream delay uses the client send wall time only.
        assert_eq!(
            reply_one_way(&kernel_events).unwrap().client_to_server,
            reply_one_way(&userspace_events).unwrap().client_to_server
        );
        assert_ne!(
            reply_one_way(&kernel_events).unwrap().server_to_client,
            reply_one_way(&userspace_events).unwrap().server_to_client
        );
    }

    #[test]
    fn kernel_receive_time_applies_against_a_server_midpoint_timestamp() {
        let mono = Instant::now();
        let midpoint = TimestampFields {
            midpoint_wall: Some(OWD_SERVER_SEND_WALL_NS),
            ..Default::default()
        };
        let mut machine = machine_with_pending_probe(owd_sent_at(mono));

        let events = process_owd_reply(
            &mut machine,
            midpoint,
            kernel_rx_meta(25_000_000),
            owd_received_at(mono),
        );

        let one_way = reply_one_way(&events).unwrap();
        assert_eq!(
            one_way.server_to_client,
            Some(SignedDuration::from_nanos(15_000_000))
        );
        assert_eq!(
            one_way.client_to_server,
            Some(SignedDuration::from_nanos(10_000_000))
        );
    }

    #[test]
    fn kernel_receive_time_does_not_create_missing_cross_instant_directions() {
        let mono = Instant::now();
        let receive_only = TimestampFields {
            recv_wall: Some(OWD_SERVER_RECV_WALL_NS),
            ..Default::default()
        };
        let send_only = TimestampFields {
            send_wall: Some(OWD_SERVER_SEND_WALL_NS),
            ..Default::default()
        };

        let mut receive_machine = machine_with_pending_probe(owd_sent_at(mono));
        let receive_events = process_owd_reply(
            &mut receive_machine,
            receive_only,
            kernel_rx_meta(25_000_000),
            owd_received_at(mono),
        );
        let receive_sample = reply_one_way(&receive_events).unwrap();
        assert_eq!(
            receive_sample.client_to_server,
            Some(SignedDuration::from_nanos(5_000_000))
        );
        assert_eq!(receive_sample.server_to_client, None);

        let mut send_machine = machine_with_pending_probe(owd_sent_at(mono));
        let send_events = process_owd_reply(
            &mut send_machine,
            send_only,
            kernel_rx_meta(25_000_000),
            owd_received_at(mono),
        );
        let send_sample = reply_one_way(&send_events).unwrap();
        assert_eq!(send_sample.client_to_server, None);
        assert_eq!(
            send_sample.server_to_client,
            Some(SignedDuration::from_nanos(15_000_000))
        );
    }

    // Kernel TX timestamp correlation (`record_kernel_tx_timestamp`).
    //
    // These probe `SessionMachine`'s private association state directly.
    // The kernel TX timestamp is dormant metadata in this change: none of
    // these tests assert anything about RTT/OWD/IPDV output, only about
    // where `PendingProbe::kernel_tx_timestamp` ends up.

    fn tx_ts(offset_secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(offset_secs)
    }

    fn send_probe(machine: &mut SessionMachine, sent_at: ClientTimestamp) -> u32 {
        let prepared = machine.prepare_probe().unwrap().unwrap();
        let preflight = machine.preflight_probe_commit(&prepared).unwrap();
        let commit = machine.finalize_probe_commit(preflight, sent_at).unwrap();
        machine
            .commit_probe_sent(commit, sent_at, prepared.bytes.len())
            .seq
    }

    #[test]
    fn kernel_tx_timestamp_attaches_to_pending_probe() {
        let mut machine = open_machine(4, Duration::from_secs(1));
        let now = Instant::now();
        let seq = send_probe(&mut machine, timestamp(now));

        machine.record_kernel_tx_timestamp(seq, tx_ts(1));

        assert_eq!(
            active_mut(&mut machine)
                .pending
                .get_mut(seq)
                .unwrap()
                .kernel_tx_timestamp,
            Some(tx_ts(1))
        );
    }

    #[test]
    fn kernel_tx_timestamp_for_unknown_id_does_nothing() {
        let mut machine = open_machine(4, Duration::from_secs(1));
        let now = Instant::now();
        let seq = send_probe(&mut machine, timestamp(now));

        // No probe was ever sent for sequence 41; recording against it must
        // not panic, allocate a phantom entry, or affect the real probe.
        machine.record_kernel_tx_timestamp(41, tx_ts(9));

        assert!(active_mut(&mut machine)
            .pending
            .get_mut(seq)
            .unwrap()
            .kernel_tx_timestamp
            .is_none());
        assert_eq!(active(&machine).pending.len(), 1);
    }

    #[test]
    fn kernel_tx_timestamps_associate_correctly_when_observed_out_of_order() {
        let mut machine = open_machine(4, Duration::from_secs(1));
        let now = Instant::now();
        let seq10 = send_probe(&mut machine, timestamp(now));
        let seq11 = send_probe(&mut machine, timestamp(now));
        let seq12 = send_probe(&mut machine, timestamp(now));
        assert_eq!((seq10, seq11, seq12), (0, 1, 2));

        // Linux does not guarantee MSG_ERRQUEUE dequeue order matches send
        // order; observe them out of order (12, 10, 11).
        machine.record_kernel_tx_timestamp(seq12, tx_ts(12));
        machine.record_kernel_tx_timestamp(seq10, tx_ts(10));
        machine.record_kernel_tx_timestamp(seq11, tx_ts(11));

        let session = active_mut(&mut machine);
        assert_eq!(
            session.pending.get_mut(seq10).unwrap().kernel_tx_timestamp,
            Some(tx_ts(10))
        );
        assert_eq!(
            session.pending.get_mut(seq11).unwrap().kernel_tx_timestamp,
            Some(tx_ts(11))
        );
        assert_eq!(
            session.pending.get_mut(seq12).unwrap().kernel_tx_timestamp,
            Some(tx_ts(12))
        );
    }

    #[test]
    fn first_valid_kernel_tx_timestamp_wins_over_a_later_duplicate() {
        let mut machine = open_machine(4, Duration::from_secs(1));
        let now = Instant::now();
        let seq = send_probe(&mut machine, timestamp(now));

        machine.record_kernel_tx_timestamp(seq, tx_ts(1));
        machine.record_kernel_tx_timestamp(seq, tx_ts(2));

        assert_eq!(
            active_mut(&mut machine)
                .pending
                .get_mut(seq)
                .unwrap()
                .kernel_tx_timestamp,
            Some(tx_ts(1))
        );
    }

    #[test]
    fn probe_timeout_preserves_an_existing_kernel_tx_timestamp() {
        let mut machine = open_machine(4, Duration::from_secs(1));
        let now = Instant::now();
        let seq = send_probe(&mut machine, timestamp(now));
        machine.record_kernel_tx_timestamp(seq, tx_ts(1));

        let events = machine
            .poll_timeouts_at(now + Duration::from_secs(2))
            .unwrap();
        assert!(matches!(events.as_slice(), [ClientEvent::EchoLoss { .. }]));

        let session = active_mut(&mut machine);
        assert!(!session.pending.contains(seq));
        assert_eq!(
            session.timed_out.get_mut(seq).unwrap().kernel_tx_timestamp,
            Some(tx_ts(1))
        );
    }

    #[test]
    fn kernel_tx_timestamp_arriving_after_timeout_attaches_to_timed_out_probe() {
        let mut machine = open_machine(4, Duration::from_secs(1));
        let now = Instant::now();
        let seq = send_probe(&mut machine, timestamp(now));

        machine
            .poll_timeouts_at(now + Duration::from_secs(2))
            .unwrap();
        assert!(active(&machine).timed_out.contains(seq));

        machine.record_kernel_tx_timestamp(seq, tx_ts(3));

        assert_eq!(
            active_mut(&mut machine)
                .timed_out
                .get_mut(seq)
                .unwrap()
                .kernel_tx_timestamp,
            Some(tx_ts(3))
        );
    }

    #[test]
    fn measurable_late_reply_retains_the_kernel_tx_timestamp() {
        let now = Instant::now();
        let mut machine = machine_with_timed_out_probe(owd_sent_at(now));
        machine.record_kernel_tx_timestamp(0, tx_ts(4));
        assert_eq!(
            active_mut(&mut machine)
                .timed_out
                .get_mut(0)
                .unwrap()
                .kernel_tx_timestamp,
            Some(tx_ts(4))
        );

        // The reply is measurable (a late reply with a retained sent_at);
        // OWD/RTT still come from sent_at only, per NO_OWD_CHANGE, but the
        // dormant kernel timestamp must have already been retained above
        // and this call must not disturb that.
        let events = process_owd_reply(
            &mut machine,
            owd_timestamps(),
            kernel_rx_meta(25_000_000),
            owd_received_at(now),
        );
        assert!(matches!(events.as_slice(), [ClientEvent::LateReply { .. }]));
    }

    #[test]
    fn completed_probe_ignores_a_later_kernel_tx_timestamp() {
        let mut machine = open_machine(4, Duration::from_secs(1));
        let now = Instant::now();
        let seq = send_probe(&mut machine, timestamp(now));
        let reply = EchoReply {
            flags: 0,
            token: active(&machine).token,
            sequence: seq,
            recv_count: None,
            recv_window: None,
            timestamps: TimestampFields::default(),
            payload: Vec::new(),
        };
        machine
            .process_echo_reply(
                reply,
                64,
                ClientTimestamp {
                    mono: now,
                    wall: SystemTime::now(),
                },
                ReceiveMeta::default(),
            )
            .unwrap();
        assert!(active(&machine).completed.contains(seq));
        assert!(!active(&machine).pending.contains(seq));

        // Must not resurrect the probe into either map.
        machine.record_kernel_tx_timestamp(seq, tx_ts(5));

        let session = active_mut(&mut machine);
        assert!(session.pending.get_mut(seq).is_none());
        assert!(session.timed_out.get_mut(seq).is_none());
    }

    #[test]
    fn evicted_timed_out_probe_ignores_a_later_kernel_tx_timestamp() {
        let mut machine = open_machine(1, Duration::from_secs(1));
        let now = Instant::now();
        let first = send_probe(&mut machine, timestamp(now));
        machine
            .poll_timeouts_at(now + Duration::from_secs(2))
            .unwrap();
        assert!(active(&machine).timed_out.contains(first));

        // capacity 1: sending and timing out a second probe evicts the first
        // from the bounded TimedOutMap.
        let second = send_probe(&mut machine, timestamp(now + Duration::from_secs(2)));
        machine
            .poll_timeouts_at(now + Duration::from_secs(4))
            .unwrap();
        assert!(active(&machine).timed_out.contains(second));
        assert!(!active(&machine).timed_out.contains(first));

        // Recording against the evicted ID must not panic or resurrect it.
        machine.record_kernel_tx_timestamp(first, tx_ts(6));
        assert!(active_mut(&mut machine).timed_out.get_mut(first).is_none());
    }

    #[test]
    fn kernel_tx_timestamp_recording_is_a_no_op_once_closed() {
        let mut machine = open_machine(4, Duration::from_secs(1));
        let now = Instant::now();
        let seq = send_probe(&mut machine, timestamp(now));
        let prepared = machine.prepare_close().unwrap();
        let commit = prepared.commit;
        machine.commit_local_close(commit, timestamp(now));
        assert!(machine.is_terminal());

        // Recording after close must not panic (there is no open session to
        // index into) and must remain inert.
        machine.record_kernel_tx_timestamp(seq, tx_ts(7));
    }

    #[test]
    fn kernel_tx_timestamp_ids_wrap_like_wire_seq() {
        let mut machine = open_machine(4, Duration::from_secs(1));
        active_mut(&mut machine).next_wire_seq = u32::MAX;
        let now = Instant::now();
        let last = send_probe(&mut machine, timestamp(now));
        let wrapped = send_probe(&mut machine, timestamp(now));
        assert_eq!(last, u32::MAX);
        assert_eq!(wrapped, 0);

        machine.record_kernel_tx_timestamp(u32::MAX, tx_ts(1));
        machine.record_kernel_tx_timestamp(0, tx_ts(2));

        let session = active_mut(&mut machine);
        assert_eq!(
            session
                .pending
                .get_mut(u32::MAX)
                .unwrap()
                .kernel_tx_timestamp,
            Some(tx_ts(1))
        );
        assert_eq!(
            session.pending.get_mut(0).unwrap().kernel_tx_timestamp,
            Some(tx_ts(2))
        );
    }

    #[test]
    fn kernel_tx_timestamp_changes_only_upstream_one_way_delay() {
        let mono = Instant::now();
        let mut without = machine_with_pending_probe(owd_sent_at(mono));
        let mut with = machine_with_pending_probe(owd_sent_at(mono));
        // Plausible: strictly between sent_at and received_at on the
        // client's own wall clock.
        with.record_kernel_tx_timestamp(0, owd_wall(2_000_000));

        let without_events = process_owd_reply(
            &mut without,
            owd_timestamps(),
            kernel_rx_meta(25_000_000),
            owd_received_at(mono),
        );
        let with_events = process_owd_reply(
            &mut with,
            owd_timestamps(),
            kernel_rx_meta(25_000_000),
            owd_received_at(mono),
        );

        let without_reply = match without_events.as_slice() {
            [ClientEvent::EchoReply { rtt, one_way, .. }] => (*rtt, *one_way),
            other => panic!("expected one EchoReply, got {other:?}"),
        };
        let with_reply = match with_events.as_slice() {
            [ClientEvent::EchoReply { rtt, one_way, .. }] => (*rtt, *one_way),
            other => panic!("expected one EchoReply, got {other:?}"),
        };

        // RTT is a purely monotonic userspace measurement.
        assert_eq!(without_reply.0, with_reply.0, "RTT must be unaffected");
        // Downstream delay is governed solely by the kernel RX selection.
        assert_eq!(
            without_reply.1.unwrap().server_to_client,
            with_reply.1.unwrap().server_to_client,
            "downstream delay must be unaffected"
        );
        // Upstream delay is the one thing the kernel TX timestamp changes.
        assert_ne!(
            without_reply.1.unwrap().client_to_server,
            with_reply.1.unwrap().client_to_server
        );
        assert_eq!(
            with_reply.1.unwrap().client_to_server,
            Some(SignedDuration::from_nanos(3_000_000))
        );
    }

    // Upstream one-way delay: preferred client send wall selection.
    //
    // `preferred_send_wall` is exercised directly first (pure plausibility
    // logic), then through `compute_one_way`/`process_echo_reply` to pin the
    // end-to-end measurement behavior described in the client crate's
    // `AGENTS.md`.

    fn send_wall_ms(offset_ms: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(offset_ms)
    }

    #[test]
    fn preferred_send_wall_without_kernel_timestamp_uses_sent_at() {
        let anchor = send_wall_ms(1_000);
        let received_at = send_wall_ms(1_040);
        assert_eq!(
            preferred_send_wall(anchor, anchor, None, received_at),
            anchor
        );
    }

    #[test]
    fn preferred_send_wall_uses_kernel_timestamp_strictly_between_bounds() {
        let anchor = send_wall_ms(1_000);
        let kernel_tx = send_wall_ms(1_005);
        let received_at = send_wall_ms(1_040);
        assert_eq!(
            preferred_send_wall(anchor, anchor, Some(kernel_tx), received_at),
            kernel_tx
        );
    }

    #[test]
    fn preferred_send_wall_accepts_kernel_timestamp_equal_to_lower_bound() {
        let anchor = send_wall_ms(1_000);
        let received_at = send_wall_ms(1_040);
        assert_eq!(
            preferred_send_wall(anchor, anchor, Some(anchor), received_at),
            anchor
        );
    }

    #[test]
    fn preferred_send_wall_accepts_kernel_timestamp_equal_to_received_at() {
        let anchor = send_wall_ms(1_000);
        let received_at = send_wall_ms(1_040);
        assert_eq!(
            preferred_send_wall(anchor, anchor, Some(received_at), received_at),
            received_at
        );
    }

    #[test]
    fn preferred_send_wall_rejects_kernel_timestamp_earlier_than_lower_bound() {
        let anchor = send_wall_ms(1_000);
        let kernel_tx = anchor - Duration::from_nanos(1);
        let received_at = send_wall_ms(1_040);
        assert_eq!(
            preferred_send_wall(anchor, anchor, Some(kernel_tx), received_at),
            anchor
        );
    }

    #[test]
    fn preferred_send_wall_rejects_kernel_timestamp_later_than_received_at() {
        let anchor = send_wall_ms(1_000);
        let received_at = send_wall_ms(1_040);
        let kernel_tx = received_at + Duration::from_nanos(1);
        assert_eq!(
            preferred_send_wall(anchor, anchor, Some(kernel_tx), received_at),
            anchor
        );
    }

    #[test]
    fn preferred_send_wall_handles_extreme_timestamps_without_panicking() {
        let anchor = send_wall_ms(1_000);
        let received_at = send_wall_ms(1_040);
        let far_future = UNIX_EPOCH + Duration::from_secs(u64::from(u32::MAX)) * 4;
        let before_epoch = UNIX_EPOCH - Duration::from_secs(u64::from(u32::MAX));

        assert_eq!(
            preferred_send_wall(anchor, anchor, Some(far_future), received_at),
            anchor
        );
        assert_eq!(
            preferred_send_wall(anchor, anchor, Some(before_epoch), received_at),
            anchor
        );
        assert_eq!(
            preferred_send_wall(before_epoch, before_epoch, None, received_at),
            before_epoch
        );
    }

    // Pre-/post-send decoupling (F-04): the plausibility lower bound is the
    // pre-send anchor, but the fallback endpoint is the post-send sample,
    // and the two can legitimately differ.

    #[test]
    fn preferred_send_wall_accepts_kernel_timestamp_before_post_send_sample() {
        let tx_not_before_wall = send_wall_ms(1_000);
        let post_send_sent_at = send_wall_ms(1_008);
        let kernel_tx = send_wall_ms(1_005);
        let received_at = send_wall_ms(1_040);

        // The kernel timestamp precedes the post-send sample but follows
        // the pre-send anchor: it must still be accepted.
        assert_eq!(
            preferred_send_wall(
                tx_not_before_wall,
                post_send_sent_at,
                Some(kernel_tx),
                received_at
            ),
            kernel_tx
        );
    }

    #[test]
    fn preferred_send_wall_rejected_kernel_timestamp_falls_back_to_post_send_sample() {
        let tx_not_before_wall = send_wall_ms(1_000);
        let post_send_sent_at = send_wall_ms(1_008);
        let kernel_tx = send_wall_ms(999);
        let received_at = send_wall_ms(1_040);

        // Below the pre-send lower bound: rejected, and the fallback is the
        // post-send sample, not the pre-send anchor.
        assert_eq!(
            preferred_send_wall(
                tx_not_before_wall,
                post_send_sent_at,
                Some(kernel_tx),
                received_at
            ),
            post_send_sent_at
        );
    }

    fn upstream_probe(sent_wall: SystemTime, mono: Instant) -> SessionMachine {
        machine_with_pending_probe(ClientTimestamp {
            mono,
            wall: sent_wall,
        })
    }

    fn upstream_reply(
        machine: &mut SessionMachine,
        server_recv_wall_ms: u64,
        received_wall: SystemTime,
        mono: Instant,
    ) -> Vec<ClientEvent> {
        let timestamps = TimestampFields {
            recv_wall: Some(
                i64::try_from(Duration::from_millis(server_recv_wall_ms).as_nanos()).unwrap(),
            ),
            ..Default::default()
        };
        let received_at = ClientTimestamp {
            mono: mono + Duration::from_millis(40),
            wall: received_wall,
        };
        process_owd_reply(machine, timestamps, ReceiveMeta::default(), received_at)
    }

    #[test]
    fn upstream_one_way_delay_prefers_plausible_kernel_tx_time() {
        let mono = Instant::now();
        let mut machine = upstream_probe(send_wall_ms(1_000), mono);
        machine.record_kernel_tx_timestamp(0, send_wall_ms(1_005));

        let events = upstream_reply(&mut machine, 1_020, send_wall_ms(1_040), mono);

        assert_eq!(
            reply_one_way(&events).unwrap().client_to_server,
            Some(SignedDuration::from_nanos(15_000_000))
        );
    }

    #[test]
    fn upstream_one_way_delay_falls_back_without_kernel_tx() {
        let mono = Instant::now();
        let mut machine = upstream_probe(send_wall_ms(1_000), mono);

        let events = upstream_reply(&mut machine, 1_020, send_wall_ms(1_040), mono);

        assert_eq!(
            reply_one_way(&events).unwrap().client_to_server,
            Some(SignedDuration::from_nanos(20_000_000))
        );
    }

    #[test]
    fn upstream_one_way_delay_falls_back_for_kernel_tx_earlier_than_sent_at() {
        let mono = Instant::now();
        let mut machine = upstream_probe(send_wall_ms(1_000), mono);
        machine.record_kernel_tx_timestamp(0, send_wall_ms(999));

        let events = upstream_reply(&mut machine, 1_020, send_wall_ms(1_040), mono);

        assert_eq!(
            reply_one_way(&events).unwrap().client_to_server,
            Some(SignedDuration::from_nanos(20_000_000))
        );
    }

    #[test]
    fn upstream_one_way_delay_falls_back_for_kernel_tx_later_than_received_at() {
        let mono = Instant::now();
        let mut machine = upstream_probe(send_wall_ms(1_000), mono);
        machine.record_kernel_tx_timestamp(0, send_wall_ms(1_041));

        let events = upstream_reply(&mut machine, 1_020, send_wall_ms(1_040), mono);

        assert_eq!(
            reply_one_way(&events).unwrap().client_to_server,
            Some(SignedDuration::from_nanos(20_000_000))
        );
    }

    #[test]
    fn upstream_one_way_delay_measurable_late_reply_uses_the_retained_kernel_tx_time() {
        let mono = Instant::now();
        let mut machine = machine_with_timed_out_probe(ClientTimestamp {
            mono,
            wall: send_wall_ms(1_000),
        });
        machine.record_kernel_tx_timestamp(0, send_wall_ms(1_005));

        let events = upstream_reply(&mut machine, 1_020, send_wall_ms(1_040), mono);

        assert!(matches!(events.as_slice(), [ClientEvent::LateReply { .. }]));
        assert_eq!(
            reply_one_way(&events).unwrap().client_to_server,
            Some(SignedDuration::from_nanos(15_000_000))
        );
    }

    #[test]
    fn upstream_one_way_delay_untracked_late_reply_stays_unmeasurable() {
        let mono = Instant::now();
        let mut machine = open_machine(4, Duration::from_secs(1));
        active_mut(&mut machine).highest_received_seq = Some(5);

        let events = upstream_reply(&mut machine, 1_020, send_wall_ms(1_040), mono);

        match events.as_slice() {
            [ClientEvent::LateReply { one_way, .. }] => assert!(one_way.is_none()),
            other => panic!("expected an untracked LateReply, got {other:?}"),
        }
    }

    #[test]
    fn upstream_one_way_delay_applies_against_a_server_midpoint_timestamp() {
        let mono = Instant::now();
        let mut machine = upstream_probe(send_wall_ms(100), mono);
        machine.record_kernel_tx_timestamp(0, send_wall_ms(105));

        let timestamps = TimestampFields {
            midpoint_wall: Some(120_000_000),
            ..Default::default()
        };
        let received_at = ClientTimestamp {
            mono: mono + Duration::from_millis(40),
            wall: send_wall_ms(140),
        };
        let events = process_owd_reply(
            &mut machine,
            timestamps,
            ReceiveMeta::default(),
            received_at,
        );

        assert_eq!(
            reply_one_way(&events).unwrap().client_to_server,
            Some(SignedDuration::from_nanos(15_000_000))
        );
    }

    #[test]
    fn upstream_one_way_delay_remains_unavailable_for_send_only_server_timestamps() {
        let mono = Instant::now();
        let mut machine = upstream_probe(send_wall_ms(1_000), mono);
        machine.record_kernel_tx_timestamp(0, send_wall_ms(1_005));

        let timestamps = TimestampFields {
            send_wall: Some(1_010_000_000),
            ..Default::default()
        };
        let received_at = ClientTimestamp {
            mono: mono + Duration::from_millis(40),
            wall: send_wall_ms(1_040),
        };
        let events = process_owd_reply(
            &mut machine,
            timestamps,
            ReceiveMeta::default(),
            received_at,
        );

        assert_eq!(reply_one_way(&events).unwrap().client_to_server, None);
    }

    // F-04: post-send `sent_at` measurement timing.
    //
    // `sent_at` moved from immediately before timeout finalization/socket
    // send to immediately after a successful send. These tests pin the
    // resulting end-to-end behavior: raw RTT and the userspace upstream OWD
    // fallback both shrink because they no longer include the send-call
    // interval, while kernel TX plausibility keeps a separate pre-send
    // lower bound so a legitimate `TX_SOFTWARE` timestamp generated during
    // the send path is not rejected merely for preceding the post-send
    // sample.

    #[test]
    fn raw_rtt_uses_post_send_sent_at_not_pre_send_anchor() {
        let base = Instant::now();
        // A pre-send anchor at base+100ms is deliberately not used here:
        // raw RTT must be measured from the post-send `sent_at` at
        // base+110ms, giving 50ms, not the 60ms a pre-send anchor would
        // have produced.
        let sent_at = ClientTimestamp {
            mono: base + Duration::from_millis(110),
            wall: SystemTime::now(),
        };
        let received_at = ClientTimestamp {
            mono: base + Duration::from_millis(160),
            wall: SystemTime::now(),
        };

        let rtt = compute_rtt(&sent_at, &received_at, &TimestampFields::default());

        assert_eq!(rtt.raw, Duration::from_millis(50));
        assert_eq!(rtt.effective, SignedDuration::from_duration(rtt.raw));
    }

    #[test]
    fn userspace_upstream_owd_fallback_uses_post_send_sent_at() {
        let mono = Instant::now();
        let mut machine = machine_with_pending_probe_anchored(
            send_wall_ms(1_000), // pre-send anchor
            ClientTimestamp {
                mono,
                wall: send_wall_ms(1_008), // post-send sent_at
            },
        );

        let events = upstream_reply(&mut machine, 1_020, send_wall_ms(1_040), mono);

        // Old behavior would have used the pre-send 1000ms sample, giving
        // 20ms. The new fallback uses the post-send 1008ms sample.
        assert_eq!(
            reply_one_way(&events).unwrap().client_to_server,
            Some(SignedDuration::from_nanos(12_000_000))
        );
    }

    #[test]
    fn kernel_tx_accepted_even_though_earlier_than_post_send_sent_at() {
        let mono = Instant::now();
        let mut machine = machine_with_pending_probe_anchored(
            send_wall_ms(1_000), // pre-send anchor / tx_not_before_wall
            ClientTimestamp {
                mono,
                wall: send_wall_ms(1_008), // post-send sent_at
            },
        );
        // The kernel TX timestamp is after the pre-send anchor and before
        // the reply, but before the post-send `sent_at` sample. It must
        // remain accepted: rejecting it here would be the regression this
        // test guards against.
        machine.record_kernel_tx_timestamp(0, send_wall_ms(1_005));

        let events = upstream_reply(&mut machine, 1_020, send_wall_ms(1_040), mono);

        assert_eq!(
            reply_one_way(&events).unwrap().client_to_server,
            Some(SignedDuration::from_nanos(15_000_000))
        );
    }

    #[test]
    fn kernel_tx_below_pre_send_anchor_falls_back_to_post_send_not_pre_send() {
        let mono = Instant::now();
        let mut machine = machine_with_pending_probe_anchored(
            send_wall_ms(1_000), // pre-send anchor / tx_not_before_wall
            ClientTimestamp {
                mono,
                wall: send_wall_ms(1_008), // post-send sent_at
            },
        );
        // Earlier than even the pre-send anchor: implausible, rejected.
        machine.record_kernel_tx_timestamp(0, send_wall_ms(999));

        let events = upstream_reply(&mut machine, 1_020, send_wall_ms(1_040), mono);

        // Fallback is the post-send 1008ms sample (12ms), not the pre-send
        // 1000ms anchor (which would have given 20ms).
        assert_eq!(
            reply_one_way(&events).unwrap().client_to_server,
            Some(SignedDuration::from_nanos(12_000_000))
        );
    }

    /// Model-based tests for `SessionMachine`'s open-session operation
    /// contract: probe sends, reply classification, timeouts, and close.
    ///
    /// ## Reference-model boundary
    ///
    /// The open handshake itself (accept/reject, run-mode, version and
    /// token checks) is deliberately out of scope here: it already has
    /// focused deterministic coverage above in `mod tests`, and every
    /// generated case here starts from an already-`Open` session built the
    /// same way `open_machine` above builds one, by constructing
    /// `MachineState::Open` directly. What *is* generated and checked
    /// against an independent reference model is the combinatorial space
    /// that only exists once a session is open: interleaved probe sends,
    /// valid/duplicate/late/unknown/wrong-token replies, peer and local
    /// close, bounded and unbounded timeout polling, and `u32` wire-sequence
    /// wraparound.
    ///
    /// The reference [`Model`] tracks only state the public contract
    /// documents: open/closed(local)/closed(peer) state, the next wire
    /// sequence and total sent-packet counters (mirroring
    /// `SessionMachine::packets_sent`), which sequences are pending, timed
    /// out, or completed (this drives the exact
    /// `EchoReply`/`LateReply`/`DuplicateReply`/`Warning` classification
    /// documented on `ClientEvent`), each pending probe's timeout deadline
    /// (documented on `ClientEvent::EchoLoss::timeout_at`), and the highest
    /// accepted in-window sequence used for late-reply classification. It
    /// deliberately does *not* model the bounded FIFO eviction that
    /// `PendingMap`/`TimedOutMap`/`CompletedSet` apply once
    /// `max_pending_probes` is exceeded: that is a private capacity policy
    /// with its own focused unit tests in `probe.rs`, not part of this
    /// contract. Every generated case here uses a `max_pending_probes` far
    /// larger than the generated operation count, so capacity/eviction
    /// behavior never confounds the classification assertions made here.
    ///
    /// `u32` sequence wraparound is exercised by seeding a session's
    /// starting wire sequence near `u32::MAX` through direct construction,
    /// the same way `open_machine` above builds an open session without
    /// running the handshake. The repository's `AGENTS.md` testing policy
    /// names "sequence wrap" as a sanctioned reason to construct state
    /// directly rather than drive billions of real sends.
    mod model_properties {
        use std::collections::{HashSet, VecDeque};

        use proptest::prelude::*;
        use proptest::test_runner::TestCaseError;

        use super::*;
        use irtt_proto::{encode_echo_reply, ReceivedStats, StampAt};

        const TOKEN: u64 = 0x1122_3344_5566_7788;
        const WRONG_TOKEN: u64 = 0x8877_6655_4433_2211;
        const PROBE_TIMEOUT: Duration = Duration::from_millis(30);
        /// Far larger than the generated operation count (`1..=OP_LIMIT`),
        /// so pending/timed-out/completed bounded eviction never triggers;
        /// see the module doc comment.
        const MAX_PENDING_PROBES: usize = 4096;
        const OP_LIMIT: usize = 50;
        const CASES: u32 = 256;

        fn model_config() -> ClientConfig {
            ClientConfig {
                received_stats: ReceivedStats::None,
                stamp_at: StampAt::None,
                clock: Clock::Wall,
                probe_timeout: PROBE_TIMEOUT,
                max_pending_probes: MAX_PENDING_PROBES,
                run_mode: RunMode::Normal,
                ..ClientConfig::default()
            }
        }

        /// Builds an already-`Open` session directly, the same way `mod
        /// tests`'s `open_machine` does, starting its wire sequence at
        /// `start_seq` instead of always zero. See the module doc comment
        /// for why this bypasses the open handshake and for the sequence
        /// seeding rationale.
        fn open_machine_for_model(start_seq: u32) -> (SessionMachine, Params) {
            let config = model_config();
            let remote = "127.0.0.1:2112".parse().unwrap();
            let mut machine = SessionMachine::new(config, remote).unwrap();
            let params = machine.requested.clone();
            let negotiated = NegotiatedParams {
                params: params.clone(),
                restrictions: Vec::new(),
            };
            machine.state = MachineState::Open(Box::new(ActiveSession {
                token: TOKEN,
                negotiated,
                local_close_packet: encode_request(RequestToEncode::Close { token: TOKEN }, None)
                    .unwrap()
                    .into_boxed_slice(),
                next_wire_seq: start_seq,
                highest_received_seq: None,
                packets_sent: 0,
                pending: PendingMap::new(MAX_PENDING_PROBES),
                timed_out: TimedOutMap::new(MAX_PENDING_PROBES),
                completed: CompletedSet::new(MAX_PENDING_PROBES),
            }));
            (machine, params)
        }

        fn build_echo_reply(params: &Params, token: u64, seq: u32, close: bool) -> Vec<u8> {
            let mut flags_value = flags::FLAG_REPLY;
            if close {
                flags_value |= flags::FLAG_CLOSE;
            }
            let reply = EchoReply {
                flags: flags_value,
                token,
                sequence: seq,
                recv_count: None,
                recv_window: None,
                timestamps: TimestampFields::default(),
                payload: Vec::new(),
            };
            encode_echo_reply(&reply, params, None).unwrap()
        }

        /// Independently-authored mirror of the sequence-space half-window
        /// "after" comparison (RFC 1982 style), used only to predict
        /// expected late/in-window classification. Deliberately not a call
        /// into `super::sequence_is_after`/`sequence_is_before`, so a bug in
        /// the production comparator is not automatically invisible to this
        /// model.
        fn seq_after(candidate: u32, current: u32) -> bool {
            candidate != current && candidate.wrapping_sub(current) < (1 << 31)
        }

        fn seq_before(candidate: u32, current: u32) -> bool {
            seq_after(current, candidate)
        }

        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        enum ModelState {
            Open,
            ClosedLocal,
            ClosedPeer,
        }

        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        enum ExpectedEvent {
            EchoReply(u32),
            LateReply(u32),
            DuplicateReply(u32),
            WarnWrongToken,
            WarnUntracked,
            SessionClosed,
        }

        /// The independent reference model. See the module doc comment for
        /// its deliberate boundary against `SessionMachine`'s private state.
        struct Model {
            state: ModelState,
            next_wire_seq: u32,
            packets_sent: u64,
            pending: VecDeque<(u32, Instant)>,
            timed_out: HashSet<u32>,
            completed: HashSet<u32>,
            highest_received_seq: Option<u32>,
        }

        impl Model {
            fn new(start_seq: u32) -> Self {
                Self {
                    state: ModelState::Open,
                    next_wire_seq: start_seq,
                    packets_sent: 0,
                    pending: VecDeque::new(),
                    timed_out: HashSet::new(),
                    completed: HashSet::new(),
                    highest_received_seq: None,
                }
            }

            fn pending_remove(&mut self, seq: u32) -> bool {
                if let Some(pos) = self.pending.iter().position(|&(s, _)| s == seq) {
                    self.pending.remove(pos);
                    true
                } else {
                    false
                }
            }

            fn sorted_pending(&self) -> Vec<u32> {
                let mut v: Vec<u32> = self.pending.iter().map(|&(s, _)| s).collect();
                v.sort_unstable();
                v
            }

            fn sorted_timed_out(&self) -> Vec<u32> {
                let mut v: Vec<u32> = self.timed_out.iter().copied().collect();
                v.sort_unstable();
                v
            }

            fn sorted_completed(&self) -> Vec<u32> {
                let mut v: Vec<u32> = self.completed.iter().copied().collect();
                v.sort_unstable();
                v
            }

            /// Resolves a generated [`ReplyTarget`] to a concrete sequence
            /// number against the model's *current* membership. When the
            /// requested bucket is empty this falls back to the raw index as
            /// a literal (likely-unknown) sequence, so every `ReplyTarget`
            /// variant remains a valid, meaningful generator input
            /// regardless of what has happened so far.
            fn resolve(&self, target: ReplyTarget) -> u32 {
                fn pick(v: &[u32], i: u32) -> u32 {
                    if v.is_empty() {
                        i
                    } else {
                        v[(i as usize) % v.len()]
                    }
                }
                match target {
                    ReplyTarget::Pending(i) => pick(&self.sorted_pending(), i),
                    ReplyTarget::TimedOut(i) => pick(&self.sorted_timed_out(), i),
                    ReplyTarget::Completed(i) => pick(&self.sorted_completed(), i),
                    ReplyTarget::Unknown(seq) => seq,
                }
            }

            fn update_highest(&mut self, seq: u32) {
                self.highest_received_seq = Some(match self.highest_received_seq {
                    None => seq,
                    Some(h) if seq_after(seq, h) => seq,
                    Some(h) => h,
                });
            }

            fn apply_send(&mut self, seq: u32, timeout_at: Instant) {
                assert_eq!(
                    seq, self.next_wire_seq,
                    "model/machine wire sequence desync"
                );
                self.pending.push_back((seq, timeout_at));
                self.next_wire_seq = self.next_wire_seq.wrapping_add(1);
                self.packets_sent += 1;
            }

            fn apply_poll(&mut self, now: Instant, limit: usize) -> (Vec<u32>, bool) {
                let mut expired = Vec::new();
                while expired.len() < limit {
                    let Some(&(seq, timeout_at)) = self.pending.front() else {
                        break;
                    };
                    if timeout_at > now {
                        break;
                    }
                    self.pending.pop_front();
                    self.timed_out.insert(seq);
                    expired.push(seq);
                }
                let more_due = self
                    .pending
                    .front()
                    .is_some_and(|&(_, timeout_at)| timeout_at <= now);
                (expired, more_due)
            }

            /// Mirrors `SessionMachine::process_echo_reply`'s classification
            /// exactly (see that function's branches), mutating this model
            /// the same way. Returns the event kinds a correct
            /// implementation must produce, in order.
            fn reply(&mut self, seq: u32, wrong_token: bool, close: bool) -> Vec<ExpectedEvent> {
                if wrong_token {
                    // A wrong-token reply is rejected before the close flag
                    // is ever inspected, so `close` has no effect here.
                    return vec![ExpectedEvent::WarnWrongToken];
                }
                let mut events = Vec::new();
                if self.pending_remove(seq) {
                    let is_late = self
                        .highest_received_seq
                        .is_some_and(|h| seq_before(seq, h));
                    self.update_highest(seq);
                    self.completed.insert(seq);
                    events.push(if is_late {
                        ExpectedEvent::LateReply(seq)
                    } else {
                        ExpectedEvent::EchoReply(seq)
                    });
                } else if self.completed.contains(&seq) {
                    self.update_highest(seq);
                    events.push(ExpectedEvent::DuplicateReply(seq));
                } else if self.timed_out.remove(&seq) {
                    self.update_highest(seq);
                    self.completed.insert(seq);
                    events.push(ExpectedEvent::LateReply(seq));
                } else if self
                    .highest_received_seq
                    .is_some_and(|h| seq_before(seq, h))
                {
                    events.push(ExpectedEvent::LateReply(seq));
                } else {
                    events.push(ExpectedEvent::WarnUntracked);
                }
                if close {
                    self.state = ModelState::ClosedPeer;
                    events.push(ExpectedEvent::SessionClosed);
                }
                events
            }
        }

        fn classify_real(events: &[ClientEvent]) -> Vec<ExpectedEvent> {
            events
                .iter()
                .map(|event| match event {
                    ClientEvent::EchoReply { seq, .. } => ExpectedEvent::EchoReply(*seq),
                    ClientEvent::LateReply { seq, .. } => ExpectedEvent::LateReply(*seq),
                    ClientEvent::DuplicateReply { seq, .. } => ExpectedEvent::DuplicateReply(*seq),
                    ClientEvent::Warning {
                        kind: WarningKind::WrongToken,
                        ..
                    } => ExpectedEvent::WarnWrongToken,
                    ClientEvent::Warning {
                        kind: WarningKind::UntrackedReply,
                        ..
                    } => ExpectedEvent::WarnUntracked,
                    ClientEvent::SessionClosed { .. } => ExpectedEvent::SessionClosed,
                    other => panic!("unexpected event from an open session: {other:?}"),
                })
                .collect()
        }

        fn is_already_closed<T: std::fmt::Debug>(result: &Result<T, ClientError>) -> bool {
            matches!(result, Err(ClientError::AlreadyClosed))
        }

        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        struct Observable {
            is_open: bool,
            is_terminal: bool,
            is_peer_closed: bool,
            packets_sent: u64,
            pending_is_empty: bool,
        }

        fn observe(machine: &SessionMachine) -> Observable {
            Observable {
                is_open: machine.is_open(),
                is_terminal: machine.is_terminal(),
                is_peer_closed: machine.is_peer_closed(),
                packets_sent: machine.packets_sent(),
                pending_is_empty: machine.pending_is_empty(),
            }
        }

        fn check_generic_invariants(
            machine: &SessionMachine,
            model: &Model,
        ) -> Result<(), TestCaseError> {
            prop_assert_eq!(machine.is_open(), model.state == ModelState::Open);
            prop_assert_eq!(
                machine.is_terminal(),
                matches!(
                    model.state,
                    ModelState::ClosedLocal | ModelState::ClosedPeer
                )
            );
            prop_assert_eq!(
                machine.is_peer_closed(),
                model.state == ModelState::ClosedPeer
            );
            prop_assert_eq!(machine.packets_sent(), model.packets_sent);
            if model.state == ModelState::Open {
                prop_assert_eq!(machine.pending_is_empty(), model.pending.is_empty());
            } else {
                prop_assert!(machine.pending_is_empty());
            }
            Ok(())
        }

        /// Drives the full real send transaction: prepare, preflight,
        /// finalize, commit. Mirrors how `Client`/`AsyncClient` drive
        /// `SessionMachine` around a real socket send, using `now` for both
        /// the pre-send anchor and the post-send `sent_at` sample since no
        /// real send elapses time here.
        fn do_send(
            machine: &mut SessionMachine,
            now: ClientTimestamp,
        ) -> Result<ProbeSent, ClientError> {
            let prepared = machine
                .prepare_probe()?
                .expect("prepare_probe always returns Some when Ok");
            let preflight = machine.preflight_probe_commit(&prepared)?;
            let commit = machine.finalize_probe_commit(preflight, now)?;
            let bytes = prepared.bytes.len();
            Ok(machine.commit_probe_sent(commit, now, bytes))
        }

        #[derive(Debug, Clone, Copy)]
        enum ReplyTarget {
            Pending(u32),
            TimedOut(u32),
            Completed(u32),
            Unknown(u32),
        }

        #[derive(Debug, Clone, Copy)]
        enum Op {
            Send,
            StaleProbe,
            PrepareAbandon,
            Reply {
                target: ReplyTarget,
                wrong_token: bool,
                close: bool,
            },
            PollTimeouts,
            PollTimeoutsBounded(u8),
            AdvanceClock(u16),
            LocalClose,
            ReopenAttempt,
        }

        fn op_strategy() -> impl Strategy<Value = Op> {
            let reply_target = prop_oneof![
                any::<u32>().prop_map(ReplyTarget::Pending),
                any::<u32>().prop_map(ReplyTarget::TimedOut),
                any::<u32>().prop_map(ReplyTarget::Completed),
                any::<u32>().prop_map(ReplyTarget::Unknown),
            ];
            prop_oneof![
                4 => Just(Op::Send),
                1 => Just(Op::StaleProbe),
                1 => Just(Op::PrepareAbandon),
                5 => (reply_target, any::<bool>(), any::<bool>()).prop_map(
                    |(target, wrong_token, close)| Op::Reply {
                        target,
                        wrong_token,
                        close,
                    }
                ),
                2 => Just(Op::PollTimeouts),
                1 => (0u8..=6).prop_map(Op::PollTimeoutsBounded),
                4 => (0u16..=120).prop_map(Op::AdvanceClock),
                1 => Just(Op::LocalClose),
                1 => Just(Op::ReopenAttempt),
            ]
        }

        /// Biased toward `0` (the common case) and toward the last few
        /// values before `u32::MAX` (so a handful of sends from here cross
        /// the wraparound boundary), with a uniformly random third case for
        /// general coverage.
        fn start_seq_strategy() -> impl Strategy<Value = u32> {
            prop_oneof![
                2 => Just(0u32),
                3 => (u32::MAX - 4)..=u32::MAX,
                2 => any::<u32>(),
            ]
        }

        fn apply_op(
            machine: &mut SessionMachine,
            model: &mut Model,
            params: &Params,
            now: &mut ClientTimestamp,
            op: Op,
        ) -> Result<(), TestCaseError> {
            match op {
                Op::Send => {
                    let result = do_send(machine, *now);
                    match model.state {
                        ModelState::Open => {
                            let sent = result.expect(
                                "open session send should succeed within chosen capacity bounds",
                            );
                            prop_assert_eq!(sent.seq, model.next_wire_seq);
                            let timeout_at = now
                                .mono
                                .checked_add(PROBE_TIMEOUT)
                                .expect("no overflow at test timescales");
                            model.apply_send(sent.seq, timeout_at);
                        }
                        _ => prop_assert!(
                            is_already_closed(&result),
                            "expected AlreadyClosed, got {:?}",
                            result
                        ),
                    }
                }
                Op::StaleProbe => {
                    if model.state != ModelState::Open {
                        let result = machine.prepare_probe();
                        prop_assert!(
                            is_already_closed(&result),
                            "expected AlreadyClosed, got {:?}",
                            result
                        );
                    } else {
                        let prepared_a = machine
                            .prepare_probe()
                            .unwrap()
                            .expect("open session prepares probes");
                        prop_assert_eq!(prepared_a.seq, model.next_wire_seq);

                        let sent = do_send(machine, *now).expect(
                            "intervening send should succeed within chosen capacity bounds",
                        );
                        prop_assert_eq!(sent.seq, prepared_a.seq);
                        let timeout_at = now
                            .mono
                            .checked_add(PROBE_TIMEOUT)
                            .expect("no overflow at test timescales");
                        model.apply_send(sent.seq, timeout_at);

                        match machine.preflight_probe_commit(&prepared_a) {
                            Err(ClientError::StalePreparedProbe {
                                prepared_seq,
                                next_wire_seq,
                            }) => {
                                prop_assert_eq!(prepared_seq, prepared_a.seq);
                                prop_assert_eq!(next_wire_seq, model.next_wire_seq);
                            }
                            other => {
                                prop_assert!(false, "expected StalePreparedProbe, got {:?}", other)
                            }
                        }
                    }
                }
                Op::PrepareAbandon => {
                    let before = observe(machine);
                    let result = machine.prepare_probe();
                    match model.state {
                        ModelState::Open => {
                            let prepared = result.unwrap().expect("open session prepares probes");
                            prop_assert_eq!(prepared.seq, model.next_wire_seq);
                            prop_assert!(!prepared.bytes.is_empty());
                        }
                        _ => prop_assert!(
                            is_already_closed(&result),
                            "expected AlreadyClosed, got {:?}",
                            result
                        ),
                    }
                    prop_assert_eq!(observe(machine), before);
                }
                Op::Reply {
                    target,
                    wrong_token,
                    close,
                } => {
                    let seq = model.resolve(target);
                    let token = if wrong_token { WRONG_TOKEN } else { TOKEN };
                    let packet = build_echo_reply(params, token, seq, close);
                    let result =
                        machine.process_received_echo_packet(&packet, *now, ReceiveMeta::default());
                    match model.state {
                        ModelState::Open => {
                            let events = result.expect("open session processes echo packets");
                            let expected = model.reply(seq, wrong_token, close);
                            prop_assert_eq!(classify_real(&events), expected);
                        }
                        _ => prop_assert!(
                            is_already_closed(&result),
                            "expected AlreadyClosed, got {:?}",
                            result
                        ),
                    }
                }
                Op::PollTimeouts => {
                    let result = machine.poll_timeouts_at(now.mono);
                    match model.state {
                        ModelState::Open => {
                            let events = result.expect("open session polls succeed");
                            let (expired, _more_due) = model.apply_poll(now.mono, usize::MAX);
                            let real: Vec<u32> = events
                                .iter()
                                .map(|event| match event {
                                    ClientEvent::EchoLoss { seq, .. } => *seq,
                                    other => {
                                        panic!("unexpected event from poll_timeouts_at: {other:?}")
                                    }
                                })
                                .collect();
                            prop_assert_eq!(real, expired);
                        }
                        _ => prop_assert!(
                            is_already_closed(&result),
                            "expected AlreadyClosed, got {:?}",
                            result
                        ),
                    }
                }
                Op::PollTimeoutsBounded(limit) => {
                    let result = machine.poll_timeouts_bounded_at(now.mono, limit as usize);
                    match model.state {
                        ModelState::Open => {
                            let batch = result.expect("open session polls succeed");
                            let (expired, more_due) = model.apply_poll(now.mono, limit as usize);
                            let real: Vec<u32> = batch
                                .events
                                .iter()
                                .map(|event| match event {
                                    ClientEvent::EchoLoss { seq, .. } => *seq,
                                    other => panic!(
                                        "unexpected event from poll_timeouts_bounded_at: {other:?}"
                                    ),
                                })
                                .collect();
                            prop_assert_eq!(real, expired);
                            prop_assert_eq!(batch.more_due, more_due);
                        }
                        _ => prop_assert!(
                            is_already_closed(&result),
                            "expected AlreadyClosed, got {:?}",
                            result
                        ),
                    }
                }
                Op::AdvanceClock(millis) => {
                    let delta = Duration::from_millis(u64::from(millis));
                    now.mono += delta;
                    now.wall += delta;
                }
                Op::LocalClose => match model.state {
                    ModelState::Open => {
                        let PreparedClose { commit, .. } = machine
                            .prepare_close()
                            .expect("open session can prepare close");
                        let event = machine.commit_local_close(commit, *now);
                        let is_session_closed = matches!(event, ClientEvent::SessionClosed { .. });
                        prop_assert!(is_session_closed);
                        model.state = ModelState::ClosedLocal;
                    }
                    _ => {
                        let result = machine.prepare_close();
                        prop_assert!(
                            is_already_closed(&result),
                            "expected AlreadyClosed, got {:?}",
                            result
                        );
                    }
                },
                Op::ReopenAttempt => {
                    let before = observe(machine);
                    let result = machine.prepare_open_request();
                    match model.state {
                        ModelState::Open => {
                            prop_assert!(matches!(result, Err(ClientError::AlreadyOpen)));
                        }
                        ModelState::ClosedLocal | ModelState::ClosedPeer => {
                            prop_assert!(is_already_closed(&result));
                        }
                    }
                    prop_assert_eq!(observe(machine), before);
                }
            }
            check_generic_invariants(machine, model)
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(CASES))]

            /// Drives a generated sequence of operations through both a real,
            /// already-`Open` `SessionMachine` and an independent reference
            /// [`Model`], asserting after every operation that the observable
            /// state (open/terminal/peer-closed/packets-sent/pending-empty)
            /// matches, every error returned matches the model's
            /// expectation, and every event/classification produced for a
            /// probe send, a reply, or a timeout poll matches the model's
            /// prediction. No operation should ever panic.
            #[test]
            fn open_session_matches_reference_model(
                start_seq in start_seq_strategy(),
                ops in proptest::collection::vec(op_strategy(), 1..=OP_LIMIT),
            ) {
                let (mut machine, params) = open_machine_for_model(start_seq);
                let mut model = Model::new(start_seq);
                let mut now = ClientTimestamp {
                    mono: Instant::now(),
                    wall: SystemTime::now(),
                };

                check_generic_invariants(&machine, &model)?;
                for op in ops {
                    apply_op(&mut machine, &mut model, &params, &mut now, op)?;
                }
            }
        }
    }
}
