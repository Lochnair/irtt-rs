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
    kernel_tx_correlation_valid: bool,
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
            _ => None,
        }
    }

    pub(crate) fn cleanup_close_packet(&self) -> Option<&[u8]> {
        match &self.next_state {
            MachineState::Open(session) => Some(&session.local_close_packet),
            _ => None,
        }
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

    pub(crate) fn prepare_probe(&self) -> Result<PreparedProbe, ClientError> {
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
        Ok(PreparedProbe {
            bytes: bytes.into_boxed_slice(),
            seq: session.next_wire_seq,
        })
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

    /// Disable Linux kernel TX timestamp correlation after a probe send fails.
    ///
    /// A failed submission can consume a kernel `SOF_TIMESTAMPING_OPT_ID`
    /// without advancing the wire sequence. Existing associated timestamps
    /// remain valid observations; later error-queue timestamps cannot be
    /// associated safely and are ignored for the rest of this session.
    pub(crate) fn invalidate_kernel_tx_correlation(&mut self) {
        if let MachineState::Open(session) = &mut self.state {
            session.kernel_tx_correlation_valid = false;
        }
    }

    /// Record an observed Linux kernel TX timestamp for `wire_seq`, the
    /// automatic `SOF_TIMESTAMPING_OPT_ID` the kernel assigned when the
    /// datagram was submitted.
    ///
    /// `wire_seq` normally matches that correlation ID after successful
    /// sends. This is best effort rather than an invariant: a kernel ID can
    /// theoretically be consumed by a send that later fails, leaving the
    /// counters desynchronized. An unmatched or implausible ID is discarded;
    /// the probe's userspace `sent_at` remains the fallback.
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
        if !session.kernel_tx_correlation_valid {
            return;
        }
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

    pub(crate) fn negotiated_params(&self) -> Option<&NegotiatedParams> {
        match &self.state {
            MachineState::Open(session) => Some(&session.negotiated),
            _ => None,
        }
    }

    pub(crate) fn pending_is_empty(&self) -> bool {
        match &self.state {
            MachineState::Open(session) => session.pending.is_empty(),
            _ => true,
        }
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

    pub(crate) fn packets_sent(&self) -> u64 {
        match &self.state {
            MachineState::Open(session) => session.packets_sent,
            MachineState::Closed { packets_sent, .. } => *packets_sent,
            MachineState::Connected | MachineState::NoTestCompleted => 0,
        }
    }

    pub(crate) fn next_probe_timeout_deadline(&self) -> Option<Instant> {
        match &self.state {
            MachineState::Open(session) => session.pending.next_timeout_deadline(),
            _ => None,
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
            _ => None,
        }
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
            kernel_tx_correlation_valid: true,
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
            let packets_sent = self.packets_sent();
            self.state = MachineState::Closed {
                source: CloseSource::Peer,
                packets_sent,
            };
            events.push(ClientEvent::SessionClosed {
                remote: self.remote,
                token,
                at: now,
            });
        }
        Ok(events)
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
    sequence_is_after(current, candidate)
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
mod tests {
    use super::*;
    use irtt_proto::{FLAG_OPEN, FLAG_REPLY};

    #[test]
    fn timing_arithmetic_preserves_negative_samples() {
        let mono = Instant::now();
        let sent = ClientTimestamp {
            mono,
            wall: UNIX_EPOCH + Duration::from_nanos(100),
        };
        let received = ClientTimestamp {
            mono: mono + Duration::from_nanos(10),
            wall: UNIX_EPOCH + Duration::from_nanos(110),
        };
        // Processing longer than raw RTT must remain negative with either clock.
        for timestamps in [
            TimestampFields {
                recv_mono: Some(100),
                send_mono: Some(120),
                ..TimestampFields::default()
            },
            TimestampFields {
                recv_wall: Some(100),
                send_wall: Some(120),
                ..TimestampFields::default()
            },
        ] {
            let rtt = compute_rtt(&sent, &received, &timestamps);
            assert_eq!(rtt.raw, Duration::from_nanos(10));
            assert_eq!(rtt.adjusted.unwrap().as_nanos(), -10);
            assert_eq!(rtt.effective.as_nanos(), -10);
        }
        // A server clock behind or ahead can make either direction negative.
        for (recv_wall, send_wall, upstream, downstream) in [(90, 95, -10, 15), (115, 120, 15, -10)]
        {
            let sample = compute_one_way(
                &sent,
                sent.wall,
                &received,
                None,
                &ReceiveMeta::default(),
                &TimestampFields {
                    recv_wall: Some(recv_wall),
                    send_wall: Some(send_wall),
                    ..TimestampFields::default()
                },
            )
            .unwrap();
            assert_eq!(sample.client_to_server.unwrap().as_nanos(), upstream);
            assert_eq!(sample.server_to_client.unwrap().as_nanos(), downstream);
        }
    }

    fn opened() -> SessionMachine {
        let mut machine = SessionMachine::new(
            ClientConfig {
                max_pending_probes: 2,
                clock: Clock::Wall,
                stamp_at: irtt_proto::StampAt::Receive,
                ..ClientConfig::default()
            },
            "127.0.0.1:2112".parse().unwrap(),
        )
        .unwrap();
        let acceptance = machine
            .prepare_open_acceptance(
                OpenReply {
                    flags: FLAG_OPEN | FLAG_REPLY,
                    token: 1,
                    params: machine.requested.clone(),
                },
                ClientTimestamp::now(),
            )
            .unwrap();
        machine.commit_open(acceptance);
        machine
    }

    fn send(machine: &mut SessionMachine) -> ProbeSent {
        let probe = machine.prepare_probe().unwrap();
        let preflight = machine.preflight_probe_commit(&probe).unwrap();
        let commit = machine
            .finalize_probe_commit(preflight, ClientTimestamp::now())
            .unwrap();
        machine.commit_probe_sent(commit, ClientTimestamp::now(), probe.bytes.len())
    }

    fn reply(machine: &mut SessionMachine, seq: u32) -> ClientEvent {
        let events = machine
            .process_echo_reply(
                EchoReply {
                    flags: FLAG_REPLY,
                    token: 1,
                    sequence: seq,
                    recv_count: None,
                    recv_window: None,
                    timestamps: TimestampFields {
                        recv_wall: Some(1),
                        ..TimestampFields::default()
                    },
                    payload: vec![],
                },
                32,
                ClientTimestamp::now(),
                ReceiveMeta::default(),
            )
            .unwrap();
        events.into_iter().next().unwrap()
    }

    // The latest retained deadline is internal to managed draining. Exercise
    // ordinary session transitions to check its classification/retention policy.
    #[cfg(feature = "tokio")]
    #[test]
    fn retained_deadline_tracks_timeouts_late_replies_eviction_and_close() {
        let mut machine = opened();
        assert_eq!(send(&mut machine).seq, 0);
        let first = machine.latest_probe_timeout_deadline().unwrap();
        assert_eq!(send(&mut machine).seq, 1);
        let second = machine.latest_probe_timeout_deadline().unwrap();
        let batch = machine.poll_timeouts_bounded_at(second, 1).unwrap();
        assert!(
            matches!(batch.events[0], ClientEvent::EchoLoss { seq: 0, timeout_at, .. } if timeout_at == first)
        );
        assert!(batch.more_due);
        assert_eq!(machine.latest_probe_timeout_deadline(), Some(second));
        let batch = machine.poll_timeouts_bounded_at(second, 1).unwrap();
        assert!(
            matches!(batch.events[0], ClientEvent::EchoLoss { seq: 1, timeout_at, .. } if timeout_at == second)
        );
        assert!(!batch.more_due);
        assert!(machine.pending_is_empty());
        assert_eq!(machine.next_probe_timeout_deadline(), None);
        assert_eq!(machine.latest_probe_timeout_deadline(), Some(second));
        assert!(matches!(
            reply(&mut machine, 1),
            ClientEvent::LateReply {
                sent_at: Some(_),
                rtt: Some(_),
                ..
            }
        ));
        assert_eq!(machine.latest_probe_timeout_deadline(), Some(first));
        assert!(matches!(
            reply(&mut machine, 1),
            ClientEvent::DuplicateReply { .. }
        ));

        for seq in 2..4 {
            assert_eq!(send(&mut machine).seq, seq);
            let deadline = machine.latest_probe_timeout_deadline().unwrap();
            machine.poll_timeouts_at(deadline).unwrap();
            assert_eq!(machine.latest_probe_timeout_deadline(), Some(deadline));
        }
        // Capacity eviction removes seq 0's measurement, not late classification.
        assert!(matches!(
            reply(&mut machine, 0),
            ClientEvent::LateReply {
                sent_at: None,
                rtt: None,
                ..
            }
        ));
        assert!(matches!(
            reply(&mut machine, 3),
            ClientEvent::LateReply {
                sent_at: Some(_),
                rtt: Some(_),
                ..
            }
        ));
        assert!(machine.latest_probe_timeout_deadline().is_some());
        assert!(matches!(
            reply(&mut machine, 2),
            ClientEvent::LateReply {
                sent_at: Some(_),
                rtt: Some(_),
                ..
            }
        ));
        assert_eq!(machine.latest_probe_timeout_deadline(), None);

        assert_eq!(send(&mut machine).seq, 4);
        let deadline = machine.next_probe_timeout_deadline().unwrap();
        machine.poll_timeouts_at(deadline).unwrap();
        let commit = machine.prepare_close().unwrap().commit;
        machine.commit_local_close(commit, ClientTimestamp::now());
        assert_eq!(machine.latest_probe_timeout_deadline(), None);
    }

    // Advancing a real client through 2^32 submissions is unreasonable. Only
    // move the send/receive sequence epoch; construct completion/timeout history through
    // ordinary machine transitions and assert emitted behavior on reuse.
    #[test]
    fn sequence_wrap_reuses_completed_and_timed_out_ids_as_new_probes() {
        for timed_out in [false, true] {
            let mut machine = opened();
            assert_eq!(send(&mut machine).seq, 0);
            if timed_out {
                let deadline = machine.next_probe_timeout_deadline().unwrap();
                assert!(matches!(
                    machine.poll_timeouts_at(deadline).unwrap()[0],
                    ClientEvent::EchoLoss { seq: 0, .. }
                ));
            } else {
                assert!(matches!(
                    reply(&mut machine, 0),
                    ClientEvent::EchoReply { seq: 0, .. }
                ));
            }
            let session = machine.open_session_mut().unwrap();
            session.next_wire_seq = u32::MAX;
            session.highest_received_seq = Some(u32::MAX - 1);
            assert_eq!(send(&mut machine).seq, u32::MAX);
            assert!(matches!(
                reply(&mut machine, u32::MAX),
                ClientEvent::EchoReply { seq: u32::MAX, .. }
            ));
            assert_eq!(send(&mut machine).seq, 0);
            assert!(matches!(
                reply(&mut machine, 0),
                ClientEvent::EchoReply { seq: 0, .. }
            ));
            #[cfg(feature = "tokio")]
            assert_eq!(machine.latest_probe_timeout_deadline(), None);
            assert!(matches!(
                reply(&mut machine, 0),
                ClientEvent::DuplicateReply { seq: 0, .. }
            ));
            assert_eq!(send(&mut machine).seq, 1);
            assert!(matches!(
                reply(&mut machine, 1),
                ClientEvent::EchoReply { seq: 1, .. }
            ));
            assert!(
                matches!(
                    reply(&mut machine, 0),
                    ClientEvent::DuplicateReply { seq: 0, .. }
                ),
                "reused sequence must receive a fresh completion retention lifetime"
            );
            assert_eq!(send(&mut machine).seq, 2);
            assert!(matches!(
                reply(&mut machine, 2),
                ClientEvent::EchoReply { seq: 2, .. }
            ));
            assert!(
                matches!(
                    reply(&mut machine, 0),
                    ClientEvent::LateReply {
                        sent_at: None,
                        rtt: None,
                        ..
                    }
                ),
                "evicted reused sequence must not resurrect a previous epoch's timeout"
            );
        }
    }

    // Kernel completion order relative to timeout cannot be forced reliably
    // through a real socket. Drive both transitions through production methods
    // and observe the selected endpoint on the emitted measurable LateReply.
    #[test]
    fn late_replies_use_tx_timestamps_attached_before_or_after_timeout() {
        for attach_before_timeout in [true, false] {
            let mut machine = opened();
            let anchor = ClientTimestamp {
                mono: Instant::now(),
                wall: UNIX_EPOCH + Duration::from_secs(10),
            };
            let sent_at = ClientTimestamp {
                mono: anchor.mono + Duration::from_millis(1),
                wall: anchor.wall + Duration::from_millis(1),
            };
            let kernel_tx = anchor.wall + Duration::from_micros(500);
            let probe = machine.prepare_probe().unwrap();
            let preflight = machine.preflight_probe_commit(&probe).unwrap();
            let commit = machine.finalize_probe_commit(preflight, anchor).unwrap();
            let sent = machine.commit_probe_sent(commit, sent_at, probe.bytes.len());
            if attach_before_timeout {
                machine.record_kernel_tx_timestamp(sent.seq, kernel_tx);
            }
            let deadline = machine.next_probe_timeout_deadline().unwrap();
            assert!(matches!(
                machine.poll_timeouts_at(deadline).unwrap()[0],
                ClientEvent::EchoLoss { seq: 0, .. }
            ));
            if !attach_before_timeout {
                machine.record_kernel_tx_timestamp(sent.seq, kernel_tx);
            }
            let events = machine
                .process_echo_reply(
                    EchoReply {
                        flags: FLAG_REPLY,
                        token: 1,
                        sequence: sent.seq,
                        recv_count: None,
                        recv_window: None,
                        timestamps: TimestampFields {
                            recv_wall: Some(12_000_000_000),
                            ..TimestampFields::default()
                        },
                        payload: vec![],
                    },
                    probe.bytes.len(),
                    ClientTimestamp {
                        mono: deadline + Duration::from_millis(1),
                        wall: anchor.wall + Duration::from_secs(10),
                    },
                    ReceiveMeta::default(),
                )
                .unwrap();
            let ClientEvent::LateReply {
                sent_at: Some(reported_send),
                server_timing: Some(timing),
                one_way: Some(one_way),
                ..
            } = &events[0]
            else {
                panic!("expected measurable LateReply: {events:?}");
            };
            assert_eq!(*reported_send, sent_at);
            let selected = i128::from(timing.receive_wall_ns.unwrap())
                - one_way.client_to_server.unwrap().as_nanos();
            assert_eq!(
                selected, 10_000_500_000,
                "kernel TX endpoint must survive timeout"
            );
        }
    }

    // A recoverable send failure that consumes an OPT_ID, followed by a
    // plausible wrong-ID completion, cannot be induced reliably with public
    // sockets. Exercise that failure boundary directly, without a socket hook
    // or inspecting the stored correlation flag/metadata.
    #[test]
    fn failed_submission_disables_later_tx_endpoints_for_the_session() {
        let mut machine = opened();
        for attempt in 0..3 {
            if attempt == 1 {
                machine.invalidate_kernel_tx_correlation();
            }
            let prepared = machine.prepare_probe().unwrap();
            let preflight = machine.preflight_probe_commit(&prepared).unwrap();
            let pre_send = ClientTimestamp::now();
            let commit = machine.finalize_probe_commit(preflight, pre_send).unwrap();
            std::thread::sleep(Duration::from_millis(1));
            let sent =
                machine.commit_probe_sent(commit, ClientTimestamp::now(), prepared.bytes.len());
            machine.record_kernel_tx_timestamp(sent.seq, pre_send.wall);
            let event = reply(&mut machine, sent.seq);
            let ClientEvent::EchoReply {
                one_way: Some(one_way),
                ..
            } = event
            else {
                panic!("expected measured reply")
            };
            let expected_wall = if attempt == 0 {
                pre_send.wall
            } else {
                sent.sent_at.wall
            };
            let wall_ns =
                i128::try_from(expected_wall.duration_since(UNIX_EPOCH).unwrap().as_nanos())
                    .unwrap();
            assert_eq!(one_way.client_to_server.unwrap().as_nanos(), 1 - wall_ns);
        }
    }
}
