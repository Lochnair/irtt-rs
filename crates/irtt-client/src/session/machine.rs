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

    #[cfg(feature = "tokio")]
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

    fn open_machine(max_pending_probes: usize, probe_timeout: Duration) -> SessionMachine {
        let config = ClientConfig {
            max_pending_probes,
            probe_timeout,
            ..ClientConfig::default()
        };
        let remote = "127.0.0.1:2112".parse().unwrap();
        let mut machine = SessionMachine::new(config, remote).unwrap();
        let prepared = machine
            .prepare_open_acceptance(
                normal_open_reply(&machine, 0x0102_0304_0506_0708),
                timestamp(Instant::now()),
            )
            .unwrap();
        machine.commit_open(prepared);
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

    fn normal_open_reply(machine: &SessionMachine, token: u64) -> OpenReply {
        OpenReply {
            flags: flags::FLAG_OPEN | flags::FLAG_REPLY,
            token,
            params: machine.requested.clone(),
        }
    }

    #[test]
    fn uncommitted_probe_preparation_changes_no_authoritative_state() {
        let mut machine = open_machine(4, Duration::from_secs(1));
        let prepared = machine.prepare_probe().unwrap();
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
    fn repeated_would_block_style_preflight_commits_once() {
        let mut machine = open_machine(4, Duration::from_secs(1));
        let prepared = machine.prepare_probe().unwrap();
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
    fn pending_sequence_collision_is_detected_before_commit() {
        let mut machine = open_machine(2, Duration::from_secs(1));
        let first = machine.prepare_probe().unwrap();
        let sent_at = timestamp(Instant::now());
        let preflight = machine.preflight_probe_commit(&first).unwrap();
        let commit = machine.finalize_probe_commit(preflight, sent_at).unwrap();
        machine.commit_probe_sent(commit, sent_at, first.bytes.len());
        active_mut(&mut machine).next_wire_seq = 0;

        let reused = machine.prepare_probe().unwrap();
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
        let prepared = machine.prepare_probe().unwrap();

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
        let prepared = machine.prepare_probe().unwrap();
        let preflight = machine.preflight_probe_commit(&prepared).unwrap();

        assert!(matches!(
            machine.finalize_probe_commit(preflight, timestamp(Instant::now())),
            Err(ClientError::DurationOverflow)
        ));
        assert_eq!(active(&machine).pending.len(), 0);
    }

    #[test]
    fn wrapping_sequence_from_max_to_zero_remains_valid() {
        let mut machine = open_machine(2, Duration::from_secs(1));
        active_mut(&mut machine).next_wire_seq = u32::MAX;
        let prepared = machine.prepare_probe().unwrap();
        let preflight = machine.preflight_probe_commit(&prepared).unwrap();
        let commit = machine
            .finalize_probe_commit(preflight, timestamp(Instant::now()))
            .unwrap();
        machine.commit_probe_sent(commit, timestamp(Instant::now()), prepared.bytes.len());

        assert_eq!(active(&machine).next_wire_seq, 0);
        assert_eq!(machine.prepare_probe().unwrap().seq, 0);
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

        let prepared = machine.prepare_probe().unwrap();
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
        let prepared = machine.prepare_probe().unwrap();
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
    fn invalidated_kernel_tx_correlation_uses_userspace_send_time() {
        let mono = Instant::now();
        let mut machine = upstream_probe(send_wall_ms(1_000), mono);

        // A failed send can consume a kernel ID without advancing wire_seq.
        // A later timestamp whose ID happens to match a pending wire sequence
        // must not be associated after that gap.
        machine.invalidate_kernel_tx_correlation();
        machine.record_kernel_tx_timestamp(0, send_wall_ms(1_005));

        let events = upstream_reply(&mut machine, 1_020, send_wall_ms(1_040), mono);

        assert_eq!(
            reply_one_way(&events).unwrap().client_to_server,
            Some(SignedDuration::from_nanos(20_000_000))
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
}
