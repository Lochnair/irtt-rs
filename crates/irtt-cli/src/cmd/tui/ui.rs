use std::{
    collections::{BTreeMap, VecDeque},
    io::{self, Stdout},
    time::{Duration, Instant},
};

use crossterm::{
    cursor::Show,
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use irtt_client::{ClientEvent, NegotiatedParams, SignedDuration, TargetEvent};
use irtt_stats::{Snapshot, StatsCollector, TimeStats};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    symbols,
    text::{Line, Span},
    widgets::{Axis, Block, Borders, Chart, Dataset, GraphType, Paragraph, Wrap},
    Frame, Terminal,
};

use crate::{
    cmd::tui::args::TuiArgs,
    shared::client::{expected_probe_count, GroupPacingArg},
};

const HISTORY_LIMIT: usize = 240;
const RECENT_EVENT_LIMIT: usize = 80;
const MIN_CHART_POINTS: usize = 12;
const MIN_WIDTH: u16 = 56;
const MIN_HEIGHT: u16 = 18;

pub(super) struct TuiTerminal {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl TuiTerminal {
    pub(super) fn enter() -> io::Result<Self> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        if let Err(err) = execute!(stdout, EnterAlternateScreen) {
            let _ = disable_raw_mode();
            return Err(err);
        }

        let backend = CrosstermBackend::new(stdout);
        match Terminal::new(backend) {
            Ok(mut terminal) => {
                if let Err(err) = terminal.clear() {
                    let _ = disable_raw_mode();
                    let _ = execute!(terminal.backend_mut(), LeaveAlternateScreen, Show);
                    let _ = terminal.show_cursor();
                    return Err(err);
                }
                Ok(Self { terminal })
            }
            Err(err) => {
                let _ = disable_raw_mode();
                let mut stdout = io::stdout();
                let _ = execute!(stdout, LeaveAlternateScreen, Show);
                Err(err)
            }
        }
    }

    pub(super) fn draw(&mut self, state: &TuiState) -> io::Result<()> {
        self.terminal
            .draw(|frame| draw_dashboard(frame, state))
            .map(|_| ())
    }
}

impl Drop for TuiTerminal {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen, Show);
        let _ = self.terminal.show_cursor();
    }
}

#[derive(Debug)]
pub(super) struct TuiState {
    status: TuiStatus,
    started_at: Instant,
    config: TuiConfig,
    target_index: BTreeMap<String, usize>,
    targets: Vec<TuiTargetState>,
    recent_events: VecDeque<String>,
    last_warning: Option<String>,
    graph_mode: GraphMode,
    full_graph: bool,
    pub(super) paused: bool,
    pub(super) quit_requested: bool,
}

impl TuiState {
    pub(super) fn new(config: TuiConfig) -> Self {
        Self::with_target_labels(config, ["target".to_owned()])
    }

    pub(super) fn with_target_labels(
        config: TuiConfig,
        labels: impl IntoIterator<Item = String>,
    ) -> Self {
        let stats_config = stats_config(config.duration.is_none());
        let targets = labels
            .into_iter()
            .map(|label| TuiTargetState::new(label, stats_config))
            .collect::<Vec<_>>();
        let target_index = targets
            .iter()
            .enumerate()
            .map(|(idx, target)| (target.label.clone(), idx))
            .collect();
        Self {
            status: TuiStatus::Opening,
            started_at: Instant::now(),
            config,
            target_index,
            targets,
            recent_events: VecDeque::with_capacity(RECENT_EVENT_LIMIT),
            last_warning: None,
            graph_mode: GraphMode::Rtt,
            full_graph: false,
            paused: false,
            quit_requested: false,
        }
    }

    pub(super) fn process_events(&mut self, events: &[ClientEvent]) {
        for event in events {
            self.process_event(event);
        }
    }

    pub(super) fn process_event(&mut self, event: &ClientEvent) {
        self.process_event_for_target(0, event);
    }

    pub(super) fn process_target_event(&mut self, event: &TargetEvent) {
        let label = event.target.as_str();
        let idx = if let Some(idx) = self.target_index.get(label).copied() {
            idx
        } else {
            let idx = self.targets.len();
            let mut target = TuiTargetState::new(
                label.to_owned(),
                stats_config(self.config.duration.is_none()),
            );
            target.status = TargetStatus::Unknown;
            self.targets.push(target);
            self.target_index.insert(label.to_owned(), idx);
            idx
        };
        self.process_event_for_target(idx, &event.event);
    }

    fn process_event_for_target(&mut self, target_idx: usize, event: &ClientEvent) {
        let recent;
        let mut global_status = None;
        let mut global_warning = None;
        let label = {
            let Some(target) = self.targets.get_mut(target_idx) else {
                return;
            };
            process_tui_stats(event, &mut target.stats);
            match event {
                ClientEvent::SessionStarted {
                    remote,
                    token,
                    negotiated,
                    ..
                } => {
                    target.remote = Some(remote.to_string());
                    target.session = Some(format!("{token:#x}"));
                    target.negotiated = Some(negotiated.clone());
                    target.status = TargetStatus::Active;
                    global_status = Some(TuiStatus::Running);
                    recent = Some(format!("session started token={token:#x}"));
                }
                ClientEvent::NoTestCompleted {
                    remote, negotiated, ..
                } => {
                    target.remote = Some(remote.to_string());
                    target.negotiated = Some(negotiated.clone());
                    target.status = TargetStatus::NoTest;
                    global_status = Some(TuiStatus::Complete);
                    recent = Some("no-test negotiation completed".to_owned());
                }
                ClientEvent::SessionClosed { token, .. } => {
                    target.session = Some(format!("{token:#x}"));
                    target.status = TargetStatus::Closed;
                    global_status = Some(TuiStatus::Complete);
                    recent = Some(format!("session closed token={token:#x}"));
                }
                ClientEvent::EchoSent { seq, bytes, .. } => {
                    recent = Some(format!("sent seq={} bytes={bytes}", format_seq(*seq)));
                }
                ClientEvent::EchoReply {
                    seq,
                    rtt,
                    one_way,
                    server_timing,
                    ..
                } => {
                    let client_to_server_ns = one_way
                        .and_then(|sample| sample.client_to_server)
                        .map(SignedDuration::as_nanos);
                    let server_to_client_ns = one_way
                        .and_then(|sample| sample.server_to_client)
                        .map(SignedDuration::as_nanos);
                    let server_processing_ns = server_timing
                        .and_then(|timing| timing.processing)
                        .map(duration_ns);
                    target.push_graph_sample(GraphSample {
                        seq: *seq,
                        effective_ns: rtt.effective.as_nanos(),
                        raw_ns: duration_ns(rtt.raw),
                        adjusted_ns: rtt.adjusted.map(SignedDuration::as_nanos),
                        client_to_server_ns,
                        server_to_client_ns,
                        server_processing_ns,
                    });
                    target.last_sample = Some(LastSample {
                        seq: *seq,
                        raw_ns: duration_ns(rtt.raw),
                        adjusted_ns: rtt.adjusted.map(SignedDuration::as_nanos),
                        effective_ns: rtt.effective.as_nanos(),
                        client_to_server_ns,
                        server_to_client_ns,
                        server_processing_ns,
                    });
                    recent = Some(format!(
                        "reply seq={} effective={}",
                        format_seq(*seq),
                        format_ns_i128(Some(rtt.effective.as_nanos()))
                    ));
                }
                ClientEvent::EchoLoss { seq, .. } => {
                    recent = Some(format!("loss seq={}", format_seq(*seq)));
                }
                ClientEvent::DuplicateReply { seq, remote, .. } => {
                    recent = Some(format!("duplicate seq={} from {remote}", format_seq(*seq)));
                }
                ClientEvent::LateReply {
                    seq,
                    highest_seen,
                    rtt,
                    ..
                } => {
                    let timing = rtt
                        .map(|sample| {
                            format!(
                                " effective={}",
                                format_ns_i128(Some(sample.effective.as_nanos()))
                            )
                        })
                        .unwrap_or_default();
                    recent = Some(format!(
                        "late seq={} highest_seen={}{}",
                        format_seq(*seq),
                        format_seq(*highest_seen),
                        timing
                    ));
                }
                ClientEvent::Warning { kind, message, .. } => {
                    let warning = format!("{kind:?}: {message}");
                    target.last_warning = Some(warning.clone());
                    global_warning = Some(warning.clone());
                    recent = Some(format!("warning {warning}"));
                }
            }
            target.label.clone()
        };
        if let Some(status) = global_status {
            self.status = if status == TuiStatus::Complete
                && self.is_multi_target()
                && !self
                    .targets
                    .iter()
                    .all(|target| target.status.is_terminal())
            {
                TuiStatus::Running
            } else {
                status
            };
        }
        if let Some(warning) = global_warning {
            self.last_warning = Some(warning);
        }
        if let Some(recent) = recent {
            self.push_event(format!("{label}: {recent}"));
        }
    }

    pub(super) fn set_status(&mut self, status: TuiStatus) {
        self.status = status;
    }

    pub(super) fn set_error(&mut self, message: String) {
        self.status = TuiStatus::Error;
        self.last_warning = Some(message.clone());
        if let Some(target) = self.targets.first_mut() {
            target.status = TargetStatus::Failed;
            target.last_warning = Some(message.clone());
        }
        self.push_event(format!("error {message}"));
    }

    pub(super) fn clear_visible_history(&mut self) {
        for target in &mut self.targets {
            target.graph_history.clear();
        }
        self.push_event("visible graph history reset".to_owned());
    }

    pub(super) fn toggle_pause(&mut self) {
        self.paused = !self.paused;
    }

    pub(super) fn cycle_graph_mode(&mut self) {
        self.graph_mode = self.graph_mode.next();
    }

    pub(super) fn toggle_full_graph(&mut self) {
        self.full_graph = !self.full_graph;
    }

    fn push_event(&mut self, event: String) {
        push_bounded(&mut self.recent_events, event, RECENT_EVENT_LIMIT);
    }

    fn selected_target(&self) -> Option<&TuiTargetState> {
        self.targets.first()
    }

    fn selected_snapshot(&self) -> Snapshot {
        self.selected_target()
            .map(|target| target.stats.snapshot())
            .unwrap_or_else(|| StatsCollector::new(stats_config(true)).snapshot())
    }

    fn is_multi_target(&self) -> bool {
        self.targets.len() > 1
    }
}

#[derive(Debug)]
pub(super) struct TuiTargetState {
    label: String,
    remote: Option<String>,
    session: Option<String>,
    status: TargetStatus,
    negotiated: Option<NegotiatedParams>,
    graph_history: VecDeque<GraphSample>,
    last_sample: Option<LastSample>,
    last_warning: Option<String>,
    stats: StatsCollector,
}

impl TuiTargetState {
    fn new(label: String, stats_config: irtt_stats::StatsConfig) -> Self {
        Self {
            label,
            remote: None,
            session: None,
            status: TargetStatus::Opening,
            negotiated: None,
            graph_history: VecDeque::with_capacity(HISTORY_LIMIT),
            last_sample: None,
            last_warning: None,
            stats: StatsCollector::new(stats_config),
        }
    }

    fn push_graph_sample(&mut self, sample: GraphSample) {
        push_bounded(&mut self.graph_history, sample, HISTORY_LIMIT);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TargetStatus {
    Opening,
    Active,
    Closed,
    Failed,
    NoTest,
    Unknown,
}

impl TargetStatus {
    fn label(self) -> &'static str {
        match self {
            Self::Opening => "opening",
            Self::Active => "active",
            Self::Closed => "closed",
            Self::Failed => "failed",
            Self::NoTest => "no-test",
            Self::Unknown => "unknown",
        }
    }

    fn is_terminal(self) -> bool {
        matches!(self, Self::Closed | Self::Failed | Self::NoTest)
    }
}

impl GroupPacingArg {
    fn label(self) -> &'static str {
        match self {
            Self::Staggered => "staggered",
            Self::Burst => "burst",
        }
    }
}

fn stats_config(continuous: bool) -> irtt_stats::StatsConfig {
    if continuous {
        irtt_stats::StatsConfig::continuous()
    } else {
        irtt_stats::StatsConfig::finite()
    }
}

fn process_tui_stats(event: &ClientEvent, stats: &mut StatsCollector) {
    if let ClientEvent::LateReply {
        seq,
        highest_seen,
        remote,
        received_at,
        bytes,
        packet_meta,
        ..
    } = event
    {
        // The TUI treats late replies as diagnostics, even when retained send
        // metadata lets the client attach RTT fields to the event.
        let late_counter_event = ClientEvent::LateReply {
            seq: *seq,
            highest_seen: *highest_seen,
            remote: *remote,
            sent_at: None,
            received_at: *received_at,
            rtt: None,
            server_timing: None,
            one_way: None,
            received_stats: None,
            bytes: *bytes,
            packet_meta: *packet_meta,
        };
        stats.process(&late_counter_event);
    } else {
        stats.process(event);
    }
}

impl Default for TuiState {
    fn default() -> Self {
        Self::new(TuiConfig::default())
    }
}

#[derive(Debug, Clone)]
pub(super) struct TuiConfig {
    interval: Duration,
    duration: Option<Duration>,
    timeout: Duration,
    target_probes: Option<u64>,
    pacing: GroupPacingArg,
}

impl TuiConfig {
    pub(super) fn from_args(args: &TuiArgs) -> Self {
        Self {
            interval: args.interval,
            duration: (!args.is_continuous()).then_some(args.duration),
            timeout: args.to_client_config().probe_timeout,
            target_probes: (!args.is_continuous())
                .then(|| expected_probe_count(args.duration, args.interval)),
            pacing: args.pacing,
        }
    }
}

impl Default for TuiConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(1),
            duration: Some(Duration::from_secs(10)),
            timeout: Duration::from_secs(2),
            target_probes: Some(10),
            pacing: GroupPacingArg::Staggered,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TuiStatus {
    Opening,
    Running,
    Draining,
    Interrupted,
    Closing,
    Complete,
    Error,
}

impl TuiStatus {
    fn label(self) -> &'static str {
        match self {
            Self::Opening => "opening",
            Self::Running => "running",
            Self::Draining => "draining",
            Self::Interrupted => "interrupted",
            Self::Closing => "closing",
            Self::Complete => "complete",
            Self::Error => "error",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GraphMode {
    Rtt,
    OneWay,
    Combined,
    Split,
}

impl GraphMode {
    fn next(self) -> Self {
        match self {
            Self::Rtt => Self::OneWay,
            Self::OneWay => Self::Combined,
            Self::Combined => Self::Split,
            Self::Split => Self::Rtt,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Rtt => "RTT",
            Self::OneWay => "one-way",
            Self::Combined => "combined",
            Self::Split => "split",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct GraphSample {
    seq: u32,
    effective_ns: i128,
    raw_ns: i128,
    adjusted_ns: Option<i128>,
    client_to_server_ns: Option<i128>,
    server_to_client_ns: Option<i128>,
    server_processing_ns: Option<i128>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LastSample {
    seq: u32,
    raw_ns: i128,
    adjusted_ns: Option<i128>,
    effective_ns: i128,
    client_to_server_ns: Option<i128>,
    server_to_client_ns: Option<i128>,
    server_processing_ns: Option<i128>,
}

fn push_bounded<T>(items: &mut VecDeque<T>, item: T, limit: usize) {
    if items.len() == limit {
        items.pop_front();
    }
    items.push_back(item);
}

pub(super) fn draw_dashboard(frame: &mut Frame<'_>, state: &TuiState) {
    let area = frame.area();
    if area.width < MIN_WIDTH || area.height < MIN_HEIGHT {
        frame.render_widget(too_small(), area);
        return;
    }

    let snapshot = state.selected_snapshot();
    if state.full_graph {
        draw_full_graph(frame, area, state, &snapshot);
        return;
    }

    if area.width >= 110 && area.height >= 32 {
        draw_large(frame, area, state, &snapshot);
    } else {
        draw_compact(frame, area, state, &snapshot);
    }
}

pub(super) fn should_render(now: Instant, next_render: Instant, paused: bool, force: bool) -> bool {
    force || (!paused && now >= next_render)
}

fn draw_full_graph(frame: &mut Frame<'_>, area: Rect, state: &TuiState, snapshot: &Snapshot) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(10),
            Constraint::Length(3),
        ])
        .split(area);

    frame.render_widget(full_graph_header(state, snapshot), rows[0]);
    render_graph_area(frame, rows[1], state);
    frame.render_widget(status_line(state), rows[2]);
}

fn draw_large(frame: &mut Frame<'_>, area: Rect, state: &TuiState, snapshot: &Snapshot) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(8),
            Constraint::Min(12),
            Constraint::Length(9),
            Constraint::Length(3),
        ])
        .split(area);
    let top = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(rows[0]);
    let middle = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(36), Constraint::Percentage(64)])
        .split(rows[1]);
    let bottom = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(rows[2]);

    frame.render_widget(header(state, HeaderDensity::Large), top[0]);
    frame.render_widget(packet_panel(state, snapshot, top[1].height), top[1]);
    frame.render_widget(timing_panel(state, snapshot), middle[0]);
    render_graph_area(frame, middle[1], state);
    frame.render_widget(recent_events_panel(state, bottom[0].height), bottom[0]);
    frame.render_widget(sample_panel(state, snapshot), bottom[1]);
    frame.render_widget(status_line(state), rows[3]);
}

fn draw_compact(frame: &mut Frame<'_>, area: Rect, state: &TuiState, snapshot: &Snapshot) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(6),
            Constraint::Length(7),
            Constraint::Min(7),
            Constraint::Length(3),
        ])
        .split(area);

    frame.render_widget(header(state, HeaderDensity::Compact), rows[0]);
    frame.render_widget(packet_panel(state, snapshot, rows[1].height), rows[1]);
    render_graph_area(frame, rows[2], state);
    frame.render_widget(status_line(state), rows[3]);
}

fn too_small() -> Paragraph<'static> {
    Paragraph::new(vec![
        Line::from("terminal too small"),
        Line::from("resize, or press q / Ctrl-C to quit gracefully"),
    ])
    .block(Block::default().title("irtt-rs").borders(Borders::ALL))
    .wrap(Wrap { trim: true })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HeaderDensity {
    Large,
    Compact,
}

fn header(state: &TuiState, density: HeaderDensity) -> Paragraph<'_> {
    let selected = state.selected_target();
    let selected_label = selected
        .map(|target| target.label.as_str())
        .unwrap_or("target");
    let remote = selected
        .and_then(|target| target.remote.as_deref())
        .unwrap_or("-");
    let session = selected
        .and_then(|target| target.session.as_deref())
        .unwrap_or("-");
    let elapsed = format_duration(state.started_at.elapsed());
    let duration = format_optional_duration(state.config.duration);
    let mode = if state.config.duration.is_some() {
        "finite"
    } else {
        "continuous"
    };
    let negotiated = selected
        .and_then(|target| target.negotiated.as_ref())
        .map(|params| format!("negotiated: {}", format_negotiated(params)))
        .unwrap_or_else(|| "negotiated: -".to_owned());
    let target_count = state.targets.len();
    let pacing = state.config.pacing.label();

    let mut lines = vec![
        Line::from(vec![
            Span::styled("irtt-rs", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(format!(
                "  status: {}  targets: {target_count}  pacing: {pacing}",
                state.status.label()
            )),
        ]),
        Line::from(if state.is_multi_target() {
            format!("first target: {selected_label}  remote: {remote}")
        } else {
            format!("remote: {remote}")
        }),
    ];
    match density {
        HeaderDensity::Large => {
            lines.extend([
                Line::from(format!("session: {session}")),
                Line::from(format!(
                    "mode: {mode}  elapsed: {elapsed}  duration: {duration}"
                )),
                Line::from(format!(
                    "interval: {}  timeout: {}",
                    format_duration(state.config.interval),
                    format_duration(state.config.timeout)
                )),
                Line::from(negotiated),
            ]);
        }
        HeaderDensity::Compact => {
            lines.extend([
                Line::from(format!("session: {session}")),
                Line::from(format!(
                    "{mode}  elapsed: {elapsed}  interval: {}",
                    format_duration(state.config.interval)
                )),
            ]);
        }
    }

    Paragraph::new(lines)
        .block(Block::default().title("session").borders(Borders::ALL))
        .wrap(Wrap { trim: true })
}

fn full_graph_header(state: &TuiState, snapshot: &Snapshot) -> Paragraph<'static> {
    let selected = state.selected_target();
    let selected_label = selected
        .map(|target| target.label.as_str())
        .unwrap_or("target");
    let remote = selected
        .and_then(|target| target.remote.as_deref())
        .unwrap_or("-");
    let elapsed = format_duration(state.started_at.elapsed());
    let packets = snapshot.packets;
    let last = state
        .selected_target()
        .and_then(|target| target.last_sample)
        .map(|sample| format_ns_i128(Some(sample.effective_ns)))
        .unwrap_or_else(|| "-".to_owned());

    let target_context = if state.is_multi_target() {
        format!("first target {selected_label} remote {remote}")
    } else {
        format!("remote {remote}")
    };

    Paragraph::new(Line::from(format!(
        "irtt-rs  {}  {target_context}  elapsed {elapsed}  sent {}  replies {}  last {last}",
        state.status.label(),
        format_count(packets.packets_sent),
        format_count(packets.unique_replies)
    )))
    .block(
        Block::default()
            .title(if state.is_multi_target() {
                "session - first target"
            } else {
                "session"
            })
            .borders(Borders::ALL),
    )
}

fn packet_panel(state: &TuiState, snapshot: &Snapshot, panel_height: u16) -> Paragraph<'static> {
    if state.is_multi_target() {
        return target_table_panel(state, panel_height);
    }

    let packets = snapshot.packets;
    let loss = snapshot.loss;
    let progress = state
        .config
        .target_probes
        .map(|target| {
            format!(
                "{}/{} ({})",
                packets.packets_sent,
                target,
                format_percent_ratio(packets.packets_sent, target)
            )
        })
        .unwrap_or_else(|| "-".to_owned());
    let elapsed = state.started_at.elapsed();
    let recv_rate = format_rate(packets.unique_replies, elapsed);

    Paragraph::new(vec![
        Line::from(format!(
            "sent {:>8}   received {:>8}   unique {:>8}",
            format_count(packets.packets_sent),
            format_count(packets.packets_received),
            format_count(packets.unique_replies)
        )),
        Line::from(format!(
            "lost {:>8}   duplicates {:>6}   late {:>10}   warnings {:>6}",
            format_count(loss.lost_packets),
            format_count(packets.duplicates),
            format_count(packets.late_packets),
            format_count(snapshot.events.warning_events)
        )),
        Line::from(format!(
            "loss {:>8}   reply rate {:>10}   progress {progress}",
            format_percent(loss.packet_loss_percent),
            recv_rate
        )),
        Line::from(format!(
            "bytes sent {}   received {}",
            format_count(packets.bytes_sent),
            format_count(packets.bytes_received)
        )),
        Line::from(format!(
            "server received {}   window {}",
            format_optional_u64(packets.server_packets_received),
            format_optional_hex(packets.server_received_window)
        )),
    ])
    .block(Block::default().title("packets").borders(Borders::ALL))
    .wrap(Wrap { trim: true })
}

fn target_table_panel(state: &TuiState, panel_height: u16) -> Paragraph<'static> {
    let visible = usize::from(panel_height.saturating_sub(3));
    let mut lines = vec![Line::from(format!(
        "{:<16} {:<8} {:>9} {:>5} {:>4} {:>4} {:>4}",
        "target", "status", "last", "loss", "dup", "late", "warn"
    ))];

    for target in state.targets.iter().take(visible) {
        let snapshot = target.stats.snapshot();
        let last = target
            .last_sample
            .map(|sample| format_ns_i128(Some(sample.effective_ns)))
            .unwrap_or_else(|| "-".to_owned());
        lines.push(Line::from(format!(
            "{:<16} {:<8} {:>9} {:>5} {:>4} {:>4} {:>4}",
            truncate(&target.label, 16),
            target.status.label(),
            last,
            format_count(snapshot.loss.lost_packets),
            format_count(snapshot.packets.duplicates),
            format_count(snapshot.packets.late_packets),
            format_count(snapshot.events.warning_events)
        )));
    }

    Paragraph::new(lines)
        .block(Block::default().title("targets").borders(Borders::ALL))
        .wrap(Wrap { trim: false })
}

fn timing_panel(state: &TuiState, snapshot: &Snapshot) -> Paragraph<'static> {
    let mut lines = vec![Line::from(format!(
        "{:<18} {:>5} {:>9} {:>9} {:>9} {:>9}",
        "metric", "n", "min", "mean", "max", "stddev"
    ))];
    push_time_line(&mut lines, "effective RTT", &snapshot.rtt.primary);
    push_time_line(&mut lines, "raw RTT", &snapshot.rtt.raw);
    push_time_line(&mut lines, "adjusted RTT", &snapshot.rtt.adjusted);
    push_time_line(&mut lines, "IPDV/jitter", &snapshot.ipdv.round_trip);
    push_time_line(&mut lines, "send IPDV", &snapshot.ipdv.send);
    push_time_line(&mut lines, "receive IPDV", &snapshot.ipdv.receive);
    push_time_line(&mut lines, "send delay", &snapshot.one_way_delay.send_delay);
    push_time_line(
        &mut lines,
        "receive delay",
        &snapshot.one_way_delay.receive_delay,
    );
    push_time_line(
        &mut lines,
        "server process",
        &snapshot.server_processing.processing,
    );
    push_time_line(&mut lines, "send call", &snapshot.send_call);
    push_time_line(&mut lines, "timer error", &snapshot.timer_error);

    Paragraph::new(lines)
        .block(
            Block::default()
                .title(if state.is_multi_target() {
                    "timing - first target"
                } else {
                    "timing"
                })
                .borders(Borders::ALL),
        )
        .wrap(Wrap { trim: false })
}

fn render_graph_area(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    if state.is_multi_target() {
        render_multi_target_graph(frame, area, state);
        return;
    }

    match state.graph_mode {
        GraphMode::Split => render_split_graph(frame, area, state),
        mode => {
            let history = state
                .selected_target()
                .map(|target| &target.graph_history)
                .expect("TuiState always has at least one target");
            let visible = visible_history_window(history, area.width);
            let series = graph_series(mode, &visible);
            render_chart(frame, area, mode.title(), &visible, &series);
        }
    }
}

fn render_split_graph(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);
    let history = state
        .selected_target()
        .map(|target| &target.graph_history)
        .expect("TuiState always has at least one target");
    let visible = visible_history_window(history, area.width);
    let rtt = graph_series(GraphMode::Rtt, &visible);
    let one_way = graph_series(GraphMode::OneWay, &visible);

    render_chart(frame, rows[0], GraphMode::Rtt.title(), &visible, &rtt);
    render_chart(
        frame,
        rows[1],
        GraphMode::OneWay.title(),
        &visible,
        &one_way,
    );
}

fn render_multi_target_graph(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let capacity = visible_sample_capacity(area.width);
    let series = state
        .targets
        .iter()
        .enumerate()
        .filter_map(|(idx, target)| target_rtt_series(target, idx, capacity))
        .collect::<Vec<_>>();
    if series.is_empty() {
        frame.render_widget(
            Paragraph::new("waiting for primary replies")
                .block(
                    Block::default()
                        .title("RTT history - per target")
                        .borders(Borders::ALL),
                )
                .wrap(Wrap { trim: true }),
            area,
        );
        return;
    }

    let x_max = series
        .iter()
        .map(|series| series.data.len().saturating_sub(1))
        .max()
        .unwrap_or(1)
        .max(1) as f64;
    let (min_y, max_y) = chart_y_bounds(&series);
    let datasets = chart_datasets(&series);
    let chart = Chart::new(datasets)
        .block(
            Block::default()
                .title("RTT history - per target")
                .borders(Borders::ALL),
        )
        .x_axis(
            Axis::default()
                .bounds([0.0, x_max])
                .labels(vec![Span::raw("old"), Span::raw("new")])
                .style(Style::default().fg(Color::Gray)),
        )
        .y_axis(
            Axis::default()
                .bounds([min_y, max_y])
                .labels(vec![
                    Span::raw(format_ms_label(min_y)),
                    Span::raw(format_ms_label(max_y)),
                ])
                .style(Style::default().fg(Color::Gray)),
        );
    frame.render_widget(chart, area);
}

fn render_chart(
    frame: &mut Frame<'_>,
    area: Rect,
    title: &'static str,
    visible: &[&GraphSample],
    series: &[ChartSeries],
) {
    if series.is_empty() {
        let note = if visible.is_empty() {
            "waiting for primary replies"
        } else {
            "one-way data unavailable for negotiated timestamp mode"
        };
        frame.render_widget(
            Paragraph::new(note)
                .block(Block::default().title(title).borders(Borders::ALL))
                .wrap(Wrap { trim: true }),
            area,
        );
        return;
    }

    let (min_y, max_y) = chart_y_bounds(series);
    let x_max = visible.len().saturating_sub(1).max(1) as f64;
    let datasets = chart_datasets(series);
    let chart = Chart::new(datasets)
        .block(Block::default().title(title).borders(Borders::ALL))
        .x_axis(
            Axis::default()
                .bounds([0.0, x_max])
                .labels(x_axis_labels(visible))
                .style(Style::default().fg(Color::Gray)),
        )
        .y_axis(
            Axis::default()
                .bounds([min_y, max_y])
                .labels(vec![
                    Span::raw(format_ms_label(min_y)),
                    Span::raw(format_ms_label(max_y)),
                ])
                .style(Style::default().fg(Color::Gray)),
        );
    frame.render_widget(chart, area);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GraphValue {
    EffectiveRtt,
    ClientToServer,
    ServerToClient,
}

impl GraphValue {
    fn name(self) -> &'static str {
        match self {
            Self::EffectiveRtt => "eff RTT",
            Self::ClientToServer => "c2s",
            Self::ServerToClient => "s2c",
        }
    }

    fn style(self) -> Style {
        match self {
            Self::EffectiveRtt => target_style(0).add_modifier(Modifier::BOLD),
            Self::ClientToServer => Style::default().fg(Color::Magenta),
            Self::ServerToClient => Style::default().fg(Color::LightBlue),
        }
    }

    fn value_ns(self, sample: &GraphSample) -> Option<i128> {
        match self {
            Self::EffectiveRtt => Some(sample.effective_ns),
            Self::ClientToServer => sample.client_to_server_ns,
            Self::ServerToClient => sample.server_to_client_ns,
        }
    }
}

impl GraphMode {
    fn title(self) -> &'static str {
        match self {
            Self::Rtt => "RTT history — effective",
            Self::OneWay => "one-way delay — c2s / s2c",
            Self::Combined => "RTT + one-way correlation",
            Self::Split => "split history",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct ChartSeries {
    value: Option<GraphValue>,
    name: String,
    style: Style,
    data: Vec<(f64, f64)>,
}

fn visible_sample_capacity(chart_width: u16) -> usize {
    let derived = usize::from(chart_width.saturating_sub(10)).max(MIN_CHART_POINTS);
    derived.min(HISTORY_LIMIT)
}

fn visible_history_window(history: &VecDeque<GraphSample>, chart_width: u16) -> Vec<&GraphSample> {
    let capacity = visible_sample_capacity(chart_width);
    let start = history.len().saturating_sub(capacity);
    history.iter().skip(start).collect()
}

fn graph_series(mode: GraphMode, visible: &[&GraphSample]) -> Vec<ChartSeries> {
    let values: &[GraphValue] = match mode {
        GraphMode::Rtt => &[GraphValue::EffectiveRtt],
        GraphMode::OneWay => &[GraphValue::ClientToServer, GraphValue::ServerToClient],
        GraphMode::Combined => &[
            GraphValue::EffectiveRtt,
            GraphValue::ClientToServer,
            GraphValue::ServerToClient,
        ],
        GraphMode::Split => &[],
    };
    values
        .iter()
        .filter_map(|value| chart_series(*value, visible))
        .collect()
}

fn chart_series(value: GraphValue, visible: &[&GraphSample]) -> Option<ChartSeries> {
    let data: Vec<(f64, f64)> = visible
        .iter()
        .enumerate()
        .filter_map(|(idx, sample)| {
            value
                .value_ns(sample)
                .map(|ns| (idx as f64, ns as f64 / 1_000_000.0))
        })
        .collect();

    (!data.is_empty()).then_some(ChartSeries {
        value: Some(value),
        name: value.name().to_owned(),
        style: value.style(),
        data,
    })
}

fn target_rtt_series(
    target: &TuiTargetState,
    target_idx: usize,
    capacity: usize,
) -> Option<ChartSeries> {
    let start = target.graph_history.len().saturating_sub(capacity);
    let data = target
        .graph_history
        .iter()
        .skip(start)
        .enumerate()
        .map(|(idx, sample)| (idx as f64, sample.effective_ns as f64 / 1_000_000.0))
        .collect::<Vec<_>>();

    (!data.is_empty()).then_some(ChartSeries {
        value: None,
        name: target.label.clone(),
        style: target_style(target_idx),
        data,
    })
}

fn chart_datasets(series: &[ChartSeries]) -> Vec<Dataset<'_>> {
    series
        .iter()
        .map(|series| {
            Dataset::default()
                .name(series.name.as_str())
                .marker(symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .style(series.style)
                .data(&series.data)
        })
        .collect()
}

fn target_style(idx: usize) -> Style {
    const COLORS: [Color; 8] = [
        Color::Cyan,
        Color::Yellow,
        Color::Green,
        Color::Magenta,
        Color::LightBlue,
        Color::LightRed,
        Color::LightGreen,
        Color::White,
    ];
    Style::default().fg(COLORS[idx % COLORS.len()])
}

fn chart_y_bounds(series: &[ChartSeries]) -> (f64, f64) {
    let mut values = series.iter().flat_map(|series| {
        series
            .data
            .iter()
            .map(|(_, value)| *value)
            .filter(|value| value.is_finite())
    });
    let Some(first) = values.next() else {
        return (0.0, 1.0);
    };

    let (mut min_y, mut max_y) = (first, first);
    for value in values {
        min_y = min_y.min(value);
        max_y = max_y.max(value);
    }

    let span = max_y - min_y;
    let pad = if span <= f64::EPSILON {
        (max_y.abs() * 0.05).max(1.0)
    } else {
        (span * 0.1).max(1.0)
    };
    min_y -= pad;
    max_y += pad;
    if min_y >= max_y {
        (min_y - 1.0, max_y + 1.0)
    } else {
        (min_y, max_y)
    }
}

fn x_axis_labels(visible: &[&GraphSample]) -> Vec<Span<'static>> {
    let Some(first) = visible.first() else {
        return Vec::new();
    };
    let last = visible.last().unwrap_or(first);
    vec![
        Span::raw(format!("#{}", first.seq)),
        Span::raw(format!("#{}", last.seq)),
    ]
}

fn format_ms_label(value: f64) -> String {
    if value.abs() < 10.0 {
        format!("{value:.2}ms")
    } else if value.abs() < 100.0 {
        format!("{value:.1}ms")
    } else {
        format!("{value:.0}ms")
    }
}

fn recent_events_panel(state: &TuiState, panel_height: u16) -> Paragraph<'_> {
    let lines: Vec<Line<'_>> = state
        .recent_events
        .iter()
        .rev()
        .take(recent_events_visible_count(panel_height))
        .map(|event| Line::from(event.as_str()))
        .collect();
    Paragraph::new(lines)
        .block(
            Block::default()
                .title("recent events")
                .borders(Borders::ALL),
        )
        .wrap(Wrap { trim: true })
}

fn recent_events_visible_count(panel_height: u16) -> usize {
    usize::from(panel_height.saturating_sub(2))
}

fn sample_panel(state: &TuiState, snapshot: &Snapshot) -> Paragraph<'static> {
    let selected = state.selected_target();
    let last = selected.and_then(|target| target.last_sample);
    let warning = selected
        .and_then(|target| target.last_warning.clone())
        .or_else(|| state.last_warning.clone())
        .unwrap_or_else(|| "-".to_owned());
    Paragraph::new(vec![
        Line::from(format!(
            "last seq: {}",
            last.map(|sample| format_seq(sample.seq))
                .unwrap_or_else(|| "-".to_owned())
        )),
        Line::from(format!(
            "raw / adjusted / effective: {} / {} / {}",
            last.map(|sample| format_ns_i128(Some(sample.raw_ns)))
                .unwrap_or_else(|| "-".to_owned()),
            last.map(|sample| format_ns_i128(sample.adjusted_ns))
                .unwrap_or_else(|| "-".to_owned()),
            last.map(|sample| format_ns_i128(Some(sample.effective_ns)))
                .unwrap_or_else(|| "-".to_owned())
        )),
        Line::from(format!(
            "one-way c2s / s2c: {} / {}",
            last.map(|sample| format_ns_i128(sample.client_to_server_ns))
                .unwrap_or_else(|| "-".to_owned()),
            last.map(|sample| format_ns_i128(sample.server_to_client_ns))
                .unwrap_or_else(|| "-".to_owned())
        )),
        Line::from(format!(
            "server processing: {}",
            last.map(|sample| format_ns_i128(sample.server_processing_ns))
                .unwrap_or_else(|| "-".to_owned())
        )),
        Line::from(format!(
            "events sent={} replies={} losses={} warnings={}",
            format_count(snapshot.events.sent_events),
            format_count(snapshot.events.echo_replies),
            format_count(snapshot.events.loss_events),
            format_count(snapshot.events.warning_events)
        )),
        Line::from(format!("last warning: {warning}")),
    ])
    .block(
        Block::default()
            .title(if state.is_multi_target() {
                "sample - first target"
            } else {
                "sample"
            })
            .borders(Borders::ALL),
    )
    .wrap(Wrap { trim: true })
}

fn status_line(state: &TuiState) -> Paragraph<'_> {
    let paused = if state.paused { " display paused" } else { "" };
    let quitting = if state.quit_requested {
        " quit requested"
    } else {
        ""
    };
    let legend = if state.is_multi_target() {
        Some(Line::from(format!("legend: {}", legend_text(state))))
    } else {
        None
    };
    let mut lines = vec![Line::from(format!(
        "{}{}{} | graph {}{} | q quit | Ctrl-C quit | r reset | p pause | g graph | f full",
        state.status.label(),
        paused,
        quitting,
        state.graph_mode.label(),
        if state.full_graph { " full" } else { "" }
    ))];
    if let Some(legend) = legend {
        lines.push(legend);
    }
    Paragraph::new(lines).block(Block::default().borders(Borders::ALL))
}

fn push_time_line(lines: &mut Vec<Line<'_>>, label: &str, stats: &TimeStats) {
    if stats.count == 0 {
        lines.push(Line::from(format!(
            "{label:<18} {:>5} {:>9} {:>9} {:>9} {:>9}",
            0, "-", "-", "-", "-"
        )));
        return;
    }

    lines.push(Line::from(format!(
        "{label:<18} {:>5} {:>9} {:>9} {:>9} {:>9}",
        format_count(stats.count),
        format_ns_i128(stats.min_ns),
        format_ns_f64(stats.mean_ns),
        format_ns_i128(stats.max_ns),
        format_ns_f64(stats.stddev_ns())
    )));
}

fn format_negotiated(negotiated: &NegotiatedParams) -> String {
    let params = &negotiated.params;
    let duration = if params.duration_ns == 0 {
        "-".to_owned()
    } else {
        format_ns_i128(Some(i128::from(params.duration_ns)))
    };
    let restrictions = if negotiated.restrictions.is_empty() {
        "none".to_owned()
    } else {
        negotiated.restrictions.len().to_string()
    };
    format!(
        "duration={} interval={} length={} clock={:?} timestamps={:?} stats={:?} restrictions={}",
        duration,
        format_ns_i128(Some(i128::from(params.interval_ns))),
        params.length,
        params.clock,
        params.stamp_at,
        params.received_stats,
        restrictions
    )
}

fn format_optional_duration(value: Option<Duration>) -> String {
    value.map(format_duration).unwrap_or_else(|| "-".to_owned())
}

fn format_duration(value: Duration) -> String {
    if value.is_zero() {
        return "0s".to_owned();
    }
    let nanos = value.as_nanos();
    if nanos < 1_000 {
        format!("{nanos}ns")
    } else if nanos < 1_000_000 {
        format!("{:.1}us", nanos as f64 / 1_000.0)
    } else if nanos < 1_000_000_000 {
        format!("{:.1}ms", nanos as f64 / 1_000_000.0)
    } else if nanos < 60_000_000_000 {
        format!("{:.1}s", nanos as f64 / 1_000_000_000.0)
    } else {
        let secs = value.as_secs();
        format!("{}m{:02}s", secs / 60, secs % 60)
    }
}

fn format_ns_i128(value: Option<i128>) -> String {
    value.map(format_ns_value).unwrap_or_else(|| "-".to_owned())
}

fn format_ns_f64(value: f64) -> String {
    format_ns_value(value.round() as i128)
}

fn format_ns_value(value: i128) -> String {
    let sign = if value < 0 { "-" } else { "" };
    let value = value.saturating_abs() as f64;
    if value < 1_000.0 {
        format!("{sign}{value:.0}ns")
    } else if value < 1_000_000.0 {
        format!("{sign}{:.1}us", value / 1_000.0)
    } else if value < 1_000_000_000.0 {
        format!("{sign}{:.1}ms", value / 1_000_000.0)
    } else {
        format!("{sign}{:.3}s", value / 1_000_000_000.0)
    }
}

fn format_percent(value: f64) -> String {
    if value.is_finite() {
        format!("{value:.2}%")
    } else {
        "-".to_owned()
    }
}

fn format_percent_ratio(value: u64, total: u64) -> String {
    if total == 0 {
        "-".to_owned()
    } else {
        format_percent((value as f64 / total as f64 * 100.0).min(100.0))
    }
}

fn format_rate(count: u64, elapsed: Duration) -> String {
    let secs = elapsed.as_secs_f64();
    if secs <= f64::EPSILON {
        "-".to_owned()
    } else {
        format!("{:.2}/s", count as f64 / secs)
    }
}

fn format_count(value: u64) -> String {
    value.to_string()
}

fn truncate(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_owned();
    }
    value
        .chars()
        .take(max_chars.saturating_sub(1))
        .chain(std::iter::once('~'))
        .collect()
}

fn legend_text(state: &TuiState) -> String {
    state
        .targets
        .iter()
        .enumerate()
        .map(|(idx, target)| format!("{} {}", idx + 1, target.label))
        .collect::<Vec<_>>()
        .join("  ")
}

fn format_seq(value: u32) -> String {
    value.to_string()
}

fn format_optional_u64(value: Option<u64>) -> String {
    value.map(format_count).unwrap_or_else(|| "-".to_owned())
}

fn format_optional_hex(value: Option<u64>) -> String {
    value
        .map(|value| format!("0x{value:x}"))
        .unwrap_or_else(|| "-".to_owned())
}

fn duration_ns(value: Duration) -> i128 {
    i128::try_from(value.as_nanos()).unwrap_or(i128::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use irtt_client::{
        ClientTimestamp, OneWayDelaySample, PacketMeta, RttSample, ServerTiming, TargetId,
        WarningKind,
    };
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        time::UNIX_EPOCH,
    };

    fn remote() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 2112)
    }

    fn ts(offset: Duration) -> ClientTimestamp {
        ClientTimestamp {
            wall: UNIX_EPOCH + offset,
            mono: Instant::now() + offset,
        }
    }

    fn rtt(effective_ns: i128) -> RttSample {
        RttSample {
            raw: Duration::from_micros(1_500),
            adjusted: Some(SignedDuration::from_nanos(effective_ns)),
            effective: SignedDuration::from_nanos(effective_ns),
        }
    }

    fn reply(seq: u32, effective_ns: i128) -> ClientEvent {
        ClientEvent::EchoReply {
            seq,
            remote: remote(),
            sent_at: ts(Duration::from_millis(seq as u64)),
            received_at: ts(Duration::from_millis(seq as u64) + Duration::from_micros(1500)),
            rtt: rtt(effective_ns),
            server_timing: Some(ServerTiming {
                receive_wall_ns: None,
                receive_mono_ns: None,
                send_wall_ns: None,
                send_mono_ns: None,
                midpoint_wall_ns: None,
                midpoint_mono_ns: None,
                processing: Some(Duration::from_micros(100)),
            }),
            one_way: Some(OneWayDelaySample {
                client_to_server: Some(SignedDuration::from_nanos(-20_000)),
                server_to_client: Some(SignedDuration::from_nanos(30_000)),
            }),
            received_stats: None,
            bytes: 64,
            packet_meta: PacketMeta::default(),
        }
    }

    fn graph_sample(seq: u32, effective_ns: i128) -> GraphSample {
        GraphSample {
            seq,
            effective_ns,
            raw_ns: effective_ns + 1_000,
            adjusted_ns: Some(effective_ns),
            client_to_server_ns: None,
            server_to_client_ns: None,
            server_processing_ns: None,
        }
    }

    fn graph_sample_with_timing(seq: u32, effective_ns: i128) -> GraphSample {
        GraphSample {
            seq,
            effective_ns,
            raw_ns: effective_ns + 1_000,
            adjusted_ns: Some(effective_ns + 500),
            client_to_server_ns: Some(effective_ns / 3),
            server_to_client_ns: Some(effective_ns / 2),
            server_processing_ns: Some(100_000),
        }
    }

    fn series(data: Vec<(f64, f64)>) -> ChartSeries {
        ChartSeries {
            value: Some(GraphValue::EffectiveRtt),
            name: GraphValue::EffectiveRtt.name().to_owned(),
            style: GraphValue::EffectiveRtt.style(),
            data,
        }
    }

    fn primary_target(state: &TuiState) -> &TuiTargetState {
        &state.targets[0]
    }

    fn target_event(label: &str, event: ClientEvent) -> TargetEvent {
        TargetEvent {
            target: TargetId::from(label),
            event,
        }
    }

    #[test]
    fn formats_signed_durations_and_missing_values() {
        assert_eq!(format_ns_i128(Some(-1_500_000)), "-1.5ms");
        assert_eq!(format_ns_i128(Some(750)), "750ns");
        assert_eq!(format_ns_i128(None), "-");
        assert_eq!(format_optional_duration(None), "-");
        assert_eq!(format_duration(Duration::from_millis(25)), "25.0ms");
        assert_eq!(format_percent_ratio(1, 4), "25.00%");
        assert_eq!(format_optional_hex(Some(0x1f)), "0x1f");
    }

    #[test]
    fn session_started_sets_remote_session_and_running_status() {
        let mut state = TuiState::default();
        state.process_event(&ClientEvent::SessionStarted {
            remote: remote(),
            token: 0xabc,
            negotiated: NegotiatedParams {
                params: irtt_proto::Params::default(),
                restrictions: Vec::new(),
            },
            at: ts(Duration::ZERO),
        });

        let target = primary_target(&state);
        assert_eq!(target.remote.as_deref(), Some("127.0.0.1:2112"));
        assert_eq!(target.session.as_deref(), Some("0xabc"));
        assert_eq!(state.status, TuiStatus::Running);
        assert!(target.negotiated.is_some());
    }

    #[test]
    fn echo_reply_appends_bounded_primary_history() {
        let mut state = TuiState::default();
        for seq in 0..(HISTORY_LIMIT as u32 + 3) {
            state.process_event(&reply(seq, i128::from(seq) * 1_000));
        }

        let target = primary_target(&state);
        assert_eq!(target.graph_history.len(), HISTORY_LIMIT);
        assert_eq!(target.graph_history.front().unwrap().seq, 3);
        assert_eq!(
            target.graph_history.back().unwrap().effective_ns,
            i128::from(HISTORY_LIMIT as u32 + 2) * 1_000
        );
        assert_eq!(
            target.stats.snapshot().packets.unique_replies,
            HISTORY_LIMIT as u64 + 3
        );
    }

    #[test]
    fn duplicate_and_late_replies_do_not_append_primary_history() {
        let mut state = TuiState::default();
        state.process_event(&ClientEvent::DuplicateReply {
            seq: 7,
            remote: remote(),
            received_at: ts(Duration::from_secs(1)),
            bytes: 64,
        });
        state.process_event(&ClientEvent::LateReply {
            seq: 8,
            highest_seen: 9,
            remote: remote(),
            sent_at: Some(ts(Duration::from_secs(1))),
            received_at: ts(Duration::from_secs(2)),
            rtt: Some(rtt(2_000_000)),
            server_timing: None,
            one_way: None,
            received_stats: None,
            bytes: 64,
            packet_meta: PacketMeta::default(),
        });

        let target = primary_target(&state);
        assert!(target.graph_history.is_empty());
        let snapshot = target.stats.snapshot();
        assert_eq!(snapshot.events.duplicate_replies, 1);
        assert_eq!(snapshot.events.late_unique_replies, 0);
        assert_eq!(snapshot.events.untracked_late_replies, 1);
        assert_eq!(snapshot.packets.unique_replies, 0);
        assert_eq!(snapshot.packets.duplicates, 1);
        assert_eq!(snapshot.packets.late_packets, 1);
        assert_eq!(snapshot.rtt.primary.count, 0);
        assert!(state
            .recent_events
            .iter()
            .any(|event| event.contains("duplicate seq=7")));
        assert!(state
            .recent_events
            .iter()
            .any(|event| event.contains("late seq=8")));
    }

    #[test]
    fn target_scoped_echo_reply_updates_only_that_target() {
        let mut state =
            TuiState::with_target_labels(TuiConfig::default(), ["a".to_owned(), "b".to_owned()]);

        state.process_target_event(&target_event("b", reply(11, 2_500_000)));

        assert!(state.targets[0].graph_history.is_empty());
        assert_eq!(state.targets[0].stats.snapshot().packets.unique_replies, 0);
        assert_eq!(state.targets[1].graph_history.len(), 1);
        assert_eq!(
            state.targets[1].graph_history.back().unwrap().effective_ns,
            2_500_000
        );
        assert_eq!(state.targets[1].last_sample.unwrap().seq, 11);
        assert_eq!(state.targets[1].stats.snapshot().packets.unique_replies, 1);
    }

    #[test]
    fn target_scoped_duplicate_and_late_are_diagnostic_only() {
        let mut state =
            TuiState::with_target_labels(TuiConfig::default(), ["a".to_owned(), "b".to_owned()]);

        state.process_target_event(&target_event(
            "a",
            ClientEvent::DuplicateReply {
                seq: 7,
                remote: remote(),
                received_at: ts(Duration::from_secs(1)),
                bytes: 64,
            },
        ));
        state.process_target_event(&target_event(
            "b",
            ClientEvent::LateReply {
                seq: 8,
                highest_seen: 9,
                remote: remote(),
                sent_at: Some(ts(Duration::from_secs(1))),
                received_at: ts(Duration::from_secs(2)),
                rtt: Some(rtt(2_000_000)),
                server_timing: None,
                one_way: None,
                received_stats: None,
                bytes: 64,
                packet_meta: PacketMeta::default(),
            },
        ));

        assert!(state.targets[0].graph_history.is_empty());
        assert!(state.targets[1].graph_history.is_empty());
        assert_eq!(state.targets[0].stats.snapshot().packets.duplicates, 1);
        assert_eq!(state.targets[1].stats.snapshot().packets.late_packets, 1);
        assert_eq!(state.targets[1].stats.snapshot().rtt.primary.count, 0);
    }

    #[test]
    fn target_scoped_loss_warning_and_terminal_status_update_correct_target() {
        let mut state =
            TuiState::with_target_labels(TuiConfig::default(), ["a".to_owned(), "b".to_owned()]);

        state.process_target_event(&target_event(
            "a",
            ClientEvent::EchoLoss {
                seq: 3,
                sent_at: ts(Duration::from_millis(3)),
                timeout_at: Instant::now(),
            },
        ));
        state.process_target_event(&target_event(
            "b",
            ClientEvent::Warning {
                kind: WarningKind::WrongToken,
                message: "wrong token".to_owned(),
                at: ts(Duration::ZERO),
            },
        ));
        state.process_target_event(&target_event(
            "a",
            ClientEvent::NoTestCompleted {
                remote: remote(),
                negotiated: NegotiatedParams {
                    params: irtt_proto::Params::default(),
                    restrictions: Vec::new(),
                },
                at: ts(Duration::ZERO),
            },
        ));

        assert_eq!(state.targets[0].stats.snapshot().events.loss_events, 1);
        assert_eq!(state.targets[0].status, TargetStatus::NoTest);
        assert_eq!(state.targets[1].stats.snapshot().events.warning_events, 1);
        assert_eq!(state.targets[1].status, TargetStatus::Opening);
        assert!(state.targets[1]
            .last_warning
            .as_deref()
            .is_some_and(|warning| warning.contains("WrongToken")));
    }

    #[test]
    fn multi_target_global_status_waits_for_all_targets_terminal() {
        let mut state =
            TuiState::with_target_labels(TuiConfig::default(), ["a".to_owned(), "b".to_owned()]);

        state.process_target_event(&target_event(
            "a",
            ClientEvent::NoTestCompleted {
                remote: remote(),
                negotiated: NegotiatedParams {
                    params: irtt_proto::Params::default(),
                    restrictions: Vec::new(),
                },
                at: ts(Duration::ZERO),
            },
        ));

        assert_eq!(state.targets[0].status, TargetStatus::NoTest);
        assert_eq!(state.targets[1].status, TargetStatus::Opening);
        assert_eq!(state.status, TuiStatus::Running);

        state.process_target_event(&target_event(
            "b",
            ClientEvent::SessionClosed {
                remote: remote(),
                token: 0xabc,
                at: ts(Duration::ZERO),
            },
        ));

        assert_eq!(state.targets[1].status, TargetStatus::Closed);
        assert_eq!(state.status, TuiStatus::Complete);
    }

    #[test]
    fn reset_clears_graph_history_for_all_targets() {
        let mut state =
            TuiState::with_target_labels(TuiConfig::default(), ["a".to_owned(), "b".to_owned()]);

        state.process_target_event(&target_event("a", reply(1, 1_000_000)));
        state.process_target_event(&target_event("b", reply(2, 2_000_000)));
        state.clear_visible_history();

        assert!(state.targets[0].graph_history.is_empty());
        assert!(state.targets[1].graph_history.is_empty());
        assert_eq!(
            state.recent_events.back().map(String::as_str),
            Some("visible graph history reset")
        );
    }

    #[test]
    fn warning_updates_recent_events_and_last_warning() {
        let mut state = TuiState::default();
        state.process_event(&ClientEvent::Warning {
            kind: WarningKind::WrongToken,
            message: "wrong token".to_owned(),
            at: ts(Duration::ZERO),
        });

        assert_eq!(
            state.last_warning.as_deref(),
            Some("WrongToken: wrong token")
        );
        assert!(state
            .recent_events
            .back()
            .unwrap()
            .contains("warning WrongToken: wrong token"));
    }

    #[test]
    fn recent_event_buffer_stays_bounded() {
        let mut state = TuiState::default();
        for seq in 0..(RECENT_EVENT_LIMIT as u32 + 5) {
            state.push_event(format!("event {seq}"));
        }

        assert_eq!(state.recent_events.len(), RECENT_EVENT_LIMIT);
        assert_eq!(state.recent_events.front().unwrap(), "event 5");
    }

    #[test]
    fn recent_event_visible_count_tracks_panel_inner_height() {
        assert_eq!(recent_events_visible_count(0), 0);
        assert_eq!(recent_events_visible_count(2), 0);
        assert_eq!(recent_events_visible_count(9), 7);
    }

    #[test]
    fn visible_window_length_scales_with_chart_width_and_remains_bounded() {
        let history: VecDeque<_> = (0..300).map(|seq| graph_sample(seq, seq.into())).collect();

        let narrow = visible_history_window(&history, 20);
        let wide = visible_history_window(&history, 80);
        let huge = visible_history_window(&history, 1_000);

        assert_eq!(narrow.len(), MIN_CHART_POINTS);
        assert!(wide.len() > narrow.len(), "wide={}", wide.len());
        assert_eq!(huge.len(), HISTORY_LIMIT);
        assert_eq!(huge.first().unwrap().seq, 60);
    }

    #[test]
    fn chart_bounds_use_visible_positive_cluster_without_zero() {
        let series = [series(vec![(0.0, 42.0), (1.0, 43.0), (2.0, 44.0)])];
        let (min_y, max_y) = chart_y_bounds(&series);

        assert!(min_y > 0.0, "min_y={min_y}");
        assert!(min_y < 42.0, "min_y={min_y}");
        assert!(max_y > 44.0, "max_y={max_y}");
    }

    #[test]
    fn chart_bounds_preserve_negative_values() {
        let series = [series(vec![(0.0, -3.0), (1.0, -1.5), (2.0, 0.5)])];
        let (min_y, max_y) = chart_y_bounds(&series);

        assert!(min_y < -3.0, "min_y={min_y}");
        assert!(max_y > 0.5, "max_y={max_y}");
    }

    #[test]
    fn chart_bounds_handle_empty_and_flat_series() {
        assert_eq!(chart_y_bounds(&[]), (0.0, 1.0));

        let flat = [series(vec![(0.0, 12.0), (1.0, 12.0)])];
        let (min_y, max_y) = chart_y_bounds(&flat);
        assert!(min_y < 12.0, "min_y={min_y}");
        assert!(max_y > 12.0, "max_y={max_y}");
        assert!(min_y > 0.0, "min_y={min_y}");
    }

    #[test]
    fn optional_missing_one_way_series_are_omitted_not_zero_filled() {
        let visible_samples = [graph_sample(1, 2_000_000)];
        let visible: Vec<_> = visible_samples.iter().collect();

        let one_way = graph_series(GraphMode::OneWay, &visible);
        let rtt = graph_series(GraphMode::Rtt, &visible);

        assert!(one_way.is_empty());
        assert_eq!(
            rtt.iter()
                .filter_map(|series| series.value)
                .collect::<Vec<_>>(),
            vec![GraphValue::EffectiveRtt]
        );
    }

    #[test]
    fn graph_modes_use_readable_default_series() {
        let visible_samples = [graph_sample_with_timing(1, 3_000_000)];
        let visible: Vec<_> = visible_samples.iter().collect();

        assert_eq!(
            graph_series(GraphMode::Rtt, &visible)
                .iter()
                .filter_map(|series| series.value)
                .collect::<Vec<_>>(),
            vec![GraphValue::EffectiveRtt]
        );
        assert_eq!(
            graph_series(GraphMode::OneWay, &visible)
                .iter()
                .filter_map(|series| series.value)
                .collect::<Vec<_>>(),
            vec![GraphValue::ClientToServer, GraphValue::ServerToClient]
        );
        assert_eq!(
            graph_series(GraphMode::Combined, &visible)
                .iter()
                .filter_map(|series| series.value)
                .collect::<Vec<_>>(),
            vec![
                GraphValue::EffectiveRtt,
                GraphValue::ClientToServer,
                GraphValue::ServerToClient
            ]
        );
    }

    #[test]
    fn graph_mode_cycling_walks_all_modes() {
        let mut state = TuiState::default();

        assert_eq!(state.graph_mode, GraphMode::Rtt);
        state.cycle_graph_mode();
        assert_eq!(state.graph_mode, GraphMode::OneWay);
        state.cycle_graph_mode();
        assert_eq!(state.graph_mode, GraphMode::Combined);
        state.cycle_graph_mode();
        assert_eq!(state.graph_mode, GraphMode::Split);
        state.cycle_graph_mode();
        assert_eq!(state.graph_mode, GraphMode::Rtt);
    }

    #[test]
    fn pause_suppresses_scheduled_renders_but_allows_forced_controls() {
        let now = Instant::now();
        let due = now - Duration::from_millis(1);

        assert!(!should_render(now, due, true, false));
        assert!(should_render(now, due, true, true));

        let mut state = TuiState::default();
        state.toggle_pause();
        assert!(state.paused);
    }

    #[test]
    fn sample_details_preserve_signed_values() {
        let mut state = TuiState::default();
        state.process_event(&reply(4, -1_250_000));

        let target = primary_target(&state);
        let sample = target.last_sample.unwrap();
        assert_eq!(sample.seq, 4);
        assert_eq!(sample.effective_ns, -1_250_000);
        assert_eq!(sample.client_to_server_ns, Some(-20_000));
        assert_eq!(sample.server_processing_ns, Some(100_000));

        let graph = target.graph_history.back().unwrap();
        assert_eq!(graph.seq, 4);
        assert_eq!(graph.effective_ns, -1_250_000);
        assert_eq!(graph.client_to_server_ns, Some(-20_000));
        assert_eq!(graph.server_to_client_ns, Some(30_000));
        assert_eq!(graph.server_processing_ns, Some(100_000));
    }
}
