use std::{
    collections::{BTreeMap, VecDeque},
    io::{self, Stdout},
    time::{Duration, Instant, SystemTime},
};

use chrono::{DateTime, Local, TimeZone, Utc};
use crossterm::{
    cursor::Show,
    event::KeyCode,
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use irtt_client::managed::{
    ManagedStatus, ManagedTargetEndReason, ManagedTargetLifecycle, ManagedTargetOutcome,
    TargetInstance,
};
use irtt_client::{
    ClientEvent, ClientTimestamp, NegotiationResult, OneWayDelaySample, RttSample, SignedDuration,
};
use irtt_stats::{StatsCollector, TimeStats};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    symbols,
    text::{Line, Span, Text},
    widgets::{
        canvas::{Canvas, Line as CanvasLine, Painter, Shape},
        Axis, Block, Chart, Paragraph, Row, Table, Wrap,
    },
    Frame, Terminal,
};

use crate::{
    cmd::{
        format::{
            format_count, format_duration, format_ns_f64, format_optional_count,
            format_optional_ns_i128, format_percent, format_percent_ratio, ABSENT,
        },
        tui::args::TuiArgs,
    },
    shared::client::{expected_probe_count, GroupPacingArg, ManagedRunSetup},
};

const HISTORY_LIMIT: usize = 500_000;
const RECENT_EVENT_LIMIT: usize = 80;
const MIN_WIDTH: u16 = 56;
const MIN_HEIGHT: u16 = 18;
const DEFAULT_GRAPH_WINDOW: Duration = Duration::from_secs(60);
const MIN_GRAPH_WINDOW: Duration = Duration::from_secs(5);
const MAX_GRAPH_WINDOW: Duration = Duration::from_secs(24 * 60 * 60);

const GRAPH_WINDOWS: &[Duration] = &[
    Duration::from_secs(5),
    Duration::from_secs(10),
    Duration::from_secs(15),
    Duration::from_secs(30),
    Duration::from_secs(60),
    Duration::from_secs(2 * 60),
    Duration::from_secs(5 * 60),
    Duration::from_secs(10 * 60),
    Duration::from_secs(15 * 60),
    Duration::from_secs(30 * 60),
    Duration::from_secs(60 * 60),
    Duration::from_secs(2 * 60 * 60),
    Duration::from_secs(4 * 60 * 60),
    Duration::from_secs(8 * 60 * 60),
    Duration::from_secs(12 * 60 * 60),
    Duration::from_secs(24 * 60 * 60),
];

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
    dropped_events: u64,
    graph_metric: GraphMetric,
    graph_viewport: GraphViewport,
    selected_target: usize,
    details_open: bool,
    details_scroll: (u16, u16),
    pub(super) paused: bool,
}

impl TuiState {
    pub(super) fn with_target_labels(
        config: TuiConfig,
        labels: impl IntoIterator<Item = String>,
    ) -> Self {
        let stats_config = stats_config(config.duration.is_none());
        let targets = labels
            .into_iter()
            .map(|label| TuiTargetState::new(label, stats_config))
            .collect::<Vec<_>>();
        // The sole caller supplies prepared, nonempty targets. Entries are never
        // removed, and selection only cycles within this vector.
        assert!(!targets.is_empty(), "TUI requires prepared targets");
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
            dropped_events: 0,
            graph_metric: GraphMetric::EffectiveRtt,
            graph_viewport: GraphViewport::default(),
            selected_target: 0,
            details_open: false,
            details_scroll: (0, 0),
            paused: false,
        }
    }

    fn ensure_target(&mut self, label: &str) -> usize {
        if let Some(&idx) = self.target_index.get(label) {
            return idx;
        }
        let idx = self.targets.len();
        let mut target = TuiTargetState::new(
            label.to_owned(),
            stats_config(self.config.duration.is_none()),
        );
        target.status = TargetStatus::Unknown;
        self.targets.push(target);
        self.target_index.insert(label.to_owned(), idx);
        idx
    }

    pub(super) fn process_target_event(&mut self, target: &TargetInstance, event: &ClientEvent) {
        let idx = self.ensure_target(target.id.as_str());
        if !self.accept_generation(idx, target.generation) {
            return;
        }
        if self.targets[idx].terminal_generation == Some(target.generation) {
            self.targets[idx].process_session_metadata(event);
            if matches!(
                event,
                ClientEvent::SessionStarted(_)
                    | ClientEvent::NoTestCompleted(_)
                    | ClientEvent::SessionClosed { .. }
            ) {
                return;
            }
        }
        self.process_event_for_target(idx, event);
    }

    fn accept_generation(&mut self, idx: usize, generation: u64) -> bool {
        let target = &mut self.targets[idx];
        if target
            .generation
            .is_some_and(|current| current > generation)
        {
            return false;
        }
        if target.generation != Some(generation) {
            target.generation = Some(generation);
            target.stats = StatsCollector::new(stats_config(self.config.duration.is_none()));
            target.session = None;
            target.negotiated = None;
        }
        true
    }

    pub(super) fn process_target_opening(&mut self, instance: &TargetInstance) {
        let idx = self.ensure_target(instance.id.as_str());
        let new_generation = self.targets[idx].generation != Some(instance.generation);
        if self.accept_generation(idx, instance.generation) && new_generation {
            self.targets[idx].status = TargetStatus::Opening;
        }
    }

    pub(super) fn process_managed_status(&mut self, status: &ManagedStatus) {
        // Consume durable endings before accepting replacement generations.
        // Receipts retain this history even if a watch update was coalesced.
        for outcome in status.recent_target_outcomes.iter().chain(
            status
                .targets
                .iter()
                .filter_map(|target| target.outcome.as_deref()),
        ) {
            self.process_target_outcome(outcome);
        }
        for target in status.targets.iter() {
            if matches!(
                target.lifecycle,
                ManagedTargetLifecycle::Pending
                    | ManagedTargetLifecycle::Connecting
                    | ManagedTargetLifecycle::Opening
            ) {
                self.process_target_opening(&target.target);
            } else {
                let idx = self.ensure_target(target.target.id.as_str());
                if self.accept_generation(idx, target.target.generation)
                    && self.targets[idx].terminal_generation != Some(target.target.generation)
                    && target.lifecycle == ManagedTargetLifecycle::Active
                {
                    self.targets[idx].status = TargetStatus::Active;
                }
            }
        }
    }

    pub(super) fn process_target_outcome(&mut self, outcome: &ManagedTargetOutcome) {
        let label = outcome.target.id.as_str();
        let idx = self.ensure_target(label);

        if self.targets[idx]
            .terminal_generation
            .is_some_and(|generation| generation >= outcome.target.generation)
        {
            return;
        }
        let current = self.accept_generation(idx, outcome.target.generation);
        self.targets[idx].terminal_generation = Some(outcome.target.generation);
        let (status, recent, primary_warning) = match &outcome.end_reason {
            ManagedTargetEndReason::TestComplete => {
                (TargetStatus::Closed, "test completed".to_owned(), None)
            }
            ManagedTargetEndReason::PeerClosed => (
                TargetStatus::Closed,
                "session closed by peer".to_owned(),
                None,
            ),
            ManagedTargetEndReason::NoTestComplete => (
                TargetStatus::NoTest,
                "no-test negotiation completed".to_owned(),
                None,
            ),
            ManagedTargetEndReason::Stopped => {
                (TargetStatus::Stopped, "target stopped".to_owned(), None)
            }
            ManagedTargetEndReason::Removed => {
                (TargetStatus::Removed, "target removed".to_owned(), None)
            }
            ManagedTargetEndReason::Replaced => {
                (TargetStatus::Replaced, "target replaced".to_owned(), None)
            }
            ManagedTargetEndReason::Failed(failure) => {
                let message = format!("{}: {}", failure.kind, failure.message);
                (
                    TargetStatus::Failed,
                    format!("target failed: {message}"),
                    Some(message),
                )
            }
        };

        let cleanup_warning = outcome.cleanup_failure.as_ref().map(|failure| {
            format!(
                "cleanup failed ({} {}): {}",
                failure.phase, failure.kind, failure.message
            )
        });
        let warning = match (primary_warning, cleanup_warning.as_ref()) {
            (Some(primary), Some(cleanup)) => Some(format!("{primary}; {cleanup}")),
            (Some(primary), None) => Some(primary),
            (None, Some(cleanup)) => Some(cleanup.clone()),
            (None, None) => None,
        };

        let target = &mut self.targets[idx];
        if current {
            target.remote = outcome.remote.map(|remote| remote.to_string());
            target.status = status;
        }
        if let Some(warning) = &warning {
            target.last_warning = Some(warning.clone());
        }
        if let Some(warning) = warning {
            self.last_warning = Some(format!("{label}: {warning}"));
        }
        self.push_event(format!("{label}: {recent}"));
        if let Some(cleanup_warning) = cleanup_warning {
            self.push_event(format!("{label}: {cleanup_warning}"));
        }

        if self.config.duration.is_some()
            && self
                .targets
                .iter()
                .all(|target| target.status.is_terminal())
        {
            self.status = if self.targets.iter().any(|target| target.status.is_success()) {
                TuiStatus::Complete
            } else if self
                .targets
                .iter()
                .any(|target| target.status == TargetStatus::Failed)
            {
                TuiStatus::Error
            } else {
                TuiStatus::Complete
            };
        }
    }

    fn process_event_for_target(&mut self, target_idx: usize, event: &ClientEvent) {
        let recent;
        let mut global_status = None;
        let mut global_warning = None;
        let label = {
            let target = &mut self.targets[target_idx];
            target.stats.process(event);
            match event {
                ClientEvent::SessionStarted(irtt_client::SessionStarted {
                    remote,
                    token,
                    negotiation: negotiated,
                    ..
                }) => {
                    target.remote = Some(remote.to_string());
                    target.session = Some(format!("{token:#x}"));
                    target.negotiated = Some(negotiated.clone());
                    target.status = TargetStatus::Active;
                    global_status = Some(TuiStatus::Running);
                    recent = format!("session started token={token:#x}");
                }
                ClientEvent::NoTestCompleted(irtt_client::NoTestCompleted {
                    remote,
                    negotiation: negotiated,
                    ..
                }) => {
                    target.remote = Some(remote.to_string());
                    target.negotiated = Some(negotiated.clone());
                    target.status = TargetStatus::NoTest;
                    global_status = Some(TuiStatus::Complete);
                    recent = "no-test negotiation completed".to_owned();
                }
                ClientEvent::SessionClosed { token, .. } => {
                    target.session = Some(format!("{token:#x}"));
                    target.status = TargetStatus::Closed;
                    global_status = Some(TuiStatus::Complete);
                    recent = format!("session closed token={token:#x}");
                }
                ClientEvent::EchoSent { seq, bytes, .. } => {
                    recent = format!("sent seq={} bytes={bytes}", seq);
                }
                ClientEvent::EchoReply {
                    seq,
                    received_at,
                    rtt,
                    one_way,
                    server_timing,
                    ..
                } => {
                    target.push_graph_sample(LatestSample {
                        generation: target.generation.expect("client event has a generation"),
                        timestamp: *received_at,
                        seq: *seq,
                        rtt: *rtt,
                        one_way: *one_way,
                        server_processing: server_timing.and_then(|timing| timing.processing),
                    });
                    recent = format!(
                        "reply seq={} effective={}",
                        seq,
                        format_optional_ns_i128(Some(rtt.effective.as_nanos()))
                    );
                }
                ClientEvent::EchoLoss { seq, .. } => {
                    recent = format!("loss seq={}", seq);
                }
                ClientEvent::DuplicateReply { seq, remote, .. } => {
                    recent = format!("duplicate seq={} from {remote}", seq);
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
                                format_optional_ns_i128(Some(sample.effective.as_nanos()))
                            )
                        })
                        .unwrap_or_default();
                    recent = format!("late seq={} highest_seen={}{}", seq, highest_seen, timing);
                }
                ClientEvent::Warning { kind, message, .. } => {
                    let warning = format!("{kind:?}: {message}");
                    target.last_warning = Some(warning.clone());
                    global_warning = Some(warning.clone());
                    recent = format!("warning {warning}");
                }
            }
            target.label.clone()
        };
        if let Some(status) = global_status {
            self.status = if status == TuiStatus::Complete
                && (self.config.duration.is_none()
                    || !self
                        .targets
                        .iter()
                        .all(|target| target.status.is_terminal()))
            {
                TuiStatus::Running
            } else {
                status
            };
        }
        if let Some(warning) = global_warning {
            self.last_warning = Some(format!("{label}: {warning}"));
        }
        self.push_event(format!("{label}: {recent}"));
    }

    pub(super) fn set_status(&mut self, status: TuiStatus) {
        self.status = status;
    }

    pub(super) fn mark_dropped_managed_events(&mut self, dropped_events: u64) {
        if dropped_events == 0 {
            return;
        }
        self.dropped_events = dropped_events;
        let event_word = if dropped_events == 1 {
            "event"
        } else {
            "events"
        };
        let warning = format!(
            "dropped {dropped_events} managed run {event_word}; statistics may be incomplete"
        );
        self.last_warning = Some(warning.clone());
        self.push_event(format!("warning {warning}"));
    }

    pub(super) fn set_error(&mut self, message: String) {
        let target = &mut self.targets[0];
        target.status = TargetStatus::Failed;
        target.last_warning = Some(message.clone());
        self.set_run_error(message);
    }

    pub(super) fn set_run_error(&mut self, message: String) {
        self.status = TuiStatus::Error;
        self.last_warning = Some(message.clone());
        self.push_event(format!("error {message}"));
    }

    pub(super) fn clear_visible_history(&mut self) {
        for target in &mut self.targets {
            target.graph_history.clear();
        }
        self.graph_viewport.follow_live();
        self.push_event("visible graph history reset".to_owned());
    }

    pub(super) fn toggle_pause(&mut self) {
        self.paused = !self.paused;
    }

    pub(super) fn handle_key(&mut self, key: KeyCode, area: Rect) -> bool {
        match key {
            KeyCode::Char('d' | 'g') | KeyCode::Esc if key != KeyCode::Esc || self.details_open => {
                self.details_open = !self.details_open;
                self.details_scroll = (0, 0);
            }
            KeyCode::Tab | KeyCode::BackTab => {
                let count = self.targets.len();
                self.selected_target = if key == KeyCode::Tab {
                    (self.selected_target + 1) % count
                } else {
                    (self.selected_target + count - 1) % count
                };
                self.details_scroll = (0, 0);
            }
            KeyCode::Char('p') => self.toggle_pause(),
            KeyCode::Char('r') => self.clear_visible_history(),
            _ if self.details_open => {
                let text = details_text(self);
                let body = dashboard_layout(area, self)[2];
                let inner = Block::bordered().inner(body);
                let (max_y, max_x) = details_scroll_limits(&text, body);
                let (y, x) = self.details_scroll;
                let (y, x) = (y.min(max_y), x.min(max_x));
                self.details_scroll = match key {
                    KeyCode::Up => (y.saturating_sub(1), x),
                    KeyCode::Down => (y.saturating_add(1).min(max_y), x),
                    KeyCode::PageUp => (y.saturating_sub(inner.height.max(1)), x),
                    KeyCode::PageDown => (y.saturating_add(inner.height.max(1)).min(max_y), x),
                    KeyCode::Home => (0, 0),
                    KeyCode::End => (max_y, x),
                    KeyCode::Left => (y, x.saturating_sub(8)),
                    KeyCode::Right => (y, x.saturating_add(8).min(max_x)),
                    _ => return false,
                };
            }
            _ => {
                let oldest = self.oldest_graph_sample_time();
                let newest = self.newest_graph_sample_time();
                match key {
                    KeyCode::Char('m') => self.graph_metric = self.graph_metric.next(),
                    KeyCode::Left => self.graph_viewport.pan_backward(oldest, newest),
                    KeyCode::Right => self.graph_viewport.pan_forward(newest),
                    KeyCode::PageUp => self.graph_viewport.page_backward(oldest, newest),
                    KeyCode::PageDown => self.graph_viewport.page_forward(newest),
                    KeyCode::Home => self.graph_viewport.jump_oldest(oldest, newest),
                    KeyCode::End => self.graph_viewport.follow_live(),
                    KeyCode::Char('+' | '=') => self.graph_viewport.zoom_in(newest),
                    KeyCode::Char('-') => self.graph_viewport.zoom_out(newest),
                    KeyCode::Char('0') => self.graph_viewport.reset_window(newest),
                    _ => return false,
                }
            }
        }
        true
    }

    fn push_event(&mut self, event: String) {
        push_bounded(&mut self.recent_events, event, RECENT_EVENT_LIMIT);
    }

    fn selected_target(&self) -> &TuiTargetState {
        &self.targets[self.selected_target]
    }

    fn oldest_graph_sample_time(&self) -> Option<Instant> {
        self.targets
            .iter()
            .filter_map(|target| {
                target
                    .graph_history
                    .front()
                    .map(|sample| sample.timestamp.mono)
            })
            .min()
    }

    fn newest_graph_sample_time(&self) -> Option<Instant> {
        self.targets
            .iter()
            .filter_map(|target| {
                target
                    .graph_history
                    .back()
                    .map(|sample| sample.timestamp.mono)
            })
            .max()
    }
}

#[derive(Debug)]
struct TuiTargetState {
    label: String,
    generation: Option<u64>,
    terminal_generation: Option<u64>,
    remote: Option<String>,
    session: Option<String>,
    status: TargetStatus,
    negotiated: Option<NegotiationResult>,
    graph_history: VecDeque<GraphSample>,
    last_sample: Option<LatestSample>,
    last_warning: Option<String>,
    stats: StatsCollector,
}

impl TuiTargetState {
    fn new(label: String, stats_config: irtt_stats::StatsConfig) -> Self {
        Self {
            label,
            generation: None,
            terminal_generation: None,
            remote: None,
            session: None,
            status: TargetStatus::Opening,
            negotiated: None,
            graph_history: VecDeque::new(),
            last_sample: None,
            last_warning: None,
            stats: StatsCollector::new(stats_config),
        }
    }

    fn process_session_metadata(&mut self, event: &ClientEvent) {
        match event {
            ClientEvent::SessionStarted(started) => {
                self.remote = Some(started.remote.to_string());
                self.session = Some(format!("{:#x}", started.token));
                self.negotiated = Some(started.negotiation.clone());
            }
            ClientEvent::NoTestCompleted(completed) => {
                self.remote = Some(completed.remote.to_string());
                self.negotiated = Some(completed.negotiation.clone());
            }
            ClientEvent::SessionClosed { remote, token, .. } => {
                self.remote = Some(remote.to_string());
                self.session = Some(format!("{token:#x}"));
            }
            _ => {}
        }
    }

    fn push_graph_sample(&mut self, sample: LatestSample) {
        let history = GraphSample {
            timestamp: sample.timestamp,
            values_ms: GraphMetric::ALL.map(|metric| {
                metric
                    .value_ns(&sample)
                    .map_or(f64::NAN, |ns| ns as f64 / 1_000_000.0)
            }),
            break_before: self
                .last_sample
                .is_none_or(|last| last.generation != sample.generation),
        };
        self.last_sample = Some(sample);
        push_bounded(&mut self.graph_history, history, HISTORY_LIMIT);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TargetStatus {
    Opening,
    Active,
    Closed,
    Failed,
    NoTest,
    Stopped,
    Removed,
    Replaced,
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
            Self::Stopped => "stopped",
            Self::Removed => "removed",
            Self::Replaced => "replaced",
            Self::Unknown => "unknown",
        }
    }

    fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Closed
                | Self::Failed
                | Self::NoTest
                | Self::Stopped
                | Self::Removed
                | Self::Replaced
        )
    }

    fn is_success(self) -> bool {
        matches!(self, Self::Closed | Self::NoTest)
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

/// Statistics policy for the TUI.
///
/// The TUI treats late replies as diagnostics, so it counts a matched late
/// reply without letting it contribute timing samples. That is an explicit
/// statistics policy rather than something the TUI expresses by degrading the
/// event it forwards.
fn stats_config(continuous: bool) -> irtt_stats::StatsConfig {
    let base = if continuous {
        irtt_stats::StatsConfig::continuous()
    } else {
        irtt_stats::StatsConfig::finite()
    };
    irtt_stats::StatsConfig {
        late_replies: irtt_stats::LateReplyMode::CountOnly,
        ..base
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
    pub(super) fn from_args(args: &TuiArgs, setup: &ManagedRunSetup) -> Self {
        Self {
            interval: args.interval,
            duration: (!args.is_continuous()).then_some(args.duration),
            timeout: setup.client.probe_timeout,
            target_probes: (!args.is_continuous())
                .then(|| expected_probe_count(args.duration, args.interval)),
            pacing: args.pacing,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TuiStatus {
    Opening,
    Running,
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
            Self::Interrupted => "interrupted",
            Self::Closing => "closing",
            Self::Complete => "complete",
            Self::Error => "error",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GraphViewport {
    mode: GraphViewportMode,
    window: Duration,
}

impl Default for GraphViewport {
    fn default() -> Self {
        Self {
            mode: GraphViewportMode::Follow,
            window: DEFAULT_GRAPH_WINDOW,
        }
    }
}

impl GraphViewport {
    fn range(self, now: Instant, newest_sample: Option<Instant>) -> GraphViewportRange {
        let end = match self.mode {
            GraphViewportMode::Follow => newest_sample.unwrap_or(now).max(now),
            GraphViewportMode::Historical { end } => end,
        };
        GraphViewportRange {
            start: end.checked_sub(self.window).unwrap_or(end),
            end,
            window: self.window,
        }
    }

    fn follow_live(&mut self) {
        self.mode = GraphViewportMode::Follow;
    }

    fn pan_backward(&mut self, oldest: Option<Instant>, newest: Option<Instant>) {
        self.pan_by(self.window / 4, true, oldest, newest);
    }

    fn pan_forward(&mut self, newest: Option<Instant>) {
        self.pan_by(self.window / 4, false, None, newest);
    }

    fn page_backward(&mut self, oldest: Option<Instant>, newest: Option<Instant>) {
        self.pan_by(self.window, true, oldest, newest);
    }

    fn page_forward(&mut self, newest: Option<Instant>) {
        self.pan_by(self.window, false, None, newest);
    }

    fn jump_oldest(&mut self, oldest: Option<Instant>, newest: Option<Instant>) {
        let Some(oldest) = oldest else {
            return;
        };
        let live_end = newest.unwrap_or(oldest);
        self.mode = GraphViewportMode::Historical {
            end: (oldest + self.window).min(live_end),
        };
    }

    fn zoom_in(&mut self, newest: Option<Instant>) {
        self.set_window(graph_zoom_window(self.window, false), newest);
    }

    fn zoom_out(&mut self, newest: Option<Instant>) {
        self.set_window(graph_zoom_window(self.window, true), newest);
    }

    fn reset_window(&mut self, newest: Option<Instant>) {
        self.set_window(DEFAULT_GRAPH_WINDOW, newest);
    }

    fn set_window(&mut self, window: Duration, newest: Option<Instant>) {
        self.window = window.clamp(MIN_GRAPH_WINDOW, MAX_GRAPH_WINDOW);
        if let GraphViewportMode::Historical { end } = &mut self.mode {
            if let Some(newest) = newest {
                *end = (*end).min(newest);
            }
        }
    }

    fn pan_by(
        &mut self,
        amount: Duration,
        backward: bool,
        oldest: Option<Instant>,
        newest: Option<Instant>,
    ) {
        let Some(newest) = newest else {
            return;
        };
        let current_end = match self.mode {
            GraphViewportMode::Follow => newest,
            GraphViewportMode::Historical { end } => end,
        };
        let mut end = if backward {
            current_end.checked_sub(amount).unwrap_or(current_end)
        } else {
            current_end + amount
        };
        if let Some(oldest) = oldest {
            end = end.max(oldest + self.window);
        }
        end = end.min(newest);
        self.mode = GraphViewportMode::Historical { end };
    }
}

fn graph_zoom_window(current: Duration, larger: bool) -> Duration {
    if larger {
        GRAPH_WINDOWS
            .iter()
            .copied()
            .find(|&window| window > current)
            .unwrap_or(MAX_GRAPH_WINDOW)
    } else {
        GRAPH_WINDOWS
            .iter()
            .copied()
            .rev()
            .find(|&window| window < current)
            .unwrap_or(MIN_GRAPH_WINDOW)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GraphViewportMode {
    Follow,
    Historical { end: Instant },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GraphViewportRange {
    start: Instant,
    end: Instant,
    window: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GraphMetric {
    EffectiveRtt,
    RawRtt,
    AdjustedRtt,
    ClientToServer,
    ServerToClient,
    ServerProcessing,
}

impl GraphMetric {
    // Enum order also indexes the retained graph values.
    const ALL: [Self; 6] = [
        Self::EffectiveRtt,
        Self::RawRtt,
        Self::AdjustedRtt,
        Self::ClientToServer,
        Self::ServerToClient,
        Self::ServerProcessing,
    ];

    fn next(self) -> Self {
        match self {
            Self::EffectiveRtt => Self::RawRtt,
            Self::RawRtt => Self::AdjustedRtt,
            Self::AdjustedRtt => Self::ClientToServer,
            Self::ClientToServer => Self::ServerToClient,
            Self::ServerToClient => Self::ServerProcessing,
            Self::ServerProcessing => Self::EffectiveRtt,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::EffectiveRtt => "effective RTT",
            Self::RawRtt => "raw RTT",
            Self::AdjustedRtt => "adjusted RTT",
            Self::ClientToServer => "client to server",
            Self::ServerToClient => "server to client",
            Self::ServerProcessing => "server processing",
        }
    }

    fn empty_message(self) -> &'static str {
        match self {
            Self::EffectiveRtt | Self::RawRtt => "waiting for primary replies",
            Self::AdjustedRtt => "waiting for adjusted RTT samples",
            Self::ClientToServer | Self::ServerToClient => "waiting for one-way delay samples",
            Self::ServerProcessing => "waiting for server processing samples",
        }
    }

    fn value_ns(self, sample: &LatestSample) -> Option<i128> {
        match self {
            Self::EffectiveRtt => Some(sample.rtt.effective.as_nanos()),
            Self::RawRtt => Some(SignedDuration::from(sample.rtt.raw).as_nanos()),
            Self::AdjustedRtt => sample.rtt.adjusted.map(SignedDuration::as_nanos),
            Self::ClientToServer => sample
                .one_way
                .and_then(|sample| sample.client_to_server)
                .map(SignedDuration::as_nanos),
            Self::ServerToClient => sample
                .one_way
                .and_then(|sample| sample.server_to_client)
                .map(SignedDuration::as_nanos),
            Self::ServerProcessing => sample
                .server_processing
                .map(|duration| SignedDuration::from(duration).as_nanos()),
        }
    }

    fn axis_kind(self) -> ChartAxisKind {
        match self {
            Self::RawRtt | Self::ServerProcessing => ChartAxisKind::NonNegative,
            Self::EffectiveRtt
            | Self::AdjustedRtt
            | Self::ClientToServer
            | Self::ServerToClient => ChartAxisKind::Signed,
        }
    }
}

// Retained values use the same nanoseconds-to-f64 milliseconds conversion as
// the chart. NaN means absent and never reaches the canvas as a finite point.
// Keep both clocks: geometry is monotonic, while labels use local wall time.
#[derive(Debug, Clone, Copy)]
struct GraphSample {
    timestamp: ClientTimestamp,
    values_ms: [f64; 6],
    break_before: bool,
}

impl GraphSample {
    fn value_ms(&self, metric: GraphMetric) -> Option<f64> {
        let value = self.values_ms[metric as usize];
        value.is_finite().then_some(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LatestSample {
    generation: u64,
    timestamp: ClientTimestamp,
    seq: u32,
    rtt: RttSample,
    one_way: Option<OneWayDelaySample>,
    server_processing: Option<Duration>,
}

fn push_bounded<T>(items: &mut VecDeque<T>, item: T, limit: usize) {
    if items.len() == limit {
        items.pop_front();
    } else if items.len() == items.capacity() && items.capacity() >= limit / 2 {
        // Growth is naturally exponential; once we're within reach of `limit`,
        // reserve exactly up to it instead of letting the next doubling step
        // overshoot it for long-running, heavily populated buffers.
        items.reserve_exact(limit - items.len());
    }
    items.push_back(item);
}

fn draw_dashboard(frame: &mut Frame<'_>, state: &TuiState) {
    let area = frame.area();
    if area.width < MIN_WIDTH || area.height < MIN_HEIGHT {
        frame.render_widget(
            Paragraph::new("terminal too small\nresize, or press q / Ctrl-C to quit gracefully")
                .block(Block::bordered().title("irtt-rs"))
                .wrap(Wrap { trim: true }),
            area,
        );
        return;
    }
    let rows = dashboard_layout(area, state);
    frame.render_widget(header(state), rows[0]);
    frame.render_widget(target_table(state, rows[1]), rows[1]);
    if state.details_open {
        let text = details_text(state);
        let (max_y, max_x) = details_scroll_limits(&text, rows[2]);
        frame.render_widget(
            Paragraph::new(text)
                .scroll((
                    state.details_scroll.0.min(max_y),
                    state.details_scroll.1.min(max_x),
                ))
                .block(Block::bordered().title("target details | arrows scroll | d graph")),
            rows[2],
        );
    } else {
        render_graph_area(frame, rows[2], state);
    }
    frame.render_widget(status_line(state), rows[3]);
}

fn dashboard_layout(area: Rect, state: &TuiState) -> [Rect; 4] {
    let footer = 3 + u16::from(state.last_warning.is_some());
    let target_rows = state.targets.len().min(6) as u16;
    let target_height = (target_rows + 3).min(area.height.saturating_sub(3 + footer + 7));
    Layout::vertical([
        Constraint::Length(3),
        Constraint::Length(target_height),
        Constraint::Min(7),
        Constraint::Length(footer),
    ])
    .areas(area)
}

fn header(state: &TuiState) -> Paragraph<'static> {
    let selected = state.selected_target();
    let last = selected.last_sample;
    let incomplete = if state.dropped_events > 0 {
        format!(" incomplete:dropped={}", state.dropped_events)
    } else {
        String::new()
    };
    Paragraph::new(vec![
        Line::from(format!(
            "irtt-rs | {}{}{} | {} / {}",
            state.status.label(),
            if state.paused { " display paused" } else { "" },
            incomplete,
            format_span(state.started_at.elapsed()),
            state
                .config
                .duration
                .map(format_span)
                .unwrap_or_else(|| "continuous".to_owned()),
        )),
        Line::from(format!(
            "{} | {}",
            selected.label,
            selected.remote.as_deref().unwrap_or(ABSENT),
        )),
        Line::from(format!(
            "one-way c2s {} / s2c {}",
            format_optional_ns_i128(
                last.and_then(|sample| GraphMetric::ClientToServer.value_ns(&sample))
            ),
            format_optional_ns_i128(
                last.and_then(|sample| GraphMetric::ServerToClient.value_ns(&sample))
            ),
        )),
    ])
}

fn target_table(state: &TuiState, area: Rect) -> Table<'static> {
    let visible = usize::from(area.height.saturating_sub(3)).max(1);
    // Derive the page from focus and terminal height; no separate scroll state.
    let start = state.selected_target / visible * visible;
    let rows = state
        .targets
        .iter()
        .enumerate()
        .skip(start)
        .take(visible)
        .map(|(idx, target)| {
            let snapshot = target.stats.snapshot();
            Row::new(vec![
                format!(
                    "{}{}",
                    if idx == state.selected_target {
                        ">"
                    } else {
                        " "
                    },
                    target.label
                ),
                if target.status == TargetStatus::Active && target.last_sample.is_none() {
                    "waiting".to_owned()
                } else {
                    target.status.label().to_owned()
                },
                format_optional_ns_i128(
                    target
                        .last_sample
                        .map(|sample| sample.rtt.effective.as_nanos()),
                ),
                if snapshot.packets.packets_sent == 0 {
                    ABSENT.to_owned()
                } else {
                    format_percent(snapshot.loss.packet_loss_percent)
                },
                if snapshot.ipdv.round_trip.count == 0 {
                    ABSENT.to_owned()
                } else {
                    format_ns_f64(snapshot.ipdv.round_trip.stddev_ns())
                },
                target
                    .last_sample
                    .map(|sample| format_age(sample.timestamp.mono.elapsed()))
                    .unwrap_or_else(|| ABSENT.to_owned()),
            ])
            .style(target_style(idx))
        });
    Table::new(
        rows,
        [
            Constraint::Fill(1),
            Constraint::Length(8),
            Constraint::Length(9),
            Constraint::Length(7),
            Constraint::Length(9),
            Constraint::Length(6),
        ],
    )
    .header(
        Row::new(["target", "state", "RTT", "loss %", "jitter σ", "age"])
            .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(Block::bordered().title(format!(
        "targets {}/{} | Tab select",
        state.selected_target + 1,
        state.targets.len()
    )))
}

fn details_text(state: &TuiState) -> Text<'static> {
    let target = state.selected_target();
    let snapshot = target.stats.snapshot();
    let last = target.last_sample;
    let packets = snapshot.packets;
    let mut lines = vec![
        Line::from(format!("target: {}", target.label)),
        Line::from(format!(
            "remote: {}  session: {}",
            target.remote.as_deref().unwrap_or(ABSENT),
            target.session.as_deref().unwrap_or(ABSENT)
        )),
        Line::from(format!(
            "interval: {}  timeout: {}  pacing: {}",
            format_span(state.config.interval),
            format_span(state.config.timeout),
            state.config.pacing.label()
        )),
        Line::from(format!(
            "negotiated: {}",
            target
                .negotiated
                .as_ref()
                .map(format_negotiated)
                .unwrap_or_else(|| ABSENT.to_owned())
        )),
        Line::from(format!(
            "target warning: {}",
            target.last_warning.as_deref().unwrap_or(ABSENT)
        )),
        Line::from(format!(
            "run warning: {}",
            state.last_warning.as_deref().unwrap_or(ABSENT)
        )),
        Line::from(""),
        Line::from(format!(
            "sent {}  received {}  unique {}",
            packets.packets_sent, packets.packets_received, packets.unique_replies
        )),
        Line::from(format!(
            "lost {} ({})  duplicates {}  late {}  warnings {}",
            snapshot.loss.lost_packets,
            format_percent(snapshot.loss.packet_loss_percent),
            packets.duplicates,
            packets.late_packets,
            snapshot.events.warning_events
        )),
        Line::from(format!(
            "bytes sent {}  received {}",
            packets.bytes_sent, packets.bytes_received
        )),
        Line::from(format!(
            "server received {}  window {}",
            format_optional_count(packets.server_packets_received),
            format_optional_hex(packets.server_received_window)
        )),
        Line::from(format!(
            "progress: {}",
            state
                .config
                .target_probes
                .map(|count| format!(
                    "{}/{} ({})",
                    packets.packets_sent,
                    count,
                    format_percent_ratio(packets.packets_sent, count)
                ))
                .unwrap_or_else(|| "continuous".to_owned())
        )),
        Line::from(""),
        Line::from(format!(
            "latest seq: {}",
            last.map(|sample| sample.seq.to_string())
                .unwrap_or_else(|| ABSENT.to_owned())
        )),
    ];
    for metric in GraphMetric::ALL {
        lines.push(Line::from(format!(
            "{}: {}",
            metric.label(),
            format_optional_ns_i128(last.and_then(|sample| metric.value_ns(&sample)))
        )));
    }
    lines.extend([
        Line::from(""),
        Line::from(format!(
            "{:<18} {:>5} {:>9} {:>9} {:>9} {:>9}",
            "metric", "n", "min", "mean", "max", "stddev"
        )),
    ]);
    for (label, stats) in [
        ("effective RTT", &snapshot.rtt.primary),
        ("raw RTT", &snapshot.rtt.raw),
        ("adjusted RTT", &snapshot.rtt.adjusted),
        ("IPDV/jitter", &snapshot.ipdv.round_trip),
        ("send IPDV", &snapshot.ipdv.send),
        ("receive IPDV", &snapshot.ipdv.receive),
        ("send delay", &snapshot.one_way_delay.send_delay),
        ("receive delay", &snapshot.one_way_delay.receive_delay),
        ("server process", &snapshot.server_processing.processing),
        ("send call", &snapshot.send_call),
        ("timer error", &snapshot.timer_error),
    ] {
        push_time_line(&mut lines, label, stats);
    }
    lines.extend([
        Line::from(""),
        Line::from("recent events (all targets, newest first)"),
    ]);
    lines.extend(
        state
            .recent_events
            .iter()
            .rev()
            .map(|event| Line::from(event.clone())),
    );
    Text::from(lines)
}

fn details_scroll_limits(text: &Text<'_>, area: Rect) -> (u16, u16) {
    let inner = Block::bordered().inner(area);
    (
        text.height()
            .saturating_sub(usize::from(inner.height))
            .min(usize::from(u16::MAX)) as u16,
        text.width()
            .saturating_sub(usize::from(inner.width))
            .min(usize::from(u16::MAX)) as u16,
    )
}

fn status_line(state: &TuiState) -> Paragraph<'static> {
    let mut lines = if state.details_open {
        vec![
            Line::from("q quit | p pause | d graph | Tab target"),
            Line::from("↑/↓ scroll | ←/→ columns | PgUp/PgDn page"),
            Line::from("Home top | End bottom | r clear graph history"),
        ]
    } else {
        vec![
            Line::from("q quit | p pause | d details | Tab target | m metric"),
            Line::from("←/→ pan | PgUp/PgDn page | Home oldest | End live"),
            Line::from("+/- zoom | 0 reset window | r clear graph history"),
        ]
    };
    if let Some(warning) = &state.last_warning {
        lines.push(Line::styled(
            warning.clone(),
            Style::default().fg(Color::LightRed),
        ));
    }
    Paragraph::new(lines)
}

fn render_graph_area(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let viewport = state
        .graph_viewport
        .range(Instant::now(), state.newest_graph_sample_time());
    let metric = state.graph_metric;
    let series = state
        .targets
        .iter()
        .enumerate()
        .filter_map(|(idx, target)| target_metric_series(target, idx, viewport, metric))
        .collect::<Vec<_>>();
    let block = Block::bordered().title(format!(
        "{} | {} | window {}",
        metric.label(),
        graph_viewport_status(state),
        format_span(viewport.window)
    ));
    if series.is_empty() {
        frame.render_widget(Paragraph::new(metric.empty_message()).block(block), area);
        return;
    }
    let (min_y, max_y) = chart_y_bounds(&series, metric.axis_kind());
    let x_labels = viewport_x_axis_labels(state, viewport, area.width);
    let y_labels = y_axis_labels(min_y, max_y, y_axis_label_count(area.height));
    let inner = block.inner(area);
    // This chart has labels on both axes, no axis titles and no legend.
    // Match the pinned Chart's label gutter and its two bottom axis rows.
    let gutter = y_labels
        .iter()
        .map(Span::width)
        .max()
        .unwrap_or(0)
        .max(
            x_labels
                .first()
                .map_or(0, |label| label.width().saturating_sub(1)),
        )
        .min(usize::from(inner.width / 3)) as u16
        + 1;
    let plot = Rect::new(
        inner.x + gutter,
        inner.y,
        inner.width.saturating_sub(gutter),
        inner.height.saturating_sub(2),
    );
    frame.render_widget(
        Chart::new(vec![])
            .block(block)
            .x_axis(
                Axis::default()
                    .bounds(viewport_x_bounds(viewport))
                    .labels(x_labels)
                    .style(Style::default().fg(Color::Gray)),
            )
            .y_axis(
                Axis::default()
                    .bounds([min_y, max_y])
                    .labels(y_labels)
                    .style(Style::default().fg(Color::Gray)),
            ),
        area,
    );
    frame.render_widget(
        Canvas::default()
            .marker(symbols::Marker::Braille)
            .x_bounds(viewport_x_bounds(viewport))
            .y_bounds([min_y, max_y])
            .paint(|ctx| {
                for target in &series {
                    ctx.draw(target);
                }
            }),
        plot,
    );
}

#[derive(Debug, Clone, PartialEq)]
struct ChartSeries {
    style: Style,
    data: Vec<(f64, f64)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChartAxisKind {
    NonNegative,
    Signed,
}

fn target_metric_series(
    target: &TuiTargetState,
    target_idx: usize,
    viewport: GraphViewportRange,
    metric: GraphMetric,
) -> Option<ChartSeries> {
    // Histories are monotonic. Skip old samples without converting every
    // retained measurement on each repaint, then keep the nearest valid
    // predecessor for interpolation across the left boundary.
    let first = target
        .graph_history
        .partition_point(|sample| sample.timestamp.mono < viewport.start);
    let mut previous = None;
    for sample in target.graph_history.range(..first).rev() {
        if let Some(value) = sample.value_ms(metric) {
            previous = Some((sample.timestamp.mono, value));
            break;
        }
        if sample.break_before {
            break;
        }
    }
    let mut data = Vec::new();
    for sample in target.graph_history.range(first..) {
        // Consume boundaries even when this metric is absent in the first
        // replies of a new session; filtering them out would bridge an outage.
        if sample.break_before {
            if !data.is_empty() && data.last().is_some_and(|(x, _): &(f64, f64)| x.is_finite()) {
                data.push((f64::NAN, f64::NAN));
            }
            previous = None;
        }
        let Some(value) = sample.value_ms(metric) else {
            continue;
        };
        let point = (sample.timestamp.mono, value);
        if let Some(before) = previous {
            // Only genuine crossing segments contribute boundary points. In
            // particular, never extend the latest sample to the live edge.
            if before.0 < viewport.start && point.0 > viewport.start {
                data.push((0.0, interpolate_graph_value(before, point, viewport.start)));
            }
            if before.0 < viewport.end && point.0 > viewport.end {
                data.push((
                    viewport.window.as_secs_f64(),
                    interpolate_graph_value(before, point, viewport.end),
                ));
            }
        }
        if point.0 > viewport.end {
            break;
        }
        if point.0 >= viewport.start {
            data.push((
                point.0.duration_since(viewport.start).as_secs_f64(),
                point.1,
            ));
        }
        previous = Some(point);
    }
    // A boundary beyond the viewport needs no trailing break.
    if data.last().is_some_and(|(x, _)| !x.is_finite()) {
        data.pop();
    }
    (!data.is_empty()).then_some(ChartSeries {
        style: target_style(target_idx),
        data,
    })
}

fn interpolate_graph_value(a: (Instant, f64), b: (Instant, f64), at: Instant) -> f64 {
    let whole = b.0.duration_since(a.0).as_secs_f64();
    if whole == 0.0 {
        return b.1;
    }
    let part = at.duration_since(a.0).as_secs_f64();
    a.1 + (b.1 - a.1) * (part / whole)
}

// Ratatui 0.30.2 casts NaN coordinates to grid positions. Filter breaks
// ourselves so one canvas shape and one backing buffer suffice per target.
impl Shape for ChartSeries {
    fn draw(&self, painter: &mut Painter) {
        let color = self.style.fg.unwrap_or(Color::Reset);
        for &(x, y) in &self.data {
            if x.is_finite() && y.is_finite() {
                if let Some((x, y)) = painter.get_point(x, y) {
                    painter.paint(x, y, color);
                }
            }
        }
        for pair in self.data.windows(2) {
            let [(x1, y1), (x2, y2)] = [pair[0], pair[1]];
            if [x1, y1, x2, y2].iter().all(|value| value.is_finite()) {
                CanvasLine {
                    x1,
                    y1,
                    x2,
                    y2,
                    color,
                }
                .draw(painter);
            }
        }
    }
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

fn chart_y_bounds(series: &[ChartSeries], axis_kind: ChartAxisKind) -> (f64, f64) {
    let mut values = series.iter().flat_map(|series| {
        series
            .data
            .iter()
            .filter(|(_, value)| value.is_finite())
            .map(|(_, value)| match axis_kind {
                ChartAxisKind::NonNegative => (*value).max(0.0),
                ChartAxisKind::Signed => *value,
            })
    });
    let Some(first) = values.next() else {
        return default_y_bounds(axis_kind);
    };

    let (mut min_y, mut max_y) = (first, first);
    for value in values {
        min_y = min_y.min(value);
        max_y = max_y.max(value);
    }

    match axis_kind {
        ChartAxisKind::NonNegative => padded_non_negative_chart_y_bounds(min_y, max_y),
        ChartAxisKind::Signed => padded_signed_chart_y_bounds(min_y, max_y),
    }
}

fn default_y_bounds(axis_kind: ChartAxisKind) -> (f64, f64) {
    match axis_kind {
        ChartAxisKind::NonNegative => (0.0, 10.0),
        ChartAxisKind::Signed => (-1.0, 1.0),
    }
}

fn padded_non_negative_chart_y_bounds(min_y: f64, max_y: f64) -> (f64, f64) {
    let pad = chart_y_padding(min_y, max_y);
    let lower = (min_y - pad).max(0.0);
    let upper = max_y + pad;
    if lower < upper {
        (lower, upper)
    } else {
        (0.0, pad.max(1.0))
    }
}

fn padded_signed_chart_y_bounds(mut min_y: f64, mut max_y: f64) -> (f64, f64) {
    let pad = chart_y_padding(min_y, max_y);
    min_y -= pad;
    max_y += pad;
    if min_y >= max_y {
        (min_y - pad, max_y + pad)
    } else {
        (min_y, max_y)
    }
}

fn chart_y_padding(min_y: f64, max_y: f64) -> f64 {
    const MIN_PADDING_MS: f64 = 0.1;
    let span = max_y - min_y;
    if span <= f64::EPSILON {
        (max_y.abs() * 0.1).max(MIN_PADDING_MS)
    } else {
        (span * 0.1).max(MIN_PADDING_MS)
    }
}

fn viewport_x_bounds(viewport: GraphViewportRange) -> [f64; 2] {
    [0.0, viewport.window.as_secs_f64().max(1.0)]
}

// Use a sample near the viewport end, including when viewing retained history.
// Wall-clock jumps affect labels only; the viewport and series remain monotonic.
fn graph_timestamp_anchor(state: &TuiState, end: Instant) -> Option<ClientTimestamp> {
    state
        .targets
        .iter()
        .filter_map(|target| {
            let after = target
                .graph_history
                .partition_point(|sample| sample.timestamp.mono <= end);
            target.graph_history.get(after.saturating_sub(1))
        })
        .map(|sample| sample.timestamp)
        .min_by_key(|timestamp| {
            if timestamp.mono <= end {
                end.duration_since(timestamp.mono)
            } else {
                timestamp.mono.duration_since(end)
            }
        })
}

fn graph_wall_time(anchor: ClientTimestamp, tick: Instant) -> Option<SystemTime> {
    if tick >= anchor.mono {
        anchor.wall.checked_add(tick.duration_since(anchor.mono))
    } else {
        anchor.wall.checked_sub(anchor.mono.duration_since(tick))
    }
}

fn graph_local_time(wall: SystemTime) -> Option<DateTime<chrono::FixedOffset>> {
    let nanos = match wall.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => duration.as_nanos() as i128,
        Err(error) => -(error.duration().as_nanos() as i128),
    };
    let seconds = i64::try_from(nanos.div_euclid(1_000_000_000)).ok()?;
    let subsecond = nanos.rem_euclid(1_000_000_000) as u32;
    let utc = DateTime::<Utc>::from_timestamp(seconds, subsecond)?;
    // Chrono also falls back to UTC when the system timezone cannot be found.
    Some(
        Local
            .timestamp_opt(seconds, subsecond)
            .single()
            .map(|local| local.fixed_offset())
            .unwrap_or_else(|| utc.fixed_offset()),
    )
}

fn viewport_x_axis_labels(
    state: &TuiState,
    viewport: GraphViewportRange,
    width: u16,
) -> Vec<Span<'static>> {
    let Some(anchor) = graph_timestamp_anchor(state, viewport.end) else {
        return vec![Span::raw(ABSENT), Span::raw(ABSENT)];
    };
    let local_at = |tick| graph_wall_time(anchor, tick).and_then(graph_local_time);
    let date_boundary = local_at(viewport.start)
        .zip(local_at(viewport.end))
        .is_some_and(|(start, end)| start.date_naive() != end.date_naive());
    // Reserve space for the borders/y axis and a gap between labels. Use the
    // longest possible label for this window so narrow charts remain readable.
    let fractional = viewport.window < Duration::from_secs(8);
    let label_width = 8 + if fractional { 2 } else { 0 } + if date_boundary { 6 } else { 0 };
    let count = (usize::from(width.saturating_sub(14)) / (label_width + 3)).clamp(2, 5);
    let step = viewport.window / (count - 1) as u32;
    let fractional = step < Duration::from_secs(2);
    (0..count)
        .map(|idx| {
            let tick = if idx == count - 1 {
                Some(viewport.end)
            } else {
                viewport.start.checked_add(step * idx as u32)
            };
            let label = tick.and_then(local_at).map(|local| {
                let mut label = local.format("%H:%M:%S").to_string();
                if fractional {
                    label.push_str(&format!(".{}", local.timestamp_subsec_millis() / 100));
                }
                if date_boundary && (idx == 0 || idx == count - 1) {
                    label = format!("{} {label}", local.format("%m-%d"));
                }
                label
            });
            Span::raw(label.unwrap_or_else(|| ABSENT.to_owned()))
        })
        .collect()
}

fn y_axis_label_count(height: u16) -> usize {
    let inner = height.saturating_sub(2);
    if inner >= 14 {
        7
    } else if inner >= 9 {
        5
    } else if inner >= 5 {
        3
    } else {
        2
    }
}

fn y_axis_labels(min_y: f64, max_y: f64, label_count: usize) -> Vec<Span<'static>> {
    let label_count = label_count.max(2);
    let step = (max_y - min_y) / (label_count - 1) as f64;
    (0..label_count)
        .map(|idx| Span::raw(format_axis_time_ms(min_y + step * idx as f64)))
        .collect()
}

fn format_axis_time_ms(value_ms: f64) -> String {
    let value_ms = if value_ms.abs() < 0.000_5 {
        0.0
    } else {
        value_ms
    };
    let sign = if value_ms < 0.0 { "-" } else { "" };
    let abs_ms = value_ms.abs();
    if abs_ms < 1.0 {
        let us = abs_ms * 1_000.0;
        if us < 10.0 {
            format!("{sign}{us:.1}µs")
        } else {
            format!("{sign}{us:.0}µs")
        }
    } else if abs_ms < 1_000.0 {
        if abs_ms < 10.0 {
            format!("{sign}{abs_ms:.2}ms")
        } else if abs_ms < 100.0 {
            format!("{sign}{abs_ms:.1}ms")
        } else {
            format!("{sign}{abs_ms:.0}ms")
        }
    } else {
        let secs = abs_ms / 1_000.0;
        if secs < 10.0 {
            format!("{sign}{secs:.2}s")
        } else if secs < 100.0 {
            format!("{sign}{secs:.1}s")
        } else {
            format!("{sign}{secs:.0}s")
        }
    }
}

fn graph_viewport_status(state: &TuiState) -> String {
    match state.graph_viewport.mode {
        GraphViewportMode::Follow => "live".to_owned(),
        GraphViewportMode::Historical { end } => {
            let now = Instant::now();
            let live_end = state.newest_graph_sample_time().unwrap_or(now).max(now);
            format!(
                "history -{}",
                format_span(live_end.saturating_duration_since(end))
            )
        }
    }
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
        format_optional_ns_i128(stats.min_ns),
        format_ns_f64(stats.mean_ns),
        format_optional_ns_i128(stats.max_ns),
        format_ns_f64(stats.stddev_ns())
    )));
}

fn format_negotiated(negotiated: &NegotiationResult) -> String {
    let params = &negotiated.accepted;
    let duration = params.duration.map_or_else(
        || "-".to_owned(),
        |duration| format_optional_ns_i128(Some(duration.as_nanos() as i128)),
    );
    let changes = if negotiated.changes.is_empty() {
        "none".to_owned()
    } else {
        negotiated.changes.len().to_string()
    };
    format!(
        "duration={} interval={} length={} clock={:?} timestamps={:?} stats={:?} changes={}",
        duration,
        format_optional_ns_i128(Some(params.interval.as_nanos() as i128)),
        params.length,
        params.clock,
        params.stamp_at,
        params.received_stats,
        changes
    )
}

fn format_age(age: Duration) -> String {
    match age.as_secs() {
        0 => "<1s".to_owned(),
        seconds @ 1..=59 => format!("{seconds}s"),
        seconds => format!("{}m", seconds / 60),
    }
}

/// Format a wall-clock span shown in the TUI's headers, config lines, and
/// graph window labels.
///
/// Spans are read at a glance rather than compared numerically, so they are
/// deliberately coarser than the shared scalar policy: a zero span is `0s`,
/// whole seconds carry one decimal, and a span of a minute or more is written
/// as minutes and seconds. Below one second there is nothing to gain from a
/// second spelling, so the shared scalar vocabulary is used directly.
fn format_span(value: Duration) -> String {
    if value.is_zero() {
        return "0s".to_owned();
    }

    let nanos = value.as_nanos();
    if nanos < 1_000_000_000 {
        return format_duration(value);
    }

    let secs = value.as_secs();

    if secs < 60 {
        return format!("{:.1}s", nanos as f64 / 1_000_000_000.0);
    }

    let seconds = secs % 60;
    let minutes = (secs / 60) % 60;
    let hours = (secs / 3600) % 24;
    let days = secs / 86_400;

    if days != 0 {
        if hours == 0 {
            format!("{days}d")
        } else {
            format!("{days}d{hours:02}h")
        }
    } else if hours != 0 {
        if minutes == 0 {
            format!("{hours}h")
        } else {
            format!("{hours}h{minutes:02}m")
        }
    } else if seconds == 0 {
        format!("{minutes}m")
    } else {
        format!("{minutes}m{seconds:02}s")
    }
}

fn format_optional_hex(value: Option<u64>) -> String {
    value
        .map(|value| format!("0x{value:x}"))
        .unwrap_or_else(|| ABSENT.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_failure_and_replacement_reset_stats_without_client_events() {
        use irtt_client::managed::{
            ManagedClient, ManagedClientConfig, ManagedCompletionPolicy, ManagedTargetConfig,
        };

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let desired = vec![ManagedTargetConfig::new("monitor", "127.0.0.1:99999")];
            let (task, handle) = ManagedClient::task(
                ManagedClientConfig {
                    completion: ManagedCompletionPolicy::ExplicitStop,
                    ..Default::default()
                },
                desired.clone(),
            )
            .unwrap();
            let mut status = handle.subscribe_status();
            let instance = status.borrow().targets[0].target.clone();
            let mut state = TuiState::with_target_labels(
                TuiConfig {
                    interval: Duration::from_secs(1),
                    duration: None,
                    timeout: Duration::from_secs(4),
                    target_probes: None,
                    pacing: GroupPacingArg::Staggered,
                },
                ["monitor".to_owned()],
            );
            state.process_target_event(
                &instance,
                &ClientEvent::EchoSent {
                    seq: 42,
                    remote: "127.0.0.1:2112".parse().unwrap(),
                    scheduled_at: None,
                    sent_at: ClientTimestamp::now(),
                    bytes: 60,
                    send_call: Duration::ZERO,
                    timer_error: None,
                },
            );
            assert_eq!(state.targets[0].stats.snapshot().packets.packets_sent, 1);
            let run = tokio::spawn(task);
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    status.changed().await.unwrap();
                    if status.borrow().targets[0].outcome.is_some() {
                        break;
                    }
                }
                // Never consume broadcast events: status owns the failure evidence.
                let snapshot = status.borrow_and_update().clone();
                let outcome = snapshot.targets[0].outcome.as_ref().unwrap();
                state.process_managed_status(&snapshot);
                assert_eq!(state.targets[0].status, TargetStatus::Failed);
                assert!(state.targets[0].last_warning.is_some());
                let negotiation = NegotiationResult {
                    accepted: irtt_client::AcceptedSessionParameters {
                        duration: None,
                        interval: Duration::from_secs(1),
                        length: 60,
                        received_stats: irtt_proto::ReceivedStats::Both,
                        stamp_at: irtt_proto::StampAt::Both,
                        clock: irtt_proto::Clock::Both,
                        dscp: 0,
                        server_fill: None,
                    },
                    peer_params: irtt_proto::Params::default(),
                    changes: vec![],
                };
                let remote = "127.0.0.1:2112".parse().unwrap();
                let global_status = state.status;
                for event in [
                    ClientEvent::SessionStarted(irtt_client::SessionStarted {
                        remote,
                        token: 123,
                        negotiation: negotiation.clone(),
                        at: ClientTimestamp::now(),
                    }),
                    ClientEvent::NoTestCompleted(irtt_client::NoTestCompleted {
                        remote,
                        negotiation: negotiation.clone(),
                        at: ClientTimestamp::now(),
                    }),
                    ClientEvent::SessionClosed {
                        remote,
                        token: 123,
                        at: ClientTimestamp::now(),
                    },
                ] {
                    state.process_target_event(&instance, &event);
                    assert_eq!(state.targets[0].remote.as_deref(), Some("127.0.0.1:2112"));
                    assert_eq!(state.targets[0].session.as_deref(), Some("0x7b"));
                    assert_eq!(state.targets[0].negotiated.as_ref(), Some(&negotiation));
                    assert_eq!(state.targets[0].status, TargetStatus::Failed);
                    assert_eq!(state.status, global_status);
                    assert_eq!(state.targets[0].stats.snapshot().packets.packets_sent, 1);
                }
                let event_count = state.recent_events.len();
                state.process_target_outcome(outcome);
                assert_eq!(
                    state.recent_events.len(),
                    event_count,
                    "no duplicate ending"
                );

                let ack = handle.update_targets(desired).unwrap().await.unwrap();
                // Opening events may arrive before their update receipt.
                let mut late_state =
                    TuiState::with_target_labels(state.config.clone(), ["monitor".to_owned()]);
                late_state.process_target_opening(&ack.status.targets[0].target);
                late_state.process_managed_status(&ack.status);
                assert_eq!(late_state.targets[0].status, TargetStatus::Opening);
                assert!(late_state.targets[0].last_warning.is_some());

                state.process_managed_status(&ack.status);
                assert_eq!(state.targets[0].status, TargetStatus::Opening);
                assert_eq!(state.targets[0].stats.snapshot().packets.packets_sent, 0);
                assert!(
                    state.targets[0].last_warning.is_some(),
                    "failure evidence survives reprovisioning"
                );
                loop {
                    status.changed().await.unwrap();
                    if status.borrow().targets[0].outcome.is_some() {
                        break;
                    }
                }
                state.process_managed_status(&status.borrow_and_update());
                assert_eq!(state.targets[0].status, TargetStatus::Failed);
                assert_eq!(state.targets[0].stats.snapshot().packets.packets_sent, 0);
                // An older receipt must not revive a durably failed generation.
                state.process_managed_status(&ack.status);
                assert_eq!(state.targets[0].status, TargetStatus::Failed);
                handle.stop().await;
                run.await.unwrap();
            })
            .await
            .unwrap();
        });
    }

    #[test]
    fn reconnect_history_clips_within_sessions_and_leaves_the_outage_empty() {
        let start = Instant::now();
        let mut target = TuiTargetState::new("monitor".into(), stats_config(true));
        for (generation, seconds) in [(0, 0), (0, 2), (1, 6), (1, 8)] {
            let raw = Duration::from_millis(seconds + 1);
            target.push_graph_sample(LatestSample {
                generation,
                timestamp: ClientTimestamp {
                    mono: start + Duration::from_secs(seconds),
                    wall: SystemTime::UNIX_EPOCH + Duration::from_secs(seconds),
                },
                seq: 0,
                rtt: RttSample {
                    raw,
                    adjusted: (seconds != 6)
                        .then_some(SignedDuration::from_nanos(-(raw.as_nanos() as i128))),
                    effective: SignedDuration::from_nanos(raw.as_nanos() as i128),
                },
                one_way: None,
                server_processing: None,
            });
        }
        let series = |left, right| {
            target_metric_series(
                &target,
                0,
                GraphViewportRange {
                    start: start + Duration::from_secs(left),
                    end: start + Duration::from_secs(right),
                    window: Duration::from_secs(right - left),
                },
                GraphMetric::RawRtt,
            )
        };
        let clipped = series(1, 7).unwrap();
        assert_eq!(clipped.data.len(), 5);
        assert_eq!(&clipped.data[..2], [(0.0, 2.0), (1.0, 3.0)]);
        assert!(clipped.data[2].0.is_nan() && clipped.data[2].1.is_nan());
        assert_eq!(&clipped.data[3..], [(5.0, 7.0), (6.0, 8.0)]);
        // Check the pinned renderer: one guarded shape must render exactly
        // like independent finite datasets, including their isolated points.
        use ratatui::{
            buffer::Buffer,
            widgets::{Dataset, GraphType, Widget},
        };
        let area = Rect::new(0, 0, 50, 20);
        let mut actual = Buffer::empty(area);
        Canvas::default()
            .marker(symbols::Marker::Braille)
            .x_bounds([0.0, 6.0])
            .y_bounds([0.0, 10.0])
            .paint(|ctx| ctx.draw(&clipped))
            .render(area, &mut actual);
        let datasets = [&clipped.data[..2], &clipped.data[3..]].map(|data| {
            Dataset::default()
                .marker(symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .style(clipped.style)
                .data(data)
        });
        let mut expected = Buffer::empty(area);
        Chart::new(datasets.to_vec())
            .x_axis(Axis::default().bounds([0.0, 6.0]))
            .y_axis(Axis::default().bounds([0.0, 10.0]))
            .render(area, &mut expected);
        assert_eq!(
            actual, expected,
            "no bogus pixels or lines through NaN breaks"
        );
        let finite = ChartSeries {
            style: clipped.style,
            data: clipped
                .data
                .iter()
                .copied()
                .filter(|(x, _)| x.is_finite())
                .collect(),
        };
        assert_eq!(
            chart_y_bounds(std::slice::from_ref(&clipped), ChartAxisKind::NonNegative),
            chart_y_bounds(&[finite], ChartAxisKind::NonNegative)
        );
        assert!(series(3, 5).is_none(), "no interpolation across sessions");
        assert!(series(9, 10).is_none(), "no extrapolation to the live edge");
        let adjusted = |left, right| {
            target_metric_series(
                &target,
                0,
                GraphViewportRange {
                    start: start + Duration::from_secs(left),
                    end: start + Duration::from_secs(right),
                    window: Duration::from_secs(right - left),
                },
                GraphMetric::AdjustedRtt,
            )
        };
        let missing = adjusted(1, 9).unwrap();
        assert_eq!(&missing.data[..2], [(0.0, -2.0), (1.0, -3.0)]);
        assert!(missing.data[2].0.is_nan());
        assert_eq!(&missing.data[3..], [(7.0, -9.0)]);
        assert!(
            adjusted(3, 7).is_none(),
            "absent metric cannot bridge sessions"
        );
        assert_eq!(
            adjusted(7, 9).unwrap().data,
            [(1.0, -9.0)],
            "predecessor search stops at an absent boundary metric"
        );
        let template = target.last_sample.unwrap();
        target.graph_history.clear();
        for generation in 0..10_000 {
            target.push_graph_sample(LatestSample {
                generation,
                timestamp: ClientTimestamp {
                    mono: start + Duration::from_secs(generation),
                    ..template.timestamp
                },
                ..template
            });
        }
        let visible = target_metric_series(
            &target,
            0,
            GraphViewportRange {
                start: start + Duration::from_secs(9_994),
                end: start + Duration::from_secs(9_999),
                window: Duration::from_secs(5),
            },
            GraphMetric::RawRtt,
        )
        .unwrap();
        assert_eq!(
            visible.data.len(),
            11,
            "six visible generations in one buffer"
        );
    }
}
