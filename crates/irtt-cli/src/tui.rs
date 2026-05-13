use std::{
    io::{self, Stdout},
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use irtt_cli::CliArgs;
use irtt_client::{Client, ClientEvent};
use irtt_stats::{Snapshot, StatsCollector, TimeStats};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
    Frame, Terminal,
};

use crate::{
    final_drain_duration, is_shutdown_requested, open_event, should_continue_run,
    should_drain_final, should_send_probe, sleep_until_next_send, stats_config, IDLE_SLEEP,
    RECV_BUDGET,
};

const RENDER_INTERVAL: Duration = Duration::from_millis(250);

pub fn run_tui(
    args: CliArgs,
    shutdown_requested: &AtomicBool,
) -> Result<(), Box<dyn std::error::Error>> {
    let continuous = args.is_continuous();
    let mut terminal = TuiTerminal::enter()?;
    let mut state = TuiState::default();
    let mut stats = StatsCollector::new(stats_config(continuous));
    let mut next_render = Instant::now();

    if is_shutdown_requested(shutdown_requested) {
        return Ok(());
    }

    let mut client = Client::connect(args.to_client_config())?;

    if is_shutdown_requested(shutdown_requested) {
        return Ok(());
    }

    let open = client.open()?;
    state.process_event(open_event(&open), &mut stats);
    render_if_due(&mut terminal, &state, &stats, &mut next_render, true)?;

    let mut interrupted = false;
    while should_continue_run(continuous, client.next_send_deadline(), shutdown_requested) {
        if handle_input(shutdown_requested)? {
            interrupted = true;
            break;
        }

        if should_send_probe(client.next_send_deadline(), shutdown_requested) {
            let events = client.send_probe()?;
            state.process_events(&events, &mut stats);
        }

        let events = client.recv_available(RECV_BUDGET)?;
        state.process_events(&events, &mut stats);

        let events = client.poll_timeouts()?;
        state.process_events(&events, &mut stats);

        if is_shutdown_requested(shutdown_requested) {
            interrupted = true;
            break;
        }

        render_if_due(&mut terminal, &state, &stats, &mut next_render, false)?;
        sleep_until_next_send(client.next_send_deadline());
    }
    interrupted |= is_shutdown_requested(shutdown_requested);

    if interrupted {
        state.status = TuiStatus::InterruptedClosing;
        render_if_due(&mut terminal, &state, &stats, &mut next_render, true)?;
    }

    if should_drain_final(continuous, interrupted) {
        drain_final_replies(&mut client, &mut state, &mut stats, &mut terminal)?;
    }

    let events = client.poll_timeouts()?;
    state.process_events(&events, &mut stats);

    let events = client.close()?;
    state.process_events(&events, &mut stats);
    state.status = TuiStatus::Complete;
    render_if_due(&mut terminal, &state, &stats, &mut next_render, true)?;
    Ok(())
}

fn drain_final_replies(
    client: &mut Client,
    state: &mut TuiState,
    stats: &mut StatsCollector,
    terminal: &mut TuiTerminal,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + final_drain_duration(client.probe_timeout());
    let mut next_render = Instant::now();
    loop {
        let mut received = false;

        let events = client.recv_available(RECV_BUDGET)?;
        received |= !events.is_empty();
        state.process_events(&events, stats);

        let events = client.poll_timeouts()?;
        received |= !events.is_empty();
        state.process_events(&events, stats);

        render_if_due(terminal, state, stats, &mut next_render, false)?;

        if client.is_run_complete() || Instant::now() >= deadline {
            break;
        }

        if !received {
            std::thread::sleep(IDLE_SLEEP);
        }
    }
    render_if_due(terminal, state, stats, &mut next_render, true)?;
    Ok(())
}

fn handle_input(shutdown_requested: &AtomicBool) -> io::Result<bool> {
    let mut quit = false;
    while event::poll(Duration::ZERO)? {
        let Event::Key(key) = event::read()? else {
            continue;
        };
        if key.kind == KeyEventKind::Release {
            continue;
        }
        match key.code {
            KeyCode::Char('q') => quit = true,
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                shutdown_requested.store(true, Ordering::Relaxed);
                quit = true;
            }
            _ => {}
        }
    }
    Ok(quit)
}

fn render_if_due(
    terminal: &mut TuiTerminal,
    state: &TuiState,
    stats: &StatsCollector,
    next_render: &mut Instant,
    force: bool,
) -> io::Result<()> {
    let now = Instant::now();
    if !force && now < *next_render {
        return Ok(());
    }
    terminal.draw(state, &stats.snapshot())?;
    *next_render = now + RENDER_INTERVAL;
    Ok(())
}

struct TuiTerminal {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl TuiTerminal {
    fn enter() -> io::Result<Self> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        if let Err(err) = execute!(stdout, EnterAlternateScreen) {
            let _ = disable_raw_mode();
            return Err(err);
        }

        let backend = CrosstermBackend::new(stdout);
        match Terminal::new(backend) {
            Ok(terminal) => Ok(Self { terminal }),
            Err(err) => {
                let _ = disable_raw_mode();
                let mut stdout = io::stdout();
                let _ = execute!(stdout, LeaveAlternateScreen);
                Err(err)
            }
        }
    }

    fn draw(&mut self, state: &TuiState, snapshot: &Snapshot) -> io::Result<()> {
        self.terminal
            .draw(|frame| draw_dashboard(frame, state, snapshot))
            .map(|_| ())
    }
}

impl Drop for TuiTerminal {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen);
        let _ = self.terminal.show_cursor();
    }
}

#[derive(Debug, Default)]
struct TuiState {
    remote: Option<String>,
    session: Option<String>,
    status: TuiStatus,
}

impl TuiState {
    fn process_events(&mut self, events: &[ClientEvent], stats: &mut StatsCollector) {
        for event in events {
            self.process_event(event, stats);
        }
    }

    fn process_event(&mut self, event: &ClientEvent, stats: &mut StatsCollector) {
        stats.process(event);
        match event {
            ClientEvent::SessionStarted { remote, token, .. } => {
                self.remote = Some(remote.to_string());
                self.session = Some(format!("{token:#x}"));
                self.status = TuiStatus::Running;
            }
            ClientEvent::NoTestCompleted { remote, .. } => {
                self.remote = Some(remote.to_string());
                self.status = TuiStatus::Complete;
            }
            ClientEvent::SessionClosed { .. } => {
                self.status = TuiStatus::Complete;
            }
            _ => {}
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
enum TuiStatus {
    #[default]
    Running,
    InterruptedClosing,
    Complete,
}

impl TuiStatus {
    fn label(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::InterruptedClosing => "interrupted, closing...",
            Self::Complete => "complete",
        }
    }
}

fn draw_dashboard(frame: &mut Frame<'_>, state: &TuiState, snapshot: &Snapshot) {
    let area = frame.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),
            Constraint::Length(7),
            Constraint::Min(8),
            Constraint::Length(3),
        ])
        .split(area);

    frame.render_widget(header(state), chunks[0]);
    frame.render_widget(packet_panel(snapshot), chunks[1]);
    frame.render_widget(timing_panel(snapshot), chunks[2]);
    frame.render_widget(status_line(state), chunks[3]);
}

fn header(state: &TuiState) -> Paragraph<'_> {
    let remote = state.remote.as_deref().unwrap_or("-");
    let session = state.session.as_deref().unwrap_or("-");
    Paragraph::new(vec![
        Line::from(Span::styled(
            "irtt-rs",
            ratatui::style::Style::default().add_modifier(ratatui::style::Modifier::BOLD),
        )),
        Line::from(format!("remote: {remote}")),
        Line::from(format!("session: {session}")),
    ])
    .block(Block::default().borders(Borders::ALL))
}

fn packet_panel(snapshot: &Snapshot) -> Paragraph<'_> {
    let packets = snapshot.packets;
    let loss = snapshot.loss;
    Paragraph::new(vec![
        Line::from(format!("sent:     {}", packets.packets_sent)),
        Line::from(format!("received: {}", packets.packets_received)),
        Line::from(format!("unique:   {}", packets.unique_replies)),
        Line::from(format!("lost:     {}", loss.lost_packets)),
        Line::from(format!("loss:     {:.2}%", loss.packet_loss_percent)),
    ])
    .block(Block::default().title("packets").borders(Borders::ALL))
}

fn timing_panel(snapshot: &Snapshot) -> Paragraph<'_> {
    let mut lines = vec![Line::from(format!(
        "{:<16} {:>8} {:>10} {:>10} {:>10} {:>10}",
        "metric", "count", "min", "mean", "max", "stddev"
    ))];
    push_time_line(&mut lines, "RTT", &snapshot.rtt.primary);
    push_time_line(&mut lines, "IPDV/jitter", &snapshot.ipdv.round_trip);
    push_mean_line(&mut lines, "send delay", &snapshot.one_way_delay.send_delay);
    push_mean_line(
        &mut lines,
        "receive delay",
        &snapshot.one_way_delay.receive_delay,
    );

    Paragraph::new(lines).block(Block::default().title("timing").borders(Borders::ALL))
}

fn status_line(state: &TuiState) -> Paragraph<'_> {
    Paragraph::new(format!("{} | press q to quit", state.status.label()))
        .block(Block::default().borders(Borders::ALL))
}

fn push_time_line(lines: &mut Vec<Line<'_>>, label: &str, stats: &TimeStats) {
    lines.push(Line::from(format!(
        "{label:<16} {:>8} {:>10} {:>10} {:>10} {:>10}",
        stats.count,
        format_ns_i128(stats.min_ns),
        format_optional_mean(stats),
        format_ns_i128(stats.max_ns),
        format_optional_stddev(stats)
    )));
}

fn push_mean_line(lines: &mut Vec<Line<'_>>, label: &str, stats: &TimeStats) {
    lines.push(Line::from(format!(
        "{label:<16} {:>8} {:>10} {:>10} {:>10} {:>10}",
        stats.count,
        "-",
        format_optional_mean(stats),
        "-",
        "-"
    )));
}

fn format_optional_mean(stats: &TimeStats) -> String {
    if stats.count == 0 {
        "-".to_owned()
    } else {
        format_ns_f64(stats.mean_ns)
    }
}

fn format_optional_stddev(stats: &TimeStats) -> String {
    if stats.count == 0 {
        "-".to_owned()
    } else {
        format_ns_f64(stats.stddev_ns())
    }
}

fn format_ns_i128(value: Option<i128>) -> String {
    value
        .map(|value| format_ns_f64(value as f64))
        .unwrap_or_else(|| "-".to_owned())
}

fn format_ns_f64(value: f64) -> String {
    let sign = if value.is_sign_negative() { "-" } else { "" };
    let value = value.abs();
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
