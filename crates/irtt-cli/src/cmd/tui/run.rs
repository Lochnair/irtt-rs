use std::{
    io,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use irtt_stats::StatsCollector;

use crate::{
    cmd::tui::args::TuiArgs,
    shared::client::{is_shutdown_requested, ClientSession},
};

use super::ui::{should_render, TuiConfig, TuiState, TuiStatus, TuiTerminal};

fn stats_config(continuous: bool) -> irtt_stats::StatsConfig {
    if continuous {
        irtt_stats::StatsConfig::continuous()
    } else {
        irtt_stats::StatsConfig::finite()
    }
}

const RENDER_INTERVAL: Duration = Duration::from_millis(250);
const TUI_WAIT_SLICE: Duration = Duration::from_millis(20);
const IDLE_SLEEP: Duration = Duration::from_millis(5);

pub fn run_tui(
    args: TuiArgs,
    shutdown_requested: &AtomicBool,
) -> Result<(), Box<dyn std::error::Error>> {
    let continuous = args.is_continuous();
    let mut terminal = TuiTerminal::enter()?;
    let mut state = TuiState::new(TuiConfig::from_args(&args));
    let mut stats = StatsCollector::new(stats_config(continuous));
    let mut next_render = Instant::now();

    if is_shutdown_requested(shutdown_requested) {
        return Ok(());
    }

    state.set_status(TuiStatus::Opening);
    render_if_due(&mut terminal, &state, &stats, &mut next_render, true)?;

    let mut session = match ClientSession::connect(args.to_client_config(), continuous) {
        Ok(session) => session,
        Err(err) => {
            state.set_error(err.to_string());
            render_if_due(&mut terminal, &state, &stats, &mut next_render, true)?;
            return Err(Box::new(err));
        }
    };

    if is_shutdown_requested(shutdown_requested) {
        return Ok(());
    }

    let events = session.open()?;
    state.process_events(&events, &mut stats);
    render_if_due(&mut terminal, &state, &stats, &mut next_render, true)?;

    let mut interrupted = false;
    // Keep this in lockstep with run_stream: send due probes, drain available
    // replies, poll timeouts, sleep toward the next absolute send deadline,
    // then perform the same final drain, timeout poll, and close sequence.
    while session.should_continue(shutdown_requested) {
        if handle_input(&mut state, shutdown_requested)? {
            render_if_due(&mut terminal, &state, &stats, &mut next_render, true)?;
        }
        if state.quit_requested {
            interrupted = true;
            break;
        }

        let events = session.step(shutdown_requested)?;
        state.process_events(&events, &mut stats);

        if is_shutdown_requested(shutdown_requested) {
            interrupted = true;
            break;
        }

        render_if_due(&mut terminal, &state, &stats, &mut next_render, false)?;
        wait_for_tui_activity(
            session.next_send_deadline(),
            &mut next_render,
            &mut state,
            &stats,
            &mut terminal,
            shutdown_requested,
        )?;
    }
    interrupted |= is_shutdown_requested(shutdown_requested);

    if interrupted {
        state.set_status(TuiStatus::Interrupted);
        render_if_due(&mut terminal, &state, &stats, &mut next_render, true)?;
    }

    if session.should_drain_final(interrupted) {
        state.set_status(TuiStatus::Draining);
        let mut drain_render = Instant::now();
        session.drain_final(|events| {
            state.process_events(events, &mut stats);
            let _ = render_if_due(&mut terminal, &state, &stats, &mut drain_render, false);
        })?;
        render_if_due(&mut terminal, &state, &stats, &mut next_render, true)?;
    }

    let events = session.poll_timeouts()?;
    state.process_events(&events, &mut stats);

    state.set_status(TuiStatus::Closing);
    render_if_due(&mut terminal, &state, &stats, &mut next_render, true)?;

    let events = session.close()?;
    state.process_events(&events, &mut stats);
    state.set_status(TuiStatus::Complete);
    render_if_due(&mut terminal, &state, &stats, &mut next_render, true)?;
    Ok(())
}

fn handle_input(state: &mut TuiState, shutdown_requested: &AtomicBool) -> io::Result<bool> {
    let mut force_render = false;
    while event::poll(Duration::ZERO)? {
        let Event::Key(key) = event::read()? else {
            continue;
        };
        if key.kind == KeyEventKind::Release {
            continue;
        }
        match key.code {
            KeyCode::Char('q') => {
                state.quit_requested = true;
                shutdown_requested.store(true, Ordering::Relaxed);
                force_render = true;
            }
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                state.quit_requested = true;
                shutdown_requested.store(true, Ordering::Relaxed);
                force_render = true;
            }
            KeyCode::Char('r') => {
                state.clear_visible_history();
                force_render = true;
            }
            KeyCode::Char('p') => {
                state.toggle_pause();
                force_render = true;
            }
            KeyCode::Char('g') => {
                state.cycle_graph_mode();
                force_render = true;
            }
            KeyCode::Char('f') => {
                state.toggle_full_graph();
                force_render = true;
            }
            _ => {}
        }
    }
    Ok(force_render)
}

fn render_if_due(
    terminal: &mut TuiTerminal,
    state: &TuiState,
    stats: &StatsCollector,
    next_render: &mut Instant,
    force: bool,
) -> io::Result<()> {
    let now = Instant::now();
    if !should_render(now, *next_render, state.paused, force) {
        return Ok(());
    }
    terminal.draw(state, &stats.snapshot())?;
    *next_render = now + RENDER_INTERVAL;
    Ok(())
}

fn wait_for_tui_activity(
    next_send_deadline: Option<Instant>,
    next_render: &mut Instant,
    state: &mut TuiState,
    stats: &StatsCollector,
    terminal: &mut TuiTerminal,
    shutdown_requested: &AtomicBool,
) -> io::Result<()> {
    let wait_for = tui_wait_duration(next_send_deadline, *next_render, state.paused);
    if wait_for.is_zero() || !event::poll(wait_for)? {
        return Ok(());
    }

    if handle_input(state, shutdown_requested)? {
        render_if_due(terminal, state, stats, next_render, true)?;
    }
    Ok(())
}

fn tui_wait_duration(
    next_send_deadline: Option<Instant>,
    next_render: Instant,
    paused: bool,
) -> Duration {
    let now = Instant::now();
    let send_wait = next_send_deadline
        .map(|deadline| deadline.saturating_duration_since(now))
        .unwrap_or(IDLE_SLEEP);
    let render_wait = if paused {
        send_wait
    } else {
        next_render.saturating_duration_since(now)
    };
    send_wait.min(render_wait).min(TUI_WAIT_SLICE)
}
