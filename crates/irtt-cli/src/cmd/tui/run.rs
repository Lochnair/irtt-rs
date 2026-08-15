use std::{
    collections::HashSet,
    io,
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::{Duration, Instant},
};

use super::ui::{should_render, TuiConfig, TuiState, TuiStatus, TuiTerminal};
use crate::{
    cmd::tui::args::TuiArgs,
    shared::client::{
        is_shutdown_requested,
        session::{
            drain_managed_events, peer_close_run_error, request_managed_stop_for_peer_close,
            request_managed_stop_once, ManagedDrainState,
        },
    },
};
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use irtt_client::managed::{
    BlockingManagedClient, ManagedEndReason, ManagedEvent, ManagedEventSubscription,
    ManagedEventTryRecvError, TargetInstance,
};

const RENDER_INTERVAL: Duration = Duration::from_millis(250);
const TUI_WAIT_SLICE: Duration = Duration::from_millis(20);
const INPUT_EVENT_WORK_BUDGET: usize = 128;

pub fn run_tui(
    args: TuiArgs,
    shutdown_requested: &AtomicBool,
) -> Result<(), Box<dyn std::error::Error>> {
    let setup = args
        .prepare()
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    let continuous = args.is_continuous();
    let mut terminal = TuiTerminal::enter()?;
    let mut state = TuiState::with_target_labels(
        TuiConfig::from_args(&args, &setup),
        setup.targets.iter().map(|target| target.label.clone()),
    );
    let mut next_render = Instant::now();
    if is_shutdown_requested(shutdown_requested) {
        return Ok(());
    }
    state.set_status(TuiStatus::Opening);
    render_if_due(&mut terminal, &state, &mut next_render, true)?;
    let (owner, mut events) = match BlockingManagedClient::start_with_subscription(
        setup.managed_config(),
        setup.managed_targets(),
    ) {
        Ok(value) => value,
        Err(error) => {
            state.set_error(error.to_string());
            render_if_due(&mut terminal, &state, &mut next_render, true)?;
            return Err(Box::new(error));
        }
    };
    let handle = owner.handle();
    let mut interrupted = false;
    let mut stop_requested = false;
    let mut terminal_targets = HashSet::new();
    let mut dropped_events = 0;
    let mut subscription_closed = false;
    loop {
        if is_shutdown_requested(shutdown_requested) {
            interrupted = true;
            if request_managed_stop_once(&mut stop_requested) {
                drop(handle.stop());
            }
        }
        if handle_input(&mut state, shutdown_requested)? {
            render_if_due(&mut terminal, &state, &mut next_render, true)?;
        }
        if state.quit_requested {
            interrupted = true;
            if request_managed_stop_once(&mut stop_requested) {
                drop(handle.stop());
            }
        }
        if request_managed_stop_for_peer_close(
            continuous,
            interrupted,
            handle.status().peer_closed_target_outcomes,
            &mut stop_requested,
        ) {
            drop(handle.stop());
        }
        let drain_state = drain_tui_events(
            &mut events,
            &mut state,
            &mut terminal_targets,
            &mut dropped_events,
        );
        match drain_state {
            ManagedDrainState::Empty => {
                thread::sleep(managed_tui_wait_duration(&next_render, state.paused));
            }
            ManagedDrainState::Closed => subscription_closed = true,
            ManagedDrainState::BudgetExhausted => {}
        }
        if handle.status().final_outcome.is_some() || subscription_closed {
            break;
        }
        render_if_due(&mut terminal, &state, &mut next_render, false)?;
    }
    if interrupted {
        state.set_status(TuiStatus::Interrupted);
        render_if_due(&mut terminal, &state, &mut next_render, true)?;
    }
    state.set_status(TuiStatus::Closing);
    render_if_due(&mut terminal, &state, &mut next_render, true)?;
    let outcome = owner.join()?;
    drain_final_tui_events(
        &mut events,
        &mut state,
        &mut terminal_targets,
        &mut dropped_events,
    );
    state.mark_dropped_managed_events(dropped_events);
    if outcome.discarded_target_outcomes != 0 {
        state.set_run_error(format!(
            "{} final target outcomes were discarded",
            outcome.discarded_target_outcomes
        ));
    }
    for target in outcome.recent_target_outcomes.iter() {
        if terminal_targets.insert(target.target.clone()) {
            state.process_target_outcome(target);
        }
    }
    interrupted |= is_shutdown_requested(shutdown_requested);
    let error = match &outcome.end_reason {
        ManagedEndReason::DriverFailed(failure) => {
            Some(format!("managed driver failed: {failure}"))
        }
        _ => peer_close_run_error(continuous, interrupted, outcome.peer_closed_target_outcomes)
            .or_else(|| {
                (!interrupted
                    && outcome.successful_target_outcomes == 0
                    && outcome.failed_target_outcomes > 0)
                    .then(|| {
                        format!(
                            "no managed target completed successfully ({} failed)",
                            outcome.failed_target_outcomes
                        )
                    })
            }),
    };
    if let Some(error) = error {
        state.set_run_error(error.clone());
        render_if_due(&mut terminal, &state, &mut next_render, true)?;
        return Err(error.into());
    }
    state.set_status(TuiStatus::Complete);
    render_if_due(&mut terminal, &state, &mut next_render, true)?;
    Ok(())
}

fn process_tui_event(
    event: ManagedEvent,
    state: &mut TuiState,
    terminal_targets: &mut HashSet<TargetInstance>,
) {
    match event {
        ManagedEvent::Client { target, event } => state.process_target_event(&target, &event),
        ManagedEvent::TargetFinished { outcome } => {
            terminal_targets.insert(outcome.target.clone());
            state.process_target_outcome(&outcome);
        }
        _ => {}
    }
}
fn drain_tui_events(
    events: &mut ManagedEventSubscription,
    state: &mut TuiState,
    terminal_targets: &mut HashSet<TargetInstance>,
    dropped_events: &mut u64,
) -> ManagedDrainState {
    drain_managed_events(events, dropped_events, |event| {
        process_tui_event(event, state, terminal_targets);
        Ok::<(), std::convert::Infallible>(())
    })
    .expect("processing TUI managed events is infallible")
}

fn drain_final_tui_events(
    events: &mut ManagedEventSubscription,
    state: &mut TuiState,
    terminal_targets: &mut HashSet<TargetInstance>,
    dropped_events: &mut u64,
) {
    loop {
        match events.try_recv() {
            Ok(event) => process_tui_event(event, state, terminal_targets),
            Err(ManagedEventTryRecvError::Empty | ManagedEventTryRecvError::Closed) => break,
            Err(ManagedEventTryRecvError::Lagged(count)) => {
                *dropped_events = dropped_events.saturating_add(count);
            }
        }
    }
}
fn handle_input(state: &mut TuiState, shutdown_requested: &AtomicBool) -> io::Result<bool> {
    let mut force_render = false;
    for _ in 0..INPUT_EVENT_WORK_BUDGET {
        if !event::poll(Duration::ZERO)? {
            break;
        }
        let Event::Key(key) = event::read()? else {
            continue;
        };
        if key.kind == KeyEventKind::Release {
            continue;
        }
        match key.code {
            KeyCode::Char('q') | KeyCode::Char('c')
                if key.code == KeyCode::Char('q')
                    || key.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                state.quit_requested = true;
                shutdown_requested.store(true, Ordering::Relaxed);
                force_render = true;
                break;
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
                state.toggle_view();
                force_render = true;
            }
            KeyCode::Char('m') => {
                state.cycle_graph_metric();
                force_render = true;
            }
            KeyCode::Left => {
                state.pan_graph_left();
                force_render = true;
            }
            KeyCode::Right => {
                state.pan_graph_right();
                force_render = true;
            }
            KeyCode::PageUp => {
                state.pan_graph_page_left();
                force_render = true;
            }
            KeyCode::PageDown => {
                state.pan_graph_page_right();
                force_render = true;
            }
            KeyCode::Home => {
                state.jump_graph_oldest();
                force_render = true;
            }
            KeyCode::End => {
                state.jump_graph_live();
                force_render = true;
            }
            KeyCode::Char('+') | KeyCode::Char('=') => {
                state.zoom_graph_in();
                force_render = true;
            }
            KeyCode::Char('-') => {
                state.zoom_graph_out();
                force_render = true;
            }
            KeyCode::Char('0') => {
                state.reset_graph_window();
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
    next_render: &mut Instant,
    force: bool,
) -> io::Result<()> {
    let now = Instant::now();
    if should_render(now, *next_render, state.paused, force) {
        terminal.draw(state)?;
        *next_render = now + RENDER_INTERVAL;
    }
    Ok(())
}
fn managed_tui_wait_duration(next_render: &Instant, paused: bool) -> Duration {
    let render_wait = if paused {
        TUI_WAIT_SLICE
    } else {
        next_render.saturating_duration_since(Instant::now())
    };
    render_wait.min(TUI_WAIT_SLICE)
}
