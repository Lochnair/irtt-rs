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
            peer_close_run_error, request_managed_stop_for_peer_close, request_managed_stop_once,
        },
    },
};
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use irtt_client::managed::{
    BlockingManagedClient, ManagedClientConfig, ManagedCompletionPolicy, ManagedEndReason,
    ManagedEvent, ManagedEventSubscription, ManagedEventTryRecvError, TargetInstance,
};

const RENDER_INTERVAL: Duration = Duration::from_millis(250);
const TUI_WAIT_SLICE: Duration = Duration::from_millis(20);
const MANAGED_EVENT_CAPACITY: usize = 16_384;

pub fn run_tui(
    args: TuiArgs,
    shutdown_requested: &AtomicBool,
) -> Result<(), Box<dyn std::error::Error>> {
    let targets = args
        .managed_targets()
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    let continuous = args.is_continuous();
    let mut terminal = TuiTerminal::enter()?;
    let mut state = TuiState::with_target_labels(
        TuiConfig::from_args(&args),
        targets.iter().map(|target| target.label.clone()),
    );
    let mut next_render = Instant::now();
    if is_shutdown_requested(shutdown_requested) {
        return Ok(());
    }
    state.set_status(TuiStatus::Opening);
    render_if_due(&mut terminal, &state, &mut next_render, true)?;
    let target_count = targets.len();
    let config = ManagedClientConfig {
        client: args.to_client_config(),
        pacing: args.pacing.into(),
        completion: ManagedCompletionPolicy::FinishWhenQuiescent,
        event_capacity: MANAGED_EVENT_CAPACITY,
        outcome_history_limit: target_count,
        max_live_target_generations: target_count,
        ..ManagedClientConfig::default()
    };
    let (owner, mut events) = match BlockingManagedClient::start_with_subscription(
        config,
        targets
            .iter()
            .map(|target| target.managed.clone())
            .collect(),
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
    let mut peer_close_requested_stop = false;
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
            &mut peer_close_requested_stop,
            &mut stop_requested,
        ) {
            drop(handle.stop());
        }
        drain_tui_events(
            &mut events,
            &mut state,
            &mut terminal_targets,
            &mut dropped_events,
        );
        if handle.status().final_outcome.is_some() || subscription_closed {
            break;
        }
        match events.try_recv() {
            Ok(event) => process_tui_event(event, &mut state, &mut terminal_targets),
            Err(ManagedEventTryRecvError::Empty) => {
                thread::sleep(managed_tui_wait_duration(&next_render, state.paused))
            }
            Err(ManagedEventTryRecvError::Lagged(count)) => dropped_events += count,
            Err(ManagedEventTryRecvError::Closed) => subscription_closed = true,
        }
        render_if_due(&mut terminal, &state, &mut next_render, false)?;
    }
    if interrupted {
        state.set_status(TuiStatus::Interrupted);
        render_if_due(&mut terminal, &state, &mut next_render, true)?;
    }
    state.set_status(TuiStatus::Closing);
    render_if_due(&mut terminal, &state, &mut next_render, true)?;
    drain_tui_events(
        &mut events,
        &mut state,
        &mut terminal_targets,
        &mut dropped_events,
    );
    let outcome = owner.join()?;
    drain_tui_events(
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
) {
    loop {
        match events.try_recv() {
            Ok(event) => process_tui_event(event, state, terminal_targets),
            Err(ManagedEventTryRecvError::Empty | ManagedEventTryRecvError::Closed) => break,
            Err(ManagedEventTryRecvError::Lagged(count)) => *dropped_events += count,
        }
    }
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
            KeyCode::Char('q') | KeyCode::Char('c')
                if key.code == KeyCode::Char('q')
                    || key.modifiers.contains(KeyModifiers::CONTROL) =>
            {
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
