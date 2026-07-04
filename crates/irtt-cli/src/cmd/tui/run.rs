use std::{
    io,
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::{Duration, Instant},
};

use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use irtt_client::{
    ClientEvent, EventSubscriptionError, ManagedClientGroup, ManagedClientGroupConfig,
    ManagedGroupEndReason, SubscriberConfig, SubscriberOverflow,
};

use crate::{
    cmd::tui::args::{ResolvedTuiTarget, TuiArgs},
    shared::client::{is_shutdown_requested, ClientSession},
};

use super::ui::{should_render, TuiConfig, TuiState, TuiStatus, TuiTerminal};

const RENDER_INTERVAL: Duration = Duration::from_millis(250);
const TUI_WAIT_SLICE: Duration = Duration::from_millis(20);
const IDLE_SLEEP: Duration = Duration::from_millis(5);
const GROUP_COMPLETION_GRACE: Duration = Duration::from_secs(1);

pub fn run_tui(
    args: TuiArgs,
    shutdown_requested: &AtomicBool,
) -> Result<(), Box<dyn std::error::Error>> {
    let targets = args
        .resolved_managed_targets()
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

    if targets.len() > 1 {
        return run_group_tui(
            args,
            targets,
            &mut terminal,
            &mut state,
            &mut next_render,
            shutdown_requested,
        );
    }

    let mut session = match ClientSession::connect(args.to_client_config(), continuous) {
        Ok(session) => session,
        Err(err) => {
            state.set_error(err.to_string());
            render_if_due(&mut terminal, &state, &mut next_render, true)?;
            return Err(Box::new(err));
        }
    };

    if is_shutdown_requested(shutdown_requested) {
        return Ok(());
    }

    let events = session.open()?;
    state.process_events(&events);
    render_if_due(&mut terminal, &state, &mut next_render, true)?;

    let mut interrupted = false;
    // Keep this in lockstep with run_stream: send due probes, drain available
    // replies, poll timeouts, sleep toward the next absolute send deadline,
    // then perform the same final drain, timeout poll, and close sequence.
    while session.should_continue(shutdown_requested) {
        if handle_input(&mut state, shutdown_requested)? {
            render_if_due(&mut terminal, &state, &mut next_render, true)?;
        }
        if state.quit_requested {
            interrupted = true;
            break;
        }

        let events = session.step(shutdown_requested)?;
        state.process_events(&events);

        if is_shutdown_requested(shutdown_requested) {
            interrupted = true;
            break;
        }

        render_if_due(&mut terminal, &state, &mut next_render, false)?;
        wait_for_tui_activity(
            session.next_send_deadline(),
            &mut next_render,
            &mut state,
            &mut terminal,
            shutdown_requested,
        )?;
    }
    interrupted |= is_shutdown_requested(shutdown_requested);

    if interrupted {
        state.set_status(TuiStatus::Interrupted);
        render_if_due(&mut terminal, &state, &mut next_render, true)?;
    }

    if session.should_drain_final(interrupted) {
        state.set_status(TuiStatus::Draining);
        let mut drain_render = Instant::now();
        session.drain_final(|events| {
            state.process_events(events);
            let _ = render_if_due(&mut terminal, &state, &mut drain_render, false);
        })?;
        render_if_due(&mut terminal, &state, &mut next_render, true)?;
    }

    let events = session.poll_timeouts()?;
    state.process_events(&events);

    state.set_status(TuiStatus::Closing);
    render_if_due(&mut terminal, &state, &mut next_render, true)?;

    let events = session.close()?;
    state.process_events(&events);
    state.set_status(TuiStatus::Complete);
    render_if_due(&mut terminal, &state, &mut next_render, true)?;
    Ok(())
}

fn run_group_tui(
    args: TuiArgs,
    targets: Vec<ResolvedTuiTarget>,
    terminal: &mut TuiTerminal,
    state: &mut TuiState,
    next_render: &mut Instant,
    shutdown_requested: &AtomicBool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut config = args.to_client_config();
    let managed_targets = targets
        .iter()
        .map(|target| target.managed.clone())
        .collect::<Vec<_>>();
    let expected_target_count = managed_targets.len();
    if let Some(first) = managed_targets.first() {
        config.server_addr = first.remote.to_string();
    }

    let group_config = ManagedClientGroupConfig {
        client: config,
        pacing: args.pacing.into(),
    };
    let (session, events) = ManagedClientGroup::start_with_subscription(
        group_config,
        managed_targets,
        SubscriberConfig {
            capacity: 16_384,
            overflow: SubscriberOverflow::DropOldest,
        },
    )?;

    let mut interrupted = false;
    let mut terminal_targets = std::collections::HashSet::new();
    let mut saw_target_event = false;
    let mut last_event_at = Instant::now();

    let exit = loop {
        if is_shutdown_requested(shutdown_requested) {
            interrupted = true;
            session.stop();
        }

        if handle_input(state, shutdown_requested)? {
            render_if_due(terminal, state, next_render, true)?;
        }
        if state.quit_requested {
            interrupted = true;
            session.stop();
        }

        match events.try_recv() {
            Ok(Some(target_event)) => {
                saw_target_event = true;
                last_event_at = Instant::now();
                if is_terminal_target_event(&target_event.event) {
                    terminal_targets.insert(target_event.target.as_str().to_owned());
                }
                state.process_target_event(&target_event);
                render_if_due(terminal, state, next_render, false)?;
            }
            Ok(None) => {
                if interrupted {
                    break GroupLoopExit::Interrupted;
                }
                if terminal_targets.len() >= expected_target_count {
                    break GroupLoopExit::AllTargetsTerminal;
                }
                if should_join_group_after_idle(&args, saw_target_event, last_event_at) {
                    break GroupLoopExit::IdleGraceElapsed;
                }
                wait_for_tui_activity(None, next_render, state, terminal, shutdown_requested)?;
                thread::sleep(IDLE_SLEEP);
            }
            Err(EventSubscriptionError::Disconnected) => {
                break GroupLoopExit::SubscriptionDisconnected
            }
        }
    };

    if exit.should_stop_before_join() {
        session.stop();
    }

    if interrupted {
        state.set_status(TuiStatus::Interrupted);
        render_if_due(terminal, state, next_render, true)?;
    }

    state.set_status(TuiStatus::Closing);
    render_if_due(terminal, state, next_render, true)?;

    let outcome = session.join()?;
    while let Ok(Some(target_event)) = events.try_recv() {
        state.process_target_event(&target_event);
    }

    if outcome.end_reason == ManagedGroupEndReason::Cancelled && !interrupted {
        return match exit {
            GroupLoopExit::IdleGraceElapsed => {
                Err("managed client group stayed idle before all targets completed".into())
            }
            GroupLoopExit::SubscriptionDisconnected => {
                Err("managed client group event subscription disconnected before completion".into())
            }
            GroupLoopExit::Interrupted | GroupLoopExit::AllTargetsTerminal => {
                Err("managed client group was cancelled".into())
            }
        };
    }

    state.set_status(TuiStatus::Complete);
    render_if_due(terminal, state, next_render, true)?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupLoopExit {
    Interrupted,
    AllTargetsTerminal,
    IdleGraceElapsed,
    SubscriptionDisconnected,
}

impl GroupLoopExit {
    fn should_stop_before_join(self) -> bool {
        matches!(
            self,
            Self::Interrupted | Self::IdleGraceElapsed | Self::SubscriptionDisconnected
        )
    }
}

fn is_terminal_target_event(event: &ClientEvent) -> bool {
    matches!(
        event,
        ClientEvent::SessionClosed { .. } | ClientEvent::NoTestCompleted { .. }
    )
}

fn estimated_group_completion_grace(args: &TuiArgs) -> Duration {
    let open_timeout: Duration = args.to_client_config().open_timeouts.iter().sum();
    open_timeout
        .saturating_add(args.duration)
        .saturating_add(GROUP_COMPLETION_GRACE)
}

fn should_join_group_after_idle(
    args: &TuiArgs,
    saw_target_event: bool,
    last_event_at: Instant,
) -> bool {
    if args.is_continuous() && saw_target_event {
        return false;
    }
    last_event_at.elapsed() > estimated_group_completion_grace(args)
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
            KeyCode::Char('g') | KeyCode::Char('m') => {
                state.cycle_graph_mode();
                force_render = true;
            }
            KeyCode::Char('s') => {
                state.cycle_graph_scale();
                force_render = true;
            }
            KeyCode::Char('f') => {
                state.toggle_full_graph();
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
    if !should_render(now, *next_render, state.paused, force) {
        return Ok(());
    }
    terminal.draw(state)?;
    *next_render = now + RENDER_INTERVAL;
    Ok(())
}

fn wait_for_tui_activity(
    next_send_deadline: Option<Instant>,
    next_render: &mut Instant,
    state: &mut TuiState,
    terminal: &mut TuiTerminal,
    shutdown_requested: &AtomicBool,
) -> io::Result<()> {
    let wait_for = tui_wait_duration(next_send_deadline, *next_render, state.paused);
    if wait_for.is_zero() || !event::poll(wait_for)? {
        return Ok(());
    }

    if handle_input(state, shutdown_requested)? {
        render_if_due(terminal, state, next_render, true)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn parse(args: &[&str]) -> TuiArgs {
        let mut argv = vec!["irtt-tui"];
        argv.extend_from_slice(args);
        TuiArgs::try_parse_from(argv).unwrap()
    }

    #[test]
    fn continuous_multi_target_idle_does_not_stop_after_events() {
        let args = parse(&["127.0.0.1:2112", "127.0.0.2:2112"]);
        let old_event_at = Instant::now() - estimated_group_completion_grace(&args) - IDLE_SLEEP;

        assert!(!should_join_group_after_idle(&args, true, old_event_at));
    }

    #[test]
    fn finite_or_unopened_group_can_leave_after_idle_grace() {
        let finite = parse(&["--duration", "1s", "127.0.0.1:2112", "127.0.0.2:2112"]);
        let finite_old_event_at =
            Instant::now() - estimated_group_completion_grace(&finite) - IDLE_SLEEP;
        assert!(should_join_group_after_idle(
            &finite,
            true,
            finite_old_event_at
        ));

        let continuous = parse(&["127.0.0.1:2112", "127.0.0.2:2112"]);
        let unopened_old_event_at =
            Instant::now() - estimated_group_completion_grace(&continuous) - IDLE_SLEEP;
        assert!(should_join_group_after_idle(
            &continuous,
            false,
            unopened_old_event_at
        ));
    }

    #[test]
    fn protective_group_exits_stop_before_joining() {
        assert!(GroupLoopExit::Interrupted.should_stop_before_join());
        assert!(GroupLoopExit::IdleGraceElapsed.should_stop_before_join());
        assert!(GroupLoopExit::SubscriptionDisconnected.should_stop_before_join());
        assert!(!GroupLoopExit::AllTargetsTerminal.should_stop_before_join());
    }
}
