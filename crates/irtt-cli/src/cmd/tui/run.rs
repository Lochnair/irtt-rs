use std::{
    io,
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::{Duration, Instant},
};

use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use irtt_client::{
    EventSubscription, EventSubscriptionError, ManagedClient, ManagedClientGroup,
    ManagedClientGroupConfig, ManagedGroupCompletionPolicy, ManagedGroupEndReason,
    ManagedGroupEvent, SessionEndReason, SubscriberConfig, SubscriberOverflow,
};

use crate::{
    cmd::tui::args::{ResolvedTuiTarget, TuiArgs},
    shared::client::{
        is_shutdown_requested,
        session::{peer_close_run_error, request_group_stop_once, should_stop_group_on_peer_close},
    },
};

use super::ui::{should_render, TuiConfig, TuiState, TuiStatus, TuiTerminal};

const RENDER_INTERVAL: Duration = Duration::from_millis(250);
const TUI_WAIT_SLICE: Duration = Duration::from_millis(20);
const IDLE_SLEEP: Duration = Duration::from_millis(5);
const GROUP_COMPLETION_GRACE: Duration = Duration::from_secs(1);
const MANAGED_EVENT_CAPACITY: usize = 16_384;

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

    let (session, events) = match ManagedClient::start_with_subscription(
        args.to_client_config(),
        managed_event_subscriber_config(),
    ) {
        Ok(session) => session,
        Err(err) => {
            state.set_error(err.to_string());
            render_if_due(&mut terminal, &state, &mut next_render, true)?;
            return Err(Box::new(err));
        }
    };

    let mut interrupted = false;
    loop {
        if is_shutdown_requested(shutdown_requested) {
            interrupted = true;
            session.stop();
            break;
        }

        if handle_input(&mut state, shutdown_requested)? {
            render_if_due(&mut terminal, &state, &mut next_render, true)?;
        }
        if state.quit_requested {
            interrupted = true;
            session.stop();
            break;
        }

        match events.recv_timeout(managed_tui_wait_duration(&next_render, state.paused)) {
            Ok(Some(event)) => {
                state.process_event(&event);
                render_if_due(&mut terminal, &state, &mut next_render, false)?;
            }
            Ok(None) => {
                render_if_due(&mut terminal, &state, &mut next_render, false)?;
            }
            Err(EventSubscriptionError::Disconnected) => break,
        }
    }
    interrupted |= is_shutdown_requested(shutdown_requested);

    if interrupted {
        state.set_status(TuiStatus::Interrupted);
        render_if_due(&mut terminal, &state, &mut next_render, true)?;
    }

    if interrupted {
        state.set_status(TuiStatus::Draining);
        drain_single_tui_events(&events, &mut state);
        render_if_due(&mut terminal, &state, &mut next_render, true)?;
    }

    state.set_status(TuiStatus::Closing);
    render_if_due(&mut terminal, &state, &mut next_render, true)?;

    let outcome = session.join();
    drain_single_tui_events(&events, &mut state);
    state.mark_dropped_client_events(events.dropped_events());
    let outcome = match outcome {
        Ok(outcome) => outcome,
        Err(err) => {
            state.set_error(err.to_string());
            render_if_due(&mut terminal, &state, &mut next_render, true)?;
            return Err(Box::new(err));
        }
    };

    interrupted |= is_shutdown_requested(shutdown_requested);
    if let Some(error) = peer_close_run_error(
        continuous,
        interrupted,
        u64::from(outcome.end_reason == SessionEndReason::PeerClosed),
    ) {
        state.set_run_error(error.clone());
        render_if_due(&mut terminal, &state, &mut next_render, true)?;
        return Err(error.into());
    }
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
        completion: ManagedGroupCompletionPolicy::AllTargetsComplete,
    };
    let (session, events) = ManagedClientGroup::start_with_subscription(
        group_config,
        managed_targets,
        SubscriberConfig {
            capacity: MANAGED_EVENT_CAPACITY,
            overflow: SubscriberOverflow::DropOldest,
        },
    )?;

    let mut interrupted = false;
    let mut stop_requested = false;
    let mut peer_close_requested_stop = false;
    let mut terminal_targets = std::collections::HashSet::new();
    let mut saw_target_event = false;
    let mut last_event_at = Instant::now();

    let exit = loop {
        if is_shutdown_requested(shutdown_requested) {
            interrupted = true;
            if request_group_stop_once(&mut stop_requested) {
                session.stop();
            }
        }

        if handle_input(state, shutdown_requested)? {
            render_if_due(terminal, state, next_render, true)?;
        }
        if state.quit_requested {
            interrupted = true;
            if request_group_stop_once(&mut stop_requested) {
                session.stop();
            }
        }

        match events.try_recv() {
            Ok(Some(group_event)) => {
                saw_target_event = true;
                last_event_at = Instant::now();
                match group_event {
                    ManagedGroupEvent::Client(target_event) => {
                        state.process_target_event(&target_event);
                    }
                    ManagedGroupEvent::TargetFinished(target) => {
                        terminal_targets.insert(target.id.as_str().to_owned());
                        state.process_target_outcome(&target);
                        if should_stop_group_on_peer_close(
                            args.is_continuous(),
                            interrupted,
                            &target.end_reason,
                        ) {
                            peer_close_requested_stop = true;
                            if request_group_stop_once(&mut stop_requested) {
                                session.stop();
                            }
                        }
                    }
                }
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

    if exit.should_stop_before_join() && request_group_stop_once(&mut stop_requested) {
        session.stop();
    }

    if interrupted {
        state.set_status(TuiStatus::Interrupted);
        render_if_due(terminal, state, next_render, true)?;
    }

    state.set_status(TuiStatus::Closing);
    render_if_due(terminal, state, next_render, true)?;

    let outcome = session.join()?;
    while let Ok(Some(group_event)) = events.try_recv() {
        match group_event {
            ManagedGroupEvent::Client(target_event) => state.process_target_event(&target_event),
            ManagedGroupEvent::TargetFinished(target) => {
                if terminal_targets.insert(target.id.as_str().to_owned()) {
                    state.process_target_outcome(&target);
                }
            }
        }
    }
    state.mark_dropped_group_events(events.dropped_events());
    for target in &outcome.targets {
        if terminal_targets.insert(target.id.as_str().to_owned()) {
            state.process_target_outcome(target);
        }
    }

    interrupted |= is_shutdown_requested(shutdown_requested);
    let peer_closed_target_outcomes = outcome
        .peer_closed_target_outcomes
        .max(u64::from(peer_close_requested_stop));
    if let Some(error) = peer_close_run_error(
        args.is_continuous(),
        interrupted,
        peer_closed_target_outcomes,
    ) {
        state.set_run_error(error.clone());
        render_if_due(terminal, state, next_render, true)?;
        return Err(error.into());
    }
    if outcome.end_reason == ManagedGroupEndReason::Cancelled
        && !interrupted
        && !peer_close_requested_stop
    {
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
    let successful_targets = outcome.successful_target_outcomes;
    let failed_targets = outcome.failed_target_outcomes;
    if !interrupted && successful_targets == 0 && failed_targets > 0 {
        state.set_status(TuiStatus::Error);
        render_if_due(terminal, state, next_render, true)?;
        return Err(
            format!("no managed target completed successfully ({failed_targets} failed)").into(),
        );
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
    if !should_render(now, *next_render, state.paused, force) {
        return Ok(());
    }
    terminal.draw(state)?;
    *next_render = now + RENDER_INTERVAL;
    Ok(())
}

fn managed_event_subscriber_config() -> SubscriberConfig {
    SubscriberConfig {
        capacity: MANAGED_EVENT_CAPACITY,
        overflow: SubscriberOverflow::DropOldest,
    }
}

fn drain_single_tui_events(events: &EventSubscription, state: &mut TuiState) {
    while let Ok(Some(event)) = events.try_recv() {
        state.process_event(&event);
    }
}

fn managed_tui_wait_duration(next_render: &Instant, paused: bool) -> Duration {
    let render_wait = if paused {
        TUI_WAIT_SLICE
    } else {
        next_render.saturating_duration_since(Instant::now())
    };
    render_wait.min(TUI_WAIT_SLICE)
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

    #[test]
    fn continuous_group_peer_close_requests_stop_unless_interrupted() {
        assert!(should_stop_group_on_peer_close(
            true,
            false,
            &irtt_client::ManagedTargetEndReason::PeerClosed
        ));
        assert!(!should_stop_group_on_peer_close(
            false,
            false,
            &irtt_client::ManagedTargetEndReason::PeerClosed
        ));
        assert!(!should_stop_group_on_peer_close(
            true,
            true,
            &irtt_client::ManagedTargetEndReason::PeerClosed
        ));
    }

    #[test]
    fn single_target_managed_drain_consumes_queued_terminal_event() {
        let hub = irtt_client::EventHub::new();
        let events = hub
            .subscribe(SubscriberConfig {
                capacity: 4,
                overflow: SubscriberOverflow::DropOldest,
            })
            .unwrap();
        hub.publish(irtt_client::ClientEvent::SessionClosed {
            remote: "127.0.0.1:2112".parse().unwrap(),
            token: 7,
            at: irtt_client::ClientTimestamp::now(),
        });
        hub.disconnect_all();
        let mut state = TuiState::new(TuiConfig::default());

        drain_single_tui_events(&events, &mut state);

        assert_eq!(state.status(), TuiStatus::Complete);
        assert_eq!(
            events.try_recv().unwrap_err(),
            EventSubscriptionError::Disconnected
        );
    }
}
