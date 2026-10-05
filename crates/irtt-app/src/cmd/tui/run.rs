use std::{collections::HashSet, future::poll_fn, io, time::Duration};

use super::ui::{TuiConfig, TuiState, TuiStatus, TuiTerminal};
use crate::{
    cmd::tui::args::TuiArgs,
    shared::client::{
        session::{
            drain_final_events, peer_close_run_error, request_managed_stop_for_peer_close,
            request_managed_stop_once,
        },
        worker::ManagedWorker,
    },
};
use crossterm::{
    event::{Event, EventStream, KeyCode, KeyEventKind, KeyModifiers},
    terminal,
};
use futures_core::Stream;
use irtt_client::{
    managed::{ManagedClient, ManagedEndReason, ManagedEvent, TargetInstance},
    ClientEvent,
};
use ratatui::layout::Rect;
use tokio::{
    sync::{broadcast::error::RecvError, watch},
    time::{sleep, Instant},
};

const RENDER_INTERVAL: Duration = Duration::from_millis(250);

pub async fn run_tui(
    args: TuiArgs,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), Box<dyn std::error::Error>> {
    let setup = args
        .prepare()
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    let continuous = args.is_continuous();
    if *shutdown.borrow() {
        return Ok(());
    }
    let mut terminal = TuiTerminal::enter()?;
    let mut state = TuiState::with_target_labels(
        TuiConfig::from_args(&args, &setup),
        setup.targets.iter().map(|target| target.label.clone()),
    );
    state.set_status(TuiStatus::Opening);
    terminal.draw(&state)?;
    let (task, handle) = match ManagedClient::task(setup.managed_config(), setup.managed_targets())
    {
        Ok(value) => value,
        Err(error) => {
            state.set_error(error.to_string());
            terminal.draw(&state)?;
            return Err(Box::new(error));
        }
    };
    // Both subscriptions exist before the measurement task can be polled.
    let mut events = handle.subscribe()?;
    let mut status = handle.subscribe_status();
    let worker = match ManagedWorker::start(task, handle.clone()) {
        Ok(worker) => worker,
        Err(error) => {
            state.set_run_error(error.to_string());
            terminal.draw(&state)?;
            return Err(error.into());
        }
    };
    let completion = worker.join();
    tokio::pin!(completion);
    // Input and rendering stay on the frontend executor. EventStream's drop
    // wakes its reader; no terminal task can outlive the terminal guard.
    let mut input = EventStream::new();
    let render = sleep(RENDER_INTERVAL);
    tokio::pin!(render);
    let mut interrupted = false;
    let mut stop_requested = false;
    let mut terminal_targets = HashSet::new();
    let mut dropped_events = 0_u64;
    let mut events_closed = false;
    let mut status_closed = false;
    let outcome = loop {
        let mut force_render = false;
        // Default select fairness lets input, status and rendering progress
        // even when measurement events are continuously ready.
        tokio::select! {
            outcome = &mut completion => break outcome,
            _ = shutdown.changed(), if !interrupted => {
                interrupted = true;
                state.set_status(TuiStatus::Interrupted);
                force_render = true;
            }
            changed = status.changed(), if !status_closed => {
                status_closed = changed.is_err();
                let snapshot = status.borrow_and_update().clone();
                if request_managed_stop_for_peer_close(
                    continuous, interrupted, snapshot.peer_closed_target_outcomes,
                    &mut stop_requested,
                ) {
                    drop(handle.stop());
                }
                force_render = true;
            }
            event = events.recv(), if !events_closed => match event {
                Ok(event) => force_render = process_tui_event(
                    event, &mut state, &mut terminal_targets,
                ),
                Err(RecvError::Lagged(count)) => {
                    dropped_events = dropped_events.saturating_add(count);
                    state.mark_dropped_managed_events(dropped_events);
                    force_render = true;
                }
                Err(RecvError::Closed) => events_closed = true,
            },
            event = poll_fn(|cx| std::pin::Pin::new(&mut input).poll_next(cx)) => {
                let event = event.ok_or_else(|| io::Error::new(
                    io::ErrorKind::UnexpectedEof, "terminal input closed",
                ))??;
                match event {
                    Event::Resize(_, _) => force_render = true,
                    Event::Key(key) if key.kind != KeyEventKind::Release => {
                        match key.code {
                            KeyCode::Char('q') | KeyCode::Char('c')
                                if key.code == KeyCode::Char('q')
                                    || key.modifiers.contains(KeyModifiers::CONTROL) => {
                                interrupted = true;
                                state.set_status(TuiStatus::Interrupted);
                                force_render = true;
                            }
                            _ => {
                                let (width, height) = terminal::size()?;
                                force_render = state.handle_key(
                                    key.code, Rect::new(0, 0, width, height),
                                );
                            }
                        }
                    }
                    _ => {}
                }
            }
            _ = &mut render, if !state.paused => force_render = true,
        }
        if interrupted && request_managed_stop_once(&mut stop_requested) {
            drop(handle.stop());
        }
        if force_render {
            terminal.draw(&state)?;
            // Schedule from the completed draw, never catch up missed frames.
            render.as_mut().reset(Instant::now() + RENDER_INTERVAL);
        }
    };
    state.set_status(TuiStatus::Closing);
    terminal.draw(&state)?;
    let outcome = match outcome {
        Ok(outcome) => outcome,
        Err(error) => {
            state.set_run_error(error.to_string());
            terminal.draw(&state)?;
            return Err(error);
        }
    };
    drain_final_events(&mut events, &mut dropped_events, |event| {
        process_tui_event(event, &mut state, &mut terminal_targets);
        Ok::<(), std::convert::Infallible>(())
    })?;
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
    interrupted |= *shutdown.borrow();
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
        terminal.draw(&state)?;
        return Err(error.into());
    }
    state.set_status(TuiStatus::Complete);
    terminal.draw(&state)?;
    Ok(())
}

fn process_tui_event(
    event: ManagedEvent,
    state: &mut TuiState,
    terminal_targets: &mut HashSet<TargetInstance>,
) -> bool {
    match event {
        ManagedEvent::Client { target, event } => {
            let force_render = matches!(
                event,
                ClientEvent::SessionStarted(_)
                    | ClientEvent::NoTestCompleted(_)
                    | ClientEvent::SessionClosed { .. }
                    | ClientEvent::Warning { .. }
            );
            state.process_target_event(&target, &event);
            force_render
        }
        ManagedEvent::TargetFinished { outcome } => {
            terminal_targets.insert(outcome.target.clone());
            state.process_target_outcome(&outcome);
            true
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::{backend::TestBackend, Terminal};

    #[tokio::test(flavor = "current_thread")]
    async fn subscription_lag_keeps_incomplete_statistics_visible_while_paused() {
        for paused in [false, true] {
            let (sender, mut events) = tokio::sync::broadcast::channel(1);
            sender.send(ManagedEvent::Started).unwrap();
            sender.send(ManagedEvent::Started).unwrap();
            let mut state = TuiState::default();
            if paused {
                state.toggle_pause();
            }
            let Err(RecvError::Lagged(dropped)) = events.recv().await else {
                panic!("subscription must report real broadcast overflow");
            };
            state.mark_dropped_managed_events(dropped);
            let mut terminal = Terminal::new(TestBackend::new(200, 40)).unwrap();
            for _ in 0..2 {
                terminal
                    .draw(|frame| super::super::ui::draw_dashboard(frame, &state))
                    .unwrap();
                let text = terminal
                    .backend()
                    .buffer()
                    .content()
                    .iter()
                    .map(|cell| cell.symbol())
                    .collect::<String>();
                assert!(text.contains("incomplete:dropped=1"));
                assert_eq!(text.contains("display paused"), paused);
            }
        }
    }
}
