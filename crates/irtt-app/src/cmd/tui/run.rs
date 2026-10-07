use std::{future::poll_fn, io, time::Duration};

use super::ui::{TuiConfig, TuiState, TuiStatus, TuiTerminal};
use crate::{
    cmd::tui::args::TuiArgs,
    shared::client::{
        session::{drain_final_events, request_managed_stop_once},
        worker::ManagedWorker,
    },
};
use crossterm::{
    event::{Event, EventStream, KeyCode, KeyEventKind, KeyModifiers},
    terminal,
};
use futures_core::Stream;
use irtt_client::{
    managed::{
        ManagedClient, ManagedCommandApplyError, ManagedCommandError, ManagedCompletionPolicy,
        ManagedEndReason, ManagedEvent, ManagedTargetEndReason, ManagedTargetLifecycle,
    },
    ClientEvent,
};
use ratatui::layout::Rect;
use tokio::{
    sync::{broadcast::error::RecvError, watch},
    time::{sleep, Instant},
};

const RETRY_DELAY: Duration = Duration::from_millis(1500);

const RENDER_INTERVAL: Duration = Duration::from_millis(100);

pub async fn run_tui(
    args: TuiArgs,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), Box<dyn std::error::Error>> {
    let setup = tokio::select! {
        result = args.prepare() => result.map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?,
        _ = shutdown.wait_for(|requested| *requested) => return Ok(()),
    };
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
    let desired_targets = setup.managed_targets();
    let mut config = setup.managed_config();
    if continuous {
        config.completion = ManagedCompletionPolicy::ExplicitStop;
    }
    let (task, handle) = match ManagedClient::task(config, desired_targets.clone()) {
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
    let retry = sleep(RETRY_DELAY);
    tokio::pin!(retry);
    let mut retry_armed = false;
    let mut update_receipt = None;
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
                state.process_managed_status(&status.borrow_and_update());
                force_render = true;
            }
            event = events.recv(), if !events_closed => match event {
                Ok(event) => force_render = process_tui_event(
                    event, &mut state,
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
            _ = &mut retry, if retry_armed && !interrupted && !*shutdown.borrow() => {
                state.process_managed_status(&status.borrow());
                retry_armed = false;
                update_receipt = match handle.update_targets(desired_targets.clone()) {
                    Ok(receipt) => Some(receipt),
                    Err(ManagedCommandError::Stopping | ManagedCommandError::DriverClosed)
                        if stop_requested || *shutdown.borrow() => None,
                    Err(error) => return Err(format!("reconnect update failed: {error}").into()),
                };
            }
            result = async { update_receipt.as_mut().unwrap().await },
                if update_receipt.is_some() => {
                update_receipt = None;
                match result {
                    Ok(ack) => {
                        state.process_managed_status(&ack.status);
                        force_render = true;
                    }
                    Err(ManagedCommandApplyError::Stopping
                        | ManagedCommandApplyError::AcknowledgementDisconnected)
                        if stop_requested || *shutdown.borrow() => {}
                    Err(error) => return Err(format!("reconnect update failed: {error}").into()),
                }
            }
            _ = &mut render, if !state.paused => force_render = true,
        }
        if interrupted && request_managed_stop_once(&mut stop_requested) {
            drop(handle.stop());
        }
        if continuous
            && !interrupted
            && !stop_requested
            && !retry_armed
            && update_receipt.is_none()
            && status.borrow().targets.iter().any(|target| {
                target.desired
                    && target.outcome.as_ref().is_some_and(|outcome| {
                        matches!(
                            outcome.end_reason,
                            ManagedTargetEndReason::Failed(_)
                                | ManagedTargetEndReason::PeerClosed
                                | ManagedTargetEndReason::TestComplete
                        )
                    })
            })
        {
            retry.as_mut().reset(Instant::now() + RETRY_DELAY);
            retry_armed = true;
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
        process_tui_event(event, &mut state);
        Ok::<(), std::convert::Infallible>(())
    })?;
    state.mark_dropped_managed_events(dropped_events);
    if !continuous && outcome.discarded_target_outcomes != 0 {
        state.set_run_error(format!(
            "{} final target outcomes were discarded",
            outcome.discarded_target_outcomes
        ));
    }
    for target in outcome.recent_target_outcomes.iter() {
        state.process_target_outcome(target);
    }
    interrupted |= *shutdown.borrow();
    let error = match &outcome.end_reason {
        ManagedEndReason::DriverFailed(failure) => {
            Some(format!("managed driver failed: {failure}"))
        }
        _ => (!interrupted
            && outcome.successful_target_outcomes == 0
            && outcome.failed_target_outcomes > 0)
            .then(|| {
                format!(
                    "no managed target completed successfully ({} failed)",
                    outcome.failed_target_outcomes
                )
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

fn process_tui_event(event: ManagedEvent, state: &mut TuiState) -> bool {
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
            state.process_target_outcome(&outcome);
            true
        }
        ManagedEvent::TargetStateChanged {
            target,
            lifecycle:
                ManagedTargetLifecycle::Pending
                | ManagedTargetLifecycle::Connecting
                | ManagedTargetLifecycle::Opening,
        } => {
            state.process_target_opening(&target);
            true
        }
        _ => false,
    }
}
