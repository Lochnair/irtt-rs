use irtt_client::managed::{ManagedEvent, ManagedEventSubscription, ManagedEventTryRecvError};

pub(crate) fn drain_final_events<E>(
    events: &mut ManagedEventSubscription,
    dropped_events: &mut u64,
    mut process: impl FnMut(ManagedEvent) -> Result<(), E>,
) -> Result<(), E> {
    loop {
        match events.try_recv() {
            Ok(event) => process(event)?,
            Err(ManagedEventTryRecvError::Empty | ManagedEventTryRecvError::Closed) => break,
            Err(ManagedEventTryRecvError::Lagged(count)) => {
                *dropped_events = dropped_events.saturating_add(count);
            }
        }
    }
    Ok(())
}

pub fn should_print_final_summary(continuous: bool, interrupted: bool) -> bool {
    !continuous || interrupted
}

pub fn peer_close_run_error(
    continuous: bool,
    interrupted: bool,
    peer_closed_target_outcomes: u64,
) -> Option<String> {
    if !continuous || interrupted || peer_closed_target_outcomes == 0 {
        return None;
    }

    let sessions = if peer_closed_target_outcomes == 1 {
        "target session"
    } else {
        "target sessions"
    };
    Some(format!(
        "continuous run ended because of peer closure ({peer_closed_target_outcomes} {sessions})"
    ))
}

pub fn request_managed_stop_for_peer_close(
    continuous: bool,
    interrupted: bool,
    peer_closed_target_outcomes: u64,
    stop_requested: &mut bool,
) -> bool {
    if !continuous || interrupted || peer_closed_target_outcomes == 0 {
        return false;
    }
    request_managed_stop_once(stop_requested)
}

pub fn request_managed_stop_once(stop_requested: &mut bool) -> bool {
    if *stop_requested {
        return false;
    }
    *stop_requested = true;
    true
}
