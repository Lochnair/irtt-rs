use std::sync::atomic::{AtomicBool, Ordering};

pub fn is_shutdown_requested(shutdown_requested: &AtomicBool) -> bool {
    shutdown_requested.load(Ordering::Relaxed)
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
    peer_closed_target_count: u64,
    peer_close_requested_stop: &mut bool,
    stop_requested: &mut bool,
) -> bool {
    if !continuous || interrupted || peer_closed_target_count == 0 {
        return false;
    }
    *peer_close_requested_stop = true;
    request_managed_stop_once(stop_requested)
}

pub fn request_managed_stop_once(stop_requested: &mut bool) -> bool {
    if *stop_requested {
        return false;
    }
    *stop_requested = true;
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_close_run_error_is_run_mode_and_interruption_aware() {
        assert_eq!(peer_close_run_error(false, false, 1), None);
        assert_eq!(peer_close_run_error(true, false, 0), None);
        assert_eq!(peer_close_run_error(true, true, 1), None);
        assert_eq!(
            peer_close_run_error(true, false, 1).as_deref(),
            Some("continuous run ended because of peer closure (1 target session)")
        );
        assert_eq!(
            peer_close_run_error(true, false, 3).as_deref(),
            Some("continuous run ended because of peer closure (3 target sessions)")
        );
    }

    #[test]
    fn grouped_peer_close_stop_policy_is_run_mode_and_one_shot() {
        let mut peer_close_requested_stop = false;
        let mut stop_requested = false;
        assert!(!request_managed_stop_for_peer_close(
            false,
            false,
            1,
            &mut peer_close_requested_stop,
            &mut stop_requested,
        ));
        assert!(!request_managed_stop_for_peer_close(
            true,
            true,
            1,
            &mut peer_close_requested_stop,
            &mut stop_requested,
        ));
        assert!(!request_managed_stop_for_peer_close(
            true,
            false,
            0,
            &mut peer_close_requested_stop,
            &mut stop_requested,
        ));
        assert!(!peer_close_requested_stop);
        assert!(!stop_requested);

        assert!(request_managed_stop_for_peer_close(
            true,
            false,
            1,
            &mut peer_close_requested_stop,
            &mut stop_requested,
        ));
        assert!(peer_close_requested_stop);
        assert!(stop_requested);
        assert!(!request_managed_stop_for_peer_close(
            true,
            false,
            1,
            &mut peer_close_requested_stop,
            &mut stop_requested,
        ));
    }
}
