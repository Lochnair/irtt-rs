use std::sync::atomic::{AtomicBool, Ordering};

use irtt_client::ManagedTargetEndReason;

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

pub fn should_stop_group_on_peer_close(
    continuous: bool,
    interrupted: bool,
    end_reason: &ManagedTargetEndReason,
) -> bool {
    continuous && !interrupted && matches!(end_reason, ManagedTargetEndReason::PeerClosed)
}

pub fn request_group_stop_once(stop_requested: &mut bool) -> bool {
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
    fn grouped_peer_close_stop_policy_is_run_mode_and_interruption_aware() {
        assert!(should_stop_group_on_peer_close(
            true,
            false,
            &ManagedTargetEndReason::PeerClosed
        ));
        assert!(!should_stop_group_on_peer_close(
            false,
            false,
            &ManagedTargetEndReason::PeerClosed
        ));
        assert!(!should_stop_group_on_peer_close(
            true,
            true,
            &ManagedTargetEndReason::PeerClosed
        ));
        assert!(!should_stop_group_on_peer_close(
            true,
            false,
            &ManagedTargetEndReason::TestComplete
        ));

        let mut stop_requested = false;
        assert!(request_group_stop_once(&mut stop_requested));
        assert!(!request_group_stop_once(&mut stop_requested));
    }
}
