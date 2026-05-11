use super::*;
use std::time::Instant;

use crate::{
    probe::{CompletedSet, PendingMap, TimedOutMap},
    session::ActiveSession,
};

#[test]
fn sequence_ordering_handles_normal_and_wrapped_values() {
    assert!(sequence_is_after(11, 10));
    assert!(sequence_is_before(9, 10));
    assert!(!sequence_is_after(10, 10));
    assert!(!sequence_is_before(10, 10));

    assert!(sequence_is_after(0, u32::MAX - 1));
    assert!(!sequence_is_before(0, u32::MAX - 1));
    assert!(sequence_is_after(1, u32::MAX));
    assert!(!sequence_is_before(1, u32::MAX));

    assert!(sequence_is_before(u32::MAX, 1));
    assert!(!sequence_is_after(u32::MAX, 1));
}

#[test]
fn highest_received_seq_updates_across_wrap() {
    let mut session = ActiveSession {
        next_wire_seq: 0,
        highest_received_seq: Some(u32::MAX),
        packets_sent: 0,
        start_mono: Instant::now(),
        end_mono: None,
        next_send_at: Instant::now(),
        pending: PendingMap::new(8),
        timed_out: TimedOutMap::new(8),
        completed: CompletedSet::new(8),
        sending_done: false,
    };

    update_highest_received(&mut session, 0);
    assert_eq!(session.highest_received_seq, Some(0));
    update_highest_received(&mut session, u32::MAX);
    assert_eq!(session.highest_received_seq, Some(0));
}
