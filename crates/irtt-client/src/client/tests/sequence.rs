use super::*;

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
    let mut highest = Some(u32::MAX);

    update_highest_received(&mut highest, 0);
    assert_eq!(highest, Some(0));
    update_highest_received(&mut highest, u32::MAX);
    assert_eq!(highest, Some(0));
}
