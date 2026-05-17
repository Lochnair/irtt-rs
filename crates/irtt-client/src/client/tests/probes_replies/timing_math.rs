use super::*;

#[test]
fn server_processing_greater_than_raw_does_not_underflow() {
    let base = Instant::now();
    let rtt = compute_rtt(
        &ClientTimestamp {
            mono: base,
            wall: SystemTime::now(),
        },
        &ClientTimestamp {
            mono: base + Duration::from_nanos(1),
            wall: SystemTime::now(),
        },
        &TimestampFields {
            recv_mono: Some(0),
            send_mono: Some(1_000_000_000),
            ..Default::default()
        },
    );
    assert_eq!(rtt.adjusted, Some(SignedDuration { ns: -999_999_999 }));
    assert_eq!(rtt.effective, SignedDuration { ns: -999_999_999 });
}

#[test]
fn effective_rtt_uses_raw_when_adjusted_rtt_is_unavailable() {
    let base = Instant::now();
    let rtt = compute_rtt(
        &ClientTimestamp {
            mono: base,
            wall: SystemTime::now(),
        },
        &ClientTimestamp {
            mono: base + Duration::from_nanos(42),
            wall: SystemTime::now(),
        },
        &TimestampFields::default(),
    );

    assert_eq!(rtt.adjusted, None);
    assert_eq!(rtt.effective, SignedDuration::from_duration(rtt.raw));
}

#[test]
fn unix_epoch_ns_i64_rejects_out_of_range_wall_time() {
    let i64_max_ns = u64::try_from(i64::MAX).unwrap();
    let max = SystemTime::UNIX_EPOCH + Duration::from_nanos(i64_max_ns);
    let overflow = max + Duration::from_nanos(1);

    assert_eq!(unix_epoch_ns_i64(max), Some(i64::MAX));
    assert_eq!(unix_epoch_ns_i64(overflow), None);
}

#[test]
fn compute_one_way_omits_samples_when_client_wall_time_overflows_i64() {
    let i64_max_ns = u64::try_from(i64::MAX).unwrap();
    let overflow_wall =
        SystemTime::UNIX_EPOCH + Duration::from_nanos(i64_max_ns) + Duration::from_nanos(1);
    let sent_at = ClientTimestamp {
        mono: Instant::now(),
        wall: overflow_wall,
    };
    let received_at = ClientTimestamp {
        mono: sent_at.mono + Duration::from_millis(40),
        wall: overflow_wall + Duration::from_millis(40),
    };
    let ts = TimestampFields {
        recv_wall: Some(i64::MAX),
        send_wall: Some(i64::MAX),
        ..Default::default()
    };

    assert_eq!(compute_one_way(&sent_at, &received_at, &ts), None);
}

#[test]
fn compute_one_way_returns_available_direction_samples() {
    let sent_at = ClientTimestamp {
        mono: Instant::now(),
        wall: SystemTime::UNIX_EPOCH + Duration::from_secs(10),
    };
    let received_at = ClientTimestamp {
        mono: sent_at.mono + Duration::from_millis(40),
        wall: SystemTime::UNIX_EPOCH + Duration::from_secs(10) + Duration::from_millis(40),
    };
    let ts = TimestampFields {
        recv_wall: Some(10_000_000_000 + 15_000_000),
        send_wall: Some(10_000_000_000 + 25_000_000),
        ..Default::default()
    };

    let sample = compute_one_way(&sent_at, &received_at, &ts).unwrap();
    assert_eq!(sample.client_to_server, Some(Duration::from_millis(15)));
    assert_eq!(sample.server_to_client, Some(Duration::from_millis(15)));
}
