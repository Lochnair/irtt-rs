//! Public behavior of the retained-memory estimate.

use std::time::Duration;

use irtt_stats::{SampleMode, StatsConfig};

#[test]
fn zero_probes_retain_nothing() {
    assert_eq!(StatsConfig::finite().estimated_retained_bytes(0), 0);
    assert_eq!(StatsConfig::continuous().estimated_retained_bytes(0), 0);
}

#[test]
fn exact_mode_grows_with_probe_count() {
    let config = StatsConfig::finite();
    assert_eq!(config.samples, SampleMode::Exact);

    let mut previous = config.estimated_retained_bytes(0);
    for probes in [1_u64, 1_000, 100_000, 10_000_000] {
        let current = config.estimated_retained_bytes(probes);
        assert!(
            current > previous,
            "estimate should grow with probe count, but {probes} probes gave {current} \
             after {previous}"
        );
        previous = current;
    }
}

#[test]
fn exact_mode_scales_linearly_with_probe_count() {
    let config = StatsConfig::finite();
    let single = config.estimated_retained_bytes(1);

    assert_eq!(config.estimated_retained_bytes(1_000), single * 1_000);
}

#[test]
fn enormous_probe_counts_saturate_instead_of_wrapping() {
    for config in [StatsConfig::finite(), StatsConfig::continuous()] {
        // Reaching this at all proves no panic; a wrapping multiplication
        // would land near zero rather than near the maximum.
        assert!(
            config.estimated_retained_bytes(u64::MAX) > 0,
            "a saturating estimate should not wrap to zero"
        );
    }

    assert_eq!(
        StatsConfig::finite().estimated_retained_bytes(u64::MAX),
        u64::MAX,
        "an unbounded exact-mode estimate should saturate at the maximum"
    );
}

#[test]
fn running_only_does_not_report_unbounded_per_probe_retention() {
    let continuous = StatsConfig::continuous();
    assert_eq!(continuous.samples, SampleMode::RunningOnly);

    let huge = continuous.estimated_retained_bytes(u64::MAX);
    let modest = continuous.estimated_retained_bytes(1_000_000);

    assert_eq!(
        huge, modest,
        "running-only retention is bounded, so a far larger run should not raise the estimate"
    );
    assert!(
        huge < StatsConfig::finite().estimated_retained_bytes(1_000_000),
        "running-only should estimate far less than exact mode for the same run"
    );
}

#[test]
fn finite_config_estimate_is_deterministic_and_usable_for_planning() {
    let config = StatsConfig::finite();
    let probes = 1_000_000;
    let first = config.estimated_retained_bytes(probes);

    assert_eq!(first, config.estimated_retained_bytes(probes));

    // A million probes should land in a range a caller can reason about: well
    // over a megabyte, and well under a terabyte. The exact figure is free to
    // move with the retention model.
    assert!(
        first > 1024 * 1024,
        "a million probes should estimate more than a MiB, got {first}"
    );
    assert!(
        first < 1024 * 1024 * 1024 * 1024,
        "a million probes should estimate less than a TiB, got {first}"
    );
}

#[test]
fn rolling_count_adds_bounded_retention() {
    let probes = 1_000_000;
    let plain = StatsConfig::finite();
    let rolling = StatsConfig {
        rolling_count: Some(10_000),
        ..plain
    };

    let with_rolling = rolling.estimated_retained_bytes(probes);
    assert!(
        with_rolling > plain.estimated_retained_bytes(probes),
        "a count-based rolling window retains events the plain config does not"
    );

    // The window is capped, so a larger run adds nothing further to its share.
    let rolling_share = with_rolling - plain.estimated_retained_bytes(probes);
    let larger = 10 * probes;
    let larger_share =
        rolling.estimated_retained_bytes(larger) - plain.estimated_retained_bytes(larger);
    assert_eq!(
        rolling_share, larger_share,
        "rolling retention should stop growing once the configured count is reached"
    );
}

#[test]
fn time_based_rolling_is_documented_as_excluded() {
    let plain = StatsConfig::finite();
    let timed = StatsConfig {
        rolling_time: Some(Duration::from_secs(10)),
        ..plain
    };

    assert_eq!(
        timed.estimated_retained_bytes(1_000_000),
        plain.estimated_retained_bytes(1_000_000),
        "time-based rolling cannot be bounded from a probe count and is excluded"
    );
}
