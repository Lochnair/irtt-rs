//! Scalar presentation vocabulary shared by the CLI frontends.
//!
//! Stream output, the run summary, and the TUI display different metrics in
//! different layouts, but they render the same *scalars*: nanosecond-scale
//! durations, percentages, and counts. This module owns those spellings so the
//! frontends cannot drift apart on units, rounding, or how an absent value is
//! rendered.
//!
//! It is presentation only. Nothing here belongs in `irtt-stats`, and it is
//! deliberately not a general formatting framework: it holds exactly the
//! vocabulary more than one frontend needs.

use std::time::Duration;

#[cfg(feature = "client")]
use irtt_client::SignedDuration;

/// Rendering used wherever a scalar is unavailable.
pub(crate) const ABSENT: &str = "-";

/// Format a nanosecond scalar.
///
/// Unit selection and rounding are one policy for every frontend:
///
/// | magnitude | unit | precision |
/// |-----------|------|-----------|
/// | `< 1µs`   | `ns` | whole     |
/// | `< 1ms`   | `µs` | 1 decimal |
/// | `< 1s`    | `ms` | 1 decimal |
/// | `>= 1s`   | `s`  | 3 decimals |
///
/// Negative values are meaningful (clock offsets, one-way deltas, IPDV), so
/// the unit is selected from the magnitude and the sign is preserved. Values
/// that are not finite render as [`ABSENT`].
pub(crate) fn format_ns_f64(value: f64) -> String {
    if !value.is_finite() {
        return ABSENT.to_owned();
    }
    let sign = if value.is_sign_negative() { "-" } else { "" };
    let magnitude = value.abs();
    if magnitude < 1_000.0 {
        format!("{sign}{magnitude:.0}ns")
    } else if magnitude < 1_000_000.0 {
        format!("{sign}{:.1}µs", magnitude / 1_000.0)
    } else if magnitude < 1_000_000_000.0 {
        format!("{sign}{:.1}ms", magnitude / 1_000_000.0)
    } else {
        format!("{sign}{:.3}s", magnitude / 1_000_000_000.0)
    }
}

/// Format an exact nanosecond scalar with [`format_ns_f64`]'s policy.
pub(crate) fn format_ns_i128(value: i128) -> String {
    let sign = if value < 0 { "-" } else { "" };
    let magnitude = value.saturating_abs() as f64;
    format!("{sign}{}", format_ns_f64(magnitude))
}

/// Format an optional nanosecond scalar, rendering `None` as [`ABSENT`].
pub(crate) fn format_optional_ns_i128(value: Option<i128>) -> String {
    value.map(format_ns_i128).unwrap_or_else(absent)
}

/// Format an optional nanosecond scalar, rendering `None` as [`ABSENT`].
#[cfg(feature = "client")]
pub(crate) fn format_optional_ns_f64(value: Option<f64>) -> String {
    value.map(format_ns_f64).unwrap_or_else(absent)
}

/// Format a duration as a nanosecond scalar.
pub(crate) fn format_duration(value: Duration) -> String {
    format_ns_i128(i128::try_from(value.as_nanos()).unwrap_or(i128::MAX))
}

/// Format a signed duration as a nanosecond scalar, preserving its sign.
#[cfg(feature = "client")]
pub(crate) fn format_signed_duration(value: SignedDuration) -> String {
    format_ns_i128(value.as_nanos())
}

/// Format a percentage with two decimals; non-finite values render as
/// [`ABSENT`].
pub(crate) fn format_percent(value: f64) -> String {
    if value.is_finite() {
        format!("{value:.2}%")
    } else {
        ABSENT.to_owned()
    }
}

/// Format `value` as a percentage of `total`, capped at 100%.
///
/// A zero total has no percentage to report and renders as [`ABSENT`].
#[cfg(feature = "tui")]
pub(crate) fn format_percent_ratio(value: u64, total: u64) -> String {
    if total == 0 {
        ABSENT.to_owned()
    } else {
        format_percent((value as f64 / total as f64 * 100.0).min(100.0))
    }
}

/// Format a scalar count.
#[cfg(feature = "tui")]
pub(crate) fn format_count(value: u64) -> String {
    value.to_string()
}

/// Format an optional scalar count, rendering `None` as [`ABSENT`].
#[cfg(feature = "tui")]
pub(crate) fn format_optional_count(value: Option<u64>) -> String {
    value.map(format_count).unwrap_or_else(absent)
}

fn absent() -> String {
    ABSENT.to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nanosecond_scalars_select_units_at_the_defined_boundaries() {
        assert_eq!(format_ns_f64(0.0), "0ns");
        assert_eq!(format_ns_f64(999.0), "999ns");
        assert_eq!(format_ns_f64(1_000.0), "1.0µs");
        assert_eq!(format_ns_f64(999_999.0), "1000.0µs");
        assert_eq!(format_ns_f64(1_000_000.0), "1.0ms");
        assert_eq!(format_ns_f64(999_999_999.0), "1000.0ms");
        assert_eq!(format_ns_f64(1_000_000_000.0), "1.000s");
        assert_eq!(format_ns_f64(1_500_000_000.0), "1.500s");
    }

    #[test]
    fn negative_nanosecond_scalars_keep_their_sign() {
        assert_eq!(format_ns_f64(-500.0), "-500ns");
        assert_eq!(format_ns_f64(-1_500.0), "-1.5µs");
        assert_eq!(format_ns_f64(-2_500_000.0), "-2.5ms");
        assert_eq!(format_ns_f64(-1_000_000_000.0), "-1.000s");
        assert_eq!(format_ns_i128(-1_500), "-1.5µs");
    }

    #[test]
    fn exact_and_floating_nanosecond_scalars_agree() {
        for value in [0_i128, 1, 999, 1_000, 1_234_567, -1_234_567, 5_000_000_000] {
            assert_eq!(format_ns_i128(value), format_ns_f64(value as f64));
        }
    }

    #[test]
    fn microseconds_use_the_micro_sign() {
        assert!(format_ns_f64(1_500.0).ends_with("µs"));
        assert!(!format_ns_f64(1_500.0).contains("us"));
    }

    #[test]
    fn durations_render_as_nanosecond_scalars() {
        assert_eq!(format_duration(Duration::from_millis(25)), "25.0ms");
        assert_eq!(format_duration(Duration::from_secs(1)), "1.000s");
    }

    #[test]
    #[cfg(feature = "client")]
    fn signed_durations_render_as_nanosecond_scalars() {
        assert_eq!(
            format_signed_duration(SignedDuration::from_nanos(-1_500)),
            "-1.5µs"
        );
    }

    #[test]
    fn absent_and_non_finite_scalars_render_as_a_placeholder() {
        assert_eq!(format_optional_ns_i128(None), ABSENT);
        assert_eq!(format_ns_f64(f64::NAN), ABSENT);
        assert_eq!(format_ns_f64(f64::INFINITY), ABSENT);
        assert_eq!(format_percent(f64::NAN), ABSENT);
    }

    #[test]
    #[cfg(feature = "client")]
    fn absent_optional_ns_f64_renders_as_a_placeholder() {
        assert_eq!(format_optional_ns_f64(None), ABSENT);
    }

    #[test]
    #[cfg(feature = "tui")]
    fn absent_counts_and_ratios_render_as_a_placeholder() {
        assert_eq!(format_optional_count(None), ABSENT);
        assert_eq!(format_percent_ratio(1, 0), ABSENT);
    }

    #[test]
    fn percentages_have_one_spelling() {
        assert_eq!(format_percent(0.0), "0.00%");
        assert_eq!(format_percent(12.345), "12.35%");
    }

    #[test]
    #[cfg(feature = "tui")]
    fn percent_ratios_and_counts_have_one_spelling() {
        assert_eq!(format_percent_ratio(1, 4), "25.00%");
        assert_eq!(format_percent_ratio(5, 4), "100.00%");
        assert_eq!(format_count(42), "42");
        assert_eq!(format_optional_count(Some(0)), "0");
    }
}
