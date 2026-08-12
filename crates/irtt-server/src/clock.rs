//! Clock sampling for echo timestamps.
//!
//! An echo reply reports when the server received the request and when it sent
//! the answer, so the core needs a clock. It takes one through this private
//! seam rather than reading the system clock inline, so that timestamp behavior
//! is deterministically testable without a runtime, sleeps or timing
//! tolerances.
//!
//! This is the clock counterpart of [`TokenSource`](crate::token::TokenSource)
//! and nothing more. It is **not** a runtime abstraction, not a transport
//! boundary and not a pluggable product API: it is crate-private, the public
//! constructor still takes only a [`ServerConfig`](crate::ServerConfig), and
//! callers never pass timestamps into the core.

use std::{
    fmt,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// One instant, read from both clock domains together.
///
/// Both fields are signed nanoseconds, matching the wire encoding: `wall_ns`
/// counts from the Unix epoch, and `mono_ns` from an origin the clock source
/// owns. The specification lets a server pick any monotonic origin as long as
/// it is stable for as long as any session it stamps may live, so `mono_ns` is
/// deliberately not process uptime or anything else externally meaningful —
/// only the difference between two samples from the same source has meaning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ClockSample {
    pub(crate) wall_ns: i64,
    pub(crate) mono_ns: i64,
}

impl ClockSample {
    /// The arithmetic mean of two samples, taken separately per clock domain.
    ///
    /// This is what a midpoint timestamp is: the mean of one reply's receive
    /// and send instants. The two domains never mix, and the mean goes through
    /// `i128` because `a + b` can overflow `i64` while the mean itself never
    /// can.
    pub(crate) fn midpoint(self, other: Self) -> Self {
        Self {
            wall_ns: mean_ns(self.wall_ns, other.wall_ns),
            mono_ns: mean_ns(self.mono_ns, other.mono_ns),
        }
    }

    /// This sample, held back in any domain where it runs ahead of `later`.
    ///
    /// A reply's receive instant must not be later than its send instant. The
    /// monotonic domain gets that from its source, which only moves forward.
    /// The wall clock does not: it can be stepped backwards between two
    /// readings by NTP, a hypervisor or an administrator, which would otherwise
    /// invert the pair.
    ///
    /// The *earlier* reading is the one that moves, so the pair settles on the
    /// clock as it now stands rather than on a value the clock has already
    /// disowned. Nothing is remembered between calls: a latch that carried a
    /// pre-step value forward would keep reporting a wall time the host has
    /// corrected away, which is the smoothing across packets the specification
    /// forbids — and it would make one-way delays wrong for as long as the
    /// latch held, rather than for one reply.
    pub(crate) fn not_after(self, later: Self) -> Self {
        Self {
            wall_ns: self.wall_ns.min(later.wall_ns),
            mono_ns: self.mono_ns.min(later.mono_ns),
        }
    }
}

/// A source of clock samples.
///
/// Deliberately private, like [`TokenSource`](crate::token::TokenSource): it
/// exists so tests can script the receive and send instants of a reply, not as
/// an extension point. The production implementation is [`SystemClock`].
///
/// Sampling is infallible. A clock that could refuse would make an admitted
/// echo fail for a reason that is neither the peer's fault nor recoverable, so
/// the production source saturates rather than reporting an error.
pub(crate) trait ClockSource: fmt::Debug + Send {
    fn sample(&mut self) -> ClockSample;
}

/// Reads the wall clock from [`SystemTime`] and the monotonic clock from an
/// [`Instant`] captured when the source was created.
///
/// That instant is the monotonic origin, so it is fixed for the life of the
/// source and shared by every session the core using it holds — which is
/// exactly the stability the specification asks for.
#[derive(Debug)]
pub(crate) struct SystemClock {
    origin: Instant,
}

impl SystemClock {
    pub(crate) fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl ClockSource for SystemClock {
    fn sample(&mut self) -> ClockSample {
        ClockSample {
            wall_ns: wall_ns(),
            mono_ns: saturating_ns(self.origin.elapsed()),
        }
    }
}

/// Nanoseconds since the Unix epoch, negative before it.
fn wall_ns() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(since_epoch) => saturating_ns(since_epoch),
        Err(before_epoch) => saturating_ns(before_epoch.duration()).saturating_neg(),
    }
}

/// A duration as nanoseconds, saturating rather than panicking.
///
/// [`Duration`] counts nanoseconds in `u128` and reaches far beyond `i64`,
/// which the wire field is. A host clock set past the year 2262 is a local
/// misconfiguration; it must not become a panic that a remote peer can reach by
/// sending an echo request.
fn saturating_ns(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
}

/// The mean of two nanosecond values, computed without overflowing.
fn mean_ns(a: i64, b: i64) -> i64 {
    let mean = (i128::from(a) + i128::from(b)) / 2;
    // The mean of two `i64` values is always an `i64`, so this conversion
    // cannot actually fail. Clamping rather than unwrapping keeps timestamp
    // arithmetic free of any panic at all.
    i64::try_from(mean).unwrap_or(if mean.is_negative() {
        i64::MIN
    } else {
        i64::MAX
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Constructed values only: the production clock's real readings are not
    /// assertable without a timing tolerance, and the behavioral echo tests use
    /// a scripted source instead.
    #[test]
    fn a_midpoint_is_the_per_domain_mean_and_cannot_overflow() {
        let receive = ClockSample {
            wall_ns: 1_000,
            mono_ns: 10_000,
        };
        let send = ClockSample {
            wall_ns: 1_200,
            mono_ns: 10_300,
        };
        assert_eq!(
            receive.midpoint(send),
            ClockSample {
                wall_ns: 1_100,
                mono_ns: 10_150,
            }
        );

        // The sum overflows `i64`; the mean does not.
        let high = ClockSample {
            wall_ns: i64::MAX,
            mono_ns: i64::MAX,
        };
        assert_eq!(
            high.midpoint(high),
            ClockSample {
                wall_ns: i64::MAX,
                mono_ns: i64::MAX,
            }
        );
        let low = ClockSample {
            wall_ns: i64::MIN,
            mono_ns: i64::MIN,
        };
        assert_eq!(
            low.midpoint(low),
            ClockSample {
                wall_ns: i64::MIN,
                mono_ns: i64::MIN,
            }
        );
        assert_eq!(
            high.midpoint(low),
            ClockSample {
                wall_ns: 0,
                mono_ns: 0,
            }
        );
    }

    #[test]
    fn a_duration_beyond_the_wire_field_saturates_rather_than_panicking() {
        assert_eq!(saturating_ns(Duration::from_nanos(1_500)), 1_500);
        assert_eq!(saturating_ns(Duration::MAX), i64::MAX);
    }
}
