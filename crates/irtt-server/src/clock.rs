//! Clock sampling for echo timestamps.
//!
//! The public [`ServerCore::handle_datagram`](crate::ServerCore::handle_datagram)
//! entry point takes no timestamp: the core samples every instant it reports.
//! The Tokio runtime uses a crate-private entry point that may additionally hand
//! over a kernel-observed wall receive time for one datagram, which this module
//! contributes only the [`wall_ns_of`] conversion for. Whether such an
//! observation is usable, and which reply field it may reach, is measurement
//! policy and lives in the core — this stays the module that reads clocks, not
//! one that models transport metadata. A [`ClockSample`] remains one paired
//! userspace instant and never carries a kernel reading.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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

impl SystemClock {
    pub(crate) fn sample(&mut self) -> ClockSample {
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

/// An observed instant as nanoseconds since the Unix epoch, or `None` when it
/// does not fit the wire's signed-nanosecond field.
///
/// This is the conversion for an instant the server did **not** read from its
/// own clock — today, a kernel receive timestamp the transport
/// observed. It reports unrepresentability rather than saturating, because a
/// clamped instant is indistinguishable from a real one at the boundary and
/// would then be compared against a genuine sample as though it were plausible.
/// A local reading has no such problem and keeps [`saturating_ns`].
///
/// The negation cannot fail for any [`SystemTime`] a host can hold: it is
/// reachable only from a value already inside `i64`, and only `i64::MIN` has no
/// negation. It is checked anyway so that no path here can panic.
pub(crate) fn wall_ns_of(at: SystemTime) -> Option<i64> {
    match at.duration_since(UNIX_EPOCH) {
        Ok(since_epoch) => i64::try_from(since_epoch.as_nanos()).ok(),
        Err(before_epoch) => i64::try_from(before_epoch.duration().as_nanos())
            .ok()
            .and_then(i64::checked_neg),
    }
}

/// A duration as nanoseconds, saturating rather than panicking.
///
/// This is the crate's one [`Duration`] → wire-nanoseconds conversion, used for
/// clock readings and for the configured intervals and deadlines that
/// negotiation and lifetime policy compare against them. Everything on both
/// sides is signed nanoseconds, so there is no second rule anywhere.
///
/// [`Duration`] counts nanoseconds in `u128` and reaches far beyond `i64`,
/// which the wire field is. A host clock set past the year 2262, or an operator
/// configuring a millennium-long idle timeout, is a local matter; neither may
/// become a panic that a remote peer can reach by sending a packet. Saturating
/// loses nothing that could have been honored, since no wire interval or
/// duration can express more than `i64::MAX` nanoseconds either.
pub(crate) fn saturating_ns(duration: Duration) -> i64 {
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
