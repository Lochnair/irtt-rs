#[derive(Debug, Clone, Copy, PartialEq)]
/// Summary statistics for timing values.
///
/// All fields use nanoseconds except `variance_ns2`, which is nanoseconds
/// squared. Values may represent signed timing quantities such as scheduling
/// error or one-way delay.
pub struct TimeStats {
    /// Number of samples included in this summary.
    pub count: u64,
    /// Sum of all samples, in nanoseconds.
    pub total_ns: i128,
    /// Smallest sample, in nanoseconds.
    pub min_ns: Option<i128>,
    /// Largest sample, in nanoseconds.
    pub max_ns: Option<i128>,
    /// Arithmetic mean, in nanoseconds.
    pub mean_ns: f64,
    /// Median, in nanoseconds, when exact samples were retained.
    pub median_ns: Option<f64>,
    /// Sample variance, in nanoseconds squared.
    pub variance_ns2: f64,
}

impl TimeStats {
    /// Returns the sample standard deviation, in nanoseconds.
    pub fn stddev_ns(&self) -> f64 {
        self.variance_ns2.sqrt()
    }

    /// Returns whether this summary contains no samples.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TimeMetric {
    running: RunningStats,
    samples: Option<Vec<i128>>,
}

impl TimeMetric {
    pub(crate) fn new(retain_samples: bool) -> Self {
        Self {
            running: RunningStats::default(),
            samples: retain_samples.then(Vec::new),
        }
    }

    pub(crate) fn push_ns(&mut self, value: i128) {
        self.running.push(value);
        if let Some(samples) = self.samples.as_mut() {
            samples.push(value);
        }
    }

    pub(crate) fn stats(&self) -> TimeStats {
        self.running
            .stats(self.samples.as_ref().and_then(|samples| median_ns(samples)))
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
struct RunningStats {
    count: u64,
    total_ns: i128,
    min_ns: Option<i128>,
    max_ns: Option<i128>,
    mean_ns: f64,
    m2_ns2: f64,
}

impl RunningStats {
    fn push(&mut self, value: i128) {
        self.count += 1;
        self.total_ns = self.total_ns.saturating_add(value);
        self.min_ns = Some(self.min_ns.map_or(value, |min| min.min(value)));
        self.max_ns = Some(self.max_ns.map_or(value, |max| max.max(value)));
        let x = value as f64;
        let delta = x - self.mean_ns;
        self.mean_ns += delta / self.count as f64;
        let delta2 = x - self.mean_ns;
        self.m2_ns2 += delta * delta2;
    }

    fn stats(&self, median_ns: Option<f64>) -> TimeStats {
        TimeStats {
            count: self.count,
            total_ns: self.total_ns,
            min_ns: self.min_ns,
            max_ns: self.max_ns,
            mean_ns: if self.count == 0 { 0.0 } else { self.mean_ns },
            median_ns,
            variance_ns2: sample_variance(self.count, self.m2_ns2),
        }
    }
}

fn sample_variance(count: u64, m2: f64) -> f64 {
    if count < 2 {
        0.0
    } else {
        m2 / (count - 1) as f64
    }
}

fn median_ns(samples: &[i128]) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let mid = sorted.len() / 2;
    Some(if sorted.len() % 2 == 1 {
        sorted[mid] as f64
    } else {
        (sorted[mid - 1] as f64 + sorted[mid] as f64) / 2.0
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn running_duration_stats_use_sample_variance() {
        let mut metric = TimeMetric::new(false);
        metric.push_ns(1);
        metric.push_ns(2);
        metric.push_ns(3);
        let stats = metric.stats();
        assert_eq!(stats.count, 3);
        assert_eq!(stats.total_ns, 6);
        assert_eq!(stats.min_ns, Some(1));
        assert_eq!(stats.max_ns, Some(3));
        assert_eq!(stats.mean_ns, 2.0);
        assert_eq!(stats.variance_ns2, 1.0);
        assert_eq!(stats.stddev_ns(), 1.0);
    }

    #[test]
    fn exact_median_handles_odd_and_even_samples() {
        assert_eq!(median_ns(&[3, 1, 2]), Some(2.0));
        assert_eq!(median_ns(&[4, 1, 2, 3]), Some(2.5));
        assert_eq!(median_ns(&[-5, 1, 3]), Some(1.0));
        assert_eq!(median_ns(&[-5, 1, 3, 7]), Some(2.0));
    }

    #[test]
    fn single_sample_stddev_is_zero() {
        let mut metric = TimeMetric::new(false);
        metric.push_ns(42);
        let stats = metric.stats();
        assert_eq!(stats.variance_ns2, 0.0);
        assert_eq!(stats.stddev_ns(), 0.0);
    }
}
