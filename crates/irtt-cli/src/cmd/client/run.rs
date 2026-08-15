use std::collections::BTreeMap;
use std::{
    collections::HashSet,
    io::{self, Write},
    sync::atomic::AtomicBool,
    thread,
    time::Duration,
};

use irtt_client::{
    managed::{
        BlockingManagedClient, ManagedClientConfig, ManagedCompletionPolicy, ManagedEndReason,
        ManagedEvent, ManagedEventSubscription, ManagedEventTryRecvError, ManagedTargetEndReason,
        ManagedTargetOutcome, TargetInstance,
    },
    ClientEvent,
};

use super::{
    args::ClientArgs,
    output::{EventRenderStats, OutputConfig},
};

use crate::shared::client::expected_probe_count;
use crate::shared::client::{
    is_shutdown_requested,
    session::{
        drain_managed_events, peer_close_run_error, request_managed_stop_for_peer_close,
        request_managed_stop_once, should_print_final_summary, ManagedDrainState,
    },
};

use irtt_stats::{StatsCollector, StatsConfig};

const FINITE_STATS_BYTES_PER_PROBE: u64 = 500;

const MIB: u64 = 1024 * 1024;

const GIB: u64 = 1024 * MIB;

const FINITE_STATS_MEMORY_WARNING_BYTES: u64 = 128 * MIB;

const FINITE_STATS_MEMORY_STRONG_WARNING_BYTES: u64 = 512 * MIB;

const FINITE_STATS_MEMORY_VERY_STRONG_WARNING_BYTES: u64 = GIB;
const MANAGED_EVENT_WAIT_SLICE: Duration = Duration::from_millis(20);
const MANAGED_EVENT_CAPACITY: usize = 16_384;

pub fn run_stream(
    args: ClientArgs,
    shutdown_requested: &AtomicBool,
) -> Result<(), Box<dyn std::error::Error>> {
    if args.list_columns {
        print!("{}", OutputConfig::list_columns());
        return Ok(());
    }
    let targets = args
        .managed_targets()
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    let multi_target = targets.len() > 1;
    let output_config = OutputConfig::new(
        args.format,
        args.columns.as_deref(),
        args.header,
        args.verbose,
        multi_target,
    )
    .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    let continuous = args.is_continuous();
    let target_count = targets.len();

    if let Some(warning) = finite_stats_memory_warning(&args, target_count) {
        eprintln!("{warning}");
    }
    if is_shutdown_requested(shutdown_requested) {
        return Ok(());
    }
    let config = ManagedClientConfig {
        client: args.to_client_config(),
        pacing: args.pacing.into(),
        completion: ManagedCompletionPolicy::FinishWhenQuiescent,
        event_capacity: MANAGED_EVENT_CAPACITY,
        outcome_history_limit: target_count,
        max_live_target_generations: target_count,
        ..ManagedClientConfig::default()
    };
    let (owner, mut events) = BlockingManagedClient::start_with_subscription(
        config,
        targets
            .iter()
            .map(|target| target.managed.clone())
            .collect(),
    )?;
    let handle = owner.handle();
    let mut stdout = io::LineWriter::new(io::stdout().lock());
    let mut stream_output = StreamOutput {
        config: output_config,
        header_printed: false,
        print_final_summary: false,
        show_running_only_summary_note: false,
        out: &mut stdout,
    };

    let mut stats = targets
        .iter()
        .map(|target| {
            (
                target.label.clone(),
                StatsCollector::new(stats_config(continuous)),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut terminal_targets = HashSet::new();
    let mut dropped_events = 0_u64;
    let mut interrupted = false;
    let mut stop_requested = false;
    let mut subscription_closed = false;
    loop {
        if is_shutdown_requested(shutdown_requested) {
            interrupted = true;
            if request_managed_stop_once(&mut stop_requested) {
                drop(handle.stop());
            }
        }
        if request_managed_stop_for_peer_close(
            continuous,
            interrupted,
            handle.status().peer_closed_target_outcomes,
            &mut stop_requested,
        ) {
            drop(handle.stop());
        }
        let drain_state = drain_events(
            &mut events,
            &mut stream_output,
            &mut stats,
            &mut terminal_targets,
            &mut dropped_events,
        )?;
        match drain_state {
            ManagedDrainState::Empty => thread::sleep(MANAGED_EVENT_WAIT_SLICE),
            ManagedDrainState::Closed => subscription_closed = true,
            ManagedDrainState::BudgetExhausted => {}
        }
        if handle.status().final_outcome.is_some() || subscription_closed {
            break;
        }
    }
    if interrupted {
        eprintln!("interrupted, closing managed run...");
    }
    let outcome = owner.join()?;
    drain_final_events(
        &mut events,
        &mut stream_output,
        &mut stats,
        &mut terminal_targets,
        &mut dropped_events,
    )?;
    if let Some(warning) = dropped_event_warning(dropped_events) {
        eprintln!("{warning}");
    }
    if outcome.discarded_target_outcomes != 0 {
        eprintln!(
            "irtt-rs: warning: {} final target outcomes were discarded",
            outcome.discarded_target_outcomes
        );
    }
    for target in outcome.recent_target_outcomes.iter() {
        if terminal_targets.insert(target.target.clone()) {
            report_target_failure(target);
        }
    }
    interrupted |= is_shutdown_requested(shutdown_requested);
    let terminal_error = match &outcome.end_reason {
        ManagedEndReason::DriverFailed(failure) => {
            Some(format!("managed driver failed: {failure}"))
        }
        _ => peer_close_run_error(continuous, interrupted, outcome.peer_closed_target_outcomes)
            .or_else(|| {
                (!interrupted
                    && outcome.successful_target_outcomes == 0
                    && outcome.failed_target_outcomes > 0)
                    .then(|| {
                        format!(
                            "no managed target completed successfully ({} failed)",
                            outcome.failed_target_outcomes
                        )
                    })
            }),
    };
    stream_output.print_final_summary = should_print_final_summary(continuous, interrupted);
    stream_output.show_running_only_summary_note =
        continuous && interrupted && stream_output.print_final_summary;
    for (label, stats) in &stats {
        if multi_target
            && stream_output.print_final_summary
            && stream_output.config.prints_summary()
        {
            writeln!(stream_output.out)?;
            writeln!(stream_output.out, "target: {label}")?;
        }
        stream_output.print_summary(stats)?;
    }
    stream_output.out.flush()?;
    if let Some(error) = terminal_error {
        return Err(error.into());
    }
    Ok(())
}

fn process_event<W: Write>(
    event: ManagedEvent,
    stream_output: &mut StreamOutput<'_, W>,
    stats: &mut BTreeMap<String, StatsCollector>,
    terminal_targets: &mut HashSet<TargetInstance>,
) -> io::Result<()> {
    match event {
        ManagedEvent::Client { target, event } => {
            let collector = stats
                .entry(target.id.as_str().to_owned())
                .or_insert_with(|| StatsCollector::new(stats_config(false)));
            print_events_with_stats(
                stream_output,
                std::slice::from_ref(&event),
                Some(target.id.as_str()),
                collector,
            )?;
        }
        ManagedEvent::TargetFinished { outcome } => {
            terminal_targets.insert(outcome.target.clone());
            report_target_failure(&outcome);
        }
        _ => {}
    }
    Ok(())
}

fn drain_events<W: Write>(
    events: &mut ManagedEventSubscription,
    stream_output: &mut StreamOutput<'_, W>,
    stats: &mut BTreeMap<String, StatsCollector>,
    terminal_targets: &mut HashSet<TargetInstance>,
    dropped_events: &mut u64,
) -> io::Result<ManagedDrainState> {
    drain_managed_events(events, dropped_events, |event| {
        process_event(event, stream_output, stats, terminal_targets)
    })
}

fn drain_final_events<W: Write>(
    events: &mut ManagedEventSubscription,
    stream_output: &mut StreamOutput<'_, W>,
    stats: &mut BTreeMap<String, StatsCollector>,
    terminal_targets: &mut HashSet<TargetInstance>,
    dropped_events: &mut u64,
) -> io::Result<()> {
    loop {
        match events.try_recv() {
            Ok(event) => process_event(event, stream_output, stats, terminal_targets)?,
            Err(ManagedEventTryRecvError::Empty | ManagedEventTryRecvError::Closed) => break,
            Err(ManagedEventTryRecvError::Lagged(count)) => {
                *dropped_events = dropped_events.saturating_add(count);
            }
        }
    }
    Ok(())
}

fn report_target_failure(target: &ManagedTargetOutcome) {
    for message in target_failure_messages(target) {
        eprintln!("{message}");
    }
}

fn target_failure_messages(target: &ManagedTargetOutcome) -> Vec<String> {
    let mut messages = Vec::with_capacity(2);
    if let ManagedTargetEndReason::Failed(failure) = &target.end_reason {
        messages.push(format!(
            "irtt-rs: target {} failed ({} {}): {}",
            target.target.id, failure.phase, failure.kind, failure.message
        ));
    }
    if let Some(failure) = &target.cleanup_failure {
        messages.push(format!(
            "irtt-rs: target {} cleanup failed ({} {}): {}",
            target.target.id, failure.phase, failure.kind, failure.message
        ));
    }
    messages
}
fn dropped_event_warning(dropped_events: u64) -> Option<String> {
    (dropped_events > 0).then(|| format!("irtt-rs: warning: dropped {dropped_events} managed run event{}; output and statistics may be incomplete", if dropped_events == 1 { "" } else { "s" }))
}

struct StreamOutput<'a, W: Write> {
    config: OutputConfig,
    header_printed: bool,
    print_final_summary: bool,
    show_running_only_summary_note: bool,
    out: &'a mut W,
}
impl<W: Write> StreamOutput<'_, W> {
    fn print_events(
        &mut self,
        events: &[ClientEvent],
        target: Option<&str>,
        stats_updates: &[EventRenderStats],
    ) -> io::Result<()> {
        self.print_header()?;
        for (event, stats_update) in events.iter().zip(stats_updates) {
            if let Some(line) = self.config.render_event(event, target, Some(stats_update)) {
                writeln!(self.out, "{line}")?;
            }
        }
        Ok(())
    }
    fn print_header(&mut self) -> io::Result<()> {
        if self.header_printed {
            return Ok(());
        }
        self.header_printed = true;
        if let Some(header) = self.config.render_header() {
            writeln!(self.out, "{header}")?;
        }
        Ok(())
    }

    fn print_summary(&mut self, stats: &StatsCollector) -> io::Result<()> {
        if self.print_final_summary && self.config.prints_summary() {
            write!(
                self.out,
                "{}",
                crate::cmd::client::summary::format_summary_with_options(
                    &stats.snapshot(),
                    crate::cmd::client::summary::SummaryFormatOptions {
                        verbose: self.config.summary_verbose(),
                        show_running_only_note: self.show_running_only_summary_note
                    }
                )
            )?;
        }
        Ok(())
    }
}

fn print_events_with_stats<W: Write>(
    stream_output: &mut StreamOutput<'_, W>,
    events: &[ClientEvent],
    target: Option<&str>,
    stats: &mut StatsCollector,
) -> io::Result<()> {
    let updates = events
        .iter()
        .map(|event| EventRenderStats::from(stats.process(event)))
        .collect::<Vec<_>>();
    stream_output.print_events(events, target, &updates)
}

fn stats_config(continuous: bool) -> StatsConfig {
    if continuous {
        StatsConfig::continuous()
    } else {
        StatsConfig::finite()
    }
}
fn finite_stats_memory_warning(args: &ClientArgs, target_count: usize) -> Option<String> {
    if args.is_continuous() || args.duration.is_zero() {
        return None;
    }
    let target_count = u64::try_from(target_count).unwrap_or(u64::MAX);
    let estimated_bytes = expected_probe_count(args.duration, args.interval)
        .saturating_mul(FINITE_STATS_BYTES_PER_PROBE)
        .saturating_mul(target_count);
    if estimated_bytes < FINITE_STATS_MEMORY_WARNING_BYTES {
        return None;
    }
    let formatted = if estimated_bytes >= GIB {
        format!("{} GiB", estimated_bytes.saturating_add(GIB / 2) / GIB)
    } else {
        format!("{} MiB", estimated_bytes.saturating_add(MIB / 2) / MIB)
    };
    let guidance = if estimated_bytes >= FINITE_STATS_MEMORY_VERY_STRONG_WARNING_BYTES {
        "this may be unsuitable on memory-constrained systems"
    } else if estimated_bytes >= FINITE_STATS_MEMORY_STRONG_WARNING_BYTES {
        "consider shortening the run, increasing the interval, or using continuous mode"
    } else {
        "use continuous mode for bounded-memory long-running tests"
    };
    Some(format!("irtt-rs: warning: finite exact statistics may retain about {formatted} for this run; {guidance}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use irtt_client::managed::{
        ManagedTargetFailure, ManagedTargetFailureKind, ManagedTargetFailurePhase, TargetId,
    };
    use std::sync::Arc;

    #[test]
    fn finite_stats_memory_warning_accounts_for_target_count() {
        use clap::Parser;

        let args = ClientArgs::try_parse_from([
            "irtt-cli",
            "--duration",
            "4200s",
            "--interval",
            "100ms",
            "127.0.0.1:2112",
        ])
        .unwrap();

        assert!(
            finite_stats_memory_warning(&args, 1).is_none(),
            "a single target's estimate should stay below the warning threshold"
        );
        assert!(
            finite_stats_memory_warning(&args, 7).is_some(),
            "seven targets' aggregate estimate should cross the warning threshold"
        );
    }

    #[test]
    fn finite_stats_memory_warning_saturates_target_count_multiplication() {
        use clap::Parser;

        let args = ClientArgs::try_parse_from([
            "irtt-cli",
            "--duration",
            "4200s",
            "--interval",
            "100ms",
            "127.0.0.1:2112",
        ])
        .unwrap();

        assert!(finite_stats_memory_warning(&args, usize::MAX).is_some());
    }

    #[test]
    fn lagged_events_are_counted_saturatingly() {
        let mut dropped = 0_u64;
        dropped = dropped.saturating_add(7);
        dropped = dropped.saturating_add(3);
        assert_eq!(dropped, 10);
        assert_eq!(u64::MAX.saturating_add(1), u64::MAX);
        assert!(dropped_event_warning(dropped)
            .unwrap()
            .contains("dropped 10 managed run events"));
    }

    #[test]
    fn target_failure_diagnostics_include_primary_and_cleanup_failures() {
        let target = ManagedTargetOutcome {
            target: TargetInstance {
                id: TargetId::from("edge"),
                generation: 1,
            },
            server_addr: Arc::from("127.0.0.1:2112"),
            remote: None,
            end_reason: ManagedTargetEndReason::Failed(ManagedTargetFailure {
                phase: ManagedTargetFailurePhase::Receiving,
                kind: ManagedTargetFailureKind::Protocol,
                message: "primary failure".into(),
            }),
            packets_sent: 0,
            replies_received: 0,
            duplicates: 0,
            late: 0,
            warning_events: 0,
            cleanup_failure: Some(ManagedTargetFailure {
                phase: ManagedTargetFailurePhase::Closing,
                kind: ManagedTargetFailureKind::Socket,
                message: "cleanup failure".into(),
            }),
        };

        assert_eq!(
            target_failure_messages(&target),
            [
                "irtt-rs: target edge failed (receiving protocol): primary failure",
                "irtt-rs: target edge cleanup failed (closing socket): cleanup failure",
            ]
        );
    }
}
