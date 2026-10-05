use std::{
    collections::{BTreeMap, BTreeSet, HashSet, VecDeque},
    io::{self, BufRead, Write},
    sync::Arc,
    thread,
};

use irtt_client::{
    managed::{
        ManagedClient, ManagedCommandApplyError, ManagedEndReason, ManagedEvent,
        ManagedEventSubscription, ManagedEventTryRecvError, ManagedStatus, ManagedTargetConfig,
        ManagedTargetEndReason, ManagedTargetOutcome, TargetInstance,
    },
    ClientEvent,
};

use super::{
    args::ClientArgs,
    output::{EventRenderStats, OutputConfig},
    worker::ManagedWorker,
};

use crate::shared::client::{
    expected_probe_count, parse_stdin_target_set,
    session::{
        peer_close_run_error, request_managed_stop_for_peer_close, request_managed_stop_once,
        should_print_final_summary,
    },
    ManagedRunSetup, STDIN_MAX_DESIRED_TARGETS, STDIN_OUTCOME_HISTORY_LIMIT,
};

use irtt_stats::{StatsCollector, StatsConfig};

const MIB: u64 = 1024 * 1024;

const GIB: u64 = 1024 * MIB;

const FINITE_STATS_MEMORY_WARNING_BYTES: u64 = 128 * MIB;

const FINITE_STATS_MEMORY_STRONG_WARNING_BYTES: u64 = 512 * MIB;

const FINITE_STATS_MEMORY_VERY_STRONG_WARNING_BYTES: u64 = GIB;
const MAX_STDIN_RECORD_BYTES: usize = 64 * 1024;

pub async fn run_stream(
    args: ClientArgs,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), Box<dyn std::error::Error>> {
    if args.list_columns {
        print!("{}", OutputConfig::list_columns());
        return Ok(());
    }
    let setup = args
        .prepare()
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    let multi_target = setup.is_multi_target();
    let output_config = OutputConfig::new(
        args.format,
        args.columns.as_deref(),
        args.header,
        args.verbose,
    )
    .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    if setup.stdin_controlled {
        return run_stdin_stream(setup, output_config, shutdown).await;
    }
    let continuous = args.is_continuous();
    let target_count = setup.target_count();

    if let Some(warning) = finite_stats_memory_warning(&args, target_count) {
        eprintln!("{warning}");
    }
    if *shutdown.borrow() {
        return Ok(());
    }
    let (task, handle) = ManagedClient::task(setup.managed_config(), setup.managed_targets())?;
    let mut events = handle.subscribe()?;
    let mut status = handle.subscribe_status();
    let owner = ManagedWorker::start(task, handle.clone())?;
    let mut stdout = io::LineWriter::new(io::stdout().lock());
    let mut stream_output = StreamOutput {
        config: output_config,
        header_printed: false,
        print_final_summary: false,
        show_running_only_summary_note: false,
        out: &mut stdout,
    };

    let mut stats = setup
        .targets
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
    loop {
        let snapshot = status.borrow_and_update().clone();
        if *shutdown.borrow() {
            interrupted = true;
            if request_managed_stop_once(&mut stop_requested) {
                drop(handle.stop());
            }
        }
        if request_managed_stop_for_peer_close(
            continuous,
            interrupted,
            snapshot.peer_closed_target_outcomes,
            &mut stop_requested,
        ) {
            drop(handle.stop());
        }
        if snapshot.final_outcome.is_some() {
            break;
        }
        tokio::select! {
            _ = shutdown.changed(), if !interrupted => {}
            changed = status.changed() => { if changed.is_err() { break; } }
            event = events.recv() => match event {
                Ok(event) => process_event(event, &mut stream_output, &mut stats, &mut terminal_targets)?,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
                    dropped_events = dropped_events.saturating_add(count);
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    }
    if interrupted {
        eprintln!("interrupted, closing managed run...");
    }
    let outcome = owner.join().await?;
    drain_final_events(&mut events, &mut dropped_events, |event| {
        process_event(event, &mut stream_output, &mut stats, &mut terminal_targets)
    })?;
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
    interrupted |= *shutdown.borrow();
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
    for target in &setup.targets {
        let target_stats = stats.get(&target.label).expect(
            "every prepared target must have a stats collector inserted before the run begins",
        );
        if multi_target
            && stream_output.print_final_summary
            && stream_output.config.prints_summary()
        {
            writeln!(stream_output.out)?;
            writeln!(stream_output.out, "target: {}", target.label)?;
        }
        stream_output.print_summary(target_stats)?;
    }
    stream_output.out.flush()?;
    if let Some(error) = terminal_error {
        return Err(error.into());
    }
    Ok(())
}

#[derive(Clone)]
enum StdinUpdate {
    Targets(Vec<ManagedTargetConfig>),
    Stop(StdinStop),
}

fn read_stdin_record<R: BufRead>(
    reader: &mut R,
    buffer: &mut Vec<u8>,
) -> io::Result<Option<String>> {
    buffer.clear();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            if buffer.is_empty() {
                return Ok(None);
            }
            if buffer.len() > MAX_STDIN_RECORD_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "stdin target record exceeds the maximum size",
                ));
            }
            return std::str::from_utf8(buffer)
                .map(str::to_owned)
                .map(Some)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "stdin is not UTF-8"));
        }

        if buffer.len() == MAX_STDIN_RECORD_BYTES + 1 {
            if available.first() == Some(&b'\n') && buffer.last() == Some(&b'\r') {
                reader.consume(1);
                buffer.pop();
                return std::str::from_utf8(buffer)
                    .map(str::to_owned)
                    .map(Some)
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "stdin is not UTF-8"));
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "stdin target record exceeds the maximum size",
            ));
        }

        if let Some(newline) = available.iter().position(|byte| *byte == b'\n') {
            let before_newline = &available[..newline];
            if buffer.len().saturating_add(before_newline.len()) > MAX_STDIN_RECORD_BYTES + 1 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "stdin target record exceeds the maximum size",
                ));
            }
            buffer.extend_from_slice(before_newline);
            reader.consume(newline + 1);
            if buffer.last() == Some(&b'\r') {
                buffer.pop();
            }
            if buffer.len() > MAX_STDIN_RECORD_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "stdin target record exceeds the maximum size",
                ));
            }
            return std::str::from_utf8(buffer)
                .map(str::to_owned)
                .map(Some)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "stdin is not UTF-8"));
        }

        let remaining = MAX_STDIN_RECORD_BYTES + 1 - buffer.len();
        let take = available.len().min(remaining);
        buffer.extend_from_slice(&available[..take]);
        reader.consume(take);
        if buffer.len() > MAX_STDIN_RECORD_BYTES && buffer.last() != Some(&b'\r') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "stdin target record exceeds the maximum size",
            ));
        }
    }
}

fn read_stdin_target_sets<R: BufRead>(
    reader: &mut R,
    updates: &tokio::sync::watch::Sender<StdinUpdate>,
) {
    let mut line = 0_u64;
    let mut record = Vec::with_capacity(MAX_STDIN_RECORD_BYTES + 1);
    loop {
        match read_stdin_record(reader, &mut record) {
            Ok(Some(record)) => {
                line = line.saturating_add(1);
                if record.is_empty() {
                    continue;
                }
                match parse_stdin_target_set(&record, STDIN_MAX_DESIRED_TARGETS) {
                    Ok(targets) => {
                        updates.send_replace(StdinUpdate::Targets(
                            targets.into_iter().map(|target| target.managed).collect(),
                        ));
                    }
                    Err(error) => {
                        updates.send_replace(StdinUpdate::Stop(StdinStop::Fatal(format!(
                            "invalid --targets-stdin line {line}: {error}"
                        ))));
                        return;
                    }
                }
            }
            Ok(None) => {
                updates.send_replace(StdinUpdate::Stop(StdinStop::Eof));
                return;
            }
            Err(_) => {
                updates.send_replace(StdinUpdate::Stop(StdinStop::Fatal(format!(
                    "failed to read --targets-stdin line {}",
                    line + 1
                ))));
                return;
            }
        }
    }
}

fn spawn_stdin_target_reader(updates: tokio::sync::watch::Sender<StdinUpdate>) -> io::Result<()> {
    thread::Builder::new()
        .name("irtt-targets-stdin".to_owned())
        .spawn(move || {
            let stdin = io::stdin();
            read_stdin_target_sets(&mut stdin.lock(), &updates);
        })
        .map(|_| ())
}

struct BoundedTargetSet {
    limit: usize,
    order: VecDeque<TargetInstance>,
    members: HashSet<TargetInstance>,
}

impl BoundedTargetSet {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            order: VecDeque::with_capacity(limit),
            members: HashSet::with_capacity(limit),
        }
    }

    fn insert(&mut self, target: TargetInstance) -> bool {
        if !self.members.insert(target.clone()) {
            return false;
        }
        if self.order.len() == self.limit {
            let evicted = self
                .order
                .pop_front()
                .expect("bounded target order is nonempty");
            self.members.remove(&evicted);
        }
        self.order.push_back(target);
        true
    }
}

fn process_stdin_event<W: Write>(
    event: ManagedEvent,
    stream_output: &mut StreamOutput<'_, W>,
    stats: &mut BTreeMap<TargetInstance, StatsCollector>,
    terminal_targets: &mut BoundedTargetSet,
) -> io::Result<()> {
    match event {
        ManagedEvent::TargetStateChanged { target, .. } => {
            stats
                .entry(target)
                .or_insert_with(|| StatsCollector::new(stats_config(true)));
        }
        ManagedEvent::Client { target, event } => {
            let collector = stats
                .entry(target.clone())
                .or_insert_with(|| StatsCollector::new(stats_config(true)));
            print_events_with_stats(
                stream_output,
                std::slice::from_ref(&event),
                Some(target.id.as_str()),
                collector,
            )?;
        }
        ManagedEvent::TargetFinished { outcome }
            if terminal_targets.insert(outcome.target.clone()) =>
        {
            report_target_failure(&outcome);
        }
        _ => {}
    }
    Ok(())
}

fn reconcile_stdin_stats(
    stats: &mut BTreeMap<TargetInstance, StatsCollector>,
    status: &ManagedStatus,
    final_summary_targets: Option<&BTreeSet<TargetInstance>>,
) {
    let live = status
        .targets
        .iter()
        .map(|target| target.target.clone())
        .collect::<HashSet<_>>();
    stats.retain(|target, _| {
        live.contains(target)
            || final_summary_targets.is_some_and(|targets| targets.contains(target))
    });
}

fn snapshot_stdin_summary_targets(
    stats: &mut BTreeMap<TargetInstance, StatsCollector>,
    status: &ManagedStatus,
) -> BTreeSet<TargetInstance> {
    let targets = status
        .targets
        .iter()
        .map(|target| target.target.clone())
        .collect::<BTreeSet<_>>();
    for target in &targets {
        stats
            .entry(target.clone())
            .or_insert_with(|| StatsCollector::new(stats_config(true)));
    }
    targets
}

#[derive(Clone)]
enum StdinStop {
    Interrupted,
    Eof,
    Fatal(String),
}

async fn run_stdin_stream(
    setup: ManagedRunSetup,
    output_config: OutputConfig,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), Box<dyn std::error::Error>> {
    if *shutdown.borrow() {
        return Ok(());
    }
    let (task, handle) = ManagedClient::task(setup.managed_config(), setup.managed_targets())?;
    let mut events = handle.subscribe()?;
    let mut status = handle.subscribe_status();
    let owner = ManagedWorker::start(task, handle.clone())?;
    let (updates, mut stdin) = tokio::sync::watch::channel(StdinUpdate::Targets(Vec::new()));
    spawn_stdin_target_reader(updates)?;

    let mut stdout = io::LineWriter::new(io::stdout().lock());
    let mut stream_output = StreamOutput {
        config: output_config,
        header_printed: false,
        print_final_summary: false,
        show_running_only_summary_note: false,
        out: &mut stdout,
    };
    let mut stats = BTreeMap::new();
    let mut terminal_targets = BoundedTargetSet::new(STDIN_OUTCOME_HISTORY_LIMIT);
    let mut stdin_changed = false;
    let mut desired = None;
    let mut pending = None;
    let mut submitted = Vec::new();
    let mut retry_after = None;
    let mut dropped_events = 0_u64;
    let mut stop_requested = false;
    let mut stop = None;
    let mut summary_targets = None;
    loop {
        let snapshot = status.borrow_and_update().clone();
        if stop.is_none() {
            if *shutdown.borrow() {
                stop = Some(StdinStop::Interrupted);
            } else if stdin_changed || stdin.has_changed().unwrap_or(true) {
                stdin_changed = false;
                match stdin.borrow_and_update().clone() {
                    StdinUpdate::Targets(targets) => {
                        desired = Some(targets);
                        retry_after = None;
                    }
                    StdinUpdate::Stop(reason) => stop = Some(reason),
                }
            }
            if stop.is_none()
                && pending.is_none()
                && !retry_after
                    .as_ref()
                    .is_some_and(|previous| Arc::ptr_eq(previous, &snapshot))
            {
                if let Some(targets) = desired.take() {
                    submitted = targets;
                    match handle.update_targets(submitted.clone()) {
                        Ok(receipt) => {
                            pending = Some(receipt);
                            retry_after = Some(snapshot.clone());
                        }
                        Err(_) => {
                            stop = Some(StdinStop::Fatal(
                                "failed to submit --targets-stdin update".to_owned(),
                            ))
                        }
                    }
                }
            }
            if stop.is_some() {
                summary_targets = Some(snapshot_stdin_summary_targets(&mut stats, &snapshot));
                if request_managed_stop_once(&mut stop_requested) {
                    drop(handle.stop());
                }
            }
        }
        reconcile_stdin_stats(&mut stats, &snapshot, summary_targets.as_ref());
        if snapshot.final_outcome.is_some() {
            break;
        }
        tokio::select! {
            _ = shutdown.changed(), if stop.is_none() => {}
            _ = stdin.changed(), if stop.is_none() => { stdin_changed = true; }
            changed = status.changed() => { if changed.is_err() { break; } }
            result = async { pending.as_mut().expect("guarded pending receipt").await }, if pending.is_some() && stop.is_none() => {
                pending = None;
                match result {
                    Ok(_) => retry_after = None,
                    Err(ManagedCommandApplyError::LiveGenerationLimitExceeded { .. }) => {
                        // A newer desired set supersedes this rejected set. Otherwise,
                        // retry only after the driver's status advances from submission.
                        if desired.is_none() {
                            desired = Some(std::mem::take(&mut submitted));
                        } else {
                            retry_after = None;
                        }
                    }
                    Err(_) => {
                        stop = Some(StdinStop::Fatal("--targets-stdin update was rejected".to_owned()));
                        summary_targets = Some(snapshot_stdin_summary_targets(&mut stats, &status.borrow()));
                        if request_managed_stop_once(&mut stop_requested) { drop(handle.stop()); }
                    }
                }
            }
            event = events.recv() => match event {
                Ok(event) => {
                    process_stdin_event(event, &mut stream_output, &mut stats, &mut terminal_targets)?;
                    reconcile_stdin_stats(&mut stats, &status.borrow(), summary_targets.as_ref());
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
                    dropped_events = dropped_events.saturating_add(count);
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    }

    let outcome = owner.join().await?;
    drain_final_events(&mut events, &mut dropped_events, |event| {
        process_stdin_event(event, &mut stream_output, &mut stats, &mut terminal_targets)
    })?;
    if let Some(warning) = dropped_event_warning(dropped_events) {
        eprintln!("{warning}");
    }
    for target in outcome.recent_target_outcomes.iter() {
        if terminal_targets.insert(target.target.clone()) {
            report_target_failure(target);
        }
    }
    if outcome.discarded_target_outcomes != 0 {
        eprintln!(
            "irtt-rs: warning: {} final target outcomes were discarded",
            outcome.discarded_target_outcomes
        );
    }

    let print_summary = matches!(stop, Some(StdinStop::Interrupted | StdinStop::Eof));
    stream_output.print_final_summary = print_summary;
    stream_output.show_running_only_summary_note = print_summary;
    if let Some(summary_targets) = summary_targets {
        let multi_target = summary_targets.len() > 1;
        for target in summary_targets {
            let stats = stats
                .get(&target)
                .expect("summary snapshot retains every selected target collector");
            if multi_target && stream_output.config.prints_summary() {
                writeln!(stream_output.out)?;
                writeln!(
                    stream_output.out,
                    "target: {} (generation {})",
                    target.id, target.generation
                )?;
            }
            stream_output.print_summary(stats)?;
        }
    }
    stream_output.out.flush()?;

    match stop {
        Some(StdinStop::Fatal(error)) => Err(error.into()),
        _ => match outcome.end_reason {
            ManagedEndReason::DriverFailed(failure) => {
                Err(format!("managed driver failed: {failure}").into())
            }
            _ => Ok(()),
        },
    }
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

fn drain_final_events<E>(
    events: &mut ManagedEventSubscription,
    dropped_events: &mut u64,
    mut process: impl FnMut(ManagedEvent) -> Result<(), E>,
) -> Result<(), E> {
    loop {
        match events.try_recv() {
            Ok(event) => process(event)?,
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
    let total_probe_count =
        expected_probe_count(args.duration, args.interval).saturating_mul(target_count);
    // Ask the stats crate what the configuration this run will actually use
    // retains; the CLI owns the probe count and the thresholds, not the
    // retention model.
    let estimated_bytes = stats_config(false).estimated_retained_bytes(total_probe_count);
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
