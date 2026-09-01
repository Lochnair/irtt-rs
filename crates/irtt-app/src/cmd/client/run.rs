use std::{
    collections::{BTreeMap, BTreeSet, HashSet, VecDeque},
    io::{self, BufRead, Write},
    sync::{atomic::AtomicBool, Arc, Mutex},
    thread,
    time::Duration,
};

use irtt_client::{
    managed::{
        BlockingManagedClient, ManagedClientHandle, ManagedCommandApplyError, ManagedEndReason,
        ManagedEvent, ManagedEventSubscription, ManagedEventTryRecvError, ManagedStatus,
        ManagedTargetConfig, ManagedTargetEndReason, ManagedTargetOutcome, TargetInstance,
    },
    ClientEvent,
};

use super::{
    args::ClientArgs,
    output::{EventRenderStats, OutputConfig},
};

use crate::shared::client::{
    expected_probe_count, is_shutdown_requested, parse_stdin_target_set,
    session::{
        drain_managed_events, peer_close_run_error, request_managed_stop_for_peer_close,
        request_managed_stop_once, should_print_final_summary, ManagedDrainState,
    },
    ManagedRunSetup, STDIN_MAX_DESIRED_TARGETS, STDIN_OUTCOME_HISTORY_LIMIT,
};

use irtt_stats::{StatsCollector, StatsConfig};

const MIB: u64 = 1024 * 1024;

const GIB: u64 = 1024 * MIB;

const FINITE_STATS_MEMORY_WARNING_BYTES: u64 = 128 * MIB;

const FINITE_STATS_MEMORY_STRONG_WARNING_BYTES: u64 = 512 * MIB;

const FINITE_STATS_MEMORY_VERY_STRONG_WARNING_BYTES: u64 = GIB;
const MANAGED_EVENT_WAIT_SLICE: Duration = Duration::from_millis(20);
const MAX_STDIN_RECORD_BYTES: usize = 64 * 1024;

pub fn run_stream(
    args: ClientArgs,
    shutdown_requested: &AtomicBool,
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
        return run_stdin_stream(setup, output_config, shutdown_requested);
    }
    let continuous = args.is_continuous();
    let target_count = setup.target_count();

    if let Some(warning) = finite_stats_memory_warning(&args, target_count) {
        eprintln!("{warning}");
    }
    if is_shutdown_requested(shutdown_requested) {
        return Ok(());
    }
    let (owner, mut events) = BlockingManagedClient::start_with_subscription(
        setup.managed_config(),
        setup.managed_targets(),
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
struct StdinTargetSet {
    revision: u64,
    targets: Vec<ManagedTargetConfig>,
}

#[derive(Default)]
struct StdinMailboxState {
    next_revision: u64,
    latest: Option<StdinTargetSet>,
    eof: bool,
    fatal: Option<String>,
}

#[derive(Default)]
struct StdinMailbox {
    state: Mutex<StdinMailboxState>,
}

impl StdinMailbox {
    fn publish(&self, targets: Vec<ManagedTargetConfig>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.next_revision = state.next_revision.saturating_add(1);
        state.latest = Some(StdinTargetSet {
            revision: state.next_revision,
            targets,
        });
    }

    fn finish(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .eof = true;
    }

    fn fail(&self, error: String) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.fatal = Some(error);
        state.latest = None;
    }

    fn latest(&self) -> Option<StdinTargetSet> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .latest
            .clone()
    }

    fn clear_if_current(&self, revision: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state
            .latest
            .as_ref()
            .is_some_and(|set| set.revision == revision)
        {
            state.latest = None;
        }
    }

    fn is_current(&self, revision: u64) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .latest
            .as_ref()
            .is_some_and(|set| set.revision == revision)
    }

    fn fatal(&self) -> Option<String> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .fatal
            .clone()
    }

    fn is_finished(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .eof
    }
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

fn read_stdin_target_sets<R: BufRead>(reader: &mut R, mailbox: &StdinMailbox) {
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
                        mailbox.publish(targets.into_iter().map(|target| target.managed).collect())
                    }
                    Err(error) => {
                        mailbox.fail(format!("invalid --targets-stdin line {line}: {error}"));
                        return;
                    }
                }
            }
            Ok(None) => {
                mailbox.finish();
                return;
            }
            Err(_) => {
                mailbox.fail(format!("failed to read --targets-stdin line {}", line + 1));
                return;
            }
        }
    }
}

fn spawn_stdin_target_reader(mailbox: Arc<StdinMailbox>) -> io::Result<()> {
    thread::Builder::new()
        .name("irtt-targets-stdin".to_owned())
        .spawn(move || {
            let stdin = io::stdin();
            read_stdin_target_sets(&mut stdin.lock(), &mailbox);
        })
        .map(|_| ())
}

#[derive(Clone)]
struct RetryAfterStatus {
    revision: u64,
    status: Arc<ManagedStatus>,
}

fn retry_waits_for_status_change(
    retry_after: &mut Option<RetryAfterStatus>,
    target_set: &StdinTargetSet,
    status: &Arc<ManagedStatus>,
) -> bool {
    let Some(retry) = retry_after else {
        return false;
    };
    if retry.revision != target_set.revision {
        *retry_after = None;
        return false;
    }
    Arc::ptr_eq(&retry.status, status)
}

fn retry_after_capacity_rejection(
    mailbox: &StdinMailbox,
    target_set: &StdinTargetSet,
    status: Arc<ManagedStatus>,
) -> Option<RetryAfterStatus> {
    mailbox
        .is_current(target_set.revision)
        .then_some(RetryAfterStatus {
            revision: target_set.revision,
            status,
        })
}

fn apply_latest_stdin_target_set(
    handle: &ManagedClientHandle,
    mailbox: &StdinMailbox,
    retry_after: &mut Option<RetryAfterStatus>,
) -> Result<(), String> {
    let Some(target_set) = mailbox.latest() else {
        *retry_after = None;
        return Ok(());
    };
    let status = handle.status();
    if retry_waits_for_status_change(retry_after, &target_set, &status) {
        return Ok(());
    }

    let receipt = handle
        .update_targets(target_set.targets.clone())
        .map_err(|_| "failed to submit --targets-stdin update".to_owned())?;
    match receipt.blocking_wait() {
        Ok(_) => {
            mailbox.clear_if_current(target_set.revision);
            *retry_after = None;
            Ok(())
        }
        Err(ManagedCommandApplyError::LiveGenerationLimitExceeded { .. }) => {
            *retry_after = retry_after_capacity_rejection(mailbox, &target_set, status);
            Ok(())
        }
        Err(_) => Err("--targets-stdin update was rejected".to_owned()),
    }
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

fn drain_stdin_events<W: Write>(
    events: &mut ManagedEventSubscription,
    stream_output: &mut StreamOutput<'_, W>,
    stats: &mut BTreeMap<TargetInstance, StatsCollector>,
    terminal_targets: &mut BoundedTargetSet,
    dropped_events: &mut u64,
) -> io::Result<ManagedDrainState> {
    drain_managed_events(events, dropped_events, |event| {
        process_stdin_event(event, stream_output, stats, terminal_targets)
    })
}

fn drain_final_stdin_events<W: Write>(
    events: &mut ManagedEventSubscription,
    stream_output: &mut StreamOutput<'_, W>,
    stats: &mut BTreeMap<TargetInstance, StatsCollector>,
    terminal_targets: &mut BoundedTargetSet,
    dropped_events: &mut u64,
) -> io::Result<()> {
    loop {
        match events.try_recv() {
            Ok(event) => process_stdin_event(event, stream_output, stats, terminal_targets)?,
            Err(ManagedEventTryRecvError::Empty | ManagedEventTryRecvError::Closed) => break,
            Err(ManagedEventTryRecvError::Lagged(count)) => {
                *dropped_events = dropped_events.saturating_add(count);
            }
        }
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

fn stdin_stop_request(
    shutdown_requested: &AtomicBool,
    mailbox: &StdinMailbox,
) -> Option<StdinStop> {
    if is_shutdown_requested(shutdown_requested) {
        Some(StdinStop::Interrupted)
    } else if let Some(error) = mailbox.fatal() {
        Some(StdinStop::Fatal(error))
    } else if mailbox.is_finished() {
        Some(StdinStop::Eof)
    } else {
        None
    }
}

fn run_stdin_stream(
    setup: ManagedRunSetup,
    output_config: OutputConfig,
    shutdown_requested: &AtomicBool,
) -> Result<(), Box<dyn std::error::Error>> {
    if is_shutdown_requested(shutdown_requested) {
        return Ok(());
    }
    let (owner, mut events) = BlockingManagedClient::start_with_subscription(
        setup.managed_config(),
        setup.managed_targets(),
    )?;
    let handle = owner.handle();
    let mailbox = Arc::new(StdinMailbox::default());
    spawn_stdin_target_reader(Arc::clone(&mailbox))?;

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
    let mut retry_after = None;
    let mut dropped_events = 0_u64;
    let mut stop_requested = false;
    let mut stop = None;
    let mut summary_targets = None;
    let mut subscription_closed = false;

    loop {
        let status = handle.status();
        if stop.is_none() {
            if let Some(requested_stop) = stdin_stop_request(shutdown_requested, &mailbox) {
                summary_targets = Some(snapshot_stdin_summary_targets(&mut stats, &status));
                stop = Some(requested_stop);
                if request_managed_stop_once(&mut stop_requested) {
                    drop(handle.stop());
                }
            } else if let Err(error) =
                apply_latest_stdin_target_set(&handle, &mailbox, &mut retry_after)
            {
                summary_targets = Some(snapshot_stdin_summary_targets(&mut stats, &status));
                stop = Some(StdinStop::Fatal(error));
                if request_managed_stop_once(&mut stop_requested) {
                    drop(handle.stop());
                }
            }
        }

        let drain_state = drain_stdin_events(
            &mut events,
            &mut stream_output,
            &mut stats,
            &mut terminal_targets,
            &mut dropped_events,
        )?;
        reconcile_stdin_stats(&mut stats, &handle.status(), summary_targets.as_ref());
        match drain_state {
            ManagedDrainState::Empty => thread::sleep(MANAGED_EVENT_WAIT_SLICE),
            ManagedDrainState::Closed => subscription_closed = true,
            ManagedDrainState::BudgetExhausted => {}
        }
        if handle.status().final_outcome.is_some() || subscription_closed {
            break;
        }
    }

    let outcome = owner.join()?;
    drain_final_stdin_events(
        &mut events,
        &mut stream_output,
        &mut stats,
        &mut terminal_targets,
        &mut dropped_events,
    )?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use irtt_client::managed::{
        ManagedLifecycle, ManagedTargetFailure, ManagedTargetFailureKind,
        ManagedTargetFailurePhase, ManagedTargetLifecycle, ManagedTargetStatus, TargetId,
    };
    use std::{io::BufReader, io::Cursor, sync::Arc};

    #[test]
    fn stdin_record_reader_removes_only_line_framing() {
        let mut reader = BufReader::with_capacity(2, Cursor::new(b" first \r\n\nthird\r"));
        let mut buffer = Vec::with_capacity(MAX_STDIN_RECORD_BYTES + 1);

        assert_eq!(
            read_stdin_record(&mut reader, &mut buffer).unwrap(),
            Some(" first ".to_owned())
        );
        assert_eq!(
            read_stdin_record(&mut reader, &mut buffer).unwrap(),
            Some(String::new())
        );
        assert_eq!(
            read_stdin_record(&mut reader, &mut buffer).unwrap(),
            Some("third\r".to_owned())
        );
        assert_eq!(read_stdin_record(&mut reader, &mut buffer).unwrap(), None);
    }

    #[test]
    fn stdin_record_reader_rejects_over_limit_unterminated_record() {
        let input = vec![b'x'; MAX_STDIN_RECORD_BYTES + 1];
        let mut reader = BufReader::with_capacity(17, Cursor::new(input));
        let mut buffer = Vec::with_capacity(MAX_STDIN_RECORD_BYTES + 1);

        let error = read_stdin_record(&mut reader, &mut buffer).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("maximum size"));
        assert!(buffer.len() <= MAX_STDIN_RECORD_BYTES + 1);
    }

    #[test]
    fn newer_mailbox_value_survives_an_earlier_submitted_acknowledgement() {
        let mailbox = StdinMailbox::default();
        mailbox.publish(vec![ManagedTargetConfig::new("a", "127.0.0.1:2112")]);
        let submitted = mailbox.latest().unwrap();
        mailbox.publish(vec![ManagedTargetConfig::new("b", "127.0.0.1:2113")]);
        let pending = mailbox.latest().unwrap();

        mailbox.clear_if_current(submitted.revision);

        assert_eq!(mailbox.latest().unwrap().revision, pending.revision);
    }

    #[test]
    fn rejected_update_retries_after_status_publishes_during_acknowledgement() {
        let mailbox = StdinMailbox::default();
        mailbox.publish(Vec::new());
        let target_set = mailbox.latest().unwrap();
        let saturated = Arc::new(status_with_targets(&[TargetInstance {
            id: TargetId::from("old"),
            generation: 1,
        }]));
        let capacity_freed = Arc::new(status_with_targets(&[]));
        // Capacity becomes available after the transaction rejected but before
        // its acknowledgement reaches the controller. The rejection path must
        // retain the pre-submission status, not this later snapshot.
        let mut retry_after =
            retry_after_capacity_rejection(&mailbox, &target_set, Arc::clone(&saturated));

        assert!(retry_waits_for_status_change(
            &mut retry_after,
            &target_set,
            &saturated
        ));
        assert!(!retry_waits_for_status_change(
            &mut retry_after,
            &target_set,
            &capacity_freed
        ));
    }

    #[test]
    fn later_desired_set_supersedes_a_capacity_rejected_candidate_before_retry() {
        let mailbox = StdinMailbox::default();
        mailbox.publish(vec![ManagedTargetConfig::new("a", "127.0.0.1:2112")]);
        let applied = mailbox.latest().unwrap();
        mailbox.clear_if_current(applied.revision);

        mailbox.publish(vec![ManagedTargetConfig::new("b", "127.0.0.1:2113")]);
        let rejected = mailbox.latest().unwrap();
        let saturated = Arc::new(status_with_targets(&[TargetInstance {
            id: TargetId::from("old"),
            generation: 1,
        }]));
        let mut retry_after = retry_after_capacity_rejection(&mailbox, &rejected, saturated);
        mailbox.publish(vec![ManagedTargetConfig::new("c", "127.0.0.1:2114")]);
        let replacement = mailbox.latest().unwrap();

        assert!(!retry_waits_for_status_change(
            &mut retry_after,
            &replacement,
            &Arc::new(status_with_targets(&[TargetInstance {
                id: TargetId::from("old"),
                generation: 1,
            }]))
        ));
        assert!(retry_after.is_none());
        assert_eq!(replacement.targets[0].id.as_str(), "c");
        mailbox.clear_if_current(replacement.revision);
        assert!(mailbox.latest().is_none());
    }

    #[test]
    fn eof_and_shutdown_stop_without_waiting_for_the_stdin_reader() {
        let mailbox = StdinMailbox::default();
        let shutdown_requested = AtomicBool::new(false);
        assert!(stdin_stop_request(&shutdown_requested, &mailbox).is_none());

        mailbox.finish();
        assert!(matches!(
            stdin_stop_request(&shutdown_requested, &mailbox),
            Some(StdinStop::Eof)
        ));

        let mailbox = StdinMailbox::default();
        shutdown_requested.store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(matches!(
            stdin_stop_request(&shutdown_requested, &mailbox),
            Some(StdinStop::Interrupted)
        ));
    }

    fn status_with_targets(targets: &[TargetInstance]) -> ManagedStatus {
        ManagedStatus {
            lifecycle: ManagedLifecycle::Running,
            stop_requested: false,
            applied_command_sequence: 0,
            desired_target_count: targets.len(),
            connecting_target_count: 0,
            opening_target_count: 0,
            active_target_count: 0,
            draining_target_count: 0,
            closing_target_count: 0,
            terminal_target_count: 0,
            total_target_outcomes: 0,
            successful_target_outcomes: 0,
            failed_target_outcomes: 0,
            peer_closed_target_outcomes: 0,
            discarded_target_outcomes: 0,
            targets: Arc::from(
                targets
                    .iter()
                    .cloned()
                    .map(|target| ManagedTargetStatus {
                        target,
                        desired: true,
                        lifecycle: ManagedTargetLifecycle::Active,
                        server_addr: Arc::from("127.0.0.1:2112"),
                        remote: None,
                    })
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ),
            recent_target_outcomes: Arc::from([]),
            final_outcome: None,
        }
    }

    #[test]
    fn dynamic_accounting_prunes_retired_generations_and_retains_only_shutdown_snapshot() {
        let retired = TargetInstance {
            id: TargetId::from("edge"),
            generation: 1,
        };
        let replacement = TargetInstance {
            id: TargetId::from("edge"),
            generation: 2,
        };
        let live_status = status_with_targets(std::slice::from_ref(&replacement));
        let empty_status = status_with_targets(&[]);
        let mut stats = BTreeMap::from([
            (retired.clone(), StatsCollector::new(stats_config(true))),
            (replacement.clone(), StatsCollector::new(stats_config(true))),
        ]);

        reconcile_stdin_stats(&mut stats, &live_status, None);
        assert_eq!(stats.keys().cloned().collect::<Vec<_>>(), [replacement]);

        let summary_targets = snapshot_stdin_summary_targets(&mut stats, &live_status);
        stats.insert(retired, StatsCollector::new(stats_config(true)));
        reconcile_stdin_stats(&mut stats, &empty_status, Some(&summary_targets));

        assert_eq!(
            stats.keys().cloned().collect::<Vec<_>>(),
            [TargetInstance {
                id: TargetId::from("edge"),
                generation: 2,
            }]
        );
    }

    #[test]
    fn terminal_deduplication_is_bounded_under_target_churn() {
        let mut terminal_targets = BoundedTargetSet::new(2);
        let target = |generation| TargetInstance {
            id: TargetId::from("edge"),
            generation,
        };

        assert!(terminal_targets.insert(target(1)));
        assert!(terminal_targets.insert(target(2)));
        assert!(terminal_targets.insert(target(3)));
        assert!(terminal_targets.insert(target(1)));
        assert!(!terminal_targets.insert(target(3)));
    }

    fn long_finite_args() -> ClientArgs {
        use clap::Parser;

        ClientArgs::try_parse_from([
            "irtt-client",
            "--duration",
            "4200s",
            "--interval",
            "100ms",
            "127.0.0.1:2112",
        ])
        .unwrap()
    }

    /// Smallest target count whose aggregate estimate reaches the warning
    /// threshold, derived from the stats crate's own estimate rather than from
    /// any bytes-per-probe assumption held here.
    fn first_warning_target_count(args: &ClientArgs) -> usize {
        let probes = expected_probe_count(args.duration, args.interval);
        (1..=1024)
            .find(|targets| {
                let total = probes.saturating_mul(*targets as u64);
                stats_config(false).estimated_retained_bytes(total)
                    >= FINITE_STATS_MEMORY_WARNING_BYTES
            })
            .expect("a long finite run should cross the warning threshold within 1024 targets")
    }

    #[test]
    fn finite_stats_memory_warning_accounts_for_target_count() {
        let args = long_finite_args();
        let crossing = first_warning_target_count(&args);

        assert!(
            crossing > 1,
            "this run should need more than one target to cross the threshold, \
             otherwise the test proves nothing about target-count accounting"
        );
        assert!(
            finite_stats_memory_warning(&args, crossing - 1).is_none(),
            "an aggregate estimate below the threshold should not warn"
        );
        assert!(
            finite_stats_memory_warning(&args, crossing).is_some(),
            "an aggregate estimate at the threshold should warn"
        );
    }

    #[test]
    fn finite_stats_memory_warning_saturates_target_count_multiplication() {
        assert!(finite_stats_memory_warning(&long_finite_args(), usize::MAX).is_some());
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
