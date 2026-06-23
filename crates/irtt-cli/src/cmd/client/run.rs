#[cfg(feature = "stats")]
use std::collections::BTreeMap;
use std::{
    collections::HashSet,
    io::{self, Write},
    sync::atomic::AtomicBool,
    thread,
    time::{Duration, Instant},
};

use irtt_client::{
    ClientEvent, EventSubscriptionError, ManagedClientGroup, ManagedClientGroupConfig,
    ManagedGroupEndReason, SubscriberConfig, SubscriberOverflow, TargetEvent,
};
#[cfg(all(test, feature = "stats"))]
use irtt_client::{ClientTimestamp, PacketMeta, RttSample, SignedDuration};

use super::{
    args::{ClientArgs, ResolvedCliTarget},
    output::{EventRenderStats, OutputConfig},
};
#[cfg(feature = "stats")]
use crate::shared::client::expected_probe_count;
use crate::shared::client::{
    is_shutdown_requested, session::should_print_final_summary, ClientSession,
};

#[cfg(feature = "stats")]
use irtt_stats::{StatsCollector, StatsConfig};

#[cfg(feature = "stats")]
const FINITE_STATS_BYTES_PER_PROBE: u64 = 500;
#[cfg(feature = "stats")]
const MIB: u64 = 1024 * 1024;
#[cfg(feature = "stats")]
const GIB: u64 = 1024 * MIB;
#[cfg(feature = "stats")]
const FINITE_STATS_MEMORY_WARNING_BYTES: u64 = 128 * MIB;
#[cfg(feature = "stats")]
const FINITE_STATS_MEMORY_STRONG_WARNING_BYTES: u64 = 512 * MIB;
#[cfg(feature = "stats")]
const FINITE_STATS_MEMORY_VERY_STRONG_WARNING_BYTES: u64 = GIB;

const GROUP_IDLE_SLEEP: Duration = Duration::from_millis(5);
const GROUP_COMPLETION_GRACE: Duration = Duration::from_secs(1);

pub fn run_stream(
    args: ClientArgs,
    shutdown_requested: &AtomicBool,
) -> Result<(), Box<dyn std::error::Error>> {
    if args.list_columns {
        print!("{}", OutputConfig::list_columns());
        return Ok(());
    }
    let output_config = OutputConfig::new(
        args.format,
        args.columns.as_deref(),
        args.header,
        args.verbose,
        args.target_specs()
            .map(|targets| targets.len() > 1)
            .unwrap_or(false),
    )
    .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

    let targets = args
        .resolved_managed_targets()
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    let multi_target = targets.len() > 1;

    let continuous = args.is_continuous();
    #[cfg(feature = "stats")]
    if let Some(warning) = finite_stats_memory_warning(&args) {
        eprintln!("{warning}");
    }

    if multi_target {
        return run_group_stream(args, targets, output_config, continuous, shutdown_requested);
    }

    let target_label = targets
        .first()
        .map(|target| target.label.as_str())
        .expect("at least one target was validated");
    let mut stdout = io::LineWriter::new(io::stdout().lock());
    #[cfg(feature = "stats")]
    let mut stats = StatsCollector::new(stats_config(continuous));
    let mut stream_output = StreamOutput {
        config: output_config,
        header_printed: false,
        print_final_summary: false,
        show_running_only_summary_note: false,
        out: &mut stdout,
    };

    if is_shutdown_requested(shutdown_requested) {
        return Ok(());
    }

    let mut session = ClientSession::connect(args.to_client_config(), continuous)?;

    if is_shutdown_requested(shutdown_requested) {
        return Ok(());
    }

    let events = session.open()?;
    #[cfg(feature = "stats")]
    print_events_with_stats(&mut stream_output, &events, Some(target_label), &mut stats)?;
    #[cfg(not(feature = "stats"))]
    print_events_with_stats(&mut stream_output, &events, Some(target_label))?;

    let mut interrupted = false;
    while session.should_continue(shutdown_requested) {
        let events = session.step(shutdown_requested)?;
        #[cfg(feature = "stats")]
        print_events_with_stats(&mut stream_output, &events, Some(target_label), &mut stats)?;
        #[cfg(not(feature = "stats"))]
        print_events_with_stats(&mut stream_output, &events, Some(target_label))?;

        if is_shutdown_requested(shutdown_requested) {
            interrupted = true;
            break;
        }

        session.sleep_until_next_send();
    }
    interrupted |= is_shutdown_requested(shutdown_requested);

    if interrupted {
        eprintln!("interrupted, closing session...");
    }

    if session.should_drain_final(interrupted) {
        session.drain_final(|events| {
            #[cfg(feature = "stats")]
            let _ =
                print_events_with_stats(&mut stream_output, events, Some(target_label), &mut stats);
            #[cfg(not(feature = "stats"))]
            let _ = print_events_with_stats(&mut stream_output, events, Some(target_label));
        })?;
    }

    let events = session.poll_timeouts()?;
    #[cfg(feature = "stats")]
    print_events_with_stats(&mut stream_output, &events, Some(target_label), &mut stats)?;
    #[cfg(not(feature = "stats"))]
    print_events_with_stats(&mut stream_output, &events, Some(target_label))?;

    let events = session.close()?;
    #[cfg(feature = "stats")]
    print_events_with_stats(&mut stream_output, &events, Some(target_label), &mut stats)?;
    #[cfg(not(feature = "stats"))]
    print_events_with_stats(&mut stream_output, &events, Some(target_label))?;
    let print_final_summary = should_print_final_summary(continuous, interrupted);
    stream_output.print_final_summary = print_final_summary;
    stream_output.show_running_only_summary_note = continuous && interrupted && print_final_summary;
    #[cfg(feature = "stats")]
    stream_output.print_summary(&stats)?;
    #[cfg(not(feature = "stats"))]
    stream_output.print_summary()?;
    stream_output.out.flush()?;
    Ok(())
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
            self.print_event(event, target, stats_update)?;
        }
        Ok(())
    }

    fn print_event(
        &mut self,
        event: &ClientEvent,
        target: Option<&str>,
        stats_update: &EventRenderStats,
    ) -> io::Result<()> {
        if let Some(line) = self.config.render_event(event, target, Some(stats_update)) {
            writeln!(self.out, "{line}")?;
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

    #[cfg(feature = "stats")]
    fn print_summary(&mut self, stats: &StatsCollector) -> io::Result<()> {
        if !self.print_final_summary || !self.config.prints_summary() {
            return Ok(());
        }

        write!(
            self.out,
            "{}",
            crate::cmd::client::summary::format_summary_with_options(
                &stats.snapshot(),
                crate::cmd::client::summary::SummaryFormatOptions {
                    verbose: self.config.summary_verbose(),
                    show_running_only_note: self.show_running_only_summary_note,
                },
            )
        )?;

        Ok(())
    }

    #[cfg(not(feature = "stats"))]
    fn print_summary(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(feature = "stats")]
fn print_events_with_stats<W: Write>(
    stream_output: &mut StreamOutput<'_, W>,
    events: &[ClientEvent],
    target: Option<&str>,
    stats: &mut StatsCollector,
) -> io::Result<()> {
    let stats_updates = events
        .iter()
        .map(|event| EventRenderStats::from(stats.process(event)))
        .collect::<Vec<_>>();
    stream_output.print_events(events, target, &stats_updates)
}

#[cfg(not(feature = "stats"))]
fn print_events_with_stats<W: Write>(
    stream_output: &mut StreamOutput<'_, W>,
    events: &[ClientEvent],
    target: Option<&str>,
) -> io::Result<()> {
    let stats_updates = vec![EventRenderStats::default(); events.len()];
    stream_output.print_events(events, target, &stats_updates)
}

fn run_group_stream(
    args: ClientArgs,
    targets: Vec<ResolvedCliTarget>,
    output_config: OutputConfig,
    continuous: bool,
    shutdown_requested: &AtomicBool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut config = args.to_client_config();
    let managed_targets = targets
        .iter()
        .map(|target| target.managed.clone())
        .collect::<Vec<_>>();
    let expected_target_count = managed_targets.len();
    if let Some(first) = managed_targets.first() {
        config.server_addr = first.remote.to_string();
    }

    let group_config = ManagedClientGroupConfig {
        client: config,
        pacing: args.pacing.into(),
    };

    let (session, events) = ManagedClientGroup::start_with_subscription(
        group_config,
        managed_targets,
        SubscriberConfig {
            capacity: 16_384,
            // CLI output is best-effort under sustained backpressure. Dropping
            // stale rows keeps continuous runs attached to the running group
            // instead of turning a full output queue into a disconnected
            // subscriber and a potentially blocking join.
            overflow: SubscriberOverflow::DropOldest,
        },
    )?;

    let mut stdout = io::LineWriter::new(io::stdout().lock());
    #[cfg(feature = "stats")]
    let mut stats = targets
        .iter()
        .map(|target| {
            (
                target.label.clone(),
                StatsCollector::new(stats_config(continuous)),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut stream_output = StreamOutput {
        config: output_config,
        header_printed: false,
        print_final_summary: false,
        show_running_only_summary_note: false,
        out: &mut stdout,
    };

    let mut interrupted = false;
    let mut terminal_targets = HashSet::new();
    let mut last_event_at = Instant::now();
    let mut saw_target_event = false;

    loop {
        if is_shutdown_requested(shutdown_requested) {
            interrupted = true;
            session.stop();
        }

        match events.try_recv() {
            Ok(Some(target_event)) => {
                last_event_at = Instant::now();
                saw_target_event = true;
                let label = target_event.target.as_str();
                if is_terminal_target_event(&target_event.event) {
                    terminal_targets.insert(label.to_owned());
                }
                #[cfg(feature = "stats")]
                {
                    let stats = stats
                        .entry(label.to_owned())
                        .or_insert_with(|| StatsCollector::new(stats_config(continuous)));
                    print_target_event_with_stats(&mut stream_output, &target_event, stats)?;
                }
                #[cfg(not(feature = "stats"))]
                {
                    print_target_event_with_stats(&mut stream_output, &target_event)?;
                }
            }
            Ok(None) => {
                if interrupted {
                    break;
                }
                if terminal_targets.len() >= expected_target_count {
                    break;
                }
                if should_join_group_after_idle(&args, continuous, saw_target_event, last_event_at)
                {
                    break;
                }
                thread::sleep(GROUP_IDLE_SLEEP);
            }
            Err(EventSubscriptionError::Disconnected) => break,
        }
    }

    if interrupted {
        eprintln!("interrupted, closing group...");
    }

    let outcome = session.join()?;
    while let Ok(Some(target_event)) = events.try_recv() {
        let label = target_event.target.as_str();
        #[cfg(feature = "stats")]
        {
            let stats = stats
                .entry(label.to_owned())
                .or_insert_with(|| StatsCollector::new(stats_config(continuous)));
            print_target_event_with_stats(&mut stream_output, &target_event, stats)?;
        }
        #[cfg(not(feature = "stats"))]
        {
            print_target_event_with_stats(&mut stream_output, &target_event)?;
        }
    }

    if outcome.end_reason == ManagedGroupEndReason::Cancelled && !interrupted {
        return Err("managed client group was cancelled".into());
    }

    let print_final_summary = should_print_final_summary(continuous, interrupted);
    stream_output.print_final_summary = print_final_summary;
    stream_output.show_running_only_summary_note = continuous && interrupted && print_final_summary;
    #[cfg(feature = "stats")]
    {
        for (label, stats) in &stats {
            if stream_output.print_final_summary && stream_output.config.prints_summary() {
                writeln!(stream_output.out)?;
                writeln!(stream_output.out, "target: {label}")?;
            }
            stream_output.print_summary(stats)?;
        }
    }
    #[cfg(not(feature = "stats"))]
    {
        stream_output.print_summary()?;
    }
    stream_output.out.flush()?;
    Ok(())
}

fn is_terminal_target_event(event: &ClientEvent) -> bool {
    matches!(
        event,
        ClientEvent::SessionClosed { .. } | ClientEvent::NoTestCompleted { .. }
    )
}

fn estimated_group_completion_grace(args: &ClientArgs) -> Duration {
    let open_timeout: Duration = args.to_client_config().open_timeouts.iter().sum();
    open_timeout
        .saturating_add(args.duration)
        .saturating_add(GROUP_COMPLETION_GRACE)
}

fn should_join_group_after_idle(
    args: &ClientArgs,
    continuous: bool,
    saw_target_event: bool,
    last_event_at: Instant,
) -> bool {
    if continuous && saw_target_event {
        return false;
    }
    last_event_at.elapsed() > estimated_group_completion_grace(args)
}

#[cfg(feature = "stats")]
fn print_target_event_with_stats<W: Write>(
    stream_output: &mut StreamOutput<'_, W>,
    target_event: &TargetEvent,
    stats: &mut StatsCollector,
) -> io::Result<()> {
    print_events_with_stats(
        stream_output,
        std::slice::from_ref(&target_event.event),
        Some(target_event.target.as_str()),
        stats,
    )
}

#[cfg(not(feature = "stats"))]
fn print_target_event_with_stats<W: Write>(
    stream_output: &mut StreamOutput<'_, W>,
    target_event: &TargetEvent,
) -> io::Result<()> {
    print_events_with_stats(
        stream_output,
        std::slice::from_ref(&target_event.event),
        Some(target_event.target.as_str()),
    )
}

#[cfg(feature = "stats")]
fn stats_config(continuous: bool) -> StatsConfig {
    if continuous {
        StatsConfig::continuous()
    } else {
        StatsConfig::finite()
    }
}

#[cfg(feature = "stats")]
fn estimate_finite_stats_memory_bytes(expected_probes: u64) -> u64 {
    expected_probes.saturating_mul(FINITE_STATS_BYTES_PER_PROBE)
}

#[cfg(feature = "stats")]
fn finite_stats_memory_warning(args: &ClientArgs) -> Option<String> {
    if args.is_continuous() || args.duration.is_zero() {
        return None;
    }

    let expected_probes = expected_probe_count(args.duration, args.interval);
    let estimated_bytes = estimate_finite_stats_memory_bytes(expected_probes);
    if estimated_bytes < FINITE_STATS_MEMORY_WARNING_BYTES {
        return None;
    }

    let formatted = format_bytes_for_warning(estimated_bytes);
    let guidance = if estimated_bytes >= FINITE_STATS_MEMORY_VERY_STRONG_WARNING_BYTES {
        "this may be unsuitable on memory-constrained systems"
    } else if estimated_bytes >= FINITE_STATS_MEMORY_STRONG_WARNING_BYTES {
        "consider shortening the run, increasing the interval, or using continuous mode"
    } else {
        "use continuous mode for bounded-memory long-running tests"
    };

    Some(format!(
        "irtt-rs: warning: finite exact statistics may retain about {formatted} for this run; {guidance}"
    ))
}

#[cfg(feature = "stats")]
fn format_bytes_for_warning(bytes: u64) -> String {
    if bytes >= GIB {
        format!("{} GiB", bytes.saturating_add(GIB / 2) / GIB)
    } else {
        format!("{} MiB", bytes.saturating_add(MIB / 2) / MIB)
    }
}

#[cfg(all(test, feature = "stats"))]
mod tests {
    use super::*;
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        time::{Duration, Instant, UNIX_EPOCH},
    };

    fn test_timestamp(offset: Duration) -> ClientTimestamp {
        ClientTimestamp {
            wall: UNIX_EPOCH + offset,
            mono: Instant::now() + offset,
        }
    }

    fn test_remote() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 2112)
    }

    fn reply_event(seq: u32, rtt_us: u64) -> ClientEvent {
        let sent_at = test_timestamp(Duration::from_millis(seq as u64));
        let received_at = ClientTimestamp {
            wall: sent_at.wall + Duration::from_micros(rtt_us),
            mono: sent_at.mono + Duration::from_micros(rtt_us),
        };
        ClientEvent::EchoReply {
            seq,
            remote: test_remote(),
            sent_at,
            received_at,
            rtt: RttSample {
                raw: Duration::from_micros(rtt_us),
                adjusted: None,
                effective: SignedDuration::from_nanos(i128::from(rtt_us) * 1_000),
            },
            server_timing: None,
            one_way: None,
            received_stats: None,
            bytes: 64,
            packet_meta: PacketMeta::default(),
        }
    }

    fn cli_args(args: &[&str]) -> ClientArgs {
        let mut argv = vec!["irtt-rs"];
        argv.extend_from_slice(args);
        <ClientArgs as clap::Parser>::try_parse_from(argv).unwrap()
    }

    fn output_config(
        format: crate::cmd::client::OutputFormat,
        columns: Option<&str>,
        header: crate::cmd::client::HeaderMode,
        verbose: bool,
    ) -> OutputConfig {
        OutputConfig::new(format, columns, header, verbose, false).unwrap()
    }

    #[test]
    fn output_helper_streams_and_collects_events() {
        let sent_at = test_timestamp(Duration::from_secs(1));
        let received_at = test_timestamp(Duration::from_secs(1) + Duration::from_micros(1200));
        let events = [
            ClientEvent::EchoSent {
                seq: 1,
                remote: test_remote(),
                scheduled_at: sent_at.mono,
                sent_at,
                bytes: 64,
                send_call: Duration::from_micros(10),
                timer_error: Duration::ZERO,
            },
            ClientEvent::EchoReply {
                seq: 1,
                remote: test_remote(),
                sent_at,
                received_at,
                rtt: RttSample {
                    raw: Duration::from_micros(1200),
                    adjusted: None,
                    effective: SignedDuration::from_nanos(1_200_000),
                },
                server_timing: None,
                one_way: None,
                received_stats: None,
                bytes: 64,
                packet_meta: PacketMeta::default(),
            },
        ];

        let mut stats = StatsCollector::new(StatsConfig::finite());
        let mut out = Vec::new();
        {
            let mut stream_output = StreamOutput {
                config: output_config(
                    crate::cmd::client::OutputFormat::Tsv,
                    Some("effective_rtt_us"),
                    crate::cmd::client::HeaderMode::Never,
                    false,
                ),
                header_printed: false,
                print_final_summary: true,
                show_running_only_summary_note: false,
                out: &mut out,
            };
            print_events_with_stats(&mut stream_output, &events, None, &mut stats).unwrap();
            stream_output.print_summary(&stats).unwrap();
        }

        let rendered = String::from_utf8(out).unwrap();
        let summary = stats.snapshot();
        assert!(!rendered.is_empty());
        assert_eq!(summary.packets.packets_sent, 1);
        assert_eq!(summary.packets.unique_replies, 1);
    }

    #[test]
    fn continuous_summary_prints_when_enabled_and_suppresses_when_disabled() {
        let mut stats = StatsCollector::new(StatsConfig::continuous());
        let mut out = Vec::new();
        {
            let mut stream_output = StreamOutput {
                config: output_config(
                    crate::cmd::client::OutputFormat::Table,
                    None,
                    crate::cmd::client::HeaderMode::Auto,
                    false,
                ),
                header_printed: false,
                print_final_summary: true,
                show_running_only_summary_note: true,
                out: &mut out,
            };
            print_events_with_stats(
                &mut stream_output,
                &[reply_event(1, 1200)],
                None,
                &mut stats,
            )
            .unwrap();
            stream_output.print_summary(&stats).unwrap();
        }

        let rendered = String::from_utf8(out).unwrap();
        assert!(rendered.contains("irtt-rs summary"));
        assert!(rendered.contains("medians unavailable"));
        assert!(rendered.contains("continuous mode"));
        assert!(rendered.contains("packets:"));
        assert!(rendered.contains("received=1"));

        let stats = StatsCollector::new(StatsConfig::continuous());
        let mut out = Vec::new();
        {
            let mut stream_output = StreamOutput {
                config: output_config(
                    crate::cmd::client::OutputFormat::Table,
                    None,
                    crate::cmd::client::HeaderMode::Auto,
                    false,
                ),
                header_printed: false,
                print_final_summary: false,
                show_running_only_summary_note: true,
                out: &mut out,
            };
            stream_output.print_summary(&stats).unwrap();
        }

        assert!(out.is_empty());
    }

    #[test]
    fn continuous_mode_uses_continuous_stats_config() {
        let mut collector = StatsCollector::new(stats_config(true));
        for seq in 0..5000_u32 {
            let sent_at = test_timestamp(Duration::from_micros(seq as u64));
            let received_at = test_timestamp(Duration::from_micros(seq as u64 + 1));
            collector.process(&ClientEvent::EchoReply {
                seq,
                remote: test_remote(),
                sent_at,
                received_at,
                rtt: RttSample {
                    raw: Duration::from_micros(1),
                    adjusted: None,
                    effective: SignedDuration::from_nanos(1_000),
                },
                server_timing: None,
                one_way: None,
                received_stats: None,
                bytes: 64,
                packet_meta: PacketMeta::default(),
            });
        }

        assert_eq!(collector.snapshot().rtt.primary.median_ns, None);
    }

    #[test]
    fn finite_stats_memory_warning_reports_only_large_finite_runs() {
        for args in [
            cli_args(&["--duration", "0", "--interval", "1ms", "127.0.0.1:2112"]),
            cli_args(&[
                "--duration",
                "1000ms",
                "--interval",
                "1ms",
                "127.0.0.1:2112",
            ]),
        ] {
            assert_eq!(finite_stats_memory_warning(&args), None);
        }

        for (threshold, size, guidance) in [
            (
                FINITE_STATS_MEMORY_WARNING_BYTES,
                "about 128 MiB",
                "bounded-memory long-running tests",
            ),
            (
                FINITE_STATS_MEMORY_STRONG_WARNING_BYTES,
                "about 512 MiB",
                "shortening the run",
            ),
            (
                FINITE_STATS_MEMORY_VERY_STRONG_WARNING_BYTES,
                "about 1 GiB",
                "memory-constrained systems",
            ),
        ] {
            let expected_probes = threshold.div_ceil(FINITE_STATS_BYTES_PER_PROBE);
            let args = cli_args(&[
                "--duration",
                &format!("{expected_probes}ms"),
                "--interval",
                "1ms",
                "127.0.0.1:2112",
            ]);

            let warning = finite_stats_memory_warning(&args).unwrap();
            assert!(warning.contains(size));
            assert!(warning.contains(guidance));
        }
    }

    #[test]
    fn expected_probe_count_rounds_up() {
        assert_eq!(
            expected_probe_count(Duration::from_millis(1001), Duration::from_secs(1)),
            2
        );
    }

    #[test]
    fn estimate_finite_stats_memory_bytes_saturates() {
        assert_eq!(estimate_finite_stats_memory_bytes(u64::MAX), u64::MAX);
    }
}
