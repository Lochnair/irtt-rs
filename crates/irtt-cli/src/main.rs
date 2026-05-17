use std::{
    io::{self, Write},
    process::ExitCode,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use clap::Parser;
use irtt_cli::{
    format_event, format_human_event_with_options, CliArgs, HumanOutputOptions, OutputMode,
};
use irtt_client::{Client, ClientEvent, OpenOutcome, RecvBudget};

#[cfg(test)]
use irtt_client::ClientTimestamp;

#[cfg(feature = "stats")]
use irtt_stats::{StatsCollector, StatsConfig};

const RECV_BUDGET: RecvBudget = RecvBudget { max_packets: 16 };
const MAX_FINAL_DRAIN: Duration = Duration::from_secs(30);
const IDLE_SLEEP: Duration = Duration::from_millis(5);
const MAX_SLEEP: Duration = Duration::from_millis(20);
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

fn main() -> ExitCode {
    let args = CliArgs::parse();
    let shutdown_requested = Arc::new(AtomicBool::new(false));
    if let Err(err) = install_signal_handler(Arc::clone(&shutdown_requested)) {
        eprintln!("irtt-rs: failed to install signal handler: {err}");
        return ExitCode::FAILURE;
    }

    match run(args, shutdown_requested.as_ref()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("irtt-rs: {err}");
            ExitCode::FAILURE
        }
    }
}

fn install_signal_handler(shutdown_requested: Arc<AtomicBool>) -> Result<(), ctrlc::Error> {
    ctrlc::set_handler(move || {
        shutdown_requested.store(true, Ordering::Relaxed);
    })
}

fn run(args: CliArgs, shutdown_requested: &AtomicBool) -> Result<(), Box<dyn std::error::Error>> {
    match args.output {
        OutputMode::Human | OutputMode::Machine | OutputMode::Simple | OutputMode::RttUs => {
            run_stream(args, shutdown_requested)
        }
    }
}

fn run_stream(
    args: CliArgs,
    shutdown_requested: &AtomicBool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mode = args.output;
    let continuous = args.is_continuous();
    #[cfg(feature = "stats")]
    if let Some(warning) = finite_stats_memory_warning(&args) {
        eprintln!("{warning}");
    }
    let mut stdout = io::LineWriter::new(io::stdout().lock());
    #[cfg(feature = "stats")]
    let mut stats = StatsCollector::new(stats_config(continuous));
    let mut stream_output = StreamOutput {
        mode,
        human_options: HumanOutputOptions {
            verbose: args.verbose,
        },
        print_final_summary: false,
        show_running_only_summary_note: false,
        out: &mut stdout,
        #[cfg(feature = "stats")]
        stats: &mut stats,
    };

    if is_shutdown_requested(shutdown_requested) {
        return Ok(());
    }

    let mut client = Client::connect(args.to_client_config())?;

    if is_shutdown_requested(shutdown_requested) {
        return Ok(());
    }

    let open = client.open()?;
    stream_output.print_event(open_event(&open))?;

    let mut interrupted = false;
    while should_continue_run(continuous, client.next_send_deadline(), shutdown_requested) {
        if should_send_probe(client.next_send_deadline(), shutdown_requested) {
            let events = client.send_probe()?;
            stream_output.print_events(&events)?;
        }

        let events = client.recv_available(RECV_BUDGET)?;
        stream_output.print_events(&events)?;

        let events = client.poll_timeouts()?;
        stream_output.print_events(&events)?;

        if is_shutdown_requested(shutdown_requested) {
            interrupted = true;
            break;
        }

        sleep_until_next_send(client.next_send_deadline());
    }
    interrupted |= is_shutdown_requested(shutdown_requested);

    if interrupted {
        eprintln!("interrupted, closing session...");
    }

    if should_drain_final(continuous, interrupted) {
        drain_final_replies(&mut client, &mut stream_output)?;
    }

    let events = client.poll_timeouts()?;
    stream_output.print_events(&events)?;

    let events = client.close()?;
    stream_output.print_events(&events)?;
    let print_final_summary = should_print_final_summary(continuous, interrupted);
    stream_output.print_final_summary = print_final_summary;
    stream_output.show_running_only_summary_note = continuous && interrupted && print_final_summary;
    stream_output.print_summary()?;
    stream_output.out.flush()?;
    Ok(())
}

fn is_shutdown_requested(shutdown_requested: &AtomicBool) -> bool {
    shutdown_requested.load(Ordering::Relaxed)
}

fn should_continue_run(
    continuous: bool,
    next_send_deadline: Option<Instant>,
    shutdown_requested: &AtomicBool,
) -> bool {
    !is_shutdown_requested(shutdown_requested) && (continuous || next_send_deadline.is_some())
}

fn should_send_probe(next_send_deadline: Option<Instant>, shutdown_requested: &AtomicBool) -> bool {
    !is_shutdown_requested(shutdown_requested)
        && next_send_deadline.is_some_and(|deadline| deadline <= Instant::now())
}

fn should_drain_final(continuous: bool, interrupted: bool) -> bool {
    !continuous || interrupted
}

fn should_print_final_summary(continuous: bool, interrupted: bool) -> bool {
    !continuous || interrupted
}

fn open_event(outcome: &OpenOutcome) -> &ClientEvent {
    match outcome {
        OpenOutcome::Started { event, .. } | OpenOutcome::NoTestCompleted { event, .. } => event,
    }
}

fn drain_final_replies<W: Write>(
    client: &mut Client,
    stream_output: &mut StreamOutput<'_, W>,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + final_drain_duration(client.probe_timeout());
    loop {
        let mut printed = false;

        let events = client.recv_available(RECV_BUDGET)?;
        printed |= !events.is_empty();
        stream_output.print_events(&events)?;

        let events = client.poll_timeouts()?;
        printed |= !events.is_empty();
        stream_output.print_events(&events)?;

        if client.is_run_complete() || Instant::now() >= deadline {
            break;
        }

        if !printed {
            thread::sleep(IDLE_SLEEP);
        }
    }
    Ok(())
}

fn sleep_until_next_send(deadline: Option<Instant>) {
    let sleep_for = match deadline {
        Some(deadline) => deadline
            .saturating_duration_since(Instant::now())
            .min(MAX_SLEEP),
        None => Duration::from_millis(1),
    };
    if !sleep_for.is_zero() {
        thread::sleep(sleep_for);
    }
}

struct StreamOutput<'a, W: Write> {
    mode: irtt_cli::OutputMode,
    human_options: HumanOutputOptions,
    print_final_summary: bool,
    show_running_only_summary_note: bool,
    out: &'a mut W,
    #[cfg(feature = "stats")]
    stats: &'a mut StatsCollector,
}

impl<W: Write> StreamOutput<'_, W> {
    fn print_events(&mut self, events: &[ClientEvent]) -> io::Result<()> {
        for event in events {
            self.print_event(event)?;
        }
        Ok(())
    }

    fn print_event(&mut self, event: &ClientEvent) -> io::Result<()> {
        #[cfg(feature = "stats")]
        let stats_update = self.stats.process(event);

        #[cfg(not(feature = "stats"))]
        let stats_update = irtt_cli::HumanEventStats::default();

        let line =
            if self.mode == OutputMode::Human && !matches!(event, ClientEvent::EchoSent { .. }) {
                Some(format_human_event_with_options(
                    event,
                    Some(stats_update.into()),
                    self.human_options,
                ))
            } else {
                format_event(event, self.mode)
            };

        if let Some(line) = line {
            writeln!(self.out, "{line}")?;
        }
        Ok(())
    }

    fn print_summary(&mut self) -> io::Result<()> {
        if !self.print_final_summary || !self.mode.prints_summary() {
            return Ok(());
        }

        #[cfg(feature = "stats")]
        {
            write!(
                self.out,
                "{}",
                irtt_cli::summary::format_summary_with_options(
                    &self.stats.snapshot(),
                    irtt_cli::summary::SummaryFormatOptions {
                        verbose: self.human_options.verbose,
                        show_running_only_note: self.show_running_only_summary_note,
                    },
                )
            )?;
        }

        Ok(())
    }
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
fn expected_probe_count(duration: Duration, interval: Duration) -> u64 {
    let interval_nanos = interval.as_nanos();
    if interval_nanos == 0 {
        return u64::MAX;
    }

    let expected = duration
        .as_nanos()
        .saturating_add(interval_nanos.saturating_sub(1))
        / interval_nanos;
    expected.min(u128::from(u64::MAX)) as u64
}

#[cfg(feature = "stats")]
fn estimate_finite_stats_memory_bytes(expected_probes: u64) -> u64 {
    expected_probes.saturating_mul(FINITE_STATS_BYTES_PER_PROBE)
}

#[cfg(feature = "stats")]
fn finite_stats_memory_warning(args: &CliArgs) -> Option<String> {
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

fn final_drain_duration(probe_timeout: Duration) -> Duration {
    probe_timeout.min(MAX_FINAL_DRAIN)
}

#[cfg(test)]
mod shutdown_tests {
    use super::*;

    #[test]
    fn shutdown_flag_stops_run_loop() {
        let shutdown = AtomicBool::new(false);
        assert!(should_continue_run(true, Some(Instant::now()), &shutdown));

        shutdown.store(true, Ordering::Relaxed);
        assert!(!should_continue_run(true, Some(Instant::now()), &shutdown));
    }

    #[test]
    fn shutdown_flag_suppresses_due_probe_send() {
        let shutdown = AtomicBool::new(false);
        let due = Instant::now() - Duration::from_millis(1);
        assert!(should_send_probe(Some(due), &shutdown));

        shutdown.store(true, Ordering::Relaxed);
        assert!(!should_send_probe(Some(due), &shutdown));
    }

    #[test]
    fn interrupted_continuous_run_uses_final_drain_before_close() {
        assert!(should_drain_final(true, true));
        assert!(!should_drain_final(true, false));
        assert!(should_drain_final(false, false));
    }

    #[test]
    fn final_summary_gate_prints_finite_or_interrupted_continuous() {
        assert!(should_print_final_summary(false, false));
        assert!(should_print_final_summary(false, true));
        assert!(!should_print_final_summary(true, false));
        assert!(should_print_final_summary(true, true));
    }
}

#[cfg(all(test, feature = "stats"))]
mod tests {
    use super::*;
    use irtt_client::{PacketMeta, RttSample, SignedDuration};
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        time::UNIX_EPOCH,
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
                effective: SignedDuration {
                    ns: i128::from(rtt_us) * 1_000,
                },
            },
            server_timing: None,
            one_way: None,
            received_stats: None,
            bytes: 64,
            packet_meta: PacketMeta::default(),
        }
    }

    fn cli_args(args: &[&str]) -> CliArgs {
        let mut argv = vec!["irtt-rs"];
        argv.extend_from_slice(args);
        CliArgs::try_parse_from(argv).unwrap()
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
                    effective: SignedDuration { ns: 1_200_000 },
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
                mode: irtt_cli::OutputMode::RttUs,
                human_options: HumanOutputOptions::default(),
                print_final_summary: true,
                show_running_only_summary_note: false,
                out: &mut out,
                stats: &mut stats,
            };
            stream_output.print_events(&events).unwrap();
            stream_output.print_summary().unwrap();
        }

        let rendered = String::from_utf8(out).unwrap();
        let summary = stats.snapshot();
        assert_eq!(rendered, "1200\n");
        assert_eq!(summary.packets.packets_sent, 1);
        assert_eq!(summary.packets.unique_replies, 1);
    }

    #[test]
    fn output_does_not_print_final_summary_when_disabled() {
        let mut stats = StatsCollector::new(StatsConfig::continuous());
        let mut out = Vec::new();
        {
            let mut stream_output = StreamOutput {
                mode: irtt_cli::OutputMode::Human,
                human_options: HumanOutputOptions::default(),
                print_final_summary: false,
                show_running_only_summary_note: true,
                out: &mut out,
                stats: &mut stats,
            };
            stream_output.print_summary().unwrap();
        }

        assert!(out.is_empty());
    }

    #[test]
    fn continuous_interrupted_output_prints_bounded_summary_when_enabled() {
        let mut stats = StatsCollector::new(StatsConfig::continuous());
        let mut out = Vec::new();
        {
            let mut stream_output = StreamOutput {
                mode: irtt_cli::OutputMode::Human,
                human_options: HumanOutputOptions::default(),
                print_final_summary: true,
                show_running_only_summary_note: true,
                out: &mut out,
                stats: &mut stats,
            };
            stream_output.print_event(&reply_event(1, 1200)).unwrap();
            stream_output.print_summary().unwrap();
        }

        let rendered = String::from_utf8(out).unwrap();
        assert!(rendered.contains("irtt-rs summary"));
        assert!(rendered.contains("medians unavailable"));
        assert!(rendered.contains("continuous mode"));
        assert!(rendered.contains("packets:"));
        assert!(rendered.contains("received=1"));
    }

    #[test]
    fn human_output_prints_ipdv_from_stats_update() {
        let events = [reply_event(0, 1200), reply_event(1, 1250)];
        let mut stats = StatsCollector::new(StatsConfig::finite());
        let mut out = Vec::new();
        {
            let mut stream_output = StreamOutput {
                mode: irtt_cli::OutputMode::Human,
                human_options: HumanOutputOptions::default(),
                print_final_summary: false,
                show_running_only_summary_note: false,
                out: &mut out,
                stats: &mut stats,
            };
            stream_output.print_events(&events).unwrap();
        }

        let rendered = String::from_utf8(out).unwrap();
        let mut lines = rendered.lines();
        assert!(lines.next().unwrap().contains("ipdv=n/a"));
        assert!(lines.next().unwrap().contains("ipdv=50.0µs"));
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
                    effective: SignedDuration { ns: 1_000 },
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
    fn finite_stats_memory_warning_skips_non_warning_runs() {
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
    }

    #[test]
    fn finite_stats_memory_warning_reports_threshold_tiers() {
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

    #[test]
    fn final_drain_uses_capped_probe_timeout() {
        assert_eq!(
            final_drain_duration(Duration::from_secs(4)),
            Duration::from_secs(4)
        );
        assert_eq!(
            final_drain_duration(Duration::from_secs(60)),
            Duration::from_secs(30)
        );
    }
}
