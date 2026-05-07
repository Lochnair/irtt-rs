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
use irtt_cli::{format_event, CliArgs};
use irtt_client::{Client, ClientEvent, ClientTimestamp, OpenOutcome, RecvBudget};
#[cfg(feature = "stats")]
use irtt_stats::{StatsCollector, StatsConfig};

const RECV_BUDGET: RecvBudget = RecvBudget { max_packets: 16 };
const MAX_FINAL_DRAIN: Duration = Duration::from_secs(30);
const IDLE_SLEEP: Duration = Duration::from_millis(5);
const MAX_SLEEP: Duration = Duration::from_millis(20);

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
    let mode = args.output;
    let continuous = args.is_continuous();
    let mut stdout = io::LineWriter::new(io::stdout().lock());
    #[cfg(feature = "stats")]
    let mut stats = StatsCollector::new(stats_config(continuous));
    let mut output = EventOutput {
        mode,
        print_finite_summary: !continuous,
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

    let open = client.open(ClientTimestamp::now())?;
    output.print_event(open_event(&open))?;

    let mut interrupted = false;
    while should_continue_run(continuous, client.next_send_deadline(), shutdown_requested) {
        if should_send_probe(client.next_send_deadline(), shutdown_requested) {
            let events = client.send_probe()?;
            output.print_events(&events)?;
        }

        let events = client.recv_available(RECV_BUDGET)?;
        output.print_events(&events)?;

        let events = client.poll_timeouts(ClientTimestamp::now())?;
        output.print_events(&events)?;

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
        drain_final_replies(&mut client, &mut output)?;
    }

    let events = client.poll_timeouts(ClientTimestamp::now())?;
    output.print_events(&events)?;

    let events = client.close(ClientTimestamp::now())?;
    output.print_events(&events)?;
    output.print_summary()?;
    output.out.flush()?;
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

fn open_event(outcome: &OpenOutcome) -> &ClientEvent {
    match outcome {
        OpenOutcome::Started { event, .. } | OpenOutcome::NoTestCompleted { event, .. } => event,
    }
}

fn drain_final_replies<W: Write>(
    client: &mut Client,
    output: &mut EventOutput<'_, W>,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + final_drain_duration(client.probe_timeout());
    loop {
        let mut printed = false;

        let events = client.recv_available(RECV_BUDGET)?;
        printed |= !events.is_empty();
        output.print_events(&events)?;

        let events = client.poll_timeouts(ClientTimestamp::now())?;
        printed |= !events.is_empty();
        output.print_events(&events)?;

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

struct EventOutput<'a, W: Write> {
    mode: irtt_cli::OutputMode,
    print_finite_summary: bool,
    out: &'a mut W,
    #[cfg(feature = "stats")]
    stats: &'a mut StatsCollector,
}

impl<W: Write> EventOutput<'_, W> {
    fn print_events(&mut self, events: &[ClientEvent]) -> io::Result<()> {
        for event in events {
            self.print_event(event)?;
        }
        Ok(())
    }

    fn print_event(&mut self, event: &ClientEvent) -> io::Result<()> {
        #[cfg(feature = "stats")]
        self.stats.process(event);

        if let Some(line) = format_event(event, self.mode) {
            writeln!(self.out, "{line}")?;
        }
        Ok(())
    }

    fn print_summary(&mut self) -> io::Result<()> {
        if !self.print_finite_summary || !self.mode.prints_summary() {
            return Ok(());
        }

        #[cfg(feature = "stats")]
        {
            write!(
                self.out,
                "{}",
                irtt_cli::summary::format_summary(&self.stats.summary())
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

    #[test]
    fn output_helper_streams_and_collects_events() {
        let sent_at = test_timestamp(Duration::from_secs(1));
        let received_at = test_timestamp(Duration::from_secs(1) + Duration::from_micros(1200));
        let events = [
            ClientEvent::EchoSent {
                seq: 1,
                logical_seq: 1,
                remote: test_remote(),
                scheduled_at: sent_at.mono,
                sent_at,
                bytes: 64,
                send_call: Duration::from_micros(10),
                timer_error: Duration::ZERO,
            },
            ClientEvent::EchoReply {
                seq: 1,
                logical_seq: 1,
                remote: test_remote(),
                sent_at,
                received_at,
                rtt: RttSample {
                    raw: Duration::from_micros(1200),
                    adjusted: None,
                    effective: Duration::from_micros(1200),
                    adjusted_signed: None,
                    effective_signed: SignedDuration { ns: 1_200_000 },
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
            let mut output = EventOutput {
                mode: irtt_cli::OutputMode::RttUs,
                print_finite_summary: true,
                out: &mut out,
                stats: &mut stats,
            };
            output.print_events(&events).unwrap();
            output.print_summary().unwrap();
        }

        let rendered = String::from_utf8(out).unwrap();
        let summary = stats.summary();
        assert_eq!(rendered, "1200\n");
        assert_eq!(summary.packets.packets_sent, 1);
        assert_eq!(summary.packets.unique_replies, 1);
    }

    #[test]
    fn continuous_output_does_not_print_finite_summary() {
        let mut stats = StatsCollector::new(StatsConfig::continuous());
        let mut out = Vec::new();
        {
            let mut output = EventOutput {
                mode: irtt_cli::OutputMode::Human,
                print_finite_summary: false,
                out: &mut out,
                stats: &mut stats,
            };
            output.print_summary().unwrap();
        }

        assert!(out.is_empty());
    }

    #[test]
    fn continuous_mode_uses_continuous_stats_config() {
        let mut collector = StatsCollector::new(stats_config(true));
        for seq in 0..5000 {
            let sent_at = test_timestamp(Duration::from_micros(seq));
            let received_at = test_timestamp(Duration::from_micros(seq + 1));
            collector.process(&ClientEvent::EchoReply {
                seq: seq as u32,
                logical_seq: seq,
                remote: test_remote(),
                sent_at,
                received_at,
                rtt: RttSample {
                    raw: Duration::from_micros(1),
                    adjusted: None,
                    effective: Duration::from_micros(1),
                    adjusted_signed: None,
                    effective_signed: SignedDuration { ns: 1_000 },
                },
                server_timing: None,
                one_way: None,
                received_stats: None,
                bytes: 64,
                packet_meta: PacketMeta::default(),
            });
        }

        assert_eq!(collector.summary().rtt.primary.median_ns, None);
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
