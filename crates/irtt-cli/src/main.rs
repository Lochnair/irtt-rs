use std::{
    io::{self, Write},
    process::ExitCode,
    thread,
    time::{Duration, Instant},
};

use clap::Parser;
use irtt_cli::{format_event, CliArgs};
use irtt_client::{Client, ClientEvent, ClientTimestamp, OpenOutcome, RecvBudget};
#[cfg(feature = "stats")]
use irtt_stats::{StatsCollector, StatsConfig};

const RECV_BUDGET: RecvBudget = RecvBudget { max_packets: 16 };
// TODO: derive final drain from pending probe timeout behavior once the client
// exposes enough state for the CLI to stop guessing.
const FINAL_DRAIN: Duration = Duration::from_millis(100);
const MAX_SLEEP: Duration = Duration::from_millis(20);

fn main() -> ExitCode {
    let args = CliArgs::parse();
    match run(args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("irtt-rs: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: CliArgs) -> Result<(), Box<dyn std::error::Error>> {
    let mode = args.output;
    let mut stdout = io::LineWriter::new(io::stdout().lock());
    #[cfg(feature = "stats")]
    let mut stats = StatsCollector::new(StatsConfig::finite());
    let mut output = EventOutput {
        mode,
        out: &mut stdout,
        #[cfg(feature = "stats")]
        stats: &mut stats,
    };
    let mut client = Client::connect(args.to_client_config())?;

    let open = client.open(ClientTimestamp::now())?;
    output.print_event(open_event(&open))?;

    while let Some(deadline) = client.next_send_deadline() {
        let now = Instant::now();
        if deadline <= now {
            let events = client.send_probe()?;
            output.print_events(&events)?;
        }

        let events = client.recv_available(RECV_BUDGET)?;
        output.print_events(&events)?;

        let events = client.poll_timeouts(ClientTimestamp::now())?;
        output.print_events(&events)?;

        sleep_until_next_send(client.next_send_deadline());
    }

    drain_final_replies(&mut client, &mut output)?;

    let events = client.poll_timeouts(ClientTimestamp::now())?;
    output.print_events(&events)?;

    let events = client.close(ClientTimestamp::now())?;
    output.print_events(&events)?;
    output.print_summary()?;
    output.out.flush()?;
    Ok(())
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
    let deadline = Instant::now() + FINAL_DRAIN;
    while Instant::now() < deadline {
        let events = client.recv_available(RECV_BUDGET)?;
        if events.is_empty() {
            thread::sleep(Duration::from_millis(5));
        } else {
            output.print_events(&events)?;
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
        if !self.mode.prints_summary() {
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
}
