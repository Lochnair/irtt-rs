use std::{
    io::{self, Write},
    process::ExitCode,
    thread,
    time::{Duration, Instant},
};

use clap::Parser;
use irtt_cli::{format_event, CliArgs};
use irtt_client::{Client, ClientEvent, ClientTimestamp, OpenOutcome, RecvBudget};

const RECV_BUDGET: RecvBudget = RecvBudget { max_packets: 16 };
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
    let mut stdout = io::BufWriter::new(io::stdout().lock());
    let mut client = Client::connect(args.to_client_config())?;

    let open = client.open(ClientTimestamp::now())?;
    print_event(open_event(&open), mode, &mut stdout)?;

    while let Some(deadline) = client.next_send_deadline() {
        let now = Instant::now();
        if deadline <= now {
            let events = client.send_probe()?;
            print_events(&events, mode, &mut stdout)?;
        }

        let events = client.recv_available(RECV_BUDGET)?;
        print_events(&events, mode, &mut stdout)?;

        let events = client.poll_timeouts(ClientTimestamp::now())?;
        print_events(&events, mode, &mut stdout)?;

        sleep_until_next_send(client.next_send_deadline());
    }

    drain_final_replies(&mut client, mode, &mut stdout)?;

    let events = client.poll_timeouts(ClientTimestamp::now())?;
    print_events(&events, mode, &mut stdout)?;

    let events = client.close(ClientTimestamp::now())?;
    print_events(&events, mode, &mut stdout)?;
    stdout.flush()?;
    Ok(())
}

fn open_event(outcome: &OpenOutcome) -> &ClientEvent {
    match outcome {
        OpenOutcome::Started { event, .. } | OpenOutcome::NoTestCompleted { event, .. } => event,
    }
}

fn drain_final_replies<W: Write>(
    client: &mut Client,
    mode: irtt_cli::OutputMode,
    out: &mut W,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + FINAL_DRAIN;
    while Instant::now() < deadline {
        let events = client.recv_available(RECV_BUDGET)?;
        if events.is_empty() {
            thread::sleep(Duration::from_millis(5));
        } else {
            print_events(&events, mode, out)?;
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

fn print_events<W: Write>(
    events: &[ClientEvent],
    mode: irtt_cli::OutputMode,
    out: &mut W,
) -> io::Result<()> {
    for event in events {
        print_event(event, mode, out)?;
    }
    Ok(())
}

fn print_event<W: Write>(
    event: &ClientEvent,
    mode: irtt_cli::OutputMode,
    out: &mut W,
) -> io::Result<()> {
    if let Some(line) = format_event(event, mode) {
        writeln!(out, "{line}")?;
    }
    Ok(())
}
