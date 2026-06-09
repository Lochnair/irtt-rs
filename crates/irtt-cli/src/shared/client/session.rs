use std::{
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::{Duration, Instant},
};

use irtt_client::{Client, ClientConfig, ClientEvent, OpenOutcome, RecvBudget};

pub const RECV_BUDGET: RecvBudget = RecvBudget { max_packets: 16 };
const MAX_FINAL_DRAIN: Duration = Duration::from_secs(30);
const IDLE_SLEEP: Duration = Duration::from_millis(5);
const MAX_SLEEP: Duration = Duration::from_millis(20);

pub struct ClientSession {
    client: Client,
    continuous: bool,
}

impl ClientSession {
    pub fn connect(
        config: ClientConfig,
        continuous: bool,
    ) -> Result<Self, irtt_client::ClientError> {
        Ok(Self {
            client: Client::connect(config)?,
            continuous,
        })
    }

    pub fn open(&mut self) -> Result<Vec<ClientEvent>, irtt_client::ClientError> {
        Ok(vec![open_event(&self.client.open()?).clone()])
    }

    pub fn step(
        &mut self,
        shutdown_requested: &AtomicBool,
    ) -> Result<Vec<ClientEvent>, irtt_client::ClientError> {
        let mut events = Vec::new();
        if should_send_probe(self.client.next_send_deadline(), shutdown_requested) {
            events.extend(self.client.send_probe()?);
        }
        events.extend(self.client.recv_available(RECV_BUDGET)?);
        events.extend(self.client.poll_timeouts()?);
        Ok(events)
    }

    pub fn should_continue(&self, shutdown_requested: &AtomicBool) -> bool {
        !is_shutdown_requested(shutdown_requested)
            && (self.continuous || self.client.next_send_deadline().is_some())
    }

    pub fn should_drain_final(&self, interrupted: bool) -> bool {
        should_drain_final(self.continuous, interrupted)
    }

    pub fn drain_final<F>(&mut self, mut on_events: F) -> Result<(), irtt_client::ClientError>
    where
        F: FnMut(&[ClientEvent]),
    {
        let deadline = Instant::now() + final_drain_duration(self.client.probe_timeout());
        loop {
            let mut received = false;

            let events = self.client.recv_available(RECV_BUDGET)?;
            received |= !events.is_empty();
            on_events(&events);

            let events = self.client.poll_timeouts()?;
            received |= !events.is_empty();
            on_events(&events);

            if self.client.is_run_complete() || Instant::now() >= deadline {
                break;
            }

            if !received {
                thread::sleep(IDLE_SLEEP);
            }
        }
        Ok(())
    }

    pub fn poll_timeouts(&mut self) -> Result<Vec<ClientEvent>, irtt_client::ClientError> {
        self.client.poll_timeouts()
    }

    pub fn close(&mut self) -> Result<Vec<ClientEvent>, irtt_client::ClientError> {
        self.client.close()
    }

    pub fn next_send_deadline(&self) -> Option<Instant> {
        self.client.next_send_deadline()
    }

    pub fn probe_timeout(&self) -> Duration {
        self.client.probe_timeout()
    }

    pub fn sleep_until_next_send(&self) {
        let sleep_for = match self.next_send_deadline() {
            Some(deadline) => deadline
                .saturating_duration_since(Instant::now())
                .min(MAX_SLEEP),
            None => Duration::from_millis(1),
        };
        if !sleep_for.is_zero() {
            thread::sleep(sleep_for);
        }
    }
}

pub fn is_shutdown_requested(shutdown_requested: &AtomicBool) -> bool {
    shutdown_requested.load(Ordering::Relaxed)
}

pub fn should_drain_final(continuous: bool, interrupted: bool) -> bool {
    !continuous || interrupted
}

pub fn should_print_final_summary(continuous: bool, interrupted: bool) -> bool {
    !continuous || interrupted
}

pub fn final_drain_duration(probe_timeout: Duration) -> Duration {
    probe_timeout.min(MAX_FINAL_DRAIN)
}

fn should_send_probe(next_send_deadline: Option<Instant>, shutdown_requested: &AtomicBool) -> bool {
    !is_shutdown_requested(shutdown_requested)
        && next_send_deadline.is_some_and(|deadline| deadline <= Instant::now())
}

fn open_event(outcome: &OpenOutcome) -> &ClientEvent {
    match outcome {
        OpenOutcome::Started { event, .. } | OpenOutcome::NoTestCompleted { event, .. } => event,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn should_drain_final_for_finite_or_interrupted_runs() {
        assert!(should_drain_final(false, false));
        assert!(should_drain_final(false, true));
        assert!(should_drain_final(true, true));
        assert!(!should_drain_final(true, false));
    }
}
