use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Condvar, Mutex, Weak,
    },
};

use crate::{error::EventSubscriptionError, event::ClientEvent};

/// Configuration for one managed event subscriber.
///
/// Each subscriber has an independent bounded queue. The queue bounds memory
/// growth when event production is faster than that subscriber's consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubscriberConfig {
    /// Maximum number of events buffered for this subscriber.
    ///
    /// Must be greater than zero.
    pub capacity: usize,
    /// Policy applied when publishing to a full subscriber queue.
    pub overflow: SubscriberOverflow,
}

impl Default for SubscriberConfig {
    fn default() -> Self {
        Self {
            capacity: 1024,
            overflow: SubscriberOverflow::DropNewest,
        }
    }
}

/// Overflow behavior for a bounded event subscription queue.
///
/// These policies are applied independently per subscriber when that
/// subscriber's queue is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriberOverflow {
    /// Leave the existing queue unchanged and discard the newly published event.
    DropNewest,
    /// Remove the oldest queued event and enqueue the newly published event.
    DropOldest,
    /// Disconnect the subscriber and clear its queue.
    Disconnect,
}

/// Publish/subscribe fan-out for [`ClientEvent`] values.
///
/// `EventHub` is used by managed sessions internally and is also exported for
/// callers that want the same bounded subscription behavior around their own
/// event producer. Publishing clones each event once per subscriber.
#[derive(Debug, Clone)]
pub struct EventHub {
    inner: Arc<HubInner>,
}

#[derive(Debug)]
struct HubInner {
    next_id: AtomicU64,
    subscribers: Mutex<HashMap<u64, Arc<SubscriberInner>>>,
}

/// Handle for receiving managed client events.
///
/// Each subscription has its own bounded queue configured by
/// [`SubscriberConfig`]. The configured [`SubscriberOverflow`] policy applies
/// when that queue is full. Dropping the handle unregisters the subscriber.
#[must_use = "dropping the subscription unregisters it"]
#[derive(Debug)]
pub struct EventSubscription {
    id: u64,
    hub: Weak<HubInner>,
    inner: Arc<SubscriberInner>,
}

#[derive(Debug)]
struct SubscriberInner {
    state: Mutex<SubscriberState>,
    available: Condvar,
    config: SubscriberConfig,
}

#[derive(Debug)]
struct SubscriberState {
    queue: VecDeque<ClientEvent>,
    connected: bool,
}

impl EventHub {
    /// Create an empty event hub with no subscribers.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(HubInner {
                next_id: AtomicU64::new(1),
                subscribers: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Register a new subscriber with its own bounded queue.
    ///
    /// Returns an error when `config.capacity` is zero.
    pub fn subscribe(
        &self,
        config: SubscriberConfig,
    ) -> Result<EventSubscription, crate::ClientError> {
        if config.capacity == 0 {
            return Err(crate::ClientError::InvalidConfig {
                reason: "subscriber capacity must be greater than zero".to_owned(),
            });
        }

        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let inner = Arc::new(SubscriberInner {
            state: Mutex::new(SubscriberState {
                queue: VecDeque::with_capacity(config.capacity),
                connected: true,
            }),
            available: Condvar::new(),
            config,
        });
        self.inner
            .subscribers
            .lock()
            .expect("event hub mutex poisoned")
            .insert(id, inner.clone());

        Ok(EventSubscription {
            id,
            hub: Arc::downgrade(&self.inner),
            inner,
        })
    }

    /// Publish an event to all currently connected subscribers.
    ///
    /// Slow subscribers are handled according to their
    /// [`SubscriberOverflow`] policy. Publishing does not block waiting for a
    /// subscriber to consume queued events.
    pub fn publish(&self, event: ClientEvent) {
        let subscribers: Vec<(u64, Arc<SubscriberInner>)> = self
            .inner
            .subscribers
            .lock()
            .expect("event hub mutex poisoned")
            .iter()
            .map(|(id, subscriber)| (*id, subscriber.clone()))
            .collect();

        let mut disconnected = Vec::new();
        for (id, subscriber) in subscribers {
            // Events are cloned per subscriber; bounded queues keep slow
            // consumers from causing unbounded memory growth.
            if !subscriber.publish(event.clone()) {
                disconnected.push(id);
            }
        }

        if disconnected.is_empty() {
            return;
        }

        let mut subscribers = self
            .inner
            .subscribers
            .lock()
            .expect("event hub mutex poisoned");
        for id in disconnected {
            subscribers.remove(&id);
        }
    }

    /// Disconnect all subscribers after leaving their already queued events
    /// available to drain.
    pub fn disconnect_all(&self) {
        let subscribers: Vec<Arc<SubscriberInner>> = self
            .inner
            .subscribers
            .lock()
            .expect("event hub mutex poisoned")
            .drain()
            .map(|(_, subscriber)| subscriber)
            .collect();

        for subscriber in subscribers {
            subscriber.disconnect();
        }
    }

    #[cfg(test)]
    fn subscriber_count(&self) -> usize {
        self.inner
            .subscribers
            .lock()
            .expect("event hub mutex poisoned")
            .len()
    }
}

impl Default for EventHub {
    fn default() -> Self {
        Self::new()
    }
}

impl EventSubscription {
    /// Block until the next queued event is available.
    ///
    /// If the subscription is disconnected, this returns
    /// [`EventSubscriptionError::Disconnected`] after any already queued events
    /// have been drained.
    pub fn recv(&self) -> Result<ClientEvent, EventSubscriptionError> {
        let mut state = self.inner.state.lock().expect("subscriber mutex poisoned");
        loop {
            if let Some(event) = state.queue.pop_front() {
                return Ok(event);
            }
            if !state.connected {
                return Err(EventSubscriptionError::Disconnected);
            }
            state = self
                .inner
                .available
                .wait(state)
                .expect("subscriber mutex poisoned");
        }
    }

    /// Try to receive one queued event without blocking.
    ///
    /// Returns `Ok(None)` when the subscription is still connected but no event
    /// is queued. If the subscription is disconnected, this returns
    /// [`EventSubscriptionError::Disconnected`] after any already queued events
    /// have been drained.
    pub fn try_recv(&self) -> Result<Option<ClientEvent>, EventSubscriptionError> {
        let mut state = self.inner.state.lock().expect("subscriber mutex poisoned");
        if let Some(event) = state.queue.pop_front() {
            return Ok(Some(event));
        }
        if !state.connected {
            return Err(EventSubscriptionError::Disconnected);
        }
        Ok(None)
    }
}

impl Drop for EventSubscription {
    fn drop(&mut self) {
        self.inner.disconnect();
        if let Some(hub) = self.hub.upgrade() {
            hub.subscribers
                .lock()
                .expect("event hub mutex poisoned")
                .remove(&self.id);
        }
    }
}

impl SubscriberInner {
    fn publish(&self, event: ClientEvent) -> bool {
        let mut state = self.state.lock().expect("subscriber mutex poisoned");
        if !state.connected {
            return false;
        }

        if state.queue.len() < self.config.capacity {
            state.queue.push_back(event);
            self.available.notify_one();
            return true;
        }

        match self.config.overflow {
            SubscriberOverflow::DropNewest => true,
            SubscriberOverflow::DropOldest => {
                state.queue.pop_front();
                state.queue.push_back(event);
                self.available.notify_one();
                true
            }
            SubscriberOverflow::Disconnect => {
                state.queue.clear();
                state.connected = false;
                self.available.notify_all();
                false
            }
        }
    }

    fn disconnect(&self) {
        let mut state = self.state.lock().expect("subscriber mutex poisoned");
        state.connected = false;
        self.available.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClientTimestamp, WarningKind};

    fn event(n: usize) -> ClientEvent {
        ClientEvent::Warning {
            kind: WarningKind::UntrackedReply,
            message: format!("event-{n}"),
            at: ClientTimestamp::now(),
        }
    }

    fn event_message(event: ClientEvent) -> String {
        match event {
            ClientEvent::Warning { message, .. } => message,
            other => panic!("unexpected event: {other:?}"),
        }
    }

    fn assert_next(
        sub: &EventSubscription,
        expected: Result<Option<&str>, EventSubscriptionError>,
    ) {
        match (sub.try_recv(), expected) {
            (Ok(Some(event)), Ok(Some(message))) => assert_eq!(event_message(event), message),
            (Ok(None), Ok(None)) => {}
            (Err(actual), Err(expected)) => assert_eq!(actual, expected),
            (actual, expected) => panic!("expected {expected:?}, got {actual:?}"),
        }
    }

    #[test]
    fn publishes_to_multiple_subscribers() {
        let hub = EventHub::new();
        let a = hub.subscribe(SubscriberConfig::default()).unwrap();
        let b = hub.subscribe(SubscriberConfig::default()).unwrap();

        hub.publish(event(1));

        assert_eq!(event_message(a.try_recv().unwrap().unwrap()), "event-1");
        assert_eq!(event_message(b.try_recv().unwrap().unwrap()), "event-1");
    }

    #[test]
    fn full_subscriber_overflow_policy_is_applied() {
        let cases = [
            (
                SubscriberOverflow::DropNewest,
                Ok(Some("event-1")),
                Ok(None),
            ),
            (
                SubscriberOverflow::DropOldest,
                Ok(Some("event-2")),
                Ok(None),
            ),
            (
                SubscriberOverflow::Disconnect,
                Err(EventSubscriptionError::Disconnected),
                Err(EventSubscriptionError::Disconnected),
            ),
        ];

        for (overflow, first_expected, second_expected) in cases {
            let hub = EventHub::new();
            let sub = hub
                .subscribe(SubscriberConfig {
                    capacity: 1,
                    overflow,
                })
                .unwrap();

            hub.publish(event(1));
            hub.publish(event(2));

            assert_next(&sub, first_expected);
            assert_next(&sub, second_expected);
        }
    }

    #[test]
    fn full_subscriber_does_not_prevent_other_delivery() {
        let hub = EventHub::new();
        let slow = hub
            .subscribe(SubscriberConfig {
                capacity: 1,
                overflow: SubscriberOverflow::DropNewest,
            })
            .unwrap();
        let fast = hub
            .subscribe(SubscriberConfig {
                capacity: 4,
                overflow: SubscriberOverflow::DropNewest,
            })
            .unwrap();

        hub.publish(event(1));
        hub.publish(event(2));

        assert_eq!(event_message(slow.try_recv().unwrap().unwrap()), "event-1");
        assert_eq!(event_message(fast.try_recv().unwrap().unwrap()), "event-1");
        assert_eq!(event_message(fast.try_recv().unwrap().unwrap()), "event-2");
    }

    #[test]
    fn subscribing_after_events_does_not_replay() {
        let hub = EventHub::new();
        hub.publish(event(1));

        let sub = hub.subscribe(SubscriberConfig::default()).unwrap();

        assert!(sub.try_recv().unwrap().is_none());
    }

    #[test]
    fn dropping_subscription_unregisters_from_hub() {
        let hub = EventHub::new();
        let sub = hub.subscribe(SubscriberConfig::default()).unwrap();
        assert_eq!(hub.subscriber_count(), 1);

        drop(sub);

        assert_eq!(hub.subscriber_count(), 0);
    }

    #[test]
    fn disconnect_all_wakes_blocking_receivers() {
        let hub = EventHub::new();
        let sub = hub.subscribe(SubscriberConfig::default()).unwrap();
        hub.disconnect_all();

        assert_eq!(
            sub.recv().unwrap_err(),
            EventSubscriptionError::Disconnected
        );
    }

    #[test]
    fn session_closed_event_can_be_queued() {
        let hub = EventHub::new();
        let sub = hub.subscribe(SubscriberConfig::default()).unwrap();
        hub.publish(ClientEvent::SessionClosed {
            remote: "127.0.0.1:1".parse().unwrap(),
            token: 1,
            at: ClientTimestamp::now(),
        });

        assert!(matches!(
            sub.recv().unwrap(),
            ClientEvent::SessionClosed { token: 1, .. }
        ));
    }
}
