use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Condvar, Mutex, Weak,
    },
};

use crate::{error::EventSubscriptionError, event::ClientEvent};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubscriberConfig {
    pub capacity: usize,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriberOverflow {
    DropNewest,
    DropOldest,
    Disconnect,
}

#[derive(Debug, Clone)]
pub struct EventHub {
    inner: Arc<HubInner>,
}

#[derive(Debug)]
struct HubInner {
    next_id: AtomicU64,
    subscribers: Mutex<HashMap<u64, Arc<SubscriberInner>>>,
}

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
    pub fn new() -> Self {
        Self {
            inner: Arc::new(HubInner {
                next_id: AtomicU64::new(1),
                subscribers: Mutex::new(HashMap::new()),
            }),
        }
    }

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
        }
    }

    fn event_message(event: ClientEvent) -> String {
        match event {
            ClientEvent::Warning { message, .. } => message,
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn publishes_to_one_subscriber() {
        let hub = EventHub::new();
        let sub = hub.subscribe(SubscriberConfig::default()).unwrap();

        hub.publish(event(1));

        assert_eq!(event_message(sub.try_recv().unwrap().unwrap()), "event-1");
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
    fn drop_newest_keeps_existing_events() {
        let hub = EventHub::new();
        let sub = hub
            .subscribe(SubscriberConfig {
                capacity: 1,
                overflow: SubscriberOverflow::DropNewest,
            })
            .unwrap();

        hub.publish(event(1));
        hub.publish(event(2));

        assert_eq!(event_message(sub.try_recv().unwrap().unwrap()), "event-1");
        assert!(sub.try_recv().unwrap().is_none());
    }

    #[test]
    fn drop_oldest_replaces_existing_events() {
        let hub = EventHub::new();
        let sub = hub
            .subscribe(SubscriberConfig {
                capacity: 1,
                overflow: SubscriberOverflow::DropOldest,
            })
            .unwrap();

        hub.publish(event(1));
        hub.publish(event(2));

        assert_eq!(event_message(sub.try_recv().unwrap().unwrap()), "event-2");
        assert!(sub.try_recv().unwrap().is_none());
    }

    #[test]
    fn disconnect_removes_full_subscriber() {
        let hub = EventHub::new();
        let sub = hub
            .subscribe(SubscriberConfig {
                capacity: 1,
                overflow: SubscriberOverflow::Disconnect,
            })
            .unwrap();

        hub.publish(event(1));
        hub.publish(event(2));

        assert_eq!(
            sub.try_recv().unwrap_err(),
            EventSubscriptionError::Disconnected
        );
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
