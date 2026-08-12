//! Notification system matching Temporal's service/history/notification.
//!
//! Handles workflow state change notifications, time-skipping notifications,
//! and event fan-out to subscribers (frontend, matching, worker services).

use std::collections::{HashMap, VecDeque};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, RwLock,
};
use std::time::{Duration, SystemTime};

// ═══════════════════════════════════════════════════════════════════════════════
// Notification Types
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NotificationType {
    WorkflowStarted,
    WorkflowCompleted,
    WorkflowFailed,
    WorkflowCancelled,
    WorkflowTerminated,
    WorkflowContinuedAsNew,
    WorkflowTimedOut,
    ActivityScheduled,
    ActivityStarted,
    ActivityCompleted,
    ActivityFailed,
    TimerStarted,
    TimerFired,
    TimerCancelled,
    ChildWorkflowStarted,
    ChildWorkflowCompleted,
    ChildWorkflowFailed,
    SignalReceived,
    SignalExternal,
    QueryReceived,
    QueryCompleted,
    UpdateAdmitted,
    UpdateAccepted,
    UpdateCompleted,
    UpdateRejected,
    NamespaceReplicationConfigUpdated,
    NamespaceFailover,
    ShardOwnershipChanged,
    ShardMoved,
}

impl NotificationType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::WorkflowStarted => "WorkflowStarted",
            Self::WorkflowCompleted => "WorkflowCompleted",
            Self::WorkflowFailed => "WorkflowFailed",
            Self::WorkflowCancelled => "WorkflowCancelled",
            Self::WorkflowTerminated => "WorkflowTerminated",
            Self::WorkflowContinuedAsNew => "WorkflowContinuedAsNew",
            Self::WorkflowTimedOut => "WorkflowTimedOut",
            Self::ActivityScheduled => "ActivityScheduled",
            Self::ActivityStarted => "ActivityStarted",
            Self::ActivityCompleted => "ActivityCompleted",
            Self::ActivityFailed => "ActivityFailed",
            Self::TimerStarted => "TimerStarted",
            Self::TimerFired => "TimerFired",
            Self::TimerCancelled => "TimerCancelled",
            Self::ChildWorkflowStarted => "ChildWorkflowStarted",
            Self::ChildWorkflowCompleted => "ChildWorkflowCompleted",
            Self::ChildWorkflowFailed => "ChildWorkflowFailed",
            Self::SignalReceived => "SignalReceived",
            Self::SignalExternal => "SignalExternal",
            Self::QueryReceived => "QueryReceived",
            Self::QueryCompleted => "QueryCompleted",
            Self::UpdateAdmitted => "UpdateAdmitted",
            Self::UpdateAccepted => "UpdateAccepted",
            Self::UpdateCompleted => "UpdateCompleted",
            Self::UpdateRejected => "UpdateRejected",
            Self::NamespaceReplicationConfigUpdated => "NamespaceReplicationConfigUpdated",
            Self::NamespaceFailover => "NamespaceFailover",
            Self::ShardOwnershipChanged => "ShardOwnershipChanged",
            Self::ShardMoved => "ShardMoved",
        }
    }

    pub fn category(&self) -> NotificationCategory {
        match self {
            Self::WorkflowStarted
            | Self::WorkflowCompleted
            | Self::WorkflowFailed
            | Self::WorkflowCancelled
            | Self::WorkflowTerminated
            | Self::WorkflowContinuedAsNew
            | Self::WorkflowTimedOut => NotificationCategory::Workflow,
            Self::ActivityScheduled
            | Self::ActivityStarted
            | Self::ActivityCompleted
            | Self::ActivityFailed => NotificationCategory::Activity,
            Self::TimerStarted | Self::TimerFired | Self::TimerCancelled => {
                NotificationCategory::Timer
            }
            Self::ChildWorkflowStarted
            | Self::ChildWorkflowCompleted
            | Self::ChildWorkflowFailed => NotificationCategory::ChildWorkflow,
            Self::SignalReceived | Self::SignalExternal => NotificationCategory::Signal,
            Self::QueryReceived | Self::QueryCompleted => NotificationCategory::Query,
            Self::UpdateAdmitted
            | Self::UpdateAccepted
            | Self::UpdateCompleted
            | Self::UpdateRejected => NotificationCategory::Update,
            Self::NamespaceReplicationConfigUpdated | Self::NamespaceFailover => {
                NotificationCategory::Namespace
            }
            Self::ShardOwnershipChanged | Self::ShardMoved => NotificationCategory::Shard,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NotificationCategory {
    Workflow,
    Activity,
    Timer,
    ChildWorkflow,
    Signal,
    Query,
    Update,
    Namespace,
    Shard,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Notification Event
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct NotificationEvent {
    pub event_id: u64,
    pub notification_type: NotificationType,
    pub namespace_id: String,
    pub workflow_id: String,
    pub run_id: String,
    pub shard_id: u32,
    pub version: i64,
    pub payload: HashMap<String, Vec<u8>>,
    pub created_at: i64,
    pub priority: NotificationPriority,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum NotificationPriority {
    Low,
    Normal,
    High,
    Critical,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Subscription — a subscriber interested in certain notification types
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubscriberId {
    Frontend,
    Matching,
    Worker,
    Replication,
    Archival,
    Visibility,
    Admin,
    Custom(u32),
}

pub struct Subscription {
    pub subscriber: SubscriberId,
    pub filter: NotificationFilter,
    pub buffer: RwLock<VecDeque<NotificationEvent>>,
    pub max_buffer_size: usize,
    pub dropped_count: AtomicU64,
    pub delivered_count: AtomicU64,
    pub active: AtomicBool,
}

#[derive(Debug, Clone)]
pub enum NotificationFilter {
    All,
    Categories(Vec<NotificationCategory>),
    Types(Vec<NotificationType>),
    Namespace(String),
    Shard(u32),
    Composite {
        categories: Vec<NotificationCategory>,
        namespace: Option<String>,
    },
}

impl NotificationFilter {
    pub fn matches(&self, event: &NotificationEvent) -> bool {
        match self {
            NotificationFilter::All => true,
            NotificationFilter::Categories(cats) => {
                cats.contains(&event.notification_type.category())
            }
            NotificationFilter::Types(types) => types.contains(&event.notification_type),
            NotificationFilter::Namespace(ns) => event.namespace_id == *ns,
            NotificationFilter::Shard(sid) => event.shard_id == *sid,
            NotificationFilter::Composite {
                categories,
                namespace,
            } => {
                let cat_match = categories.contains(&event.notification_type.category());
                let ns_match = namespace
                    .as_ref()
                    .map_or(true, |ns| event.namespace_id == *ns);
                cat_match && ns_match
            }
        }
    }
}

impl Subscription {
    pub fn new(subscriber: SubscriberId, filter: NotificationFilter, max_buffer: usize) -> Self {
        Self {
            subscriber,
            filter,
            buffer: RwLock::new(VecDeque::new()),
            max_buffer_size: max_buffer,
            dropped_count: AtomicU64::new(0),
            delivered_count: AtomicU64::new(0),
            active: AtomicBool::new(true),
        }
    }

    pub fn deliver(&self, event: NotificationEvent) -> bool {
        if !self.active.load(Ordering::Relaxed) {
            return false;
        }
        if !self.filter.matches(&event) {
            return false;
        }
        let mut buf = self.buffer.write().unwrap();
        if buf.len() >= self.max_buffer_size {
            buf.pop_front();
            self.dropped_count.fetch_add(1, Ordering::Relaxed);
        }
        buf.push_back(event);
        self.delivered_count.fetch_add(1, Ordering::Relaxed);
        true
    }

    pub fn drain(&self, max: usize) -> Vec<NotificationEvent> {
        let mut buf = self.buffer.write().unwrap();
        let count = max.min(buf.len());
        buf.drain(..count).collect()
    }

    pub fn pending_count(&self) -> usize {
        self.buffer.read().unwrap().len()
    }
    pub fn deactivate(&self) {
        self.active.store(false, Ordering::Relaxed);
    }
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Notification Hub — central dispatcher
// ═══════════════════════════════════════════════════════════════════════════════

pub struct NotificationHub {
    pub subscriptions: RwLock<HashMap<SubscriberId, Arc<Subscription>>>,
    pub next_event_id: AtomicU64,
    pub stats: NotificationHubStats,
    pub time_skip_enabled: AtomicBool,
    pub time_skip_offset_ms: AtomicU64,
}

#[derive(Debug, Default)]
pub struct NotificationHubStats {
    pub events_published: AtomicU64,
    pub events_delivered: AtomicU64,
    pub events_dropped: AtomicU64,
    pub subscribers_added: AtomicU64,
    pub subscribers_removed: AtomicU64,
    pub time_skip_notifications: AtomicU64,
}

impl NotificationHub {
    pub fn new() -> Self {
        Self {
            subscriptions: RwLock::new(HashMap::new()),
            next_event_id: AtomicU64::new(1),
            stats: NotificationHubStats::default(),
            time_skip_enabled: AtomicBool::new(false),
            time_skip_offset_ms: AtomicU64::new(0),
        }
    }

    pub fn subscribe(
        &self,
        subscriber: SubscriberId,
        filter: NotificationFilter,
        max_buffer: usize,
    ) -> Arc<Subscription> {
        let sub = Arc::new(Subscription::new(subscriber, filter, max_buffer));
        self.subscriptions
            .write()
            .unwrap()
            .insert(subscriber, sub.clone());
        self.stats.subscribers_added.fetch_add(1, Ordering::Relaxed);
        sub
    }

    pub fn unsubscribe(&self, subscriber: &SubscriberId) {
        if let Some(sub) = self.subscriptions.write().unwrap().remove(subscriber) {
            sub.deactivate();
            self.stats
                .subscribers_removed
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn publish(
        &self,
        notification_type: NotificationType,
        namespace_id: &str,
        workflow_id: &str,
        run_id: &str,
        shard_id: u32,
    ) -> u64 {
        let event_id = self.next_event_id.fetch_add(1, Ordering::Relaxed);
        let event = NotificationEvent {
            event_id,
            notification_type,
            namespace_id: namespace_id.to_string(),
            workflow_id: workflow_id.to_string(),
            run_id: run_id.to_string(),
            shard_id,
            version: 0,
            payload: HashMap::new(),
            created_at: now_millis(),
            priority: NotificationPriority::Normal,
        };
        self.fan_out(event);
        event_id
    }

    pub fn publish_with_payload(
        &self,
        notification_type: NotificationType,
        namespace_id: &str,
        workflow_id: &str,
        run_id: &str,
        shard_id: u32,
        payload: HashMap<String, Vec<u8>>,
        priority: NotificationPriority,
    ) -> u64 {
        let event_id = self.next_event_id.fetch_add(1, Ordering::Relaxed);
        let event = NotificationEvent {
            event_id,
            notification_type,
            namespace_id: namespace_id.to_string(),
            workflow_id: workflow_id.to_string(),
            run_id: run_id.to_string(),
            shard_id,
            version: 0,
            payload,
            created_at: now_millis(),
            priority,
        };
        self.fan_out(event);
        event_id
    }

    fn fan_out(&self, event: NotificationEvent) {
        let subs = self.subscriptions.read().unwrap();
        for (_, sub) in subs.iter() {
            if sub.deliver(event.clone()) {
                self.stats.events_delivered.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.stats.events_published.fetch_add(1, Ordering::Relaxed);
    }

    pub fn subscriber_count(&self) -> usize {
        self.subscriptions.read().unwrap().len()
    }

    pub fn get_subscription(&self, subscriber: &SubscriberId) -> Option<Arc<Subscription>> {
        self.subscriptions.read().unwrap().get(subscriber).cloned()
    }

    // Time skipping
    pub fn enable_time_skip(&self, offset_ms: u64) {
        self.time_skip_enabled.store(true, Ordering::Relaxed);
        self.time_skip_offset_ms.store(offset_ms, Ordering::Relaxed);
    }

    pub fn disable_time_skip(&self) {
        self.time_skip_enabled.store(false, Ordering::Relaxed);
        self.time_skip_offset_ms.store(0, Ordering::Relaxed);
    }

    pub fn is_time_skip_enabled(&self) -> bool {
        self.time_skip_enabled.load(Ordering::Relaxed)
    }

    pub fn adjusted_time(&self) -> i64 {
        let now = now_millis();
        if self.time_skip_enabled.load(Ordering::Relaxed) {
            now + self.time_skip_offset_ms.load(Ordering::Relaxed) as i64
        } else {
            now
        }
    }

    pub fn publish_time_skip_notification(&self, namespace_id: &str, shard_id: u32) {
        self.publish(NotificationType::TimerFired, namespace_id, "", "", shard_id);
        self.stats
            .time_skip_notifications
            .fetch_add(1, Ordering::Relaxed);
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_notification_type_category() {
        assert_eq!(
            NotificationType::WorkflowStarted.category(),
            NotificationCategory::Workflow
        );
        assert_eq!(
            NotificationType::ActivityStarted.category(),
            NotificationCategory::Activity
        );
        assert_eq!(
            NotificationType::TimerFired.category(),
            NotificationCategory::Timer
        );
        assert_eq!(
            NotificationType::SignalReceived.category(),
            NotificationCategory::Signal
        );
    }

    #[test]
    fn test_subscription_delivery() {
        let sub = Subscription::new(SubscriberId::Frontend, NotificationFilter::All, 100);
        let event = NotificationEvent {
            event_id: 1,
            notification_type: NotificationType::WorkflowStarted,
            namespace_id: "ns".into(),
            workflow_id: "wf".into(),
            run_id: "r".into(),
            shard_id: 0,
            version: 0,
            payload: HashMap::new(),
            created_at: 0,
            priority: NotificationPriority::Normal,
        };
        assert!(sub.deliver(event));
        assert_eq!(sub.pending_count(), 1);
        assert_eq!(sub.delivered_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_subscription_filter_categories() {
        let sub = Subscription::new(
            SubscriberId::Frontend,
            NotificationFilter::Categories(vec![NotificationCategory::Workflow]),
            100,
        );
        let wf_event = NotificationEvent {
            event_id: 1,
            notification_type: NotificationType::WorkflowStarted,
            namespace_id: "ns".into(),
            workflow_id: "wf".into(),
            run_id: "r".into(),
            shard_id: 0,
            version: 0,
            payload: HashMap::new(),
            created_at: 0,
            priority: NotificationPriority::Normal,
        };
        let act_event = NotificationEvent {
            event_id: 2,
            notification_type: NotificationType::ActivityStarted,
            namespace_id: "ns".into(),
            workflow_id: "wf".into(),
            run_id: "r".into(),
            shard_id: 0,
            version: 0,
            payload: HashMap::new(),
            created_at: 0,
            priority: NotificationPriority::Normal,
        };
        assert!(sub.deliver(wf_event));
        assert!(!sub.deliver(act_event));
        assert_eq!(sub.pending_count(), 1);
    }

    #[test]
    fn test_subscription_filter_namespace() {
        let sub = Subscription::new(
            SubscriberId::Frontend,
            NotificationFilter::Namespace("ns-a".into()),
            100,
        );
        let e1 = NotificationEvent {
            event_id: 1,
            notification_type: NotificationType::WorkflowStarted,
            namespace_id: "ns-a".into(),
            workflow_id: "wf".into(),
            run_id: "r".into(),
            shard_id: 0,
            version: 0,
            payload: HashMap::new(),
            created_at: 0,
            priority: NotificationPriority::Normal,
        };
        let e2 = NotificationEvent {
            event_id: 2,
            notification_type: NotificationType::WorkflowStarted,
            namespace_id: "ns-b".into(),
            workflow_id: "wf".into(),
            run_id: "r".into(),
            shard_id: 0,
            version: 0,
            payload: HashMap::new(),
            created_at: 0,
            priority: NotificationPriority::Normal,
        };
        assert!(sub.deliver(e1));
        assert!(!sub.deliver(e2));
    }

    #[test]
    fn test_subscription_buffer_overflow() {
        let sub = Subscription::new(SubscriberId::Frontend, NotificationFilter::All, 2);
        for i in 0..5 {
            let event = NotificationEvent {
                event_id: i,
                notification_type: NotificationType::WorkflowStarted,
                namespace_id: "ns".into(),
                workflow_id: "wf".into(),
                run_id: "r".into(),
                shard_id: 0,
                version: 0,
                payload: HashMap::new(),
                created_at: 0,
                priority: NotificationPriority::Normal,
            };
            sub.deliver(event);
        }
        assert_eq!(sub.pending_count(), 2);
        assert_eq!(sub.dropped_count.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn test_subscription_drain() {
        let sub = Subscription::new(SubscriberId::Frontend, NotificationFilter::All, 100);
        for i in 0..5 {
            sub.deliver(NotificationEvent {
                event_id: i,
                notification_type: NotificationType::WorkflowStarted,
                namespace_id: "ns".into(),
                workflow_id: "wf".into(),
                run_id: "r".into(),
                shard_id: 0,
                version: 0,
                payload: HashMap::new(),
                created_at: 0,
                priority: NotificationPriority::Normal,
            });
        }
        let drained = sub.drain(3);
        assert_eq!(drained.len(), 3);
        assert_eq!(sub.pending_count(), 2);
    }

    #[test]
    fn test_subscription_deactivate() {
        let sub = Subscription::new(SubscriberId::Frontend, NotificationFilter::All, 100);
        assert!(sub.is_active());
        sub.deactivate();
        assert!(!sub.is_active());
        let event = NotificationEvent {
            event_id: 1,
            notification_type: NotificationType::WorkflowStarted,
            namespace_id: "ns".into(),
            workflow_id: "wf".into(),
            run_id: "r".into(),
            shard_id: 0,
            version: 0,
            payload: HashMap::new(),
            created_at: 0,
            priority: NotificationPriority::Normal,
        };
        assert!(!sub.deliver(event));
    }

    #[test]
    fn test_notification_hub_subscribe_publish() {
        let hub = NotificationHub::new();
        let sub = hub.subscribe(SubscriberId::Frontend, NotificationFilter::All, 100);
        hub.publish(NotificationType::WorkflowStarted, "ns", "wf", "r", 0);
        assert_eq!(sub.pending_count(), 1);
        assert_eq!(hub.stats.events_published.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_notification_hub_multiple_subscribers() {
        let hub = NotificationHub::new();
        let sub1 = hub.subscribe(SubscriberId::Frontend, NotificationFilter::All, 100);
        let sub2 = hub.subscribe(
            SubscriberId::Matching,
            NotificationFilter::Categories(vec![NotificationCategory::Workflow]),
            100,
        );
        let sub3 = hub.subscribe(
            SubscriberId::Worker,
            NotificationFilter::Categories(vec![NotificationCategory::Activity]),
            100,
        );
        hub.publish(NotificationType::WorkflowStarted, "ns", "wf", "r", 0);
        assert_eq!(sub1.pending_count(), 1);
        assert_eq!(sub2.pending_count(), 1);
        assert_eq!(sub3.pending_count(), 0);
    }

    #[test]
    fn test_notification_hub_unsubscribe() {
        let hub = NotificationHub::new();
        hub.subscribe(SubscriberId::Frontend, NotificationFilter::All, 100);
        assert_eq!(hub.subscriber_count(), 1);
        hub.unsubscribe(&SubscriberId::Frontend);
        assert_eq!(hub.subscriber_count(), 0);
    }

    #[test]
    fn test_notification_hub_with_payload() {
        let hub = NotificationHub::new();
        let sub = hub.subscribe(SubscriberId::Admin, NotificationFilter::All, 100);
        let mut payload = HashMap::new();
        payload.insert("key".into(), vec![1, 2, 3]);
        hub.publish_with_payload(
            NotificationType::ShardOwnershipChanged,
            "ns",
            "",
            "",
            5,
            payload,
            NotificationPriority::Critical,
        );
        let events = sub.drain(1);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].priority, NotificationPriority::Critical);
        assert_eq!(events[0].shard_id, 5);
    }

    #[test]
    fn test_time_skip() {
        let hub = NotificationHub::new();
        assert!(!hub.is_time_skip_enabled());
        hub.enable_time_skip(60_000);
        assert!(hub.is_time_skip_enabled());
        let adjusted = hub.adjusted_time();
        let now = now_millis();
        assert!(adjusted >= now + 59_000);
        hub.disable_time_skip();
        assert!(!hub.is_time_skip_enabled());
    }

    #[test]
    fn test_composite_filter() {
        let sub = Subscription::new(
            SubscriberId::Frontend,
            NotificationFilter::Composite {
                categories: vec![NotificationCategory::Workflow, NotificationCategory::Signal],
                namespace: Some("ns-a".into()),
            },
            100,
        );
        let e1 = NotificationEvent {
            event_id: 1,
            notification_type: NotificationType::WorkflowStarted,
            namespace_id: "ns-a".into(),
            workflow_id: "wf".into(),
            run_id: "r".into(),
            shard_id: 0,
            version: 0,
            payload: HashMap::new(),
            created_at: 0,
            priority: NotificationPriority::Normal,
        };
        let e2 = NotificationEvent {
            event_id: 2,
            notification_type: NotificationType::WorkflowStarted,
            namespace_id: "ns-b".into(),
            workflow_id: "wf".into(),
            run_id: "r".into(),
            shard_id: 0,
            version: 0,
            payload: HashMap::new(),
            created_at: 0,
            priority: NotificationPriority::Normal,
        };
        assert!(sub.deliver(e1));
        assert!(!sub.deliver(e2));
    }

    #[test]
    fn test_shard_filter() {
        let sub = Subscription::new(SubscriberId::Frontend, NotificationFilter::Shard(5), 100);
        let e1 = NotificationEvent {
            event_id: 1,
            notification_type: NotificationType::ShardOwnershipChanged,
            namespace_id: "ns".into(),
            workflow_id: "".into(),
            run_id: "".into(),
            shard_id: 5,
            version: 0,
            payload: HashMap::new(),
            created_at: 0,
            priority: NotificationPriority::Normal,
        };
        let e2 = NotificationEvent {
            event_id: 2,
            notification_type: NotificationType::ShardOwnershipChanged,
            namespace_id: "ns".into(),
            workflow_id: "".into(),
            run_id: "".into(),
            shard_id: 10,
            version: 0,
            payload: HashMap::new(),
            created_at: 0,
            priority: NotificationPriority::Normal,
        };
        assert!(sub.deliver(e1));
        assert!(!sub.deliver(e2));
    }
}
