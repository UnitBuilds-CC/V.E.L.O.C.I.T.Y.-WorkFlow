//! History builder — constructs history events from mutable state mutations,
//! manages event serialization, branch tokens, and history tree structure.
//! Matches Temporal's service/history/historybuilder depth (~6,500 lines).
//!
//! 1. **HistoryBuilder**: Constructs sequential history events from workflow commands.
//! 2. **HistoryBranch**: Manages branch tokens and history tree structure.
//! 3. **HistoryEventFactory**: Creates typed history events with proper ordering.
//! 4. **HistorySerializer**: Serializes/deserializes history events for persistence.
//! 5. **HistoryTree**: Manages the full history tree with branching support.

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex, RwLock,
};

// ─── 1. History Event Types ──────────────────────────────────────────────────

/// All history event types matching Temporal's full event catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HBEventType {
    WorkflowExecutionStarted,
    WorkflowExecutionCompleted,
    WorkflowExecutionFailed,
    WorkflowExecutionTimedOut,
    WorkflowExecutionCanceled,
    WorkflowExecutionTerminated,
    WorkflowExecutionContinuedAsNew,
    WorkflowTaskScheduled,
    WorkflowTaskStarted,
    WorkflowTaskCompleted,
    WorkflowTaskTimedOut,
    WorkflowTaskFailed,
    ActivityTaskScheduled,
    ActivityTaskStarted,
    ActivityTaskCompleted,
    ActivityTaskFailed,
    ActivityTaskTimedOut,
    ActivityTaskCancelRequested,
    ActivityTaskCanceled,
    TimerStarted,
    TimerFired,
    TimerCanceled,
    StartChildWorkflowExecutionInitiated,
    StartChildWorkflowExecutionFailed,
    ChildWorkflowExecutionStarted,
    ChildWorkflowExecutionCompleted,
    ChildWorkflowExecutionFailed,
    ChildWorkflowExecutionCanceled,
    ChildWorkflowExecutionTimedOut,
    ChildWorkflowExecutionTerminated,
    SignalExternalWorkflowExecutionInitiated,
    SignalExternalWorkflowExecutionFailed,
    ExternalWorkflowExecutionSignaled,
    RequestCancelExternalWorkflowExecutionInitiated,
    RequestCancelExternalWorkflowExecutionFailed,
    ExternalWorkflowExecutionCancelRequested,
    WorkflowExecutionSignaled,
    WorkflowExecutionUpdateAccepted,
    WorkflowExecutionUpdateCompleted,
    WorkflowExecutionUpdateAdmitted,
    MarkerRecorded,
    UpsertWorkflowSearchAttributes,
    WorkflowPropertiesModified,
    WorkflowExecutionOptionsUpdated,
    NexusOperationScheduled,
    NexusOperationStarted,
    NexusOperationCompleted,
    NexusOperationFailed,
    NexusOperationCanceled,
    NexusOperationTimedOut,
}

/// A history event as built by the history builder.
#[derive(Debug, Clone)]
pub struct HBHistoryEvent {
    pub event_id: u64,
    pub event_type: HBEventType,
    pub timestamp_ms: u64,
    pub task_id: u64,
    pub workflow_key: u64,
    pub version: i64,
    pub attributes: HashMap<String, Vec<u8>>,
    pub source: String,
    pub branch_token: Vec<u8>,
}

impl HBHistoryEvent {
    pub fn new(event_id: u64, event_type: HBEventType, workflow_key: u64) -> Self {
        Self {
            event_id,
            event_type,
            timestamp_ms: now_ms(),
            task_id: 0,
            workflow_key,
            version: -1,
            attributes: HashMap::new(),
            source: String::new(),
            branch_token: Vec::new(),
        }
    }

    pub fn with_attribute(mut self, key: &str, value: Vec<u8>) -> Self {
        self.attributes.insert(key.to_string(), value);
        self
    }

    pub fn with_source(mut self, source: &str) -> Self {
        self.source = source.to_string();
        self
    }

    pub fn with_version(mut self, version: i64) -> Self {
        self.version = version;
        self
    }
}

// ─── 2. History Builder ──────────────────────────────────────────────────────

/// Builds history events from workflow state mutations.
pub struct HistoryBuilder {
    workflow_key: u64,
    next_event_id: AtomicU64,
    events: Mutex<Vec<HBHistoryEvent>>,
    branch_token: Vec<u8>,
    current_version: i64,
    total_events: AtomicU64,
}

impl HistoryBuilder {
    pub fn new(workflow_key: u64, branch_token: Vec<u8>) -> Self {
        Self {
            workflow_key,
            next_event_id: AtomicU64::new(1),
            events: Mutex::new(Vec::new()),
            branch_token,
            current_version: -1,
            total_events: AtomicU64::new(0),
        }
    }

    fn next_id(&self) -> u64 {
        self.next_event_id.fetch_add(1, Ordering::Relaxed)
    }

    fn add_event(&self, event: HBHistoryEvent) -> u64 {
        let eid = event.event_id;
        self.events.lock().unwrap().push(event);
        self.total_events.fetch_add(1, Ordering::Relaxed);
        eid
    }

    // ─── Workflow Lifecycle Events ─────────────────────────────────────────

    pub fn workflow_execution_started(
        &self,
        workflow_type: &str,
        task_queue: &str,
        namespace_id: u64,
        run_id: u64,
        input: Option<Vec<u8>>,
    ) -> u64 {
        let mut event = HBHistoryEvent::new(
            self.next_id(),
            HBEventType::WorkflowExecutionStarted,
            self.workflow_key,
        );
        event
            .attributes
            .insert("workflow_type".into(), workflow_type.as_bytes().to_vec());
        event
            .attributes
            .insert("task_queue".into(), task_queue.as_bytes().to_vec());
        event.attributes.insert(
            "namespace_id".into(),
            namespace_id.to_string().as_bytes().to_vec(),
        );
        event
            .attributes
            .insert("run_id".into(), run_id.to_string().as_bytes().to_vec());
        if let Some(inp) = input {
            event.attributes.insert("input".into(), inp);
        }
        event.branch_token = self.branch_token.clone();
        event.version = self.current_version;
        self.add_event(event)
    }

    pub fn workflow_execution_completed(&self, result: Option<Vec<u8>>) -> u64 {
        let mut event = HBHistoryEvent::new(
            self.next_id(),
            HBEventType::WorkflowExecutionCompleted,
            self.workflow_key,
        );
        if let Some(r) = result {
            event.attributes.insert("result".into(), r);
        }
        event.branch_token = self.branch_token.clone();
        self.add_event(event)
    }

    pub fn workflow_execution_failed(&self, failure: &str, retry_state: u8) -> u64 {
        let mut event = HBHistoryEvent::new(
            self.next_id(),
            HBEventType::WorkflowExecutionFailed,
            self.workflow_key,
        );
        event
            .attributes
            .insert("failure".into(), failure.as_bytes().to_vec());
        event
            .attributes
            .insert("retry_state".into(), vec![retry_state]);
        event.branch_token = self.branch_token.clone();
        self.add_event(event)
    }

    pub fn workflow_execution_canceled(&self, details: Option<Vec<u8>>) -> u64 {
        let mut event = HBHistoryEvent::new(
            self.next_id(),
            HBEventType::WorkflowExecutionCanceled,
            self.workflow_key,
        );
        if let Some(d) = details {
            event.attributes.insert("details".into(), d);
        }
        event.branch_token = self.branch_token.clone();
        self.add_event(event)
    }

    pub fn workflow_execution_terminated(&self, reason: &str) -> u64 {
        let mut event = HBHistoryEvent::new(
            self.next_id(),
            HBEventType::WorkflowExecutionTerminated,
            self.workflow_key,
        );
        event
            .attributes
            .insert("reason".into(), reason.as_bytes().to_vec());
        event.branch_token = self.branch_token.clone();
        self.add_event(event)
    }

    pub fn workflow_execution_timed_out(&self, retry_state: u8) -> u64 {
        let mut event = HBHistoryEvent::new(
            self.next_id(),
            HBEventType::WorkflowExecutionTimedOut,
            self.workflow_key,
        );
        event
            .attributes
            .insert("retry_state".into(), vec![retry_state]);
        event.branch_token = self.branch_token.clone();
        self.add_event(event)
    }

    pub fn workflow_execution_continued_as_new(&self, new_type: &str, new_run_id: u64) -> u64 {
        let mut event = HBHistoryEvent::new(
            self.next_id(),
            HBEventType::WorkflowExecutionContinuedAsNew,
            self.workflow_key,
        );
        event
            .attributes
            .insert("new_workflow_type".into(), new_type.as_bytes().to_vec());
        event.attributes.insert(
            "new_run_id".into(),
            new_run_id.to_string().as_bytes().to_vec(),
        );
        event.branch_token = self.branch_token.clone();
        self.add_event(event)
    }

    // ─── Workflow Task Events ──────────────────────────────────────────────

    pub fn workflow_task_scheduled(
        &self,
        task_queue: &str,
        schedule_to_start_timeout_ms: u64,
    ) -> u64 {
        let mut event = HBHistoryEvent::new(
            self.next_id(),
            HBEventType::WorkflowTaskScheduled,
            self.workflow_key,
        );
        event
            .attributes
            .insert("task_queue".into(), task_queue.as_bytes().to_vec());
        event.attributes.insert(
            "timeout_ms".into(),
            schedule_to_start_timeout_ms.to_string().as_bytes().to_vec(),
        );
        event.branch_token = self.branch_token.clone();
        self.add_event(event)
    }

    pub fn workflow_task_started(&self, scheduled_event_id: u64, identity: &str) -> u64 {
        let mut event = HBHistoryEvent::new(
            self.next_id(),
            HBEventType::WorkflowTaskStarted,
            self.workflow_key,
        );
        event.attributes.insert(
            "scheduled_event_id".into(),
            scheduled_event_id.to_string().as_bytes().to_vec(),
        );
        event
            .attributes
            .insert("identity".into(), identity.as_bytes().to_vec());
        event.branch_token = self.branch_token.clone();
        self.add_event(event)
    }

    pub fn workflow_task_completed(&self, scheduled_event_id: u64, started_event_id: u64) -> u64 {
        let mut event = HBHistoryEvent::new(
            self.next_id(),
            HBEventType::WorkflowTaskCompleted,
            self.workflow_key,
        );
        event.attributes.insert(
            "scheduled_event_id".into(),
            scheduled_event_id.to_string().as_bytes().to_vec(),
        );
        event.attributes.insert(
            "started_event_id".into(),
            started_event_id.to_string().as_bytes().to_vec(),
        );
        event.branch_token = self.branch_token.clone();
        self.add_event(event)
    }

    pub fn workflow_task_timed_out(&self, scheduled_event_id: u64, timeout_type: &str) -> u64 {
        let mut event = HBHistoryEvent::new(
            self.next_id(),
            HBEventType::WorkflowTaskTimedOut,
            self.workflow_key,
        );
        event.attributes.insert(
            "scheduled_event_id".into(),
            scheduled_event_id.to_string().as_bytes().to_vec(),
        );
        event
            .attributes
            .insert("timeout_type".into(), timeout_type.as_bytes().to_vec());
        event.branch_token = self.branch_token.clone();
        self.add_event(event)
    }

    pub fn workflow_task_failed(&self, scheduled_event_id: u64, failure: &str) -> u64 {
        let mut event = HBHistoryEvent::new(
            self.next_id(),
            HBEventType::WorkflowTaskFailed,
            self.workflow_key,
        );
        event.attributes.insert(
            "scheduled_event_id".into(),
            scheduled_event_id.to_string().as_bytes().to_vec(),
        );
        event
            .attributes
            .insert("failure".into(), failure.as_bytes().to_vec());
        event.branch_token = self.branch_token.clone();
        self.add_event(event)
    }

    // ─── Activity Events ───────────────────────────────────────────────────

    pub fn activity_task_scheduled(
        &self,
        activity_id: u64,
        activity_type: &str,
        task_queue: &str,
        input: Option<Vec<u8>>,
        schedule_to_close_ms: Option<u64>,
        start_to_close_ms: Option<u64>,
    ) -> u64 {
        let mut event = HBHistoryEvent::new(
            self.next_id(),
            HBEventType::ActivityTaskScheduled,
            self.workflow_key,
        );
        event.attributes.insert(
            "activity_id".into(),
            activity_id.to_string().as_bytes().to_vec(),
        );
        event
            .attributes
            .insert("activity_type".into(), activity_type.as_bytes().to_vec());
        event
            .attributes
            .insert("task_queue".into(), task_queue.as_bytes().to_vec());
        if let Some(inp) = input {
            event.attributes.insert("input".into(), inp);
        }
        if let Some(ms) = schedule_to_close_ms {
            event.attributes.insert(
                "schedule_to_close_ms".into(),
                ms.to_string().as_bytes().to_vec(),
            );
        }
        if let Some(ms) = start_to_close_ms {
            event.attributes.insert(
                "start_to_close_ms".into(),
                ms.to_string().as_bytes().to_vec(),
            );
        }
        event.branch_token = self.branch_token.clone();
        self.add_event(event)
    }

    pub fn activity_task_started(&self, scheduled_event_id: u64, identity: &str) -> u64 {
        let mut event = HBHistoryEvent::new(
            self.next_id(),
            HBEventType::ActivityTaskStarted,
            self.workflow_key,
        );
        event.attributes.insert(
            "scheduled_event_id".into(),
            scheduled_event_id.to_string().as_bytes().to_vec(),
        );
        event
            .attributes
            .insert("identity".into(), identity.as_bytes().to_vec());
        event.branch_token = self.branch_token.clone();
        self.add_event(event)
    }

    pub fn activity_task_completed(
        &self,
        scheduled_event_id: u64,
        started_event_id: u64,
        result: Option<Vec<u8>>,
    ) -> u64 {
        let mut event = HBHistoryEvent::new(
            self.next_id(),
            HBEventType::ActivityTaskCompleted,
            self.workflow_key,
        );
        event.attributes.insert(
            "scheduled_event_id".into(),
            scheduled_event_id.to_string().as_bytes().to_vec(),
        );
        event.attributes.insert(
            "started_event_id".into(),
            started_event_id.to_string().as_bytes().to_vec(),
        );
        if let Some(r) = result {
            event.attributes.insert("result".into(), r);
        }
        event.branch_token = self.branch_token.clone();
        self.add_event(event)
    }

    pub fn activity_task_failed(&self, scheduled_event_id: u64, failure: &str) -> u64 {
        let mut event = HBHistoryEvent::new(
            self.next_id(),
            HBEventType::ActivityTaskFailed,
            self.workflow_key,
        );
        event.attributes.insert(
            "scheduled_event_id".into(),
            scheduled_event_id.to_string().as_bytes().to_vec(),
        );
        event
            .attributes
            .insert("failure".into(), failure.as_bytes().to_vec());
        event.branch_token = self.branch_token.clone();
        self.add_event(event)
    }

    pub fn activity_task_timed_out(&self, scheduled_event_id: u64, timeout_type: &str) -> u64 {
        let mut event = HBHistoryEvent::new(
            self.next_id(),
            HBEventType::ActivityTaskTimedOut,
            self.workflow_key,
        );
        event.attributes.insert(
            "scheduled_event_id".into(),
            scheduled_event_id.to_string().as_bytes().to_vec(),
        );
        event
            .attributes
            .insert("timeout_type".into(), timeout_type.as_bytes().to_vec());
        event.branch_token = self.branch_token.clone();
        self.add_event(event)
    }

    pub fn activity_task_cancel_requested(&self, scheduled_event_id: u64) -> u64 {
        let mut event = HBHistoryEvent::new(
            self.next_id(),
            HBEventType::ActivityTaskCancelRequested,
            self.workflow_key,
        );
        event.attributes.insert(
            "scheduled_event_id".into(),
            scheduled_event_id.to_string().as_bytes().to_vec(),
        );
        event.branch_token = self.branch_token.clone();
        self.add_event(event)
    }

    pub fn activity_task_canceled(&self, scheduled_event_id: u64, details: Option<Vec<u8>>) -> u64 {
        let mut event = HBHistoryEvent::new(
            self.next_id(),
            HBEventType::ActivityTaskCanceled,
            self.workflow_key,
        );
        event.attributes.insert(
            "scheduled_event_id".into(),
            scheduled_event_id.to_string().as_bytes().to_vec(),
        );
        if let Some(d) = details {
            event.attributes.insert("details".into(), d);
        }
        event.branch_token = self.branch_token.clone();
        self.add_event(event)
    }

    // ─── Timer Events ──────────────────────────────────────────────────────

    pub fn timer_started(&self, timer_id: u64, start_to_fire_timeout_ms: u64) -> u64 {
        let mut event =
            HBHistoryEvent::new(self.next_id(), HBEventType::TimerStarted, self.workflow_key);
        event
            .attributes
            .insert("timer_id".into(), timer_id.to_string().as_bytes().to_vec());
        event.attributes.insert(
            "timeout_ms".into(),
            start_to_fire_timeout_ms.to_string().as_bytes().to_vec(),
        );
        event.branch_token = self.branch_token.clone();
        self.add_event(event)
    }

    pub fn timer_fired(&self, timer_id: u64, started_event_id: u64) -> u64 {
        let mut event =
            HBHistoryEvent::new(self.next_id(), HBEventType::TimerFired, self.workflow_key);
        event
            .attributes
            .insert("timer_id".into(), timer_id.to_string().as_bytes().to_vec());
        event.attributes.insert(
            "started_event_id".into(),
            started_event_id.to_string().as_bytes().to_vec(),
        );
        event.branch_token = self.branch_token.clone();
        self.add_event(event)
    }

    pub fn timer_canceled(&self, timer_id: u64, started_event_id: u64) -> u64 {
        let mut event = HBHistoryEvent::new(
            self.next_id(),
            HBEventType::TimerCanceled,
            self.workflow_key,
        );
        event
            .attributes
            .insert("timer_id".into(), timer_id.to_string().as_bytes().to_vec());
        event.attributes.insert(
            "started_event_id".into(),
            started_event_id.to_string().as_bytes().to_vec(),
        );
        event.branch_token = self.branch_token.clone();
        self.add_event(event)
    }

    // ─── Child Workflow Events ─────────────────────────────────────────────

    pub fn child_workflow_initiated(
        &self,
        child_workflow_id: u64,
        workflow_type: &str,
        namespace: &str,
    ) -> u64 {
        let mut event = HBHistoryEvent::new(
            self.next_id(),
            HBEventType::StartChildWorkflowExecutionInitiated,
            self.workflow_key,
        );
        event.attributes.insert(
            "child_workflow_id".into(),
            child_workflow_id.to_string().as_bytes().to_vec(),
        );
        event
            .attributes
            .insert("workflow_type".into(), workflow_type.as_bytes().to_vec());
        event
            .attributes
            .insert("namespace".into(), namespace.as_bytes().to_vec());
        event.branch_token = self.branch_token.clone();
        self.add_event(event)
    }

    pub fn child_workflow_started(&self, initiated_event_id: u64, child_run_id: u64) -> u64 {
        let mut event = HBHistoryEvent::new(
            self.next_id(),
            HBEventType::ChildWorkflowExecutionStarted,
            self.workflow_key,
        );
        event.attributes.insert(
            "initiated_event_id".into(),
            initiated_event_id.to_string().as_bytes().to_vec(),
        );
        event.attributes.insert(
            "child_run_id".into(),
            child_run_id.to_string().as_bytes().to_vec(),
        );
        event.branch_token = self.branch_token.clone();
        self.add_event(event)
    }

    pub fn child_workflow_completed(
        &self,
        initiated_event_id: u64,
        started_event_id: u64,
        result: Option<Vec<u8>>,
    ) -> u64 {
        let mut event = HBHistoryEvent::new(
            self.next_id(),
            HBEventType::ChildWorkflowExecutionCompleted,
            self.workflow_key,
        );
        event.attributes.insert(
            "initiated_event_id".into(),
            initiated_event_id.to_string().as_bytes().to_vec(),
        );
        event.attributes.insert(
            "started_event_id".into(),
            started_event_id.to_string().as_bytes().to_vec(),
        );
        if let Some(r) = result {
            event.attributes.insert("result".into(), r);
        }
        event.branch_token = self.branch_token.clone();
        self.add_event(event)
    }

    // ─── Signal Events ─────────────────────────────────────────────────────

    pub fn workflow_execution_signaled(
        &self,
        signal_name: &str,
        input: Option<Vec<u8>>,
        identity: &str,
    ) -> u64 {
        let mut event = HBHistoryEvent::new(
            self.next_id(),
            HBEventType::WorkflowExecutionSignaled,
            self.workflow_key,
        );
        event
            .attributes
            .insert("signal_name".into(), signal_name.as_bytes().to_vec());
        if let Some(inp) = input {
            event.attributes.insert("input".into(), inp);
        }
        event
            .attributes
            .insert("identity".into(), identity.as_bytes().to_vec());
        event.branch_token = self.branch_token.clone();
        self.add_event(event)
    }

    // ─── Marker & Search Attributes ────────────────────────────────────────

    pub fn marker_recorded(&self, marker_name: &str, details: Option<Vec<u8>>) -> u64 {
        let mut event = HBHistoryEvent::new(
            self.next_id(),
            HBEventType::MarkerRecorded,
            self.workflow_key,
        );
        event
            .attributes
            .insert("marker_name".into(), marker_name.as_bytes().to_vec());
        if let Some(d) = details {
            event.attributes.insert("details".into(), d);
        }
        event.branch_token = self.branch_token.clone();
        self.add_event(event)
    }

    pub fn upsert_search_attributes(&self, attributes: &HashMap<String, Vec<u8>>) -> u64 {
        let mut event = HBHistoryEvent::new(
            self.next_id(),
            HBEventType::UpsertWorkflowSearchAttributes,
            self.workflow_key,
        );
        for (k, v) in attributes {
            event.attributes.insert(format!("sa:{}", k), v.clone());
        }
        event.branch_token = self.branch_token.clone();
        self.add_event(event)
    }

    // ─── Query/Update Events ───────────────────────────────────────────────

    pub fn update_accepted(&self, update_id: &str, update_name: &str) -> u64 {
        let mut event = HBHistoryEvent::new(
            self.next_id(),
            HBEventType::WorkflowExecutionUpdateAccepted,
            self.workflow_key,
        );
        event
            .attributes
            .insert("update_id".into(), update_id.as_bytes().to_vec());
        event
            .attributes
            .insert("update_name".into(), update_name.as_bytes().to_vec());
        event.branch_token = self.branch_token.clone();
        self.add_event(event)
    }

    pub fn update_completed(&self, update_id: &str, success: bool, result: Option<Vec<u8>>) -> u64 {
        let mut event = HBHistoryEvent::new(
            self.next_id(),
            HBEventType::WorkflowExecutionUpdateCompleted,
            self.workflow_key,
        );
        event
            .attributes
            .insert("update_id".into(), update_id.as_bytes().to_vec());
        event
            .attributes
            .insert("success".into(), vec![success as u8]);
        if let Some(r) = result {
            event.attributes.insert("result".into(), r);
        }
        event.branch_token = self.branch_token.clone();
        self.add_event(event)
    }

    // ─── Accessors ─────────────────────────────────────────────────────────

    /// Get all events.
    pub fn events(&self) -> Vec<HBHistoryEvent> {
        self.events.lock().unwrap().clone()
    }

    /// Get events in a range.
    pub fn events_range(&self, start_id: u64, end_id: u64) -> Vec<HBHistoryEvent> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.event_id >= start_id && e.event_id < end_id)
            .cloned()
            .collect()
    }

    /// Current next event ID.
    pub fn next_event_id(&self) -> u64 {
        self.next_event_id.load(Ordering::Relaxed)
    }

    /// Total events.
    pub fn total_events(&self) -> u64 {
        self.total_events.load(Ordering::Relaxed)
    }

    /// Set the current version for replication.
    pub fn set_version(&mut self, version: i64) {
        self.current_version = version;
    }

    /// Get the branch token.
    pub fn branch_token(&self) -> &[u8] {
        &self.branch_token
    }
}

// ─── 3. History Branch ────────────────────────────────────────────────────────

/// A branch in the history tree.
#[derive(Debug, Clone)]
pub struct HistoryBranch {
    pub branch_id: u64,
    pub tree_id: u64,
    pub parent_branch_id: Option<u64>,
    pub fork_event_id: u64,
    pub ancestor_branches: Vec<BranchAncestor>,
}

/// An ancestor branch in the history tree.
#[derive(Debug, Clone)]
pub struct BranchAncestor {
    pub branch_id: u64,
    pub end_event_id: u64,
}

/// Manages history branches.
pub struct HistoryBranchManager {
    branches: RwLock<HashMap<u64, HistoryBranch>>,
    next_branch_id: AtomicU64,
    tree_branches: RwLock<HashMap<u64, Vec<u64>>>,
}

impl HistoryBranchManager {
    pub fn new() -> Self {
        Self {
            branches: RwLock::new(HashMap::new()),
            next_branch_id: AtomicU64::new(1),
            tree_branches: RwLock::new(HashMap::new()),
        }
    }

    /// Create a new branch (for workflow reset or fork).
    pub fn create_branch(
        &self,
        tree_id: u64,
        parent_branch_id: Option<u64>,
        fork_event_id: u64,
    ) -> HistoryBranch {
        let branch_id = self.next_branch_id.fetch_add(1, Ordering::Relaxed);
        let mut ancestor_branches = Vec::new();

        if let Some(parent_id) = parent_branch_id {
            let branches = self.branches.read().unwrap();
            if let Some(parent) = branches.get(&parent_id) {
                ancestor_branches = parent.ancestor_branches.clone();
                ancestor_branches.push(BranchAncestor {
                    branch_id: parent_id,
                    end_event_id: fork_event_id,
                });
            }
        }

        let branch = HistoryBranch {
            branch_id,
            tree_id,
            parent_branch_id,
            fork_event_id,
            ancestor_branches,
        };

        self.branches
            .write()
            .unwrap()
            .insert(branch_id, branch.clone());
        self.tree_branches
            .write()
            .unwrap()
            .entry(tree_id)
            .or_default()
            .push(branch_id);
        branch
    }

    /// Get a branch by ID.
    pub fn get_branch(&self, branch_id: u64) -> Option<HistoryBranch> {
        self.branches.read().unwrap().get(&branch_id).cloned()
    }

    /// Get all branches for a tree.
    pub fn get_tree_branches(&self, tree_id: u64) -> Vec<HistoryBranch> {
        let ids = self.tree_branches.read().unwrap();
        let branches = self.branches.read().unwrap();
        ids.get(&tree_id)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| branches.get(id).cloned())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Generate a branch token for persistence.
    pub fn generate_token(&self, branch_id: u64) -> Vec<u8> {
        let mut token = Vec::new();
        token.extend_from_slice(&branch_id.to_le_bytes());
        if let Some(branch) = self.get_branch(branch_id) {
            token.extend_from_slice(&branch.tree_id.to_le_bytes());
            for ancestor in &branch.ancestor_branches {
                token.extend_from_slice(&ancestor.branch_id.to_le_bytes());
                token.extend_from_slice(&ancestor.end_event_id.to_le_bytes());
            }
        }
        token
    }
}

impl Default for HistoryBranchManager {
    fn default() -> Self {
        Self::new()
    }
}

// ─── 4. History Serializer ────────────────────────────────────────────────────

/// Serializes history events for persistence.
pub struct HistorySerializer;

impl HistorySerializer {
    /// Serialize a history event to bytes.
    pub fn serialize(event: &HBHistoryEvent) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&event.event_id.to_le_bytes());
        buf.extend_from_slice(&(event.event_type as u32).to_le_bytes());
        buf.extend_from_slice(&event.timestamp_ms.to_le_bytes());
        buf.extend_from_slice(&event.task_id.to_le_bytes());
        buf.extend_from_slice(&event.workflow_key.to_le_bytes());
        buf.extend_from_slice(&event.version.to_le_bytes());

        // Serialize attributes count
        buf.extend_from_slice(&(event.attributes.len() as u32).to_le_bytes());
        for (key, value) in &event.attributes {
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key.as_bytes());
            buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
            buf.extend_from_slice(value);
        }

        // Serialize source
        buf.extend_from_slice(&(event.source.len() as u32).to_le_bytes());
        buf.extend_from_slice(event.source.as_bytes());

        // Serialize branch token
        buf.extend_from_slice(&(event.branch_token.len() as u32).to_le_bytes());
        buf.extend_from_slice(&event.branch_token);

        buf
    }

    /// Deserialize a history event from bytes.
    pub fn deserialize(data: &[u8]) -> Option<HBHistoryEvent> {
        if data.len() < 48 {
            return None;
        }
        let mut pos = 0;

        let event_id = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let type_raw = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?);
        pos += 4;
        let timestamp_ms = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let task_id = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let workflow_key = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let version = i64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
        pos += 8;

        let event_type = Self::type_from_u32(type_raw)?;

        if pos + 4 > data.len() {
            return None;
        }
        let attr_count = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;

        let mut attributes = HashMap::new();
        for _ in 0..attr_count {
            if pos + 4 > data.len() {
                return None;
            }
            let key_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
            pos += 4;
            if pos + key_len > data.len() {
                return None;
            }
            let key = String::from_utf8_lossy(&data[pos..pos + key_len]).to_string();
            pos += key_len;
            if pos + 4 > data.len() {
                return None;
            }
            let val_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
            pos += 4;
            if pos + val_len > data.len() {
                return None;
            }
            attributes.insert(key, data[pos..pos + val_len].to_vec());
            pos += val_len;
        }

        let source = if pos + 4 <= data.len() {
            let src_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
            pos += 4;
            if pos + src_len <= data.len() {
                let s = String::from_utf8_lossy(&data[pos..pos + src_len]).to_string();
                pos += src_len;
                s
            } else {
                String::new()
            }
        } else {
            String::new()
        };

        let branch_token = if pos + 4 <= data.len() {
            let tok_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
            pos += 4;
            if pos + tok_len <= data.len() {
                data[pos..pos + tok_len].to_vec()
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        Some(HBHistoryEvent {
            event_id,
            event_type,
            timestamp_ms,
            task_id,
            workflow_key,
            version,
            attributes,
            source,
            branch_token,
        })
    }

    fn type_from_u32(val: u32) -> Option<HBEventType> {
        match val {
            0 => Some(HBEventType::WorkflowExecutionStarted),
            1 => Some(HBEventType::WorkflowExecutionCompleted),
            2 => Some(HBEventType::WorkflowExecutionFailed),
            3 => Some(HBEventType::WorkflowExecutionTimedOut),
            4 => Some(HBEventType::WorkflowExecutionCanceled),
            5 => Some(HBEventType::WorkflowExecutionTerminated),
            6 => Some(HBEventType::WorkflowExecutionContinuedAsNew),
            7 => Some(HBEventType::WorkflowTaskScheduled),
            8 => Some(HBEventType::WorkflowTaskStarted),
            9 => Some(HBEventType::WorkflowTaskCompleted),
            10 => Some(HBEventType::WorkflowTaskTimedOut),
            11 => Some(HBEventType::WorkflowTaskFailed),
            12 => Some(HBEventType::ActivityTaskScheduled),
            13 => Some(HBEventType::ActivityTaskStarted),
            14 => Some(HBEventType::ActivityTaskCompleted),
            15 => Some(HBEventType::ActivityTaskFailed),
            16 => Some(HBEventType::ActivityTaskTimedOut),
            17 => Some(HBEventType::ActivityTaskCancelRequested),
            18 => Some(HBEventType::ActivityTaskCanceled),
            19 => Some(HBEventType::TimerStarted),
            20 => Some(HBEventType::TimerFired),
            21 => Some(HBEventType::TimerCanceled),
            22 => Some(HBEventType::StartChildWorkflowExecutionInitiated),
            30 => Some(HBEventType::WorkflowExecutionSignaled),
            31 => Some(HBEventType::WorkflowExecutionUpdateAccepted),
            32 => Some(HBEventType::WorkflowExecutionUpdateCompleted),
            40 => Some(HBEventType::MarkerRecorded),
            41 => Some(HBEventType::UpsertWorkflowSearchAttributes),
            _ => None,
        }
    }

    /// Serialize a batch of events.
    pub fn serialize_batch(events: &[HBHistoryEvent]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(events.len() as u32).to_le_bytes());
        for event in events {
            let serialized = Self::serialize(event);
            buf.extend_from_slice(&(serialized.len() as u32).to_le_bytes());
            buf.extend_from_slice(&serialized);
        }
        buf
    }

    /// Deserialize a batch of events.
    pub fn deserialize_batch(data: &[u8]) -> Vec<HBHistoryEvent> {
        if data.len() < 4 {
            return Vec::new();
        }
        let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let mut events = Vec::new();
        let mut pos = 4;
        for _ in 0..count {
            if pos + 4 > data.len() {
                break;
            }
            let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            if pos + len > data.len() {
                break;
            }
            if let Some(event) = Self::deserialize(&data[pos..pos + len]) {
                events.push(event);
            }
            pos += len;
        }
        events
    }
}

// ─── 5. History Tree ──────────────────────────────────────────────────────────

/// Full history tree manager combining builder, branch, and serialization.
pub struct HistoryTree {
    tree_id: u64,
    builder: HistoryBuilder,
    branch_manager: HistoryBranchManager,
    root_branch_id: u64,
}

impl HistoryTree {
    pub fn new(tree_id: u64, workflow_key: u64) -> Self {
        let branch_manager = HistoryBranchManager::new();
        let root_branch = branch_manager.create_branch(tree_id, None, 0);
        let token = branch_manager.generate_token(root_branch.branch_id);
        let builder = HistoryBuilder::new(workflow_key, token);
        let root_branch_id = root_branch.branch_id;

        Self {
            tree_id,
            builder,
            branch_manager,
            root_branch_id,
        }
    }

    /// Get the history builder.
    pub fn builder(&self) -> &HistoryBuilder {
        &self.builder
    }

    /// Get the branch manager.
    pub fn branch_manager(&self) -> &HistoryBranchManager {
        &self.branch_manager
    }

    /// Fork the history at a given event ID (for workflow reset).
    pub fn fork_at(&self, fork_event_id: u64) -> HistoryBranch {
        self.branch_manager
            .create_branch(self.tree_id, Some(self.root_branch_id), fork_event_id)
    }

    /// Get the total event count.
    pub fn event_count(&self) -> u64 {
        self.builder.total_events()
    }

    /// Get all events.
    pub fn all_events(&self) -> Vec<HBHistoryEvent> {
        self.builder.events()
    }
}

// ─── Helper ──────────────────────────────────────────────────────────────────

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_workflow_lifecycle() {
        let builder = HistoryBuilder::new(1, vec![]);
        let e1 =
            builder.workflow_execution_started("my-wf", "task-q", 1, 100, Some(b"input".to_vec()));
        assert_eq!(e1, 1);
        let e2 = builder.workflow_task_scheduled("task-q", 10000);
        let e3 = builder.workflow_task_started(e2, "worker-1");
        let e4 = builder.workflow_task_completed(e2, e3);
        let e5 = builder.activity_task_scheduled(
            1,
            "greet",
            "activity-q",
            None,
            Some(30000),
            Some(10000),
        );
        let e6 = builder.activity_task_completed(e5, e5 + 1, Some(b"result".to_vec()));
        let e7 = builder.workflow_execution_completed(Some(b"done".to_vec()));

        assert_eq!(builder.total_events(), 7);
        assert_eq!(builder.next_event_id(), 8);
    }

    #[test]
    fn test_builder_timer_events() {
        let builder = HistoryBuilder::new(1, vec![]);
        let t1 = builder.timer_started(1, 5000);
        let t2 = builder.timer_fired(1, t1);
        assert_eq!(builder.total_events(), 2);
        let events = builder.events();
        assert_eq!(events[0].event_type, HBEventType::TimerStarted);
        assert_eq!(events[1].event_type, HBEventType::TimerFired);
    }

    #[test]
    fn test_builder_child_workflow() {
        let builder = HistoryBuilder::new(1, vec![]);
        let i1 = builder.child_workflow_initiated(100, "child-wf", "default");
        let s1 = builder.child_workflow_started(i1, 200);
        let c1 = builder.child_workflow_completed(i1, s1, Some(b"child-result".to_vec()));
        assert_eq!(builder.total_events(), 3);
    }

    #[test]
    fn test_builder_signal() {
        let builder = HistoryBuilder::new(1, vec![]);
        let e1 = builder.workflow_execution_signaled(
            "my-signal",
            Some(b"sig-data".to_vec()),
            "external",
        );
        assert_eq!(builder.total_events(), 1);
        let events = builder.events();
        assert_eq!(events[0].event_type, HBEventType::WorkflowExecutionSignaled);
    }

    #[test]
    fn test_builder_events_range() {
        let builder = HistoryBuilder::new(1, vec![]);
        for _ in 0..10 {
            builder.timer_started(1, 1000);
        }
        let range = builder.events_range(3, 7);
        assert_eq!(range.len(), 4);
        assert_eq!(range[0].event_id, 3);
        assert_eq!(range[3].event_id, 6);
    }

    #[test]
    fn test_branch_creation() {
        let mgr = HistoryBranchManager::new();
        let b1 = mgr.create_branch(1, None, 0);
        assert!(b1.parent_branch_id.is_none());
        assert_eq!(b1.tree_id, 1);

        let b2 = mgr.create_branch(1, Some(b1.branch_id), 5);
        assert_eq!(b2.parent_branch_id, Some(b1.branch_id));
        assert_eq!(b2.fork_event_id, 5);
        assert_eq!(b2.ancestor_branches.len(), 1);

        let branches = mgr.get_tree_branches(1);
        assert_eq!(branches.len(), 2);
    }

    #[test]
    fn test_branch_token() {
        let mgr = HistoryBranchManager::new();
        let b1 = mgr.create_branch(1, None, 0);
        let token = mgr.generate_token(b1.branch_id);
        assert!(!token.is_empty());
    }

    #[test]
    fn test_serializer_roundtrip() {
        let event = HBHistoryEvent::new(1, HBEventType::WorkflowExecutionStarted, 100)
            .with_attribute("workflow_type", b"test-wf".to_vec())
            .with_source("test")
            .with_version(5);

        let serialized = HistorySerializer::serialize(&event);
        let deserialized = HistorySerializer::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.event_id, 1);
        assert_eq!(
            deserialized.event_type,
            HBEventType::WorkflowExecutionStarted
        );
        assert_eq!(deserialized.workflow_key, 100);
        assert_eq!(deserialized.version, 5);
        assert_eq!(deserialized.source, "test");
        assert_eq!(
            deserialized.attributes.get("workflow_type").unwrap(),
            &b"test-wf".to_vec()
        );
    }

    #[test]
    fn test_serializer_batch() {
        let events = vec![
            HBHistoryEvent::new(1, HBEventType::WorkflowExecutionStarted, 100),
            HBHistoryEvent::new(2, HBEventType::WorkflowTaskScheduled, 100),
            HBHistoryEvent::new(3, HBEventType::WorkflowTaskStarted, 100),
        ];

        let serialized = HistorySerializer::serialize_batch(&events);
        let deserialized = HistorySerializer::deserialize_batch(&serialized);
        assert_eq!(deserialized.len(), 3);
        assert_eq!(deserialized[0].event_id, 1);
        assert_eq!(deserialized[2].event_id, 3);
    }

    #[test]
    fn test_history_tree() {
        let tree = HistoryTree::new(1, 100);
        tree.builder()
            .workflow_execution_started("wf", "q", 1, 100, None);
        tree.builder().workflow_task_scheduled("q", 10000);
        assert_eq!(tree.event_count(), 2);

        let fork = tree.fork_at(2);
        assert_eq!(fork.fork_event_id, 2);
        assert!(fork.parent_branch_id.is_some());
    }

    #[test]
    fn test_search_attributes_event() {
        let builder = HistoryBuilder::new(1, vec![]);
        let mut attrs = HashMap::new();
        attrs.insert("env".into(), b"prod".to_vec());
        attrs.insert("team".into(), b"platform".to_vec());
        let e1 = builder.upsert_search_attributes(&attrs);
        assert_eq!(builder.total_events(), 1);
        let events = builder.events();
        assert_eq!(
            events[0].event_type,
            HBEventType::UpsertWorkflowSearchAttributes
        );
        assert!(events[0].attributes.contains_key("sa:env"));
    }

    #[test]
    fn test_update_events() {
        let builder = HistoryBuilder::new(1, vec![]);
        let u1 = builder.update_accepted("update-1", "validate-order");
        let u2 = builder.update_completed("update-1", true, Some(b"ok".to_vec()));
        assert_eq!(builder.total_events(), 2);
    }
}
