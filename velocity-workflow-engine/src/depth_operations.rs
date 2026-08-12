//! Operational depth modules closing remaining parity gaps with Temporal.
//!
//! - **HistoryExtensions**: Reverse pagination, extended event types, history branching
//! - **EngineStatistics**: Workflow aggregation, status counts, latency histograms
//! - **SizeLimitEnforcer**: Integrated payload/history/execution size enforcement
//! - **PollContextManager**: Outstanding poll tracking and cancellation
//! - **NamespaceRetention**: History TTL, retention policies, cleanup scheduling
//! - **WorkflowTaskTracker**: Workflow task scheduling, attempt tracking, sticky reset

use std::collections::{HashMap, VecDeque};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex, RwLock,
};
use std::time::{Duration, Instant};

// ─── Extended History Event Types ──────────────────────────────────────────

/// Extended history event types matching Temporal's full event vocabulary.
/// The base `HistoryEventType` in event_history.rs covers core types.
/// This enum covers the remaining events Temporal records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum ExtendedEventType {
    // Base workflow events (1-7)
    WorkflowExecutionStarted = 1,
    WorkflowExecutionCompleted = 2,
    WorkflowExecutionFailed = 3,
    WorkflowExecutionCanceled = 4,
    WorkflowExecutionTerminated = 5,
    WorkflowExecutionTimedOut = 6,
    WorkflowExecutionContinuedAsNew = 7,

    // Workflow task events (10-14)
    WorkflowTaskScheduled = 10,
    WorkflowTaskStarted = 11,
    WorkflowTaskCompleted = 12,
    WorkflowTaskFailed = 13,
    WorkflowTaskTimedOut = 14,

    // Activity events (20-29)
    ActivityTaskScheduled = 20,
    ActivityTaskStarted = 21,
    ActivityTaskCompleted = 22,
    ActivityTaskFailed = 23,
    ActivityTaskTimedOut = 24,
    ActivityTaskCanceled = 25,
    ActivityTaskCancelRequested = 26,
    ActivityTaskPaused = 27,
    ActivityTaskUnpaused = 28,

    // Timer events (40-42)
    TimerStarted = 40,
    TimerFired = 41,
    TimerCanceled = 42,

    // Signal events (50-52)
    WorkflowExecutionSignaled = 50,
    SignalExternalWorkflowExecutionInitiated = 51,
    SignalExternalWorkflowExecutionFailed = 52,

    // Child workflow events (60-66)
    StartChildWorkflowExecutionInitiated = 60,
    StartChildWorkflowExecutionFailed = 61,
    ChildWorkflowExecutionStarted = 62,
    ChildWorkflowExecutionCompleted = 63,
    ChildWorkflowExecutionFailed = 64,
    ChildWorkflowExecutionCanceled = 65,
    ChildWorkflowExecutionTimedOut = 66,
    ChildWorkflowExecutionTerminated = 67,

    // Marker events (70-71)
    MarkerRecorded = 70,
    SideEffectMarker = 71,

    // Update events (80-84)
    WorkflowExecutionUpdateAccepted = 80,
    WorkflowExecutionUpdateCompleted = 81,
    WorkflowExecutionUpdateRejected = 82,

    // Nexus events (90-95)
    NexusOperationScheduled = 90,
    NexusOperationStarted = 91,
    NexusOperationCompleted = 92,
    NexusOperationFailed = 93,
    NexusOperationCanceled = 94,
    NexusOperationTimedOut = 95,

    // Reset events (100-101)
    WorkflowExecutionReset = 100,
    HistoryBranchCreated = 101,

    // Cancel events (110-112)
    WorkflowExecutionCancelRequested = 110,
    RequestCancelExternalWorkflowExecutionInitiated = 111,
    RequestCancelExternalWorkflowExecutionFailed = 112,

    // External signal/cancel response events (120-121)
    ExternalWorkflowExecutionSignaled = 120,
    ExternalWorkflowExecutionCancelRequested = 121,

    // Upsert events (130)
    UpsertWorkflowSearchAttributes = 130,

    // Workflow options changed (140)
    WorkflowExecutionOptionsUpdated = 140,
}

impl ExtendedEventType {
    /// Whether this event type represents a command (workflow → server).
    pub fn is_command(&self) -> bool {
        matches!(
            self,
            ExtendedEventType::WorkflowTaskCompleted
                | ExtendedEventType::ActivityTaskScheduled
                | ExtendedEventType::TimerStarted
                | ExtendedEventType::StartChildWorkflowExecutionInitiated
                | ExtendedEventType::SignalExternalWorkflowExecutionInitiated
                | ExtendedEventType::RequestCancelExternalWorkflowExecutionInitiated
                | ExtendedEventType::ActivityTaskCancelRequested
                | ExtendedEventType::TimerCanceled
                | ExtendedEventType::MarkerRecorded
                | ExtendedEventType::UpsertWorkflowSearchAttributes
        )
    }

    /// Whether this event type represents a failure terminal state.
    pub fn is_failure(&self) -> bool {
        matches!(
            self,
            ExtendedEventType::WorkflowExecutionFailed
                | ExtendedEventType::WorkflowExecutionTimedOut
                | ExtendedEventType::WorkflowExecutionTerminated
                | ExtendedEventType::WorkflowExecutionCanceled
                | ExtendedEventType::ActivityTaskFailed
                | ExtendedEventType::ActivityTaskTimedOut
                | ExtendedEventType::ChildWorkflowExecutionFailed
                | ExtendedEventType::ChildWorkflowExecutionTimedOut
                | ExtendedEventType::NexusOperationFailed
                | ExtendedEventType::NexusOperationTimedOut
        )
    }

    /// Whether this event type represents a completion terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            ExtendedEventType::WorkflowExecutionCompleted
                | ExtendedEventType::WorkflowExecutionFailed
                | ExtendedEventType::WorkflowExecutionCanceled
                | ExtendedEventType::WorkflowExecutionTerminated
                | ExtendedEventType::WorkflowExecutionTimedOut
                | ExtendedEventType::WorkflowExecutionContinuedAsNew
        )
    }

    /// Category name for grouping.
    pub fn category(&self) -> &'static str {
        match *self {
            Self::WorkflowExecutionStarted
            | Self::WorkflowExecutionCompleted
            | Self::WorkflowExecutionFailed
            | Self::WorkflowExecutionCanceled
            | Self::WorkflowExecutionTerminated
            | Self::WorkflowExecutionTimedOut
            | Self::WorkflowExecutionContinuedAsNew
            | Self::WorkflowExecutionReset
            | Self::WorkflowExecutionOptionsUpdated => "workflow",

            Self::WorkflowTaskScheduled
            | Self::WorkflowTaskStarted
            | Self::WorkflowTaskCompleted
            | Self::WorkflowTaskFailed
            | Self::WorkflowTaskTimedOut => "workflow_task",

            Self::ActivityTaskScheduled
            | Self::ActivityTaskStarted
            | Self::ActivityTaskCompleted
            | Self::ActivityTaskFailed
            | Self::ActivityTaskTimedOut
            | Self::ActivityTaskCanceled
            | Self::ActivityTaskCancelRequested
            | Self::ActivityTaskPaused
            | Self::ActivityTaskUnpaused => "activity",

            Self::TimerStarted | Self::TimerFired | Self::TimerCanceled => "timer",

            Self::WorkflowExecutionSignaled
            | Self::SignalExternalWorkflowExecutionInitiated
            | Self::SignalExternalWorkflowExecutionFailed
            | Self::ExternalWorkflowExecutionSignaled => "signal",

            Self::StartChildWorkflowExecutionInitiated
            | Self::StartChildWorkflowExecutionFailed
            | Self::ChildWorkflowExecutionStarted
            | Self::ChildWorkflowExecutionCompleted
            | Self::ChildWorkflowExecutionFailed
            | Self::ChildWorkflowExecutionCanceled
            | Self::ChildWorkflowExecutionTimedOut
            | Self::ChildWorkflowExecutionTerminated
            | Self::ExternalWorkflowExecutionCancelRequested
            | Self::WorkflowExecutionCancelRequested
            | Self::RequestCancelExternalWorkflowExecutionInitiated
            | Self::RequestCancelExternalWorkflowExecutionFailed => "child_workflow",

            Self::MarkerRecorded | Self::SideEffectMarker => "marker",
            Self::WorkflowExecutionUpdateAccepted
            | Self::WorkflowExecutionUpdateCompleted
            | Self::WorkflowExecutionUpdateRejected => "update",
            Self::NexusOperationScheduled
            | Self::NexusOperationStarted
            | Self::NexusOperationCompleted
            | Self::NexusOperationFailed
            | Self::NexusOperationCanceled
            | Self::NexusOperationTimedOut => "nexus",
            Self::HistoryBranchCreated => "branch",
            Self::UpsertWorkflowSearchAttributes => "search_attributes",
        }
    }
}

// ─── History Extensions (Reverse Pagination, Branching) ────────────────────

/// An extended history event with full metadata.
#[derive(Debug, Clone)]
pub struct ExtendedHistoryEvent {
    pub event_id: u64,
    pub event_type: ExtendedEventType,
    pub timestamp_ms: u64,
    pub workflow_key: u64,
    pub payload: Vec<u8>,
    pub branch_id: Option<u64>,
    pub version: i64,
}

/// Extended history store with reverse pagination and branching support.
#[derive(Debug, Default)]
pub struct ExtendedHistoryStore {
    /// workflow_key -> events (ordered by event_id)
    histories: RwLock<HashMap<u64, Vec<ExtendedHistoryEvent>>>,
    /// workflow_key -> branch_id -> events (for history branches from resets)
    branches: RwLock<HashMap<u64, HashMap<u64, Vec<ExtendedHistoryEvent>>>>,
    /// Next event ID counter.
    next_event_id: AtomicU64,
}

impl ExtendedHistoryStore {
    pub fn new() -> Self {
        Self {
            histories: RwLock::new(HashMap::new()),
            branches: RwLock::new(HashMap::new()),
            next_event_id: AtomicU64::new(1),
        }
    }

    /// Append an event to a workflow's history.
    pub fn append(
        &self,
        workflow_key: u64,
        event_type: ExtendedEventType,
        payload: Vec<u8>,
    ) -> u64 {
        let event_id = self.next_event_id.fetch_add(1, Ordering::Relaxed);
        let event = ExtendedHistoryEvent {
            event_id,
            event_type,
            timestamp_ms: 0,
            workflow_key,
            payload,
            branch_id: None,
            version: 0,
        };
        self.histories
            .write()
            .unwrap()
            .entry(workflow_key)
            .or_default()
            .push(event);
        event_id
    }

    /// Forward pagination (like Temporal's GetWorkflowExecutionHistory).
    pub fn get_page_forward(
        &self,
        workflow_key: u64,
        start_id: u64,
        max_count: usize,
    ) -> Vec<ExtendedHistoryEvent> {
        self.histories
            .read()
            .unwrap()
            .get(&workflow_key)
            .map(|events| {
                events
                    .iter()
                    .filter(|e| e.event_id >= start_id)
                    .take(max_count)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Reverse pagination (like Temporal's GetWorkflowExecutionHistoryReverse).
    /// Returns events in reverse order starting from the latest.
    pub fn get_page_reverse(
        &self,
        workflow_key: u64,
        start_id: u64,
        max_count: usize,
    ) -> Vec<ExtendedHistoryEvent> {
        self.histories
            .read()
            .unwrap()
            .get(&workflow_key)
            .map(|events| {
                events
                    .iter()
                    .rev()
                    .filter(|e| e.event_id <= start_id)
                    .take(max_count)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get the full history for a workflow.
    pub fn get_full_history(&self, workflow_key: u64) -> Vec<ExtendedHistoryEvent> {
        self.histories
            .read()
            .unwrap()
            .get(&workflow_key)
            .cloned()
            .unwrap_or_default()
    }

    /// Get event count for a workflow.
    pub fn event_count(&self, workflow_key: u64) -> usize {
        self.histories
            .read()
            .unwrap()
            .get(&workflow_key)
            .map(|e| e.len())
            .unwrap_or(0)
    }

    /// Create a history branch from a specific event (for workflow reset).
    /// Returns the branch_id.
    pub fn create_branch(&self, workflow_key: u64, fork_event_id: u64) -> Option<u64> {
        let histories = self.histories.read().unwrap();
        let events = histories.get(&workflow_key)?;

        // Copy events up to fork_event_id
        let branch_events: Vec<ExtendedHistoryEvent> = events
            .iter()
            .filter(|e| e.event_id <= fork_event_id)
            .cloned()
            .collect();

        let branch_id = self.next_event_id.fetch_add(1, Ordering::Relaxed);
        drop(histories);

        self.branches
            .write()
            .unwrap()
            .entry(workflow_key)
            .or_default()
            .insert(branch_id, branch_events);

        Some(branch_id)
    }

    /// Get events from a specific branch.
    pub fn get_branch_events(
        &self,
        workflow_key: u64,
        branch_id: u64,
    ) -> Vec<ExtendedHistoryEvent> {
        self.branches
            .read()
            .unwrap()
            .get(&workflow_key)
            .and_then(|branches| branches.get(&branch_id))
            .cloned()
            .unwrap_or_default()
    }

    /// List all branch IDs for a workflow.
    pub fn list_branches(&self, workflow_key: u64) -> Vec<u64> {
        self.branches
            .read()
            .unwrap()
            .get(&workflow_key)
            .map(|b| b.keys().copied().collect())
            .unwrap_or_default()
    }

    /// Get events by type.
    pub fn get_events_by_type(
        &self,
        workflow_key: u64,
        event_type: ExtendedEventType,
    ) -> Vec<ExtendedHistoryEvent> {
        self.histories
            .read()
            .unwrap()
            .get(&workflow_key)
            .map(|events| {
                events
                    .iter()
                    .filter(|e| e.event_type == event_type)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get the last event in a workflow's history.
    pub fn last_event(&self, workflow_key: u64) -> Option<ExtendedHistoryEvent> {
        self.histories
            .read()
            .unwrap()
            .get(&workflow_key)
            .and_then(|events| events.last().cloned())
    }

    /// Remove a workflow's history.
    pub fn remove(&self, workflow_key: u64) -> bool {
        let removed = self
            .histories
            .write()
            .unwrap()
            .remove(&workflow_key)
            .is_some();
        self.branches.write().unwrap().remove(&workflow_key);
        removed
    }

    /// Count of tracked workflows.
    pub fn workflow_count(&self) -> usize {
        self.histories.read().unwrap().len()
    }

    /// Total event count across all workflows.
    pub fn total_event_count(&self) -> usize {
        self.histories
            .read()
            .unwrap()
            .values()
            .map(|e| e.len())
            .sum()
    }
}

// ─── Engine Statistics ──────────────────────────────────────────────────────

/// Aggregate workflow execution statistics.
#[derive(Debug, Clone, Default)]
pub struct EngineStats {
    pub total_started: u64,
    pub total_completed: u64,
    pub total_failed: u64,
    pub total_canceled: u64,
    pub total_terminated: u64,
    pub total_timed_out: u64,
    pub total_continued_as_new: u64,
    pub currently_running: u64,
    pub total_signals_received: u64,
    pub total_queries_received: u64,
    pub total_updates_received: u64,
    pub total_activities_scheduled: u64,
    pub total_activities_completed: u64,
    pub total_timers_started: u64,
    pub total_timers_fired: u64,
    pub total_child_workflows_started: u64,
    pub total_child_workflows_completed: u64,
}

/// Engine-level workflow statistics tracker.
#[derive(Debug, Default)]
pub struct EngineStatistics {
    stats: Mutex<EngineStats>,
    /// Status -> count for quick lookup.
    status_counts: Mutex<HashMap<String, u64>>,
    /// Latency buckets for workflow execution duration (ms).
    latency_buckets: Mutex<VecDeque<u64>>,
    max_latency_samples: usize,
}

impl EngineStatistics {
    pub fn new(max_latency_samples: usize) -> Self {
        Self {
            stats: Mutex::new(EngineStats::default()),
            status_counts: Mutex::new(HashMap::new()),
            latency_buckets: Mutex::new(VecDeque::new()),
            max_latency_samples,
        }
    }

    /// Record a workflow start.
    pub fn record_start(&self) {
        let mut stats = self.stats.lock().unwrap();
        stats.total_started += 1;
        stats.currently_running += 1;
        *self
            .status_counts
            .lock()
            .unwrap()
            .entry("running".into())
            .or_insert(0) += 1;
    }

    /// Record a workflow completion with duration.
    pub fn record_completion(&self, duration_ms: u64) {
        let mut stats = self.stats.lock().unwrap();
        stats.total_completed += 1;
        stats.currently_running = stats.currently_running.saturating_sub(1);
        self.record_latency_locked(duration_ms);
    }

    /// Record a workflow failure.
    pub fn record_failure(&self, duration_ms: u64) {
        let mut stats = self.stats.lock().unwrap();
        stats.total_failed += 1;
        stats.currently_running = stats.currently_running.saturating_sub(1);
        self.record_latency_locked(duration_ms);
    }

    /// Record a workflow cancellation.
    pub fn record_cancellation(&self) {
        let mut stats = self.stats.lock().unwrap();
        stats.total_canceled += 1;
        stats.currently_running = stats.currently_running.saturating_sub(1);
    }

    /// Record a workflow termination.
    pub fn record_termination(&self) {
        let mut stats = self.stats.lock().unwrap();
        stats.total_terminated += 1;
        stats.currently_running = stats.currently_running.saturating_sub(1);
    }

    /// Record a workflow timeout.
    pub fn record_timeout(&self) {
        let mut stats = self.stats.lock().unwrap();
        stats.total_timed_out += 1;
        stats.currently_running = stats.currently_running.saturating_sub(1);
    }

    /// Record a continue-as-new.
    pub fn record_continued_as_new(&self) {
        let mut stats = self.stats.lock().unwrap();
        stats.total_continued_as_new += 1;
        stats.currently_running = stats.currently_running.saturating_sub(1);
    }

    /// Record a signal received.
    pub fn record_signal(&self) {
        self.stats.lock().unwrap().total_signals_received += 1;
    }

    /// Record a query received.
    pub fn record_query(&self) {
        self.stats.lock().unwrap().total_queries_received += 1;
    }

    /// Record an update received.
    pub fn record_update(&self) {
        self.stats.lock().unwrap().total_updates_received += 1;
    }

    /// Record an activity scheduled.
    pub fn record_activity_scheduled(&self) {
        self.stats.lock().unwrap().total_activities_scheduled += 1;
    }

    /// Record an activity completed.
    pub fn record_activity_completed(&self) {
        self.stats.lock().unwrap().total_activities_completed += 1;
    }

    /// Record a timer started.
    pub fn record_timer_started(&self) {
        self.stats.lock().unwrap().total_timers_started += 1;
    }

    /// Record a timer fired.
    pub fn record_timer_fired(&self) {
        self.stats.lock().unwrap().total_timers_fired += 1;
    }

    /// Record a child workflow started.
    pub fn record_child_started(&self) {
        self.stats.lock().unwrap().total_child_workflows_started += 1;
    }

    /// Record a child workflow completed.
    pub fn record_child_completed(&self) {
        self.stats.lock().unwrap().total_child_workflows_completed += 1;
    }

    /// Get a snapshot of the current statistics.
    pub fn snapshot(&self) -> EngineStats {
        self.stats.lock().unwrap().clone()
    }

    /// Get status counts.
    pub fn status_counts(&self) -> HashMap<String, u64> {
        self.status_counts.lock().unwrap().clone()
    }

    /// Get latency percentile (p50, p95, p99).
    pub fn latency_percentiles(&self) -> (u64, u64, u64) {
        let buckets = self.latency_buckets.lock().unwrap();
        if buckets.is_empty() {
            return (0, 0, 0);
        }
        let mut sorted: Vec<u64> = buckets.iter().copied().collect();
        sorted.sort();
        let len = sorted.len();
        let p50 = sorted[len * 50 / 100];
        let p95 = sorted[len * 95 / 100];
        let p99 = sorted[len.saturating_sub(1) * 99 / 100];
        (p50, p95, p99)
    }

    fn record_latency_locked(&self, duration_ms: u64) {
        let mut buckets = self.latency_buckets.lock().unwrap();
        buckets.push_back(duration_ms);
        while buckets.len() > self.max_latency_samples {
            buckets.pop_front();
        }
    }

    /// Reset all statistics.
    pub fn reset(&self) {
        *self.stats.lock().unwrap() = EngineStats::default();
        self.status_counts.lock().unwrap().clear();
        self.latency_buckets.lock().unwrap().clear();
    }
}

// ─── Size Limit Enforcer ───────────────────────────────────────────────────

/// Configuration for size limits.
#[derive(Debug, Clone)]
pub struct SizeLimitConfig {
    /// Maximum payload size in bytes (default: 2MB).
    pub max_payload_size: usize,
    /// Maximum history size for a single workflow (default: 50MB).
    pub max_history_size: usize,
    /// Maximum number of events in a workflow history (default: 50,000).
    pub max_event_count: usize,
    /// Maximum number of pending signals per workflow (default: 1000).
    pub max_pending_signals: usize,
    /// Maximum number of in-flight activities per workflow (default: 1000).
    pub max_inflight_activities: usize,
    /// Maximum total search attributes per workflow (default: 200).
    pub max_search_attributes: usize,
    /// Maximum memo size per workflow (default: 2MB).
    pub max_memo_size: usize,
    /// Warn threshold as fraction of limit (default: 0.8).
    pub warn_threshold: f64,
}

impl Default for SizeLimitConfig {
    fn default() -> Self {
        Self {
            max_payload_size: 2 * 1024 * 1024,
            max_history_size: 50 * 1024 * 1024,
            max_event_count: 50_000,
            max_pending_signals: 1000,
            max_inflight_activities: 1000,
            max_search_attributes: 200,
            max_memo_size: 2 * 1024 * 1024,
            warn_threshold: 0.8,
        }
    }
}

/// Result of a size limit check.
#[derive(Debug, Clone)]
pub enum SizeCheckResult {
    Ok,
    Warn {
        limit: &'static str,
        actual: usize,
        max: usize,
    },
    Exceeded {
        limit: &'static str,
        actual: usize,
        max: usize,
    },
}

/// Integrated size limit enforcer.
#[derive(Debug, Clone)]
pub struct SizeLimitEnforcer {
    config: SizeLimitConfig,
}

impl SizeLimitEnforcer {
    pub fn new(config: SizeLimitConfig) -> Self {
        Self { config }
    }

    /// Check a payload against the size limit.
    pub fn check_payload(&self, size: usize) -> SizeCheckResult {
        if size > self.config.max_payload_size {
            SizeCheckResult::Exceeded {
                limit: "max_payload_size",
                actual: size,
                max: self.config.max_payload_size,
            }
        } else if size as f64 > self.config.max_payload_size as f64 * self.config.warn_threshold {
            SizeCheckResult::Warn {
                limit: "max_payload_size",
                actual: size,
                max: self.config.max_payload_size,
            }
        } else {
            SizeCheckResult::Ok
        }
    }

    /// Check history size.
    pub fn check_history_size(&self, total_bytes: usize) -> SizeCheckResult {
        if total_bytes > self.config.max_history_size {
            SizeCheckResult::Exceeded {
                limit: "max_history_size",
                actual: total_bytes,
                max: self.config.max_history_size,
            }
        } else if total_bytes as f64
            > self.config.max_history_size as f64 * self.config.warn_threshold
        {
            SizeCheckResult::Warn {
                limit: "max_history_size",
                actual: total_bytes,
                max: self.config.max_history_size,
            }
        } else {
            SizeCheckResult::Ok
        }
    }

    /// Check event count.
    pub fn check_event_count(&self, count: usize) -> SizeCheckResult {
        if count > self.config.max_event_count {
            SizeCheckResult::Exceeded {
                limit: "max_event_count",
                actual: count,
                max: self.config.max_event_count,
            }
        } else if count as f64 > self.config.max_event_count as f64 * self.config.warn_threshold {
            SizeCheckResult::Warn {
                limit: "max_event_count",
                actual: count,
                max: self.config.max_event_count,
            }
        } else {
            SizeCheckResult::Ok
        }
    }

    /// Check pending signal count.
    pub fn check_pending_signals(&self, count: usize) -> SizeCheckResult {
        if count > self.config.max_pending_signals {
            SizeCheckResult::Exceeded {
                limit: "max_pending_signals",
                actual: count,
                max: self.config.max_pending_signals,
            }
        } else {
            SizeCheckResult::Ok
        }
    }

    /// Check in-flight activity count.
    pub fn check_inflight_activities(&self, count: usize) -> SizeCheckResult {
        if count > self.config.max_inflight_activities {
            SizeCheckResult::Exceeded {
                limit: "max_inflight_activities",
                actual: count,
                max: self.config.max_inflight_activities,
            }
        } else {
            SizeCheckResult::Ok
        }
    }

    /// Check search attribute count.
    pub fn check_search_attributes(&self, count: usize) -> SizeCheckResult {
        if count > self.config.max_search_attributes {
            SizeCheckResult::Exceeded {
                limit: "max_search_attributes",
                actual: count,
                max: self.config.max_search_attributes,
            }
        } else {
            SizeCheckResult::Ok
        }
    }

    /// Check memo size.
    pub fn check_memo_size(&self, size: usize) -> SizeCheckResult {
        if size > self.config.max_memo_size {
            SizeCheckResult::Exceeded {
                limit: "max_memo_size",
                actual: size,
                max: self.config.max_memo_size,
            }
        } else {
            SizeCheckResult::Ok
        }
    }

    /// Get the config.
    pub fn config(&self) -> &SizeLimitConfig {
        &self.config
    }
}

// ─── Poll Context Manager ──────────────────────────────────────────────────

/// Tracks outstanding poll contexts for cancellation when clients disconnect.
#[derive(Debug, Default)]
pub struct PollContextManager {
    /// poller_id -> (namespace, registered_at)
    outstanding_polls: Mutex<HashMap<String, PollContext>>,
    /// Total polls registered.
    total_registered: AtomicU64,
    /// Total polls canceled.
    total_canceled: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct PollContext {
    pub poller_id: String,
    pub namespace: String,
    pub task_queue: String,
    pub registered_at: Instant,
}

impl PollContextManager {
    pub fn new() -> Self {
        Self {
            outstanding_polls: Mutex::new(HashMap::new()),
            total_registered: AtomicU64::new(0),
            total_canceled: AtomicU64::new(0),
        }
    }

    /// Register an outstanding poll.
    pub fn register(&self, poller_id: String, namespace: String, task_queue: String) {
        self.outstanding_polls.lock().unwrap().insert(
            poller_id.clone(),
            PollContext {
                poller_id,
                namespace,
                task_queue,
                registered_at: Instant::now(),
            },
        );
        self.total_registered.fetch_add(1, Ordering::Relaxed);
    }

    /// Cancel an outstanding poll. Returns true if found.
    pub fn cancel(&self, poller_id: &str) -> bool {
        let removed = self
            .outstanding_polls
            .lock()
            .unwrap()
            .remove(poller_id)
            .is_some();
        if removed {
            self.total_canceled.fetch_add(1, Ordering::Relaxed);
        }
        removed
    }

    /// Cancel all outstanding polls for a namespace. Returns count canceled.
    pub fn cancel_for_namespace(&self, namespace: &str) -> usize {
        let mut polls = self.outstanding_polls.lock().unwrap();
        let to_remove: Vec<String> = polls
            .iter()
            .filter(|(_, ctx)| ctx.namespace == namespace)
            .map(|(id, _)| id.clone())
            .collect();
        let count = to_remove.len();
        for id in to_remove {
            polls.remove(&id);
        }
        self.total_canceled
            .fetch_add(count as u64, Ordering::Relaxed);
        count
    }

    /// Cancel all outstanding polls for a task queue. Returns count canceled.
    pub fn cancel_for_task_queue(&self, task_queue: &str) -> usize {
        let mut polls = self.outstanding_polls.lock().unwrap();
        let to_remove: Vec<String> = polls
            .iter()
            .filter(|(_, ctx)| ctx.task_queue == task_queue)
            .map(|(id, _)| id.clone())
            .collect();
        let count = to_remove.len();
        for id in to_remove {
            polls.remove(&id);
        }
        self.total_canceled
            .fetch_add(count as u64, Ordering::Relaxed);
        count
    }

    /// Get count of outstanding polls.
    pub fn outstanding_count(&self) -> usize {
        self.outstanding_polls.lock().unwrap().len()
    }

    /// Get outstanding polls for a namespace.
    pub fn outstanding_for_namespace(&self, namespace: &str) -> Vec<PollContext> {
        self.outstanding_polls
            .lock()
            .unwrap()
            .values()
            .filter(|ctx| ctx.namespace == namespace)
            .cloned()
            .collect()
    }

    /// Get total registered polls.
    pub fn total_registered(&self) -> u64 {
        self.total_registered.load(Ordering::Relaxed)
    }

    /// Get total canceled polls.
    pub fn total_canceled(&self) -> u64 {
        self.total_canceled.load(Ordering::Relaxed)
    }
}

// ─── Namespace Retention ───────────────────────────────────────────────────

/// Retention policy for a namespace.
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    /// How long to retain completed workflow histories (days).
    pub retention_days: u32,
    /// Whether to archive before deleting.
    pub archive_before_delete: bool,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            retention_days: 30,
            archive_before_delete: false,
        }
    }
}

/// Namespace retention manager with history TTL cleanup.
#[derive(Debug, Default)]
pub struct NamespaceRetentionManager {
    /// namespace -> retention policy
    policies: RwLock<HashMap<String, RetentionPolicy>>,
    /// workflow_key -> completion_time (for TTL cleanup)
    completion_times: Mutex<HashMap<u64, Instant>>,
    /// Total workflows cleaned up.
    total_cleaned: AtomicU64,
}

impl NamespaceRetentionManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set retention policy for a namespace.
    pub fn set_policy(&self, namespace: String, policy: RetentionPolicy) {
        self.policies.write().unwrap().insert(namespace, policy);
    }

    /// Get retention policy for a namespace.
    pub fn get_policy(&self, namespace: &str) -> Option<RetentionPolicy> {
        self.policies.read().unwrap().get(namespace).cloned()
    }

    /// Record a workflow completion for TTL tracking.
    pub fn record_completion(&self, workflow_key: u64) {
        self.completion_times
            .lock()
            .unwrap()
            .insert(workflow_key, Instant::now());
    }

    /// Remove a workflow from TTL tracking.
    pub fn remove_tracking(&self, workflow_key: u64) {
        self.completion_times.lock().unwrap().remove(&workflow_key);
    }

    /// Find workflows that have exceeded their retention period.
    /// Returns workflow_keys eligible for cleanup.
    pub fn find_expired(&self, namespace: &str) -> Vec<u64> {
        let policy = match self.get_policy(namespace) {
            Some(p) => p,
            None => return vec![],
        };

        let retention = Duration::from_secs(policy.retention_days as u64 * 86400);
        let now = Instant::now();

        self.completion_times
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, completed_at)| now.duration_since(**completed_at) > retention)
            .map(|(key, _)| *key)
            .collect()
    }

    /// Clean up expired workflows. Returns count cleaned.
    pub fn cleanup_expired(&self, namespace: &str) -> usize {
        let expired = self.find_expired(namespace);
        let count = expired.len();
        let mut times = self.completion_times.lock().unwrap();
        for key in &expired {
            times.remove(key);
        }
        self.total_cleaned
            .fetch_add(count as u64, Ordering::Relaxed);
        count
    }

    /// Get count of tracked completions.
    pub fn tracked_count(&self) -> usize {
        self.completion_times.lock().unwrap().len()
    }

    /// Get total cleaned count.
    pub fn total_cleaned(&self) -> u64 {
        self.total_cleaned.load(Ordering::Relaxed)
    }

    /// List all namespaces with retention policies.
    pub fn namespaces_with_policies(&self) -> Vec<String> {
        self.policies.read().unwrap().keys().cloned().collect()
    }
}

// ─── Workflow Task Tracker ─────────────────────────────────────────────────

/// Tracks workflow task scheduling and attempts.
#[derive(Debug, Default)]
pub struct WorkflowTaskTracker {
    /// workflow_key -> task state
    tasks: Mutex<HashMap<u64, WorkflowTaskState>>,
    /// Total workflow tasks scheduled.
    total_scheduled: AtomicU64,
    /// Total workflow tasks completed.
    total_completed: AtomicU64,
    /// Total workflow tasks failed.
    total_failed: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct WorkflowTaskState {
    pub workflow_key: u64,
    pub attempt: u32,
    pub scheduled_at: Instant,
    pub started_at: Option<Instant>,
    pub sticky_worker: Option<String>,
    pub schedule_to_start_timeout: Duration,
    pub start_to_close_timeout: Duration,
}

impl WorkflowTaskTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Schedule a workflow task.
    pub fn schedule(&self, workflow_key: u64, schedule_to_start_ms: u64, start_to_close_ms: u64) {
        let state = WorkflowTaskState {
            workflow_key,
            attempt: 1,
            scheduled_at: Instant::now(),
            started_at: None,
            sticky_worker: None,
            schedule_to_start_timeout: Duration::from_millis(schedule_to_start_ms),
            start_to_close_timeout: Duration::from_millis(start_to_close_ms),
        };
        self.tasks.lock().unwrap().insert(workflow_key, state);
        self.total_scheduled.fetch_add(1, Ordering::Relaxed);
    }

    /// Mark a workflow task as started by a worker.
    pub fn mark_started(&self, workflow_key: u64, worker_identity: &str) -> bool {
        let mut tasks = self.tasks.lock().unwrap();
        if let Some(state) = tasks.get_mut(&workflow_key) {
            state.started_at = Some(Instant::now());
            state.sticky_worker = Some(worker_identity.to_string());
            true
        } else {
            false
        }
    }

    /// Mark a workflow task as completed.
    pub fn mark_completed(&self, workflow_key: u64) -> bool {
        let removed = self.tasks.lock().unwrap().remove(&workflow_key).is_some();
        if removed {
            self.total_completed.fetch_add(1, Ordering::Relaxed);
        }
        removed
    }

    /// Mark a workflow task as failed and increment attempt.
    pub fn mark_failed(&self, workflow_key: u64) -> Option<u32> {
        let mut tasks = self.tasks.lock().unwrap();
        if let Some(state) = tasks.get_mut(&workflow_key) {
            state.attempt += 1;
            state.started_at = None;
            state.sticky_worker = None;
            self.total_failed.fetch_add(1, Ordering::Relaxed);
            Some(state.attempt)
        } else {
            None
        }
    }

    /// Check for timed-out workflow tasks.
    pub fn check_timeouts(&self) -> Vec<u64> {
        let tasks = self.tasks.lock().unwrap();
        let now = Instant::now();
        let mut timed_out = Vec::new();

        for (key, state) in tasks.iter() {
            if state.started_at.is_none() {
                // Schedule-to-start timeout
                if now.duration_since(state.scheduled_at) > state.schedule_to_start_timeout {
                    timed_out.push(*key);
                }
            } else if let Some(started) = state.started_at {
                // Start-to-close timeout
                if now.duration_since(started) > state.start_to_close_timeout {
                    timed_out.push(*key);
                }
            }
        }

        timed_out
    }

    /// Reset sticky assignment for a workflow (like Temporal's ResetStickyTaskQueue).
    pub fn reset_sticky(&self, workflow_key: u64) -> bool {
        let mut tasks = self.tasks.lock().unwrap();
        if let Some(state) = tasks.get_mut(&workflow_key) {
            state.sticky_worker = None;
            true
        } else {
            false
        }
    }

    /// Get the current attempt number for a workflow task.
    pub fn get_attempt(&self, workflow_key: u64) -> Option<u32> {
        self.tasks
            .lock()
            .unwrap()
            .get(&workflow_key)
            .map(|s| s.attempt)
    }

    /// Get the scheduling latency (time from schedule to start).
    pub fn get_schedule_latency(&self, workflow_key: u64) -> Option<Duration> {
        let tasks = self.tasks.lock().unwrap();
        let state = tasks.get(&workflow_key)?;
        state
            .started_at
            .map(|started| started.duration_since(state.scheduled_at))
    }

    /// Count of in-flight workflow tasks.
    pub fn in_flight_count(&self) -> usize {
        self.tasks.lock().unwrap().len()
    }

    /// Total scheduled.
    pub fn total_scheduled(&self) -> u64 {
        self.total_scheduled.load(Ordering::Relaxed)
    }

    /// Total completed.
    pub fn total_completed(&self) -> u64 {
        self.total_completed.load(Ordering::Relaxed)
    }

    /// Total failed.
    pub fn total_failed(&self) -> u64 {
        self.total_failed.load(Ordering::Relaxed)
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // --- Extended Event Type Tests ---

    #[test]
    fn test_event_type_categories() {
        assert_eq!(
            ExtendedEventType::WorkflowExecutionStarted.category(),
            "workflow"
        );
        assert_eq!(
            ExtendedEventType::ActivityTaskScheduled.category(),
            "activity"
        );
        assert_eq!(ExtendedEventType::TimerStarted.category(), "timer");
        assert_eq!(
            ExtendedEventType::WorkflowExecutionSignaled.category(),
            "signal"
        );
        assert_eq!(
            ExtendedEventType::StartChildWorkflowExecutionInitiated.category(),
            "child_workflow"
        );
        assert_eq!(
            ExtendedEventType::NexusOperationScheduled.category(),
            "nexus"
        );
    }

    #[test]
    fn test_event_type_is_terminal() {
        assert!(ExtendedEventType::WorkflowExecutionCompleted.is_terminal());
        assert!(ExtendedEventType::WorkflowExecutionFailed.is_terminal());
        assert!(ExtendedEventType::WorkflowExecutionTimedOut.is_terminal());
        assert!(!ExtendedEventType::ActivityTaskCompleted.is_terminal());
        assert!(!ExtendedEventType::TimerStarted.is_terminal());
    }

    #[test]
    fn test_event_type_is_failure() {
        assert!(ExtendedEventType::WorkflowExecutionFailed.is_failure());
        assert!(ExtendedEventType::ActivityTaskTimedOut.is_failure());
        assert!(ExtendedEventType::NexusOperationFailed.is_failure());
        assert!(!ExtendedEventType::WorkflowExecutionCompleted.is_failure());
    }

    #[test]
    fn test_event_type_is_command() {
        assert!(ExtendedEventType::ActivityTaskScheduled.is_command());
        assert!(ExtendedEventType::TimerStarted.is_command());
        assert!(!ExtendedEventType::ActivityTaskCompleted.is_command());
    }

    // --- Extended History Store Tests ---

    #[test]
    fn test_history_append_and_forward() {
        let store = ExtendedHistoryStore::new();
        store.append(1, ExtendedEventType::WorkflowExecutionStarted, vec![]);
        store.append(1, ExtendedEventType::ActivityTaskScheduled, vec![1, 2]);
        store.append(1, ExtendedEventType::ActivityTaskCompleted, vec![3, 4]);
        store.append(1, ExtendedEventType::WorkflowExecutionCompleted, vec![]);

        let page = store.get_page_forward(1, 1, 10);
        assert_eq!(page.len(), 4);
        assert_eq!(
            page[0].event_type,
            ExtendedEventType::WorkflowExecutionStarted
        );
    }

    #[test]
    fn test_history_reverse_pagination() {
        let store = ExtendedHistoryStore::new();
        for i in 0..10 {
            store.append(1, ExtendedEventType::ActivityTaskCompleted, vec![i as u8]);
        }

        let rev = store.get_page_reverse(1, 100, 3);
        assert_eq!(rev.len(), 3);
        // Should be in reverse order (highest event_id first)
        assert!(rev[0].event_id > rev[1].event_id);
        assert!(rev[1].event_id > rev[2].event_id);
    }

    #[test]
    fn test_history_branching() {
        let store = ExtendedHistoryStore::new();
        store.append(1, ExtendedEventType::WorkflowExecutionStarted, vec![]);
        let eid2 = store.append(1, ExtendedEventType::ActivityTaskScheduled, vec![1]);
        store.append(1, ExtendedEventType::ActivityTaskCompleted, vec![2]);

        let branch_id = store.create_branch(1, eid2).unwrap();
        let branch_events = store.get_branch_events(1, branch_id);
        assert_eq!(branch_events.len(), 2); // Started + Scheduled (up to eid2)

        let branches = store.list_branches(1);
        assert_eq!(branches.len(), 1);
    }

    #[test]
    fn test_history_by_type() {
        let store = ExtendedHistoryStore::new();
        store.append(1, ExtendedEventType::WorkflowExecutionStarted, vec![]);
        store.append(1, ExtendedEventType::ActivityTaskScheduled, vec![]);
        store.append(1, ExtendedEventType::ActivityTaskScheduled, vec![]);
        store.append(1, ExtendedEventType::WorkflowExecutionCompleted, vec![]);

        let activities = store.get_events_by_type(1, ExtendedEventType::ActivityTaskScheduled);
        assert_eq!(activities.len(), 2);
    }

    #[test]
    fn test_history_remove() {
        let store = ExtendedHistoryStore::new();
        store.append(1, ExtendedEventType::WorkflowExecutionStarted, vec![]);
        assert_eq!(store.event_count(1), 1);
        assert!(store.remove(1));
        assert_eq!(store.event_count(1), 0);
    }

    // --- Engine Statistics Tests ---

    #[test]
    fn test_engine_stats_lifecycle() {
        let stats = EngineStatistics::new(1000);
        stats.record_start();
        stats.record_start();
        stats.record_completion(100);
        stats.record_failure(200);

        let snap = stats.snapshot();
        assert_eq!(snap.total_started, 2);
        assert_eq!(snap.total_completed, 1);
        assert_eq!(snap.total_failed, 1);
        assert_eq!(snap.currently_running, 0);
    }

    #[test]
    fn test_engine_stats_signals_queries() {
        let stats = EngineStatistics::new(100);
        stats.record_signal();
        stats.record_signal();
        stats.record_query();
        stats.record_update();

        let snap = stats.snapshot();
        assert_eq!(snap.total_signals_received, 2);
        assert_eq!(snap.total_queries_received, 1);
        assert_eq!(snap.total_updates_received, 1);
    }

    #[test]
    fn test_engine_stats_latency_percentiles() {
        let stats = EngineStatistics::new(1000);
        for i in 1..=100 {
            stats.record_completion(i);
        }
        let (p50, p95, _p99) = stats.latency_percentiles();
        assert!(p50 > 0);
        assert!(p95 >= p50);
    }

    #[test]
    fn test_engine_stats_activities_timers() {
        let stats = EngineStatistics::new(100);
        stats.record_activity_scheduled();
        stats.record_activity_scheduled();
        stats.record_activity_completed();
        stats.record_timer_started();
        stats.record_timer_fired();
        stats.record_child_started();
        stats.record_child_completed();

        let snap = stats.snapshot();
        assert_eq!(snap.total_activities_scheduled, 2);
        assert_eq!(snap.total_activities_completed, 1);
        assert_eq!(snap.total_timers_started, 1);
        assert_eq!(snap.total_timers_fired, 1);
        assert_eq!(snap.total_child_workflows_started, 1);
        assert_eq!(snap.total_child_workflows_completed, 1);
    }

    // --- Size Limit Tests ---

    #[test]
    fn test_size_limit_payload_ok() {
        let enforcer = SizeLimitEnforcer::new(SizeLimitConfig::default());
        assert!(matches!(enforcer.check_payload(1024), SizeCheckResult::Ok));
    }

    #[test]
    fn test_size_limit_payload_exceeded() {
        let enforcer = SizeLimitEnforcer::new(SizeLimitConfig::default());
        let result = enforcer.check_payload(3 * 1024 * 1024);
        assert!(matches!(result, SizeCheckResult::Exceeded { .. }));
    }

    #[test]
    fn test_size_limit_payload_warn() {
        let config = SizeLimitConfig {
            max_payload_size: 1000,
            warn_threshold: 0.8,
            ..Default::default()
        };
        let enforcer = SizeLimitEnforcer::new(config);
        let result = enforcer.check_payload(850);
        assert!(matches!(result, SizeCheckResult::Warn { .. }));
    }

    #[test]
    fn test_size_limit_event_count() {
        let config = SizeLimitConfig {
            max_event_count: 100,
            ..Default::default()
        };
        let enforcer = SizeLimitEnforcer::new(config);
        assert!(matches!(
            enforcer.check_event_count(50),
            SizeCheckResult::Ok
        ));
        assert!(matches!(
            enforcer.check_event_count(150),
            SizeCheckResult::Exceeded { .. }
        ));
    }

    #[test]
    fn test_size_limit_pending_signals() {
        let config = SizeLimitConfig {
            max_pending_signals: 10,
            ..Default::default()
        };
        let enforcer = SizeLimitEnforcer::new(config);
        assert!(matches!(
            enforcer.check_pending_signals(5),
            SizeCheckResult::Ok
        ));
        assert!(matches!(
            enforcer.check_pending_signals(15),
            SizeCheckResult::Exceeded { .. }
        ));
    }

    // --- Poll Context Manager Tests ---

    #[test]
    fn test_poll_register_and_cancel() {
        let mgr = PollContextManager::new();
        mgr.register("p1".into(), "ns1".into(), "q1".into());
        mgr.register("p2".into(), "ns1".into(), "q1".into());
        assert_eq!(mgr.outstanding_count(), 2);

        assert!(mgr.cancel("p1"));
        assert_eq!(mgr.outstanding_count(), 1);
        assert!(!mgr.cancel("p99"));
    }

    #[test]
    fn test_poll_cancel_for_namespace() {
        let mgr = PollContextManager::new();
        mgr.register("p1".into(), "ns1".into(), "q1".into());
        mgr.register("p2".into(), "ns1".into(), "q2".into());
        mgr.register("p3".into(), "ns2".into(), "q1".into());

        let canceled = mgr.cancel_for_namespace("ns1");
        assert_eq!(canceled, 2);
        assert_eq!(mgr.outstanding_count(), 1);
    }

    #[test]
    fn test_poll_cancel_for_task_queue() {
        let mgr = PollContextManager::new();
        mgr.register("p1".into(), "ns1".into(), "q1".into());
        mgr.register("p2".into(), "ns2".into(), "q1".into());
        mgr.register("p3".into(), "ns1".into(), "q2".into());

        let canceled = mgr.cancel_for_task_queue("q1");
        assert_eq!(canceled, 2);
        assert_eq!(mgr.outstanding_count(), 1);
    }

    #[test]
    fn test_poll_totals() {
        let mgr = PollContextManager::new();
        mgr.register("p1".into(), "ns1".into(), "q1".into());
        mgr.register("p2".into(), "ns1".into(), "q1".into());
        mgr.cancel("p1");

        assert_eq!(mgr.total_registered(), 2);
        assert_eq!(mgr.total_canceled(), 1);
    }

    // --- Namespace Retention Tests ---

    #[test]
    fn test_retention_policy() {
        let mgr = NamespaceRetentionManager::new();
        mgr.set_policy(
            "ns1".into(),
            RetentionPolicy {
                retention_days: 7,
                archive_before_delete: true,
            },
        );

        let policy = mgr.get_policy("ns1").unwrap();
        assert_eq!(policy.retention_days, 7);
        assert!(policy.archive_before_delete);
    }

    #[test]
    fn test_retention_tracking() {
        let mgr = NamespaceRetentionManager::new();
        mgr.record_completion(1);
        mgr.record_completion(2);
        assert_eq!(mgr.tracked_count(), 2);
        mgr.remove_tracking(1);
        assert_eq!(mgr.tracked_count(), 1);
    }

    #[test]
    fn test_retention_namespaces() {
        let mgr = NamespaceRetentionManager::new();
        mgr.set_policy("ns1".into(), RetentionPolicy::default());
        mgr.set_policy("ns2".into(), RetentionPolicy::default());
        let ns = mgr.namespaces_with_policies();
        assert_eq!(ns.len(), 2);
    }

    // --- Workflow Task Tracker Tests ---

    #[test]
    fn test_task_schedule_and_complete() {
        let tracker = WorkflowTaskTracker::new();
        tracker.schedule(1, 5000, 10000);
        assert_eq!(tracker.in_flight_count(), 1);
        assert_eq!(tracker.get_attempt(1), Some(1));

        assert!(tracker.mark_started(1, "worker-1"));
        assert!(tracker.mark_completed(1));
        assert_eq!(tracker.in_flight_count(), 0);
        assert_eq!(tracker.total_completed(), 1);
    }

    #[test]
    fn test_task_failure_increments_attempt() {
        let tracker = WorkflowTaskTracker::new();
        tracker.schedule(1, 5000, 10000);

        let attempt = tracker.mark_failed(1).unwrap();
        assert_eq!(attempt, 2);
        assert_eq!(tracker.get_attempt(1), Some(2));
        assert_eq!(tracker.total_failed(), 1);
    }

    #[test]
    fn test_task_reset_sticky() {
        let tracker = WorkflowTaskTracker::new();
        tracker.schedule(1, 5000, 10000);
        tracker.mark_started(1, "worker-1");

        assert!(tracker.reset_sticky(1));
        assert!(!tracker.reset_sticky(99)); // Non-existent
    }

    #[test]
    fn test_task_schedule_latency() {
        let tracker = WorkflowTaskTracker::new();
        tracker.schedule(1, 5000, 10000);
        std::thread::sleep(Duration::from_millis(10));
        tracker.mark_started(1, "worker-1");

        let latency = tracker.get_schedule_latency(1).unwrap();
        assert!(latency >= Duration::from_millis(5));
    }

    #[test]
    fn test_task_totals() {
        let tracker = WorkflowTaskTracker::new();
        tracker.schedule(1, 5000, 10000);
        tracker.schedule(2, 5000, 10000);
        tracker.mark_completed(1);
        tracker.mark_failed(2);

        assert_eq!(tracker.total_scheduled(), 2);
        assert_eq!(tracker.total_completed(), 1);
        assert_eq!(tracker.total_failed(), 1);
    }
}
