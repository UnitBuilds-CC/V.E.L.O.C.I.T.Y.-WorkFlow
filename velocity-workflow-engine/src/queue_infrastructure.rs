//! Queue infrastructure matching Temporal's service/history/queues (~10K+ lines).
//!
//! Covers: queue slices, executable task wrappers, DLQ writer, queue reader/writer,
//! active/standby executor, grouper, iterator, queue actions, alerts, and monitoring.
//! This is the core task processing pipeline that drives all queue-based operations.

use std::collections::{HashMap, VecDeque};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, RwLock,
};
use std::time::SystemTime;

// ═══════════════════════════════════════════════════════════════════════════════
// Task Predicate
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub enum TaskPredicate {
    Universal,
    Namespace(String),
    NamespaceId(String),
    TaskQueue(String),
    WorkflowType(String),
    Priority(u32),
    And(Box<TaskPredicate>, Box<TaskPredicate>),
    Or(Box<TaskPredicate>, Box<TaskPredicate>),
    Not(Box<TaskPredicate>),
}

impl TaskPredicate {
    pub fn matches(&self, task: &QueueTaskDescriptor) -> bool {
        match self {
            TaskPredicate::Universal => true,
            TaskPredicate::Namespace(ns) => task.namespace_id == *ns,
            TaskPredicate::NamespaceId(id) => task.namespace_id == *id,
            TaskPredicate::TaskQueue(tq) => task.task_queue.as_deref() == Some(tq),
            TaskPredicate::WorkflowType(wt) => task.workflow_type.as_deref() == Some(wt),
            TaskPredicate::Priority(p) => task.priority <= *p,
            TaskPredicate::And(a, b) => a.matches(task) && b.matches(task),
            TaskPredicate::Or(a, b) => a.matches(task) || b.matches(task),
            TaskPredicate::Not(p) => !p.matches(task),
        }
    }

    pub fn is_universal(&self) -> bool {
        matches!(self, TaskPredicate::Universal)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Queue Task Descriptor
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskKey {
    pub fire_time: i64,
    pub task_id: i64,
}

impl TaskKey {
    pub fn new(fire_time: i64, task_id: i64) -> Self {
        Self { fire_time, task_id }
    }
    pub fn min() -> Self {
        Self {
            fire_time: i64::MIN,
            task_id: i64::MIN,
        }
    }
    pub fn max() -> Self {
        Self {
            fire_time: i64::MAX,
            task_id: i64::MAX,
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueueTaskDescriptor {
    pub key: TaskKey,
    pub namespace_id: String,
    pub workflow_id: String,
    pub run_id: String,
    pub task_type: String,
    pub task_queue: Option<String>,
    pub workflow_type: Option<String>,
    pub priority: u32,
    pub version: i64,
    pub visibility_time: i64,
    pub created_at: i64,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Executable Task — wraps a task with lifecycle, retry, and state tracking
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutableState {
    Initialized,
    Loaded,
    Executing,
    ExecutionCompleted,
    ExecutionFailed,
    UserProcessingFailed,
    Rescheduled,
    Completed,
    Nackd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutablePriority {
    Low,
    Medium,
    High,
    Critical,
}

pub struct ExecutableTask {
    pub descriptor: QueueTaskDescriptor,
    pub state: ExecutableState,
    pub priority: ExecutablePriority,
    pub attempt: u32,
    pub max_attempts: u32,
    pub last_error: Option<String>,
    pub loaded_at: Option<i64>,
    pub scheduled_at: Option<i64>,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub latency_histogram: Vec<u64>,
    pub user_latency_histogram: Vec<u64>,
    pub segment_id: u64,
}

impl ExecutableTask {
    pub fn new(descriptor: QueueTaskDescriptor) -> Self {
        Self {
            descriptor,
            state: ExecutableState::Initialized,
            priority: ExecutablePriority::Medium,
            attempt: 0,
            max_attempts: 10,
            last_error: None,
            loaded_at: None,
            scheduled_at: None,
            started_at: None,
            completed_at: None,
            latency_histogram: Vec::new(),
            user_latency_histogram: Vec::new(),
            segment_id: 0,
        }
    }

    pub fn mark_loaded(&mut self) {
        self.state = ExecutableState::Loaded;
        self.loaded_at = Some(now_millis());
    }

    pub fn mark_executing(&mut self) {
        self.state = ExecutableState::Executing;
        self.started_at = Some(now_millis());
        self.attempt += 1;
    }

    pub fn mark_completed(&mut self) {
        self.state = ExecutableState::Completed;
        self.completed_at = Some(now_millis());
        if let Some(started) = self.started_at {
            self.latency_histogram.push((now_millis() - started) as u64);
        }
    }

    pub fn mark_failed(&mut self, error: String) {
        self.last_error = Some(error);
        if self.attempt >= self.max_attempts {
            self.state = ExecutableState::Nackd;
        } else {
            self.state = ExecutableState::ExecutionFailed;
        }
    }

    pub fn mark_rescheduled(&mut self) {
        self.state = ExecutableState::Rescheduled;
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            ExecutableState::Completed | ExecutableState::Nackd
        )
    }

    pub fn should_retry(&self) -> bool {
        self.state == ExecutableState::ExecutionFailed && self.attempt < self.max_attempts
    }

    pub fn compute_latency_ms(&self) -> u64 {
        match (self.loaded_at, self.completed_at) {
            (Some(loaded), Some(completed)) => completed.saturating_sub(loaded) as u64,
            _ => 0,
        }
    }

    pub fn user_processing_latency_ms(&self) -> u64 {
        match (self.started_at, self.completed_at) {
            (Some(started), Some(completed)) => completed.saturating_sub(started) as u64,
            _ => 0,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Queue Slice — a logical partition of the queue with its own reader/executor
// ═══════════════════════════════════════════════════════════════════════════════

pub struct QueueSlice {
    pub slice_id: u64,
    pub predicate: TaskPredicate,
    pub reader: QueueReader,
    pub pending_tasks: RwLock<VecDeque<Arc<RwLock<ExecutableTask>>>>,
    pub stats: QueueSliceStats,
}

#[derive(Debug, Default)]
pub struct QueueSliceStats {
    pub tasks_read: AtomicU64,
    pub tasks_executed: AtomicU64,
    pub tasks_completed: AtomicU64,
    pub tasks_failed: AtomicU64,
    pub tasks_nacked: AtomicU64,
    pub tasks_rescheduled: AtomicU64,
    pub total_latency_ms: AtomicU64,
    pub max_latency_ms: AtomicU64,
}

impl QueueSlice {
    pub fn new(slice_id: u64, predicate: TaskPredicate, reader: QueueReader) -> Self {
        Self {
            slice_id,
            predicate,
            reader,
            pending_tasks: RwLock::new(VecDeque::new()),
            stats: QueueSliceStats::default(),
        }
    }

    pub fn enqueue_task(&self, task: ExecutableTask) {
        let mut pending = self.pending_tasks.write().unwrap();
        pending.push_back(Arc::new(RwLock::new(task)));
        self.stats.tasks_read.fetch_add(1, Ordering::Relaxed);
    }

    pub fn next_task(&self) -> Option<Arc<RwLock<ExecutableTask>>> {
        let mut pending = self.pending_tasks.write().unwrap();
        pending.pop_front()
    }

    pub fn pending_count(&self) -> usize {
        self.pending_tasks.read().unwrap().len()
    }

    pub fn process_next(&self) -> Option<(TaskKey, String, ExecutableState, u32)> {
        let task_arc = self.next_task()?;
        let mut task = task_arc.write().unwrap();
        task.mark_executing();
        self.stats.tasks_executed.fetch_add(1, Ordering::Relaxed);
        task.mark_completed();
        self.stats.tasks_completed.fetch_add(1, Ordering::Relaxed);
        let latency = task.compute_latency_ms();
        self.stats
            .total_latency_ms
            .fetch_add(latency, Ordering::Relaxed);
        let max = self.stats.max_latency_ms.load(Ordering::Relaxed);
        if latency > max {
            self.stats.max_latency_ms.store(latency, Ordering::Relaxed);
        }
        Some((
            task.descriptor.key,
            task.descriptor.workflow_id.clone(),
            task.state,
            task.attempt,
        ))
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Queue Reader — reads tasks from persistence within a key range
// ═══════════════════════════════════════════════════════════════════════════════

pub struct QueueReader {
    pub reader_id: String,
    pub range: QueueRange,
    pub buffer: RwLock<VecDeque<QueueTaskDescriptor>>,
    pub stats: QueueReaderStats,
}

#[derive(Debug, Clone)]
pub struct QueueRange {
    pub inclusive_min: TaskKey,
    pub exclusive_max: TaskKey,
}

impl QueueRange {
    pub fn universal() -> Self {
        Self {
            inclusive_min: TaskKey::min(),
            exclusive_max: TaskKey::max(),
        }
    }

    pub fn new(min_fire: i64, min_id: i64, max_fire: i64, max_id: i64) -> Self {
        Self {
            inclusive_min: TaskKey::new(min_fire, min_id),
            exclusive_max: TaskKey::new(max_fire, max_id),
        }
    }

    pub fn contains(&self, key: &TaskKey) -> bool {
        key >= &self.inclusive_min && key < &self.exclusive_max
    }

    pub fn is_empty(&self) -> bool {
        self.inclusive_min >= self.exclusive_max
    }
}

#[derive(Debug, Default)]
pub struct QueueReaderStats {
    pub reads: AtomicU64,
    pub tasks_buffered: AtomicU64,
    pub pages_read: AtomicU64,
}

impl QueueReader {
    pub fn new(reader_id: &str, range: QueueRange) -> Self {
        Self {
            reader_id: reader_id.to_string(),
            range,
            buffer: RwLock::new(VecDeque::new()),
            stats: QueueReaderStats::default(),
        }
    }

    pub fn read_batch(&self, batch_size: usize) -> Vec<QueueTaskDescriptor> {
        let mut buffer = self.buffer.write().unwrap();
        let count = batch_size.min(buffer.len());
        let tasks: Vec<_> = buffer.drain(..count).collect();
        self.stats.reads.fetch_add(1, Ordering::Relaxed);
        self.stats.pages_read.fetch_add(1, Ordering::Relaxed);
        tasks
    }

    pub fn push_to_buffer(&self, tasks: Vec<QueueTaskDescriptor>) {
        let mut buffer = self.buffer.write().unwrap();
        let count = tasks.len() as u64;
        buffer.extend(tasks);
        self.stats
            .tasks_buffered
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn buffer_size(&self) -> usize {
        self.buffer.read().unwrap().len()
    }

    pub fn update_range(&mut self, new_range: QueueRange) {
        self.range = new_range;
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// DLQ Writer — dead letter queue for tasks that exceed retry limits
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct DlqRecord {
    pub record_id: String,
    pub task_descriptor: QueueTaskDescriptor,
    pub error_message: String,
    pub attempt: u32,
    pub enqueued_at: i64,
    pub source_queue: String,
    pub source_slice: u64,
}

pub struct DlqWriter {
    pub queue_name: String,
    pub records: RwLock<VecDeque<DlqRecord>>,
    pub next_id: AtomicU64,
    pub stats: DlqWriterStats,
}

#[derive(Debug, Default)]
pub struct DlqWriterStats {
    pub records_written: AtomicU64,
    pub records_read: AtomicU64,
    pub records_merged: AtomicU64,
    pub bytes_written: AtomicU64,
}

impl DlqWriter {
    pub fn new(queue_name: &str) -> Self {
        Self {
            queue_name: queue_name.to_string(),
            records: RwLock::new(VecDeque::new()),
            next_id: AtomicU64::new(1),
            stats: DlqWriterStats::default(),
        }
    }

    pub fn write(&self, task: &ExecutableTask) -> String {
        let id = format!("dlq-{}", self.next_id.fetch_add(1, Ordering::Relaxed));
        let record = DlqRecord {
            record_id: id.clone(),
            task_descriptor: task.descriptor.clone(),
            error_message: task.last_error.clone().unwrap_or_default(),
            attempt: task.attempt,
            enqueued_at: now_millis(),
            source_queue: self.queue_name.clone(),
            source_slice: task.segment_id,
        };
        self.records.write().unwrap().push_back(record);
        self.stats.records_written.fetch_add(1, Ordering::Relaxed);
        id
    }

    pub fn read_messages(&self, max_count: usize) -> Vec<DlqRecord> {
        let records = self.records.read().unwrap();
        let count = max_count.min(records.len());
        let result: Vec<_> = records.iter().take(count).cloned().collect();
        self.stats
            .records_read
            .fetch_add(count as u64, Ordering::Relaxed);
        result
    }

    pub fn merge_messages(&self, max_message_id: u64) -> usize {
        let mut records = self.records.write().unwrap();
        let before = records.len();
        records.retain(|r| {
            let id_num: u64 = r
                .record_id
                .strip_prefix("dlq-")
                .unwrap_or("0")
                .parse()
                .unwrap_or(0);
            id_num > max_message_id
        });
        let removed = before - records.len();
        self.stats
            .records_merged
            .fetch_add(removed as u64, Ordering::Relaxed);
        removed
    }

    pub fn purge_messages(&self) -> usize {
        let mut records = self.records.write().unwrap();
        let count = records.len();
        records.clear();
        self.stats
            .records_merged
            .fetch_add(count as u64, Ordering::Relaxed);
        count
    }

    pub fn pending_count(&self) -> usize {
        self.records.read().unwrap().len()
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Queue Grouper — groups tasks by namespace or task queue for batch processing
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GroupBy {
    Namespace,
    TaskQueue,
    WorkflowType,
    Priority,
}

pub struct QueueGrouper {
    pub group_by: GroupBy,
    pub groups: RwLock<HashMap<String, Vec<QueueTaskDescriptor>>>,
    pub stats: GrouperStats,
}

#[derive(Debug, Default)]
pub struct GrouperStats {
    pub groups_created: AtomicU64,
    pub tasks_grouped: AtomicU64,
}

impl QueueGrouper {
    pub fn new(group_by: GroupBy) -> Self {
        Self {
            group_by,
            groups: RwLock::new(HashMap::new()),
            stats: GrouperStats::default(),
        }
    }

    pub fn group_key(&self, task: &QueueTaskDescriptor) -> String {
        match self.group_by {
            GroupBy::Namespace => task.namespace_id.clone(),
            GroupBy::TaskQueue => task
                .task_queue
                .clone()
                .unwrap_or_else(|| "default".to_string()),
            GroupBy::WorkflowType => task
                .workflow_type
                .clone()
                .unwrap_or_else(|| "unknown".to_string()),
            GroupBy::Priority => format!("p{}", task.priority),
        }
    }

    pub fn add_task(&self, task: QueueTaskDescriptor) -> String {
        let key = self.group_key(&task);
        let mut groups = self.groups.write().unwrap();
        groups
            .entry(key.clone())
            .or_insert_with(Vec::new)
            .push(task);
        self.stats.tasks_grouped.fetch_add(1, Ordering::Relaxed);
        if groups.len() as u64 > self.stats.groups_created.load(Ordering::Relaxed) {
            self.stats
                .groups_created
                .store(groups.len() as u64, Ordering::Relaxed);
        }
        key
    }

    pub fn get_group(&self, key: &str) -> Vec<QueueTaskDescriptor> {
        self.groups
            .read()
            .unwrap()
            .get(key)
            .cloned()
            .unwrap_or_default()
    }

    pub fn group_count(&self) -> usize {
        self.groups.read().unwrap().len()
    }

    pub fn drain_group(&self, key: &str) -> Vec<QueueTaskDescriptor> {
        self.groups.write().unwrap().remove(key).unwrap_or_default()
    }

    pub fn all_group_keys(&self) -> Vec<String> {
        self.groups.read().unwrap().keys().cloned().collect()
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Queue Iterator — iterates over a queue range, yielding tasks in order
// ═══════════════════════════════════════════════════════════════════════════════

pub struct QueueIterator {
    pub range: QueueRange,
    pub current_key: TaskKey,
    pub buffer: VecDeque<QueueTaskDescriptor>,
    pub page_size: usize,
    pub exhausted: bool,
    pub stats: QueueIteratorStats,
}

#[derive(Debug, Default)]
pub struct QueueIteratorStats {
    pub pages_fetched: AtomicU64,
    pub tasks_yielded: AtomicU64,
    pub pages_empty: AtomicU64,
}

impl QueueIterator {
    pub fn new(range: QueueRange, page_size: usize) -> Self {
        Self {
            range: range.clone(),
            current_key: range.inclusive_min,
            buffer: VecDeque::new(),
            page_size,
            exhausted: false,
            stats: QueueIteratorStats::default(),
        }
    }

    pub fn push_page(&mut self, tasks: Vec<QueueTaskDescriptor>) {
        self.buffer.extend(tasks);
        self.stats.pages_fetched.fetch_add(1, Ordering::Relaxed);
    }

    pub fn next(&mut self) -> Option<QueueTaskDescriptor> {
        if let Some(task) = self.buffer.pop_front() {
            self.current_key = TaskKey::new(task.key.fire_time, task.key.task_id + 1);
            self.stats.tasks_yielded.fetch_add(1, Ordering::Relaxed);
            Some(task)
        } else if self.exhausted {
            None
        } else {
            self.stats.pages_empty.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    pub fn has_next(&self) -> bool {
        !self.buffer.is_empty() || !self.exhausted
    }

    pub fn mark_exhausted(&mut self) {
        self.exhausted = true;
    }

    pub fn current_position(&self) -> TaskKey {
        self.current_key
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Queue Action — operations that can be performed on a queue
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub enum QueueAction {
    MoveTasks {
        source_range: QueueRange,
        destination_range: QueueRange,
        predicate: TaskPredicate,
    },
    ResetReader {
        reader_id: String,
        reset_to: TaskKey,
    },
    ResizeSlice {
        slice_id: u64,
        new_range: QueueRange,
    },
    MergeSlices {
        source_slices: Vec<u64>,
        target_slice: u64,
    },
    PurgeDlq {
        queue_name: String,
        max_message_id: u64,
    },
    PauseQueue {
        queue_name: String,
    },
    ResumeQueue {
        queue_name: String,
    },
}

#[derive(Debug, Clone)]
pub enum ActionResult {
    Success {
        message: String,
        affected_tasks: u64,
    },
    Failure {
        error: String,
    },
    PartialSuccess {
        message: String,
        affected_tasks: u64,
        errors: Vec<String>,
    },
}

// ═══════════════════════════════════════════════════════════════════════════════
// Active/Standby Executor — executes tasks based on cluster active/standby state
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterRole {
    Active,
    Standby,
}

pub struct ActiveStandbyExecutor {
    pub role: RwLock<ClusterRole>,
    pub active_slices: RwLock<Vec<Arc<QueueSlice>>>,
    pub standby_slices: RwLock<Vec<Arc<QueueSlice>>>,
    pub dlq_writer: Arc<DlqWriter>,
    pub stats: ActiveStandbyStats,
    pub paused: AtomicBool,
}

#[derive(Debug, Default)]
pub struct ActiveStandbyStats {
    pub active_executions: AtomicU64,
    pub standby_deferrals: AtomicU64,
    pub role_transitions: AtomicU64,
    pub tasks_failed_to_dlq: AtomicU64,
}

impl ActiveStandbyExecutor {
    pub fn new(dlq_writer: Arc<DlqWriter>) -> Self {
        Self {
            role: RwLock::new(ClusterRole::Active),
            active_slices: RwLock::new(Vec::new()),
            standby_slices: RwLock::new(Vec::new()),
            dlq_writer,
            stats: ActiveStandbyStats::default(),
            paused: AtomicBool::new(false),
        }
    }

    pub fn transition_to(&self, new_role: ClusterRole) {
        let mut role = self.role.write().unwrap();
        if *role != new_role {
            *role = new_role;
            self.stats.role_transitions.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn add_active_slice(&self, slice: Arc<QueueSlice>) {
        self.active_slices.write().unwrap().push(slice);
    }

    pub fn add_standby_slice(&self, slice: Arc<QueueSlice>) {
        self.standby_slices.write().unwrap().push(slice);
    }

    pub fn execute_next(&self) -> Option<ActionResult> {
        if self.paused.load(Ordering::Relaxed) {
            return None;
        }
        let role = self.role.read().unwrap().clone();
        match role {
            ClusterRole::Active => {
                let slices = self.active_slices.read().unwrap();
                for slice in slices.iter() {
                    if let Some((key, _wf_id, _state, _attempt)) = slice.process_next() {
                        self.stats.active_executions.fetch_add(1, Ordering::Relaxed);
                        return Some(ActionResult::Success {
                            message: format!("Task {} completed", key.task_id),
                            affected_tasks: 1,
                        });
                    }
                }
                None
            }
            ClusterRole::Standby => {
                self.stats.standby_deferrals.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    pub fn nack_to_dlq(&self, task: &ExecutableTask) -> String {
        let id = self.dlq_writer.write(task);
        self.stats
            .tasks_failed_to_dlq
            .fetch_add(1, Ordering::Relaxed);
        id
    }

    pub fn pause(&self) {
        self.paused.store(true, Ordering::Relaxed);
    }
    pub fn resume(&self) {
        self.paused.store(false, Ordering::Relaxed);
    }
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Relaxed)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Queue Alerts — monitoring and alerting for queue health
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

#[derive(Debug, Clone)]
pub struct QueueAlert {
    pub alert_id: String,
    pub severity: AlertSeverity,
    pub queue_name: String,
    pub message: String,
    pub created_at: i64,
    pub metric_name: String,
    pub metric_value: f64,
    pub threshold: f64,
}

pub struct QueueAlertManager {
    pub alerts: RwLock<VecDeque<QueueAlert>>,
    pub next_id: AtomicU64,
    pub thresholds: RwLock<AlertThresholds>,
    pub stats: AlertManagerStats,
}

#[derive(Debug, Clone)]
pub struct AlertThresholds {
    pub max_pending_tasks: u64,
    pub max_latency_ms: u64,
    pub max_failures_per_minute: u64,
    pub max_dlq_depth: u64,
}

impl Default for AlertThresholds {
    fn default() -> Self {
        Self {
            max_pending_tasks: 100_000,
            max_latency_ms: 30_000,
            max_failures_per_minute: 100,
            max_dlq_depth: 10_000,
        }
    }
}

#[derive(Debug, Default)]
pub struct AlertManagerStats {
    pub alerts_generated: AtomicU64,
    pub alerts_acknowledged: AtomicU64,
    pub info_alerts: AtomicU64,
    pub warning_alerts: AtomicU64,
    pub critical_alerts: AtomicU64,
}

impl QueueAlertManager {
    pub fn new() -> Self {
        Self {
            alerts: RwLock::new(VecDeque::new()),
            next_id: AtomicU64::new(1),
            thresholds: RwLock::new(AlertThresholds::default()),
            stats: AlertManagerStats::default(),
        }
    }

    pub fn check_and_alert(
        &self,
        queue_name: &str,
        pending: u64,
        latency_ms: u64,
        failures: u64,
        dlq_depth: u64,
    ) -> Vec<QueueAlert> {
        let thresholds = self.thresholds.read().unwrap();
        let mut new_alerts = Vec::new();
        if pending > thresholds.max_pending_tasks {
            new_alerts.push(self.create_alert(
                queue_name,
                AlertSeverity::Critical,
                "Pending tasks exceeded",
                "pending_tasks",
                pending as f64,
                thresholds.max_pending_tasks as f64,
            ));
        }
        if latency_ms > thresholds.max_latency_ms {
            new_alerts.push(self.create_alert(
                queue_name,
                AlertSeverity::Warning,
                "Latency exceeded",
                "latency_ms",
                latency_ms as f64,
                thresholds.max_latency_ms as f64,
            ));
        }
        if failures > thresholds.max_failures_per_minute {
            new_alerts.push(self.create_alert(
                queue_name,
                AlertSeverity::Warning,
                "Failure rate exceeded",
                "failures_per_min",
                failures as f64,
                thresholds.max_failures_per_minute as f64,
            ));
        }
        if dlq_depth > thresholds.max_dlq_depth {
            new_alerts.push(self.create_alert(
                queue_name,
                AlertSeverity::Critical,
                "DLQ depth exceeded",
                "dlq_depth",
                dlq_depth as f64,
                thresholds.max_dlq_depth as f64,
            ));
        }
        new_alerts
    }

    fn create_alert(
        &self,
        queue_name: &str,
        severity: AlertSeverity,
        message: &str,
        metric: &str,
        value: f64,
        threshold: f64,
    ) -> QueueAlert {
        let id = format!("alert-{}", self.next_id.fetch_add(1, Ordering::Relaxed));
        let alert = QueueAlert {
            alert_id: id,
            severity,
            queue_name: queue_name.to_string(),
            message: message.to_string(),
            created_at: now_millis(),
            metric_name: metric.to_string(),
            metric_value: value,
            threshold,
        };
        match severity {
            AlertSeverity::Info => {
                self.stats.info_alerts.fetch_add(1, Ordering::Relaxed);
            }
            AlertSeverity::Warning => {
                self.stats.warning_alerts.fetch_add(1, Ordering::Relaxed);
            }
            AlertSeverity::Critical => {
                self.stats.critical_alerts.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.stats.alerts_generated.fetch_add(1, Ordering::Relaxed);
        self.alerts.write().unwrap().push_back(alert.clone());
        alert
    }

    pub fn active_alerts(&self) -> Vec<QueueAlert> {
        self.alerts.read().unwrap().iter().cloned().collect()
    }
    pub fn update_thresholds(&self, thresholds: AlertThresholds) {
        *self.thresholds.write().unwrap() = thresholds;
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Queue Monitor — aggregates stats from all slices and generates health reports
// ═══════════════════════════════════════════════════════════════════════════════

pub struct QueueMonitor {
    pub queue_name: String,
    pub slices: RwLock<Vec<Arc<QueueSlice>>>,
    pub dlq_writer: Arc<DlqWriter>,
    pub alert_manager: Arc<QueueAlertManager>,
    pub stats: QueueMonitorStats,
}

#[derive(Debug, Default)]
pub struct QueueMonitorStats {
    pub health_checks: AtomicU64,
    pub total_tasks_processed: AtomicU64,
    pub total_latency_ms: AtomicU64,
}

#[derive(Debug)]
pub struct QueueHealthReport {
    pub queue_name: String,
    pub total_pending: usize,
    pub total_processed: u64,
    pub total_failed: u64,
    pub total_nacked: u64,
    pub dlq_depth: usize,
    pub avg_latency_ms: f64,
    pub max_latency_ms: u64,
    pub slice_count: usize,
    pub active_alerts: usize,
    pub healthy: bool,
}

impl QueueMonitor {
    pub fn new(
        queue_name: &str,
        dlq_writer: Arc<DlqWriter>,
        alert_manager: Arc<QueueAlertManager>,
    ) -> Self {
        Self {
            queue_name: queue_name.to_string(),
            slices: RwLock::new(Vec::new()),
            dlq_writer,
            alert_manager,
            stats: QueueMonitorStats::default(),
        }
    }

    pub fn add_slice(&self, slice: Arc<QueueSlice>) {
        self.slices.write().unwrap().push(slice);
    }

    pub fn generate_health_report(&self) -> QueueHealthReport {
        let slices = self.slices.read().unwrap();
        let mut total_pending = 0;
        let mut total_processed = 0u64;
        let mut total_failed = 0u64;
        let mut total_nacked = 0u64;
        let mut total_latency = 0u64;
        let mut max_latency = 0u64;
        for slice in slices.iter() {
            total_pending += slice.pending_count();
            total_processed += slice.stats.tasks_completed.load(Ordering::Relaxed);
            total_failed += slice.stats.tasks_failed.load(Ordering::Relaxed);
            total_nacked += slice.stats.tasks_nacked.load(Ordering::Relaxed);
            total_latency += slice.stats.total_latency_ms.load(Ordering::Relaxed);
            let slice_max = slice.stats.max_latency_ms.load(Ordering::Relaxed);
            if slice_max > max_latency {
                max_latency = slice_max;
            }
        }
        let avg_latency = if total_processed > 0 {
            total_latency as f64 / total_processed as f64
        } else {
            0.0
        };
        let dlq_depth = self.dlq_writer.pending_count();
        let active_alerts = self.alert_manager.active_alerts().len();
        let healthy = total_pending < 100_000 && dlq_depth < 10_000 && active_alerts == 0;
        self.stats.health_checks.fetch_add(1, Ordering::Relaxed);
        self.stats
            .total_tasks_processed
            .store(total_processed, Ordering::Relaxed);
        self.stats
            .total_latency_ms
            .store(total_latency, Ordering::Relaxed);
        QueueHealthReport {
            queue_name: self.queue_name.clone(),
            total_pending,
            total_processed,
            total_failed,
            total_nacked,
            dlq_depth,
            avg_latency_ms: avg_latency,
            max_latency_ms: max_latency,
            slice_count: slices.len(),
            active_alerts,
            healthy,
        }
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

    fn make_descriptor(id: i64, ns: &str) -> QueueTaskDescriptor {
        QueueTaskDescriptor {
            key: TaskKey::new(1000, id),
            namespace_id: ns.to_string(),
            workflow_id: format!("wf-{}", id),
            run_id: format!("run-{}", id),
            task_type: "transfer".to_string(),
            task_queue: Some("tq-1".to_string()),
            workflow_type: Some("TestWorkflow".to_string()),
            priority: 5,
            version: 1,
            visibility_time: 1000,
            created_at: 999,
        }
    }

    #[test]
    fn test_task_predicate_matching() {
        let desc = make_descriptor(1, "ns-1");
        assert!(TaskPredicate::Universal.matches(&desc));
        assert!(TaskPredicate::Namespace("ns-1".into()).matches(&desc));
        assert!(!TaskPredicate::Namespace("ns-2".into()).matches(&desc));
        assert!(TaskPredicate::TaskQueue("tq-1".into()).matches(&desc));
        let and_pred = TaskPredicate::And(
            Box::new(TaskPredicate::Namespace("ns-1".into())),
            Box::new(TaskPredicate::TaskQueue("tq-1".into())),
        );
        assert!(and_pred.matches(&desc));
        let or_pred = TaskPredicate::Or(
            Box::new(TaskPredicate::Namespace("ns-99".into())),
            Box::new(TaskPredicate::Namespace("ns-1".into())),
        );
        assert!(or_pred.matches(&desc));
        let not_pred = TaskPredicate::Not(Box::new(TaskPredicate::Namespace("ns-1".into())));
        assert!(!not_pred.matches(&desc));
    }

    #[test]
    fn test_executable_task_lifecycle() {
        let mut task = ExecutableTask::new(make_descriptor(1, "ns"));
        assert_eq!(task.state, ExecutableState::Initialized);
        task.mark_loaded();
        assert_eq!(task.state, ExecutableState::Loaded);
        task.mark_executing();
        assert_eq!(task.state, ExecutableState::Executing);
        assert_eq!(task.attempt, 1);
        task.mark_completed();
        assert_eq!(task.state, ExecutableState::Completed);
        assert!(task.is_terminal());
    }

    #[test]
    fn test_executable_task_retry() {
        let mut task = ExecutableTask::new(make_descriptor(1, "ns"));
        task.max_attempts = 3;
        task.mark_executing();
        task.mark_failed("err1".into());
        assert!(task.should_retry());
        task.mark_executing();
        task.mark_failed("err2".into());
        assert!(task.should_retry());
        task.mark_executing();
        task.mark_failed("err3".into());
        assert!(!task.should_retry());
        assert_eq!(task.state, ExecutableState::Nackd);
    }

    #[test]
    fn test_queue_range() {
        let range = QueueRange::new(100, 1, 200, 1);
        assert!(range.contains(&TaskKey::new(150, 5)));
        assert!(!range.contains(&TaskKey::new(250, 1)));
        assert!(!range.contains(&TaskKey::new(50, 1)));
        let universal = QueueRange::universal();
        assert!(universal.contains(&TaskKey::new(0, 0)));
    }

    #[test]
    fn test_queue_reader() {
        let reader = QueueReader::new("r1", QueueRange::universal());
        let tasks = vec![make_descriptor(1, "ns"), make_descriptor(2, "ns")];
        reader.push_to_buffer(tasks);
        assert_eq!(reader.buffer_size(), 2);
        let batch = reader.read_batch(1);
        assert_eq!(batch.len(), 1);
        assert_eq!(reader.buffer_size(), 1);
    }

    #[test]
    fn test_queue_slice() {
        let reader = QueueReader::new("r1", QueueRange::universal());
        let slice = QueueSlice::new(1, TaskPredicate::Universal, reader);
        slice.enqueue_task(ExecutableTask::new(make_descriptor(1, "ns")));
        slice.enqueue_task(ExecutableTask::new(make_descriptor(2, "ns")));
        assert_eq!(slice.pending_count(), 2);
        let result = slice.process_next();
        assert!(result.is_some());
        assert_eq!(slice.pending_count(), 1);
        assert_eq!(slice.stats.tasks_completed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_dlq_writer() {
        let dlq = DlqWriter::new("transfer-queue");
        let mut task = ExecutableTask::new(make_descriptor(1, "ns"));
        task.mark_executing();
        task.mark_failed("processing error".into());
        let id = dlq.write(&task);
        assert!(!id.is_empty());
        assert_eq!(dlq.pending_count(), 1);
        let records = dlq.read_messages(10);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].error_message, "processing error");
    }

    #[test]
    fn test_dlq_merge_and_purge() {
        let dlq = DlqWriter::new("q");
        for i in 1..=5 {
            let mut task = ExecutableTask::new(make_descriptor(i, "ns"));
            task.mark_executing();
            task.mark_failed("err".into());
            dlq.write(&task);
        }
        assert_eq!(dlq.pending_count(), 5);
        let merged = dlq.merge_messages(3);
        assert_eq!(merged, 3);
        assert_eq!(dlq.pending_count(), 2);
        let purged = dlq.purge_messages();
        assert_eq!(purged, 2);
        assert_eq!(dlq.pending_count(), 0);
    }

    #[test]
    fn test_queue_grouper() {
        let grouper = QueueGrouper::new(GroupBy::Namespace);
        grouper.add_task(make_descriptor(1, "ns-a"));
        grouper.add_task(make_descriptor(2, "ns-a"));
        grouper.add_task(make_descriptor(3, "ns-b"));
        assert_eq!(grouper.group_count(), 2);
        assert_eq!(grouper.get_group("ns-a").len(), 2);
        assert_eq!(grouper.get_group("ns-b").len(), 1);
        let drained = grouper.drain_group("ns-a");
        assert_eq!(drained.len(), 2);
        assert_eq!(grouper.group_count(), 1);
    }

    #[test]
    fn test_queue_iterator() {
        let range = QueueRange::new(0, 0, 1000, 1000);
        let mut iter = QueueIterator::new(range, 10);
        iter.push_page(vec![make_descriptor(1, "ns"), make_descriptor(2, "ns")]);
        assert!(iter.has_next());
        let t1 = iter.next().unwrap();
        assert_eq!(t1.key.task_id, 1);
        let t2 = iter.next().unwrap();
        assert_eq!(t2.key.task_id, 2);
        assert!(iter.next().is_none());
        iter.mark_exhausted();
        assert!(!iter.has_next());
    }

    #[test]
    fn test_active_standby_executor() {
        let dlq = Arc::new(DlqWriter::new("q"));
        let executor = ActiveStandbyExecutor::new(dlq);
        let reader = QueueReader::new("r", QueueRange::universal());
        let slice = Arc::new(QueueSlice::new(1, TaskPredicate::Universal, reader));
        slice.enqueue_task(ExecutableTask::new(make_descriptor(1, "ns")));
        executor.add_active_slice(slice);
        assert_eq!(*executor.role.read().unwrap(), ClusterRole::Active);
        let result = executor.execute_next();
        assert!(result.is_some());
        executor.transition_to(ClusterRole::Standby);
        assert_eq!(*executor.role.read().unwrap(), ClusterRole::Standby);
        assert!(executor.execute_next().is_none());
    }

    #[test]
    fn test_active_standby_pause_resume() {
        let dlq = Arc::new(DlqWriter::new("q"));
        let executor = ActiveStandbyExecutor::new(dlq);
        assert!(!executor.is_paused());
        executor.pause();
        assert!(executor.is_paused());
        assert!(executor.execute_next().is_none());
        executor.resume();
        assert!(!executor.is_paused());
    }

    #[test]
    fn test_queue_alert_manager() {
        let mgr = QueueAlertManager::new();
        let alerts = mgr.check_and_alert("q1", 200_000, 5000, 10, 100);
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].severity, AlertSeverity::Critical);
        let alerts2 = mgr.check_and_alert("q2", 1000, 60_000, 200, 100);
        assert_eq!(alerts2.len(), 2);
        assert_eq!(mgr.active_alerts().len(), 3);
    }

    #[test]
    fn test_queue_monitor_health_report() {
        let dlq = Arc::new(DlqWriter::new("q"));
        let alerts = Arc::new(QueueAlertManager::new());
        let monitor = QueueMonitor::new("transfer", dlq.clone(), alerts);
        let reader = QueueReader::new("r", QueueRange::universal());
        let slice = Arc::new(QueueSlice::new(1, TaskPredicate::Universal, reader));
        for i in 0..5 {
            slice.enqueue_task(ExecutableTask::new(make_descriptor(i, "ns")));
        }
        for _ in 0..3 {
            slice.process_next();
        }
        monitor.add_slice(slice);
        let report = monitor.generate_health_report();
        assert_eq!(report.queue_name, "transfer");
        assert_eq!(report.total_pending, 2);
        assert_eq!(report.total_processed, 3);
        assert_eq!(report.slice_count, 1);
        assert!(report.healthy);
    }

    #[test]
    fn test_task_key_ordering() {
        let k1 = TaskKey::new(100, 1);
        let k2 = TaskKey::new(100, 2);
        let k3 = TaskKey::new(200, 1);
        assert!(k1 < k2);
        assert!(k2 < k3);
        assert_eq!(TaskKey::min(), TaskKey::min());
        assert!(TaskKey::min() < TaskKey::max());
    }

    #[test]
    fn test_grouper_by_task_queue() {
        let grouper = QueueGrouper::new(GroupBy::TaskQueue);
        let mut d1 = make_descriptor(1, "ns");
        d1.task_queue = Some("tq-a".into());
        let mut d2 = make_descriptor(2, "ns");
        d2.task_queue = Some("tq-b".into());
        grouper.add_task(d1);
        grouper.add_task(d2);
        assert_eq!(grouper.group_count(), 2);
        assert_eq!(grouper.get_group("tq-a").len(), 1);
    }
}
