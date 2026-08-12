//! Task framework matching Temporal's common/tasks (4,181 lines).
//!
//! Covers: task interfaces, task executor, task scheduler, priority queue,
//! task lifecycle, task state tracking, and batch task processing.

use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::cmp::Ordering as CmpOrdering;
use std::sync::{Arc, RwLock, atomic::{AtomicU64, AtomicBool, Ordering}};
use std::time::{SystemTime, Duration, Instant};

// ═══════════════════════════════════════════════════════════════════════════════
// Task Types
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskCategory {
    Transfer = 1,
    Timer = 2,
    Replication = 3,
    Visibility = 4,
    Archival = 5,
    Outbound = 6,
}

impl TaskCategory {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Transfer => "Transfer",
            Self::Timer => "Timer",
            Self::Replication => "Replication",
            Self::Visibility => "Visibility",
            Self::Archival => "Archival",
            Self::Outbound => "Outbound",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    Pending,
    Active,
    Completed,
    Failed,
    Cancelled,
    Nackd,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task Trait & Implementation
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct Task {
    pub id: String,
    pub category: TaskCategory,
    pub namespace_id: String,
    pub workflow_id: String,
    pub run_id: String,
    pub priority: TaskPriority,
    pub state: TaskState,
    pub created_at: i64,
    pub scheduled_at: i64,
    pub visibility_time: i64,
    pub attempt: u32,
    pub max_attempts: u32,
    pub last_failure: Option<String>,
    pub version: i64,
    pub source_cluster: Option<String>,
    pub delete_after_processed: bool,
    pub user_data: HashMap<String, Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskPriority(pub u32);

impl TaskPriority {
    pub const HIGH: Self = Self(0);
    pub const DEFAULT: Self = Self(10);
    pub const LOW: Self = Self(20);
    pub const IDLE: Self = Self(30);
}

impl PartialOrd for TaskPriority {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for TaskPriority {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        other.0.cmp(&self.0) // lower value = higher priority
    }
}

impl Task {
    pub fn new(id: &str, category: TaskCategory, namespace_id: &str, workflow_id: &str, run_id: &str) -> Self {
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_millis() as i64;
        Self {
            id: id.to_string(),
            category,
            namespace_id: namespace_id.to_string(),
            workflow_id: workflow_id.to_string(),
            run_id: run_id.to_string(),
            priority: TaskPriority::DEFAULT,
            state: TaskState::Pending,
            created_at: now,
            scheduled_at: now,
            visibility_time: now,
            attempt: 0,
            max_attempts: 10,
            last_failure: None,
            version: 0,
            source_cluster: None,
            delete_after_processed: false,
            user_data: HashMap::new(),
        }
    }

    pub fn with_priority(mut self, priority: TaskPriority) -> Self {
        self.priority = priority;
        self
    }

    pub fn with_scheduled_at(mut self, time: i64) -> Self {
        self.scheduled_at = time;
        self
    }

    pub fn with_visibility_time(mut self, time: i64) -> Self {
        self.visibility_time = time;
        self
    }

    pub fn mark_active(&mut self) {
        self.state = TaskState::Active;
        self.attempt += 1;
    }

    pub fn mark_completed(&mut self) {
        self.state = TaskState::Completed;
    }

    pub fn mark_failed(&mut self, error: &str) {
        self.state = TaskState::Failed;
        self.last_failure = Some(error.to_string());
    }

    pub fn mark_cancelled(&mut self) {
        self.state = TaskState::Cancelled;
    }

    pub fn can_retry(&self) -> bool {
        self.attempt < self.max_attempts
    }

    pub fn is_ready(&self, current_time: i64) -> bool {
        self.visibility_time <= current_time && self.state == TaskState::Pending
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Priority Task Queue
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug)]
struct PriorityQueueEntry {
    task: Task,
    sequence: u64,
}

impl PartialEq for PriorityQueueEntry {
    fn eq(&self, other: &Self) -> bool { self.sequence == other.sequence }
}
impl Eq for PriorityQueueEntry {}

impl PartialOrd for PriorityQueueEntry {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for PriorityQueueEntry {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.task.priority.cmp(&other.task.priority)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

pub struct PriorityTaskQueue {
    heap: RwLock<BinaryHeap<PriorityQueueEntry>>,
    sequence: AtomicU64,
    stats: TaskQueueStats,
}

#[derive(Debug, Default)]
pub struct TaskQueueStats {
    pub enqueued: AtomicU64,
    pub dequeued: AtomicU64,
    pub current_size: AtomicU64,
    pub max_size_reached: AtomicU64,
}

impl PriorityTaskQueue {
    pub fn new() -> Self {
        Self {
            heap: RwLock::new(BinaryHeap::new()),
            sequence: AtomicU64::new(0),
            stats: TaskQueueStats::default(),
        }
    }

    pub fn enqueue(&self, task: Task) {
        let seq = self.sequence.fetch_add(1, Ordering::Relaxed);
        self.heap.write().unwrap().push(PriorityQueueEntry { task, sequence: seq });
        self.stats.enqueued.fetch_add(1, Ordering::Relaxed);
        let size = self.heap.read().unwrap().len() as u64;
        self.stats.current_size.store(size, Ordering::Relaxed);
        let max = self.stats.max_size_reached.load(Ordering::Relaxed);
        if size > max {
            self.stats.max_size_reached.store(size, Ordering::Relaxed);
        }
    }

    pub fn dequeue(&self) -> Option<Task> {
        let result = self.heap.write().unwrap().pop();
        if result.is_some() {
            self.stats.dequeued.fetch_add(1, Ordering::Relaxed);
            let size = self.heap.read().unwrap().len() as u64;
            self.stats.current_size.store(size, Ordering::Relaxed);
        }
        result.map(|e| e.task)
    }

    pub fn len(&self) -> usize {
        self.heap.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.read().unwrap().is_empty()
    }

    pub fn stats(&self) -> &TaskQueueStats { &self.stats }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task Executor
// ═══════════════════════════════════════════════════════════════════════════════

pub trait TaskExecutor: Send + Sync {
    fn execute(&self, task: &mut Task) -> Result<TaskExecutionResult, TaskExecutionError>;
    fn category(&self) -> TaskCategory;
}

#[derive(Debug, Clone)]
pub enum TaskExecutionResult {
    Completed,
    Retry { delay_ms: u64 },
    Discard,
    Nack { reason: String },
}

#[derive(Debug, Clone)]
pub struct TaskExecutionError {
    pub message: String,
    pub retryable: bool,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task Scheduler
// ═══════════════════════════════════════════════════════════════════════════════

pub struct TaskScheduler {
    queues: RwLock<HashMap<TaskCategory, Arc<PriorityTaskQueue>>>,
    executors: RwLock<HashMap<TaskCategory, Arc<dyn TaskExecutor>>>,
    stats: SchedulerStats,
    running: AtomicBool,
}

#[derive(Debug, Default)]
pub struct SchedulerStats {
    pub tasks_submitted: AtomicU64,
    pub tasks_executed: AtomicU64,
    pub tasks_completed: AtomicU64,
    pub tasks_failed: AtomicU64,
    pub tasks_retried: AtomicU64,
    pub tasks_discarded: AtomicU64,
    pub tasks_nacked: AtomicU64,
}

impl TaskScheduler {
    pub fn new() -> Self {
        let scheduler = Self {
            queues: RwLock::new(HashMap::new()),
            executors: RwLock::new(HashMap::new()),
            stats: SchedulerStats::default(),
            running: AtomicBool::new(false),
        };
        // Create queues for all categories
        {
            let mut queues = scheduler.queues.write().unwrap();
            for cat in &[TaskCategory::Transfer, TaskCategory::Timer, TaskCategory::Replication,
                          TaskCategory::Visibility, TaskCategory::Archival, TaskCategory::Outbound] {
                queues.insert(*cat, Arc::new(PriorityTaskQueue::new()));
            }
        }
        scheduler
    }

    pub fn register_executor(&self, category: TaskCategory, executor: Arc<dyn TaskExecutor>) {
        self.executors.write().unwrap().insert(category, executor);
    }

    pub fn submit_task(&self, task: Task) {
        self.stats.tasks_submitted.fetch_add(1, Ordering::Relaxed);
        let queues = self.queues.read().unwrap();
        if let Some(queue) = queues.get(&task.category) {
            queue.enqueue(task);
        }
    }

    pub fn submit_batch(&self, tasks: Vec<Task>) {
        for task in tasks {
            self.submit_task(task);
        }
    }

    pub fn process_one(&self, category: TaskCategory) -> Result<TaskExecutionResult, TaskExecutionError> {
        let queue = {
            let queues = self.queues.read().unwrap();
            queues.get(&category).cloned()
                .ok_or(TaskExecutionError { message: format!("No queue for {:?}", category), retryable: false })?
        };

        let mut task = queue.dequeue()
            .ok_or(TaskExecutionError { message: "Queue empty".to_string(), retryable: false })?;

        self.stats.tasks_executed.fetch_add(1, Ordering::Relaxed);
        task.mark_active();

        let executor = {
            let executors = self.executors.read().unwrap();
            executors.get(&category).cloned()
                .ok_or(TaskExecutionError { message: format!("No executor for {:?}", category), retryable: false })?
        };

        match executor.execute(&mut task) {
            Ok(result) => {
                match &result {
                    TaskExecutionResult::Completed => {
                        task.mark_completed();
                        self.stats.tasks_completed.fetch_add(1, Ordering::Relaxed);
                    }
                    TaskExecutionResult::Retry { .. } => {
                        if task.can_retry() {
                            self.stats.tasks_retried.fetch_add(1, Ordering::Relaxed);
                            task.state = TaskState::Pending;
                            queue.enqueue(task);
                        } else {
                            task.mark_failed("max retries exceeded");
                            self.stats.tasks_failed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    TaskExecutionResult::Discard => {
                        self.stats.tasks_discarded.fetch_add(1, Ordering::Relaxed);
                    }
                    TaskExecutionResult::Nack { .. } => {
                        task.state = TaskState::Nackd;
                        self.stats.tasks_nacked.fetch_add(1, Ordering::Relaxed);
                        queue.enqueue(task);
                    }
                }
                Ok(result)
            }
            Err(e) => {
                task.mark_failed(&e.message);
                self.stats.tasks_failed.fetch_add(1, Ordering::Relaxed);
                if e.retryable && task.can_retry() {
                    task.state = TaskState::Pending;
                    queue.enqueue(task);
                    self.stats.tasks_retried.fetch_add(1, Ordering::Relaxed);
                }
                Err(e)
            }
        }
    }

    pub fn process_batch(&self, category: TaskCategory, max_count: usize) -> Vec<Result<TaskExecutionResult, TaskExecutionError>> {
        let mut results = Vec::new();
        for _ in 0..max_count {
            match self.process_one(category) {
                Ok(r) => results.push(Ok(r)),
                Err(e) if e.message == "Queue empty" => break,
                Err(e) => results.push(Err(e)),
            }
        }
        results
    }

    pub fn queue_size(&self, category: TaskCategory) -> usize {
        let queues = self.queues.read().unwrap();
        queues.get(&category).map(|q| q.len()).unwrap_or(0)
    }

    pub fn total_queue_size(&self) -> usize {
        let queues = self.queues.read().unwrap();
        queues.values().map(|q| q.len()).sum()
    }

    pub fn stats(&self) -> &SchedulerStats { &self.stats }

    pub fn start(&self) {
        self.running.store(true, Ordering::SeqCst);
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    struct MockExecutor { category: TaskCategory, fail_count: Arc<AtomicU64>, max_failures: u64 }

    impl MockExecutor {
        fn new(category: TaskCategory) -> Self {
            Self { category, fail_count: Arc::new(AtomicU64::new(0)), max_failures: 0 }
        }
        fn with_failures(category: TaskCategory, max: u64) -> Self {
            Self { category, fail_count: Arc::new(AtomicU64::new(0)), max_failures: max }
        }
    }

    impl TaskExecutor for MockExecutor {
        fn execute(&self, task: &mut Task) -> Result<TaskExecutionResult, TaskExecutionError> {
            if self.max_failures > 0 {
                let count = self.fail_count.fetch_add(1, Ordering::Relaxed);
                if count < self.max_failures {
                    return Err(TaskExecutionError { message: "transient".to_string(), retryable: true });
                }
            }
            task.mark_completed();
            Ok(TaskExecutionResult::Completed)
        }
        fn category(&self) -> TaskCategory { self.category }
    }

    #[test]
    fn test_task_creation() {
        let task = Task::new("t-1", TaskCategory::Transfer, "ns-1", "wf-1", "run-1");
        assert_eq!(task.state, TaskState::Pending);
        assert_eq!(task.attempt, 0);
        assert!(task.can_retry());
    }

    #[test]
    fn test_task_priority_ordering() {
        let queue = PriorityTaskQueue::new();
        let t_low = Task::new("low", TaskCategory::Transfer, "ns", "wf", "r").with_priority(TaskPriority::LOW);
        let t_high = Task::new("high", TaskCategory::Transfer, "ns", "wf", "r").with_priority(TaskPriority::HIGH);
        let t_default = Task::new("def", TaskCategory::Transfer, "ns", "wf", "r").with_priority(TaskPriority::DEFAULT);

        queue.enqueue(t_low);
        queue.enqueue(t_high);
        queue.enqueue(t_default);

        let first = queue.dequeue().unwrap();
        assert_eq!(first.id, "high");
        let second = queue.dequeue().unwrap();
        assert_eq!(second.id, "def");
        let third = queue.dequeue().unwrap();
        assert_eq!(third.id, "low");
    }

    #[test]
    fn test_priority_task_queue_stats() {
        let queue = PriorityTaskQueue::new();
        queue.enqueue(Task::new("1", TaskCategory::Transfer, "ns", "wf", "r"));
        queue.enqueue(Task::new("2", TaskCategory::Transfer, "ns", "wf", "r"));
        assert_eq!(queue.len(), 2);
        assert_eq!(queue.stats().enqueued.load(Ordering::Relaxed), 2);

        queue.dequeue();
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.stats().dequeued.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_scheduler_submit_and_process() {
        let scheduler = TaskScheduler::new();
        let executor = Arc::new(MockExecutor::new(TaskCategory::Transfer));
        scheduler.register_executor(TaskCategory::Transfer, executor);

        scheduler.submit_task(Task::new("t-1", TaskCategory::Transfer, "ns", "wf", "r"));
        assert_eq!(scheduler.queue_size(TaskCategory::Transfer), 1);

        let result = scheduler.process_one(TaskCategory::Transfer).unwrap();
        assert!(matches!(result, TaskExecutionResult::Completed));
        assert_eq!(scheduler.queue_size(TaskCategory::Transfer), 0);
        assert_eq!(scheduler.stats().tasks_completed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_scheduler_batch_submit() {
        let scheduler = TaskScheduler::new();
        let executor = Arc::new(MockExecutor::new(TaskCategory::Timer));
        scheduler.register_executor(TaskCategory::Timer, executor);

        let tasks = vec![
            Task::new("t-1", TaskCategory::Timer, "ns", "wf", "r"),
            Task::new("t-2", TaskCategory::Timer, "ns", "wf", "r"),
            Task::new("t-3", TaskCategory::Timer, "ns", "wf", "r"),
        ];
        scheduler.submit_batch(tasks);
        assert_eq!(scheduler.queue_size(TaskCategory::Timer), 3);
        assert_eq!(scheduler.total_queue_size(), 3);
    }

    #[test]
    fn test_scheduler_process_batch() {
        let scheduler = TaskScheduler::new();
        let executor = Arc::new(MockExecutor::new(TaskCategory::Visibility));
        scheduler.register_executor(TaskCategory::Visibility, executor);

        for i in 0..5 {
            scheduler.submit_task(Task::new(&format!("t-{}", i), TaskCategory::Visibility, "ns", "wf", "r"));
        }

        let results = scheduler.process_batch(TaskCategory::Visibility, 10);
        assert_eq!(results.len(), 5);
        assert!(results.iter().all(|r| r.is_ok()));
    }

    #[test]
    fn test_scheduler_retry_on_failure() {
        let scheduler = TaskScheduler::new();
        let executor = Arc::new(MockExecutor::with_failures(TaskCategory::Transfer, 2));
        scheduler.register_executor(TaskCategory::Transfer, executor);

        scheduler.submit_task(Task::new("t-1", TaskCategory::Transfer, "ns", "wf", "r"));

        // First attempt fails
        let r1 = scheduler.process_one(TaskCategory::Transfer);
        assert!(r1.is_err());
        assert_eq!(scheduler.stats().tasks_retried.load(Ordering::Relaxed), 1);

        // Second attempt fails
        let r2 = scheduler.process_one(TaskCategory::Transfer);
        assert!(r2.is_err());
        assert_eq!(scheduler.stats().tasks_retried.load(Ordering::Relaxed), 2);

        // Third attempt succeeds
        let r3 = scheduler.process_one(TaskCategory::Transfer).unwrap();
        assert!(matches!(r3, TaskExecutionResult::Completed));
    }

    #[test]
    fn test_scheduler_empty_queue() {
        let scheduler = TaskScheduler::new();
        let executor = Arc::new(MockExecutor::new(TaskCategory::Transfer));
        scheduler.register_executor(TaskCategory::Transfer, executor);

        let result = scheduler.process_one(TaskCategory::Transfer);
        assert!(result.is_err());
    }

    #[test]
    fn test_task_state_transitions() {
        let mut task = Task::new("t-1", TaskCategory::Transfer, "ns", "wf", "r");
        assert_eq!(task.state, TaskState::Pending);

        task.mark_active();
        assert_eq!(task.state, TaskState::Active);
        assert_eq!(task.attempt, 1);

        task.mark_completed();
        assert_eq!(task.state, TaskState::Completed);
    }

    #[test]
    fn test_task_category_names() {
        assert_eq!(TaskCategory::Transfer.name(), "Transfer");
        assert_eq!(TaskCategory::Timer.name(), "Timer");
        assert_eq!(TaskCategory::Replication.name(), "Replication");
    }

    #[test]
    fn test_scheduler_start_stop() {
        let scheduler = TaskScheduler::new();
        assert!(!scheduler.is_running());
        scheduler.start();
        assert!(scheduler.is_running());
        scheduler.stop();
        assert!(!scheduler.is_running());
    }
}
