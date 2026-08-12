// Copyright (c) VELOCITY Suite. All rights reserved.
// Licensed under the MIT License.

//! Activity Worker — Separate process for executing activity tasks.
//!
//! Unlike workflow code (which must be deterministic), activity code runs in a
//! separate worker process that can perform I/O, call external services, and
//! use non-deterministic operations. The worker polls for activity tasks,
//! executes them, and reports results back.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// ═══════════════════════════════════════════════════════════════════════════════
// Activity Definition
// ═══════════════════════════════════════════════════════════════════════════════

/// Trait for implementing an activity.
pub trait ActivityExecutor: Send + Sync + 'static {
    /// Execute the activity with the given input.
    fn execute(&self, input: ActivityInput) -> Result<ActivityOutput, ActivityError>;

    /// Get the activity type name.
    fn activity_type(&self) -> &str;

    /// Get the activity timeout.
    fn timeout(&self) -> Duration {
        Duration::from_secs(30)
    }

    /// Get the heartbeat timeout (None = no heartbeat required).
    fn heartbeat_timeout(&self) -> Option<Duration> {
        None
    }
}

/// Boxed activity executor for dynamic registration.
pub type BoxedActivity = Box<dyn ActivityExecutor>;

/// Activity input data.
#[derive(Debug, Clone)]
pub struct ActivityInput {
    pub data: Vec<u8>,
    pub headers: HashMap<String, Vec<u8>>,
    pub task_token: String,
    pub workflow_id: String,
    pub run_id: String,
    pub activity_id: String,
    pub activity_type: String,
    pub attempt: u32,
    pub heartbeat_details: Vec<Vec<u8>>,
}

impl ActivityInput {
    pub fn new(activity_id: &str, activity_type: &str) -> Self {
        Self {
            data: Vec::new(),
            headers: HashMap::new(),
            task_token: format!("tt-{}", generate_id()),
            workflow_id: format!("wf-{}", generate_id()),
            run_id: generate_id(),
            activity_id: activity_id.to_string(),
            activity_type: activity_type.to_string(),
            attempt: 1,
            heartbeat_details: Vec::new(),
        }
    }

    pub fn with_data(mut self, data: Vec<u8>) -> Self {
        self.data = data;
        self
    }

    pub fn with_header(mut self, key: &str, value: Vec<u8>) -> Self {
        self.headers.insert(key.to_string(), value);
        self
    }

    pub fn with_attempt(mut self, attempt: u32) -> Self {
        self.attempt = attempt;
        self
    }
}

/// Activity output data.
#[derive(Debug, Clone)]
pub struct ActivityOutput {
    pub data: Vec<u8>,
    pub headers: HashMap<String, Vec<u8>>,
}

impl ActivityOutput {
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            data,
            headers: HashMap::new(),
        }
    }

    pub fn with_header(mut self, key: &str, value: Vec<u8>) -> Self {
        self.headers.insert(key.to_string(), value);
        self
    }
}

/// Activity execution error.
#[derive(Debug, Clone)]
pub struct ActivityError {
    pub error_type: ActivityErrorType,
    pub message: String,
    pub details: Vec<u8>,
    pub non_retryable: bool,
    pub cause: Option<Box<ActivityError>>,
}

impl ActivityError {
    pub fn new(error_type: ActivityErrorType, message: &str) -> Self {
        Self {
            error_type,
            message: message.to_string(),
            details: Vec::new(),
            non_retryable: false,
            cause: None,
        }
    }

    pub fn non_retryable(mut self) -> Self {
        self.non_retryable = true;
        self
    }

    pub fn with_cause(mut self, cause: ActivityError) -> Self {
        self.cause = Some(Box::new(cause));
        self
    }

    pub fn timeout(message: &str) -> Self {
        Self::new(ActivityErrorType::Timeout, message)
    }

    pub fn application(message: &str) -> Self {
        Self::new(ActivityErrorType::ApplicationError, message)
    }

    pub fn cancelled(message: &str) -> Self {
        Self::new(ActivityErrorType::Cancelled, message).non_retryable()
    }
}

impl std::fmt::Display for ActivityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.error_type, self.message)
    }
}

/// Types of activity errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActivityErrorType {
    ApplicationError,
    Timeout,
    Cancelled,
    NotFound,
    Internal,
    RetryableError,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Activity Worker
// ═══════════════════════════════════════════════════════════════════════════════

/// Configuration for an activity worker.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub namespace: String,
    pub task_queue: String,
    pub identity: String,
    pub max_concurrent_activities: usize,
    pub max_concurrent_activity_task_polls: usize,
    pub poll_backoff_interval: Duration,
    pub sticky_queue_enabled: bool,
    pub build_id: String,
    pub use_versioning: bool,
    pub graceful_shutdown_timeout: Duration,
    pub max_heartbeat_throttle_interval: Duration,
    pub default_heartbeat_throttle_interval: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            namespace: "default".to_string(),
            task_queue: "default-queue".to_string(),
            identity: format!("worker-{}", generate_id()),
            max_concurrent_activities: 100,
            max_concurrent_activity_task_polls: 5,
            poll_backoff_interval: Duration::from_millis(100),
            sticky_queue_enabled: false,
            build_id: String::new(),
            use_versioning: false,
            graceful_shutdown_timeout: Duration::from_secs(10),
            max_heartbeat_throttle_interval: Duration::from_secs(60),
            default_heartbeat_throttle_interval: Duration::from_secs(30),
        }
    }
}

/// Activity task from the task queue.
#[derive(Debug, Clone)]
pub struct ActivityTask {
    pub task_token: String,
    pub workflow_id: String,
    pub run_id: String,
    pub activity_id: String,
    pub activity_type: String,
    pub input: Vec<u8>,
    pub headers: HashMap<String, Vec<u8>>,
    pub scheduled_time: i64,
    pub started_time: i64,
    pub heartbeat_timeout: Option<Duration>,
    pub attempt: u32,
    pub heartbeat_details: Vec<Vec<u8>>,
}

/// Result of completing an activity task.
#[derive(Debug, Clone)]
pub enum ActivityTaskResult {
    Completed {
        task_token: String,
        result: Vec<u8>,
    },
    Failed {
        task_token: String,
        error: ActivityError,
    },
    Cancelled {
        task_token: String,
        details: Vec<u8>,
    },
}

/// Activity worker that polls for and executes activity tasks.
pub struct ActivityWorker {
    config: WorkerConfig,
    activities: RwLock<HashMap<String, Arc<dyn ActivityExecutor>>>,
    task_queue: RwLock<VecDeque<ActivityTask>>,
    results: RwLock<Vec<ActivityTaskResult>>,
    heartbeats: RwLock<HashMap<String, Vec<u8>>>,
    running: AtomicBool,
    stats: Arc<WorkerStats>,
    cancellation_tokens: RwLock<HashMap<String, Arc<AtomicBool>>>,
}

struct WorkerStats {
    tasks_polled: AtomicU64,
    tasks_completed: AtomicU64,
    tasks_failed: AtomicU64,
    tasks_cancelled: AtomicU64,
    tasks_timed_out: AtomicU64,
    heartbeats_sent: AtomicU64,
    active_activities: AtomicU64,
}

impl ActivityWorker {
    pub fn new(config: WorkerConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            activities: RwLock::new(HashMap::new()),
            task_queue: RwLock::new(VecDeque::new()),
            results: RwLock::new(Vec::new()),
            heartbeats: RwLock::new(HashMap::new()),
            running: AtomicBool::new(false),
            stats: Arc::new(WorkerStats {
                tasks_polled: AtomicU64::new(0),
                tasks_completed: AtomicU64::new(0),
                tasks_failed: AtomicU64::new(0),
                tasks_cancelled: AtomicU64::new(0),
                tasks_timed_out: AtomicU64::new(0),
                heartbeats_sent: AtomicU64::new(0),
                active_activities: AtomicU64::new(0),
            }),
            cancellation_tokens: RwLock::new(HashMap::new()),
        })
    }

    /// Register an activity executor.
    pub fn register_activity(&self, executor: impl ActivityExecutor) {
        let activity_type = executor.activity_type().to_string();
        self.activities
            .write()
            .unwrap()
            .insert(activity_type, Arc::new(executor));
    }

    /// Register a boxed activity executor.
    pub fn register_boxed_activity(
        &self,
        activity_type: &str,
        executor: Arc<dyn ActivityExecutor>,
    ) {
        self.activities
            .write()
            .unwrap()
            .insert(activity_type.to_string(), executor);
    }

    /// Enqueue an activity task for processing.
    pub fn enqueue_task(&self, task: ActivityTask) {
        self.task_queue.write().unwrap().push_back(task);
    }

    /// Poll for the next available task.
    pub fn poll_task(&self) -> Option<ActivityTask> {
        let task = self.task_queue.write().unwrap().pop_front();
        if task.is_some() {
            self.stats.tasks_polled.fetch_add(1, Ordering::Relaxed);
        }
        task
    }

    /// Execute an activity task.
    pub fn execute_task(&self, task: &ActivityTask) -> ActivityTaskResult {
        self.stats.active_activities.fetch_add(1, Ordering::Relaxed);

        // Create cancellation token
        let cancel_token = Arc::new(AtomicBool::new(false));
        self.cancellation_tokens
            .write()
            .unwrap()
            .insert(task.task_token.clone(), cancel_token.clone());

        let activities = self.activities.read().unwrap();
        let result = match activities.get(&task.activity_type) {
            Some(executor) => {
                let input = ActivityInput {
                    data: task.input.clone(),
                    headers: task.headers.clone(),
                    task_token: task.task_token.clone(),
                    workflow_id: task.workflow_id.clone(),
                    run_id: task.run_id.clone(),
                    activity_id: task.activity_id.clone(),
                    activity_type: task.activity_type.clone(),
                    attempt: task.attempt,
                    heartbeat_details: task.heartbeat_details.clone(),
                };

                match executor.execute(input) {
                    Ok(output) => {
                        self.stats.tasks_completed.fetch_add(1, Ordering::Relaxed);
                        ActivityTaskResult::Completed {
                            task_token: task.task_token.clone(),
                            result: output.data,
                        }
                    }
                    Err(err) => {
                        if err.error_type == ActivityErrorType::Cancelled {
                            self.stats.tasks_cancelled.fetch_add(1, Ordering::Relaxed);
                            ActivityTaskResult::Cancelled {
                                task_token: task.task_token.clone(),
                                details: err.details,
                            }
                        } else {
                            self.stats.tasks_failed.fetch_add(1, Ordering::Relaxed);
                            ActivityTaskResult::Failed {
                                task_token: task.task_token.clone(),
                                error: err,
                            }
                        }
                    }
                }
            }
            None => {
                self.stats.tasks_failed.fetch_add(1, Ordering::Relaxed);
                ActivityTaskResult::Failed {
                    task_token: task.task_token.clone(),
                    error: ActivityError::new(
                        ActivityErrorType::NotFound,
                        &format!("Activity type '{}' not registered", task.activity_type),
                    )
                    .non_retryable(),
                }
            }
        };

        self.cancellation_tokens
            .write()
            .unwrap()
            .remove(&task.task_token);
        self.stats.active_activities.fetch_sub(1, Ordering::Relaxed);
        self.results.write().unwrap().push(result.clone());
        result
    }

    /// Record a heartbeat for an activity.
    pub fn record_heartbeat(
        &self,
        task_token: &str,
        details: Vec<u8>,
    ) -> Result<bool, ActivityError> {
        // Check if cancelled
        if let Some(token) = self.cancellation_tokens.read().unwrap().get(task_token) {
            if token.load(Ordering::Relaxed) {
                return Ok(true); // cancel_requested = true
            }
        }

        self.heartbeats
            .write()
            .unwrap()
            .insert(task_token.to_string(), details);
        self.stats.heartbeats_sent.fetch_add(1, Ordering::Relaxed);
        Ok(false) // cancel_requested = false
    }

    /// Request cancellation of an activity.
    pub fn request_cancel(&self, task_token: &str) -> bool {
        if let Some(token) = self.cancellation_tokens.read().unwrap().get(task_token) {
            token.store(true, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Start the worker (marks as running).
    pub fn start(&self) {
        self.running.store(true, Ordering::Relaxed);
    }

    /// Stop the worker (marks as not running).
    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    /// Check if the worker is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Get worker statistics.
    pub fn get_stats(&self) -> WorkerStatsSnapshot {
        WorkerStatsSnapshot {
            tasks_polled: self.stats.tasks_polled.load(Ordering::Relaxed),
            tasks_completed: self.stats.tasks_completed.load(Ordering::Relaxed),
            tasks_failed: self.stats.tasks_failed.load(Ordering::Relaxed),
            tasks_cancelled: self.stats.tasks_cancelled.load(Ordering::Relaxed),
            tasks_timed_out: self.stats.tasks_timed_out.load(Ordering::Relaxed),
            heartbeats_sent: self.stats.heartbeats_sent.load(Ordering::Relaxed),
            active_activities: self.stats.active_activities.load(Ordering::Relaxed),
            registered_activities: self.activities.read().unwrap().len(),
            pending_tasks: self.task_queue.read().unwrap().len(),
        }
    }

    /// Get registered activity types.
    pub fn registered_activities(&self) -> Vec<String> {
        self.activities.read().unwrap().keys().cloned().collect()
    }

    /// Get all results.
    pub fn get_results(&self) -> Vec<ActivityTaskResult> {
        self.results.read().unwrap().clone()
    }

    /// Get the worker config.
    pub fn config(&self) -> &WorkerConfig {
        &self.config
    }
}

/// Snapshot of worker statistics.
#[derive(Debug, Clone)]
pub struct WorkerStatsSnapshot {
    pub tasks_polled: u64,
    pub tasks_completed: u64,
    pub tasks_failed: u64,
    pub tasks_cancelled: u64,
    pub tasks_timed_out: u64,
    pub heartbeats_sent: u64,
    pub active_activities: u64,
    pub registered_activities: usize,
    pub pending_tasks: usize,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Worker Pool
// ═══════════════════════════════════════════════════════════════════════════════

/// Manages multiple activity workers for different task queues.
pub struct WorkerPool {
    workers: RwLock<HashMap<String, Arc<ActivityWorker>>>,
    running: AtomicBool,
}

impl WorkerPool {
    pub fn new() -> Self {
        Self {
            workers: RwLock::new(HashMap::new()),
            running: AtomicBool::new(false),
        }
    }

    /// Add a worker to the pool.
    pub fn add_worker(&self, worker: Arc<ActivityWorker>) {
        let key = format!(
            "{}/{}",
            worker.config().namespace,
            worker.config().task_queue
        );
        self.workers.write().unwrap().insert(key, worker);
    }

    /// Remove a worker from the pool.
    pub fn remove_worker(&self, namespace: &str, task_queue: &str) -> Option<Arc<ActivityWorker>> {
        let key = format!("{}/{}", namespace, task_queue);
        self.workers.write().unwrap().remove(&key)
    }

    /// Get a worker by namespace and task queue.
    pub fn get_worker(&self, namespace: &str, task_queue: &str) -> Option<Arc<ActivityWorker>> {
        let key = format!("{}/{}", namespace, task_queue);
        self.workers.read().unwrap().get(&key).cloned()
    }

    /// Start all workers.
    pub fn start_all(&self) {
        self.running.store(true, Ordering::Relaxed);
        for worker in self.workers.read().unwrap().values() {
            worker.start();
        }
    }

    /// Stop all workers.
    pub fn stop_all(&self) {
        self.running.store(false, Ordering::Relaxed);
        for worker in self.workers.read().unwrap().values() {
            worker.stop();
        }
    }

    /// Get aggregate stats across all workers.
    pub fn aggregate_stats(&self) -> PoolStats {
        let workers = self.workers.read().unwrap();
        let mut total = PoolStats::default();
        for worker in workers.values() {
            let s = worker.get_stats();
            total.tasks_polled += s.tasks_polled;
            total.tasks_completed += s.tasks_completed;
            total.tasks_failed += s.tasks_failed;
            total.tasks_cancelled += s.tasks_cancelled;
            total.heartbeats_sent += s.heartbeats_sent;
            total.active_activities += s.active_activities;
            total.registered_activities += s.registered_activities;
            total.pending_tasks += s.pending_tasks;
            total.worker_count += 1;
        }
        total
    }

    /// List all worker keys.
    pub fn list_workers(&self) -> Vec<String> {
        self.workers.read().unwrap().keys().cloned().collect()
    }

    /// Check if the pool is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }
}

impl Default for WorkerPool {
    fn default() -> Self {
        Self::new()
    }
}

/// Aggregate pool statistics.
#[derive(Debug, Clone, Default)]
pub struct PoolStats {
    pub worker_count: usize,
    pub tasks_polled: u64,
    pub tasks_completed: u64,
    pub tasks_failed: u64,
    pub tasks_cancelled: u64,
    pub heartbeats_sent: u64,
    pub active_activities: u64,
    pub registered_activities: usize,
    pub pending_tasks: usize,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Built-in Activities
// ═══════════════════════════════════════════════════════════════════════════════

/// A simple echo activity that returns its input.
pub struct EchoActivity;

impl ActivityExecutor for EchoActivity {
    fn execute(&self, input: ActivityInput) -> Result<ActivityOutput, ActivityError> {
        Ok(ActivityOutput::new(input.data))
    }

    fn activity_type(&self) -> &str {
        "Echo"
    }
}

/// A sleep activity that pauses for a specified duration.
pub struct SleepActivity;

impl ActivityExecutor for SleepActivity {
    fn execute(&self, input: ActivityInput) -> Result<ActivityOutput, ActivityError> {
        let duration_ms = if input.data.len() >= 8 {
            u64::from_le_bytes(input.data[..8].try_into().unwrap_or([0; 8]))
        } else {
            100
        };
        std::thread::sleep(Duration::from_millis(duration_ms));
        Ok(ActivityOutput::new(duration_ms.to_le_bytes().to_vec()))
    }

    fn activity_type(&self) -> &str {
        "Sleep"
    }
}

/// An HTTP activity that makes HTTP requests.
pub struct HttpActivity {
    pub base_url: String,
}

impl ActivityExecutor for HttpActivity {
    fn execute(&self, input: ActivityInput) -> Result<ActivityOutput, ActivityError> {
        // In a real implementation, this would use an HTTP client
        Ok(ActivityOutput::new(
            format!("HTTP response from {} for {:?}", self.base_url, input.data).into_bytes(),
        ))
    }

    fn activity_type(&self) -> &str {
        "Http"
    }

    fn timeout(&self) -> Duration {
        Duration::from_secs(60)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════════

fn generate_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let c = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{:x}{:04x}", ts, c)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    struct TestActivity;
    impl ActivityExecutor for TestActivity {
        fn execute(&self, input: ActivityInput) -> Result<ActivityOutput, ActivityError> {
            Ok(ActivityOutput::new(
                format!("processed: {}", String::from_utf8_lossy(&input.data)).into_bytes(),
            ))
        }
        fn activity_type(&self) -> &str {
            "Test"
        }
    }

    struct FailingActivity;
    impl ActivityExecutor for FailingActivity {
        fn execute(&self, _input: ActivityInput) -> Result<ActivityOutput, ActivityError> {
            Err(ActivityError::application("intentional failure"))
        }
        fn activity_type(&self) -> &str {
            "Failing"
        }
    }

    fn make_task(activity_type: &str) -> ActivityTask {
        ActivityTask {
            task_token: format!("tt-{}", generate_id()),
            workflow_id: "wf-test".to_string(),
            run_id: "run-1".to_string(),
            activity_id: "act-1".to_string(),
            activity_type: activity_type.to_string(),
            input: b"hello".to_vec(),
            headers: HashMap::new(),
            scheduled_time: now_millis(),
            started_time: now_millis(),
            heartbeat_timeout: None,
            attempt: 1,
            heartbeat_details: Vec::new(),
        }
    }

    fn now_millis() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }

    #[test]
    fn test_register_and_execute_activity() {
        let worker = ActivityWorker::new(WorkerConfig::default());
        worker.register_activity(TestActivity);
        let task = make_task("Test");
        let result = worker.execute_task(&task);
        match result {
            ActivityTaskResult::Completed { result, .. } => {
                let output = String::from_utf8_lossy(&result);
                assert!(output.contains("processed: hello"));
            }
            _ => panic!("Expected Completed"),
        }
    }

    #[test]
    fn test_failing_activity() {
        let worker = ActivityWorker::new(WorkerConfig::default());
        worker.register_activity(FailingActivity);
        let task = make_task("Failing");
        let result = worker.execute_task(&task);
        match result {
            ActivityTaskResult::Failed { error, .. } => {
                assert_eq!(error.message, "intentional failure");
            }
            _ => panic!("Expected Failed"),
        }
    }

    #[test]
    fn test_unregistered_activity() {
        let worker = ActivityWorker::new(WorkerConfig::default());
        let task = make_task("Unknown");
        let result = worker.execute_task(&task);
        match result {
            ActivityTaskResult::Failed { error, .. } => {
                assert_eq!(error.error_type, ActivityErrorType::NotFound);
                assert!(error.non_retryable);
            }
            _ => panic!("Expected Failed"),
        }
    }

    #[test]
    fn test_worker_stats() {
        let worker = ActivityWorker::new(WorkerConfig::default());
        worker.register_activity(TestActivity);
        let task = make_task("Test");
        worker.execute_task(&task);
        let stats = worker.get_stats();
        assert_eq!(stats.tasks_completed, 1);
        assert_eq!(stats.tasks_failed, 0);
        assert_eq!(stats.tasks_polled, 0); // poll is separate from execute
    }

    #[test]
    fn test_enqueue_and_poll() {
        let worker = ActivityWorker::new(WorkerConfig::default());
        let task = make_task("Test");
        worker.enqueue_task(task);
        let polled = worker.poll_task();
        assert!(polled.is_some());
        assert_eq!(polled.unwrap().activity_type, "Test");
        assert!(worker.poll_task().is_none());
    }

    #[test]
    fn test_heartbeat() {
        let worker = ActivityWorker::new(WorkerConfig::default());
        worker.register_activity(TestActivity);
        let task = make_task("Test");
        let cancel = worker.record_heartbeat(&task.task_token, b"progress".to_vec());
        assert!(cancel.is_ok());
        assert!(!cancel.unwrap()); // not cancelled
    }

    #[test]
    fn test_request_cancel() {
        let worker = ActivityWorker::new(WorkerConfig::default());
        let task = make_task("Test");
        worker
            .cancellation_tokens
            .write()
            .unwrap()
            .insert(task.task_token.clone(), Arc::new(AtomicBool::new(false)));
        let result = worker.request_cancel(&task.task_token);
        assert!(result);
        let cancel = worker.record_heartbeat(&task.task_token, Vec::new());
        assert!(cancel.unwrap()); // cancelled = true
    }

    #[test]
    fn test_worker_start_stop() {
        let worker = ActivityWorker::new(WorkerConfig::default());
        assert!(!worker.is_running());
        worker.start();
        assert!(worker.is_running());
        worker.stop();
        assert!(!worker.is_running());
    }

    #[test]
    fn test_registered_activities() {
        let worker = ActivityWorker::new(WorkerConfig::default());
        worker.register_activity(TestActivity);
        worker.register_activity(FailingActivity);
        let types = worker.registered_activities();
        assert_eq!(types.len(), 2);
        assert!(types.contains(&"Test".to_string()));
        assert!(types.contains(&"Failing".to_string()));
    }

    #[test]
    fn test_worker_pool() {
        let pool = WorkerPool::new();
        let w1 = ActivityWorker::new(WorkerConfig {
            task_queue: "queue-1".to_string(),
            ..Default::default()
        });
        let w2 = ActivityWorker::new(WorkerConfig {
            task_queue: "queue-2".to_string(),
            ..Default::default()
        });
        pool.add_worker(w1);
        pool.add_worker(w2);
        assert_eq!(pool.list_workers().len(), 2);
        pool.start_all();
        assert!(pool.is_running());
        let stats = pool.aggregate_stats();
        assert_eq!(stats.worker_count, 2);
        pool.stop_all();
        assert!(!pool.is_running());
    }

    #[test]
    fn test_echo_activity() {
        let echo = EchoActivity;
        let input = ActivityInput::new("act-1", "Echo").with_data(b"echo me".to_vec());
        let result = echo.execute(input).unwrap();
        assert_eq!(result.data, b"echo me");
    }

    #[test]
    fn test_activity_input_builder() {
        let input = ActivityInput::new("act-1", "Test")
            .with_data(b"test".to_vec())
            .with_header("key", b"value".to_vec())
            .with_attempt(3);
        assert_eq!(input.data, b"test");
        assert_eq!(input.attempt, 3);
        assert!(input.headers.contains_key("key"));
    }

    #[test]
    fn test_activity_error_display() {
        let err = ActivityError::application("test error");
        assert_eq!(format!("{}", err), "ApplicationError: test error");
    }
}
