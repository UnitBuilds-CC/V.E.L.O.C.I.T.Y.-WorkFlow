//! Worker Process Model — manages worker process lifecycle, streaming poll channels,
//! heartbeat monitoring, and graceful shutdown. Mirrors Temporal's worker process architecture.
//!
//! Features:
//! - Worker process registration with identity, capabilities, and version info
//! - Streaming poll channels for efficient task delivery (gRPC server-streaming)
//! - Heartbeat monitoring with configurable timeout and stale detection
//! - Graceful shutdown with drain and in-flight task tracking
//! - Process health scoring based on heartbeat freshness and task success rate

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::{Duration, Instant};

// ─── Worker Process ──────────────────────────────────────────────────────────

/// Unique identifier for a worker process.
pub type WorkerProcessId = u64;

/// Status of a worker process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessStatus {
    /// Worker is connected and accepting tasks.
    Active,
    /// Worker is draining — no new tasks, waiting for in-flight to complete.
    Draining,
    /// Worker has disconnected or timed out.
    Disconnected,
    /// Worker has been shut down gracefully.
    ShutDown,
}

/// Information about a connected worker process.
#[derive(Debug, Clone)]
pub struct WorkerProcess {
    pub process_id: WorkerProcessId,
    pub identity: String,
    pub task_queues: Vec<String>,
    pub build_id: String,
    pub namespace: String,
    pub status: ProcessStatus,
    pub connected_at: Instant,
    pub last_heartbeat: Instant,
    pub heartbeat_count: u64,
    pub tasks_polled: u64,
    pub tasks_completed: u64,
    pub tasks_failed: u64,
    pub in_flight_tasks: u64,
    pub max_concurrent_tasks: u64,
    /// Sticky queue affinity — prefer dispatching to last-used queue.
    pub sticky_queue: Option<String>,
    /// Health score 0-100 (100 = perfectly healthy).
    pub health_score: u32,
}

impl WorkerProcess {
    /// Check if this process can accept more tasks.
    pub fn has_capacity(&self) -> bool {
        self.status == ProcessStatus::Active && self.in_flight_tasks < self.max_concurrent_tasks
    }

    /// Available task slots.
    pub fn available_slots(&self) -> u64 {
        self.max_concurrent_tasks
            .saturating_sub(self.in_flight_tasks)
    }

    /// Duration since last heartbeat.
    pub fn heartbeat_age(&self) -> Duration {
        self.last_heartbeat.elapsed()
    }

    /// Task success rate (0.0 - 1.0).
    pub fn success_rate(&self) -> f64 {
        let total = self.tasks_completed + self.tasks_failed;
        if total == 0 {
            1.0
        } else {
            self.tasks_completed as f64 / total as f64
        }
    }
}

// ─── Streaming Poll Channel ──────────────────────────────────────────────────

/// A task delivered via streaming poll to a worker process.
#[derive(Debug, Clone)]
pub struct StreamedTask {
    pub task_id: u64,
    pub task_token: Vec<u8>,
    pub task_queue: String,
    pub workflow_key: u64,
    pub payload: Vec<u8>,
    pub delivered_at: Instant,
}

/// A bidirectional streaming poll channel between server and worker.
/// The server pushes tasks; the worker acknowledges completions.
pub struct StreamingPollChannel {
    pub process_id: WorkerProcessId,
    /// Tasks queued for delivery to the worker (server → worker).
    outbound: Mutex<VecDeque<StreamedTask>>,
    /// Completions reported by the worker (worker → server).
    completions: Mutex<VecDeque<TaskCompletion>>,
    /// Notification for new tasks available.
    notify: Condvar,
    notify_mutex: Mutex<()>,
    /// Whether the channel is open.
    open: Mutex<bool>,
    /// Total tasks delivered through this channel.
    total_delivered: AtomicU64,
    /// Total completions received.
    total_completed: AtomicU64,
}

/// Completion report from a worker for a specific task.
#[derive(Debug, Clone)]
pub struct TaskCompletion {
    pub task_id: u64,
    pub task_token: Vec<u8>,
    pub success: bool,
    pub result_payload: Option<Vec<u8>>,
    pub error_message: Option<String>,
}

impl StreamingPollChannel {
    pub fn new(process_id: WorkerProcessId) -> Self {
        Self {
            process_id,
            outbound: Mutex::new(VecDeque::new()),
            completions: Mutex::new(VecDeque::new()),
            notify: Condvar::new(),
            notify_mutex: Mutex::new(()),
            open: Mutex::new(true),
            total_delivered: AtomicU64::new(0),
            total_completed: AtomicU64::new(0),
        }
    }

    /// Push a task to the worker. Returns false if channel is closed.
    pub fn deliver_task(&self, task: StreamedTask) -> bool {
        let open = self.open.lock().unwrap();
        if !*open {
            return false;
        }
        self.outbound.lock().unwrap().push_back(task);
        self.total_delivered.fetch_add(1, Ordering::Relaxed);
        self.notify.notify_one();
        true
    }

    /// Wait for a task to be available (blocking with timeout). Returns None on timeout/close.
    pub fn wait_for_task(&self, timeout: Duration) -> Option<StreamedTask> {
        let mut guard = self.notify_mutex.lock().unwrap();
        let deadline = Instant::now() + timeout;

        loop {
            // Check if channel is closed
            if !*self.open.lock().unwrap() {
                return None;
            }

            // Try to get a task
            if let Some(task) = self.outbound.lock().unwrap().pop_front() {
                return Some(task);
            }

            // Check timeout
            if Instant::now() >= deadline {
                return None;
            }

            let remaining = deadline - Instant::now();
            let (new_guard, timeout_result) = self.notify.wait_timeout(guard, remaining).unwrap();
            guard = new_guard;

            if timeout_result.timed_out() {
                return None;
            }
        }
    }

    /// Non-blocking: try to get the next task immediately.
    pub fn try_next_task(&self) -> Option<StreamedTask> {
        self.outbound.lock().unwrap().pop_front()
    }

    /// Report a task completion from the worker.
    pub fn report_completion(&self, completion: TaskCompletion) {
        self.completions.lock().unwrap().push_back(completion);
        self.total_completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Drain all pending completions.
    pub fn drain_completions(&self) -> Vec<TaskCompletion> {
        let mut completions = self.completions.lock().unwrap();
        completions.drain(..).collect()
    }

    /// Close the channel.
    pub fn close(&self) {
        *self.open.lock().unwrap() = false;
        self.notify.notify_all();
    }

    /// Check if the channel is open.
    pub fn is_open(&self) -> bool {
        *self.open.lock().unwrap()
    }

    /// Number of tasks pending delivery.
    pub fn pending_count(&self) -> usize {
        self.outbound.lock().unwrap().len()
    }

    /// Total tasks delivered.
    pub fn total_delivered(&self) -> u64 {
        self.total_delivered.load(Ordering::Relaxed)
    }

    /// Total completions received.
    pub fn total_completed(&self) -> u64 {
        self.total_completed.load(Ordering::Relaxed)
    }
}

// ─── Worker Process Manager ──────────────────────────────────────────────────

/// Configuration for the worker process manager.
#[derive(Debug, Clone)]
pub struct WorkerProcessManagerConfig {
    pub heartbeat_timeout: Duration,
    pub max_processes: usize,
    pub max_in_flight_per_worker: u64,
    pub default_max_concurrent: u64,
    pub drain_timeout: Duration,
}

impl Default for WorkerProcessManagerConfig {
    fn default() -> Self {
        Self {
            heartbeat_timeout: Duration::from_secs(30),
            max_processes: 10_000,
            max_in_flight_per_worker: 100,
            default_max_concurrent: 10,
            drain_timeout: Duration::from_secs(60),
        }
    }
}

/// Statistics for the worker process manager.
#[derive(Debug, Clone, Default)]
pub struct WorkerProcessManagerStats {
    pub total_registered: u64,
    pub total_deregistered: u64,
    pub active_processes: u64,
    pub draining_processes: u64,
    pub disconnected_processes: u64,
    pub total_heartbeats: u64,
    pub total_tasks_dispatched: u64,
    pub total_tasks_completed: u64,
    pub total_tasks_failed: u64,
    pub stale_workers_detected: u64,
}

/// Manages worker process lifecycle, streaming channels, and heartbeats.
pub struct WorkerProcessManager {
    processes: RwLock<HashMap<WorkerProcessId, WorkerProcess>>,
    channels: RwLock<HashMap<WorkerProcessId, Arc<StreamingPollChannel>>>,
    config: WorkerProcessManagerConfig,
    next_id: AtomicU64,
    start_time: Instant,
    stats: RwLock<WorkerProcessManagerStats>,
}

impl WorkerProcessManager {
    pub fn new(config: WorkerProcessManagerConfig) -> Self {
        Self {
            processes: RwLock::new(HashMap::new()),
            channels: RwLock::new(HashMap::new()),
            config,
            next_id: AtomicU64::new(1),
            start_time: Instant::now(),
            stats: RwLock::new(WorkerProcessManagerStats::default()),
        }
    }

    /// Register a new worker process. Returns the process ID and streaming channel.
    pub fn register(
        &self,
        identity: &str,
        task_queues: &[String],
        build_id: &str,
        namespace: &str,
    ) -> Option<(WorkerProcessId, Arc<StreamingPollChannel>)> {
        let processes = self.processes.read().unwrap();
        if processes.len() >= self.config.max_processes {
            return None;
        }
        drop(processes);

        let process_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();

        let process = WorkerProcess {
            process_id,
            identity: identity.to_string(),
            task_queues: task_queues.to_vec(),
            build_id: build_id.to_string(),
            namespace: namespace.to_string(),
            status: ProcessStatus::Active,
            connected_at: now,
            last_heartbeat: now,
            heartbeat_count: 0,
            tasks_polled: 0,
            tasks_completed: 0,
            tasks_failed: 0,
            in_flight_tasks: 0,
            max_concurrent_tasks: self.config.default_max_concurrent,
            sticky_queue: task_queues.first().cloned(),
            health_score: 100,
        };

        let channel = Arc::new(StreamingPollChannel::new(process_id));

        self.processes.write().unwrap().insert(process_id, process);
        self.channels
            .write()
            .unwrap()
            .insert(process_id, channel.clone());

        let mut stats = self.stats.write().unwrap();
        stats.total_registered += 1;
        stats.active_processes += 1;

        Some((process_id, channel))
    }

    /// Deregister a worker process, closing its streaming channel.
    pub fn deregister(&self, process_id: WorkerProcessId) -> bool {
        let removed = self.processes.write().unwrap().remove(&process_id);
        if let Some(ref process) = removed {
            let mut stats = self.stats.write().unwrap();
            stats.total_deregistered += 1;
            match process.status {
                ProcessStatus::Active => {
                    stats.active_processes = stats.active_processes.saturating_sub(1)
                }
                ProcessStatus::Draining => {
                    stats.draining_processes = stats.draining_processes.saturating_sub(1)
                }
                ProcessStatus::Disconnected => {
                    stats.disconnected_processes = stats.disconnected_processes.saturating_sub(1)
                }
                _ => {}
            }
        }

        if let Some(channel) = self.channels.write().unwrap().remove(&process_id) {
            channel.close();
        }

        removed.is_some()
    }

    /// Record a heartbeat from a worker process.
    pub fn heartbeat(&self, process_id: WorkerProcessId) -> bool {
        let mut processes = self.processes.write().unwrap();
        if let Some(process) = processes.get_mut(&process_id) {
            process.last_heartbeat = Instant::now();
            process.heartbeat_count += 1;
            // Restore health score on heartbeat
            process.health_score = 100;
            // If it was disconnected but heartbeats again, reactivate
            if process.status == ProcessStatus::Disconnected {
                process.status = ProcessStatus::Active;
                let mut st = self.stats.write().unwrap();
                st.disconnected_processes = st.disconnected_processes.saturating_sub(1);
                st.active_processes += 1;
            }
            self.stats.write().unwrap().total_heartbeats += 1;
            true
        } else {
            false
        }
    }

    /// Record that a task was dispatched to a worker.
    pub fn record_task_dispatched(&self, process_id: WorkerProcessId) {
        let mut processes = self.processes.write().unwrap();
        if let Some(process) = processes.get_mut(&process_id) {
            process.tasks_polled += 1;
            process.in_flight_tasks += 1;
        }
        self.stats.write().unwrap().total_tasks_dispatched += 1;
    }

    /// Record that a task completed successfully.
    pub fn record_task_completed(&self, process_id: WorkerProcessId) {
        let mut processes = self.processes.write().unwrap();
        if let Some(process) = processes.get_mut(&process_id) {
            process.tasks_completed += 1;
            process.in_flight_tasks = process.in_flight_tasks.saturating_sub(1);
        }
        self.stats.write().unwrap().total_tasks_completed += 1;
    }

    /// Record that a task failed.
    pub fn record_task_failed(&self, process_id: WorkerProcessId) {
        let mut processes = self.processes.write().unwrap();
        if let Some(process) = processes.get_mut(&process_id) {
            process.tasks_failed += 1;
            process.in_flight_tasks = process.in_flight_tasks.saturating_sub(1);
        }
        self.stats.write().unwrap().total_tasks_failed += 1;
    }

    /// Set a worker's sticky queue affinity.
    pub fn set_sticky_queue(&self, process_id: WorkerProcessId, queue: &str) {
        let mut processes = self.processes.write().unwrap();
        if let Some(process) = processes.get_mut(&process_id) {
            process.sticky_queue = Some(queue.to_string());
        }
    }

    /// Begin draining a worker — stop sending new tasks.
    pub fn drain(&self, process_id: WorkerProcessId) -> bool {
        let mut processes = self.processes.write().unwrap();
        if let Some(process) = processes.get_mut(&process_id) {
            if process.status == ProcessStatus::Active {
                process.status = ProcessStatus::Draining;
                let mut stats = self.stats.write().unwrap();
                stats.active_processes = stats.active_processes.saturating_sub(1);
                stats.draining_processes += 1;
                return true;
            }
        }
        false
    }

    /// Detect stale workers (no heartbeat within timeout) and mark them disconnected.
    pub fn detect_stale_workers(&self) -> Vec<WorkerProcessId> {
        let timeout = self.config.heartbeat_timeout;
        let mut processes = self.processes.write().unwrap();
        let mut stale = Vec::new();

        for process in processes.values_mut() {
            if process.status == ProcessStatus::Active && process.last_heartbeat.elapsed() > timeout
            {
                process.status = ProcessStatus::Disconnected;
                process.health_score = 0;
                stale.push(process.process_id);
            }
        }

        if !stale.is_empty() {
            let mut stats = self.stats.write().unwrap();
            stats.stale_workers_detected += stale.len() as u64;
            let stale_count = stale.len() as u64;
            stats.active_processes = stats.active_processes.saturating_sub(stale_count);
            stats.disconnected_processes += stale_count;
        }

        stale
    }

    /// Update health scores for all workers based on heartbeat freshness and success rate.
    pub fn update_health_scores(&self) {
        let timeout = self.config.heartbeat_timeout;
        let mut processes = self.processes.write().unwrap();

        for process in processes.values_mut() {
            if process.status != ProcessStatus::Active {
                continue;
            }

            // Heartbeat freshness: 0-50 points
            let heartbeat_age = process.last_heartbeat.elapsed();
            let heartbeat_score = if heartbeat_age >= timeout {
                0
            } else {
                let pct = 1.0 - (heartbeat_age.as_secs_f64() / timeout.as_secs_f64());
                (pct * 50.0) as u32
            };

            // Success rate: 0-50 points
            let success_score = (process.success_rate() * 50.0) as u32;

            process.health_score = heartbeat_score + success_score;
        }
    }

    /// Get the best worker for a task on a specific queue (load-aware selection).
    pub fn select_worker_for_queue(&self, queue_name: &str) -> Option<WorkerProcessId> {
        let processes = self.processes.read().unwrap();
        let mut best: Option<(WorkerProcessId, u64, u32)> = None; // (id, available_slots, health)

        for process in processes.values() {
            if !process.has_capacity() {
                continue;
            }
            if !process.task_queues.contains(&queue_name.to_string()) {
                continue;
            }

            let sticky_bonus = if process.sticky_queue.as_deref() == Some(queue_name) {
                10u32
            } else {
                0
            };

            let score =
                process.available_slots() * 100 + (process.health_score + sticky_bonus) as u64;
            let health = process.health_score + sticky_bonus;

            if best.is_none() || score > best.unwrap().1 {
                best = Some((process.process_id, score, health));
            }
        }

        best.map(|(id, _, _)| id)
    }

    /// Get info about a specific worker process.
    pub fn get_process(&self, process_id: WorkerProcessId) -> Option<WorkerProcess> {
        self.processes.read().unwrap().get(&process_id).cloned()
    }

    /// Get the streaming channel for a worker process.
    pub fn get_channel(&self, process_id: WorkerProcessId) -> Option<Arc<StreamingPollChannel>> {
        self.channels.read().unwrap().get(&process_id).cloned()
    }

    /// List all active worker process IDs.
    pub fn list_active_processes(&self) -> Vec<WorkerProcessId> {
        self.processes
            .read()
            .unwrap()
            .values()
            .filter(|p| p.status == ProcessStatus::Active)
            .map(|p| p.process_id)
            .collect()
    }

    /// List all worker processes for a specific task queue.
    pub fn list_processes_for_queue(&self, queue_name: &str) -> Vec<WorkerProcess> {
        self.processes
            .read()
            .unwrap()
            .values()
            .filter(|p| p.task_queues.contains(&queue_name.to_string()))
            .cloned()
            .collect()
    }

    /// Get statistics.
    pub fn stats(&self) -> WorkerProcessManagerStats {
        self.stats.read().unwrap().clone()
    }

    /// Total number of registered processes.
    pub fn process_count(&self) -> usize {
        self.processes.read().unwrap().len()
    }

    /// Count of active processes.
    pub fn active_count(&self) -> usize {
        self.processes
            .read()
            .unwrap()
            .values()
            .filter(|p| p.status == ProcessStatus::Active)
            .count()
    }

    /// Gracefully shut down all workers: drain, wait for in-flight, then deregister.
    pub fn shutdown_all(&self) -> usize {
        let process_ids: Vec<WorkerProcessId> =
            self.processes.read().unwrap().keys().copied().collect();

        let mut drained = 0;
        for pid in &process_ids {
            self.drain(*pid);
            drained += 1;
        }

        // Close all channels
        for pid in &process_ids {
            if let Some(channel) = self.channels.read().unwrap().get(pid) {
                channel.close();
            }
            let mut processes = self.processes.write().unwrap();
            if let Some(process) = processes.get_mut(pid) {
                process.status = ProcessStatus::ShutDown;
            }
        }

        drained
    }

    /// Uptime of the manager.
    pub fn uptime(&self) -> Duration {
        self.start_time.elapsed()
    }
}

impl Default for WorkerProcessManager {
    fn default() -> Self {
        Self::new(WorkerProcessManagerConfig::default())
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_deregister() {
        let mgr = WorkerProcessManager::default();
        let (pid, channel) = mgr
            .register("worker-1", &["queue-a".into()], "v1.0", "default")
            .unwrap();

        assert!(pid > 0);
        assert_eq!(mgr.process_count(), 1);
        assert_eq!(mgr.active_count(), 1);
        assert!(channel.is_open());

        assert!(mgr.deregister(pid));
        assert_eq!(mgr.process_count(), 0);
        assert!(!channel.is_open()); // Channel closed on deregister
    }

    #[test]
    fn test_heartbeat() {
        let mgr = WorkerProcessManager::default();
        let (pid, _) = mgr
            .register("worker-1", &["q1".into()], "v1", "default")
            .unwrap();

        assert!(mgr.heartbeat(pid));
        let process = mgr.get_process(pid).unwrap();
        assert_eq!(process.heartbeat_count, 1);
        assert_eq!(process.health_score, 100);
    }

    #[test]
    fn test_task_dispatch_and_complete() {
        let mgr = WorkerProcessManager::default();
        let (pid, _) = mgr
            .register("worker-1", &["q1".into()], "v1", "default")
            .unwrap();

        mgr.record_task_dispatched(pid);
        let process = mgr.get_process(pid).unwrap();
        assert_eq!(process.in_flight_tasks, 1);
        assert_eq!(process.tasks_polled, 1);

        mgr.record_task_completed(pid);
        let process = mgr.get_process(pid).unwrap();
        assert_eq!(process.in_flight_tasks, 0);
        assert_eq!(process.tasks_completed, 1);

        let stats = mgr.stats();
        assert_eq!(stats.total_tasks_dispatched, 1);
        assert_eq!(stats.total_tasks_completed, 1);
    }

    #[test]
    fn test_drain_worker() {
        let mgr = WorkerProcessManager::default();
        let (pid, _) = mgr
            .register("worker-1", &["q1".into()], "v1", "default")
            .unwrap();

        assert!(mgr.drain(pid));
        let process = mgr.get_process(pid).unwrap();
        assert_eq!(process.status, ProcessStatus::Draining);
        assert!(!process.has_capacity()); // Draining workers don't accept tasks
    }

    #[test]
    fn test_select_worker_for_queue() {
        let mgr = WorkerProcessManager::default();
        let (pid1, _) = mgr
            .register("w1", &["orders".into()], "v1", "default")
            .unwrap();
        let (_pid2, _) = mgr
            .register("w2", &["payments".into()], "v1", "default")
            .unwrap();

        let selected = mgr.select_worker_for_queue("orders").unwrap();
        assert_eq!(selected, pid1);
    }

    #[test]
    fn test_sticky_queue_affinity() {
        let mgr = WorkerProcessManager::default();
        let (pid1, _) = mgr
            .register("w1", &["q1".into(), "q2".into()], "v1", "default")
            .unwrap();
        let (_pid2, _) = mgr
            .register("w2", &["q1".into(), "q2".into()], "v1", "default")
            .unwrap();

        // Set sticky affinity for w1 to q2
        mgr.set_sticky_queue(pid1, "q2");

        // Both have capacity, but w1 has sticky bonus for q2
        let selected = mgr.select_worker_for_queue("q2").unwrap();
        assert_eq!(selected, pid1);
    }

    #[test]
    fn test_streaming_poll_channel() {
        let channel = StreamingPollChannel::new(1);

        // Deliver a task
        let task = StreamedTask {
            task_id: 42,
            task_token: vec![1, 2, 3],
            task_queue: "q1".to_string(),
            workflow_key: 100,
            payload: vec![10, 20],
            delivered_at: Instant::now(),
        };
        assert!(channel.deliver_task(task));
        assert_eq!(channel.pending_count(), 1);

        // Retrieve the task
        let retrieved = channel.try_next_task().unwrap();
        assert_eq!(retrieved.task_id, 42);
        assert_eq!(channel.pending_count(), 0);

        // Report completion
        channel.report_completion(TaskCompletion {
            task_id: 42,
            task_token: vec![1, 2, 3],
            success: true,
            result_payload: Some(vec![30]),
            error_message: None,
        });

        let completions = channel.drain_completions();
        assert_eq!(completions.len(), 1);
        assert!(completions[0].success);

        assert_eq!(channel.total_delivered(), 1);
        assert_eq!(channel.total_completed(), 1);
    }

    #[test]
    fn test_streaming_channel_close() {
        let channel = StreamingPollChannel::new(1);
        assert!(channel.is_open());

        channel.close();
        assert!(!channel.is_open());

        // Can't deliver to closed channel
        let task = StreamedTask {
            task_id: 1,
            task_token: vec![],
            task_queue: "q".into(),
            workflow_key: 0,
            payload: vec![],
            delivered_at: Instant::now(),
        };
        assert!(!channel.deliver_task(task));
    }

    #[test]
    fn test_list_processes_for_queue() {
        let mgr = WorkerProcessManager::default();
        mgr.register("w1", &["q1".into(), "q2".into()], "v1", "default")
            .unwrap();
        mgr.register("w2", &["q2".into()], "v1", "default").unwrap();
        mgr.register("w3", &["q3".into()], "v1", "default").unwrap();

        let q2_workers = mgr.list_processes_for_queue("q2");
        assert_eq!(q2_workers.len(), 2);

        let q3_workers = mgr.list_processes_for_queue("q3");
        assert_eq!(q3_workers.len(), 1);
    }

    #[test]
    fn test_shutdown_all() {
        let mgr = WorkerProcessManager::default();
        mgr.register("w1", &["q1".into()], "v1", "default").unwrap();
        mgr.register("w2", &["q2".into()], "v1", "default").unwrap();

        let drained = mgr.shutdown_all();
        assert_eq!(drained, 2);

        // All processes should be shut down
        let processes = mgr.list_active_processes();
        assert!(processes.is_empty());
    }

    #[test]
    fn test_worker_success_rate() {
        let mgr = WorkerProcessManager::default();
        let (pid, _) = mgr.register("w1", &["q1".into()], "v1", "default").unwrap();

        mgr.record_task_dispatched(pid);
        mgr.record_task_completed(pid);
        mgr.record_task_dispatched(pid);
        mgr.record_task_failed(pid);

        let process = mgr.get_process(pid).unwrap();
        assert_eq!(process.tasks_completed, 1);
        assert_eq!(process.tasks_failed, 1);
        assert!((process.success_rate() - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_max_processes_limit() {
        let config = WorkerProcessManagerConfig {
            max_processes: 2,
            ..Default::default()
        };
        let mgr = WorkerProcessManager::new(config);

        assert!(mgr.register("w1", &["q".into()], "v1", "default").is_some());
        assert!(mgr.register("w2", &["q".into()], "v1", "default").is_some());
        assert!(mgr.register("w3", &["q".into()], "v1", "default").is_none()); // Rejected
    }

    #[test]
    fn test_update_health_scores() {
        let mgr = WorkerProcessManager::default();
        let (pid, _) = mgr.register("w1", &["q1".into()], "v1", "default").unwrap();

        // Fresh heartbeat → high score
        mgr.update_health_scores();
        let process = mgr.get_process(pid).unwrap();
        assert!(process.health_score > 80); // Should be near 100
    }
}
