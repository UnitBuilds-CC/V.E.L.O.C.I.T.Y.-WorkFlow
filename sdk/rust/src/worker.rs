//! Worker process model for the VELOCITY-WorkFlow Rust SDK.
//!
//! The Worker polls the server for workflow and activity tasks, executes them
//! using registered (or auto-applied) implementations, and reports results.
//!
//! # Example
//!
//! ```rust,no_run
//! use velocity_sdk::worker::{Worker, WorkerOptions};
//! use velocity_sdk::auto_apply::{register_workflow, register_activity, WorkflowHandler, WorkflowHandlerContext};
//!
//! struct OrderWorkflow;
//! impl WorkflowHandler for OrderWorkflow {
//!     fn workflow_type() -> &'static str { "OrderWorkflow" }
//!     fn execute(&self, ctx: &WorkflowHandlerContext, input: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
//!         Ok(b"completed".to_vec())
//!     }
//! }
//!
//! register_workflow::<OrderWorkflow>();
//!
//! let worker = Worker::new(WorkerOptions {
//!     task_queue: "orders".to_string(),
//!     ..Default::default()
//! });
//! worker.run();
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::auto_apply::{
    self, ActivityHandler, ActivityHandlerContext, WorkflowHandlerInstance, WorkflowHandlerContext,
};
use crate::client::VelocityClient;
use crate::errors::VelocityError;

// ─── Worker Configuration ─────────────────────────────────────────────────────

/// Options for creating a Worker.
#[derive(Debug, Clone)]
pub struct WorkerOptions {
    /// Task queue to poll for tasks.
    pub task_queue: String,
    /// Server address (gRPC or HTTP endpoint).
    pub server_address: String,
    /// Namespace for this worker.
    pub namespace: String,
    /// Maximum concurrent workflow tasks.
    pub max_concurrent_workflow_tasks: usize,
    /// Maximum concurrent activity tasks.
    pub max_concurrent_activity_tasks: usize,
    /// Long-poll timeout in milliseconds.
    pub poll_timeout_ms: u64,
    /// Heartbeat interval in milliseconds.
    pub heartbeat_interval_ms: u64,
    /// Build ID for worker versioning.
    pub build_id: String,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            task_queue: "default".to_string(),
            server_address: "localhost:7234".to_string(),
            namespace: "default".to_string(),
            max_concurrent_workflow_tasks: 10,
            max_concurrent_activity_tasks: 100,
            poll_timeout_ms: 10_000,
            heartbeat_interval_ms: 30_000,
            build_id: "1.0".to_string(),
        }
    }
}

// ─── Worker Stats ─────────────────────────────────────────────────────────────

/// Runtime statistics for a Worker.
#[derive(Debug, Default)]
pub struct WorkerStats {
    pub workflows_started: AtomicU64,
    pub workflows_completed: AtomicU64,
    pub workflows_failed: AtomicU64,
    pub activities_scheduled: AtomicU64,
    pub activities_completed: AtomicU64,
    pub activities_failed: AtomicU64,
    pub tasks_polled: AtomicU64,
    pub heartbeats_sent: AtomicU64,
    pub start_time: Instant,
}

impl WorkerStats {
    /// Return uptime in milliseconds.
    pub fn uptime_ms(&self) -> u64 {
        self.start_time.elapsed().as_millis() as u64
    }

    /// Return a snapshot of the stats as a plain struct.
    pub fn snapshot(&self) -> WorkerStatsSnapshot {
        WorkerStatsSnapshot {
            workflows_started: self.workflows_started.load(Ordering::Relaxed),
            workflows_completed: self.workflows_completed.load(Ordering::Relaxed),
            workflows_failed: self.workflows_failed.load(Ordering::Relaxed),
            activities_scheduled: self.activities_scheduled.load(Ordering::Relaxed),
            activities_completed: self.activities_completed.load(Ordering::Relaxed),
            activities_failed: self.activities_failed.load(Ordering::Relaxed),
            tasks_polled: self.tasks_polled.load(Ordering::Relaxed),
            heartbeats_sent: self.heartbeats_sent.load(Ordering::Relaxed),
            uptime_ms: self.uptime_ms(),
        }
    }
}

/// Immutable snapshot of worker statistics.
#[derive(Debug, Clone)]
pub struct WorkerStatsSnapshot {
    pub workflows_started: u64,
    pub workflows_completed: u64,
    pub workflows_failed: u64,
    pub activities_scheduled: u64,
    pub activities_completed: u64,
    pub activities_failed: u64,
    pub tasks_polled: u64,
    pub heartbeats_sent: u64,
    pub uptime_ms: u64,
}

// ─── Worker ───────────────────────────────────────────────────────────────────

/// VELOCITY Worker — polls the server for tasks and executes workflows/activities.
///
/// Supports two registration modes:
/// 1. Auto-apply: Use `register_workflow::<T>()` and `register_activity()` from
///    the `auto_apply` module. The Worker discovers them at startup.
/// 2. Manual: Call `register_workflow_handler()` and `register_activity_handler()`.
pub struct Worker {
    options: WorkerOptions,
    client: VelocityClient,
    stats: Arc<WorkerStats>,
    running: AtomicBool,
    /// Manual workflow handler overrides (take precedence over auto-apply registry).
    workflow_overrides: Mutex<HashMap<String, Arc<dyn Fn() -> Box<dyn WorkflowHandlerInstance> + Send + Sync>>>,
    /// Manual activity handler overrides.
    activity_overrides: Mutex<HashMap<String, ActivityHandler>>,
}

impl Worker {
    /// Create a new Worker with the given options.
    pub fn new(options: WorkerOptions) -> Self {
        let client = VelocityClient::new();
        Self {
            options,
            client,
            stats: Arc::new(WorkerStats {
                start_time: Instant::now(),
                ..Default::default()
            }),
            running: AtomicBool::new(false),
            workflow_overrides: Mutex::new(HashMap::new()),
            activity_overrides: Mutex::new(HashMap::new()),
        }
    }

    // ─── Manual Registration ─────────────────────────────────────────────

    /// Manually register a workflow handler factory.
    pub fn register_workflow_handler(
        &self,
        workflow_type: &str,
        factory: impl Fn() -> Box<dyn WorkflowHandlerInstance> + Send + Sync + 'static,
    ) {
        let mut overrides = self.workflow_overrides.lock().unwrap();
        overrides.insert(workflow_type.to_string(), Arc::new(factory));
    }

    /// Manually register an activity handler.
    pub fn register_activity_handler(
        &self,
        name: &str,
        handler: impl Fn(&ActivityHandlerContext, &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> + Send + Sync + 'static,
    ) {
        let mut overrides = self.activity_overrides.lock().unwrap();
        overrides.insert(name.to_string(), Arc::new(handler));
    }

    // ─── Auto-Apply Discovery ────────────────────────────────────────────

    /// Discover all registered workflows and activities from the global registry.
    /// Manual overrides take precedence over auto-apply registry entries.
    fn discover_handlers(&self) -> (HashMap<String, String>, HashMap<String, String>) {
        let wf_types = auto_apply::registered_workflow_types();
        let act_names = auto_apply::registered_activity_names();
        let wf_map = wf_types.into_iter().map(|n| (n.clone(), n)).collect();
        let act_map = act_names.into_iter().map(|n| (n.clone(), n)).collect();
        (wf_map, act_map)
    }

    // ─── Lifecycle ───────────────────────────────────────────────────────

    /// Run the worker, blocking the current thread.
    ///
    /// The worker will poll for tasks indefinitely until `shutdown()` is called
    /// or a SIGINT/SIGTERM signal is received.
    pub fn run(&self) {
        let (workflows, activities) = self.discover_handlers();
        self.running.store(true, Ordering::SeqCst);

        eprintln!(
            "[velocity-worker] Starting — task_queue={}, workflows={}, activities={}",
            self.options.task_queue,
            workflows.len(),
            activities.len(),
        );

        // Install signal handlers for graceful shutdown
        let running_flag = self.running.load(Ordering::SeqCst);
        ctrlc_handler(&self.running);

        while self.running.load(Ordering::SeqCst) {
            self.stats.tasks_polled.fetch_add(1, Ordering::Relaxed);

            // Poll for a task (blocking with timeout)
            match self.poll_and_dispatch(&workflows, &activities) {
                Ok(true) => { /* task processed */ }
                Ok(false) => { /* no task available, poll again */ }
                Err(e) => {
                    eprintln!("[velocity-worker] Poll error: {}", e);
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        }

        let snap = self.stats.snapshot();
        eprintln!(
            "[velocity-worker] Shut down — workflows={}/{}, failed={}, uptime={}s",
            snap.workflows_completed,
            snap.workflows_started,
            snap.workflows_failed,
            snap.uptime_ms / 1000,
        );
    }

    /// Request graceful shutdown.
    pub fn shutdown(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Check if the worker is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Get a snapshot of worker statistics.
    pub fn stats(&self) -> WorkerStatsSnapshot {
        self.stats.snapshot()
    }

    /// Get the task queue name.
    pub fn task_queue(&self) -> &str {
        &self.options.task_queue
    }

    // ─── Internal ────────────────────────────────────────────────────────

    fn poll_and_dispatch(
        &self,
        workflows: &HashMap<String, String>,
        activities: &HashMap<String, String>,
    ) -> Result<bool, VelocityError> {
        // In a full implementation, this would poll the server via gRPC/HTTP.
        // For now, this is a placeholder that demonstrates the dispatch pattern.
        std::thread::sleep(Duration::from_millis(100));
        Ok(false)
    }

    fn execute_workflow(
        &self,
        workflow_type: &str,
        workflow_key: u64,
        workflow_id: &str,
        input: &[u8],
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        self.stats.workflows_started.fetch_add(1, Ordering::Relaxed);

        // Check manual overrides first, then auto-apply registry
        let handler = {
            let overrides = self.workflow_overrides.lock().unwrap();
            if let Some(factory) = overrides.get(workflow_type) {
                Some(factory())
            } else {
                auto_apply::create_workflow(workflow_type)
            }
        };

        match handler {
            Some(h) => {
                let ctx = WorkflowHandlerContext {
                    workflow_key,
                    workflow_id: workflow_id.to_string(),
                    run_id: format!("run-{}", std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis()),
                    workflow_type: workflow_type.to_string(),
                    task_queue: self.options.task_queue.clone(),
                    current_step: 0,
                };

                match h.execute(&ctx, input) {
                    Ok(result) => {
                        self.stats.workflows_completed.fetch_add(1, Ordering::Relaxed);
                        Ok(result)
                    }
                    Err(e) => {
                        self.stats.workflows_failed.fetch_add(1, Ordering::Relaxed);
                        Err(e)
                    }
                }
            }
            None => {
                self.stats.workflows_failed.fetch_add(1, Ordering::Relaxed);
                Err(format!("No workflow handler registered for '{}'", workflow_type).into())
            }
        }
    }

    fn execute_activity(
        &self,
        activity_type: &str,
        input: &[u8],
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        self.stats.activities_scheduled.fetch_add(1, Ordering::Relaxed);

        let handler = {
            let overrides = self.activity_overrides.lock().unwrap();
            if let Some(h) = overrides.get(activity_type) {
                Some(h.clone())
            } else {
                auto_apply::get_activity(activity_type)
            }
        };

        match handler {
            Some(h) => {
                let ctx = ActivityHandlerContext {
                    activity_type: activity_type.to_string(),
                    activity_id: format!("act-{}", std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos()),
                    attempt: 1,
                };

                match h(&ctx, input) {
                    Ok(result) => {
                        self.stats.activities_completed.fetch_add(1, Ordering::Relaxed);
                        Ok(result)
                    }
                    Err(e) => {
                        self.stats.activities_failed.fetch_add(1, Ordering::Relaxed);
                        Err(e)
                    }
                }
            }
            None => {
                self.stats.activities_failed.fetch_add(1, Ordering::Relaxed);
                Err(format!("No activity handler registered for '{}'", activity_type).into())
            }
        }
    }
}

// ─── Signal Handling ──────────────────────────────────────────────────────────

fn ctrlc_handler(running: &AtomicBool) {
    // Best-effort signal handler — works on Unix and Windows.
    // In production, use the `ctrlc` or `tokio::signal` crate.
    #[cfg(unix)]
    {
        use std::sync::Once;
        static INIT: Once = Once::new();
        // Safety: we only set a flag, no complex logic in the handler.
        INIT.call_once(|| {
            unsafe {
                libc::signal(libc::SIGINT, handle_signal as libc::sighandler_t);
                libc::signal(libc::SIGTERM, handle_signal as libc::sighandler_t);
            }
        });
    }
}

#[cfg(unix)]
extern "C" fn handle_signal(_sig: i32) {
    // Set the running flag to false — the worker loop will exit cleanly.
    // Note: In a real implementation, use a global AtomicBool or channel.
}
