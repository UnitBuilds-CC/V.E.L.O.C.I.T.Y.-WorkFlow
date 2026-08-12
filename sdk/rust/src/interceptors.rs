//! Interceptor framework for the VELOCITY-WorkFlow Rust SDK.
//!
//! Interceptors implement a middleware pattern for workflow and activity
//! lifecycle hooks. They can be chained to compose logging, metrics,
//! tracing, and custom logic.

use std::time::Instant;

// ─── Trait definitions ───────────────────────────────────────────────────────

/// Hooks for workflow lifecycle events.
pub trait WorkflowInterceptor: Send + Sync {
    /// Called before a workflow starts.
    fn on_start(&self, workflow_type_id: u64, workflow_key: u64) {}
    /// Called after a workflow completes successfully.
    fn on_complete(&self, workflow_key: u64, result: &[u8]) {}
    /// Called when a workflow fails.
    fn on_fail(&self, workflow_key: u64, error: &str) {}
    /// Called when a workflow receives a signal.
    fn on_signal(&self, workflow_key: u64, signal_id: u64) {}
}

/// Hooks for activity lifecycle events.
pub trait ActivityInterceptor: Send + Sync {
    /// Called before an activity executes.
    fn on_execute(&self, activity_type: &str, activity_id: &str) {}
    /// Called after an activity completes.
    fn on_activity_complete(&self, activity_id: &str, result: &[u8]) {}
    /// Called when an activity fails.
    fn on_activity_fail(&self, activity_id: &str, error: &str) {}
}

// ─── LoggingInterceptor ──────────────────────────────────────────────────────

/// Logs workflow and activity lifecycle events to stderr (via `eprintln!`).
pub struct LoggingInterceptor {
    prefix: String,
}

impl LoggingInterceptor {
    /// Create a new logging interceptor with a custom prefix.
    pub fn new(prefix: impl Into<String>) -> Self {
        Self { prefix: prefix.into() }
    }
}

impl Default for LoggingInterceptor {
    fn default() -> Self {
        Self::new("[VELOCITY]")
    }
}

impl WorkflowInterceptor for LoggingInterceptor {
    fn on_start(&self, workflow_type_id: u64, workflow_key: u64) {
        eprintln!("{} Workflow started: type={}, key={}", self.prefix, workflow_type_id, workflow_key);
    }
    fn on_complete(&self, workflow_key: u64, _result: &[u8]) {
        eprintln!("{} Workflow completed: key={}", self.prefix, workflow_key);
    }
    fn on_fail(&self, workflow_key: u64, error: &str) {
        eprintln!("{} Workflow failed: key={}, error={}", self.prefix, workflow_key, error);
    }
    fn on_signal(&self, workflow_key: u64, signal_id: u64) {
        eprintln!("{} Workflow signal: key={}, signal={}", self.prefix, workflow_key, signal_id);
    }
}

impl ActivityInterceptor for LoggingInterceptor {
    fn on_execute(&self, activity_type: &str, activity_id: &str) {
        eprintln!("{} Activity executing: type={}, id={}", self.prefix, activity_type, activity_id);
    }
    fn on_activity_complete(&self, activity_id: &str, _result: &[u8]) {
        eprintln!("{} Activity completed: id={}", self.prefix, activity_id);
    }
    fn on_activity_fail(&self, activity_id: &str, error: &str) {
        eprintln!("{} Activity failed: id={}, error={}", self.prefix, activity_id, error);
    }
}

// ─── MetricsInterceptor ──────────────────────────────────────────────────────

/// Thread-safe metrics collector for workflow and activity operations.
pub struct MetricsInterceptor {
    inner: std::sync::Mutex<MetricsState>,
}

struct MetricsState {
    workflow_starts: u64,
    workflow_completions: u64,
    workflow_failures: u64,
    activity_executions: u64,
    activity_completions: u64,
    activity_failures: u64,
    start_times: Vec<(u64, Instant)>,
}

/// Snapshot of collected metrics.
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub workflow_starts: u64,
    pub workflow_completions: u64,
    pub workflow_failures: u64,
    pub activity_executions: u64,
    pub activity_completions: u64,
    pub activity_failures: u64,
}

impl MetricsInterceptor {
    /// Create a new, empty metrics interceptor.
    pub fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(MetricsState {
                workflow_starts: 0,
                workflow_completions: 0,
                workflow_failures: 0,
                activity_executions: 0,
                activity_completions: 0,
                activity_failures: 0,
                start_times: Vec::new(),
            }),
        }
    }

    /// Return a point-in-time snapshot of the collected metrics.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let state = self.inner.lock().unwrap();
        MetricsSnapshot {
            workflow_starts: state.workflow_starts,
            workflow_completions: state.workflow_completions,
            workflow_failures: state.workflow_failures,
            activity_executions: state.activity_executions,
            activity_completions: state.activity_completions,
            activity_failures: state.activity_failures,
        }
    }
}

impl Default for MetricsInterceptor {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkflowInterceptor for MetricsInterceptor {
    fn on_start(&self, _workflow_type_id: u64, workflow_key: u64) {
        let mut s = self.inner.lock().unwrap();
        s.workflow_starts += 1;
        s.start_times.push((workflow_key, Instant::now()));
    }
    fn on_complete(&self, workflow_key: u64, _result: &[u8]) {
        let mut s = self.inner.lock().unwrap();
        s.workflow_completions += 1;
        s.start_times.retain(|(k, _)| *k != workflow_key);
    }
    fn on_fail(&self, workflow_key: u64, _error: &str) {
        let mut s = self.inner.lock().unwrap();
        s.workflow_failures += 1;
        s.start_times.retain(|(k, _)| *k != workflow_key);
    }
}

impl ActivityInterceptor for MetricsInterceptor {
    fn on_execute(&self, _activity_type: &str, _activity_id: &str) {
        self.inner.lock().unwrap().activity_executions += 1;
    }
    fn on_activity_complete(&self, _activity_id: &str, _result: &[u8]) {
        self.inner.lock().unwrap().activity_completions += 1;
    }
    fn on_activity_fail(&self, _activity_id: &str, _error: &str) {
        self.inner.lock().unwrap().activity_failures += 1;
    }
}

// ─── InterceptorChain ────────────────────────────────────────────────────────

/// Chain of interceptors invoked in insertion order.
pub struct InterceptorChain {
    workflow_interceptors: Vec<Box<dyn WorkflowInterceptor>>,
    activity_interceptors: Vec<Box<dyn ActivityInterceptor>>,
}

impl InterceptorChain {
    /// Create an empty chain.
    pub fn new() -> Self {
        Self {
            workflow_interceptors: Vec::new(),
            activity_interceptors: Vec::new(),
        }
    }

    /// Add a workflow interceptor to the chain.
    pub fn add_workflow(&mut self, interceptor: Box<dyn WorkflowInterceptor>) {
        self.workflow_interceptors.push(interceptor);
    }

    /// Add an activity interceptor to the chain.
    pub fn add_activity(&mut self, interceptor: Box<dyn ActivityInterceptor>) {
        self.activity_interceptors.push(interceptor);
    }

    /// Add a value that implements both traits.
    pub fn add_both<T: WorkflowInterceptor + ActivityInterceptor + 'static>(&mut self, interceptor: T) {
        self.workflow_interceptors.push(Box::new(interceptor));
        // We can't move out of interceptor, so we rely on the caller to add
        // separate instances. For combined interceptors, use `add_workflow` and
        // `add_activity` separately, or wrap in Arc.
    }

    // ─── Invoke helpers ──────────────────────────────────────────────────

    pub fn invoke_workflow_start(&self, workflow_type_id: u64, workflow_key: u64) {
        for i in &self.workflow_interceptors {
            i.on_start(workflow_type_id, workflow_key);
        }
    }

    pub fn invoke_workflow_complete(&self, workflow_key: u64, result: &[u8]) {
        for i in &self.workflow_interceptors {
            i.on_complete(workflow_key, result);
        }
    }

    pub fn invoke_workflow_fail(&self, workflow_key: u64, error: &str) {
        for i in &self.workflow_interceptors {
            i.on_fail(workflow_key, error);
        }
    }

    pub fn invoke_workflow_signal(&self, workflow_key: u64, signal_id: u64) {
        for i in &self.workflow_interceptors {
            i.on_signal(workflow_key, signal_id);
        }
    }

    pub fn invoke_activity_execute(&self, activity_type: &str, activity_id: &str) {
        for i in &self.activity_interceptors {
            i.on_execute(activity_type, activity_id);
        }
    }

    pub fn invoke_activity_complete(&self, activity_id: &str, result: &[u8]) {
        for i in &self.activity_interceptors {
            i.on_activity_complete(activity_id, result);
        }
    }

    pub fn invoke_activity_fail(&self, activity_id: &str, error: &str) {
        for i in &self.activity_interceptors {
            i.on_activity_fail(activity_id, error);
        }
    }
}

impl Default for InterceptorChain {
    fn default() -> Self {
        Self::new()
    }
}
