// Copyright (c) VELOCITY Suite. All rights reserved.
// Licensed under the MIT License.

//! VELOCITY Dev Server — One-command local development experience.
//!
//! Starts an in-memory workflow engine with HTTP API, gRPC, and web UI.
//! No external dependencies (no Postgres, no Cassandra) required.
//!
//! Usage:
//!   velocity-dev                    # Start with defaults
//!   velocity-dev --port 7233        # Custom HTTP port
//!   velocity-dev --namespace myapp  # Custom default namespace
//!   velocity-dev --log-level debug  # Verbose logging
//!   velocity-dev --ui-port 8233     # Custom UI port

use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::{broadcast, Notify};

// jemalloc — significantly faster allocator for allocation-heavy workloads
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// Hyper HTTP server (production-grade HTTP/1.1 with keep-alive)
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ServerBuilder;

// TLS support — optional, enabled via --tls-cert / --tls-key
use std::io::BufReader;
use tokio_rustls::TlsAcceptor;

// gRPC BenchmarkService — apples-to-apples comparison with Temporal.
mod grpc_bench;

/// Load TLS configuration from PEM certificate and key files.
/// Returns None if either path is empty (TLS disabled).
fn load_tls_config(cert_path: &str, key_path: &str) -> Option<tokio_rustls::TlsAcceptor> {
    if cert_path.is_empty() || key_path.is_empty() {
        return None;
    }

    let cert_file = std::fs::File::open(cert_path).ok()?;
    let key_file = std::fs::File::open(key_path).ok()?;

    let mut cert_reader = BufReader::new(cert_file);
    let certs: Vec<_> = rustls_pemfile::certs(&mut cert_reader)
        .filter_map(|c| c.ok())
        .collect();

    let mut key_reader = BufReader::new(key_file);
    let key = rustls_pemfile::private_key(&mut key_reader).ok()??;

    let config = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .ok()?;

    Some(TlsAcceptor::from(Arc::new(config)))
}

// ═══════════════════════════════════════════════════════════════════════════════
// CLI Configuration
// ═══════════════════════════════════════════════════════════════════════════════

/// VELOCITY Dev Server — Local development workflow engine.
///
/// Start a complete VELOCITY-WorkFlow server with in-memory storage.
/// Perfect for local development, testing, and CI/CD pipelines.
#[derive(Parser, Debug, Clone)]
#[command(name = "velocity-dev", version, about)]
struct DevServerConfig {
    /// HTTP API port.
    #[arg(short, long, default_value_t = 7233, env = "VELOCITY_DEV_PORT")]
    port: u16,

    /// gRPC port.
    #[arg(
        short = 'g',
        long,
        default_value_t = 7234,
        env = "VELOCITY_DEV_GRPC_PORT"
    )]
    grpc_port: u16,

    /// Web UI port (0 to disable).
    #[arg(
        short = 'u',
        long,
        default_value_t = 8233,
        env = "VELOCITY_DEV_UI_PORT"
    )]
    ui_port: u16,

    /// Default namespace to create on startup.
    #[arg(short, long, default_value = "default", env = "VELOCITY_DEV_NAMESPACE")]
    namespace: String,

    /// Log level (trace, debug, info, warn, error).
    #[arg(short, long, default_value = "info", env = "VELOCITY_DEV_LOG_LEVEL")]
    log_level: String,

    /// Number of history shards.
    #[arg(long, default_value_t = 4)]
    shards: u32,

    /// Workflow execution retention period in days.
    #[arg(long, default_value_t = 7)]
    retention_days: u32,

    /// Enable dynamic config updates via API.
    #[arg(long, default_value_t = true)]
    dynamic_config: bool,

    /// SQLite database path (empty for in-memory).
    #[arg(long, default_value = "")]
    sqlite_path: String,

    /// Enable cluster mode (multi-node simulation).
    #[arg(long, default_value_t = false)]
    cluster_mode: bool,

    /// Number of simulated cluster nodes.
    #[arg(long, default_value_t = 3)]
    cluster_nodes: u32,

    /// Enable auto-compaction.
    #[arg(long, default_value_t = true)]
    auto_compact: bool,

    /// Compaction interval in seconds.
    #[arg(long, default_value_t = 300)]
    compact_interval_secs: u64,

    /// Enable chaos testing mode.
    #[arg(long, default_value_t = false)]
    chaos: bool,

    /// IP address to bind to.
    #[arg(long, default_value = "127.0.0.1")]
    ip: String,

    /// Enable OpenTelemetry export.
    #[arg(long, default_value_t = false)]
    otel: bool,

    /// OpenTelemetry endpoint.
    #[arg(long, default_value = "http://localhost:4317")]
    otel_endpoint: String,

    /// Headless mode (no interactive console).
    #[arg(long, default_value_t = false)]
    headless: bool,

    /// Data directory for persistence.
    #[arg(long, default_value = "")]
    data_dir: String,

    /// Enable workflow search attributes.
    #[arg(long, default_value_t = true)]
    search_attributes: bool,

    /// Max workflow execution history size (events).
    #[arg(long, default_value_t = 50_000)]
    max_history_size: u64,

    /// Enable namespace-level rate limiting.
    #[arg(long, default_value_t = false)]
    rate_limiting: bool,

    /// Rate limit (requests per second per namespace).
    #[arg(long, default_value_t = 1000)]
    rate_limit_rps: u32,

    /// TLS certificate file path (PEM). Enables TLS on HTTP and gRPC servers.
    #[arg(long, default_value = "")]
    tls_cert: String,

    /// TLS private key file path (PEM).
    #[arg(long, default_value = "")]
    tls_key: String,

    /// Auth bearer token. When set, all /api/ requests require Authorization: Bearer <token>.
    #[arg(long, default_value = "", env = "VELOCITY_AUTH_TOKEN")]
    auth_token: String,
}

// ═══════════════════════════════════════════════════════════════════════════════
// In-Memory Engine State
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkflowExecution {
    workflow_id: String,
    run_id: String,
    workflow_type: String,
    task_queue: String,
    status: String,
    namespace: String,
    started_at: i64,
    closed_at: Option<i64>,
    history_length: u64,
    memo: HashMap<String, String>,
    search_attributes: HashMap<String, String>,
    parent_workflow_id: Option<String>,
    parent_run_id: Option<String>,
    attempt: u32,
    retry_policy: Option<RetryPolicy>,
    cancel_requested: bool,
    cron_schedule: Option<String>,
    execution_timeout_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RetryPolicy {
    initial_interval_ms: u64,
    max_attempts: u32,
    backoff_coefficient: f64,
    max_interval_ms: u64,
    non_retryable_errors: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActivityTask {
    activity_id: String,
    workflow_id: String,
    run_id: String,
    namespace: String,
    activity_type: String,
    input: serde_json::Value,
    status: String, // SCHEDULED, STARTED, COMPLETED, FAILED, CANCELLED
    attempt: u32,
    retry_policy: Option<RetryPolicy>,
    last_heartbeat: i64,
    heartbeat_timeout_ms: Option<i64>,
    scheduled_at: i64,
    started_at: Option<i64>,
    completed_at: Option<i64>,
    result: Option<serde_json::Value>,
    failure_reason: Option<String>,
    task_queue: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TimerEntry {
    timer_id: String,
    workflow_id: String,
    namespace: String,
    fire_at: i64,
    created_at: i64,
    status: String, // PENDING, FIRED, CANCELLED
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChildWorkflowHandle {
    child_workflow_id: String,
    child_run_id: String,
    parent_workflow_id: String,
    namespace: String,
    status: String, // RUNNING, COMPLETED, FAILED, CANCELLED
    started_at: i64,
    completed_at: Option<i64>,
    result: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UpdateEntry {
    update_id: String,
    workflow_id: String,
    namespace: String,
    update_name: String,
    payload: serde_json::Value,
    status: String, // ACCEPTED, COMPLETED, REJECTED
    result: Option<serde_json::Value>,
    created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Namespace {
    name: String,
    id: String,
    description: String,
    owner_email: String,
    state: String,
    retention_days: u32,
    created_at: i64,
    is_global: bool,
    data: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaskQueue {
    name: String,
    namespace: String,
    task_type: String,
    pollers: Vec<PollerInfo>,
    backlog_count: u64,
    last_poll_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PollerInfo {
    identity: String,
    last_access_time: i64,
    rate_per_second: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HistoryEvent {
    event_id: u64,
    event_type: String,
    event_time: i64,
    task_id: u64,
    attributes: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScheduleInfo {
    schedule_id: String,
    workflow_type: String,
    state: String,
    cron_schedule: String,
    last_action_time: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BatchOperationInfo {
    job_id: String,
    operation: String,
    status: String,
    total_workflows: u64,
    succeeded: u64,
    failed: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SearchResult {
    executions: Vec<WorkflowExecution>,
    next_page_token: Option<String>,
    total_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ServerStats {
    uptime_secs: u64,
    workflow_count: u64,
    running_workflows: u64,
    completed_workflows: u64,
    failed_workflows: u64,
    cancelled_workflows: u64,
    terminated_workflows: u64,
    namespace_count: u64,
    task_queue_count: u64,
    history_event_count: u64,
    signal_count: u64,
    query_count: u64,
    update_count: u64,
    activity_count: u64,
    active_activities: u64,
    pending_timers: u64,
    child_workflows: u64,
    memory_usage_bytes: u64,
    features: Vec<String>,
}

#[derive(Debug)]
struct DevEngine {
    config: DevServerConfig,
    started_at: Instant,
    workflows: RwLock<HashMap<String, WorkflowExecution>>,
    namespaces: RwLock<HashMap<String, Namespace>>,
    task_queues: RwLock<HashMap<String, TaskQueue>>,
    history: RwLock<HashMap<String, Vec<HistoryEvent>>>,
    signals: RwLock<HashMap<String, Vec<SignalEntry>>>,
    activities: RwLock<HashMap<String, ActivityTask>>,
    timers: RwLock<HashMap<String, TimerEntry>>,
    child_workflows: RwLock<HashMap<String, Vec<ChildWorkflowHandle>>>,
    updates: RwLock<HashMap<String, Vec<UpdateEntry>>>,
    stats: Arc<EngineStats>,
    shutdown: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SignalEntry {
    signal_name: String,
    input: serde_json::Value,
    identity: String,
    timestamp: i64,
}

/// Static feature list — allocated once, never changes.
const FEATURES_LIST: &[&str] = &[
    "signals",
    "queries",
    "updates",
    "cancellation",
    "child_workflows",
    "timers",
    "activities",
    "heartbeats",
    "retry",
    "continue_as_new",
    "search_attributes",
    "memo",
    "signal_with_start",
    "batch_operations",
    "cron",
    "replay",
    "reset",
    "namespace_mgmt",
    "worker_poll",
    "history_archival",
];

#[derive(Debug)]
struct EngineStats {
    workflow_count: AtomicU64,
    running_count: AtomicU64,
    completed_count: AtomicU64,
    failed_count: AtomicU64,
    cancelled_count: AtomicU64,
    terminated_count: AtomicU64,
    signal_count: AtomicU64,
    query_count: AtomicU64,
    update_count: AtomicU64,
    history_event_count: AtomicU64,
    activity_count: AtomicU64,
}

impl DevEngine {
    fn new(config: DevServerConfig) -> Arc<Self> {
        let engine = Arc::new(Self {
            config: config.clone(),
            started_at: Instant::now(),
            workflows: RwLock::new(HashMap::new()),
            namespaces: RwLock::new(HashMap::new()),
            task_queues: RwLock::new(HashMap::new()),
            history: RwLock::new(HashMap::new()),
            signals: RwLock::new(HashMap::new()),
            activities: RwLock::new(HashMap::new()),
            timers: RwLock::new(HashMap::new()),
            child_workflows: RwLock::new(HashMap::new()),
            updates: RwLock::new(HashMap::new()),
            stats: Arc::new(EngineStats {
                workflow_count: AtomicU64::new(0),
                running_count: AtomicU64::new(0),
                completed_count: AtomicU64::new(0),
                failed_count: AtomicU64::new(0),
                cancelled_count: AtomicU64::new(0),
                terminated_count: AtomicU64::new(0),
                signal_count: AtomicU64::new(0),
                query_count: AtomicU64::new(0),
                update_count: AtomicU64::new(0),
                history_event_count: AtomicU64::new(0),
                activity_count: AtomicU64::new(0),
            }),
            shutdown: Arc::new(AtomicBool::new(false)),
            shutdown_notify: Arc::new(Notify::new()),
        });

        // Create default namespace
        let default_ns = Namespace {
            name: config.namespace.clone(),
            id: format!("ns-{}", generate_id()),
            description: "Default namespace for local development".to_string(),
            owner_email: "dev@localhost".to_string(),
            state: "REGISTERED".to_string(),
            retention_days: config.retention_days,
            created_at: now_millis(),
            is_global: false,
            data: HashMap::new(),
        };
        engine
            .namespaces
            .write()
            .unwrap()
            .insert(config.namespace.clone(), default_ns);

        // Create system namespace
        let system_ns = Namespace {
            name: "velocity-system".to_string(),
            id: format!("ns-{}", generate_id()),
            description: "System namespace for internal workflows".to_string(),
            owner_email: "system@velocity".to_string(),
            state: "REGISTERED".to_string(),
            retention_days: 7,
            created_at: now_millis(),
            is_global: false,
            data: HashMap::new(),
        };
        engine
            .namespaces
            .write()
            .unwrap()
            .insert("velocity-system".to_string(), system_ns);

        engine
    }

    fn start_workflow(
        &self,
        namespace: &str,
        wf_type: &str,
        task_queue: &str,
        input: serde_json::Value,
        requested_id: &str,
    ) -> Result<WorkflowExecution, String> {
        let wf_id = if requested_id.is_empty() {
            format!("wf-{}", generate_id())
        } else {
            requested_id.to_string()
        };
        let run_id = generate_id();
        let now = now_millis();

        let execution = WorkflowExecution {
            workflow_id: wf_id.clone(),
            run_id: run_id.clone(),
            workflow_type: wf_type.to_string(),
            task_queue: task_queue.to_string(),
            status: "RUNNING".to_string(),
            namespace: namespace.to_string(),
            started_at: now,
            closed_at: None,
            history_length: 2,
            memo: HashMap::new(),
            search_attributes: HashMap::new(),
            parent_workflow_id: None,
            parent_run_id: None,
            attempt: 1,
            retry_policy: None,
            cancel_requested: false,
            cron_schedule: None,
            execution_timeout_ms: None,
        };

        let key = format!("{}/{}", namespace, wf_id);
        self.workflows
            .write()
            .unwrap()
            .insert(key.clone(), execution.clone());

        // Add history events
        let events = vec![
            HistoryEvent {
                event_id: 1,
                event_type: "WorkflowExecutionStarted".to_string(),
                event_time: now,
                task_id: 1,
                attributes: serde_json::json!({
                    "workflow_type": wf_type,
                    "task_queue": task_queue,
                    "input": input,
                    "workflow_run_timeout": "60s",
                    "workflow_task_timeout": "10s",
                }),
            },
            HistoryEvent {
                event_id: 2,
                event_type: "WorkflowTaskScheduled".to_string(),
                event_time: now,
                task_id: 2,
                attributes: serde_json::json!({
                    "task_queue": task_queue,
                    "start_to_close_timeout": "10s",
                    "attempt": 1,
                }),
            },
        ];
        self.history.write().unwrap().insert(key, events);

        // Register task queue
        let tq_key = format!("{}/{}", namespace, task_queue);
        let mut tqs = self.task_queues.write().unwrap();
        let tq = tqs.entry(tq_key).or_insert_with(|| TaskQueue {
            name: task_queue.to_string(),
            namespace: namespace.to_string(),
            task_type: "WORKFLOW".to_string(),
            pollers: Vec::new(),
            backlog_count: 0,
            last_poll_at: None,
        });
        tq.backlog_count += 1;

        self.stats.workflow_count.fetch_add(1, Ordering::Relaxed);
        self.stats.running_count.fetch_add(1, Ordering::Relaxed);
        self.stats
            .history_event_count
            .fetch_add(2, Ordering::Relaxed);

        Ok(execution)
    }

    fn signal_workflow(
        &self,
        namespace: &str,
        wf_id: &str,
        signal_name: &str,
        input: serde_json::Value,
    ) -> Result<(), String> {
        let key = format!("{}/{}", namespace, wf_id);
        let workflows = self.workflows.read().unwrap();
        if !workflows.contains_key(&key) {
            return Err(format!("Workflow {} not found", wf_id));
        }
        drop(workflows);

        let signal = SignalEntry {
            signal_name: signal_name.to_string(),
            input,
            identity: "dev-server".to_string(),
            timestamp: now_millis(),
        };
        self.signals
            .write()
            .unwrap()
            .entry(key.clone())
            .or_default()
            .push(signal);
        self.stats.signal_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn complete_workflow(
        &self,
        namespace: &str,
        wf_id: &str,
        result: serde_json::Value,
    ) -> Result<(), String> {
        let key = format!("{}/{}", namespace, wf_id);
        let mut workflows = self.workflows.write().unwrap();
        let wf = workflows.get_mut(&key).ok_or("Workflow not found")?;
        wf.status = "COMPLETED".to_string();
        wf.closed_at = Some(now_millis());

        let now = now_millis();
        let event = HistoryEvent {
            event_id: wf.history_length + 1,
            event_type: "WorkflowExecutionCompleted".to_string(),
            event_time: now,
            task_id: wf.history_length + 2,
            attributes: serde_json::json!({ "result": result }),
        };
        wf.history_length += 1;
        self.history
            .write()
            .unwrap()
            .entry(key)
            .or_default()
            .push(event);

        self.stats.running_count.fetch_sub(1, Ordering::Relaxed);
        self.stats.completed_count.fetch_add(1, Ordering::Relaxed);
        self.stats
            .history_event_count
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn fail_workflow(&self, namespace: &str, wf_id: &str, reason: &str) -> Result<(), String> {
        let key = format!("{}/{}", namespace, wf_id);
        let mut workflows = self.workflows.write().unwrap();
        let wf = workflows.get_mut(&key).ok_or("Workflow not found")?;
        wf.status = "FAILED".to_string();
        wf.closed_at = Some(now_millis());

        let now = now_millis();
        let event = HistoryEvent {
            event_id: wf.history_length + 1,
            event_type: "WorkflowExecutionFailed".to_string(),
            event_time: now,
            task_id: wf.history_length + 2,
            attributes: serde_json::json!({ "reason": reason }),
        };
        wf.history_length += 1;
        self.history
            .write()
            .unwrap()
            .entry(key)
            .or_default()
            .push(event);

        self.stats.running_count.fetch_sub(1, Ordering::Relaxed);
        self.stats.failed_count.fetch_add(1, Ordering::Relaxed);
        self.stats
            .history_event_count
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn get_workflow(&self, namespace: &str, wf_id: &str) -> Option<WorkflowExecution> {
        let key = format!("{}/{}", namespace, wf_id);
        self.workflows.read().unwrap().get(&key).cloned()
    }

    fn list_workflows(
        &self,
        namespace: &str,
        status_filter: Option<&str>,
        page_size: usize,
    ) -> SearchResult {
        let workflows = self.workflows.read().unwrap();
        let mut results: Vec<WorkflowExecution> = workflows
            .values()
            .filter(|w| w.namespace == namespace)
            .filter(|w| status_filter.map(|s| w.status == s).unwrap_or(true))
            .take(page_size)
            .cloned()
            .collect();
        results.sort_by(|a, b| b.started_at.cmp(&a.started_at));
        let total = results.len() as u64;
        SearchResult {
            executions: results,
            next_page_token: None,
            total_count: total,
        }
    }

    fn get_history(&self, namespace: &str, wf_id: &str) -> Vec<HistoryEvent> {
        let key = format!("{}/{}", namespace, wf_id);
        self.history
            .read()
            .unwrap()
            .get(&key)
            .cloned()
            .unwrap_or_default()
    }

    fn list_namespaces(&self) -> Vec<Namespace> {
        self.namespaces.read().unwrap().values().cloned().collect()
    }

    fn create_namespace(&self, name: &str, description: &str) -> Result<Namespace, String> {
        let mut namespaces = self.namespaces.write().unwrap();
        if namespaces.contains_key(name) {
            return Err(format!("Namespace {} already exists", name));
        }
        let ns = Namespace {
            name: name.to_string(),
            id: format!("ns-{}", generate_id()),
            description: description.to_string(),
            owner_email: String::new(),
            state: "REGISTERED".to_string(),
            retention_days: self.config.retention_days,
            created_at: now_millis(),
            is_global: false,
            data: HashMap::new(),
        };
        namespaces.insert(name.to_string(), ns.clone());
        Ok(ns)
    }

    fn list_task_queues(&self, namespace: &str) -> Vec<TaskQueue> {
        self.task_queues
            .read()
            .unwrap()
            .values()
            .filter(|tq| tq.namespace == namespace)
            .cloned()
            .collect()
    }

    fn list_schedules(&self) -> Vec<ScheduleInfo> {
        // Return empty list for now - schedules would be tracked in a real implementation
        vec![]
    }

    fn list_batch_operations(&self) -> Vec<BatchOperationInfo> {
        // Return empty list for now - batch operations would be tracked in a real implementation
        vec![]
    }

    fn get_task_queue(&self, namespace: &str, queue_name: &str) -> Option<TaskQueue> {
        let key = format!("{}/{}", namespace, queue_name);
        self.task_queues.read().unwrap().get(&key).cloned()
    }

    fn get_workflow_activities(&self, namespace: &str, workflow_id: &str) -> Vec<ActivityTask> {
        let activities = self.activities.read().unwrap();
        activities
            .values()
            .filter(|a| a.namespace == namespace && a.workflow_id == workflow_id)
            .cloned()
            .collect()
    }

    fn get_stats(&self) -> ServerStats {
        // Only acquire locks for computed counts — most stats are already atomic.
        let activities = self.activities.read().unwrap();
        let timers = self.timers.read().unwrap();
        let child_workflows = self.child_workflows.read().unwrap();

        let active_activities = activities
            .values()
            .filter(|a| a.status == "STARTED" || a.status == "SCHEDULED")
            .count() as u64;
        let pending_timers = timers.values().filter(|t| t.status == "PENDING").count() as u64;
        let total_children = child_workflows.values().map(|v| v.len() as u64).sum();

        // Namespace/task_queue counts — small maps, read lock is cheap.
        let namespaces = self.namespaces.read().unwrap();
        let task_queues = self.task_queues.read().unwrap();
        let namespace_count = namespaces.len() as u64;
        let task_queue_count = task_queues.len() as u64;

        ServerStats {
            uptime_secs: self.started_at.elapsed().as_secs(),
            workflow_count: self.stats.workflow_count.load(Ordering::Relaxed),
            running_workflows: self.stats.running_count.load(Ordering::Relaxed),
            completed_workflows: self.stats.completed_count.load(Ordering::Relaxed),
            failed_workflows: self.stats.failed_count.load(Ordering::Relaxed),
            cancelled_workflows: self.stats.cancelled_count.load(Ordering::Relaxed),
            terminated_workflows: self.stats.terminated_count.load(Ordering::Relaxed),
            namespace_count,
            task_queue_count,
            history_event_count: self.stats.history_event_count.load(Ordering::Relaxed),
            signal_count: self.stats.signal_count.load(Ordering::Relaxed),
            query_count: self.stats.query_count.load(Ordering::Relaxed),
            update_count: self.stats.update_count.load(Ordering::Relaxed),
            activity_count: self.stats.activity_count.load(Ordering::Relaxed),
            active_activities,
            pending_timers,
            child_workflows: total_children,
            memory_usage_bytes: Self::current_rss_bytes(),
            features: FEATURES_LIST.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// Get current process RSS (resident set size) in bytes using sysinfo.
    fn current_rss_bytes() -> u64 {
        let pid = sysinfo::Pid::from_u32(std::process::id());
        let mut sys = sysinfo::System::new();
        sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]));
        if let Some(process) = sys.process(pid) {
            process.memory()
        } else {
            0
        }
    }

    fn query_workflow(
        &self,
        namespace: &str,
        wf_id: &str,
        query_type: &str,
    ) -> Result<serde_json::Value, String> {
        let key = format!("{}/{}", namespace, wf_id);
        let workflows = self.workflows.read().unwrap();
        let wf = workflows.get(&key).ok_or("Workflow not found")?;
        self.stats.query_count.fetch_add(1, Ordering::Relaxed);

        match query_type {
            "__stack_trace" => Ok(serde_json::json!({
                "stack_traces": []
            })),
            "__open_sessions" => Ok(serde_json::json!({
                "sessions": []
            })),
            _ => Ok(serde_json::json!({
                "query_type": query_type,
                "workflow_id": wf.workflow_id,
                "status": wf.status,
                "result": null,
            })),
        }
    }

    fn terminate_workflow(&self, namespace: &str, wf_id: &str, reason: &str) -> Result<(), String> {
        let key = format!("{}/{}", namespace, wf_id);
        let mut workflows = self.workflows.write().unwrap();
        let wf = workflows.get_mut(&key).ok_or("Workflow not found")?;
        wf.status = "TERMINATED".to_string();
        wf.closed_at = Some(now_millis());

        let now = now_millis();
        let event = HistoryEvent {
            event_id: wf.history_length + 1,
            event_type: "WorkflowExecutionTerminated".to_string(),
            event_time: now,
            task_id: wf.history_length + 2,
            attributes: serde_json::json!({ "reason": reason }),
        };
        wf.history_length += 1;
        self.history
            .write()
            .unwrap()
            .entry(key)
            .or_default()
            .push(event);

        self.stats.running_count.fetch_sub(1, Ordering::Relaxed);
        Ok(())
    }

    fn reset_all(&self, namespace: &str) -> u64 {
        let mut workflows = self.workflows.write().unwrap();
        let keys: Vec<String> = workflows
            .keys()
            .filter(|k| k.starts_with(&format!("{}/", namespace)))
            .cloned()
            .collect();
        let count = keys.len() as u64;
        for key in &keys {
            workflows.remove(key);
        }
        let mut history = self.history.write().unwrap();
        for key in &keys {
            history.remove(key);
        }
        let mut signals = self.signals.write().unwrap();
        for key in &keys {
            signals.remove(key);
        }
        // Clear activities, timers, child workflows, updates for this namespace
        let mut activities = self.activities.write().unwrap();
        activities.retain(|_, a| a.namespace != namespace);
        let mut timers = self.timers.write().unwrap();
        timers.retain(|_, t| t.namespace != namespace);
        let mut child_workflows = self.child_workflows.write().unwrap();
        child_workflows.retain(|k, _| !k.starts_with(&format!("{}/", namespace)));
        let mut updates = self.updates.write().unwrap();
        updates.retain(|k, _| !k.starts_with(&format!("{}/", namespace)));
        // Reset stats
        self.stats.workflow_count.store(0, Ordering::Relaxed);
        self.stats.running_count.store(0, Ordering::Relaxed);
        self.stats.completed_count.store(0, Ordering::Relaxed);
        self.stats.failed_count.store(0, Ordering::Relaxed);
        self.stats.cancelled_count.store(0, Ordering::Relaxed);
        self.stats.terminated_count.store(0, Ordering::Relaxed);
        self.stats.signal_count.store(0, Ordering::Relaxed);
        self.stats.query_count.store(0, Ordering::Relaxed);
        self.stats.update_count.store(0, Ordering::Relaxed);
        self.stats.history_event_count.store(0, Ordering::Relaxed);
        self.stats.activity_count.store(0, Ordering::Relaxed);
        count
    }

    fn count_workflows(&self, namespace: &str, status_filter: &str) -> u64 {
        let workflows = self.workflows.read().unwrap();
        workflows
            .values()
            .filter(|w| w.namespace == namespace)
            .filter(|w| match status_filter {
                "running" => w.status == "RUNNING",
                "completed" => w.status == "COMPLETED",
                "failed" => w.status == "FAILED",
                "terminated" => w.status == "TERMINATED",
                _ => true, // "all" or anything else
            })
            .count() as u64
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Tier 1 — Core workflow features
    // ═══════════════════════════════════════════════════════════════════════════

    /// Cancel a running workflow — sets cancel_requested flag and transitions to CANCELLED.
    fn cancel_workflow(&self, namespace: &str, wf_id: &str, reason: &str) -> Result<(), String> {
        let key = format!("{}/{}", namespace, wf_id);
        let mut workflows = self.workflows.write().unwrap();
        let wf = workflows.get_mut(&key).ok_or("Workflow not found")?;
        if wf.status != "RUNNING" {
            return Err(format!("Workflow is {}, cannot cancel", wf.status));
        }
        wf.cancel_requested = true;
        wf.status = "CANCELLED".to_string();
        wf.closed_at = Some(now_millis());

        let now = now_millis();
        let event = HistoryEvent {
            event_id: wf.history_length + 1,
            event_type: "WorkflowExecutionCancelled".to_string(),
            event_time: now,
            task_id: wf.history_length + 2,
            attributes: serde_json::json!({ "reason": reason }),
        };
        wf.history_length += 1;
        self.history
            .write()
            .unwrap()
            .entry(key)
            .or_default()
            .push(event);

        self.stats.running_count.fetch_sub(1, Ordering::Relaxed);
        self.stats.cancelled_count.fetch_add(1, Ordering::Relaxed);
        self.stats
            .history_event_count
            .fetch_add(1, Ordering::Relaxed);

        // Cancel any pending timers for this workflow
        let mut timers = self.timers.write().unwrap();
        for (_, t) in timers.iter_mut() {
            if t.workflow_id == wf_id && t.namespace == namespace && t.status == "PENDING" {
                t.status = "CANCELLED".to_string();
            }
        }

        Ok(())
    }

    /// Update a running workflow's mutable state (Temporal's signal replacement).
    fn update_workflow(
        &self,
        namespace: &str,
        wf_id: &str,
        update_name: &str,
        update_id: &str,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let key = format!("{}/{}", namespace, wf_id);
        let workflows = self.workflows.read().unwrap();
        let wf = workflows.get(&key).ok_or("Workflow not found")?;
        if wf.status != "RUNNING" {
            return Err(format!("Workflow is {}, cannot update", wf.status));
        }
        drop(workflows);

        let uid = if update_id.is_empty() {
            format!("upd-{}", generate_id())
        } else {
            update_id.to_string()
        };
        let now = now_millis();

        let entry = UpdateEntry {
            update_id: uid.clone(),
            workflow_id: wf_id.to_string(),
            namespace: namespace.to_string(),
            update_name: update_name.to_string(),
            payload: payload.clone(),
            status: "COMPLETED".to_string(),
            result: Some(serde_json::json!({ "accepted": true, "update_name": update_name })),
            created_at: now,
        };
        self.updates
            .write()
            .unwrap()
            .entry(key.clone())
            .or_default()
            .push(entry);
        self.stats.update_count.fetch_add(1, Ordering::Relaxed);

        // Record in history
        let event = HistoryEvent {
            event_id: self
                .history
                .read()
                .unwrap()
                .get(&key)
                .map(|h| h.len())
                .unwrap_or(0) as u64
                + 1,
            event_type: "WorkflowExecutionUpdated".to_string(),
            event_time: now,
            task_id: now as u64,
            attributes: serde_json::json!({ "update_name": update_name, "update_id": uid, "payload": payload }),
        };
        self.history
            .write()
            .unwrap()
            .entry(key)
            .or_default()
            .push(event);
        self.stats
            .history_event_count
            .fetch_add(1, Ordering::Relaxed);

        Ok(serde_json::json!({ "update_id": uid, "status": "COMPLETED" }))
    }

    /// Start a child workflow from a parent.
    fn start_child_workflow(
        &self,
        namespace: &str,
        parent_wf_id: &str,
        wf_type: &str,
        wf_id: &str,
        task_queue: &str,
        input: serde_json::Value,
    ) -> Result<WorkflowExecution, String> {
        let parent_key = format!("{}/{}", namespace, parent_wf_id);
        let workflows = self.workflows.read().unwrap();
        if !workflows.contains_key(&parent_key) {
            return Err(format!("Parent workflow {} not found", parent_wf_id));
        }
        drop(workflows);

        let child_id = if wf_id.is_empty() {
            format!("child-{}", generate_id())
        } else {
            wf_id.to_string()
        };
        let mut exec = self.start_workflow(namespace, wf_type, task_queue, input, &child_id)?;
        exec.parent_workflow_id = Some(parent_wf_id.to_string());

        // Update the stored workflow with parent info
        let key = format!("{}/{}", namespace, child_id);
        {
            let mut workflows = self.workflows.write().unwrap();
            if let Some(wf) = workflows.get_mut(&key) {
                wf.parent_workflow_id = Some(parent_wf_id.to_string());
                exec = wf.clone();
            }
        }

        // Track child workflow handle
        let handle = ChildWorkflowHandle {
            child_workflow_id: exec.workflow_id.clone(),
            child_run_id: exec.run_id.clone(),
            parent_workflow_id: parent_wf_id.to_string(),
            namespace: namespace.to_string(),
            status: "RUNNING".to_string(),
            started_at: now_millis(),
            completed_at: None,
            result: None,
        };
        self.child_workflows
            .write()
            .unwrap()
            .entry(parent_key.clone())
            .or_default()
            .push(handle);

        // Record child-initiated event in parent history
        let event = HistoryEvent {
            event_id: self
                .history
                .read()
                .unwrap()
                .get(&parent_key)
                .map(|h| h.len())
                .unwrap_or(0) as u64
                + 1,
            event_type: "StartChildWorkflowExecutionInitiated".to_string(),
            event_time: now_millis(),
            task_id: now_millis() as u64,
            attributes: serde_json::json!({
                "child_workflow_id": exec.workflow_id,
                "workflow_type": wf_type,
                "task_queue": task_queue,
            }),
        };
        self.history
            .write()
            .unwrap()
            .entry(parent_key)
            .or_default()
            .push(event);
        self.stats
            .history_event_count
            .fetch_add(1, Ordering::Relaxed);

        Ok(exec)
    }

    /// Schedule a timer for a running workflow.
    fn schedule_timer(
        &self,
        namespace: &str,
        wf_id: &str,
        timer_id: &str,
        duration_ms: i64,
    ) -> Result<String, String> {
        let key = format!("{}/{}", namespace, wf_id);
        let workflows = self.workflows.read().unwrap();
        if !workflows.contains_key(&key) {
            return Err(format!("Workflow {} not found", wf_id));
        }
        drop(workflows);

        let tid = if timer_id.is_empty() {
            format!("timer-{}", generate_id())
        } else {
            timer_id.to_string()
        };
        let now = now_millis();
        let timer = TimerEntry {
            timer_id: tid.clone(),
            workflow_id: wf_id.to_string(),
            namespace: namespace.to_string(),
            fire_at: now + duration_ms,
            created_at: now,
            status: "PENDING".to_string(),
        };
        self.timers
            .write()
            .unwrap()
            .insert(format!("{}/{}", namespace, tid), timer);

        // Record timer event in history
        let event = HistoryEvent {
            event_id: self
                .history
                .read()
                .unwrap()
                .get(&key)
                .map(|h| h.len())
                .unwrap_or(0) as u64
                + 1,
            event_type: "TimerStarted".to_string(),
            event_time: now,
            task_id: now as u64,
            attributes: serde_json::json!({ "timer_id": tid, "duration_ms": duration_ms, "fire_at": now + duration_ms }),
        };
        self.history
            .write()
            .unwrap()
            .entry(key)
            .or_default()
            .push(event);
        self.stats
            .history_event_count
            .fetch_add(1, Ordering::Relaxed);

        Ok(tid)
    }

    /// Cancel a pending timer.
    fn cancel_timer(&self, namespace: &str, wf_id: &str, timer_id: &str) -> Result<(), String> {
        let tkey = format!("{}/{}", namespace, timer_id);
        let mut timers = self.timers.write().unwrap();
        match timers.get_mut(&tkey) {
            Some(t) if t.workflow_id == wf_id && t.status == "PENDING" => {
                t.status = "CANCELLED".to_string();
                Ok(())
            }
            Some(_) => Err("Timer is not in PENDING state".to_string()),
            None => Err(format!("Timer {} not found", timer_id)),
        }
    }

    /// Continue-as-new: complete current execution and start a fresh one.
    fn continue_as_new(
        &self,
        namespace: &str,
        wf_id: &str,
        wf_type: &str,
        task_queue: &str,
        input: serde_json::Value,
    ) -> Result<String, String> {
        let key = format!("{}/{}", namespace, wf_id);
        // Complete the current execution
        {
            let mut workflows = self.workflows.write().unwrap();
            let wf = workflows.get_mut(&key).ok_or("Workflow not found")?;
            wf.status = "CONTINUED_AS_NEW".to_string();
            wf.closed_at = Some(now_millis());
        }
        self.stats.running_count.fetch_sub(1, Ordering::Relaxed);
        self.stats.completed_count.fetch_add(1, Ordering::Relaxed);

        let now = now_millis();
        let event = HistoryEvent {
            event_id: self
                .history
                .read()
                .unwrap()
                .get(&key)
                .map(|h| h.len())
                .unwrap_or(0) as u64
                + 1,
            event_type: "WorkflowExecutionContinuedAsNew".to_string(),
            event_time: now,
            task_id: now as u64,
            attributes: serde_json::json!({ "new_workflow_type": wf_type, "task_queue": task_queue }),
        };
        self.history
            .write()
            .unwrap()
            .entry(key.clone())
            .or_default()
            .push(event);
        self.stats
            .history_event_count
            .fetch_add(1, Ordering::Relaxed);

        // Start a new execution with the same workflow ID
        let new_exec = self.start_workflow(namespace, wf_type, task_queue, input, wf_id)?;
        Ok(new_exec.run_id)
    }

    /// Upsert search attributes on a running workflow.
    fn upsert_search_attributes(
        &self,
        namespace: &str,
        wf_id: &str,
        attrs: HashMap<String, String>,
    ) -> Result<(), String> {
        let key = format!("{}/{}", namespace, wf_id);
        let mut workflows = self.workflows.write().unwrap();
        let wf = workflows.get_mut(&key).ok_or("Workflow not found")?;
        for (k, v) in &attrs {
            wf.search_attributes.insert(k.clone(), v.clone());
        }

        let now = now_millis();
        let event = HistoryEvent {
            event_id: wf.history_length + 1,
            event_type: "UpsertWorkflowSearchAttributes".to_string(),
            event_time: now,
            task_id: now as u64,
            attributes: serde_json::json!({ "attributes": attrs }),
        };
        wf.history_length += 1;
        self.history
            .write()
            .unwrap()
            .entry(key)
            .or_default()
            .push(event);
        self.stats
            .history_event_count
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Set memo key/value pairs on a running workflow.
    fn set_memo(
        &self,
        namespace: &str,
        wf_id: &str,
        memo: HashMap<String, String>,
    ) -> Result<(), String> {
        let key = format!("{}/{}", namespace, wf_id);
        let mut workflows = self.workflows.write().unwrap();
        let wf = workflows.get_mut(&key).ok_or("Workflow not found")?;
        for (k, v) in &memo {
            wf.memo.insert(k.clone(), v.clone());
        }
        Ok(())
    }

    /// Signal-with-start: signal existing workflow or start a new one and signal it.
    #[allow(clippy::too_many_arguments)]
    fn signal_with_start(
        &self,
        namespace: &str,
        wf_type: &str,
        wf_id: &str,
        task_queue: &str,
        input: serde_json::Value,
        signal_name: &str,
        signal_payload: serde_json::Value,
    ) -> Result<(WorkflowExecution, bool, bool), String> {
        let key = format!("{}/{}", namespace, wf_id);
        // Check if workflow already exists and is running
        let existing = {
            let workflows = self.workflows.read().unwrap();
            workflows
                .get(&key)
                .filter(|w| w.status == "RUNNING")
                .cloned()
        };

        match existing {
            Some(wf) => {
                // Just signal the existing workflow
                self.signal_workflow(namespace, wf_id, signal_name, signal_payload)?;
                Ok((wf, false, true))
            }
            None => {
                // Start a new workflow and signal it
                let exec = self.start_workflow(namespace, wf_type, task_queue, input, wf_id)?;
                self.signal_workflow(namespace, wf_id, signal_name, signal_payload)?;
                Ok((exec, true, true))
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Tier 2 — Activity & operational features
    // ═══════════════════════════════════════════════════════════════════════════

    /// Record an activity heartbeat.
    fn record_heartbeat(
        &self,
        namespace: &str,
        wf_id: &str,
        activity_id: &str,
        _details: serde_json::Value,
    ) -> Result<bool, String> {
        let akey = format!("{}/{}/{}", namespace, wf_id, activity_id);
        let mut activities = self.activities.write().unwrap();
        let act = activities.get_mut(&akey).ok_or("Activity not found")?;
        act.last_heartbeat = now_millis();

        // Check if cancellation was requested on the parent workflow
        let wf_key = format!("{}/{}", namespace, wf_id);
        let cancel_requested = self
            .workflows
            .read()
            .unwrap()
            .get(&wf_key)
            .map(|w| w.cancel_requested)
            .unwrap_or(false);
        Ok(cancel_requested)
    }

    /// Schedule an activity task for a workflow.
    #[allow(clippy::too_many_arguments)]
    fn schedule_activity(
        &self,
        namespace: &str,
        wf_id: &str,
        run_id: &str,
        activity_id: &str,
        activity_type: &str,
        task_queue: &str,
        input: serde_json::Value,
        heartbeat_timeout_ms: Option<i64>,
    ) -> Result<ActivityTask, String> {
        let key = format!("{}/{}", namespace, wf_id);
        let workflows = self.workflows.read().unwrap();
        if !workflows.contains_key(&key) {
            return Err(format!("Workflow {} not found", wf_id));
        }
        drop(workflows);

        let aid = if activity_id.is_empty() {
            format!("act-{}", generate_id())
        } else {
            activity_id.to_string()
        };
        let now = now_millis();
        let akey = format!("{}/{}/{}", namespace, wf_id, aid);

        let activity = ActivityTask {
            activity_id: aid.clone(),
            workflow_id: wf_id.to_string(),
            run_id: run_id.to_string(),
            namespace: namespace.to_string(),
            activity_type: activity_type.to_string(),
            input,
            status: "SCHEDULED".to_string(),
            attempt: 1,
            retry_policy: None,
            last_heartbeat: now,
            heartbeat_timeout_ms,
            scheduled_at: now,
            started_at: None,
            completed_at: None,
            result: None,
            failure_reason: None,
            task_queue: task_queue.to_string(),
        };
        self.activities
            .write()
            .unwrap()
            .insert(akey, activity.clone());
        self.stats.activity_count.fetch_add(1, Ordering::Relaxed);

        // Record in history
        let event = HistoryEvent {
            event_id: self
                .history
                .read()
                .unwrap()
                .get(&key)
                .map(|h| h.len())
                .unwrap_or(0) as u64
                + 1,
            event_type: "ActivityTaskScheduled".to_string(),
            event_time: now,
            task_id: now as u64,
            attributes: serde_json::json!({
                "activity_id": aid, "activity_type": activity_type,
                "task_queue": task_queue, "attempt": 1,
            }),
        };
        self.history
            .write()
            .unwrap()
            .entry(key)
            .or_default()
            .push(event);
        self.stats
            .history_event_count
            .fetch_add(1, Ordering::Relaxed);

        Ok(activity)
    }

    /// Complete an activity task.
    fn complete_activity(
        &self,
        namespace: &str,
        wf_id: &str,
        activity_id: &str,
        result: serde_json::Value,
    ) -> Result<(), String> {
        let akey = format!("{}/{}/{}", namespace, wf_id, activity_id);
        let mut activities = self.activities.write().unwrap();
        let act = activities.get_mut(&akey).ok_or("Activity not found")?;
        act.status = "COMPLETED".to_string();
        act.completed_at = Some(now_millis());
        act.result = Some(result.clone());

        // Record in workflow history
        let wf_key = format!("{}/{}", namespace, wf_id);
        let now = now_millis();
        let event = HistoryEvent {
            event_id: self
                .history
                .read()
                .unwrap()
                .get(&wf_key)
                .map(|h| h.len())
                .unwrap_or(0) as u64
                + 1,
            event_type: "ActivityTaskCompleted".to_string(),
            event_time: now,
            task_id: now as u64,
            attributes: serde_json::json!({ "activity_id": activity_id, "result": result }),
        };
        self.history
            .write()
            .unwrap()
            .entry(wf_key)
            .or_default()
            .push(event);
        self.stats
            .history_event_count
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Fail an activity task (triggers retry if policy allows).
    fn fail_activity(
        &self,
        namespace: &str,
        wf_id: &str,
        activity_id: &str,
        reason: &str,
        non_retryable: bool,
    ) -> Result<(bool, u32), String> {
        let akey = format!("{}/{}/{}", namespace, wf_id, activity_id);
        let mut activities = self.activities.write().unwrap();
        let act = activities.get_mut(&akey).ok_or("Activity not found")?;

        let will_retry = !non_retryable
            && act.attempt
                < act
                    .retry_policy
                    .as_ref()
                    .map(|r| r.max_attempts)
                    .unwrap_or(3);
        if will_retry {
            act.attempt += 1;
            act.status = "SCHEDULED".to_string();
            act.failure_reason = Some(reason.to_string());
        } else {
            act.status = "FAILED".to_string();
            act.completed_at = Some(now_millis());
            act.failure_reason = Some(reason.to_string());
        }

        // Record in history
        let wf_key = format!("{}/{}", namespace, wf_id);
        let now = now_millis();
        let event_type = if will_retry {
            "ActivityTaskRetry"
        } else {
            "ActivityTaskFailed"
        };
        let event = HistoryEvent {
            event_id: self
                .history
                .read()
                .unwrap()
                .get(&wf_key)
                .map(|h| h.len())
                .unwrap_or(0) as u64
                + 1,
            event_type: event_type.to_string(),
            event_time: now,
            task_id: now as u64,
            attributes: serde_json::json!({ "activity_id": activity_id, "reason": reason, "attempt": act.attempt, "will_retry": will_retry }),
        };
        self.history
            .write()
            .unwrap()
            .entry(wf_key)
            .or_default()
            .push(event);
        self.stats
            .history_event_count
            .fetch_add(1, Ordering::Relaxed);

        Ok((will_retry, act.attempt))
    }

    /// Replay a workflow from its history — validates event chain and returns final status.
    fn replay_workflow(&self, namespace: &str, wf_id: &str) -> Result<(u64, String), String> {
        let key = format!("{}/{}", namespace, wf_id);
        let history = self.history.read().unwrap();
        let events = history.get(&key).ok_or("Workflow not found")?;
        let event_count = events.len() as u64;

        // Validate event chain: event IDs must be sequential starting from 1
        for (i, event) in events.iter().enumerate() {
            if event.event_id != (i as u64 + 1) {
                return Err(format!(
                    "Event ID gap at position {}: expected {}, got {}",
                    i,
                    i + 1,
                    event.event_id
                ));
            }
        }

        // Determine final status from last event
        let final_status = events
            .last()
            .map(|e| match e.event_type.as_str() {
                "WorkflowExecutionCompleted" => "COMPLETED".to_string(),
                "WorkflowExecutionFailed" => "FAILED".to_string(),
                "WorkflowExecutionTerminated" => "TERMINATED".to_string(),
                "WorkflowExecutionCancelled" => "CANCELLED".to_string(),
                "WorkflowExecutionContinuedAsNew" => "CONTINUED_AS_NEW".to_string(),
                _ => "RUNNING".to_string(),
            })
            .unwrap_or_else(|| "UNKNOWN".to_string());

        Ok((event_count, final_status))
    }

    /// Reset a workflow to a previous event ID — creates a new run from that point.
    fn reset_workflow(
        &self,
        namespace: &str,
        wf_id: &str,
        reset_to_event_id: i64,
        reason: &str,
    ) -> Result<String, String> {
        let key = format!("{}/{}", namespace, wf_id);
        let history = self.history.read().unwrap();
        let events = history.get(&key).ok_or("Workflow not found")?;
        if reset_to_event_id <= 0 || reset_to_event_id > events.len() as i64 {
            return Err(format!(
                "Event ID {} out of range (1..{})",
                reset_to_event_id,
                events.len()
            ));
        }
        let workflows = self.workflows.read().unwrap();
        let wf = workflows.get(&key).ok_or("Workflow not found")?;
        let wf_type = wf.workflow_type.clone();
        let task_queue = wf.task_queue.clone();
        drop(workflows);
        drop(history);

        // Start a new run with the same workflow ID
        let input =
            serde_json::json!({ "__reset_from_event": reset_to_event_id, "__reason": reason });
        let new_exec = self.start_workflow(namespace, &wf_type, &task_queue, input, wf_id)?;

        // Record reset event
        let event = HistoryEvent {
            event_id: self
                .history
                .read()
                .unwrap()
                .get(&key)
                .map(|h| h.len())
                .unwrap_or(0) as u64
                + 1,
            event_type: "WorkflowExecutionReset".to_string(),
            event_time: now_millis(),
            task_id: now_millis() as u64,
            attributes: serde_json::json!({ "reset_to_event_id": reset_to_event_id, "reason": reason, "new_run_id": new_exec.run_id }),
        };
        self.history
            .write()
            .unwrap()
            .entry(key)
            .or_default()
            .push(event);
        self.stats
            .history_event_count
            .fetch_add(1, Ordering::Relaxed);

        Ok(new_exec.run_id)
    }

    /// Batch terminate all running workflows in a namespace.
    fn batch_terminate(
        &self,
        namespace: &str,
        status_filter: &str,
        reason: &str,
        max_count: i64,
    ) -> u64 {
        let workflows = self.workflows.read().unwrap();
        let keys: Vec<String> = workflows
            .values()
            .filter(|w| w.namespace == namespace)
            .filter(|w| match status_filter {
                "running" => w.status == "RUNNING",
                _ => w.status == "RUNNING" || w.status == "STARTED",
            })
            .map(|w| format!("{}/{}", w.namespace, w.workflow_id))
            .take(if max_count > 0 {
                max_count as usize
            } else {
                usize::MAX
            })
            .collect();
        drop(workflows);

        let mut count = 0u64;
        for key in &keys {
            let parts: Vec<&str> = key.splitn(2, '/').collect();
            if parts.len() == 2 && self.terminate_workflow(parts[0], parts[1], reason).is_ok() {
                count += 1;
            }
        }
        count
    }

    /// Batch signal all running workflows in a namespace.
    fn batch_signal(
        &self,
        namespace: &str,
        status_filter: &str,
        signal_name: &str,
        payload: serde_json::Value,
        max_count: i64,
    ) -> u64 {
        let workflows = self.workflows.read().unwrap();
        let targets: Vec<(String, String)> = workflows
            .values()
            .filter(|w| w.namespace == namespace)
            .filter(|w| match status_filter {
                "running" => w.status == "RUNNING",
                _ => w.status == "RUNNING",
            })
            .map(|w| (w.namespace.clone(), w.workflow_id.clone()))
            .take(if max_count > 0 {
                max_count as usize
            } else {
                usize::MAX
            })
            .collect();
        drop(workflows);

        let mut count = 0u64;
        for (ns, wf_id) in &targets {
            if self
                .signal_workflow(ns, wf_id, signal_name, payload.clone())
                .is_ok()
            {
                count += 1;
            }
        }
        count
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Tier 3 — Namespace management & production features
    // ═══════════════════════════════════════════════════════════════════════════

    /// Describe a namespace.
    fn describe_namespace(&self, name: &str) -> Result<Namespace, String> {
        self.namespaces
            .read()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or(format!("Namespace {} not found", name))
    }

    /// Update a namespace's configuration.
    fn update_namespace(
        &self,
        name: &str,
        description: Option<&str>,
        retention_days: Option<u32>,
        owner_email: Option<&str>,
    ) -> Result<(), String> {
        let mut namespaces = self.namespaces.write().unwrap();
        let ns = namespaces
            .get_mut(name)
            .ok_or(format!("Namespace {} not found", name))?;
        if let Some(d) = description {
            ns.description = d.to_string();
        }
        if let Some(r) = retention_days {
            ns.retention_days = r;
        }
        if let Some(e) = owner_email {
            ns.owner_email = e.to_string();
        }
        Ok(())
    }

    /// Delete a namespace and all its workflows.
    fn delete_namespace(&self, name: &str) -> Result<(), String> {
        if name == self.config.namespace {
            return Err("Cannot delete the default namespace".to_string());
        }
        let mut namespaces = self.namespaces.write().unwrap();
        if !namespaces.contains_key(name) {
            return Err(format!("Namespace {} not found", name));
        }
        namespaces.remove(name);
        drop(namespaces);
        // Clean up all workflows in this namespace
        self.reset_all(name);
        Ok(())
    }

    /// Poll for a workflow task from a task queue (worker poll loop).
    fn poll_workflow_task(
        &self,
        namespace: &str,
        task_queue: &str,
        identity: &str,
    ) -> Option<(String, u64, String)> {
        let tq_key = format!("{}/{}", namespace, task_queue);
        let mut tqs = self.task_queues.write().unwrap();
        if let Some(tq) = tqs.get_mut(&tq_key) {
            tq.last_poll_at = Some(now_millis());
            // Update poller info
            if let Some(poller) = tq.pollers.iter_mut().find(|p| p.identity == identity) {
                poller.last_access_time = now_millis();
            } else {
                tq.pollers.push(PollerInfo {
                    identity: identity.to_string(),
                    last_access_time: now_millis(),
                    rate_per_second: 0.0,
                });
            }
        }

        // Find a running workflow with pending tasks for this queue
        let workflows = self.workflows.read().unwrap();
        for wf in workflows.values() {
            if wf.namespace == namespace && wf.task_queue == task_queue && wf.status == "RUNNING" {
                let key = format!("{}/{}", namespace, wf.workflow_id);
                let history = self.history.read().unwrap();
                if let Some(events) = history.get(&key) {
                    if let Some(last_event) = events.last() {
                        let task_token = format!("wt-{}-{}", wf.workflow_id, generate_id());
                        return Some((
                            task_token,
                            last_event.event_id,
                            last_event.event_type.clone(),
                        ));
                    }
                }
            }
        }
        None
    }

    /// Poll for an activity task from a task queue.
    fn poll_activity_task(
        &self,
        namespace: &str,
        task_queue: &str,
        _identity: &str,
    ) -> Option<ActivityTask> {
        let mut activities = self.activities.write().unwrap();
        for act in activities.values_mut() {
            if act.namespace == namespace
                && act.task_queue == task_queue
                && act.status == "SCHEDULED"
            {
                act.status = "STARTED".to_string();
                act.started_at = Some(now_millis());
                return Some(act.clone());
            }
        }
        None
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// HTTP API Server
// ═══════════════════════════════════════════════════════════════════════════════

async fn run_http_server(
    engine: Arc<DevEngine>,
    addr: SocketAddr,
    mut shutdown_rx: broadcast::Receiver<()>,
    tls_acceptor: Option<TlsAcceptor>,
) {
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("Failed to bind HTTP server to {}: {}", addr, e);
            return;
        }
    };
    if tls_acceptor.is_some() {
        tracing::info!(
            "HTTP API listening on https://{} (Hyper + TLS, keep-alive)",
            addr
        );
    } else {
        tracing::info!("HTTP API listening on http://{} (Hyper, keep-alive)", addr);
    }

    let (shutdown_tx, mut shutdown_rx_oneshot) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        let _ = shutdown_rx.recv().await;
        let _ = shutdown_tx.send(());
    });

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, _)) => {
                        let _ = stream.set_nodelay(true);
                        let engine = engine.clone();
                        let tls = tls_acceptor.clone();
                        tokio::spawn(async move {
                            if let Some(acceptor) = tls {
                                // TLS path: wrap TCP stream with rustls
                                match acceptor.accept(stream).await {
                                    Ok(tls_stream) => {
                                        let io = TokioIo::new(tls_stream);
                                        let svc = service_fn(move |req: Request<Incoming>| {
                                            let engine = engine.clone();
                                            async move {
                                                route_to_http_response(&engine, req).await
                                            }
                                        });
                                        if let Err(e) = ServerBuilder::new(TokioExecutor::new())
                                            .serve_connection(io, svc)
                                            .await
                                        {
                                            tracing::debug!("HTTPS connection error: {}", e);
                                        }
                                    }
                                    Err(e) => {
                                        tracing::debug!("TLS handshake failed: {}", e);
                                    }
                                }
                            } else {
                                // Plain TCP path
                                let io = TokioIo::new(stream);
                                let svc = service_fn(move |req: Request<Incoming>| {
                                    let engine = engine.clone();
                                    async move {
                                        route_to_http_response(&engine, req).await
                                    }
                                });
                                if let Err(e) = ServerBuilder::new(TokioExecutor::new())
                                    .serve_connection(io, svc)
                                    .await
                                {
                                    tracing::debug!("HTTP connection error: {}", e);
                                }
                            }
                        });
                    }
                    Err(e) => {
                        tracing::debug!("Accept error: {}", e);
                    }
                }
            }
            _ = &mut shutdown_rx_oneshot => {
                tracing::info!("HTTP server shutting down");
                break;
            }
        }
    }
}

/// Maximum allowed request body size (10 MB). Requests exceeding this are rejected
/// with 413 Payload Too Large to prevent memory exhaustion attacks.
const MAX_BODY_SIZE: usize = 10 * 1024 * 1024;

/// Server version reported in /health responses.
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Convert a hyper Request into an HTTP Response by delegating to route_request.
async fn route_to_http_response(
    engine: &Arc<DevEngine>,
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, std::convert::Infallible> {
    use http_body_util::BodyExt;

    let method = req.method().as_str().to_string();
    let path = req.uri().path().to_string();

    // Extract or generate X-Request-Id for request correlation
    let request_id = req
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("req-{}", generate_id()));

    // Auth enforcement: check bearer token for /api/ routes when auth is configured
    if !engine.config.auth_token.is_empty() && path.starts_with("/api/") {
        let auth_header = req
            .headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let expected = format!("Bearer {}", engine.config.auth_token);
        if auth_header != expected {
            return Ok(Response::builder()
                .status(401)
                .header("Content-Type", "application/json")
                .header("X-Request-Id", &request_id)
                .body(Full::new(Bytes::from(r#"{"error":"unauthorized"}"#)))
                .unwrap());
        }
    }

    // Content-Type validation: POST/PUT/PATCH to /api/ must send application/json
    if (method == "POST" || method == "PUT" || method == "PATCH") && path.starts_with("/api/") {
        let ct = req
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if !ct.is_empty() && !ct.contains("application/json") && !ct.contains("multipart/") {
            return Ok(Response::builder()
                .status(415)
                .header("Content-Type", "application/json")
                .header("X-Request-Id", &request_id)
                .body(Full::new(Bytes::from(
                    r#"{"error":"unsupported media type","expected":"application/json"}"#,
                )))
                .unwrap());
        }
    }

    // Request body size limit: check Content-Length first for fast rejection
    let content_length = req
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok());
    if let Some(len) = content_length {
        if len > MAX_BODY_SIZE {
            let msg = format!(
                r#"{{"error":"payload too large","max_bytes":{}}}"#,
                MAX_BODY_SIZE
            );
            return Ok(Response::builder()
                .status(413)
                .header("Content-Type", "application/json")
                .header("X-Request-Id", &request_id)
                .body(Full::new(Bytes::from(msg)))
                .unwrap());
        }
    }

    let body_bytes = match req.into_body().collect().await {
        Ok(collected) => {
            let bytes = collected.to_bytes();
            if bytes.len() > MAX_BODY_SIZE {
                let msg = format!(
                    r#"{{"error":"payload too large","max_bytes":{}}}"#,
                    MAX_BODY_SIZE
                );
                return Ok(Response::builder()
                    .status(413)
                    .header("Content-Type", "application/json")
                    .header("X-Request-Id", &request_id)
                    .body(Full::new(Bytes::from(msg)))
                    .unwrap());
            }
            bytes
        }
        Err(_) => Bytes::new(),
    };

    let (status, content_type, response_body) =
        route_request(engine, &method, &path, &body_bytes).await;

    let Ok(response) = Response::builder()
        .status(status)
        .header("Content-Type", content_type)
        .header("Access-Control-Allow-Origin", "*")
        .header("X-Request-Id", &request_id)
        .header("X-Response-Time-Ms", "0") // placeholder — real timing requires middleware
        .body(Full::new(Bytes::from(response_body)))
    else {
        return Ok(Response::builder()
            .status(500)
            .header("X-Request-Id", &request_id)
            .body(Full::new(Bytes::from("Internal Server Error")))
            .unwrap());
    };
    Ok(response)
}

async fn route_request(
    engine: &Arc<DevEngine>,
    method: &str,
    path: &str,
    body: &[u8],
) -> (u16, &'static str, String) {
    let json: &'static str = "application/json";

    match (method, path) {
        ("GET", "/health") => {
            let stats = engine.get_stats();
            let health = serde_json::json!({
                "status": "ok",
                "server": "velocity-dev",
                "version": SERVER_VERSION,
                "uptime_secs": stats.uptime_secs,
                "workflow_count": stats.workflow_count,
                "running_workflows": stats.running_workflows,
                "completed_workflows": stats.completed_workflows,
                "failed_workflows": stats.failed_workflows,
                "namespace_count": stats.namespace_count,
            });
            (
                200,
                json,
                health.to_string(),
            )
        },
        ("GET", "/api/v1/stats") => (
            200,
            json,
            serde_json::to_string(&engine.get_stats()).unwrap_or_default(),
        ),
        ("GET", "/api/v1/namespaces") => (
            200,
            json,
            serde_json::to_string(&engine.list_namespaces()).unwrap_or_default(),
        ),
        ("POST", "/api/v1/namespaces") => {
            match serde_json::from_slice::<serde_json::Value>(body) {
                Ok(v) => {
                    let name = v["name"].as_str().unwrap_or("unnamed");
                    let desc = v["description"].as_str().unwrap_or("");
                    match engine.create_namespace(name, desc) {
                        Ok(ns) => (201, json, serde_json::to_string(&ns).unwrap_or_default()),
                        Err(e) => (400, json, serde_json::json!({"error": e}).to_string()),
                    }
                }
                Err(_) => (400, json, serde_json::json!({"error": "invalid JSON"}).to_string()),
            }
        }
        ("POST", "/api/v1/workflows") => {
            match serde_json::from_slice::<serde_json::Value>(body) {
                Ok(v) => {
                    let wf_type = v["workflowType"].as_str().unwrap_or("Unknown");
                    let task_queue = v["taskQueue"].as_str().unwrap_or("default-queue");
                    let input = v.get("input").cloned().unwrap_or(serde_json::Value::Null);
                    let namespace = v["namespace"].as_str().unwrap_or(&engine.config.namespace);
                    match engine.start_workflow(namespace, wf_type, task_queue, input, "") {
                        Ok(exec) => (
                            201,
                            json,
                            serde_json::json!({
                                "workflowId": exec.workflow_id,
                                "runId": exec.run_id,
                            })
                            .to_string(),
                        ),
                        Err(e) => (400, json, serde_json::json!({"error": e}).to_string()),
                    }
                }
                Err(_) => (400, json, serde_json::json!({"error": "invalid JSON"}).to_string()),
            }
        }
        ("GET", p) if p.starts_with("/api/v1/workflows/") => {
            let parts: Vec<&str> = p.split('/').collect();
            if parts.len() >= 5 {
                let namespace = &engine.config.namespace;
                let wf_id = parts[4];
                if parts.len() > 5 {
                    match parts[5] {
                        "history" => {
                            let history = engine.get_history(namespace, wf_id);
                            (200, json, serde_json::to_string(&history).unwrap_or_default())
                        }
                        "query" => {
                            let query_type = parts.get(6).copied().unwrap_or("__stack_trace");
                            match engine.query_workflow(namespace, wf_id, query_type) {
                                Ok(result) => (200, json, result.to_string()),
                                Err(e) => (404, json, serde_json::json!({"error": e}).to_string()),
                            }
                        }
                        _ => (404, json, serde_json::json!({"error": "unknown sub-path"}).to_string()),
                    }
                } else {
                    match engine.get_workflow(namespace, wf_id) {
                        Some(wf) => (200, json, serde_json::to_string(&wf).unwrap_or_default()),
                        None => (404, json, serde_json::json!({"error": "not found"}).to_string()),
                    }
                }
            } else {
                (404, json, serde_json::json!({"error": "invalid path"}).to_string())
            }
        }
        ("GET", "/api/v1/workflows") => {
            let namespace = &engine.config.namespace;
            let result = engine.list_workflows(namespace, None, 100);
            (200, json, serde_json::to_string(&result).unwrap_or_default())
        }
        ("GET", "/api/v1/task-queues") => {
            let namespace = &engine.config.namespace;
            let tqs = engine.list_task_queues(namespace);
            (200, json, serde_json::to_string(&tqs).unwrap_or_default())
        }
        ("POST", p) if p.starts_with("/api/v1/workflows/") && p.ends_with("/signal") => {
            let parts: Vec<&str> = p.split('/').collect();
            if parts.len() >= 6 {
                let namespace = &engine.config.namespace;
                let wf_id = parts[4];
                match serde_json::from_slice::<serde_json::Value>(body) {
                    Ok(v) => {
                        let signal_name = v["signalName"].as_str().unwrap_or("unknown");
                        let input = v.get("input").cloned().unwrap_or(serde_json::Value::Null);
                        match engine.signal_workflow(namespace, wf_id, signal_name, input) {
                            Ok(()) => (200, json, serde_json::json!({"signaled": true}).to_string()),
                            Err(e) => (404, json, serde_json::json!({"error": e}).to_string()),
                        }
                    }
                    Err(_) => (400, json, serde_json::json!({"error": "invalid JSON"}).to_string()),
                }
            } else {
                (400, json, serde_json::json!({"error": "invalid path"}).to_string())
            }
        }
        ("POST", p) if p.starts_with("/api/v1/workflows/") && p.ends_with("/cancel") => {
            let parts: Vec<&str> = p.split('/').collect();
            if parts.len() >= 6 {
                let namespace = &engine.config.namespace;
                let wf_id = parts[4];
                let v = serde_json::from_slice::<serde_json::Value>(body).unwrap_or(serde_json::Value::Null);
                let reason = v["reason"].as_str().unwrap_or("cancelled via API");
                match engine.cancel_workflow(namespace, wf_id, reason) {
                    Ok(()) => (200, json, serde_json::json!({"cancelled": true}).to_string()),
                    Err(e) => (404, json, serde_json::json!({"error": e}).to_string()),
                }
            } else {
                (400, json, serde_json::json!({"error": "invalid path"}).to_string())
            }
        }
        ("POST", p) if p.starts_with("/api/v1/workflows/") && p.ends_with("/update") => {
            let parts: Vec<&str> = p.split('/').collect();
            if parts.len() >= 6 {
                let namespace = &engine.config.namespace;
                let wf_id = parts[4];
                match serde_json::from_slice::<serde_json::Value>(body) {
                    Ok(v) => {
                        let update_name = v["updateName"].as_str().unwrap_or("unknown");
                        let update_id = v["updateId"].as_str().unwrap_or("");
                        let payload = v.get("payload").cloned().unwrap_or(serde_json::Value::Null);
                        match engine.update_workflow(namespace, wf_id, update_name, update_id, payload) {
                            Ok(result) => (200, json, serde_json::to_string(&result).unwrap_or_default()),
                            Err(e) => (404, json, serde_json::json!({"error": e}).to_string()),
                        }
                    }
                    Err(_) => (400, json, serde_json::json!({"error": "invalid JSON"}).to_string()),
                }
            } else {
                (400, json, serde_json::json!({"error": "invalid path"}).to_string())
            }
        }
        ("POST", p) if p.starts_with("/api/v1/workflows/") && p.ends_with("/search-attributes") => {
            let parts: Vec<&str> = p.split('/').collect();
            if parts.len() >= 6 {
                let namespace = &engine.config.namespace;
                let wf_id = parts[4];
                match serde_json::from_slice::<HashMap<String, String>>(body) {
                    Ok(attrs) => match engine.upsert_search_attributes(namespace, wf_id, attrs) {
                        Ok(()) => (200, json, serde_json::json!({"upserted": true}).to_string()),
                        Err(e) => (404, json, serde_json::json!({"error": e}).to_string()),
                    },
                    Err(_) => (400, json, serde_json::json!({"error": "invalid JSON"}).to_string()),
                }
            } else {
                (400, json, serde_json::json!({"error": "invalid path"}).to_string())
            }
        }
        ("POST", p) if p.starts_with("/api/v1/workflows/") && p.ends_with("/memo") => {
            let parts: Vec<&str> = p.split('/').collect();
            if parts.len() >= 6 {
                let namespace = &engine.config.namespace;
                let wf_id = parts[4];
                match serde_json::from_slice::<HashMap<String, String>>(body) {
                    Ok(memo) => match engine.set_memo(namespace, wf_id, memo) {
                        Ok(()) => (200, json, serde_json::json!({"memo_set": true}).to_string()),
                        Err(e) => (404, json, serde_json::json!({"error": e}).to_string()),
                    },
                    Err(_) => (400, json, serde_json::json!({"error": "invalid JSON"}).to_string()),
                }
            } else {
                (400, json, serde_json::json!({"error": "invalid path"}).to_string())
            }
        }
        ("POST", p) if p.starts_with("/api/v1/workflows/") && p.ends_with("/timers") => {
            let parts: Vec<&str> = p.split('/').collect();
            if parts.len() >= 6 {
                let namespace = &engine.config.namespace;
                let wf_id = parts[4];
                match serde_json::from_slice::<serde_json::Value>(body) {
                    Ok(v) => {
                        let timer_id = v["timerId"].as_str().unwrap_or("");
                        let duration_ms = v["durationMs"].as_i64().unwrap_or(1000);
                        match engine.schedule_timer(namespace, wf_id, timer_id, duration_ms) {
                            Ok(tid) => (201, json, serde_json::json!({"timerId": tid}).to_string()),
                            Err(e) => (400, json, serde_json::json!({"error": e}).to_string()),
                        }
                    }
                    Err(_) => (400, json, serde_json::json!({"error": "invalid JSON"}).to_string()),
                }
            } else {
                (400, json, serde_json::json!({"error": "invalid path"}).to_string())
            }
        }
        ("POST", p) if p.starts_with("/api/v1/workflows/") && p.ends_with("/continue-as-new") => {
            let parts: Vec<&str> = p.split('/').collect();
            if parts.len() >= 6 {
                let namespace = &engine.config.namespace;
                let wf_id = parts[4];
                let v = serde_json::from_slice::<serde_json::Value>(body).unwrap_or(serde_json::Value::Null);
                let wf_type = v["workflowType"].as_str().unwrap_or("default");
                let tq = v["taskQueue"].as_str().unwrap_or("default-queue");
                let input = v.get("input").cloned().unwrap_or(serde_json::Value::Null);
                match engine.continue_as_new(namespace, wf_id, wf_type, tq, input) {
                    Ok(new_run_id) => (200, json, serde_json::json!({"newRunId": new_run_id}).to_string()),
                    Err(e) => (400, json, serde_json::json!({"error": e}).to_string()),
                }
            } else {
                (400, json, serde_json::json!({"error": "invalid path"}).to_string())
            }
        }
        ("POST", p) if p.starts_with("/api/v1/workflows/") && p.ends_with("/replay") => {
            let parts: Vec<&str> = p.split('/').collect();
            if parts.len() >= 6 {
                let namespace = &engine.config.namespace;
                let wf_id = parts[4];
                match engine.replay_workflow(namespace, wf_id) {
                    Ok((events, status)) => (200, json, serde_json::json!({"eventsReplayed": events, "finalStatus": status}).to_string()),
                    Err(e) => (400, json, serde_json::json!({"error": e}).to_string()),
                }
            } else {
                (400, json, serde_json::json!({"error": "invalid path"}).to_string())
            }
        }
        ("POST", p) if p.starts_with("/api/v1/workflows/") && p.ends_with("/reset") => {
            let parts: Vec<&str> = p.split('/').collect();
            if parts.len() >= 6 {
                let namespace = &engine.config.namespace;
                let wf_id = parts[4];
                let v = serde_json::from_slice::<serde_json::Value>(body).unwrap_or(serde_json::Value::Null);
                let event_id = v["resetToEventId"].as_i64().unwrap_or(1);
                let reason = v["reason"].as_str().unwrap_or("reset via API");
                match engine.reset_workflow(namespace, wf_id, event_id, reason) {
                    Ok(new_run_id) => (200, json, serde_json::json!({"newRunId": new_run_id}).to_string()),
                    Err(e) => (400, json, serde_json::json!({"error": e}).to_string()),
                }
            } else {
                (400, json, serde_json::json!({"error": "invalid path"}).to_string())
            }
        }
        ("POST", "/api/v1/batch/terminate") => {
            match serde_json::from_slice::<serde_json::Value>(body) {
                Ok(v) => {
                    let namespace = v["namespace"].as_str().unwrap_or(&engine.config.namespace);
                    let filter = v["statusFilter"].as_str().unwrap_or("running");
                    let reason = v["reason"].as_str().unwrap_or("batch terminated");
                    let max = v["maxCount"].as_i64().unwrap_or(0);
                    let count = engine.batch_terminate(namespace, filter, reason, max);
                    (200, json, serde_json::json!({"terminatedCount": count}).to_string())
                }
                Err(_) => (400, json, serde_json::json!({"error": "invalid JSON"}).to_string()),
            }
        }
        ("POST", "/api/v1/batch/signal") => {
            match serde_json::from_slice::<serde_json::Value>(body) {
                Ok(v) => {
                    let namespace = v["namespace"].as_str().unwrap_or(&engine.config.namespace);
                    let filter = v["statusFilter"].as_str().unwrap_or("running");
                    let signal_name = v["signalName"].as_str().unwrap_or("unknown");
                    let payload = v.get("payload").cloned().unwrap_or(serde_json::Value::Null);
                    let max = v["maxCount"].as_i64().unwrap_or(0);
                    let count = engine.batch_signal(namespace, filter, signal_name, payload, max);
                    (200, json, serde_json::json!({"signaledCount": count}).to_string())
                }
                Err(_) => (400, json, serde_json::json!({"error": "invalid JSON"}).to_string()),
            }
        }
        ("POST", p) if p.starts_with("/api/v1/workflows/") && p.ends_with("/complete") => {
            let parts: Vec<&str> = p.split('/').collect();
            if parts.len() >= 6 {
                let namespace = &engine.config.namespace;
                let wf_id = parts[4];
                let result = serde_json::from_slice::<serde_json::Value>(body).unwrap_or(serde_json::Value::Null);
                match engine.complete_workflow(namespace, wf_id, result) {
                    Ok(()) => (200, json, serde_json::json!({"completed": true}).to_string()),
                    Err(e) => (404, json, serde_json::json!({"error": e}).to_string()),
                }
            } else {
                (400, json, serde_json::json!({"error": "invalid path"}).to_string())
            }
        }
        ("POST", p) if p.starts_with("/api/v1/workflows/") && p.ends_with("/fail") => {
            let parts: Vec<&str> = p.split('/').collect();
            if parts.len() >= 6 {
                let namespace = &engine.config.namespace;
                let wf_id = parts[4];
                let v = serde_json::from_slice::<serde_json::Value>(body).unwrap_or(serde_json::Value::Null);
                let reason = v["reason"].as_str().unwrap_or("unknown");
                match engine.fail_workflow(namespace, wf_id, reason) {
                    Ok(()) => (200, json, serde_json::json!({"failed": true}).to_string()),
                    Err(e) => (404, json, serde_json::json!({"error": e}).to_string()),
                }
            } else {
                (400, json, serde_json::json!({"error": "invalid path"}).to_string())
            }
        }
        ("GET", "/metrics") => {
            let stats = engine.get_stats();
            let prom = format!(
                "# HELP velocity_uptime_seconds Server uptime in seconds\n\
                 # TYPE velocity_uptime_seconds counter\n\
                 velocity_uptime_seconds {}\n\
                 # HELP velocity_workflows_total Total workflow executions\n\
                 # TYPE velocity_workflows_total counter\n\
                 velocity_workflows_total {}\n\
                 # HELP velocity_workflows_running Currently running workflows\n\
                 # TYPE velocity_workflows_running gauge\n\
                 velocity_workflows_running {}\n\
                 # HELP velocity_workflows_completed Completed workflows\n\
                 # TYPE velocity_workflows_completed counter\n\
                 velocity_workflows_completed {}\n\
                 # HELP velocity_workflows_failed Failed workflows\n\
                 # TYPE velocity_workflows_failed counter\n\
                 velocity_workflows_failed {}\n\
                 # HELP velocity_workflows_cancelled Cancelled workflows\n\
                 # TYPE velocity_workflows_cancelled counter\n\
                 velocity_workflows_cancelled {}\n\
                 # HELP velocity_workflows_terminated Terminated workflows\n\
                 # TYPE velocity_workflows_terminated counter\n\
                 velocity_workflows_terminated {}\n\
                 # HELP velocity_signals_total Total signals delivered\n\
                 # TYPE velocity_signals_total counter\n\
                 velocity_signals_total {}\n\
                 # HELP velocity_queries_total Total queries served\n\
                 # TYPE velocity_queries_total counter\n\
                 velocity_queries_total {}\n\
                 # HELP velocity_updates_total Total workflow updates\n\
                 # TYPE velocity_updates_total counter\n\
                 velocity_updates_total {}\n\
                 # HELP velocity_activities_total Total activities scheduled\n\
                 # TYPE velocity_activities_total counter\n\
                 velocity_activities_total {}\n\
                 # HELP velocity_active_activities Currently active activities\n\
                 # TYPE velocity_active_activities gauge\n\
                 velocity_active_activities {}\n\
                 # HELP velocity_pending_timers Currently pending timers\n\
                 # TYPE velocity_pending_timers gauge\n\
                 velocity_pending_timers {}\n\
                 # HELP velocity_child_workflows Active child workflows\n\
                 # TYPE velocity_child_workflows gauge\n\
                 velocity_child_workflows {}\n\
                 # HELP velocity_history_events_total Total history events\n\
                 # TYPE velocity_history_events_total counter\n\
                 velocity_history_events_total {}\n\
                 # HELP velocity_namespaces Registered namespaces\n\
                 # TYPE velocity_namespaces gauge\n\
                 velocity_namespaces {}\n\
                 # HELP velocity_task_queues Active task queues\n\
                 # TYPE velocity_task_queues gauge\n\
                 velocity_task_queues {}\n",
                stats.uptime_secs,
                stats.workflow_count,
                stats.running_workflows,
                stats.completed_workflows,
                stats.failed_workflows,
                stats.cancelled_workflows,
                stats.terminated_workflows,
                stats.signal_count,
                stats.query_count,
                stats.update_count,
                stats.activity_count,
                stats.active_activities,
                stats.pending_timers,
                stats.child_workflows,
                stats.history_event_count,
                stats.namespace_count,
                stats.task_queue_count,
            );
            (200, "text/plain; version=0.0.4", prom)
        }
        _ => (
            404,
            json,
            r#"{"error":"not found","hint":"Try /health, /api/v1/workflows, /api/v1/namespaces, /api/v1/stats, /metrics"}"#.to_string(),
        ),
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Web UI Server
// ═══════════════════════════════════════════════════════════════════════════════

async fn run_ui_server(
    engine: Arc<DevEngine>,
    addr: SocketAddr,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("Failed to bind UI server to {}: {}", addr, e);
            return;
        }
    };
    tracing::info!("Web UI listening on http://{} (Hyper, keep-alive)", addr);

    let (shutdown_tx, mut shutdown_rx_oneshot) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        let _ = shutdown_rx.recv().await;
        let _ = shutdown_tx.send(());
    });

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, _)) => {
                        // TCP_NODELAY: disable Nagle's algorithm for lower latency
                        let _ = stream.set_nodelay(true);
                        let engine = engine.clone();
                        let io = TokioIo::new(stream);
                        tokio::spawn(async move {
                            let svc = service_fn(move |req: Request<Incoming>| {
                                let engine = engine.clone();
                                async move {
                                    route_ui_to_http_response(&engine, req).await
                                }
                            });
                            if let Err(e) = ServerBuilder::new(TokioExecutor::new())
                                .serve_connection(io, svc)
                                .await
                            {
                                tracing::debug!("UI connection error: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        tracing::debug!("UI accept error: {}", e);
                    }
                }
            }
            _ = &mut shutdown_rx_oneshot => {
                tracing::info!("UI server shutting down");
                break;
            }
        }
    }
}

/// Route a UI request to an HTTP response using Hyper types.
async fn route_ui_to_http_response(
    engine: &Arc<DevEngine>,
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, std::convert::Infallible> {
    let path = req.uri().path().to_string();

    let (status, content_type, body) = match path.as_str() {
        "/" | "/index.html" => (200, "text/html", generate_ui_html(engine)),
        "/health" => (200, "application/json", r#"{"status":"ok"}"#.to_string()),
        p if p.starts_with("/workflows/") => {
            let wf_id = p.trim_start_matches("/workflows/");
            let namespace = &engine.config.namespace;
            match engine.get_workflow(namespace, wf_id) {
                Some(wf) => (200, "text/html", generate_workflow_detail_html(engine, &wf)),
                None => (
                    404,
                    "text/html",
                    format!(
                        "<h1>Workflow {} not found</h1><a href='/'>Back to dashboard</a>",
                        wf_id
                    ),
                ),
            }
        }
        "/schedules" => (200, "text/html", generate_schedules_html(engine)),
        "/task-queues" => (200, "text/html", generate_task_queues_html(engine)),
        "/batch-operations" => (200, "text/html", generate_batch_operations_html(engine)),
        "/search" => (200, "text/html", generate_search_html(engine)),
        _ => (404, "text/html", "<h1>404 Not Found</h1>".to_string()),
    };

    let Ok(response) = Response::builder()
        .status(status)
        .header("Content-Type", content_type)
        .body(Full::new(Bytes::from(body)))
    else {
        return Ok(Response::builder()
            .status(500)
            .body(Full::new(Bytes::from("Internal Server Error")))
            .unwrap());
    };
    Ok(response)
}

fn generate_ui_html(engine: &Arc<DevEngine>) -> String {
    let stats = engine.get_stats();
    let workflows = engine.list_workflows(&engine.config.namespace, None, 20);
    let namespaces = engine.list_namespaces();

    let workflow_rows: String = workflows
        .executions
        .iter()
        .map(|w| {
            let status_color = match w.status.as_str() {
                "RUNNING" => "#2196F3",
                "COMPLETED" => "#4CAF50",
                "FAILED" => "#f44336",
                _ => "#9E9E9E",
            };
            format!(
                "<tr><td><a href='/workflows/{}'>{}</a></td><td>{}</td><td style='color:{}'>{}</td><td>{}</td><td>{}</td></tr>",
                w.workflow_id,
                &w.workflow_id[..16.min(w.workflow_id.len())],
                w.workflow_type,
                status_color,
                w.status,
                w.task_queue,
                format_duration(w.started_at),
            )
        })
        .collect();

    let ns_rows: String = namespaces
        .iter()
        .map(|ns| {
            format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{} days</td></tr>",
                ns.name, ns.id, ns.state, ns.retention_days,
            )
        })
        .collect();

    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>VELOCITY Dev Server</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f1117; color: #e0e0e0; }}
.header {{ background: #1a1a2e; padding: 20px 40px; border-bottom: 1px solid #333; }}
.header h1 {{ color: #00d4ff; font-size: 24px; }}
.header .subtitle {{ color: #888; font-size: 14px; margin-top: 4px; }}
.container {{ max-width: 1400px; margin: 0 auto; padding: 20px 40px; }}
.stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 30px; }}
.stat-card {{ background: #1a1a2e; border: 1px solid #333; border-radius: 8px; padding: 16px; }}
.stat-card .label {{ color: #888; font-size: 12px; text-transform: uppercase; }}
.stat-card .value {{ color: #00d4ff; font-size: 28px; font-weight: bold; margin-top: 4px; }}
.section {{ margin-bottom: 30px; }}
.section h2 {{ color: #fff; margin-bottom: 12px; font-size: 18px; }}
table {{ width: 100%; border-collapse: collapse; background: #1a1a2e; border-radius: 8px; overflow: hidden; }}
th {{ background: #252540; color: #888; text-align: left; padding: 10px 16px; font-size: 12px; text-transform: uppercase; }}
td {{ padding: 10px 16px; border-top: 1px solid #252540; font-size: 14px; }}
a {{ color: #00d4ff; text-decoration: none; }}
a:hover {{ text-decoration: underline; }}
.endpoints {{ background: #1a1a2e; border: 1px solid #333; border-radius: 8px; padding: 16px; }}
.endpoints code {{ color: #4CAF50; display: block; margin: 4px 0; }}
.nav {{ background: #1a1a2e; padding: 12px 40px; border-bottom: 1px solid #333; display: flex; gap: 20px; }}
.nav a {{ color: #00d4ff; font-size: 14px; }}
.nav a:hover {{ text-decoration: underline; }}
</style>
</head>
<body>
<div class="header">
  <h1>VELOCITY Dev Server</h1>
  <div class="subtitle">In-memory workflow engine for local development</div>
</div>
<div class="nav">
  <a href="/">Dashboard</a>
  <a href="/schedules">Schedules</a>
  <a href="/task-queues">Task Queues</a>
  <a href="/batch-operations">Batch Operations</a>
  <a href="/search">Search</a>
</div>
<div class="container">
  <div class="stats">
    <div class="stat-card"><div class="label">Uptime</div><div class="value">{}s</div></div>
    <div class="stat-card"><div class="label">Workflows</div><div class="value">{}</div></div>
    <div class="stat-card"><div class="label">Running</div><div class="value">{}</div></div>
    <div class="stat-card"><div class="label">Completed</div><div class="value">{}</div></div>
    <div class="stat-card"><div class="label">Failed</div><div class="value">{}</div></div>
    <div class="stat-card"><div class="label">Cancelled</div><div class="value">{}</div></div>
    <div class="stat-card"><div class="label">Activities</div><div class="value">{}</div></div>
    <div class="stat-card"><div class="label">Timers</div><div class="value">{}</div></div>
    <div class="stat-card"><div class="label">Namespaces</div><div class="value">{}</div></div>
    <div class="stat-card"><div class="label">History Events</div><div class="value">{}</div></div>
  </div>
  <div class="section">
    <h2>Recent Workflows</h2>
    <table><thead><tr><th>Workflow ID</th><th>Type</th><th>Status</th><th>Task Queue</th><th>Started</th></tr></thead><tbody>{}</tbody></table>
  </div>
  <div class="section">
    <h2>Namespaces</h2>
    <table><thead><tr><th>Name</th><th>ID</th><th>State</th><th>Retention</th></tr></thead><tbody>{}</tbody></table>
  </div>
  <div class="section">
    <h2>API Endpoints</h2>
    <div class="endpoints">
      <code>GET  /health</code>
      <code>GET  /api/v1/stats</code>
      <code>GET  /api/v1/workflows</code>
      <code>POST /api/v1/workflows</code>
      <code>GET  /api/v1/workflows/:id</code>
      <code>GET  /api/v1/workflows/:id/history</code>
      <code>POST /api/v1/workflows/:id/signal</code>
      <code>POST /api/v1/workflows/:id/complete</code>
      <code>POST /api/v1/workflows/:id/fail</code>
      <code>GET  /api/v1/workflows/:id/query/:type</code>
      <code>POST /api/v1/workflows/:id/cancel</code>
      <code>POST /api/v1/workflows/:id/update</code>
      <code>POST /api/v1/workflows/:id/timers</code>
      <code>POST /api/v1/workflows/:id/continue-as-new</code>
      <code>POST /api/v1/workflows/:id/replay</code>
      <code>POST /api/v1/workflows/:id/reset</code>
      <code>POST /api/v1/workflows/:id/search-attributes</code>
      <code>POST /api/v1/workflows/:id/memo</code>
      <code>POST /api/v1/batch/terminate</code>
      <code>POST /api/v1/batch/signal</code>
      <code>GET  /api/v1/namespaces</code>
      <code>POST /api/v1/namespaces</code>
      <code>GET  /api/v1/task-queues</code>
      <code>GET  /metrics</code>
    </div>
  </div>
</div>
<script>setTimeout(function(){{ location.reload(); }}, 5000);</script>
</body>
</html>"#,
        stats.uptime_secs,
        stats.workflow_count,
        stats.running_workflows,
        stats.completed_workflows,
        stats.failed_workflows,
        stats.cancelled_workflows,
        stats.active_activities,
        stats.pending_timers,
        stats.namespace_count,
        stats.history_event_count,
        workflow_rows,
        ns_rows,
    )
}

fn generate_workflow_detail_html(engine: &Arc<DevEngine>, wf: &WorkflowExecution) -> String {
    let history = engine.get_history(&engine.config.namespace, &wf.workflow_id);

    let history_rows: String = history
        .iter()
        .map(|e| {
            let attrs_str = e.attributes.to_string();
            format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                e.event_id,
                e.event_type,
                format_duration(e.event_time),
                if attrs_str.is_empty() || attrs_str == "null" {
                    "-"
                } else {
                    &attrs_str[..attrs_str.len().min(50)]
                }
            )
        })
        .collect();

    let status_color = match wf.status.as_str() {
        "RUNNING" => "#2196F3",
        "COMPLETED" => "#4CAF50",
        "FAILED" => "#f44336",
        "CANCELLED" => "#FF9800",
        _ => "#9E9E9E",
    };

    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Workflow {} - VELOCITY</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f1117; color: #e0e0e0; }}
.header {{ background: #1a1a2e; padding: 20px 40px; border-bottom: 1px solid #333; }}
.header h1 {{ color: #00d4ff; font-size: 24px; }}
.header .subtitle {{ color: #888; font-size: 14px; margin-top: 4px; }}
.container {{ max-width: 1400px; margin: 0 auto; padding: 20px 40px; }}
.info-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 16px; margin-bottom: 30px; }}
.info-card {{ background: #1a1a2e; border: 1px solid #333; border-radius: 8px; padding: 16px; }}
.info-card .label {{ color: #888; font-size: 12px; text-transform: uppercase; margin-bottom: 4px; }}
.info-card .value {{ color: #fff; font-size: 16px; word-break: break-all; }}
.status-badge {{ display: inline-block; padding: 4px 12px; border-radius: 4px; font-size: 12px; font-weight: bold; color: #fff; background: {}; }}
.section {{ margin-bottom: 30px; }}
.section h2 {{ color: #fff; margin-bottom: 12px; font-size: 18px; }}
table {{ width: 100%; border-collapse: collapse; background: #1a1a2e; border-radius: 8px; overflow: hidden; }}
th {{ background: #252540; color: #888; text-align: left; padding: 10px 16px; font-size: 12px; text-transform: uppercase; }}
td {{ padding: 10px 16px; border-top: 1px solid #252540; font-size: 14px; }}
a {{ color: #00d4ff; text-decoration: none; }}
a:hover {{ text-decoration: underline; }}
.actions {{ background: #1a1a2e; border: 1px solid #333; border-radius: 8px; padding: 16px; margin-bottom: 20px; }}
.btn {{ display: inline-block; padding: 8px 16px; margin-right: 8px; border-radius: 4px; font-size: 14px; font-weight: bold; color: #fff; cursor: pointer; border: none; }}
.btn-cancel {{ background: #FF9800; }}
.btn-terminate {{ background: #f44336; }}
.btn-signal {{ background: #2196F3; }}
.btn:hover {{ opacity: 0.9; }}
.back-link {{ display: inline-block; margin-bottom: 20px; color: #00d4ff; }}
</style>
</head>
<body>
<div class="header">
  <h1>Workflow Details</h1>
  <div class="subtitle">{}</div>
</div>
<div class="container">
  <a href='/' class="back-link">← Back to dashboard</a>
  
  <div class="info-grid">
    <div class="info-card">
      <div class="label">Workflow ID</div>
      <div class="value">{}</div>
    </div>
    <div class="info-card">
      <div class="label">Run ID</div>
      <div class="value">{}</div>
    </div>
    <div class="info-card">
      <div class="label">Type</div>
      <div class="value">{}</div>
    </div>
    <div class="info-card">
      <div class="label">Status</div>
      <div class="value"><span class="status-badge">{}</span></div>
    </div>
    <div class="info-card">
      <div class="label">Task Queue</div>
      <div class="value">{}</div>
    </div>
    <div class="info-card">
      <div class="label">Started</div>
      <div class="value">{}</div>
    </div>
    <div class="info-card">
      <div class="label">Closed</div>
      <div class="value">{}</div>
    </div>
    <div class="info-card">
      <div class="label">History Events</div>
      <div class="value">{}</div>
    </div>
  </div>
  
  <div class="section">
    <h2>Actions</h2>
    <div class="actions">
      <button class="btn btn-signal" onclick="alert('Signal workflow via API: POST /api/v1/workflows/{}/signal')">Signal</button>
      <button class="btn btn-cancel" onclick="alert('Cancel workflow via API: POST /api/v1/workflows/{}/cancel')">Cancel</button>
      <button class="btn btn-terminate" onclick="alert('Terminate workflow via API: POST /api/v1/batch/terminate')">Terminate</button>
    </div>
  </div>
  
  <div class="section">
    <h2>History Events ({})</h2>
    <table>
      <thead><tr><th>Event ID</th><th>Event Type</th><th>Timestamp</th><th>Payload</th></tr></thead>
      <tbody>{}</tbody>
    </table>
  </div>
</div>
</body>
</html>"#,
        wf.workflow_id,
        wf.workflow_id,
        wf.workflow_id,
        wf.run_id,
        wf.workflow_type,
        status_color,
        wf.status,
        wf.task_queue,
        format_duration(wf.started_at),
        wf.closed_at
            .map(format_duration)
            .unwrap_or_else(|| "-".to_string()),
        history.len(),
        wf.workflow_id,
        wf.workflow_id,
        history.len(),
        history_rows,
    )
}

fn generate_schedules_html(engine: &Arc<DevEngine>) -> String {
    let schedules = engine.list_schedules();

    let schedule_rows: String = schedules
        .iter()
        .map(|s| {
            let state_color = match s.state.as_str() {
                "ACTIVE" => "#4CAF50",
                "PAUSED" => "#FF9800",
                "COMPLETED" => "#2196F3",
                _ => "#9E9E9E",
            };
            format!(
                "<tr><td>{}</td><td>{}</td><td style='color:{}'>{}</td><td>{}</td><td>{}</td></tr>",
                s.schedule_id,
                s.workflow_type,
                state_color,
                s.state,
                s.cron_schedule,
                s.last_action_time,
            )
        })
        .collect();

    generate_page_html(
        "Schedules",
        &format!(
            r#"<div class="section">
    <h2>Schedules ({})</h2>
    <table>
      <thead><tr><th>Schedule ID</th><th>Workflow Type</th><th>State</th><th>Cron</th><th>Last Action</th></tr></thead>
      <tbody>{}</tbody>
    </table>
  </div>"#,
            schedules.len(),
            schedule_rows,
        ),
    )
}

fn generate_task_queues_html(engine: &Arc<DevEngine>) -> String {
    let namespace = &engine.config.namespace;
    let task_queues = engine.list_task_queues(namespace);

    let tq_rows: String = task_queues
        .iter()
        .map(|tq| {
            format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                tq.name,
                tq.task_type,
                tq.backlog_count,
                tq.pollers.len(),
            )
        })
        .collect();

    generate_page_html(
        "Task Queues",
        &format!(
            r#"<div class="section">
    <h2>Task Queues ({})</h2>
    <table>
      <thead><tr><th>Name</th><th>Type</th><th>Pending Tasks</th><th>Pollers</th></tr></thead>
      <tbody>{}</tbody>
    </table>
  </div>"#,
            task_queues.len(),
            tq_rows,
        ),
    )
}

fn generate_batch_operations_html(engine: &Arc<DevEngine>) -> String {
    let batches = engine.list_batch_operations();

    let batch_rows: String = batches
        .iter()
        .map(|b| {
            let status_color = match b.status.as_str() {
                "RUNNING" => "#2196F3",
                "COMPLETED" => "#4CAF50",
                "FAILED" => "#f44336",
                _ => "#9E9E9E",
            };
            format!(
                "<tr><td>{}</td><td>{}</td><td style='color:{}'>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                b.job_id,
                b.operation,
                status_color,
                b.status,
                b.total_workflows,
                b.succeeded,
                b.failed,
            )
        })
        .collect();

    generate_page_html(
        "Batch Operations",
        &format!(
            r#"<div class="section">
    <h2>Batch Operations ({})</h2>
    <table>
      <thead><tr><th>Job ID</th><th>Operation</th><th>Status</th><th>Total</th><th>Succeeded</th><th>Failed</th></tr></thead>
      <tbody>{}</tbody>
    </table>
  </div>"#,
            batches.len(),
            batch_rows,
        ),
    )
}

fn generate_search_html(_engine: &Arc<DevEngine>) -> String {
    generate_page_html(
        "Search",
        r#"
  <div class="section">
    <h2>Search Workflows</h2>
    <div class="actions">
      <form method="GET" action="/search">
        <input type="text" name="query" placeholder="Enter search query (e.g., Status='running' AND WorkflowType='MyWorkflow')" 
               style="width: 100%; padding: 10px; background: #252540; border: 1px solid #333; color: #fff; border-radius: 4px; margin-bottom: 10px;">
        <button type="submit" class="btn btn-signal">Search</button>
      </form>
    </div>
    <div style="margin-top: 20px; color: #888; font-size: 14px;">
      <h3 style="color: #fff; margin-bottom: 10px;">Query Examples:</h3>
      <ul style="list-style: none; padding: 0;">
        <li style="margin: 8px 0;"><code style="color: #4CAF50;">Status='running'</code> - Find all running workflows</li>
        <li style="margin: 8px 0;"><code style="color: #4CAF50;">Status='completed' AND WorkflowType='OrderWorkflow'</code> - Find completed orders</li>
        <li style="margin: 8px 0;"><code style="color: #4CAF50;">StartTime > '2024-01-01'</code> - Find workflows started after date</li>
        <li style="margin: 8px 0;"><code style="color: #4CAF50;">NamespaceId = 0</code> - Find workflows in default namespace</li>
      </ul>
    </div>
  </div>"#,
    )
}

fn generate_page_html(title: &str, content: &str) -> String {
    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{} - VELOCITY</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f1117; color: #e0e0e0; }}
.header {{ background: #1a1a2e; padding: 20px 40px; border-bottom: 1px solid #333; }}
.header h1 {{ color: #00d4ff; font-size: 24px; }}
.nav {{ background: #1a1a2e; padding: 12px 40px; border-bottom: 1px solid #333; display: flex; gap: 20px; }}
.nav a {{ color: #00d4ff; font-size: 14px; }}
.nav a:hover {{ text-decoration: underline; }}
.container {{ max-width: 1400px; margin: 0 auto; padding: 20px 40px; }}
.section {{ margin-bottom: 30px; }}
.section h2 {{ color: #fff; margin-bottom: 12px; font-size: 18px; }}
table {{ width: 100%; border-collapse: collapse; background: #1a1a2e; border-radius: 8px; overflow: hidden; }}
th {{ background: #252540; color: #888; text-align: left; padding: 10px 16px; font-size: 12px; text-transform: uppercase; }}
td {{ padding: 10px 16px; border-top: 1px solid #252540; font-size: 14px; }}
a {{ color: #00d4ff; text-decoration: none; }}
a:hover {{ text-decoration: underline; }}
.actions {{ background: #1a1a2e; border: 1px solid #333; border-radius: 8px; padding: 16px; }}
.btn {{ display: inline-block; padding: 8px 16px; margin-right: 8px; border-radius: 4px; font-size: 14px; font-weight: bold; color: #fff; cursor: pointer; border: none; }}
.btn-signal {{ background: #2196F3; }}
.btn:hover {{ opacity: 0.9; }}
.back-link {{ display: inline-block; margin-bottom: 20px; color: #00d4ff; }}
code {{ background: #252540; padding: 2px 6px; border-radius: 3px; }}
</style>
</head>
<body>
<div class="header">
  <h1>{}</h1>
</div>
<div class="nav">
  <a href="/">Dashboard</a>
  <a href="/schedules">Schedules</a>
  <a href="/task-queues">Task Queues</a>
  <a href="/batch-operations">Batch Operations</a>
  <a href="/search">Search</a>
</div>
<div class="container">
  <a href='/' class="back-link">← Back to dashboard</a>
  {}
</div>
</body>
</html>"#,
        title, title, content,
    )
}

// ═══════════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════════

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn generate_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let ts = now_millis();
    let c = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{:x}{:04x}", ts, c)
}

fn format_duration(millis: i64) -> String {
    let secs = (now_millis() - millis) / 1000;
    if secs < 60 {
        format!("{}s ago", secs)
    } else if secs < 3600 {
        format!("{}m ago", secs / 60)
    } else if secs < 86400 {
        format!("{}h ago", secs / 3600)
    } else {
        format!("{}d ago", secs / 86400)
    }
}

fn print_banner(config: &DevServerConfig) {
    println!();
    println!("  \x1b[36m██    ██ ███████  ██████ ██   ██ ██████  ████████ ███████\x1b[0m");
    println!("  \x1b[36m██    ██ ██      ██      ██   ██ ██   ██    ██    ██\x1b[0m");
    println!("  \x1b[36m██ ██ ██ ███████ ██      ███████ ██████     ██    █████\x1b[0m");
    println!("  \x1b[36m███  ███      ██ ██      ██   ██ ██   ██    ██    ██\x1b[0m");
    println!("  \x1b[36m █  █  ███████  ██████ ██   ██ ██   ██    ██    ███████\x1b[0m");
    println!("  \x1b[1mVELOCITY Dev Server\x1b[0m  v0.1.0 — In-memory mode");
    println!();
    println!(
        "  \x1b[33mHTTP API:\x1b[0m  http://{}:{}",
        config.ip, config.port
    );
    println!(
        "  \x1b[33mgRPC:    \x1b[0m  http://{}:{}",
        config.ip, config.grpc_port
    );
    if config.ui_port > 0 {
        println!(
            "  \x1b[33mWeb UI:  \x1b[0m  http://{}:{}",
            config.ip, config.ui_port
        );
    }
    println!(
        "  \x1b[33mMetrics: \x1b[0m  http://{}:{}/metrics",
        config.ip, config.port
    );
    println!();
    println!("  \x1b[32mNamespace:\x1b[0m  {}", config.namespace);
    println!("  \x1b[32mShards:   \x1b[0m  {}", config.shards);
    println!(
        "  \x1b[32mRetention:\x1b[0m  {} days",
        config.retention_days
    );
    println!("  \x1b[32mLog Level:\x1b[0m  {}", config.log_level);
    if config.chaos {
        println!("  \x1b[31mChaos:    \x1b[0m  ENABLED");
    }
    if config.cluster_mode {
        println!(
            "  \x1b[35mCluster:  \x1b[0m  {} nodes (simulated)",
            config.cluster_nodes
        );
    }
    println!();
    println!("  \x1b[90mPress Ctrl+C to stop\x1b[0m");
    println!();
}

// ═══════════════════════════════════════════════════════════════════════════════
// Main
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::main]
async fn main() {
    let config = DevServerConfig::parse();

    // Initialize tracing
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&config.log_level));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    print_banner(&config);

    let engine = DevEngine::new(config.clone());
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    // Graceful shutdown — tracks component drain progress
    let shutdown_started = Arc::new(AtomicBool::new(false));
    let drained_count = Arc::new(AtomicU64::new(0));
    let total_components: u64 = if config.ui_port > 0 { 3 } else { 2 }; // http + grpc + optional ui

    let http_addr: SocketAddr = format!("{}:{}", config.ip, config.port)
        .parse()
        .expect("Invalid HTTP address");
    let tls_acceptor = load_tls_config(&config.tls_cert, &config.tls_key);
    let http_engine = engine.clone();
    let http_shutdown = shutdown_tx.subscribe();
    let http_handle = tokio::spawn(async move {
        run_http_server(http_engine, http_addr, http_shutdown, tls_acceptor).await;
    });

    let ui_handle = if config.ui_port > 0 {
        let ui_addr: SocketAddr = format!("{}:{}", config.ip, config.ui_port)
            .parse()
            .expect("Invalid UI address");
        let ui_engine = engine.clone();
        let ui_shutdown = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            run_ui_server(ui_engine, ui_addr, ui_shutdown).await;
        }))
    } else {
        None
    };

    // ─── gRPC BenchmarkService ──────────────────────────────────────────
    let grpc_addr: SocketAddr = format!("{}:{}", config.ip, config.grpc_port)
        .parse()
        .expect("Invalid gRPC address");
    let grpc_engine = engine.clone();
    let grpc_shutdown = shutdown_tx.subscribe();
    let grpc_handle = tokio::spawn(async move {
        use grpc_bench::velocity_bench_proto::benchmark_service_server::BenchmarkServiceServer;
        use grpc_bench::BenchmarkServiceImpl;

        let service = BenchmarkServiceImpl {
            engine: grpc_engine,
        };
        tracing::info!("gRPC BenchmarkService listening on {}", grpc_addr);

        tonic::transport::Server::builder()
            .add_service(BenchmarkServiceServer::new(service))
            .serve_with_shutdown(grpc_addr, async move {
                let mut rx = grpc_shutdown;
                let _ = rx.recv().await;
            })
            .await
            .unwrap_or_else(|e| tracing::error!("gRPC server error: {}", e));
    });

    // Wait for shutdown signal
    tokio::select! {
        _ = signal::ctrl_c() => {
            println!("\n  Shutting down gracefully...");
        }
    }

    // Phase 1: Signal all servers to stop accepting new connections
    shutdown_started.store(true, Ordering::Release);
    let _ = shutdown_tx.send(());
    engine.shutdown.store(true, Ordering::Relaxed);
    engine.shutdown_notify.notify_waiters();
    tracing::info!(
        "Shutdown signal sent — draining {} components",
        total_components
    );

    // Phase 2: Wait for each server to drain with a shared deadline
    let drain_deadline = Duration::from_secs(10);
    let drain_start = Instant::now();

    // HTTP server
    match tokio::time::timeout(
        drain_deadline.saturating_sub(drain_start.elapsed()),
        http_handle,
    )
    .await
    {
        Ok(_) => {
            drained_count.fetch_add(1, Ordering::Relaxed);
            tracing::info!("HTTP server drained");
        }
        Err(_) => {
            tracing::warn!("HTTP server did not drain within deadline");
        }
    }

    // gRPC server
    match tokio::time::timeout(
        drain_deadline.saturating_sub(drain_start.elapsed()),
        grpc_handle,
    )
    .await
    {
        Ok(_) => {
            drained_count.fetch_add(1, Ordering::Relaxed);
            tracing::info!("gRPC server drained");
        }
        Err(_) => {
            tracing::warn!("gRPC server did not drain within deadline");
        }
    }

    // UI server (if enabled)
    if let Some(handle) = ui_handle {
        match tokio::time::timeout(drain_deadline.saturating_sub(drain_start.elapsed()), handle)
            .await
        {
            Ok(_) => {
                drained_count.fetch_add(1, Ordering::Relaxed);
                tracing::info!("UI server drained");
            }
            Err(_) => {
                tracing::warn!("UI server did not drain within deadline");
            }
        }
    }

    let drained = drained_count.load(Ordering::Relaxed);
    let elapsed = drain_start.elapsed();
    if drained == total_components {
        tracing::info!(
            "All {} components drained in {:.1}s",
            total_components,
            elapsed.as_secs_f64()
        );
    } else {
        tracing::warn!(
            "Only {}/{} components drained in {:.1}s — forcing shutdown",
            drained,
            total_components,
            elapsed.as_secs_f64()
        );
    }

    let stats = engine.get_stats();
    println!();
    println!("  \x1b[32mSession Summary:\x1b[0m");
    println!("    Workflows started:  {}", stats.workflow_count);
    println!("    Workflows completed: {}", stats.completed_workflows);
    println!("    Workflows failed:    {}", stats.failed_workflows);
    println!("    Signals delivered:   {}", stats.signal_count);
    println!("    Uptime:              {}s", stats.uptime_secs);
    println!(
        "    Drain:               {}/{} components",
        drained, total_components
    );
    println!();
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> DevServerConfig {
        DevServerConfig {
            port: 0,
            grpc_port: 0,
            ui_port: 0,
            namespace: "test".to_string(),
            log_level: "error".to_string(),
            shards: 4,
            retention_days: 7,
            dynamic_config: true,
            sqlite_path: String::new(),
            cluster_mode: false,
            cluster_nodes: 3,
            auto_compact: false,
            compact_interval_secs: 300,
            chaos: false,
            ip: "127.0.0.1".to_string(),
            otel: false,
            otel_endpoint: String::new(),
            headless: true,
            data_dir: String::new(),
            search_attributes: true,
            max_history_size: 50_000,
            rate_limiting: false,
            rate_limit_rps: 1000,
            tls_cert: String::new(),
            tls_key: String::new(),
            auth_token: String::new(),
        }
    }

    #[test]
    fn test_engine_creation() {
        let engine = DevEngine::new(test_config());
        let namespaces = engine.list_namespaces();
        assert_eq!(namespaces.len(), 2); // default + system
        assert!(namespaces.iter().any(|ns| ns.name == "test"));
        assert!(namespaces.iter().any(|ns| ns.name == "velocity-system"));
    }

    #[test]
    fn test_start_workflow() {
        let engine = DevEngine::new(test_config());
        let result = engine.start_workflow(
            "test",
            "TestWorkflow",
            "test-queue",
            serde_json::json!({"key": "value"}),
            "",
        );
        assert!(result.is_ok());
        let wf = result.unwrap();
        assert_eq!(wf.workflow_type, "TestWorkflow");
        assert_eq!(wf.task_queue, "test-queue");
        assert_eq!(wf.status, "RUNNING");
        assert_eq!(wf.namespace, "test");
        assert!(wf.history_length >= 1);
    }

    #[test]
    fn test_complete_workflow() {
        let engine = DevEngine::new(test_config());
        let wf = engine
            .start_workflow(
                "test",
                "TestWorkflow",
                "test-queue",
                serde_json::Value::Null,
                "",
            )
            .unwrap();
        let result = engine.complete_workflow(
            "test",
            &wf.workflow_id,
            serde_json::json!({"result": "done"}),
        );
        assert!(result.is_ok());
        let updated = engine.get_workflow("test", &wf.workflow_id).unwrap();
        assert_eq!(updated.status, "COMPLETED");
        assert!(updated.closed_at.is_some());
    }

    #[test]
    fn test_fail_workflow() {
        let engine = DevEngine::new(test_config());
        let wf = engine
            .start_workflow(
                "test",
                "TestWorkflow",
                "test-queue",
                serde_json::Value::Null,
                "",
            )
            .unwrap();
        let result = engine.fail_workflow("test", &wf.workflow_id, "something went wrong");
        assert!(result.is_ok());
        let updated = engine.get_workflow("test", &wf.workflow_id).unwrap();
        assert_eq!(updated.status, "FAILED");
    }

    #[test]
    fn test_signal_workflow() {
        let engine = DevEngine::new(test_config());
        let wf = engine
            .start_workflow(
                "test",
                "TestWorkflow",
                "test-queue",
                serde_json::Value::Null,
                "",
            )
            .unwrap();
        let result = engine.signal_workflow(
            "test",
            &wf.workflow_id,
            "my-signal",
            serde_json::json!({"data": 42}),
        );
        assert!(result.is_ok());
        let key = format!("test/{}", wf.workflow_id);
        let signals = engine.signals.read().unwrap();
        assert_eq!(signals.get(&key).unwrap().len(), 1);
        assert_eq!(signals.get(&key).unwrap()[0].signal_name, "my-signal");
    }

    #[test]
    fn test_signal_nonexistent_workflow() {
        let engine = DevEngine::new(test_config());
        let result = engine.signal_workflow("test", "nonexistent", "sig", serde_json::Value::Null);
        assert!(result.is_err());
    }

    #[test]
    fn test_list_workflows() {
        let engine = DevEngine::new(test_config());
        engine
            .start_workflow("test", "WF1", "q1", serde_json::Value::Null, "")
            .unwrap();
        engine
            .start_workflow("test", "WF2", "q2", serde_json::Value::Null, "")
            .unwrap();
        engine
            .start_workflow("test", "WF3", "q1", serde_json::Value::Null, "")
            .unwrap();
        let result = engine.list_workflows("test", None, 100);
        assert_eq!(result.executions.len(), 3);
        assert_eq!(result.total_count, 3);
    }

    #[test]
    fn test_list_workflows_with_status_filter() {
        let engine = DevEngine::new(test_config());
        let wf1 = engine
            .start_workflow("test", "WF1", "q1", serde_json::Value::Null, "")
            .unwrap();
        engine
            .start_workflow("test", "WF2", "q2", serde_json::Value::Null, "")
            .unwrap();
        engine
            .complete_workflow("test", &wf1.workflow_id, serde_json::Value::Null)
            .unwrap();
        let running = engine.list_workflows("test", Some("RUNNING"), 100);
        assert_eq!(running.executions.len(), 1);
        let completed = engine.list_workflows("test", Some("COMPLETED"), 100);
        assert_eq!(completed.executions.len(), 1);
    }

    #[test]
    fn test_get_history() {
        let engine = DevEngine::new(test_config());
        let wf = engine
            .start_workflow(
                "test",
                "TestWorkflow",
                "test-queue",
                serde_json::Value::Null,
                "",
            )
            .unwrap();
        let history = engine.get_history("test", &wf.workflow_id);
        assert!(history.len() >= 2); // Started + TaskScheduled
        assert_eq!(history[0].event_type, "WorkflowExecutionStarted");
        assert_eq!(history[1].event_type, "WorkflowTaskScheduled");
    }

    #[test]
    fn test_create_namespace() {
        let engine = DevEngine::new(test_config());
        let result = engine.create_namespace("myapp", "My application namespace");
        assert!(result.is_ok());
        let ns = result.unwrap();
        assert_eq!(ns.name, "myapp");
        assert_eq!(ns.state, "REGISTERED");
        // Duplicate should fail
        let dup = engine.create_namespace("myapp", "duplicate");
        assert!(dup.is_err());
    }

    #[test]
    fn test_task_queues() {
        let engine = DevEngine::new(test_config());
        engine
            .start_workflow("test", "WF1", "queue-a", serde_json::Value::Null, "")
            .unwrap();
        engine
            .start_workflow("test", "WF2", "queue-b", serde_json::Value::Null, "")
            .unwrap();
        engine
            .start_workflow("test", "WF3", "queue-a", serde_json::Value::Null, "")
            .unwrap();
        let tqs = engine.list_task_queues("test");
        assert_eq!(tqs.len(), 2);
    }

    #[test]
    fn test_stats() {
        let engine = DevEngine::new(test_config());
        let wf = engine
            .start_workflow(
                "test",
                "TestWorkflow",
                "test-queue",
                serde_json::Value::Null,
                "",
            )
            .unwrap();
        engine
            .complete_workflow("test", &wf.workflow_id, serde_json::Value::Null)
            .unwrap();
        let stats = engine.get_stats();
        assert_eq!(stats.workflow_count, 1);
        assert_eq!(stats.completed_workflows, 1);
        assert_eq!(stats.running_workflows, 0);
        assert!(stats.history_event_count >= 3); // start + task_sched + complete
    }

    #[test]
    fn test_query_workflow() {
        let engine = DevEngine::new(test_config());
        let wf = engine
            .start_workflow(
                "test",
                "TestWorkflow",
                "test-queue",
                serde_json::Value::Null,
                "",
            )
            .unwrap();
        let result = engine.query_workflow("test", &wf.workflow_id, "__stack_trace");
        assert!(result.is_ok());
        let val = result.unwrap();
        assert!(val.get("stack_traces").is_some());
    }

    #[test]
    fn test_query_nonexistent() {
        let engine = DevEngine::new(test_config());
        let result = engine.query_workflow("test", "nonexistent", "__stack_trace");
        assert!(result.is_err());
    }

    #[test]
    fn test_generate_id() {
        let id1 = generate_id();
        let id2 = generate_id();
        assert_ne!(id1, id2);
        assert!(!id1.is_empty());
    }

    #[test]
    fn test_format_duration() {
        let now = now_millis();
        let result = format_duration(now);
        assert!(result.contains("s ago"));
    }

    // ── New feature tests ──────────────────────────────────────────────────

    #[test]
    fn test_cancel_workflow() {
        let engine = DevEngine::new(test_config());
        let wf = engine
            .start_workflow("test", "WF", "q1", serde_json::Value::Null, "")
            .unwrap();
        assert!(engine
            .cancel_workflow("test", &wf.workflow_id, "test cancel")
            .is_ok());
        let updated = engine.get_workflow("test", &wf.workflow_id).unwrap();
        assert_eq!(updated.status, "CANCELLED");
        assert!(updated.cancel_requested);
    }

    #[test]
    fn test_cancel_non_running_fails() {
        let engine = DevEngine::new(test_config());
        let wf = engine
            .start_workflow("test", "WF", "q1", serde_json::Value::Null, "")
            .unwrap();
        engine
            .complete_workflow("test", &wf.workflow_id, serde_json::Value::Null)
            .unwrap();
        assert!(engine
            .cancel_workflow("test", &wf.workflow_id, "test")
            .is_err());
    }

    #[test]
    fn test_update_workflow() {
        let engine = DevEngine::new(test_config());
        let wf = engine
            .start_workflow("test", "WF", "q1", serde_json::Value::Null, "")
            .unwrap();
        let result = engine.update_workflow(
            "test",
            &wf.workflow_id,
            "my_update",
            "upd-1",
            serde_json::json!({"key": "val"}),
        );
        assert!(result.is_ok());
        let val = result.unwrap();
        assert_eq!(val["status"], "COMPLETED");
    }

    #[test]
    fn test_child_workflow() {
        let engine = DevEngine::new(test_config());
        let parent = engine
            .start_workflow("test", "Parent", "q1", serde_json::Value::Null, "")
            .unwrap();
        let child = engine.start_child_workflow(
            "test",
            &parent.workflow_id,
            "Child",
            "",
            "q1",
            serde_json::Value::Null,
        );
        assert!(child.is_ok());
        let child_exec = child.unwrap();
        assert!(child_exec.parent_workflow_id.is_some());
        assert_eq!(child_exec.parent_workflow_id.unwrap(), parent.workflow_id);
    }

    #[test]
    fn test_schedule_and_cancel_timer() {
        let engine = DevEngine::new(test_config());
        let wf = engine
            .start_workflow("test", "WF", "q1", serde_json::Value::Null, "")
            .unwrap();
        let tid = engine
            .schedule_timer("test", &wf.workflow_id, "", 5000)
            .unwrap();
        assert!(!tid.is_empty());
        assert!(engine.cancel_timer("test", &wf.workflow_id, &tid).is_ok());
    }

    #[test]
    fn test_continue_as_new() {
        let engine = DevEngine::new(test_config());
        let wf = engine
            .start_workflow("test", "WF", "q1", serde_json::Value::Null, "")
            .unwrap();
        let new_run =
            engine.continue_as_new("test", &wf.workflow_id, "WF", "q1", serde_json::Value::Null);
        assert!(new_run.is_ok());
        // After CASR, the workflow with this ID is the NEW execution (RUNNING)
        let updated = engine.get_workflow("test", &wf.workflow_id).unwrap();
        assert_eq!(updated.status, "RUNNING");
        // The new run_id should be different
        assert_ne!(updated.run_id, wf.run_id);
    }

    #[test]
    fn test_upsert_search_attributes() {
        let engine = DevEngine::new(test_config());
        let wf = engine
            .start_workflow("test", "WF", "q1", serde_json::Value::Null, "")
            .unwrap();
        let mut attrs = HashMap::new();
        attrs.insert("env".to_string(), "production".to_string());
        attrs.insert("team".to_string(), "backend".to_string());
        assert!(engine
            .upsert_search_attributes("test", &wf.workflow_id, attrs)
            .is_ok());
        let updated = engine.get_workflow("test", &wf.workflow_id).unwrap();
        assert_eq!(updated.search_attributes.get("env").unwrap(), "production");
        assert_eq!(updated.search_attributes.get("team").unwrap(), "backend");
    }

    #[test]
    fn test_set_memo() {
        let engine = DevEngine::new(test_config());
        let wf = engine
            .start_workflow("test", "WF", "q1", serde_json::Value::Null, "")
            .unwrap();
        let mut memo = HashMap::new();
        memo.insert("note".to_string(), "important".to_string());
        assert!(engine.set_memo("test", &wf.workflow_id, memo).is_ok());
        let updated = engine.get_workflow("test", &wf.workflow_id).unwrap();
        assert_eq!(updated.memo.get("note").unwrap(), "important");
    }

    #[test]
    fn test_signal_with_start() {
        let engine = DevEngine::new(test_config());
        let (_exec, started, signaled) = engine
            .signal_with_start(
                "test",
                "WF",
                "wf-sws",
                "q1",
                serde_json::Value::Null,
                "my_signal",
                serde_json::json!(42),
            )
            .unwrap();
        assert!(started);
        assert!(signaled);
        // Signal again — should not start a new workflow
        let (_, started2, signaled2) = engine
            .signal_with_start(
                "test",
                "WF",
                "wf-sws",
                "q1",
                serde_json::Value::Null,
                "my_signal",
                serde_json::json!(99),
            )
            .unwrap();
        assert!(!started2);
        assert!(signaled2);
    }

    #[test]
    fn test_activity_lifecycle() {
        let engine = DevEngine::new(test_config());
        let wf = engine
            .start_workflow("test", "WF", "q1", serde_json::Value::Null, "")
            .unwrap();
        let act = engine
            .schedule_activity(
                "test",
                &wf.workflow_id,
                &wf.run_id,
                "act-1",
                "DoWork",
                "q1",
                serde_json::json!("data"),
                None,
            )
            .unwrap();
        assert_eq!(act.status, "SCHEDULED");
        // Heartbeat
        assert!(engine
            .record_heartbeat("test", &wf.workflow_id, "act-1", serde_json::Value::Null)
            .is_ok());
        // Complete
        assert!(engine
            .complete_activity(
                "test",
                &wf.workflow_id,
                "act-1",
                serde_json::json!({"done": true})
            )
            .is_ok());
    }

    #[test]
    fn test_activity_retry() {
        let engine = DevEngine::new(test_config());
        let wf = engine
            .start_workflow("test", "WF", "q1", serde_json::Value::Null, "")
            .unwrap();
        engine
            .schedule_activity(
                "test",
                &wf.workflow_id,
                &wf.run_id,
                "act-retry",
                "DoWork",
                "q1",
                serde_json::Value::Null,
                None,
            )
            .unwrap();
        let (will_retry, next_attempt) = engine
            .fail_activity(
                "test",
                &wf.workflow_id,
                "act-retry",
                "transient error",
                false,
            )
            .unwrap();
        assert!(will_retry);
        assert_eq!(next_attempt, 2);
    }

    #[test]
    fn test_replay_workflow() {
        let engine = DevEngine::new(test_config());
        let wf = engine
            .start_workflow("test", "WF", "q1", serde_json::Value::Null, "")
            .unwrap();
        engine
            .complete_workflow("test", &wf.workflow_id, serde_json::json!({"ok": true}))
            .unwrap();
        let (events, status) = engine.replay_workflow("test", &wf.workflow_id).unwrap();
        assert!(events >= 3); // Started + TaskScheduled + Completed
        assert_eq!(status, "COMPLETED");
    }

    #[test]
    fn test_reset_workflow() {
        let engine = DevEngine::new(test_config());
        let wf = engine
            .start_workflow("test", "WF", "q1", serde_json::Value::Null, "")
            .unwrap();
        let new_run = engine.reset_workflow("test", &wf.workflow_id, 1, "debug reset");
        assert!(new_run.is_ok());
    }

    #[test]
    fn test_batch_terminate() {
        let engine = DevEngine::new(test_config());
        engine
            .start_workflow("test", "WF1", "q1", serde_json::Value::Null, "")
            .unwrap();
        engine
            .start_workflow("test", "WF2", "q1", serde_json::Value::Null, "")
            .unwrap();
        engine
            .start_workflow("test", "WF3", "q1", serde_json::Value::Null, "")
            .unwrap();
        let count = engine.batch_terminate("test", "running", "batch test", 0);
        assert_eq!(count, 3);
    }

    #[test]
    fn test_batch_signal() {
        let engine = DevEngine::new(test_config());
        engine
            .start_workflow("test", "WF1", "q1", serde_json::Value::Null, "")
            .unwrap();
        engine
            .start_workflow("test", "WF2", "q1", serde_json::Value::Null, "")
            .unwrap();
        let count = engine.batch_signal("test", "running", "broadcast", serde_json::json!("go"), 0);
        assert_eq!(count, 2);
    }

    #[test]
    fn test_describe_namespace() {
        let engine = DevEngine::new(test_config());
        let ns = engine.describe_namespace("test");
        assert!(ns.is_ok());
        assert_eq!(ns.unwrap().name, "test");
    }

    #[test]
    fn test_update_namespace() {
        let engine = DevEngine::new(test_config());
        assert!(engine
            .update_namespace("test", Some("updated desc"), Some(30), None)
            .is_ok());
        let ns = engine.describe_namespace("test").unwrap();
        assert_eq!(ns.description, "updated desc");
        assert_eq!(ns.retention_days, 30);
    }

    #[test]
    fn test_delete_namespace() {
        let engine = DevEngine::new(test_config());
        engine.create_namespace("to-delete", "temp").unwrap();
        assert!(engine.delete_namespace("to-delete").is_ok());
        assert!(engine.describe_namespace("to-delete").is_err());
    }

    #[test]
    fn test_cannot_delete_default_namespace() {
        let engine = DevEngine::new(test_config());
        assert!(engine.delete_namespace("test").is_err());
    }

    #[test]
    fn test_poll_workflow_task() {
        let engine = DevEngine::new(test_config());
        engine
            .start_workflow("test", "WF", "poll-queue", serde_json::Value::Null, "")
            .unwrap();
        let task = engine.poll_workflow_task("test", "poll-queue", "worker-1");
        assert!(task.is_some());
        let (token, event_id, _event_type) = task.unwrap();
        assert!(!token.is_empty());
        assert!(event_id > 0);
    }

    #[test]
    fn test_poll_activity_task() {
        let engine = DevEngine::new(test_config());
        let wf = engine
            .start_workflow("test", "WF", "q1", serde_json::Value::Null, "")
            .unwrap();
        engine
            .schedule_activity(
                "test",
                &wf.workflow_id,
                &wf.run_id,
                "a1",
                "DoWork",
                "act-queue",
                serde_json::Value::Null,
                None,
            )
            .unwrap();
        let task = engine.poll_activity_task("test", "act-queue", "worker-1");
        assert!(task.is_some());
        let act = task.unwrap();
        assert_eq!(act.activity_id, "a1");
        assert_eq!(act.status, "STARTED");
    }

    #[test]
    fn test_stats_includes_new_fields() {
        let engine = DevEngine::new(test_config());
        let wf = engine
            .start_workflow("test", "WF", "q1", serde_json::Value::Null, "")
            .unwrap();
        engine
            .cancel_workflow("test", &wf.workflow_id, "test")
            .unwrap();
        let stats = engine.get_stats();
        assert_eq!(stats.cancelled_workflows, 1);
        assert!(!stats.features.is_empty());
        assert!(stats.features.contains(&"cancellation".to_string()));
        assert!(stats.features.contains(&"activities".to_string()));
        assert!(stats.features.contains(&"timers".to_string()));
    }
}
