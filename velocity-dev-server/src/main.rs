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
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::{broadcast, Notify};

// gRPC BenchmarkService — apples-to-apples comparison with Temporal.
mod grpc_bench;

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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RetryPolicy {
    initial_interval_ms: u64,
    max_attempts: u32,
    backoff_coefficient: f64,
    max_interval_ms: u64,
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
    namespace_count: u64,
    task_queue_count: u64,
    history_event_count: u64,
    signal_count: u64,
    query_count: u64,
    memory_usage_bytes: u64,
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

#[derive(Debug)]
struct EngineStats {
    workflow_count: AtomicU64,
    running_count: AtomicU64,
    completed_count: AtomicU64,
    failed_count: AtomicU64,
    signal_count: AtomicU64,
    query_count: AtomicU64,
    history_event_count: AtomicU64,
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
            stats: Arc::new(EngineStats {
                workflow_count: AtomicU64::new(0),
                running_count: AtomicU64::new(0),
                completed_count: AtomicU64::new(0),
                failed_count: AtomicU64::new(0),
                signal_count: AtomicU64::new(0),
                query_count: AtomicU64::new(0),
                history_event_count: AtomicU64::new(0),
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
            history_length: 1,
            memo: HashMap::new(),
            search_attributes: HashMap::new(),
            parent_workflow_id: None,
            parent_run_id: None,
            attempt: 1,
            retry_policy: None,
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

    fn get_stats(&self) -> ServerStats {
        let _workflows = self.workflows.read().unwrap();
        let namespaces = self.namespaces.read().unwrap();
        let task_queues = self.task_queues.read().unwrap();

        ServerStats {
            uptime_secs: self.started_at.elapsed().as_secs(),
            workflow_count: self.stats.workflow_count.load(Ordering::Relaxed),
            running_workflows: self.stats.running_count.load(Ordering::Relaxed),
            completed_workflows: self.stats.completed_count.load(Ordering::Relaxed),
            failed_workflows: self.stats.failed_count.load(Ordering::Relaxed),
            namespace_count: namespaces.len() as u64,
            task_queue_count: task_queues.len() as u64,
            history_event_count: self.stats.history_event_count.load(Ordering::Relaxed),
            signal_count: self.stats.signal_count.load(Ordering::Relaxed),
            query_count: self.stats.query_count.load(Ordering::Relaxed),
            memory_usage_bytes: 0, // Would need platform-specific code
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
        // Also clear history and signals for those workflows
        let mut history = self.history.write().unwrap();
        for key in &keys {
            history.remove(key);
        }
        let mut signals = self.signals.write().unwrap();
        for key in &keys {
            signals.remove(key);
        }
        // Reset stats
        self.stats.workflow_count.store(0, Ordering::Relaxed);
        self.stats.running_count.store(0, Ordering::Relaxed);
        self.stats.completed_count.store(0, Ordering::Relaxed);
        self.stats.failed_count.store(0, Ordering::Relaxed);
        self.stats.signal_count.store(0, Ordering::Relaxed);
        self.stats.query_count.store(0, Ordering::Relaxed);
        self.stats.history_event_count.store(0, Ordering::Relaxed);
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
}

// ═══════════════════════════════════════════════════════════════════════════════
// HTTP API Server
// ═══════════════════════════════════════════════════════════════════════════════

async fn run_http_server(
    engine: Arc<DevEngine>,
    addr: SocketAddr,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("Failed to bind HTTP server to {}: {}", addr, e);
            return;
        }
    };
    tracing::info!("HTTP API listening on http://{}", addr);

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, _)) => {
                        let engine = engine.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_http_connection(engine, stream).await {
                                tracing::debug!("HTTP connection error: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        tracing::debug!("Accept error: {}", e);
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                tracing::info!("HTTP server shutting down");
                break;
            }
        }
    }
}

async fn handle_http_connection(
    engine: Arc<DevEngine>,
    stream: tokio::net::TcpStream,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (reader, mut writer) = stream.into_split();
    let mut buf_reader = BufReader::new(reader);
    let mut request_line = String::new();
    buf_reader.read_line(&mut request_line).await?;

    let parts: Vec<&str> = request_line.trim().split_whitespace().collect();
    if parts.len() < 2 {
        return Ok(());
    }
    let method = parts[0];
    let path = parts[1];

    // Read headers (skip them for simplicity)
    let mut headers = String::new();
    loop {
        let mut line = String::new();
        buf_reader.read_line(&mut line).await?;
        if line == "\r\n" || line.is_empty() {
            break;
        }
        headers.push_str(&line);
    }

    // Read body if present
    let content_length: usize = headers
        .lines()
        .find(|l| l.to_lowercase().starts_with("content-length:"))
        .and_then(|l| l.split(':').nth(1))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0);

    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        use tokio::io::AsyncReadExt;
        buf_reader.read_exact(&mut body).await?;
    }

    let (status, content_type, response_body) = route_request(&engine, method, path, &body).await;

    let response = format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n{}",
        status,
        content_type,
        response_body.len(),
        response_body
    );
    writer.write_all(response.as_bytes()).await?;
    Ok(())
}

async fn route_request(
    engine: &Arc<DevEngine>,
    method: &str,
    path: &str,
    body: &[u8],
) -> (String, String, String) {
    let json = "application/json";
    let ok = "200 OK";
    let created = "201 Created";
    let bad = "400 Bad Request";
    let not_found = "404 Not Found";

    match (method, path) {
        ("GET", "/health") => (
            ok.to_string(),
            json.to_string(),
            serde_json::json!({"status": "ok", "server": "velocity-dev"}).to_string(),
        ),
        ("GET", "/api/v1/stats") => (
            ok.to_string(),
            json.to_string(),
            serde_json::to_string(&engine.get_stats()).unwrap_or_default(),
        ),
        ("GET", "/api/v1/namespaces") => (
            ok.to_string(),
            json.to_string(),
            serde_json::to_string(&engine.list_namespaces()).unwrap_or_default(),
        ),
        ("POST", "/api/v1/namespaces") => {
            match serde_json::from_slice::<serde_json::Value>(body) {
                Ok(v) => {
                    let name = v["name"].as_str().unwrap_or("unnamed");
                    let desc = v["description"].as_str().unwrap_or("");
                    match engine.create_namespace(name, desc) {
                        Ok(ns) => (created.to_string(), json.to_string(), serde_json::to_string(&ns).unwrap_or_default()),
                        Err(e) => (bad.to_string(), json.to_string(), serde_json::json!({"error": e}).to_string()),
                    }
                }
                Err(_) => (bad.to_string(), json.to_string(), serde_json::json!({"error": "invalid JSON"}).to_string()),
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
                            created.to_string(),
                            json.to_string(),
                            serde_json::json!({
                                "workflowId": exec.workflow_id,
                                "runId": exec.run_id,
                            })
                            .to_string(),
                        ),
                        Err(e) => (bad.to_string(), json.to_string(), serde_json::json!({"error": e}).to_string()),
                    }
                }
                Err(_) => (bad.to_string(), json.to_string(), serde_json::json!({"error": "invalid JSON"}).to_string()),
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
                            (ok.to_string(), json.to_string(), serde_json::to_string(&history).unwrap_or_default())
                        }
                        "query" => {
                            let query_type = parts.get(6).copied().unwrap_or("__stack_trace");
                            match engine.query_workflow(namespace, wf_id, query_type) {
                                Ok(result) => (ok.to_string(), json.to_string(), result.to_string()),
                                Err(e) => (not_found.to_string(), json.to_string(), serde_json::json!({"error": e}).to_string()),
                            }
                        }
                        _ => (not_found.to_string(), json.to_string(), serde_json::json!({"error": "unknown sub-path"}).to_string()),
                    }
                } else {
                    match engine.get_workflow(namespace, wf_id) {
                        Some(wf) => (ok.to_string(), json.to_string(), serde_json::to_string(&wf).unwrap_or_default()),
                        None => (not_found.to_string(), json.to_string(), serde_json::json!({"error": "not found"}).to_string()),
                    }
                }
            } else {
                (not_found.to_string(), json.to_string(), serde_json::json!({"error": "invalid path"}).to_string())
            }
        }
        ("GET", "/api/v1/workflows") => {
            let namespace = &engine.config.namespace;
            let result = engine.list_workflows(namespace, None, 100);
            (ok.to_string(), json.to_string(), serde_json::to_string(&result).unwrap_or_default())
        }
        ("GET", "/api/v1/task-queues") => {
            let namespace = &engine.config.namespace;
            let tqs = engine.list_task_queues(namespace);
            (ok.to_string(), json.to_string(), serde_json::to_string(&tqs).unwrap_or_default())
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
                            Ok(()) => (ok.to_string(), json.to_string(), serde_json::json!({"signaled": true}).to_string()),
                            Err(e) => (not_found.to_string(), json.to_string(), serde_json::json!({"error": e}).to_string()),
                        }
                    }
                    Err(_) => (bad.to_string(), json.to_string(), serde_json::json!({"error": "invalid JSON"}).to_string()),
                }
            } else {
                (bad.to_string(), json.to_string(), serde_json::json!({"error": "invalid path"}).to_string())
            }
        }
        ("POST", p) if p.starts_with("/api/v1/workflows/") && p.ends_with("/complete") => {
            let parts: Vec<&str> = p.split('/').collect();
            if parts.len() >= 6 {
                let namespace = &engine.config.namespace;
                let wf_id = parts[4];
                let result = serde_json::from_slice::<serde_json::Value>(body).unwrap_or(serde_json::Value::Null);
                match engine.complete_workflow(namespace, wf_id, result) {
                    Ok(()) => (ok.to_string(), json.to_string(), serde_json::json!({"completed": true}).to_string()),
                    Err(e) => (not_found.to_string(), json.to_string(), serde_json::json!({"error": e}).to_string()),
                }
            } else {
                (bad.to_string(), json.to_string(), serde_json::json!({"error": "invalid path"}).to_string())
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
                    Ok(()) => (ok.to_string(), json.to_string(), serde_json::json!({"failed": true}).to_string()),
                    Err(e) => (not_found.to_string(), json.to_string(), serde_json::json!({"error": e}).to_string()),
                }
            } else {
                (bad.to_string(), json.to_string(), serde_json::json!({"error": "invalid path"}).to_string())
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
                 # HELP velocity_signals_total Total signals delivered\n\
                 # TYPE velocity_signals_total counter\n\
                 velocity_signals_total {}\n\
                 # HELP velocity_queries_total Total queries served\n\
                 # TYPE velocity_queries_total counter\n\
                 velocity_queries_total {}\n\
                 # HELP velocity_history_events_total Total history events\n\
                 # TYPE velocity_history_events_total counter\n\
                 velocity_history_events_total {}\n",
                stats.uptime_secs,
                stats.workflow_count,
                stats.running_workflows,
                stats.completed_workflows,
                stats.failed_workflows,
                stats.signal_count,
                stats.query_count,
                stats.history_event_count,
            );
            (ok.to_string(), "text/plain; version=0.0.4".to_string(), prom)
        }
        _ => (
            not_found.to_string(),
            json.to_string(),
            serde_json::json!({
                "error": "not found",
                "hint": "Try /health, /api/v1/workflows, /api/v1/namespaces, /api/v1/stats, /metrics"
            })
            .to_string(),
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
    tracing::info!("Web UI listening on http://{}", addr);

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, _)) => {
                        let engine = engine.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_ui_connection(engine, stream).await {
                                tracing::debug!("UI connection error: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        tracing::debug!("UI accept error: {}", e);
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                tracing::info!("UI server shutting down");
                break;
            }
        }
    }
}

async fn handle_ui_connection(
    engine: Arc<DevEngine>,
    stream: tokio::net::TcpStream,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (reader, mut writer) = stream.into_split();
    let mut buf_reader = BufReader::new(reader);
    let mut request_line = String::new();
    buf_reader.read_line(&mut request_line).await?;

    let parts: Vec<&str> = request_line.trim().split_whitespace().collect();
    if parts.len() < 2 {
        return Ok(());
    }
    let path = parts[1];

    // Read remaining headers
    loop {
        let mut line = String::new();
        buf_reader.read_line(&mut line).await?;
        if line == "\r\n" || line.is_empty() {
            break;
        }
    }

    let (status, content_type, body) = match path {
        "/" | "/index.html" => (
            "200 OK".to_string(),
            "text/html".to_string(),
            generate_ui_html(&engine),
        ),
        "/health" => (
            "200 OK".to_string(),
            "application/json".to_string(),
            r#"{"status":"ok"}"#.to_string(),
        ),
        _ => (
            "404 Not Found".to_string(),
            "text/html".to_string(),
            "<h1>404 Not Found</h1>".to_string(),
        ),
    };

    let response = format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status,
        content_type,
        body.len(),
        body
    );
    writer.write_all(response.as_bytes()).await?;
    Ok(())
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
</style>
</head>
<body>
<div class="header">
  <h1>VELOCITY Dev Server</h1>
  <div class="subtitle">In-memory workflow engine for local development</div>
</div>
<div class="container">
  <div class="stats">
    <div class="stat-card"><div class="label">Uptime</div><div class="value">{}s</div></div>
    <div class="stat-card"><div class="label">Workflows</div><div class="value">{}</div></div>
    <div class="stat-card"><div class="label">Running</div><div class="value">{}</div></div>
    <div class="stat-card"><div class="label">Completed</div><div class="value">{}</div></div>
    <div class="stat-card"><div class="label">Failed</div><div class="value">{}</div></div>
    <div class="stat-card"><div class="label">Namespaces</div><div class="value">{}</div></div>
    <div class="stat-card"><div class="label">Task Queues</div><div class="value">{}</div></div>
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
        stats.namespace_count,
        stats.task_queue_count,
        stats.history_event_count,
        workflow_rows,
        ns_rows,
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
    println!("  \x1b[36m╦  ╦ ╔╗╔ ╦╔═ \x1b[0m\x1b[1mVELOCITY Dev Server\x1b[0m");
    println!("  \x1b[36m╚╗╔╝ ║║║ ╠╩╗ \x1b[0mv0.1.0 — In-memory mode");
    println!("  \x1b[36m  ╚╝  ╝╚╝ ╩ ╩ \x1b[0m");
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

    let http_addr: SocketAddr = format!("{}:{}", config.ip, config.port)
        .parse()
        .expect("Invalid HTTP address");
    let http_engine = engine.clone();
    let http_shutdown = shutdown_tx.subscribe();
    let http_handle = tokio::spawn(async move {
        run_http_server(http_engine, http_addr, http_shutdown).await;
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
            println!("\n  Shutting down...");
        }
    }

    // Graceful shutdown
    let _ = shutdown_tx.send(());
    engine.shutdown.store(true, Ordering::Relaxed);
    engine.shutdown_notify.notify_waiters();

    // Wait for servers to stop
    let _ = tokio::time::timeout(Duration::from_secs(5), http_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), grpc_handle).await;
    if let Some(handle) = ui_handle {
        let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
    }

    let stats = engine.get_stats();
    println!();
    println!("  \x1b[32mSession Summary:\x1b[0m");
    println!("    Workflows started:  {}", stats.workflow_count);
    println!("    Workflows completed: {}", stats.completed_workflows);
    println!("    Workflows failed:    {}", stats.failed_workflows);
    println!("    Signals delivered:   {}", stats.signal_count);
    println!("    Uptime:              {}s", stats.uptime_secs);
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
}
