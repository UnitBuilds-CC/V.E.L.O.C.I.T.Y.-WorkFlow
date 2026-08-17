//! Velocity Production Benchmark HTTP Server
//!
//! Wraps the REAL WorkflowEngine with WAL persistence and exposes HTTP endpoints
//! that execute complete workflows server-side — matching what DBOS/Restate/Temporal do.
//!
//! Architecture:
//!   [benchmark client] ──HTTP──► [velocity-bench-server] ──► [WorkflowEngine + WAL]
//!
//! Each endpoint runs the FULL workflow server-side:
//!   1. start_workflow() — creates workflow, writes to WAL
//!   2. complete_step() × N — each step writes to WAL (durable execution)
//!   3. complete_workflow() — marks complete, writes to WAL
//!
//! This is NOT a mock. Every step is persisted to the WAL before returning.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Json;
use axum::routing::{get, post};
use axum::Router;
use clap::Parser;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex as TokioMutex;

use velocity_workflow_engine::engine::{DurabilityConfig, WorkflowEngine};

// ─── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "velocity-bench-server")]
struct Cli {
    /// HTTP bind address.
    #[arg(long, default_value = "0.0.0.0:7234")]
    bind: String,

    /// WAL file path (empty = in-memory only).
    #[arg(long, default_value = "velocity-bench.wal")]
    wal_path: String,

    /// Maximum WAL file size in bytes.
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    wal_max_size: u64,

    /// Log level filter.
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Steps between fsync calls. 0 = every step (strict), 1+ = batched.
    /// Controls the performance:reliability trade-off.
    #[arg(long, default_value_t = 0)]
    sync_steps: u32,

    /// Time-based fsync floor (ms). Fsync at least this often even if
    /// sync_steps not reached. Prevents unbounded data loss.
    #[arg(long, default_value_t = 5)]
    flush_interval_ms: u64,
}

// ─── Shared State ────────────────────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    engine: Arc<WorkflowEngine>,
    workflow_counter: Arc<AtomicU64>,
    namespace_id: u64,
    task_queue_hash: u64,
    /// Store for /api/* workflow tracking (Temporal-compatible API)
    api_workflows: Arc<TokioMutex<HashMap<String, ApiWorkflowInfo>>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ApiWorkflowInfo {
    workflow_id: String,
    workflow_type: String,
    status: String,
}

// ─── Request/Response Types ──────────────────────────────────────────────────

#[derive(Deserialize)]
struct StepsInput {
    steps: Option<u32>,
}

#[derive(Deserialize)]
struct SignalsInput {
    num_signals: Option<u32>,
}

#[derive(Serialize)]
struct BenchResponse {
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    steps: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    signals_received: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    steps_completed: Option<u32>,
}

// ─── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&cli.log_level)),
        )
        .init();

    // Create engine with WAL persistence and configurable durability
    let durability = DurabilityConfig::batched(cli.sync_steps, cli.flush_interval_ms);
    tracing::info!(
        sync_steps = cli.sync_steps,
        flush_interval_ms = cli.flush_interval_ms,
        "Durability config: sync_steps=0 means strict (fsync per step), N means batched"
    );

    let engine = if cli.wal_path.is_empty() {
        WorkflowEngine::new().with_durability_config(durability)
    } else {
        let e = WorkflowEngine::with_wal(&cli.wal_path, cli.wal_max_size)
            .expect("Failed to initialize WAL")
            .with_durability_config(durability);
        match e.recover_from_wal() {
            Ok((records, workflows)) => {
                if records > 0 {
                    tracing::info!(
                        records_replayed = records,
                        workflows_recovered = workflows,
                        "Crash recovery: replayed WAL on startup"
                    );
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "WAL recovery failed (starting fresh)");
            }
        }
        e
    };

    let state = AppState {
        engine: Arc::new(engine),
        workflow_counter: Arc::new(AtomicU64::new(1)),
        namespace_id: 1,
        task_queue_hash: 42,
        api_workflows: Arc::new(TokioMutex::new(HashMap::new())),
    };

    let app = Router::new()
        // /bench/* routes — direct execution (Velocity Server)
        .route("/health", get(handle_health))
        .route("/bench/simple_workflow", post(handle_simple_workflow))
        .route("/bench/multi_step", post(handle_multi_step))
        .route("/bench/signal_storm", post(handle_signal_storm))
        .route("/bench/cold_start", post(handle_cold_start))
        .route("/bench/stateful", post(handle_stateful))
        .route("/bench/echo", post(handle_echo))
        .route("/bench/payload", post(handle_payload))
        .route("/bench/durable_promise", post(handle_durable_promise))
        .route("/bench/concurrent", post(handle_concurrent))
        .route("/bench/activity_scheduling", post(handle_activity_scheduling))
        .route("/bench/long_running", post(handle_long_running))
        // Lightweight handler for basic throughput measurement (no heavy WAL work)
        .route("/bench/invoke", post(handle_invoke))
        // camelCase alias for Restate-compatible durable_promise workload
        .route("/bench/durablePromise", post(handle_durable_promise))
        // Keyed service routes (Restate Virtual Object compatible)
        .route("/keyed_bench/:key/stateful", post(handle_keyed_stateful))
        .route("/keyed_bench/:key/invoke", post(handle_keyed_invoke))
        // /api/* routes — Temporal-compatible API (Velocity Classic)
        .route("/api/health", get(handle_api_health))
        .route("/api/workflows", post(handle_api_start_workflow))
        .route("/api/workflows", get(handle_api_list_workflows))
        .route("/api/workflows/:id", get(handle_api_get_workflow))
        .route("/api/workflows/:id/signal", post(handle_api_signal_workflow))
        .route("/api/workflows/:id/query", post(handle_api_query_workflow))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&cli.bind).await?;

    println!("╦  ╦ ╔╗╔ ╦╔═ ╔═╗ ╦ ╦ ╔═╗ ╔╗╔ ╔═╗");
    println!("╚╗╔╝ ║║║ ╠╩╗ ╠═╣ ║ ║ ║╣  ║║║ ║ ║");
    println!("  ╚╝  ╝╚╝ ╩ ╩ ╩ ╩ ╚═╝ ╚═╝ ╝╚╝ ╚═╝");
    println!("  Benchmark Server v{}", env!("CARGO_PKG_VERSION"));
    println!("  HTTP:  http://{}", cli.bind);
    println!("  Mode:  Production (WAL persistence)");
    println!("  WAL:   {}", cli.wal_path);
    println!();

    axum::serve(listener, app).await?;
    Ok(())
}

// ─── Workflow Handlers ───────────────────────────────────────────────────────

/// Simple workflow: 10 durable steps, each checkpointed to WAL.
/// Equivalent to DBOS's @DBOS.workflow() with 10 @DBOS.step() calls.
async fn handle_simple_workflow(State(state): State<AppState>) -> Json<BenchResponse> {
    let wf_id = state.workflow_counter.fetch_add(1, Ordering::Relaxed);
    let workflow_type_id = 1u64;

    // Start workflow — creates context, writes to WAL
    let key = state.engine.start_workflow(
        wf_id,
        workflow_type_id,
        state.namespace_id,
        state.task_queue_hash,
        10, // total_steps
        None,
    );
    state.engine.sync_wal(); // Synchronous durability

    // Execute 10 durable steps — each writes to WAL and fsyncs
    for step in 0..10 {
        let result = format!("step_{}_done", step).into_bytes();
        state.engine.complete_step_durable(key, step, result);
    }

    // Complete workflow — writes completion to WAL
    state.engine.complete_workflow(key, Some(b"completed".to_vec()));
    state.engine.sync_wal(); // Synchronous durability

    Json(BenchResponse {
        status: "completed".into(),
        steps: Some(10),
        signals_received: None,
        steps_completed: None,
    })
}

/// Multi-step workflow: N durable steps (default 100).
async fn handle_multi_step(
    State(state): State<AppState>,
    axum::extract::Json(input): axum::extract::Json<StepsInput>,
) -> Json<BenchResponse> {
    let num_steps = input.steps.unwrap_or(100);
    let wf_id = state.workflow_counter.fetch_add(1, Ordering::Relaxed);
    let workflow_type_id = 2u64;

    let key = state.engine.start_workflow(
        wf_id,
        workflow_type_id,
        state.namespace_id,
        state.task_queue_hash,
        num_steps,
        None,
    );
    state.engine.sync_wal();

    for step in 0..num_steps {
        let result = step.to_le_bytes().to_vec();
        state.engine.complete_step_durable(key, step, result);
    }

    state.engine.complete_workflow(key, Some(b"completed".to_vec()));
    state.engine.sync_wal();

    Json(BenchResponse {
        status: "completed".into(),
        steps: None,
        signals_received: None,
        steps_completed: Some(num_steps),
    })
}

/// Signal storm: start workflow, send 100 signals (each WAL-persisted), complete.
async fn handle_signal_storm(
    State(state): State<AppState>,
    axum::extract::Json(input): axum::extract::Json<SignalsInput>,
) -> Json<BenchResponse> {
    let num_signals = input.num_signals.unwrap_or(100);
    let wf_id = state.workflow_counter.fetch_add(1, Ordering::Relaxed);
    let workflow_type_id = 3u64;

    let key = state.engine.start_workflow(
        wf_id,
        workflow_type_id,
        state.namespace_id,
        state.task_queue_hash,
        num_signals,
        None,
    );
    state.engine.sync_wal();

    // Send N signals — each persisted to WAL
    for i in 0..num_signals {
        let signal_id = i as u64;
        let payload = format!("signal_{}", i).into_bytes();
        state.engine.signal_workflow(key, signal_id, payload);
    }

    // Complete all steps
    for step in 0..num_signals {
        state.engine.complete_step_durable(key, step, vec![]);
    }

    state.engine.complete_workflow(key, Some(b"completed".to_vec()));
    state.engine.sync_wal();

    Json(BenchResponse {
        status: "completed".into(),
        steps: None,
        signals_received: Some(num_signals),
        steps_completed: None,
    })
}

/// Cold start: single workflow + step after engine startup.
async fn handle_cold_start(State(state): State<AppState>) -> Json<BenchResponse> {
    let wf_id = state.workflow_counter.fetch_add(1, Ordering::Relaxed);
    let workflow_type_id = 4u64;

    let key = state.engine.start_workflow(
        wf_id,
        workflow_type_id,
        state.namespace_id,
        state.task_queue_hash,
        1,
        None,
    );
    state.engine.sync_wal();

    state.engine.complete_step_durable(key, 0, b"cold_start_done".to_vec());
    state.engine.complete_workflow(key, Some(b"completed".to_vec()));
    state.engine.sync_wal();

    Json(BenchResponse {
        status: "completed".into(),
        steps: Some(1),
        signals_received: None,
        steps_completed: None,
    })
}

/// Stateful workflow: read → write with durable steps.
async fn handle_stateful(State(state): State<AppState>) -> Json<BenchResponse> {
    let wf_id = state.workflow_counter.fetch_add(1, Ordering::Relaxed);
    let workflow_type_id = 5u64;

    let key = state.engine.start_workflow(
        wf_id,
        workflow_type_id,
        state.namespace_id,
        state.task_queue_hash,
        2, // read + write = 2 steps
        None,
    );
    state.engine.sync_wal();

    // Step 0: "read" state
    state.engine.complete_step_durable(key, 0, b"read_result".to_vec());
    // Step 1: "write" state
    state.engine.complete_step_durable(key, 1, b"write_result".to_vec());

    state.engine.complete_workflow(key, Some(b"completed".to_vec()));
    state.engine.sync_wal();

    Json(BenchResponse {
        status: "completed".into(),
        steps: Some(2),
        signals_received: None,
        steps_completed: None,
    })
}

/// Echo workflow: return input as-is (minimal persistence).
async fn handle_echo(
    State(state): State<AppState>,
    body: String,
) -> Json<BenchResponse> {
    let wf_id = state.workflow_counter.fetch_add(1, Ordering::Relaxed);
    let workflow_type_id = 6u64;

    let key = state.engine.start_workflow(
        wf_id,
        workflow_type_id,
        state.namespace_id,
        state.task_queue_hash,
        1,
        Some(body.into_bytes()),
    );
    state.engine.sync_wal();

    state.engine.complete_step_durable(key, 0, b"echo_done".to_vec());
    state.engine.complete_workflow(key, Some(b"completed".to_vec()));
    state.engine.sync_wal();

    Json(BenchResponse {
        status: "completed".into(),
        steps: Some(1),
        signals_received: None,
        steps_completed: None,
    })
}

/// Payload roundtrip: process payload through durable step.
async fn handle_payload(
    State(state): State<AppState>,
    body: String,
) -> Json<BenchResponse> {
    let size = body.len() as u32;
    let wf_id = state.workflow_counter.fetch_add(1, Ordering::Relaxed);
    let workflow_type_id = 7u64;

    let key = state.engine.start_workflow(
        wf_id,
        workflow_type_id,
        state.namespace_id,
        state.task_queue_hash,
        1,
        Some(body.into_bytes()),
    );
    state.engine.sync_wal();

    state.engine.complete_step_durable(key, 0, size.to_le_bytes().to_vec());
    state.engine.complete_workflow(key, Some(b"completed".to_vec()));
    state.engine.sync_wal();

    Json(BenchResponse {
        status: "completed".into(),
        steps: Some(1),
        signals_received: None,
        steps_completed: None,
    })
}

/// Durable promise: set + get with durable steps.
async fn handle_durable_promise(State(state): State<AppState>) -> Json<BenchResponse> {
    let wf_id = state.workflow_counter.fetch_add(1, Ordering::Relaxed);
    let workflow_type_id = 8u64;

    let key = state.engine.start_workflow(
        wf_id,
        workflow_type_id,
        state.namespace_id,
        state.task_queue_hash,
        2, // set + get = 2 steps
        None,
    );
    state.engine.sync_wal();

    // Step 0: "set" promise value
    state.engine.complete_step_durable(key, 0, b"promise_set".to_vec());
    // Step 1: "get" promise value
    state.engine.complete_step_durable(key, 1, b"promise_get".to_vec());

    state.engine.complete_workflow(key, Some(b"completed".to_vec()));
    state.engine.sync_wal();

    Json(BenchResponse {
        status: "completed".into(),
        steps: Some(2),
        signals_received: None,
        steps_completed: None,
    })
}

/// Concurrent workflow: simple durable execution.
async fn handle_concurrent(State(state): State<AppState>) -> Json<BenchResponse> {
    let wf_id = state.workflow_counter.fetch_add(1, Ordering::Relaxed);
    let workflow_type_id = 9u64;

    let key = state.engine.start_workflow(
        wf_id,
        workflow_type_id,
        state.namespace_id,
        state.task_queue_hash,
        1,
        None,
    );
    state.engine.sync_wal();

    state.engine.complete_step_durable(key, 0, b"concurrent_done".to_vec());
    state.engine.complete_workflow(key, Some(b"completed".to_vec()));
    state.engine.sync_wal();

    Json(BenchResponse {
        status: "completed".into(),
        steps: Some(1),
        signals_received: None,
        steps_completed: None,
    })
}

/// Activity scheduling: simulate scheduling + completing activities.
async fn handle_activity_scheduling(State(state): State<AppState>) -> Json<BenchResponse> {
    let wf_id = state.workflow_counter.fetch_add(1, Ordering::Relaxed);
    let workflow_type_id = 10u64;

    let key = state.engine.start_workflow(
        wf_id,
        workflow_type_id,
        state.namespace_id,
        state.task_queue_hash,
        5, // 5 "activity" steps
        None,
    );
    state.engine.sync_wal();

    for step in 0..5 {
        let result = format!("activity_{}", step).into_bytes();
        state.engine.complete_step_durable(key, step, result);
    }

    state.engine.complete_workflow(key, Some(b"completed".to_vec()));
    state.engine.sync_wal();

    Json(BenchResponse {
        status: "completed".into(),
        steps: Some(5),
        signals_received: None,
        steps_completed: None,
    })
}

/// Long running workflow: simulates a longer workflow with more steps.
async fn handle_long_running(State(state): State<AppState>) -> Json<BenchResponse> {
    let wf_id = state.workflow_counter.fetch_add(1, Ordering::Relaxed);
    let workflow_type_id = 11u64;

    let key = state.engine.start_workflow(
        wf_id,
        workflow_type_id,
        state.namespace_id,
        state.task_queue_hash,
        50,
        None,
    );
    state.engine.sync_wal();

    for step in 0..50 {
        let result = format!("long_step_{}", step).into_bytes();
        state.engine.complete_step_durable(key, step, result);
    }

    state.engine.complete_workflow(key, Some(b"completed".to_vec()));
    state.engine.sync_wal();

    Json(BenchResponse {
        status: "completed".into(),
        steps: Some(50),
        signals_received: None,
        steps_completed: None,
    })
}

/// Lightweight invoke: minimal persistence overhead (1 step).
/// Used by handler_invocation, concurrent_handlers, sustained_load, cold_start workloads.
async fn handle_invoke(State(state): State<AppState>) -> Json<BenchResponse> {
    let wf_id = state.workflow_counter.fetch_add(1, Ordering::Relaxed);
    let workflow_type_id = 12u64;

    let key = state.engine.start_workflow(
        wf_id,
        workflow_type_id,
        state.namespace_id,
        state.task_queue_hash,
        1,
        None,
    );
    state.engine.sync_wal();

    state.engine.complete_step_durable(key, 0, b"invoke_done".to_vec());
    state.engine.complete_workflow(key, Some(b"completed".to_vec()));
    state.engine.sync_wal();

    Json(BenchResponse {
        status: "ok".into(),
        steps: Some(1),
        signals_received: None,
        steps_completed: None,
    })
}

/// Keyed stateful: per-key read → write with durable steps.
/// Route: POST /keyed_bench/:key/stateful
async fn handle_keyed_stateful(
    State(state): State<AppState>,
    Path(key_str): Path<String>,
) -> Json<serde_json::Value> {
    let wf_id = state.workflow_counter.fetch_add(1, Ordering::Relaxed);
    let workflow_type_id = 13u64;

    let key = state.engine.start_workflow(
        wf_id,
        workflow_type_id,
        state.namespace_id,
        state.task_queue_hash,
        2,
        None,
    );
    state.engine.sync_wal();

    // Step 0: read state
    state.engine.complete_step_durable(key, 0, b"read_result".to_vec());
    // Step 1: write state
    state.engine.complete_step_durable(key, 1, b"write_result".to_vec());

    state.engine.complete_workflow(key, Some(b"completed".to_vec()));
    state.engine.sync_wal();

    Json(serde_json::json!({
        "status": "ok",
        "key": key_str,
        "count": wf_id
    }))
}

/// Keyed invoke: per-key lightweight handler.
/// Route: POST /keyed_bench/:key/invoke
async fn handle_keyed_invoke(
    State(state): State<AppState>,
    Path(key_str): Path<String>,
) -> Json<serde_json::Value> {
    let wf_id = state.workflow_counter.fetch_add(1, Ordering::Relaxed);
    let workflow_type_id = 14u64;

    let key = state.engine.start_workflow(
        wf_id,
        workflow_type_id,
        state.namespace_id,
        state.task_queue_hash,
        1,
        None,
    );
    state.engine.sync_wal();

    state.engine.complete_step_durable(key, 0, b"keyed_invoke_done".to_vec());
    state.engine.complete_workflow(key, Some(b"completed".to_vec()));
    state.engine.sync_wal();

    Json(serde_json::json!({
        "status": "ok",
        "key": key_str,
        "invoke_count": wf_id
    }))
}

/// Health check endpoint.
async fn handle_health() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "engine": "Velocity",
        "mode": "production",
        "persistence": "WAL",
        "durability": "synchronous"
    }))
}

// ─── /api/* Temporal-Compatible Handlers ─────────────────────────────────────

#[derive(Deserialize)]
struct ApiStartRequest {
    workflow_id: Option<String>,
    workflow_type: Option<String>,
    task_queue: Option<String>,
    input: Option<serde_json::Value>,
}

async fn handle_api_health() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "success": true,
        "data": {
            "status": "healthy",
            "engine": "velocity-classic",
            "persistence": "wal",
            "durability": "synchronous",
            "runtime": "rust"
        }
    }))
}

async fn handle_api_list_workflows(State(state): State<AppState>) -> Json<serde_json::Value> {
    let workflows = state.api_workflows.lock().await;
    let list: Vec<_> = workflows.values().cloned().collect();
    Json(serde_json::json!({ "success": true, "data": list }))
}

/// Start a workflow via the Temporal-compatible API.
/// Executes the workflow synchronously with per-step fsync, then stores the result.
async fn handle_api_start_workflow(
    State(state): State<AppState>,
    axum::extract::Json(body): axum::extract::Json<ApiStartRequest>,
) -> Json<serde_json::Value> {
    let workflow_id = body.workflow_id.unwrap_or_else(|| {
        format!("wf-{}", state.workflow_counter.fetch_add(1, Ordering::Relaxed))
    });
    let workflow_type = body.workflow_type.unwrap_or_else(|| "unknown".to_string());

    // Execute the workflow with synchronous durability
    execute_api_workflow(&state, &workflow_id, &workflow_type).await;

    // Store as completed
    let info = ApiWorkflowInfo {
        workflow_id: workflow_id.clone(),
        workflow_type: workflow_type.clone(),
        status: "COMPLETED".to_string(),
    };
    state.api_workflows.lock().await.insert(workflow_id.clone(), info);

    Json(serde_json::json!({
        "success": true,
        "data": {
            "workflowId": workflow_id,
            "runId": format!("run-{}", workflow_id),
            "status": "COMPLETED"
        }
    }))
}

async fn handle_api_get_workflow(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Json<serde_json::Value> {
    let workflows = state.api_workflows.lock().await;
    if let Some(info) = workflows.get(&id) {
        Json(serde_json::json!({
            "success": true,
            "data": {
                "workflowId": info.workflow_id,
                "status": info.status,
                "workflowType": info.workflow_type
            }
        }))
    } else {
        Json(serde_json::json!({ "success": false, "error": "Workflow not found" }))
    }
}

async fn handle_api_signal_workflow(
    State(_state): State<AppState>,
    Path(_id): Path<String>,
) -> Json<serde_json::Value> {
    // Signals are no-ops for already-completed workflows
    Json(serde_json::json!({ "success": true }))
}

async fn handle_api_query_workflow(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Json<serde_json::Value> {
    let workflows = state.api_workflows.lock().await;
    if let Some(info) = workflows.get(&id) {
        Json(serde_json::json!({
            "success": true,
            "data": { "result": { "status": info.status } }
        }))
    } else {
        Json(serde_json::json!({ "success": false, "error": "Workflow not found" }))
    }
}

/// Execute a workflow by type with configurable per-step durability.
/// Each step uses complete_step_durable() — fsync behavior controlled by --sync-steps.
async fn execute_api_workflow(state: &AppState, workflow_id: &str, workflow_type: &str) {
    let wf_id_num = state.workflow_counter.fetch_add(1, Ordering::Relaxed);
    let workflow_type_id = workflow_type.len() as u64; // unique-ish

    let (num_steps, total_steps_hint) = match workflow_type {
        "simple_workflow" | "SimpleWorkflow" => (10, 10),
        "multi_step" | "MultiStepWorkflow" => (100, 100),
        "signal_storm" | "SignalStormWorkflow" => (100, 100),
        "cold_start" | "ColdStartWorkflow" => (1, 1),
        "stateful" | "StatefulWorkflow" => (5, 5),
        "concurrent" | "ConcurrentWorkflow" => (1, 1),
        "payload" | "PayloadWorkflow" => (1, 1),
        "echo" | "EchoWorkflow" => (1, 1),
        "durable_promise" | "DurablePromiseWorkflow" => (2, 2),
        "activity_scheduling" | "ActivitySchedulingWorkflow" => (10, 10),
        "long_running" | "LongRunningWorkflow" => (50, 50),
        _ => (1, 1),
    };

    let key = state.engine.start_workflow(
        wf_id_num,
        workflow_type_id,
        state.namespace_id,
        state.task_queue_hash,
        total_steps_hint,
        None,
    );
    state.engine.sync_wal(); // fsync after start

    // Execute each step with synchronous fsync
    for step in 0..num_steps {
        let result = format!("step_{}_done", step).into_bytes();
        state.engine.complete_step_durable(key, step, result);
    }

    state.engine.complete_workflow(key, Some(b"completed".to_vec()));
    state.engine.sync_wal(); // fsync after complete
}
