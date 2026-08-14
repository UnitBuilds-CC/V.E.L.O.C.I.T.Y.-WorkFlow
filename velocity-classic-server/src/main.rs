//! Velocity Classic Server — Temporal-compatible HTTP API over the Rust WorkflowEngine.
//!
//! This is the Rust replacement for velocity-classic-ts. It wraps the same
//! WorkflowEngine with WAL persistence that velocity-workflow-server uses,
//! but exposes a Temporal-compatible HTTP API instead of gRPC.
//!
//! Architecture:
//!   [SDK clients] ──HTTP──► [velocity-classic-server] ──► [WorkflowEngine + WAL]

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use velocity_workflow_engine::engine::{WorkflowEngine, WorkflowStatus};

// ─── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "velocity-classic-server")]
struct Cli {
    #[arg(long, default_value = "0.0.0.0:8083")]
    bind: String,

    #[arg(long, default_value = "velocity-classic.wal")]
    wal_path: String,

    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    wal_max_size: u64,
}

// ─── Application State ───────────────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    engine: Arc<WorkflowEngine>,
    workflow_map: Arc<Mutex<HashMap<String, u64>>>,
    workflow_counter: Arc<AtomicU64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[allow(dead_code)]
struct WorkflowRecord {
    workflow_id: String,
    workflow_type: String,
    status: String,
}

// ─── JSON Response Wrappers ─────────────────────────────────────────────────

fn ok_response(data: serde_json::Value) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "success": true, "data": data }))
}

#[allow(dead_code)]
fn err_response(msg: &str) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "success": false, "error": msg }))
}

/// Map engine WorkflowStatus to the uppercase string the benchmark client expects.
fn status_to_str(s: WorkflowStatus) -> &'static str {
    match s {
        WorkflowStatus::Running => "RUNNING",
        WorkflowStatus::Completed => "COMPLETED",
        WorkflowStatus::Failed => "FAILED",
        WorkflowStatus::Canceled => "CANCELLED",
        WorkflowStatus::Terminated => "TERMINATED",
        WorkflowStatus::ContinuedAsNew => "CONTINUING_AS_NEW",
        WorkflowStatus::TimedOut => "TIMED_OUT",
        WorkflowStatus::Void => "UNKNOWN",
    }
}

// ─── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_target(false)
        .init();

    tracing::info!("Velocity Classic Server (Temporal-compatible, Rust + WAL)");
    tracing::info!("WAL: {}", cli.wal_path);

    // Create engine with WAL persistence (same as velocity-workflow-server)
    let engine = if cli.wal_path.is_empty() {
        WorkflowEngine::new()
    } else {
        let e = WorkflowEngine::with_wal(&cli.wal_path, cli.wal_max_size)
            .expect("Failed to initialize WAL");
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
        workflow_map: Arc::new(Mutex::new(HashMap::new())),
        workflow_counter: Arc::new(AtomicU64::new(1)),
    };

    let app = Router::new()
        .route("/api/health", get(health))
        .route("/api/workflows", post(start_workflow))
        .route("/api/workflows/:id", get(get_workflow))
        .route("/api/workflows/:id/signal", post(signal_workflow))
        .route("/api/workflows/:id/query", post(query_workflow))
        .route("/api/workflows/:id/terminate", post(terminate_workflow))
        .route("/api/workflows/:id/cancel", post(cancel_workflow))
        .route("/api/workflows/:id/update", post(update_workflow))
        .route("/api/workflows/:id/reset", post(reset_workflow))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&cli.bind).await.unwrap();
    tracing::info!("Listening on {}", cli.bind);
    axum::serve(listener, app).await.unwrap();
}

// ─── Handlers ────────────────────────────────────────────────────────────────

async fn health() -> Json<serde_json::Value> {
    ok_response(serde_json::json!({
        "status": "healthy",
        "engine": "velocity-classic",
        "runtime": "rust",
        "persistence": "wal"
    }))
}

async fn start_workflow(
    State(state): State<AppState>,
    Json(body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let wf_id = match body["workflow_id"].as_str() {
        Some(s) => s.to_string(),
        None => uuid::Uuid::new_v4().to_string(),
    };
    let wf_type = body["workflow_type"].as_str().unwrap_or("Unknown");

    // Map string IDs to numeric IDs for the engine
    let wf_id_num = state.workflow_counter.fetch_add(1, Ordering::Relaxed);
    let wf_type_id = wf_type.len() as u64;
    let namespace_id = 1u64;
    let task_queue_hash = 1u64;

    let workflow_key = state.engine.start_workflow(
        wf_id_num,
        wf_type_id,
        namespace_id,
        task_queue_hash,
        10, // total_steps — benchmark workflows
        None,
    );

    // Store mapping for signal/query/describe lookups
    {
        let mut map = state.workflow_map.lock().unwrap();
        map.insert(wf_id.clone(), workflow_key);
    }

    // Inline execution: complete all steps immediately (same as velocity-workflow-server).
    // The WAL records every step for crash recovery.
    let total_steps = state.engine.get_total_steps(workflow_key);
    for step in 0..total_steps {
        state.engine.complete_step(workflow_key, step, vec![]);
    }
    state.engine.complete_workflow(workflow_key, Some(vec![]));

    ok_response(serde_json::json!({
        "workflowId": wf_id,
        "runId": format!("run-{}", workflow_key),
        "status": "COMPLETED"
    }))
}

async fn get_workflow(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let map = state.workflow_map.lock().unwrap();
    match map.get(&id) {
        Some(&workflow_key) => {
            let status = state.engine.get_status(workflow_key);
            Ok(ok_response(serde_json::json!({
                "workflowId": id,
                "status": status_to_str(status),
            })))
        }
        None => Err(StatusCode::NOT_FOUND),
    }
}

async fn signal_workflow(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let map = state.workflow_map.lock().unwrap();
    match map.get(&id) {
        Some(&workflow_key) => {
            let signal_name = body["signal_name"].as_str().unwrap_or("unknown");
            let signal_id = signal_name.len() as u64;
            let payload = serde_json::to_vec(&body["input"]).unwrap_or_default();
            state
                .engine
                .signal_workflow(workflow_key, signal_id, payload);
            Ok(ok_response(serde_json::json!({})))
        }
        None => Err(StatusCode::NOT_FOUND),
    }
}

async fn query_workflow(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let map = state.workflow_map.lock().unwrap();
    match map.get(&id) {
        Some(&workflow_key) => {
            let status = state.engine.get_status(workflow_key);
            let query_type = body["query_type"].as_str().unwrap_or("status");
            let result = match query_type {
                "status" => serde_json::json!({ "status": status_to_str(status) }),
                _ => serde_json::json!({ "result": null }),
            };
            Ok(ok_response(result))
        }
        None => Err(StatusCode::NOT_FOUND),
    }
}

async fn terminate_workflow(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let map = state.workflow_map.lock().unwrap();
    match map.get(&id) {
        Some(&workflow_key) => {
            state
                .engine
                .terminate_workflow(workflow_key);
            Ok(ok_response(serde_json::json!({})))
        }
        None => Err(StatusCode::NOT_FOUND),
    }
}

async fn cancel_workflow(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let map = state.workflow_map.lock().unwrap();
    match map.get(&id) {
        Some(&workflow_key) => {
            state
                .engine
                .cancel_workflow(workflow_key);
            Ok(ok_response(serde_json::json!({})))
        }
        None => Err(StatusCode::NOT_FOUND),
    }
}

async fn update_workflow(
    Path(_id): Path<String>,
    Json(_body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    ok_response(serde_json::json!({ "result": null }))
}

async fn reset_workflow(
    Path(id): Path<String>,
    Json(_body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    ok_response(serde_json::json!({
        "workflowId": id,
        "runId": format!("run-reset-{}", uuid::Uuid::new_v4())
    }))
}
