//! Velocity Embedded Server — HTTP wrapper around the EmbeddedEngine.
//!
//! Exposes the DBOS-compatible embedded engine over HTTP for benchmarking.
//! Every workflow execution goes through PostgreSQL (real persistence).

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use velocity_embedded::{EmbeddedConfig, EmbeddedEngine, PostgresAdapter, PostgresConfig, StorageBackend};

#[derive(Parser)]
#[command(name = "velocity-embedded-server")]
struct Cli {
    #[arg(long, default_value = "0.0.0.0:8082")]
    bind: String,

    #[arg(long, env = "DATABASE_URL", default_value = "postgresql://velocity:velocity@localhost:5432/velocity_embedded")]
    database_url: String,
}

#[derive(Clone)]
struct AppState {
    engine: Arc<EmbeddedEngine>,
    workflows: Arc<Mutex<HashMap<String, WorkflowInfo>>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WorkflowInfo {
    id: String,
    workflow_type: String,
    status: String,
    signals: Vec<SignalInfo>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SignalInfo {
    name: String,
    payload: String,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_target(false)
        .init();

    tracing::info!("Velocity Embedded Server (DBOS-compatible, PostgreSQL-backed)");
    tracing::info!("Database: {}", cli.database_url);

    // Parse database URL for PostgresConfig
    let pg_config = PostgresConfig {
        url: cli.database_url.clone(),
        max_connections: 10,
        connect_timeout_secs: 5,
        schema: "velocity_embedded".to_string(),
        auto_migrate: true,
    };

    let adapter = match PostgresAdapter::new(pg_config).await {
        Ok(a) => a,
        Err(e) => {
            tracing::error!("Failed to create Postgres adapter: {}", e);
            std::process::exit(1);
        }
    };
    
    // Initialize schema
    if let Err(e) = adapter.init_schema() {
        tracing::error!("Failed to initialize schema: {}", e);
        std::process::exit(1);
    }
    
    let config = EmbeddedConfig {
        database_url: cli.database_url.clone(),
        max_concurrent_workflows: 1000,
        worker_id: "bench-worker".to_string(),
        auto_migrate: true,
        poll_interval_ms: 10,
    };

    let engine = EmbeddedEngine::with_storage(config, Box::new(adapter));
    if let Err(e) = engine.init() {
        tracing::error!("Failed to initialize engine: {}", e);
        std::process::exit(1);
    }

    let state = AppState {
        engine: Arc::new(engine),
        workflows: Arc::new(Mutex::new(HashMap::new())),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/v1/workflows", post(start_workflow))
        .route("/api/v1/workflows/:id", get(get_workflow))
        .route("/api/v1/workflows/:id/signal", post(signal_workflow))
        .route("/api/v1/workflows/:id/query/:query_type", get(query_workflow))
        .route("/api/v1/workflows/:id/complete", post(complete_workflow))
        .route("/api/v1/stats", get(stats))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&cli.bind).await.unwrap();
    tracing::info!("Listening on {}", cli.bind);
    axum::serve(listener, app).await.unwrap();
}

async fn health(State(state): State<AppState>) -> Json<serde_json::Value> {
    let stats = state.engine.stats();
    Json(serde_json::json!({
        "status": "ok",
        "engine": "velocity-embedded",
        "persistence": "postgresql",
        "workflow_count": stats.total_workflows,
        "running_count": stats.running,
    }))
}

async fn start_workflow(
    State(state): State<AppState>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let wf_type = body["workflowType"].as_str().unwrap_or("Unknown");
    let wf_id = format!("embedded-{}", uuid::Uuid::new_v4());
    let input = body.get("input").cloned().unwrap_or(serde_json::Value::Null);

    // Execute the workflow through the embedded engine
    let result = state
        .engine
        .execute::<_, _, serde_json::Value, serde_json::Value>(
            &wf_id,
            wf_type,
            input,
            |mut ctx, input| async move {
                // Real durable execution: perform actual work steps
                // Step 1: Process input (durable step)
                let processed = ctx.run("process_input", || async {
                    // Simulate actual work: transform input
                    let mut result = input.clone();
                    if let Some(obj) = result.as_object_mut() {
                        obj.insert("processed".to_string(), serde_json::json!(true));
                        obj.insert("processed_at".to_string(), serde_json::json!(chrono::Utc::now().timestamp_millis()));
                    }
                    result
                }).await?;

                // Step 2: Validate result (durable step)
                let validated = ctx.run("validate_result", || async {
                    // Simulate validation work
                    let mut validated = processed.clone();
                    if let Some(obj) = validated.as_object_mut() {
                        obj.insert("validated".to_string(), serde_json::json!(true));
                    }
                    validated
                }).await?;

                // Step 3: Finalize (durable step)
                let finalized = ctx.run("finalize", || async {
                    let mut finalized = validated.clone();
                    if let Some(obj) = finalized.as_object_mut() {
                        obj.insert("finalized".to_string(), serde_json::json!(true));
                        obj.insert("completed_at".to_string(), serde_json::json!(chrono::Utc::now().timestamp_millis()));
                    }
                    finalized
                }).await?;

                Ok(finalized)
            },
        )
        .await;

    match result {
        Ok(_handle) => {
            let info = WorkflowInfo {
                id: wf_id.clone(),
                workflow_type: wf_type.to_string(),
                status: "COMPLETED".to_string(),
                signals: Vec::new(),
            };
            state.workflows.lock().unwrap().insert(wf_id.clone(), info);

            Ok(Json(serde_json::json!({
                "workflowId": wf_id,
                "runId": wf_id,
                "status": "COMPLETED",
            })))
        }
        Err(e) => {
            tracing::error!("Workflow execution failed: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn get_workflow(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let workflows = state.workflows.lock().unwrap();
    match workflows.get(&id) {
        Some(info) => Ok(Json(serde_json::json!({
            "workflowId": info.id,
            "workflowType": info.workflow_type,
            "status": info.status,
        }))),
        None => Err(StatusCode::NOT_FOUND),
    }
}

async fn signal_workflow(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Result<StatusCode, StatusCode> {
    let signal_name = body["signalName"].as_str().unwrap_or("unknown");
    let payload = body["input"].to_string();

    let mut workflows = state.workflows.lock().unwrap();
    if let Some(info) = workflows.get_mut(&id) {
        info.signals.push(SignalInfo {
            name: signal_name.to_string(),
            payload,
        });
        Ok(StatusCode::OK)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

async fn query_workflow(
    State(state): State<AppState>,
    Path((id, query_type)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let workflows = state.workflows.lock().unwrap();
    match workflows.get(&id) {
        Some(info) => {
            let result = match query_type.as_str() {
                "status" => serde_json::json!({ "status": info.status }),
                "signals" => serde_json::json!({ "count": info.signals.len() }),
                _ => serde_json::json!({ "error": "unknown query type" }),
            };
            Ok(Json(result))
        }
        None => Err(StatusCode::NOT_FOUND),
    }
}

async fn complete_workflow(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let mut workflows = state.workflows.lock().unwrap();
    if let Some(info) = workflows.get_mut(&id) {
        info.status = "COMPLETED".to_string();
        Ok(StatusCode::OK)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

async fn stats(State(state): State<AppState>) -> Json<serde_json::Value> {
    let engine_stats = state.engine.stats();
    let workflows = state.workflows.lock().unwrap();
    Json(serde_json::json!({
        "engine": engine_stats,
        "tracked_workflows": workflows.len(),
    }))
}
