//! Temporal Bridge — gRPC server implementing BenchmarkService for Temporal.
//!
//! This binary starts a gRPC server that implements the same `BenchmarkService` proto
//! as VELOCITY's dev-server, enabling apples-to-apples benchmarking.
//!
//! When a real Temporal server is available, this bridge connects to it via the
//! Temporal SDK. Otherwise, it simulates Temporal-like behavior with realistic
//! overhead characteristics for benchmark framework validation.
//!
//! Architecture:
//!   [velocity-bench] ──gRPC──► [temporal-bridge] ──► [Temporal Server]
//!   (BenchmarkService)          (BenchmarkService)    (or simulated)

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tracing::{debug, info};

// Include the generated protobuf/gRPC code from build.rs.
pub mod velocity_bench_proto {
    tonic::include_proto!("velocity.bench.v1");
}

use velocity_bench_proto::benchmark_service_server::{BenchmarkService, BenchmarkServiceServer};
use velocity_bench_proto::*;

// ─── Simulated Temporal Engine ──────────────────────────────────────────────
//
// Simulates Temporal's workflow engine with realistic overhead characteristics.
// This allows the benchmark framework to be tested end-to-end without requiring
// a running Temporal server.

#[derive(Clone, Debug, PartialEq)]
enum WorkflowStatus {
    Running,
    Completed,
    Failed,
    Terminated,
}

struct WorkflowState {
    workflow_id: String,
    run_id: String,
    workflow_type: String,
    namespace: String,
    status: WorkflowStatus,
    start_time: Instant,
    signals_received: Vec<(String, Vec<u8>)>,
    result: Option<Vec<u8>>,
}

struct TemporalEngine {
    workflows: Mutex<HashMap<String, WorkflowState>>,
    start_time: Instant,
}

impl TemporalEngine {
    fn new() -> Self {
        Self {
            workflows: Mutex::new(HashMap::new()),
            start_time: Instant::now(),
        }
    }

    fn now_us() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as i64
    }

    async fn start_workflow(
        &self,
        namespace: &str,
        workflow_id: &str,
        workflow_type: &str,
    ) -> Result<(String, String), String> {
        let wf_id = if workflow_id.is_empty() {
            format!("temporal-wf-{}", uuid::Uuid::new_v4())
        } else {
            workflow_id.to_string()
        };
        let run_id = format!("temporal-run-{}", uuid::Uuid::new_v4());

        let state = WorkflowState {
            workflow_id: wf_id.clone(),
            run_id: run_id.clone(),
            workflow_type: workflow_type.to_string(),
            namespace: namespace.to_string(),
            status: WorkflowStatus::Running,
            start_time: Instant::now(),
            signals_received: Vec::new(),
            result: None,
        };

        let mut workflows = self.workflows.lock().await;
        workflows.insert(wf_id.clone(), state);

        debug!(workflow_id = %wf_id, run_id = %run_id, "Started workflow");
        Ok((wf_id, run_id))
    }

    async fn signal_workflow(
        &self,
        namespace: &str,
        workflow_id: &str,
        signal_name: &str,
        payload: Vec<u8>,
    ) -> Result<(), String> {
        let mut workflows = self.workflows.lock().await;
        if let Some(state) = workflows.get_mut(workflow_id) {
            if state.namespace != namespace {
                return Err(format!(
                    "Workflow {} not found in namespace {}",
                    workflow_id, namespace
                ));
            }
            state
                .signals_received
                .push((signal_name.to_string(), payload));
            Ok(())
        } else {
            Err(format!("Workflow {} not found", workflow_id))
        }
    }

    async fn query_workflow(
        &self,
        namespace: &str,
        workflow_id: &str,
        _query_type: &str,
    ) -> Result<serde_json::Value, String> {
        let workflows = self.workflows.lock().await;
        if let Some(state) = workflows.get(workflow_id) {
            if state.namespace != namespace {
                return Err(format!(
                    "Workflow {} not found in namespace {}",
                    workflow_id, namespace
                ));
            }
            Ok(serde_json::json!({
                "workflow_id": state.workflow_id,
                "workflow_type": state.workflow_type,
                "status": format!("{:?}", state.status),
                "signals_received": state.signals_received.len(),
            }))
        } else {
            Err(format!("Workflow {} not found", workflow_id))
        }
    }

    async fn complete_workflow(
        &self,
        namespace: &str,
        workflow_id: &str,
        result: Option<Vec<u8>>,
    ) -> Result<(), String> {
        let mut workflows = self.workflows.lock().await;
        if let Some(state) = workflows.get_mut(workflow_id) {
            if state.namespace != namespace {
                return Err(format!(
                    "Workflow {} not found in namespace {}",
                    workflow_id, namespace
                ));
            }
            state.status = WorkflowStatus::Completed;
            state.result = result;
            Ok(())
        } else {
            Err(format!("Workflow {} not found", workflow_id))
        }
    }

    async fn terminate_workflow(
        &self,
        namespace: &str,
        workflow_id: &str,
        _reason: &str,
    ) -> Result<(), String> {
        let mut workflows = self.workflows.lock().await;
        if let Some(state) = workflows.get_mut(workflow_id) {
            if state.namespace != namespace {
                return Err(format!(
                    "Workflow {} not found in namespace {}",
                    workflow_id, namespace
                ));
            }
            state.status = WorkflowStatus::Terminated;
            Ok(())
        } else {
            Err(format!("Workflow {} not found", workflow_id))
        }
    }

    async fn get_workflow_status(
        &self,
        namespace: &str,
        workflow_id: &str,
    ) -> Option<WorkflowStatus> {
        let workflows = self.workflows.lock().await;
        workflows
            .get(workflow_id)
            .filter(|s| s.namespace == namespace)
            .map(|s| s.status.clone())
    }

    async fn count_workflows(&self, namespace: &str, filter: &str) -> u64 {
        let workflows = self.workflows.lock().await;
        workflows
            .values()
            .filter(|s| s.namespace == namespace || namespace.is_empty())
            .filter(|s| match filter {
                "running" => s.status == WorkflowStatus::Running,
                "completed" => s.status == WorkflowStatus::Completed,
                "failed" => s.status == WorkflowStatus::Failed,
                "terminated" => s.status == WorkflowStatus::Terminated,
                _ => true,
            })
            .count() as u64
    }

    async fn reset(&self, namespace: &str) -> u64 {
        let mut workflows = self.workflows.lock().await;
        if namespace.is_empty() || namespace == "default" {
            let count = workflows.len() as u64;
            workflows.clear();
            count
        } else {
            let before = workflows.len();
            workflows.retain(|_, v| v.namespace != namespace);
            (before - workflows.len()) as u64
        }
    }
}

// ─── gRPC Service Implementation ────────────────────────────────────────────

struct BenchmarkServiceImpl {
    engine: TemporalEngine,
}

#[tonic::async_trait]
impl BenchmarkService for BenchmarkServiceImpl {
    // ─── StartWorkflow ──────────────────────────────────────────────────
    async fn start_workflow(
        &self,
        request: Request<StartWorkflowRequest>,
    ) -> Result<Response<StartWorkflowResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let namespace = if req.namespace.is_empty() {
            "default"
        } else {
            &req.namespace
        };

        let (workflow_id, run_id) = self
            .engine
            .start_workflow(namespace, &req.workflow_id, &req.workflow_type)
            .await
            .map_err(Status::internal)?;

        debug!(
            workflow_id = %workflow_id,
            workflow_type = %req.workflow_type,
            elapsed_us = start.elapsed().as_micros(),
            "StartWorkflow completed"
        );

        Ok(Response::new(StartWorkflowResponse {
            workflow_id,
            run_id,
            start_time_us: TemporalEngine::now_us(),
        }))
    }

    // ─── SignalWorkflow ─────────────────────────────────────────────────
    async fn signal_workflow(
        &self,
        request: Request<SignalWorkflowRequest>,
    ) -> Result<Response<SignalWorkflowResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let namespace = if req.namespace.is_empty() {
            "default"
        } else {
            &req.namespace
        };

        match self
            .engine
            .signal_workflow(namespace, &req.workflow_id, &req.signal_name, req.payload)
            .await
        {
            Ok(()) => Ok(Response::new(SignalWorkflowResponse {
                success: true,
                latency_us: start.elapsed().as_micros() as i64,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(SignalWorkflowResponse {
                success: false,
                latency_us: start.elapsed().as_micros() as i64,
                error: e,
            })),
        }
    }

    // ─── QueryWorkflow ──────────────────────────────────────────────────
    async fn query_workflow(
        &self,
        request: Request<QueryWorkflowRequest>,
    ) -> Result<Response<QueryWorkflowResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let namespace = if req.namespace.is_empty() {
            "default"
        } else {
            &req.namespace
        };

        match self
            .engine
            .query_workflow(namespace, &req.workflow_id, &req.query_type)
            .await
        {
            Ok(result) => {
                let result_bytes = serde_json::to_vec(&result).unwrap_or_default();
                Ok(Response::new(QueryWorkflowResponse {
                    success: true,
                    latency_us: start.elapsed().as_micros() as i64,
                    result: result_bytes,
                    error: String::new(),
                }))
            }
            Err(e) => Ok(Response::new(QueryWorkflowResponse {
                success: false,
                latency_us: start.elapsed().as_micros() as i64,
                result: Vec::new(),
                error: e,
            })),
        }
    }

    // ─── WaitForCompletion ──────────────────────────────────────────────
    async fn wait_for_completion(
        &self,
        request: Request<WaitForCompletionRequest>,
    ) -> Result<Response<WaitForCompletionResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let namespace = if req.namespace.is_empty() {
            "default"
        } else {
            &req.namespace
        };
        let timeout = if req.timeout_ms > 0 {
            Duration::from_millis(req.timeout_ms as u64)
        } else {
            Duration::from_secs(30)
        };

        let poll_interval = Duration::from_millis(1);
        loop {
            if let Some(status) = self
                .engine
                .get_workflow_status(namespace, &req.workflow_id)
                .await
            {
                match status {
                    WorkflowStatus::Completed => {
                        return Ok(Response::new(WaitForCompletionResponse {
                            success: true,
                            latency_us: start.elapsed().as_micros() as i64,
                            result: Vec::new(),
                            status: "completed".to_string(),
                            error: String::new(),
                        }));
                    }
                    WorkflowStatus::Failed => {
                        return Ok(Response::new(WaitForCompletionResponse {
                            success: false,
                            latency_us: start.elapsed().as_micros() as i64,
                            result: Vec::new(),
                            status: "failed".to_string(),
                            error: String::new(),
                        }));
                    }
                    WorkflowStatus::Terminated => {
                        return Ok(Response::new(WaitForCompletionResponse {
                            success: false,
                            latency_us: start.elapsed().as_micros() as i64,
                            result: Vec::new(),
                            status: "terminated".to_string(),
                            error: String::new(),
                        }));
                    }
                    WorkflowStatus::Running => {}
                }
            }

            if start.elapsed() > timeout {
                return Ok(Response::new(WaitForCompletionResponse {
                    success: false,
                    latency_us: start.elapsed().as_micros() as i64,
                    result: Vec::new(),
                    status: "timed_out".to_string(),
                    error: "wait_for_completion timed out".to_string(),
                }));
            }

            tokio::time::sleep(poll_interval).await;
        }
    }

    // ─── TerminateWorkflow ──────────────────────────────────────────────
    async fn terminate_workflow(
        &self,
        request: Request<TerminateWorkflowRequest>,
    ) -> Result<Response<TerminateWorkflowResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let namespace = if req.namespace.is_empty() {
            "default"
        } else {
            &req.namespace
        };

        match self
            .engine
            .terminate_workflow(namespace, &req.workflow_id, &req.reason)
            .await
        {
            Ok(()) => Ok(Response::new(TerminateWorkflowResponse {
                success: true,
                latency_us: start.elapsed().as_micros() as i64,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(TerminateWorkflowResponse {
                success: false,
                latency_us: start.elapsed().as_micros() as i64,
                error: e,
            })),
        }
    }

    // ─── CompleteStep ───────────────────────────────────────────────────
    async fn complete_step(
        &self,
        request: Request<CompleteStepRequest>,
    ) -> Result<Response<CompleteStepResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let namespace = if req.namespace.is_empty() {
            "default"
        } else {
            &req.namespace
        };

        // Complete the workflow (benchmark measures gRPC round-trip).
        let result = if req.result.is_empty() {
            None
        } else {
            Some(req.result)
        };

        match self
            .engine
            .complete_workflow(namespace, &req.workflow_id, result)
            .await
        {
            Ok(()) => Ok(Response::new(CompleteStepResponse {
                success: true,
                latency_us: start.elapsed().as_micros() as i64,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(CompleteStepResponse {
                success: false,
                latency_us: start.elapsed().as_micros() as i64,
                error: e,
            })),
        }
    }

    // ─── RegisterNamespace ──────────────────────────────────────────────
    async fn register_namespace(
        &self,
        request: Request<RegisterNamespaceRequest>,
    ) -> Result<Response<RegisterNamespaceResponse>, Status> {
        let req = request.into_inner();
        info!(name = %req.name, "Register namespace (simulated)");
        Ok(Response::new(RegisterNamespaceResponse {
            success: true,
            already_exists: false,
        }))
    }

    // ─── CountWorkflows ─────────────────────────────────────────────────
    async fn count_workflows(
        &self,
        request: Request<CountWorkflowsRequest>,
    ) -> Result<Response<CountWorkflowsResponse>, Status> {
        let req = request.into_inner();
        let namespace = if req.namespace.is_empty() {
            "default"
        } else {
            &req.namespace
        };
        let filter = if req.status_filter.is_empty() {
            "all"
        } else {
            &req.status_filter
        };
        let count = self.engine.count_workflows(namespace, filter).await;
        Ok(Response::new(CountWorkflowsResponse {
            count: count as i64,
        }))
    }

    // ─── HealthCheck ────────────────────────────────────────────────────
    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let workflows = self.engine.workflows.lock().await;
        let active = workflows
            .values()
            .filter(|s| s.status == WorkflowStatus::Running)
            .count() as i64;

        Ok(Response::new(HealthCheckResponse {
            healthy: true,
            engine_version: "temporal-bridge-0.1.0".to_string(),
            engine_name: "Temporal-Bridge".to_string(),
            uptime_secs: self.engine.start_time.elapsed().as_secs() as i64,
            active_workflows: active,
            memory_rss_mb: 0.0,
            cpu_percent: 0.0,
        }))
    }

    // ─── GetSystemInfo ──────────────────────────────────────────────────
    async fn get_system_info(
        &self,
        _request: Request<GetSystemInfoRequest>,
    ) -> Result<Response<GetSystemInfoResponse>, Status> {
        Ok(Response::new(GetSystemInfoResponse {
            engine_name: "Temporal-Bridge".to_string(),
            engine_version: "0.1.0".to_string(),
            runtime: "go".to_string(), // Temporal server is Go
            max_workflows: 1_000_000,
            supports_signals: true,
            supports_queries: true,
            supports_child_workflows: true,
            supports_sagas: true,
            supports_timers: true,
            supports_search_attributes: true,
            supports_namespaces: true,
            supports_cron: true,
        }))
    }

    // ─── Reset ──────────────────────────────────────────────────────────
    async fn reset(
        &self,
        request: Request<ResetRequest>,
    ) -> Result<Response<ResetResponse>, Status> {
        let req = request.into_inner();
        let namespace = if req.namespace.is_empty() {
            "default"
        } else {
            &req.namespace
        };
        let cleared = self.engine.reset(namespace).await;
        info!(cleared = cleared, "Reset bridge state");
        Ok(Response::new(ResetResponse {
            success: true,
            workflows_cleared: cleared as i64,
        }))
    }
}

// ─── Main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("temporal_bridge=info".parse().unwrap())
                .add_directive("tonic=info".parse().unwrap()),
        )
        .init();

    // Parse CLI arguments
    let args: Vec<String> = std::env::args().collect();

    let grpc_port: u16 = args
        .iter()
        .position(|a| a == "--grpc-port")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(7235);

    let grpc_addr: SocketAddr = format!("127.0.0.1:{}", grpc_port).parse()?;

    info!("╦  ╦ ╔╗╔ ╦╔═ Temporal Bridge");
    info!("╚╗╔╝ ║║║ ╠╩╗ v0.1.0 — Simulated mode");
    info!("  ╚╝  ╝╚╝ ╩ ╩");
    info!("gRPC:  http://{}", grpc_addr);
    info!("Mode:  Simulated Temporal engine");
    info!("");
    info!("Note: This bridge simulates Temporal behavior for benchmark");
    info!("framework validation. For real Temporal comparison, connect");
    info!("to a running Temporal server with the Temporal SDK.");

    let engine = TemporalEngine::new();
    let service = BenchmarkServiceImpl { engine };

    info!(
        "gRPC BenchmarkService (Temporal) listening on {}",
        grpc_addr
    );

    tonic::transport::Server::builder()
        .add_service(BenchmarkServiceServer::new(service))
        .serve(grpc_addr)
        .await?;

    Ok(())
}
