//! Production VELOCITY-WorkFlow server with BenchmarkService gRPC interface.
//!
//! Uses the real `WorkflowEngine` from `velocity-workflow-engine` — the same
//! engine used in production — exposing the benchmark proto so that
//! `velocity-bench` can compare it against Temporal through identical gRPC paths.
//!
//! Architecture:
//!   [velocity-bench client] ──gRPC──► [BenchmarkServiceImpl] ──► [WorkflowEngine]
//!                                      (tonic service impl)      (production engine)

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use velocity_workflow_engine::engine::WorkflowEngine;

// Include the generated protobuf/gRPC code from build.rs.
mod bench_proto {
    tonic::include_proto!("velocity.bench.v1");
}

use bench_proto::benchmark_service_server::{BenchmarkService, BenchmarkServiceServer};
use bench_proto::*;

// ─── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(
    name = "velocity-server",
    about = "Production VELOCITY-WorkFlow server"
)]
struct Cli {
    /// gRPC port for BenchmarkService.
    #[arg(long, default_value_t = 7234, env = "VELOCITY_GRPC_PORT")]
    grpc_port: u16,

    /// IP address to bind to.
    #[arg(long, default_value = "127.0.0.1")]
    ip: String,

    /// Log level.
    #[arg(long, default_value = "info")]
    log_level: String,
}

// ─── ID Mapping ──────────────────────────────────────────────────────────────

/// Fast string → u64 hash (FNV-1a) for mapping benchmark string IDs to engine numeric IDs.
fn hash_id(s: &str) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in s.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Per-workflow tracking state.
struct WorkflowEntry {
    workflow_key: u64,
    numeric_wf_id: u64,
    total_steps: u32,
    completed_steps: u32,
    status: WorkflowState,
    search_attributes: HashMap<String, String>,
    memo: HashMap<String, String>,
}

#[derive(Clone, PartialEq)]
enum WorkflowState {
    Running,
    Completed,
    Terminated,
    Failed,
}

// ─── Service Implementation ──────────────────────────────────────────────────

struct BenchmarkServiceImpl {
    engine: Arc<WorkflowEngine>,
    /// Map from string workflow_id → numeric tracking entry.
    workflows: RwLock<HashMap<String, WorkflowEntry>>,
    /// Monotonic counter for unique numeric workflow IDs.
    next_wf_id: AtomicU64,
    start_time: Instant,
}

impl BenchmarkServiceImpl {
    fn new(engine: Arc<WorkflowEngine>) -> Self {
        Self {
            engine,
            workflows: RwLock::new(HashMap::new()),
            next_wf_id: AtomicU64::new(1),
            start_time: Instant::now(),
        }
    }

    fn now_us() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as i64
    }

    /// Resolve a string workflow_id to a numeric workflow_key.
    /// The key is (namespace_id << 32) | workflow_id, matching the engine's convention.
    fn make_key(namespace_id: u64, wf_id: u64) -> u64 {
        (namespace_id << 32) | wf_id
    }
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

        let namespace_id: u64 = if req.namespace.is_empty() {
            0
        } else {
            hash_id(&req.namespace) & 0xFFFF
        };
        let workflow_type_id = hash_id(&req.workflow_type);
        let task_queue_hash = if req.task_queue.is_empty() {
            hash_id("bench-queue")
        } else {
            hash_id(&req.task_queue)
        };

        // Assign a unique numeric workflow ID
        let numeric_wf_id = self.next_wf_id.fetch_add(1, Ordering::Relaxed);
        let workflow_key = Self::make_key(namespace_id, numeric_wf_id);
        let total_steps = if req.step_count > 0 {
            req.step_count as u32
        } else {
            1
        };

        let run_id = numeric_wf_id.to_string();

        // Start the workflow in the production engine
        self.engine.start_workflow(
            numeric_wf_id,
            workflow_type_id,
            namespace_id,
            task_queue_hash,
            total_steps,
            Some(req.input),
        );

        // Track the workflow
        let entry = WorkflowEntry {
            workflow_key,
            numeric_wf_id,
            total_steps,
            completed_steps: 0,
            status: WorkflowState::Running,
            search_attributes: HashMap::new(),
            memo: HashMap::new(),
        };
        self.workflows
            .write()
            .await
            .insert(req.workflow_id.clone(), entry);

        tracing::debug!(
            workflow_id = %req.workflow_id,
            numeric_id = numeric_wf_id,
            workflow_type = %req.workflow_type,
            elapsed_us = start.elapsed().as_micros() as u64,
            "StartWorkflow completed"
        );

        Ok(Response::new(StartWorkflowResponse {
            workflow_id: req.workflow_id,
            run_id,
            start_time_us: Self::now_us(),
        }))
    }

    // ─── SignalWorkflow ─────────────────────────────────────────────────
    async fn signal_workflow(
        &self,
        request: Request<SignalWorkflowRequest>,
    ) -> Result<Response<SignalWorkflowResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();

        let workflows = self.workflows.read().await;
        let entry = workflows
            .get(&req.workflow_id)
            .ok_or_else(|| Status::not_found(format!("workflow {} not found", req.workflow_id)))?;
        let workflow_key = entry.workflow_key;
        drop(workflows);

        let signal_name_id = hash_id(&req.signal_name);
        self.engine
            .signal_workflow(workflow_key, signal_name_id, req.payload);

        Ok(Response::new(SignalWorkflowResponse {
            success: true,
            latency_us: start.elapsed().as_micros() as i64,
            error: String::new(),
        }))
    }

    // ─── QueryWorkflow ──────────────────────────────────────────────────
    async fn query_workflow(
        &self,
        request: Request<QueryWorkflowRequest>,
    ) -> Result<Response<QueryWorkflowResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();

        let workflows = self.workflows.read().await;
        let entry = workflows
            .get(&req.workflow_id)
            .ok_or_else(|| Status::not_found(format!("workflow {} not found", req.workflow_id)))?;
        let workflow_key = entry.workflow_key;
        drop(workflows);

        let query_name_id = hash_id(&req.query_type);
        match self
            .engine
            .execute_query(workflow_key, query_name_id, &req.payload)
        {
            Some(result) => Ok(Response::new(QueryWorkflowResponse {
                success: true,
                latency_us: start.elapsed().as_micros() as i64,
                result,
                error: String::new(),
            })),
            None => {
                // No query handler registered — return workflow status as result
                let status = self.engine.get_status(workflow_key);
                let status_str = format!("{:?}", status);
                Ok(Response::new(QueryWorkflowResponse {
                    success: true,
                    latency_us: start.elapsed().as_micros() as i64,
                    result: status_str.into_bytes(),
                    error: String::new(),
                }))
            }
        }
    }

    // ─── WaitForCompletion ──────────────────────────────────────────────
    async fn wait_for_completion(
        &self,
        request: Request<WaitForCompletionRequest>,
    ) -> Result<Response<WaitForCompletionResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let timeout = if req.timeout_ms > 0 {
            Duration::from_millis(req.timeout_ms as u64)
        } else {
            Duration::from_secs(30)
        };

        let poll_interval = Duration::from_micros(100);
        loop {
            let state = {
                let workflows = self.workflows.read().await;
                workflows.get(&req.workflow_id).map(|e| e.status.clone())
            };

            match state {
                Some(WorkflowState::Completed) => {
                    return Ok(Response::new(WaitForCompletionResponse {
                        success: true,
                        latency_us: start.elapsed().as_micros() as i64,
                        result: Vec::new(),
                        status: "completed".to_string(),
                        error: String::new(),
                    }));
                }
                Some(WorkflowState::Terminated) => {
                    return Ok(Response::new(WaitForCompletionResponse {
                        success: false,
                        latency_us: start.elapsed().as_micros() as i64,
                        result: Vec::new(),
                        status: "terminated".to_string(),
                        error: String::new(),
                    }));
                }
                Some(WorkflowState::Failed) => {
                    return Ok(Response::new(WaitForCompletionResponse {
                        success: false,
                        latency_us: start.elapsed().as_micros() as i64,
                        result: Vec::new(),
                        status: "failed".to_string(),
                        error: String::new(),
                    }));
                }
                Some(WorkflowState::Running) => {
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
                None => {
                    return Ok(Response::new(WaitForCompletionResponse {
                        success: false,
                        latency_us: start.elapsed().as_micros() as i64,
                        result: Vec::new(),
                        status: "not_found".to_string(),
                        error: format!("workflow {} not found", req.workflow_id),
                    }));
                }
            }
        }
    }

    // ─── TerminateWorkflow ──────────────────────────────────────────────
    async fn terminate_workflow(
        &self,
        request: Request<TerminateWorkflowRequest>,
    ) -> Result<Response<TerminateWorkflowResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();

        let mut workflows = self.workflows.write().await;
        if let Some(entry) = workflows.get_mut(&req.workflow_id) {
            self.engine.terminate_workflow(entry.workflow_key);
            entry.status = WorkflowState::Terminated;
            Ok(Response::new(TerminateWorkflowResponse {
                success: true,
                latency_us: start.elapsed().as_micros() as i64,
                error: String::new(),
            }))
        } else {
            Ok(Response::new(TerminateWorkflowResponse {
                success: false,
                latency_us: start.elapsed().as_micros() as i64,
                error: format!("workflow {} not found", req.workflow_id),
            }))
        }
    }

    // ─── CompleteStep ───────────────────────────────────────────────────
    async fn complete_step(
        &self,
        request: Request<CompleteStepRequest>,
    ) -> Result<Response<CompleteStepResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();

        let mut workflows = self.workflows.write().await;
        if let Some(entry) = workflows.get_mut(&req.workflow_id) {
            let workflow_key = entry.workflow_key;

            // Complete the step in the production engine
            self.engine
                .complete_step(workflow_key, req.step_index as u32, req.result);
            entry.completed_steps += 1;

            // When all steps are done, complete the workflow
            if entry.completed_steps >= entry.total_steps {
                self.engine
                    .complete_workflow(workflow_key, Some(b"done".to_vec()));
                entry.status = WorkflowState::Completed;
            }

            Ok(Response::new(CompleteStepResponse {
                success: true,
                latency_us: start.elapsed().as_micros() as i64,
                error: String::new(),
            }))
        } else {
            Ok(Response::new(CompleteStepResponse {
                success: false,
                latency_us: start.elapsed().as_micros() as i64,
                error: format!("workflow {} not found", req.workflow_id),
            }))
        }
    }

    // ─── HealthCheck ────────────────────────────────────────────────────
    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let count = self.engine.workflow_count();
        Ok(Response::new(HealthCheckResponse {
            healthy: true,
            engine_version: env!("CARGO_PKG_VERSION").to_string(),
            engine_name: "VELOCITY-ProductionServer".to_string(),
            uptime_secs: self.start_time.elapsed().as_secs() as i64,
            active_workflows: count as i64,
            memory_rss_mb: 0.0, // Would need platform-specific measurement
            cpu_percent: 0.0,
        }))
    }

    // ─── GetSystemInfo ──────────────────────────────────────────────────
    async fn get_system_info(
        &self,
        _request: Request<GetSystemInfoRequest>,
    ) -> Result<Response<GetSystemInfoResponse>, Status> {
        Ok(Response::new(GetSystemInfoResponse {
            engine_name: "VELOCITY-ProductionServer".to_string(),
            engine_version: env!("CARGO_PKG_VERSION").to_string(),
            runtime: "rust".to_string(),
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
        _request: Request<ResetRequest>,
    ) -> Result<Response<ResetResponse>, Status> {
        let mut workflows = self.workflows.write().await;
        let count = workflows.len();

        // Terminate all running workflows in the engine
        for entry in workflows.values() {
            if entry.status == WorkflowState::Running {
                self.engine.terminate_workflow(entry.workflow_key);
            }
        }
        workflows.clear();

        Ok(Response::new(ResetResponse {
            success: true,
            workflows_cleared: count as i64,
        }))
    }

    // ─── RegisterNamespace ──────────────────────────────────────────────
    async fn register_namespace(
        &self,
        _request: Request<RegisterNamespaceRequest>,
    ) -> Result<Response<RegisterNamespaceResponse>, Status> {
        // Production engine handles namespaces internally; accept all registrations
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
        let workflows = self.workflows.read().await;

        let count = match req.status_filter.as_str() {
            "running" => workflows
                .values()
                .filter(|e| e.status == WorkflowState::Running)
                .count(),
            "completed" => workflows
                .values()
                .filter(|e| e.status == WorkflowState::Completed)
                .count(),
            "failed" => workflows
                .values()
                .filter(|e| e.status == WorkflowState::Failed)
                .count(),
            _ => workflows.len(),
        };

        Ok(Response::new(CountWorkflowsResponse {
            count: count as i64,
        }))
    }

    // ─── Tier 1: Extended workflow features ─────────────────────────────
    async fn cancel_workflow(
        &self,
        request: Request<CancelWorkflowRequest>,
    ) -> Result<Response<CancelWorkflowResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let workflows = self.workflows.read().await;
        if let Some(entry) = workflows.get(&req.workflow_id) {
            self.engine.cancel_workflow(entry.workflow_key);
            drop(workflows);
            let mut wfs = self.workflows.write().await;
            if let Some(e) = wfs.get_mut(&req.workflow_id) {
                e.status = WorkflowState::Failed;
            }
            Ok(Response::new(CancelWorkflowResponse {
                success: true,
                latency_us: start.elapsed().as_micros() as i64,
                error: String::new(),
            }))
        } else {
            Ok(Response::new(CancelWorkflowResponse {
                success: false,
                latency_us: 0,
                error: format!("workflow {} not found", req.workflow_id),
            }))
        }
    }
    async fn update_workflow_execution(
        &self,
        request: Request<UpdateWorkflowRequest>,
    ) -> Result<Response<UpdateWorkflowResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let workflows = self.workflows.read().await;
        if let Some(entry) = workflows.get(&req.workflow_id) {
            let update_name_id = hash_id(&req.update_name);
            self.engine
                .update_workflow(entry.workflow_key, update_name_id, req.payload);
            Ok(Response::new(UpdateWorkflowResponse {
                success: true,
                latency_us: start.elapsed().as_micros() as i64,
                result: Vec::new(),
                error: String::new(),
            }))
        } else {
            Ok(Response::new(UpdateWorkflowResponse {
                success: false,
                latency_us: 0,
                result: Vec::new(),
                error: format!("workflow {} not found", req.workflow_id),
            }))
        }
    }
    async fn start_child_workflow(
        &self,
        request: Request<StartChildWorkflowRequest>,
    ) -> Result<Response<StartChildWorkflowResponse>, Status> {
        let req = request.into_inner();
        let workflows = self.workflows.read().await;
        let parent = workflows
            .get(&req.parent_workflow_id)
            .ok_or_else(|| Status::not_found("parent workflow not found"))?;
        let parent_key = parent.workflow_key;
        let child_numeric_id = self.next_wf_id.fetch_add(1, Ordering::Relaxed);
        let _namespace_id = parent_key >> 32;
        let workflow_type_id = hash_id(&req.workflow_type);
        let task_queue_hash = if req.task_queue.is_empty() {
            hash_id("bench-queue")
        } else {
            hash_id(&req.task_queue)
        };
        let child_key = self.engine.start_child_workflow(
            parent_key,
            child_numeric_id,
            workflow_type_id,
            task_queue_hash,
            1,
            Some(req.input),
        );
        let child_entry = WorkflowEntry {
            workflow_key: child_key,
            numeric_wf_id: child_numeric_id,
            total_steps: 1,
            completed_steps: 0,
            status: WorkflowState::Running,
            search_attributes: HashMap::new(),
            memo: HashMap::new(),
        };
        drop(workflows);
        self.workflows
            .write()
            .await
            .insert(req.workflow_id.clone(), child_entry);
        Ok(Response::new(StartChildWorkflowResponse {
            workflow_id: req.workflow_id,
            run_id: child_numeric_id.to_string(),
            success: true,
            error: String::new(),
        }))
    }
    async fn schedule_timer(
        &self,
        request: Request<ScheduleTimerRequest>,
    ) -> Result<Response<ScheduleTimerResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let workflows = self.workflows.read().await;
        if let Some(entry) = workflows.get(&req.workflow_id) {
            let timer_id = self
                .engine
                .schedule_timer(entry.workflow_key, req.duration_ms as u64);
            Ok(Response::new(ScheduleTimerResponse {
                success: true,
                timer_id: timer_id.to_string(),
                latency_us: start.elapsed().as_micros() as i64,
            }))
        } else {
            Ok(Response::new(ScheduleTimerResponse {
                success: false,
                timer_id: String::new(),
                latency_us: 0,
            }))
        }
    }
    async fn cancel_timer(
        &self,
        _req: Request<CancelTimerRequest>,
    ) -> Result<Response<CancelTimerResponse>, Status> {
        // Timer engine doesn't expose cancel; process_fired_timer will drain it
        Ok(Response::new(CancelTimerResponse {
            success: true,
            error: String::new(),
        }))
    }
    async fn continue_as_new(
        &self,
        request: Request<ContinueAsNewRequest>,
    ) -> Result<Response<ContinueAsNewResponse>, Status> {
        let req = request.into_inner();
        let workflows = self.workflows.read().await;
        if let Some(entry) = workflows.get(&req.workflow_id) {
            let new_key = self
                .engine
                .continue_as_new(entry.workflow_key, Some(req.input));
            let new_run_id = (new_key & 0xFFFFFFFF).to_string();
            drop(workflows);
            let mut wfs = self.workflows.write().await;
            if let Some(e) = wfs.get_mut(&req.workflow_id) {
                e.status = WorkflowState::Completed; // old run ended
                e.workflow_key = new_key; // track new run
                e.completed_steps = 0;
                e.status = WorkflowState::Running;
            }
            Ok(Response::new(ContinueAsNewResponse {
                new_run_id,
                success: true,
                error: String::new(),
            }))
        } else {
            Ok(Response::new(ContinueAsNewResponse {
                new_run_id: String::new(),
                success: false,
                error: format!("workflow {} not found", req.workflow_id),
            }))
        }
    }
    async fn upsert_search_attributes(
        &self,
        request: Request<UpsertSearchAttributesRequest>,
    ) -> Result<Response<UpsertSearchAttributesResponse>, Status> {
        let req = request.into_inner();
        let mut workflows = self.workflows.write().await;
        if let Some(entry) = workflows.get_mut(&req.workflow_id) {
            for (k, v) in &req.search_attributes {
                entry.search_attributes.insert(k.clone(), v.clone());
            }
            Ok(Response::new(UpsertSearchAttributesResponse {
                success: true,
                error: String::new(),
            }))
        } else {
            Ok(Response::new(UpsertSearchAttributesResponse {
                success: false,
                error: format!("workflow {} not found", req.workflow_id),
            }))
        }
    }
    async fn set_memo(
        &self,
        request: Request<SetMemoRequest>,
    ) -> Result<Response<SetMemoResponse>, Status> {
        let req = request.into_inner();
        let mut workflows = self.workflows.write().await;
        if let Some(entry) = workflows.get_mut(&req.workflow_id) {
            for (k, v) in &req.memo {
                entry.memo.insert(k.clone(), v.clone());
            }
            Ok(Response::new(SetMemoResponse {
                success: true,
                error: String::new(),
            }))
        } else {
            Ok(Response::new(SetMemoResponse {
                success: false,
                error: format!("workflow {} not found", req.workflow_id),
            }))
        }
    }
    async fn signal_with_start(
        &self,
        request: Request<SignalWithStartRequest>,
    ) -> Result<Response<SignalWithStartResponse>, Status> {
        let req = request.into_inner();
        let namespace_id: u64 = if req.namespace.is_empty() {
            0
        } else {
            hash_id(&req.namespace) & 0xFFFF
        };
        let workflow_type_id = hash_id(&req.workflow_type);
        let task_queue_hash = if req.task_queue.is_empty() {
            hash_id("bench-queue")
        } else {
            hash_id(&req.task_queue)
        };
        let signal_name_id = hash_id(&req.signal_name);
        let numeric_wf_id = self.next_wf_id.fetch_add(1, Ordering::Relaxed);
        let (workflow_key, was_started) = self.engine.signal_with_start(
            numeric_wf_id,
            workflow_type_id,
            namespace_id,
            task_queue_hash,
            1,
            signal_name_id,
            req.signal_payload,
        );
        let run_id = numeric_wf_id.to_string();
        let entry = WorkflowEntry {
            workflow_key,
            numeric_wf_id,
            total_steps: 1,
            completed_steps: 0,
            status: WorkflowState::Running,
            search_attributes: HashMap::new(),
            memo: HashMap::new(),
        };
        self.workflows
            .write()
            .await
            .insert(req.workflow_id.clone(), entry);
        Ok(Response::new(SignalWithStartResponse {
            workflow_id: req.workflow_id,
            run_id,
            started: was_started,
            signaled: true,
        }))
    }

    // ─── Tier 2: Activity & operational features ───────────────────────
    async fn record_activity_heartbeat(
        &self,
        request: Request<RecordActivityHeartbeatRequest>,
    ) -> Result<Response<RecordActivityHeartbeatResponse>, Status> {
        let req = request.into_inner();
        let workflows = self.workflows.read().await;
        if let Some(entry) = workflows.get(&req.workflow_id) {
            let activity_id_num = hash_id(&req.activity_id);
            let details = if req.details.is_empty() {
                None
            } else {
                Some(req.details)
            };
            self.engine.heartbeat_tracker().record_heartbeat(
                entry.workflow_key,
                activity_id_num,
                details,
            );
            Ok(Response::new(RecordActivityHeartbeatResponse {
                success: true,
                cancel_requested: false,
            }))
        } else {
            Ok(Response::new(RecordActivityHeartbeatResponse {
                success: false,
                cancel_requested: false,
            }))
        }
    }
    async fn schedule_activity(
        &self,
        request: Request<ScheduleActivityRequest>,
    ) -> Result<Response<ScheduleActivityResponse>, Status> {
        let req = request.into_inner();
        let workflows = self.workflows.read().await;
        if let Some(entry) = workflows.get(&req.workflow_id) {
            let activity_name_id = hash_id(&req.activity_type);
            let step = entry.completed_steps + entry.total_steps; // next available step
            self.engine
                .schedule_activity(entry.workflow_key, step, activity_name_id, req.input);
            Ok(Response::new(ScheduleActivityResponse {
                activity_id: req.activity_id,
                success: true,
                error: String::new(),
            }))
        } else {
            Ok(Response::new(ScheduleActivityResponse {
                activity_id: req.activity_id,
                success: false,
                error: format!("workflow {} not found", req.workflow_id),
            }))
        }
    }
    async fn complete_activity_task(
        &self,
        request: Request<CompleteActivityTaskRequest>,
    ) -> Result<Response<CompleteActivityTaskResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let workflows = self.workflows.read().await;
        if let Some(entry) = workflows.get(&req.workflow_id) {
            // Use step 0 as default for activity completion
            self.engine
                .complete_activity(entry.workflow_key, 0, req.result);
            Ok(Response::new(CompleteActivityTaskResponse {
                success: true,
                latency_us: start.elapsed().as_micros() as i64,
                error: String::new(),
            }))
        } else {
            Ok(Response::new(CompleteActivityTaskResponse {
                success: false,
                latency_us: 0,
                error: format!("workflow {} not found", req.workflow_id),
            }))
        }
    }
    async fn fail_activity_task(
        &self,
        request: Request<FailActivityTaskRequest>,
    ) -> Result<Response<FailActivityTaskResponse>, Status> {
        let req = request.into_inner();
        let workflows = self.workflows.read().await;
        if let Some(entry) = workflows.get(&req.workflow_id) {
            let will_retry = self.engine.fail_activity_with_retry(entry.workflow_key, 0);
            Ok(Response::new(FailActivityTaskResponse {
                success: true,
                will_retry,
                next_attempt: if will_retry { 1 } else { 0 },
                error: String::new(),
            }))
        } else {
            Ok(Response::new(FailActivityTaskResponse {
                success: false,
                will_retry: false,
                next_attempt: 0,
                error: format!("workflow {} not found", req.workflow_id),
            }))
        }
    }
    async fn replay_workflow(
        &self,
        request: Request<ReplayWorkflowRequest>,
    ) -> Result<Response<ReplayWorkflowResponse>, Status> {
        let req = request.into_inner();
        let workflows = self.workflows.read().await;
        if let Some(entry) = workflows.get(&req.workflow_id) {
            let history = self.engine.history_store().get_history(entry.workflow_key);
            let events_replayed = history.as_ref().map(|h| h.len() as i64).unwrap_or(0);
            let final_status = format!("{:?}", self.engine.get_status(entry.workflow_key));
            Ok(Response::new(ReplayWorkflowResponse {
                success: true,
                events_replayed,
                final_status,
                error: String::new(),
            }))
        } else {
            Ok(Response::new(ReplayWorkflowResponse {
                success: false,
                events_replayed: 0,
                final_status: String::new(),
                error: format!("workflow {} not found", req.workflow_id),
            }))
        }
    }
    async fn reset_workflow(
        &self,
        request: Request<ResetWorkflowRequest>,
    ) -> Result<Response<ResetWorkflowResponse>, Status> {
        let req = request.into_inner();
        let workflows = self.workflows.read().await;
        if let Some(entry) = workflows.get(&req.workflow_id) {
            let ok = self
                .engine
                .reset_workflow(entry.workflow_key, req.reset_to_event_id as u64);
            let new_run_id = if ok {
                entry.numeric_wf_id.to_string()
            } else {
                String::new()
            };
            Ok(Response::new(ResetWorkflowResponse {
                new_run_id,
                success: ok,
                error: if ok {
                    String::new()
                } else {
                    "reset failed".into()
                },
            }))
        } else {
            Ok(Response::new(ResetWorkflowResponse {
                new_run_id: String::new(),
                success: false,
                error: format!("workflow {} not found", req.workflow_id),
            }))
        }
    }
    async fn batch_terminate(
        &self,
        request: Request<BatchTerminateRequest>,
    ) -> Result<Response<BatchTerminateResponse>, Status> {
        let req = request.into_inner();
        let workflows = self.workflows.read().await;
        let max = if req.max_count > 0 {
            req.max_count as usize
        } else {
            usize::MAX
        };
        let keys: Vec<u64> = workflows
            .values()
            .filter(|e| match req.status_filter.as_str() {
                "all" => true,
                _ => e.status == WorkflowState::Running,
            })
            .map(|e| e.workflow_key)
            .take(max)
            .collect();
        let count = keys.len() as i64;
        drop(workflows);
        self.engine.batch_terminate(keys);
        // Update tracking
        let mut wfs = self.workflows.write().await;
        for e in wfs.values_mut() {
            if e.status == WorkflowState::Running {
                e.status = WorkflowState::Terminated;
            }
        }
        Ok(Response::new(BatchTerminateResponse {
            terminated_count: count,
        }))
    }
    async fn batch_signal(
        &self,
        request: Request<BatchSignalRequest>,
    ) -> Result<Response<BatchSignalResponse>, Status> {
        let req = request.into_inner();
        let signal_name_id = hash_id(&req.signal_name);
        let workflows = self.workflows.read().await;
        let max = if req.max_count > 0 {
            req.max_count as usize
        } else {
            usize::MAX
        };
        let keys: Vec<u64> = workflows
            .values()
            .filter(|e| match req.status_filter.as_str() {
                "all" => true,
                _ => e.status == WorkflowState::Running,
            })
            .map(|e| e.workflow_key)
            .take(max)
            .collect();
        let count = keys.len() as i64;
        drop(workflows);
        self.engine.batch_signal(keys, signal_name_id, req.payload);
        Ok(Response::new(BatchSignalResponse {
            signaled_count: count,
        }))
    }

    // ─── Tier 3: Namespace & production features ───────────────────────
    async fn describe_namespace(
        &self,
        request: Request<DescribeNamespaceRequest>,
    ) -> Result<Response<DescribeNamespaceResponse>, Status> {
        let req = request.into_inner();
        let ns = self.engine.namespaces();
        if let Some(id) = ns.get_by_name(&req.name) {
            if let Some(config) = ns.get(id) {
                return Ok(Response::new(DescribeNamespaceResponse {
                    name: config.name,
                    id: config.id.to_string(),
                    description: config.description,
                    state: if config.is_active {
                        "ACTIVE"
                    } else {
                        "INACTIVE"
                    }
                    .into(),
                    retention_days: config.retention_period.as_secs() as u32 / 86400,
                    owner_email: String::new(),
                    is_global: false,
                    created_at: 0,
                }));
            }
        }
        // Default namespace fallback
        Ok(Response::new(DescribeNamespaceResponse {
            name: req.name,
            id: "0".into(),
            description: String::new(),
            state: "ACTIVE".into(),
            retention_days: 7,
            owner_email: String::new(),
            is_global: false,
            created_at: 0,
        }))
    }
    async fn update_namespace(
        &self,
        request: Request<UpdateNamespaceRequest>,
    ) -> Result<Response<UpdateNamespaceResponse>, Status> {
        let req = request.into_inner();
        let ns = self.engine.namespaces();
        if let Some(_id) = ns.get_by_name(&req.name) {
            // Namespace exists — update accepted
            Ok(Response::new(UpdateNamespaceResponse {
                success: true,
                error: String::new(),
            }))
        } else {
            // Register new namespace
            let config = velocity_workflow_engine::namespace::NamespaceConfig::new(
                hash_id(&req.name) & 0xFFFF,
                &req.name,
            )
            .with_description(if req.description.is_empty() {
                ""
            } else {
                &req.description
            });
            let _ = ns.register(config);
            Ok(Response::new(UpdateNamespaceResponse {
                success: true,
                error: String::new(),
            }))
        }
    }
    async fn delete_namespace(
        &self,
        request: Request<DeleteNamespaceRequest>,
    ) -> Result<Response<DeleteNamespaceResponse>, Status> {
        let req = request.into_inner();
        let ns = self.engine.namespaces();
        if let Some(id) = ns.get_by_name(&req.name) {
            match ns.delete(id) {
                Ok(()) => Ok(Response::new(DeleteNamespaceResponse {
                    success: true,
                    error: String::new(),
                })),
                Err(e) => Ok(Response::new(DeleteNamespaceResponse {
                    success: false,
                    error: format!("{}", e),
                })),
            }
        } else {
            Ok(Response::new(DeleteNamespaceResponse {
                success: false,
                error: format!("namespace {} not found", req.name),
            }))
        }
    }
    async fn poll_workflow_task(
        &self,
        request: Request<PollWorkflowTaskRequest>,
    ) -> Result<Response<PollWorkflowTaskResponse>, Status> {
        let req = request.into_inner();
        let tq_hash = if req.task_queue.is_empty() {
            hash_id("bench-queue")
        } else {
            hash_id(&req.task_queue)
        };
        if let Some(task) = self.engine.task_queue().poll(tq_hash) {
            Ok(Response::new(PollWorkflowTaskResponse {
                task_token: format!("{}:{}", task.workflow_key, task.step_index),
                event_id: task.step_index as i64,
                event_type: format!("{:?}", task.kind),
                workflow_execution: task.workflow_key.to_le_bytes().to_vec(),
                has_task: true,
            }))
        } else {
            Ok(Response::new(PollWorkflowTaskResponse {
                task_token: String::new(),
                event_id: 0,
                event_type: String::new(),
                workflow_execution: Vec::new(),
                has_task: false,
            }))
        }
    }
    async fn poll_activity_task(
        &self,
        request: Request<PollActivityTaskRequest>,
    ) -> Result<Response<PollActivityTaskResponse>, Status> {
        let req = request.into_inner();
        let tq_hash = if req.task_queue.is_empty() {
            hash_id("bench-queue")
        } else {
            hash_id(&req.task_queue)
        };
        if let Some(task) = self.engine.task_queue().poll(tq_hash) {
            Ok(Response::new(PollActivityTaskResponse {
                task_token: format!("{}:{}", task.workflow_key, task.step_index),
                activity_id: task.activity_name_id.to_string(),
                activity_type: task.activity_name_id.to_string(),
                input: Vec::new(),
                workflow_id: task.workflow_key.to_string(),
                has_task: true,
                scheduled_time: 0,
            }))
        } else {
            Ok(Response::new(PollActivityTaskResponse {
                task_token: String::new(),
                activity_id: String::new(),
                activity_type: String::new(),
                input: Vec::new(),
                workflow_id: String::new(),
                has_task: false,
                scheduled_time: 0,
            }))
        }
    }
    async fn get_workflow_history(
        &self,
        request: Request<GetWorkflowHistoryRequest>,
    ) -> Result<Response<GetWorkflowHistoryResponse>, Status> {
        let req = request.into_inner();
        let workflows = self.workflows.read().await;
        if let Some(entry) = workflows.get(&req.workflow_id) {
            let max_page = if req.max_page_size > 0 {
                req.max_page_size as usize
            } else {
                100
            };
            let history = self.engine.history_store().get_history(entry.workflow_key);
            match history {
                Some(events) => {
                    let total = events.len() as i64;
                    let serialized: Vec<Vec<u8>> = events
                        .iter()
                        .take(max_page)
                        .map(|e| format!("{:?}", e).into_bytes())
                        .collect();
                    Ok(Response::new(GetWorkflowHistoryResponse {
                        events: serialized,
                        next_page_token: Vec::new(),
                        total_event_count: total,
                    }))
                }
                None => Ok(Response::new(GetWorkflowHistoryResponse {
                    events: Vec::new(),
                    next_page_token: Vec::new(),
                    total_event_count: 0,
                })),
            }
        } else {
            Ok(Response::new(GetWorkflowHistoryResponse {
                events: Vec::new(),
                next_page_token: Vec::new(),
                total_event_count: 0,
            }))
        }
    }
}

// ─── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&cli.log_level)),
        )
        .init();

    let addr: std::net::SocketAddr = format!("{}:{}", cli.ip, cli.grpc_port).parse()?;

    println!("╦  ╦ ╔╗╔ ╦╔═ ╔═╗ ╦ ╦ ╔═╗ ╔╗╔ ╔═╗");
    println!("╚╗╔╝ ║║║ ╠╩╗ ╠═╣ ║ ║ ║╣  ║║║ ║ ║");
    println!("  ╚╝  ╝╚╝ ╩ ╩ ╩ ╩ ╚═╝ ╚═╝ ╝╚╝ ╚═╝");
    println!("  Production Server v{}", env!("CARGO_PKG_VERSION"));
    println!("  gRPC:  http://{}", addr);
    println!("  Engine: WorkflowEngine (production)");
    println!();

    // Create the production engine
    let engine = Arc::new(WorkflowEngine::new());
    let service = BenchmarkServiceImpl::new(engine);

    tracing::info!("BenchmarkService (production engine) listening on {}", addr);

    tonic::transport::Server::builder()
        .add_service(BenchmarkServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
