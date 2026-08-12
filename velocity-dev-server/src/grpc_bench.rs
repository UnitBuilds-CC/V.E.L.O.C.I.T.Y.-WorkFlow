//! gRPC BenchmarkService server — implements the common benchmark proto on
//! top of the DevEngine so that velocity-bench can compare VELOCITY and
//! Temporal through identical gRPC paths.
//!
//! Architecture:
//!   [velocity-bench client] ──gRPC──► [BenchmarkServiceImpl] ──► [DevEngine]
//!                                      (tonic service impl)      (in-memory)

use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tonic::{Request, Response, Status};

use crate::DevEngine;

// Include the generated protobuf/gRPC code from build.rs.
pub mod velocity_bench_proto {
    tonic::include_proto!("velocity.bench.v1");
}

use velocity_bench_proto::benchmark_service_server::BenchmarkService;
use velocity_bench_proto::*;

// ─── Service Implementation ─────────────────────────────────────────────────

pub struct BenchmarkServiceImpl {
    pub engine: Arc<DevEngine>,
}

impl BenchmarkServiceImpl {
    fn now_us() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as i64
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
        let namespace = if req.namespace.is_empty() {
            "default"
        } else {
            &req.namespace
        };
        let task_queue = if req.task_queue.is_empty() {
            "bench-queue"
        } else {
            &req.task_queue
        };
        let input: serde_json::Value = if req.input.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&req.input).unwrap_or(serde_json::Value::Null)
        };

        let execution = self
            .engine
            .start_workflow(
                namespace,
                &req.workflow_type,
                task_queue,
                input,
                &req.workflow_id,
            )
            .map_err(Status::internal)?;

        tracing::debug!(
            workflow_id = %execution.workflow_id,
            workflow_type = %req.workflow_type,
            elapsed_us = start.elapsed().as_micros() as u64,
            "StartWorkflow completed"
        );

        Ok(Response::new(StartWorkflowResponse {
            workflow_id: execution.workflow_id,
            run_id: execution.run_id,
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
        let namespace = if req.namespace.is_empty() {
            "default"
        } else {
            &req.namespace
        };
        let payload: serde_json::Value = if req.payload.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&req.payload).unwrap_or(serde_json::Value::Null)
        };

        match self
            .engine
            .signal_workflow(namespace, &req.workflow_id, &req.signal_name, payload)
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
            std::time::Duration::from_millis(req.timeout_ms as u64)
        } else {
            std::time::Duration::from_secs(30)
        };

        // Poll until the workflow reaches a terminal state or timeout.
        let poll_interval = std::time::Duration::from_micros(100);
        loop {
            if let Some(wf) = self.engine.get_workflow(namespace, &req.workflow_id) {
                let status = wf.status.as_str();
                if status == "COMPLETED" || status == "FAILED" || status == "TERMINATED" || status == "CANCELLED" || status == "CONTINUED_AS_NEW" {
                    let elapsed = start.elapsed();
                    return Ok(Response::new(WaitForCompletionResponse {
                        success: status == "COMPLETED",
                        latency_us: elapsed.as_micros() as i64,
                        result: Vec::new(),
                        status: status.to_string(),
                        error: String::new(),
                    }));
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

    // ─── HealthCheck ────────────────────────────────────────────────────
    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let stats = self.engine.get_stats();
        Ok(Response::new(HealthCheckResponse {
            healthy: true,
            engine_version: env!("CARGO_PKG_VERSION").to_string(),
            engine_name: "VELOCITY-DevServer".to_string(),
            uptime_secs: stats.uptime_secs as i64,
            active_workflows: stats.running_workflows as i64,
            memory_rss_mb: stats.memory_usage_bytes as f64 / 1_048_576.0,
            cpu_percent: 0.0,
        }))
    }

    // ─── GetSystemInfo ──────────────────────────────────────────────────
    async fn get_system_info(
        &self,
        _request: Request<GetSystemInfoRequest>,
    ) -> Result<Response<GetSystemInfoResponse>, Status> {
        Ok(Response::new(GetSystemInfoResponse {
            engine_name: "VELOCITY-DevServer".to_string(),
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
        request: Request<ResetRequest>,
    ) -> Result<Response<ResetResponse>, Status> {
        let req = request.into_inner();
        let namespace = if req.namespace.is_empty() {
            "default"
        } else {
            &req.namespace
        };
        let cleared = self.engine.reset_all(namespace);
        Ok(Response::new(ResetResponse {
            success: true,
            workflows_cleared: cleared as i64,
        }))
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

        // In the dev-server, completing a step immediately completes the
        // workflow (the benchmark measures gRPC round-trip latency, not
        // internal step execution).
        let result: serde_json::Value = if req.result.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&req.result).unwrap_or(serde_json::Value::Null)
        };

        match self
            .engine
            .complete_workflow(namespace, &req.workflow_id, result)
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
        match self.engine.create_namespace(&req.name, &req.description) {
            Ok(_) => Ok(Response::new(RegisterNamespaceResponse {
                success: true,
                already_exists: false,
            })),
            Err(e) if e.contains("already exists") => {
                Ok(Response::new(RegisterNamespaceResponse {
                    success: true,
                    already_exists: true,
                }))
            }
            Err(e) => Err(Status::internal(e)),
        }
    }

    // ─── CountWorkflows ─────────────────────────────────────────────────
    async fn count_workflows(
        &self,
        request: Request<CountWorkflowsRequest>,
    ) -> Result<Response<CountWorkflowsResponse>, Status> {
        let req = request.into_inner();
        let namespace = if req.namespace.is_empty() { "default" } else { &req.namespace };
        let filter = if req.status_filter.is_empty() { "all" } else { &req.status_filter };
        let count = self.engine.count_workflows(namespace, filter);
        Ok(Response::new(CountWorkflowsResponse { count: count as i64 }))
    }

    // ─── CancelWorkflow ──────────────────────────────────────────────────
    async fn cancel_workflow(
        &self, request: Request<CancelWorkflowRequest>,
    ) -> Result<Response<CancelWorkflowResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let ns = if req.namespace.is_empty() { "default" } else { &req.namespace };
        match self.engine.cancel_workflow(ns, &req.workflow_id, &req.reason) {
            Ok(()) => Ok(Response::new(CancelWorkflowResponse { success: true, latency_us: start.elapsed().as_micros() as i64, error: String::new() })),
            Err(e) => Ok(Response::new(CancelWorkflowResponse { success: false, latency_us: start.elapsed().as_micros() as i64, error: e })),
        }
    }

    // ─── UpdateWorkflowExecution ─────────────────────────────────────────
    async fn update_workflow_execution(
        &self, request: Request<UpdateWorkflowRequest>,
    ) -> Result<Response<UpdateWorkflowResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let ns = if req.namespace.is_empty() { "default" } else { &req.namespace };
        let payload = if req.payload.is_empty() { serde_json::Value::Null } else { serde_json::from_slice(&req.payload).unwrap_or(serde_json::Value::Null) };
        match self.engine.update_workflow(ns, &req.workflow_id, &req.update_name, &req.update_id, payload) {
            Ok(result) => Ok(Response::new(UpdateWorkflowResponse { success: true, latency_us: start.elapsed().as_micros() as i64, result: serde_json::to_vec(&result).unwrap_or_default(), error: String::new() })),
            Err(e) => Ok(Response::new(UpdateWorkflowResponse { success: false, latency_us: start.elapsed().as_micros() as i64, result: Vec::new(), error: e })),
        }
    }

    // ─── StartChildWorkflow ──────────────────────────────────────────────
    async fn start_child_workflow(
        &self, request: Request<StartChildWorkflowRequest>,
    ) -> Result<Response<StartChildWorkflowResponse>, Status> {
        let req = request.into_inner();
        let ns = if req.namespace.is_empty() { "default" } else { &req.namespace };
        let tq = if req.task_queue.is_empty() { "default-queue" } else { &req.task_queue };
        let input = if req.input.is_empty() { serde_json::Value::Null } else { serde_json::from_slice(&req.input).unwrap_or(serde_json::Value::Null) };
        match self.engine.start_child_workflow(ns, &req.parent_workflow_id, &req.workflow_type, &req.workflow_id, tq, input) {
            Ok(exec) => Ok(Response::new(StartChildWorkflowResponse { workflow_id: exec.workflow_id, run_id: exec.run_id, success: true, error: String::new() })),
            Err(e) => Ok(Response::new(StartChildWorkflowResponse { workflow_id: String::new(), run_id: String::new(), success: false, error: e })),
        }
    }

    // ─── ScheduleTimer ───────────────────────────────────────────────────
    async fn schedule_timer(
        &self, request: Request<ScheduleTimerRequest>,
    ) -> Result<Response<ScheduleTimerResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let ns = if req.namespace.is_empty() { "default" } else { &req.namespace };
        match self.engine.schedule_timer(ns, &req.workflow_id, &req.timer_id, req.duration_ms) {
            Ok(tid) => Ok(Response::new(ScheduleTimerResponse { success: true, timer_id: tid, latency_us: start.elapsed().as_micros() as i64 })),
            Err(_e) => Ok(Response::new(ScheduleTimerResponse { success: false, timer_id: String::new(), latency_us: 0 })),
        }
    }

    // ─── CancelTimer ─────────────────────────────────────────────────────
    async fn cancel_timer(
        &self, request: Request<CancelTimerRequest>,
    ) -> Result<Response<CancelTimerResponse>, Status> {
        let req = request.into_inner();
        let ns = if req.namespace.is_empty() { "default" } else { &req.namespace };
        match self.engine.cancel_timer(ns, &req.workflow_id, &req.timer_id) {
            Ok(()) => Ok(Response::new(CancelTimerResponse { success: true, error: String::new() })),
            Err(e) => Ok(Response::new(CancelTimerResponse { success: false, error: e })),
        }
    }

    // ─── ContinueAsNew ───────────────────────────────────────────────────
    async fn continue_as_new(
        &self, request: Request<ContinueAsNewRequest>,
    ) -> Result<Response<ContinueAsNewResponse>, Status> {
        let req = request.into_inner();
        let ns = if req.namespace.is_empty() { "default" } else { &req.namespace };
        let tq = if req.task_queue.is_empty() { "default-queue" } else { &req.task_queue };
        let input = if req.input.is_empty() { serde_json::Value::Null } else { serde_json::from_slice(&req.input).unwrap_or(serde_json::Value::Null) };
        let wf_type = if req.workflow_type.is_empty() { "default" } else { &req.workflow_type };
        match self.engine.continue_as_new(ns, &req.workflow_id, wf_type, tq, input) {
            Ok(new_run_id) => Ok(Response::new(ContinueAsNewResponse { new_run_id, success: true, error: String::new() })),
            Err(e) => Ok(Response::new(ContinueAsNewResponse { new_run_id: String::new(), success: false, error: e })),
        }
    }

    // ─── UpsertSearchAttributes ──────────────────────────────────────────
    async fn upsert_search_attributes(
        &self, request: Request<UpsertSearchAttributesRequest>,
    ) -> Result<Response<UpsertSearchAttributesResponse>, Status> {
        let req = request.into_inner();
        let ns = if req.namespace.is_empty() { "default" } else { &req.namespace };
        match self.engine.upsert_search_attributes(ns, &req.workflow_id, req.search_attributes) {
            Ok(()) => Ok(Response::new(UpsertSearchAttributesResponse { success: true, error: String::new() })),
            Err(e) => Ok(Response::new(UpsertSearchAttributesResponse { success: false, error: e })),
        }
    }

    // ─── SetMemo ─────────────────────────────────────────────────────────
    async fn set_memo(
        &self, request: Request<SetMemoRequest>,
    ) -> Result<Response<SetMemoResponse>, Status> {
        let req = request.into_inner();
        let ns = if req.namespace.is_empty() { "default" } else { &req.namespace };
        match self.engine.set_memo(ns, &req.workflow_id, req.memo) {
            Ok(()) => Ok(Response::new(SetMemoResponse { success: true, error: String::new() })),
            Err(e) => Ok(Response::new(SetMemoResponse { success: false, error: e })),
        }
    }

    // ─── SignalWithStart ─────────────────────────────────────────────────
    async fn signal_with_start(
        &self, request: Request<SignalWithStartRequest>,
    ) -> Result<Response<SignalWithStartResponse>, Status> {
        let req = request.into_inner();
        let ns = if req.namespace.is_empty() { "default" } else { &req.namespace };
        let tq = if req.task_queue.is_empty() { "default-queue" } else { &req.task_queue };
        let input = if req.input.is_empty() { serde_json::Value::Null } else { serde_json::from_slice(&req.input).unwrap_or(serde_json::Value::Null) };
        let signal_payload = if req.signal_payload.is_empty() { serde_json::Value::Null } else { serde_json::from_slice(&req.signal_payload).unwrap_or(serde_json::Value::Null) };
        match self.engine.signal_with_start(ns, &req.workflow_type, &req.workflow_id, tq, input, &req.signal_name, signal_payload) {
            Ok((exec, started, signaled)) => Ok(Response::new(SignalWithStartResponse { workflow_id: exec.workflow_id, run_id: exec.run_id, started, signaled })),
            Err(e) => Err(Status::internal(e)),
        }
    }

    // ─── RecordActivityHeartbeat ─────────────────────────────────────────
    async fn record_activity_heartbeat(
        &self, request: Request<RecordActivityHeartbeatRequest>,
    ) -> Result<Response<RecordActivityHeartbeatResponse>, Status> {
        let req = request.into_inner();
        let ns = if req.namespace.is_empty() { "default" } else { &req.namespace };
        let details = if req.details.is_empty() { serde_json::Value::Null } else { serde_json::from_slice(&req.details).unwrap_or(serde_json::Value::Null) };
        match self.engine.record_heartbeat(ns, &req.workflow_id, &req.activity_id, details) {
            Ok(cancel_requested) => Ok(Response::new(RecordActivityHeartbeatResponse { success: true, cancel_requested })),
            Err(_e) => Ok(Response::new(RecordActivityHeartbeatResponse { success: false, cancel_requested: false })),
        }
    }

    // ─── ScheduleActivity ────────────────────────────────────────────────
    async fn schedule_activity(
        &self, request: Request<ScheduleActivityRequest>,
    ) -> Result<Response<ScheduleActivityResponse>, Status> {
        let req = request.into_inner();
        let ns = if req.namespace.is_empty() { "default" } else { &req.namespace };
        let tq = if req.task_queue.is_empty() { "default-queue" } else { &req.task_queue };
        let input = if req.input.is_empty() { serde_json::Value::Null } else { serde_json::from_slice(&req.input).unwrap_or(serde_json::Value::Null) };
        let hb_timeout = if req.heartbeat_timeout_ms > 0 { Some(req.heartbeat_timeout_ms) } else { None };
        match self.engine.schedule_activity(ns, &req.workflow_id, &req.run_id, &req.activity_id, &req.activity_type, tq, input, hb_timeout) {
            Ok(act) => Ok(Response::new(ScheduleActivityResponse { activity_id: act.activity_id, success: true, error: String::new() })),
            Err(e) => Ok(Response::new(ScheduleActivityResponse { activity_id: String::new(), success: false, error: e })),
        }
    }

    // ─── CompleteActivityTask ────────────────────────────────────────────
    async fn complete_activity_task(
        &self, request: Request<CompleteActivityTaskRequest>,
    ) -> Result<Response<CompleteActivityTaskResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let ns = if req.namespace.is_empty() { "default" } else { &req.namespace };
        let result = if req.result.is_empty() { serde_json::Value::Null } else { serde_json::from_slice(&req.result).unwrap_or(serde_json::Value::Null) };
        match self.engine.complete_activity(ns, &req.workflow_id, &req.activity_id, result) {
            Ok(()) => Ok(Response::new(CompleteActivityTaskResponse { success: true, latency_us: start.elapsed().as_micros() as i64, error: String::new() })),
            Err(e) => Ok(Response::new(CompleteActivityTaskResponse { success: false, latency_us: 0, error: e })),
        }
    }

    // ─── FailActivityTask ────────────────────────────────────────────────
    async fn fail_activity_task(
        &self, request: Request<FailActivityTaskRequest>,
    ) -> Result<Response<FailActivityTaskResponse>, Status> {
        let req = request.into_inner();
        let ns = if req.namespace.is_empty() { "default" } else { &req.namespace };
        match self.engine.fail_activity(ns, &req.workflow_id, &req.activity_id, &req.reason, req.non_retryable) {
            Ok((will_retry, next_attempt)) => Ok(Response::new(FailActivityTaskResponse { success: true, will_retry, next_attempt: next_attempt as i32, error: String::new() })),
            Err(e) => Ok(Response::new(FailActivityTaskResponse { success: false, will_retry: false, next_attempt: 0, error: e })),
        }
    }

    // ─── ReplayWorkflow ──────────────────────────────────────────────────
    async fn replay_workflow(
        &self, request: Request<ReplayWorkflowRequest>,
    ) -> Result<Response<ReplayWorkflowResponse>, Status> {
        let req = request.into_inner();
        let ns = if req.namespace.is_empty() { "default" } else { &req.namespace };
        match self.engine.replay_workflow(ns, &req.workflow_id) {
            Ok((events_replayed, final_status)) => Ok(Response::new(ReplayWorkflowResponse { success: true, events_replayed: events_replayed as i64, final_status, error: String::new() })),
            Err(e) => Ok(Response::new(ReplayWorkflowResponse { success: false, events_replayed: 0, final_status: String::new(), error: e })),
        }
    }

    // ─── ResetWorkflow ───────────────────────────────────────────────────
    async fn reset_workflow(
        &self, request: Request<ResetWorkflowRequest>,
    ) -> Result<Response<ResetWorkflowResponse>, Status> {
        let req = request.into_inner();
        let ns = if req.namespace.is_empty() { "default" } else { &req.namespace };
        match self.engine.reset_workflow(ns, &req.workflow_id, req.reset_to_event_id, &req.reason) {
            Ok(new_run_id) => Ok(Response::new(ResetWorkflowResponse { new_run_id, success: true, error: String::new() })),
            Err(e) => Ok(Response::new(ResetWorkflowResponse { new_run_id: String::new(), success: false, error: e })),
        }
    }

    // ─── BatchTerminate ──────────────────────────────────────────────────
    async fn batch_terminate(
        &self, request: Request<BatchTerminateRequest>,
    ) -> Result<Response<BatchTerminateResponse>, Status> {
        let req = request.into_inner();
        let ns = if req.namespace.is_empty() { "default" } else { &req.namespace };
        let filter = if req.status_filter.is_empty() { "running" } else { &req.status_filter };
        let count = self.engine.batch_terminate(ns, filter, &req.reason, req.max_count);
        Ok(Response::new(BatchTerminateResponse { terminated_count: count as i64 }))
    }

    // ─── BatchSignal ─────────────────────────────────────────────────────
    async fn batch_signal(
        &self, request: Request<BatchSignalRequest>,
    ) -> Result<Response<BatchSignalResponse>, Status> {
        let req = request.into_inner();
        let ns = if req.namespace.is_empty() { "default" } else { &req.namespace };
        let filter = if req.status_filter.is_empty() { "running" } else { &req.status_filter };
        let payload = if req.payload.is_empty() { serde_json::Value::Null } else { serde_json::from_slice(&req.payload).unwrap_or(serde_json::Value::Null) };
        let count = self.engine.batch_signal(ns, filter, &req.signal_name, payload, req.max_count);
        Ok(Response::new(BatchSignalResponse { signaled_count: count as i64 }))
    }

    // ─── DescribeNamespace ───────────────────────────────────────────────
    async fn describe_namespace(
        &self, request: Request<DescribeNamespaceRequest>,
    ) -> Result<Response<DescribeNamespaceResponse>, Status> {
        let req = request.into_inner();
        match self.engine.describe_namespace(&req.name) {
            Ok(ns) => Ok(Response::new(DescribeNamespaceResponse { name: ns.name, id: ns.id, description: ns.description, state: ns.state, retention_days: ns.retention_days, owner_email: ns.owner_email, is_global: ns.is_global, created_at: ns.created_at })),
            Err(e) => Err(Status::not_found(e)),
        }
    }

    // ─── UpdateNamespace ─────────────────────────────────────────────────
    async fn update_namespace(
        &self, request: Request<UpdateNamespaceRequest>,
    ) -> Result<Response<UpdateNamespaceResponse>, Status> {
        let req = request.into_inner();
        let desc = if req.description.is_empty() { None } else { Some(req.description.as_str()) };
        let ret = if req.retention_days > 0 { Some(req.retention_days) } else { None };
        let email = if req.owner_email.is_empty() { None } else { Some(req.owner_email.as_str()) };
        match self.engine.update_namespace(&req.name, desc, ret, email) {
            Ok(()) => Ok(Response::new(UpdateNamespaceResponse { success: true, error: String::new() })),
            Err(e) => Ok(Response::new(UpdateNamespaceResponse { success: false, error: e })),
        }
    }

    // ─── DeleteNamespace ─────────────────────────────────────────────────
    async fn delete_namespace(
        &self, request: Request<DeleteNamespaceRequest>,
    ) -> Result<Response<DeleteNamespaceResponse>, Status> {
        let req = request.into_inner();
        match self.engine.delete_namespace(&req.name) {
            Ok(()) => Ok(Response::new(DeleteNamespaceResponse { success: true, error: String::new() })),
            Err(e) => Ok(Response::new(DeleteNamespaceResponse { success: false, error: e })),
        }
    }

    // ─── PollWorkflowTask ────────────────────────────────────────────────
    async fn poll_workflow_task(
        &self, request: Request<PollWorkflowTaskRequest>,
    ) -> Result<Response<PollWorkflowTaskResponse>, Status> {
        let req = request.into_inner();
        let ns = if req.namespace.is_empty() { "default" } else { &req.namespace };
        match self.engine.poll_workflow_task(ns, &req.task_queue, &req.identity) {
            Some((token, event_id, event_type)) => Ok(Response::new(PollWorkflowTaskResponse { task_token: token, event_id: event_id as i64, event_type, workflow_execution: Vec::new(), has_task: true })),
            None => Ok(Response::new(PollWorkflowTaskResponse { task_token: String::new(), event_id: 0, event_type: String::new(), workflow_execution: Vec::new(), has_task: false })),
        }
    }

    // ─── PollActivityTask ────────────────────────────────────────────────
    async fn poll_activity_task(
        &self, request: Request<PollActivityTaskRequest>,
    ) -> Result<Response<PollActivityTaskResponse>, Status> {
        let req = request.into_inner();
        let ns = if req.namespace.is_empty() { "default" } else { &req.namespace };
        match self.engine.poll_activity_task(ns, &req.task_queue, &req.identity) {
            Some(act) => Ok(Response::new(PollActivityTaskResponse {
                task_token: format!("at-{}", act.activity_id),
                activity_id: act.activity_id,
                activity_type: act.activity_type,
                input: serde_json::to_vec(&act.input).unwrap_or_default(),
                workflow_id: act.workflow_id,
                has_task: true,
                scheduled_time: act.scheduled_at,
            })),
            None => Ok(Response::new(PollActivityTaskResponse { task_token: String::new(), activity_id: String::new(), activity_type: String::new(), input: Vec::new(), workflow_id: String::new(), has_task: false, scheduled_time: 0 })),
        }
    }

    // ─── GetWorkflowHistory ──────────────────────────────────────────────
    async fn get_workflow_history(
        &self, request: Request<GetWorkflowHistoryRequest>,
    ) -> Result<Response<GetWorkflowHistoryResponse>, Status> {
        let req = request.into_inner();
        let ns = if req.namespace.is_empty() { "default" } else { &req.namespace };
        let history = self.engine.get_history(ns, &req.workflow_id);
        let total = history.len() as i64;
        let page_size = if req.max_page_size > 0 { req.max_page_size as usize } else { 1000 };
        let events: Vec<Vec<u8>> = history.iter().take(page_size).map(|e| serde_json::to_vec(e).unwrap_or_default()).collect();
        Ok(Response::new(GetWorkflowHistoryResponse { events, next_page_token: Vec::new(), total_event_count: total }))
    }
}
