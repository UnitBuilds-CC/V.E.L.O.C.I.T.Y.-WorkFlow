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
            .map_err(|e| Status::internal(e))?;

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
        let poll_interval = std::time::Duration::from_millis(1);
        loop {
            if let Some(wf) = self.engine.get_workflow(namespace, &req.workflow_id) {
                let status = wf.status.as_str();
                if status == "COMPLETED" || status == "FAILED" || status == "TERMINATED" {
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
        let count = self.engine.count_workflows(namespace, filter);
        Ok(Response::new(CountWorkflowsResponse {
            count: count as i64,
        }))
    }
}
