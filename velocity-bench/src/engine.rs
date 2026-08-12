//! Benchmark engine abstraction — gRPC client adapters for VELOCITY and Temporal.
//!
//! Both adapters connect to their respective engines via **identical gRPC paths**,
//! ensuring an apples-to-apples comparison. Neither adapter uses a direct/in-process
//! API — both pay the same serialization, network, and protocol overhead.
//!
//! Architecture:
//!   [velocity-bench] ──gRPC──► [velocity-dev-server] ──► [DevEngine]  (VELOCITY)
//!   [velocity-bench] ──gRPC──► [temporal-server]       ──► [Matching/History] (Temporal)

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};

// Include the generated gRPC client code from build.rs.
pub mod velocity_bench_proto {
    tonic::include_proto!("velocity.bench.v1");
}

use velocity_bench_proto::benchmark_service_client::BenchmarkServiceClient;
use velocity_bench_proto::*;

// ─── Engine Configuration ────────────────────────────────────────────────────

/// Which engine to benchmark.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EngineKind {
    Velocity,
    Temporal,
}

impl std::fmt::Display for EngineKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineKind::Velocity => write!(f, "VELOCITY"),
            EngineKind::Temporal => write!(f, "Temporal"),
        }
    }
}

/// Configuration for connecting to an engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    pub kind: EngineKind,
    /// gRPC address (e.g., "http://localhost:7234").
    pub address: String,
    /// Namespace to use.
    pub namespace: String,
    /// Task queue name.
    pub task_queue: String,
    /// Connection timeout.
    pub connect_timeout_ms: u64,
    /// Operation timeout.
    pub operation_timeout_ms: u64,
}

impl EngineConfig {
    pub fn velocity(addr: &str) -> Self {
        Self {
            kind: EngineKind::Velocity,
            address: addr.into(),
            namespace: "benchmark".into(),
            task_queue: "bench-queue".into(),
            connect_timeout_ms: 5_000,
            operation_timeout_ms: 30_000,
        }
    }

    pub fn temporal(addr: &str) -> Self {
        Self {
            kind: EngineKind::Temporal,
            address: addr.into(),
            namespace: "benchmark".into(),
            task_queue: "bench-queue".into(),
            connect_timeout_ms: 10_000,
            operation_timeout_ms: 60_000,
        }
    }
}

// ─── Workflow Handle ─────────────────────────────────────────────────────────

/// An opaque handle to a running workflow.
#[derive(Debug, Clone)]
pub struct WorkflowHandle {
    pub workflow_id: String,
    pub run_id: String,
    pub engine: EngineKind,
    pub started_at: Instant,
}

/// Result of a workflow operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationResult {
    pub success: bool,
    pub latency_us: u64,
    pub error: Option<String>,
}

impl OperationResult {
    pub fn ok(latency: Duration) -> Self {
        Self {
            success: true,
            latency_us: latency.as_micros() as u64,
            error: None,
        }
    }

    pub fn err(latency: Duration, error: String) -> Self {
        Self {
            success: false,
            latency_us: latency.as_micros() as u64,
            error: Some(error),
        }
    }
}

// ─── Benchmark Result ────────────────────────────────────────────────────────

/// Aggregate result from running a workload on an engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    pub engine: EngineKind,
    pub workload_name: String,
    pub total_operations: u64,
    pub successful_operations: u64,
    pub failed_operations: u64,
    pub total_duration_ms: u64,
    pub operations_per_second: f64,
    pub latency_p50_us: u64,
    pub latency_p95_us: u64,
    pub latency_p99_us: u64,
    pub latency_p999_us: u64,
    pub latency_min_us: u64,
    pub latency_max_us: u64,
    pub latency_mean_us: u64,
    pub peak_memory_mb: f64,
    pub peak_cpu_percent: f64,
    pub errors: HashMap<String, u64>,
}

impl BenchmarkResult {
    pub fn error_rate(&self) -> f64 {
        if self.total_operations == 0 {
            0.0
        } else {
            self.failed_operations as f64 / self.total_operations as f64 * 100.0
        }
    }
}

// ─── Benchmark Engine Trait ──────────────────────────────────────────────────

/// Common interface for benchmarking workflow engines.
///
/// Both VELOCITY and Temporal connect via identical gRPC paths,
/// ensuring a fair apples-to-apples comparison.
#[async_trait::async_trait]
pub trait BenchmarkEngine: Send + Sync {
    /// Returns the engine kind.
    fn kind(&self) -> EngineKind;

    /// Connect to / initialize the engine.
    async fn connect(&mut self, config: &EngineConfig) -> Result<(), String>;

    /// Disconnect / cleanup.
    async fn disconnect(&mut self) -> Result<(), String>;

    /// Start a new workflow execution.
    async fn start_workflow(
        &self,
        workflow_type: &str,
        workflow_id: &str,
        input: &[u8],
    ) -> Result<WorkflowHandle, String>;

    /// Signal a running workflow.
    async fn signal_workflow(
        &self,
        handle: &WorkflowHandle,
        signal_name: &str,
        payload: &[u8],
    ) -> Result<OperationResult, String>;

    /// Query a running workflow.
    async fn query_workflow(
        &self,
        handle: &WorkflowHandle,
        query_type: &str,
        payload: &[u8],
    ) -> Result<OperationResult, String>;

    /// Wait for a workflow to complete and return its result.
    async fn wait_for_completion(
        &self,
        handle: &WorkflowHandle,
        timeout: Duration,
    ) -> Result<OperationResult, String>;

    /// Terminate a running workflow.
    async fn terminate_workflow(
        &self,
        handle: &WorkflowHandle,
        reason: &str,
    ) -> Result<OperationResult, String>;

    /// Get current workflow count (for throughput measurement).
    async fn get_workflow_count(&self) -> Result<u64, String>;

    /// Reset the engine state (clear all workflows).
    async fn reset(&self) -> Result<(), String>;

    /// Get engine health status.
    async fn health_check(&self) -> Result<bool, String>;

    /// Complete a workflow step (for driving workflows forward).
    async fn complete_step(
        &self,
        handle: &WorkflowHandle,
        step_index: i32,
        result: &[u8],
    ) -> Result<OperationResult, String>;
}

// ─── gRPC Client Adapter (shared by both engines) ───────────────────────────

/// Generic gRPC client adapter. Both VELOCITY and Temporal connect through
/// the same `BenchmarkServiceClient`, ensuring identical protocol overhead.
///
/// The only difference is the server address — VELOCITY connects to
/// `velocity-dev-server` and Temporal connects to its own server.
pub struct GrpcAdapter {
    engine_kind: EngineKind,
    client: Option<BenchmarkServiceClient<tonic::transport::Channel>>,
    address: String,
    namespace: String,
    task_queue: String,
}

impl GrpcAdapter {
    pub fn new(kind: EngineKind) -> Self {
        Self {
            engine_kind: kind,
            client: None,
            address: String::new(),
            namespace: String::new(),
            task_queue: String::new(),
        }
    }

    fn require_client(&self) -> Result<&BenchmarkServiceClient<tonic::transport::Channel>, String> {
        self.client
            .as_ref()
            .ok_or_else(|| "Not connected — call connect() first".into())
    }
}

#[async_trait::async_trait]
impl BenchmarkEngine for GrpcAdapter {
    fn kind(&self) -> EngineKind {
        self.engine_kind
    }

    async fn connect(&mut self, config: &EngineConfig) -> Result<(), String> {
        self.address = config.address.clone();
        self.namespace = config.namespace.clone();
        self.task_queue = config.task_queue.clone();

        let timeout = Duration::from_millis(config.connect_timeout_ms);
        let endpoint = tonic::transport::Channel::from_shared(self.address.clone())
            .map_err(|e| format!("Invalid gRPC address: {}", e))?
            .connect_timeout(timeout)
            .timeout(Duration::from_millis(config.operation_timeout_ms));

        let channel = endpoint
            .connect()
            .await
            .map_err(|e| format!("gRPC connect to {} failed: {}", self.address, e))?;

        self.client = Some(BenchmarkServiceClient::new(channel));

        // Register the benchmark namespace via gRPC (same call for both engines).
        let client = self.require_client()?;
        let mut client = client.clone();
        let _ = client
            .register_namespace(RegisterNamespaceRequest {
                name: self.namespace.clone(),
                description: format!("Benchmark namespace for {}", self.engine_kind),
            })
            .await;

        tracing::info!(
            engine = %self.engine_kind,
            address = %self.address,
            "Connected via gRPC (BenchmarkService)"
        );
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), String> {
        self.client = None;
        Ok(())
    }

    async fn start_workflow(
        &self,
        workflow_type: &str,
        workflow_id: &str,
        input: &[u8],
    ) -> Result<WorkflowHandle, String> {
        let client = self.require_client()?;
        let mut client = client.clone();

        let req = StartWorkflowRequest {
            workflow_type: workflow_type.to_string(),
            workflow_id: workflow_id.to_string(),
            namespace: self.namespace.clone(),
            task_queue: self.task_queue.clone(),
            input: input.to_vec(),
            step_count: 10,
            search_attributes: HashMap::new(),
            execution_timeout_ms: 30_000,
        };

        let start = Instant::now();
        let resp = client
            .start_workflow(req)
            .await
            .map_err(|e| format!("StartWorkflow gRPC error: {}", e))?;
        let _rtt = start.elapsed();

        let inner = resp.into_inner();
        Ok(WorkflowHandle {
            workflow_id: inner.workflow_id,
            run_id: inner.run_id,
            engine: self.engine_kind,
            started_at: start,
        })
    }

    async fn signal_workflow(
        &self,
        handle: &WorkflowHandle,
        signal_name: &str,
        payload: &[u8],
    ) -> Result<OperationResult, String> {
        let client = self.require_client()?;
        let mut client = client.clone();

        let start = Instant::now();
        let resp = client
            .signal_workflow(SignalWorkflowRequest {
                workflow_id: handle.workflow_id.clone(),
                run_id: handle.run_id.clone(),
                signal_name: signal_name.to_string(),
                payload: payload.to_vec(),
                namespace: self.namespace.clone(),
            })
            .await
            .map_err(|e| format!("SignalWorkflow gRPC error: {}", e))?;

        let inner = resp.into_inner();
        if inner.success {
            Ok(OperationResult::ok(start.elapsed()))
        } else {
            Ok(OperationResult::err(start.elapsed(), inner.error))
        }
    }

    async fn query_workflow(
        &self,
        handle: &WorkflowHandle,
        query_type: &str,
        payload: &[u8],
    ) -> Result<OperationResult, String> {
        let client = self.require_client()?;
        let mut client = client.clone();

        let start = Instant::now();
        let resp = client
            .query_workflow(QueryWorkflowRequest {
                workflow_id: handle.workflow_id.clone(),
                run_id: handle.run_id.clone(),
                query_type: query_type.to_string(),
                payload: payload.to_vec(),
                namespace: self.namespace.clone(),
            })
            .await
            .map_err(|e| format!("QueryWorkflow gRPC error: {}", e))?;

        let inner = resp.into_inner();
        if inner.success {
            Ok(OperationResult::ok(start.elapsed()))
        } else {
            Ok(OperationResult::err(start.elapsed(), inner.error))
        }
    }

    async fn wait_for_completion(
        &self,
        handle: &WorkflowHandle,
        timeout: Duration,
    ) -> Result<OperationResult, String> {
        let client = self.require_client()?;
        let mut client = client.clone();

        let start = Instant::now();
        let resp = client
            .wait_for_completion(WaitForCompletionRequest {
                workflow_id: handle.workflow_id.clone(),
                run_id: handle.run_id.clone(),
                namespace: self.namespace.clone(),
                timeout_ms: timeout.as_millis() as i64,
            })
            .await
            .map_err(|e| format!("WaitForCompletion gRPC error: {}", e))?;

        let inner = resp.into_inner();
        if inner.success {
            Ok(OperationResult::ok(start.elapsed()))
        } else {
            Ok(OperationResult::err(start.elapsed(), inner.error))
        }
    }

    async fn terminate_workflow(
        &self,
        handle: &WorkflowHandle,
        reason: &str,
    ) -> Result<OperationResult, String> {
        let client = self.require_client()?;
        let mut client = client.clone();

        let start = Instant::now();
        let resp = client
            .terminate_workflow(TerminateWorkflowRequest {
                workflow_id: handle.workflow_id.clone(),
                run_id: handle.run_id.clone(),
                namespace: self.namespace.clone(),
                reason: reason.to_string(),
            })
            .await
            .map_err(|e| format!("TerminateWorkflow gRPC error: {}", e))?;

        let inner = resp.into_inner();
        if inner.success {
            Ok(OperationResult::ok(start.elapsed()))
        } else {
            Ok(OperationResult::err(start.elapsed(), inner.error))
        }
    }

    async fn get_workflow_count(&self) -> Result<u64, String> {
        let client = self.require_client()?;
        let mut client = client.clone();

        let resp = client
            .count_workflows(CountWorkflowsRequest {
                namespace: self.namespace.clone(),
                status_filter: "all".to_string(),
            })
            .await
            .map_err(|e| format!("CountWorkflows gRPC error: {}", e))?;

        Ok(resp.into_inner().count as u64)
    }

    async fn reset(&self) -> Result<(), String> {
        let client = self.require_client()?;
        let mut client = client.clone();

        client
            .reset(ResetRequest {
                namespace: self.namespace.clone(),
            })
            .await
            .map_err(|e| format!("Reset gRPC error: {}", e))?;

        Ok(())
    }

    async fn health_check(&self) -> Result<bool, String> {
        let client = self.require_client()?;
        let mut client = client.clone();

        let resp = client
            .health_check(HealthCheckRequest {})
            .await
            .map_err(|e| format!("HealthCheck gRPC error: {}", e))?;

        Ok(resp.into_inner().healthy)
    }

    async fn complete_step(
        &self,
        handle: &WorkflowHandle,
        step_index: i32,
        result: &[u8],
    ) -> Result<OperationResult, String> {
        let client = self.require_client()?;
        let mut client = client.clone();

        let start = Instant::now();
        let resp = client
            .complete_step(CompleteStepRequest {
                workflow_id: handle.workflow_id.clone(),
                run_id: handle.run_id.clone(),
                step_index,
                result: result.to_vec(),
                namespace: self.namespace.clone(),
            })
            .await
            .map_err(|e| format!("CompleteStep gRPC error: {}", e))?;

        let inner = resp.into_inner();
        if inner.success {
            Ok(OperationResult::ok(start.elapsed()))
        } else {
            Ok(OperationResult::err(start.elapsed(), inner.error))
        }
    }
}

// ─── Convenience Type Aliases ────────────────────────────────────────────────

/// Adapter for VELOCITY-DevServer via gRPC.
pub type VelocityAdapter = GrpcAdapter;

/// Adapter for Temporal via gRPC.
pub type TemporalAdapter = GrpcAdapter;
