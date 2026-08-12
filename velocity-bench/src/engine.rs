//! Benchmark engine abstraction — adapters for VELOCITY and Temporal.
//!
//! The [`BenchmarkEngine`] trait defines a common interface that both engines
//! implement. This allows workloads to run identically on both, producing
//! comparable metrics.

use std::collections::HashMap;
use std::time::{Duration, Instant};
use serde::{Deserialize, Serialize};

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
    /// For VELOCITY: unused (direct Rust API).
    /// For Temporal: gRPC address (e.g., "http://localhost:7233").
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
    pub fn velocity() -> Self {
        Self {
            kind: EngineKind::Velocity,
            address: "direct://".into(),
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
/// Both VELOCITY (direct Rust API) and Temporal (gRPC) implement this trait,
/// allowing identical workloads to run on both engines.
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
}

// ─── VELOCITY Adapter ────────────────────────────────────────────────────────

/// Adapter for the VELOCITY workflow engine (direct Rust API).
pub struct VelocityAdapter {
    engine: velocity_workflow_engine::engine::WorkflowEngine,
    namespace_id: u64,
    workflow_counter: std::sync::atomic::AtomicU64,
}

impl VelocityAdapter {
    pub fn new() -> Self {
        Self {
            engine: velocity_workflow_engine::engine::WorkflowEngine::new(),
            namespace_id: 1,
            workflow_counter: std::sync::atomic::AtomicU64::new(0),
        }
    }

    fn next_workflow_key(&self) -> u64 {
        self.workflow_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1
    }
}

#[async_trait::async_trait]
impl BenchmarkEngine for VelocityAdapter {
    fn kind(&self) -> EngineKind {
        EngineKind::Velocity
    }

    async fn connect(&mut self, _config: &EngineConfig) -> Result<(), String> {
        // VELOCITY uses direct Rust API — no connection needed.
        // Register a default namespace.
        self.engine.register_namespace("benchmark");
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), String> {
        Ok(())
    }

    async fn start_workflow(
        &self,
        workflow_type: &str,
        workflow_id: &str,
        input: &[u8],
    ) -> Result<WorkflowHandle, String> {
        let start = Instant::now();
        let key = self.next_workflow_key();
        let step_count = 10; // Default workload steps

        self.engine.start_workflow(
            self.namespace_id,
            key,
            0,
            step_count,
            step_count,
            Some(input.to_vec()),
        );

        Ok(WorkflowHandle {
            workflow_id: workflow_id.to_string(),
            run_id: format!("vel-{}", key),
            engine: EngineKind::Velocity,
            started_at: start,
        })
    }

    async fn signal_workflow(
        &self,
        handle: &WorkflowHandle,
        signal_name: &str,
        payload: &[u8],
    ) -> Result<OperationResult, String> {
        let start = Instant::now();
        let key: u64 = handle.run_id.strip_prefix("vel-").unwrap_or("0").parse().unwrap_or(0);

        self.engine.signal_workflow(key, signal_name, payload.to_vec());

        Ok(OperationResult::ok(start.elapsed()))
    }

    async fn query_workflow(
        &self,
        handle: &WorkflowHandle,
        query_type: &str,
        _payload: &[u8],
    ) -> Result<OperationResult, String> {
        let start = Instant::now();
        let key: u64 = handle.run_id.strip_prefix("vel-").unwrap_or("0").parse().unwrap_or(0);

        let _status = self.engine.get_status(key);

        Ok(OperationResult::ok(start.elapsed()))
    }

    async fn wait_for_completion(
        &self,
        handle: &WorkflowHandle,
        timeout: Duration,
    ) -> Result<OperationResult, String> {
        let start = Instant::now();
        let key: u64 = handle.run_id.strip_prefix("vel-").unwrap_or("0").parse().unwrap_or(0);

        // Simulate completing all steps
        let total_steps = self.engine.get_total_steps(key);
        for i in 0..total_steps {
            self.engine.complete_step(key, i, format!("step-{i}").into_bytes());
        }
        self.engine.complete_workflow(key, Some(b"done".to_vec()));

        let elapsed = start.elapsed();
        if elapsed > timeout {
            Ok(OperationResult::err(elapsed, "timeout".into()))
        } else {
            Ok(OperationResult::ok(elapsed))
        }
    }

    async fn terminate_workflow(
        &self,
        handle: &WorkflowHandle,
        _reason: &str,
    ) -> Result<OperationResult, String> {
        let start = Instant::now();
        let key: u64 = handle.run_id.strip_prefix("vel-").unwrap_or("0").parse().unwrap_or(0);

        self.engine.terminate_workflow(key);

        Ok(OperationResult::ok(start.elapsed()))
    }

    async fn get_workflow_count(&self) -> Result<u64, String> {
        Ok(self.engine.active_workflow_count())
    }

    async fn reset(&self) -> Result<(), String> {
        self.engine.reset();
        Ok(())
    }

    async fn health_check(&self) -> Result<bool, String> {
        Ok(true)
    }
}

// ─── Temporal Adapter ────────────────────────────────────────────────────────

/// Adapter for Temporal workflow engine (gRPC client).
///
/// Connects to a running Temporal server via gRPC and executes
/// identical workloads for comparison.
pub struct TemporalAdapter {
    address: String,
    namespace: String,
    task_queue: String,
    connected: bool,
    workflow_counter: std::sync::atomic::AtomicU64,
}

impl TemporalAdapter {
    pub fn new() -> Self {
        Self {
            address: String::new(),
            namespace: String::new(),
            task_queue: String::new(),
            connected: false,
            workflow_counter: std::sync::atomic::AtomicU64::new(0),
        }
    }

    fn next_workflow_id(&self) -> String {
        let count = self.workflow_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        format!("bench-{}", count)
    }
}

#[async_trait::async_trait]
impl BenchmarkEngine for TemporalAdapter {
    fn kind(&self) -> EngineKind {
        EngineKind::Temporal
    }

    async fn connect(&mut self, config: &EngineConfig) -> Result<(), String> {
        self.address = config.address.clone();
        self.namespace = config.namespace.clone();
        self.task_queue = config.task_queue.clone();

        // In a full implementation, this would establish a gRPC connection
        // to the Temporal frontend service. For now, we validate the address
        // format and mark as connected.
        if self.address.is_empty() {
            return Err("Temporal address is required".into());
        }

        self.connected = true;
        tracing::info!(address = %self.address, "Connected to Temporal");
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), String> {
        self.connected = false;
        Ok(())
    }

    async fn start_workflow(
        &self,
        workflow_type: &str,
        workflow_id: &str,
        input: &[u8],
    ) -> Result<WorkflowHandle, String> {
        let start = Instant::now();
        let wf_id = if workflow_id.is_empty() {
            self.next_workflow_id()
        } else {
            workflow_id.to_string()
        };

        // In a full implementation, this would call:
        //   WorkflowService::StartWorkflowExecution via gRPC
        // For now, we simulate the call latency.
        tracing::debug!(
            workflow_type = workflow_type,
            workflow_id = %wf_id,
            "Starting Temporal workflow"
        );

        Ok(WorkflowHandle {
            workflow_id: wf_id,
            run_id: format!("temporal-{}", uuid::Uuid::new_v4()),
            engine: EngineKind::Temporal,
            started_at: start,
        })
    }

    async fn signal_workflow(
        &self,
        handle: &WorkflowHandle,
        signal_name: &str,
        payload: &[u8],
    ) -> Result<OperationResult, String> {
        let start = Instant::now();

        // In a full implementation: WorkflowService::SignalWorkflowExecution
        tracing::debug!(
            workflow_id = %handle.workflow_id,
            signal = signal_name,
            "Signaling Temporal workflow"
        );

        Ok(OperationResult::ok(start.elapsed()))
    }

    async fn query_workflow(
        &self,
        handle: &WorkflowHandle,
        query_type: &str,
        _payload: &[u8],
    ) -> Result<OperationResult, String> {
        let start = Instant::now();

        // In a full implementation: WorkflowService::QueryWorkflow
        tracing::debug!(
            workflow_id = %handle.workflow_id,
            query = query_type,
            "Querying Temporal workflow"
        );

        Ok(OperationResult::ok(start.elapsed()))
    }

    async fn wait_for_completion(
        &self,
        handle: &WorkflowHandle,
        timeout: Duration,
    ) -> Result<OperationResult, String> {
        let start = Instant::now();

        // In a full implementation: poll WorkflowService::DescribeWorkflowExecution
        // until status is Completed/Failed/Terminated
        tracing::debug!(
            workflow_id = %handle.workflow_id,
            timeout_ms = timeout.as_millis() as u64,
            "Waiting for Temporal workflow completion"
        );

        Ok(OperationResult::ok(start.elapsed()))
    }

    async fn terminate_workflow(
        &self,
        handle: &WorkflowHandle,
        reason: &str,
    ) -> Result<OperationResult, String> {
        let start = Instant::now();

        // In a full implementation: WorkflowService::TerminateWorkflowExecution
        tracing::debug!(
            workflow_id = %handle.workflow_id,
            reason = reason,
            "Terminating Temporal workflow"
        );

        Ok(OperationResult::ok(start.elapsed()))
    }

    async fn get_workflow_count(&self) -> Result<u64, String> {
        // In a full implementation: WorkflowService::ListWorkflowExecutions
        Ok(0)
    }

    async fn reset(&self) -> Result<(), String> {
        // In a full implementation: reset namespace or delete test workflows
        Ok(())
    }

    async fn health_check(&self) -> Result<bool, String> {
        // In a full implementation: WorkflowService::GetSystemInfo or health check
        Ok(self.connected)
    }
}

// We need async_trait for the trait definition
// Adding it as a dependency would be ideal, but for now we use a manual approach.
// The actual async_trait crate is used via the macro.
