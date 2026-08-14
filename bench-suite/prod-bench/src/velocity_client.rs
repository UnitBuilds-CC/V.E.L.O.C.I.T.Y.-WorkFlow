//! Velocity production bench client — talks to the REAL velocity-workflow-server
//! via gRPC (BenchmarkService proto) with WAL persistence.
//!
//! This measures the ACTUAL production engine with real WAL persistence,
//! not a mock or in-memory adapter.

use std::collections::HashMap;
use std::time::Instant;

use crate::workloads::WorkloadKind;

// Include the generated gRPC client code from build.rs.
pub mod velocity_bench_proto {
    tonic::include_proto!("velocity.bench.v1");
}

use velocity_bench_proto::benchmark_service_client::BenchmarkServiceClient;
use velocity_bench_proto::*;

pub struct VelocityClient {
    client: BenchmarkServiceClient<tonic::transport::Channel>,
    namespace: String,
}

impl VelocityClient {
    pub async fn new(grpc_url: &str) -> Result<Self, String> {
        let endpoint = tonic::transport::Channel::from_shared(grpc_url.to_string())
            .map_err(|e| format!("Invalid gRPC address: {}", e))?
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(120));

        let channel = endpoint
            .connect()
            .await
            .map_err(|e| format!("Velocity gRPC connect to {} failed: {}", grpc_url, e))?;

        let mut client = BenchmarkServiceClient::new(channel);

        // Register benchmark namespace
        let _ = client
            .register_namespace(RegisterNamespaceRequest {
                name: "benchmark".to_string(),
                description: "Production benchmark namespace".to_string(),
            })
            .await;

        // Health check
        let health = client
            .health_check(HealthCheckRequest {})
            .await
            .map_err(|e| format!("Velocity health check failed: {}", e))?;

        tracing::info!(
            "Connected to Velocity (real engine + WAL) at {} — uptime={}s",
            grpc_url,
            health.get_ref().uptime_secs
        );

        Ok(Self {
            client,
            namespace: "benchmark".to_string(),
        })
    }

    /// Run a single workflow end-to-end. Returns latency in microseconds.
    pub async fn run_workflow(
        &self,
        wf_id: &str,
        workload_name: &str,
        kind: &WorkloadKind,
    ) -> Result<f64, String> {
        let start = Instant::now();

        match kind {
            WorkloadKind::SimpleWorkflow
            | WorkloadKind::HighStep
            | WorkloadKind::ConcurrentWorkflows
            | WorkloadKind::ThroughputCeiling
            | WorkloadKind::TailLatencySustained
            | WorkloadKind::ColdStart
            | WorkloadKind::ChildWorkflows
            | WorkloadKind::SagaPattern => {
                self.start_and_complete_workflow(wf_id, workload_name).await?;
            }

            WorkloadKind::SignalStorm => {
                let (workflow_id, run_id) = self.start_workflow_raw(wf_id, "signal_target").await?;
                // Send 100 signals
                for i in 0..100 {
                    let signal_name = format!("signal_{}", i);
                    self.signal_workflow_raw(&workflow_id, &run_id, &signal_name, b"ping")
                        .await?;
                }
                // Complete the step and wait
                self.complete_step_raw(&workflow_id, &run_id).await?;
                self.wait_for_completion_raw(&workflow_id, &run_id).await?;
            }

            WorkloadKind::QueryBurst => {
                let (workflow_id, run_id) = self.start_workflow_raw(wf_id, "query_target").await?;
                // Send 100 queries
                for _ in 0..100 {
                    self.query_workflow_raw(&workflow_id, &run_id, "status").await?;
                }
                self.complete_step_raw(&workflow_id, &run_id).await?;
                self.wait_for_completion_raw(&workflow_id, &run_id).await?;
            }

            WorkloadKind::MixedOperations => {
                let (workflow_id, run_id) =
                    self.start_workflow_raw(wf_id, workload_name).await?;
                for i in 0..10 {
                    self.signal_workflow_raw(
                        &workflow_id,
                        &run_id,
                        "data",
                        b"payload",
                    )
                    .await?;
                    if i % 3 == 0 {
                        let _ = self.query_workflow_raw(&workflow_id, &run_id, "status").await;
                    }
                }
                self.complete_step_raw(&workflow_id, &run_id).await?;
                self.wait_for_completion_raw(&workflow_id, &run_id).await?;
            }

            WorkloadKind::SearchAttributes => {
                self.start_and_complete_workflow(wf_id, workload_name).await?;
            }

            WorkloadKind::PayloadRoundtrip => {
                let payload = "x".repeat(1024);
                let (workflow_id, run_id) = self
                    .start_workflow_with_input(wf_id, workload_name, payload.as_bytes())
                    .await?;
                self.complete_step_raw(&workflow_id, &run_id).await?;
                self.wait_for_completion_raw(&workflow_id, &run_id).await?;
            }
        }

        Ok(start.elapsed().as_micros() as f64)
    }

    /// Reset engine state between workloads.
    pub async fn reset(&self) -> Result<(), String> {
        let mut client = self.client.clone();
        client
            .reset(ResetRequest {
                namespace: self.namespace.clone(),
            })
            .await
            .map_err(|e| format!("Reset failed: {}", e))?;
        Ok(())
    }

    // ─── Internal helpers ─────────────────────────────────────────────────

    async fn start_and_complete_workflow(
        &self,
        wf_id: &str,
        wf_type: &str,
    ) -> Result<(String, String), String> {
        let (workflow_id, run_id) = self.start_workflow_raw(wf_id, wf_type).await?;
        self.complete_step_raw(&workflow_id, &run_id).await?;
        self.wait_for_completion_raw(&workflow_id, &run_id).await?;
        Ok((workflow_id, run_id))
    }

    async fn start_workflow_raw(
        &self,
        wf_id: &str,
        wf_type: &str,
    ) -> Result<(String, String), String> {
        let mut client = self.client.clone();
        let resp = client
            .start_workflow(StartWorkflowRequest {
                workflow_type: wf_type.to_string(),
                workflow_id: wf_id.to_string(),
                namespace: self.namespace.clone(),
                task_queue: "bench-queue".to_string(),
                input: vec![],
                step_count: 1,
                search_attributes: HashMap::new(),
                execution_timeout_ms: 30_000,
            })
            .await
            .map_err(|e| format!("start_workflow {} failed: {}", wf_id, e))?;

        let inner = resp.into_inner();
        Ok((inner.workflow_id, inner.run_id))
    }

    async fn start_workflow_with_input(
        &self,
        wf_id: &str,
        wf_type: &str,
        input: &[u8],
    ) -> Result<(String, String), String> {
        let mut client = self.client.clone();
        let resp = client
            .start_workflow(StartWorkflowRequest {
                workflow_type: wf_type.to_string(),
                workflow_id: wf_id.to_string(),
                namespace: self.namespace.clone(),
                task_queue: "bench-queue".to_string(),
                input: input.to_vec(),
                step_count: 1,
                search_attributes: HashMap::new(),
                execution_timeout_ms: 30_000,
            })
            .await
            .map_err(|e| format!("start_workflow {} failed: {}", wf_id, e))?;

        let inner = resp.into_inner();
        Ok((inner.workflow_id, inner.run_id))
    }

    async fn signal_workflow_raw(
        &self,
        workflow_id: &str,
        run_id: &str,
        signal_name: &str,
        payload: &[u8],
    ) -> Result<(), String> {
        let mut client = self.client.clone();
        client
            .signal_workflow(SignalWorkflowRequest {
                workflow_id: workflow_id.to_string(),
                run_id: run_id.to_string(),
                signal_name: signal_name.to_string(),
                payload: payload.to_vec(),
                namespace: self.namespace.clone(),
            })
            .await
            .map_err(|e| format!("signal {} on {} failed: {}", signal_name, workflow_id, e))?;
        Ok(())
    }

    async fn query_workflow_raw(
        &self,
        workflow_id: &str,
        run_id: &str,
        query_type: &str,
    ) -> Result<Vec<u8>, String> {
        let mut client = self.client.clone();
        let resp = client
            .query_workflow(QueryWorkflowRequest {
                workflow_id: workflow_id.to_string(),
                run_id: run_id.to_string(),
                query_type: query_type.to_string(),
                payload: vec![],
                namespace: self.namespace.clone(),
            })
            .await
            .map_err(|e| format!("query {} on {} failed: {}", query_type, workflow_id, e))?;
        Ok(resp.into_inner().result)
    }

    async fn complete_step_raw(
        &self,
        workflow_id: &str,
        run_id: &str,
    ) -> Result<(), String> {
        let mut client = self.client.clone();
        client
            .complete_step(CompleteStepRequest {
                workflow_id: workflow_id.to_string(),
                run_id: run_id.to_string(),
                step_index: 0,
                result: vec![],
                namespace: self.namespace.clone(),
            })
            .await
            .map_err(|e| format!("complete_step on {} failed: {}", workflow_id, e))?;
        Ok(())
    }

    async fn wait_for_completion_raw(
        &self,
        workflow_id: &str,
        run_id: &str,
    ) -> Result<(), String> {
        let mut client = self.client.clone();
        client
            .wait_for_completion(WaitForCompletionRequest {
                workflow_id: workflow_id.to_string(),
                run_id: run_id.to_string(),
                namespace: self.namespace.clone(),
                timeout_ms: 30_000,
            })
            .await
            .map_err(|e| format!("wait_for_completion {} failed: {}", workflow_id, e))?;
        Ok(())
    }
}
