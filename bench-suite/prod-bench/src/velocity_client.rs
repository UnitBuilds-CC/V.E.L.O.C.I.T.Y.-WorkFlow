//! Velocity production bench client — talks to the real Velocity dev server
//! via its HTTP API (POST /api/v1/workflows, signal, query, etc.).
//!
//! This measures the ACTUAL production API, not a mock or benchmark adapter.

use reqwest::Client;
use std::time::Instant;

use crate::workloads::WorkloadKind;

pub struct VelocityClient {
    client: Client,
    base_url: String,
}

impl VelocityClient {
    pub async fn new(base_url: &str) -> Result<Self, String> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(120))
            .pool_max_idle_per_host(50)
            .build()
            .map_err(|e| format!("HTTP client error: {}", e))?;

        // Health check
        let health_url = format!("{}/health", base_url);
        let resp = client
            .get(&health_url)
            .send()
            .await
            .map_err(|e| format!("Velocity health check failed: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("Velocity health check returned {}", resp.status()));
        }

        tracing::info!("Connected to Velocity at {}", base_url);
        Ok(Self { client, base_url: base_url.trim_end_matches('/').to_string() })
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
                self.start_workflow(wf_id, workload_name).await?;
            }

            WorkloadKind::SignalStorm => {
                self.start_workflow(wf_id, "signal_target").await?;
                // Send 100 signals
                for i in 0..100 {
                    let signal_name = format!("signal_{}", i);
                    self.signal_workflow(wf_id, &signal_name, b"ping").await?;
                }
            }

            WorkloadKind::QueryBurst => {
                self.start_workflow(wf_id, "query_target").await?;
                // Send 100 queries
                for _ in 0..100 {
                    self.query_workflow(wf_id, "status").await?;
                }
            }

            WorkloadKind::MixedOperations => {
                self.start_workflow(wf_id, workload_name).await?;
                // Mix of signals and queries
                for i in 0..10 {
                    self.signal_workflow(wf_id, "data", b"payload").await?;
                    if i % 3 == 0 {
                        let _ = self.query_workflow(wf_id, "status").await;
                    }
                }
            }

            WorkloadKind::SearchAttributes => {
                self.start_workflow_with_attrs(wf_id, workload_name).await?;
            }

            WorkloadKind::PayloadRoundtrip => {
                self.start_workflow_with_payload(wf_id, workload_name, 1024).await?;
            }
        }

        let elapsed_us = start.elapsed().as_micros() as f64;
        Ok(elapsed_us)
    }

    async fn start_workflow(&self, wf_id: &str, wf_type: &str) -> Result<(), String> {
        let url = format!("{}/api/v1/workflows", self.base_url);
        let body = serde_json::json!({
            "workflowType": wf_type,
            "taskQueue": "bench-queue",
            "namespace": "benchmark",
            "input": {"bench": true, "id": wf_id},
        });

        let resp = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("start_workflow HTTP error: {}", e))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("start_workflow {} failed: {} — {}", wf_id, status, body));
        }
        Ok(())
    }

    async fn start_workflow_with_attrs(&self, wf_id: &str, wf_type: &str) -> Result<(), String> {
        let url = format!("{}/api/v1/workflows", self.base_url);
        let body = serde_json::json!({
            "workflowType": wf_type,
            "taskQueue": "bench-queue",
            "namespace": "benchmark",
            "input": {"bench": true},
            "searchAttributes": {
                "environment": "benchmark",
                "workload": wf_type,
                "priority": "high",
            },
        });

        let resp = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("start_workflow HTTP error: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("start_workflow {} failed: {}", wf_id, resp.status()));
        }
        Ok(())
    }

    async fn start_workflow_with_payload(
        &self,
        wf_id: &str,
        wf_type: &str,
        payload_size: usize,
    ) -> Result<(), String> {
        let url = format!("{}/api/v1/workflows", self.base_url);
        let payload = "x".repeat(payload_size);
        let body = serde_json::json!({
            "workflowType": wf_type,
            "taskQueue": "bench-queue",
            "namespace": "benchmark",
            "input": {"data": payload},
        });

        let resp = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("start_workflow HTTP error: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("start_workflow {} failed: {}", wf_id, resp.status()));
        }
        Ok(())
    }

    async fn signal_workflow(
        &self,
        wf_id: &str,
        signal_name: &str,
        payload: &[u8],
    ) -> Result<(), String> {
        let url = format!("{}/api/v1/workflows/{}/signal", self.base_url, wf_id);
        let body = serde_json::json!({
            "signalName": signal_name,
            "input": serde_json::Value::String(String::from_utf8_lossy(payload).to_string()),
        });

        let resp = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("signal HTTP error: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("signal {} on {} failed: {}", signal_name, wf_id, resp.status()));
        }
        Ok(())
    }

    async fn query_workflow(&self, wf_id: &str, query_type: &str) -> Result<(), String> {
        let url = format!("{}/api/v1/workflows/{}/query/{}", self.base_url, wf_id, query_type);

        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("query HTTP error: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("query {} on {} failed: {}", query_type, wf_id, resp.status()));
        }
        Ok(())
    }
}
