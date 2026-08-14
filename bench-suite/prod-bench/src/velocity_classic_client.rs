//! Velocity Classic bench client — HTTP API to the TypeScript Classic server.
//!
//! This talks to velocity-classic-ts which provides a Temporal-compatible
//! HTTP API with in-memory or persistent storage.

use std::time::Instant;

use crate::workloads::WorkloadKind;

pub struct VelocityClassicClient {
    base_url: String,
    http: reqwest::Client,
}

impl VelocityClassicClient {
    pub async fn new(base_url: &str) -> Result<Self, String> {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(120))
            .pool_max_idle_per_host(200)
            .build()
            .map_err(|e| format!("Build HTTP client: {}", e))?;

        // Health check
        let resp = http
            .get(format!("{}/api/health", base_url))
            .send()
            .await
            .map_err(|e| format!("Velocity Classic health check failed: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!(
                "Velocity Classic health returned status {}",
                resp.status()
            ));
        }

        tracing::info!(
            "Connected to Velocity Classic (Temporal-compatible) at {}",
            base_url
        );

        Ok(Self {
            base_url: base_url.to_string(),
            http,
        })
    }

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
            | WorkloadKind::SagaPattern
            | WorkloadKind::SearchAttributes
            | WorkloadKind::PayloadRoundtrip => {
                self.start_and_wait_workflow(wf_id, workload_name).await?;
            }

            WorkloadKind::SignalStorm => {
                let workflow_id = self.start_workflow_raw(wf_id, workload_name).await?;
                // Send 100 signals
                for i in 0..100 {
                    self.signal_workflow(&workflow_id, &format!("signal_{}", i), "ping")
                        .await?;
                }
            }

            WorkloadKind::QueryBurst => {
                let workflow_id = self.start_workflow_raw(wf_id, workload_name).await?;
                // Send 100 queries
                for _ in 0..100 {
                    self.query_workflow(&workflow_id, "status").await?;
                }
            }

            WorkloadKind::MixedOperations => {
                let workflow_id = self.start_workflow_raw(wf_id, workload_name).await?;
                for i in 0..10 {
                    self.signal_workflow(&workflow_id, "data", "payload").await?;
                    if i % 3 == 0 {
                        let _ = self.query_workflow(&workflow_id, "status").await;
                    }
                }
            }
        }

        Ok(start.elapsed().as_micros() as f64)
    }

    pub async fn reset(&self) -> Result<(), String> {
        // No reset endpoint on classic server
        Ok(())
    }

    // ─── Internal helpers ─────────────────────────────────────────────────

    async fn start_and_wait_workflow(&self, wf_id: &str, wf_type: &str) -> Result<(), String> {
        let workflow_id = self.start_workflow_raw(wf_id, wf_type).await?;
        // Poll for completion
        for _ in 0..300 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let status = self.get_workflow_status(&workflow_id).await?;
            if status == "COMPLETED" || status == "FAILED" {
                return Ok(());
            }
        }
        Err(format!("Workflow {} did not complete within timeout", wf_id))
    }

    async fn start_workflow_raw(&self, wf_id: &str, wf_type: &str) -> Result<String, String> {
        let resp = self
            .http
            .post(format!("{}/api/workflows", self.base_url))
            .json(&serde_json::json!({
                "workflow_id": wf_id,
                "workflow_type": wf_type,
                "task_queue": "bench-queue",
                "input": []
            }))
            .send()
            .await
            .map_err(|e| format!("start_workflow {} failed: {}", wf_id, e))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("start_workflow {} returned {}: {}", wf_id, status, body));
        }

        let result: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| format!("Parse start_workflow response: {}", e))?;

        result["data"]["workflowId"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| "Missing workflowId in response".to_string())
    }

    async fn get_workflow_status(&self, workflow_id: &str) -> Result<String, String> {
        let resp = self
            .http
            .get(format!("{}/api/workflows/{}", self.base_url, workflow_id))
            .send()
            .await
            .map_err(|e| format!("get_workflow {} failed: {}", workflow_id, e))?;

        if !resp.status().is_success() {
            return Err(format!("get_workflow {} returned {}", workflow_id, resp.status()));
        }

        let result: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| format!("Parse get_workflow response: {}", e))?;

        Ok(result["data"]["status"]
            .as_str()
            .unwrap_or("UNKNOWN")
            .to_string())
    }

    async fn signal_workflow(
        &self,
        workflow_id: &str,
        signal_name: &str,
        input: &str,
    ) -> Result<(), String> {
        let resp = self
            .http
            .post(format!("{}/api/workflows/{}/signal", self.base_url, workflow_id))
            .json(&serde_json::json!({
                "signal_name": signal_name,
                "input": input
            }))
            .send()
            .await
            .map_err(|e| format!("signal {} on {} failed: {}", signal_name, workflow_id, e))?;

        if !resp.status().is_success() {
            return Err(format!(
                "signal {} on {} returned {}",
                signal_name,
                workflow_id,
                resp.status()
            ));
        }
        Ok(())
    }

    async fn query_workflow(&self, workflow_id: &str, query_type: &str) -> Result<String, String> {
        let resp = self
            .http
            .post(format!("{}/api/workflows/{}/query", self.base_url, workflow_id))
            .json(&serde_json::json!({
                "query_type": query_type,
                "input": null
            }))
            .send()
            .await
            .map_err(|e| format!("query {} on {} failed: {}", query_type, workflow_id, e))?;

        if !resp.status().is_success() {
            return Err(format!(
                "query {} on {} returned {}",
                query_type,
                workflow_id,
                resp.status()
            ));
        }

        let result: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| format!("Parse query response: {}", e))?;

        Ok(result["data"]["result"].to_string())
    }
}
