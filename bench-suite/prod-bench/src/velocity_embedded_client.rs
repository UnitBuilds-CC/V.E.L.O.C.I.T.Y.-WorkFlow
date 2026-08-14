//! Velocity Embedded bench client — HTTP API to the PostgreSQL-backed embedded server.
//!
//! This talks to velocity-embedded-server which wraps the EmbeddedEngine
//! with an HTTP API. Every workflow goes through PostgreSQL (real persistence).

use std::time::Instant;

use crate::workloads::WorkloadKind;

pub struct VelocityEmbeddedClient {
    base_url: String,
    http: reqwest::Client,
}

impl VelocityEmbeddedClient {
    pub async fn new(base_url: &str) -> Result<Self, String> {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(120))
            .pool_max_idle_per_host(200)
            .build()
            .map_err(|e| format!("Build HTTP client: {}", e))?;

        // Health check
        let resp = http
            .get(format!("{}/health", base_url))
            .send()
            .await
            .map_err(|e| format!("Velocity Embedded health check failed: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!(
                "Velocity Embedded health returned status {}",
                resp.status()
            ));
        }

        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| format!("Parse health: {}", e))?;

        tracing::info!(
            "Connected to Velocity Embedded (PostgreSQL-backed) at {} — persistence={}",
            base_url,
            body["persistence"].as_str().unwrap_or("unknown")
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
        _kind: &WorkloadKind,
    ) -> Result<f64, String> {
        let start = Instant::now();

        // Start workflow (embedded engine executes inline and returns immediately)
        let resp = self
            .http
            .post(format!("{}/api/v1/workflows", self.base_url))
            .json(&serde_json::json!({
                "workflowId": wf_id,
                "workflowType": workload_name,
                "input": {"bench": true}
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

        let status = result["status"].as_str().unwrap_or("UNKNOWN");
        if status != "COMPLETED" {
            return Err(format!(
                "Workflow {} ended with status {} (expected COMPLETED)",
                wf_id, status
            ));
        }

        Ok(start.elapsed().as_micros() as f64)
    }

    pub async fn reset(&self) -> Result<(), String> {
        // No reset endpoint on embedded server — workflows are independent
        Ok(())
    }
}
