//! Temporal production bench client — talks to a real Temporal server via HTTP.
//!
//! Temporal uses PostgreSQL for durable execution. Each HTTP request triggers
//! a real Temporal workflow via the Task Queue, measuring the full cost of
//! durable execution with the Temporal scheduler.

use reqwest::Client;
use std::time::Instant;

use crate::workloads::WorkloadKind;

pub struct TemporalClient {
    client: Client,
    base_url: String,
}

impl TemporalClient {
    pub async fn new(base_url: &str) -> Result<Self, String> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(120))
            .pool_max_idle_per_host(50)
            .build()
            .map_err(|e| format!("HTTP client error: {}", e))?;

        // Health check — use /health endpoint
        let url = format!("{}/health", base_url);
        let resp = client
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("Temporal health check failed: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("Temporal health check returned {}", resp.status()));
        }

        tracing::info!("Connected to Temporal at {}", base_url);
        Ok(Self { client, base_url: base_url.trim_end_matches('/').to_string() })
    }

    /// Run a workload operation. Returns latency in microseconds.
    pub async fn run_workload(
        &self,
        workload_name: &str,
        kind: &WorkloadKind,
    ) -> Result<f64, String> {
        let start = Instant::now();

        // Map each workload kind to the correct Temporal service endpoint.
        // The Temporal service exposes specific endpoints per workload type,
        // with Temporal-specific endpoints for activity scheduling and visibility.
        let endpoint = match kind {
            WorkloadKind::SimpleWorkflow
            | WorkloadKind::ThroughputCeiling
            | WorkloadKind::TailLatencySustained
            | WorkloadKind::ChildWorkflows
            | WorkloadKind::SagaPattern => "/bench/simple_workflow",

            WorkloadKind::HighStep => "/bench/multi_step",

            WorkloadKind::ConcurrentWorkflows => "/bench/concurrent",

            WorkloadKind::SignalStorm => "/bench/signal_storm",

            // Temporal uses activity_scheduling for query-like workloads
            WorkloadKind::QueryBurst => "/bench/activity_scheduling",

            WorkloadKind::MixedOperations => "/bench/stateful",

            // Temporal uses durable_promise for visibility-like workloads
            WorkloadKind::SearchAttributes => "/bench/durable_promise",

            WorkloadKind::ColdStart => "/bench/cold_start",

            WorkloadKind::PayloadRoundtrip => "/bench/payload",
        };

        match kind {
            WorkloadKind::PayloadRoundtrip => {
                self.payload_roundtrip(1024).await?;
            }
            _ => {
                self.invoke_endpoint(endpoint).await?;
            }
        }

        Ok(start.elapsed().as_micros() as f64)
    }

    async fn invoke_endpoint(&self, endpoint: &str) -> Result<(), String> {
        let url = format!("{}{}", self.base_url, endpoint);
        let resp = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .body("{}")
            .send()
            .await
            .map_err(|e| format!("Temporal invoke HTTP error: {}", e))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("Temporal {} failed: {} — {}", endpoint, status, body));
        }
        Ok(())
    }

    async fn payload_roundtrip(&self, size: usize) -> Result<(), String> {
        let url = format!("{}/bench/payload", self.base_url);
        let payload = "x".repeat(size);
        let resp = self
            .client
            .post(&url)
            .body(payload)
            .header("Content-Type", "application/json")
            .send()
            .await
            .map_err(|e| format!("Temporal payload HTTP error: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("Temporal payload failed: {}", resp.status()));
        }
        Ok(())
    }
}
