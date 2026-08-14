//! DBOS production bench client — talks to a real DBOS server via HTTP.
//!
//! DBOS uses PostgreSQL for durable execution. Each HTTP request triggers
//! a real database transaction, measuring the full cost of durable execution.

use reqwest::Client;
use std::time::Instant;

use crate::workloads::WorkloadKind;

pub struct DbosClient {
    client: Client,
    base_url: String,
}

impl DbosClient {
    pub async fn new(base_url: &str) -> Result<Self, String> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(120))
            .pool_max_idle_per_host(50)
            .build()
            .map_err(|e| format!("HTTP client error: {}", e))?;

        // Health check — DBOS uses /bench/invoke as the basic endpoint
        let url = format!("{}/bench/echo", base_url);
        let resp = client
            .post(&url)
            .body("health")
            .header("Content-Type", "application/json")
            .send()
            .await
            .map_err(|e| format!("DBOS health check failed: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("DBOS echo endpoint returned {}", resp.status()));
        }

        tracing::info!("Connected to DBOS at {}", base_url);
        Ok(Self { client, base_url: base_url.trim_end_matches('/').to_string() })
    }

    /// Run a workload operation. Returns latency in microseconds.
    pub async fn run_workload(
        &self,
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
                self.invoke_handler(workload_name).await?;
            }

            WorkloadKind::SignalStorm => {
                // DBOS doesn't have signals — use invoke as equivalent
                for _ in 0..10 {
                    self.invoke_handler("signal_equiv").await?;
                }
            }

            WorkloadKind::QueryBurst => {
                for _ in 0..10 {
                    self.invoke_handler("query_equiv").await?;
                }
            }

            WorkloadKind::MixedOperations => {
                self.invoke_handler("mixed").await?;
            }

            WorkloadKind::SearchAttributes => {
                self.invoke_handler("with_attrs").await?;
            }

            WorkloadKind::PayloadRoundtrip => {
                self.payload_roundtrip(1024).await?;
            }
        }

        Ok(start.elapsed().as_micros() as f64)
    }

    async fn invoke_handler(&self, handler_name: &str) -> Result<(), String> {
        let url = format!("{}/bench/invoke", self.base_url);
        let resp = self
            .client
            .post(&url)
            .body(format!(r#"{{"handler":"{}"}}"#, handler_name))
            .header("Content-Type", "application/json")
            .send()
            .await
            .map_err(|e| format!("DBOS invoke HTTP error: {}", e))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("DBOS invoke {} failed: {} — {}", handler_name, status, body));
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
            .map_err(|e| format!("DBOS payload HTTP error: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("DBOS payload failed: {}", resp.status()));
        }
        Ok(())
    }
}
