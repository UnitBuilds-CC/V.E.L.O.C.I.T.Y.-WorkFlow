//! Restate production bench client — talks to a real Restate server via HTTP.
//!
//! Restate uses its durable execution model with log-structured storage.
//! Each invocation is durably persisted.

use reqwest::Client;
use std::time::Instant;

use crate::workloads::WorkloadKind;

pub struct RestateClient {
    client: Client,
    base_url: String,
}

impl RestateClient {
    pub async fn new(base_url: &str) -> Result<Self, String> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(120))
            .pool_max_idle_per_host(50)
            .build()
            .map_err(|e| format!("HTTP client error: {}", e))?;

        // Health check — Restate exposes discovery at /discover and handlers via /invoke
        let url = format!("{}/BenchmarkService/handler_invocation", base_url);
        let resp = client
            .post(&url)
            .body(r#""health""#)
            .header("Content-Type", "application/json")
            .send()
            .await;

        // Restate may return 400 if the service isn't registered yet — that's OK
        // as long as the server is reachable
        match resp {
            Ok(_) => {
                tracing::info!("Connected to Restate at {}", base_url);
            }
            Err(e) => {
                return Err(format!("Restate connection failed: {}", e));
            }
        }

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
                self.invoke_handler("handler_invocation", workload_name).await?;
            }

            WorkloadKind::SignalStorm => {
                // Restate equivalent: multiple handler invocations
                for _ in 0..10 {
                    self.invoke_handler("handler_invocation", "signal_equiv").await?;
                }
            }

            WorkloadKind::QueryBurst => {
                for _ in 0..10 {
                    self.invoke_handler("handler_invocation", "query_equiv").await?;
                }
            }

            WorkloadKind::MixedOperations => {
                self.invoke_handler("mixed_operations", workload_name).await?;
            }

            WorkloadKind::SearchAttributes => {
                self.invoke_handler("handler_invocation", "with_attrs").await?;
            }

            WorkloadKind::PayloadRoundtrip => {
                self.invoke_handler("payload_roundtrip", "1kb").await?;
            }
        }

        Ok(start.elapsed().as_micros() as f64)
    }

    async fn invoke_handler(&self, handler: &str, input: &str) -> Result<(), String> {
        let url = format!("{}/BenchmarkService/{}", self.base_url, handler);
        let body = serde_json::json!({"input": input});

        let resp = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Restate invoke HTTP error: {}", e))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("Restate {} failed: {} — {}", handler, status, body));
        }
        Ok(())
    }
}
