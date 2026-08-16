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

        // Health check — invoke the 'invoke' handler on the 'bench' keyed object
        let url = format!("{}/bench/bench_health/invoke", base_url);
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

        // The Restate bench service is a keyed object named "bench".
        // URL pattern: /bench/{key}/{handler}
        // Each workload maps to the appropriate handler.
        let key = format!("wf_{}", workload_name);

        match kind {
            WorkloadKind::SimpleWorkflow
            | WorkloadKind::ThroughputCeiling
            | WorkloadKind::TailLatencySustained
            | WorkloadKind::ChildWorkflows
            | WorkloadKind::SagaPattern => {
                self.invoke_keyed_handler(&key, "simple").await?;
            }

            WorkloadKind::HighStep => {
                self.invoke_keyed_handler(&key, "multiStep").await?;
            }

            WorkloadKind::ConcurrentWorkflows => {
                self.invoke_keyed_handler(&key, "invoke").await?;
            }

            WorkloadKind::SignalStorm => {
                self.invoke_keyed_handler(&key, "signalStorm").await?;
            }

            WorkloadKind::QueryBurst => {
                // Multiple stateful reads/writes on the same key
                for _ in 0..10 {
                    self.invoke_keyed_handler(&key, "invoke").await?;
                }
            }

            WorkloadKind::MixedOperations => {
                self.invoke_keyed_handler(&key, "durablePromise").await?;
            }

            WorkloadKind::SearchAttributes => {
                self.invoke_keyed_handler(&key, "echo").await?;
            }

            WorkloadKind::ColdStart => {
                self.invoke_keyed_handler(&key, "coldStart").await?;
            }

            WorkloadKind::PayloadRoundtrip => {
                self.invoke_keyed_handler(&key, "payload").await?;
            }
        }

        Ok(start.elapsed().as_micros() as f64)
    }

    async fn invoke_keyed_handler(&self, key: &str, handler: &str) -> Result<(), String> {
        let url = format!("{}/bench/{}/{}", self.base_url, key, handler);
        let body = serde_json::json!({"input": key});

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
            return Err(format!("Restate bench/{}/{} failed: {} — {}", key, handler, status, body));
        }
        Ok(())
    }
}
