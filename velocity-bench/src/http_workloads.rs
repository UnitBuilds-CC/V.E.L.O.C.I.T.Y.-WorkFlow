//! HTTP-specific workload definitions for Velocity Runtime vs Restate.
//!
//! These workloads measure handler invocation throughput, stateful operations,
//! concurrent handler performance, and payload handling — the core operations
//! that both Velocity Runtime and Restate expose via HTTP.

use serde::{Deserialize, Serialize};

// ─── HTTP Workload Kind ─────────────────────────────────────────────────────

/// Kinds of HTTP workloads for the Runtime flavor benchmark.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HttpWorkloadKind {
    /// Single handler invocation throughput (1000 sequential calls).
    HandlerInvocation,
    /// Stateful handler: read state → mutate → write state (100 calls).
    StatefulHandler,
    /// 100 concurrent handler invocations (measures scheduling overhead).
    ConcurrentHandlers,
    /// Payload roundtrip at various sizes (1KB, 10KB, 100KB, 1MB).
    PayloadRoundtrip,
    /// Sustained load at high concurrency for 30 seconds.
    SustainedLoad,
    /// Mixed workload: 70% reads, 20% writes, 10% deletes.
    MixedOperations,
    /// Cold start: first handler invocation after idle period.
    ColdStart,
    /// Handler with durable promise (suspend/resume pattern).
    DurablePromise,
}

impl std::fmt::Display for HttpWorkloadKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HttpWorkloadKind::HandlerInvocation => write!(f, "handler_invocation"),
            HttpWorkloadKind::StatefulHandler => write!(f, "stateful_handler"),
            HttpWorkloadKind::ConcurrentHandlers => write!(f, "concurrent_handlers"),
            HttpWorkloadKind::PayloadRoundtrip => write!(f, "payload_roundtrip"),
            HttpWorkloadKind::SustainedLoad => write!(f, "sustained_load"),
            HttpWorkloadKind::MixedOperations => write!(f, "mixed_operations"),
            HttpWorkloadKind::ColdStart => write!(f, "cold_start"),
            HttpWorkloadKind::DurablePromise => write!(f, "durable_promise"),
        }
    }
}

// ─── HTTP Workload Definition ───────────────────────────────────────────────

/// Definition of an HTTP workload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpWorkloadDefinition {
    pub kind: HttpWorkloadKind,
    pub name: String,
    pub description: String,
    /// Number of operations per run.
    pub operation_count: u64,
    /// Concurrency level (parallel requests).
    pub concurrency: u32,
    /// Duration for sustained workloads (seconds). 0 = use operation_count.
    pub duration_secs: u64,
    /// Payload size in bytes (for payload roundtrip).
    pub payload_size: usize,
    /// Service name to invoke.
    pub service: String,
    /// Handler name to invoke.
    pub handler: String,
    /// Service name for keyed (stateful) operations. Falls back to `service` if empty.
    /// Needed when the engine routes keyed handlers on a different service (e.g. Restate).
    pub keyed_service: String,
}

impl HttpWorkloadDefinition {
    /// All HTTP workloads for the Runtime flavor benchmark.
    pub fn all() -> Vec<Self> {
        vec![
            Self {
                kind: HttpWorkloadKind::HandlerInvocation,
                name: "handler_invocation".into(),
                description: "1000 sequential handler calls. Measures basic HTTP throughput and latency.".into(),
                operation_count: 1000,
                concurrency: 1,
                duration_secs: 0,
                payload_size: 64,
                service: "bench".into(),
                handler: "invoke".into(),
                keyed_service: "bench".into(),
            },
            Self {
                kind: HttpWorkloadKind::StatefulHandler,
                name: "stateful_handler".into(),
                description: "100 keyed handler calls with state read/write per call. Measures stateful durable execution.".into(),
                operation_count: 100,
                concurrency: 1,
                duration_secs: 0,
                payload_size: 128,
                service: "keyed_bench".into(),
                handler: "stateful".into(),
                keyed_service: "keyed_bench".into(),
            },
            Self {
                kind: HttpWorkloadKind::ConcurrentHandlers,
                name: "concurrent_handlers".into(),
                description: "100 concurrent handler invocations. Measures concurrent scheduling overhead.".into(),
                operation_count: 100,
                concurrency: 100,
                duration_secs: 0,
                payload_size: 64,
                service: "bench".into(),
                handler: "invoke".into(),
                keyed_service: "bench".into(),
            },
            Self {
                kind: HttpWorkloadKind::PayloadRoundtrip,
                name: "payload_roundtrip".into(),
                description: "Handler calls with 1KB payloads. Measures serialization overhead at typical size.".into(),
                operation_count: 500,
                concurrency: 10,
                duration_secs: 0,
                payload_size: 1024,
                service: "bench".into(),
                handler: "echo".into(),
                keyed_service: "bench".into(),
            },
            Self {
                kind: HttpWorkloadKind::SustainedLoad,
                name: "sustained_load".into(),
                description: "30s sustained load at concurrency 50. Measures throughput stability and tail latency.".into(),
                operation_count: 0,
                concurrency: 50,
                duration_secs: 30,
                payload_size: 64,
                service: "bench".into(),
                handler: "invoke".into(),
                keyed_service: "bench".into(),
            },
            Self {
                kind: HttpWorkloadKind::MixedOperations,
                name: "mixed_operations".into(),
                description: "500 mixed calls: 70% invoke, 20% stateful, 10% echo. Measures realistic workload mix.".into(),
                operation_count: 500,
                concurrency: 10,
                duration_secs: 0,
                payload_size: 128,
                service: "bench".into(),
                handler: "invoke".into(),
                keyed_service: "keyed_bench".into(),
            },
            Self {
                kind: HttpWorkloadKind::ColdStart,
                name: "cold_start".into(),
                description: "First 10 handler invocations after 5s idle. Measures cold start latency.".into(),
                operation_count: 10,
                concurrency: 1,
                duration_secs: 0,
                payload_size: 64,
                service: "bench".into(),
                handler: "invoke".into(),
                keyed_service: "bench".into(),
            },
            Self {
                kind: HttpWorkloadKind::DurablePromise,
                name: "durable_promise".into(),
                description: "50 handler calls that create and resolve durable promises. Measures suspend/resume overhead.".into(),
                operation_count: 50,
                concurrency: 5,
                duration_secs: 0,
                payload_size: 64,
                service: "bench".into(),
                handler: "durablePromise".into(),
                keyed_service: "bench".into(),
            },
        ]
    }

    /// Smoke workloads (quick validation subset).
    pub fn smoke() -> Vec<Self> {
        Self::all()
            .into_iter()
            .filter(|w| {
                matches!(
                    w.kind,
                    HttpWorkloadKind::HandlerInvocation
                        | HttpWorkloadKind::ConcurrentHandlers
                        | HttpWorkloadKind::SustainedLoad
                )
            })
            .collect()
    }
}
