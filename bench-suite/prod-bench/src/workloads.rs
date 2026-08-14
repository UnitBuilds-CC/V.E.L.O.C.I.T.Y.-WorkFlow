//! Workload definitions for the production benchmark suite.
//!
//! Each workload maps to equivalent operations across all engines:
//! - Velocity: HTTP POST /api/v1/workflows + signal/query
//! - DBOS:     HTTP POST /bench/invoke + stateful endpoints
//! - Restate:  HTTP POST /invoke/{service}/{handler}

/// Workload kind — determines what operations the workload performs.
#[derive(Debug, Clone)]
pub enum WorkloadKind {
    /// Start workflow → execute → complete. Basic throughput.
    SimpleWorkflow,
    /// Start workflow → send N signals. Signal throughput.
    SignalStorm,
    /// Start workflow → send N queries. Query throughput.
    QueryBurst,
    /// Start workflow → 10 steps → complete. Multi-step overhead.
    HighStep,
    /// Start N concurrent workflows. Concurrency overhead.
    ConcurrentWorkflows,
    /// Parent spawns 10 children. Hierarchy overhead.
    ChildWorkflows,
    /// 5-step saga with compensation. Transaction overhead.
    SagaPattern,
    /// Mixed signals + queries. Realistic mix.
    MixedOperations,
    /// Workflow with search attributes. Visibility overhead.
    SearchAttributes,
    /// Maximum throughput push. Engine ceiling.
    ThroughputCeiling,
    /// Sustained load for tail latency measurement.
    TailLatencySustained,
    /// First workflow after cold start. Startup overhead.
    ColdStart,
    /// Payload roundtrip at various sizes. Serialization overhead.
    PayloadRoundtrip,
}

/// Workload configuration.
#[derive(Debug, Clone)]
pub struct WorkloadConfig {
    pub workflow_count: u64,
    pub concurrency: u64,
    pub timeout_ms: u64,
}

/// A workload definition.
#[derive(Debug, Clone)]
pub struct WorkloadDef {
    pub name: &'static str,
    pub description: &'static str,
    pub kind: WorkloadKind,
    pub config: WorkloadConfig,
}

/// Profile multipliers.
pub struct ProfileConfig {
    pub count_multiplier: f64,
}

pub const PROFILE_QUICK: ProfileConfig = ProfileConfig { count_multiplier: 0.1 };
pub const PROFILE_STANDARD: ProfileConfig = ProfileConfig { count_multiplier: 1.0 };
pub const PROFILE_STRESS: ProfileConfig = ProfileConfig { count_multiplier: 10.0 };

/// All workload definitions.
pub fn all_workloads() -> Vec<WorkloadDef> {
    vec![
        WorkloadDef {
            name: "simple_workflow",
            description: "Start → execute → complete. Measures basic throughput.",
            kind: WorkloadKind::SimpleWorkflow,
            config: WorkloadConfig { workflow_count: 500, concurrency: 10, timeout_ms: 30_000 },
        },
        WorkloadDef {
            name: "signal_storm",
            description: "Start → send 100 signals → complete. Signal throughput.",
            kind: WorkloadKind::SignalStorm,
            config: WorkloadConfig { workflow_count: 100, concurrency: 5, timeout_ms: 30_000 },
        },
        WorkloadDef {
            name: "query_burst",
            description: "Start → send 100 queries → complete. Query throughput.",
            kind: WorkloadKind::QueryBurst,
            config: WorkloadConfig { workflow_count: 100, concurrency: 5, timeout_ms: 30_000 },
        },
        WorkloadDef {
            name: "high_step",
            description: "Single workflow with 10 steps. Step execution overhead.",
            kind: WorkloadKind::HighStep,
            config: WorkloadConfig { workflow_count: 200, concurrency: 5, timeout_ms: 30_000 },
        },
        WorkloadDef {
            name: "concurrent_100",
            description: "100 concurrent workflows. Concurrency scheduling overhead.",
            kind: WorkloadKind::ConcurrentWorkflows,
            config: WorkloadConfig { workflow_count: 500, concurrency: 100, timeout_ms: 60_000 },
        },
        WorkloadDef {
            name: "mixed_operations",
            description: "Mixed starts, signals, and queries. Realistic workload.",
            kind: WorkloadKind::MixedOperations,
            config: WorkloadConfig { workflow_count: 300, concurrency: 10, timeout_ms: 30_000 },
        },
        WorkloadDef {
            name: "search_attributes",
            description: "Start with attributes → query by attributes. Visibility.",
            kind: WorkloadKind::SearchAttributes,
            config: WorkloadConfig { workflow_count: 200, concurrency: 5, timeout_ms: 30_000 },
        },
        WorkloadDef {
            name: "throughput_ceiling",
            description: "Maximum sustainable throughput. Push engine to limits.",
            kind: WorkloadKind::ThroughputCeiling,
            config: WorkloadConfig { workflow_count: 5000, concurrency: 50, timeout_ms: 120_000 },
        },
        WorkloadDef {
            name: "tail_latency",
            description: "Sustained load at high concurrency. p99/p999 stability.",
            kind: WorkloadKind::TailLatencySustained,
            config: WorkloadConfig { workflow_count: 2000, concurrency: 20, timeout_ms: 120_000 },
        },
        WorkloadDef {
            name: "cold_start",
            description: "First workflow after engine startup. Cold start latency.",
            kind: WorkloadKind::ColdStart,
            config: WorkloadConfig { workflow_count: 10, concurrency: 1, timeout_ms: 30_000 },
        },
        WorkloadDef {
            name: "payload_1kb",
            description: "1KB payloads. Serialization overhead at typical size.",
            kind: WorkloadKind::PayloadRoundtrip,
            config: WorkloadConfig { workflow_count: 500, concurrency: 10, timeout_ms: 30_000 },
        },
    ]
}
