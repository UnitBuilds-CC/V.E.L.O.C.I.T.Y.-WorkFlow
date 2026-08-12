//! Canonical workload definitions for side-by-side comparison.
//!
//! Each workload is defined once and executed identically on both engines.
//! This ensures fair comparison — the only variable is the engine itself.

use serde::{Deserialize, Serialize};

// ─── Workload Kind ───────────────────────────────────────────────────────────

/// The type of workload to run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkloadKind {
    /// Simple workflow: start → execute N steps → complete.
    SimpleWorkflow,
    /// Signal storm: start workflow → send N signals → complete.
    SignalStorm,
    /// Query burst: start workflow → send N queries → complete.
    QueryBurst,
    /// High-step workflow: single workflow with many steps (1K+).
    HighStepWorkflow,
    /// Concurrent workflows: start N workflows simultaneously.
    ConcurrentWorkflows,
    /// Child workflows: parent spawns N children, waits for all.
    ChildWorkflows,
    /// Saga pattern: multi-step with compensation on failure.
    SagaPattern,
    /// Timer workflow: start → sleep → signal → complete.
    TimerWorkflow,
    /// Search attributes: start with attributes → query by attributes.
    SearchAttributes,
    /// Signal + query interleaved: mix of signals and queries.
    SignalQueryMix,
    /// Batch operations: start/terminate/query many workflows.
    BatchOperations,
    /// Long-running workflow: runs for extended duration.
    LongRunningWorkflow,
    /// Crash recovery: start workflows → kill engine → restart → verify.
    CrashRecovery,
    /// Payload size test: vary payload sizes from 1B to 10MB.
    PayloadSizeTest,
    /// Namespace isolation: workflows in separate namespaces.
    NamespaceIsolation,
    /// Throughput ceiling: max workflows/sec the engine can sustain.
    ThroughputCeiling,
    /// Memory scaling: measure memory at 1K, 10K, 100K, 1M workflows.
    MemoryScaling,
    /// Cold start: first workflow execution after engine startup.
    ColdStart,
    /// Replay amplification: measures signal latency vs history length.
    /// Exposes O(1) vs O(n) replay cost — Velocity's key architectural advantage.
    ReplayAmplification,
    /// WAL durability: measures throughput with fsync on vs off.
    /// Shows group commit amortization efficiency.
    WalDurability,
    /// Tail latency under sustained load: p99/p999 at 80% max throughput.
    /// Measures latency stability over extended duration.
    TailLatencySustained,
}

impl std::fmt::Display for WorkloadKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkloadKind::SimpleWorkflow => write!(f, "simple_workflow"),
            WorkloadKind::SignalStorm => write!(f, "signal_storm"),
            WorkloadKind::QueryBurst => write!(f, "query_burst"),
            WorkloadKind::HighStepWorkflow => write!(f, "high_step_workflow"),
            WorkloadKind::ConcurrentWorkflows => write!(f, "concurrent_workflows"),
            WorkloadKind::ChildWorkflows => write!(f, "child_workflows"),
            WorkloadKind::SagaPattern => write!(f, "saga_pattern"),
            WorkloadKind::TimerWorkflow => write!(f, "timer_workflow"),
            WorkloadKind::SearchAttributes => write!(f, "search_attributes"),
            WorkloadKind::SignalQueryMix => write!(f, "signal_query_mix"),
            WorkloadKind::BatchOperations => write!(f, "batch_operations"),
            WorkloadKind::LongRunningWorkflow => write!(f, "long_running"),
            WorkloadKind::CrashRecovery => write!(f, "crash_recovery"),
            WorkloadKind::PayloadSizeTest => write!(f, "payload_size"),
            WorkloadKind::NamespaceIsolation => write!(f, "namespace_isolation"),
            WorkloadKind::ThroughputCeiling => write!(f, "throughput_ceiling"),
            WorkloadKind::MemoryScaling => write!(f, "memory_scaling"),
            WorkloadKind::ColdStart => write!(f, "cold_start"),
            WorkloadKind::ReplayAmplification => write!(f, "replay_amplification"),
            WorkloadKind::WalDurability => write!(f, "wal_durability"),
            WorkloadKind::TailLatencySustained => write!(f, "tail_latency_sustained"),
        }
    }
}

// ─── Workload Configuration ──────────────────────────────────────────────────

/// Configuration parameters for a workload run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadConfig {
    /// Number of workflows to create (for concurrent workloads).
    pub workflow_count: u64,
    /// Number of steps per workflow.
    pub steps_per_workflow: u64,
    /// Number of signals per workflow (for signal workloads).
    pub signals_per_workflow: u64,
    /// Number of queries per workflow (for query workloads).
    pub queries_per_workflow: u64,
    /// Number of child workflows per parent.
    pub children_per_parent: u64,
    /// Payload size in bytes.
    pub payload_size_bytes: u64,
    /// Number of namespaces (for isolation tests).
    pub namespace_count: u64,
    /// Duration in seconds (for sustained workloads).
    pub duration_secs: u64,
    /// Timeout per operation in milliseconds.
    pub timeout_ms: u64,
    /// Concurrency level (parallel operations).
    pub concurrency: u32,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            workflow_count: 100,
            steps_per_workflow: 10,
            signals_per_workflow: 10,
            queries_per_workflow: 10,
            children_per_parent: 5,
            payload_size_bytes: 1024,
            namespace_count: 3,
            duration_secs: 30,
            timeout_ms: 30_000,
            concurrency: 10,
        }
    }
}

impl WorkloadConfig {
    pub fn quick() -> Self {
        Self {
            workflow_count: 10,
            steps_per_workflow: 5,
            signals_per_workflow: 5,
            queries_per_workflow: 5,
            children_per_parent: 3,
            payload_size_bytes: 256,
            namespace_count: 2,
            duration_secs: 5,
            timeout_ms: 10_000,
            concurrency: 4,
        }
    }

    pub fn standard() -> Self {
        Self::default()
    }

    pub fn stress() -> Self {
        Self {
            workflow_count: 10_000,
            steps_per_workflow: 100,
            signals_per_workflow: 100,
            queries_per_workflow: 100,
            children_per_parent: 10,
            payload_size_bytes: 4096,
            namespace_count: 5,
            duration_secs: 120,
            timeout_ms: 60_000,
            concurrency: 100,
        }
    }
}

// ─── Workload Definition ─────────────────────────────────────────────────────

/// A complete workload definition that can be executed on any engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadDefinition {
    /// Human-readable name.
    pub name: String,
    /// The kind of workload.
    pub kind: WorkloadKind,
    /// Configuration parameters.
    pub config: WorkloadConfig,
    /// Description of what this workload measures.
    pub description: String,
    /// Which metrics are most important for this workload.
    pub primary_metrics: Vec<String>,
}

impl WorkloadDefinition {
    /// Returns the canonical set of all workload definitions.
    pub fn all() -> Vec<WorkloadDefinition> {
        vec![
            WorkloadDefinition {
                name: "simple_workflow".into(),
                kind: WorkloadKind::SimpleWorkflow,
                config: WorkloadConfig {
                    workflow_count: 1000,
                    steps_per_workflow: 10,
                    ..WorkloadConfig::default()
                },
                description:
                    "Start → execute 10 steps → complete. Measures basic throughput and latency."
                        .into(),
                primary_metrics: vec!["ops/sec".into(), "p50_latency".into(), "p99_latency".into()],
            },
            WorkloadDefinition {
                name: "signal_storm".into(),
                kind: WorkloadKind::SignalStorm,
                config: WorkloadConfig {
                    workflow_count: 100,
                    signals_per_workflow: 100,
                    ..WorkloadConfig::default()
                },
                description:
                    "Start workflow → send 100 signals → complete. Measures signal throughput."
                        .into(),
                primary_metrics: vec!["signals/sec".into(), "p99_signal_latency".into()],
            },
            WorkloadDefinition {
                name: "query_burst".into(),
                kind: WorkloadKind::QueryBurst,
                config: WorkloadConfig {
                    workflow_count: 100,
                    queries_per_workflow: 100,
                    ..WorkloadConfig::default()
                },
                description:
                    "Start workflow → send 100 queries → complete. Measures query throughput."
                        .into(),
                primary_metrics: vec!["queries/sec".into(), "p99_query_latency".into()],
            },
            WorkloadDefinition {
                name: "high_step".into(),
                kind: WorkloadKind::HighStepWorkflow,
                config: WorkloadConfig {
                    workflow_count: 10,
                    steps_per_workflow: 10_000,
                    ..WorkloadConfig::default()
                },
                description: "Single workflow with 10K steps. Measures step execution overhead."
                    .into(),
                primary_metrics: vec![
                    "steps/sec".into(),
                    "total_duration".into(),
                    "memory_mb".into(),
                ],
            },
            WorkloadDefinition {
                name: "concurrent_1k".into(),
                kind: WorkloadKind::ConcurrentWorkflows,
                config: WorkloadConfig {
                    workflow_count: 1000,
                    concurrency: 100,
                    ..WorkloadConfig::default()
                },
                description: "1000 concurrent workflows. Measures concurrent scheduling overhead."
                    .into(),
                primary_metrics: vec!["ops/sec".into(), "p99_latency".into(), "memory_mb".into()],
            },
            WorkloadDefinition {
                name: "child_workflows".into(),
                kind: WorkloadKind::ChildWorkflows,
                config: WorkloadConfig {
                    workflow_count: 100,
                    children_per_parent: 10,
                    ..WorkloadConfig::default()
                },
                description:
                    "Parent spawns 10 children, waits for all. Measures hierarchy overhead.".into(),
                primary_metrics: vec!["ops/sec".into(), "p99_latency".into()],
            },
            WorkloadDefinition {
                name: "saga_pattern".into(),
                kind: WorkloadKind::SagaPattern,
                config: WorkloadConfig {
                    workflow_count: 100,
                    steps_per_workflow: 5,
                    ..WorkloadConfig::default()
                },
                description: "5-step saga with compensation. Measures transaction overhead.".into(),
                primary_metrics: vec!["ops/sec".into(), "p99_latency".into(), "error_rate".into()],
            },
            WorkloadDefinition {
                name: "timer_workflow".into(),
                kind: WorkloadKind::TimerWorkflow,
                config: WorkloadConfig {
                    workflow_count: 100,
                    ..WorkloadConfig::default()
                },
                description: "Workflow with timer (sleep). Measures timer scheduling accuracy."
                    .into(),
                primary_metrics: vec!["timer_accuracy_ms".into(), "p99_latency".into()],
            },
            WorkloadDefinition {
                name: "search_attributes".into(),
                kind: WorkloadKind::SearchAttributes,
                config: WorkloadConfig {
                    workflow_count: 1000,
                    ..WorkloadConfig::default()
                },
                description:
                    "Start with attributes → query by attributes. Measures visibility performance."
                        .into(),
                primary_metrics: vec!["query_latency".into(), "index_throughput".into()],
            },
            WorkloadDefinition {
                name: "signal_query_mix".into(),
                kind: WorkloadKind::SignalQueryMix,
                config: WorkloadConfig {
                    workflow_count: 100,
                    signals_per_workflow: 50,
                    queries_per_workflow: 50,
                    ..WorkloadConfig::default()
                },
                description:
                    "Interleaved signals and queries. Measures mixed workload performance.".into(),
                primary_metrics: vec!["ops/sec".into(), "p99_latency".into()],
            },
            WorkloadDefinition {
                name: "batch_operations".into(),
                kind: WorkloadKind::BatchOperations,
                config: WorkloadConfig {
                    workflow_count: 5000,
                    ..WorkloadConfig::default()
                },
                description:
                    "Batch start/terminate/query 5000 workflows. Measures admin throughput.".into(),
                primary_metrics: vec!["ops/sec".into(), "p99_latency".into()],
            },
            WorkloadDefinition {
                name: "payload_1kb".into(),
                kind: WorkloadKind::PayloadSizeTest,
                config: WorkloadConfig {
                    workflow_count: 1000,
                    payload_size_bytes: 1024,
                    ..WorkloadConfig::default()
                },
                description: "1KB payloads. Measures serialization overhead at typical size."
                    .into(),
                primary_metrics: vec!["ops/sec".into(), "throughput_mb_sec".into()],
            },
            WorkloadDefinition {
                name: "payload_1mb".into(),
                kind: WorkloadKind::PayloadSizeTest,
                config: WorkloadConfig {
                    workflow_count: 100,
                    payload_size_bytes: 1_048_576,
                    ..WorkloadConfig::default()
                },
                description: "1MB payloads. Measures large payload handling.".into(),
                primary_metrics: vec![
                    "ops/sec".into(),
                    "throughput_mb_sec".into(),
                    "memory_mb".into(),
                ],
            },
            WorkloadDefinition {
                name: "namespace_isolation".into(),
                kind: WorkloadKind::NamespaceIsolation,
                config: WorkloadConfig {
                    workflow_count: 500,
                    namespace_count: 5,
                    ..WorkloadConfig::default()
                },
                description: "Workflows across 5 namespaces. Measures isolation overhead.".into(),
                primary_metrics: vec!["ops/sec".into(), "p99_latency".into()],
            },
            WorkloadDefinition {
                name: "throughput_ceiling".into(),
                kind: WorkloadKind::ThroughputCeiling,
                config: WorkloadConfig {
                    workflow_count: 100_000,
                    duration_secs: 60,
                    concurrency: 1000,
                    ..WorkloadConfig::default()
                },
                description: "Maximum sustainable throughput. Pushes engine to its limits.".into(),
                primary_metrics: vec!["peak_ops_sec".into(), "sustained_ops_sec".into()],
            },
            WorkloadDefinition {
                name: "memory_scaling".into(),
                kind: WorkloadKind::MemoryScaling,
                config: WorkloadConfig {
                    workflow_count: 100_000,
                    ..WorkloadConfig::default()
                },
                description: "Measure memory at 1K, 10K, 100K active workflows.".into(),
                primary_metrics: vec!["memory_per_workflow_kb".into(), "peak_memory_mb".into()],
            },
            WorkloadDefinition {
                name: "cold_start".into(),
                kind: WorkloadKind::ColdStart,
                config: WorkloadConfig {
                    workflow_count: 1,
                    ..WorkloadConfig::default()
                },
                description: "First workflow after engine startup. Measures cold start latency."
                    .into(),
                primary_metrics: vec!["cold_start_ms".into()],
            },
            WorkloadDefinition {
                name: "crash_recovery".into(),
                kind: WorkloadKind::CrashRecovery,
                config: WorkloadConfig {
                    workflow_count: 100,
                    ..WorkloadConfig::default()
                },
                description: "Start workflows → simulate crash → restart → verify recovery.".into(),
                primary_metrics: vec!["recovery_time_ms".into(), "data_loss_count".into()],
            },
            // ─── Differentiator Workloads ─────────────────────────────────────────
            // These expose Velocity's architectural advantages over event-sourced engines.
            WorkloadDefinition {
                name: "replay_amplification".into(),
                kind: WorkloadKind::ReplayAmplification,
                config: WorkloadConfig {
                    workflow_count: 100,
                    signals_per_workflow: 1000,
                    ..WorkloadConfig::default()
                },
                description: "Signal a workflow 1000 times. Measures how signal latency scales \
                    with history length. Event-sourced engines (Temporal) replay the full event \
                    log on each signal — O(n²) total. Velocity uses direct mutation — O(n) total. \
                    The curve should be flat for Velocity and steeply rising for Temporal."
                    .into(),
                primary_metrics: vec![
                    "signal_p50_us".into(),
                    "signal_p99_us".into(),
                    "replay_amplification_factor".into(),
                ],
            },
            WorkloadDefinition {
                name: "wal_durability".into(),
                kind: WorkloadKind::WalDurability,
                config: WorkloadConfig {
                    workflow_count: 5000,
                    concurrency: 50,
                    ..WorkloadConfig::default()
                },
                description: "High-throughput workflow creation with WAL fsync enabled. \
                    Measures how much throughput the durability guarantee costs. \
                    Velocity's group commit amortizes fsync across many workflows."
                    .into(),
                primary_metrics: vec![
                    "ops/sec".into(),
                    "durability_overhead_pct".into(),
                    "p99_latency".into(),
                ],
            },
            WorkloadDefinition {
                name: "tail_latency_sustained".into(),
                kind: WorkloadKind::TailLatencySustained,
                config: WorkloadConfig {
                    workflow_count: 50000,
                    duration_secs: 120,
                    concurrency: 100,
                    ..WorkloadConfig::default()
                },
                description: "Sustained load at high concurrency for 2 minutes. \
                    Measures p99/p999 tail latency stability. Shows whether the engine \
                    maintains consistent latency or degrades under prolonged pressure."
                    .into(),
                primary_metrics: vec![
                    "sustained_ops/sec".into(),
                    "p99_latency".into(),
                    "p999_latency".into(),
                    "latency_stability_ratio".into(),
                ],
            },
        ]
    }

    /// Returns a quick subset for smoke testing.
    pub fn smoke_test() -> Vec<WorkloadDefinition> {
        let all = Self::all();
        all.into_iter()
            .filter(|w| {
                matches!(
                    w.kind,
                    WorkloadKind::SimpleWorkflow
                        | WorkloadKind::SignalStorm
                        | WorkloadKind::ColdStart
                )
            })
            .map(|mut w| {
                w.config = WorkloadConfig::quick();
                w
            })
            .collect()
    }
}
