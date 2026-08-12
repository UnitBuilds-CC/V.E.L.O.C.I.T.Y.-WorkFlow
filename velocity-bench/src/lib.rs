//! velocity-bench — Side-by-side benchmark harness: VELOCITY-WorkFlow vs Temporal.
//!
//! Architecture:
//!   [Workload Definitions] ──► [BenchmarkEngine trait] ──► [VELOCITY Adapter]
//!                              (common interface)          [Temporal Adapter]
//!                                        │
//!                              [MetricsCollector] ◄───────┘
//!                                        │
//!                              [ReportGenerator] ──► Markdown / CSV / JSON

pub mod engine;
pub mod metrics;
pub mod report;
pub mod workloads;

pub use engine::{BenchmarkEngine, BenchmarkResult, EngineConfig, EngineKind};
pub use metrics::{MetricsCollector, MetricsSnapshot, LatencyBucket};
pub use report::{ReportGenerator, ComparisonReport, ComparisonRow};
pub use workloads::{WorkloadDefinition, WorkloadConfig, WorkloadKind};
