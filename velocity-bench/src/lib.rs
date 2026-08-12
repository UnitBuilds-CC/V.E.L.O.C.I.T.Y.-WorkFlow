//! velocity-bench — Side-by-side benchmark harness: VELOCITY-WorkFlow vs Temporal.
//!
//! Architecture (apples-to-apples via identical gRPC paths):
//!
//!   [velocity-bench] ──gRPC──► [velocity-dev-server] ──► [DevEngine]  (VELOCITY)
//!   [velocity-bench] ──gRPC──► [temporal-server]       ──► [Matching/History] (Temporal)
//!
//! Both engines implement the same `BenchmarkService` proto, so the benchmark
//! client communicates identically with both. No direct/in-process API calls.

pub mod engine;
pub mod metrics;
pub mod report;
pub mod workloads;

pub use engine::{
    BenchmarkEngine, BenchmarkResult, EngineConfig, EngineKind, GrpcAdapter, TemporalAdapter,
    VelocityAdapter,
};
pub use metrics::{LatencyBucket, MetricsCollector, MetricsSnapshot};
pub use report::{ComparisonReport, ComparisonRow, ReportGenerator};
pub use workloads::{WorkloadConfig, WorkloadDefinition, WorkloadKind};
