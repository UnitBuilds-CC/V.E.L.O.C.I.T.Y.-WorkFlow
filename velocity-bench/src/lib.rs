//! velocity-bench — Side-by-side benchmark harness: VELOCITY-WorkFlow vs Temporal.
//!
//! Architecture (apples-to-apples via identical gRPC paths):
//!
//!   [velocity-bench] ──gRPC──► [velocity-dev-server] ──► [DevEngine]  (VELOCITY)
//!   [velocity-bench] ──gRPC──► [temporal-server]       ──► [Matching/History] (Temporal)
//!
//! Both engines implement the same `BenchmarkService` proto, so the benchmark
//! client communicates identically with both. No direct/in-process API calls.
//!
//! HTTP benchmark (Velocity Runtime vs Restate):
//!
//!   [velocity-bench-http] ──HTTP──► [Velocity Runtime]  (handler invocation)
//!   [velocity-bench-http] ──HTTP──► [Restate Ingress]   (service handler)

pub mod engine;
pub mod http_adapter;
pub mod http_workloads;
pub mod metrics;
pub mod report;
pub mod workloads;

pub use engine::{
    BenchmarkEngine, BenchmarkResult, EngineConfig, EngineKind, GrpcAdapter, ServerMetrics,
    TemporalAdapter, VelocityAdapter,
};
pub use http_adapter::{HttpAdapter, HttpBenchmarkResult, HttpEngineConfig, HttpEngineKind, HttpOperationResult};
pub use http_workloads::{HttpWorkloadDefinition, HttpWorkloadKind};
pub use metrics::{
    AggregateMetrics, LatencyBucket, MetricsCollector, MetricsSnapshot, SignificanceTest,
    StatisticalSummary,
};
pub use report::{ComparisonReport, ComparisonRow, ReportGenerator, StatisticalReport};
pub use workloads::{WorkloadConfig, WorkloadDefinition, WorkloadKind};
