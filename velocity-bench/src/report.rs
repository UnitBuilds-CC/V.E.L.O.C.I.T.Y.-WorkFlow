//! Report generator — side-by-side comparison tables.
//!
//! Produces Markdown, CSV, and JSON reports comparing VELOCITY vs Temporal
//! metrics for each workload. Includes delta percentages and verdicts.

use std::collections::HashMap;
use std::io::Write;
use serde::{Deserialize, Serialize};
use crate::metrics::{MetricsSnapshot, LatencyBucket};
use crate::engine::EngineKind;

// ─── Comparison Row ──────────────────────────────────────────────────────────

/// A single row in the comparison report — one workload, both engines.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonRow {
    pub workload_name: String,
    pub workload_description: String,

    // VELOCITY metrics
    pub velocity_ops_per_sec: f64,
    pub velocity_p50_us: u64,
    pub velocity_p95_us: u64,
    pub velocity_p99_us: u64,
    pub velocity_p999_us: u64,
    pub velocity_peak_memory_mb: f64,
    pub velocity_peak_cpu: f64,
    pub velocity_error_rate: f64,
    pub velocity_total_ops: u64,

    // Temporal metrics
    pub temporal_ops_per_sec: f64,
    pub temporal_p50_us: u64,
    pub temporal_p95_us: u64,
    pub temporal_p99_us: u64,
    pub temporal_p999_us: u64,
    pub temporal_peak_memory_mb: f64,
    pub temporal_peak_cpu: f64,
    pub temporal_error_rate: f64,
    pub temporal_total_ops: u64,

    // Deltas
    pub throughput_delta_pct: f64,
    pub p50_latency_delta_pct: f64,
    pub p99_latency_delta_pct: f64,
    pub memory_delta_pct: f64,
    pub error_rate_delta_pct: f64,

    // Verdict
    pub verdict: String,
}

impl ComparisonRow {
    /// Create a comparison row from two metric snapshots.
    pub fn from_snapshots(
        name: &str,
        description: &str,
        velocity: &MetricsSnapshot,
        temporal: &MetricsSnapshot,
    ) -> Self {
        let throughput_delta = if temporal.operations_per_second > 0.0 {
            ((velocity.operations_per_second - temporal.operations_per_second)
                / temporal.operations_per_second)
                * 100.0
        } else if velocity.operations_per_second > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };

        let p50_delta = if temporal.start_latency.p50_us > 0 {
            ((velocity.start_latency.p50_us as f64 - temporal.start_latency.p50_us as f64)
                / temporal.start_latency.p50_us as f64)
                * 100.0
        } else {
            0.0
        };

        let p99_delta = if temporal.start_latency.p99_us > 0 {
            ((velocity.start_latency.p99_us as f64 - temporal.start_latency.p99_us as f64)
                / temporal.start_latency.p99_us as f64)
                * 100.0
        } else {
            0.0
        };

        let mem_delta = if temporal.peak_memory_mb > 0.0 {
            ((velocity.peak_memory_mb - temporal.peak_memory_mb) / temporal.peak_memory_mb)
                * 100.0
        } else {
            0.0
        };

        let err_delta = velocity.error_rate() - temporal.error_rate();

        // Determine verdict
        let verdict = if throughput_delta > 50.0 && p99_delta < 0.0 {
            "VELOCITY dominates".into()
        } else if throughput_delta > 20.0 {
            "VELOCITY faster".into()
        } else if throughput_delta > -20.0 && throughput_delta < 20.0 {
            "Comparable".into()
        } else if throughput_delta < -50.0 {
            "Temporal faster".into()
        } else {
            "See details".into()
        };

        let vel_err = velocity.error_rate();
        let tmp_err = temporal.error_rate();

        ComparisonRow {
            workload_name: name.into(),
            workload_description: description.into(),
            velocity_ops_per_sec: velocity.operations_per_second,
            velocity_p50_us: velocity.start_latency.p50_us,
            velocity_p95_us: velocity.start_latency.p95_us,
            velocity_p99_us: velocity.start_latency.p99_us,
            velocity_p999_us: velocity.start_latency.p999_us,
            velocity_peak_memory_mb: velocity.peak_memory_mb,
            velocity_peak_cpu: velocity.peak_cpu_percent,
            velocity_error_rate: vel_err,
            velocity_total_ops: velocity.total_operations,
            temporal_ops_per_sec: temporal.operations_per_second,
            temporal_p50_us: temporal.start_latency.p50_us,
            temporal_p95_us: temporal.start_latency.p95_us,
            temporal_p99_us: temporal.start_latency.p99_us,
            temporal_p999_us: temporal.start_latency.p999_us,
            temporal_peak_memory_mb: temporal.peak_memory_mb,
            temporal_peak_cpu: temporal.peak_cpu_percent,
            temporal_error_rate: tmp_err,
            temporal_total_ops: temporal.total_operations,
            throughput_delta_pct: throughput_delta,
            p50_latency_delta_pct: p50_delta,
            p99_latency_delta_pct: p99_delta,
            memory_delta_pct: mem_delta,
            error_rate_delta_pct: err_delta,
            verdict,
        }
    }
}

// ─── Comparison Report ───────────────────────────────────────────────────────

/// A complete comparison report across all workloads.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonReport {
    pub generated_at: String,
    pub velocity_version: String,
    pub temporal_version: String,
    pub rows: Vec<ComparisonRow>,
    pub summary: ReportSummary,
}

/// Summary statistics across all workloads.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportSummary {
    pub total_workloads: usize,
    pub velocity_wins: usize,
    pub temporal_wins: usize,
    pub comparable: usize,
    pub avg_throughput_delta_pct: f64,
    pub avg_p99_latency_delta_pct: f64,
    pub avg_memory_delta_pct: f64,
    pub overall_verdict: String,
}

impl ReportSummary {
    pub fn from_rows(rows: &[ComparisonRow]) -> Self {
        let total = rows.len();
        let vel_wins = rows.iter().filter(|r| r.throughput_delta_pct > 20.0).count();
        let tmp_wins = rows.iter().filter(|r| r.throughput_delta_pct < -20.0).count();
        let comparable = total - vel_wins - tmp_wins;

        let avg_throughput = if total > 0 {
            rows.iter().map(|r| r.throughput_delta_pct).sum::<f64>() / total as f64
        } else {
            0.0
        };

        let avg_p99 = if total > 0 {
            rows.iter().map(|r| r.p99_latency_delta_pct).sum::<f64>() / total as f64
        } else {
            0.0
        };

        let avg_mem = if total > 0 {
            rows.iter().map(|r| r.memory_delta_pct).sum::<f64>() / total as f64
        } else {
            0.0
        };

        let verdict = if vel_wins > tmp_wins * 2 {
            "VELOCITY is a viable Temporal replacement — significantly faster in most workloads"
        } else if vel_wins > tmp_wins {
            "VELOCITY is competitive with Temporal — faster in some areas, slower in others"
        } else if vel_wins == tmp_wins {
            "VELOCITY and Temporal are roughly comparable"
        } else {
            "Temporal outperforms VELOCITY in most workloads — more optimization needed"
        };

        ReportSummary {
            total_workloads: total,
            velocity_wins: vel_wins,
            temporal_wins: tmp_wins,
            comparable,
            avg_throughput_delta_pct: avg_throughput,
            avg_p99_latency_delta_pct: avg_p99,
            avg_memory_delta_pct: avg_mem,
            overall_verdict: verdict.into(),
        }
    }
}

// ─── Report Generator ────────────────────────────────────────────────────────

/// Generates comparison reports in multiple formats.
pub struct ReportGenerator;

impl ReportGenerator {
    /// Generate a Markdown comparison report.
    pub fn generate_markdown(report: &ComparisonReport) -> String {
        let mut out = String::new();

        // Header
        out.push_str("# VELOCITY-WorkFlow vs Temporal — Benchmark Report\n\n");
        out.push_str(&format!("**Generated:** {}  \n", report.generated_at));
        out.push_str(&format!("**VELOCITY version:** {}  \n", report.velocity_version));
        out.push_str(&format!("**Temporal version:** {}  \n\n", report.temporal_version));

        // Summary box
        let s = &report.summary;
        out.push_str("## Summary\n\n");
        out.push_str(&format!("| Metric | Value |\n"));
        out.push_str(&format!("|--------|-------|\n"));
        out.push_str(&format!("| Total workloads | {} |\n", s.total_workloads));
        out.push_str(&format!("| VELOCITY wins | {} |\n", s.velocity_wins));
        out.push_str(&format!("| Temporal wins | {} |\n", s.temporal_wins));
        out.push_str(&format!("| Comparable | {} |\n", s.comparable));
        out.push_str(&format!("| Avg throughput delta | {:+.1}% |\n", s.avg_throughput_delta_pct));
        out.push_str(&format!("| Avg p99 latency delta | {:+.1}% |\n", s.avg_p99_latency_delta_pct));
        out.push_str(&format!("| Avg memory delta | {:+.1}% |\n", s.avg_memory_delta_pct));
        out.push_str(&format!("\n**Overall verdict:** {}\n\n", s.overall_verdict));

        // Detailed table
        out.push_str("## Detailed Comparison\n\n");
        out.push_str("| Workload | VELOCITY ops/s | Temporal ops/s | Δ Throughput | VELOCITY p99 | Temporal p99 | Δ p99 | VELOCITY Mem | Temporal Mem | Verdict |\n");
        out.push_str("|----------|---------------|----------------|-------------|-------------|-------------|-------|-------------|-------------|----------|\n");

        for row in &report.rows {
            out.push_str(&format!(
                "| {} | {:.0} | {:.0} | {:+.1}% | {:.0}µs | {:.0}µs | {:+.1}% | {:.1}MB | {:.1}MB | {} |\n",
                row.workload_name,
                row.velocity_ops_per_sec,
                row.temporal_ops_per_sec,
                row.throughput_delta_pct,
                row.velocity_p99_us,
                row.temporal_p99_us,
                row.p99_latency_delta_pct,
                row.velocity_peak_memory_mb,
                row.temporal_peak_memory_mb,
                row.verdict,
            ));
        }

        // Per-workload detail sections
        out.push_str("\n## Per-Workload Details\n\n");

        for row in &report.rows {
            out.push_str(&format!("### {}\n\n", row.workload_name));
            out.push_str(&format!("*{}*\n\n", row.workload_description));
            out.push_str("| Metric | VELOCITY | Temporal | Delta |\n");
            out.push_str("|--------|----------|----------|-------|\n");
            out.push_str(&format!("| Ops/sec | {:.0} | {:.0} | {:+.1}% |\n",
                row.velocity_ops_per_sec, row.temporal_ops_per_sec, row.throughput_delta_pct));
            out.push_str(&format!("| p50 latency | {}µs | {}µs | {:+.1}% |\n",
                row.velocity_p50_us, row.temporal_p50_us, row.p50_latency_delta_pct));
            out.push_str(&format!("| p95 latency | {}µs | {}µs | — |\n",
                row.velocity_p95_us, row.temporal_p95_us));
            out.push_str(&format!("| p99 latency | {}µs | {}µs | {:+.1}% |\n",
                row.velocity_p99_us, row.temporal_p99_us, row.p99_latency_delta_pct));
            out.push_str(&format!("| p999 latency | {}µs | {}µs | — |\n",
                row.velocity_p999_us, row.temporal_p999_us));
            out.push_str(&format!("| Peak memory | {:.1}MB | {:.1}MB | {:+.1}% |\n",
                row.velocity_peak_memory_mb, row.temporal_peak_memory_mb, row.memory_delta_pct));
            out.push_str(&format!("| Peak CPU | {:.1}% | {:.1}% | — |\n",
                row.velocity_peak_cpu, row.temporal_peak_cpu));
            out.push_str(&format!("| Error rate | {:.2}% | {:.2}% | {:+.2}% |\n",
                row.velocity_error_rate, row.temporal_error_rate, row.error_rate_delta_pct));
            out.push_str(&format!("| Total ops | {} | {} | — |\n",
                row.velocity_total_ops, row.temporal_total_ops));
            out.push_str(&format!("\n**Verdict:** {}\n\n", row.verdict));
        }

        out
    }

    /// Generate a CSV comparison report.
    pub fn generate_csv(report: &ComparisonReport) -> Result<Vec<u8>, String> {
        let mut wtr = csv::Writer::from_writer(Vec::new());

        // Header
        wtr.write_record(&[
            "workload", "description",
            "velocity_ops_per_sec", "temporal_ops_per_sec", "throughput_delta_pct",
            "velocity_p50_us", "temporal_p50_us", "p50_delta_pct",
            "velocity_p99_us", "temporal_p99_us", "p99_delta_pct",
            "velocity_p999_us", "temporal_p999_us",
            "velocity_memory_mb", "temporal_memory_mb", "memory_delta_pct",
            "velocity_error_rate", "temporal_error_rate",
            "verdict",
        ]).map_err(|e| e.to_string())?;

        for row in &report.rows {
            wtr.write_record(&[
                &row.workload_name,
                &row.workload_description,
                &format!("{:.2}", row.velocity_ops_per_sec),
                &format!("{:.2}", row.temporal_ops_per_sec),
                &format!("{:+.2}", row.throughput_delta_pct),
                &row.velocity_p50_us.to_string(),
                &row.temporal_p50_us.to_string(),
                &format!("{:+.2}", row.p50_latency_delta_pct),
                &row.velocity_p99_us.to_string(),
                &row.temporal_p99_us.to_string(),
                &format!("{:+.2}", row.p99_latency_delta_pct),
                &row.velocity_p999_us.to_string(),
                &row.temporal_p999_us.to_string(),
                &format!("{:.2}", row.velocity_peak_memory_mb),
                &format!("{:.2}", row.temporal_peak_memory_mb),
                &format!("{:+.2}", row.memory_delta_pct),
                &format!("{:.4}", row.velocity_error_rate),
                &format!("{:.4}", row.temporal_error_rate),
                &row.verdict,
            ]).map_err(|e| e.to_string())?;
        }

        wtr.into_inner().map_err(|e| e.to_string())
    }

    /// Generate a JSON comparison report.
    pub fn generate_json(report: &ComparisonReport) -> Result<String, String> {
        serde_json::to_string_pretty(report).map_err(|e| e.to_string())
    }
}
