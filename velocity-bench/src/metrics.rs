//! Metrics collection — granular measurement for side-by-side comparison.
//!
//! Collects latency histograms (via HdrHistogram), memory snapshots (via sysinfo),
//! CPU usage, throughput counters, and error categorization.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};

// ─── Latency Bucket ──────────────────────────────────────────────────────────

/// Latency distribution captured during a benchmark run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyBucket {
    pub min_us: u64,
    pub max_us: u64,
    pub mean_us: u64,
    pub p50_us: u64,
    pub p90_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub count: u64,
}

impl Default for LatencyBucket {
    fn default() -> Self {
        Self {
            min_us: 0,
            max_us: 0,
            mean_us: 0,
            p50_us: 0,
            p90_us: 0,
            p95_us: 0,
            p99_us: 0,
            p999_us: 0,
            count: 0,
        }
    }
}

// ─── Memory Snapshot ─────────────────────────────────────────────────────────

/// Point-in-time memory measurement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemorySnapshot {
    /// Timestamp relative to benchmark start.
    pub elapsed_secs: f64,
    /// Resident set size in MB.
    pub rss_mb: f64,
    /// Heap usage in MB (if available).
    pub heap_mb: f64,
}

// ─── CPU Snapshot ────────────────────────────────────────────────────────────

/// Point-in-time CPU measurement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuSnapshot {
    pub elapsed_secs: f64,
    pub cpu_percent: f64,
}

// ─── Metrics Snapshot ────────────────────────────────────────────────────────

/// Complete metrics snapshot from a benchmark run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    /// Latency distribution for workflow starts.
    pub start_latency: LatencyBucket,
    /// Latency distribution for signals.
    pub signal_latency: LatencyBucket,
    /// Latency distribution for queries.
    pub query_latency: LatencyBucket,
    /// Latency distribution for completions.
    pub completion_latency: LatencyBucket,
    /// Total operations completed.
    pub total_operations: u64,
    /// Successful operations.
    pub successful_operations: u64,
    /// Failed operations.
    pub failed_operations: u64,
    /// Total benchmark duration.
    pub total_duration: Duration,
    /// Operations per second.
    pub operations_per_second: f64,
    /// Memory samples over time.
    pub memory_samples: Vec<MemorySnapshot>,
    /// CPU samples over time.
    pub cpu_samples: Vec<CpuSnapshot>,
    /// Peak memory usage.
    pub peak_memory_mb: f64,
    /// Peak CPU usage.
    pub peak_cpu_percent: f64,
    /// Error breakdown by category.
    pub errors: HashMap<String, u64>,
}

impl MetricsSnapshot {
    /// Error rate as a percentage (0-100).
    pub fn error_rate(&self) -> f64 {
        if self.total_operations == 0 {
            return 0.0;
        }
        self.failed_operations as f64 / self.total_operations as f64 * 100.0
    }
}

impl Default for MetricsSnapshot {
    fn default() -> Self {
        Self {
            start_latency: LatencyBucket::default(),
            signal_latency: LatencyBucket::default(),
            query_latency: LatencyBucket::default(),
            completion_latency: LatencyBucket::default(),
            total_operations: 0,
            successful_operations: 0,
            failed_operations: 0,
            total_duration: Duration::ZERO,
            operations_per_second: 0.0,
            memory_samples: Vec::new(),
            cpu_samples: Vec::new(),
            peak_memory_mb: 0.0,
            peak_cpu_percent: 0.0,
            errors: HashMap::new(),
        }
    }
}

// ─── Latency Recorder ────────────────────────────────────────────────────────

/// Records individual latency samples and computes percentiles.
#[derive(Debug, Clone)]
pub struct LatencyRecorder {
    samples: Vec<u64>,
}

impl LatencyRecorder {
    pub fn new() -> Self {
        Self {
            samples: Vec::with_capacity(10_000),
        }
    }

    pub fn record(&mut self, latency_us: u64) {
        self.samples.push(latency_us);
    }

    pub fn record_duration(&mut self, duration: Duration) {
        self.samples.push(duration.as_micros() as u64);
    }

    pub fn snapshot(&self) -> LatencyBucket {
        if self.samples.is_empty() {
            return LatencyBucket::default();
        }

        let mut sorted = self.samples.clone();
        sorted.sort_unstable();

        let count = sorted.len() as u64;
        let sum: u64 = sorted.iter().sum();
        let mean = sum / count;

        LatencyBucket {
            min_us: sorted[0],
            max_us: sorted[sorted.len() - 1],
            mean_us: mean,
            p50_us: percentile(&sorted, 50.0),
            p90_us: percentile(&sorted, 90.0),
            p95_us: percentile(&sorted, 95.0),
            p99_us: percentile(&sorted, 99.0),
            p999_us: percentile(&sorted, 99.9),
            count,
        }
    }

    pub fn count(&self) -> u64 {
        self.samples.len() as u64
    }
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

// ─── Metrics Collector ───────────────────────────────────────────────────────

/// Collects metrics during a benchmark run.
///
/// Thread-safe — can be shared across concurrent workload executors.
pub struct MetricsCollector {
    pub start_latency: std::sync::Mutex<LatencyRecorder>,
    pub signal_latency: std::sync::Mutex<LatencyRecorder>,
    pub query_latency: std::sync::Mutex<LatencyRecorder>,
    pub completion_latency: std::sync::Mutex<LatencyRecorder>,
    pub total_ops: std::sync::atomic::AtomicU64,
    pub success_ops: std::sync::atomic::AtomicU64,
    pub failed_ops: std::sync::atomic::AtomicU64,
    pub errors: std::sync::Mutex<HashMap<String, u64>>,
    pub memory_samples: std::sync::Mutex<Vec<MemorySnapshot>>,
    pub cpu_samples: std::sync::Mutex<Vec<CpuSnapshot>>,
    pub start_time: Instant,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            start_latency: std::sync::Mutex::new(LatencyRecorder::new()),
            signal_latency: std::sync::Mutex::new(LatencyRecorder::new()),
            query_latency: std::sync::Mutex::new(LatencyRecorder::new()),
            completion_latency: std::sync::Mutex::new(LatencyRecorder::new()),
            total_ops: std::sync::atomic::AtomicU64::new(0),
            success_ops: std::sync::atomic::AtomicU64::new(0),
            failed_ops: std::sync::atomic::AtomicU64::new(0),
            errors: std::sync::Mutex::new(HashMap::new()),
            memory_samples: std::sync::Mutex::new(Vec::new()),
            cpu_samples: std::sync::Mutex::new(Vec::new()),
            start_time: Instant::now(),
        }
    }

    /// Record a start-workflow latency.
    pub fn record_start(&self, latency: Duration) {
        self.start_latency.lock().unwrap().record_duration(latency);
        self.total_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.success_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Record a signal latency.
    pub fn record_signal(&self, latency: Duration) {
        self.signal_latency.lock().unwrap().record_duration(latency);
        self.total_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.success_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Record a query latency.
    pub fn record_query(&self, latency: Duration) {
        self.query_latency.lock().unwrap().record_duration(latency);
        self.total_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.success_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Record a completion latency.
    pub fn record_completion(&self, latency: Duration) {
        self.completion_latency
            .lock()
            .unwrap()
            .record_duration(latency);
        self.total_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.success_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Record an error.
    pub fn record_error(&self, category: &str) {
        let mut errors = self.errors.lock().unwrap();
        *errors.entry(category.to_string()).or_insert(0) += 1;
        self.failed_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Record a memory sample.
    pub fn record_memory(&self, rss_mb: f64, heap_mb: f64) {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        self.memory_samples.lock().unwrap().push(MemorySnapshot {
            elapsed_secs: elapsed,
            rss_mb,
            heap_mb,
        });
    }

    /// Record a CPU sample.
    pub fn record_cpu(&self, cpu_percent: f64) {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        self.cpu_samples.lock().unwrap().push(CpuSnapshot {
            elapsed_secs: elapsed,
            cpu_percent,
        });
    }

    /// Take a snapshot of all collected metrics.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let total = self.total_ops.load(std::sync::atomic::Ordering::Relaxed);
        let success = self.success_ops.load(std::sync::atomic::Ordering::Relaxed);
        let failed = self.failed_ops.load(std::sync::atomic::Ordering::Relaxed);
        let duration = self.start_time.elapsed();
        let ops_per_sec = if duration.as_secs_f64() > 0.0 {
            total as f64 / duration.as_secs_f64()
        } else {
            0.0
        };

        let mem_samples = self.memory_samples.lock().unwrap().clone();
        let cpu_samples = self.cpu_samples.lock().unwrap().clone();
        let peak_mem = mem_samples.iter().map(|s| s.rss_mb).fold(0.0_f64, f64::max);
        let peak_cpu = cpu_samples
            .iter()
            .map(|s| s.cpu_percent)
            .fold(0.0_f64, f64::max);

        MetricsSnapshot {
            start_latency: self.start_latency.lock().unwrap().snapshot(),
            signal_latency: self.signal_latency.lock().unwrap().snapshot(),
            query_latency: self.query_latency.lock().unwrap().snapshot(),
            completion_latency: self.completion_latency.lock().unwrap().snapshot(),
            total_operations: total,
            successful_operations: success,
            failed_operations: failed,
            total_duration: duration,
            operations_per_second: ops_per_sec,
            memory_samples: mem_samples,
            cpu_samples: cpu_samples,
            peak_memory_mb: peak_mem,
            peak_cpu_percent: peak_cpu,
            errors: self.errors.lock().unwrap().clone(),
        }
    }

    /// Reset all metrics.
    pub fn reset(&self) {
        *self.start_latency.lock().unwrap() = LatencyRecorder::new();
        *self.signal_latency.lock().unwrap() = LatencyRecorder::new();
        *self.query_latency.lock().unwrap() = LatencyRecorder::new();
        *self.completion_latency.lock().unwrap() = LatencyRecorder::new();
        self.total_ops
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.success_ops
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.failed_ops
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.errors.lock().unwrap().clear();
        self.memory_samples.lock().unwrap().clear();
        self.cpu_samples.lock().unwrap().clear();
    }
}

// ─── System Metrics Probe ────────────────────────────────────────────────────

/// Probes system-level metrics (memory, CPU) for the current process.
pub struct SystemMetricsProbe {
    _pid: u32,
}

impl SystemMetricsProbe {
    pub fn new() -> Self {
        Self {
            _pid: std::process::id(),
        }
    }

    /// Get current process RSS memory in MB.
    pub fn current_rss_mb(&self) -> f64 {
        // Read from /proc/self/status on Linux
        #[cfg(target_os = "linux")]
        {
            if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
                for line in status.lines() {
                    if line.starts_with("VmRSS:") {
                        let parts: Vec<&str> = line.split_whitespace().collect();
                        if parts.len() >= 2 {
                            if let Ok(kb) = parts[1].parse::<f64>() {
                                return kb / 1024.0;
                            }
                        }
                    }
                }
            }
            return 0.0;
        }

        // Fallback: estimate from working set
        #[cfg(target_os = "windows")]
        {
            // On Windows, we'd use GetProcessMemoryInfo
            // For now, return 0 as placeholder
            return 0.0;
        }

        #[cfg(not(any(target_os = "linux", target_os = "windows")))]
        0.0
    }

    /// Get current process CPU usage (approximate).
    pub fn current_cpu_percent(&self) -> f64 {
        // Simplified — in production, track CPU time deltas
        0.0
    }
}
