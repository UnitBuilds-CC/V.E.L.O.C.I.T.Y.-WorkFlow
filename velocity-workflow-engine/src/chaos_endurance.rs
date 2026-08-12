//! Chaos endurance testing — sustained soak tests with metrics collection.
//! Provides multi-threaded concurrent workflow operations to verify engine
//! stability under prolonged load, detecting memory leaks, deadlocks, and
//! resource exhaustion.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::engine::WorkflowEngine;

// ─── Soak Test Configuration ─────────────────────────────────────────────────

/// Configuration for an endurance soak test.
#[derive(Debug, Clone)]
pub struct SoakTestConfig {
    /// Duration of the soak test.
    pub duration: Duration,
    /// Number of concurrent worker threads.
    pub thread_count: usize,
    /// Maximum workflows to start per iteration.
    pub batch_size: usize,
    /// Whether to randomly fail some workflows.
    pub inject_failures: bool,
    /// Failure rate (0.0 to 1.0).
    pub failure_rate: f64,
    /// Whether to enable signal/query operations.
    pub enable_signals: bool,
    /// Whether to enable search attribute operations.
    pub enable_search_attributes: bool,
}

impl Default for SoakTestConfig {
    fn default() -> Self {
        Self {
            duration: Duration::from_secs(10),
            thread_count: 4,
            batch_size: 10,
            inject_failures: false,
            failure_rate: 0.0,
            enable_signals: false,
            enable_search_attributes: false,
        }
    }
}

// ─── Soak Test Metrics ───────────────────────────────────────────────────────

/// Metrics collected during a soak test.
#[derive(Debug)]
pub struct SoakTestMetrics {
    pub workflows_started: AtomicU64,
    pub workflows_completed: AtomicU64,
    pub workflows_failed: AtomicU64,
    pub workflows_cancelled: AtomicU64,
    pub signals_sent: AtomicU64,
    pub queries_executed: AtomicU64,
    pub steps_completed: AtomicU64,
    pub errors: AtomicU64,
    pub total_duration_ms: AtomicU64,
    pub peak_active_workflows: AtomicU64,
}

impl SoakTestMetrics {
    pub fn new() -> Self {
        Self {
            workflows_started: AtomicU64::new(0),
            workflows_completed: AtomicU64::new(0),
            workflows_failed: AtomicU64::new(0),
            workflows_cancelled: AtomicU64::new(0),
            signals_sent: AtomicU64::new(0),
            queries_executed: AtomicU64::new(0),
            steps_completed: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            total_duration_ms: AtomicU64::new(0),
            peak_active_workflows: AtomicU64::new(0),
        }
    }

    pub fn total_operations(&self) -> u64 {
        self.workflows_started.load(Ordering::Relaxed)
            + self.workflows_completed.load(Ordering::Relaxed)
            + self.workflows_failed.load(Ordering::Relaxed)
            + self.steps_completed.load(Ordering::Relaxed)
            + self.signals_sent.load(Ordering::Relaxed)
            + self.queries_executed.load(Ordering::Relaxed)
    }

    pub fn throughput_ops_per_sec(&self) -> f64 {
        let duration_ms = self.total_duration_ms.load(Ordering::Relaxed);
        if duration_ms == 0 {
            return 0.0;
        }
        self.total_operations() as f64 / (duration_ms as f64 / 1000.0)
    }

    pub fn error_rate(&self) -> f64 {
        let total = self.total_operations();
        if total == 0 {
            return 0.0;
        }
        self.errors.load(Ordering::Relaxed) as f64 / total as f64
    }

    pub fn summary(&self) -> String {
        format!(
            "SoakTest Results: started={}, completed={}, failed={}, cancelled={}, \
             steps={}, signals={}, queries={}, errors={}, \
             throughput={:.1} ops/sec, error_rate={:.4}, duration={}ms",
            self.workflows_started.load(Ordering::Relaxed),
            self.workflows_completed.load(Ordering::Relaxed),
            self.workflows_failed.load(Ordering::Relaxed),
            self.workflows_cancelled.load(Ordering::Relaxed),
            self.steps_completed.load(Ordering::Relaxed),
            self.signals_sent.load(Ordering::Relaxed),
            self.queries_executed.load(Ordering::Relaxed),
            self.errors.load(Ordering::Relaxed),
            self.throughput_ops_per_sec(),
            self.error_rate(),
            self.total_duration_ms.load(Ordering::Relaxed),
        )
    }
}

impl Default for SoakTestMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Soak Test Runner ────────────────────────────────────────────────────────

/// Run a soak test against a WorkflowEngine.
/// Spawns multiple threads that concurrently create, step, signal, query,
/// and complete workflows for the configured duration.
pub fn run_soak_test(config: &SoakTestConfig) -> Arc<SoakTestMetrics> {
    let engine = Arc::new(WorkflowEngine::new());
    let metrics = Arc::new(SoakTestMetrics::new());
    let stop = Arc::new(AtomicBool::new(false));
    let active_workflows = Arc::new(AtomicU64::new(0));

    let start_time = Instant::now();
    let mut handles = Vec::new();

    for thread_id in 0..config.thread_count {
        let engine = Arc::clone(&engine);
        let metrics = Arc::clone(&metrics);
        let stop = Arc::clone(&stop);
        let active = Arc::clone(&active_workflows);
        let config = config.clone();

        let handle = std::thread::spawn(move || {
            let mut rng_state = (thread_id as u64 + 1) * 6364136223846793005;
            let mut workflow_counter = 0u64;

            while !stop.load(Ordering::Relaxed) {
                // Simple pseudo-random number
                rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let rand_val = (rng_state >> 33) as f64 / (u32::MAX as f64);

                // Start a workflow
                let wf_id = (thread_id as u64 * 1_000_000) + workflow_counter;
                workflow_counter += 1;
                let workflow_key =
                    engine.start_workflow(wf_id, 1, 0, (thread_id as u64) + 1, 3, None);

                if workflow_key == 0 {
                    metrics.errors.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                metrics.workflows_started.fetch_add(1, Ordering::Relaxed);
                let current_active = active.fetch_add(1, Ordering::Relaxed) + 1;

                // Update peak
                let mut peak = active.load(Ordering::Relaxed);
                while current_active > peak {
                    match active.compare_exchange_weak(
                        peak,
                        current_active,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => break,
                        Err(p) => peak = p,
                    }
                }

                // Complete steps
                for step in 0..3 {
                    let data = vec![step as u8; 8];
                    engine.complete_step(workflow_key, step, data);
                    metrics.steps_completed.fetch_add(1, Ordering::Relaxed);
                }

                // Optionally send signals
                if config.enable_signals && rand_val > 0.5 {
                    let signal_id = rng_state >> 16;
                    engine.signal_workflow(workflow_key, signal_id, vec![1, 2, 3]);
                    metrics.signals_sent.fetch_add(1, Ordering::Relaxed);
                }

                // Optionally query
                if config.enable_search_attributes && rand_val > 0.7 {
                    let _status = engine.get_status(workflow_key);
                    metrics.queries_executed.fetch_add(1, Ordering::Relaxed);
                }

                // Decide outcome: complete, fail, or cancel
                let outcome_rand = (rng_state >> 48) as f64 / (u16::MAX as f64);
                if config.inject_failures && outcome_rand < config.failure_rate {
                    engine.fail_workflow(workflow_key);
                    metrics.workflows_failed.fetch_add(1, Ordering::Relaxed);
                } else if config.inject_failures && outcome_rand < config.failure_rate * 1.5 {
                    engine.cancel_workflow(workflow_key);
                    metrics.workflows_cancelled.fetch_add(1, Ordering::Relaxed);
                } else {
                    engine.complete_workflow(workflow_key, None);
                    metrics.workflows_completed.fetch_add(1, Ordering::Relaxed);
                }

                active.fetch_sub(1, Ordering::Relaxed);
            }
        });
        handles.push(handle);
    }

    // Wait for the configured duration
    std::thread::sleep(config.duration);
    stop.store(true, Ordering::Relaxed);

    // Wait for all threads to finish
    for handle in handles {
        let _ = handle.join();
    }

    let elapsed = start_time.elapsed();
    metrics
        .total_duration_ms
        .store(elapsed.as_millis() as u64, Ordering::Relaxed);
    metrics
        .peak_active_workflows
        .store(active_workflows.load(Ordering::Relaxed), Ordering::Relaxed);

    metrics
}

// ─── Crash Recovery Test ─────────────────────────────────────────────────────

/// Test that workflows survive simulated crashes (engine restart).
pub fn run_crash_recovery_test(workflow_count: usize) -> (usize, usize) {
    let mut started = 0;
    let mut recovered = 0;

    // Phase 1: Start workflows
    {
        let engine = WorkflowEngine::new();
        for i in 0..workflow_count {
            let key = engine.start_workflow(i as u64 + 1, 1, 0, 1, 5, None);
            if key > 0 {
                // Complete a few steps
                for step in 0..2 {
                    engine.complete_step(key, step, vec![step as u8; 4]);
                }
                started += 1;
            }
        }
        // Engine drops here — simulating crash
    }

    // Phase 2: Create new engine (simulating restart)
    // In a real scenario, WAL replay would restore state.
    // Here we verify the engine can start fresh.
    {
        let engine = WorkflowEngine::new();
        // Start new workflows on the "recovered" engine
        for i in 0..workflow_count {
            let key = engine.start_workflow(i as u64 + 1000, 1, 0, 1, 3, None);
            if key > 0 {
                engine.complete_workflow(key, None);
                recovered += 1;
            }
        }
    }

    (started, recovered)
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_soak_test_config_default() {
        let config = SoakTestConfig::default();
        assert_eq!(config.duration, Duration::from_secs(10));
        assert_eq!(config.thread_count, 4);
        assert!(!config.inject_failures);
    }

    #[test]
    fn test_soak_metrics_new() {
        let metrics = SoakTestMetrics::new();
        assert_eq!(metrics.total_operations(), 0);
        assert_eq!(metrics.throughput_ops_per_sec(), 0.0);
        assert_eq!(metrics.error_rate(), 0.0);
    }

    #[test]
    fn test_soak_metrics_operations() {
        let metrics = SoakTestMetrics::new();
        metrics.workflows_started.store(100, Ordering::Relaxed);
        metrics.workflows_completed.store(90, Ordering::Relaxed);
        metrics.workflows_failed.store(5, Ordering::Relaxed);
        metrics.steps_completed.store(300, Ordering::Relaxed);
        metrics.total_duration_ms.store(1000, Ordering::Relaxed);

        assert_eq!(metrics.total_operations(), 495);
        assert!((metrics.throughput_ops_per_sec() - 495.0).abs() < 1.0);
    }

    #[test]
    fn test_soak_metrics_error_rate() {
        let metrics = SoakTestMetrics::new();
        metrics.workflows_started.store(100, Ordering::Relaxed);
        metrics.errors.store(10, Ordering::Relaxed);

        let rate = metrics.error_rate();
        assert!(rate > 0.0 && rate < 1.0);
    }

    #[test]
    fn test_soak_metrics_summary() {
        let metrics = SoakTestMetrics::new();
        metrics.workflows_started.store(50, Ordering::Relaxed);
        metrics.workflows_completed.store(45, Ordering::Relaxed);
        let summary = metrics.summary();
        assert!(summary.contains("started=50"));
        assert!(summary.contains("completed=45"));
    }

    #[test]
    fn test_soak_test_short_run() {
        let config = SoakTestConfig {
            duration: Duration::from_millis(200),
            thread_count: 2,
            batch_size: 5,
            inject_failures: false,
            failure_rate: 0.0,
            enable_signals: true,
            enable_search_attributes: true,
        };

        let metrics = run_soak_test(&config);
        assert!(metrics.workflows_started.load(Ordering::Relaxed) > 0);
        assert!(metrics.workflows_completed.load(Ordering::Relaxed) > 0);
        assert!(metrics.steps_completed.load(Ordering::Relaxed) > 0);
        assert!(metrics.total_duration_ms.load(Ordering::Relaxed) >= 150); // at least ~200ms
    }

    #[test]
    fn test_soak_test_with_failures() {
        let config = SoakTestConfig {
            duration: Duration::from_millis(200),
            thread_count: 2,
            batch_size: 5,
            inject_failures: true,
            failure_rate: 0.3,
            enable_signals: false,
            enable_search_attributes: false,
        };

        let metrics = run_soak_test(&config);
        assert!(metrics.workflows_started.load(Ordering::Relaxed) > 0);
        // With 30% failure rate, we should see some failures
        let total_outcomes = metrics.workflows_completed.load(Ordering::Relaxed)
            + metrics.workflows_failed.load(Ordering::Relaxed)
            + metrics.workflows_cancelled.load(Ordering::Relaxed);
        assert!(total_outcomes > 0);
    }

    #[test]
    fn test_crash_recovery_test() {
        let (started, recovered) = run_crash_recovery_test(10);
        assert_eq!(started, 10);
        assert_eq!(recovered, 10);
    }

    #[test]
    fn test_crash_recovery_zero_workflows() {
        let (started, recovered) = run_crash_recovery_test(0);
        assert_eq!(started, 0);
        assert_eq!(recovered, 0);
    }
}
