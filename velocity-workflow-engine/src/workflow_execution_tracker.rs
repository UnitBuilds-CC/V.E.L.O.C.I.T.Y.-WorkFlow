//! Workflow Execution Tracker — per-workflow SLO tracking, latency histograms,
//! error budgets with burn-rate alerts, throughput metering, and SLA compliance.
//!
//! Exceeds Temporal's native observability by providing built-in SLO management,
//! error budget tracking (Google SRE model), and automatic burn-rate alerting.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

// ─── Latency Histogram ─────────────────────────────────────────────────────

/// Bucket boundaries for latency histograms (in milliseconds).
const DEFAULT_BUCKETS_MS: &[u64] = &[
    1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10_000, 30_000, 60_000,
];

/// A latency histogram with configurable bucket boundaries.
#[derive(Debug, Clone)]
pub struct LatencyHistogram {
    pub buckets_ms: Vec<u64>,
    pub counts: Vec<u64>,
    pub total_count: u64,
    pub total_sum_ms: u64,
    pub min_ms: u64,
    pub max_ms: u64,
}

impl LatencyHistogram {
    pub fn new() -> Self {
        Self::with_buckets(DEFAULT_BUCKETS_MS)
    }

    pub fn with_buckets(buckets: &[u64]) -> Self {
        let mut sorted = buckets.to_vec();
        sorted.sort();
        Self {
            buckets_ms: sorted,
            counts: vec![0; buckets.len() + 1], // +1 for overflow bucket
            total_count: 0,
            total_sum_ms: 0,
            min_ms: u64::MAX,
            max_ms: 0,
        }
    }

    /// Record a latency observation.
    pub fn observe(&mut self, latency_ms: u64) {
        self.total_count += 1;
        self.total_sum_ms += latency_ms;
        if latency_ms < self.min_ms {
            self.min_ms = latency_ms;
        }
        if latency_ms > self.max_ms {
            self.max_ms = latency_ms;
        }
        // Find the right bucket
        for (i, &boundary) in self.buckets_ms.iter().enumerate() {
            if latency_ms <= boundary {
                self.counts[i] += 1;
                return;
            }
        }
        // Overflow bucket
        *self.counts.last_mut().unwrap() += 1;
    }

    /// Get the approximate percentile value (e.g., 0.99 for p99).
    pub fn percentile(&self, p: f64) -> u64 {
        if self.total_count == 0 {
            return 0;
        }
        let target = (self.total_count as f64 * p).ceil() as u64;
        let mut cumulative = 0u64;
        for (i, &count) in self.counts.iter().enumerate() {
            cumulative += count;
            if cumulative >= target {
                if i < self.buckets_ms.len() {
                    return self.buckets_ms[i];
                } else {
                    return self.max_ms;
                }
            }
        }
        self.max_ms
    }

    pub fn mean_ms(&self) -> f64 {
        if self.total_count == 0 {
            return 0.0;
        }
        self.total_sum_ms as f64 / self.total_count as f64
    }

    pub fn reset(&mut self) {
        self.counts.fill(0);
        self.total_count = 0;
        self.total_sum_ms = 0;
        self.min_ms = u64::MAX;
        self.max_ms = 0;
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

// ─── SLO Definition ────────────────────────────────────────────────────────

/// A Service Level Objective for a workflow type.
#[derive(Debug, Clone)]
pub struct SloDefinition {
    /// Human-readable SLO name.
    pub name: String,
    /// Target latency percentile (e.g., 0.99 for p99).
    pub latency_percentile: f64,
    /// Target latency in milliseconds.
    pub latency_target_ms: u64,
    /// Target success rate (e.g., 0.999 for 99.9%).
    pub success_rate_target: f64,
    /// Error budget window in milliseconds.
    pub error_budget_window_ms: u64,
    /// Burn rate alert thresholds (multipliers of budget consumption rate).
    pub burn_rate_alert_thresholds: Vec<f64>,
}

impl SloDefinition {
    /// Create a standard SLO: p99 < 5s, 99.9% success rate, 30-day window.
    pub fn standard() -> Self {
        Self {
            name: "standard".into(),
            latency_percentile: 0.99,
            latency_target_ms: 5000,
            success_rate_target: 0.999,
            error_budget_window_ms: 30 * 24 * 3600 * 1000, // 30 days
            burn_rate_alert_thresholds: vec![1.0, 2.0, 5.0, 10.0],
        }
    }

    /// Create a strict SLO: p99 < 1s, 99.99% success rate, 7-day window.
    pub fn strict() -> Self {
        Self {
            name: "strict".into(),
            latency_percentile: 0.99,
            latency_target_ms: 1000,
            success_rate_target: 0.9999,
            error_budget_window_ms: 7 * 24 * 3600 * 1000, // 7 days
            burn_rate_alert_thresholds: vec![1.0, 2.0, 5.0],
        }
    }

    /// Create a relaxed SLO: p95 < 30s, 99% success rate, 30-day window.
    pub fn relaxed() -> Self {
        Self {
            name: "relaxed".into(),
            latency_percentile: 0.95,
            latency_target_ms: 30_000,
            success_rate_target: 0.99,
            error_budget_window_ms: 30 * 24 * 3600 * 1000,
            burn_rate_alert_thresholds: vec![2.0, 5.0, 10.0],
        }
    }
}

// ─── Error Budget ──────────────────────────────────────────────────────────

/// Tracks error budget consumption using the Google SRE model.
#[derive(Debug, Clone)]
pub struct ErrorBudget {
    /// Total budget (derived from SLO target and window).
    pub total_budget: f64,
    /// Remaining budget (starts at total_budget, decreases with errors).
    pub remaining_budget: f64,
    /// Budget consumed so far.
    pub consumed_budget: f64,
    /// Window start timestamp (ms).
    pub window_start_ms: u64,
    /// Window duration in ms.
    pub window_ms: u64,
    /// Total errors in this window.
    pub total_errors: u64,
    /// Total requests in this window.
    pub total_requests: u64,
    /// Current burn rate (ratio of error rate to allowed error rate).
    pub current_burn_rate: f64,
    /// Active burn rate alerts.
    pub active_alerts: Vec<BurnRateAlert>,
}

/// A burn rate alert.
#[derive(Debug, Clone)]
pub struct BurnRateAlert {
    pub threshold: f64,
    pub current_burn_rate: f64,
    pub triggered_at_ms: u64,
    pub severity: AlertSeverity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
    Page,
}

impl ErrorBudget {
    pub fn new(slo: &SloDefinition) -> Self {
        let now = now_ms();
        // Error budget = 1 - success_rate_target (e.g., 0.001 for 99.9%)
        let total_budget = 1.0 - slo.success_rate_target;
        Self {
            total_budget,
            remaining_budget: total_budget,
            consumed_budget: 0.0,
            window_start_ms: now,
            window_ms: slo.error_budget_window_ms,
            total_errors: 0,
            total_requests: 0,
            current_burn_rate: 0.0,
            active_alerts: Vec::new(),
        }
    }

    /// Record a request outcome.
    pub fn record(&mut self, success: bool, burn_rate_thresholds: &[f64]) {
        self.total_requests += 1;
        if !success {
            self.total_errors += 1;
        }
        self.recalculate(burn_rate_thresholds);
    }

    /// Recalculate budget consumption and burn rate.
    pub fn recalculate(&mut self, burn_rate_thresholds: &[f64]) {
        if self.total_requests == 0 {
            return;
        }
        let error_rate = self.total_errors as f64 / self.total_requests as f64;
        let allowed_error_rate = self.total_budget;

        // Burn rate = actual error rate / allowed error rate
        if allowed_error_rate > 0.0 {
            self.current_burn_rate = error_rate / allowed_error_rate;
        } else {
            self.current_burn_rate = if error_rate > 0.0 { f64::INFINITY } else { 0.0 };
        }

        // Calculate consumed budget based on time elapsed
        let elapsed = now_ms().saturating_sub(self.window_start_ms);
        let _time_fraction = if self.window_ms > 0 {
            (elapsed as f64 / self.window_ms as f64).min(1.0)
        } else {
            1.0
        };

        // Budget consumed = error_rate * total_requests * time_fraction adjustment
        self.consumed_budget = error_rate;
        self.remaining_budget = (self.total_budget - self.consumed_budget).max(0.0);

        // Check burn rate alerts
        self.active_alerts.clear();
        for &threshold in burn_rate_thresholds {
            if self.current_burn_rate >= threshold {
                let severity = if self.current_burn_rate >= threshold * 5.0 {
                    AlertSeverity::Page
                } else if self.current_burn_rate >= threshold * 2.0 {
                    AlertSeverity::Critical
                } else if self.current_burn_rate >= threshold * 1.5 {
                    AlertSeverity::Warning
                } else {
                    AlertSeverity::Info
                };
                self.active_alerts.push(BurnRateAlert {
                    threshold,
                    current_burn_rate: self.current_burn_rate,
                    triggered_at_ms: now_ms(),
                    severity,
                });
            }
        }
    }

    /// Reset the error budget window.
    pub fn reset(&mut self) {
        self.remaining_budget = self.total_budget;
        self.consumed_budget = 0.0;
        self.window_start_ms = now_ms();
        self.total_errors = 0;
        self.total_requests = 0;
        self.current_burn_rate = 0.0;
        self.active_alerts.clear();
    }

    /// Check if the error budget is exhausted.
    pub fn is_exhausted(&self) -> bool {
        self.remaining_budget <= 0.0
    }

    /// Get the budget consumption percentage (0.0 - 100.0+).
    pub fn consumption_percentage(&self) -> f64 {
        if self.total_budget <= 0.0 {
            return 0.0;
        }
        (self.consumed_budget / self.total_budget) * 100.0
    }
}

// ─── Throughput Tracker ────────────────────────────────────────────────────

/// Tracks workflow throughput with rate calculations.
#[derive(Debug, Clone)]
pub struct ThroughputTracker {
    /// Total requests observed.
    pub total_requests: u64,
    /// Total successful requests.
    pub total_success: u64,
    /// Total failed requests.
    pub total_failures: u64,
    /// Requests in the current window.
    pub window_requests: u64,
    /// Window start timestamp (ms).
    pub window_start_ms: u64,
    /// Window duration for rate calculation (ms).
    pub rate_window_ms: u64,
    /// Current requests per second.
    pub current_rps: f64,
    /// Peak requests per second observed.
    pub peak_rps: f64,
    /// Per-second request counts for recent history (last 60 seconds).
    pub recent_counts: Vec<u64>,
    /// Last second timestamp.
    pub last_second_ms: u64,
}

impl ThroughputTracker {
    pub fn new() -> Self {
        let now = now_ms();
        Self {
            total_requests: 0,
            total_success: 0,
            total_failures: 0,
            window_requests: 0,
            window_start_ms: now,
            rate_window_ms: 1000,
            current_rps: 0.0,
            peak_rps: 0.0,
            recent_counts: vec![0; 60],
            last_second_ms: now,
        }
    }

    /// Record a request.
    pub fn record(&mut self, success: bool) {
        let now = now_ms();
        self.total_requests += 1;
        self.window_requests += 1;
        if success {
            self.total_success += 1;
        } else {
            self.total_failures += 1;
        }

        // Update per-second tracking
        let elapsed = now.saturating_sub(self.last_second_ms);
        if elapsed >= 1000 {
            let seconds_elapsed = (elapsed / 1000).min(60) as usize;
            // Shift history
            for _ in 0..seconds_elapsed.min(self.recent_counts.len()) {
                self.recent_counts.rotate_right(1);
                self.recent_counts[0] = 0;
            }
            self.recent_counts[0] = self.window_requests;
            self.window_requests = 0;
            self.last_second_ms = now;
        }

        // Calculate current RPS
        let window_elapsed = now.saturating_sub(self.window_start_ms);
        if window_elapsed > 0 {
            self.current_rps =
                self.total_requests as f64 / (window_elapsed as f64 / 1000.0);
        }
        if self.current_rps > self.peak_rps {
            self.peak_rps = self.current_rps;
        }
    }

    /// Get the success rate.
    pub fn success_rate(&self) -> f64 {
        if self.total_requests == 0 {
            return 1.0;
        }
        self.total_success as f64 / self.total_requests as f64
    }

    /// Get the failure rate.
    pub fn failure_rate(&self) -> f64 {
        1.0 - self.success_rate()
    }

    /// Reset the tracker.
    pub fn reset(&mut self) {
        let now = now_ms();
        self.total_requests = 0;
        self.total_success = 0;
        self.total_failures = 0;
        self.window_requests = 0;
        self.window_start_ms = now;
        self.current_rps = 0.0;
        self.peak_rps = 0.0;
        self.recent_counts.fill(0);
        self.last_second_ms = now;
    }
}

impl Default for ThroughputTracker {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Per-Workflow Execution Stats ──────────────────────────────────────────

/// Execution statistics for a single workflow type.
#[derive(Debug, Clone)]
pub struct WorkflowExecutionStats {
    /// Workflow type identifier.
    pub workflow_type_id: u64,
    /// Latency histogram.
    pub latency: LatencyHistogram,
    /// Throughput tracker.
    pub throughput: ThroughputTracker,
    /// SLO definition (if any).
    pub slo: Option<SloDefinition>,
    /// Error budget (if SLO is defined).
    pub error_budget: Option<ErrorBudget>,
    /// Total workflows started.
    pub started: u64,
    /// Total workflows completed successfully.
    pub completed: u64,
    /// Total workflows failed.
    pub failed: u64,
    /// Total workflows canceled.
    pub canceled: u64,
    /// Total workflows timed out.
    pub timed_out: u64,
    /// Total workflows terminated.
    pub terminated: u64,
    /// Active workflow count.
    pub active: u64,
}

impl WorkflowExecutionStats {
    pub fn new(workflow_type_id: u64) -> Self {
        Self {
            workflow_type_id,
            latency: LatencyHistogram::new(),
            throughput: ThroughputTracker::new(),
            slo: None,
            error_budget: None,
            started: 0,
            completed: 0,
            failed: 0,
            canceled: 0,
            timed_out: 0,
            terminated: 0,
            active: 0,
        }
    }

    /// Set an SLO for this workflow type.
    pub fn set_slo(&mut self, slo: SloDefinition) {
        self.error_budget = Some(ErrorBudget::new(&slo));
        self.slo = Some(slo);
    }

    /// Record a workflow start.
    pub fn record_start(&mut self) {
        self.started += 1;
        self.active += 1;
        self.throughput.record(true);
    }

    /// Record a successful completion with latency.
    pub fn record_completion(&mut self, latency_ms: u64) {
        self.completed += 1;
        self.active = self.active.saturating_sub(1);
        self.latency.observe(latency_ms);
        self.throughput.record(true);
        if let Some(budget) = &mut self.error_budget {
            let thresholds = self
                .slo
                .as_ref()
                .map(|s| s.burn_rate_alert_thresholds.clone())
                .unwrap_or_default();
            budget.record(true, &thresholds);
        }
    }

    /// Record a failure.
    pub fn record_failure(&mut self) {
        self.failed += 1;
        self.active = self.active.saturating_sub(1);
        self.throughput.record(false);
        if let Some(budget) = &mut self.error_budget {
            let thresholds = self
                .slo
                .as_ref()
                .map(|s| s.burn_rate_alert_thresholds.clone())
                .unwrap_or_default();
            budget.record(false, &thresholds);
        }
    }

    /// Record a cancellation.
    pub fn record_cancellation(&mut self) {
        self.canceled += 1;
        self.active = self.active.saturating_sub(1);
    }

    /// Record a timeout.
    pub fn record_timeout(&mut self) {
        self.timed_out += 1;
        self.active = self.active.saturating_sub(1);
        self.throughput.record(false);
    }

    /// Record a termination.
    pub fn record_termination(&mut self) {
        self.terminated += 1;
        self.active = self.active.saturating_sub(1);
    }

    /// Get the overall completion rate.
    pub fn completion_rate(&self) -> f64 {
        if self.started == 0 {
            return 0.0;
        }
        self.completed as f64 / self.started as f64
    }

    /// Get the overall failure rate.
    pub fn failure_rate(&self) -> f64 {
        if self.started == 0 {
            return 0.0;
        }
        self.failed as f64 / self.started as f64
    }
}

// ─── Workflow Execution Tracker ────────────────────────────────────────────

/// Global tracker for all workflow execution statistics.
pub struct WorkflowExecutionTracker {
    /// Per-workflow-type statistics.
    stats_by_type: RwLock<HashMap<u64, WorkflowExecutionStats>>,
    /// Global counters.
    global_started: AtomicU64,
    global_completed: AtomicU64,
    global_failed: AtomicU64,
    global_canceled: AtomicU64,
    global_timed_out: AtomicU64,
    global_terminated: AtomicU64,
    /// Global latency histogram.
    global_latency: RwLock<LatencyHistogram>,
}

impl WorkflowExecutionTracker {
    pub fn new() -> Self {
        Self {
            stats_by_type: RwLock::new(HashMap::new()),
            global_started: AtomicU64::new(0),
            global_completed: AtomicU64::new(0),
            global_failed: AtomicU64::new(0),
            global_canceled: AtomicU64::new(0),
            global_timed_out: AtomicU64::new(0),
            global_terminated: AtomicU64::new(0),
            global_latency: RwLock::new(LatencyHistogram::new()),
        }
    }

    /// Set SLO for a workflow type.
    pub fn set_slo(&self, workflow_type_id: u64, slo: SloDefinition) {
        let mut stats = self.stats_by_type.write().unwrap();
        let entry = stats
            .entry(workflow_type_id)
            .or_insert_with(|| WorkflowExecutionStats::new(workflow_type_id));
        entry.set_slo(slo);
    }

    /// Record a workflow start.
    pub fn record_start(&self, workflow_type_id: u64) {
        self.global_started.fetch_add(1, Ordering::Relaxed);
        let mut stats = self.stats_by_type.write().unwrap();
        let entry = stats
            .entry(workflow_type_id)
            .or_insert_with(|| WorkflowExecutionStats::new(workflow_type_id));
        entry.record_start();
    }

    /// Record a successful workflow completion.
    pub fn record_completion(&self, workflow_type_id: u64, latency_ms: u64) {
        self.global_completed.fetch_add(1, Ordering::Relaxed);
        self.global_latency.write().unwrap().observe(latency_ms);
        let mut stats = self.stats_by_type.write().unwrap();
        let entry = stats
            .entry(workflow_type_id)
            .or_insert_with(|| WorkflowExecutionStats::new(workflow_type_id));
        entry.record_completion(latency_ms);
    }

    /// Record a workflow failure.
    pub fn record_failure(&self, workflow_type_id: u64) {
        self.global_failed.fetch_add(1, Ordering::Relaxed);
        let mut stats = self.stats_by_type.write().unwrap();
        let entry = stats
            .entry(workflow_type_id)
            .or_insert_with(|| WorkflowExecutionStats::new(workflow_type_id));
        entry.record_failure();
    }

    /// Record a workflow cancellation.
    pub fn record_cancellation(&self, workflow_type_id: u64) {
        self.global_canceled.fetch_add(1, Ordering::Relaxed);
        let mut stats = self.stats_by_type.write().unwrap();
        let entry = stats
            .entry(workflow_type_id)
            .or_insert_with(|| WorkflowExecutionStats::new(workflow_type_id));
        entry.record_cancellation();
    }

    /// Record a workflow timeout.
    pub fn record_timeout(&self, workflow_type_id: u64) {
        self.global_timed_out.fetch_add(1, Ordering::Relaxed);
        let mut stats = self.stats_by_type.write().unwrap();
        let entry = stats
            .entry(workflow_type_id)
            .or_insert_with(|| WorkflowExecutionStats::new(workflow_type_id));
        entry.record_timeout();
    }

    /// Record a workflow termination.
    pub fn record_termination(&self, workflow_type_id: u64) {
        self.global_terminated.fetch_add(1, Ordering::Relaxed);
        let mut stats = self.stats_by_type.write().unwrap();
        let entry = stats
            .entry(workflow_type_id)
            .or_insert_with(|| WorkflowExecutionStats::new(workflow_type_id));
        entry.record_termination();
    }

    /// Get stats for a specific workflow type.
    pub fn get_stats(&self, workflow_type_id: u64) -> Option<WorkflowExecutionStats> {
        self.stats_by_type.read().unwrap().get(&workflow_type_id).cloned()
    }

    /// Get global summary statistics.
    pub fn global_summary(&self) -> TrackerGlobalSummary {
        let started = self.global_started.load(Ordering::Relaxed);
        let completed = self.global_completed.load(Ordering::Relaxed);
        let failed = self.global_failed.load(Ordering::Relaxed);
        let canceled = self.global_canceled.load(Ordering::Relaxed);
        let timed_out = self.global_timed_out.load(Ordering::Relaxed);
        let terminated = self.global_terminated.load(Ordering::Relaxed);
        let latency = self.global_latency.read().unwrap();

        TrackerGlobalSummary {
            started,
            completed,
            failed,
            canceled,
            timed_out,
            terminated,
            active: started.saturating_sub(completed + failed + canceled + timed_out + terminated),
            global_p50_ms: latency.percentile(0.50),
            global_p95_ms: latency.percentile(0.95),
            global_p99_ms: latency.percentile(0.99),
            global_mean_ms: latency.mean_ms(),
        }
    }

    /// Get all workflow types being tracked.
    pub fn tracked_types(&self) -> Vec<u64> {
        self.stats_by_type.read().unwrap().keys().copied().collect()
    }

    /// Get SLO compliance report for all tracked workflow types.
    pub fn slo_compliance_report(&self) -> Vec<SloComplianceEntry> {
        let stats = self.stats_by_type.read().unwrap();
        stats
            .values()
            .filter_map(|s| {
                let slo = s.slo.as_ref()?;
                let budget = s.error_budget.as_ref()?;
                let p99 = s.latency.percentile(slo.latency_percentile);
                let latency_met = p99 <= slo.latency_target_ms;
                let success_rate = s.throughput.success_rate();
                let success_met = success_rate >= slo.success_rate_target;

                Some(SloComplianceEntry {
                    workflow_type_id: s.workflow_type_id,
                    slo_name: slo.name.clone(),
                    latency_p99_ms: p99,
                    latency_target_ms: slo.latency_target_ms,
                    latency_compliant: latency_met,
                    actual_success_rate: success_rate,
                    target_success_rate: slo.success_rate_target,
                    success_compliant: success_met,
                    error_budget_remaining_pct: budget.remaining_budget / budget.total_budget * 100.0,
                    current_burn_rate: budget.current_burn_rate,
                    active_alerts: budget.active_alerts.len(),
                })
            })
            .collect()
    }

    /// Reset all statistics.
    pub fn reset_all(&self) {
        self.stats_by_type.write().unwrap().clear();
        self.global_started.store(0, Ordering::Relaxed);
        self.global_completed.store(0, Ordering::Relaxed);
        self.global_failed.store(0, Ordering::Relaxed);
        self.global_canceled.store(0, Ordering::Relaxed);
        self.global_timed_out.store(0, Ordering::Relaxed);
        self.global_terminated.store(0, Ordering::Relaxed);
        self.global_latency.write().unwrap().reset();
    }
}

impl Default for WorkflowExecutionTracker {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Report Types ──────────────────────────────────────────────────────────

/// Global summary of all workflow executions.
#[derive(Debug, Clone)]
pub struct TrackerGlobalSummary {
    pub started: u64,
    pub completed: u64,
    pub failed: u64,
    pub canceled: u64,
    pub timed_out: u64,
    pub terminated: u64,
    pub active: u64,
    pub global_p50_ms: u64,
    pub global_p95_ms: u64,
    pub global_p99_ms: u64,
    pub global_mean_ms: f64,
}

/// SLO compliance report entry for a single workflow type.
#[derive(Debug, Clone)]
pub struct SloComplianceEntry {
    pub workflow_type_id: u64,
    pub slo_name: String,
    pub latency_p99_ms: u64,
    pub latency_target_ms: u64,
    pub latency_compliant: bool,
    pub actual_success_rate: f64,
    pub target_success_rate: f64,
    pub success_compliant: bool,
    pub error_budget_remaining_pct: f64,
    pub current_burn_rate: f64,
    pub active_alerts: usize,
}

// ─── Helpers ───────────────────────────────────────────────────────────────

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ─── Latency Histogram ─────────────────────────────────────────────

    #[test]
    fn test_histogram_basic() {
        let mut h = LatencyHistogram::new();
        h.observe(50);
        h.observe(100);
        h.observe(250);

        assert_eq!(h.total_count, 3);
        assert_eq!(h.total_sum_ms, 400);
        assert_eq!(h.min_ms, 50);
        assert_eq!(h.max_ms, 250);
        assert!((h.mean_ms() - 133.33).abs() < 1.0);
    }

    #[test]
    fn test_histogram_percentiles() {
        let mut h = LatencyHistogram::new();
        for i in 1..=100 {
            h.observe(i); // 1ms to 100ms
        }

        let p50 = h.percentile(0.50);
        let p95 = h.percentile(0.95);
        let p99 = h.percentile(0.99);

        assert!(p50 >= 45 && p50 <= 55); // ~50ms
        assert!(p95 >= 90 && p95 <= 100); // ~95ms
        assert!(p99 >= 95 && p99 <= 100); // ~99ms
    }

    #[test]
    fn test_histogram_overflow_bucket() {
        let mut h = LatencyHistogram::new();
        h.observe(100_000); // Way above max bucket (60s)
        assert_eq!(h.total_count, 1);
        assert_eq!(h.max_ms, 100_000);
        // Should be in the overflow bucket
        assert_eq!(*h.counts.last().unwrap(), 1);
    }

    #[test]
    fn test_histogram_reset() {
        let mut h = LatencyHistogram::new();
        h.observe(100);
        h.observe(200);
        h.reset();
        assert_eq!(h.total_count, 0);
        assert_eq!(h.total_sum_ms, 0);
        assert_eq!(h.min_ms, u64::MAX);
        assert_eq!(h.max_ms, 0);
    }

    #[test]
    fn test_histogram_empty_percentile() {
        let h = LatencyHistogram::new();
        assert_eq!(h.percentile(0.99), 0);
        assert_eq!(h.mean_ms(), 0.0);
    }

    // ─── Error Budget ──────────────────────────────────────────────────

    #[test]
    fn test_error_budget_no_errors() {
        let slo = SloDefinition::standard();
        let mut budget = ErrorBudget::new(&slo);

        // Record 1000 successful requests
        for _ in 0..1000 {
            budget.record(true, &slo.burn_rate_alert_thresholds);
        }

        assert_eq!(budget.total_requests, 1000);
        assert_eq!(budget.total_errors, 0);
        assert_eq!(budget.current_burn_rate, 0.0);
        assert!(!budget.is_exhausted());
        assert!(budget.active_alerts.is_empty());
    }

    #[test]
    fn test_error_budget_with_failures() {
        let slo = SloDefinition::standard(); // 99.9% = 0.001 error budget
        let mut budget = ErrorBudget::new(&slo);

        // Record 990 successes and 10 failures (1% error rate)
        for _ in 0..990 {
            budget.record(true, &slo.burn_rate_alert_thresholds);
        }
        for _ in 0..10 {
            budget.record(false, &slo.burn_rate_alert_thresholds);
        }

        assert_eq!(budget.total_requests, 1000);
        assert_eq!(budget.total_errors, 10);
        // Error rate = 0.01, allowed = 0.001, burn rate = 10.0
        assert!((budget.current_burn_rate - 10.0).abs() < 0.1);
        // Should have burn rate alerts
        assert!(!budget.active_alerts.is_empty());
    }

    #[test]
    fn test_error_budget_reset() {
        let slo = SloDefinition::standard();
        let mut budget = ErrorBudget::new(&slo);

        for _ in 0..100 {
            budget.record(false, &slo.burn_rate_alert_thresholds);
        }
        assert!(budget.total_errors > 0);

        budget.reset();
        assert_eq!(budget.total_errors, 0);
        assert_eq!(budget.total_requests, 0);
        assert_eq!(budget.current_burn_rate, 0.0);
    }

    #[test]
    fn test_error_budget_consumption() {
        let slo = SloDefinition {
            name: "test".into(),
            latency_percentile: 0.99,
            latency_target_ms: 1000,
            success_rate_target: 0.99, // 1% error budget
            error_budget_window_ms: 1000,
            burn_rate_alert_thresholds: vec![1.0, 5.0],
        };
        let mut budget = ErrorBudget::new(&slo);
        assert!((budget.total_budget - 0.01).abs() < 0.001);

        // 5% error rate (5x the budget)
        for _ in 0..95 {
            budget.record(true, &slo.burn_rate_alert_thresholds);
        }
        for _ in 0..5 {
            budget.record(false, &slo.burn_rate_alert_thresholds);
        }

        // Burn rate should be ~5x
        assert!(budget.current_burn_rate > 4.0);
        assert!(budget.consumption_percentage() > 100.0); // Over-consuming
    }

    // ─── Throughput Tracker ────────────────────────────────────────────

    #[test]
    fn test_throughput_basic() {
        let mut t = ThroughputTracker::new();
        t.record(true);
        t.record(true);
        t.record(false);

        assert_eq!(t.total_requests, 3);
        assert_eq!(t.total_success, 2);
        assert_eq!(t.total_failures, 1);
        assert!((t.success_rate() - 0.666).abs() < 0.01);
        assert!((t.failure_rate() - 0.333).abs() < 0.01);
    }

    #[test]
    fn test_throughput_reset() {
        let mut t = ThroughputTracker::new();
        t.record(true);
        t.record(true);
        t.reset();
        assert_eq!(t.total_requests, 0);
        assert_eq!(t.total_success, 0);
        assert_eq!(t.peak_rps, 0.0);
    }

    #[test]
    fn test_throughput_empty() {
        let t = ThroughputTracker::new();
        assert_eq!(t.success_rate(), 1.0);
        assert_eq!(t.failure_rate(), 0.0);
    }

    // ─── Workflow Execution Stats ──────────────────────────────────────

    #[test]
    fn test_workflow_stats_lifecycle() {
        let mut stats = WorkflowExecutionStats::new(1);
        stats.record_start();
        stats.record_start();
        stats.record_completion(100);
        stats.record_failure();

        assert_eq!(stats.started, 2);
        assert_eq!(stats.completed, 1);
        assert_eq!(stats.failed, 1);
        assert_eq!(stats.active, 0);
        assert_eq!(stats.latency.total_count, 1);
        assert!((stats.completion_rate() - 0.5).abs() < 0.01);
        assert!((stats.failure_rate() - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_workflow_stats_with_slo() {
        let mut stats = WorkflowExecutionStats::new(1);
        stats.set_slo(SloDefinition::standard());

        for _ in 0..100 {
            stats.record_start();
            stats.record_completion(100);
        }
        for _ in 0..5 {
            stats.record_start();
            stats.record_failure();
        }

        assert!(stats.error_budget.is_some());
        let budget = stats.error_budget.as_ref().unwrap();
        assert_eq!(budget.total_requests, 105);
        assert_eq!(budget.total_errors, 5);
    }

    #[test]
    fn test_workflow_stats_all_terminal_states() {
        let mut stats = WorkflowExecutionStats::new(1);
        stats.record_start();
        stats.record_cancellation();
        stats.record_start();
        stats.record_timeout();
        stats.record_start();
        stats.record_termination();

        assert_eq!(stats.started, 3);
        assert_eq!(stats.canceled, 1);
        assert_eq!(stats.timed_out, 1);
        assert_eq!(stats.terminated, 1);
        assert_eq!(stats.active, 0);
    }

    // ─── Workflow Execution Tracker ────────────────────────────────────

    #[test]
    fn test_tracker_record_all() {
        let tracker = WorkflowExecutionTracker::new();

        tracker.record_start(1);
        tracker.record_start(1);
        tracker.record_completion(1, 100);
        tracker.record_failure(1);
        tracker.record_start(2);
        tracker.record_completion(2, 200);

        let summary = tracker.global_summary();
        assert_eq!(summary.started, 3);
        assert_eq!(summary.completed, 2);
        assert_eq!(summary.failed, 1);
        assert_eq!(summary.active, 0);
    }

    #[test]
    fn test_tracker_slo_compliance() {
        let tracker = WorkflowExecutionTracker::new();
        tracker.set_slo(1, SloDefinition::standard());

        for _ in 0..100 {
            tracker.record_start(1);
            tracker.record_completion(1, 50);
        }

        let report = tracker.slo_compliance_report();
        assert_eq!(report.len(), 1);
        assert!(report[0].latency_compliant);
        assert!(report[0].success_compliant);
        assert!(report[0].error_budget_remaining_pct > 0.0);
    }

    #[test]
    fn test_tracker_tracked_types() {
        let tracker = WorkflowExecutionTracker::new();
        tracker.record_start(1);
        tracker.record_start(2);
        tracker.record_start(3);

        let types = tracker.tracked_types();
        assert_eq!(types.len(), 3);
        assert!(types.contains(&1));
        assert!(types.contains(&2));
        assert!(types.contains(&3));
    }

    #[test]
    fn test_tracker_reset() {
        let tracker = WorkflowExecutionTracker::new();
        tracker.record_start(1);
        tracker.record_completion(1, 100);
        tracker.reset_all();

        let summary = tracker.global_summary();
        assert_eq!(summary.started, 0);
        assert_eq!(summary.completed, 0);
        assert!(tracker.tracked_types().is_empty());
    }

    #[test]
    fn test_tracker_per_type_stats() {
        let tracker = WorkflowExecutionTracker::new();
        tracker.record_start(1);
        tracker.record_start(1);
        tracker.record_completion(1, 50);
        tracker.record_failure(1);

        let stats = tracker.get_stats(1).unwrap();
        assert_eq!(stats.started, 2);
        assert_eq!(stats.completed, 1);
        assert_eq!(stats.failed, 1);
        assert_eq!(stats.latency.total_count, 1);
    }

    #[test]
    fn test_tracker_no_slo_compliance_without_slo() {
        let tracker = WorkflowExecutionTracker::new();
        tracker.record_start(1);
        tracker.record_completion(1, 100);

        let report = tracker.slo_compliance_report();
        assert!(report.is_empty()); // No SLO set, no report entries
    }

    #[test]
    fn test_slo_presets() {
        let standard = SloDefinition::standard();
        assert_eq!(standard.latency_target_ms, 5000);
        assert!((standard.success_rate_target - 0.999).abs() < 0.001);

        let strict = SloDefinition::strict();
        assert_eq!(strict.latency_target_ms, 1000);
        assert!((strict.success_rate_target - 0.9999).abs() < 0.0001);

        let relaxed = SloDefinition::relaxed();
        assert_eq!(relaxed.latency_target_ms, 30_000);
        assert!((relaxed.success_rate_target - 0.99).abs() < 0.01);
    }

    #[test]
    fn test_global_summary_latencies() {
        let tracker = WorkflowExecutionTracker::new();
        for _ in 0..100 {
            tracker.record_start(1);
            tracker.record_completion(1, 100);
        }
        let summary = tracker.global_summary();
        assert!(summary.global_p50_ms > 0);
        assert!(summary.global_p99_ms >= summary.global_p50_ms);
        assert!(summary.global_mean_ms > 0.0);
    }

    #[test]
    fn test_cancellation_and_timeout_tracking() {
        let tracker = WorkflowExecutionTracker::new();
        tracker.record_start(1);
        tracker.record_cancellation(1);
        tracker.record_start(1);
        tracker.record_timeout(1);
        tracker.record_start(1);
        tracker.record_termination(1);

        let summary = tracker.global_summary();
        assert_eq!(summary.started, 3);
        assert_eq!(summary.canceled, 1);
        assert_eq!(summary.timed_out, 1);
        assert_eq!(summary.terminated, 1);
    }
}
