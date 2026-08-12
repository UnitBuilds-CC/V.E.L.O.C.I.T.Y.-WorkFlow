//! Metrics and telemetry export for the workflow engine.
//! Provides Prometheus-compatible counters, gauges, and histograms.
//! All metrics are stored in Rust with zero GC — can be scraped via FFI.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::RwLock;

/// A named counter that only increments.
pub struct Counter {
    name: String,
    value: AtomicU64,
    labels: HashMap<String, String>,
}

impl Counter {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            value: AtomicU64::new(0),
            labels: HashMap::new(),
        }
    }

    pub fn with_labels(mut self, labels: HashMap<String, String>) -> Self {
        self.labels = labels;
        self
    }

    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }
    pub fn add(&self, n: u64) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// A named gauge that can increase or decrease.
pub struct Gauge {
    name: String,
    value: AtomicI64,
    labels: HashMap<String, String>,
}

impl Gauge {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            value: AtomicI64::new(0),
            labels: HashMap::new(),
        }
    }

    pub fn set(&self, v: i64) {
        self.value.store(v, Ordering::Relaxed);
    }
    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }
    pub fn dec(&self) {
        self.value.fetch_sub(1, Ordering::Relaxed);
    }
    pub fn get(&self) -> i64 {
        self.value.load(Ordering::Relaxed)
    }
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// A simple histogram with fixed buckets.
pub struct Histogram {
    name: String,
    buckets: Vec<f64>,      // upper bounds
    counts: Vec<AtomicU64>, // count per bucket
    sum: AtomicU64,         // sum of all values (as millis for integer metrics)
    count: AtomicU64,
}

impl Histogram {
    pub fn new(name: &str, buckets: Vec<f64>) -> Self {
        let counts = buckets.iter().map(|_| AtomicU64::new(0)).collect();
        Self {
            name: name.to_string(),
            buckets,
            counts,
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    pub fn observe(&self, value: f64) {
        for (i, bound) in self.buckets.iter().enumerate() {
            if value <= *bound {
                self.counts[i].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.sum.fetch_add(value as u64, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
    pub fn sum(&self) -> u64 {
        self.sum.load(Ordering::Relaxed)
    }
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Central metrics registry. All engine metrics are registered here.
pub struct MetricsRegistry {
    counters: RwLock<HashMap<String, Counter>>,
    gauges: RwLock<HashMap<String, Gauge>>,
    histograms: RwLock<HashMap<String, Histogram>>,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        let registry = Self {
            counters: RwLock::new(HashMap::new()),
            gauges: RwLock::new(HashMap::new()),
            histograms: RwLock::new(HashMap::new()),
        };
        registry.register_defaults();
        registry
    }

    fn register_defaults(&self) {
        // Workflow lifecycle counters
        self.register_counter("velocity_workflow_started_total");
        self.register_counter("velocity_workflow_completed_total");
        self.register_counter("velocity_workflow_failed_total");
        self.register_counter("velocity_workflow_canceled_total");
        self.register_counter("velocity_workflow_terminated_total");
        self.register_counter("velocity_workflow_continued_as_new_total");

        // Activity counters
        self.register_counter("velocity_activity_scheduled_total");
        self.register_counter("velocity_activity_completed_total");
        self.register_counter("velocity_activity_failed_total");

        // Task queue counters
        self.register_counter("velocity_tasks_polled_total");
        self.register_counter("velocity_tasks_completed_total");

        // Signal/update counters
        self.register_counter("velocity_signals_received_total");
        self.register_counter("velocity_updates_received_total");
        self.register_counter("velocity_queries_received_total");

        // Gauges
        self.register_gauge("velocity_workflows_running");
        self.register_gauge("velocity_tasks_pending");
        self.register_gauge("velocity_timers_pending");

        // Histograms (latency in ms)
        self.register_histogram(
            "velocity_workflow_duration_ms",
            vec![
                10.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0, 30000.0, 60000.0,
            ],
        );
        self.register_histogram(
            "velocity_activity_duration_ms",
            vec![
                1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 5000.0,
            ],
        );
    }

    pub fn register_counter(&self, name: &str) {
        self.counters
            .write()
            .unwrap()
            .insert(name.to_string(), Counter::new(name));
    }

    pub fn register_gauge(&self, name: &str) {
        self.gauges
            .write()
            .unwrap()
            .insert(name.to_string(), Gauge::new(name));
    }

    pub fn register_histogram(&self, name: &str, buckets: Vec<f64>) {
        self.histograms
            .write()
            .unwrap()
            .insert(name.to_string(), Histogram::new(name, buckets));
    }

    pub fn inc_counter(&self, name: &str) {
        if let Some(c) = self.counters.read().unwrap().get(name) {
            c.inc();
        }
    }

    pub fn add_counter(&self, name: &str, n: u64) {
        if let Some(c) = self.counters.read().unwrap().get(name) {
            c.add(n);
        }
    }

    pub fn get_counter(&self, name: &str) -> u64 {
        self.counters
            .read()
            .unwrap()
            .get(name)
            .map_or(0, |c| c.get())
    }

    pub fn set_gauge(&self, name: &str, value: i64) {
        if let Some(g) = self.gauges.read().unwrap().get(name) {
            g.set(value);
        }
    }

    pub fn get_gauge(&self, name: &str) -> i64 {
        self.gauges.read().unwrap().get(name).map_or(0, |g| g.get())
    }

    pub fn observe_histogram(&self, name: &str, value: f64) {
        if let Some(h) = self.histograms.read().unwrap().get(name) {
            h.observe(value);
        }
    }

    /// Export all metrics in Prometheus text exposition format.
    pub fn export_prometheus(&self) -> String {
        let mut output = String::new();

        // Counters
        let counters = self.counters.read().unwrap();
        for (name, counter) in counters.iter() {
            output.push_str(&format!("# TYPE {} counter\n", name));
            output.push_str(&format!("{} {}\n", name, counter.get()));
        }

        // Gauges
        let gauges = self.gauges.read().unwrap();
        for (name, gauge) in gauges.iter() {
            output.push_str(&format!("# TYPE {} gauge\n", name));
            output.push_str(&format!("{} {}\n", name, gauge.get()));
        }

        // Histograms
        let histograms = self.histograms.read().unwrap();
        for (name, hist) in histograms.iter() {
            output.push_str(&format!("# TYPE {} histogram\n", name));
            for (i, bound) in hist.buckets.iter().enumerate() {
                output.push_str(&format!(
                    "{}_bucket{{le=\"{}\"}} {}\n",
                    name,
                    bound,
                    hist.counts[i].load(Ordering::Relaxed)
                ));
            }
            output.push_str(&format!(
                "{}_bucket{{le=\"+Inf\"}} {}\n",
                name,
                hist.count()
            ));
            output.push_str(&format!("{}_sum {}\n", name, hist.sum()));
            output.push_str(&format!("{}_count {}\n", name, hist.count()));
        }

        output
    }

    /// Get the total number of registered metrics.
    pub fn metric_count(&self) -> usize {
        self.counters.read().unwrap().len()
            + self.gauges.read().unwrap().len()
            + self.histograms.read().unwrap().len()
    }

    /// Return a snapshot of all counter names and their current values.
    pub fn counter_snapshot(&self) -> Vec<(String, u64)> {
        self.counters
            .read()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.get()))
            .collect()
    }

    /// Return a snapshot of all gauge names and their current values.
    pub fn gauge_snapshot(&self) -> Vec<(String, i64)> {
        self.gauges
            .read()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.get()))
            .collect()
    }

    /// Return a snapshot of all histogram names, counts, and sums.
    pub fn histogram_snapshot(&self) -> Vec<(String, u64, u64)> {
        self.histograms
            .read()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.count(), v.sum()))
            .collect()
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter() {
        let c = Counter::new("test_counter");
        assert_eq!(c.get(), 0);
        c.inc();
        assert_eq!(c.get(), 1);
        c.add(5);
        assert_eq!(c.get(), 6);
    }

    #[test]
    fn test_gauge() {
        let g = Gauge::new("test_gauge");
        assert_eq!(g.get(), 0);
        g.set(42);
        assert_eq!(g.get(), 42);
        g.dec();
        assert_eq!(g.get(), 41);
    }

    #[test]
    fn test_histogram() {
        let h = Histogram::new("test_hist", vec![10.0, 50.0, 100.0]);
        h.observe(5.0);
        h.observe(25.0);
        h.observe(75.0);
        assert_eq!(h.count(), 3);
    }

    #[test]
    fn test_registry() {
        let reg = MetricsRegistry::new();
        assert!(reg.metric_count() > 0); // Default metrics registered

        reg.inc_counter("velocity_workflow_started_total");
        assert_eq!(reg.get_counter("velocity_workflow_started_total"), 1);

        reg.set_gauge("velocity_workflows_running", 5);
        assert_eq!(reg.get_gauge("velocity_workflows_running"), 5);
    }

    #[test]
    fn test_prometheus_export() {
        let reg = MetricsRegistry::new();
        reg.inc_counter("velocity_workflow_started_total");
        reg.set_gauge("velocity_workflows_running", 3);

        let output = reg.export_prometheus();
        assert!(output.contains("velocity_workflow_started_total 1"));
        assert!(output.contains("velocity_workflows_running 3"));
        assert!(output.contains("# TYPE"));
    }
}
