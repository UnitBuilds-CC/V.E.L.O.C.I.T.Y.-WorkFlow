//! Metrics export subsystem for the VELOCITY workflow engine.
//!
//! Captures a point-in-time snapshot of all engine metrics and exports them
//! in JSON, Prometheus text, or StatsD formats. Designed for scraping by
//! monitoring systems (Prometheus, Datadog, Grafana) or for on-demand
//! diagnostic dumps.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::metrics::MetricsRegistry;

// ─── MetricsSnapshot ────────────────────────────────────────────────────────

/// A point-in-time snapshot of all engine metrics.
///
/// Created by [`MetricsSnapshot::capture`] from a [`MetricsRegistry`].
/// All values are copied at capture time — the snapshot is immutable.
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    /// Counters: monotonically increasing values.
    pub counters: HashMap<String, u64>,
    /// Gauges: values that can increase or decrease.
    pub gauges: HashMap<String, i64>,
    /// Histograms: (name, count, sum) tuples.
    pub histograms: HashMap<String, (u64, u64)>,
    /// Unix timestamp (seconds) when the snapshot was captured.
    pub timestamp: u64,
}

impl MetricsSnapshot {
    /// Capture a snapshot from the given metrics registry.
    pub fn capture(registry: &MetricsRegistry) -> Self {
        let counters: HashMap<String, u64> = registry.counter_snapshot().into_iter().collect();
        let gauges: HashMap<String, i64> = registry.gauge_snapshot().into_iter().collect();
        let histograms: HashMap<String, (u64, u64)> = registry
            .histogram_snapshot()
            .into_iter()
            .map(|(name, count, sum)| (name, (count, sum)))
            .collect();

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Self {
            counters,
            gauges,
            histograms,
            timestamp,
        }
    }

    /// Create a snapshot from raw values (useful for testing).
    pub fn from_raw(
        counters: HashMap<String, u64>,
        gauges: HashMap<String, i64>,
        histograms: HashMap<String, (u64, u64)>,
    ) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Self {
            counters,
            gauges,
            histograms,
            timestamp,
        }
    }

    // ─── Export: JSON ───────────────────────────────────────────────────────

    /// Export the snapshot as a JSON string.
    pub fn export_json(&self) -> String {
        let mut out = String::with_capacity(512);
        out.push_str("{\"timestamp\":");
        push_u64(&mut out, self.timestamp);
        out.push_str(",\"counters\":{");
        push_map_u64(&mut out, &self.counters);
        out.push_str("},\"gauges\":{");
        push_map_i64(&mut out, &self.gauges);
        out.push_str("},\"histograms\":{");
        push_map_hist(&mut out, &self.histograms);
        out.push_str("}}");
        out
    }

    // ─── Export: Prometheus text format ─────────────────────────────────────

    /// Export the snapshot in Prometheus text exposition format.
    ///
    /// See: <https://prometheus.io/docs/instrumenting/exposition_formats/>
    pub fn export_prometheus(&self) -> String {
        let mut out = String::with_capacity(512);

        for (name, value) in &self.counters {
            let safe = prometheus_name(name);
            out.push_str("# TYPE ");
            out.push_str(&safe);
            out.push_str(" counter\n");
            out.push_str(&safe);
            out.push(' ');
            push_u64(&mut out, *value);
            out.push('\n');
        }

        for (name, value) in &self.gauges {
            let safe = prometheus_name(name);
            out.push_str("# TYPE ");
            out.push_str(&safe);
            out.push_str(" gauge\n");
            out.push_str(&safe);
            out.push(' ');
            push_i64(&mut out, *value);
            out.push('\n');
        }

        for (name, (count, sum)) in &self.histograms {
            let safe = prometheus_name(name);
            out.push_str("# TYPE ");
            out.push_str(&safe);
            out.push_str(" histogram\n");
            out.push_str(&safe);
            out.push_str("_count ");
            push_u64(&mut out, *count);
            out.push('\n');
            out.push_str(&safe);
            out.push_str("_sum ");
            push_u64(&mut out, *sum);
            out.push('\n');
        }

        out
    }

    // ─── Export: StatsD format ──────────────────────────────────────────────

    /// Export the snapshot in StatsD line protocol format.
    ///
    /// Format: `metric_name:value|type` where type is `c` (counter), `g` (gauge).
    pub fn export_statsd(&self) -> String {
        let mut out = String::with_capacity(512);

        for (name, value) in &self.counters {
            out.push_str(name);
            out.push(':');
            push_u64(&mut out, *value);
            out.push_str("|c\n");
        }

        for (name, value) in &self.gauges {
            out.push_str(name);
            out.push(':');
            push_i64(&mut out, *value);
            out.push_str("|g\n");
        }

        for (name, (count, sum)) in &self.histograms {
            out.push_str(name);
            out.push_str(".count:");
            push_u64(&mut out, *count);
            out.push_str("|c\n");
            out.push_str(name);
            out.push_str(".sum:");
            push_u64(&mut out, *sum);
            out.push_str("|c\n");
        }

        out
    }
}

// ─── Helpers ────────────────────────────────────────────────────────────────

/// Sanitise a metric name for Prometheus (replace non-alphanumeric with `_`).
fn prometheus_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn push_u64(out: &mut String, v: u64) {
    // Use a small stack buffer to avoid allocation.
    let mut buf = [0u8; 20];
    let s = format_u64(v, &mut buf);
    out.push_str(s);
}

fn push_i64(out: &mut String, v: i64) {
    let s = if v < 0 {
        let mut buf = [0u8; 20];
        let abs = format_u64(v.unsigned_abs(), &mut buf);
        format!("-{}", abs)
    } else {
        let mut buf = [0u8; 20];
        format_u64(v as u64, &mut buf).to_string()
    };
    out.push_str(&s);
}

fn format_u64(mut v: u64, buf: &mut [u8; 20]) -> &str {
    if v == 0 {
        return "0";
    }
    let mut i = 20;
    while v > 0 {
        i -= 1;
        buf[i] = b'0' + (v % 10) as u8;
        v /= 10;
    }
    std::str::from_utf8(&buf[i..]).unwrap()
}

fn push_map_u64(out: &mut String, map: &HashMap<String, u64>) {
    let mut first = true;
    for (k, v) in map {
        if !first {
            out.push(',');
        }
        first = false;
        out.push('"');
        out.push_str(k);
        out.push_str("\":");
        push_u64(out, *v);
    }
}

fn push_map_i64(out: &mut String, map: &HashMap<String, i64>) {
    let mut first = true;
    for (k, v) in map {
        if !first {
            out.push(',');
        }
        first = false;
        out.push('"');
        out.push_str(k);
        out.push_str("\":");
        push_i64(out, *v);
    }
}

fn push_map_hist(out: &mut String, map: &HashMap<String, (u64, u64)>) {
    let mut first = true;
    for (k, (count, sum)) in map {
        if !first {
            out.push(',');
        }
        first = false;
        out.push('"');
        out.push_str(k);
        out.push_str("\":{\"count\":");
        push_u64(out, *count);
        out.push_str(",\"sum\":");
        push_u64(out, *sum);
        out.push('}');
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_snapshot() -> MetricsSnapshot {
        let mut counters = HashMap::new();
        counters.insert("workflow_starts_total".into(), 1500);
        counters.insert("workflow_completions_total".into(), 1480);

        let mut gauges = HashMap::new();
        gauges.insert("active_workflows".into(), 20);
        gauges.insert("task_queue_depth".into(), 5);

        let mut histograms = HashMap::new();
        histograms.insert("workflow_duration_ms".into(), (1480, 7_400_000));

        MetricsSnapshot::from_raw(counters, gauges, histograms)
    }

    #[test]
    fn test_export_json_contains_all_sections() {
        let snap = sample_snapshot();
        let json = snap.export_json();

        assert!(json.contains("\"timestamp\":"));
        assert!(json.contains("\"counters\":{"));
        assert!(json.contains("\"gauges\":{"));
        assert!(json.contains("\"histograms\":{"));
        assert!(json.contains("\"workflow_starts_total\":1500"));
        assert!(json.contains("\"active_workflows\":20"));
        assert!(json.contains("\"workflow_duration_ms\""));
    }

    #[test]
    fn test_export_prometheus_format() {
        let snap = sample_snapshot();
        let prom = snap.export_prometheus();

        assert!(prom.contains("# TYPE workflow_starts_total counter"));
        assert!(prom.contains("workflow_starts_total 1500"));
        assert!(prom.contains("# TYPE active_workflows gauge"));
        assert!(prom.contains("active_workflows 20"));
        assert!(prom.contains("# TYPE workflow_duration_ms histogram"));
        assert!(prom.contains("workflow_duration_ms_count 1480"));
        assert!(prom.contains("workflow_duration_ms_sum 7400000"));
    }

    #[test]
    fn test_export_statsd_format() {
        let snap = sample_snapshot();
        let statsd = snap.export_statsd();

        assert!(statsd.contains("workflow_starts_total:1500|c"));
        assert!(statsd.contains("active_workflows:20|g"));
        assert!(statsd.contains("workflow_duration_ms.count:1480|c"));
        assert!(statsd.contains("workflow_duration_ms.sum:7400000|c"));
    }

    #[test]
    fn test_empty_snapshot() {
        let snap = MetricsSnapshot::from_raw(HashMap::new(), HashMap::new(), HashMap::new());
        let json = snap.export_json();
        assert!(json.contains("\"counters\":{}"));
        assert!(json.contains("\"gauges\":{}"));
        assert!(json.contains("\"histograms\":{}"));

        let prom = snap.export_prometheus();
        assert!(prom.is_empty() || !prom.contains("# TYPE"));

        let statsd = snap.export_statsd();
        assert!(statsd.is_empty());
    }

    #[test]
    fn test_prometheus_name_sanitisation() {
        assert_eq!(prometheus_name("my.metric.name"), "my_metric_name");
        assert_eq!(prometheus_name("already_valid_123"), "already_valid_123");
        assert_eq!(prometheus_name("with-dashes"), "with_dashes");
    }
}
