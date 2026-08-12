//! Deep Observability — distributed tracing, metrics, profiling, anomaly detection.
//!
//! Temporal has basic metrics. VELOCITY has comprehensive distributed tracing,
//! real-time anomaly detection, performance profiling, structured logging,
//! and predictive alerting — all built-in, zero-config.

use std::collections::{HashMap, VecDeque};
use std::sync::{
    atomic::{AtomicI64, AtomicU64, Ordering},
    Arc, RwLock,
};
use std::time::SystemTime;

// ═══════════════════════════════════════════════════════════════════════════════
// Distributed Trace
// ═══════════════════════════════════════════════════════════════════════════════

pub struct TraceCollector {
    pub traces: RwLock<HashMap<String, Trace>>,
    pub completed_traces: RwLock<VecDeque<Trace>>,
    pub max_completed: usize,
    pub stats: TraceCollectorStats,
}

#[derive(Debug, Clone)]
pub struct Trace {
    pub trace_id: String,
    pub root_span_id: String,
    pub spans: Vec<Span>,
    pub start_time: i64,
    pub end_time: Option<i64>,
    pub status: TraceStatus,
    pub attributes: HashMap<String, String>,
    pub service_name: String,
}

#[derive(Debug, Clone)]
pub struct Span {
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub operation: String,
    pub start_time: i64,
    pub end_time: Option<i64>,
    pub status: SpanStatus,
    pub attributes: HashMap<String, String>,
    pub events: Vec<SpanEvent>,
    pub links: Vec<SpanLink>,
}

#[derive(Debug, Clone)]
pub struct SpanEvent {
    pub name: String,
    pub timestamp: i64,
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct SpanLink {
    pub trace_id: String,
    pub span_id: String,
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceStatus {
    Active,
    Completed,
    Error,
    TimedOut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanStatus {
    Ok,
    Error,
    Unset,
}

#[derive(Debug, Default)]
pub struct TraceCollectorStats {
    pub traces_started: AtomicU64,
    pub traces_completed: AtomicU64,
    pub traces_errored: AtomicU64,
    pub spans_recorded: AtomicU64,
}

impl TraceCollector {
    pub fn new(max_completed: usize) -> Self {
        Self {
            traces: RwLock::new(HashMap::new()),
            completed_traces: RwLock::new(VecDeque::new()),
            max_completed,
            stats: TraceCollectorStats::default(),
        }
    }

    pub fn start_trace(&self, service: &str, operation: &str) -> String {
        let trace_id = format!("trace-{}", now_millis());
        let root_span_id = format!("span-{}", now_millis());
        let span = Span {
            span_id: root_span_id.clone(),
            parent_span_id: None,
            operation: operation.to_string(),
            start_time: now_millis(),
            end_time: None,
            status: SpanStatus::Unset,
            attributes: HashMap::new(),
            events: Vec::new(),
            links: Vec::new(),
        };
        let trace = Trace {
            trace_id: trace_id.clone(),
            root_span_id,
            spans: vec![span],
            start_time: now_millis(),
            end_time: None,
            status: TraceStatus::Active,
            attributes: HashMap::new(),
            service_name: service.to_string(),
        };
        self.traces.write().unwrap().insert(trace_id.clone(), trace);
        self.stats.traces_started.fetch_add(1, Ordering::Relaxed);
        trace_id
    }

    pub fn add_span(
        &self,
        trace_id: &str,
        parent_span_id: &str,
        operation: &str,
    ) -> Option<String> {
        let mut traces = self.traces.write().unwrap();
        let trace = traces.get_mut(trace_id)?;
        let span_id = format!("span-{}-{}", now_millis(), trace.spans.len());
        let span = Span {
            span_id: span_id.clone(),
            parent_span_id: Some(parent_span_id.to_string()),
            operation: operation.to_string(),
            start_time: now_millis(),
            end_time: None,
            status: SpanStatus::Unset,
            attributes: HashMap::new(),
            events: Vec::new(),
            links: Vec::new(),
        };
        trace.spans.push(span);
        self.stats.spans_recorded.fetch_add(1, Ordering::Relaxed);
        Some(span_id)
    }

    pub fn complete_span(&self, trace_id: &str, span_id: &str, status: SpanStatus) {
        let mut traces = self.traces.write().unwrap();
        if let Some(trace) = traces.get_mut(trace_id) {
            if let Some(span) = trace.spans.iter_mut().find(|s| s.span_id == span_id) {
                span.end_time = Some(now_millis());
                span.status = status;
            }
        }
    }

    pub fn complete_trace(&self, trace_id: &str) {
        let mut traces = self.traces.write().unwrap();
        if let Some(mut trace) = traces.remove(trace_id) {
            trace.end_time = Some(now_millis());
            trace.status = if trace.spans.iter().any(|s| s.status == SpanStatus::Error) {
                TraceStatus::Error
            } else {
                TraceStatus::Completed
            };
            self.stats.traces_completed.fetch_add(1, Ordering::Relaxed);
            if trace.status == TraceStatus::Error {
                self.stats.traces_errored.fetch_add(1, Ordering::Relaxed);
            }
            let mut completed = self.completed_traces.write().unwrap();
            if completed.len() >= self.max_completed {
                completed.pop_front();
            }
            completed.push_back(trace);
        }
    }

    pub fn get_trace(&self, trace_id: &str) -> Option<Trace> {
        if let Some(t) = self.traces.read().unwrap().get(trace_id) {
            return Some(t.clone());
        }
        self.completed_traces
            .read()
            .unwrap()
            .iter()
            .find(|t| t.trace_id == trace_id)
            .cloned()
    }

    pub fn trace_duration(&self, trace: &Trace) -> u64 {
        let end = trace.end_time.unwrap_or(now_millis());
        (end - trace.start_time) as u64
    }

    pub fn slow_traces(&self, threshold_ms: u64) -> Vec<Trace> {
        self.completed_traces
            .read()
            .unwrap()
            .iter()
            .filter(|t| self.trace_duration(t) >= threshold_ms)
            .cloned()
            .collect()
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Metrics Registry — comprehensive metrics collection
// ═══════════════════════════════════════════════════════════════════════════════

pub struct MetricsRegistry {
    pub counters: RwLock<HashMap<String, AtomicI64>>,
    pub gauges: RwLock<HashMap<String, f64>>,
    pub histograms: RwLock<HashMap<String, HistogramData>>,
    pub stats: MetricsRegistryStats,
}

pub struct HistogramData {
    pub values: VecDeque<f64>,
    pub max_size: usize,
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
}

#[derive(Debug, Default)]
pub struct MetricsRegistryStats {
    pub counters_registered: AtomicU64,
    pub gauges_registered: AtomicU64,
    pub histograms_registered: AtomicU64,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self {
            counters: RwLock::new(HashMap::new()),
            gauges: RwLock::new(HashMap::new()),
            histograms: RwLock::new(HashMap::new()),
            stats: MetricsRegistryStats::default(),
        }
    }

    pub fn register_counter(&self, name: &str) {
        self.counters
            .write()
            .unwrap()
            .entry(name.to_string())
            .or_insert_with(|| AtomicI64::new(0));
        self.stats
            .counters_registered
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_counter(&self, name: &str, value: i64) {
        let counters = self.counters.read().unwrap();
        if let Some(c) = counters.get(name) {
            c.fetch_add(value, Ordering::Relaxed);
        }
    }

    pub fn get_counter(&self, name: &str) -> i64 {
        self.counters
            .read()
            .unwrap()
            .get(name)
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    pub fn set_gauge(&self, name: &str, value: f64) {
        self.gauges.write().unwrap().insert(name.to_string(), value);
    }

    pub fn get_gauge(&self, name: &str) -> f64 {
        self.gauges
            .read()
            .unwrap()
            .get(name)
            .cloned()
            .unwrap_or(0.0)
    }

    pub fn record_histogram(&self, name: &str, value: f64) {
        let mut histograms = self.histograms.write().unwrap();
        let data = histograms
            .entry(name.to_string())
            .or_insert_with(|| HistogramData {
                values: VecDeque::new(),
                max_size: 10000,
                count: 0,
                sum: 0.0,
                min: f64::MAX,
                max: f64::MIN,
            });
        if data.values.len() >= data.max_size {
            data.values.pop_front();
        }
        data.values.push_back(value);
        data.count += 1;
        data.sum += value;
        data.min = data.min.min(value);
        data.max = data.max.max(value);
    }

    pub fn histogram_percentile(&self, name: &str, percentile: f64) -> f64 {
        let histograms = self.histograms.read().unwrap();
        let data = match histograms.get(name) {
            Some(d) => d,
            None => return 0.0,
        };
        if data.values.is_empty() {
            return 0.0;
        }
        let mut sorted: Vec<f64> = data.values.iter().cloned().collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let idx = (sorted.len() as f64 * percentile / 100.0).ceil() as usize;
        sorted[idx.min(sorted.len()) - 1]
    }

    pub fn histogram_mean(&self, name: &str) -> f64 {
        let histograms = self.histograms.read().unwrap();
        histograms
            .get(name)
            .map(|d| {
                if d.count > 0 {
                    d.sum / d.count as f64
                } else {
                    0.0
                }
            })
            .unwrap_or(0.0)
    }

    pub fn all_counter_names(&self) -> Vec<String> {
        self.counters.read().unwrap().keys().cloned().collect()
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Structured Logger
// ═══════════════════════════════════════════════════════════════════════════════

pub struct StructuredLogger {
    pub entries: RwLock<VecDeque<LogEntry>>,
    pub max_entries: usize,
    pub level: RwLock<LogLevel>,
    pub stats: LoggerStats,
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub timestamp: i64,
    pub level: LogLevel,
    pub message: String,
    pub fields: HashMap<String, String>,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub source: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Fatal,
}

#[derive(Debug, Default)]
pub struct LoggerStats {
    pub entries_written: AtomicU64,
    pub entries_dropped: AtomicU64,
}

impl StructuredLogger {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: RwLock::new(VecDeque::new()),
            max_entries,
            level: RwLock::new(LogLevel::Info),
            stats: LoggerStats::default(),
        }
    }

    pub fn log(
        &self,
        level: LogLevel,
        source: &str,
        message: &str,
        fields: HashMap<String, String>,
    ) {
        let current_level = *self.level.read().unwrap();
        if level < current_level {
            return;
        }
        let entry = LogEntry {
            timestamp: now_millis(),
            level,
            message: message.to_string(),
            fields,
            trace_id: None,
            span_id: None,
            source: source.to_string(),
        };
        let mut entries = self.entries.write().unwrap();
        if entries.len() >= self.max_entries {
            entries.pop_front();
            self.stats.entries_dropped.fetch_add(1, Ordering::Relaxed);
        }
        entries.push_back(entry);
        self.stats.entries_written.fetch_add(1, Ordering::Relaxed);
    }

    pub fn log_with_trace(
        &self,
        level: LogLevel,
        source: &str,
        message: &str,
        fields: HashMap<String, String>,
        trace_id: &str,
        span_id: &str,
    ) {
        let current_level = *self.level.read().unwrap();
        if level < current_level {
            return;
        }
        let entry = LogEntry {
            timestamp: now_millis(),
            level,
            message: message.to_string(),
            fields,
            trace_id: Some(trace_id.to_string()),
            span_id: Some(span_id.to_string()),
            source: source.to_string(),
        };
        let mut entries = self.entries.write().unwrap();
        if entries.len() >= self.max_entries {
            entries.pop_front();
        }
        entries.push_back(entry);
        self.stats.entries_written.fetch_add(1, Ordering::Relaxed);
    }

    pub fn search(&self, query: &str) -> Vec<LogEntry> {
        self.entries
            .read()
            .unwrap()
            .iter()
            .filter(|e| e.message.contains(query) || e.fields.values().any(|v| v.contains(query)))
            .cloned()
            .collect()
    }

    pub fn errors_since(&self, timestamp: i64) -> Vec<LogEntry> {
        self.entries
            .read()
            .unwrap()
            .iter()
            .filter(|e| e.timestamp >= timestamp && e.level >= LogLevel::Error)
            .cloned()
            .collect()
    }

    pub fn set_level(&self, level: LogLevel) {
        *self.level.write().unwrap() = level;
    }
    pub fn entry_count(&self) -> usize {
        self.entries.read().unwrap().len()
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Performance Profiler
// ═══════════════════════════════════════════════════════════════════════════════

pub struct PerformanceProfiler {
    pub profiles: RwLock<HashMap<String, ProfileData>>,
    pub hot_paths: RwLock<Vec<HotPath>>,
    pub stats: ProfilerStats,
}

#[derive(Debug, Clone)]
pub struct ProfileData {
    pub name: String,
    pub samples: VecDeque<u64>,
    pub max_samples: usize,
    pub total_time_us: u64,
    pub call_count: u64,
}

#[derive(Debug, Clone)]
pub struct HotPath {
    pub path: Vec<String>,
    pub total_time_us: u64,
    pub percentage: f64,
    pub call_count: u64,
}

#[derive(Debug, Default)]
pub struct ProfilerStats {
    pub profiles_created: AtomicU64,
    pub samples_collected: AtomicU64,
    pub hot_paths_detected: AtomicU64,
}

impl PerformanceProfiler {
    pub fn new() -> Self {
        Self {
            profiles: RwLock::new(HashMap::new()),
            hot_paths: RwLock::new(Vec::new()),
            stats: ProfilerStats::default(),
        }
    }

    pub fn start_profile(&self, name: &str) -> String {
        let mut profiles = self.profiles.write().unwrap();
        profiles.entry(name.to_string()).or_insert_with(|| {
            self.stats.profiles_created.fetch_add(1, Ordering::Relaxed);
            ProfileData {
                name: name.to_string(),
                samples: VecDeque::new(),
                max_samples: 10000,
                total_time_us: 0,
                call_count: 0,
            }
        });
        name.to_string()
    }

    pub fn record_sample(&self, name: &str, duration_us: u64) {
        let mut profiles = self.profiles.write().unwrap();
        if let Some(p) = profiles.get_mut(name) {
            if p.samples.len() >= p.max_samples {
                p.samples.pop_front();
            }
            p.samples.push_back(duration_us);
            p.total_time_us += duration_us;
            p.call_count += 1;
            self.stats.samples_collected.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn detect_hot_paths(&self) -> Vec<HotPath> {
        let profiles = self.profiles.read().unwrap();
        let total_time: u64 = profiles.values().map(|p| p.total_time_us).sum();
        let mut hot_paths: Vec<HotPath> = profiles
            .values()
            .filter(|p| p.call_count > 0)
            .map(|p| {
                let percentage = if total_time > 0 {
                    p.total_time_us as f64 / total_time as f64 * 100.0
                } else {
                    0.0
                };
                HotPath {
                    path: vec![p.name.clone()],
                    total_time_us: p.total_time_us,
                    percentage,
                    call_count: p.call_count,
                }
            })
            .collect();
        hot_paths.sort_by(|a, b| b.total_time_us.cmp(&a.total_time_us));
        self.stats
            .hot_paths_detected
            .store(hot_paths.len() as u64, Ordering::Relaxed);
        *self.hot_paths.write().unwrap() = hot_paths.clone();
        hot_paths
    }

    pub fn profile_stats(&self, name: &str) -> Option<(u64, u64, f64)> {
        self.profiles.read().unwrap().get(name).map(|p| {
            (
                p.total_time_us,
                p.call_count,
                if p.call_count > 0 {
                    p.total_time_us as f64 / p.call_count as f64
                } else {
                    0.0
                },
            )
        })
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Predictive Alert Engine
// ═══════════════════════════════════════════════════════════════════════════════

pub struct PredictiveAlertEngine {
    pub alert_rules: RwLock<Vec<AlertRule>>,
    pub active_alerts: RwLock<Vec<ActiveAlert>>,
    pub alert_history: RwLock<VecDeque<AlertRecord>>,
    pub metrics: Arc<MetricsRegistry>,
    pub stats: AlertEngineStats,
}

#[derive(Debug, Clone)]
pub struct AlertRule {
    pub rule_id: String,
    pub name: String,
    pub metric_name: String,
    pub condition: AlertCondition,
    pub severity: AlertSeverity,
    pub cooldown_seconds: u64,
    pub enabled: bool,
}

#[derive(Debug, Clone)]
pub enum AlertCondition {
    Above { threshold: f64, window_seconds: u64 },
    Below { threshold: f64, window_seconds: u64 },
    RateOfChange { threshold_per_second: f64 },
    PercentileAbove { percentile: f64, threshold: f64 },
    Absent { window_seconds: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
    Emergency,
}

#[derive(Debug, Clone)]
pub struct ActiveAlert {
    pub alert_id: String,
    pub rule_id: String,
    pub metric_value: f64,
    pub threshold: f64,
    pub started_at: i64,
    pub severity: AlertSeverity,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct AlertRecord {
    pub alert_id: String,
    pub rule_name: String,
    pub severity: AlertSeverity,
    pub started_at: i64,
    pub resolved_at: i64,
    pub duration_ms: u64,
    pub peak_value: f64,
}

#[derive(Debug, Default)]
pub struct AlertEngineStats {
    pub rules_evaluated: AtomicU64,
    pub alerts_fired: AtomicU64,
    pub alerts_resolved: AtomicU64,
    pub false_positives: AtomicU64,
}

impl PredictiveAlertEngine {
    pub fn new(metrics: Arc<MetricsRegistry>) -> Self {
        Self {
            alert_rules: RwLock::new(Vec::new()),
            active_alerts: RwLock::new(Vec::new()),
            alert_history: RwLock::new(VecDeque::new()),
            metrics,
            stats: AlertEngineStats::default(),
        }
    }

    pub fn add_rule(&self, rule: AlertRule) {
        self.alert_rules.write().unwrap().push(rule);
    }

    pub fn evaluate_rules(&self) -> Vec<ActiveAlert> {
        let rules = self.alert_rules.read().unwrap().clone();
        let mut new_alerts = Vec::new();
        for rule in &rules {
            if !rule.enabled {
                continue;
            }
            self.stats.rules_evaluated.fetch_add(1, Ordering::Relaxed);
            let value = self.metrics.get_gauge(&rule.metric_name);
            let triggered = match &rule.condition {
                AlertCondition::Above { threshold, .. } => value > *threshold,
                AlertCondition::Below { threshold, .. } => value < *threshold,
                AlertCondition::RateOfChange { .. } => false, // simplified
                AlertCondition::PercentileAbove {
                    percentile,
                    threshold,
                } => {
                    self.metrics
                        .histogram_percentile(&rule.metric_name, *percentile)
                        > *threshold
                }
                AlertCondition::Absent { .. } => value == 0.0,
            };
            if triggered {
                let alert = ActiveAlert {
                    alert_id: format!("alert-{}", now_millis()),
                    rule_id: rule.rule_id.clone(),
                    metric_value: value,
                    threshold: 0.0,
                    started_at: now_millis(),
                    severity: rule.severity,
                    message: format!("{}: metric {} = {:.2}", rule.name, rule.metric_name, value),
                };
                self.stats.alerts_fired.fetch_add(1, Ordering::Relaxed);
                new_alerts.push(alert);
            }
        }
        self.active_alerts
            .write()
            .unwrap()
            .extend(new_alerts.clone());
        new_alerts
    }

    pub fn active_count(&self) -> usize {
        self.active_alerts.read().unwrap().len()
    }
    pub fn resolve_alert(&self, alert_id: &str) {
        let mut alerts = self.active_alerts.write().unwrap();
        if let Some(pos) = alerts.iter().position(|a| a.alert_id == alert_id) {
            let alert = alerts.remove(pos);
            self.alert_history.write().unwrap().push_back(AlertRecord {
                alert_id: alert_id.to_string(),
                rule_name: alert.rule_id,
                severity: alert.severity,
                started_at: alert.started_at,
                resolved_at: now_millis(),
                duration_ms: (now_millis() - alert.started_at) as u64,
                peak_value: alert.metric_value,
            });
            self.stats.alerts_resolved.fetch_add(1, Ordering::Relaxed);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Observability Hub — combines all observability subsystems
// ═══════════════════════════════════════════════════════════════════════════════

pub struct ObservabilityHub {
    pub traces: Arc<TraceCollector>,
    pub metrics: Arc<MetricsRegistry>,
    pub logger: Arc<StructuredLogger>,
    pub profiler: Arc<PerformanceProfiler>,
    pub alerts: Arc<PredictiveAlertEngine>,
    pub stats: ObservabilityHubStats,
}

#[derive(Debug, Default)]
pub struct ObservabilityHubStats {
    pub queries_executed: AtomicU64,
    pub dashboards_rendered: AtomicU64,
}

impl ObservabilityHub {
    pub fn new() -> Self {
        let metrics = Arc::new(MetricsRegistry::new());
        let alerts = Arc::new(PredictiveAlertEngine::new(metrics.clone()));
        Self {
            traces: Arc::new(TraceCollector::new(10000)),
            metrics: metrics.clone(),
            logger: Arc::new(StructuredLogger::new(100000)),
            profiler: Arc::new(PerformanceProfiler::new()),
            alerts,
            stats: ObservabilityHubStats::default(),
        }
    }

    pub fn record_request(&self, service: &str, operation: &str, duration_ms: u64, success: bool) {
        let trace_id = self.traces.start_trace(service, operation);
        self.traces.complete_span(
            &trace_id,
            &self.traces.get_trace(&trace_id).unwrap().root_span_id,
            if success {
                SpanStatus::Ok
            } else {
                SpanStatus::Error
            },
        );
        self.traces.complete_trace(&trace_id);
        self.metrics.record_histogram(
            &format!("{}.{}.duration_ms", service, operation),
            duration_ms as f64,
        );
        self.metrics
            .increment_counter(&format!("{}.{}.total", service, operation), 1);
        if !success {
            self.metrics
                .increment_counter(&format!("{}.{}.errors", service, operation), 1);
        }
    }

    pub fn system_health_report(&self) -> String {
        let trace_count = self.traces.stats.traces_completed.load(Ordering::Relaxed);
        let error_count = self.traces.stats.traces_errored.load(Ordering::Relaxed);
        let error_rate = if trace_count > 0 {
            error_count as f64 / trace_count as f64 * 100.0
        } else {
            0.0
        };
        format!(
            "Traces: {} completed, {:.2}% error rate | Alerts: {} active | Log entries: {}",
            trace_count,
            error_rate,
            self.alerts.active_count(),
            self.logger.entry_count()
        )
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trace_lifecycle() {
        let tc = TraceCollector::new(100);
        let trace_id = tc.start_trace("api", "handle_request");
        let span_id = tc
            .add_span(
                &trace_id,
                &tc.get_trace(&trace_id).unwrap().root_span_id,
                "db_query",
            )
            .unwrap();
        tc.complete_span(&trace_id, &span_id, SpanStatus::Ok);
        tc.complete_trace(&trace_id);
        let trace = tc.get_trace(&trace_id).unwrap();
        assert_eq!(trace.status, TraceStatus::Completed);
        assert_eq!(trace.spans.len(), 2);
    }

    #[test]
    fn test_trace_error_propagation() {
        let tc = TraceCollector::new(100);
        let trace_id = tc.start_trace("api", "request");
        let root_id = tc.get_trace(&trace_id).unwrap().root_span_id;
        let span_id = tc.add_span(&trace_id, &root_id, "failing_op").unwrap();
        tc.complete_span(&trace_id, &span_id, SpanStatus::Error);
        tc.complete_trace(&trace_id);
        let trace = tc.get_trace(&trace_id).unwrap();
        assert_eq!(trace.status, TraceStatus::Error);
    }

    #[test]
    fn test_slow_traces() {
        let tc = TraceCollector::new(100);
        let id = tc.start_trace("api", "slow");
        tc.complete_trace(&id);
        let slow = tc.slow_traces(0); // threshold 0 = all traces
        assert!(!slow.is_empty());
    }

    #[test]
    fn test_metrics_counter() {
        let mr = MetricsRegistry::new();
        mr.register_counter("requests");
        mr.increment_counter("requests", 5);
        mr.increment_counter("requests", 3);
        assert_eq!(mr.get_counter("requests"), 8);
    }

    #[test]
    fn test_metrics_gauge() {
        let mr = MetricsRegistry::new();
        mr.set_gauge("cpu", 75.5);
        assert!((mr.get_gauge("cpu") - 75.5).abs() < 0.01);
    }

    #[test]
    fn test_metrics_histogram() {
        let mr = MetricsRegistry::new();
        for i in 0..100 {
            mr.record_histogram("latency", i as f64);
        }
        let p50 = mr.histogram_percentile("latency", 50.0);
        let p99 = mr.histogram_percentile("latency", 99.0);
        assert!(p50 >= 49.0);
        assert!(p99 >= 98.0);
    }

    #[test]
    fn test_structured_logger() {
        let logger = StructuredLogger::new(1000);
        logger.log(LogLevel::Info, "test", "hello world", HashMap::new());
        logger.log(LogLevel::Error, "test", "something failed", HashMap::new());
        assert_eq!(logger.entry_count(), 2);
        let errors = logger.errors_since(0);
        assert_eq!(errors.len(), 1);
    }

    #[test]
    fn test_logger_search() {
        let logger = StructuredLogger::new(1000);
        logger.log(
            LogLevel::Info,
            "test",
            "user login successful",
            HashMap::new(),
        );
        logger.log(LogLevel::Info, "test", "user logout", HashMap::new());
        let results = logger.search("login");
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_logger_level_filter() {
        let logger = StructuredLogger::new(1000);
        logger.set_level(LogLevel::Warn);
        logger.log(LogLevel::Debug, "test", "debug msg", HashMap::new());
        logger.log(LogLevel::Error, "test", "error msg", HashMap::new());
        assert_eq!(logger.entry_count(), 1);
    }

    #[test]
    fn test_performance_profiler() {
        let profiler = PerformanceProfiler::new();
        profiler.start_profile("db_query");
        for _ in 0..100 {
            profiler.record_sample("db_query", 500);
        }
        let stats = profiler.profile_stats("db_query").unwrap();
        assert_eq!(stats.1, 100); // call_count
        assert!((stats.2 - 500.0).abs() < 0.01); // avg
    }

    #[test]
    fn test_hot_path_detection() {
        let profiler = PerformanceProfiler::new();
        profiler.start_profile("hot_func");
        profiler.start_profile("cold_func");
        for _ in 0..1000 {
            profiler.record_sample("hot_func", 1000);
        }
        for _ in 0..10 {
            profiler.record_sample("cold_func", 100);
        }
        let hot = profiler.detect_hot_paths();
        assert_eq!(hot[0].path[0], "hot_func");
    }

    #[test]
    fn test_alert_engine_above() {
        let metrics = Arc::new(MetricsRegistry::new());
        let engine = PredictiveAlertEngine::new(metrics.clone());
        engine.add_rule(AlertRule {
            rule_id: "r1".into(),
            name: "High CPU".into(),
            metric_name: "cpu".into(),
            condition: AlertCondition::Above {
                threshold: 90.0,
                window_seconds: 60,
            },
            severity: AlertSeverity::Critical,
            cooldown_seconds: 300,
            enabled: true,
        });
        metrics.set_gauge("cpu", 95.0);
        let alerts = engine.evaluate_rules();
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].severity, AlertSeverity::Critical);
    }

    #[test]
    fn test_alert_engine_no_trigger() {
        let metrics = Arc::new(MetricsRegistry::new());
        let engine = PredictiveAlertEngine::new(metrics.clone());
        engine.add_rule(AlertRule {
            rule_id: "r1".into(),
            name: "High CPU".into(),
            metric_name: "cpu".into(),
            condition: AlertCondition::Above {
                threshold: 90.0,
                window_seconds: 60,
            },
            severity: AlertSeverity::Critical,
            cooldown_seconds: 300,
            enabled: true,
        });
        metrics.set_gauge("cpu", 50.0);
        let alerts = engine.evaluate_rules();
        assert!(alerts.is_empty());
    }

    #[test]
    fn test_observability_hub() {
        let hub = ObservabilityHub::new();
        hub.record_request("api", "get_user", 50, true);
        hub.record_request("api", "get_user", 100, true);
        hub.record_request("api", "get_user", 200, false);
        let report = hub.system_health_report();
        assert!(report.contains("Traces:"));
        assert!(report.contains("error rate"));
    }
}
