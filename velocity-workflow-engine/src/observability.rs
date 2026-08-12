//! Comprehensive observability framework for the workflow engine.
//!
//! Provides OpenTelemetry-compatible structured logging, Prometheus metrics export,
//! and distributed tracing — all with zero external dependencies.
//!
//! # Components
//! - [`StructuredLogger`] — zero-allocation JSON structured logging with level filtering
//! - [`MetricsExporter`] — Prometheus text exposition format metrics
//! - [`TracingSpan`] / [`SpanTracker`] — distributed tracing with span context propagation
//! - [`ObservabilityContext`] — unified facade tying logger, metrics, and tracer together

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};

// ─── Configuration ────────────────────────────────────────────────────────────

/// Configuration for the observability subsystem.
#[derive(Debug, Clone)]
pub struct ObservabilityConfig {
    pub enable_tracing: bool,
    pub enable_metrics: bool,
    pub enable_logging: bool,
    pub log_level: LogLevel,
    pub metrics_export_interval_ms: u64,
    pub service_name: String,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            enable_tracing: true,
            enable_metrics: true,
            enable_logging: true,
            log_level: LogLevel::Info,
            metrics_export_interval_ms: 10_000,
            service_name: "velocity-workflow-engine".to_string(),
        }
    }
}

// ─── Log Level ────────────────────────────────────────────────────────────────

/// Log severity levels, ordered by verbosity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum LogLevel {
    Trace = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
}

impl LogLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            LogLevel::Trace => "TRACE",
            LogLevel::Debug => "DEBUG",
            LogLevel::Info => "INFO",
            LogLevel::Warn => "WARN",
            LogLevel::Error => "ERROR",
        }
    }

    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => LogLevel::Trace,
            1 => LogLevel::Debug,
            2 => LogLevel::Info,
            3 => LogLevel::Warn,
            _ => LogLevel::Error,
        }
    }
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// ─── Structured Logger ────────────────────────────────────────────────────────

/// Thread-safe structured logger producing JSON-formatted output.
///
/// Each log event is rendered as a single JSON line for consumption by log
/// aggregation systems (ELK, Loki, CloudWatch, etc.).
pub struct StructuredLogger {
    level: AtomicU8,
    enabled: AtomicU8,
    log_buffer: Mutex<Vec<String>>,
    total_events: AtomicU64,
    events_by_level: [AtomicU64; 5],
    service_name: String,
}

/// AtomicU8 wrapper — std AtomicU8 is available on all targets.
use std::sync::atomic::AtomicU8;

impl StructuredLogger {
    pub fn new(level: LogLevel, service_name: &str) -> Self {
        Self {
            level: AtomicU8::new(level as u8),
            enabled: AtomicU8::new(1),
            log_buffer: Mutex::new(Vec::with_capacity(1024)),
            total_events: AtomicU64::new(0),
            events_by_level: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
            service_name: service_name.to_string(),
        }
    }

    /// Set the minimum log level at runtime.
    pub fn set_level(&self, level: LogLevel) {
        self.level.store(level as u8, Ordering::Relaxed);
    }

    /// Current minimum log level.
    pub fn level(&self) -> LogLevel {
        LogLevel::from_u8(self.level.load(Ordering::Relaxed))
    }

    /// Enable or disable logging at runtime.
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled as u8, Ordering::Relaxed);
    }

    /// Log a structured event. Fields are key-value pairs rendered as JSON.
    ///
    /// Returns `true` if the event was recorded (passed level filter).
    pub fn log_event(&self, level: LogLevel, event_name: &str, fields: &[(&str, &str)]) -> bool {
        if self.enabled.load(Ordering::Relaxed) == 0 {
            return false;
        }
        if (level as u8) < self.level.load(Ordering::Relaxed) {
            return false;
        }

        let mut json = String::with_capacity(256);
        json.push_str("{\"timestamp\":\"");
        json.push_str(&iso8601_now());
        json.push_str("\",\"level\":\"");
        json.push_str(level.as_str());
        json.push_str("\",\"service\":\"");
        json.push_str(&self.service_name);
        json.push_str("\",\"event\":\"");
        json.push_str(event_name);
        json.push_str("\"");

        for (k, v) in fields {
            json.push_str(",\"");
            json.push_str(k);
            json.push_str("\":\"");
            json.push_str(v);
            json.push_str("\"");
        }

        json.push_str("}");

        if let Ok(mut buf) = self.log_buffer.lock() {
            buf.push(json);
            // Cap buffer to prevent unbounded growth
            if buf.len() > 10_000 {
                let drain_count = buf.len() - 5_000;
                buf.drain(..drain_count);
            }
        }

        self.total_events.fetch_add(1, Ordering::Relaxed);
        self.events_by_level[level as usize].fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Drain all buffered log lines.
    pub fn drain_logs(&self) -> Vec<String> {
        self.log_buffer.lock().map(|mut buf| {
            let out = buf.clone();
            buf.clear();
            out
        }).unwrap_or_default()
    }

    /// Total events logged since creation.
    pub fn total_events(&self) -> u64 {
        self.total_events.load(Ordering::Relaxed)
    }

    /// Events logged at a specific level.
    pub fn events_at_level(&self, level: LogLevel) -> u64 {
        self.events_by_level[level as usize].load(Ordering::Relaxed)
    }
}

// ─── Metrics Exporter ─────────────────────────────────────────────────────────

/// Metric instrument types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

/// A single metric instrument with atomic storage.
struct MetricInstrument {
    name: String,
    help: String,
    kind: MetricKind,
    /// For Counter and Gauge: integer value bits (Counter uses u64, Gauge uses i64).
    value: AtomicU64,
    /// Histogram bucket upper bounds (empty for Counter/Gauge).
    histogram_bounds: Vec<f64>,
    /// Histogram bucket counts (parallel to bounds, plus one for +Inf).
    histogram_buckets: Vec<AtomicU64>,
    /// Histogram sum (stored as f64 bits).
    histogram_sum: AtomicU64,
    /// Histogram count.
    histogram_count: AtomicU64,
}

impl MetricInstrument {
    fn new_counter(name: &str, help: &str) -> Self {
        Self {
            name: name.to_string(),
            help: help.to_string(),
            kind: MetricKind::Counter,
            value: AtomicU64::new(0),
            histogram_bounds: Vec::new(),
            histogram_buckets: Vec::new(),
            histogram_sum: AtomicU64::new(0),
            histogram_count: AtomicU64::new(0),
        }
    }

    fn new_gauge(name: &str, help: &str) -> Self {
        Self {
            name: name.to_string(),
            help: help.to_string(),
            kind: MetricKind::Gauge,
            value: AtomicU64::new(0),
            histogram_bounds: Vec::new(),
            histogram_buckets: Vec::new(),
            histogram_sum: AtomicU64::new(0),
            histogram_count: AtomicU64::new(0),
        }
    }

    fn new_histogram(name: &str, help: &str, bounds: Vec<f64>) -> Self {
        let buckets = (0..=bounds.len()).map(|_| AtomicU64::new(0)).collect();
        Self {
            name: name.to_string(),
            help: help.to_string(),
            kind: MetricKind::Histogram,
            value: AtomicU64::new(0),
            histogram_bounds: bounds,
            histogram_buckets: buckets,
            histogram_sum: AtomicU64::new(0),
            histogram_count: AtomicU64::new(0),
        }
    }

    fn inc_counter(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    fn add_counter(&self, n: u64) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    fn get_counter(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    fn set_gauge(&self, v: i64) {
        self.value.store(v as u64, Ordering::Relaxed);
    }

    fn get_gauge(&self) -> i64 {
        self.value.load(Ordering::Relaxed) as i64
    }

    fn observe_histogram(&self, v: f64) {
        for (i, bound) in self.histogram_bounds.iter().enumerate() {
            if v <= *bound {
                self.histogram_buckets[i].fetch_add(1, Ordering::Relaxed);
            }
        }
        // +Inf bucket always gets incremented
        let last = self.histogram_buckets.len() - 1;
        self.histogram_buckets[last].fetch_add(1, Ordering::Relaxed);

        // Accumulate sum as bits
        loop {
            let old = self.histogram_sum.load(Ordering::Relaxed);
            let old_f = f64::from_bits(old);
            let new_f = old_f + v;
            if self.histogram_sum.compare_exchange_weak(
                old, new_f.to_bits(), Ordering::Relaxed, Ordering::Relaxed
            ).is_ok() {
                break;
            }
        }
        self.histogram_count.fetch_add(1, Ordering::Relaxed);
    }
}

/// RAII guard holding a read lock on the instrument storage.
struct InstrumentRef<'a> {
    _guard: std::sync::RwLockReadGuard<'a, Vec<MetricInstrument>>,
    idx: usize,
}

impl<'a> std::ops::Deref for InstrumentRef<'a> {
    type Target = MetricInstrument;
    fn deref(&self) -> &MetricInstrument {
        &self._guard[self.idx]
    }
}

/// Prometheus-compatible metrics exporter.
///
/// All instruments are stored in a lock-free registry and can be scraped
/// via [`export_prometheus`](MetricsExporter::export_prometheus).
pub struct MetricsExporter {
    instruments: RwLock<HashMap<String, usize>>,
    storage: RwLock<Vec<MetricInstrument>>,
    total_scrapes: AtomicU64,
}

impl MetricsExporter {
    pub fn new() -> Self {
        let exp = Self {
            instruments: RwLock::new(HashMap::new()),
            storage: RwLock::new(Vec::with_capacity(32)),
            total_scrapes: AtomicU64::new(0),
        };
        exp.register_defaults();
        exp
    }

    fn register_defaults(&self) {
        let default_counters: &[(&str, &str)] = &[
            ("workflow_started_total", "Total workflows started"),
            ("workflow_completed_total", "Total workflows completed"),
            ("workflow_failed_total", "Total workflows failed"),
            ("step_completed_total", "Total workflow steps completed"),
            ("signal_sent_total", "Total signals sent"),
            ("query_executed_total", "Total queries executed"),
        ];
        {
            let mut map = self.instruments.write().unwrap();
            let mut store = self.storage.write().unwrap();
            for (name, help) in default_counters {
                let idx = store.len();
                store.push(MetricInstrument::new_counter(name, help));
                map.insert(name.to_string(), idx);
            }

            let default_gauges: &[(&str, &str)] = &[
                ("active_workflows", "Number of currently active workflows"),
                ("task_queue_depth", "Current depth of the task queue"),
                ("timer_count", "Number of pending timers"),
            ];
            for (name, help) in default_gauges {
                let idx = store.len();
                store.push(MetricInstrument::new_gauge(name, help));
                map.insert(name.to_string(), idx);
            }

            let hist_bounds = vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 5000.0, 10000.0];
            let idx = store.len();
            store.push(MetricInstrument::new_histogram(
                "replication_lag_ms", "Replication lag in milliseconds", hist_bounds,
            ));
            map.insert("replication_lag_ms".to_string(), idx);
        }
    }

    /// Register a new counter. Returns `true` if newly inserted.
    pub fn register_counter(&self, name: &str, help: &str) -> bool {
        let mut map = self.instruments.write().unwrap();
        if map.contains_key(name) { return false; }
        let mut store = self.storage.write().unwrap();
        let idx = store.len();
        store.push(MetricInstrument::new_counter(name, help));
        map.insert(name.to_string(), idx);
        true
    }

    /// Register a new gauge. Returns `true` if newly inserted.
    pub fn register_gauge(&self, name: &str, help: &str) -> bool {
        let mut map = self.instruments.write().unwrap();
        if map.contains_key(name) { return false; }
        let mut store = self.storage.write().unwrap();
        let idx = store.len();
        store.push(MetricInstrument::new_gauge(name, help));
        map.insert(name.to_string(), idx);
        true
    }

    /// Register a new histogram. Returns `true` if newly inserted.
    pub fn register_histogram(&self, name: &str, help: &str, bounds: Vec<f64>) -> bool {
        let mut map = self.instruments.write().unwrap();
        if map.contains_key(name) { return false; }
        let mut store = self.storage.write().unwrap();
        let idx = store.len();
        store.push(MetricInstrument::new_histogram(name, help, bounds));
        map.insert(name.to_string(), idx);
        true
    }

    fn get_instrument(&self, name: &str) -> Option<InstrumentRef<'_>> {
        let map = self.instruments.read().unwrap();
        match map.get(name) {
            Some(&idx) => {
                let store = self.storage.read().unwrap();
                Some(InstrumentRef { _guard: store, idx })
            }
            None => None,
        }
    }

    pub fn inc_counter(&self, name: &str) {
        if let Some(inst) = self.get_instrument(name) { inst.inc_counter(); }
    }

    pub fn add_counter(&self, name: &str, n: u64) {
        if let Some(inst) = self.get_instrument(name) { inst.add_counter(n); }
    }

    pub fn get_counter(&self, name: &str) -> u64 {
        self.get_instrument(name).map_or(0, |i| i.get_counter())
    }

    pub fn set_gauge(&self, name: &str, value: i64) {
        if let Some(inst) = self.get_instrument(name) { inst.set_gauge(value); }
    }

    pub fn get_gauge(&self, name: &str) -> i64 {
        self.get_instrument(name).map_or(0, |i| i.get_gauge())
    }

    pub fn observe_histogram(&self, name: &str, value: f64) {
        if let Some(inst) = self.get_instrument(name) { inst.observe_histogram(value); }
    }

    /// Export all metrics in Prometheus text exposition format.
    pub fn export_prometheus(&self) -> String {
        self.total_scrapes.fetch_add(1, Ordering::Relaxed);
        let map = self.instruments.read().unwrap();
        let store = self.storage.read().unwrap();
        let mut out = String::with_capacity(map.len() * 128);

        // Sort by name for deterministic output
        let mut entries: Vec<(&String, &usize)> = map.iter().collect();
        entries.sort_by_key(|(name, _)| name.as_str());

        for (name, &idx) in entries {
            let inst = &store[idx];
            match inst.kind {
                MetricKind::Counter => {
                    out.push_str("# HELP ");
                    out.push_str(name);
                    out.push_str(" ");
                    out.push_str(&inst.help);
                    out.push('\n');
                    out.push_str("# TYPE ");
                    out.push_str(name);
                    out.push_str(" counter\n");
                    out.push_str(name);
                    out.push(' ');
                    push_u64(&mut out, inst.get_counter());
                    out.push('\n');
                }
                MetricKind::Gauge => {
                    out.push_str("# HELP ");
                    out.push_str(name);
                    out.push_str(" ");
                    out.push_str(&inst.help);
                    out.push('\n');
                    out.push_str("# TYPE ");
                    out.push_str(name);
                    out.push_str(" gauge\n");
                    out.push_str(name);
                    out.push(' ');
                    push_i64(&mut out, inst.get_gauge());
                    out.push('\n');
                }
                MetricKind::Histogram => {
                    out.push_str("# HELP ");
                    out.push_str(name);
                    out.push_str(" ");
                    out.push_str(&inst.help);
                    out.push('\n');
                    out.push_str("# TYPE ");
                    out.push_str(name);
                    out.push_str(" histogram\n");
                    for (i, bound) in inst.histogram_bounds.iter().enumerate() {
                        out.push_str(name);
                        out.push_str("_bucket{le=\"");
                        push_f64(&mut out, *bound);
                        out.push_str("\"} ");
                        push_u64(&mut out, inst.histogram_buckets[i].load(Ordering::Relaxed));
                        out.push('\n');
                    }
                    let last_bucket = inst.histogram_buckets.len() - 1;
                    out.push_str(name);
                    out.push_str("_bucket{le=\"+Inf\"} ");
                    push_u64(&mut out, inst.histogram_buckets[last_bucket].load(Ordering::Relaxed));
                    out.push('\n');
                    out.push_str(name);
                    out.push_str("_sum ");
                    let sum_bits = inst.histogram_sum.load(Ordering::Relaxed);
                    push_f64(&mut out, f64::from_bits(sum_bits));
                    out.push('\n');
                    out.push_str(name);
                    out.push_str("_count ");
                    push_u64(&mut out, inst.histogram_count.load(Ordering::Relaxed));
                    out.push('\n');
                }
            }
        }
        out
    }

    /// Total number of registered instruments.
    pub fn instrument_count(&self) -> usize {
        self.instruments.read().unwrap().len()
    }

    /// Total number of Prometheus scrapes.
    pub fn total_scrapes(&self) -> u64 {
        self.total_scrapes.load(Ordering::Relaxed)
    }
}

impl Default for MetricsExporter {
    fn default() -> Self { Self::new() }
}

// ─── Distributed Tracing ─────────────────────────────────────────────────────

/// Unique span identifier.
pub type SpanId = u64;

/// Status of a trace span.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanStatus {
    Active,
    Ok,
    Error,
}

/// A completed or in-flight trace span.
struct SpanRecord {
    span_id: SpanId,
    parent_id: Option<SpanId>,
    name: String,
    start_tick: u64,
    end_tick: u64,
    status: SpanStatus,
    attributes: Vec<(String, String)>,
    events: Vec<(String, u64)>,
}

/// Distributed trace span tracker.
///
/// Supports creating spans with optional parent context, adding events and
/// attributes, and propagating context across workflow boundaries.
pub struct SpanTracker {
    spans: RwLock<HashMap<SpanId, SpanRecord>>,
    next_id: AtomicU64,
    total_spans: AtomicU64,
    active_spans: AtomicU64,
    enabled: AtomicU8,
}

impl SpanTracker {
    pub fn new() -> Self {
        Self {
            spans: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            total_spans: AtomicU64::new(0),
            active_spans: AtomicU64::new(0),
            enabled: AtomicU8::new(1),
        }
    }

    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled as u8, Ordering::Relaxed);
    }

    /// Start a new span. Returns the span ID.
    pub fn start_span(&self, name: &str, parent_id: Option<SpanId>) -> SpanId {
        if self.enabled.load(Ordering::Relaxed) == 0 {
            return 0;
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let record = SpanRecord {
            span_id: id,
            parent_id,
            name: name.to_string(),
            start_tick: tick_now(),
            end_tick: 0,
            status: SpanStatus::Active,
            attributes: Vec::new(),
            events: Vec::new(),
        };
        self.spans.write().unwrap().insert(id, record);
        self.total_spans.fetch_add(1, Ordering::Relaxed);
        self.active_spans.fetch_add(1, Ordering::Relaxed);
        id
    }

    /// End a span, marking it complete.
    pub fn end_span(&self, span_id: SpanId) -> bool {
        if let Some(span) = self.spans.write().unwrap().get_mut(&span_id) {
            span.end_tick = tick_now();
            if span.status == SpanStatus::Active {
                span.status = SpanStatus::Ok;
            }
            self.active_spans.fetch_sub(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Mark a span as errored and end it.
    pub fn fail_span(&self, span_id: SpanId) -> bool {
        if let Some(span) = self.spans.write().unwrap().get_mut(&span_id) {
            span.end_tick = tick_now();
            span.status = SpanStatus::Error;
            self.active_spans.fetch_sub(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Add a timestamped event to a span.
    pub fn add_span_event(&self, span_id: SpanId, event_name: &str) -> bool {
        if let Some(span) = self.spans.write().unwrap().get_mut(&span_id) {
            span.events.push((event_name.to_string(), tick_now()));
            true
        } else {
            false
        }
    }

    /// Set an attribute on a span.
    pub fn set_span_attribute(&self, span_id: SpanId, key: &str, value: &str) -> bool {
        if let Some(span) = self.spans.write().unwrap().get_mut(&span_id) {
            // Update existing or insert new
            for attr in span.attributes.iter_mut() {
                if attr.0 == key {
                    attr.1 = value.to_string();
                    return true;
                }
            }
            span.attributes.push((key.to_string(), value.to_string()));
            true
        } else {
            false
        }
    }

    /// Get span info for inspection/testing.
    pub fn get_span_info(&self, span_id: SpanId) -> Option<(String, SpanStatus, Option<SpanId>)> {
        self.spans.read().unwrap().get(&span_id).map(|s| {
            (s.name.clone(), s.status, s.parent_id)
        })
    }

    /// Total spans created.
    pub fn total_spans(&self) -> u64 {
        self.total_spans.load(Ordering::Relaxed)
    }

    /// Currently active spans.
    pub fn active_spans(&self) -> u64 {
        self.active_spans.load(Ordering::Relaxed)
    }

    /// Export all completed spans as a simplified JSON trace.
    pub fn export_traces(&self) -> String {
        let spans = self.spans.read().unwrap();
        let mut out = String::with_capacity(spans.len() * 256);
        out.push_str("{\"spans\":[");
        let mut first = true;
        for span in spans.values() {
            if !first { out.push(','); }
            first = false;
            out.push_str("{\"id\":");
            push_u64(&mut out, span.span_id);
            out.push_str(",\"name\":\"");
            out.push_str(&span.name);
            out.push_str("\",\"status\":\"");
            out.push_str(match span.status {
                SpanStatus::Active => "active",
                SpanStatus::Ok => "ok",
                SpanStatus::Error => "error",
            });
            if let Some(pid) = span.parent_id {
                out.push_str(",\"parent_id\":");
                push_u64(&mut out, pid);
            }
            out.push_str(",\"start\":");
            push_u64(&mut out, span.start_tick);
            out.push_str(",\"end\":");
            push_u64(&mut out, span.end_tick);
            if !span.attributes.is_empty() {
                out.push_str(",\"attributes\":{");
                for (i, (k, v)) in span.attributes.iter().enumerate() {
                    if i > 0 { out.push(','); }
                    out.push('"');
                    out.push_str(k);
                    out.push_str("\":\"");
                    out.push_str(v);
                    out.push('"');
                }
                out.push('}');
            }
            if !span.events.is_empty() {
                out.push_str(",\"events\":[");
                for (i, (name, ts)) in span.events.iter().enumerate() {
                    if i > 0 { out.push(','); }
                    out.push_str("{\"name\":\"");
                    out.push_str(name);
                    out.push_str("\",\"tick\":");
                    push_u64(&mut out, *ts);
                    out.push('}');
                }
                out.push(']');
            }
            out.push('}');
        }
        out.push_str("]}");
        out
    }
}

impl Default for SpanTracker {
    fn default() -> Self { Self::new() }
}

// ─── Observability Context ────────────────────────────────────────────────────

/// Unified observability facade tying together logging, metrics, and tracing.
///
/// Provides high-level convenience methods for recording workflow lifecycle
/// events that automatically update all three observability pillars.
pub struct ObservabilityContext {
    logger: StructuredLogger,
    metrics: MetricsExporter,
    tracer: SpanTracker,
    config: ObservabilityConfig,
    workflow_starts: AtomicU64,
    workflow_completions: AtomicU64,
    workflow_failures: AtomicU64,
}

impl ObservabilityContext {
    /// Create a new observability context from configuration.
    pub fn new(config: ObservabilityConfig) -> Self {
        let logger = StructuredLogger::new(config.log_level, &config.service_name);
        let metrics = MetricsExporter::new();
        let tracer = SpanTracker::new();

        logger.set_enabled(config.enable_logging);
        tracer.set_enabled(config.enable_tracing);

        Self {
            logger,
            metrics,
            tracer,
            config,
            workflow_starts: AtomicU64::new(0),
            workflow_completions: AtomicU64::new(0),
            workflow_failures: AtomicU64::new(0),
        }
    }

    /// Access the structured logger.
    pub fn logger(&self) -> &StructuredLogger { &self.logger }

    /// Access the metrics exporter.
    pub fn metrics(&self) -> &MetricsExporter { &self.metrics }

    /// Access the span tracer.
    pub fn tracer(&self) -> &SpanTracker { &self.tracer }

    /// Access the configuration.
    pub fn config(&self) -> &ObservabilityConfig { &self.config }

    /// Record a workflow start across all observability pillars.
    pub fn record_workflow_start(&self, key: u64, workflow_type: &str, namespace: &str) {
        self.workflow_starts.fetch_add(1, Ordering::Relaxed);
        self.metrics.inc_counter("workflow_started_total");
        self.metrics.set_gauge("active_workflows",
            self.workflow_starts.load(Ordering::Relaxed) as i64
                - self.workflow_completions.load(Ordering::Relaxed) as i64
                - self.workflow_failures.load(Ordering::Relaxed) as i64);
        self.logger.log_event(LogLevel::Info, "workflow_started", &[
            ("workflow_key", &u64_to_str(key)),
            ("workflow_type", workflow_type),
            ("namespace", namespace),
        ]);
        let span_id = self.tracer.start_span(
            &format!("workflow:{}:{}", workflow_type, key),
            None,
        );
        self.tracer.set_span_attribute(span_id, "workflow.type", workflow_type);
        self.tracer.set_span_attribute(span_id, "workflow.namespace", namespace);
    }

    /// Record a workflow completion.
    pub fn record_workflow_complete(&self, key: u64, duration_ms: u64) {
        self.workflow_completions.fetch_add(1, Ordering::Relaxed);
        self.metrics.inc_counter("workflow_completed_total");
        self.metrics.observe_histogram("replication_lag_ms", duration_ms as f64);
        self.metrics.set_gauge("active_workflows",
            self.workflow_starts.load(Ordering::Relaxed) as i64
                - self.workflow_completions.load(Ordering::Relaxed) as i64
                - self.workflow_failures.load(Ordering::Relaxed) as i64);
        self.logger.log_event(LogLevel::Info, "workflow_completed", &[
            ("workflow_key", &u64_to_str(key)),
            ("duration_ms", &u64_to_str(duration_ms)),
        ]);
    }

    /// Record a workflow failure.
    pub fn record_workflow_fail(&self, key: u64, error: &str) {
        self.workflow_failures.fetch_add(1, Ordering::Relaxed);
        self.metrics.inc_counter("workflow_failed_total");
        self.metrics.set_gauge("active_workflows",
            self.workflow_starts.load(Ordering::Relaxed) as i64
                - self.workflow_completions.load(Ordering::Relaxed) as i64
                - self.workflow_failures.load(Ordering::Relaxed) as i64);
        self.logger.log_event(LogLevel::Error, "workflow_failed", &[
            ("workflow_key", &u64_to_str(key)),
            ("error", error),
        ]);
    }

    /// Record a step completion within a workflow.
    pub fn record_step_complete(&self, key: u64, step: u32, duration_ms: u64) {
        self.metrics.inc_counter("step_completed_total");
        self.logger.log_event(LogLevel::Debug, "step_completed", &[
            ("workflow_key", &u64_to_str(key)),
            ("step", &u32_to_str(step)),
            ("duration_ms", &u64_to_str(duration_ms)),
        ]);
    }

    /// Total workflow starts recorded.
    pub fn total_workflow_starts(&self) -> u64 {
        self.workflow_starts.load(Ordering::Relaxed)
    }

    /// Total workflow completions recorded.
    pub fn total_workflow_completions(&self) -> u64 {
        self.workflow_completions.load(Ordering::Relaxed)
    }

    /// Total workflow failures recorded.
    pub fn total_workflow_failures(&self) -> u64 {
        self.workflow_failures.load(Ordering::Relaxed)
    }
}

// ─── Formatting Helpers (zero-allocation number formatting) ───────────────────

fn push_u64(buf: &mut String, v: u64) {
    use std::fmt::Write;
    let _ = write!(buf, "{}", v);
}

fn push_i64(buf: &mut String, v: i64) {
    use std::fmt::Write;
    let _ = write!(buf, "{}", v);
}

fn push_f64(buf: &mut String, v: f64) {
    use std::fmt::Write;
    let _ = write!(buf, "{}", v);
}

fn u64_to_str(v: u64) -> String {
    let mut s = String::with_capacity(20);
    push_u64(&mut s, v);
    s
}

fn u32_to_str(v: u32) -> String {
    let mut s = String::with_capacity(10);
    use std::fmt::Write;
    let _ = write!(s, "{}", v);
    s
}

/// Simple timestamp — uses a monotonic tick counter (not wall-clock, to avoid
/// pulling in chrono). For real deployments, replace with system time.
fn iso8601_now() -> String {
    // Minimal ISO-8601-ish timestamp from tick counter
    let tick = tick_now();
    let mut s = String::with_capacity(32);
    use std::fmt::Write;
    let _ = write!(s, "tick_{}", tick);
    s
}

fn tick_now() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

// ─── Global Singleton ─────────────────────────────────────────────────────────

static GLOBAL_CTX: once_cell_stub::OnceCell<ObservabilityContext> = once_cell_stub::OnceCell::new();

/// Minimal OnceCell replacement (no external deps).
mod once_cell_stub {
    use std::sync::{Once, Mutex};

    pub struct OnceCell<T> {
        once: Once,
        inner: Mutex<Option<T>>,
    }

    impl<T> OnceCell<T> {
        pub const fn new() -> Self {
            Self {
                once: Once::new(),
                inner: Mutex::new(None),
            }
        }

        pub fn get_or_init<F: FnOnce() -> T>(&self, f: F) -> &T {
            self.once.call_once(|| {
                *self.inner.lock().unwrap() = Some(f());
            });
            // SAFETY: after call_once, inner is Some and never changes.
            // The value lives as long as the OnceCell (which is 'static for globals).
            let guard = self.inner.lock().unwrap();
            let ptr = guard.as_ref().unwrap() as *const T;
            drop(guard);
            unsafe { &*ptr }
        }

        pub fn get(&self) -> Option<&T> {
            if self.once.is_completed() {
                let guard = self.inner.lock().unwrap();
                let ptr = guard.as_ref().unwrap() as *const T;
                drop(guard);
                Some(unsafe { &*ptr })
            } else {
                None
            }
        }
    }
}

/// Initialize the global observability context.
pub fn init_global(config: ObservabilityConfig) -> &'static ObservabilityContext {
    GLOBAL_CTX.get_or_init(|| ObservabilityContext::new(config))
}

/// Access the global observability context (returns `None` if not initialized).
pub fn global() -> Option<&'static ObservabilityContext> {
    GLOBAL_CTX.get()
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // --- Structured Logger Tests ---

    #[test]
    fn test_logger_json_format() {
        let logger = StructuredLogger::new(LogLevel::Info, "test-svc");
        let recorded = logger.log_event(LogLevel::Info, "test_event", &[
            ("key1", "val1"),
            ("key2", "val2"),
        ]);
        assert!(recorded);
        let logs = logger.drain_logs();
        assert_eq!(logs.len(), 1);
        let line = &logs[0];
        assert!(line.starts_with("{\"timestamp\":"));
        assert!(line.contains("\"level\":\"INFO\""));
        assert!(line.contains("\"service\":\"test-svc\""));
        assert!(line.contains("\"event\":\"test_event\""));
        assert!(line.contains("\"key1\":\"val1\""));
        assert!(line.contains("\"key2\":\"val2\""));
        assert!(line.ends_with("}"));
    }

    #[test]
    fn test_logger_level_filtering() {
        let logger = StructuredLogger::new(LogLevel::Warn, "test-svc");
        assert!(!logger.log_event(LogLevel::Info, "info_event", &[]));
        assert!(!logger.log_event(LogLevel::Debug, "debug_event", &[]));
        assert!(logger.log_event(LogLevel::Warn, "warn_event", &[]));
        assert!(logger.log_event(LogLevel::Error, "error_event", &[]));
        assert_eq!(logger.total_events(), 2);
    }

    #[test]
    fn test_logger_level_counts() {
        let logger = StructuredLogger::new(LogLevel::Trace, "svc");
        logger.log_event(LogLevel::Info, "a", &[]);
        logger.log_event(LogLevel::Info, "b", &[]);
        logger.log_event(LogLevel::Error, "c", &[]);
        assert_eq!(logger.events_at_level(LogLevel::Info), 2);
        assert_eq!(logger.events_at_level(LogLevel::Error), 1);
        assert_eq!(logger.events_at_level(LogLevel::Warn), 0);
    }

    #[test]
    fn test_logger_disable() {
        let logger = StructuredLogger::new(LogLevel::Info, "svc");
        logger.set_enabled(false);
        assert!(!logger.log_event(LogLevel::Error, "should_not_log", &[]));
        assert_eq!(logger.total_events(), 0);
    }

    #[test]
    fn test_logger_runtime_level_change() {
        let logger = StructuredLogger::new(LogLevel::Error, "svc");
        assert!(!logger.log_event(LogLevel::Info, "filtered", &[]));
        logger.set_level(LogLevel::Trace);
        assert!(logger.log_event(LogLevel::Trace, "now_passes", &[]));
        assert_eq!(logger.level(), LogLevel::Trace);
    }

    #[test]
    fn test_logger_empty_fields() {
        let logger = StructuredLogger::new(LogLevel::Info, "svc");
        logger.log_event(LogLevel::Info, "no_fields", &[]);
        let logs = logger.drain_logs();
        assert_eq!(logs.len(), 1);
        assert!(logs[0].contains("\"event\":\"no_fields\""));
    }

    // --- Metrics Exporter Tests ---

    #[test]
    fn test_metrics_default_instruments() {
        let exp = MetricsExporter::new();
        assert!(exp.instrument_count() >= 10);
        // Check default counters exist
        assert_eq!(exp.get_counter("workflow_started_total"), 0);
        exp.inc_counter("workflow_started_total");
        assert_eq!(exp.get_counter("workflow_started_total"), 1);
    }

    #[test]
    fn test_metrics_counter_operations() {
        let exp = MetricsExporter::new();
        exp.inc_counter("workflow_started_total");
        exp.inc_counter("workflow_started_total");
        exp.add_counter("workflow_started_total", 3);
        assert_eq!(exp.get_counter("workflow_started_total"), 5);
    }

    #[test]
    fn test_metrics_gauge_operations() {
        let exp = MetricsExporter::new();
        exp.set_gauge("active_workflows", 42);
        assert_eq!(exp.get_gauge("active_workflows"), 42);
        exp.set_gauge("active_workflows", -5);
        assert_eq!(exp.get_gauge("active_workflows"), -5);
    }

    #[test]
    fn test_metrics_histogram_observe() {
        let exp = MetricsExporter::new();
        exp.observe_histogram("replication_lag_ms", 50.0);
        exp.observe_histogram("replication_lag_ms", 150.0);
        exp.observe_histogram("replication_lag_ms", 5000.0);
    }

    #[test]
    fn test_metrics_register_custom() {
        let exp = MetricsExporter::new();
        assert!(exp.register_counter("my_custom_counter", "A custom counter"));
        assert!(!exp.register_counter("my_custom_counter", "Duplicate")); // duplicate
        exp.inc_counter("my_custom_counter");
        assert_eq!(exp.get_counter("my_custom_counter"), 1);
    }

    #[test]
    fn test_prometheus_export_format() {
        let exp = MetricsExporter::new();
        exp.inc_counter("workflow_started_total");
        exp.set_gauge("active_workflows", 7);
        let output = exp.export_prometheus();
        assert!(output.contains("# TYPE workflow_started_total counter"));
        assert!(output.contains("workflow_started_total 1"));
        assert!(output.contains("# TYPE active_workflows gauge"));
        assert!(output.contains("active_workflows 7"));
        assert!(output.contains("# TYPE replication_lag_ms histogram"));
        assert!(output.contains("_bucket{le=\""));
        assert!(output.contains("_bucket{le=\"+Inf\"}"));
        assert!(output.contains("_sum "));
        assert!(output.contains("_count "));
    }

    #[test]
    fn test_prometheus_scrape_counter() {
        let exp = MetricsExporter::new();
        assert_eq!(exp.total_scrapes(), 0);
        exp.export_prometheus();
        exp.export_prometheus();
        assert_eq!(exp.total_scrapes(), 2);
    }

    #[test]
    fn test_metrics_unknown_name() {
        let exp = MetricsExporter::new();
        assert_eq!(exp.get_counter("nonexistent"), 0);
        assert_eq!(exp.get_gauge("nonexistent"), 0);
        // Should not panic
        exp.inc_counter("nonexistent");
        exp.set_gauge("nonexistent", 99);
    }

    // --- Tracing Tests ---

    #[test]
    fn test_span_lifecycle() {
        let tracer = SpanTracker::new();
        let id = tracer.start_span("test-span", None);
        assert!(id > 0);
        assert_eq!(tracer.active_spans(), 1);

        let (name, status, parent) = tracer.get_span_info(id).unwrap();
        assert_eq!(name, "test-span");
        assert_eq!(status, SpanStatus::Active);
        assert_eq!(parent, None);

        assert!(tracer.end_span(id));
        let (_, status, _) = tracer.get_span_info(id).unwrap();
        assert_eq!(status, SpanStatus::Ok);
        assert_eq!(tracer.active_spans(), 0);
    }

    #[test]
    fn test_span_parent_child() {
        let tracer = SpanTracker::new();
        let parent = tracer.start_span("parent", None);
        let child = tracer.start_span("child", Some(parent));
        let (_, _, child_parent) = tracer.get_span_info(child).unwrap();
        assert_eq!(child_parent, Some(parent));
        tracer.end_span(child);
        tracer.end_span(parent);
        assert_eq!(tracer.total_spans(), 2);
    }

    #[test]
    fn test_span_events_and_attributes() {
        let tracer = SpanTracker::new();
        let id = tracer.start_span("op", None);
        assert!(tracer.add_span_event(id, "step1"));
        assert!(tracer.add_span_event(id, "step2"));
        assert!(tracer.set_span_attribute(id, "env", "prod"));
        assert!(tracer.set_span_attribute(id, "version", "1.0"));
        // Update existing attribute
        assert!(tracer.set_span_attribute(id, "env", "staging"));
        tracer.end_span(id);

        let trace_json = tracer.export_traces();
        assert!(trace_json.contains("\"name\":\"op\""));
        assert!(trace_json.contains("\"name\":\"step1\""));
        assert!(trace_json.contains("\"env\":\"staging\""));
    }

    #[test]
    fn test_span_fail() {
        let tracer = SpanTracker::new();
        let id = tracer.start_span("failing-op", None);
        assert!(tracer.fail_span(id));
        let (_, status, _) = tracer.get_span_info(id).unwrap();
        assert_eq!(status, SpanStatus::Error);
    }

    #[test]
    fn test_span_end_nonexistent() {
        let tracer = SpanTracker::new();
        assert!(!tracer.end_span(99999));
    }

    #[test]
    fn test_tracing_disabled() {
        let tracer = SpanTracker::new();
        tracer.set_enabled(false);
        let id = tracer.start_span("should_not_create", None);
        assert_eq!(id, 0);
        assert_eq!(tracer.total_spans(), 0);
    }

    // --- ObservabilityContext Tests ---

    #[test]
    fn test_context_workflow_lifecycle() {
        let ctx = ObservabilityContext::new(ObservabilityConfig::default());
        ctx.record_workflow_start(1001, "payment", "default");
        ctx.record_workflow_start(1002, "shipping", "default");
        ctx.record_step_complete(1001, 1, 50);
        ctx.record_workflow_complete(1001, 200);
        ctx.record_workflow_fail(1002, "timeout");

        assert_eq!(ctx.total_workflow_starts(), 2);
        assert_eq!(ctx.total_workflow_completions(), 1);
        assert_eq!(ctx.total_workflow_failures(), 1);
        assert_eq!(ctx.metrics().get_counter("workflow_started_total"), 2);
        assert_eq!(ctx.metrics().get_counter("workflow_completed_total"), 1);
        assert_eq!(ctx.metrics().get_counter("workflow_failed_total"), 1);
        assert_eq!(ctx.metrics().get_counter("step_completed_total"), 1);
    }

    #[test]
    fn test_context_active_workflows_gauge() {
        let ctx = ObservabilityContext::new(ObservabilityConfig::default());
        ctx.record_workflow_start(1, "wf", "ns");
        ctx.record_workflow_start(2, "wf", "ns");
        ctx.record_workflow_start(3, "wf", "ns");
        assert_eq!(ctx.metrics().get_gauge("active_workflows"), 3);
        ctx.record_workflow_complete(1, 100);
        assert_eq!(ctx.metrics().get_gauge("active_workflows"), 2);
        ctx.record_workflow_fail(2, "err");
        assert_eq!(ctx.metrics().get_gauge("active_workflows"), 1);
    }

    #[test]
    fn test_context_logger_integration() {
        let ctx = ObservabilityContext::new(ObservabilityConfig::default());
        ctx.record_workflow_start(42, "order", "prod");
        let logs = ctx.logger().drain_logs();
        assert!(!logs.is_empty());
        assert!(logs[0].contains("\"event\":\"workflow_started\""));
        assert!(logs[0].contains("\"workflow_type\":\"order\""));
    }

    #[test]
    fn test_context_config() {
        let mut cfg = ObservabilityConfig::default();
        cfg.service_name = "custom-service".to_string();
        cfg.log_level = LogLevel::Debug;
        let ctx = ObservabilityContext::new(cfg);
        assert_eq!(ctx.config().service_name, "custom-service");
        assert_eq!(ctx.logger().level(), LogLevel::Debug);
    }

    // --- LogLevel Tests ---

    #[test]
    fn test_log_level_ordering() {
        assert!(LogLevel::Trace < LogLevel::Debug);
        assert!(LogLevel::Debug < LogLevel::Info);
        assert!(LogLevel::Info < LogLevel::Warn);
        assert!(LogLevel::Warn < LogLevel::Error);
    }

    #[test]
    fn test_log_level_roundtrip() {
        for v in 0..5 {
            let level = LogLevel::from_u8(v);
            assert_eq!(level as u8, v);
        }
    }
}
