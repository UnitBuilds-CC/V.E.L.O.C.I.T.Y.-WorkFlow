// Copyright (c) VELOCITY Suite. All rights reserved.
// Licensed under the MIT License.

//! OpenTelemetry Integration — Distributed tracing, metrics, and logging.
//!
//! Provides comprehensive observability through OpenTelemetry-compatible
//! instrumentation. Supports trace propagation across workflow and activity
//! boundaries, custom metrics, and structured logging.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

// ═══════════════════════════════════════════════════════════════════════════════
// Tracing
// ═══════════════════════════════════════════════════════════════════════════════

/// A trace context for distributed tracing.
#[derive(Debug, Clone)]
pub struct TraceContext {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub operation_name: String,
    pub start_time: i64,
    pub end_time: Option<i64>,
    pub attributes: HashMap<String, AttributeValue>,
    pub events: Vec<SpanEvent>,
    pub status: SpanStatus,
    pub links: Vec<SpanLink>,
}

/// Span status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpanStatus {
    Unset,
    Ok,
    Error(String),
}

/// Attribute value types.
#[derive(Debug, Clone)]
pub enum AttributeValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    StringArray(Vec<String>),
    IntArray(Vec<i64>),
}

/// A span event (timestamped annotation).
#[derive(Debug, Clone)]
pub struct SpanEvent {
    pub name: String,
    pub timestamp: i64,
    pub attributes: HashMap<String, AttributeValue>,
}

/// A span link (causal relationship to another span).
#[derive(Debug, Clone)]
pub struct SpanLink {
    pub trace_id: String,
    pub span_id: String,
    pub attributes: HashMap<String, AttributeValue>,
}

/// Span builder for creating spans.
pub struct SpanBuilder {
    operation_name: String,
    trace_id: Option<String>,
    parent_span_id: Option<String>,
    attributes: HashMap<String, AttributeValue>,
    links: Vec<SpanLink>,
}

impl SpanBuilder {
    pub fn new(operation_name: &str) -> Self {
        Self {
            operation_name: operation_name.to_string(),
            trace_id: None,
            parent_span_id: None,
            attributes: HashMap::new(),
            links: Vec::new(),
        }
    }

    pub fn with_trace_id(mut self, trace_id: &str) -> Self {
        self.trace_id = Some(trace_id.to_string());
        self
    }

    pub fn with_parent_span(mut self, parent_span_id: &str) -> Self {
        self.parent_span_id = Some(parent_span_id.to_string());
        self
    }

    pub fn with_attribute(mut self, key: &str, value: AttributeValue) -> Self {
        self.attributes.insert(key.to_string(), value);
        self
    }

    pub fn with_link(mut self, link: SpanLink) -> Self {
        self.links.push(link);
        self
    }

    pub fn start(self, tracer: &Tracer) -> TraceContext {
        tracer.start_span(self)
    }
}

/// Tracer for creating and managing spans.
pub struct Tracer {
    service_name: String,
    spans: RwLock<Vec<TraceContext>>,
    stats: Arc<TracerStats>,
}

struct TracerStats {
    spans_created: AtomicU64,
    spans_ended: AtomicU64,
    spans_active: AtomicU64,
}

impl Tracer {
    pub fn new(service_name: &str) -> Self {
        Self {
            service_name: service_name.to_string(),
            spans: RwLock::new(Vec::new()),
            stats: Arc::new(TracerStats {
                spans_created: AtomicU64::new(0),
                spans_ended: AtomicU64::new(0),
                spans_active: AtomicU64::new(0),
            }),
        }
    }

    pub fn span_builder(&self, operation_name: &str) -> SpanBuilder {
        SpanBuilder::new(operation_name)
    }

    pub fn start_span(&self, builder: SpanBuilder) -> TraceContext {
        let trace_id = builder.trace_id.unwrap_or_else(generate_trace_id);
        let span_id = generate_span_id();

        let span = TraceContext {
            trace_id,
            span_id,
            parent_span_id: builder.parent_span_id,
            operation_name: builder.operation_name,
            start_time: now_millis(),
            end_time: None,
            attributes: builder.attributes,
            events: Vec::new(),
            status: SpanStatus::Unset,
            links: builder.links,
        };

        self.stats.spans_created.fetch_add(1, Ordering::Relaxed);
        self.stats.spans_active.fetch_add(1, Ordering::Relaxed);
        span
    }

    pub fn end_span(&self, span: &mut TraceContext) {
        span.end_time = Some(now_millis());
        self.spans.write().unwrap().push(span.clone());
        self.stats.spans_ended.fetch_add(1, Ordering::Relaxed);
        self.stats.spans_active.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn add_event(&self, span: &mut TraceContext, name: &str) {
        span.events.push(SpanEvent {
            name: name.to_string(),
            timestamp: now_millis(),
            attributes: HashMap::new(),
        });
    }

    pub fn add_event_with_attributes(
        &self,
        span: &mut TraceContext,
        name: &str,
        attributes: HashMap<String, AttributeValue>,
    ) {
        span.events.push(SpanEvent {
            name: name.to_string(),
            timestamp: now_millis(),
            attributes,
        });
    }

    pub fn set_status(&self, span: &mut TraceContext, status: SpanStatus) {
        span.status = status;
    }

    pub fn get_spans(&self) -> Vec<TraceContext> {
        self.spans.read().unwrap().clone()
    }

    pub fn get_traces(&self) -> HashMap<String, Vec<TraceContext>> {
        let spans = self.spans.read().unwrap();
        let mut traces: HashMap<String, Vec<TraceContext>> = HashMap::new();
        for span in spans.iter() {
            traces
                .entry(span.trace_id.clone())
                .or_default()
                .push(span.clone());
        }
        traces
    }

    pub fn get_stats(&self) -> TracerStatsSnapshot {
        TracerStatsSnapshot {
            spans_created: self.stats.spans_created.load(Ordering::Relaxed),
            spans_ended: self.stats.spans_ended.load(Ordering::Relaxed),
            spans_active: self.stats.spans_active.load(Ordering::Relaxed),
            service_name: self.service_name.clone(),
        }
    }

    pub fn service_name(&self) -> &str {
        &self.service_name
    }
}

#[derive(Debug, Clone)]
pub struct TracerStatsSnapshot {
    pub spans_created: u64,
    pub spans_ended: u64,
    pub spans_active: u64,
    pub service_name: String,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Metrics
// ═══════════════════════════════════════════════════════════════════════════════

/// Metrics recorder for OpenTelemetry-compatible metrics.
pub struct MetricsRecorder {
    counters: RwLock<HashMap<String, u64>>,
    gauges: RwLock<HashMap<String, f64>>,
    histograms: RwLock<HashMap<String, Vec<f64>>>,
    stats: Arc<MetricsStats>,
}

struct MetricsStats {
    records_made: AtomicU64,
}

impl MetricsRecorder {
    pub fn new() -> Self {
        Self {
            counters: RwLock::new(HashMap::new()),
            gauges: RwLock::new(HashMap::new()),
            histograms: RwLock::new(HashMap::new()),
            stats: Arc::new(MetricsStats {
                records_made: AtomicU64::new(0),
            }),
        }
    }

    pub fn counter_add(&self, name: &str, value: u64) {
        let mut counters = self.counters.write().unwrap();
        *counters.entry(name.to_string()).or_insert(0) += value;
        self.stats.records_made.fetch_add(1, Ordering::Relaxed);
    }

    pub fn counter_get(&self, name: &str) -> u64 {
        self.counters
            .read()
            .unwrap()
            .get(name)
            .copied()
            .unwrap_or(0)
    }

    pub fn gauge_set(&self, name: &str, value: f64) {
        self.gauges.write().unwrap().insert(name.to_string(), value);
        self.stats.records_made.fetch_add(1, Ordering::Relaxed);
    }

    pub fn gauge_get(&self, name: &str) -> f64 {
        self.gauges
            .read()
            .unwrap()
            .get(name)
            .copied()
            .unwrap_or(0.0)
    }

    pub fn histogram_record(&self, name: &str, value: f64) {
        self.histograms
            .write()
            .unwrap()
            .entry(name.to_string())
            .or_default()
            .push(value);
        self.stats.records_made.fetch_add(1, Ordering::Relaxed);
    }

    pub fn histogram_stats(&self, name: &str) -> Option<HistogramStats> {
        let histograms = self.histograms.read().unwrap();
        histograms.get(name).map(|values| {
            if values.is_empty() {
                return HistogramStats {
                    count: 0,
                    sum: 0.0,
                    min: 0.0,
                    max: 0.0,
                    mean: 0.0,
                    p50: 0.0,
                    p95: 0.0,
                    p99: 0.0,
                };
            }
            let mut sorted = values.clone();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let count = sorted.len();
            let sum: f64 = sorted.iter().sum();
            HistogramStats {
                count,
                sum,
                min: sorted[0],
                max: sorted[count - 1],
                mean: sum / count as f64,
                p50: percentile(&sorted, 50.0),
                p95: percentile(&sorted, 95.0),
                p99: percentile(&sorted, 99.0),
            }
        })
    }

    pub fn get_all_counters(&self) -> HashMap<String, u64> {
        self.counters.read().unwrap().clone()
    }

    pub fn get_all_gauges(&self) -> HashMap<String, f64> {
        self.gauges.read().unwrap().clone()
    }

    pub fn get_all_histogram_names(&self) -> Vec<String> {
        self.histograms.read().unwrap().keys().cloned().collect()
    }

    /// Export metrics in Prometheus text format.
    pub fn export_prometheus(&self) -> String {
        let mut output = String::new();

        // Counters
        for (name, value) in self.counters.read().unwrap().iter() {
            let prom_name = name.replace(['.', '-'], "_");
            output.push_str(&format!("# TYPE {} counter\n", prom_name));
            output.push_str(&format!("{} {}\n", prom_name, value));
        }

        // Gauges
        for (name, value) in self.gauges.read().unwrap().iter() {
            let prom_name = name.replace(['.', '-'], "_");
            output.push_str(&format!("# TYPE {} gauge\n", prom_name));
            output.push_str(&format!("{} {}\n", prom_name, value));
        }

        // Histograms
        for (name, values) in self.histograms.read().unwrap().iter() {
            let prom_name = name.replace(['.', '-'], "_");
            output.push_str(&format!("# TYPE {} summary\n", prom_name));
            output.push_str(&format!("{}_count {}\n", prom_name, values.len()));
            let sum: f64 = values.iter().sum();
            output.push_str(&format!("{}_sum {}\n", prom_name, sum));
        }

        output
    }
}

impl Default for MetricsRecorder {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics for a histogram.
#[derive(Debug, Clone)]
pub struct HistogramStats {
    pub count: usize,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub mean: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Context Propagation
// ═══════════════════════════════════════════════════════════════════════════════

/// Propagator for trace context across service boundaries.
pub struct ContextPropagator {
    header_prefix: String,
}

impl ContextPropagator {
    pub fn new() -> Self {
        Self {
            header_prefix: "velocity-ctx-".to_string(),
        }
    }

    pub fn with_prefix(mut self, prefix: &str) -> Self {
        self.header_prefix = prefix.to_string();
        self
    }

    /// Inject trace context into headers.
    pub fn inject(&self, context: &TraceContext) -> HashMap<String, String> {
        let mut headers = HashMap::new();
        headers.insert(
            format!("{}trace-id", self.header_prefix),
            context.trace_id.clone(),
        );
        headers.insert(
            format!("{}span-id", self.header_prefix),
            context.span_id.clone(),
        );
        if let Some(ref parent) = context.parent_span_id {
            headers.insert(
                format!("{}parent-span-id", self.header_prefix),
                parent.clone(),
            );
        }
        headers
    }

    /// Extract trace context from headers.
    pub fn extract(&self, headers: &HashMap<String, String>) -> Option<TraceContext> {
        let trace_id = headers.get(&format!("{}trace-id", self.header_prefix))?;
        let span_id = headers.get(&format!("{}span-id", self.header_prefix))?;
        let parent_span_id = headers
            .get(&format!("{}parent-span-id", self.header_prefix))
            .cloned();

        Some(TraceContext {
            trace_id: trace_id.clone(),
            span_id: span_id.clone(),
            parent_span_id,
            operation_name: "extracted".to_string(),
            start_time: now_millis(),
            end_time: None,
            attributes: HashMap::new(),
            events: Vec::new(),
            status: SpanStatus::Unset,
            links: Vec::new(),
        })
    }
}

impl Default for ContextPropagator {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Workflow Telemetry
// ═══════════════════════════════════════════════════════════════════════════════

/// High-level telemetry for workflow operations.
pub struct WorkflowTelemetry {
    tracer: Arc<Tracer>,
    metrics: Arc<MetricsRecorder>,
    propagator: ContextPropagator,
}

impl WorkflowTelemetry {
    pub fn new(service_name: &str) -> Self {
        Self {
            tracer: Arc::new(Tracer::new(service_name)),
            metrics: Arc::new(MetricsRecorder::new()),
            propagator: ContextPropagator::new(),
        }
    }

    pub fn tracer(&self) -> &Arc<Tracer> {
        &self.tracer
    }

    pub fn metrics(&self) -> &Arc<MetricsRecorder> {
        &self.metrics
    }

    pub fn propagator(&self) -> &ContextPropagator {
        &self.propagator
    }

    /// Record a workflow start.
    pub fn record_workflow_start(&self, workflow_id: &str, workflow_type: &str) -> TraceContext {
        let mut span = self
            .tracer
            .span_builder(&format!("workflow.{}", workflow_type))
            .start(&self.tracer);
        span.attributes.insert(
            "velocity.workflow.id".to_string(),
            AttributeValue::String(workflow_id.to_string()),
        );
        span.attributes.insert(
            "velocity.workflow.type".to_string(),
            AttributeValue::String(workflow_type.to_string()),
        );
        self.metrics.counter_add("velocity.workflows.started", 1);
        span
    }

    /// Record a workflow completion.
    pub fn record_workflow_complete(&self, span: &mut TraceContext, duration_ms: u64) {
        self.tracer.set_status(span, SpanStatus::Ok);
        self.tracer.end_span(span);
        self.metrics
            .histogram_record("velocity.workflow.duration", duration_ms as f64);
        self.metrics.counter_add("velocity.workflows.completed", 1);
    }

    /// Record a workflow failure.
    pub fn record_workflow_failure(&self, span: &mut TraceContext, error: &str) {
        self.tracer
            .set_status(span, SpanStatus::Error(error.to_string()));
        self.tracer.end_span(span);
        self.metrics.counter_add("velocity.workflows.failed", 1);
    }

    /// Record an activity execution.
    pub fn record_activity_start(
        &self,
        activity_type: &str,
        activity_id: &str,
        parent_span_id: &str,
    ) -> TraceContext {
        let mut span = self
            .tracer
            .span_builder(&format!("activity.{}", activity_type))
            .with_parent_span(parent_span_id)
            .start(&self.tracer);
        span.attributes.insert(
            "velocity.activity.id".to_string(),
            AttributeValue::String(activity_id.to_string()),
        );
        self.metrics.counter_add("velocity.activities.started", 1);
        span
    }

    /// Record an activity completion.
    pub fn record_activity_complete(&self, span: &mut TraceContext, duration_ms: u64) {
        self.tracer.set_status(span, SpanStatus::Ok);
        self.tracer.end_span(span);
        self.metrics
            .histogram_record("velocity.activity.duration", duration_ms as f64);
        self.metrics.counter_add("velocity.activities.completed", 1);
    }

    /// Record a signal delivery.
    pub fn record_signal(&self, _signal_name: &str) {
        self.metrics.counter_add("velocity.signals.delivered", 1);
    }

    /// Record a query.
    pub fn record_query(&self, _query_type: &str) {
        self.metrics.counter_add("velocity.queries.served", 1);
    }

    /// Record a task queue poll.
    pub fn record_poll(&self, _task_queue: &str, latency_ms: u64) {
        self.metrics
            .histogram_record("velocity.poll.latency", latency_ms as f64);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════════

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn generate_trace_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let ts = now_millis();
    let c = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{:016x}{:016x}", ts, c)
}

fn generate_span_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1000);
    let c = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{:016x}", c)
}

fn percentile(sorted_values: &[f64], p: f64) -> f64 {
    if sorted_values.is_empty() {
        return 0.0;
    }
    let idx = ((p / 100.0) * (sorted_values.len() - 1) as f64).round() as usize;
    sorted_values[idx.min(sorted_values.len() - 1)]
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tracer_create_span() {
        let tracer = Tracer::new("test-service");
        let span = tracer.span_builder("test-operation").start(&tracer);
        assert_eq!(span.operation_name, "test-operation");
        assert!(!span.trace_id.is_empty());
        assert!(!span.span_id.is_empty());
        assert_eq!(span.status, SpanStatus::Unset);
    }

    #[test]
    fn test_tracer_end_span() {
        let tracer = Tracer::new("test-service");
        let mut span = tracer.span_builder("op").start(&tracer);
        tracer.end_span(&mut span);
        assert!(span.end_time.is_some());
        let spans = tracer.get_spans();
        assert_eq!(spans.len(), 1);
    }

    #[test]
    fn test_tracer_add_event() {
        let tracer = Tracer::new("test-service");
        let mut span = tracer.span_builder("op").start(&tracer);
        tracer.add_event(&mut span, "test-event");
        assert_eq!(span.events.len(), 1);
        assert_eq!(span.events[0].name, "test-event");
    }

    #[test]
    fn test_tracer_stats() {
        let tracer = Tracer::new("test-service");
        let mut span = tracer.span_builder("op").start(&tracer);
        tracer.end_span(&mut span);
        let stats = tracer.get_stats();
        assert_eq!(stats.spans_created, 1);
        assert_eq!(stats.spans_ended, 1);
        assert_eq!(stats.spans_active, 0);
    }

    #[test]
    fn test_metrics_counter() {
        let metrics = MetricsRecorder::new();
        metrics.counter_add("test.counter", 5);
        metrics.counter_add("test.counter", 3);
        assert_eq!(metrics.counter_get("test.counter"), 8);
    }

    #[test]
    fn test_metrics_gauge() {
        let metrics = MetricsRecorder::new();
        metrics.gauge_set("test.gauge", 42.5);
        assert_eq!(metrics.gauge_get("test.gauge"), 42.5);
        metrics.gauge_set("test.gauge", 10.0);
        assert_eq!(metrics.gauge_get("test.gauge"), 10.0);
    }

    #[test]
    fn test_metrics_histogram() {
        let metrics = MetricsRecorder::new();
        for i in 1..=100 {
            metrics.histogram_record("test.hist", i as f64);
        }
        let stats = metrics.histogram_stats("test.hist").unwrap();
        assert_eq!(stats.count, 100);
        assert_eq!(stats.min, 1.0);
        assert_eq!(stats.max, 100.0);
        assert!((stats.mean - 50.5).abs() < 0.01);
    }

    #[test]
    fn test_metrics_prometheus_export() {
        let metrics = MetricsRecorder::new();
        metrics.counter_add("test.counter", 42);
        metrics.gauge_set("test.gauge", 1.5);
        let output = metrics.export_prometheus();
        assert!(output.contains("test_counter"));
        assert!(output.contains("42"));
        assert!(output.contains("test_gauge"));
        assert!(output.contains("1.5"));
    }

    #[test]
    fn test_context_propagation() {
        let tracer = Tracer::new("test");
        let span = tracer.span_builder("op").start(&tracer);
        let propagator = ContextPropagator::new();
        let headers = propagator.inject(&span);
        assert!(headers.contains_key("velocity-ctx-trace-id"));
        assert!(headers.contains_key("velocity-ctx-span-id"));

        let extracted = propagator.extract(&headers).unwrap();
        assert_eq!(extracted.trace_id, span.trace_id);
        assert_eq!(extracted.span_id, span.span_id);
    }

    #[test]
    fn test_context_propagation_custom_prefix() {
        let propagator = ContextPropagator::new().with_prefix("custom-");
        let tracer = Tracer::new("test");
        let span = tracer.span_builder("op").start(&tracer);
        let headers = propagator.inject(&span);
        assert!(headers.contains_key("custom-trace-id"));
    }

    #[test]
    fn test_workflow_telemetry() {
        let telemetry = WorkflowTelemetry::new("test-service");
        let mut span = telemetry.record_workflow_start("wf-1", "TestWorkflow");
        telemetry.record_workflow_complete(&mut span, 100);

        let metrics = telemetry.metrics();
        assert_eq!(metrics.counter_get("velocity.workflows.started"), 1);
        assert_eq!(metrics.counter_get("velocity.workflows.completed"), 1);

        let hist = metrics
            .histogram_stats("velocity.workflow.duration")
            .unwrap();
        assert_eq!(hist.count, 1);
        assert_eq!(hist.max, 100.0);
    }

    #[test]
    fn test_workflow_telemetry_failure() {
        let telemetry = WorkflowTelemetry::new("test-service");
        let mut span = telemetry.record_workflow_start("wf-1", "TestWorkflow");
        telemetry.record_workflow_failure(&mut span, "something broke");

        assert_eq!(
            telemetry.metrics().counter_get("velocity.workflows.failed"),
            1
        );
    }

    #[test]
    fn test_activity_telemetry() {
        let telemetry = WorkflowTelemetry::new("test-service");
        let parent = telemetry.record_workflow_start("wf-1", "TestWorkflow");
        let mut act_span = telemetry.record_activity_start("MyActivity", "act-1", &parent.span_id);
        telemetry.record_activity_complete(&mut act_span, 50);

        assert_eq!(
            telemetry
                .metrics()
                .counter_get("velocity.activities.started"),
            1
        );
        assert_eq!(
            telemetry
                .metrics()
                .counter_get("velocity.activities.completed"),
            1
        );
    }

    #[test]
    fn test_percentile() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        assert_eq!(percentile(&values, 50.0), 6.0);
        assert_eq!(percentile(&values, 0.0), 1.0);
        assert_eq!(percentile(&values, 100.0), 10.0);
    }

    #[test]
    fn test_tracer_get_traces() {
        let tracer = Tracer::new("test");
        let mut span1 = tracer
            .span_builder("op1")
            .with_trace_id("trace-abc")
            .start(&tracer);
        let mut span2 = tracer
            .span_builder("op2")
            .with_trace_id("trace-abc")
            .start(&tracer);
        tracer.end_span(&mut span1);
        tracer.end_span(&mut span2);

        let traces = tracer.get_traces();
        assert_eq!(traces.len(), 1);
        assert_eq!(traces.get("trace-abc").unwrap().len(), 2);
    }
}
