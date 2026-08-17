//! Distributed tracing foundation using OpenTelemetry.
//!
//! Provides configurable tracing with optional OTLP export to collectors
//! like Jaeger, Tempo, or Grafana OTLP endpoint.
//!
//! # Architecture
//!
//! ```text
//! [Velocity Server] ──tracing──► [tracing-opentelemetry] ──OTLP──► [Collector]
//!      │                              │                              │
//!      │  span: workflow.execute      │  batch export               │  Jaeger
//!      │  span: step.persist          │  every 1s                   │  Tempo
//!      │  span: signal.deliver        │                             │  Grafana
//! ```
//!
//! # Usage
//!
//! ```rust,no_run
//! use velocity_server_bootstrap::tracing_setup::{TracingConfig, init_tracing};
//!
//! let config = TracingConfig {
//!     service_name: "velocity-vctp".to_string(),
//!     otlp_endpoint: Some("http://localhost:4317".to_string()),
//!     log_format: LogFormat::Json,
//!     log_level: "info".to_string(),
//!     sample_rate: 1.0,
//! };
//! let _guard = init_tracing(&config);
//! ```
//!
//! When the `otel` feature is not enabled, tracing works with local fmt/json
//! output only (no OTLP export).

use std::fmt;

/// Log output format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogFormat {
    /// Human-readable single-line format.
    Compact,
    /// JSON structured format (recommended for production).
    Json,
}

impl fmt::Display for LogFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogFormat::Compact => write!(f, "compact"),
            LogFormat::Json => write!(f, "json"),
        }
    }
}

impl LogFormat {
    /// Parse from string (case-insensitive).
    pub fn parse(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "json" => LogFormat::Json,
            _ => LogFormat::Compact,
        }
    }
}

/// Configuration for the tracing system.
#[derive(Debug, Clone)]
pub struct TracingConfig {
    /// Service name for OTLP resource attributes.
    pub service_name: String,
    /// OTLP gRPC endpoint (e.g., "http://localhost:4317").
    /// If None, OTLP export is disabled.
    pub otlp_endpoint: Option<String>,
    /// Log output format.
    pub log_format: LogFormat,
    /// Minimum log level filter (e.g., "info", "debug", "velocity=trace").
    pub log_level: String,
    /// Trace sampling rate (0.0 = none, 1.0 = all). Only used with OTLP.
    pub sample_rate: f64,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            service_name: "velocity-server".to_string(),
            otlp_endpoint: None,
            log_format: LogFormat::Compact,
            log_level: "info".to_string(),
            sample_rate: 1.0,
        }
    }
}

impl TracingConfig {
    /// Create a config from environment variables.
    ///
    /// - `VELOCITY_OTEL_ENDPOINT` — OTLP gRPC endpoint
    /// - `VELOCITY_LOG_FORMAT` — "json" or "compact"
    /// - `VELOCITY_LOG_LEVEL` — log level filter
    /// - `VELOCITY_SERVICE_NAME` — service name for tracing
    /// - `VELOCITY_SAMPLE_RATE` — trace sample rate (0.0-1.0)
    pub fn from_env() -> Self {
        Self {
            service_name: std::env::var("VELOCITY_SERVICE_NAME")
                .unwrap_or_else(|_| "velocity-server".to_string()),
            otlp_endpoint: std::env::var("VELOCITY_OTEL_ENDPOINT").ok(),
            log_format: std::env::var("VELOCITY_LOG_FORMAT")
                .map(|s| LogFormat::parse(&s))
                .unwrap_or(LogFormat::Compact),
            log_level: std::env::var("VELOCITY_LOG_LEVEL")
                .unwrap_or_else(|_| "info".to_string()),
            sample_rate: std::env::var("VELOCITY_SAMPLE_RATE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1.0),
        }
    }

    /// Whether OTLP export is enabled.
    pub fn otel_enabled(&self) -> bool {
        self.otlp_endpoint.is_some()
    }
}

/// Guard that shuts down the tracing system when dropped.
/// Must be held for the lifetime of the server.
pub struct TracingGuard {
    #[cfg(feature = "otel")]
    _otel_guard: Option<OtelGuard>,
}

#[cfg(feature = "otel")]
struct OtelGuard {
    provider: Option<opentelemetry_sdk::trace::TracerProvider>,
}

#[cfg(feature = "otel")]
impl Drop for OtelGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.provider.take() {
            if let Err(e) = provider.shutdown() {
                eprintln!("[tracing] error shutting down OTLP provider: {:?}", e);
            }
        }
    }
}

impl Drop for TracingGuard {
    fn drop(&mut self) {
        // OTLP guard is dropped automatically via its field
    }
}

/// Initialize the tracing subscriber with the given configuration.
///
/// Returns a `TracingGuard` that must be held for the server's lifetime.
/// When dropped, it flushes any pending OTLP spans and shuts down cleanly.
///
/// # Feature Flags
///
/// - Without `otel`: Sets up `tracing_subscriber` with fmt/json output and env filter.
/// - With `otel`: Additionally configures OTLP export if `otlp_endpoint` is set.
pub fn init_tracing(config: &TracingConfig) -> TracingGuard {
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::EnvFilter;

    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.log_level));

    #[cfg(feature = "otel")]
    {
        if let Some(ref endpoint) = config.otlp_endpoint {
            return init_with_otel(config, endpoint, env_filter);
        }
    }

    // Without OTLP (or without otel feature)
    match config.log_format {
        LogFormat::Json => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(tracing_subscriber::fmt::layer().json())
                .init();
        }
        LogFormat::Compact => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(tracing_subscriber::fmt::layer())
                .init();
        }
    }

    TracingGuard {
        #[cfg(feature = "otel")]
        _otel_guard: None,
    }
}

#[cfg(feature = "otel")]
fn init_with_otel(
    config: &TracingConfig,
    endpoint: &str,
    env_filter: EnvFilter,
) -> TracingGuard {
    use opentelemetry::trace::TraceConfig;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::trace::{self, Sampler};
    use tracing_subscriber::prelude::*;

    let sampler = if config.sample_rate >= 1.0 {
        Sampler::AlwaysOn
    } else if config.sample_rate <= 0.0 {
        Sampler::AlwaysOff
    } else {
        Sampler::TraceIdRatioBased(config.sample_rate)
    };

    let trace_config = TraceConfig::default().with_sampler(sampler);

    let exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint(endpoint.to_string());

    let provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
        .with_config(trace_config)
        .build();

    let tracer = provider.tracer(&config.service_name);
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    match config.log_format {
        LogFormat::Json => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(tracing_subscriber::fmt::layer().json())
                .with(otel_layer)
                .init();
        }
        LogFormat::Compact => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(tracing_subscriber::fmt::layer())
                .with(otel_layer)
                .init();
        }
    }

    TracingGuard {
        _otel_guard: Some(OtelGuard {
            provider: Some(provider),
        }),
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Workflow Span Helpers
// ═══════════════════════════════════════════════════════════════════════════════

/// Well-known span names for workflow operations.
/// Using constants ensures consistency across all server flavors.
pub mod span_names {
    pub const WORKFLOW_START: &str = "workflow.start";
    pub const WORKFLOW_SIGNAL: &str = "workflow.signal";
    pub const WORKFLOW_QUERY: &str = "workflow.query";
    pub const WORKFLOW_COMPLETE: &str = "workflow.complete";
    pub const WORKFLOW_FAIL: &str = "workflow.fail";
    pub const WORKFLOW_CANCEL: &str = "workflow.cancel";
    pub const STEP_EXECUTE: &str = "step.execute";
    pub const STEP_PERSIST: &str = "step.persist";
    pub const WAL_APPEND: &str = "wal.append";
    pub const WAL_RECOVER: &str = "wal.recover";
    pub const PG_SAVE_STEP: &str = "pg.save_step";
    pub const PG_RECOVER: &str = "pg.recover";
    pub const ACTIVITY_EXECUTE: &str = "activity.execute";
    pub const TIMER_SCHEDULE: &str = "timer.schedule";
    pub const TIMER_FIRE: &str = "timer.fire";
    pub const SIGNAL_DELIVER: &str = "signal.deliver";
    pub const QUERY_HANDLE: &str = "query.handle";
}

/// Well-known attribute keys for workflow spans.
pub mod attr_keys {
    pub const WORKFLOW_KEY: &str = "velocity.workflow_key";
    pub const WORKFLOW_ID: &str = "velocity.workflow_id";
    pub const NAMESPACE: &str = "velocity.namespace";
    pub const STEP_NUMBER: &str = "velocity.step_number";
    pub const SIGNAL_NAME: &str = "velocity.signal_name";
    pub const QUERY_TYPE: &str = "velocity.query_type";
    pub const ACTIVITY_TYPE: &str = "velocity.activity_type";
    pub const INSTANCE_ID: &str = "velocity.instance_id";
    pub const SERVER_FLAVOR: &str = "velocity.server_flavor";
}

/// Create a tracing span for starting a workflow.
pub fn span_workflow_start(workflow_key: u64, namespace: &str) -> tracing::Span {
    tracing::info_span!(
        span_names::WORKFLOW_START,
        velocity.workflow_key = workflow_key,
        velocity.namespace = namespace,
    )
}

/// Create a tracing span for signaling a workflow.
pub fn span_workflow_signal(workflow_key: u64, signal_name: &str) -> tracing::Span {
    tracing::info_span!(
        span_names::WORKFLOW_SIGNAL,
        velocity.workflow_key = workflow_key,
        velocity.signal_name = signal_name,
    )
}

/// Create a tracing span for querying a workflow.
pub fn span_workflow_query(workflow_key: u64, query_type: &str) -> tracing::Span {
    tracing::info_span!(
        span_names::WORKFLOW_QUERY,
        velocity.workflow_key = workflow_key,
        velocity.query_type = query_type,
    )
}

/// Create a tracing span for a step operation.
pub fn span_step_execute(workflow_key: u64, step_number: u32) -> tracing::Span {
    tracing::info_span!(
        span_names::STEP_EXECUTE,
        velocity.workflow_key = workflow_key,
        velocity.step_number = step_number,
    )
}

/// Create a tracing span for step persistence.
pub fn span_step_persist(workflow_key: u64, step_number: u32) -> tracing::Span {
    tracing::info_span!(
        span_names::STEP_PERSIST,
        velocity.workflow_key = workflow_key,
        velocity.step_number = step_number,
    )
}

/// Create a tracing span for WAL append operations.
pub fn span_wal_append() -> tracing::Span {
    tracing::info_span!(span_names::WAL_APPEND)
}

/// Create a tracing span for WAL recovery operations.
pub fn span_wal_recover() -> tracing::Span {
    tracing::info_span!(span_names::WAL_RECOVER)
}

/// Create a tracing span for PG step save operations.
pub fn span_pg_save_step() -> tracing::Span {
    tracing::info_span!(span_names::PG_SAVE_STEP)
}

/// Create a tracing span for PG recovery operations.
pub fn span_pg_recover() -> tracing::Span {
    tracing::info_span!(span_names::PG_RECOVER)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Context Propagation
// ═══════════════════════════════════════════════════════════════════════════════

/// Propagation header keys for distributed tracing context.
/// These match the W3C Trace Context standard.
pub mod propagation {
    pub const TRACEPARENT: &str = "traceparent";
    pub const TRACESTATE: &str = "tracestate";
}

/// Extract trace context from key-value pairs (e.g., HTTP headers).
/// Returns a HashMap suitable for injecting into the tracing context.
pub fn extract_trace_context(
    headers: &[(String, String)],
) -> std::collections::HashMap<String, String> {
    let mut context = std::collections::HashMap::new();
    for (key, value) in headers {
        let lower = key.to_lowercase();
        if lower == propagation::TRACEPARENT || lower == propagation::TRACESTATE {
            context.insert(lower, value.clone());
        }
    }
    context
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tracing_config_default() {
        let config = TracingConfig::default();
        assert_eq!(config.service_name, "velocity-server");
        assert!(config.otlp_endpoint.is_none());
        assert_eq!(config.log_format, LogFormat::Compact);
        assert_eq!(config.log_level, "info");
        assert!((config.sample_rate - 1.0).abs() < f64::EPSILON);
        assert!(!config.otel_enabled());
    }

    #[test]
    fn test_tracing_config_with_otel() {
        let config = TracingConfig {
            otlp_endpoint: Some("http://jaeger:4317".to_string()),
            ..Default::default()
        };
        assert!(config.otel_enabled());
    }

    #[test]
    fn test_log_format_parse() {
        assert_eq!(LogFormat::parse("json"), LogFormat::Json);
        assert_eq!(LogFormat::parse("JSON"), LogFormat::Json);
        assert_eq!(LogFormat::parse("Json"), LogFormat::Json);
        assert_eq!(LogFormat::parse("compact"), LogFormat::Compact);
        assert_eq!(LogFormat::parse("anything"), LogFormat::Compact);
    }

    #[test]
    fn test_log_format_display() {
        assert_eq!(format!("{}", LogFormat::Json), "json");
        assert_eq!(format!("{}", LogFormat::Compact), "compact");
    }

    #[test]
    fn test_span_names_are_unique() {
        let names = vec![
            span_names::WORKFLOW_START,
            span_names::WORKFLOW_SIGNAL,
            span_names::WORKFLOW_QUERY,
            span_names::WORKFLOW_COMPLETE,
            span_names::WORKFLOW_FAIL,
            span_names::WORKFLOW_CANCEL,
            span_names::STEP_EXECUTE,
            span_names::STEP_PERSIST,
            span_names::WAL_APPEND,
            span_names::WAL_RECOVER,
            span_names::PG_SAVE_STEP,
            span_names::PG_RECOVER,
            span_names::ACTIVITY_EXECUTE,
            span_names::TIMER_SCHEDULE,
            span_names::TIMER_FIRE,
            span_names::SIGNAL_DELIVER,
            span_names::QUERY_HANDLE,
        ];
        let mut unique = names.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(names.len(), unique.len(), "span names should be unique");
    }

    #[test]
    fn test_attr_keys_are_valid() {
        // Just verify they're non-empty strings
        assert!(!attr_keys::WORKFLOW_KEY.is_empty());
        assert!(!attr_keys::NAMESPACE.is_empty());
        assert!(!attr_keys::STEP_NUMBER.is_empty());
        assert!(!attr_keys::SERVER_FLAVOR.is_empty());
    }

    #[test]
    fn test_extract_trace_context() {
        let headers = vec![
            ("Traceparent".to_string(), "00-abc123-def456-01".to_string()),
            ("Tracestate".to_string(), "key=value".to_string()),
            ("X-Custom-Header".to_string(), "ignored".to_string()),
            ("Content-Type".to_string(), "application/json".to_string()),
        ];

        let context = extract_trace_context(&headers);
        assert_eq!(context.len(), 2);
        assert_eq!(context.get("traceparent").unwrap(), "00-abc123-def456-01");
        assert_eq!(context.get("tracestate").unwrap(), "key=value");
    }

    #[test]
    fn test_extract_trace_context_empty() {
        let headers: Vec<(String, String)> = vec![];
        let context = extract_trace_context(&headers);
        assert!(context.is_empty());
    }

    #[test]
    fn test_extract_trace_context_no_trace_headers() {
        let headers = vec![
            ("Content-Type".to_string(), "application/json".to_string()),
            ("Authorization".to_string(), "Bearer token".to_string()),
        ];
        let context = extract_trace_context(&headers);
        assert!(context.is_empty());
    }

    #[test]
    fn test_workflow_span_creation() {
        let span = span_workflow_start(42, "default");
        // Verify the span was created (we can't easily inspect span contents
        // without a subscriber, but we can verify it doesn't panic)
        let _enter = span.enter();
    }

    #[test]
    fn test_step_span_creation() {
        let span = span_step_execute(42, 3);
        let _enter = span.enter();
    }

    #[test]
    fn test_propagation_constants() {
        assert_eq!(propagation::TRACEPARENT, "traceparent");
        assert_eq!(propagation::TRACESTATE, "tracestate");
    }
}
