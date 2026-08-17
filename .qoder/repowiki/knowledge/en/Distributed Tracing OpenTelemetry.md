---
kind: observability
name: Distributed Tracing OpenTelemetry
category: monitoring
scope:
    - 'velocity-server-bootstrap/src/tracing_setup.rs'
source_files:
    - velocity-server-bootstrap/src/tracing_setup.rs
---

Velocity uses OpenTelemetry as its distributed tracing foundation, with optional OTLP export to collectors like Jaeger, Tempo, or Grafana.

**Architecture:**
```
[Velocity Server] ──tracing──► [tracing-opentelemetry] ──OTLP──► [Collector]
     │                              │                              │
     │  span: workflow.execute      │  batch export               │  Jaeger
     │  span: step.persist          │  every 1s                   │  Tempo
     │  span: signal.deliver        │                             │  Grafana
```

**Configuration:**
```rust
pub struct TracingConfig {
    pub service_name: String,         // e.g., "velocity-classic"
    pub otlp_endpoint: Option<String>, // e.g., "http://localhost:4317"
    pub log_format: LogFormat,         // Compact or Json
    pub log_level: String,             // trace, debug, info, warn, error
    pub sample_rate: f64,              // 0.0 to 1.0
}

pub enum LogFormat {
    Compact,  // Human-readable single-line
    Json,     // Structured (recommended for production)
}
```

**Span Hierarchy:**
- `workflow.execute` — Top-level span for workflow execution
- `step.persist` — Per-step persistence (journal INSERT)
- `signal.deliver` — Signal delivery to workflow
- `nmcp.dispatch` — NMCP frame dispatch
- `auth.check` — Authentication verification

**Feature Flags:**
- Without `otel` feature: local fmt/JSON output only (no OTLP export)
- With `otel` feature: full OTLP export to configured collector

**Usage:**
```rust
use velocity_server_bootstrap::tracing_setup::{TracingConfig, LogFormat, init_tracing};

let config = TracingConfig {
    service_name: "velocity-classic".to_string(),
    otlp_endpoint: Some("http://localhost:4317".to_string()),
    log_format: LogFormat::Json,
    log_level: "info".to_string(),
    sample_rate: 1.0,
};
let _guard = init_tracing(&config);
// _guard must be kept alive for the tracing pipeline to flush on shutdown
```

**Key files:**
- `velocity-server-bootstrap/src/tracing_setup.rs` — Tracing initialization (515 lines)

**Rules for developers:**
1. Always use `init_tracing()` in server bootstrap, not manual tracing setup
2. Keep the returned guard alive for the server's lifetime
3. Use `sample_rate < 1.0` in production to reduce OTLP export volume
4. Prefer JSON log format in production for structured log aggregation
5. Add spans for significant operations (workflow start, step persist, signal deliver)
6. Never log secrets or PII in span attributes
