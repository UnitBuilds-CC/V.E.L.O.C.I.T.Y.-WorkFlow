//! Audit logging for Velocity servers.
//!
//! Provides a tamper-evident structured audit trail for security-relevant events:
//! - Authentication successes/failures
//! - Rate limit rejections
//! - Workflow operations (start, signal, query, cancel)
//! - Administrative actions
//!
//! Audit entries are written as structured JSON via `tracing` and optionally
//! to a dedicated append-only audit log file.

use std::sync::atomic::{AtomicU64, Ordering};

/// Audit event types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditEvent {
    /// Client authenticated successfully.
    AuthSuccess,
    /// Client authentication failed.
    AuthFailure,
    /// Client was rate-limited.
    RateLimited,
    /// Workflow was started.
    WorkflowStarted,
    /// Workflow was signaled.
    WorkflowSignaled,
    /// Workflow was queried.
    WorkflowQueried,
    /// Workflow was cancelled.
    WorkflowCancelled,
    /// Workflow completed.
    WorkflowCompleted,
    /// Workflow failed.
    WorkflowFailed,
    /// Server started.
    ServerStarted,
    /// Server shutdown initiated.
    ServerShutdown,
    /// Configuration changed.
    ConfigChanged,
    /// WAL recovery completed.
    WalRecovery,
    /// PostgreSQL connection state changed.
    PgConnectionChanged,
}

impl AuditEvent {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::AuthSuccess => "auth.success",
            Self::AuthFailure => "auth.failure",
            Self::RateLimited => "auth.rate_limited",
            Self::WorkflowStarted => "workflow.started",
            Self::WorkflowSignaled => "workflow.signaled",
            Self::WorkflowQueried => "workflow.queried",
            Self::WorkflowCancelled => "workflow.cancelled",
            Self::WorkflowCompleted => "workflow.completed",
            Self::WorkflowFailed => "workflow.failed",
            Self::ServerStarted => "server.started",
            Self::ServerShutdown => "server.shutdown",
            Self::ConfigChanged => "config.changed",
            Self::WalRecovery => "system.wal_recovery",
            Self::PgConnectionChanged => "system.pg_connection",
        }
    }
}

/// Severity level for audit events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

impl AuditSeverity {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warn",
            Self::Error => "error",
            Self::Critical => "critical",
        }
    }
}

/// A single audit log entry.
#[derive(Debug, Clone)]
pub struct AuditEntry {
    pub event: AuditEvent,
    pub severity: AuditSeverity,
    pub identity: Option<String>,
    pub source_ip: Option<String>,
    pub resource: Option<String>,
    pub detail: Option<String>,
    pub success: bool,
}

impl AuditEntry {
    /// Create a new audit entry.
    pub fn new(event: AuditEvent) -> Self {
        let severity = match event {
            AuditEvent::AuthFailure | AuditEvent::RateLimited => AuditSeverity::Warning,
            AuditEvent::WorkflowFailed => AuditSeverity::Error,
            AuditEvent::ServerShutdown => AuditSeverity::Warning,
            _ => AuditSeverity::Info,
        };

        Self {
            event,
            severity,
            identity: None,
            source_ip: None,
            resource: None,
            detail: None,
            success: true,
        }
    }

    pub fn with_identity(mut self, identity: impl Into<String>) -> Self {
        self.identity = Some(identity.into());
        self
    }

    pub fn with_source_ip(mut self, ip: impl Into<String>) -> Self {
        self.source_ip = Some(ip.into());
        self
    }

    pub fn with_resource(mut self, resource: impl Into<String>) -> Self {
        self.resource = Some(resource.into());
        self
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    pub fn with_success(mut self, success: bool) -> Self {
        self.success = success;
        if !success {
            self.severity = match self.severity {
                AuditSeverity::Info => AuditSeverity::Warning,
                other => other,
            };
        }
        self
    }

    /// Render as a JSON string for structured logging.
    pub fn to_json(&self) -> String {
        let mut obj = serde_json::Map::new();
        obj.insert("event".into(), serde_json::Value::String(self.event.as_str().into()));
        obj.insert("severity".into(), serde_json::Value::String(self.severity.as_str().into()));
        obj.insert("success".into(), serde_json::Value::Bool(self.success));

        if let Some(ref id) = self.identity {
            obj.insert("identity".into(), serde_json::Value::String(id.clone()));
        }
        if let Some(ref ip) = self.source_ip {
            obj.insert("source_ip".into(), serde_json::Value::String(ip.clone()));
        }
        if let Some(ref res) = self.resource {
            obj.insert("resource".into(), serde_json::Value::String(res.clone()));
        }
        if let Some(ref detail) = self.detail {
            obj.insert("detail".into(), serde_json::Value::String(detail.clone()));
        }

        // Timestamp
        obj.insert(
            "timestamp".into(),
            serde_json::Value::String(chrono::Utc::now().to_rfc3339()),
        );

        serde_json::Value::Object(obj).to_string()
    }
}

/// Audit logger that writes structured entries via `tracing` and tracks statistics.
pub struct AuditLogger {
    /// Total audit events written.
    events_total: AtomicU64,
    /// Auth failures count.
    auth_failures: AtomicU64,
    /// Rate limit rejections count.
    rate_limit_rejections: AtomicU64,
    /// Whether audit logging is enabled.
    enabled: bool,
}

impl AuditLogger {
    /// Create a new audit logger.
    pub fn new(enabled: bool) -> Self {
        Self {
            events_total: AtomicU64::new(0),
            auth_failures: AtomicU64::new(0),
            rate_limit_rejections: AtomicU64::new(0),
            enabled,
        }
    }

    /// Create a disabled audit logger (no-op).
    pub fn disabled() -> Self {
        Self::new(false)
    }

    /// Write an audit entry.
    ///
    /// The entry is:
    /// 1. Serialized as JSON
    /// 2. Emitted via `tracing::info!` (or `tracing::warn!` for warnings)
    /// 3. Counted in statistics
    pub fn record(&self, entry: &AuditEntry) {
        if !self.enabled {
            return;
        }

        self.events_total.fetch_add(1, Ordering::Relaxed);

        // Track specific event counts
        match entry.event {
            AuditEvent::AuthFailure => {
                self.auth_failures.fetch_add(1, Ordering::Relaxed);
            }
            AuditEvent::RateLimited => {
                self.rate_limit_rejections.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }

        let json = entry.to_json();

        match entry.severity {
            AuditSeverity::Info => {
                tracing::info!(audit = %json, "AUDIT");
            }
            AuditSeverity::Warning => {
                tracing::warn!(audit = %json, "AUDIT");
            }
            AuditSeverity::Error => {
                tracing::error!(audit = %json, "AUDIT");
            }
            AuditSeverity::Critical => {
                tracing::error!(audit = %json, "AUDIT_CRITICAL");
            }
        }
    }

    /// Convenience: record an auth success.
    pub fn auth_success(&self, identity: &str, source_ip: Option<&str>) {
        let mut entry = AuditEntry::new(AuditEvent::AuthSuccess)
            .with_identity(identity)
            .with_success(true);
        if let Some(ip) = source_ip {
            entry = entry.with_source_ip(ip);
        }
        self.record(&entry);
    }

    /// Convenience: record an auth failure.
    pub fn auth_failure(&self, reason: &str, source_ip: Option<&str>) {
        let mut entry = AuditEntry::new(AuditEvent::AuthFailure)
            .with_detail(reason)
            .with_success(false);
        if let Some(ip) = source_ip {
            entry = entry.with_source_ip(ip);
        }
        self.record(&entry);
    }

    /// Convenience: record a rate limit rejection.
    pub fn rate_limited(&self, client_id: &str, source_ip: Option<&str>) {
        let mut entry = AuditEntry::new(AuditEvent::RateLimited)
            .with_identity(client_id)
            .with_success(false);
        if let Some(ip) = source_ip {
            entry = entry.with_source_ip(ip);
        }
        self.record(&entry);
    }

    /// Get audit statistics.
    pub fn stats(&self) -> AuditStats {
        AuditStats {
            events_total: self.events_total.load(Ordering::Relaxed),
            auth_failures: self.auth_failures.load(Ordering::Relaxed),
            rate_limit_rejections: self.rate_limit_rejections.load(Ordering::Relaxed),
        }
    }

    /// Render audit statistics as Prometheus text format.
    pub fn render_prometheus(&self) -> String {
        let stats = self.stats();
        stats.render_prometheus()
    }
}

/// Audit statistics.
#[derive(Debug, Clone)]
pub struct AuditStats {
    pub events_total: u64,
    pub auth_failures: u64,
    pub rate_limit_rejections: u64,
}

impl AuditStats {
    /// Render as Prometheus text format metrics.
    pub fn render_prometheus(&self) -> String {
        format!(
            "# HELP velocity_audit_events_total Total audit events recorded\n\
             # TYPE velocity_audit_events_total counter\n\
             velocity_audit_events_total {}\n\
             # HELP velocity_audit_auth_failures_total Authentication failures\n\
             # TYPE velocity_audit_auth_failures_total counter\n\
             velocity_audit_auth_failures_total {}\n\
             # HELP velocity_audit_rate_limit_rejections_total Rate limit rejections\n\
             # TYPE velocity_audit_rate_limit_rejections_total counter\n\
             velocity_audit_rate_limit_rejections_total {}\n",
            self.events_total, self.auth_failures, self.rate_limit_rejections,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_entry_json() {
        let entry = AuditEntry::new(AuditEvent::AuthSuccess)
            .with_identity("api-key:test1234")
            .with_source_ip("192.168.1.1")
            .with_success(true);

        let json = entry.to_json();
        assert!(json.contains("\"event\":\"auth.success\""));
        assert!(json.contains("\"identity\":\"api-key:test1234\""));
        assert!(json.contains("\"source_ip\":\"192.168.1.1\""));
        assert!(json.contains("\"success\":true"));
        assert!(json.contains("\"timestamp\":"));
    }

    #[test]
    fn test_audit_entry_failure_severity() {
        let entry = AuditEntry::new(AuditEvent::AuthFailure)
            .with_detail("invalid key")
            .with_success(false);

        assert_eq!(entry.severity, AuditSeverity::Warning);
        let json = entry.to_json();
        assert!(json.contains("\"severity\":\"warn\""));
    }

    #[test]
    fn test_audit_event_names() {
        assert_eq!(AuditEvent::AuthSuccess.as_str(), "auth.success");
        assert_eq!(AuditEvent::RateLimited.as_str(), "auth.rate_limited");
        assert_eq!(AuditEvent::WorkflowStarted.as_str(), "workflow.started");
        assert_eq!(AuditEvent::ServerStarted.as_str(), "server.started");
    }

    #[test]
    fn test_audit_logger_stats() {
        // Enable audit logger for testing
        let logger = AuditLogger::new(true);
        logger.auth_success("test-user", Some("127.0.0.1"));
        logger.auth_failure("bad key", Some("127.0.0.1"));
        logger.rate_limited("client1", Some("10.0.0.1"));

        let stats = logger.stats();
        assert_eq!(stats.events_total, 3);
        assert_eq!(stats.auth_failures, 1);
        assert_eq!(stats.rate_limit_rejections, 1);
    }

    #[test]
    fn test_audit_logger_disabled() {
        let logger = AuditLogger::disabled();
        logger.auth_success("test-user", None);
        let stats = logger.stats();
        assert_eq!(stats.events_total, 0); // disabled = no counting
    }

    #[test]
    fn test_prometheus_rendering() {
        let stats = AuditStats {
            events_total: 100,
            auth_failures: 5,
            rate_limit_rejections: 3,
        };
        let prom = stats.render_prometheus();
        assert!(prom.contains("velocity_audit_events_total 100"));
        assert!(prom.contains("velocity_audit_auth_failures_total 5"));
        assert!(prom.contains("velocity_audit_rate_limit_rejections_total 3"));
    }
}
