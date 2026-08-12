//! Health check subsystem for the VELOCITY workflow engine.
//!
//! Provides structured health probes for the engine core, database adapter,
//! and replication layer. Results are aggregated into a single health report
//! suitable for Kubernetes liveness/readiness probes and load balancer checks.

use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::HashMap;
use std::sync::Arc;

use crate::engine::WorkflowEngine;
use crate::db_adapter::{DatabaseAdapter, StatusFilter};
use crate::cluster::ClusterManager;

// ─── Health Status ──────────────────────────────────────────────────────────

/// Health status of an individual component.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealthStatus {
    /// Component is operating normally.
    Healthy,
    /// Component is functional but experiencing issues.
    Degraded(String),
    /// Component is non-functional.
    Unhealthy(String),
}

impl HealthStatus {
    /// Returns `true` if the status is `Healthy`.
    pub fn is_healthy(&self) -> bool {
        matches!(self, HealthStatus::Healthy)
    }

    /// Returns a machine-readable status string.
    pub fn as_str(&self) -> &str {
        match self {
            HealthStatus::Healthy => "healthy",
            HealthStatus::Degraded(_) => "degraded",
            HealthStatus::Unhealthy(_) => "unhealthy",
        }
    }

    /// Extracts the reason, if any.
    pub fn reason(&self) -> Option<&str> {
        match self {
            HealthStatus::Healthy => None,
            HealthStatus::Degraded(r) | HealthStatus::Unhealthy(r) => Some(r.as_str()),
        }
    }
}

impl std::fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthStatus::Healthy => write!(f, "healthy"),
            HealthStatus::Degraded(r) => write!(f, "degraded: {}", r),
            HealthStatus::Unhealthy(r) => write!(f, "unhealthy: {}", r),
        }
    }
}

// ─── Aggregate Health ───────────────────────────────────────────────────────

/// Aggregated health report across all engine components.
#[derive(Debug, Clone)]
pub struct AggregateHealth {
    /// Overall status — the worst status among all components.
    pub overall_status: HealthStatus,
    /// Per-component status map.
    pub component_statuses: HashMap<String, HealthStatus>,
    /// Unix timestamp (seconds) when the check was performed.
    pub timestamp: u64,
}

impl AggregateHealth {
    /// Compute the overall status from individual component statuses.
    fn compute_overall(statuses: &HashMap<String, HealthStatus>) -> HealthStatus {
        let mut has_degraded = false;
        for status in statuses.values() {
            match status {
                HealthStatus::Unhealthy(r) => return HealthStatus::Unhealthy(r.clone()),
                HealthStatus::Degraded(_) => has_degraded = true,
                HealthStatus::Healthy => {}
            }
        }
        if has_degraded {
            HealthStatus::Degraded("one or more components degraded".into())
        } else {
            HealthStatus::Healthy
        }
    }
}

impl std::fmt::Display for AggregateHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "overall={} timestamp={}", self.overall_status, self.timestamp)?;
        for (name, status) in &self.component_statuses {
            write!(f, " {}={}", name, status)?;
        }
        Ok(())
    }
}

// ─── HealthChecker ──────────────────────────────────────────────────────────

/// Performs health checks against engine subsystems.
///
/// Holds references to the engine, database adapter, and cluster manager
/// so that checks do not prevent shutdown or cause borrow conflicts.
pub struct HealthChecker {
    engine: Arc<WorkflowEngine>,
    db_adapter: Option<Arc<dyn DatabaseAdapter>>,
    cluster_manager: Option<Arc<ClusterManager>>,
}

impl HealthChecker {
    /// Create a new `HealthChecker` bound to the given engine.
    pub fn new(
        engine: Arc<WorkflowEngine>,
        db_adapter: Option<Arc<dyn DatabaseAdapter>>,
        cluster_manager: Option<Arc<ClusterManager>>,
    ) -> Self {
        Self { engine, db_adapter, cluster_manager }
    }

    /// Check the health of the workflow engine core.
    ///
    /// Verifies that the engine is responsive by calling `workflow_count()`.
    pub fn check_engine(&self) -> HealthStatus {
        // Calling a public method that internally acquires the read lock.
        // If the lock is poisoned this will panic, which we catch.
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.engine.workflow_count())) {
            Ok(_) => HealthStatus::Healthy,
            Err(_) => HealthStatus::Unhealthy("engine lock poisoned or panicked".into()),
        }
    }

    /// Check the health of the database adapter.
    ///
    /// If no adapter is configured, returns `Healthy` (no-op).
    pub fn check_database(&self) -> HealthStatus {
        let adapter = match &self.db_adapter {
            Some(a) => a,
            None => return HealthStatus::Healthy,
        };

        // Attempt a lightweight ping via list_workflows with a minimal request.
        match adapter.list_workflows(None, StatusFilter::All, 1, 0) {
            Ok(_) => HealthStatus::Healthy,
            Err(e) => HealthStatus::Unhealthy(format!("database check failed: {}", e)),
        }
    }

    /// Check the health of the replication layer.
    ///
    /// If no cluster manager is configured, returns `Healthy` (no-op).
    pub fn check_replication(&self) -> HealthStatus {
        let cluster = match &self.cluster_manager {
            Some(c) => c,
            None => return HealthStatus::Healthy,
        };

        let local_id = cluster.local_cluster_id();
        match cluster.get_cluster(local_id) {
            Some(ci) if ci.is_active => HealthStatus::Healthy,
            Some(_) => HealthStatus::Degraded("local cluster is not active".into()),
            None => HealthStatus::Unhealthy("local cluster not found".into()),
        }
    }

    /// Run all health checks and return an aggregate report.
    pub fn check_all(&self) -> AggregateHealth {
        let mut statuses = HashMap::new();

        statuses.insert("engine".into(), self.check_engine());
        statuses.insert("database".into(), self.check_database());
        statuses.insert("replication".into(), self.check_replication());

        let overall = AggregateHealth::compute_overall(&statuses);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        AggregateHealth {
            overall_status: overall,
            component_statuses: statuses,
            timestamp,
        }
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_engine() -> Arc<WorkflowEngine> {
        Arc::new(WorkflowEngine::new())
    }

    #[test]
    fn test_healthy_engine_check() {
        let engine = make_engine();
        let checker = HealthChecker::new(engine, None, None);
        let status = checker.check_engine();
        assert!(status.is_healthy(), "expected Healthy, got {:?}", status);
    }

    #[test]
    fn test_database_no_adapter_is_healthy() {
        let engine = make_engine();
        let checker = HealthChecker::new(engine, None, None);
        let status = checker.check_database();
        assert!(status.is_healthy());
    }

    #[test]
    fn test_replication_no_cluster_is_healthy() {
        let engine = make_engine();
        let checker = HealthChecker::new(engine, None, None);
        let status = checker.check_replication();
        assert!(status.is_healthy());
    }

    #[test]
    fn test_check_all_returns_aggregate() {
        let engine = make_engine();
        let checker = HealthChecker::new(engine, None, None);
        let aggregate = checker.check_all();

        assert!(aggregate.overall_status.is_healthy());
        assert_eq!(aggregate.component_statuses.len(), 3);
        assert!(aggregate.timestamp > 0);
    }

    #[test]
    fn test_health_status_display() {
        assert_eq!(HealthStatus::Healthy.to_string(), "healthy");
        assert_eq!(
            HealthStatus::Degraded("high latency".into()).to_string(),
            "degraded: high latency"
        );
        assert_eq!(
            HealthStatus::Unhealthy("connection lost".into()).to_string(),
            "unhealthy: connection lost"
        );
    }

    #[test]
    fn test_aggregate_overall_worst_wins() {
        let mut statuses = HashMap::new();
        statuses.insert("a".into(), HealthStatus::Healthy);
        statuses.insert("b".into(), HealthStatus::Degraded("slow".into()));
        let overall = AggregateHealth::compute_overall(&statuses);
        assert_eq!(overall, HealthStatus::Degraded("one or more components degraded".into()));

        statuses.insert("c".into(), HealthStatus::Unhealthy("down".into()));
        let overall = AggregateHealth::compute_overall(&statuses);
        assert_eq!(overall, HealthStatus::Unhealthy("down".into()));
    }
}
