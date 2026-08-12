//! Worker deployment matching Temporal's worker deployment subsystem (~11K lines).
//! Covers: deployment registration, version tracking, traffic routing, drain, rollback.

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    RwLock,
};
use std::time::SystemTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeploymentState {
    Active,
    Draining,
    Drained,
    Deprecated,
}

#[derive(Debug, Clone)]
pub struct WorkerDeployment {
    pub deployment_id: String,
    pub namespace_id: String,
    pub build_id: String,
    pub task_queue: String,
    pub state: DeploymentState,
    pub created_at: i64,
    pub last_activity_at: Option<i64>,
    pub worker_count: u32,
    pub traffic_percentage: f64,
    pub version: i64,
    pub is_current: bool,
    pub metadata: HashMap<String, String>,
}

pub struct DeploymentManager {
    deployments: RwLock<HashMap<String, WorkerDeployment>>,
    current_by_queue: RwLock<HashMap<String, String>>,
    stats: DeploymentManagerStats,
}

#[derive(Debug, Default)]
pub struct DeploymentManagerStats {
    pub deployments_created: AtomicU64,
    pub deployments_deprecated: AtomicU64,
    pub deployments_drained: AtomicU64,
    pub traffic_changes: AtomicU64,
    pub rollbacks: AtomicU64,
}

impl DeploymentManager {
    pub fn new() -> Self {
        Self {
            deployments: RwLock::new(HashMap::new()),
            current_by_queue: RwLock::new(HashMap::new()),
            stats: DeploymentManagerStats::default(),
        }
    }

    pub fn register_deployment(
        &self,
        namespace_id: &str,
        build_id: &str,
        task_queue: &str,
    ) -> Result<String, DeploymentError> {
        let id = format!(
            "dep-{}",
            self.stats.deployments_created.load(Ordering::Relaxed) + 1
        );
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let dep = WorkerDeployment {
            deployment_id: id.clone(),
            namespace_id: namespace_id.to_string(),
            build_id: build_id.to_string(),
            task_queue: task_queue.to_string(),
            state: DeploymentState::Active,
            created_at: now,
            last_activity_at: None,
            worker_count: 0,
            traffic_percentage: 0.0,
            version: 1,
            is_current: false,
            metadata: HashMap::new(),
        };
        self.deployments.write().unwrap().insert(id.clone(), dep);
        self.stats
            .deployments_created
            .fetch_add(1, Ordering::Relaxed);
        Ok(id)
    }

    pub fn set_current(&self, deployment_id: &str) -> Result<(), DeploymentError> {
        let mut deps = self.deployments.write().unwrap();
        // Extract task_queue first to avoid holding mutable borrow
        let task_queue = {
            let dep = deps
                .get_mut(deployment_id)
                .ok_or(DeploymentError::NotFound)?;
            dep.task_queue.clone()
        };
        // Unmark previous current
        let mut current = self.current_by_queue.write().unwrap();
        if let Some(prev_id) = current.get(&task_queue).cloned() {
            if let Some(prev) = deps.get_mut(&prev_id) {
                prev.is_current = false;
            }
        }
        // Now mark new current
        if let Some(dep) = deps.get_mut(deployment_id) {
            dep.is_current = true;
            dep.traffic_percentage = 100.0;
        }
        current.insert(task_queue, deployment_id.to_string());
        self.stats.traffic_changes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn start_drain(&self, deployment_id: &str) -> Result<(), DeploymentError> {
        let mut deps = self.deployments.write().unwrap();
        let dep = deps
            .get_mut(deployment_id)
            .ok_or(DeploymentError::NotFound)?;
        dep.state = DeploymentState::Draining;
        dep.traffic_percentage = 0.0;
        Ok(())
    }

    pub fn complete_drain(&self, deployment_id: &str) -> Result<(), DeploymentError> {
        let mut deps = self.deployments.write().unwrap();
        let dep = deps
            .get_mut(deployment_id)
            .ok_or(DeploymentError::NotFound)?;
        if dep.state != DeploymentState::Draining {
            return Err(DeploymentError::InvalidTransition);
        }
        dep.state = DeploymentState::Drained;
        dep.worker_count = 0;
        self.stats
            .deployments_drained
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn deprecate(&self, deployment_id: &str) -> Result<(), DeploymentError> {
        let mut deps = self.deployments.write().unwrap();
        let dep = deps
            .get_mut(deployment_id)
            .ok_or(DeploymentError::NotFound)?;
        dep.state = DeploymentState::Deprecated;
        dep.traffic_percentage = 0.0;
        self.stats
            .deployments_deprecated
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn rollback(&self, task_queue: &str) -> Result<String, DeploymentError> {
        let deps = self.deployments.read().unwrap();
        let candidates: Vec<_> = deps
            .values()
            .filter(|d| {
                d.task_queue == task_queue && d.state == DeploymentState::Active && !d.is_current
            })
            .collect();
        if candidates.is_empty() {
            return Err(DeploymentError::NoRollbackTarget);
        }
        let target = candidates.iter().max_by_key(|d| d.created_at).unwrap();
        let target_id = target.deployment_id.clone();
        drop(deps);
        self.set_current(&target_id)?;
        self.stats.rollbacks.fetch_add(1, Ordering::Relaxed);
        Ok(target_id)
    }

    pub fn get_current(&self, task_queue: &str) -> Option<WorkerDeployment> {
        let current = self.current_by_queue.read().unwrap();
        let dep_id = current.get(task_queue)?;
        self.deployments.read().unwrap().get(dep_id).cloned()
    }

    pub fn get_deployment(&self, id: &str) -> Option<WorkerDeployment> {
        self.deployments.read().unwrap().get(id).cloned()
    }

    pub fn list_deployments(&self, namespace_id: &str) -> Vec<WorkerDeployment> {
        self.deployments
            .read()
            .unwrap()
            .values()
            .filter(|d| d.namespace_id == namespace_id)
            .cloned()
            .collect()
    }

    pub fn register_worker(&self, deployment_id: &str) -> Result<(), DeploymentError> {
        let mut deps = self.deployments.write().unwrap();
        let dep = deps
            .get_mut(deployment_id)
            .ok_or(DeploymentError::NotFound)?;
        dep.worker_count += 1;
        dep.last_activity_at = Some(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
        );
        Ok(())
    }

    pub fn stats(&self) -> &DeploymentManagerStats {
        &self.stats
    }
}

#[derive(Debug, Clone)]
pub enum DeploymentError {
    NotFound,
    InvalidTransition,
    NoRollbackTarget,
    AlreadyCurrent,
}

impl std::fmt::Display for DeploymentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "deployment not found"),
            Self::InvalidTransition => write!(f, "invalid state transition"),
            Self::NoRollbackTarget => write!(f, "no rollback target"),
            Self::AlreadyCurrent => write!(f, "already current"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_deployment() {
        let mgr = DeploymentManager::new();
        let id = mgr
            .register_deployment("ns-1", "build-1", "queue-1")
            .unwrap();
        assert!(!id.is_empty());
        let dep = mgr.get_deployment(&id).unwrap();
        assert_eq!(dep.build_id, "build-1");
        assert_eq!(dep.state, DeploymentState::Active);
    }

    #[test]
    fn test_set_current() {
        let mgr = DeploymentManager::new();
        let id = mgr.register_deployment("ns", "b1", "q").unwrap();
        mgr.set_current(&id).unwrap();
        let dep = mgr.get_deployment(&id).unwrap();
        assert!(dep.is_current);
        assert_eq!(dep.traffic_percentage, 100.0);
    }

    #[test]
    fn test_drain_lifecycle() {
        let mgr = DeploymentManager::new();
        let id = mgr.register_deployment("ns", "b1", "q").unwrap();
        mgr.set_current(&id).unwrap();
        mgr.start_drain(&id).unwrap();
        assert_eq!(
            mgr.get_deployment(&id).unwrap().state,
            DeploymentState::Draining
        );
        mgr.complete_drain(&id).unwrap();
        assert_eq!(
            mgr.get_deployment(&id).unwrap().state,
            DeploymentState::Drained
        );
    }

    #[test]
    fn test_deprecate() {
        let mgr = DeploymentManager::new();
        let id = mgr.register_deployment("ns", "b1", "q").unwrap();
        mgr.deprecate(&id).unwrap();
        assert_eq!(
            mgr.get_deployment(&id).unwrap().state,
            DeploymentState::Deprecated
        );
    }

    #[test]
    fn test_rollback() {
        let mgr = DeploymentManager::new();
        let id1 = mgr.register_deployment("ns", "b1", "q").unwrap();
        mgr.set_current(&id1).unwrap();
        let id2 = mgr.register_deployment("ns", "b2", "q").unwrap();
        mgr.set_current(&id2).unwrap();
        // Rollback should go back to id1
        let rolled_back = mgr.rollback("q").unwrap();
        assert_eq!(rolled_back, id1);
        assert!(mgr.get_deployment(&id1).unwrap().is_current);
    }

    #[test]
    fn test_register_worker() {
        let mgr = DeploymentManager::new();
        let id = mgr.register_deployment("ns", "b1", "q").unwrap();
        mgr.register_worker(&id).unwrap();
        mgr.register_worker(&id).unwrap();
        assert_eq!(mgr.get_deployment(&id).unwrap().worker_count, 2);
    }

    #[test]
    fn test_list_deployments() {
        let mgr = DeploymentManager::new();
        mgr.register_deployment("ns-1", "b1", "q1").unwrap();
        mgr.register_deployment("ns-1", "b2", "q2").unwrap();
        mgr.register_deployment("ns-2", "b3", "q1").unwrap();
        assert_eq!(mgr.list_deployments("ns-1").len(), 2);
        assert_eq!(mgr.list_deployments("ns-2").len(), 1);
    }

    #[test]
    fn test_get_current() {
        let mgr = DeploymentManager::new();
        let id = mgr.register_deployment("ns", "b1", "q").unwrap();
        mgr.set_current(&id).unwrap();
        let current = mgr.get_current("q").unwrap();
        assert_eq!(current.deployment_id, id);
    }

    #[test]
    fn test_drain_not_draining_fails() {
        let mgr = DeploymentManager::new();
        let id = mgr.register_deployment("ns", "b1", "q").unwrap();
        let err = mgr.complete_drain(&id).unwrap_err();
        assert!(matches!(err, DeploymentError::InvalidTransition));
    }
}
