//! Deployment API — manage worker deployments with build IDs and drainage.
//!
//! Provides deployment-level worker versioning, allowing operators to
//! promote new builds and drain old ones.

use std::collections::HashMap;
use std::sync::Mutex;

/// Status of a deployment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeploymentStatus {
    Active,
    Draining,
    Drained,
    Inactive,
}

/// Drainage status details.
#[derive(Debug, Clone)]
pub struct DrainageStatus {
    pub in_flight_workflows: usize,
    pub backlogged_workflows: usize,
    pub last_drainage_time: u64,
}

/// A deployment represents a specific build of workers.
#[derive(Debug, Clone)]
pub struct Deployment {
    pub id: String,
    pub series_name: String,
    pub build_id: String,
    pub status: DeploymentStatus,
    pub created_at: u64,
    pub task_queues: Vec<String>,
    pub drainage_status: Option<DrainageStatus>,
}

/// Manages deployments.
pub struct DeploymentManager {
    deployments: Mutex<HashMap<String, Deployment>>,
    current_by_series: Mutex<HashMap<String, String>>,
}

impl DeploymentManager {
    pub fn new() -> Self {
        Self {
            deployments: Mutex::new(HashMap::new()),
            current_by_series: Mutex::new(HashMap::new()),
        }
    }

    /// Create a new deployment.
    pub fn create_deployment(&self, id: &str, series: &str, build_id: &str, timestamp: u64) -> Deployment {
        let deployment = Deployment {
            id: id.to_string(),
            series_name: series.to_string(),
            build_id: build_id.to_string(),
            status: DeploymentStatus::Active,
            created_at: timestamp,
            task_queues: Vec::new(),
            drainage_status: None,
        };
        self.deployments.lock().unwrap().insert(id.to_string(), deployment.clone());
        deployment
    }

    /// Activate a deployment (makes it current for its series).
    pub fn activate_deployment(&self, id: &str) -> Result<(), String> {
        let deployments = self.deployments.lock().unwrap();
        let deployment = deployments.get(id).ok_or_else(|| format!("Deployment '{}' not found", id))?;
        let series = deployment.series_name.clone();
        drop(deployments);

        let mut current = self.current_by_series.lock().unwrap();
        current.insert(series, id.to_string());

        let mut deployments = self.deployments.lock().unwrap();
        if let Some(d) = deployments.get_mut(id) {
            d.status = DeploymentStatus::Active;
        }
        Ok(())
    }

    /// Start draining a deployment.
    pub fn drain_deployment(&self, id: &str) -> Result<(), String> {
        let mut deployments = self.deployments.lock().unwrap();
        let deployment = deployments.get_mut(id).ok_or_else(|| format!("Deployment '{}' not found", id))?;
        if deployment.status == DeploymentStatus::Drained {
            return Err("Deployment already drained".to_string());
        }
        deployment.status = DeploymentStatus::Draining;
        deployment.drainage_status = Some(DrainageStatus {
            in_flight_workflows: 0,
            backlogged_workflows: 0,
            last_drainage_time: 0,
        });
        Ok(())
    }

    /// Complete drainage for a deployment.
    pub fn complete_drainage(&self, id: &str) -> Result<(), String> {
        let mut deployments = self.deployments.lock().unwrap();
        let deployment = deployments.get_mut(id).ok_or_else(|| format!("Deployment '{}' not found", id))?;
        if deployment.status != DeploymentStatus::Draining {
            return Err("Deployment is not draining".to_string());
        }
        deployment.status = DeploymentStatus::Drained;
        if let Some(ref mut ds) = deployment.drainage_status {
            ds.in_flight_workflows = 0;
        }
        Ok(())
    }

    /// Get a deployment by ID.
    pub fn get_deployment(&self, id: &str) -> Option<Deployment> {
        self.deployments.lock().unwrap().get(id).cloned()
    }

    /// List deployments, optionally filtered by series.
    pub fn list_deployments(&self, series: Option<&str>) -> Vec<Deployment> {
        let deployments = self.deployments.lock().unwrap();
        match series {
            Some(s) => deployments.values().filter(|d| d.series_name == s).cloned().collect(),
            None => deployments.values().cloned().collect(),
        }
    }

    /// Get the current deployment for a series.
    pub fn get_current_deployment(&self, series: &str) -> Option<Deployment> {
        let current = self.current_by_series.lock().unwrap();
        let id = current.get(series)?;
        self.deployments.lock().unwrap().get(id).cloned()
    }

    /// Set the current deployment for a series.
    pub fn set_current_deployment(&self, series: &str, deployment_id: &str) -> Result<(), String> {
        let deployments = self.deployments.lock().unwrap();
        if !deployments.contains_key(deployment_id) {
            return Err(format!("Deployment '{}' not found", deployment_id));
        }
        drop(deployments);
        self.current_by_series.lock().unwrap().insert(series.to_string(), deployment_id.to_string());
        Ok(())
    }

    /// Add a task queue to a deployment.
    pub fn add_task_queue(&self, deployment_id: &str, task_queue: &str) -> Result<(), String> {
        let mut deployments = self.deployments.lock().unwrap();
        let deployment = deployments.get_mut(deployment_id).ok_or_else(|| format!("Deployment '{}' not found", deployment_id))?;
        if !deployment.task_queues.contains(&task_queue.to_string()) {
            deployment.task_queues.push(task_queue.to_string());
        }
        Ok(())
    }

    /// Count total deployments.
    pub fn deployment_count(&self) -> usize {
        self.deployments.lock().unwrap().len()
    }

    /// Count series.
    pub fn series_count(&self) -> usize {
        self.current_by_series.lock().unwrap().len()
    }
}

impl Default for DeploymentManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_deployment() {
        let manager = DeploymentManager::new();
        let d = manager.create_deployment("d1", "production", "v1.0.0", 1000);
        assert_eq!(d.id, "d1");
        assert_eq!(d.series_name, "production");
        assert_eq!(d.build_id, "v1.0.0");
        assert_eq!(d.status, DeploymentStatus::Active);
    }

    #[test]
    fn test_activate_deployment() {
        let manager = DeploymentManager::new();
        manager.create_deployment("d1", "production", "v1.0.0", 1000);
        manager.activate_deployment("d1").unwrap();

        let current = manager.get_current_deployment("production").unwrap();
        assert_eq!(current.id, "d1");
    }

    #[test]
    fn test_drain_deployment() {
        let manager = DeploymentManager::new();
        manager.create_deployment("d1", "production", "v1.0.0", 1000);
        manager.drain_deployment("d1").unwrap();

        let d = manager.get_deployment("d1").unwrap();
        assert_eq!(d.status, DeploymentStatus::Draining);
        assert!(d.drainage_status.is_some());
    }

    #[test]
    fn test_complete_drainage() {
        let manager = DeploymentManager::new();
        manager.create_deployment("d1", "production", "v1.0.0", 1000);
        manager.drain_deployment("d1").unwrap();
        manager.complete_drainage("d1").unwrap();

        let d = manager.get_deployment("d1").unwrap();
        assert_eq!(d.status, DeploymentStatus::Drained);
    }

    #[test]
    fn test_list_deployments() {
        let manager = DeploymentManager::new();
        manager.create_deployment("d1", "production", "v1.0.0", 1000);
        manager.create_deployment("d2", "production", "v1.1.0", 2000);
        manager.create_deployment("d3", "staging", "v1.0.0", 1500);

        assert_eq!(manager.list_deployments(None).len(), 3);
        assert_eq!(manager.list_deployments(Some("production")).len(), 2);
        assert_eq!(manager.list_deployments(Some("staging")).len(), 1);
    }

    #[test]
    fn test_add_task_queue() {
        let manager = DeploymentManager::new();
        manager.create_deployment("d1", "production", "v1.0.0", 1000);
        manager.add_task_queue("d1", "orders").unwrap();
        manager.add_task_queue("d1", "payments").unwrap();

        let d = manager.get_deployment("d1").unwrap();
        assert_eq!(d.task_queues.len(), 2);
        assert!(d.task_queues.contains(&"orders".to_string()));
    }

    #[test]
    fn test_deployment_not_found() {
        let manager = DeploymentManager::new();
        assert!(manager.activate_deployment("nonexistent").is_err());
        assert!(manager.drain_deployment("nonexistent").is_err());
        assert!(manager.get_deployment("nonexistent").is_none());
    }

    #[test]
    fn test_deployment_count() {
        let manager = DeploymentManager::new();
        assert_eq!(manager.deployment_count(), 0);
        manager.create_deployment("d1", "s1", "v1", 1000);
        manager.create_deployment("d2", "s2", "v1", 2000);
        assert_eq!(manager.deployment_count(), 2);
    }
}
