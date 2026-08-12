//! Workflow patches — version branching for workflow definitions.
//! Allows running different workflow logic based on a version marker,
//! enabling safe deployment of workflow changes without breaking in-flight executions.

use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Clone)]
pub struct WorkflowPatch {
    pub patch_id: u64,
    pub workflow_type_id: u64,
    pub version_marker: String,
    pub min_version: u64,
    pub max_version: u64,
    pub description: String,
    pub is_active: bool,
}

pub struct PatchRegistry {
    patches: Mutex<HashMap<u64, WorkflowPatch>>,
    next_id: Mutex<u64>,
}

impl PatchRegistry {
    pub fn new() -> Self {
        Self {
            patches: Mutex::new(HashMap::new()),
            next_id: Mutex::new(1),
        }
    }

    pub fn register_patch(
        &self,
        workflow_type_id: u64,
        version_marker: &str,
        min_version: u64,
        max_version: u64,
        description: &str,
    ) -> u64 {
        let mut id_lock = self.next_id.lock().unwrap();
        let id = *id_lock;
        *id_lock += 1;
        drop(id_lock);
        self.patches.lock().unwrap().insert(
            id,
            WorkflowPatch {
                patch_id: id,
                workflow_type_id,
                version_marker: version_marker.to_string(),
                min_version,
                max_version,
                description: description.to_string(),
                is_active: true,
            },
        );
        id
    }

    pub fn deactivate_patch(&self, patch_id: u64) -> bool {
        if let Some(p) = self.patches.lock().unwrap().get_mut(&patch_id) {
            p.is_active = false;
            true
        } else {
            false
        }
    }

    pub fn get_patch(&self, patch_id: u64) -> Option<WorkflowPatch> {
        self.patches.lock().unwrap().get(&patch_id).cloned()
    }

    /// Find the active patch for a given workflow type and version.
    pub fn find_patch(&self, workflow_type_id: u64, version: u64) -> Option<WorkflowPatch> {
        self.patches
            .lock()
            .unwrap()
            .values()
            .find(|p| {
                p.workflow_type_id == workflow_type_id
                    && p.is_active
                    && version >= p.min_version
                    && version <= p.max_version
            })
            .cloned()
    }

    pub fn patch_count(&self) -> usize {
        self.patches.lock().unwrap().len()
    }

    pub fn active_patches_for_type(&self, workflow_type_id: u64) -> Vec<WorkflowPatch> {
        self.patches
            .lock()
            .unwrap()
            .values()
            .filter(|p| p.workflow_type_id == workflow_type_id && p.is_active)
            .cloned()
            .collect()
    }
}

impl Default for PatchRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_find_patch() {
        let reg = PatchRegistry::new();
        let id = reg.register_patch(1, "v2", 100, 200, "New logic");
        let found = reg.find_patch(1, 150).unwrap();
        assert_eq!(found.patch_id, id);
        assert!(reg.find_patch(1, 50).is_none()); // below min
        assert!(reg.find_patch(1, 250).is_none()); // above max
    }

    #[test]
    fn test_deactivate_patch() {
        let reg = PatchRegistry::new();
        let id = reg.register_patch(1, "v2", 0, 1000, "Patch");
        assert!(reg.find_patch(1, 500).is_some());
        reg.deactivate_patch(id);
        assert!(reg.find_patch(1, 500).is_none());
    }

    #[test]
    fn test_active_patches_for_type() {
        let reg = PatchRegistry::new();
        reg.register_patch(1, "v2", 0, 100, "A");
        reg.register_patch(1, "v3", 101, 200, "B");
        reg.register_patch(2, "v2", 0, 100, "C");
        assert_eq!(reg.active_patches_for_type(1).len(), 2);
        assert_eq!(reg.active_patches_for_type(2).len(), 1);
    }
}
