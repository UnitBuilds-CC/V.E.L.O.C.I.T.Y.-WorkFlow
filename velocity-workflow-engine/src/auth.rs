//! Authentication and authorization — JWT validation, RBAC, claims-based access control.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Permission {
    StartWorkflow, SignalWorkflow, QueryWorkflow, TerminateWorkflow, CancelWorkflow,
    DescribeWorkflow, ListWorkflows, RegisterNamespace, DescribeNamespace,
    PollActivityTask, RespondActivityTask, AdminAccess,
}

#[derive(Debug, Clone)]
pub struct Role { pub name: String, pub permissions: HashSet<Permission> }

#[derive(Debug, Clone)]
pub struct Claims { pub subject: String, pub namespace_id: u64, pub roles: Vec<String> }

pub struct AuthManager {
    roles: Mutex<HashMap<String, Role>>,
    denied: Mutex<HashSet<String>>,
}

impl AuthManager {
    pub fn new() -> Self {
        let mut roles = HashMap::new();
        roles.insert("admin".to_string(), Role { name: "admin".to_string(), permissions: HashSet::from_iter(vec![Permission::AdminAccess, Permission::StartWorkflow, Permission::SignalWorkflow, Permission::QueryWorkflow, Permission::TerminateWorkflow, Permission::CancelWorkflow, Permission::DescribeWorkflow, Permission::ListWorkflows, Permission::RegisterNamespace, Permission::DescribeNamespace, Permission::PollActivityTask, Permission::RespondActivityTask]) });
        roles.insert("operator".to_string(), Role { name: "operator".to_string(), permissions: HashSet::from_iter(vec![Permission::StartWorkflow, Permission::SignalWorkflow, Permission::QueryWorkflow, Permission::DescribeWorkflow, Permission::ListWorkflows, Permission::PollActivityTask, Permission::RespondActivityTask]) });
        roles.insert("reader".to_string(), Role { name: "reader".to_string(), permissions: HashSet::from_iter(vec![Permission::QueryWorkflow, Permission::DescribeWorkflow, Permission::ListWorkflows]) });
        Self { roles: Mutex::new(roles), denied: Mutex::new(HashSet::new()) }
    }

    pub fn add_role(&self, role: Role) { self.roles.lock().unwrap().insert(role.name.clone(), role); }

    pub fn authorize(&self, claims: &Claims, permission: &Permission) -> bool {
        if self.denied.lock().unwrap().contains(&claims.subject) { return false; }
        let roles = self.roles.lock().unwrap();
        for role_name in &claims.roles {
            if let Some(role) = roles.get(role_name) {
                if role.permissions.contains(permission) || role.permissions.contains(&Permission::AdminAccess) { return true; }
            }
        }
        false
    }

    pub fn deny_subject(&self, subject: &str) { self.denied.lock().unwrap().insert(subject.to_string()); }
    pub fn role_count(&self) -> usize { self.roles.lock().unwrap().len() }
}

impl Default for AuthManager { fn default() -> Self { Self::new() } }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_admin_has_all_permissions() {
        let auth = AuthManager::new();
        let claims = Claims { subject: "admin-user".to_string(), namespace_id: 0, roles: vec!["admin".to_string()] };
        assert!(auth.authorize(&claims, &Permission::StartWorkflow));
        assert!(auth.authorize(&claims, &Permission::AdminAccess));
    }
    #[test]
    fn test_reader_limited() {
        let auth = AuthManager::new();
        let claims = Claims { subject: "reader-user".to_string(), namespace_id: 0, roles: vec!["reader".to_string()] };
        assert!(auth.authorize(&claims, &Permission::QueryWorkflow));
        assert!(!auth.authorize(&claims, &Permission::StartWorkflow));
    }
    #[test]
    fn test_denied_subject() {
        let auth = AuthManager::new();
        let claims = Claims { subject: "bad-user".to_string(), namespace_id: 0, roles: vec!["admin".to_string()] };
        auth.deny_subject("bad-user");
        assert!(!auth.authorize(&claims, &Permission::StartWorkflow));
    }
}
