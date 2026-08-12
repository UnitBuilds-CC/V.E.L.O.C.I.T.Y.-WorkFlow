// Auth E2E tests — verify authentication and authorization flows
//
// Tests JWT auth, API key auth, namespace isolation, permission enforcement,
// deny list behavior, and encryption-at-rest key rotation.

use std::collections::HashSet;
use velocity_workflow_engine::auth::{AuthManager, Claims, Permission, Role};
use velocity_workflow_engine::auth_v2::{
    ApiKeyManager, ApiPermission, EncryptionAlgorithm, EncryptionAtRest, EncryptionConfig,
};

// ============================================================================
// AuthManager — Role-Based Access Control Tests
// ============================================================================

#[test]
fn test_auth_manager_default_roles() {
    let auth = AuthManager::new();
    // Should have default roles: admin, operator, reader
    assert_eq!(auth.role_count(), 3);
}

#[test]
fn test_auth_manager_admin_has_all_permissions() {
    let auth = AuthManager::new();
    let claims = Claims {
        subject: "admin-user".to_string(),
        namespace_id: 0,
        roles: vec!["admin".to_string()],
    };
    assert!(auth.authorize(&claims, &Permission::AdminAccess));
    assert!(auth.authorize(&claims, &Permission::StartWorkflow));
    assert!(auth.authorize(&claims, &Permission::SignalWorkflow));
    assert!(auth.authorize(&claims, &Permission::QueryWorkflow));
    assert!(auth.authorize(&claims, &Permission::TerminateWorkflow));
    assert!(auth.authorize(&claims, &Permission::CancelWorkflow));
    assert!(auth.authorize(&claims, &Permission::DescribeWorkflow));
    assert!(auth.authorize(&claims, &Permission::ListWorkflows));
    assert!(auth.authorize(&claims, &Permission::RegisterNamespace));
    assert!(auth.authorize(&claims, &Permission::DescribeNamespace));
}

#[test]
fn test_auth_manager_reader_limited_permissions() {
    let auth = AuthManager::new();
    let claims = Claims {
        subject: "reader-user".to_string(),
        namespace_id: 0,
        roles: vec!["reader".to_string()],
    };
    assert!(auth.authorize(&claims, &Permission::QueryWorkflow));
    assert!(auth.authorize(&claims, &Permission::DescribeWorkflow));
    assert!(auth.authorize(&claims, &Permission::ListWorkflows));
    assert!(!auth.authorize(&claims, &Permission::StartWorkflow));
    assert!(!auth.authorize(&claims, &Permission::SignalWorkflow));
    assert!(!auth.authorize(&claims, &Permission::TerminateWorkflow));
    assert!(!auth.authorize(&claims, &Permission::AdminAccess));
}

#[test]
fn test_auth_manager_operator_permissions() {
    let auth = AuthManager::new();
    let claims = Claims {
        subject: "operator-user".to_string(),
        namespace_id: 0,
        roles: vec!["operator".to_string()],
    };
    assert!(auth.authorize(&claims, &Permission::StartWorkflow));
    assert!(auth.authorize(&claims, &Permission::SignalWorkflow));
    assert!(auth.authorize(&claims, &Permission::QueryWorkflow));
    assert!(auth.authorize(&claims, &Permission::DescribeWorkflow));
    assert!(auth.authorize(&claims, &Permission::ListWorkflows));
    // Operators should NOT have admin access
    assert!(!auth.authorize(&claims, &Permission::AdminAccess));
    assert!(!auth.authorize(&claims, &Permission::RegisterNamespace));
}

#[test]
fn test_auth_manager_custom_role() {
    let auth = AuthManager::new();
    let custom = Role {
        name: "custom-role".to_string(),
        permissions: HashSet::from_iter(vec![
            Permission::StartWorkflow,
            Permission::SignalWorkflow,
            Permission::DescribeWorkflow,
        ]),
    };
    auth.add_role(custom);
    assert_eq!(auth.role_count(), 4); // 3 defaults + 1 custom

    let claims = Claims {
        subject: "custom-user".to_string(),
        namespace_id: 0,
        roles: vec!["custom-role".to_string()],
    };
    assert!(auth.authorize(&claims, &Permission::StartWorkflow));
    assert!(auth.authorize(&claims, &Permission::SignalWorkflow));
    assert!(!auth.authorize(&claims, &Permission::AdminAccess));
}

#[test]
fn test_auth_manager_multiple_roles() {
    let auth = AuthManager::new();
    let claims = Claims {
        subject: "multi-role-user".to_string(),
        namespace_id: 0,
        roles: vec!["reader".to_string(), "operator".to_string()],
    };
    // Should have combined permissions from both roles
    assert!(auth.authorize(&claims, &Permission::StartWorkflow)); // From operator
    assert!(auth.authorize(&claims, &Permission::QueryWorkflow)); // From reader
    assert!(auth.authorize(&claims, &Permission::ListWorkflows)); // From both
}

#[test]
fn test_auth_manager_no_roles() {
    let auth = AuthManager::new();
    let claims = Claims {
        subject: "no-role-user".to_string(),
        namespace_id: 0,
        roles: vec![],
    };
    assert!(!auth.authorize(&claims, &Permission::StartWorkflow));
    assert!(!auth.authorize(&claims, &Permission::ListWorkflows));
}

#[test]
fn test_auth_manager_nonexistent_role() {
    let auth = AuthManager::new();
    let claims = Claims {
        subject: "fake-role-user".to_string(),
        namespace_id: 0,
        roles: vec!["nonexistent".to_string()],
    };
    assert!(!auth.authorize(&claims, &Permission::StartWorkflow));
}

// ============================================================================
// Deny List Tests
// ============================================================================

#[test]
fn test_auth_manager_deny_list() {
    let auth = AuthManager::new();
    let claims = Claims {
        subject: "blocked-user".to_string(),
        namespace_id: 0,
        roles: vec!["admin".to_string()],
    };

    // Admin should be authorized
    assert!(auth.authorize(&claims, &Permission::StartWorkflow));

    // Deny the subject
    auth.deny_subject("blocked-user");

    // Now should be denied despite having admin role
    assert!(!auth.authorize(&claims, &Permission::StartWorkflow));
}

#[test]
fn test_auth_manager_deny_list_does_not_affect_others() {
    let auth = AuthManager::new();
    let blocked = Claims {
        subject: "blocked-user".to_string(),
        namespace_id: 0,
        roles: vec!["admin".to_string()],
    };
    let allowed = Claims {
        subject: "allowed-user".to_string(),
        namespace_id: 0,
        roles: vec!["admin".to_string()],
    };

    auth.deny_subject("blocked-user");

    assert!(!auth.authorize(&blocked, &Permission::StartWorkflow));
    assert!(auth.authorize(&allowed, &Permission::StartWorkflow));
}

// ============================================================================
// API Key Manager Tests (auth_v2)
// ============================================================================

#[test]
fn test_api_key_manager_create_key() {
    let manager = ApiKeyManager::new();
    let raw_key = manager.create_api_key(
        "test-key",
        "default",
        vec![ApiPermission::WorkflowRead, ApiPermission::WorkflowWrite],
        0,
    );
    assert!(!raw_key.is_empty());
}

#[test]
fn test_api_key_manager_validate_key() {
    let manager = ApiKeyManager::new();
    let raw_key = manager
        .create_api_key(
            "validate-test",
            "default",
            vec![ApiPermission::WorkflowRead],
            0,
        );

    let info = manager.validate_api_key(&raw_key);
    assert!(info.is_some());
    let info = info.unwrap();
    assert_eq!(info.name, "validate-test");
    assert_eq!(info.namespace, "default");
    assert!(info.permissions.contains(&ApiPermission::WorkflowRead));
}

#[test]
fn test_api_key_manager_invalid_key() {
    let manager = ApiKeyManager::new();
    let info = manager.validate_api_key("vel_invalid_key_00000000");
    assert!(info.is_none());
}

#[test]
fn test_api_key_manager_revoke_key() {
    let manager = ApiKeyManager::new();
    let raw_key = manager
        .create_api_key(
            "revoke-test",
            "default",
            vec![ApiPermission::WorkflowRead],
            0,
        );

    assert!(manager.validate_api_key(&raw_key).is_some());
    let revoked = manager.revoke_api_key(&raw_key);
    assert!(revoked);
    assert!(manager.validate_api_key(&raw_key).is_none());
}

#[test]
fn test_api_key_manager_revoke_nonexistent() {
    let manager = ApiKeyManager::new();
    let result = manager.revoke_api_key("vel_nonexistent_key_00000000");
    assert!(!result);
}

#[test]
fn test_api_key_manager_list_keys() {
    let manager = ApiKeyManager::new();
    manager
        .create_api_key("key-1", "default", vec![ApiPermission::WorkflowRead], 0);
    manager
        .create_api_key("key-2", "default", vec![ApiPermission::WorkflowWrite], 0);

    let keys = manager.list_api_keys("default");
    assert_eq!(keys.len(), 2);
}

#[test]
fn test_api_key_manager_list_keys_namespace_isolation() {
    let manager = ApiKeyManager::new();
    manager
        .create_api_key("key-1", "ns-a", vec![ApiPermission::WorkflowRead], 0);
    manager
        .create_api_key("key-2", "ns-b", vec![ApiPermission::WorkflowRead], 0);

    let keys_a = manager.list_api_keys("ns-a");
    let keys_b = manager.list_api_keys("ns-b");
    assert_eq!(keys_a.len(), 1);
    assert_eq!(keys_b.len(), 1);
    assert_eq!(keys_a[0].name, "key-1");
    assert_eq!(keys_b[0].name, "key-2");
}

#[test]
fn test_api_key_manager_key_count() {
    let manager = ApiKeyManager::new();
    assert_eq!(manager.key_count(), 0);
    manager
        .create_api_key("key-1", "default", vec![ApiPermission::WorkflowRead], 0);
    assert_eq!(manager.key_count(), 1);
    manager
        .create_api_key("key-2", "ns-b", vec![ApiPermission::WorkflowWrite], 0);
    assert_eq!(manager.key_count(), 2);
}

#[test]
fn test_api_key_manager_rotate_key() {
    let manager = ApiKeyManager::new();
    let raw_key = manager
        .create_api_key(
            "rotate-test",
            "default",
            vec![ApiPermission::WorkflowRead],
            0,
        );

    let new_key = manager.rotate_api_key(&raw_key);
    assert!(new_key.is_some());
    let new_key = new_key.unwrap();
    assert_ne!(new_key, raw_key);

    // Old key should no longer be valid
    assert!(manager.validate_api_key(&raw_key).is_none());
    // New key should be valid
    let info = manager.validate_api_key(&new_key);
    assert!(info.is_some());
    assert_eq!(info.unwrap().name, "rotate-test");
}

// ============================================================================
// API Key Permission Tests
// ============================================================================

#[test]
fn test_api_permission_variants() {
    let perms = vec![
        ApiPermission::WorkflowRead,
        ApiPermission::WorkflowWrite,
        ApiPermission::WorkflowAdmin,
        ApiPermission::NamespaceRead,
        ApiPermission::NamespaceWrite,
        ApiPermission::SystemAdmin,
    ];
    let set: HashSet<_> = perms.iter().collect();
    assert_eq!(set.len(), perms.len());
}

#[test]
fn test_api_key_with_all_permissions() {
    let manager = ApiKeyManager::new();
    let raw_key = manager
        .create_api_key(
            "admin-key",
            "default",
            vec![
                ApiPermission::WorkflowRead,
                ApiPermission::WorkflowWrite,
                ApiPermission::WorkflowAdmin,
                ApiPermission::NamespaceRead,
                ApiPermission::NamespaceWrite,
                ApiPermission::SystemAdmin,
            ],
            0,
        );

    let info = manager.validate_api_key(&raw_key).unwrap();
    assert_eq!(info.permissions.len(), 6);
    assert!(info.permissions.contains(&ApiPermission::SystemAdmin));
}

fn test_enc_config() -> EncryptionConfig {
    EncryptionConfig {
        algorithm: EncryptionAlgorithm::Aes256Gcm,
        key_id: "test-key-1".to_string(),
        key_rotation_interval_ms: 0,
    }
}

// ============================================================================
// Encryption at Rest — Key Rotation Tests (auth_v2)
// ============================================================================

#[test]
fn test_encryption_at_rest_basic() {
    let enc = EncryptionAtRest::new(test_enc_config(), "master-key-1");
    let plaintext = b"hello world";
    let ciphertext = enc.encrypt(plaintext);
    assert_ne!(ciphertext, plaintext);
    let decrypted = enc.decrypt(&ciphertext).unwrap();
    assert_eq!(decrypted, plaintext);
}

#[test]
fn test_encryption_at_rest_key_rotation() {
    let enc = EncryptionAtRest::new(test_enc_config(), "master-key-1");
    let plaintext = b"sensitive data";

    let ciphertext_v1 = enc.encrypt(plaintext);
    enc.rotate_key("master-key-2", "test-key-2");

    // Old ciphertext should still decrypt (backward compatibility)
    let decrypted = enc.decrypt(&ciphertext_v1).unwrap();
    assert_eq!(decrypted, plaintext);

    // New encryption should use new key
    let ciphertext_v2 = enc.encrypt(plaintext);
    assert_ne!(ciphertext_v1, ciphertext_v2);

    let d1 = enc.decrypt(&ciphertext_v1).unwrap();
    let d2 = enc.decrypt(&ciphertext_v2).unwrap();
    assert_eq!(d1, plaintext);
    assert_eq!(d2, plaintext);
}

#[test]
fn test_encryption_at_rest_multiple_rotations() {
    let enc = EncryptionAtRest::new(test_enc_config(), "master-key-1");
    let plaintext = b"multi-rotation test";

    let mut ciphertexts = vec![];
    for i in 0..5 {
        ciphertexts.push(enc.encrypt(plaintext));
        enc.rotate_key(
            &format!("master-key-{}", i + 2),
            &format!("test-key-{}", i + 2),
        );
    }

    for ct in &ciphertexts {
        let decrypted = enc.decrypt(ct).unwrap();
        assert_eq!(decrypted, plaintext);
    }
}

#[test]
fn test_encryption_at_rest_decrypt_invalid_data() {
    let enc = EncryptionAtRest::new(test_enc_config(), "master-key-1");
    let invalid = b"not a valid ciphertext";
    let result = enc.decrypt(invalid);
    assert!(result.is_none());
}

// ============================================================================
// Namespace Isolation Tests
// ============================================================================

#[test]
fn test_namespace_isolation_api_keys() {
    let manager = ApiKeyManager::new();

    let key_a = manager
        .create_api_key("key-a", "namespace-a", vec![ApiPermission::WorkflowRead], 0);
    let key_b = manager
        .create_api_key("key-b", "namespace-b", vec![ApiPermission::WorkflowRead], 0);

    let info_a = manager.validate_api_key(&key_a).unwrap();
    let info_b = manager.validate_api_key(&key_b).unwrap();
    assert_eq!(info_a.namespace, "namespace-a");
    assert_eq!(info_b.namespace, "namespace-b");
}

// ============================================================================
// Auth Claims Tests
// ============================================================================

#[test]
fn test_claims_creation() {
    let claims = Claims {
        subject: "user-123".to_string(),
        namespace_id: 42,
        roles: vec!["admin".to_string(), "operator".to_string()],
    };
    assert_eq!(claims.subject, "user-123");
    assert_eq!(claims.namespace_id, 42);
    assert_eq!(claims.roles.len(), 2);
}

// ============================================================================
// Permission Edge Cases
// ============================================================================

#[test]
fn test_empty_role_has_no_permissions() {
    let auth = AuthManager::new();
    let role = Role {
        name: "empty".to_string(),
        permissions: HashSet::new(),
    };
    auth.add_role(role);

    let claims = Claims {
        subject: "empty-role-user".to_string(),
        namespace_id: 0,
        roles: vec!["empty".to_string()],
    };
    assert!(!auth.authorize(&claims, &Permission::StartWorkflow));
}

#[test]
fn test_permission_enum_completeness() {
    let perms = vec![
        Permission::StartWorkflow,
        Permission::SignalWorkflow,
        Permission::QueryWorkflow,
        Permission::TerminateWorkflow,
        Permission::CancelWorkflow,
        Permission::DescribeWorkflow,
        Permission::ListWorkflows,
        Permission::RegisterNamespace,
        Permission::DescribeNamespace,
        Permission::PollActivityTask,
        Permission::RespondActivityTask,
        Permission::AdminAccess,
    ];
    let set: HashSet<_> = perms.iter().collect();
    assert_eq!(set.len(), 12);
}

#[test]
fn test_admin_access_grants_all_permissions() {
    let auth = AuthManager::new();
    // Admin role has AdminAccess permission, which should grant all permissions
    let claims = Claims {
        subject: "admin-user".to_string(),
        namespace_id: 0,
        roles: vec!["admin".to_string()],
    };
    // Even permissions not explicitly in the admin set should be granted
    // because AdminAccess is a wildcard
    assert!(auth.authorize(&claims, &Permission::StartWorkflow));
    assert!(auth.authorize(&claims, &Permission::PollActivityTask));
    assert!(auth.authorize(&claims, &Permission::RespondActivityTask));
}
