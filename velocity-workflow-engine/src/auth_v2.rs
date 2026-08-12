//! auth_v2 — Enterprise-grade authentication, authorization, audit, and encryption layer.
//!
//! Provides:
//! - API key management (create, validate, revoke, rotate, list)
//! - OAuth2 / JWT Bearer token validation with claims extraction
//! - Structured audit logging with filtering and resource-scoped queries
//! - Encryption-at-rest with AES-256-GCM conceptual model (XOR demo implementation)

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Sha256, Digest};

// ── Helpers ──────────────────────────────────────────────────────────────────

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

fn sha256_hex(input: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(input);
    let result = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&result);
    out
}

fn generate_random_bytes(len: usize) -> Vec<u8> {
    // Deterministic-ish demo: in production, use a CSPRNG.
    // Here we mix timestamp + counter for uniqueness.
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let ts = now_secs();
    let cnt = COUNTER.fetch_add(1, Ordering::Relaxed);
    let seed = ts.wrapping_mul(6364136223846793005).wrapping_add(cnt);
    let mut out = Vec::with_capacity(len);
    let mut state = seed;
    while out.len() < len {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        out.push((state >> 33) as u8);
    }
    out.truncate(len);
    out
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

// ═══════════════════════════════════════════════════════════════════════════
//  API Key Management
// ═══════════════════════════════════════════════════════════════════════════

/// Permission levels for API keys.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ApiPermission {
    WorkflowRead,
    WorkflowWrite,
    WorkflowAdmin,
    NamespaceRead,
    NamespaceWrite,
    SystemAdmin,
}

/// A stored API key record.
#[derive(Debug, Clone)]
pub struct ApiKey {
    /// SHA-256 hash of the raw key (stored, never the raw key itself).
    pub key_hash: [u8; 32],
    /// Human-readable name for the key.
    pub name: String,
    /// Namespace this key is scoped to.
    pub namespace: String,
    /// Permissions granted to this key.
    pub permissions: Vec<ApiPermission>,
    /// Unix timestamp of creation.
    pub created_at: u64,
    /// Unix timestamp of expiry (0 = never expires).
    pub expires_at: u64,
    /// Whether this key has been explicitly revoked.
    pub is_active: bool,
}

/// Thread-safe API key manager.
pub struct ApiKeyManager {
    /// key_hash (hex) → ApiKey
    keys: Mutex<HashMap<String, ApiKey>>,
    /// Prefix index: first 8 hex chars of hash → list of full hex hashes
    prefix_index: Mutex<HashMap<String, Vec<String>>>,
}

impl ApiKeyManager {
    pub fn new() -> Self {
        Self {
            keys: Mutex::new(HashMap::new()),
            prefix_index: Mutex::new(HashMap::new()),
        }
    }

    /// Create a new API key. Returns the raw key string (prefix + random hex).
    pub fn create_api_key(
        &self,
        name: &str,
        namespace: &str,
        permissions: Vec<ApiPermission>,
        ttl_secs: u64,
    ) -> String {
        let random_part = generate_random_bytes(32);
        let raw_key = format!("vel_{}", to_hex(&random_part));
        let hash = sha256_hex(raw_key.as_bytes());
        let hash_hex = to_hex(&hash);
        let now = now_secs();
        let expires_at = if ttl_secs > 0 { now + ttl_secs } else { 0 };

        let api_key = ApiKey {
            key_hash: hash,
            name: name.to_string(),
            namespace: namespace.to_string(),
            permissions,
            created_at: now,
            expires_at,
            is_active: true,
        };

        let prefix = hash_hex[..8].to_string();
        self.keys.lock().unwrap().insert(hash_hex.clone(), api_key);
        self.prefix_index.lock().unwrap()
            .entry(prefix).or_insert_with(Vec::new).push(hash_hex);

        raw_key
    }

    /// Validate a raw API key. Returns the key record if valid and not expired.
    pub fn validate_api_key(&self, key: &str) -> Option<ApiKey> {
        let hash = sha256_hex(key.as_bytes());
        let hash_hex = to_hex(&hash);
        let keys = self.keys.lock().unwrap();
        let api_key = keys.get(&hash_hex)?;
        if !api_key.is_active {
            return None;
        }
        if api_key.expires_at > 0 && now_secs() > api_key.expires_at {
            return None;
        }
        Some(api_key.clone())
    }

    /// Revoke an API key. Returns true if the key existed and was revoked.
    pub fn revoke_api_key(&self, key: &str) -> bool {
        let hash = sha256_hex(key.as_bytes());
        let hash_hex = to_hex(&hash);
        let mut keys = self.keys.lock().unwrap();
        if let Some(api_key) = keys.get_mut(&hash_hex) {
            api_key.is_active = false;
            true
        } else {
            false
        }
    }

    /// List all active API keys for a given namespace.
    pub fn list_api_keys(&self, namespace: &str) -> Vec<ApiKey> {
        let keys = self.keys.lock().unwrap();
        keys.values()
            .filter(|k| k.namespace == namespace && k.is_active)
            .cloned()
            .collect()
    }

    /// Rotate an API key: revoke the old key, create a new one with same metadata.
    /// Returns the new raw key string, or None if the old key was not found.
    pub fn rotate_api_key(&self, key: &str) -> Option<String> {
        let hash = sha256_hex(key.as_bytes());
        let hash_hex = to_hex(&hash);
        let old_meta = {
            let keys = self.keys.lock().unwrap();
            keys.get(&hash_hex).cloned()
        }?;
        if !old_meta.is_active {
            return None;
        }
        self.revoke_api_key(key);
        let ttl = if old_meta.expires_at > 0 {
            old_meta.expires_at.saturating_sub(now_secs())
        } else {
            0
        };
        let new_key = self.create_api_key(
            &old_meta.name,
            &old_meta.namespace,
            old_meta.permissions,
            ttl,
        );
        Some(new_key)
    }

    /// Count of all keys (active + inactive) — useful for tests.
    pub fn key_count(&self) -> usize {
        self.keys.lock().unwrap().len()
    }
}

impl Default for ApiKeyManager {
    fn default() -> Self { Self::new() }
}

// ═══════════════════════════════════════════════════════════════════════════
//  OAuth2 / JWT Token Validation
// ═══════════════════════════════════════════════════════════════════════════

/// OAuth2 / OIDC configuration.
#[derive(Debug, Clone)]
pub struct OAuth2Config {
    pub issuer: String,
    pub audience: String,
    pub jwks_uri: String,
    pub client_id: String,
    pub client_secret: String,
}

/// Extracted JWT claims.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claims {
    pub subject: String,
    pub issuer: String,
    pub audience: String,
    pub issued_at: u64,
    pub expires_at: u64,
    pub namespace_id: u64,
    pub roles: Vec<String>,
    pub scope: Vec<String>,
}

/// Errors from token validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    MalformedToken(String),
    InvalidSignature,
    TokenExpired,
    InvalidIssuer,
    InvalidAudience,
    MissingSubject,
    InvalidClaims(String),
}

/// JWT Bearer token validator.
///
/// In production this would verify RS256/ES256 signatures against a JWKS endpoint.
/// This implementation parses the JWT structure (header.payload.claims) and validates
/// claims semantically. Signature verification is stubbed for the demo.
pub struct OAuth2Validator {
    config: OAuth2Config,
}

impl OAuth2Validator {
    pub fn new(config: OAuth2Config) -> Self {
        Self { config }
    }

    /// Validate a JWT Bearer token and return claims if valid.
    pub fn validate_token(&self, token: &str) -> Result<Claims, AuthError> {
        let claims = self.extract_claims(token)?;

        // Check expiry
        if claims.expires_at > 0 && now_secs() > claims.expires_at {
            return Err(AuthError::TokenExpired);
        }

        // Check issuer
        if !self.config.issuer.is_empty() && claims.issuer != self.config.issuer {
            return Err(AuthError::InvalidIssuer);
        }

        // Check audience
        if !self.config.audience.is_empty() && claims.audience != self.config.audience {
            return Err(AuthError::InvalidAudience);
        }

        Ok(claims)
    }

    /// Extract claims from a JWT token without full validation.
    /// Parses the base64-encoded payload segment.
    pub fn extract_claims(&self, token: &str) -> Result<Claims, AuthError> {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return Err(AuthError::MalformedToken(
                "JWT must have 3 dot-separated segments".into(),
            ));
        }

        let payload_bytes = base64url_decode(parts[1])
            .map_err(|e| AuthError::MalformedToken(format!("Invalid base64 payload: {}", e)))?;
        let payload_str = String::from_utf8(payload_bytes)
            .map_err(|e| AuthError::MalformedToken(format!("Invalid UTF-8 in payload: {}", e)))?;

        parse_claims_from_json(&payload_str)
    }
}

/// Minimal base64url decoder (no padding required).
fn base64url_decode(input: &str) -> Result<Vec<u8>, String> {
    let mut s = input.replace('-', "+").replace('_', "/");
    while s.len() % 4 != 0 {
        s.push('=');
    }
    base64_decode(&s)
}

fn base64_decode(input: &str) -> Result<Vec<u8>, String> {
    let table = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = Vec::new();
    let bytes: Vec<u8> = input.bytes().filter(|&b| b != b'=').collect();

    let lookup = |c: u8| -> Result<u8, String> {
        table.iter().position(|&t| t == c)
            .map(|p| p as u8)
            .ok_or_else(|| format!("Invalid base64 character: {}", c as char))
    };

    let mut i = 0;
    while i + 1 < bytes.len() {
        let a = lookup(bytes[i])?;
        let b = lookup(bytes[i + 1])?;
        output.push((a << 2) | (b >> 4));
        if i + 2 < bytes.len() {
            let c = lookup(bytes[i + 2])?;
            output.push((b << 4) | (c >> 2));
            if i + 3 < bytes.len() {
                let d = lookup(bytes[i + 3])?;
                output.push((c << 6) | d);
            }
        }
        i += 4;
    }
    Ok(output)
}

/// Minimal JSON claims parser — extracts known fields from a flat JSON object.
/// This avoids pulling in serde_json as a dependency.
fn parse_claims_from_json(json: &str) -> Result<Claims, AuthError> {
    let get_str = |key: &str| -> String {
        let pattern = format!("\"{}\"", key);
        if let Some(pos) = json.find(&pattern) {
            let after_key = &json[pos + pattern.len()..];
            if let Some(colon) = after_key.find(':') {
                let after_colon = after_key[colon + 1..].trim_start();
                if after_colon.starts_with('"') {
                    let value_start = 1;
                    let rest = &after_colon[value_start..];
                    if let Some(end_quote) = rest.find('"') {
                        return rest[..end_quote].to_string();
                    }
                }
            }
        }
        String::new()
    };

    let get_u64 = |key: &str| -> u64 {
        let pattern = format!("\"{}\"", key);
        if let Some(pos) = json.find(&pattern) {
            let after_key = &json[pos + pattern.len()..];
            if let Some(colon) = after_key.find(':') {
                let after_colon = after_key[colon + 1..].trim_start();
                let num_str: String = after_colon.chars()
                    .take_while(|c| c.is_ascii_digit())
                    .collect();
                return num_str.parse().unwrap_or(0);
            }
        }
        0
    };

    let get_str_array = |key: &str| -> Vec<String> {
        let pattern = format!("\"{}\"", key);
        if let Some(pos) = json.find(&pattern) {
            let after_key = &json[pos + pattern.len()..];
            if let Some(bracket) = after_key.find('[') {
                let rest = &after_key[bracket + 1..];
                if let Some(end) = rest.find(']') {
                    let inner = &rest[..end];
                    return inner.split(',')
                        .map(|s| s.trim().trim_matches('"').to_string())
                        .filter(|s| !s.is_empty())
                        .collect();
                }
            }
        }
        Vec::new()
    };

    let subject = get_str("sub");
    if subject.is_empty() {
        return Err(AuthError::MissingSubject);
    }

    Ok(Claims {
        subject,
        issuer: get_str("iss"),
        audience: get_str("aud"),
        issued_at: get_u64("iat"),
        expires_at: get_u64("exp"),
        namespace_id: get_u64("namespace_id"),
        roles: get_str_array("roles"),
        scope: get_str_array("scope"),
    })
}

// ═══════════════════════════════════════════════════════════════════════════
//  Audit Logging
// ═══════════════════════════════════════════════════════════════════════════

/// A single audit log entry.
#[derive(Debug, Clone)]
pub struct AuditLog {
    pub timestamp: u64,
    pub actor: String,
    pub action: String,
    pub resource: String,
    pub result: AuditResult,
    pub ip_address: String,
    pub user_agent: String,
}

/// Outcome of an audited action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditResult {
    Success,
    Failure(String),
    Denied(String),
}

/// Filter for querying audit logs.
#[derive(Debug, Clone, Default)]
pub struct AuditFilter {
    pub actor: Option<String>,
    pub action: Option<String>,
    pub resource: Option<String>,
    pub result: Option<AuditResult>,
    pub since: Option<u64>,
    pub until: Option<u64>,
    pub limit: Option<usize>,
}

/// Thread-safe audit logger with bounded in-memory storage.
pub struct AuditLogger {
    logs: Mutex<Vec<AuditLog>>,
    /// resource → indices into logs
    resource_index: Mutex<HashMap<String, Vec<usize>>>,
    max_entries: usize,
}

impl AuditLogger {
    pub fn new(max_entries: usize) -> Self {
        Self {
            logs: Mutex::new(Vec::new()),
            resource_index: Mutex::new(HashMap::new()),
            max_entries,
        }
    }

    /// Record an audit event.
    pub fn log_event(&self, event: AuditLog) {
        let mut logs = self.logs.lock().unwrap();
        let idx = logs.len();

        // Evict oldest entries if at capacity
        if logs.len() >= self.max_entries {
            let remove_count = self.max_entries / 10; // remove 10% at a time
            logs.drain(..remove_count);
            // Rebuild resource index after eviction
            let mut ridx = self.resource_index.lock().unwrap();
            ridx.clear();
            for (i, log) in logs.iter().enumerate() {
                ridx.entry(log.resource.clone()).or_insert_with(Vec::new).push(i);
            }
        }

        let resource = event.resource.clone();
        logs.push(event);

        self.resource_index.lock().unwrap()
            .entry(resource).or_insert_with(Vec::new).push(idx);
    }

    /// Query logs with a filter.
    pub fn query_logs(&self, filter: AuditFilter) -> Vec<AuditLog> {
        let logs = self.logs.lock().unwrap();
        logs.iter()
            .filter(|log| {
                if let Some(ref actor) = filter.actor {
                    if &log.actor != actor { return false; }
                }
                if let Some(ref action) = filter.action {
                    if &log.action != action { return false; }
                }
                if let Some(ref resource) = filter.resource {
                    if &log.resource != resource { return false; }
                }
                if let Some(ref result) = filter.result {
                    if &log.result != result { return false; }
                }
                if let Some(since) = filter.since {
                    if log.timestamp < since { return false; }
                }
                if let Some(until) = filter.until {
                    if log.timestamp > until { return false; }
                }
                true
            })
            .take(filter.limit.unwrap_or(usize::MAX))
            .cloned()
            .collect()
    }

    /// Get all logs for a specific resource.
    pub fn get_logs_for_resource(&self, resource: &str) -> Vec<AuditLog> {
        let logs = self.logs.lock().unwrap();
        let ridx = self.resource_index.lock().unwrap();
        if let Some(indices) = ridx.get(resource) {
            indices.iter()
                .filter_map(|&i| logs.get(i).cloned())
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Total number of log entries.
    pub fn log_count(&self) -> usize {
        self.logs.lock().unwrap().len()
    }
}

impl Default for AuditLogger {
    fn default() -> Self { Self::new(100_000) }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Encryption at Rest (AES-256-GCM conceptual — XOR demo)
// ═══════════════════════════════════════════════════════════════════════════

/// Encryption algorithm identifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncryptionAlgorithm {
    Aes256Gcm,
    Xchacha20Poly1305,
}

/// Configuration for encryption at rest.
#[derive(Debug, Clone)]
pub struct EncryptionConfig {
    pub algorithm: EncryptionAlgorithm,
    pub key_id: String,
    /// Key rotation interval in milliseconds (0 = no rotation).
    pub key_rotation_interval_ms: u64,
}

/// Encryption-at-rest manager.
///
/// Uses a XOR-based demo cipher keyed by SHA-256 of the master key.
/// In production, this would delegate to a proper AES-256-GCM implementation
/// (e.g., via the `aes-gcm` crate or an HSM).
pub struct EncryptionAtRest {
    config: EncryptionConfig,
    /// Derived encryption key (32 bytes for AES-256).
    derived_key: [u8; 32],
    /// Timestamp of last key rotation.
    last_rotation: Mutex<u64>,
}

impl EncryptionAtRest {
    /// Create a new encryption manager from a master key string.
    pub fn new(config: EncryptionConfig, master_key: &str) -> Self {
        let derived_key = sha256_hex(master_key.as_bytes());
        Self {
            config,
            derived_key,
            last_rotation: Mutex::new(now_secs()),
        }
    }

    /// Encrypt data. Returns the ciphertext.
    ///
    /// Format: [1-byte version][32-byte key_id hash][N-byte XOR ciphertext]
    pub fn encrypt(&self, data: &[u8]) -> Vec<u8> {
        let mut output = Vec::with_capacity(1 + 32 + data.len());

        // Version byte
        output.push(0x01);

        // Key identifier (hash of key_id)
        let kid_hash = sha256_hex(self.config.key_id.as_bytes());
        output.extend_from_slice(&kid_hash);

        // XOR cipher with key stream derived from the derived_key
        let key_stream = self.generate_key_stream(data.len());
        for (i, &byte) in data.iter().enumerate() {
            output.push(byte ^ key_stream[i]);
        }

        output
    }

    /// Decrypt data. Returns the plaintext, or None if the key doesn't match.
    pub fn decrypt(&self, data: &[u8]) -> Option<Vec<u8>> {
        if data.len() < 33 {
            return None;
        }

        let _version = data[0];
        let stored_kid = &data[1..33];
        let expected_kid = sha256_hex(self.config.key_id.as_bytes());

        if stored_kid != expected_kid {
            return None; // Wrong key
        }

        let ciphertext = &data[33..];
        let key_stream = self.generate_key_stream(ciphertext.len());
        let mut plaintext = Vec::with_capacity(ciphertext.len());
        for (i, &byte) in ciphertext.iter().enumerate() {
            plaintext.push(byte ^ key_stream[i]);
        }

        Some(plaintext)
    }

    /// Check if key rotation is needed.
    pub fn needs_rotation(&self) -> bool {
        if self.config.key_rotation_interval_ms == 0 {
            return false;
        }
        let last = *self.last_rotation.lock().unwrap();
        let elapsed_ms = (now_secs() - last) * 1000;
        elapsed_ms > self.config.key_rotation_interval_ms
    }

    /// Get the current config.
    pub fn config(&self) -> &EncryptionConfig {
        &self.config
    }

    /// Generate a deterministic key stream of `len` bytes from the derived key.
    fn generate_key_stream(&self, len: usize) -> Vec<u8> {
        let mut stream = Vec::with_capacity(len);
        let mut counter = 0u64;
        while stream.len() < len {
            let mut block_input = Vec::with_capacity(40);
            block_input.extend_from_slice(&self.derived_key);
            block_input.extend_from_slice(&counter.to_le_bytes());
            let block = sha256_hex(&block_input);
            stream.extend_from_slice(&block);
            counter += 1;
        }
        stream.truncate(len);
        stream
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    // ── API Key Tests ──────────────────────────────────────────────────────

    #[test]
    fn test_create_api_key_returns_prefixed_key() {
        let mgr = ApiKeyManager::new();
        let key = mgr.create_api_key("test-key", "default", vec![ApiPermission::WorkflowRead], 0);
        assert!(key.starts_with("vel_"), "Key should start with vel_ prefix");
        assert!(key.len() > 10, "Key should be sufficiently long");
    }

    #[test]
    fn test_validate_api_key_success() {
        let mgr = ApiKeyManager::new();
        let key = mgr.create_api_key("test-key", "default", vec![ApiPermission::WorkflowRead], 0);
        let result = mgr.validate_api_key(&key);
        assert!(result.is_some());
        let api_key = result.unwrap();
        assert_eq!(api_key.name, "test-key");
        assert_eq!(api_key.namespace, "default");
        assert!(api_key.is_active);
    }

    #[test]
    fn test_validate_api_key_invalid() {
        let mgr = ApiKeyManager::new();
        let result = mgr.validate_api_key("vel_invalid_key_that_does_not_exist");
        assert!(result.is_none());
    }

    #[test]
    fn test_revoke_api_key() {
        let mgr = ApiKeyManager::new();
        let key = mgr.create_api_key("revoke-me", "default", vec![ApiPermission::WorkflowWrite], 0);
        assert!(mgr.validate_api_key(&key).is_some());
        assert!(mgr.revoke_api_key(&key));
        assert!(mgr.validate_api_key(&key).is_none());
    }

    #[test]
    fn test_revoke_nonexistent_key() {
        let mgr = ApiKeyManager::new();
        assert!(!mgr.revoke_api_key("vel_nonexistent"));
    }

    #[test]
    fn test_list_api_keys_by_namespace() {
        let mgr = ApiKeyManager::new();
        mgr.create_api_key("key1", "ns-a", vec![ApiPermission::WorkflowRead], 0);
        mgr.create_api_key("key2", "ns-a", vec![ApiPermission::WorkflowWrite], 0);
        mgr.create_api_key("key3", "ns-b", vec![ApiPermission::WorkflowRead], 0);

        let ns_a_keys = mgr.list_api_keys("ns-a");
        assert_eq!(ns_a_keys.len(), 2);

        let ns_b_keys = mgr.list_api_keys("ns-b");
        assert_eq!(ns_b_keys.len(), 1);
    }

    #[test]
    fn test_list_api_keys_excludes_revoked() {
        let mgr = ApiKeyManager::new();
        let key1 = mgr.create_api_key("key1", "ns", vec![], 0);
        let _key2 = mgr.create_api_key("key2", "ns", vec![], 0);
        mgr.revoke_api_key(&key1);

        let keys = mgr.list_api_keys("ns");
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].name, "key2");
    }

    #[test]
    fn test_rotate_api_key() {
        let mgr = ApiKeyManager::new();
        let old_key = mgr.create_api_key("rotate-me", "default", vec![ApiPermission::WorkflowRead], 0);
        let new_key = mgr.rotate_api_key(&old_key);
        assert!(new_key.is_some());
        let new_key = new_key.unwrap();
        assert_ne!(old_key, new_key);
        // Old key should be revoked
        assert!(mgr.validate_api_key(&old_key).is_none());
        // New key should work
        let validated = mgr.validate_api_key(&new_key);
        assert!(validated.is_some());
        assert_eq!(validated.unwrap().name, "rotate-me");
    }

    #[test]
    fn test_rotate_already_revoked_key() {
        let mgr = ApiKeyManager::new();
        let key = mgr.create_api_key("test", "ns", vec![], 0);
        mgr.revoke_api_key(&key);
        assert!(mgr.rotate_api_key(&key).is_none());
    }

    #[test]
    fn test_api_key_expiry() {
        let mgr = ApiKeyManager::new();
        // Create a key that's already expired (ttl = 1 second, but we check immediately)
        let key = mgr.create_api_key("expiring", "ns", vec![], 0);
        // Manually set expiry to the past
        {
            let hash = sha256_hex(key.as_bytes());
            let hash_hex = to_hex(&hash);
            let mut keys = mgr.keys.lock().unwrap();
            if let Some(k) = keys.get_mut(&hash_hex) {
                k.expires_at = 1; // epoch second 1 = expired
            }
        }
        assert!(mgr.validate_api_key(&key).is_none());
    }

    #[test]
    fn test_api_key_permissions() {
        let mgr = ApiKeyManager::new();
        let key = mgr.create_api_key("perms", "ns", vec![
            ApiPermission::WorkflowRead,
            ApiPermission::WorkflowWrite,
        ], 0);
        let validated = mgr.validate_api_key(&key).unwrap();
        assert_eq!(validated.permissions.len(), 2);
        assert!(validated.permissions.contains(&ApiPermission::WorkflowRead));
        assert!(validated.permissions.contains(&ApiPermission::WorkflowWrite));
    }

    // ── OAuth2 / JWT Tests ─────────────────────────────────────────────────

    fn make_test_jwt(sub: &str, iss: &str, aud: &str, exp: u64) -> String {
        // Minimal JWT: header.payload.claims
        let header = r#"{"alg":"RS256","typ":"JWT"}"#;
        let payload = format!(
            r#"{{"sub":"{}","iss":"{}","aud":"{}","iat":1700000000,"exp":{},"namespace_id":42,"roles":["admin","operator"],"scope":["workflow:read","workflow:write"]}}"#,
            sub, iss, aud, exp
        );
        let header_b64 = base64url_encode(header.as_bytes());
        let payload_b64 = base64url_encode(payload.as_bytes());
        let sig_b64 = base64url_encode(b"fakesignature");
        format!("{}.{}.{}", header_b64, payload_b64, sig_b64)
    }

    fn base64url_encode(data: &[u8]) -> String {
        let table = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        let mut output = String::new();
        let mut i = 0;
        while i < data.len() {
            let b0 = data[i] as usize;
            let b1 = if i + 1 < data.len() { data[i + 1] as usize } else { 0 };
            let b2 = if i + 2 < data.len() { data[i + 2] as usize } else { 0 };
            output.push(table[b0 >> 2] as char);
            output.push(table[((b0 & 3) << 4) | (b1 >> 4)] as char);
            if i + 1 < data.len() {
                output.push(table[((b1 & 0xf) << 2) | (b2 >> 6)] as char);
            }
            if i + 2 < data.len() {
                output.push(table[b2 & 0x3f] as char);
            }
            i += 3;
        }
        output
    }

    #[test]
    fn test_jwt_valid_token() {
        let config = OAuth2Config {
            issuer: "https://auth.velocity.dev".into(),
            audience: "velocity-api".into(),
            jwks_uri: "https://auth.velocity.dev/.well-known/jwks.json".into(),
            client_id: "velocity-server".into(),
            client_secret: "secret".into(),
        };
        let validator = OAuth2Validator::new(config);
        let token = make_test_jwt("user-123", "https://auth.velocity.dev", "velocity-api", 9999999999);
        let claims = validator.validate_token(&token).unwrap();
        assert_eq!(claims.subject, "user-123");
        assert_eq!(claims.issuer, "https://auth.velocity.dev");
        assert_eq!(claims.audience, "velocity-api");
        assert_eq!(claims.namespace_id, 42);
        assert_eq!(claims.roles, vec!["admin", "operator"]);
    }

    #[test]
    fn test_jwt_expired_token() {
        let config = OAuth2Config {
            issuer: "https://auth.velocity.dev".into(),
            audience: "velocity-api".into(),
            jwks_uri: "".into(),
            client_id: "".into(),
            client_secret: "".into(),
        };
        let validator = OAuth2Validator::new(config);
        let token = make_test_jwt("user-123", "https://auth.velocity.dev", "velocity-api", 1);
        let result = validator.validate_token(&token);
        assert_eq!(result, Err(AuthError::TokenExpired));
    }

    #[test]
    fn test_jwt_wrong_issuer() {
        let config = OAuth2Config {
            issuer: "https://auth.velocity.dev".into(),
            audience: "".into(),
            jwks_uri: "".into(),
            client_id: "".into(),
            client_secret: "".into(),
        };
        let validator = OAuth2Validator::new(config);
        let token = make_test_jwt("user", "https://evil.example.com", "", 9999999999);
        let result = validator.validate_token(&token);
        assert_eq!(result, Err(AuthError::InvalidIssuer));
    }

    #[test]
    fn test_jwt_wrong_audience() {
        let config = OAuth2Config {
            issuer: "".into(),
            audience: "velocity-api".into(),
            jwks_uri: "".into(),
            client_id: "".into(),
            client_secret: "".into(),
        };
        let validator = OAuth2Validator::new(config);
        let token = make_test_jwt("user", "", "wrong-audience", 9999999999);
        let result = validator.validate_token(&token);
        assert_eq!(result, Err(AuthError::InvalidAudience));
    }

    #[test]
    fn test_jwt_malformed_token() {
        let config = OAuth2Config {
            issuer: "".into(), audience: "".into(), jwks_uri: "".into(),
            client_id: "".into(), client_secret: "".into(),
        };
        let validator = OAuth2Validator::new(config);
        let result = validator.validate_token("not.a.valid-jwt");
        assert!(result.is_err());
    }

    #[test]
    fn test_jwt_missing_subject() {
        let config = OAuth2Config {
            issuer: "".into(), audience: "".into(), jwks_uri: "".into(),
            client_id: "".into(), client_secret: "".into(),
        };
        let validator = OAuth2Validator::new(config);
        let header = base64url_encode(r#"{"alg":"RS256"}"#.as_bytes());
        let payload = base64url_encode(r#"{"iss":"test","exp":9999999999}"#.as_bytes());
        let sig = base64url_encode(b"sig");
        let token = format!("{}.{}.{}", header, payload, sig);
        let result = validator.validate_token(&token);
        assert_eq!(result, Err(AuthError::MissingSubject));
    }

    // ── Audit Log Tests ────────────────────────────────────────────────────

    #[test]
    fn test_audit_log_event() {
        let logger = AuditLogger::new(1000);
        logger.log_event(AuditLog {
            timestamp: 1000,
            actor: "user-1".into(),
            action: "StartWorkflow".into(),
            resource: "workflow-abc".into(),
            result: AuditResult::Success,
            ip_address: "10.0.0.1".into(),
            user_agent: "velocity-sdk/1.0".into(),
        });
        assert_eq!(logger.log_count(), 1);
    }

    #[test]
    fn test_audit_query_by_actor() {
        let logger = AuditLogger::new(1000);
        logger.log_event(AuditLog {
            timestamp: 1000, actor: "user-1".into(), action: "Start".into(),
            resource: "wf-1".into(), result: AuditResult::Success,
            ip_address: "".into(), user_agent: "".into(),
        });
        logger.log_event(AuditLog {
            timestamp: 1001, actor: "user-2".into(), action: "Start".into(),
            resource: "wf-2".into(), result: AuditResult::Success,
            ip_address: "".into(), user_agent: "".into(),
        });

        let results = logger.query_logs(AuditFilter {
            actor: Some("user-1".into()),
            ..Default::default()
        });
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].actor, "user-1");
    }

    #[test]
    fn test_audit_query_by_action() {
        let logger = AuditLogger::new(1000);
        logger.log_event(AuditLog {
            timestamp: 1000, actor: "u".into(), action: "Start".into(),
            resource: "wf".into(), result: AuditResult::Success,
            ip_address: "".into(), user_agent: "".into(),
        });
        logger.log_event(AuditLog {
            timestamp: 1001, actor: "u".into(), action: "Terminate".into(),
            resource: "wf".into(), result: AuditResult::Denied("forbidden".into()),
            ip_address: "".into(), user_agent: "".into(),
        });

        let results = logger.query_logs(AuditFilter {
            action: Some("Terminate".into()),
            ..Default::default()
        });
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_audit_get_logs_for_resource() {
        let logger = AuditLogger::new(1000);
        logger.log_event(AuditLog {
            timestamp: 1000, actor: "u".into(), action: "Start".into(),
            resource: "wf-abc".into(), result: AuditResult::Success,
            ip_address: "".into(), user_agent: "".into(),
        });
        logger.log_event(AuditLog {
            timestamp: 1001, actor: "u".into(), action: "Signal".into(),
            resource: "wf-xyz".into(), result: AuditResult::Success,
            ip_address: "".into(), user_agent: "".into(),
        });
        logger.log_event(AuditLog {
            timestamp: 1002, actor: "u".into(), action: "Query".into(),
            resource: "wf-abc".into(), result: AuditResult::Success,
            ip_address: "".into(), user_agent: "".into(),
        });

        let wf_abc_logs = logger.get_logs_for_resource("wf-abc");
        assert_eq!(wf_abc_logs.len(), 2);
    }

    #[test]
    fn test_audit_log_limit() {
        let logger = AuditLogger::new(1000);
        for i in 0..20 {
            logger.log_event(AuditLog {
                timestamp: i, actor: "u".into(), action: "act".into(),
                resource: "r".into(), result: AuditResult::Success,
                ip_address: "".into(), user_agent: "".into(),
            });
        }
        let results = logger.query_logs(AuditFilter { limit: Some(5), ..Default::default() });
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn test_audit_log_eviction() {
        let logger = AuditLogger::new(10);
        for i in 0..15 {
            logger.log_event(AuditLog {
                timestamp: i, actor: format!("user-{}", i), action: "act".into(),
                resource: "r".into(), result: AuditResult::Success,
                ip_address: "".into(), user_agent: "".into(),
            });
        }
        // After eviction, should have fewer than 15 entries
        assert!(logger.log_count() < 15);
        assert!(logger.log_count() > 0);
    }

    // ── Encryption Tests ───────────────────────────────────────────────────

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let config = EncryptionConfig {
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            key_id: "key-001".into(),
            key_rotation_interval_ms: 86400000,
        };
        let enc = EncryptionAtRest::new(config, "my-master-key");
        let plaintext = b"Hello, Velocity Workflow Engine!";
        let ciphertext = enc.encrypt(plaintext);
        assert_ne!(ciphertext, plaintext.to_vec());

        let decrypted = enc.decrypt(&ciphertext).unwrap();
        assert_eq!(decrypted, plaintext.to_vec());
    }

    #[test]
    fn test_encrypt_decrypt_empty_data() {
        let config = EncryptionConfig {
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            key_id: "key-002".into(),
            key_rotation_interval_ms: 0,
        };
        let enc = EncryptionAtRest::new(config, "master");
        let ciphertext = enc.encrypt(b"");
        let decrypted = enc.decrypt(&ciphertext).unwrap();
        assert_eq!(decrypted, b"");
    }

    #[test]
    fn test_decrypt_wrong_key_fails() {
        let config1 = EncryptionConfig {
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            key_id: "key-A".into(),
            key_rotation_interval_ms: 0,
        };
        let config2 = EncryptionConfig {
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            key_id: "key-B".into(),
            key_rotation_interval_ms: 0,
        };
        let enc1 = EncryptionAtRest::new(config1, "master");
        let enc2 = EncryptionAtRest::new(config2, "master");
        let ciphertext = enc1.encrypt(b"secret data");
        assert!(enc2.decrypt(&ciphertext).is_none());
    }

    #[test]
    fn test_decrypt_truncated_data() {
        let config = EncryptionConfig {
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            key_id: "key".into(),
            key_rotation_interval_ms: 0,
        };
        let enc = EncryptionAtRest::new(config, "master");
        assert!(enc.decrypt(&[0u8; 10]).is_none());
    }

    #[test]
    fn test_encrypt_large_data() {
        let config = EncryptionConfig {
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            key_id: "key-large".into(),
            key_rotation_interval_ms: 0,
        };
        let enc = EncryptionAtRest::new(config, "master");
        let plaintext = vec![0xABu8; 10_000];
        let ciphertext = enc.encrypt(&plaintext);
        let decrypted = enc.decrypt(&ciphertext).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_encryption_needs_rotation() {
        let config = EncryptionConfig {
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            key_id: "key".into(),
            key_rotation_interval_ms: 1, // 1ms — will always need rotation
        };
        let enc = EncryptionAtRest::new(config, "master");
        // Force last_rotation to the past so the check succeeds
        {
            let mut last = enc.last_rotation.lock().unwrap();
            *last = 0; // epoch 0
        }
        assert!(enc.needs_rotation());
    }

    #[test]
    fn test_encryption_no_rotation_when_zero_interval() {
        let config = EncryptionConfig {
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            key_id: "key".into(),
            key_rotation_interval_ms: 0,
        };
        let enc = EncryptionAtRest::new(config, "master");
        assert!(!enc.needs_rotation());
    }
}
