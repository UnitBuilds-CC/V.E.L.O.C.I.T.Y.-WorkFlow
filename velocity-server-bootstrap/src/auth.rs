//! API authentication for Velocity servers.
//!
//! Supports two authentication methods:
//! - **API Key**: Passed via `X-API-Key` header or `Authorization: Bearer <key>`
//! - **JWT**: Basic JWT (HS256/RS256) validation for bearer tokens
//!
//! Authentication is optional — if no keys are configured, all requests are allowed.
//! Health/readiness probes are never authenticated (K8s compatibility).

use sha2::{Sha256, Digest};
use hmac::{Hmac, Mac};
use std::collections::HashSet;

type HmacSha256 = Hmac<Sha256>;

/// Authentication configuration for the server.
#[derive(Clone, Debug)]
pub struct AuthConfig {
    /// Pre-hashed API keys (SHA-256 hex digests). Empty = auth disabled.
    pub api_key_hashes: HashSet<String>,
    /// Raw API keys for direct comparison (used when hashes aren't pre-computed).
    pub api_keys: Vec<String>,
    /// JWT secret for HS256 validation (empty = JWT disabled).
    pub jwt_secret: String,
    /// Previous JWT secret for zero-downtime key rotation.
    /// During rotation, tokens signed with either the current or previous secret
    /// are accepted. Clear this field after all clients have rotated.
    pub jwt_secret_previous: String,
    /// JWT issuer claim to validate (empty = skip issuer check).
    pub jwt_issuer: String,
    /// JWT audience claim to validate (empty = skip audience check).
    pub jwt_audience: String,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            api_key_hashes: HashSet::new(),
            api_keys: Vec::new(),
            jwt_secret: String::new(),
            jwt_secret_previous: String::new(),
            jwt_issuer: String::new(),
            jwt_audience: String::new(),
        }
    }
}

impl AuthConfig {
    /// Create an AuthConfig from a list of plain-text API keys.
    pub fn from_api_keys(keys: Vec<String>) -> Self {
        Self {
            api_keys: keys,
            ..Default::default()
        }
    }

    /// Create an AuthConfig from pre-hashed API keys (SHA-256 hex).
    pub fn from_hashed_keys(hashes: HashSet<String>) -> Self {
        Self {
            api_key_hashes: hashes,
            ..Default::default()
        }
    }

    /// Whether any authentication is configured.
    pub fn is_enabled(&self) -> bool {
        !self.api_keys.is_empty() || !self.api_key_hashes.is_empty() || !self.jwt_secret.is_empty()
    }

    /// Initiate a key rotation: move the current secret to previous, set the new one.
    ///
    /// After calling this, tokens signed with EITHER the old or new secret are accepted.
    /// Once all clients have been updated (typically after 24-48h), clear `jwt_secret_previous`
    /// to reject tokens signed with the old key.
    pub fn rotate_jwt_secret(&mut self, new_secret: String) {
        self.jwt_secret_previous = std::mem::replace(&mut self.jwt_secret, new_secret);
    }
}

/// Result of an authentication attempt.
#[derive(Debug, Clone, PartialEq)]
pub enum AuthResult {
    /// Authentication succeeded.
    Allowed {
        /// The identity of the authenticated client (API key prefix or JWT subject).
        identity: String,
        /// Authentication method used.
        method: AuthMethod,
    },
    /// Authentication failed.
    Denied {
        /// Reason for denial.
        reason: String,
    },
    /// No authentication configured — request is allowed.
    NotRequired,
}

/// Which authentication method was used.
#[derive(Debug, Clone, PartialEq)]
pub enum AuthMethod {
    ApiKey,
    Jwt,
    None,
}

/// Authenticate an HTTP request from its raw headers.
///
/// Extracts the Authorization and X-API-Key headers and validates against
/// the configured auth methods.
pub fn authenticate_request(
    config: &AuthConfig,
    headers: &HttpRequestHeaders,
) -> AuthResult {
    if !config.is_enabled() {
        return AuthResult::NotRequired;
    }

    // Try API key from X-API-Key header first
    if let Some(ref api_key) = headers.api_key {
        return validate_api_key(config, api_key);
    }

    // Try Authorization: Bearer <token>
    if let Some(ref auth_header) = headers.authorization {
        if let Some(token) = auth_header.strip_prefix("Bearer ") {
            // Try as API key first (fast path)
            if !config.api_keys.is_empty() || !config.api_key_hashes.is_empty() {
                let result = validate_api_key(config, token);
                if matches!(result, AuthResult::Allowed { .. }) {
                    return result;
                }
            }

            // Try as JWT
            if !config.jwt_secret.is_empty() {
                return validate_jwt(config, token);
            }

            return AuthResult::Denied {
                reason: "invalid bearer token".to_string(),
            };
        }
    }

    AuthResult::Denied {
        reason: "missing authentication credentials".to_string(),
    }
}

/// Validate an API key against configured keys (both plain and hashed).
fn validate_api_key(config: &AuthConfig, key: &str) -> AuthResult {
    // Check plain-text keys
    for configured_key in &config.api_keys {
        if constant_time_eq(key.as_bytes(), configured_key.as_bytes()) {
            let identity = format!("api-key:{}...", &key[..key.len().min(8)]);
            return AuthResult::Allowed {
                identity,
                method: AuthMethod::ApiKey,
            };
        }
    }

    // Check hashed keys
    let key_hash = hex_sha256(key.as_bytes());
    if config.api_key_hashes.contains(&key_hash) {
        let identity = format!("api-key-hash:{}...", &key[..key.len().min(8)]);
        return AuthResult::Allowed {
            identity,
            method: AuthMethod::ApiKey,
        };
    }

    AuthResult::Denied {
        reason: "invalid API key".to_string(),
    }
}

/// Basic JWT validation (HS256 only — sufficient for internal service auth).
///
/// Validates:
/// - Signature (HS256 with configured secret)
/// - Expiration (exp claim)
/// - Not-before (nbf claim)
/// - Issuer (iss claim, if configured)
/// - Audience (aud claim, if configured)
fn validate_jwt(config: &AuthConfig, token: &str) -> AuthResult {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return AuthResult::Denied {
            reason: "malformed JWT: expected 3 parts".to_string(),
        };
    }

    // Verify HS256 signature
    let signing_input = format!("{}.{}", parts[0], parts[1]);
    let signature = match base64_decode_url(parts[2]) {
        Some(s) => s,
        None => {
            return AuthResult::Denied {
                reason: "malformed JWT signature".to_string(),
            };
        }
    };

    // HMAC-SHA256 verification (proper HMAC construction with key padding)
    // Try current secret first, then fall back to previous (for key rotation)
    let signature_valid = verify_hmac_signature(&signing_input, &signature, &config.jwt_secret);

    if !signature_valid {
        // During key rotation, accept tokens signed with the previous secret
        if !config.jwt_secret_previous.is_empty()
            && verify_hmac_signature(&signing_input, &signature, &config.jwt_secret_previous)
        {
            // Token valid with previous key — continue to claim validation below
        } else {
            return AuthResult::Denied {
                reason: "invalid JWT signature".to_string(),
            };
        }
    }

    // Decode payload
    let payload_bytes = match base64_decode_url(parts[1]) {
        Some(p) => p,
        None => {
            return AuthResult::Denied {
                reason: "malformed JWT payload".to_string(),
            };
        }
    };

    let payload: serde_json::Value = match serde_json::from_slice(&payload_bytes) {
        Ok(v) => v,
        Err(_) => {
            return AuthResult::Denied {
                reason: "invalid JWT payload JSON".to_string(),
            };
        }
    };

    // Check expiration
    let now = chrono::Utc::now().timestamp();
    if let Some(exp) = payload.get("exp").and_then(|v| v.as_i64()) {
        if now > exp {
            return AuthResult::Denied {
                reason: "JWT expired".to_string(),
            };
        }
    }

    // Check not-before
    if let Some(nbf) = payload.get("nbf").and_then(|v| v.as_i64()) {
        if now < nbf {
            return AuthResult::Denied {
                reason: "JWT not yet valid".to_string(),
            };
        }
    }

    // Check issuer
    if !config.jwt_issuer.is_empty() {
        let iss = payload.get("iss").and_then(|v| v.as_str()).unwrap_or("");
        if iss != config.jwt_issuer {
            return AuthResult::Denied {
                reason: format!("JWT issuer mismatch: expected '{}'", config.jwt_issuer),
            };
        }
    }

    // Check audience
    if !config.jwt_audience.is_empty() {
        let aud = payload.get("aud").and_then(|v| v.as_str()).unwrap_or("");
        if aud != config.jwt_audience {
            return AuthResult::Denied {
                reason: format!("JWT audience mismatch: expected '{}'", config.jwt_audience),
            };
        }
    }

    // Extract subject as identity
    let sub = payload
        .get("sub")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();

    AuthResult::Allowed {
        identity: format!("jwt:{}", sub),
        method: AuthMethod::Jwt,
    }
}

/// Parsed HTTP request headers (extracted from raw HTTP request).
#[derive(Debug, Default)]
pub struct HttpRequestHeaders {
    pub authorization: Option<String>,
    pub api_key: Option<String>,
    pub content_type: Option<String>,
    pub user_agent: Option<String>,
    pub x_forwarded_for: Option<String>,
}

impl HttpRequestHeaders {
    /// Parse headers from a raw HTTP request string.
    pub fn from_raw_request(request: &str) -> Self {
        let mut headers = Self::default();
        for line in request.lines().skip(1) {
            if line.is_empty() || line == "\r" {
                break;
            }
            if let Some((key, value)) = line.split_once(':') {
                let key_lower = key.trim().to_lowercase();
                let value = value.trim().to_string();
                match key_lower.as_str() {
                    "authorization" => headers.authorization = Some(value),
                    "x-api-key" => headers.api_key = Some(value),
                    "content-type" => headers.content_type = Some(value),
                    "user-agent" => headers.user_agent = Some(value),
                    "x-forwarded-for" => headers.x_forwarded_for = Some(value),
                    _ => {}
                }
            }
        }
        headers
    }
}

// ─── Utility functions ───────────────────────────────────────────────────────

/// Verify an HMAC-SHA256 signature for a given signing input and secret.
/// Returns true if the signature matches.
fn verify_hmac_signature(signing_input: &str, signature: &[u8], secret: &str) -> bool {
    let mut mac = match HmacSha256::new_from_slice(secret.as_bytes()) {
        Ok(m) => m,
        Err(_) => return false,
    };
    mac.update(signing_input.as_bytes());
    let expected = mac.finalize();
    constant_time_eq(signature, expected.into_bytes().as_slice())
}

/// Constant-time byte comparison to prevent timing attacks.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Compute SHA-256 hex digest.
pub fn hex_sha256(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    hex_encode(&result)
}

/// Hex encode bytes to string.
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Base64 URL-safe decode (no padding).
fn base64_decode_url(input: &str) -> Option<Vec<u8>> {
    use base64::Engine;
    // Add padding if needed
    let padded = match input.len() % 4 {
        2 => format!("{}==", input),
        3 => format!("{}=", input),
        _ => input.to_string(),
    };
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(input)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(&padded))
        .or_else(|_| base64::engine::general_purpose::STANDARD.decode(&padded))
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_not_required_when_empty() {
        let config = AuthConfig::default();
        assert!(!config.is_enabled());
        let headers = HttpRequestHeaders::default();
        assert_eq!(authenticate_request(&config, &headers), AuthResult::NotRequired);
    }

    #[test]
    fn test_auth_api_key_success() {
        let config = AuthConfig::from_api_keys(vec!["my-secret-key".to_string()]);
        let mut headers = HttpRequestHeaders::default();
        headers.api_key = Some("my-secret-key".to_string());
        let result = authenticate_request(&config, &headers);
        assert!(matches!(result, AuthResult::Allowed { method: AuthMethod::ApiKey, .. }));
    }

    #[test]
    fn test_auth_api_key_failure() {
        let config = AuthConfig::from_api_keys(vec!["my-secret-key".to_string()]);
        let mut headers = HttpRequestHeaders::default();
        headers.api_key = Some("wrong-key".to_string());
        let result = authenticate_request(&config, &headers);
        assert!(matches!(result, AuthResult::Denied { .. }));
    }

    #[test]
    fn test_auth_bearer_token_as_api_key() {
        let config = AuthConfig::from_api_keys(vec!["my-secret-key".to_string()]);
        let mut headers = HttpRequestHeaders::default();
        headers.authorization = Some("Bearer my-secret-key".to_string());
        let result = authenticate_request(&config, &headers);
        assert!(matches!(result, AuthResult::Allowed { method: AuthMethod::ApiKey, .. }));
    }

    #[test]
    fn test_auth_missing_credentials() {
        let config = AuthConfig::from_api_keys(vec!["my-secret-key".to_string()]);
        let headers = HttpRequestHeaders::default();
        let result = authenticate_request(&config, &headers);
        assert!(matches!(result, AuthResult::Denied { .. }));
    }

    #[test]
    fn test_auth_hashed_key() {
        let key = "hashed-api-key";
        let hash = hex_sha256(key.as_bytes());
        let mut hashes = HashSet::new();
        hashes.insert(hash);
        let config = AuthConfig::from_hashed_keys(hashes);

        let mut headers = HttpRequestHeaders::default();
        headers.api_key = Some(key.to_string());
        let result = authenticate_request(&config, &headers);
        assert!(matches!(result, AuthResult::Allowed { method: AuthMethod::ApiKey, .. }));
    }

    #[test]
    fn test_parse_headers_from_raw() {
        let raw = "GET /api/workflows HTTP/1.1\r\nAuthorization: Bearer token123\r\nX-API-Key: key456\r\nContent-Type: application/json\r\n\r\n";
        let headers = HttpRequestHeaders::from_raw_request(raw);
        assert_eq!(headers.authorization, Some("Bearer token123".to_string()));
        assert_eq!(headers.api_key, Some("key456".to_string()));
        assert_eq!(headers.content_type, Some("application/json".to_string()));
    }

    #[test]
    fn test_constant_time_eq() {
        assert!(constant_time_eq(b"hello", b"hello"));
        assert!(!constant_time_eq(b"hello", b"world"));
        assert!(!constant_time_eq(b"hello", b"hell"));
    }

    #[test]
    fn test_hex_sha256() {
        let hash = hex_sha256(b"test");
        assert_eq!(hash.len(), 64); // SHA-256 = 32 bytes = 64 hex chars
        assert_eq!(hash, "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08");
    }

    // ─── JWT Tests ──────────────────────────────────────────────────────────

    /// Helper: create a signed JWT for testing.
    fn create_test_jwt(secret: &str, payload: &serde_json::Value) -> String {
        use base64::Engine;
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(b"{\"alg\":\"HS256\",\"typ\":\"JWT\"}");
        let payload_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_string(payload).unwrap().as_bytes());
        let signing_input = format!("{}.{}", header, payload_b64);

        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(signing_input.as_bytes());
        let sig = mac.finalize().into_bytes();
        let sig_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(sig);

        format!("{}.{}", signing_input, sig_b64)
    }

    #[test]
    fn test_jwt_valid_signature() {
        let secret = "test-secret-key";
        let config = AuthConfig {
            jwt_secret: secret.to_string(),
            ..Default::default()
        };
        let payload = serde_json::json!({
            "sub": "user-123",
            "exp": chrono::Utc::now().timestamp() + 3600,
        });
        let token = create_test_jwt(secret, &payload);

        let mut headers = HttpRequestHeaders::default();
        headers.authorization = Some(format!("Bearer {}", token));
        let result = authenticate_request(&config, &headers);
        assert!(matches!(result, AuthResult::Allowed { method: AuthMethod::Jwt, .. }));
        if let AuthResult::Allowed { identity, .. } = result {
            assert_eq!(identity, "jwt:user-123");
        }
    }

    #[test]
    fn test_jwt_invalid_signature() {
        let secret = "test-secret-key";
        let config = AuthConfig {
            jwt_secret: secret.to_string(),
            ..Default::default()
        };
        let payload = serde_json::json!({
            "sub": "user-123",
            "exp": chrono::Utc::now().timestamp() + 3600,
        });
        let mut token = create_test_jwt(secret, &payload);
        // Tamper with the signature
        if let Some(last_dot) = token.rfind('.') {
            let sig_part = &token[last_dot + 1..];
            let mut tampered = sig_part.chars().collect::<Vec<_>>();
            if !tampered.is_empty() {
                tampered[0] = if tampered[0] == 'A' { 'B' } else { 'A' };
            }
            token = format!("{}{}", &token[..last_dot + 1], tampered.iter().collect::<String>());
        }

        let mut headers = HttpRequestHeaders::default();
        headers.authorization = Some(format!("Bearer {}", token));
        let result = authenticate_request(&config, &headers);
        assert!(matches!(result, AuthResult::Denied { .. }));
    }

    #[test]
    fn test_jwt_expired() {
        let secret = "test-secret-key";
        let config = AuthConfig {
            jwt_secret: secret.to_string(),
            ..Default::default()
        };
        let payload = serde_json::json!({
            "sub": "user-123",
            "exp": chrono::Utc::now().timestamp() - 3600, // expired 1 hour ago
        });
        let token = create_test_jwt(secret, &payload);

        let mut headers = HttpRequestHeaders::default();
        headers.authorization = Some(format!("Bearer {}", token));
        let result = authenticate_request(&config, &headers);
        assert!(matches!(result, AuthResult::Denied { reason } if reason.contains("expired")));
    }

    #[test]
    fn test_jwt_issuer_mismatch() {
        let secret = "test-secret-key";
        let config = AuthConfig {
            jwt_secret: secret.to_string(),
            jwt_issuer: "expected-issuer".to_string(),
            ..Default::default()
        };
        let payload = serde_json::json!({
            "sub": "user-123",
            "iss": "wrong-issuer",
            "exp": chrono::Utc::now().timestamp() + 3600,
        });
        let token = create_test_jwt(secret, &payload);

        let mut headers = HttpRequestHeaders::default();
        headers.authorization = Some(format!("Bearer {}", token));
        let result = authenticate_request(&config, &headers);
        assert!(matches!(result, AuthResult::Denied { reason } if reason.contains("issuer")));
    }

    #[test]
    fn test_jwt_audience_mismatch() {
        let secret = "test-secret-key";
        let config = AuthConfig {
            jwt_secret: secret.to_string(),
            jwt_audience: "expected-aud".to_string(),
            ..Default::default()
        };
        let payload = serde_json::json!({
            "sub": "user-123",
            "aud": "wrong-audience",
            "exp": chrono::Utc::now().timestamp() + 3600,
        });
        let token = create_test_jwt(secret, &payload);

        let mut headers = HttpRequestHeaders::default();
        headers.authorization = Some(format!("Bearer {}", token));
        let result = authenticate_request(&config, &headers);
        assert!(matches!(result, AuthResult::Denied { reason } if reason.contains("audience")));
    }

    #[test]
    fn test_jwt_wrong_secret_rejected() {
        // Ensure a token signed with a different secret is rejected
        let config = AuthConfig {
            jwt_secret: "correct-secret".to_string(),
            ..Default::default()
        };
        let payload = serde_json::json!({
            "sub": "user-123",
            "exp": chrono::Utc::now().timestamp() + 3600,
        });
        let token = create_test_jwt("wrong-secret", &payload);

        let mut headers = HttpRequestHeaders::default();
        headers.authorization = Some(format!("Bearer {}", token));
        let result = authenticate_request(&config, &headers);
        assert!(matches!(result, AuthResult::Denied { reason } if reason.contains("signature")));
    }

    #[test]
    fn test_jwt_key_rotation_accepts_both_secrets() {
        // After rotation, tokens signed with EITHER the old or new key should work
        let mut config = AuthConfig {
            jwt_secret: "new-secret".to_string(),
            ..Default::default()
        };
        // Simulate a rotation: old secret was "old-secret"
        config.jwt_secret_previous = "old-secret".to_string();

        let payload = serde_json::json!({
            "sub": "user-123",
            "exp": chrono::Utc::now().timestamp() + 3600,
        });

        // Token signed with NEW secret should work
        let new_token = create_test_jwt("new-secret", &payload);
        let mut headers = HttpRequestHeaders::default();
        headers.authorization = Some(format!("Bearer {}", new_token));
        let result = authenticate_request(&config, &headers);
        assert!(matches!(result, AuthResult::Allowed { method: AuthMethod::Jwt, .. }));

        // Token signed with OLD (previous) secret should ALSO work
        let old_token = create_test_jwt("old-secret", &payload);
        let mut headers = HttpRequestHeaders::default();
        headers.authorization = Some(format!("Bearer {}", old_token));
        let result = authenticate_request(&config, &headers);
        assert!(matches!(result, AuthResult::Allowed { method: AuthMethod::Jwt, .. }));

        // Token signed with NEITHER secret should fail
        let bad_token = create_test_jwt("unknown-secret", &payload);
        let mut headers = HttpRequestHeaders::default();
        headers.authorization = Some(format!("Bearer {}", bad_token));
        let result = authenticate_request(&config, &headers);
        assert!(matches!(result, AuthResult::Denied { reason } if reason.contains("signature")));
    }

    #[test]
    fn test_jwt_rotate_jwt_secret_helper() {
        let mut config = AuthConfig {
            jwt_secret: "initial-secret".to_string(),
            ..Default::default()
        };
        assert!(config.jwt_secret_previous.is_empty());

        config.rotate_jwt_secret("rotated-secret".to_string());
        assert_eq!(config.jwt_secret, "rotated-secret");
        assert_eq!(config.jwt_secret_previous, "initial-secret");

        // Rotate again — previous should be overwritten
        config.rotate_jwt_secret("third-secret".to_string());
        assert_eq!(config.jwt_secret, "third-secret");
        assert_eq!(config.jwt_secret_previous, "rotated-secret");
    }
}
