//! Chaos and failure injection tests for production hardening features.
//!
//! Tests verify that auth, rate limiting, and audit logging behave correctly
//! under adversarial conditions: malformed input, concurrent load, edge cases.

use std::sync::Arc;
use std::thread;

use velocity_server_bootstrap::auth::{self, AuthConfig, AuthResult, HttpRequestHeaders};
use velocity_server_bootstrap::rate_limit::RateLimiter;
use velocity_server_bootstrap::audit::{AuditEntry, AuditEvent, AuditLogger};

// ─── Auth Chaos Tests ────────────────────────────────────────────────────────

#[test]
fn test_auth_empty_api_key_rejected() {
    let config = AuthConfig::from_api_keys(vec!["valid-key".to_string()]);
    let mut headers = HttpRequestHeaders::default();
    headers.api_key = Some("".to_string());
    let result = auth::authenticate_request(&config, &headers);
    assert!(matches!(result, AuthResult::Denied { .. }));
}

#[test]
fn test_auth_whitespace_api_key_rejected() {
    let config = AuthConfig::from_api_keys(vec!["valid-key".to_string()]);
    let mut headers = HttpRequestHeaders::default();
    headers.api_key = Some("   ".to_string());
    let result = auth::authenticate_request(&config, &headers);
    assert!(matches!(result, AuthResult::Denied { .. }));
}

#[test]
fn test_auth_very_long_api_key() {
    let config = AuthConfig::from_api_keys(vec!["valid-key".to_string()]);
    let mut headers = HttpRequestHeaders::default();
    // 10KB key should be rejected quickly, not cause OOM
    headers.api_key = Some("A".repeat(10240));
    let result = auth::authenticate_request(&config, &headers);
    assert!(matches!(result, AuthResult::Denied { .. }));
}

#[test]
fn test_auth_unicode_api_key() {
    let config = AuthConfig::from_api_keys(vec!["valid-key".to_string()]);
    let mut headers = HttpRequestHeaders::default();
    headers.api_key = Some("🔑🔒🛡️".to_string());
    let result = auth::authenticate_request(&config, &headers);
    assert!(matches!(result, AuthResult::Denied { .. }));
}

#[test]
fn test_auth_malformed_jwt() {
    let config = AuthConfig {
        jwt_secret: "secret".to_string(),
        ..Default::default()
    };
    let mut headers = HttpRequestHeaders::default();

    // Not 3 parts
    headers.authorization = Some("Bearer not.a.jwt".to_string());
    let result = auth::authenticate_request(&config, &headers);
    // Should be denied (either as API key or JWT)
    assert!(matches!(result, AuthResult::Denied { .. }));
}

#[test]
fn test_auth_empty_bearer_token() {
    let config = AuthConfig::from_api_keys(vec!["valid-key".to_string()]);
    let mut headers = HttpRequestHeaders::default();
    headers.authorization = Some("Bearer ".to_string());
    let result = auth::authenticate_request(&config, &headers);
    assert!(matches!(result, AuthResult::Denied { .. }));
}

#[test]
fn test_auth_header_injection_attempt() {
    let config = AuthConfig::from_api_keys(vec!["valid-key".to_string()]);
    let raw = "GET /metrics HTTP/1.1\r\nX-API-Key: valid-key\r\nEvil: header\r\n\r\n";
    let headers = HttpRequestHeaders::from_raw_request(raw);
    let result = auth::authenticate_request(&config, &headers);
    // Should still authenticate correctly
    assert!(matches!(result, AuthResult::Allowed { .. }));
}

#[test]
fn test_auth_concurrent_validation() {
    let config = Arc::new(AuthConfig::from_api_keys(vec!["shared-key".to_string()]));
    let mut handles = vec![];

    for i in 0..100 {
        let config = config.clone();
        handles.push(thread::spawn(move || {
            let mut headers = HttpRequestHeaders::default();
            if i % 2 == 0 {
                headers.api_key = Some("shared-key".to_string());
            } else {
                headers.api_key = Some(format!("bad-key-{}", i));
            }
            auth::authenticate_request(&config, &headers)
        }));
    }

    let mut allowed = 0;
    let mut denied = 0;
    for handle in handles {
        match handle.join().unwrap() {
            AuthResult::Allowed { .. } => allowed += 1,
            AuthResult::Denied { .. } => denied += 1,
            AuthResult::NotRequired => panic!("unexpected NotRequired"),
        }
    }
    assert_eq!(allowed, 50);
    assert_eq!(denied, 50);
}

// ─── Rate Limiter Chaos Tests ────────────────────────────────────────────────

#[test]
fn test_rate_limiter_concurrent_clients() {
    let limiter = Arc::new(RateLimiter::new(10, 100.0));
    let mut handles = vec![];

    // 50 concurrent clients, each making 20 requests
    for client_id in 0..50 {
        let limiter = limiter.clone();
        handles.push(thread::spawn(move || {
            let id = format!("client-{}", client_id);
            let mut allowed = 0;
            for _ in 0..20 {
                if limiter.check(&id) {
                    allowed += 1;
                }
            }
            allowed
        }));
    }

    let total_allowed: u64 = handles.into_iter().map(|h| h.join().unwrap() as u64).sum();
    // Each client has burst of 10, so max 500 allowed out of 1000 total
    assert!(total_allowed <= 500);
    assert!(total_allowed >= 400); // At least 80% should be allowed (burst)

    let stats = limiter.stats();
    assert_eq!(stats.active_clients, 50);
    assert_eq!(stats.allowed + stats.rejected, 1000);
}

#[test]
fn test_rate_limiter_zero_burst() {
    // Edge case: burst of 0 means all requests should be rejected
    let limiter = RateLimiter::new(0, 1.0);
    assert!(!limiter.check("client1"));
}

#[test]
fn test_rate_limiter_high_refill() {
    // Very high refill rate should allow sustained traffic after initial burst
    let limiter = RateLimiter::new(10, 10000.0);
    // First 10 should pass (burst)
    for _ in 0..10 {
        assert!(limiter.check("client1"));
    }
    // After a small delay, tokens should refill
    std::thread::sleep(std::time::Duration::from_millis(5));
    // Should have refilled 50 tokens (10000 * 0.005s), capped at 10
    for _ in 0..10 {
        assert!(limiter.check("client1"));
    }
}

#[test]
fn test_rate_limiter_empty_client_id() {
    let limiter = RateLimiter::new(5, 1.0);
    // Empty string is a valid client ID
    assert!(limiter.check(""));
    assert!(limiter.check(""));
}

// ─── Audit Logger Chaos Tests ────────────────────────────────────────────────

#[test]
fn test_audit_concurrent_logging() {
    let logger = Arc::new(AuditLogger::new(true));
    let mut handles = vec![];

    for i in 0..100 {
        let logger = logger.clone();
        handles.push(thread::spawn(move || {
            if i % 3 == 0 {
                logger.auth_failure("bad key", Some("10.0.0.1"));
            } else if i % 3 == 1 {
                logger.auth_success("user-1", Some("10.0.0.2"));
            } else {
                logger.rate_limited("client-x", Some("10.0.0.3"));
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let stats = logger.stats();
    assert_eq!(stats.events_total, 100);
    assert_eq!(stats.auth_failures, 34); // 0,3,6,...,99 = 34 items
    assert_eq!(stats.rate_limit_rejections, 33); // 2,5,8,...,98 = 33 items
}

#[test]
fn test_audit_entry_special_characters() {
    let entry = AuditEntry::new(AuditEvent::AuthFailure)
        .with_identity("user<script>alert('xss')</script>")
        .with_detail("key with \"quotes\" and \\backslashes\\ and \nnewlines")
        .with_success(false);

    let json = entry.to_json();
    // Should be valid JSON despite special characters
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed["event"], "auth.failure");
    assert!(parsed["identity"].as_str().unwrap().contains("<script>"));
}

// ─── Combined Integration Tests ──────────────────────────────────────────────

#[test]
fn test_auth_plus_rate_limit_integration() {
    let config = AuthConfig::from_api_keys(vec!["my-api-key".to_string()]);
    let limiter = RateLimiter::new(5, 1.0);

    // First 5 requests with valid key should pass both auth and rate limit
    for _ in 0..5 {
        let mut headers = HttpRequestHeaders::default();
        headers.api_key = Some("my-api-key".to_string());

        assert!(limiter.check("192.168.1.1"));
        assert!(matches!(
            auth::authenticate_request(&config, &headers),
            AuthResult::Allowed { .. }
        ));
    }

    // 6th request should pass auth but fail rate limit
    let mut headers = HttpRequestHeaders::default();
    headers.api_key = Some("my-api-key".to_string());
    assert!(!limiter.check("192.168.1.1"));
    assert!(matches!(
        auth::authenticate_request(&config, &headers),
        AuthResult::Allowed { .. }
    ));
}

#[test]
fn test_full_pipeline_auth_rate_audit() {
    let config = AuthConfig::from_api_keys(vec!["key1".to_string()]);
    let limiter = RateLimiter::new(100, 10.0);
    let logger = AuditLogger::new(true);

    // Simulate 20 requests: 15 valid, 5 invalid auth
    for i in 0..20 {
        let mut headers = HttpRequestHeaders::default();
        let client_ip = "10.0.0.1";

        if i < 15 {
            headers.api_key = Some("key1".to_string());
        } else {
            headers.api_key = Some("bad-key".to_string());
        }

        // Rate limit check
        if !limiter.check(client_ip) {
            logger.rate_limited(client_ip, Some(client_ip));
            continue;
        }

        // Auth check
        match auth::authenticate_request(&config, &headers) {
            AuthResult::Allowed { ref identity, .. } => {
                logger.auth_success(identity, Some(client_ip));
            }
            AuthResult::Denied { ref reason } => {
                logger.auth_failure(reason, Some(client_ip));
            }
            AuthResult::NotRequired => {}
        }
    }

    let stats = logger.stats();
    assert_eq!(stats.events_total, 20);
    assert_eq!(stats.auth_failures, 5);
}
