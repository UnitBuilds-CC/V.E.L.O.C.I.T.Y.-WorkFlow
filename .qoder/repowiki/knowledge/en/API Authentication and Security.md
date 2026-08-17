---
kind: security
name: API Authentication and Security
category: security
scope:
    - 'velocity-server-bootstrap/src/auth.rs'
    - 'velocity-server-bootstrap/src/rate_limit.rs'
    - 'velocity-server-bootstrap/src/audit.rs'
source_files:
    - velocity-server-bootstrap/src/auth.rs
    - velocity-server-bootstrap/src/rate_limit.rs
    - velocity-server-bootstrap/src/audit.rs
---

Velocity servers support optional API authentication, rate limiting, and audit logging. Authentication is disabled by default (development mode) and can be enabled via CLI flags or environment variables.

**Authentication Methods:**

1. **API Key Authentication:**
   - Passed via `X-API-Key` header or `Authorization: Bearer <key>`
   - Supports plain-text keys and pre-hashed SHA-256 keys
   - Multiple keys can be configured simultaneously

2. **JWT Authentication:**
   - HS256 (HMAC-SHA256) and RS256 (RSA) validation
   - Configurable issuer and audience claim validation
   - **Zero-downtime key rotation**: accepts tokens signed with either current or previous secret
   - `jwt_secret_previous` field enables seamless rotation

```rust
pub struct AuthConfig {
    pub api_key_hashes: HashSet<String>,   // Pre-hashed API keys
    pub api_keys: Vec<String>,              // Plain-text API keys
    pub jwt_secret: String,                 // Current JWT secret
    pub jwt_secret_previous: String,        // Previous secret (for rotation)
    pub jwt_issuer: String,                 // Expected issuer claim
    pub jwt_audience: String,               // Expected audience claim
}
```

**Rate Limiting:**
- Token bucket algorithm per client IP
- Configurable burst capacity (`max_tokens`) and sustained rate (`refill_rate`)
- Thread-safe via `DashMap` for lock-free concurrent access
- Automatic eviction of stale client entries
- Returns `RateLimitResult` with allow/deny + metadata

```rust
pub struct RateLimiter {
    max_tokens: f64,           // Burst capacity
    refill_rate: f64,          // Tokens per second
    buckets: DashMap<String, TokenBucket>,  // Per-client state
    rejected_total: AtomicU64,
    allowed_total: AtomicU64,
    active_clients: AtomicU64,
}
```

**Audit Logging:**
- Structured audit log entries for all API calls
- Logs: timestamp, client IP, method, path, status code, auth result
- Integrates with the tracing system for correlated logs

**Additional Security:**
- **mTLS**: TLS certificate + key loading via rustls for both WebSocket and HTTP endpoints
- **Security headers**: Added to HTTP health endpoint responses
- **Trivy scanning**: Container security scanning in CI pipeline
- **Health probes exempt**: `/health`, `/ready`, `/live` never require authentication

**Configuration:**
```bash
# API Key auth
VELOCITY_API_KEYS=key1,key2,key3

# JWT auth
VELOCITY_JWT_SECRET=my-secret-key
VELOCITY_JWT_ISSUER=velocity
VELOCITY_JWT_AUDIENCE=workflow-clients

# Rate limiting
VELOCITY_RATE_LIMIT_BURST=100
VELOCITY_RATE_LIMIT_RATE=10.0

# TLS/mTLS
VELOCITY_TLS_CERT=/path/to/cert.pem
VELOCITY_TLS_KEY=/path/to/key.pem
```

**Key files:**
- `velocity-server-bootstrap/src/auth.rs` — Authentication (671 lines)
- `velocity-server-bootstrap/src/rate_limit.rs` — Rate limiting (290 lines)
- `velocity-server-bootstrap/src/audit.rs` — Audit logging

**Rules for developers:**
1. Auth is opt-in — servers work without auth in development mode
2. Never authenticate health/readiness probes (breaks K8s)
3. Always support key rotation via `jwt_secret_previous`
4. Use SHA-256 hashed keys in production (never store plain-text)
5. Rate limiter must be thread-safe (DashMap, not Mutex<HashMap>)
6. Audit logs must include enough context for security forensics
