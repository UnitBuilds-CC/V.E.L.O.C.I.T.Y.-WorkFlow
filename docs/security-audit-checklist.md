# Velocity — Security Audit Checklist

Comprehensive security audit checklist covering all 39 production hardening items. Use this before go-live and during periodic security reviews.

---

## 1. Transport Security

| # | Check | Status | Evidence |
|---|-------|--------|----------|
| 1.1 | TLS 1.3 enabled for HTTPS gateway (axum-server + rustls) | ☐ | `velocity-classic-server/Cargo.toml` — axum-server 0.7, rustls 0.23 |
| 1.2 | TLS 1.3 enabled for WSS gateway (tokio-rustls) | ☐ | `velocity-classic-server/Cargo.toml` — tokio-rustls 0.26 |
| 1.3 | mTLS configured for server-to-server communication | ☐ | `velocity-server-bootstrap/` — rustls mTLS support |
| 1.4 | TLS certificates issued by trusted CA (cert-manager) | ☐ | `deploy/helm/velocity/templates/cert-manager-certificate.yaml` |
| 1.5 | Certificate auto-renewal configured (renewBefore: 360h) | ☐ | `deploy/helm/velocity/templates/cert-manager-certificate.yaml` |
| 1.6 | TLS secrets stored in Kubernetes Secrets (not ConfigMaps) | ☐ | `deploy/helm/velocity/templates/tls-secret.yaml` |
| 1.7 | Private keys not committed to version control | ☐ | `.gitignore` — verify `*.pem`, `*.key` excluded |

## 2. VCTP Authenticated Encryption

| # | Check | Status | Evidence |
|---|-------|--------|----------|
| 2.1 | HMAC-SHA256 packet authentication enabled | ☐ | `velocity-workflow-core/src/vctp.rs` — `compute_mac()`, `verify_mac()` |
| 2.2 | Constant-time MAC comparison (prevents timing attacks) | ☐ | `velocity-workflow-core/src/vctp.rs` — byte-by-byte comparison |
| 2.3 | Replay protection window configured (64-depth) | ☐ | `velocity-workflow-core/src/vctp.rs` — `VctpReplayWindow` |
| 2.4 | XOR cipher enabled for defense-in-depth | ☐ | `velocity-workflow-core/src/vctp.rs` — `VctpCipher` (AES-256 key schedule) |
| 2.5 | Encryption keys rotated regularly | ☐ | Verify key rotation procedure in ops runbooks |
| 2.6 | Replay detection metrics exported | ☐ | `vctp_replay_detected_total` Prometheus metric |

## 3. Access Control

| # | Check | Status | Evidence |
|---|-------|--------|----------|
| 3.1 | API key authentication enabled in production | ☐ | `VELOCITY_API_KEYS` env var or Helm `vctp.security.authRequired: true` |
| 3.2 | JWT authentication configured (HS256/RS256) | ☐ | `VELOCITY_JWT_SECRET` or RS256 public key |
| 3.3 | JWT key rotation procedure documented | ☐ | Zero-downtime key rotation in `velocity-server-bootstrap/src/auth.rs` |
| 3.4 | API keys stored as Kubernetes Secrets (not env vars in manifests) | ☐ | `deploy/helm/velocity/templates/secret.yaml` |
| 3.5 | Gateway rate limiting enabled (HTTP per-second, WS per-connection) | ☐ | `http_vctp_ingress.rs` — `check_rate_limit()`, `ws_vctp_gateway.rs` — rate_limit_per_connection |
| 3.6 | Server-side rate limiting configured (token bucket per client IP) | ☐ | `velocity-server-bootstrap/src/rate_limit.rs` — DashMap-based |

## 4. Network Security

| # | Check | Status | Evidence |
|---|-------|--------|----------|
| 4.1 | Network policies restrict pod-to-pod traffic | ☐ | `deploy/helm/velocity/templates/networkpolicy.yaml` + `networkpolicy-deny-all.yaml` |
| 4.2 | VCTP UDP port (9090) only exposed to authorized clients | ☐ | Service configuration + NetworkPolicy |
| 4.3 | HTTPS/WSS ports (8443/8444) behind ingress with TLS | ☐ | `deploy/helm/velocity/templates/ingress.yaml` |
| 4.4 | Security headers on all HTTP responses | ☐ | `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `Cache-Control: no-store` |
| 4.5 | Pod security contexts configured (non-root, read-only FS) | ☐ | `deploy/helm/velocity/templates/securitycontext.yaml` |
| 4.6 | Service account uses minimal RBAC permissions | ☐ | `deploy/helm/velocity/templates/rbac-role.yaml` |

## 5. Data Persistence Security

| # | Check | Status | Evidence |
|---|-------|--------|----------|
| 5.1 | WAL files stored on encrypted volumes | ☐ | Verify PVC storage class has encryption-at-rest |
| 5.2 | PostgreSQL connection uses TLS | ☐ | `sslmode=verify-full` in connection string |
| 5.3 | PostgreSQL credentials stored in Kubernetes Secrets | ☐ | `deploy/helm/velocity/templates/secret.yaml` |
| 5.4 | Per-step journal entries are tamper-evident (Merkle root) | ☐ | `velocity-workflow-core/src/slab.rs` — SHA-256 Merkle root |
| 5.5 | Backup encryption enabled | ☐ | Velero restic/kopia encryption or pg_dump piped through gpg |
| 5.6 | Backup retention policy configured | ☐ | `deploy/helm/velocity/templates/backup-cronjob.yaml` — retention days |

## 6. Observability Security

| # | Check | Status | Evidence |
|---|-------|--------|----------|
| 6.1 | Prometheus metrics endpoint requires authentication | ☐ | `VELOCITY_METRICS_TOKEN` — Bearer token required |
| 6.2 | Audit logging enabled for all API calls | ☐ | `velocity-server-bootstrap/src/audit.rs` |
| 6.3 | Audit logs shipped to centralized log store (Loki) | ☐ | `deploy/helm/velocity/templates/loki-deployment.yaml` |
| 6.4 | Distributed tracing enabled (OpenTelemetry/OTLP) | ☐ | `velocity-server-bootstrap/src/tracing_setup.rs` |
| 6.5 | Traces exported to secure backend (Jaeger/Tempo) | ☐ | `deploy/helm/velocity/templates/tempo-deployment.yaml` |
| 6.6 | Sensitive data excluded from logs/traces | ☐ | Verify no passwords, tokens, or PII in log output |

## 7. Production Monitoring

| # | Check | Status | Evidence |
|---|-------|--------|----------|
| 7.1 | Prometheus alert rules deployed (17 rules: 11 HTTP + 6 VCTP) | ☐ | `deploy/helm/velocity/templates/prometheus-rules.yaml` |
| 7.2 | VCTP circuit breaker alert configured | ☐ | `VctpCircuitBreakerOpen` alert rule |
| 7.3 | VCTP high error rate alert configured | ☐ | `VctpHighErrorRate` — >5% for 2m |
| 7.4 | VCTP high latency alert configured | ☐ | `VctpHighLatency` — p99 >100ms for 5m |
| 7.5 | VCTP replay detection alert configured | ☐ | `VctpReplayDetected` — attempts >0 |
| 7.6 | Grafana dashboards deployed | ☐ | `deploy/helm/velocity/templates/grafana-dashboard.yaml` |
| 7.7 | Alert notification channels configured (PagerDuty/Slack) | ☐ | Prometheus Alertmanager configuration |

## 8. Container Security

| # | Check | Status | Evidence |
|---|-------|--------|----------|
| 8.1 | Trivy container scanning enabled in CI | ☐ | `.github/workflows/ci.yml` — Trivy step |
| 8.2 | Base images use distroless or minimal images | ☐ | Dockerfiles — verify `alpine` or `distroless` base |
| 8.3 | No unnecessary packages installed in containers | ☐ | Review Dockerfile RUN layers |
| 8.4 | Container runs as non-root user | ☐ | `securitycontext.yaml` — `runAsNonRoot: true` |
| 8.5 | Read-only root filesystem enabled | ☐ | `securityContext.readOnlyRootFilesystem: true` |
| 8.6 | Resource limits set (CPU + memory) | ☐ | `deploy/helm/velocity/values.yaml` — resources.limits |

## 9. CI/CD Security

| # | Check | Status | Evidence |
|---|-------|--------|----------|
| 9.1 | Benchmark regression gate in CI (≥500 ops/s) | ☐ | `.github/workflows/benchmark.yml` |
| 9.2 | Tail latency gate in CI (p99 <100ms) | ☐ | `.github/workflows/benchmark.yml` — sustained workload step |
| 9.3 | Error rate gate in CI (<5%) | ☐ | `.github/workflows/benchmark.yml` |
| 9.4 | Chaos/failure injection tests in CI | ☐ | `.github/workflows/ci.yml` |
| 9.5 | No secrets in CI logs | ☐ | Review CI workflow — secrets use `${{ secrets.* }}` |
| 9.6 | Branch protection rules enabled | ☐ | GitHub repository settings |

## 10. Operational Readiness

| # | Check | Status | Evidence |
|---|-------|--------|----------|
| 10.1 | Graceful drain configured (preStop hook + drain timeout) | ☐ | `deploy/helm/velocity/templates/deployment.yaml` — lifecycle.preStop |
| 10.2 | Health probes configured (liveness + readiness) | ☐ | `deploy/helm/velocity/templates/deployment.yaml` |
| 10.3 | VCTP-specific health probe enabled | ☐ | Helm `vctp.healthProbe.enabled: true` |
| 10.4 | Pod disruption budget configured | ☐ | `deploy/helm/velocity/templates/pdb.yaml` |
| 10.5 | Horizontal pod autoscaler configured | ☐ | `deploy/helm/velocity/templates/hpa.yaml` |
| 10.6 | Operations runbooks documented | ☐ | `docs/ops-runbooks.md` + `deploy/RUNBOOK.md` |
| 10.7 | On-call rotation established | ☐ | `deploy/RUNBOOK.md` — Emergency Contacts section |
| 10.8 | Backup and restore procedure tested | ☐ | `deploy/helm/velocity/templates/backup-cronjob.yaml` + Velero |
| 10.9 | Rollback procedure documented and tested | ☐ | `deploy/RUNBOOK.md` — Rollback Procedures section |

---

## Audit Summary Template

| Category | Total Checks | Passed | Failed | N/A |
|----------|-------------|--------|--------|-----|
| Transport Security | 7 | | | |
| VCTP Authenticated Encryption | 6 | | | |
| Access Control | 6 | | | |
| Network Security | 6 | | | |
| Data Persistence Security | 6 | | | |
| Observability Security | 6 | | | |
| Production Monitoring | 7 | | | |
| Container Security | 6 | | | |
| CI/CD Security | 6 | | | |
| Operational Readiness | 9 | | | |
| **TOTAL** | **65** | | | |

**Auditor:** ________________
**Date:** ________________
**Result:** ☐ PASS (all critical checks pass) / ☐ FAIL (critical items remain)

---

## Critical vs. Advisory

**Critical (must pass before go-live):**
- All Transport Security checks (Section 1)
- All Access Control checks (Section 3)
- Container runs as non-root (8.4)
- No secrets in version control (1.7, 3.4)
- Audit logging enabled (6.2)
- CI/CD gates active (9.1–9.4)

**Advisory (should pass, exceptions documented):**
- All other checks
- Exceptions require written justification and risk acceptance
