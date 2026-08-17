# Velocity Server — Operations Runbooks

Production incident response procedures for all 3 server flavors (VCTP, Classic, Embedded).

---

## 1. Server Won't Start

**Symptoms:** Binary exits immediately or health endpoint never responds.

**Diagnosis:**
```bash
# Check if port is already in use
ss -tlnp | grep -E '8083|8084|7234|8093|8094|8095'

# Check WAL file permissions
ls -la velocity*.wal

# Try starting with verbose logging
VELOCITY_LOG_FORMAT=json RUST_LOG=debug ./velocity-server --wal-path /tmp/test.wal
```

**Resolution:**
1. Kill any stale process holding the port
2. Fix WAL file permissions (must be writable by the server user)
3. If WAL is corrupted, move it aside and start fresh:
   ```bash
   mv velocity.wal velocity.wal.corrupted
   ./velocity-server  # starts with fresh WAL
   ```

---

## 2. Health Endpoint Returns Non-200

**Symptoms:** `/health` or `/ready` returns 5xx or times out.

**Diagnosis:**
```bash
# Check health endpoint
curl -v http://localhost:8095/health
curl -v http://localhost:8095/ready

# Check server logs
journalctl -u velocity-server --since "5 min ago"

# Check if server is overloaded (too many running workflows)
curl -H "Authorization: Bearer $METRICS_TOKEN" http://localhost:8095/metrics | grep velocity_workflows_total
```

**Resolution:**
1. If `/health` fails: server process is dead — restart it
2. If `/ready` fails but `/health` works: server is shutting down or overloaded
   - Check `velocity_workflows_total{status="running"}` — if very high, scale up
   - Check if WAL replay is in progress (look for "Crash recovery" in logs)
3. If metrics show high `velocity_pg_write_queue_depth`: PG is slow — check PG health

---

## 3. Metrics Authentication Failure

**Symptoms:** `/metrics` returns 401 even with correct token.

**Diagnosis:**
```bash
# Test with correct token
curl -H "Authorization: Bearer velocity-prod-token" http://localhost:8095/metrics

# Check what token the server expects (from env)
echo $VELOCITY_METRICS_TOKEN

# Check audit logs for auth failures
grep "auth.failure" /var/log/velocity/*.log | tail -20
```

**Resolution:**
1. Verify the token matches between client and server configuration
2. If using API keys: check `VELOCITY_API_KEYS` env var
3. If using JWT: verify the secret matches and token hasn't expired
4. Check audit logs for the specific rejection reason

---

## 4. Rate Limiting Active

**Symptoms:** Clients receive 429 Too Many Requests.

**Diagnosis:**
```bash
# Check rate limiter stats via metrics
curl -H "Authorization: Bearer $TOKEN" http://localhost:8095/metrics | grep velocity_rate_limit

# Check audit logs for rate limit events
grep "auth.rate_limited" /var/log/velocity/*.log | tail -20
```

**Resolution:**
1. **Immediate:** Increase rate limit via env vars and restart:
   ```bash
   VELOCITY_RATE_LIMIT_BURST=500 VELOCITY_RATE_LIMIT_REFILL=50
   ```
2. **Long-term:** Identify the offending client IP from audit logs and either:
   - Whitelist them with a separate rate limit tier
   - Fix their client to implement proper backoff
3. Check if a deployment or migration is causing a traffic spike

---

## 5. WAL Recovery Failure

**Symptoms:** Server starts but logs show "WAL recovery failed" or data loss.

**Diagnosis:**
```bash
# Check WAL file integrity
ls -la velocity*.wal
file velocity*.wal

# Check server logs for WAL errors
grep -i "wal" /var/log/velocity/*.log | grep -i "error\|fail\|corrupt"
```

**Resolution:**
1. If WAL is corrupted:
   - The server will start fresh (data from last PG sync is preserved)
   - Check PG for the latest persisted state
2. If WAL is too large:
   - Reduce `--wal-max-size` to prevent unbounded growth
   - Ensure PG is connected for step journal persistence
3. Prevent recurrence:
   - Monitor `velocity_wal_unsynced_bytes` metric
   - Set up alerting for WAL size > 80% of max

---

## 6. PostgreSQL Connection Lost

**Symptoms:** `velocity_pg_connected` metric shows 0, step journals not persisting.

**Diagnosis:**
```bash
# Check PG connectivity
curl -H "Authorization: Bearer $TOKEN" http://localhost:8095/metrics | grep velocity_pg_connected

# Check server logs for PG errors
grep -i "postgres\|pg" /var/log/velocity/*.log | grep -i "error\|fail\|disconnect"

# Check PG directly
psql $DATABASE_URL -c "SELECT 1"
```

**Resolution:**
1. **Immediate:** Server continues operating with WAL-only persistence (no data loss)
2. Check PG health: connections, disk space, replication lag
3. Restart the server to re-establish the PG connection
4. After reconnection, check `velocity_pg_write_queue_depth` returns to 0

---

## 7. Graceful Shutdown Timeout

**Symptoms:** Server takes >30s to shut down, or workflows are lost during restart.

**Diagnosis:**
```bash
# Check running workflow count before shutdown
curl -H "Authorization: Bearer $TOKEN" http://localhost:8095/metrics | grep velocity_workflows_total

# Check shutdown logs
grep -i "shutdown\|drain" /var/log/velocity/*.log
```

**Resolution:**
1. If drain timeout hits: workflows are still running after 30s
   - These workflows will need to be re-executed after restart
   - Consider increasing the drain timeout in the Helm chart:
     ```yaml
     lifecycle:
       preStop:
         exec:
           command: ["/bin/sh", "-c", "sleep 30"]
     ```
2. Send SIGTERM earlier (before deployment) to start draining proactively
3. Check why workflows are taking so long — may indicate a slow step or PG bottleneck

---

## 8. Container Health Check Failing in Kubernetes

**Symptoms:** Pod enters CrashLoopBackOff or keeps restarting.

**Diagnosis:**
```bash
kubectl describe pod velocity-server-xxx
kubectl logs velocity-server-xxx --previous

# Check if health port matches Helm values
kubectl get pod velocity-server-xxx -o jsonpath='{.spec.containers[0].ports[*].containerPort}'
```

**Resolution:**
1. Verify `healthPort` in Helm values matches the server's `--health-bind` port
2. Check `livenessProbe` and `readinessProbe` configuration:
   - `initialDelaySeconds` should be >= WAL recovery time
   - `failureThreshold` should allow for transient failures
3. If OOMKilled: increase memory limits in Helm values
4. If startup probe needed: enable it for slow-starting instances

---

## 9. mTLS Certificate Issues

**Symptoms:** Clients can't connect; TLS handshake fails.

**Diagnosis:**
```bash
# Check server cert expiry
openssl x509 -in server.crt -noout -dates

# Check client cert against CA
openssl verify -CAfile ca.crt client.crt

# Test TLS connection
openssl s_client -connect localhost:8093 -cert client.crt -key client.key
```

**Resolution:**
1. If cert expired: rotate certificates
2. If client cert not signed by CA: issue new client cert from the correct CA
3. If mTLS is not needed: remove `--mtls-ca-cert` flag to disable client cert verification

---

## 10. Audit Log Volume Too High

**Symptoms:** Disk filling up with audit logs, or log aggregation pipeline overwhelmed.

**Diagnosis:**
```bash
# Check audit event rate
curl -H "Authorization: Bearer $TOKEN" http://localhost:8095/metrics | grep velocity_audit

# Check log volume
du -sh /var/log/velocity/
```

**Resolution:**
1. **Immediate:** Rotate logs with logrotate
2. Reduce audit verbosity: disable audit logging if not needed (`--audit-enabled=false`)
3. Configure log aggregation to filter audit events to a separate index
4. Set up log retention policies (30 days for audit, 7 days for debug)

---

## Quick Reference

| Metric | Normal | Warning | Critical |
|--------|--------|---------|----------|
| `velocity_workflows_total{running}` | < 100 | 100-1000 | > 1000 |
| `velocity_pg_write_queue_depth` | 0 | 1-10 | > 10 |
| `velocity_wal_unsynced_bytes` | 0 | < 1MB | > 10MB |
| `velocity_step_persist_latency_ms{p99}` | < 5ms | 5-50ms | > 50ms |
| `velocity_rate_limit_rejected_total` | 0 | > 0 | > 100/min |
| `velocity_audit_auth_failures_total` | 0 | > 0 | > 50/min |
| `velocity_pg_connected` | 1 | — | 0 |
| `vctp_requests_total` | > 0 | — | 0 for 2m |
| `vctp_errors_total` | 0 | > 0 | > 5% of requests |
| `vctp_circuit_breaker_state` | closed | half-open | open |
| `vctp_replay_detected_total` | 0 | > 0 | > 10/min |
| `vctp_hmac_verify_failures_total` | 0 | > 0 | > 5/min |

---

## 11. VCTP Circuit Breaker Tripped

**Symptoms:** VCTP clients receive 503 "service overloaded". Prometheus alert `VctpCircuitBreakerOpen` fires.

**Diagnosis:**
```bash
# Check circuit breaker state via metrics
curl -H "Authorization: Bearer $TOKEN" http://localhost:8095/metrics | grep vctp_circuit_breaker

# Check inflight request count
curl -H "Authorization: Bearer $TOKEN" http://localhost:8095/metrics | grep vctp_inflight_requests

# Check server logs for overload events
grep "circuit.breaker\|overloaded" /var/log/velocity/*.log | tail -20
```

**Resolution:**
1. **Immediate:** Check if a traffic spike caused the trip
   - If transient: circuit breaker will auto-recover (HalfOpen after cooldown_ms, default 5s)
   - If sustained: scale up server replicas or increase `max_inflight`
2. **Tuning:** Adjust circuit breaker thresholds in Helm values:
   ```yaml
   vctp:
     circuitBreaker:
       maxInflight: 15000   # increase from default 10000
       cooldownMs: 3000     # decrease for faster recovery
   ```
3. **Root cause:** Check if downstream PostgreSQL is slow (increases inflight duration)

---

## 12. VCTP Replay Attacks Detected

**Symptoms:** Prometheus alert `VctpReplayDetected` fires. `vctp_replay_detected_total` metric increasing.

**Diagnosis:**
```bash
# Check replay detection metrics
curl -H "Authorization: Bearer $TOKEN" http://localhost:8095/metrics | grep vctp_replay

# Check server logs for replay events
grep "replay" /var/log/velocity/*.log | tail -20

# Identify source IP of replayed packets
grep "replay.*from" /var/log/velocity/*.log | awk '{print $NF}' | sort | uniq -c | sort -rn
```

**Resolution:**
1. **Single occurrence:** Likely a network retransmission — no action needed
2. **Repeated from same IP:** Possible malicious replay — block the IP via NetworkPolicy
3. **Widespread:** Check if a client has a bug (reusing sequence numbers)
4. **Verify replay window:** Ensure `VctpReplayWindow` is configured with adequate depth (default 64)

---

## 13. VCTP HMAC Authentication Failures

**Symptoms:** VCTP packets rejected with MAC verification failure. `vctp_hmac_verify_failures_total` increasing.

**Diagnosis:**
```bash
# Check HMAC failure metrics
curl -H "Authorization: Bearer $TOKEN" http://localhost:8095/metrics | grep vctp_hmac

# Check server logs for HMAC failures
grep "hmac\|mac.*fail\|authentication.*fail" /var/log/velocity/*.log | tail -20
```

**Resolution:**
1. **Key mismatch:** Client and server using different HMAC keys — verify key distribution
2. **Clock skew:** If using time-based key rotation, check NTP synchronization
3. **Key rotation:** If keys were recently rotated, some clients may still use old keys
   - Wait for key propagation or force client restart
4. **Tampering:** If HMAC failures spike suddenly, check for network-level packet manipulation

---

## 14. VCTP Gateway TLS Failure

**Symptoms:** HTTPS/WSS clients cannot connect. TLS handshake errors in logs.

**Diagnosis:**
```bash
# Check TLS certificate expiry
kubectl -n velocity-system get secret velocity-tls -o jsonpath='{.data.tls\.crt}' | \
  base64 -d | openssl x509 -noout -dates

# Test HTTPS endpoint
curl -vk https://localhost:8443/health

# Test WSS endpoint
wscat -c wss://localhost:8444 --no-check

# Check gateway logs for TLS errors
grep "tls\|ssl\|rustls\|handshake" /var/log/velocity/*.log | tail -20
```

**Resolution:**
1. **Certificate expired:** Rotate via cert-manager or update the TLS secret manually
2. **Certificate mismatch:** Ensure the certificate CN/SAN matches the accessed hostname
3. **Protocol mismatch:** Client using TLS 1.2 but server requires TLS 1.3 — update client
4. **Key mismatch:** Verify the TLS key matches the certificate:
   ```bash
   openssl x509 -noout -modulus -in tls.crt | openssl md5
   openssl rsa -noout -modulus -in tls.key | openssl md5
   ```

---

## 15. VCTP Throughput Degradation

**Symptoms:** VCTP ops/s drops below CI threshold (500 ops/s). Prometheus alert `VctpLowThroughput` fires.

**Diagnosis:**
```bash
# Check current throughput
curl -H "Authorization: Bearer $TOKEN" http://localhost:8095/metrics | grep vctp_requests_total

# Calculate rate over last 5 minutes
# rate(vctp_requests_total[5m]) — should be > 8.33/s (500/60)

# Check system resources
kubectl -n velocity-system top pods -l app.kubernetes.io/name=velocity

# Check for contention
grep "lock\|contention\|slow" /var/log/velocity/*.log | tail -20
```

**Resolution:**
1. **Resource starvation:** Increase CPU/memory limits in Helm values
2. **Network saturation:** Check UDP packet loss — `netstat -su | grep "packet receive errors"`
3. **Database slowdown:** If using PostgreSQL persistence, check query performance
4. **RwLock contention:** Check logs for `expect()` failures indicating lock poisoning
5. **Scale out:** Add more server replicas — VCTP is stateless per-request
