# Velocity Workflow — Production Runbook

This runbook covers common operational procedures for the Velocity Workflow platform deployed on Kubernetes (via Helm) or Docker Compose.

---

## Table of Contents

1. [Quick Reference](#quick-reference)
2. [Common Operational Procedures](#common-operational-procedures)
3. [Scaling Guide](#scaling-guide)
4. [Backup & Restore (PostgreSQL)](#backup--restore-postgresql)
5. [Monitoring Alerts & Thresholds](#monitoring-alerts--thresholds)
6. [Troubleshooting Guide](#troubleshooting-guide)
7. [Rollback Procedures](#rollback-procedures)
8. [Certificate Management (mTLS)](#certificate-management-mtls)

---

## Quick Reference

| Component         | Port(s)         | Health Endpoint      | Default Replicas |
|-------------------|-----------------|----------------------|------------------|
| velocity-server   | 7233 (HTTP), 7234 (gRPC) | `GET /health` | 2 |
| PostgreSQL        | 5432            | `pg_isready`         | 1 |
| Prometheus        | 9090            | `/-/healthy`         | 1 |
| Grafana           | 3000            | `/api/health`        | 1 |
| velocity-operator | 8080 (metrics), 8081 (health) | `/readyz` | 1 |

---

## Common Operational Procedures

### View Server Logs

```bash
# Kubernetes
kubectl -n velocity-system logs -l app.kubernetes.io/name=velocity -f --tail=100

# Docker Compose
docker compose logs -f velocity-server --tail=100
```

### Restart Server (Rolling)

```bash
kubectl -n velocity-system rollout restart deployment/velocity-server
```

### Check Deployment Status

```bash
kubectl -n velocity-system get deploy,pod,svc -l app.kubernetes.io/part-of=velocity
```

### Drain a Node Safely

```bash
kubectl cordon <node>
kubectl drain <node> --ignore-daemonsets --delete-emptydir-data --grace-period=60
```

### Trigger a Manual Compaction (PostgreSQL)

```bash
kubectl -n velocity-system exec -it velocity-postgres -- psql -U velocity -d velocity -c "VACUUM ANALYZE;"
```

---

## Scaling Guide

### Horizontal Scaling (Server Pods)

**Via Helm:**
```bash
helm upgrade velocity ./deploy/helm/velocity \
  --set autoscaling.minReplicas=3 \
  --set autoscaling.maxReplicas=15 \
  --set autoscaling.targetCPUUtilizationPercentage=65 \
  -n velocity-system
```

**Manual override (disables HPA temporarily):**
```bash
kubectl -n velocity-system scale deployment velocity-server --replicas=5
```

### Vertical Scaling (Resource Limits)

```bash
helm upgrade velocity ./deploy/helm/velocity \
  --set resources.limits.cpu=4 \
  --set resources.limits.memory=4Gi \
  --set resources.requests.cpu=1 \
  --set resources.requests.memory=1Gi \
  -n velocity-system
```

### PostgreSQL Scaling

For vertical scaling of PostgreSQL, update the StatefulSet resource limits:
```bash
kubectl -n velocity-system patch statefulset velocity-postgres -p \
  '{"spec":{"template":{"spec":{"containers":[{"name":"postgres","resources":{"limits":{"cpu":"2","memory":"4Gi"}}}]}}}}'
```

For read replicas, consider using a PostgreSQL operator (e.g., Crunchy, Zalando) with connection pooling via PgBouncer.

---

## Backup & Restore (PostgreSQL)

### Automated Backups (CronJob)

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: velocity-pg-backup
  namespace: velocity-system
spec:
  schedule: "0 2 * * *"  # Daily at 2 AM UTC
  jobTemplate:
    spec:
      template:
        spec:
          containers:
            - name: backup
              image: postgres:16-alpine
              command:
                - /bin/sh
                - -c
                - |
                  pg_dump -h velocity-postgres -U velocity velocity | \
                    gzip > /backup/velocity-$(date +%Y%m%d-%H%M%S).sql.gz
              volumeMounts:
                - name: backup-storage
                  mountPath: /backup
              env:
                - name: PGPASSWORD
                  valueFrom:
                    secretKeyRef:
                      name: velocity-postgres
                      key: password
          volumes:
            - name: backup-storage
              persistentVolumeClaim:
                claimName: velocity-backup-pvc
          restartPolicy: OnFailure
```

### Manual Backup

```bash
# From inside the cluster
kubectl -n velocity-system exec velocity-postgres -- \
  pg_dump -U velocity --format=custom velocity > backup_$(date +%Y%m%d).dump

# Full backup with compression
kubectl -n velocity-system exec velocity-postgres -- \
  pg_dump -U velocity -Fc -Z9 velocity > backup_$(date +%Y%m%d).sql.gz
```

### Restore

```bash
# Stop the server first to prevent writes
kubectl -n velocity-system scale deployment velocity-server --replicas=0

# Restore from dump
kubectl -n velocity-system exec -i velocity-postgres -- \
  pg_restore -U velocity -d velocity --clean --if-exists < backup.dump

# Restart the server
kubectl -n velocity-system scale deployment velocity-server --replicas=2
```

### Verify Backup Integrity

```bash
pg_restore --list backup.dump | head -20
```

---

## Monitoring Alerts & Thresholds

### Critical Alerts (Page Immediately)

| Alert | Condition | Threshold | Action |
|-------|-----------|-----------|--------|
| ServerDown | `up{job="velocity-server"} == 0` | 1 min | Check pod status, restart if CrashLoopBackOff |
| HighErrorRate | `rate(velocity_workflow_executions_total{status="failed"}[5m]) / rate(velocity_workflow_executions_total[5m])` | > 5% | Check recent deployments, review logs |
| DatabaseDown | `pg_up == 0` | 1 min | Check PostgreSQL pod, verify PVC space |
| DiskFull | `node_filesystem_avail_bytes{mountpoint="/var/lib/postgresql/data"}` | < 10% | Expand PVC, run VACUUM FULL |

### Warning Alerts (Investigate Within 15 min)

| Alert | Condition | Threshold | Action |
|-------|-----------|-----------|--------|
| HighLatency | `histogram_quantile(0.99, rate(velocity_workflow_duration_seconds_bucket[5m]))` | > 10s | Check DB query performance, scale horizontally |
| HighCPU | `rate(container_cpu_usage_seconds_total[5m])` | > 80% of limit | Scale vertically or horizontally |
| HighMemory | `container_memory_working_set_bytes` | > 85% of limit | Check for memory leaks, increase limits |
| TaskQueueBacklog | `velocity_task_queue_depth` | > 1000 | Scale workers, check for stuck activities |
| SlabUtilizationHigh | `velocity_slab_utilization_ratio` | > 0.9 | Trigger slab compaction |

### Prometheus Alerting Rules

```yaml
groups:
  - name: velocity.alerts
    rules:
      - alert: VelocityHighErrorRate
        expr: |
          sum(rate(velocity_workflow_executions_total{status="failed"}[5m]))
          / sum(rate(velocity_workflow_executions_total[5m])) > 0.05
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Velocity workflow error rate > 5%"
          runbook: "https://runbooks.velocity.io/high-error-rate"
```

---

## Troubleshooting Guide

### Issue: Pods in CrashLoopBackOff

**Symptoms:** Server pods restart repeatedly.

**Diagnosis:**
```bash
kubectl -n velocity-system describe pod <pod-name>
kubectl -n velocity-system logs <pod-name> --previous
```

**Common Causes:**
- Database connection failure → Check PostgreSQL is healthy and credentials are correct
- Missing native library → Verify `LD_LIBRARY_PATH` includes `/app/lib`
- Port conflict → Ensure ports 7233/7234 are not already bound

### Issue: High Workflow Latency

**Symptoms:** p99 latency exceeds SLA threshold.

**Diagnosis:**
```bash
# Check database query performance
kubectl -n velocity-system exec velocity-postgres -- \
  psql -U velocity -c "SELECT mean_time, calls FROM pg_stat_statements ORDER BY mean_time DESC LIMIT 10;"

# Check pod resource usage
kubectl -n velocity-system top pods -l app.kubernetes.io/name=velocity
```

**Resolution:**
- Scale horizontally (increase replicas)
- Optimize slow queries (add indexes)
- Increase resource limits

### Issue: Task Queue Backlog

**Symptoms:** `velocity_task_queue_depth` grows continuously.

**Diagnosis:**
```bash
# Check if workers are polling
kubectl -n velocity-system logs -l app.kubernetes.io/name=velocity | grep "activity polled"
```

**Resolution:**
- Scale up server replicas
- Check for stuck activities with long timeouts
- Verify task queue configuration matches worker subscriptions

### Issue: Database Connection Exhaustion

**Symptoms:** "Too many connections" errors in server logs.

**Resolution:**
```bash
# Check current connections
kubectl -n velocity-system exec velocity-postgres -- \
  psql -U velocity -c "SELECT count(*) FROM pg_stat_activity;"

# Increase max_connections (requires restart)
kubectl -n velocity-system set env statefulset/velocity-postgres POSTGRES_MAX_CONNECTIONS=200
```

### Issue: Prometheus Not Scraping Metrics

**Symptoms:** No data in Grafana dashboards.

**Diagnosis:**
```bash
# Verify ServiceMonitor exists
kubectl -n velocity-system get servicemonitor

# Check Prometheus targets
kubectl -n monitoring exec prometheus-server-0 -- \
  wget -qO- http://localhost:9090/api/v1/targets | jq '.data.activeTargets'
```

---

## Rollback Procedures

### Helm Rollback

```bash
# List release history
helm history velocity -n velocity-system

# Rollback to previous version
helm rollback velocity -n velocity-system

# Rollback to specific revision
helm rollback velocity <REVISION> -n velocity-system
```

### Docker Image Rollback

```bash
# Rollback to previous image
kubectl -n velocity-system set image deployment/velocity-server \
  velocity=velocity-workflow-server:<PREVIOUS_TAG>

# Monitor rollout
kubectl -n velocity-system rollout status deployment/velocity-server
```

### Database Schema Rollback

If a migration causes issues:
1. Stop the server: `kubectl scale deployment velocity-server --replicas=0`
2. Restore from backup (see Backup & Restore section)
3. Redeploy with previous schema version
4. Restart the server

---

## Certificate Management (mTLS)

### Generate CA and Certificates

```bash
# Generate CA
openssl req -x509 -newkey rsa:4096 -days 365 -nodes \
  -keyout ca-key.pem -out ca-cert.pem \
  -subj "/CN=Velocity Workflow CA"

# Generate server certificate
openssl req -newkey rsa:4096 -nodes \
  -keyout server-key.pem -out server-req.pem \
  -subj "/CN=velocity-server.velocity-system.svc.cluster.local"

openssl x509 -req -in server-req.pem -days 365 \
  -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial \
  -out server-cert.pem \
  -extfile <(printf "subjectAltName=DNS:velocity-server,DNS:velocity-server.velocity-system.svc.cluster.local")

# Create Kubernetes secret
kubectl -n velocity-system create secret tls velocity-tls \
  --cert=server-cert.pem --key=server-key.pem
```

### Rotate Certificates

```bash
# Update the TLS secret
kubectl -n velocity-system create secret tls velocity-tls \
  --cert=new-server-cert.pem --key=new-server-key.pem --dry-run=client -o yaml | \
  kubectl apply -f -

# Restart pods to pick up new certs
kubectl -n velocity-system rollout restart deployment/velocity-server
```

### Verify Certificate Expiry

```bash
kubectl -n velocity-system get secret velocity-tls -o jsonpath='{.data.tls\.crt}' | \
  base64 -d | openssl x509 -noout -dates
```

### Enable mTLS in Helm

```yaml
# values.yaml
tls:
  enabled: true
  secretName: velocity-tls
  caSecretName: velocity-ca-cert
  mtls:
    enabled: true
    verifyClient: true
```

---

## Emergency Contacts

| Role | Contact | Escalation |
|------|---------|------------|
| On-Call Engineer | #velocity-oncall (Slack) | PagerDuty |
| Platform Team | #platform-eng (Slack) | Engineering Manager |
| Database Admin | #dba-oncall (Slack) | VP Engineering |

---

## Useful Commands Cheat Sheet

```bash
# Quick health check
kubectl -n velocity-system get pods -l app.kubernetes.io/part-of=velocity

# Watch pod status
watch kubectl -n velocity-system get pods -l app.kubernetes.io/part-of=velocity

# Port-forward for local debugging
kubectl -n velocity-system port-forward svc/velocity-server 5000:5000

# Access Grafana
kubectl -n velocity-system port-forward svc/grafana 3000:3000

# Check HPA status
kubectl -n velocity-system get hpa

# View recent events
kubectl -n velocity-system get events --sort-by='.lastTimestamp' | tail -20
```
