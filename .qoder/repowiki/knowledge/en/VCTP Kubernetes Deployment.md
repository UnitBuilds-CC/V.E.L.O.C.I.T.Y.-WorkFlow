# VCTP Kubernetes Deployment

## Overview

VCTP deployment on Kubernetes with Helm charts, including UDP port exposure, health probes, graceful drain with preStop hooks, and configurable security/durability settings.

## Helm Chart Structure

```
deploy/helm/velocity/
├── Chart.yaml
├── values.yaml
└── templates/
    ├── deployment.yaml
    ├── backup-cronjob.yaml
    └── prometheus-rules.yaml
```

## VCTP Configuration (values.yaml)

```yaml
vctp:
  enabled: false                    # Enable VCTP UDP server
  port: 9090                        # UDP port for VCTP traffic
  drainTimeoutSeconds: 30           # Seconds to wait for in-flight requests

  # VCTP-specific health probe
  healthProbe:
    enabled: false
    exec:
      command:
        - python3
        - /opt/velocity/tools/vctp-cli/vctp_cli.py
        - health
        - --server
        - "127.0.0.1:9090"

  # Security configuration
  security:
    authRequired: false
    jwtSecret: ""
    apiKeys: []
    rateLimitRps: 0
    rateLimitBurst: 0

  # Circuit breaker configuration
  circuitBreaker:
    maxInflight: 10000
    cooldownMs: 5000
    successThreshold: 3

  # Heartbeat configuration
  heartbeat:
    intervalSeconds: 30
    staleTimeoutSeconds: 90

  # TLS configuration for gateway endpoints (HTTPS/WSS)
  tls:
    enabled: false
    secretName: velocity-tls        # K8s Secret containing tls.crt and tls.key
    httpsPort: 8443                  # HTTPS ingress port
    wssPort: 8444                    # WSS gateway port
    certFile: tls.crt                # Certificate file in secret
    keyFile: tls.key                 # Key file in secret
```

## Deployment Template

### UDP Port

```yaml
# deployment.yaml
ports:
  - name: vctp
    containerPort: {{ .Values.vctp.port }}
    protocol: UDP
```

### Health Probes

Standard liveness/readiness probes on the health port:

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: health
  initialDelaySeconds: 5
  periodSeconds: 10

readinessProbe:
  httpGet:
    path: /health
    port: health
  initialDelaySeconds: 3
  periodSeconds: 5
```

VCTP-specific exec probe (when enabled):

```yaml
{{- if .Values.vctp.healthProbe.exec }}
exec:
  {{- toYaml .Values.vctp.healthProbe.exec | nindent 16 }}
{{- end }}
```

### Graceful Drain (preStop Hook)

```yaml
lifecycle:
  preStop:
    exec:
      command:
        - /bin/sh
        - -c
        - "sleep {{ .Values.vctp.drainTimeoutSeconds | default 30 }}"
```

**Drain sequence:**
1. K8s sends preStop hook → `sleep 30`
2. During sleep, `begin_drain()` is called (via signal handler or separate mechanism)
3. New VCTP requests receive 503 "server draining"
4. In-flight requests complete normally
5. After 30s, K8s sends SIGTERM
6. Pod terminates with all requests drained

## Service Configuration

```yaml
# Service exposes VCTP UDP port
apiVersion: v1
kind: Service
metadata:
  name: velocity-vctp
spec:
  selector:
    app: velocity
  ports:
    - name: vctp
      port: 9090
      targetPort: 9090
      protocol: UDP
```

## Production Checklist

| Setting | Recommended | Notes |
|---------|-------------|-------|
| `vctp.enabled` | `true` | Enable VCTP UDP server |
| `vctp.port` | `9090` | Standard VCTP port |
| `vctp.drainTimeoutSeconds` | `30` | Match K8s terminationGracePeriodSeconds |
| `vctp.healthProbe.enabled` | `true` | VCTP-specific health check |
| `vctp.security.authRequired` | `true` | Require JWT or API key |
| `vctp.circuitBreaker.maxInflight` | `10000` | Adjust based on load testing |
| `vctp.heartbeat.intervalSeconds` | `30` | Connection health monitoring |
| `vctp.tls.enabled` | `true` (external) | TLS for HTTPS/WSS gateways |
| `vctp.tls.secretName` | `velocity-tls` | cert-manager or manual secret |

## Prometheus Alert Rules

VCTP-specific alerts in `deploy/helm/velocity/templates/prometheus-rules.yaml`:

| Alert | Severity | Condition | Duration |
|-------|----------|-----------|----------|
| VctpHighErrorRate | critical | >5% VCTP error rate | 5m |
| VctpCircuitBreakerOpen | critical | Circuit state = Open | 2m |
| VctpLowThroughput | warning | <1 VCTP request/s | 10m |
| VctpHighLatency | warning | Avg duration >50ms | 5m |
| VctpDrainActive | warning | Drain active | 10m |
| VctpAuthRejectionsSpike | warning | >10 auth rejections/s | 5m |

These complement the 5 existing HTTP-level alerts for a total of 11 Prometheus alert rules.

## Source Files

| File | Lines | Role |
|------|-------|------|
| `deploy/helm/velocity/values.yaml` | 676 | VCTP configuration values |
| `deploy/helm/velocity/templates/deployment.yaml` | 167 | K8s deployment with VCTP port, probes, preStop |
| `deploy/helm/velocity/templates/backup-cronjob.yaml` | 107 | WAL backup CronJob with encryption and S3 upload |
| `deploy/helm/velocity/templates/prometheus-rules.yaml` | 135 | 6 VCTP-specific + 5 HTTP Prometheus alert rules |
