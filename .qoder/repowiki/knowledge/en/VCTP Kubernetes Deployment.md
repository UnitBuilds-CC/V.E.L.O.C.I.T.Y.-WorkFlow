# VCTP Kubernetes Deployment

## Overview

VCTP deployment on Kubernetes with Helm charts, including UDP port exposure, health probes, graceful drain with preStop hooks, and configurable security/durability settings.

## Helm Chart Structure

```
deploy/helm/velocity/
├── Chart.yaml
├── values.yaml
└── templates/
    └── deployment.yaml
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

  # Heartbeat configuration
  heartbeat:
    intervalSeconds: 30
    evictionTimeoutSeconds: 90
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

## Source Files

| File | Role |
|------|------|
| `deploy/helm/velocity/values.yaml` | VCTP configuration values |
| `deploy/helm/velocity/templates/deployment.yaml` | K8s deployment with VCTP port, probes, preStop |
