#!/usr/bin/env bash
# wait_for_healthy.sh — Wait for all services in a docker-compose file to
# become healthy.
#
# Usage:
#   ./wait_for_healthy.sh [compose-file] [timeout-seconds]

set -euo pipefail

COMPOSE_FILE="${1:-docker-compose.yml}"
TIMEOUT="${2:-300}"
INTERVAL=5

echo "[wait] Waiting for all services to be healthy (timeout: ${TIMEOUT}s)..."

elapsed=0
while [ "$elapsed" -lt "$TIMEOUT" ]; do
    all_healthy=true

    # Get list of services
    services=$(docker compose -f "$COMPOSE_FILE" ps --format '{{.Name}}' 2>/dev/null || true)

    if [ -z "$services" ]; then
        echo "[wait] No services found. Check your compose file."
        exit 1
    fi

    for svc in $services; do
        # Check if container is running
        status=$(docker inspect --format='{{.State.Status}}' "$svc" 2>/dev/null || echo "not_found")
        if [ "$status" != "running" ]; then
            all_healthy=false
            continue
        fi

        # Check health status (if healthcheck is defined)
        health=$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}healthy{{end}}' "$svc" 2>/dev/null || echo "unknown")
        if [ "$health" = "unhealthy" ]; then
            echo "[wait] $svc is unhealthy!"
            docker logs --tail 10 "$svc" 2>&1 | sed 's/^/[wait]   /'
            exit 1
        fi
        if [ "$health" != "healthy" ]; then
            all_healthy=false
        fi
    done

    if $all_healthy; then
        echo "[wait] All services healthy!"
        docker compose -f "$COMPOSE_FILE" ps
        exit 0
    fi

    sleep "$INTERVAL"
    elapsed=$((elapsed + INTERVAL))
    echo "[wait] ${elapsed}s elapsed..."
done

echo "[wait] TIMEOUT after ${TIMEOUT}s. Service status:"
docker compose -f "$COMPOSE_FILE" ps
exit 1
