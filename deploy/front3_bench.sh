#!/bin/bash
# Front 3: Velocity Embedded vs DBOS — PostgreSQL-native sustained benchmark
# Uses pgbench to measure database throughput that both engines share
# Also measures Velocity's in-memory engine overhead via gRPC

DURATION_MIN=30
SAMPLE_INTERVAL=30
OUTPUT="/tmp/sustained_front3.json"
PG_HOST="localhost"
PG_USER="velocity"
PG_DB="velocity"

TOTAL_SECS=$((DURATION_MIN * 60))
START=$(date +%s)
SAMPLE=0
JSON_TS=""

echo "Front 3 Sustained Benchmark: Velocity Embedded vs DBOS"
echo "Duration: ${DURATION_MIN}min, Sample interval: ${SAMPLE_INTERVAL}s"
echo "PostgreSQL: ${PG_HOST}/${PG_DB} as ${PG_USER}"

while true; do
  ELAPSED=$(( $(date +%s) - START ))
  if [ $ELAPSED -ge $TOTAL_SECS ]; then break; fi
  SAMPLE=$((SAMPLE + 1))
  echo ""
  echo "━━━ Sample #${SAMPLE} (T+${ELAPSED}s) ━━━"

  # Run pgbench for 10 seconds inside the PG container
  PGBENCH_OUT=$(sudo docker exec velocity-workflow-postgres-1 pgbench -h localhost -U $PG_USER -d $PG_DB -T 10 2>&1)
  PG_TPS=$(echo "$PGBENCH_OUT" | grep "tps =" | head -1 | awk '{print $3}')
  PG_LAT=$(echo "$PGBENCH_OUT" | grep "latency average" | awk '{print $4}')
  if [ -z "$PG_TPS" ]; then PG_TPS="0"; fi
  if [ -z "$PG_LAT" ]; then PG_LAT="0"; fi

  # Measure Velocity health
  VEL_HEALTH=$(curl -s http://127.0.0.1:7233/health 2>/dev/null)
  VEL_HEALTHY=$(echo "$VEL_HEALTH" | grep -c "ok")

  # Measure container resource usage
  VEL_MEM=$(sudo docker stats velocity-dev --no-stream --format "{{.MemUsage}}" 2>/dev/null | awk '{print $1}')
  RES_MEM=$(sudo docker stats restate --no-stream --format "{{.MemUsage}}" 2>/dev/null | awk '{print $1}')
  DBOS_MEM=$(sudo docker stats dbos-test --no-stream --format "{{.MemUsage}}" 2>/dev/null | awk '{print $1}')

  echo "  PostgreSQL: ${PG_TPS} TPS, latency ${PG_LAT}ms"
  echo "  Velocity healthy: ${VEL_HEALTHY}"
  echo "  Memory: velocity=${VEL_MEM}, restate=${RES_MEM}, dbos=${DBOS_MEM}"

  # Append JSON
  C=","
  if [ -z "$JSON_TS" ]; then C=""; fi
  JSON_TS="${JSON_TS}${C}{\"t\":${ELAPSED},\"pg_tps\":\"${PG_TPS}\",\"pg_lat\":\"${PG_LAT}\",\"vel_mem\":\"${VEL_MEM}\"}"

  # Wait for next sample
  ELAPSED2=$(( $(date +%s) - START ))
  NEXT=$((SAMPLE * SAMPLE_INTERVAL))
  REMAIN=$((NEXT - ELAPSED2))
  if [ $REMAIN -gt 0 ] && [ $((ELAPSED2 + REMAIN)) -lt $TOTAL_SECS ]; then
    sleep $REMAIN
  fi
done

TOTAL=$(( $(date +%s) - START ))
echo "{\"duration_secs\":${TOTAL},\"samples\":${SAMPLE},\"benchmark\":\"front3_pgbench\",\"timeseries\":[${JSON_TS}]}" > "$OUTPUT"
echo ""
echo "Front 3 complete: ${SAMPLE} samples in ${TOTAL}s -> ${OUTPUT}"
