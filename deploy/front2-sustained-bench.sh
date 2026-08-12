#!/bin/bash
# Front 2 Sustained Benchmark: Velocity HTTP API vs Restate HTTP API
# Measures raw HTTP throughput every 30 seconds for 30 minutes

DURATION_MIN=30
SAMPLE_INTERVAL=30
VELOCITY_URL="http://velocity-dev:7233"
RESTATE_INGRESS="http://restate:8080"
RESTATE_ADMIN="http://restate:9070"
OUTPUT_FILE="/tmp/sustained_front2.json"

# Register a simple Restate service (Greeter) via admin API
echo "Registering Restate service..."
curl -s -X POST "${RESTATE_ADMIN}/v2/services/greeter/handlers/greet" \
  -H 'content-type: application/json' \
  -d '{"uri":"http://localhost:9080"}' 2>/dev/null

# For Restate, we'll benchmark the ingress endpoint directly
# Even without a registered service, we can measure HTTP handling overhead

DURATION_SECS=$((DURATION_MIN * 60))
JSON_SAMPLES=""
VELOCITY_OPS_TOTAL=0
RESTATE_OPS_TOTAL=0
SAMPLE_NUM=0
START_TIME=$(date +%s)

echo "Starting Front 2 sustained benchmark: ${DURATION_MIN} minutes, ${SAMPLE_INTERVAL}s interval"
echo "Velocity: ${VELOCITY_URL}"
echo "Restate: ${RESTATE_INGRESS}"

while true; do
  ELAPSED=$(($(date +%s) - START_TIME))
  if [ $ELAPSED -ge $DURATION_SECS ]; then
    break
  fi
  
  SAMPLE_NUM=$((SAMPLE_NUM + 1))
  echo ""
  echo "━━━ Sample #${SAMPLE_NUM} (T+${ELAPSED}s) ━━━"
  
  # ─── Benchmark Velocity HTTP ───
  # Hit /health endpoint for 10 seconds, count requests
  VEL_COUNT=0
  VEL_START=$(date +%s%N)
  while true; do
    NOW=$(date +%s)
    ELAPSED_NOW=$((NOW - START_TIME))
    if [ $ELAPSED_NOW -ge $DURATION_SECS ]; then break; fi
    # Fire batch of requests
    for i in $(seq 1 10); do
      curl -s -o /dev/null -w '' "${VELOCITY_URL}/health" &
    done
    wait
    VEL_COUNT=$((VEL_COUNT + 10))
    # Check if 10 seconds have passed for this sample
    VEL_ELAPSED=$(( ($(date +%s%N) - VEL_START) / 1000000 ))
    if [ $VEL_ELAPSED -ge 10000 ]; then
      break
    fi
  done
  VEL_ELAPSED_MS=$(( ($(date +%s%N) - VEL_START) / 1000000 ))
  VEL_OPS_PER_SEC=$(echo "scale=1; $VEL_COUNT * 1000 / $VEL_ELAPSED_MS" | bc 2>/dev/null || echo "0")
  
  # ─── Benchmark Restate HTTP ───
  RES_COUNT=0
  RES_START=$(date +%s%N)
  while true; do
    NOW=$(date +%s)
    ELAPSED_NOW=$((NOW - START_TIME))
    if [ $ELAPSED_NOW -ge $DURATION_SECS ]; then break; fi
    for i in $(seq 1 10); do
      curl -s -o /dev/null -w '' "${RESTATE_INGRESS}/restate/api" &
    done
    wait
    RES_COUNT=$((RES_COUNT + 10))
    RES_ELAPSED_MS=$(( ($(date +%s%N) - RES_START) / 1000000 ))
    if [ $RES_ELAPSED_MS -ge 10000 ]; then
      break
    fi
  done
  RES_ELAPSED_MS=$(( ($(date +%s%N) - RES_START) / 1000000 ))
  RES_OPS_PER_SEC=$(echo "scale=1; $RES_COUNT * 1000 / $RES_ELAPSED_MS" | bc 2>/dev/null || echo "0")
  
  echo "  VELOCITY HTTP: ${VEL_OPS_PER_SEC} req/sec (${VEL_COUNT} reqs in ${VEL_ELAPSED_MS}ms)"
  echo "  RESTATE  HTTP: ${RES_OPS_PER_SEC} req/sec (${RES_COUNT} reqs in ${RES_ELAPSED_MS}ms)"
  
  # Build JSON sample
  COMMA=","
  if [ -z "$JSON_SAMPLES" ]; then COMMA=""; fi
  JSON_SAMPLES="${JSON_SAMPLES}${COMMA}{\"t\":${ELAPSED},\"v_ops\":${VEL_OPS_PER_SEC},\"r_ops\":${RES_OPS_PER_SEC}}"
  
  # Wait for next sample interval
  REMAINING=$((SAMPLE_INTERVAL - ELAPSED + START_TIME - $(date +%s) + START_TIME))
  if [ $((ELAPSED + SAMPLE_INTERVAL)) -lt $DURATION_SECS ]; then
    sleep $SAMPLE_INTERVAL
  fi
done

TOTAL_SECS=$(($(date +%s) - START_TIME))

# Write JSON output
cat > "$OUTPUT_FILE" << JSONEOF
{
  "sustained_duration_secs": ${TOTAL_SECS},
  "sample_interval_secs": ${SAMPLE_INTERVAL},
  "benchmark": "front2_http_throughput",
  "samples": ${SAMPLE_NUM},
  "timeseries": [${JSON_SAMPLES}]
}
JSONEOF

echo ""
echo "Front 2 benchmark complete. ${SAMPLE_NUM} samples in ${TOTAL_SECS}s"
echo "Results written to ${OUTPUT_FILE}"
