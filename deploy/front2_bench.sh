#!/bin/bash
# Front 2: Velocity HTTP vs Restate HTTP sustained benchmark
# Uses wrk for accurate HTTP throughput measurement

DURATION_MIN=30
SAMPLE_INTERVAL=30
BENCH_DURATION=10
VEL_URL="http://127.0.0.1:7233/health"
RES_URL="http://127.0.0.1:8080/"
OUTPUT="/tmp/sustained_front2.json"

TOTAL_SECS=$((DURATION_MIN * 60))
START=$(date +%s)
SAMPLE=0
JSON_TS=""

echo "Front 2 Sustained Benchmark: Velocity HTTP vs Restate HTTP"
echo "Duration: ${DURATION_MIN}min, Sample interval: ${SAMPLE_INTERVAL}s, wrk duration: ${BENCH_DURATION}s"

while true; do
  ELAPSED=$(( $(date +%s) - START ))
  if [ $ELAPSED -ge $TOTAL_SECS ]; then break; fi
  SAMPLE=$((SAMPLE + 1))
  echo ""
  echo "━━━ Sample #${SAMPLE} (T+${ELAPSED}s) ━━━"

  # Benchmark Velocity HTTP
  VEL_OUT=$(wrk -t2 -c10 -d${BENCH_DURATION}s "$VEL_URL" 2>&1)
  VEL_RPS=$(echo "$VEL_OUT" | grep "Requests/sec" | awk '{print $2}')
  VEL_LAT=$(echo "$VEL_OUT" | awk '/Latency/{print $2; exit}')
  if [ -z "$VEL_RPS" ]; then VEL_RPS="0"; fi

  # Benchmark Restate HTTP
  RES_OUT=$(wrk -t2 -c10 -d${BENCH_DURATION}s "$RES_URL" 2>&1)
  RES_RPS=$(echo "$RES_OUT" | grep "Requests/sec" | awk '{print $2}')
  RES_LAT=$(echo "$RES_OUT" | awk '/Latency/{print $2; exit}')
  if [ -z "$RES_RPS" ]; then RES_RPS="0"; fi

  echo "  VELOCITY: ${VEL_RPS} req/s, latency ${VEL_LAT}"
  echo "  RESTATE:  ${RES_RPS} req/s, latency ${RES_LAT}"

  # Append JSON timeseries entry
  C=","
  if [ -z "$JSON_TS" ]; then C=""; fi
  JSON_TS="${JSON_TS}${C}{\"t\":${ELAPSED},\"v_rps\":\"${VEL_RPS}\",\"r_rps\":\"${RES_RPS}\",\"v_lat\":\"${VEL_LAT}\",\"r_lat\":\"${RES_LAT}\"}"

  # Wait for next sample interval
  ELAPSED2=$(( $(date +%s) - START ))
  NEXT=$((SAMPLE * SAMPLE_INTERVAL))
  REMAIN=$((NEXT - ELAPSED2))
  if [ $REMAIN -gt 0 ] && [ $((ELAPSED2 + REMAIN)) -lt $TOTAL_SECS ]; then
    sleep $REMAIN
  fi
done

TOTAL=$(( $(date +%s) - START ))
echo "{\"duration_secs\":${TOTAL},\"samples\":${SAMPLE},\"timeseries\":[${JSON_TS}]}" > "$OUTPUT"
echo ""
echo "Front 2 complete: ${SAMPLE} samples in ${TOTAL}s -> ${OUTPUT}"
