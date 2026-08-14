#!/bin/sh
# Run 20 simple workflows on each Velocity flavor via grpcurl
# Then measure WAL sizes

PROTO=/proto/benchmark.proto
N=20

run_workflows() {
  local HOST=$1
  local LABEL=$2
  echo "=== $LABEL: Running $N workflows ==="
  
  # Reset first
  docker run --rm --network bench-suite_bench-net \
    -v "$PROTO:/proto/benchmark.proto:ro" \
    fullstorydev/grpcurl -plaintext -proto /proto/benchmark.proto \
    -d '{"namespace":"default"}' \
    "$HOST:7234" velocity.bench.v1.BenchmarkService/Reset 2>/dev/null
  
  OK=0
  for i in $(seq 1 $N); do
    WID="density_${i}_$(date +%s)"
    # Start workflow
    RESP=$(docker run --rm --network bench-suite_bench-net \
      -v "$PROTO:/proto/benchmark.proto:ro" \
      fullstorydev/grpcurl -plaintext -proto /proto/benchmark.proto \
      -d "{\"workflow_type\":\"simple_workflow\",\"workflow_id\":\"$WID\",\"namespace\":\"default\",\"task_queue\":\"bench\",\"step_count\":3}" \
      "$HOST:7234" velocity.bench.v1.BenchmarkService/StartWorkflow 2>&1)
    
    RUN_ID=$(echo "$RESP" | grep -o '"run_id":"[^"]*"' | head -1 | cut -d'"' -f4)
    
    if [ -n "$RUN_ID" ]; then
      # Wait for completion
      docker run --rm --network bench-suite_bench-net \
        -v "$PROTO:/proto/benchmark.proto:ro" \
        fullstorydev/grpcurl -plaintext -proto /proto/benchmark.proto \
        -d "{\"workflow_id\":\"$WID\",\"run_id\":\"$RUN_ID\",\"namespace\":\"default\",\"timeout_ms\":5000}" \
        "$HOST:7234" velocity.bench.v1.BenchmarkService/WaitForCompletion >/dev/null 2>&1
      OK=$((OK + 1))
    fi
    
    if [ $((i % 5)) -eq 0 ]; then
      echo "  $i/$N ($OK ok)"
    fi
  done
  echo "  $LABEL complete: $OK/$N succeeded"
}

# Run on all 3 flavors
run_workflows "bench-velocity-classic" "Classic"
run_workflows "bench-velocity-runtime" "Runtime"
run_workflows "bench-velocity-embedded" "Embedded"

echo ""
echo "=== WAL SIZES AFTER $N WORKFLOWS ==="
echo "Classic:"
docker exec bench-velocity-classic ls -lh /velocity.wal
echo "Runtime:"
docker exec bench-velocity-runtime ls -lh /data/runtime.wal
echo "Embedded:"
docker exec bench-velocity-embedded ls -lh /data/embedded.wal
