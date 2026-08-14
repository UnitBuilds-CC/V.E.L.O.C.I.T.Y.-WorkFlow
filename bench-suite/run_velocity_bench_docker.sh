#!/bin/sh
# Run velocity-bench binary against all 3 Velocity flavors
# Copy binary into container, run on bench-net network

cd /bench

echo "=== VELOCITY BENCH CLIENT ==="
./velocity-bench-bin --help 2>&1 || echo "Binary check failed"

echo ""
echo "=== VELOCITY CLASSIC (port 7234) ==="
./velocity-bench-bin --address http://bench-velocity-classic:7234 --profile smoke --runs 1 2>&1 || echo "Classic bench failed"

echo ""
echo "=== VELOCITY RUNTIME (port 7234) ==="
./velocity-bench-bin --address http://bench-velocity-runtime:7234 --profile smoke --runs 1 2>&1 || echo "Runtime bench failed"

echo ""
echo "=== VELOCITY EMBEDDED (port 7234) ==="
./velocity-bench-bin --address http://bench-velocity-embedded:7234 --profile smoke --runs 1 2>&1 || echo "Embedded bench failed"

echo ""
echo "=== DONE ==="
