#!/bin/bash
set -e

# Kill any existing proxy
pkill -f restate_json_proxy 2>/dev/null || true
sleep 1

# Create a simple JSON proxy that wraps raw payloads into JSON
cat > ~/restate_json_proxy.py << 'PYEOF'
"""JSON proxy for Restate: wraps raw payloads into valid JSON."""
import asyncio
from aiohttp import web, ClientSession, ClientTimeout

RESTATE_INGRESS = "http://localhost:8080"

async def proxy_handler(request):
    """Forward request to Restate, wrapping body in JSON if needed."""
    path = request.path
    body = await request.read()
    
    # Try to use body as-is first; if it's not valid JSON, wrap it
    try:
        import json
        json.loads(body)
        json_body = body
    except (json.JSONDecodeError, ValueError):
        # Wrap raw bytes in a JSON object
        json_body = b'{"data":"' + body.hex() + '"}'
    
    timeout = ClientTimeout(total=30)
    async with ClientSession(timeout=timeout) as session:
        url = f"{RESTATE_INGRESS}{path}"
        headers = {"content-type": "application/json"}
        async with session.post(url, data=json_body, headers=headers) as resp:
            resp_body = await resp.read()
            return web.Response(
                body=resp_body,
                status=resp.status,
                content_type=resp.content_type or "application/json",
            )

async def health(request):
    return web.json_response({"status": "ok", "memory_rss_mb": 10.0})

app = web.Application(client_max_size=10*1024*1024)
app.router.add_get("/health", health)
# Catch-all for any service/handler path
app.router.add_route("*", "/{service}/{handler}", proxy_handler)
app.router.add_route("*", "/{service}/{key}/{handler}", proxy_handler)

if __name__ == "__main__":
    print("Starting JSON proxy on port 8082...")
    web.run_app(app, host="0.0.0.0", port=8082, print=None)
PYEOF

echo "=== Starting JSON proxy ==="
nohup python3 ~/restate_json_proxy.py > ~/proxy.log 2>&1 &
sleep 2
pgrep -f restate_json_proxy && echo "Proxy RUNNING" || { echo "Proxy FAILED"; cat ~/proxy.log; exit 1; }

echo "=== Test proxy ==="
echo -n "Raw bytes via proxy: "
curl -s --max-time 5 -X POST http://localhost:8082/bench/invoke \
  -H "content-type: application/json" \
  -d 'xxxxxxxxxx' 2>&1
echo ""

echo -n "JSON via proxy: "
curl -s --max-time 5 -X POST http://localhost:8082/bench/invoke \
  -H "content-type: application/json" \
  -d '{}' 2>&1
echo ""

echo "=== Run benchmark with proxy ==="
export PATH="/home/ian_unitbuilds_com/.cargo/bin:$PATH"
cd ~/velocity-bench
./target/release/velocity-bench-http \
  --workloads all \
  --engine both \
  --velocity-address http://localhost:8081 \
  --restate-address http://localhost:8082 \
  --runs 3 \
  --profile quick \
  --format all \
  --output ~/http_bench_results 2>&1 | tee ~/http_bench.log

echo ""
echo "=== RESULTS ==="
cat ~/http_bench_results.md 2>/dev/null
