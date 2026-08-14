$Zone = "us-east1-b"

# Deploy Temporal production service on temporal-bench VM
Write-Host "=== Deploying Temporal production service ===" -ForegroundColor Cyan
$temporalFiles = @(
    "c:\Users\visse\OneDrive\Documents\Velocity-workflow\cloud-bench\production\temporal\service.py",
    "c:\Users\visse\OneDrive\Documents\Velocity-workflow\cloud-bench\production\temporal\workflows.py",
    "c:\Users\visse\OneDrive\Documents\Velocity-workflow\cloud-bench\production\temporal\client.py",
    "c:\Users\visse\OneDrive\Documents\Velocity-workflow\cloud-bench\production\temporal\requirements.txt"
)
foreach ($f in $temporalFiles) {
    gcloud compute scp $f "temporal-bench:/tmp/temporal-production/" --zone=$Zone --quiet 2>&1
}
# Deploy service
gcloud compute ssh temporal-bench --zone=$Zone --quiet --command "mkdir -p ~/temporal-production; cp /tmp/temporal-production/*.py /tmp/temporal-production/*.txt ~/temporal-production/ 2>/dev/null; pip3 install --quiet temporalio fastapi uvicorn aiohttp 2>&1 | tail -3; pkill -f 'service.py' 2>/dev/null; sleep 1; cd ~/temporal-production; export TEMPORAL_ADDRESS=localhost:7233; export TEMPORAL_NAMESPACE=default; export TEMPORAL_TASK_QUEUE=bench-queue; export TEMPORAL_HTTP_PORT=8080; nohup python3 service.py server > ~/temporal_service.log 2>&1 & echo PID=$!; sleep 5; curl -s http://localhost:8080/health 2>/dev/null || echo SERVICE_NOT_READY" 2>&1

Write-Host "`n=== Deploying DBOS production service ===" -ForegroundColor Cyan
$dbosFiles = @(
    "c:\Users\visse\OneDrive\Documents\Velocity-workflow\cloud-bench\production\dbos\service.py",
    "c:\Users\visse\OneDrive\Documents\Velocity-workflow\cloud-bench\production\dbos\client.py",
    "c:\Users\visse\OneDrive\Documents\Velocity-workflow\cloud-bench\production\dbos\requirements.txt"
)
foreach ($f in $dbosFiles) {
    gcloud compute scp $f "dbos-bench:/tmp/dbos-production/" --zone=$Zone --quiet 2>&1
}
# Deploy DBOS - needs PostgreSQL first
gcloud compute ssh dbos-bench --zone=$Zone --quiet --command "mkdir -p ~/dbos-production; cp /tmp/dbos-production/*.py /tmp/dbos-production/*.txt ~/dbos-production/ 2>/dev/null; echo '=== Installing PostgreSQL ==='; sudo apt-get update -qq 2>&1 | tail -1; sudo apt-get install -y -qq postgresql postgresql-contrib 2>&1 | tail -3; sudo systemctl start postgresql 2>/dev/null; sudo -u postgres psql -c ""CREATE USER dbos WITH PASSWORD 'dbos_bench';"" 2>/dev/null; sudo -u postgres psql -c ""ALTER USER dbos WITH PASSWORD 'dbos_bench';"" 2>/dev/null; sudo -u postgres psql -c ""CREATE DATABASE dbos_bench OWNER dbos;"" 2>/dev/null; echo '=== Installing Python deps ==='; pip3 install --quiet dbos fastapi uvicorn aiohttp psycopg[binary] 2>&1 | tail -3; echo '=== Starting DBOS ==='; pkill -f 'service.py' 2>/dev/null; sleep 1; cd ~/dbos-production; export DBOS_DATABASE_URL=postgresql://dbos:dbos_bench@localhost:5432/dbos_bench; export DBOS_HTTP_PORT=8080; nohup python3 service.py server > ~/dbos_service.log 2>&1 & echo PID=$!; sleep 5; curl -s http://localhost:8080/health 2>/dev/null || echo SERVICE_NOT_READY" 2>&1

Write-Host "`n=== Deploying Restate service ===" -ForegroundColor Cyan
$restateFiles = @(
    "c:\Users\visse\OneDrive\Documents\Velocity-workflow\cloud-bench\production\restate\service.js",
    "c:\Users\visse\OneDrive\Documents\Velocity-workflow\cloud-bench\production\restate\client.js"
)
foreach ($f in $restateFiles) {
    gcloud compute scp $f "restate-bench:/tmp/restate-production/" --zone=$Zone --quiet 2>&1
}
gcloud compute ssh restate-bench --zone=$Zone --quiet --command "mkdir -p ~/restate-production; cp /tmp/restate-production/*.js ~/restate-production/ 2>/dev/null; cd ~/restate-production; npm init -y 2>/dev/null; npm install @restate/sdk 2>&1 | tail -3; pkill -f 'service.js' 2>/dev/null; nohup node service.js > ~/restate_service.log 2>&1 & echo PID=$!; sleep 3; curl -s http://localhost:9080/restate/discover 2>/dev/null | head -5 || echo SERVICE_NOT_READY; echo '---'; curl -s http://localhost:8082/restate/invoke 2>/dev/null | head -3 || echo INGRESS_NOT_READY" 2>&1

Write-Host "`n=== All competitor deployments complete ===" -ForegroundColor Green
