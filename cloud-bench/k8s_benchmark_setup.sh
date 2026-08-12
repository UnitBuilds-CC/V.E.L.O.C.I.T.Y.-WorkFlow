#!/bin/bash
# K8s Benchmark Setup Script - Run from classic VM
# Builds Docker images, pushes to Artifact Registry, deploys to GKE, runs benchmarks

set -e

PROJECT="velocity-live-test-001"
REGION="us-east1"
REGISTRY="${REGION}-docker.pkg.dev/${PROJECT}/velocity"
CLUSTER="velocity-bench-k8s"
ZONE="us-east1-b"
BENCH_DIR="$HOME/velocity-bench/target/release"

echo "=== Step 1: Configure Docker auth ==="
gcloud auth configure-docker ${REGION}-docker.pkg.dev --quiet 2>/dev/null || true

echo "=== Step 2: Get GKE credentials ==="
gcloud container clusters get-credentials ${CLUSTER} --zone=${ZONE} --project=${PROJECT}
kubectl cluster-info

echo "=== Step 3: Create namespace ==="
kubectl create namespace velocity-bench --dry-run=client -o yaml | kubectl apply -f -

echo "=== Step 4: Build Velocity server Docker image ==="
cat > /tmp/Dockerfile.velocity-server << 'DEOF'
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY velocity-server /usr/local/bin/velocity-server
EXPOSE 7234
CMD ["velocity-server", "--ip", "0.0.0.0", "--grpc-port", "7234"]
DEOF

cd $HOME/velocity-bench
cp ${BENCH_DIR}/velocity-server .
docker build -f /tmp/Dockerfile.velocity-server -t ${REGISTRY}/velocity-server:v1 .
rm -f velocity-server

echo "=== Step 5: Build Velocity bench Docker image ==="
cat > /tmp/Dockerfile.velocity-bench << 'DEOF'
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY velocity-bench /usr/local/bin/velocity-bench
ENTRYPOINT ["velocity-bench"]
DEOF

cp ${BENCH_DIR}/velocity-bench .
docker build -f /tmp/Dockerfile.velocity-bench -t ${REGISTRY}/velocity-bench:v1 .
rm -f velocity-bench

echo "=== Step 6: Push images ==="
docker push ${REGISTRY}/velocity-server:v1
docker push ${REGISTRY}/velocity-bench:v1

echo "=== Step 7: Deploy Velocity server ==="
cat > /tmp/velocity-deployment.yaml << 'KEOF'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: velocity-server
  namespace: velocity-bench
spec:
  replicas: 1
  selector:
    matchLabels:
      app: velocity-server
  template:
    metadata:
      labels:
        app: velocity-server
    spec:
      containers:
      - name: velocity-server
        image: us-east1-docker.pkg.dev/velocity-live-test-001/velocity/velocity-server:v1
        ports:
        - containerPort: 7234
        resources:
          requests:
            cpu: "2"
            memory: "4Gi"
          limits:
            cpu: "4"
            memory: "8Gi"
---
apiVersion: v1
kind: Service
metadata:
  name: velocity-server
  namespace: velocity-bench
spec:
  selector:
    app: velocity-server
  ports:
  - port: 7234
    targetPort: 7234
  type: ClusterIP
KEOF

kubectl apply -f /tmp/velocity-deployment.yaml

echo "=== Step 8: Deploy Temporal ==="
cat > /tmp/temporal-deployment.yaml << 'TEOF'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: temporal
  namespace: velocity-bench
spec:
  replicas: 1
  selector:
    matchLabels:
      app: temporal
  template:
    metadata:
      labels:
        app: temporal
    spec:
      containers:
      - name: temporal
        image: temporalio/auto-setup:latest
        ports:
        - containerPort: 7233
        env:
        - name: DB
          value: "sqlite"
        - name: DB_PATH
          value: "/data/temporal.db"
        resources:
          requests:
            cpu: "1"
            memory: "2Gi"
          limits:
            cpu: "2"
            memory: "4Gi"
---
apiVersion: v1
kind: Service
metadata:
  name: temporal
  namespace: velocity-bench
spec:
  selector:
    app: temporal
  ports:
  - port: 7233
    targetPort: 7233
  type: ClusterIP
TEOF

kubectl apply -f /tmp/temporal-deployment.yaml

echo "=== Step 9: Wait for deployments ==="
kubectl -n velocity-bench rollout status deployment/velocity-server --timeout=120s
kubectl -n velocity-bench rollout status deployment/temporal --timeout=120s

echo "=== Step 10: Check services ==="
kubectl -n velocity-bench get pods
kubectl -n velocity-bench get svc

echo "=== Step 11: Run Velocity benchmark ==="
kubectl -n velocity-bench run bench-velocity --image=${REGISTRY}/velocity-bench:v1 --restart=Never --rm -i -- \
  --workloads all \
  --engine velocity \
  --velocity-address http://velocity-server:7234 \
  --profile quick \
  --format json 2>&1 | tee /tmp/k8s_velocity_results.json

echo "=== Step 12: Run Temporal benchmark ==="
kubectl -n velocity-bench run bench-temporal --image=${REGISTRY}/velocity-bench:v1 --restart=Never --rm -i -- \
  --workloads all \
  --engine temporal \
  --temporal-address temporal:7233 \
  --profile quick \
  --format json 2>&1 | tee /tmp/k8s_temporal_results.json

echo "=== DONE ==="
echo "Results saved to /tmp/k8s_velocity_results.json and /tmp/k8s_temporal_results.json"
