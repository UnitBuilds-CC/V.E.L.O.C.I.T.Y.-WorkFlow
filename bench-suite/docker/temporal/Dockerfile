# Temporal Benchmark Service — FastAPI + Temporal Worker
#
# Runs a FastAPI HTTP server that triggers Temporal workflows, plus
# a Temporal Worker that executes them.  The Temporal *server* itself
# uses the official temporalio/auto-setup image (see docker-compose.yml).
#
# Build context: repository root (velocity-workflow/)

FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY cloud-bench/production/temporal/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY cloud-bench/production/temporal/service.py   .
COPY cloud-bench/production/temporal/workflows.py .

# Default: connect to the temporal-server service in docker-compose
ENV TEMPORAL_ADDRESS=temporal-server:7233
ENV TEMPORAL_NAMESPACE=default
ENV TEMPORAL_HTTP_PORT=8080
ENV TEMPORAL_TASK_QUEUE=bench-queue

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=5s --start-period=20s --retries=5 \
    CMD curl -f http://localhost:8080/health || exit 1

CMD ["python", "service.py", "server"]
