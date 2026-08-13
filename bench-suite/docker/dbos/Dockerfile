# DBOS Benchmark Service — FastAPI + PostgreSQL durable execution
#
# Runs the DBOS benchmark service that exposes HTTP endpoints for
# the benchmark client.  Each workflow uses @DBOS.workflow() and
# @DBOS.step() decorators for real durable execution on PostgreSQL.
#
# Build context: repository root (velocity-workflow/)

FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY cloud-bench/production/dbos/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY cloud-bench/production/dbos/service.py .

# Default: connect to the dbos-postgres service in docker-compose
ENV DBOS_DATABASE_URL=postgresql://dbos:dbos_bench@dbos-postgres:5432/dbos_bench
ENV DBOS_HTTP_PORT=8080

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=5s --start-period=15s --retries=5 \
    CMD curl -f http://localhost:8080/health || exit 1

CMD ["python", "service.py", "server"]
