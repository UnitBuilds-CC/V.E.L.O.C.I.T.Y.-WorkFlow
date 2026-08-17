# Restate Benchmark Service — Node.js durable journal handlers
#
# Runs the Restate benchmark service that registers with the Restate
# server and handles benchmark workload invocations.  Every state
# mutation is journaled to Restate's durable log.
#
# NOTE: The Restate *server* itself uses the official image
#       restatedev/restate:latest (see docker-compose.yml).
#       This Dockerfile is only for the user-space service.
#
# Build context: repository root (velocity-workflow/)

FROM node:22-slim

RUN apt-get update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY cloud-bench/production/restate/service.js .
COPY bench-suite/docker/restate/entrypoint.sh .
RUN chmod +x entrypoint.sh

RUN npm init -y > /dev/null 2>&1 && \
    npm install @restatedev/restate-sdk

EXPOSE 9080

CMD ["./entrypoint.sh"]
