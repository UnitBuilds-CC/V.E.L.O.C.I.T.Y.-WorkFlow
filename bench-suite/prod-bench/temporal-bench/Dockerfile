FROM golang:1.22-bookworm AS builder

WORKDIR /app

COPY go.mod go.sum* ./
RUN go mod download 2>/dev/null || true

COPY main.go .
RUN CGO_ENABLED=0 go build -o /temporal-bench .

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /temporal-bench /usr/local/bin/temporal-bench

ENTRYPOINT ["temporal-bench"]
