# ── Build stage ────────────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /workspace

# Cache dependencies first.
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build.
COPY main.go main.go
COPY api/ api/
COPY controllers/ controllers/

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w" \
    -o /velocity-operator .

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM gcr.io/distroless/static:nonroot

LABEL org.opencontainers.image.title="velocity-operator" \
      org.opencontainers.image.description="Velocity Workflow Kubernetes Operator" \
      org.opencontainers.image.vendor="UnitBuilds"

WORKDIR /
COPY --from=builder /velocity-operator .

USER 65532:65532

ENTRYPOINT ["/velocity-operator"]
