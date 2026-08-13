# VELOCITY-WorkFlow Troubleshooting

> Common issues, debugging techniques, and frequently asked questions.

---

## Table of Contents

1. [Common Issues](#common-issues)
2. [Debugging Techniques](#debugging-techniques)
3. [FAQ](#faq)

---

## Common Issues

### Server Won't Start

**Symptom:** Server exits immediately with an error.

| Error | Cause | Solution |
|-------|-------|----------|
| `Failed to bind to port 7234` | Port already in use | Change `VELOCITY_GRPC_PORT` or stop the conflicting process |
| `Rust FFI library not found` | Native library not built | Run `cargo build --release` in `velocity-workflow-core/` |
| `Data directory not writable` | Permission denied | Check `VELOCITY_DATA_DIR` permissions |
| `Slab file corrupted` | Previous crash left bad state | Delete corrupted `.slab` files and restart |

### Worker Cannot Connect

**Symptom:** Worker throws `ConnectionError` or times out.

```
ConnectionError: Failed to connect to localhost:7234
```

**Checklist:**
1. Verify the server is running: `curl http://localhost:7233/health`
2. Check the port is correct (default: 7234 for gRPC)
3. Verify no firewall is blocking the port
4. Check TLS settings match between client and server
5. If using Docker, ensure port mapping is correct: `-p 7234:7234`

### Workflow Stuck in Running State

**Symptom:** Workflow never completes despite worker being active.

**Possible causes:**

1. **No worker polling the task queue**: Verify a worker is registered on the correct queue name.
2. **Task handler not registered**: Check that the workflow type name matches exactly.
3. **Worker crashed silently**: Check worker logs for unhandled exceptions.
4. **Total steps mismatch**: If `total_steps` is larger than actual steps, the workflow waits forever.

**Debug steps:**
```bash
# Check workflow status
curl http://localhost:7233/api/workflows/{key}

# Check task queue depth
curl http://localhost:7233/api/queues/{queue_name}

# Check worker registrations
curl http://localhost:7233/api/workers
```

### Signal Not Received

**Symptom:** `signal_workflow` returns success but the workflow doesn't react.

**Causes:**
- Signal name mismatch (check exact string or numeric ID)
- Workflow already completed when signal was sent
- Signal handler not registered in the workflow

**Debug:**
```python
# Verify workflow is still running
desc = client.describe_workflow(workflow_key)
assert desc.status == WorkflowStatus.RUNNING

# Check signal was accepted
ok = client.signal_workflow(workflow_key, "payment-confirmed", b'{}')
assert ok, "Signal was rejected"
```

### High Memory Usage

**Symptom:** Server memory grows unbounded over time.

**Causes:**
- Too many completed workflows not cleaned up
- Large payloads stored in slab overflow arena
- WAL segments not being rotated

**Solutions:**
1. Configure workflow retention policy to auto-purge completed workflows
2. Monitor slab arena size via Prometheus metrics
3. Ensure WAL segment rotation is enabled
4. Check for workflow leaks (workflows started but never completed)

### Slow Task Dispatch

**Symptom:** Tasks sit in the queue for seconds before being picked up.

**Causes:**
- Not enough workers polling the queue
- Workers are slow (blocking I/O in handlers)
- Queue depth limit reached (back-pressure)

**Solutions:**
1. Scale out workers — add more worker processes
2. Optimize task handlers — avoid blocking operations
3. Increase `VELOCITY_MAX_WORKFLOWS` if hitting the limit
4. Check network latency between workers and server

---

## Debugging Techniques

### Enable Debug Logging

Set the log level to `debug` or `trace`:

```bash
# Environment variable
VELOCITY_LOG_LEVEL=debug dotnet run

# Or in configuration file
[server]
log_level = "debug"
```

### Inspect Slab State

Use the HTTP API to inspect raw slab state:

```bash
# Get slab header for a workflow
curl http://localhost:7233/api/slabs/{workflow_key}

# Response includes:
# - version, type_id, ns_id, tq_hash
# - total_steps, current_step
# - bitmask (hex)
# - merkle_root (hex)
# - status
```

### Verify Merkle Root

```bash
# Verify slab integrity
curl http://localhost:7233/api/slabs/{workflow_key}/verify

# Response:
# { "valid": true, "merkle_root": "abc123...", "computed": "abc123..." }
```

### Trace Workflow Execution

Enable distributed tracing with OpenTelemetry:

```bash
VELOCITY_TRACING_ENABLED=true \
VELOCITY_TRACING_ENDPOINT=http://localhost:4317 \
dotnet run
```

View traces in Jaeger or Zipkin to see step-by-step execution timeline.

### WAL Inspection

```bash
# Dump WAL entries for debugging
curl http://localhost:7233/api/wal/dump?segment=0&limit=100

# Check WAL health
curl http://localhost:7233/api/wal/health
```

### SDK-Side Debugging

**Python:**
```python
import logging
logging.basicConfig(level=logging.DEBUG)
# All SDK calls are now logged with full request/response details
```

**TypeScript:**
```typescript
const client = new VelocityClient('localhost:7234', {
  debug: true,  // Enables request/response logging
});
```

**Go:**
```go
client, _ := velocity_sdk.NewClient("localhost:7234", "", velocity_sdk.WithDebug())
```

---

## FAQ

### General

**Q: Do I need a database to run VELOCITY-WorkFlow?**
A: No. VELOCITY uses slab files and WAL segments for persistence. PostgreSQL is optional and only needed for the visibility/query feature.

**Q: Can I run VELOCITY embedded in my application?**
A: Yes. The Rust core can be linked directly via FFI. See the Rust SDK for examples.

**Q: What happens if the server crashes?**
A: On restart, the server mmaps slab files and replays unflushed WAL entries. Recovery is < 0.001 ms regardless of history size. No event replay needed.

**Q: How many concurrent workflows can a single node handle?**
A: A single node handles 100,000+ concurrent workflows. Each workflow uses only 128 bytes of slab memory plus any overflow arena allocations.

### Migration

**Q: Can I migrate from Temporal without downtime?**
A: Yes. Run both systems in parallel, route new workflows to VELOCITY, and let existing Temporal workflows drain. Use the hydration tool for active workflows.

**Q: Does the AST transpiler handle all Temporal patterns?**
A: The transpiler handles the most common patterns: `proxyActivities()`, `sleep()`, `GetVersion()`, signals, and queries. Complex patterns (child workflows, continue-as-new) may need manual adjustment.

### Performance

**Q: Why is VELOCITY faster than Temporal?**
A: Three reasons: (1) O(1) pointer cast instead of O(N) event replay, (2) zero-allocation slab model eliminates GC, (3) VCTP zero-copy UDP transport bypasses kernel network stack.

**Q: What are the minimum hardware requirements?**
A: Development: 4 cores, 8 GB RAM. Production: 16+ cores, 64+ GB RAM, 1 TB NVMe SSD.

**Q: Does VELOCITY support TLS?**
A: Yes. Set `VELOCITY_TLS_ENABLED=true` and provide certificate/key paths.

### Operations

**Q: How do I back up VELOCITY-WorkFlow?**
A: Back up the data directory (`VELOCITY_DATA_DIR`) containing slab files and WAL segments. Use `rsync` for live backups — slab files are append-only.

**Q: How do I scale VELOCITY-WorkFlow?**
A: Add more workers per task queue for horizontal scaling. For server scaling, use replication with CRDT-based convergence.

**Q: Can I query workflow state with SQL?**
A: Yes, when PostgreSQL is configured as the visibility backend. Use the `ListWorkflowExecutions` API with SQL-style filters.
