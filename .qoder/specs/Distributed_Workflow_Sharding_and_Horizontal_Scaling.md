# Distributed Workflow Sharding and Horizontal Scaling

## Summary

Implement horizontal scaling for Velocity Server by adding consistent hashing-based workflow sharding across multiple server instances. Each shard manages its own WAL and workflow state, with a coordination layer for cross-shard operations (signals, queries). Target: linear throughput scaling to 10x single-node performance while maintaining sub-50ms p99 latency.

---

## Problem Statement

Current Velocity Server is single-node only. All workflows execute on one instance with one WAL. This creates bottlenecks:
- **Throughput ceiling** — Single CPU/disk limits ops/s
- **No fault tolerance** — Node failure stops all workflows
- **No horizontal scaling** — Can't add more nodes to handle load

Competitors like Temporal and Restate support multi-node clusters. Velocity needs equivalent scaling capabilities.

---

## Architecture

### Shard Topology

```mermaid
graph TB
    subgraph "Shard Cluster"
        S1[Shard 1<br/>WAL-1<br/>Workflows A-F]
        S2[Shard 2<br/>WAL-2<br/>Workflows G-M]
        S3[Shard 3<br/>WAL-3<br/>Workflows N-T]
        S4[Shard 4<br/>WAL-4<br/>Workflows U-Z]
    end
    
    subgraph "Coordination Layer"
        C1[Shard Router]
        C2[Consistent Hash Ring]
        C3[Shard Registry]
    end
    
    Client --> C1
    C1 --> C2
    C2 --> C3
    C3 --> S1
    C3 --> S2
    C3 --> S3
    C3 --> S4
```

### Consistent Hashing

Workflow assignment uses consistent hashing on `workflow_id`:

```rust
pub struct ShardRouter {
    hash_ring: ConsistentHashRing<ShardId>,
    shard_registry: Arc<RwLock<ShardRegistry>>,
}

impl ShardRouter {
    pub fn route_workflow(&self, workflow_id: &str) -> ShardId {
        let hash = self.hash_ring.hash(workflow_id);
        self.hash_ring.get_node(hash)
    }
}

pub struct ConsistentHashRing<T> {
    nodes: Vec<(u64, T)>,  // (hash, node)
    replicas: usize,        // virtual nodes per physical node
}
```

**Properties:**
- **Even distribution** — Workflows spread evenly across shards
- **Minimal rebalancing** — Adding/removing shards moves ~1/N of workflows
- **Sticky routing** — Same workflow always goes to same shard

### Cross-Shard Operations

Signals and queries may target workflows on different shards:

```mermaid
sequenceDiagram
    participant Client
    participant Router
    participant Shard1
    participant Shard2
    
    Client->>Router: SignalWorkflow(target=shard2)
    Router->>Shard1: Receive signal
    Shard1->>Shard2: Forward signal (gRPC)
    Shard2->>Shard2: Deliver to workflow
    Shard2-->>Shard1: ACK
    Shard1-->>Router: ACK
    Router-->>Client: Success
```

**Implementation:**
```rust
pub struct CrossShardClient {
    connections: HashMap<ShardId, GrpcChannel>,
}

impl CrossShardClient {
    pub async fn forward_signal(
        &self,
        target_shard: ShardId,
        signal: SignalRequest,
    ) -> Result<(), ShardError> {
        let channel = self.connections.get(&target_shard)
            .ok_or(ShardError::NodeUnavailable)?;
        
        channel.signal_workflow(signal).await
    }
}
```

---

## Shard Management

### Shard Registry

Central registry tracks shard health and membership:

```rust
pub struct ShardRegistry {
    shards: HashMap<ShardId, ShardInfo>,
    version: u64,  // Monotonic version for consistency
}

pub struct ShardInfo {
    pub shard_id: ShardId,
    pub address: String,
    pub status: ShardStatus,
    pub last_heartbeat: Instant,
    pub workflow_count: u64,
}

pub enum ShardStatus {
    Active,
    Joining,
    Leaving,
    Failed,
}
```

**Registry storage:**
- **Option 1: PostgreSQL** — Strong consistency, existing infrastructure
- **Option 2: etcd** — Distributed consensus, purpose-built for coordination
- **Option 3: In-memory + gossip** — Fast, but requires careful failure handling

**Recommendation:** Start with PostgreSQL for simplicity, migrate to etcd if needed.

### Shard Lifecycle

**Joining:**
1. New shard starts, registers in registry
2. Hash ring recalculated
3. Existing shards rebalance workflows to new shard
4. New shard becomes Active

**Leaving:**
1. Shard marked as Leaving
2. Workflows migrated to remaining shards
3. WAL replicated to new shards
4. Shard removed from registry

**Failure:**
1. Heartbeat timeout (30s default)
2. Shard marked as Failed
3. Workflows reassigned to other shards
4. WAL replayed on new shard to recover state

---

## WAL Replication

Each shard has its own WAL. For fault tolerance, replicate WAL entries across shards:

```rust
pub struct ReplicatedWal {
    local_wal: WalWriter,
    replicas: Vec<ShardId>,
    ack_count: usize,  // Number of ACKs required (quorum)
}

impl ReplicatedWal {
    pub async fn write_entry(&mut self, entry: WalEntry) -> Result<(), WalError> {
        // Write to local WAL
        self.local_wal.append(entry.clone()).await?;
        
        // Replicate to replicas
        let mut acks = 0;
        for replica in &self.replicas {
            if self.replicate_to(*replica, entry.clone()).await.is_ok() {
                acks += 1;
            }
        }
        
        // Check quorum
        if acks < self.ack_count {
            return Err(WalError::InsufficientAcks);
        }
        
        Ok(())
    }
}
```

**Replication modes:**
- **Synchronous** — Wait for all replicas (strong consistency, higher latency)
- **Asynchronous** — Fire-and-forget (eventual consistency, lower latency)
- **Quorum** — Wait for N/2+1 replicas (balanced)

**Recommendation:** Quorum mode with `ack_count = 2` for 3-replica setup.

---

## Configuration

### Shard Configuration

```yaml
# velocity-shard.yaml
shard:
  id: "shard-1"
  address: "0.0.0.0:7234"
  
cluster:
  registry: "postgres://velocity:velocity@localhost/velocity"
  heartbeat_interval: "10s"
  heartbeat_timeout: "30s"
  
replication:
  enabled: true
  replicas: ["shard-2", "shard-3"]
  ack_count: 2  # Quorum
  mode: "quorum"  # synchronous, asynchronous, quorum
  
hashing:
  replicas: 100  # Virtual nodes per shard
  algorithm: "murmur3"  # Hash function
```

### Client Configuration

```typescript
// velocity-sdk-typescript
const client = new VelocityClient({
  addresses: [
    "http://shard-1:7234",
    "http://shard-2:7234",
    "http://shard-3:7234",
  ],
  routing: "client-side",  // client-side or server-side
  retryPolicy: {
    maxRetries: 3,
    backoff: "exponential",
  },
});
```

---

## Performance Targets

| Metric | Single Node | 4-Shard Cluster | Target |
|--------|-------------|-----------------|--------|
| Throughput | 43.6 ops/s | 174+ ops/s | 4x scaling |
| p50 Latency | 180ms | 180ms | No regression |
| p99 Latency | 332ms | 400ms | <500ms |
| Memory | 98 MiB | 392 MiB | Linear |
| Recovery Time | 2s | 5s | <10s |

**Scaling efficiency:** Target 80%+ efficiency (4 shards should achieve 3.2x+ throughput).

---

## Implementation Phases

### Phase 1: Shard Router (Week 1-2)
- Implement consistent hash ring
- Add shard routing to gRPC server
- Route workflows based on `workflow_id`
- Single-shard mode (no cross-shard ops yet)

### Phase 2: Shard Registry (Week 3-4)
- Implement PostgreSQL-backed registry
- Add heartbeat mechanism
- Implement shard join/leave
- Basic health monitoring

### Phase 3: Cross-Shard Operations (Week 5-6)
- Implement cross-shard signal forwarding
- Implement cross-shard queries
- Add retry logic for transient failures
- Test with 2-shard cluster

### Phase 4: WAL Replication (Week 7-8)
- Implement WAL replication protocol
- Add quorum writes
- Implement WAL replay on shard failure
- Test fault tolerance

### Phase 5: Rebalancing (Week 9-10)
- Implement workflow migration between shards
- Add rebalancing on shard join/leave
- Minimize downtime during rebalancing
- Load testing with 4-shard cluster

### Phase 6: Production Hardening (Week 11-12)
- Add metrics and monitoring
- Implement graceful shutdown
- Add circuit breakers
- Documentation and deployment guides

---

## Files to Create/Modify

| File | Action |
|------|--------|
| `src/sharding/mod.rs` | CREATE — Shard router and hash ring |
| `src/sharding/registry.rs` | CREATE — Shard registry client |
| `src/sharding/replication.rs` | CREATE — WAL replication |
| `velocity-workflow-server/src/shard.rs` | CREATE — Shard-aware server |
| `velocity-sdk-typescript/src/sharding.ts` | CREATE — Client-side routing |
| `proto/velocity/v1/sharding.proto` | CREATE — Shard coordination RPCs |
| `migrations/003_shard_registry.sql` | CREATE — Shard registry schema |
| `deploy/k8s/shard-statefulset.yaml` | CREATE — Kubernetes deployment |
| `velocity-workflow-server/src/main.rs` | MODIFY — Add shard mode |
| `bench-suite/prod-bench/src/main.rs` | MODIFY — Multi-shard benchmarks |

---

## Key Architectural Decisions

1. **Consistent hashing** — Minimizes rebalancing when shards join/leave
2. **Quorum replication** — Balances consistency and availability
3. **PostgreSQL registry** — Leverages existing infrastructure, strong consistency
4. **Client-side routing** — Reduces server load, enables smart retries
5. **WAL per shard** — Independent persistence, no single WAL bottleneck
6. **Cross-shard forwarding** — Transparent to clients, handled by server
7. **Gradual rollout** — Single-shard first, then multi-shard, then replication

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Cross-shard latency | High p99 latency | Co-locate shards in same datacenter, use connection pooling |
| Split brain | Data inconsistency | Use PostgreSQL for registry, implement fencing tokens |
| Rebalancing storms | Performance degradation | Rate-limit migrations, schedule during low traffic |
| WAL replication lag | Data loss on failure | Quorum writes, monitor replication lag |
| Hash ring skew | Uneven load | Use virtual nodes (100+ per shard) |

---

## Testing Strategy

**Unit tests:**
- Hash ring distribution
- Shard routing logic
- Registry operations

**Integration tests:**
- 2-shard cluster with cross-shard signals
- Shard failure and recovery
- WAL replication and replay

**Load tests:**
- 4-shard cluster, 10k workflows/s
- Measure scaling efficiency
- Test rebalancing under load

**Chaos tests:**
- Kill shards randomly
- Network partitions
- Disk full scenarios

---

## Success Criteria

- [ ] 4-shard cluster achieves 3.2x+ throughput (80% efficiency)
- [ ] p99 latency < 500ms with cross-shard operations
- [ ] Zero data loss during shard failure (with quorum replication)
- [ ] Rebalancing completes in < 60s for 10k workflows
- [ ] No single point of failure (registry replicated)
- [ ] Production deployment on Kubernetes with 3+ shards
