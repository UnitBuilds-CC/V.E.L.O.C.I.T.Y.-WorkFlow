# VCTP Protocol Transport

## Overview

VCTP (Velocity Transfer Protocol) is a zero-copy UDP-based RPC protocol for high-performance workflow operations. It provides binary wire format with CRC32 integrity verification, retransmission tracking, AIMD congestion control, and sequence reorder buffering.

## Wire Format

### Packet Header (28 bytes, little-endian)

| Field | Offset | Size | Description |
|-------|--------|------|-------------|
| `magic` | 0 | 4 bytes | VCTP magic bytes `0x50544356` ("VCTP") |
| `sequence_number` | 4 | 8 bytes | Monotonic packet sequence ID |
| `workflow_id` | 12 | 8 bytes | Workflow ID or method identifier |
| `slab_offset` | 20 | 4 bytes | Slab offset or fragment metadata |
| `payload_length` | 24 | 4 bytes | Length of payload in bytes |

### Full Packet Layout

```
[magic:4][sequence:8][workflow_id:8][slab_offset:4][payload_length:4]  ← 28-byte header
[payload: payload_length bytes]                                         ← JSON or binary payload
[crc32:4]                                                               ← CRC32 checksum
```

- **Max payload:** 65,479 bytes (65,535 UDP limit − 28 header − 4 CRC − 8 IP − 20 UDP)
- **Checksum:** CRC32 over header + payload, verified on receive
- **Default port:** 9090

## Transport Layer

### UDP Socket

```rust
// velocity-workflow-engine/src/vctp_transport.rs
let socket = UdpSocket::bind(&config.bind_addr)?;
socket.set_nonblocking(true)?;
```

- Non-blocking I/O for high-throughput packet processing
- Per-destination retransmission tracking via `seq_dest: HashMap<u64, SocketAddr>`
- Receive buffer for batching (`recv_buf: Mutex<Vec<u8>>`)

### Send Path

1. Allocate sequence number (`AtomicU64::fetch_add`)
2. Build `VctpPacketHeader` with sequence, workflow_id, slab_offset, payload_length
3. Optional XOR encryption if cipher is configured
4. Encode header + payload + CRC32 → `Vec<u8>`
5. `socket.send_to(&bytes, addr)` — real OS UDP send
6. Track in `VctpRetransmitTracker` for retransmission

### Receive Path

1. `socket.recv_from(&mut buf)` — real OS UDP receive
2. Check for ACK packets (separate magic `VCTP_ACK_MAGIC`)
3. Parse `VctpPacket::from_bytes()` with CRC32 verification
4. Optional decryption if cipher is configured
5. Feed into `ReorderBuffer` for in-order delivery

## Retransmission & Congestion Control

### Retransmit Tracker

Tracks sent packets by sequence number. On timeout, retransmits to the original destination address (from `seq_dest` map).

### AIMD Congestion Control

`AimdController` computes pacing rate based on network conditions:
- Additive increase on successful ACKs
- Multiplicative decrease on timeouts
- Pacing rate in Mbps for send throttling

## Reorder Buffer

BTreeMap-based in-order delivery for reliable processing over unreliable UDP:

```rust
// velocity-workflow-engine/src/vctp_rpc.rs
struct ReorderBuffer {
    next_expected: u64,
    pending: BTreeMap<u64, (Vec<u8>, SocketAddr)>,
    max_depth: usize,
}
```

- Inserts packets by sequence number
- Drains contiguous run from `next_expected`
- Evicts oldest packets when buffer is full
- Configurable `max_depth` (0 = disabled, return immediately)

## Fragmentation

Large payloads exceeding VCTP max are fragmented:
- `slab_offset` encodes fragment index (high 16 bits) and total fragments (low 16 bits)
- Receiver reassembles in `frag_buf: HashMap<SocketAddr, HashMap<u16, Vec<u8>>>`
- Complete payload dispatched after all fragments received

## Source Files

| File | Role |
|------|------|
| `velocity-workflow-core/src/vctp.rs` | Packet types, header encode/decode, CRC32, AIMD, cipher |
| `velocity-workflow-engine/src/vctp_transport.rs` | UDP socket, send/recv, retransmission, congestion |
| `velocity-workflow-engine/src/vctp_rpc.rs` | Reorder buffer, fragment reassembly, RPC dispatch |
| `proto/vctp_service.json` | Machine-readable protocol definition schema |

## Configuration

```yaml
# deploy/helm/velocity/values.yaml
vctp:
  enabled: false
  port: 9090
  drainTimeoutSeconds: 30
  security:
    authRequired: false
    jwtSecret: ""
    apiKeys: []
    rateLimitRps: 0
    rateLimitBurst: 0
  circuitBreaker:
    maxInflight: 10000
```

## Authenticated Encryption

### HMAC-SHA256 Packet Authentication

Every VCTP packet can be authenticated using HMAC-SHA256 computed over the payload and sequence number:

```rust
// velocity-workflow-core/src/vctp.rs — VctpCipher
pub fn compute_mac(&self, data: &[u8], sequence: u64) -> [u8; 32] {
    // Inner: H(key || sequence || data)
    // Outer: H(key || inner_hash)
    // Returns 32-byte MAC tag
}

pub fn verify_mac(&self, data: &[u8], sequence: u64, expected_mac: &[u8; 32]) -> bool {
    // Constant-time comparison to prevent timing attacks
}
```

- **Key derivation:** SHA-256 hash of passphrase → 32-byte key
- **MAC computation:** Double-pass HMAC (inner hash + outer hash) over key + sequence + payload
- **Verification:** Constant-time byte comparison prevents timing side-channels
- **Performance:** ≥100,000 MAC ops/s for all payload sizes (64B to 4KB)

### Sliding Window Replay Protection

`VctpReplayWindow` tracks recently seen sequence numbers using a 64-bit bitmask:

```rust
pub struct VctpReplayWindow {
    highest_seq: u64,       // Highest sequence number seen
    window_mask: u64,       // Bitmask: bit i = (highest_seq - i) was seen
    window_size: u64,       // Window depth (default 64)
}
```

- **Accept:** New high watermark → shift window forward, mark as seen
- **Accept:** Fresh packet within window → set bit
- **Reject:** Duplicate (bit already set) → replay detected
- **Reject:** Too old (offset ≥ window_size) → outside window
- **Performance:** ≥10,000,000 checks/s (pure bitmask operations, no allocations)

## Cross-Network Benchmark

Simulates multi-zone traffic with artificial latency (0-300µs per zone):

| Metric | Value |
|--------|-------|
| Zones | 4 (0µs, 100µs, 200µs, 300µs simulated latency) |
| Clients per zone | 25 |
| Requests per client | 20 |
| Total expected | 2,000 packets |
| Delivery threshold | >85% |
| Throughput threshold | ≥1,000 ops/s |

## Performance

Benchmarked with real UDP sockets, real workflow creation (3-5 steps), WAL persistence, and DB adapter persistence:

| Benchmark | Throughput | Latency | Threshold |
|-----------|-----------|--------|----------|
| Full VCTP dispatch (UDP + WAL + DB) | 9,052 ops/s | 110µs/op | ≥5,000 ops/s |
| Start-workflow 5 steps (UDP + WAL + DB) | 7,375 ops/s | 135µs/op | ≥5,000 ops/s |
| WAL durability write | 7,962 wf/s | 125µs/wf | — |
| WAL crash recovery | 43,113 wf/s | 23µs/wf | — |
| E2E round-trip p99 | — | <5,000µs | <5ms |
| HMAC-SHA256 (all sizes) | ≥100,000 ops/s | varies | ≥100K ops/s |
| Replay window check | ≥10M ops/s | ~100ns/op | ≥10M ops/s |
| Cross-network (4 zones) | ≥1,000 ops/s | varies | ≥1,000 ops/s |
| Concurrent stress (100 clients) | ≥2,000 ops/s | varies | ≥2,000 ops/s |
