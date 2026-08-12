# VELOCITY-WorkFlow Open-Core Commercial Model

## Overview

VELOCITY-WorkFlow follows an **open-core** licensing model that provides a generous,
free Community Edition for individual developers and small teams, while offering
Enterprise Edition features for organizations that need advanced capabilities,
compliance, and support.

---

## Community Edition (Free)

**License:** Apache 2.0  
**Target:** Individual developers, startups, open-source projects, small teams

### Included Features

| Category | Features |
|----------|----------|
| **Core Engine** | Slab Engine, Merkle Verification, Bitmask256, Tier-2 Arena, WAL |
| **Workflow Primitives** | Signal/Query, Timer Engine, Task Queue, Activity Scheduling, Child Workflows, Cron |
| **Orchestration** | Saga, Namespace, Visibility, Event History, Worker Versioning |
| **Reliability** | Rate Limiter, Heartbeat, Dynamic Config, Memo, Patches, Schedules |
| **Infrastructure** | Partition, Sharding, Nexus, Replication Transport, Raft Consensus |
| **Storage** | Archival + Cold Storage, Payload Codec, History Compaction (LSM) |
| **Security** | Auth (basic), Deterministic Primitives |
| **SDKs** | C# FFI Bridge, Go SDK, Python SDK, TypeScript SDK, Java SDK |
| **Tooling** | Roslyn AST Transpiler, Source Generators (VEL0001-0003), Slab Visualizer |
| **Hardware** | HAL, Merkle ECC Self-Healing, Hardware Traits |
| **Operations** | Metrics, Cluster Management, Network Replication (TCP/UDP), Chaos Endurance |
| **Migration** | temporal2velocity Migration Tool |

### Community Support
- GitHub Issues (best-effort)
- Community Discord
- Documentation (README, walkthroughs)

---

## Enterprise Edition (Paid)

**License:** Commercial (per-seat or per-node)  
**Target:** Medium to large organizations, regulated industries, mission-critical deployments

### Additional Features

| Category | Enterprise Features |
|----------|---------------------|
| **Advanced Security** | SSO/SAML integration, RBAC with fine-grained permissions, audit logging, encryption-at-rest key management, TEE enclave support |
| **High Availability** | Multi-region active-active replication, automated failover with SLA guarantees, geo-distributed Raft consensus |
| **Compliance** | SOC 2 Type II compliance tooling, HIPAA-compliant deployment templates, GDPR data residency controls, FedRAMP deployment guide |
| **Performance** | Binary hot-swapping (JIT patching), Smart NIC offload integration, NUMA-aware memory allocation, Custom slab allocator tuning |
| **Observability** | Distributed tracing integration (OpenTelemetry), Custom metrics export (Prometheus/Grafana), Real-time workflow inspector, Performance profiling dashboard |
| **Support** | 24/7 dedicated support, SLA-backed response times, Dedicated solutions engineer, Priority bug fixes |
| **Operations** | Automated backup/restore, Rolling upgrade orchestration, Capacity planning tools, Multi-tenant isolation |
| **Integration** | Custom connector development, Professional services, Training and certification |

### Enterprise Tiers

| Tier | Nodes | Support | Features |
|------|-------|---------|----------|
| **Starter** | Up to 5 | Business hours | Core Enterprise features |
| **Professional** | Up to 50 | 24/7 | All Enterprise + priority support |
| **Unlimited** | Unlimited | 24/7 + dedicated | All features + custom development |

---

## Feature Boundary Matrix

```
Feature                          Community    Enterprise
─────────────────────────────────────────────────────────
Core workflow engine               ✓            ✓
Signal/Query/Timer                 ✓            ✓
Saga orchestration                 ✓            ✓
Replication (TCP/UDP)              ✓            ✓
Raft consensus                     ✓            ✓
Search attribute indexing          ✓            ✓
Chaos engineering                  ✓            ✓
Slab visualizer                    ✓            ✓
Binary hot-swapping                ✗            ✓
SSO/SAML auth                      ✗            ✓
Multi-region replication           ✗            ✓
SOC 2 / HIPAA tooling             ✗            ✓
Smart NIC offload                  ✗            ✓
Distributed tracing                ✗            ✓
24/7 support                       ✗            ✓
```

---

## Revenue Model

1. **Enterprise Licenses** — Annual subscription per node or per seat
2. **Professional Services** — Custom integration, migration, training
3. **Cloud Hosting** — Managed VELOCITY-WorkFlow as a Service (future)
4. **Certification** — VELOCITY-WorkFlow Certified Developer program

---

## Contribution Policy

- All contributions to Community Edition are welcome under Apache 2.0
- Enterprise features are developed in a separate private repository
- Contributors sign a CLA (Contributor License Agreement)
- Bug fixes to Community are backported to Enterprise

---

## Migration Path

Organizations can start with the Community Edition and upgrade to Enterprise
at any time. The migration is seamless — no data migration or restart required.
Enterprise features are activated via a license key that unlocks additional
modules in the existing binary.
