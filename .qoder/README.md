# Velocity .qoder Directory Structure

This directory contains AI-optimized documentation and specifications for the V.E.L.O.C.I.T.Y. Workflow project, following the pattern established by the Dwarven Stronghold project.

## Structure

```
.qoder/
├── repowiki/                    # Comprehensive documentation wiki
│   ├── en/                      # English documentation
│   │   ├── content/             # Main documentation pages
│   │   │   ├── Getting Started.md
│   │   │   ├── Development Guide.md
│   │   │   └── Architecture Overview.md
│   │   ├── meta/                # Metadata and indexes
│   │   │   └── repowiki-metadata.json
│   │   └── knowledge/           # Knowledge cards (patterns, conventions)
│   │       ├── en/
│   │       │   ├── Write-Ahead Log WAL Persistence.md
│   │       │   ├── PostgreSQL Persistence with Connection Pooling.md
│   │       │   └── gRPC Benchmark Service API.md
│   │       └── _index.yaml      # Knowledge card index
│   └── knowledge/               # Additional knowledge base
└── specs/                       # Feature specifications
    └── Distributed_Workflow_Sharding_and_Horizontal_Scaling.md
```

## Documentation Pages

### Getting Started.md
Introduction to Velocity, project structure, installation, running engines, and benchmarking.

**Contents:**
- Project overview and three flavors (Server, Embedded, Classic)
- Directory structure and core components
- Installation and setup instructions
- Running individual flavors
- Benchmark commands and results summary
- Troubleshooting guide

### Development Guide.md
Comprehensive guide for developers contributing to Velocity.

**Contents:**
- Development environment setup (Rust, Node.js, Docker)
- Project architecture and module organization
- Building and testing procedures
- Code style and conventions (Rust, TypeScript)
- Adding new features and SDK methods
- Protocol buffer development
- SDK development (TypeScript, Python, Go)
- Benchmark development
- Docker development workflow
- Performance profiling techniques
- CI/CD with GitHub Actions

### Architecture Overview.md
Deep dive into Velocity's system architecture and design decisions.

**Contents:**
- System architecture diagram
- Engine flavors comparison (Server, Embedded, Classic)
- Persistence layers (WAL, PostgreSQL, In-Memory)
- Protocol buffers and gRPC API
- SDK architecture (TypeScript, Python, Go)
- Benchmark architecture
- Deployment architecture (Docker, Kubernetes)
- Data flow diagrams
- Performance characteristics

## Knowledge Cards

### Write-Ahead Log WAL Persistence.md
Documents the WAL persistence system used by Velocity Server.

**Key topics:**
- WAL entry format and event types
- Write path and recovery path
- Performance characteristics
- Configuration options
- Developer rules and best practices

### PostgreSQL Persistence with Connection Pooling.md
Documents the PostgreSQL persistence used by Velocity Embedded.

**Key topics:**
- Database schema design
- Connection pooling with deadpool-postgres
- Transaction isolation levels
- Migration system
- Query optimization
- Performance characteristics

### gRPC Benchmark Service API.md
Documents the gRPC API used for workflow operations.

**Key topics:**
- Protocol buffer service definition
- Message types and RPCs
- Server and client implementations
- Code generation for multiple languages
- Error handling patterns
- Performance characteristics

## Specifications

### Distributed_Workflow_Sharding_and_Horizontal_Scaling.md
Specification for implementing horizontal scaling via workflow sharding.

**Key sections:**
- Problem statement and architecture
- Consistent hashing and shard routing
- Cross-shard operations (signals, queries)
- Shard registry and lifecycle management
- WAL replication for fault tolerance
- Configuration and performance targets
- Implementation phases (12 weeks)
- Testing strategy and success criteria

## Metadata

### repowiki-metadata.json
YAML-formatted metadata about the project:
- Project information (name, description, repository)
- Engine flavors with performance metrics
- SDK status and paths
- Competitor benchmarks
- Build configuration (Rust, TypeScript)
- Deployment information (Docker, Kubernetes)
- Benchmarking tool and workloads

## Usage

This documentation is designed to be:
1. **AI-readable** — Structured for AI assistants to understand the codebase
2. **Developer-friendly** — Clear guides for human developers
3. **Comprehensive** — Covers architecture, patterns, and conventions
4. **Maintainable** — Easy to update as the project evolves

### For AI Assistants
When working on Velocity, reference these docs to understand:
- Where to find specific functionality
- What patterns and conventions to follow
- How different components interact
- What performance characteristics to expect

### For Developers
Use these docs to:
- Get started with Velocity development
- Understand the architecture before making changes
- Follow established patterns and conventions
- Learn about persistence and API design
- Plan and implement new features

## Contributing

When adding new features or patterns:
1. Update relevant documentation pages in `repowiki/en/content/`
2. Add knowledge cards for new patterns in `repowiki/knowledge/en/`
3. Create specs for major features in `specs/`
4. Update metadata in `repowiki/en/meta/repowiki-metadata.json`

## License

This documentation is part of the V.E.L.O.C.I.T.Y. Workflow project and follows the same license.
