# VELOCITY-WorkFlow Protocol Buffers

gRPC service definitions for the VELOCITY-WorkFlow workflow engine.

## Structure

```
proto/
├── velocity/v1/
│   ├── common.proto           # Shared types (Payload, RetryPolicy, enums)
│   ├── messages.proto         # Domain messages (WorkflowExecution, TaskQueue, etc.)
│   ├── workflow_service.proto # Primary WorkflowService RPCs
│   ├── errordetails.proto     # Structured gRPC error details
│   ├── health_service.proto   # Health check service (gRPC health protocol)
│   └── admin_service.proto    # Admin operations (namespace CRUD, cluster info)
├── buf.yaml                   # Buf build configuration
├── buf.gen.yaml               # Buf code generation config
└── README.md                  # This file
```

## Services

| Service | Description |
|---------|-------------|
| `WorkflowService` | Primary external API — workflow lifecycle, task dispatch, visibility, namespaces |
| `HealthService` | gRPC health checking protocol for load balancers and monitoring |
| `AdminService` | Administrative operations — cluster info, dynamic config, shard management |

## Code Generation

This project uses [Buf](https://buf.build/) for protobuf linting, breaking change detection, and code generation.

### Prerequisites

```bash
# Install buf
go install github.com/bufbuild/buf/cmd/buf@latest

# Or via Homebrew
brew install bufbuild/buf/buf
```

### Generate code for all SDKs

```bash
cd proto
buf generate
```

### Lint proto files

```bash
buf lint
```

### Check for breaking changes

```bash
buf breaking --against .git#branch=main
```

## Language Options

All proto files include language-specific options:

- `go_package` → `go.velocity.dev/api/velocity/v1`
- `java_package` → `dev.velocity.api.v1`
- `java_multiple_files` → `true`

## Usage

### Go

```go
import v1 "go.velocity.dev/api/velocity/v1"
```

### Java

```java
import dev.velocity.api.v1.*;
```

### Python

```python
from velocity_sdk.gen.velocity.v1 import workflow_service_pb2_grpc
```

## Contributing

When modifying proto files:

1. Run `buf lint` to ensure style compliance
2. Run `buf breaking` against the previous version to detect breaking changes
3. Regenerate SDK stubs with `buf generate`
4. Update SDK-specific wrapper code if message shapes changed
