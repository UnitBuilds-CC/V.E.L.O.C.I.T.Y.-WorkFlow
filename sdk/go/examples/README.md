# VELOCITY-WorkFlow Go SDK Examples

## Prerequisites

1. **Start the VELOCITY-WorkFlow server:**
   ```bash
   cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server
   dotnet run
   ```

2. **Generate gRPC stubs** (if not already done):
   ```bash
   cd VELOCITY-WorkFlow/sdk/go
   protoc -I../../src/Velocity.Workflow.Server/Protos \
       --go_out=velocity_sdk --go-grpc_out=velocity_sdk \
       ../../src/Velocity.Workflow.Server/Protos/workflow_service.proto
   ```

## Examples

| File | Description |
|------|-------------|
| `basic_workflow.go` | Simple workflow with signal and query |
| `saga_pattern.go` | Multi-step saga with compensation on failure |
| `cron_schedule.go` | Scheduled (cron) workflow execution |
| `child_workflow.go` | Parent-child workflow orchestration |

## Running an Example

```bash
go run examples/basic_workflow.go
```

Each example connects to `localhost:50051` by default. Update the address in the
script if your server runs elsewhere.
