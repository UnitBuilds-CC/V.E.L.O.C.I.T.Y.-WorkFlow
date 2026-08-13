# VELOCITY-WorkFlow Python SDK Examples

## Prerequisites

1. **Start the VELOCITY-WorkFlow server:**
   ```bash
   cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server
   dotnet run
   ```

2. **Install Python dependencies:**
   ```bash
   cd VELOCITY-WorkFlow/sdk/python
   pip install -r requirements.txt
   ```

3. **Generate gRPC stubs** (if not already done):
   ```bash
   python -m grpc_tools.protoc \
       -I../../src/Velocity.Workflow.Server/Protos \
       --python_out=velocity_sdk --grpc_python_out=velocity_sdk \
       ../../src/Velocity.Workflow.Server/Protos/workflow_service.proto
   ```

## Examples

| File | Description |
|------|-------------|
| `basic_workflow.py` | Simple workflow with signal and query |
| `saga_pattern.py` | Multi-step saga with compensation on failure |
| `cron_schedule.py` | Scheduled (cron) workflow execution |
| `child_workflow.py` | Parent-child workflow orchestration |

## Running an Example

```bash
python examples/basic_workflow.py
```

Each example connects to `localhost:7234` by default. Update the address in the
script if your server runs elsewhere.
