# VELOCITY-WorkFlow TypeScript SDK Examples

## Prerequisites

1. **Start the VELOCITY-WorkFlow server:**
   ```bash
   cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server
   dotnet run
   ```

2. **Install dependencies:**
   ```bash
   cd VELOCITY-WorkFlow/sdk/typescript
   npm install
   ```

## Examples

| File | Description |
|------|-------------|
| `basic-workflow.ts` | Simple workflow with signal and query |
| `saga-pattern.ts` | Multi-step saga with compensation on failure |
| `cron-schedule.ts` | Scheduled (cron) workflow execution |
| `child-workflow.ts` | Parent-child workflow orchestration |

## Running an Example

```bash
npx ts-node examples/basic-workflow.ts
```

Each example connects to `localhost:7234` by default. Update the address in the
script if your server runs elsewhere.
