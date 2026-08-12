# V.E.L.O.C.I.T.Y.-WorkFlow TypeScript SDK

TypeScript SDK for V.E.L.O.C.I.T.Y.-WorkFlow - a hardware-native zero-allocation durable execution engine and Temporal alternative.

## Installation

```bash
npm install @velocity-workflow/sdk
```

## Quick Start

### Define a Workflow

```typescript
import { defineWorkflow, WorkflowContext, WorkflowHelpers } from '@velocity-workflow/sdk';

defineWorkflow('greeting-workflow', async (ctx: WorkflowContext, input: { name: string }) => {
  // Execute an activity
  const greeting = await WorkflowHelpers.executeActivity<string, string>({
    taskQueue: 'greeting-queue',
    activityType: 'greet-activity',
    input: input.name,
  });

  return greeting;
});
```

### Define an Activity

```typescript
import { defineActivity, ActivityContext } from '@velocity-workflow/sdk';

defineActivity('greet-activity', async (ctx: ActivityContext, name: string) => {
  return `Hello, ${name}!`;
});
```

### Start a Worker

```typescript
import { Worker } from '@velocity-workflow/sdk';

async function main() {
  const worker = await Worker.create({
    namespace: 'default',
    taskQueue: 'greeting-queue',
    workflowsPath: __dirname + '/workflows',
    activities: {
      'greet-activity': async (ctx, name) => `Hello, ${name}!`,
    },
  });

  await worker.run();
}

main().catch(console.error);
```

### Start a Workflow

```typescript
import { Client } from '@velocity-workflow/sdk';

async function main() {
  const client = new Client({
    connection: { address: 'localhost:7233' },
    namespace: 'default',
  });

  const handle = await client.start({
    workflowId: 'greeting-1',
    workflowType: 'greeting-workflow',
    taskQueue: 'greeting-queue',
    input: { name: 'World' },
  });

  console.log(`Started workflow ${handle.workflowId}`);

  // Wait for result
  const result = await handle.result();
  console.log(`Workflow result: ${result}`);
}

main().catch(console.error);
```

## Features

- **Durable Execution**: Workflows survive process crashes and server restarts
- **Activity Support**: Execute unreliable code in activities with automatic retries
- **Timers**: Sleep and schedule future work
- **Signals**: Send external events to running workflows
- **Queries**: Query workflow state without affecting execution
- **Child Workflows**: Compose workflows hierarchically
- **Search Attributes**: Index workflows for visibility
- **Memo**: Store arbitrary data with workflows

## API Reference

### Client

- `start(options)` - Start a new workflow
- `execute(options)` - Start workflow and wait for result
- `signal(workflowId, options)` - Signal a running workflow
- `query(workflowId, options)` - Query a workflow
- `terminate(workflowId, reason?)` - Terminate a workflow
- `cancel(workflowId)` - Cancel a workflow
- `describe(workflowId)` - Get workflow details
- `getHistory(workflowId)` - Get workflow history

### Worker

- `start()` - Start the worker
- `stop()` - Stop the worker
- `isRunning()` - Check if worker is running

### Workflow Helpers

- `executeActivity(options)` - Execute an activity
- `sleep(duration)` - Sleep for a duration
- `executeChildWorkflow(options)` - Start a child workflow
- `getInfo()` - Get current workflow context

### Activity Helpers

- `heartbeat(details?)` - Record a heartbeat
- `getInfo()` - Get current activity context

## Examples

See the `examples/` directory for complete examples:

- `hello-world.ts` - Simple greeting workflow
- `timer.ts` - Timer and sleep example
- `signal.ts` - Signal handling example
- `child-workflow.ts` - Child workflow composition

## Development

```bash
# Install dependencies
npm install

# Build
npm run build

# Test
npm test

# Lint
npm run lint
```

## License

MIT
