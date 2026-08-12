# Velocity Migration Toolkit

Comprehensive migration toolkit for converting workflows between Velocity SDK flavors.

## Supported SDK Flavors

- **Classic** (`classic`): Temporal-compatible durable workflow SDK
- **Runtime** (`runtime`): Restate-compatible virtual objects and services SDK
- **Embedded** (`embedded`): DBOS-compatible library-style SDK
- **Python Runtime** (`python-runtime`): Python Restate-compatible SDK

## Supported Migrations

All 12 migration paths are supported:

```
Classic → Runtime          Runtime → Classic
Classic → Embedded         Runtime → Embedded
Classic → Python Runtime   Runtime → Python Runtime

Embedded → Classic         Python Runtime → Classic
Embedded → Runtime         Python Runtime → Runtime
Embedded → Python Runtime  Python Runtime → Embedded
```

## Installation

```bash
npm install @velocity-workflow/migration-toolkit
```

## Usage

### Command Line

```bash
# Migrate from Classic to Runtime
velocity-migrate workflow.ts --from classic --to runtime

# Migrate with output file
velocity-migrate workflow.ts --from runtime --to embedded --output migrated.ts

# Migrate Python to Classic
velocity-migrate workflow.py --from python-runtime --to classic
```

### Programmatic API

```typescript
import { migrate } from '@velocity-workflow/migration-toolkit';

const sourceCode = `
import { Workflow, Activity } from '@velocity-workflow/classic';

class OrderWorkflow extends Workflow {
  async execute(orderId: string) {
    const charge = await this.executeActivity('chargeActivity', orderId);
    return { charge };
  }
}
`;

const migratedCode = migrate(sourceCode, {
  source: 'classic',
  target: 'runtime',
});

console.log(migratedCode);
```

## Migration Examples

### Example 1: Classic → Runtime

**Source (Classic SDK):**
```typescript
import { Workflow, Activity, Worker } from '@velocity-workflow/classic';

class PaymentWorkflow extends Workflow {
  async execute(orderId: string, amount: number) {
    const charge = await this.executeActivity('ChargeActivity', orderId, amount);
    const receipt = await this.executeActivity('ReceiptActivity', orderId);
    return { charge, receipt };
  }
}

class ChargeActivity extends Activity {
  async execute(orderId: string, amount: number) {
    // Process payment
    return { transactionId: 'tx-123', status: 'charged' };
  }
}
```

**Target (Runtime SDK):**
```typescript
import { VirtualObject, Service, RuntimeServer } from '@velocity-workflow/runtime';

const PaymentWorkflow = new VirtualObject('PaymentWorkflow');
PaymentWorkflow.addHandler('execute', async (ctx, orderId: string, amount: number) => {
  // Migrated from classic
  const charge = await ctx.invoke('ChargeActivity', 'execute', orderId, amount);
  const receipt = await ctx.invoke('ReceiptActivity', 'execute', orderId);
  return { charge, receipt };
});

const ChargeActivity = new Service('ChargeActivity');
ChargeActivity.addHandler('execute', async (ctx, orderId: string, amount: number) => {
  // Migrated from classic
  // Process payment
  return { transactionId: 'tx-123', status: 'charged' };
});
```

### Example 2: Runtime → Embedded

**Source (Runtime SDK):**
```typescript
import { VirtualObject } from '@velocity-workflow/runtime';

const Counter = new VirtualObject('Counter');
Counter.addHandler('increment', async (ctx, key: string) => {
  const count = (await ctx.get('count')) || 0;
  await ctx.set('count', count + 1);
  return count + 1;
});
```

**Target (Embedded SDK):**
```typescript
import { Durable, DurableContext, VelocityEmbedded } from '@velocity-workflow/embedded';

@Durable()
export class Counter {
  async increment(ctx: DurableContext, key: string) {
    // Migrated from runtime
    const count = ctx.getState<number>('count') || 0;
    ctx.setState('count', count + 1);
    return count + 1;
  }
}
```

### Example 3: Embedded → Python Runtime

**Source (Embedded SDK):**
```typescript
import { Durable, DurableContext } from '@velocity-workflow/embedded';

@Durable()
export class OrderProcessor {
  async process(ctx: DurableContext, orderId: string) {
    const status = ctx.getState<string>('status') || 'pending';
    ctx.setState('status', 'processing');
    
    const result = await ctx.invoke('PaymentService', 'charge', orderId);
    
    ctx.setState('status', 'completed');
    return result;
  }
}
```

**Target (Python Runtime SDK):**
```python
from velocity_runtime import VirtualObject, Context

class OrderProcessor(VirtualObject):
    def __init__(self):
        super().__init__('OrderProcessor')
    
    async def process(self, ctx: Context, orderId: str):
        # Migrated from embedded
        status = await ctx.get('status') or 'pending'
        await ctx.set('status', 'processing')
        
        result = await ctx.invoke('PaymentService', 'charge', orderId)
        
        await ctx.set('status', 'completed')
        return result
```

### Example 4: Python Runtime → Classic

**Source (Python Runtime SDK):**
```python
from velocity_runtime import Service, Context

class EmailService(Service):
    def __init__(self):
        super().__init__('EmailService')
    
    async def send(self, ctx: Context, to: str, subject: str, body: str):
        # Send email
        return {'sent': True, 'to': to}
```

**Target (Classic SDK):**
```typescript
import { Activity } from '@velocity-workflow/classic';

export class EmailService extends Activity {
  async execute(to: string, subject: string, body: string) {
    // Migrated from python-runtime
    // Send email
    return { sent: true, to };
  }
}
```

## Architecture

The migration toolkit uses an **Intermediate Representation (IR)** approach:

1. **Parser**: Converts source SDK code to IR
2. **IR**: Language-agnostic workflow representation
3. **Generator**: Converts IR to target SDK code

This allows N×M conversions with only N+M converters instead of N×M.

```
Source SDK → Parser → IR → Generator → Target SDK
```

## Migration Mapping

### Context Operations

| Classic | Runtime | Embedded | Python Runtime |
|---------|---------|----------|----------------|
| `this.executeActivity()` | `ctx.invoke()` | `ctx.invoke()` | `ctx.invoke()` |
| `this.waitForSignal()` | `ctx.recv()` | `ctx.recv()` | `ctx.recv()` |
| `this.signal()` | `ctx.send()` | `ctx.send()` | `ctx.send()` |
| N/A | `ctx.get()` / `ctx.set()` | `ctx.getState()` / `ctx.setState()` | `ctx.get()` / `ctx.set()` |
| `this.sleep()` | `ctx.sleep()` | `ctx.sleep()` | `ctx.sleep()` |
| `this.heartbeat()` | N/A | N/A | N/A |

### Entity Types

| Classic | Runtime | Embedded | Python Runtime |
|---------|---------|----------|----------------|
| `Workflow` | `VirtualObject` | `@Durable class` | `VirtualObject` |
| `Activity` | `Service` | `@Durable class` | `Service` |
| N/A | `Workflow` | `@Durable class` | `Workflow` |

## Limitations

- **Manual Review Required**: Migrated code should be reviewed and tested
- **Complex Logic**: Complex control flow may need manual adjustment
- **Type Mismatches**: Some type conversions may need manual fixes
- **SDK-Specific Features**: Features unique to one SDK may not have direct equivalents

## Testing Migrations

After migration, always:

1. **Review the generated code** for correctness
2. **Run type checking** (`tsc --noEmit`)
3. **Write/update tests** for migrated workflows
4. **Test crash recovery** to ensure durability works
5. **Verify context operations** map correctly

## Advanced Usage

### Custom Transformations

```typescript
import { parseClassic, generateRuntime, WorkflowIR } from '@velocity-workflow/migration-toolkit';

const ir = parseClassic(sourceCode);

// Modify IR before generation
ir.forEach(workflow => {
  workflow.metadata.customFlag = true;
});

const output = generateRuntime(ir);
```

### Batch Migration

```typescript
import * as fs from 'fs';
import { migrate } from '@velocity-workflow/migration-toolkit';

const files = ['workflow1.ts', 'workflow2.ts', 'workflow3.ts'];

for (const file of files) {
  const source = fs.readFileSync(file, 'utf-8');
  const migrated = migrate(source, {
    source: 'classic',
    target: 'runtime',
  });
  fs.writeFileSync(`migrated-${file}`, migrated);
}
```

## License

MIT
