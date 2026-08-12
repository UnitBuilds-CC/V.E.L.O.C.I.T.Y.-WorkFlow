#!/usr/bin/env ts-node

/**
 * Migration Demo
 * 
 * Demonstrates converting workflows between all 4 SDK flavors.
 */

import * as fs from 'fs';
import * as path from 'path';
import { migrate, getSupportedMigrations } from '../src/index';

// Example source files
const classicSource = `
import { Workflow, Activity } from '@velocity-workflow/classic';

class OrderWorkflow extends Workflow {
  async execute(orderId: string, amount: number): Promise<any> {
    const charge = await this.executeActivity('ChargeActivity', orderId, amount);
    const approved = await this.waitForSignal('approval');
    
    if (approved) {
      return { charge, status: 'completed' };
    } else {
      await this.executeActivity('RefundActivity', orderId);
      return { charge, status: 'cancelled' };
    }
  }
}

class ChargeActivity extends Activity {
  async execute(orderId: string, amount: number): Promise<any> {
    this.heartbeat({ progress: 'charging' });
    return { transactionId: \`tx-\${orderId}\`, amount, status: 'charged' };
  }
}
`;

const runtimeSource = `
import { VirtualObject, Service } from '@velocity-workflow/runtime';

const OrderProcessor = new VirtualObject('OrderProcessor');
OrderProcessor.addHandler('process', async (ctx, orderId: string, amount: number) => {
  const status = await ctx.get('status') || 'pending';
  await ctx.set('status', 'processing');
  
  const charge = await ctx.invoke('PaymentService', 'charge', orderId, amount);
  
  await ctx.set('status', 'completed');
  return { charge, status: 'completed' };
});

const PaymentService = new Service('PaymentService');
PaymentService.addHandler('charge', async (ctx, orderId: string, amount: number) => {
  return { transactionId: \`tx-\${orderId}\`, amount, status: 'charged' };
});
`;

const embeddedSource = `
import { Durable, DurableContext } from '@velocity-workflow/embedded';

@Durable()
class OrderProcessor {
  async process(ctx: DurableContext, orderId: string, amount: number): Promise<any> {
    const status = ctx.getState<string>('status') || 'pending';
    ctx.setState('status', 'processing');
    
    const charge = await ctx.invoke('PaymentService', 'charge', orderId, amount);
    
    ctx.setState('status', 'completed');
    return { charge, status: 'completed' };
  }
}
`;

const pythonSource = `
from velocity_runtime import VirtualObject, Service

class OrderProcessor(VirtualObject):
    def __init__(self):
        super().__init__('OrderProcessor')
    
    async def process(self, ctx, orderId: str, amount: float):
        status = await ctx.get('status') or 'pending'
        await ctx.set('status', 'processing')
        
        charge = await ctx.invoke('PaymentService', 'charge', orderId, amount)
        
        await ctx.set('status', 'completed')
        return {'charge': charge, 'status': 'completed'}
`;

function demo() {
  console.log('╔═══════════════════════════════════════════════════════════╗');
  console.log('║     Velocity Migration Toolkit - Demo                     ║');
  console.log('╚═══════════════════════════════════════════════════════════╝\n');

  console.log('Supported Migrations:');
  const migrations = getSupportedMigrations();
  migrations.forEach(m => console.log(`  • ${m}`));
  console.log();

  // Demo 1: Classic → Runtime
  console.log('═══════════════════════════════════════════════════════════');
  console.log('Demo 1: Classic → Runtime');
  console.log('═══════════════════════════════════════════════════════════\n');
  
  console.log('Source (Classic SDK):');
  console.log(classicSource);
  
  const runtimeOutput = migrate(classicSource, { source: 'classic', target: 'runtime' });
  console.log('\nMigrated (Runtime SDK):');
  console.log(runtimeOutput);
  console.log('\n');

  // Demo 2: Runtime → Embedded
  console.log('═══════════════════════════════════════════════════════════');
  console.log('Demo 2: Runtime → Embedded');
  console.log('═══════════════════════════════════════════════════════════\n');
  
  console.log('Source (Runtime SDK):');
  console.log(runtimeSource);
  
  const embeddedOutput = migrate(runtimeSource, { source: 'runtime', target: 'embedded' });
  console.log('\nMigrated (Embedded SDK):');
  console.log(embeddedOutput);
  console.log('\n');

  // Demo 3: Embedded → Python Runtime
  console.log('═══════════════════════════════════════════════════════════');
  console.log('Demo 3: Embedded → Python Runtime');
  console.log('═══════════════════════════════════════════════════════════\n');
  
  console.log('Source (Embedded SDK):');
  console.log(embeddedSource);
  
  const pythonOutput = migrate(embeddedSource, { source: 'embedded', target: 'python-runtime' });
  console.log('\nMigrated (Python Runtime SDK):');
  console.log(pythonOutput);
  console.log('\n');

  // Demo 4: Python Runtime → Classic
  console.log('═══════════════════════════════════════════════════════════');
  console.log('Demo 4: Python Runtime → Classic');
  console.log('═══════════════════════════════════════════════════════════\n');
  
  console.log('Source (Python Runtime SDK):');
  console.log(pythonSource);
  
  const classicOutput = migrate(pythonSource, { source: 'python-runtime', target: 'classic' });
  console.log('\nMigrated (Classic SDK):');
  console.log(classicOutput);
  console.log('\n');

  console.log('═══════════════════════════════════════════════════════════');
  console.log('✓ All migrations completed successfully!');
  console.log('═══════════════════════════════════════════════════════════');
}

// Run demo
demo();
