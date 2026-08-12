import {
  migrate, parseClassic, parseRuntime, parseEmbedded, parsePythonRuntime,
  generateClassic, generateRuntime, generateEmbedded, generatePythonRuntime,
  getSupportedMigrations, validateMigration, transformBody,
  pythonToTsType, tsToPyType, SDKFlavor,
} from '../src/index';

// ─── Supported Migrations ────────────────────────────────────────────────────

describe('Migration Toolkit', () => {
  describe('Supported Migrations', () => {
    test('should list all 12 migration paths', () => {
      const migrations = getSupportedMigrations();
      expect(migrations.length).toBe(12);
      expect(migrations).toContain('classic → runtime');
      expect(migrations).toContain('classic → embedded');
      expect(migrations).toContain('classic → python-runtime');
      expect(migrations).toContain('runtime → classic');
      expect(migrations).toContain('runtime → embedded');
      expect(migrations).toContain('runtime → python-runtime');
      expect(migrations).toContain('embedded → classic');
      expect(migrations).toContain('embedded → runtime');
      expect(migrations).toContain('embedded → python-runtime');
      expect(migrations).toContain('python-runtime → classic');
      expect(migrations).toContain('python-runtime → runtime');
      expect(migrations).toContain('python-runtime → embedded');
    });
  });

  // ─── Type Mapping ───────────────────────────────────────────────────────────

  describe('Type Mapping', () => {
    test('pythonToTsType maps common types', () => {
      expect(pythonToTsType('str')).toBe('string');
      expect(pythonToTsType('int')).toBe('number');
      expect(pythonToTsType('float')).toBe('number');
      expect(pythonToTsType('bool')).toBe('boolean');
      expect(pythonToTsType('None')).toBe('void');
      expect(pythonToTsType('dict')).toBe('Record<string, any>');
      expect(pythonToTsType('list')).toBe('any[]');
    });

    test('tsToPyType maps common types', () => {
      expect(tsToPyType('string')).toBe('str');
      expect(tsToPyType('number')).toBe('float');
      expect(tsToPyType('boolean')).toBe('bool');
      expect(tsToPyType('void')).toBe('None');
    });

    test('unknown types pass through unchanged', () => {
      expect(pythonToTsType('MyCustomType')).toBe('MyCustomType');
      expect(tsToPyType('MyCustomType')).toBe('MyCustomType');
    });
  });

  // ─── Body Transformation ────────────────────────────────────────────────────

  describe('Body Transformation', () => {
    test('classic executeActivity → runtime ctx.invoke', () => {
      const result = transformBody(
        `const charge = await this.executeActivity('ChargeActivity', orderId, amount);`,
        'classic', 'runtime'
      );
      expect(result).toContain(`await ctx.invoke('ChargeActivity', 'execute', orderId, amount)`);
    });

    test('classic waitForSignal → runtime ctx.promise', () => {
      const result = transformBody(
        `const approved = await this.waitForSignal('approval');`,
        'classic', 'runtime'
      );
      expect(result).toContain(`await ctx.promise('approval')`);
    });

    test('classic sleep → runtime ctx.sleep', () => {
      const result = transformBody(
        `await this.sleep(5000);`,
        'classic', 'runtime'
      );
      expect(result).toContain(`await ctx.sleep(5000)`);
    });

    test('classic heartbeat → runtime durable step', () => {
      const result = transformBody(
        `this.heartbeat({ progress: 'charging' });`,
        'classic', 'runtime'
      );
      expect(result).toContain(`ctx.run('heartbeat'`);
    });

    test('runtime ctx.get/set → embedded getState/setState', () => {
      let result = transformBody(
        `const status = await ctx.get('status') || 'pending';`,
        'runtime', 'embedded'
      );
      expect(result).toContain(`ctx.getState('status')`);

      result = transformBody(
        `await ctx.set('status', 'processing');`,
        'runtime', 'embedded'
      );
      expect(result).toContain(`ctx.setState('status', 'processing')`);
    });

    test('runtime ctx.invoke → classic executeActivity', () => {
      const result = transformBody(
        `const charge = await ctx.invoke('PaymentService', 'charge', orderId, amount);`,
        'runtime', 'classic'
      );
      expect(result).toContain(`await this.executeActivity('PaymentService'`);
    });

    test('embedded getState → runtime ctx.get', () => {
      const result = transformBody(
        `const count = ctx.getState<number>('count');`,
        'embedded', 'runtime'
      );
      expect(result).toContain(`await ctx.get('count')`);
    });

    test('embedded setState → runtime ctx.set', () => {
      const result = transformBody(
        `ctx.setState('count', count + 1);`,
        'embedded', 'runtime'
      );
      expect(result).toContain(`await ctx.set('count', count + 1)`);
    });

    test('python None → typescript undefined', () => {
      const result = transformBody(`x = None`, 'python-runtime', 'runtime');
      expect(result).toContain('undefined');
    });

    test('python dict literal → typescript object literal', () => {
      const result = transformBody(`return {'status': 'ok'}`, 'python-runtime', 'runtime');
      expect(result).toContain(`{ status: 'ok' }`);
    });

    test('python `or` → typescript `||`', () => {
      const result = transformBody(`status = x or 'pending'`, 'python-runtime', 'runtime');
      expect(result).toContain(`x || 'pending'`);
    });
  });

  // ─── Classic → Runtime ─────────────────────────────────────────────────────

  describe('Classic → Runtime', () => {
    test('should migrate workflow with body transformation', () => {
      const source = `
import { Workflow } from '@velocity-workflow/classic';

class OrderWorkflow extends Workflow {
  async execute(orderId: string): Promise<any> {
    const charge = await this.executeActivity('ChargeActivity', orderId);
    const approved = await this.waitForSignal('approval');
    if (approved) {
      return { charge, status: 'completed' };
    }
    return { status: 'cancelled' };
  }
}
`;
      const result = migrate(source, { source: 'classic', target: 'runtime' });
      expect(result).toContain('VirtualObject');
      expect(result).toContain('OrderWorkflow');
      expect(result).toContain('addHandler');
      // Verify body transformation
      expect(result).toContain(`ctx.invoke('ChargeActivity'`);
      expect(result).toContain(`ctx.promise('approval')`);
    });

    test('should migrate activity to Service', () => {
      const source = `
import { Activity } from '@velocity-workflow/classic';

class ChargeActivity extends Activity {
  async execute(orderId: string, amount: number): Promise<any> {
    this.heartbeat({ progress: 'charging' });
    return { transactionId: orderId, amount, status: 'charged' };
  }
}
`;
      const result = migrate(source, { source: 'classic', target: 'runtime' });
      expect(result).toContain('Service');
      expect(result).toContain('ChargeActivity');
      expect(result).toContain(`ctx.run('heartbeat'`);
    });
  });

  // ─── Classic → Embedded ────────────────────────────────────────────────────

  describe('Classic → Embedded', () => {
    test('should migrate workflow to @Durable with body transformation', () => {
      const source = `
class PaymentWorkflow extends Workflow {
  async execute(paymentId: string): Promise<any> {
    const charge = await this.executeActivity('ChargeActivity', paymentId);
    await this.sleep(1000);
    return { paymentId, status: 'charged' };
  }
}
`;
      const result = migrate(source, { source: 'classic', target: 'embedded' });
      expect(result).toContain('@Durable()');
      expect(result).toContain('class PaymentWorkflow');
      expect(result).toContain('DurableContext');
      expect(result).toContain(`ctx.invoke('ChargeActivity'`);
      expect(result).toContain(`ctx.sleep(1000)`);
    });
  });

  // ─── Classic → Python Runtime ──────────────────────────────────────────────

  describe('Classic → Python Runtime', () => {
    test('should migrate workflow to Python with body transformation', () => {
      const source = `
class OrderWorkflow extends Workflow {
  async execute(orderId: string): Promise<any> {
    const charge = await this.executeActivity('ChargeActivity', orderId);
    return { orderId, status: 'completed' };
  }
}
`;
      const result = migrate(source, { source: 'classic', target: 'python-runtime' });
      expect(result).toContain('class OrderWorkflow');
      expect(result).toContain('async def execute');
      expect(result).toContain(`ctx.invoke('ChargeActivity'`);
    });
  });

  // ─── Runtime → Classic ─────────────────────────────────────────────────────

  describe('Runtime → Classic', () => {
    test('should migrate VirtualObject handlers to Workflow with body transformation', () => {
      const source = `
const OrderProcessor = new VirtualObject('OrderProcessor');
OrderProcessor.addHandler('process', async (ctx, orderId: string, amount: number) => {
  const status = await ctx.get('status') || 'pending';
  await ctx.set('status', 'processing');
  const charge = await ctx.invoke('PaymentService', 'charge', orderId, amount);
  await ctx.set('status', 'completed');
  return { charge, status: 'completed' };
});
`;
      const result = migrate(source, { source: 'runtime', target: 'classic' });
      expect(result).toContain('class OrderProcessor');
      expect(result).toContain('extends Workflow');
      // Verify body transformation
      expect(result).toContain(`this.executeActivity('PaymentService'`);
    });

    test('should migrate Service to Activity', () => {
      const source = `
const EmailService = new Service('EmailService');
EmailService.addHandler('send', async (ctx, to: string) => {
  return { sent: true };
});
`;
      const result = migrate(source, { source: 'runtime', target: 'classic' });
      expect(result).toContain('class EmailService');
      expect(result).toContain('extends Activity');
    });
  });

  // ─── Runtime → Embedded ────────────────────────────────────────────────────

  describe('Runtime → Embedded', () => {
    test('should transform state operations', () => {
      const source = `
const Counter = new VirtualObject('Counter');
Counter.addHandler('increment', async (ctx, key: string) => {
  const count = await ctx.get('count') || 0;
  await ctx.set('count', count + 1);
  return count + 1;
});
`;
      const result = migrate(source, { source: 'runtime', target: 'embedded' });
      expect(result).toContain('@Durable()');
      expect(result).toContain('class Counter');
      expect(result).toContain(`ctx.getState('count')`);
      expect(result).toContain(`ctx.setState('count', count + 1)`);
    });
  });

  // ─── Runtime → Python Runtime ──────────────────────────────────────────────

  describe('Runtime → Python Runtime', () => {
    test('should migrate VirtualObject to Python class', () => {
      const source = `
const Counter = new VirtualObject('Counter');
Counter.addHandler('increment', async (ctx, key: string) => {
  const count = await ctx.get('count') || 0;
  await ctx.set('count', count + 1);
  return count + 1;
});
`;
      const result = migrate(source, { source: 'runtime', target: 'python-runtime' });
      expect(result).toContain('class Counter(VirtualObject)');
      expect(result).toContain('async def increment');
      expect(result).toContain('await ctx.get');
      expect(result).toContain('await ctx.set');
    });
  });

  // ─── Embedded → Classic ────────────────────────────────────────────────────

  describe('Embedded → Classic', () => {
    test('should transform getState/setState to classic patterns', () => {
      const source = `
@Durable()
class OrderProcessor {
  async process(ctx: DurableContext, orderId: string): Promise<any> {
    const status = ctx.getState<string>('status') || 'pending';
    ctx.setState('status', 'processing');
    const charge = await ctx.invoke('ChargeService', 'charge', orderId);
    return { orderId, status: 'done' };
  }
}
`;
      const result = migrate(source, { source: 'embedded', target: 'classic' });
      expect(result).toContain('class OrderProcessor');
      expect(result).toContain('extends Workflow');
      expect(result).toContain(`this.executeActivity('ChargeService'`);
    });
  });

  // ─── Embedded → Runtime ────────────────────────────────────────────────────

  describe('Embedded → Runtime', () => {
    test('should transform getState/setState to get/set', () => {
      const source = `
@Durable()
class Counter {
  async increment(ctx: DurableContext, key: string): Promise<number> {
    const count = ctx.getState<number>('count') || 0;
    ctx.setState('count', count + 1);
    return count + 1;
  }
}
`;
      const result = migrate(source, { source: 'embedded', target: 'runtime' });
      expect(result).toContain('VirtualObject');
      expect(result).toContain(`await ctx.get('count')`);
      expect(result).toContain(`await ctx.set('count', count + 1)`);
    });
  });

  // ─── Embedded → Python Runtime ─────────────────────────────────────────────

  describe('Embedded → Python Runtime', () => {
    test('should migrate @Durable class to Python', () => {
      const source = `
@Durable()
class OrderProcessor {
  async process(ctx: DurableContext, orderId: string): Promise<any> {
    const charge = await ctx.invoke('ChargeService', 'charge', orderId);
    return { orderId, status: 'done' };
  }
}
`;
      const result = migrate(source, { source: 'embedded', target: 'python-runtime' });
      expect(result).toContain('class OrderProcessor');
      expect(result).toContain('async def process');
      expect(result).toContain(`ctx.invoke('ChargeService'`);
    });
  });

  // ─── Python Runtime → Classic ──────────────────────────────────────────────

  describe('Python Runtime → Classic', () => {
    test('should migrate Python VirtualObject to TypeScript Workflow', () => {
      const source = `
class OrderWorkflow(VirtualObject):
    def __init__(self):
        super().__init__('OrderWorkflow')
    
    async def execute(self, ctx, orderId: str):
        charge = await ctx.invoke('ChargeActivity', 'execute', orderId)
        return {'orderId': orderId, 'status': 'completed'}
`;
      const result = migrate(source, { source: 'python-runtime', target: 'classic' });
      expect(result).toContain('class OrderWorkflow');
      expect(result).toContain('extends Workflow');
      expect(result).toContain(`this.executeActivity('ChargeActivity'`);
    });
  });

  // ─── Python Runtime → Runtime ──────────────────────────────────────────────

  describe('Python Runtime → Runtime', () => {
    test('should migrate Python Service to TypeScript Service', () => {
      const source = `
class PaymentService(Service):
    def __init__(self):
        super().__init__('PaymentService')
    
    async def charge(self, ctx, orderId: str, amount: float):
        return {'transactionId': orderId, 'amount': amount}
`;
      const result = migrate(source, { source: 'python-runtime', target: 'runtime' });
      expect(result).toContain('Service');
      expect(result).toContain('PaymentService');
      expect(result).toContain('addHandler');
    });
  });

  // ─── Python Runtime → Embedded ─────────────────────────────────────────────

  describe('Python Runtime → Embedded', () => {
    test('should migrate Python class to @Durable', () => {
      const source = `
class OrderProcessor(VirtualObject):
    def __init__(self):
        super().__init__('OrderProcessor')
    
    async def process(self, ctx, orderId: str):
        charge = await ctx.invoke('ChargeService', 'charge', orderId)
        return {'orderId': orderId}
`;
      const result = migrate(source, { source: 'python-runtime', target: 'embedded' });
      expect(result).toContain('@Durable()');
      expect(result).toContain('class OrderProcessor');
    });
  });

  // ─── Parser Tests ──────────────────────────────────────────────────────────

  describe('Parser Tests', () => {
    test('parseClassic should extract workflow with method body', () => {
      const source = `
class OrderWorkflow extends Workflow {
  async execute(orderId: string): Promise<any> {
    const charge = await this.executeActivity('ChargeActivity', orderId);
    return { orderId };
  }
}
`;
      const ir = parseClassic(source);
      expect(ir.length).toBe(1);
      expect(ir[0].name).toBe('OrderWorkflow');
      expect(ir[0].type).toBe('workflow');
      expect(ir[0].methods.length).toBe(1);
      expect(ir[0].methods[0].name).toBe('execute');
      expect(ir[0].methods[0].body).toContain('executeActivity');
    });

    test('parseClassic should extract multiple classes', () => {
      const source = `
class OrderWorkflow extends Workflow {
  async execute(orderId: string): Promise<any> { return {}; }
}
class ChargeActivity extends Activity {
  async execute(orderId: string): Promise<any> { return {}; }
}
`;
      const ir = parseClassic(source);
      expect(ir.length).toBe(2);
      expect(ir[0].type).toBe('workflow');
      expect(ir[1].type).toBe('activity');
    });

    test('parseRuntime should extract VirtualObject with handlers', () => {
      const source = `
const Counter = new VirtualObject('Counter');
Counter.addHandler('increment', async (ctx, key: string) => {
  const count = await ctx.get('count') || 0;
  await ctx.set('count', count + 1);
  return count + 1;
});
`;
      const ir = parseRuntime(source);
      expect(ir.length).toBe(1);
      expect(ir[0].name).toBe('Counter');
      expect(ir[0].type).toBe('virtualObject');
      expect(ir[0].methods.length).toBe(1);
      expect(ir[0].methods[0].name).toBe('increment');
      expect(ir[0].methods[0].body).toContain('ctx.get');
    });

    test('parseRuntime should extract Service with handlers', () => {
      const source = `
const EmailService = new Service('EmailService');
EmailService.addHandler('send', async (ctx, to: string) => {
  return { sent: true };
});
`;
      const ir = parseRuntime(source);
      expect(ir.length).toBe(1);
      expect(ir[0].name).toBe('EmailService');
      expect(ir[0].type).toBe('service');
      expect(ir[0].methods.length).toBe(1);
      expect(ir[0].methods[0].name).toBe('send');
    });

    test('parseEmbedded should extract @Durable class with methods', () => {
      const source = `
@Durable()
class OrderProcessor {
  async process(ctx: DurableContext, orderId: string): Promise<any> {
    const charge = await ctx.run('charge', () => chargeCard(orderId));
    return { orderId };
  }
}
`;
      const ir = parseEmbedded(source);
      expect(ir.length).toBe(1);
      expect(ir[0].name).toBe('OrderProcessor');
      expect(ir[0].methods.length).toBe(1);
      expect(ir[0].methods[0].name).toBe('process');
      expect(ir[0].methods[0].body).toContain('ctx.run');
    });

    test('parsePythonRuntime should extract Python class with methods', () => {
      const source = `
class OrderWorkflow(VirtualObject):
    def __init__(self):
        super().__init__('OrderWorkflow')
    
    async def execute(self, ctx, orderId: str):
        charge = await ctx.invoke('ChargeActivity', 'execute', orderId)
        return {'orderId': orderId}
`;
      const ir = parsePythonRuntime(source);
      expect(ir.length).toBe(1);
      expect(ir[0].name).toBe('OrderWorkflow');
      expect(ir[0].type).toBe('virtualObject');
      expect(ir[0].methods.length).toBe(1);
      expect(ir[0].methods[0].name).toBe('execute');
      expect(ir[0].methods[0].body).toContain('ctx.invoke');
    });
  });

  // ─── Generator Tests ───────────────────────────────────────────────────────

  describe('Generator Tests', () => {
    test('generateClassic should produce valid TypeScript class', () => {
      const ir = [{
        name: 'TestWorkflow',
        type: 'workflow' as const,
        methods: [{
          name: 'execute',
          parameters: [{ name: 'orderId', type: 'string', optional: false }],
          returnType: 'any',
          body: `return { orderId };`,
          transformedBody: '',
          decorators: [],
          contextUsage: [],
          isAsync: true,
        }],
        imports: [],
        metadata: { sdk: 'runtime' },
      }];
      const result = generateClassic(ir, 'runtime');
      expect(result).toContain('class TestWorkflow extends Workflow');
      expect(result).toContain('async execute(orderId: string)');
      expect(result).toContain('return { orderId }');
    });

    test('generateRuntime should produce VirtualObject with handler', () => {
      const ir = [{
        name: 'TestService',
        type: 'service' as const,
        methods: [{
          name: 'process',
          parameters: [{ name: 'data', type: 'string', optional: false }],
          returnType: 'any',
          body: `return { data };`,
          transformedBody: '',
          decorators: [],
          contextUsage: [],
          isAsync: true,
        }],
        imports: [],
        metadata: { sdk: 'classic' },
      }];
      const result = generateRuntime(ir, 'classic');
      expect(result).toContain('new Service');
      expect(result).toContain("addHandler('process'");
      expect(result).toContain('return { data }');
    });

    test('generateEmbedded should produce @Durable class with method', () => {
      const ir = [{
        name: 'TestProcessor',
        type: 'workflow' as const,
        methods: [{
          name: 'process',
          parameters: [{ name: 'orderId', type: 'string', optional: false }],
          returnType: 'any',
          body: `return { orderId };`,
          transformedBody: '',
          decorators: [],
          contextUsage: [],
          isAsync: true,
        }],
        imports: [],
        metadata: { sdk: 'runtime' },
      }];
      const result = generateEmbedded(ir, 'runtime');
      expect(result).toContain('@Durable()');
      expect(result).toContain('class TestProcessor');
      expect(result).toContain('async process(ctx: DurableContext, orderId: string)');
    });

    test('generatePythonRuntime should produce Python class', () => {
      const ir = [{
        name: 'TestWorkflow',
        type: 'virtualObject' as const,
        methods: [{
          name: 'execute',
          parameters: [{ name: 'orderId', type: 'string', optional: false }],
          returnType: 'any',
          body: `return { orderId };`,
          transformedBody: '',
          decorators: [],
          contextUsage: [],
          isAsync: true,
        }],
        imports: [],
        metadata: { sdk: 'classic' },
      }];
      const result = generatePythonRuntime(ir, 'classic');
      expect(result).toContain('class TestWorkflow(VirtualObject)');
      expect(result).toContain('async def execute');
    });
  });

  // ─── Validation ────────────────────────────────────────────────────────────

  describe('Validation', () => {
    test('should validate classic source with workflows', () => {
      const source = `
class OrderWorkflow extends Workflow {
  async execute(orderId: string): Promise<any> { return {}; }
}
`;
      const result = validateMigration(source, 'classic');
      expect(result.valid).toBe(true);
      expect(result.errors.length).toBe(0);
    });

    test('should fail validation for empty source', () => {
      const result = validateMigration('// empty file', 'classic');
      expect(result.valid).toBe(false);
      expect(result.errors.length).toBeGreaterThan(0);
    });
  });

  // ─── Round-trip Migrations ─────────────────────────────────────────────────

  describe('Round-trip Migrations', () => {
    test('Classic → Runtime → Classic should preserve entity name', () => {
      const source = `
class OrderWorkflow extends Workflow {
  async execute(orderId: string): Promise<any> {
    const charge = await this.executeActivity('ChargeActivity', orderId);
    return { orderId };
  }
}
`;
      const intermediate = migrate(source, { source: 'classic', target: 'runtime' });
      const result = migrate(intermediate, { source: 'runtime', target: 'classic' });
      expect(result).toContain('OrderWorkflow');
      expect(result).toContain('Workflow');
    });

    test('Runtime → Embedded → Runtime should preserve entity name', () => {
      const source = `
const Counter = new VirtualObject('Counter');
Counter.addHandler('increment', async (ctx, key: string) => {
  const count = await ctx.get('count') || 0;
  await ctx.set('count', count + 1);
  return count + 1;
});
`;
      const intermediate = migrate(source, { source: 'runtime', target: 'embedded' });
      const result = migrate(intermediate, { source: 'embedded', target: 'runtime' });
      expect(result).toContain('Counter');
      expect(result).toContain('VirtualObject');
    });
  });

  // ─── Full Integration Tests ────────────────────────────────────────────────

  describe('Full Integration', () => {
    test('classic order workflow → all targets', () => {
      const source = `
class OrderWorkflow extends Workflow {
  async execute(orderId: string, amount: number): Promise<any> {
    const charge = await this.executeActivity('ChargeActivity', orderId, amount);
    const approved = await this.waitForSignal('approval');
    if (approved) {
      await this.sleep(1000);
      return { charge, status: 'completed' };
    } else {
      return { charge, status: 'cancelled' };
    }
  }
}
class ChargeActivity extends Activity {
  async execute(orderId: string, amount: number): Promise<any> {
    this.heartbeat({ progress: 'charging' });
    return { transactionId: orderId, amount };
  }
}
`;
      // To Runtime
      const runtime = migrate(source, { source: 'classic', target: 'runtime' });
      expect(runtime).toContain('VirtualObject');
      expect(runtime).toContain('Service');
      expect(runtime).toContain(`ctx.invoke('ChargeActivity'`);
      expect(runtime).toContain(`ctx.promise('approval')`);
      expect(runtime).toContain(`ctx.sleep(1000)`);

      // To Embedded
      const embedded = migrate(source, { source: 'classic', target: 'embedded' });
      expect(embedded).toContain('@Durable()');
      expect(embedded).toContain(`ctx.invoke('ChargeActivity'`);
      expect(embedded).toContain(`ctx.sleep(1000)`);

      // To Python
      const python = migrate(source, { source: 'classic', target: 'python-runtime' });
      expect(python).toContain('class OrderWorkflow');
      expect(python).toContain('class ChargeActivity');
      expect(python).toContain('async def execute');
    });

    test('runtime order workflow → all targets', () => {
      const source = `
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
  return { transactionId: orderId, amount };
});
`;
      // To Classic
      const classic = migrate(source, { source: 'runtime', target: 'classic' });
      expect(classic).toContain('extends Workflow');
      expect(classic).toContain('extends Activity');
      expect(classic).toContain(`this.executeActivity('PaymentService'`);

      // To Embedded
      const embedded = migrate(source, { source: 'runtime', target: 'embedded' });
      expect(embedded).toContain('@Durable()');
      expect(embedded).toContain(`ctx.getState('status')`);
      expect(embedded).toContain(`ctx.setState('status', 'processing')`);

      // To Python
      const python = migrate(source, { source: 'runtime', target: 'python-runtime' });
      expect(python).toContain('class OrderProcessor(VirtualObject)');
      expect(python).toContain('class PaymentService(Service)');
    });
  });

  describe('Advanced Parser Features', () => {
    test('extractBraceBlock handles braces inside string literals', () => {
      const source = `
import { Workflow } from '@velocity-workflow/classic';
class StringTest extends Workflow {
  async execute(ctx: Context, input: string): Promise<any> {
    const msg = "this has { braces }";
    const result = await this.executeActivity('ProcessActivity', msg);
    return result;
  }
}
`;
      const ir = parseClassic(source);
      expect(ir.length).toBe(1);
      expect(ir[0].methods.length).toBe(1);
      expect(ir[0].methods[0].body).toContain('this has { braces }');
      expect(ir[0].methods[0].body).toContain("this.executeActivity('ProcessActivity'");
    });

    test('extractBraceBlock handles braces inside template literals', () => {
      const source = `
import { Workflow } from '@velocity-workflow/classic';
class TemplateTest extends Workflow {
  async execute(ctx: Context, name: string): Promise<any> {
    const greeting = \`Hello \${name}, welcome to {the} jungle\`;
    return greeting;
  }
}
`;
      const ir = parseClassic(source);
      expect(ir.length).toBe(1);
      expect(ir[0].methods[0].body).toContain('Hello');
    });

    test('extractBraceBlock handles braces inside comments', () => {
      const source = `
import { Workflow } from '@velocity-workflow/classic';
class CommentTest extends Workflow {
  async execute(ctx: Context): Promise<any> {
    // This comment has { braces }
    /* And this block comment has { too } */
    const result = await this.executeActivity('SimpleActivity');
    return result;
  }
}
`;
      const ir = parseClassic(source);
      expect(ir.length).toBe(1);
      expect(ir[0].methods[0].body).toContain('SimpleActivity');
    });

    test('parseTSMethods handles complex nested parameter types', () => {
      const source = `
import { Workflow } from '@velocity-workflow/classic';
class ComplexParams extends Workflow {
  async execute(ctx: Context, opts: { orderId: string, amount: number }): Promise<any> {
    return await this.executeActivity('ProcessActivity', opts.orderId);
  }
}
`;
      const ir = parseClassic(source);
      expect(ir.length).toBe(1);
      expect(ir[0].methods.length).toBe(1);
      expect(ir[0].methods[0].parameters.length).toBeGreaterThanOrEqual(2);
      // The opts param should have a complex type preserved
      const optsParam = ir[0].methods[0].parameters.find(p => p.name === 'opts');
      expect(optsParam).toBeDefined();
      expect(optsParam!.type).toContain('orderId');
    });

    test('parseTSMethods handles generic type parameters', () => {
      const source = `
import { Workflow } from '@velocity-workflow/classic';
class GenericTest extends Workflow {
  async execute(ctx: Context, items: Array<string>): Promise<any> {
    return items.length;
  }
}
`;
      const ir = parseClassic(source);
      expect(ir.length).toBe(1);
      const itemsParam = ir[0].methods[0].parameters.find(p => p.name === 'items');
      expect(itemsParam).toBeDefined();
      expect(itemsParam!.type).toContain('Array<string>');
    });

    test('extractImports pulls named imports from source', () => {
      const source = `
import { Workflow, Activity, Worker, Client } from '@velocity-workflow/classic';
import { helper } from './utils';
class ImportTest extends Workflow {
  async execute(): Promise<any> { return 42; }
}
`;
      const ir = parseClassic(source);
      expect(ir.length).toBe(1);
      expect(ir[0].imports).toContain('Workflow');
      expect(ir[0].imports).toContain('Activity');
      expect(ir[0].imports).toContain('helper');
    });

    test('parseEmbedded handles class with extends and implements', () => {
      const source = `
@Durable()
export class AdvancedWorkflow extends BaseWorkflow implements IWorkflow {
  async execute(ctx: DurableContext, input: string): Promise<any> {
    return input;
  }
}
`;
      const ir = parseEmbedded(source);
      expect(ir.length).toBe(1);
      expect(ir[0].name).toBe('AdvancedWorkflow');
      expect(ir[0].methods.length).toBe(1);
    });

    test('parseRuntime handles handler with complex nested params', () => {
      const source = `
const Svc = new Service('Svc');
Svc.addHandler('process', async (ctx, opts: { id: string, items: string[] }) => {
  return await ctx.invoke('Helper', 'execute', opts.id);
});
`;
      const ir = parseRuntime(source);
      expect(ir.length).toBe(1);
      expect(ir[0].methods.length).toBe(1);
      expect(ir[0].methods[0].name).toBe('process');
      const optsParam = ir[0].methods[0].parameters.find(p => p.name === 'opts');
      expect(optsParam).toBeDefined();
      expect(optsParam!.type).toContain('id');
    });

    test('parsePythonRuntime handles decorators on classes', () => {
      const source = `
from velocity_runtime import VirtualObject

class MyService(VirtualObject):
    async def handle(self, ctx, data: str):
        return data
`;
      const ir = parsePythonRuntime(source);
      expect(ir.length).toBe(1);
      expect(ir[0].name).toBe('MyService');
      expect(ir[0].methods.length).toBe(1);
    });

    test('nested workflow classes are correctly extracted', () => {
      const source = `
import { Workflow, Activity } from '@velocity-workflow/classic';

class OrderWorkflow extends Workflow {
  async execute(ctx: Context, orderId: string): Promise<any> {
    const charge = await this.executeActivity('ChargeActivity', orderId);
    return charge;
  }
}

class ChargeActivity extends Activity {
  async execute(amount: number): Promise<any> {
    return { charged: true, amount };
  }
}
`;
      const ir = parseClassic(source);
      expect(ir.length).toBe(2);
      expect(ir[0].name).toBe('OrderWorkflow');
      expect(ir[0].type).toBe('workflow');
      expect(ir[1].name).toBe('ChargeActivity');
      expect(ir[1].type).toBe('activity');
    });
  });
});
