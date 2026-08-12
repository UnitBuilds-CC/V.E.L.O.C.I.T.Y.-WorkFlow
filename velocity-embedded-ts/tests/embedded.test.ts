import {
  Durable, Transaction, DurableContext, TransactionContext,
  VelocityEmbedded, WorkflowHandle, WorkflowStatus,
  createEmbedded,
} from '../src/index';

// ─── Test Classes ────────────────────────────────────────────────────────────

@Durable()
class OrderWorkflow {
  async process(ctx: DurableContext, orderId: string) {
    const charge = await ctx.run('charge', () => `charged-${orderId}`);
    const ship = await ctx.run('ship', () => `shipped-${orderId}`);
    return { charge, ship };
  }

  async failStep(ctx: DurableContext, orderId: string) {
    await ctx.run('charge', () => `charged-${orderId}`);
    throw new Error('shipping failed');
  }
}

@Durable()
class CounterService {
  async increment(ctx: DurableContext, key: string) {
    const current = (ctx.getState<number>(key) || 0) + 1;
    ctx.setState(key, current);
    return current;
  }
}

@Durable()
class PaymentService {
  @Transaction()
  async charge(ctx: DurableContext, amount: number) {
    const result = await ctx.run('chargeCard', () => ({ status: 'ok', amount }));
    return result;
  }
}

// ─── Decorator Tests ─────────────────────────────────────────────────────────

describe('@Durable decorator', () => {
  test('marks class as durable', () => {
    const meta = (OrderWorkflow.prototype as any)[Symbol.for('velocity:durable')];
    // The decorator uses a private symbol, so we test indirectly
    const engine = new VelocityEmbedded();
    engine.register(OrderWorkflow);
    expect(engine.listClasses()).toContain('OrderWorkflow');
  });

  test('registers multiple classes', () => {
    const engine = new VelocityEmbedded();
    engine.register(OrderWorkflow);
    engine.register(CounterService);
    engine.register(PaymentService);
    expect(engine.listClasses()).toHaveLength(3);
  });
});

// ─── Durable Execution Tests ─────────────────────────────────────────────────

describe('Durable execution', () => {
  test('executes workflow with durable steps', async () => {
    const engine = new VelocityEmbedded();
    engine.register(OrderWorkflow);

    const handle = await engine.execute('OrderWorkflow', 'process', 'wf-1', 'order-123');

    expect(handle.isCompleted).toBe(true);
    expect(handle.result).toEqual({
      charge: 'charged-order-123',
      ship: 'shipped-order-123',
    });
  });

  test('records journal entries', async () => {
    const engine = new VelocityEmbedded();
    engine.register(OrderWorkflow);

    await engine.execute('OrderWorkflow', 'process', 'wf-journal', 'order-1');

    const record = engine.getWorkflow('wf-journal');
    expect(record).toBeDefined();
    expect(record!.journal.length).toBe(2);
    expect(record!.journal[0].stepName).toBe('charge');
    expect(record!.journal[1].stepName).toBe('ship');
  });

  test('handles workflow failure', async () => {
    const engine = new VelocityEmbedded();
    engine.register(OrderWorkflow);

    const handle = await engine.execute('OrderWorkflow', 'failStep', 'wf-fail', 'order-2');

    expect(handle.isFailed).toBe(true);
    expect(handle.error).toContain('shipping failed');
  });

  test('workflow idempotency', async () => {
    const engine = new VelocityEmbedded();
    engine.register(OrderWorkflow);

    const h1 = await engine.execute('OrderWorkflow', 'process', 'wf-idem', 'order-3');
    const h2 = await engine.execute('OrderWorkflow', 'process', 'wf-idem', 'order-3');

    expect(h1.result).toEqual(h2.result);
  });
});

// ─── State Tests ─────────────────────────────────────────────────────────────

describe('Durable state', () => {
  test('getState and setState', async () => {
    const engine = new VelocityEmbedded();
    engine.register(CounterService);

    const h1 = await engine.execute('CounterService', 'increment', 'wf-ctx-1', 'counter');
    expect(h1.result).toBe(1);

    // Note: state is per-invocation context, not persisted across invocations
    // in this in-memory implementation. A real implementation would use Postgres.
  });
});

// ─── DurableContext Tests ────────────────────────────────────────────────────

describe('DurableContext', () => {
  test('run creates journal entries', async () => {
    const ctx = new DurableContext('test-wf');
    const result = await ctx.run('step1', () => 42);
    expect(result).toBe(42);
    expect(ctx.journal.length).toBe(1);
    expect(ctx.journal[0].stepName).toBe('step1');
    expect(ctx.journal[0].output).toBe(42);
  });

  test('multiple steps', async () => {
    const ctx = new DurableContext('test-wf');
    const a = await ctx.run('step1', () => 10);
    const b = await ctx.run('step2', () => 20);
    expect(a).toBe(10);
    expect(b).toBe(20);
    expect(ctx.stepCount).toBe(2);
  });

  test('state operations', () => {
    const ctx = new DurableContext('test-wf');
    ctx.setState('key1', 'value1');
    expect(ctx.getState('key1')).toBe('value1');

    ctx.setState('key2', 42);
    expect(ctx.getState<number>('key2')).toBe(42);

    expect(ctx.clearState('key1')).toBe(true);
    expect(ctx.getState('key1')).toBeUndefined();
    expect(ctx.clearState('key1')).toBe(false);
  });

  test('sleep records journal', async () => {
    const ctx = new DurableContext('test-wf');
    await ctx.sleep(10);
    expect(ctx.journal.length).toBe(1);
    expect(ctx.journal[0].stepName).toBe('__sleep');
  });
});

// ─── TransactionContext Tests ────────────────────────────────────────────────

describe('TransactionContext', () => {
  test('basic transaction', async () => {
    const txCtx = new TransactionContext('wf-tx');
    const result = await txCtx.run(() => 42);
    expect(result).toBe(42);
  });

  test('commit', () => {
    const txCtx = new TransactionContext('wf-tx');
    txCtx.commit();
    expect(txCtx.committed).toBe(true);
    expect(txCtx.operations).toContain('COMMIT');
  });

  test('rollback', () => {
    const txCtx = new TransactionContext('wf-tx');
    txCtx.rollback();
    expect(txCtx.operations).toContain('ROLLBACK');
  });
});

// ─── WorkflowHandle Tests ────────────────────────────────────────────────────

describe('WorkflowHandle', () => {
  test('completed handle', () => {
    const h = new WorkflowHandle('wf-1', WorkflowStatus.COMPLETED, 'result');
    expect(h.isCompleted).toBe(true);
    expect(h.isFailed).toBe(false);
    expect(h.result).toBe('result');
  });

  test('failed handle', () => {
    const h = new WorkflowHandle('wf-2', WorkflowStatus.FAILED, undefined, 'error');
    expect(h.isFailed).toBe(true);
    expect(h.error).toBe('error');
  });

  test('running handle', () => {
    const h = new WorkflowHandle('wf-3', WorkflowStatus.RUNNING);
    expect(h.isRunning).toBe(true);
    expect(h.result).toBeUndefined();
  });
});

// ─── Engine Stats Tests ──────────────────────────────────────────────────────

describe('Engine stats', () => {
  test('tracks workflow counts', async () => {
    const engine = new VelocityEmbedded();
    engine.register(OrderWorkflow);

    await engine.execute('OrderWorkflow', 'process', 'wf-s1', 'o1');
    await engine.execute('OrderWorkflow', 'failStep', 'wf-s2', 'o2');

    const stats = engine.getStats();
    expect(stats.registeredClasses).toBe(1);
    expect(stats.totalWorkflows).toBe(2);
    expect(stats.completed).toBe(1);
    expect(stats.failed).toBe(1);
  });
});

// ─── Factory Tests ───────────────────────────────────────────────────────────

describe('createEmbedded', () => {
  test('creates engine with classes', () => {
    const engine = createEmbedded(OrderWorkflow, CounterService);
    expect(engine.listClasses()).toContain('OrderWorkflow');
    expect(engine.listClasses()).toContain('CounterService');
    expect(engine.listClasses()).toHaveLength(2);
  });
});
