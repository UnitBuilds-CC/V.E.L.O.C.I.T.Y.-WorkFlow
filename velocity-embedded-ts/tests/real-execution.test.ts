/**
 * Tests for real execution: storage, journal replay, invoke, and message passing.
 */

import {
  Durable, Transaction, DurableContext, TransactionContext,
  VelocityEmbedded, WorkflowStatus,
  EmbeddedInMemoryStorage, EmbeddedStoredJournal,
  createEmbedded,
} from '../src/index';

// ─── Test Classes ────────────────────────────────────────────────────────────

@Durable()
class MathService {
  async double(ctx: DurableContext, n: number): Promise<number> {
    return await ctx.run('double', () => n * 2);
  }

  async add(ctx: DurableContext, a: number, b: number): Promise<number> {
    return await ctx.run('add', () => a + b);
  }
}

@Durable()
class Orchestrator {
  async run(ctx: DurableContext, input: number): Promise<number> {
    const doubled = await ctx.invoke<number>('MathService', 'double', input);
    const result = await ctx.invoke<number>('MathService', 'add', doubled, 10);
    return result;
  }
}

@Durable()
class WaitingWorkflow {
  async run(ctx: DurableContext): Promise<string> {
    const msg = await ctx.recv<string>('approval');
    return `received:${msg}`;
  }
}

@Durable()
class MultiRecvWorkflow {
  async run(ctx: DurableContext): Promise<string[]> {
    const items: string[] = [];
    const first = await ctx.recv<string>('items');
    items.push(first);
    const second = await ctx.recv<string>('items');
    items.push(second);
    return items;
  }
}

@Durable()
class StatefulWorkflow {
  async run(ctx: DurableContext, name: string): Promise<string> {
    ctx.setState('name', name);
    ctx.setState('processed', true);
    const greeting = await ctx.run('greet', () => `Hello, ${name}!`);
    return greeting;
  }
}

// ─── Storage Tests ───────────────────────────────────────────────────────────

describe('EmbeddedInMemoryStorage', () => {
  test('save and load journal', () => {
    const storage = new EmbeddedInMemoryStorage();
    storage.saveJournal({
      workflowId: 'wf-1', functionName: 'Svc.method',
      entries: [], state: {}, output: 'result', status: 'completed',
      createdAt: 1000, completedAt: 1001,
    });
    const loaded = storage.loadJournal('wf-1');
    expect(loaded).not.toBeNull();
    expect(loaded!.output).toBe('result');
    expect(loaded!.status).toBe('completed');
  });

  test('load nonexistent returns null', () => {
    const storage = new EmbeddedInMemoryStorage();
    expect(storage.loadJournal('nonexistent')).toBeNull();
  });

  test('load all journals', () => {
    const storage = new EmbeddedInMemoryStorage();
    storage.saveJournal({ workflowId: 'wf-1', functionName: 'A', entries: [], state: {}, output: null, status: 'completed', createdAt: 0, completedAt: 0 });
    storage.saveJournal({ workflowId: 'wf-2', functionName: 'B', entries: [], state: {}, output: null, status: 'completed', createdAt: 0, completedAt: 0 });
    expect(storage.loadAllJournals()).toHaveLength(2);
  });

  test('delete journal', () => {
    const storage = new EmbeddedInMemoryStorage();
    storage.saveJournal({ workflowId: 'wf-1', functionName: 'A', entries: [], state: {}, output: null, status: 'completed', createdAt: 0, completedAt: 0 });
    storage.deleteJournal('wf-1');
    expect(storage.loadJournal('wf-1')).toBeNull();
  });

  test('clear', () => {
    const storage = new EmbeddedInMemoryStorage();
    storage.saveJournal({ workflowId: 'wf-1', functionName: 'A', entries: [], state: {}, output: null, status: 'completed', createdAt: 0, completedAt: 0 });
    storage.clear();
    expect(storage.loadAllJournals()).toHaveLength(0);
  });
});

// ─── Journal Persistence Tests ───────────────────────────────────────────────

describe('Journal Persistence', () => {
  test('journal persisted on completion', async () => {
    const storage = new EmbeddedInMemoryStorage();
    const engine = new VelocityEmbedded(undefined, storage);
    engine.register(MathService);

    const handle = await engine.execute<number>('MathService', 'double', 'wf-persist', 5);
    expect(handle.isCompleted).toBe(true);
    expect(handle.result).toBe(10);

    const stored = storage.loadJournal('wf-persist');
    expect(stored).not.toBeNull();
    expect(stored!.status).toBe('completed');
    expect(stored!.output).toBe(10);
  });

  test('failed journal persisted for audit', async () => {
    const storage = new EmbeddedInMemoryStorage();
    const engine = new VelocityEmbedded(undefined, storage);

    @Durable()
    class FailService {
      async run(ctx: DurableContext): Promise<void> { throw new Error('boom'); }
    }
    engine.register(FailService);

    const handle = await engine.execute('FailService', 'run', 'wf-fail');
    expect(handle.isFailed).toBe(true);

    const stored = storage.loadJournal('wf-fail');
    expect(stored).not.toBeNull();
    expect(stored!.status).toBe('failed');
  });

  test('journal replay restores workflow records', () => {
    const storage = new EmbeddedInMemoryStorage();
    storage.saveJournal({
      workflowId: 'wf-replay', functionName: 'Svc.method',
      entries: [], state: { count: 42 }, output: 'replayed',
      status: 'completed', createdAt: 1000, completedAt: 1001,
    });

    const engine = new VelocityEmbedded(undefined, storage);
    const wf = engine.getWorkflow('wf-replay');
    expect(wf).toBeDefined();
    expect(wf!.output).toBe('replayed');
    expect(wf!.status).toBe(WorkflowStatus.COMPLETED);
  });
});

// ─── Real Invoke Tests ───────────────────────────────────────────────────────

describe('Real Invoke', () => {
  test('ctx.invoke calls registered class method', async () => {
    const engine = new VelocityEmbedded({ logLevel: 'silent' });
    engine.register(MathService);
    engine.register(Orchestrator);

    const handle = await engine.execute<number>('Orchestrator', 'run', 'wf-invoke', 5);
    expect(handle.isCompleted).toBe(true);
    expect(handle.result).toBe(20); // 5*2=10, 10+10=20
  });

  test('ctx.invoke with unknown class throws', async () => {
    @Durable()
    class BadOrchestrator {
      async run(ctx: DurableContext): Promise<void> {
        await ctx.invoke('NonExistent', 'method');
      }
    }
    const engine = new VelocityEmbedded({ logLevel: 'silent' });
    engine.register(BadOrchestrator);

    const handle = await engine.execute('BadOrchestrator', 'run', 'wf-bad-invoke');
    expect(handle.isFailed).toBe(true);
  });
});

// ─── Message Passing Tests ───────────────────────────────────────────────────

describe('Message Passing', () => {
  test('recv blocks until send delivers message', async () => {
    const engine = new VelocityEmbedded({ logLevel: 'silent' });
    engine.register(WaitingWorkflow);

    const handlePromise = engine.execute<string>('WaitingWorkflow', 'run', 'wf-msg-1');

    // Small delay to let workflow start and reach recv
    await new Promise(r => setTimeout(r, 50));

    // Deliver message
    engine.send('wf-msg-1', 'approval', 'yes');

    const handle = await handlePromise;
    expect(handle.isCompleted).toBe(true);
    expect(handle.result).toBe('received:yes');
  });

  test('buffered message consumed immediately', async () => {
    const engine = new VelocityEmbedded({ logLevel: 'silent' });
    engine.register(MultiRecvWorkflow);

    const handlePromise = engine.execute<string[]>('MultiRecvWorkflow', 'run', 'wf-msg-2');

    await new Promise(r => setTimeout(r, 50));
    engine.send('wf-msg-2', 'items', 'first');
    engine.send('wf-msg-2', 'items', 'second');

    const handle = await handlePromise;
    expect(handle.isCompleted).toBe(true);
    expect(handle.result).toEqual(['first', 'second']);
  });

  test('send to nonexistent workflow throws', () => {
    const engine = new VelocityEmbedded({ logLevel: 'silent' });
    expect(() => engine.send('nonexistent', 'topic', 'val')).toThrow();
  });
});

// ─── State Persistence Tests ─────────────────────────────────────────────────

describe('State Persistence', () => {
  test('state persisted in journal', async () => {
    const storage = new EmbeddedInMemoryStorage();
    const engine = new VelocityEmbedded(undefined, storage);
    engine.register(StatefulWorkflow);

    const handle = await engine.execute<string>('StatefulWorkflow', 'run', 'wf-state', 'Alice');
    expect(handle.isCompleted).toBe(true);
    expect(handle.result).toBe('Hello, Alice!');

    const stored = storage.loadJournal('wf-state');
    expect(stored).not.toBeNull();
    expect(stored!.state.name).toBe('Alice');
    expect(stored!.state.processed).toBe(true);
  });
});

// ─── End-to-End Tests ────────────────────────────────────────────────────────

describe('End-to-End', () => {
  test('persist and replay across engine instances', async () => {
    const storage = new EmbeddedInMemoryStorage();

    // First engine
    const engine1 = new VelocityEmbedded({ logLevel: 'silent' }, storage);
    engine1.register(MathService);

    const handle1 = await engine1.execute<number>('MathService', 'double', 'wf-e2e', 7);
    expect(handle1.result).toBe(14);

    // Second engine replays from same storage
    const engine2 = new VelocityEmbedded({ logLevel: 'silent' }, storage);
    const wf = engine2.getWorkflow('wf-e2e');
    expect(wf).toBeDefined();
    expect(wf!.output).toBe(14);
    expect(wf!.status).toBe(WorkflowStatus.COMPLETED);
  });
});
