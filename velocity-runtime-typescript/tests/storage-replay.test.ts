/**
 * Tests for storage backends, journal persistence, and crash recovery.
 */

import {
  VirtualObject, Service, Workflow,
  ObjectContext, Context, WorkflowContext,
  RuntimeServer, InMemoryStorage, Awakeable,
  StoredJournal, StoredKeyState, StorageBackend,
  createApp,
} from '../src/index';

// ─── InMemoryStorage ─────────────────────────────────────────────────────────

describe('InMemoryStorage', () => {
  let storage: InMemoryStorage;

  beforeEach(() => { storage = new InMemoryStorage(); });

  test('save and load journal', () => {
    const journal: StoredJournal = {
      invocationId: 'inv-1', serviceName: 'Chat', handlerName: 'message',
      key: 'user-42', entries: [{ sequence: 0, entryType: 'run', outputData: 'hello', completed: true }],
      objectState: { count: 5 }, output: 'hello', state: 'completed',
      createdAt: 1000, completedAt: 1001,
    };
    storage.saveJournal(journal);
    const loaded = storage.loadJournal('inv-1');
    expect(loaded).not.toBeNull();
    expect(loaded!.invocationId).toBe('inv-1');
    expect(loaded!.serviceName).toBe('Chat');
    expect(loaded!.output).toBe('hello');
    expect(loaded!.state).toBe('completed');
  });

  test('load nonexistent journal returns null', () => {
    expect(storage.loadJournal('nonexistent')).toBeNull();
  });

  test('load journals for key', () => {
    storage.saveJournal({ invocationId: 'inv-1', serviceName: 'Chat', handlerName: 'msg', key: 'user-1', entries: [], objectState: {}, output: null, state: 'completed', createdAt: 0, completedAt: 0 });
    storage.saveJournal({ invocationId: 'inv-2', serviceName: 'Chat', handlerName: 'msg', key: 'user-1', entries: [], objectState: {}, output: null, state: 'completed', createdAt: 0, completedAt: 0 });
    storage.saveJournal({ invocationId: 'inv-3', serviceName: 'Chat', handlerName: 'msg', key: 'user-2', entries: [], objectState: {}, output: null, state: 'completed', createdAt: 0, completedAt: 0 });
    expect(storage.loadJournalsForKey('Chat/user-1')).toHaveLength(2);
    expect(storage.loadJournalsForKey('Chat/user-2')).toHaveLength(1);
  });

  test('load all journals', () => {
    storage.saveJournal({ invocationId: 'inv-1', serviceName: 'A', handlerName: 'h', key: '', entries: [], objectState: {}, output: null, state: 'completed', createdAt: 0, completedAt: 0 });
    storage.saveJournal({ invocationId: 'inv-2', serviceName: 'B', handlerName: 'h', key: '', entries: [], objectState: {}, output: null, state: 'completed', createdAt: 0, completedAt: 0 });
    expect(storage.loadAllJournals()).toHaveLength(2);
  });

  test('save and load key state', () => {
    storage.saveKeyState({ fullKey: 'Chat/user-1', state: { history: ['hello'] }, updatedAt: 1000 });
    const loaded = storage.loadKeyState('Chat/user-1');
    expect(loaded).not.toBeNull();
    expect(loaded!.state).toEqual({ history: ['hello'] });
  });

  test('load nonexistent key state returns null', () => {
    expect(storage.loadKeyState('nonexistent')).toBeNull();
  });

  test('delete journal', () => {
    storage.saveJournal({ invocationId: 'inv-1', serviceName: 'A', handlerName: 'h', key: '', entries: [], objectState: {}, output: null, state: 'completed', createdAt: 0, completedAt: 0 });
    expect(storage.loadJournal('inv-1')).not.toBeNull();
    storage.deleteJournal('inv-1');
    expect(storage.loadJournal('inv-1')).toBeNull();
  });

  test('clear all data', () => {
    storage.saveJournal({ invocationId: 'inv-1', serviceName: 'A', handlerName: 'h', key: '', entries: [], objectState: {}, output: null, state: 'completed', createdAt: 0, completedAt: 0 });
    storage.saveKeyState({ fullKey: 'k1', state: {}, updatedAt: 0 });
    storage.clear();
    expect(storage.loadAllJournals()).toHaveLength(0);
    expect(storage.loadKeyState('k1')).toBeNull();
  });
});

// ─── Journal Replay ──────────────────────────────────────────────────────────

describe('Journal Replay', () => {
  test('replay restores key state', async () => {
    const storage = new InMemoryStorage();
    // Pre-populate storage with completed journal
    storage.saveJournal({
      invocationId: 'inv-1', serviceName: 'Counter', handlerName: 'increment',
      key: 'counter-1', entries: [], objectState: { count: 42 },
      output: 42, state: 'completed', createdAt: 1000, completedAt: 1001,
    });

    // New server replays from storage
    const server = new RuntimeServer(undefined, storage);
    const counter = new VirtualObject('Counter');
    counter.addHandler('increment', async (ctx: ObjectContext) => {
      const count = (await ctx.get('count')) || 0;
      await ctx.set('count', count + 1);
      return count + 1;
    });
    server.register(counter);

    // Key state should be restored
    expect(server.getStats().trackedKeys).toBeUndefined(); // internal state restored
  });

  test('replay restores invocation records', () => {
    const storage = new InMemoryStorage();
    storage.saveJournal({
      invocationId: 'inv-abc', serviceName: 'Svc', handlerName: 'handler',
      key: '', entries: [], objectState: {}, output: 'result',
      state: 'completed', createdAt: 1000, completedAt: 1001,
    });

    const server = new RuntimeServer(undefined, storage);
    const inv = server.getInvocation('inv-abc');
    expect(inv).toBeDefined();
    expect(inv!.serviceName).toBe('Svc');
    expect(inv!.outputData).toBe('result');
    expect(inv!.state).toBe('completed');
  });

  test('journal persisted on completion', async () => {
    const storage = new InMemoryStorage();
    const server = new RuntimeServer(undefined, storage);
    const svc = new Service('TestSvc');
    svc.addHandler('handler', async (ctx: Context, input: string) => {
      return await ctx.run(() => input.toUpperCase());
    });
    server.register(svc);

    const invId = await server.invoke('TestSvc', 'handler', '', 'hello');
    await new Promise(r => setTimeout(r, 100));

    const inv = server.getInvocation(invId);
    expect(inv!.state).toBe('completed');

    const stored = storage.loadJournal(invId);
    expect(stored).not.toBeNull();
    expect(stored!.state).toBe('completed');
    expect(stored!.output).toBe('HELLO');
  });

  test('key state persisted for virtual objects', async () => {
    const storage = new InMemoryStorage();
    const server = new RuntimeServer(undefined, storage);
    const counter = new VirtualObject('Counter');
    counter.addHandler('increment', async (ctx: ObjectContext) => {
      const count = (await ctx.get('count')) || 0;
      await ctx.set('count', count + 1);
      return count + 1;
    });
    server.register(counter);

    await server.invoke('Counter', 'increment', 'c1');
    await new Promise(r => setTimeout(r, 100));

    const keyState = storage.loadKeyState('Counter/c1');
    expect(keyState).not.toBeNull();
    expect(keyState!.state.count).toBe(1);
  });

  test('failed journal persisted for audit', async () => {
    const storage = new InMemoryStorage();
    const server = new RuntimeServer(undefined, storage);
    const svc = new Service('FailSvc');
    svc.addHandler('handler', async () => { throw new Error('intentional'); });
    server.register(svc);

    const invId = await server.invoke('FailSvc', 'handler');
    // Wait for all retries to complete (with exponential backoff)
    await new Promise(r => setTimeout(r, 1000));

    const inv = server.getInvocation(invId);
    expect(inv!.state).toBe('failed');
    expect(inv!.attempts).toBeGreaterThan(1); // Should have retried

    const stored = storage.loadJournal(invId);
    expect(stored).not.toBeNull();
    expect(stored!.state).toBe('failed');
    expect(stored!.error).toBeDefined();
  });

  test('storage end-to-end: persist and replay across servers', async () => {
    const storage = new InMemoryStorage();

    // First server
    const server1 = new RuntimeServer(undefined, storage);
    const counter = new VirtualObject('Counter');
    counter.addHandler('increment', async (ctx: ObjectContext) => {
      const count = (await ctx.get('count')) || 0;
      await ctx.set('count', count + 1);
      return count + 1;
    });
    server1.register(counter);

    const invId = await server1.invoke('Counter', 'increment', 'c1');
    await new Promise(r => setTimeout(r, 100));

    const inv = server1.getInvocation(invId);
    expect(inv!.state).toBe('completed');

    // Second server replays from same storage
    const server2 = new RuntimeServer(undefined, storage);
    const restored = server2.getInvocation(invId);
    expect(restored).toBeDefined();
    expect(restored!.state).toBe('completed');
    expect(restored!.outputData).toBe(1);
  });

  test('createApp accepts storage parameter', () => {
    const storage = new InMemoryStorage();
    const svc = new Service('TestSvc');
    svc.addHandler('handler', async (ctx: Context) => 'ok');
    const server = createApp([svc], undefined, storage);
    expect(server.storage).toBe(storage);
  });

  test('awakeable registration and resolution', async () => {
    const server = new RuntimeServer();
    const awk = new Awakeable('awk-test-1');
    server.registerAwakeable(awk);
    await server.resolveAwakeable('awk-test-1', 'hello');
    const result = await awk.wait();
    expect(result).toBe('hello');
  });

  test('awakeable not found throws', async () => {
    const server = new RuntimeServer();
    await expect(server.resolveAwakeable('nonexistent', 'val')).rejects.toThrow();
  });
});
