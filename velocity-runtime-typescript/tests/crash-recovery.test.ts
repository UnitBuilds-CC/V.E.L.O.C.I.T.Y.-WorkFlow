import { RuntimeServer, Service, VirtualObject, InMemoryStorage, StoredJournal } from '../src/index';

describe('Crash Re-execution', () => {
  test('incomplete invocations are re-executed on restart', async () => {
    const storage = new InMemoryStorage();
    
    // Simulate a crash by saving an incomplete journal
    const incompleteJournal: StoredJournal = {
      invocationId: 'inv-crash-1',
      serviceName: 'CrashSvc',
      handlerName: 'handler',
      key: '',
      entries: [],
      objectState: {},
      output: null,
      error: undefined,
      state: 'running', // Incomplete state
      createdAt: Date.now() - 1000,
      completedAt: 0,
    };
    storage.saveJournal(incompleteJournal);
    
    // Create a new server with the storage (simulating restart)
    const server = new RuntimeServer(undefined, storage);
    const svc = new Service('CrashSvc');
    let executed = false;
    svc.addHandler('handler', async () => {
      executed = true;
      return 'recovered';
    });
    server.register(svc);
    
    // Wait for re-execution
    await new Promise(r => setTimeout(r, 200));
    
    // Verify the invocation was re-executed
    expect(executed).toBe(true);
    const inv = server.getInvocation('inv-crash-1');
    expect(inv).toBeDefined();
    expect(inv?.state).toBe('completed');
    expect(inv?.outputData).toBe('recovered');
  });

  test('queued invocations are re-executed on restart', async () => {
    const storage = new InMemoryStorage();
    
    const queuedJournal: StoredJournal = {
      invocationId: 'inv-queued-1',
      serviceName: 'QueuedSvc',
      handlerName: 'handler',
      key: '',
      entries: [],
      objectState: {},
      output: null,
      error: undefined,
      state: 'queued',
      createdAt: Date.now() - 500,
      completedAt: 0,
    };
    storage.saveJournal(queuedJournal);
    
    const server = new RuntimeServer(undefined, storage);
    const svc = new Service('QueuedSvc');
    svc.addHandler('handler', async () => 'queued-recovered');
    server.register(svc);
    
    await new Promise(r => setTimeout(r, 200));
    
    const inv = server.getInvocation('inv-queued-1');
    expect(inv?.state).toBe('completed');
    expect(inv?.outputData).toBe('queued-recovered');
  });

  test('completed invocations are not re-executed', async () => {
    const storage = new InMemoryStorage();
    
    const completedJournal: StoredJournal = {
      invocationId: 'inv-completed-1',
      serviceName: 'CompletedSvc',
      handlerName: 'handler',
      key: '',
      entries: [],
      objectState: {},
      output: 'already-done',
      error: undefined,
      state: 'completed',
      createdAt: Date.now() - 1000,
      completedAt: Date.now() - 500,
    };
    storage.saveJournal(completedJournal);
    
    const server = new RuntimeServer(undefined, storage);
    const svc = new Service('CompletedSvc');
    let executed = false;
    svc.addHandler('handler', async () => {
      executed = true;
      return 'should-not-run';
    });
    server.register(svc);
    
    await new Promise(r => setTimeout(r, 100));
    
    // Should not have executed again
    expect(executed).toBe(false);
    const inv = server.getInvocation('inv-completed-1');
    expect(inv?.outputData).toBe('already-done');
  });

  test('failed invocations are not re-executed', async () => {
    const storage = new InMemoryStorage();
    
    const failedJournal: StoredJournal = {
      invocationId: 'inv-failed-1',
      serviceName: 'FailedSvc',
      handlerName: 'handler',
      key: '',
      entries: [],
      objectState: {},
      output: null,
      error: 'previous error',
      state: 'failed',
      createdAt: Date.now() - 1000,
      completedAt: Date.now() - 500,
    };
    storage.saveJournal(failedJournal);
    
    const server = new RuntimeServer(undefined, storage);
    const svc = new Service('FailedSvc');
    let executed = false;
    svc.addHandler('handler', async () => {
      executed = true;
      return 'should-not-run';
    });
    server.register(svc);
    
    await new Promise(r => setTimeout(r, 100));
    
    expect(executed).toBe(false);
    const inv = server.getInvocation('inv-failed-1');
    expect(inv?.state).toBe('failed');
    expect(inv?.error).toBe('previous error');
  });
});

describe('Retry Execution', () => {
  test('failed invocations are retried with exponential backoff', async () => {
    const server = new RuntimeServer();
    const svc = new Service('RetrySvc');
    let attempts = 0;
    svc.addHandler('handler', async () => {
      attempts++;
      if (attempts < 3) throw new Error(`Attempt ${attempts} failed`);
      return 'success on attempt 3';
    });
    server.register(svc);
    
    const invId = await server.invoke('RetrySvc', 'handler');
    // Wait for all retries
    await new Promise(r => setTimeout(r, 1000));
    
    const inv = server.getInvocation(invId);
    expect(inv?.state).toBe('completed');
    expect(inv?.outputData).toBe('success on attempt 3');
    expect(inv?.attempts).toBe(3);
    expect(attempts).toBe(3);
  });

  test('invocation fails after max retries exhausted', async () => {
    const server = new RuntimeServer();
    const svc = new Service('AlwaysFailSvc');
    let attempts = 0;
    svc.addHandler('handler', async () => {
      attempts++;
      throw new Error('always fails');
    });
    server.register(svc);
    
    const invId = await server.invoke('AlwaysFailSvc', 'handler');
    // Wait for all retries (maxRetries=3 by default, so 4 total attempts)
    await new Promise(r => setTimeout(r, 2000));
    
    const inv = server.getInvocation(invId);
    expect(inv?.state).toBe('failed');
    expect(inv?.error).toBe('always fails');
    expect(inv?.attempts).toBe(4); // 1 initial + 3 retries
    expect(attempts).toBe(4);
  });

  test('retry respects maxRetries config', async () => {
    const server = new RuntimeServer();
    // Override config to have only 1 retry
    (server as any)._config.maxRetries = 1;
    
    const svc = new Service('LimitedRetrySvc');
    let attempts = 0;
    svc.addHandler('handler', async () => {
      attempts++;
      throw new Error('fails');
    });
    server.register(svc);
    
    const invId = await server.invoke('LimitedRetrySvc', 'handler');
    await new Promise(r => setTimeout(r, 500));
    
    const inv = server.getInvocation(invId);
    expect(inv?.state).toBe('failed');
    expect(inv?.attempts).toBe(2); // 1 initial + 1 retry
    expect(attempts).toBe(2);
  });

  test('successful invocation does not retry', async () => {
    const server = new RuntimeServer();
    const svc = new Service('SuccessSvc');
    let attempts = 0;
    svc.addHandler('handler', async () => {
      attempts++;
      return 'immediate success';
    });
    server.register(svc);
    
    const invId = await server.invoke('SuccessSvc', 'handler');
    await new Promise(r => setTimeout(r, 100));
    
    const inv = server.getInvocation(invId);
    expect(inv?.state).toBe('completed');
    expect(inv?.outputData).toBe('immediate success');
    expect(inv?.attempts).toBe(1);
    expect(attempts).toBe(1);
  });
});

describe('VirtualObject State Recovery', () => {
  test('virtual object state is restored from storage', async () => {
    const storage = new InMemoryStorage();
    
    // Save a journal with object state
    const journal: StoredJournal = {
      invocationId: 'inv-state-1',
      serviceName: 'CounterObj',
      handlerName: 'increment',
      key: 'counter-1',
      entries: [],
      objectState: { count: 5 },
      output: 5,
      error: undefined,
      state: 'completed',
      createdAt: Date.now() - 1000,
      completedAt: Date.now() - 500,
    };
    storage.saveJournal(journal);
    
    // Create new server (simulating restart)
    const server = new RuntimeServer(undefined, storage);
    const counter = new VirtualObject('CounterObj');
    counter.addHandler('increment', async (ctx) => {
      const count = (await ctx.get('count')) || 0;
      await ctx.set('count', count + 1);
      return count + 1;
    });
    counter.addHandler('get', async (ctx) => {
      return await ctx.get('count');
    });
    server.register(counter);
    
    await new Promise(r => setTimeout(r, 100));
    
    // The state should be restored
    const invId = await server.invoke('CounterObj', 'get', 'counter-1');
    await new Promise(r => setTimeout(r, 100));
    
    const inv = server.getInvocation(invId);
    expect(inv?.outputData).toBe(5); // Restored from storage
  });
});
