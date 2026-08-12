import {
  VirtualObject, Service, Workflow,
  ObjectContext, Context, WorkflowContext,
  Awakeable, DurablePromise, HandlerKind,
  RuntimeServer, createApp,
} from '../src/index';

// ─── Virtual Object Tests ──────────────────────────────────────────────────

describe('VirtualObject', () => {
  test('creation and handler registration', () => {
    const chat = new VirtualObject('ChatAgent');
    chat.addHandler('message', async (ctx: ObjectContext, query: string) => `reply: ${query}`);
    expect(chat.name).toBe('ChatAgent');
    expect(chat.getHandler('message')).toBeDefined();
  });

  test('invocation and completion', async () => {
    const chat = new VirtualObject('ChatAgent');
    chat.addHandler('message', async (ctx: ObjectContext, query: string) => `reply: ${query}`);

    const server = new RuntimeServer();
    server.register(chat);

    const invId = await server.invoke('ChatAgent', 'message', 'session-1', 'hello');
    await new Promise(r => setTimeout(r, 50));

    const inv = server.getInvocation(invId);
    expect(inv?.state).toBe('completed');
    expect(inv?.outputData).toBe('reply: hello');
  });

  test('state persistence across invocations', async () => {
    const chat = new VirtualObject('ChatAgent');
    chat.addHandler('message', async (ctx: ObjectContext, query: string) => {
      const history = (await ctx.get('history')) || [];
      history.push(query);
      await ctx.set('history', history);
      return history.length;
    });

    const server = new RuntimeServer();
    server.register(chat);

    const inv1 = await server.invoke('ChatAgent', 'message', 's1', 'hello');
    await new Promise(r => setTimeout(r, 50));
    expect(server.getInvocation(inv1)?.outputData).toBe(1);

    const inv2 = await server.invoke('ChatAgent', 'message', 's1', 'world');
    await new Promise(r => setTimeout(r, 50));
    expect(server.getInvocation(inv2)?.outputData).toBe(2);
  });

  test('state isolation between keys', async () => {
    const chat = new VirtualObject('ChatAgent');
    chat.addHandler('message', async (ctx: ObjectContext, query: string) => {
      const count = ((await ctx.get('count')) || 0) + 1;
      await ctx.set('count', count);
      return count;
    });

    const server = new RuntimeServer();
    server.register(chat);

    const inv1 = await server.invoke('ChatAgent', 'message', 's1', 'a');
    const inv2 = await server.invoke('ChatAgent', 'message', 's2', 'b');
    await new Promise(r => setTimeout(r, 50));

    expect(server.getInvocation(inv1)?.outputData).toBe(1);
    expect(server.getInvocation(inv2)?.outputData).toBe(1);
  });
});

// ─── Service Tests ─────────────────────────────────────────────────────────

describe('Service', () => {
  test('stateless handler', async () => {
    const payment = new Service('PaymentService');
    payment.addHandler('charge', async (ctx: Context, amount: number) => ({ status: 'charged', amount }));

    const server = new RuntimeServer();
    server.register(payment);

    const invId = await server.invoke('PaymentService', 'charge', '', 99.99);
    await new Promise(r => setTimeout(r, 50));

    const inv = server.getInvocation(invId);
    expect(inv?.state).toBe('completed');
    expect(inv?.outputData).toEqual({ status: 'charged', amount: 99.99 });
  });
});

// ─── Workflow Tests ────────────────────────────────────────────────────────

describe('Workflow', () => {
  test('multi-step durable workflow', async () => {
    const orderWf = new Workflow('OrderWorkflow');
    orderWf.addHandler('run', async (ctx: WorkflowContext, orderId: string) => {
      const step1 = await ctx.run(() => `charged-${orderId}`);
      const step2 = await ctx.run(() => `shipped-${orderId}`);
      return [step1, step2];
    });

    const server = new RuntimeServer();
    server.register(orderWf);

    const invId = await server.invoke('OrderWorkflow', 'run', 'order-123', 'order-123');
    await new Promise(r => setTimeout(r, 50));

    const inv = server.getInvocation(invId);
    expect(inv?.state).toBe('completed');
    expect(inv?.outputData).toEqual(['charged-order-123', 'shipped-order-123']);
    expect(inv?.journal.length).toBe(2);
  });
});

// ─── Awakeable Tests ───────────────────────────────────────────────────────

describe('Awakeable', () => {
  test('resolve and wait', async () => {
    const awk = new Awakeable('test-awk-1');
    expect(awk.resolved).toBe(false);
    awk.resolve('approved');
    expect(awk.resolved).toBe(true);
    const result = await awk.wait();
    expect(result).toBe('approved');
  });

  test('reject and wait', async () => {
    const awk = new Awakeable('test-awk-2');
    awk.reject('timeout');
    await expect(awk.wait()).rejects.toThrow('timeout');
  });
});

// ─── DurablePromise Tests ──────────────────────────────────────────────────

describe('DurablePromise', () => {
  test('resolve and await', async () => {
    const promise = new DurablePromise('approval-1');
    expect(promise.pending).toBe(true);
    promise.resolve('approved');
    expect(promise.resolved).toBe(true);
    const result = await promise.awaitValue();
    expect(result).toBe('approved');
  });

  test('reject and await', async () => {
    const promise = new DurablePromise('approval-2');
    promise.reject('denied');
    await expect(promise.awaitValue()).rejects.toThrow('denied');
  });

  test('double resolve throws', () => {
    const promise = new DurablePromise('approval-3');
    promise.resolve('ok');
    expect(() => promise.resolve('again')).toThrow();
  });
});

// ─── App Factory Tests ─────────────────────────────────────────────────────

describe('createApp', () => {
  test('registers multiple services', () => {
    const chat = new VirtualObject('Chat');
    const payment = new Service('Payment');
    chat.addHandler('msg', async () => 'ok');
    payment.addHandler('charge', async () => 'ok');

    const server = createApp([chat, payment]);
    expect(server.listServices()).toContain('Chat');
    expect(server.listServices()).toContain('Payment');
  });
});

// ─── Stats Tests ───────────────────────────────────────────────────────────

describe('RuntimeServer stats', () => {
  test('tracks invocations', async () => {
    const svc = new VirtualObject('Svc');
    svc.addHandler('handler', async () => 'ok');

    const server = new RuntimeServer();
    server.register(svc);

    await server.invoke('Svc', 'handler', 'k1', 'a');
    await server.invoke('Svc', 'handler', 'k2', 'b');
    await new Promise(r => setTimeout(r, 50));

    const stats = server.getStats();
    expect(stats.registeredServices).toBe(1);
    expect(stats.totalInvocations).toBe(2);
    expect(stats.completedInvocations).toBe(2);
  });
});

// ─── Idempotency Tests ─────────────────────────────────────────────────────

describe('Idempotency', () => {
  test('same key returns same invocation', async () => {
    const svc = new Service('Svc');
    svc.addHandler('handler', async () => 'ok');

    const server = new RuntimeServer();
    server.register(svc);

    const id1 = await server.invoke('Svc', 'handler', '', 'x', 'idem-1');
    const id2 = await server.invoke('Svc', 'handler', '', 'x', 'idem-1');
    expect(id1).toBe(id2);
  });
});
