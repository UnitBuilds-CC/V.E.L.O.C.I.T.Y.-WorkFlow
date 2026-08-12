import {
  // Core
  VirtualObject, Service, Workflow,
  ObjectContext, Context, WorkflowContext,
  RuntimeServer, createApp,
  // Errors
  VelocityError, ServiceNotFoundError, HandlerNotFoundError,
  InvocationTimeoutError, ShutdownError, AwakeableNotFoundError,
  DoubleResolveError,
  // Config
  createConfig, DEFAULT_CONFIG,
  // Logger
  createLogger, Logger,
  // Metrics
  MetricsCollector, Counter, Histogram,
  // Health
  HealthChecker, HealthCheckResult, HealthStatus,
  // Retry
  RetryPolicy, DEFAULT_RETRY_POLICY, NO_RETRY_POLICY,
  shouldRetry, getRetryDelay,
  // Middleware
  MiddlewareChain, MiddlewareContext, MiddlewareFn,
  loggingMiddleware, metricsMiddleware, timeoutMiddleware,
} from '../src/index';

// ─── Error Tests ────────────────────────────────────────────────────────────

describe('Error hierarchy', () => {
  test('ServiceNotFoundError', () => {
    const err = new ServiceNotFoundError('FooService');
    expect(err).toBeInstanceOf(VelocityError);
    expect(err.code).toBe('SERVICE_NOT_FOUND');
    expect(err.serviceName).toBe('FooService');
    expect(err.message).toContain('FooService');
  });

  test('HandlerNotFoundError', () => {
    const err = new HandlerNotFoundError('Svc', 'handler');
    expect(err.code).toBe('HANDLER_NOT_FOUND');
    expect(err.serviceName).toBe('Svc');
    expect(err.handlerName).toBe('handler');
  });

  test('InvocationTimeoutError', () => {
    const err = new InvocationTimeoutError('inv-1', 5000);
    expect(err.code).toBe('TIMEOUT');
    expect(err.timeoutMs).toBe(5000);
  });

  test('ShutdownError', () => {
    const err = new ShutdownError();
    expect(err.code).toBe('SHUTDOWN');
  });

  test('DoubleResolveError', () => {
    const err = new DoubleResolveError('p-1', 'promise');
    expect(err.code).toBe('DOUBLE_RESOLVE');
    expect(err.message).toContain('Promise');
  });

  test('AwakeableNotFoundError', () => {
    const err = new AwakeableNotFoundError('awk-1');
    expect(err.code).toBe('AWAKEABLE_NOT_FOUND');
  });
});

// ─── Config Tests ───────────────────────────────────────────────────────────

describe('Config', () => {
  test('defaults', () => {
    const cfg = createConfig();
    expect(cfg.port).toBe(9080);
    expect(cfg.maxRetries).toBe(3);
    expect(cfg.enableMetrics).toBe(true);
  });

  test('overrides', () => {
    const cfg = createConfig({ port: 8888, logLevel: 'debug' });
    expect(cfg.port).toBe(8888);
    expect(cfg.logLevel).toBe('debug');
  });

  test('validation rejects bad port', () => {
    expect(() => createConfig({ port: 0 })).toThrow();
  });

  test('validation rejects negative timeout', () => {
    expect(() => createConfig({ defaultInvocationTimeoutMs: -1 })).toThrow();
  });
});

// ─── Logger Tests ───────────────────────────────────────────────────────────

describe('Logger', () => {
  test('silent logger produces no output', () => {
    const spy = jest.spyOn(console, 'info').mockImplementation();
    const logger = createLogger('silent');
    logger.info('test');
    expect(spy).not.toHaveBeenCalled();
    spy.mockRestore();
  });

  test('info logger outputs info', () => {
    const spy = jest.spyOn(console, 'info').mockImplementation();
    const logger = createLogger('info');
    logger.info('test message');
    expect(spy).toHaveBeenCalled();
    spy.mockRestore();
  });
});

// ─── Metrics Tests ──────────────────────────────────────────────────────────

describe('Metrics', () => {
  test('Counter', () => {
    const c = new Counter('test', 'test counter');
    expect(c.value).toBe(0);
    c.inc();
    expect(c.value).toBe(1);
    c.inc(5);
    expect(c.value).toBe(6);
    c.reset();
    expect(c.value).toBe(0);
  });

  test('Histogram', () => {
    const h = new Histogram('test', 'test hist');
    h.observe(10); h.observe(20); h.observe(30);
    expect(h.count).toBe(3);
    expect(h.sum).toBe(60);
    expect(h.avg).toBe(20);
    expect(h.min).toBe(10);
    expect(h.max).toBe(30);
    h.reset();
    expect(h.count).toBe(0);
  });

  test('MetricsCollector', () => {
    const mc = new MetricsCollector();
    mc.recordStart('Svc', 'h');
    mc.recordComplete('Svc', 'h', 42, true);
    mc.recordStart('Svc', 'h');
    mc.recordComplete('Svc', 'h', 100, false);

    const snap = mc.snapshot();
    expect(snap.counters.invocationsTotal).toBe(2);
    expect(snap.counters.invocationsSuccess).toBe(1);
    expect(snap.counters.invocationsFailed).toBe(1);
    expect(snap.histograms.invocationDuration.count).toBe(2);
  });

  test('MetricsCollector reset', () => {
    const mc = new MetricsCollector();
    mc.recordStart('Svc', 'h');
    mc.reset();
    expect(mc.snapshot().counters.invocationsTotal).toBe(0);
  });
});

// ─── Health Tests ───────────────────────────────────────────────────────────

describe('Health', () => {
  test('healthy check', async () => {
    const hc = new HealthChecker();
    hc.register('liveness', () => ({ name: 'liveness', status: 'healthy', message: 'alive', latencyMs: 0 }));
    const status = await hc.check();
    expect(status.status).toBe('healthy');
    expect(status.checks).toHaveLength(1);
  });

  test('unhealthy check', async () => {
    const hc = new HealthChecker();
    hc.register('disk', () => { throw new Error('disk full'); });
    const status = await hc.check();
    expect(status.status).toBe('unhealthy');
    expect(status.checks[0].message).toContain('disk full');
  });

  test('degraded check', async () => {
    const hc = new HealthChecker();
    hc.register('memory', () => ({ name: 'memory', status: 'degraded', message: 'high', latencyMs: 0 }));
    const status = await hc.check();
    expect(status.status).toBe('degraded');
  });

  test('uptime tracking', async () => {
    const hc = new HealthChecker();
    hc.register('test', () => ({ name: 'test', status: 'healthy', latencyMs: 0 }));
    const status = await hc.check();
    expect(status.uptimeSeconds).toBeGreaterThanOrEqual(0);
    expect(status.timestamp).toBeGreaterThan(0);
  });
});

// ─── Retry Tests ────────────────────────────────────────────────────────────

describe('Retry', () => {
  test('shouldRetry respects maxAttempts', () => {
    const p: RetryPolicy = { maxAttempts: 3, initialDelayMs: 10, maxDelayMs: 100, backoffMultiplier: 2, jitter: false };
    expect(shouldRetry(p, new Error('x'), 1)).toBe(true);
    expect(shouldRetry(p, new Error('x'), 2)).toBe(true);
    expect(shouldRetry(p, new Error('x'), 3)).toBe(false);
  });

  test('shouldRetry respects nonRetryableErrors', () => {
    const p: RetryPolicy = { maxAttempts: 3, initialDelayMs: 10, maxDelayMs: 100, backoffMultiplier: 2, jitter: false, nonRetryableErrors: ['ValidationError'] };
    const ve = Object.assign(new Error('bad'), { name: 'ValidationError' });
    expect(shouldRetry(p, ve, 1)).toBe(false);
    expect(shouldRetry(p, new Error('transient'), 1)).toBe(true);
  });

  test('getRetryDelay exponential', () => {
    const p: RetryPolicy = { maxAttempts: 5, initialDelayMs: 100, maxDelayMs: 10000, backoffMultiplier: 2, jitter: false };
    expect(getRetryDelay(p, 1)).toBe(100);
    expect(getRetryDelay(p, 2)).toBe(200);
    expect(getRetryDelay(p, 3)).toBe(400);
  });

  test('getRetryDelay capped at max', () => {
    const p: RetryPolicy = { maxAttempts: 5, initialDelayMs: 100, maxDelayMs: 250, backoffMultiplier: 2, jitter: false };
    expect(getRetryDelay(p, 3)).toBe(250);
  });

  test('NO_RETRY_POLICY', () => {
    expect(NO_RETRY_POLICY.maxAttempts).toBe(1);
  });
});

// ─── Middleware Tests ───────────────────────────────────────────────────────

describe('Middleware', () => {
  test('MiddlewareChain ordering', () => {
    const chain = new MiddlewareChain();
    const calls: string[] = [];
    chain.use(async (ctx, next) => { calls.push('a:before'); const r = await next(); calls.push('a:after'); return r; });
    chain.use(async (ctx, next) => { calls.push('b:before'); const r = await next(); calls.push('b:after'); return r; });
    expect(chain.getChain('any')).toHaveLength(2);
  });

  test('per-service middleware', () => {
    const chain = new MiddlewareChain();
    const globalMw: MiddlewareFn = async (ctx, next) => next();
    const svcMw: MiddlewareFn = async (ctx, next) => next();
    chain.use(globalMw);
    chain.useFor('Payment', svcMw);
    expect(chain.getChain('Payment')).toHaveLength(2);
    expect(chain.getChain('Other')).toHaveLength(1);
  });

  test('clear removes all middleware', () => {
    const chain = new MiddlewareChain();
    chain.use(async (ctx, next) => next());
    chain.useFor('Svc', async (ctx, next) => next());
    chain.clear();
    expect(chain.getChain('Svc')).toHaveLength(0);
  });
});

// ─── Server Production Feature Tests ────────────────────────────────────────

describe('Server production features', () => {
  test('ServiceNotFoundError on unknown service', async () => {
    const server = new RuntimeServer({ logLevel: 'silent' });
    await expect(server.invoke('NonExistent', 'handler')).rejects.toThrow(ServiceNotFoundError);
  });

  test('HandlerNotFoundError on unknown handler', async () => {
    const svc = new Service('Svc');
    const server = new RuntimeServer({ logLevel: 'silent' });
    server.register(svc);
    await expect(server.invoke('Svc', 'nonexistent')).rejects.toThrow(HandlerNotFoundError);
  });

  test('duplicate registration throws', () => {
    const svc = new Service('Svc');
    const server = new RuntimeServer({ logLevel: 'silent' });
    server.register(svc);
    expect(() => server.register(svc)).toThrow('already registered');
  });

  test('listInvocations with filters', async () => {
    const svc = new Service('Svc');
    svc.addHandler('handler', async (ctx: Context, data: string) => data);
    const server = new RuntimeServer({ logLevel: 'silent' });
    server.register(svc);

    await server.invoke('Svc', 'handler', '', 'a');
    await server.invoke('Svc', 'handler', '', 'b');
    await new Promise(r => setTimeout(r, 50));

    const all = server.listInvocations();
    expect(all).toHaveLength(2);
    const completed = server.listInvocations({ state: 'completed' });
    expect(completed).toHaveLength(2);
  });

  test('graceful shutdown', async () => {
    const svc = new Service('Svc');
    svc.addHandler('handler', async (ctx: Context) => 'ok');
    const server = new RuntimeServer({ logLevel: 'silent' });
    server.register(svc);

    await server.shutdown(100);
    expect(server.isShuttingDown).toBe(true);
    await expect(server.invoke('Svc', 'handler')).rejects.toThrow(ShutdownError);
  });

  test('health check', async () => {
    const svc = new Service('Svc');
    svc.addHandler('handler', async () => 'ok');
    const server = new RuntimeServer({ logLevel: 'silent' });
    server.register(svc);

    const status = await server.healthCheck();
    expect(status.status).toBe('healthy');
    const names = status.checks.map(c => c.name);
    expect(names).toContain('liveness');
    expect(names).toContain('readiness');
  });

  test('stats with metrics', async () => {
    const svc = new Service('Svc');
    svc.addHandler('handler', async () => 'ok');
    const server = new RuntimeServer({ logLevel: 'silent' });
    server.register(svc);

    await server.invoke('Svc', 'handler', '', 'x');
    await new Promise(r => setTimeout(r, 50));

    const stats = server.getStats();
    expect(stats.metrics).toBeDefined();
    expect(stats.uptimeSeconds).toBeGreaterThanOrEqual(0);
    expect(stats.shuttingDown).toBe(false);
  });

  test('config integration', () => {
    const server = new RuntimeServer({ logLevel: 'silent', maxRetries: 5, port: 8888 });
    expect(server.config.maxRetries).toBe(5);
    expect(server.config.port).toBe(8888);
  });

  test('invocation record has timestamps', async () => {
    const svc = new Service('Svc');
    svc.addHandler('handler', async () => 'ok');
    const server = new RuntimeServer({ logLevel: 'silent' });
    server.register(svc);

    const invId = await server.invoke('Svc', 'handler');
    await new Promise(r => setTimeout(r, 50));

    const inv = server.getInvocation(invId);
    expect(inv!.createdAt).toBeGreaterThan(0);
    expect(inv!.startedAt).toBeGreaterThan(0);
    expect(inv!.completedAt).toBeGreaterThan(0);
    expect(inv!.state).toBe('completed');
  });
});
