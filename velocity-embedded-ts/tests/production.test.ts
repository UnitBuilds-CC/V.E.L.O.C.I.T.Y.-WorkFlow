import {
  // Core
  Durable, Transaction, DurableContext, TransactionContext,
  VelocityEmbedded, WorkflowHandle, WorkflowStatus, createEmbedded,
  // Errors
  VelocityEmbeddedError, ClassNotFoundError, MethodNotFoundError,
  WorkflowNotFoundError, WorkflowExecutionError, DuplicateRegistrationError,
  EmbeddedShutdownError,
  // Config
  createEmbeddedConfig, DEFAULT_EMBEDDED_CONFIG,
  // Logger
  createEmbeddedLogger, EmbeddedLogger,
  // Metrics
  EmbeddedMetrics,
} from '../src/index';

// ─── Error Tests ────────────────────────────────────────────────────────────

describe('Error hierarchy', () => {
  test('ClassNotFoundError', () => {
    const err = new ClassNotFoundError('OrderWorkflow');
    expect(err).toBeInstanceOf(VelocityEmbeddedError);
    expect(err.code).toBe('CLASS_NOT_FOUND');
    expect(err.className).toBe('OrderWorkflow');
  });

  test('MethodNotFoundError', () => {
    const err = new MethodNotFoundError('OrderWorkflow', 'process');
    expect(err.code).toBe('METHOD_NOT_FOUND');
    expect(err.methodName).toBe('process');
  });

  test('WorkflowNotFoundError', () => {
    const err = new WorkflowNotFoundError('wf-1');
    expect(err.code).toBe('WORKFLOW_NOT_FOUND');
  });

  test('DuplicateRegistrationError', () => {
    const err = new DuplicateRegistrationError('MyClass');
    expect(err.code).toBe('DUPLICATE_REGISTRATION');
  });

  test('EmbeddedShutdownError', () => {
    const err = new EmbeddedShutdownError();
    expect(err.code).toBe('SHUTDOWN');
  });

  test('WorkflowExecutionError', () => {
    const cause = new Error('boom');
    const err = new WorkflowExecutionError('wf-1', cause);
    expect(err.code).toBe('WORKFLOW_EXECUTION_ERROR');
    expect(err.cause).toBe(cause);
  });
});

// ─── Config Tests ───────────────────────────────────────────────────────────

describe('Config', () => {
  test('defaults', () => {
    const cfg = createEmbeddedConfig();
    expect(cfg.maxConcurrentWorkflows).toBe(100);
    expect(cfg.defaultTimeoutMs).toBe(60_000);
    expect(cfg.logLevel).toBe('info');
  });

  test('overrides', () => {
    const cfg = createEmbeddedConfig({ maxRetries: 5, logLevel: 'debug' });
    expect(cfg.maxRetries).toBe(5);
    expect(cfg.logLevel).toBe('debug');
  });

  test('validation rejects bad concurrency', () => {
    expect(() => createEmbeddedConfig({ maxConcurrentWorkflows: 0 })).toThrow();
  });
});

// ─── Logger Tests ───────────────────────────────────────────────────────────

describe('Logger', () => {
  test('silent logger produces no output', () => {
    const spy = jest.spyOn(console, 'info').mockImplementation();
    const logger = createEmbeddedLogger('silent');
    logger.info('test');
    expect(spy).not.toHaveBeenCalled();
    spy.mockRestore();
  });

  test('info logger outputs', () => {
    const spy = jest.spyOn(console, 'info').mockImplementation();
    const logger = createEmbeddedLogger('info');
    logger.info('test');
    expect(spy).toHaveBeenCalled();
    spy.mockRestore();
  });
});

// ─── Metrics Tests ──────────────────────────────────────────────────────────

describe('EmbeddedMetrics', () => {
  test('tracks workflows', () => {
    const m = new EmbeddedMetrics();
    m.recordStart();
    m.recordComplete(42);
    m.recordStart();
    m.recordFailure();

    const snap = m.snapshot();
    expect(snap.workflowsTotal).toBe(2);
    expect(snap.workflowsCompleted).toBe(1);
    expect(snap.workflowsFailed).toBe(1);
    expect(snap.workflowDuration.count).toBe(1);
    expect(snap.workflowDuration.avg).toBe(42);
  });

  test('reset', () => {
    const m = new EmbeddedMetrics();
    m.recordStart();
    m.reset();
    expect(m.snapshot().workflowsTotal).toBe(0);
  });
});

// ─── Engine Production Feature Tests ────────────────────────────────────────

@Durable()
class TestWorkflow {
  async process(ctx: DurableContext, input: string): Promise<string> {
    const step1 = await ctx.run('step1', () => input.toUpperCase());
    return step1;
  }

  async failing(ctx: DurableContext): Promise<string> {
    throw new Error('intentional failure');
  }
}

describe('Engine production features', () => {
  test('ClassNotFoundError on unknown class', async () => {
    const engine = new VelocityEmbedded({ logLevel: 'silent' });
    await expect(engine.execute('Unknown', 'method', 'wf-1')).rejects.toThrow(ClassNotFoundError);
  });

  test('MethodNotFoundError on unknown method', async () => {
    const engine = new VelocityEmbedded({ logLevel: 'silent' });
    engine.register(TestWorkflow);
    await expect(engine.execute('TestWorkflow', 'nonexistent', 'wf-1')).rejects.toThrow(MethodNotFoundError);
  });

  test('DuplicateRegistrationError', () => {
    const engine = new VelocityEmbedded({ logLevel: 'silent' });
    engine.register(TestWorkflow);
    expect(() => engine.register(TestWorkflow)).toThrow(DuplicateRegistrationError);
  });

  test('listWorkflows with filters', async () => {
    const engine = new VelocityEmbedded({ logLevel: 'silent' });
    engine.register(TestWorkflow);
    await engine.execute('TestWorkflow', 'process', 'wf-1', 'hello');
    await engine.execute('TestWorkflow', 'process', 'wf-2', 'world');

    const all = engine.listWorkflows();
    expect(all).toHaveLength(2);

    const completed = engine.listWorkflows({ status: WorkflowStatus.COMPLETED });
    expect(completed).toHaveLength(2);
  });

  test('graceful shutdown', async () => {
    const engine = new VelocityEmbedded({ logLevel: 'silent' });
    engine.register(TestWorkflow);
    await engine.shutdown(100);
    expect(engine.isShuttingDown).toBe(true);
    await expect(engine.execute('TestWorkflow', 'process', 'wf-1', 'test')).rejects.toThrow(EmbeddedShutdownError);
  });

  test('health check', async () => {
    const engine = new VelocityEmbedded({ logLevel: 'silent' });
    engine.register(TestWorkflow);
    const status = await engine.healthCheck();
    expect(status.status).toBe('healthy');
    expect(status.checks.some(c => c.name === 'liveness')).toBe(true);
    expect(status.checks.some(c => c.name === 'readiness')).toBe(true);
  });

  test('health check degraded when no classes', async () => {
    const engine = new VelocityEmbedded({ logLevel: 'silent' });
    const status = await engine.healthCheck();
    expect(status.status).toBe('degraded');
  });

  test('stats with metrics', async () => {
    const engine = new VelocityEmbedded({ logLevel: 'silent' });
    engine.register(TestWorkflow);
    await engine.execute('TestWorkflow', 'process', 'wf-1', 'hello');

    const stats = engine.getStats();
    expect(stats.metrics).toBeDefined();
    expect(stats.uptimeSeconds).toBeGreaterThanOrEqual(0);
    expect(stats.completed).toBe(1);
  });

  test('config integration', () => {
    const engine = new VelocityEmbedded({ logLevel: 'silent', maxRetries: 5 });
    expect(engine.config.maxRetries).toBe(5);
  });

  test('workflow record has timestamps', async () => {
    const engine = new VelocityEmbedded({ logLevel: 'silent' });
    engine.register(TestWorkflow);
    await engine.execute('TestWorkflow', 'process', 'wf-1', 'hello');

    const wf = engine.getWorkflow('wf-1');
    expect(wf).toBeDefined();
    expect(wf!.createdAt).toBeGreaterThan(0);
    expect(wf!.startedAt).toBeGreaterThan(0);
    expect(wf!.completedAt).toBeGreaterThan(0);
  });

  test('failed workflow has errorCode', async () => {
    const engine = new VelocityEmbedded({ logLevel: 'silent' });
    engine.register(TestWorkflow);
    const handle = await engine.execute('TestWorkflow', 'failing', 'wf-fail');
    expect(handle.isFailed).toBe(true);
    const wf = engine.getWorkflow('wf-fail');
    expect(wf!.errorCode).toBe('UNKNOWN');
  });
});
