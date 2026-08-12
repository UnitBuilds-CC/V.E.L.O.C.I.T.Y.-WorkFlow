/**
 * Velocity Runtime — Restate-compatible durable execution SDK for TypeScript.
 *
 * Production-grade SDK with:
 * - VirtualObject: Actor-model keyed state (single-writer per key)
 * - Service: Stateless durable handlers
 * - Workflow: Long-running durable functions
 * - Context: Durable steps (ctx.run), state (ctx.get/set), promises, awakeables
 * - Error hierarchy with typed error codes
 * - Middleware pipeline (logging, metrics, timeout)
 * - Health checks (liveness, readiness)
 * - Metrics collection
 * - Graceful shutdown with drain
 * - Retry policies with exponential backoff
 * - Configuration management
 */

// ─── Error Hierarchy ─────────────────────────────────────────────────────────

export class VelocityError extends Error {
  readonly code: string;
  readonly details: Record<string, any>;

  constructor(message: string, code = 'VELOCITY_ERROR', details: Record<string, any> = {}) {
    super(message);
    this.name = new.target.name;
    this.code = code;
    this.details = details;
  }
}

export class ServiceNotFoundError extends VelocityError {
  readonly serviceName: string;
  constructor(serviceName: string) {
    super(`Service not found: ${serviceName}`, 'SERVICE_NOT_FOUND', { serviceName });
    this.serviceName = serviceName;
  }
}

export class HandlerNotFoundError extends VelocityError {
  readonly serviceName: string;
  readonly handlerName: string;
  constructor(serviceName: string, handlerName: string) {
    super(`Handler not found: ${serviceName}/${handlerName}`, 'HANDLER_NOT_FOUND', { serviceName, handlerName });
    this.serviceName = serviceName;
    this.handlerName = handlerName;
  }
}

export class InvocationTimeoutError extends VelocityError {
  readonly invocationId: string;
  readonly timeoutMs: number;
  constructor(invocationId: string, timeoutMs: number) {
    super(`Invocation timed out after ${timeoutMs}ms: ${invocationId}`, 'TIMEOUT', { invocationId, timeoutMs });
    this.invocationId = invocationId;
    this.timeoutMs = timeoutMs;
  }
}

export class ShutdownError extends VelocityError {
  constructor() {
    super('Server is shutting down', 'SHUTDOWN');
  }
}

export class AwakeableNotFoundError extends VelocityError {
  readonly awakeableId: string;
  constructor(awakeableId: string) {
    super(`Awakeable not found: ${awakeableId}`, 'AWAKEABLE_NOT_FOUND', { awakeableId });
    this.awakeableId = awakeableId;
  }
}

export class DoubleResolveError extends VelocityError {
  constructor(entityId: string, entityType = 'promise') {
    super(`${entityType.charAt(0).toUpperCase() + entityType.slice(1)} already resolved: ${entityId}`, 'DOUBLE_RESOLVE', { entityId, entityType });
  }
}

// ─── Configuration ───────────────────────────────────────────────────────────

export interface ServerConfig {
  host: string;
  port: number;
  engineUrl: string;
  maxConcurrentInvocations: number;
  maxQueueDepthPerKey: number;
  defaultInvocationTimeoutMs: number;
  shutdownGracePeriodMs: number;
  maxRetries: number;
  retryBaseDelayMs: number;
  retryMaxDelayMs: number;
  logLevel: 'debug' | 'info' | 'warn' | 'error' | 'silent';
  enableMetrics: boolean;
  enableHealthChecks: boolean;
  enableJournaling: boolean;
}

export const DEFAULT_CONFIG: ServerConfig = {
  host: '0.0.0.0',
  port: 9080,
  engineUrl: 'http://localhost:8080',
  maxConcurrentInvocations: 256,
  maxQueueDepthPerKey: 1000,
  defaultInvocationTimeoutMs: 30_000,
  shutdownGracePeriodMs: 10_000,
  maxRetries: 3,
  retryBaseDelayMs: 100,
  retryMaxDelayMs: 10_000,
  logLevel: 'info',
  enableMetrics: true,
  enableHealthChecks: true,
  enableJournaling: true,
};

export function createConfig(overrides: Partial<ServerConfig> = {}): ServerConfig {
  const config = { ...DEFAULT_CONFIG, ...overrides };
  if (config.port < 1 || config.port > 65535) throw new Error(`Invalid port: ${config.port}`);
  if (config.maxConcurrentInvocations < 1) throw new Error('maxConcurrentInvocations must be >= 1');
  if (config.defaultInvocationTimeoutMs < 0) throw new Error('defaultInvocationTimeoutMs must be >= 0');
  return config;
}

// ─── Logger ──────────────────────────────────────────────────────────────────

export type LogLevel = 'debug' | 'info' | 'warn' | 'error' | 'silent';

export interface Logger {
  debug(msg: string, ...args: any[]): void;
  info(msg: string, ...args: any[]): void;
  warn(msg: string, ...args: any[]): void;
  error(msg: string, ...args: any[]): void;
}

const LOG_LEVELS: Record<LogLevel, number> = { debug: 0, info: 1, warn: 2, error: 3, silent: 4 };

export function createLogger(level: LogLevel = 'info', prefix = '[velocity]'): Logger {
  const threshold = LOG_LEVELS[level];
  return {
    debug: (msg, ...args) => { if (threshold <= 0) console.debug(prefix, msg, ...args); },
    info: (msg, ...args) => { if (threshold <= 1) console.info(prefix, msg, ...args); },
    warn: (msg, ...args) => { if (threshold <= 2) console.warn(prefix, msg, ...args); },
    error: (msg, ...args) => { if (threshold <= 3) console.error(prefix, msg, ...args); },
  };
}

// ─── Metrics ─────────────────────────────────────────────────────────────────

export class Counter {
  private _value = 0;
  constructor(readonly name: string, readonly description: string) {}
  inc(amount = 1): void { this._value += amount; }
  get value(): number { return this._value; }
  reset(): void { this._value = 0; }
}

export class Histogram {
  private _sum = 0;
  private _count = 0;
  private _min = Infinity;
  private _max = -Infinity;
  constructor(readonly name: string, readonly description: string) {}
  observe(value: number): void {
    this._sum += value;
    this._count++;
    if (value < this._min) this._min = value;
    if (value > this._max) this._max = value;
  }
  get sum(): number { return this._sum; }
  get count(): number { return this._count; }
  get avg(): number { return this._count > 0 ? this._sum / this._count : 0; }
  get min(): number { return this._count > 0 ? this._min : 0; }
  get max(): number { return this._count > 0 ? this._max : 0; }
  reset(): void { this._sum = 0; this._count = 0; this._min = Infinity; this._max = -Infinity; }
}

export class MetricsCollector {
  readonly invocationsTotal = new Counter('velocity_invocations_total', 'Total handler invocations');
  readonly invocationsSuccess = new Counter('velocity_invocations_success_total', 'Successful invocations');
  readonly invocationsFailed = new Counter('velocity_invocations_failed_total', 'Failed invocations');
  readonly invocationsTimeout = new Counter('velocity_invocations_timeout_total', 'Timed-out invocations');
  readonly invocationDuration = new Histogram('velocity_invocation_duration_ms', 'Invocation duration (ms)');
  readonly servicesRegistered = new Counter('velocity_services_registered_total', 'Services registered');
  readonly awakeablesCreated = new Counter('velocity_awakeables_created_total', 'Awakeables created');
  readonly promisesResolved = new Counter('velocity_promises_resolved_total', 'Promises resolved');
  readonly promisesRejected = new Counter('velocity_promises_rejected_total', 'Promises rejected');
  private _startTime = Date.now();

  recordStart(serviceName: string, handlerName: string): void { this.invocationsTotal.inc(); }
  recordComplete(serviceName: string, handlerName: string, durationMs: number, success: boolean): void {
    this.invocationDuration.observe(durationMs);
    if (success) this.invocationsSuccess.inc(); else this.invocationsFailed.inc();
  }
  recordTimeout(): void { this.invocationsTimeout.inc(); }
  recordServiceRegistered(): void { this.servicesRegistered.inc(); }
  recordAwakeableCreated(): void { this.awakeablesCreated.inc(); }
  recordPromiseResolved(): void { this.promisesResolved.inc(); }
  recordPromiseRejected(): void { this.promisesRejected.inc(); }

  snapshot(): Record<string, any> {
    return {
      uptimeSeconds: (Date.now() - this._startTime) / 1000,
      counters: {
        invocationsTotal: this.invocationsTotal.value,
        invocationsSuccess: this.invocationsSuccess.value,
        invocationsFailed: this.invocationsFailed.value,
        invocationsTimeout: this.invocationsTimeout.value,
        servicesRegistered: this.servicesRegistered.value,
        awakeablesCreated: this.awakeablesCreated.value,
        promisesResolved: this.promisesResolved.value,
        promisesRejected: this.promisesRejected.value,
      },
      histograms: {
        invocationDuration: { sum: this.invocationDuration.sum, count: this.invocationDuration.count, avg: +this.invocationDuration.avg.toFixed(3), min: +this.invocationDuration.min.toFixed(3), max: +this.invocationDuration.max.toFixed(3) },
      },
    };
  }

  reset(): void {
    [this.invocationsTotal, this.invocationsSuccess, this.invocationsFailed, this.invocationsTimeout, this.servicesRegistered, this.awakeablesCreated, this.promisesResolved, this.promisesRejected].forEach(c => c.reset());
    this.invocationDuration.reset();
    this._startTime = Date.now();
  }
}

// ─── Health ──────────────────────────────────────────────────────────────────

export interface HealthCheckResult {
  name: string;
  status: 'healthy' | 'degraded' | 'unhealthy';
  message?: string;
  details?: Record<string, any>;
  latencyMs: number;
}

export interface HealthStatus {
  status: 'healthy' | 'degraded' | 'unhealthy';
  checks: HealthCheckResult[];
  timestamp: number;
  uptimeSeconds: number;
}

export type HealthCheckFn = () => HealthCheckResult | Promise<HealthCheckResult>;

export class HealthChecker {
  private _checks = new Map<string, HealthCheckFn>();
  private _startTime = Date.now();

  register(name: string, fn: HealthCheckFn): void { this._checks.set(name, fn); }
  unregister(name: string): void { this._checks.delete(name); }

  async check(): Promise<HealthStatus> {
    const results: HealthCheckResult[] = [];
    let overall: 'healthy' | 'degraded' | 'unhealthy' = 'healthy';

    for (const [name, fn] of this._checks) {
      const start = Date.now();
      try {
        const result = await fn();
        result.latencyMs = Date.now() - start;
        results.push(result);
        if (result.status === 'unhealthy') overall = 'unhealthy';
        else if (result.status === 'degraded' && overall !== 'unhealthy') overall = 'degraded';
      } catch (err: any) {
        results.push({ name, status: 'unhealthy', message: err.message, latencyMs: Date.now() - start });
        overall = 'unhealthy';
      }
    }

    return { status: overall, checks: results, timestamp: Date.now(), uptimeSeconds: (Date.now() - this._startTime) / 1000 };
  }
}

// ─── Retry Policy ────────────────────────────────────────────────────────────

export interface RetryPolicy {
  maxAttempts: number;
  initialDelayMs: number;
  maxDelayMs: number;
  backoffMultiplier: number;
  jitter: boolean;
  nonRetryableErrors?: string[];
}

export const DEFAULT_RETRY_POLICY: RetryPolicy = { maxAttempts: 3, initialDelayMs: 100, maxDelayMs: 10_000, backoffMultiplier: 2.0, jitter: true };
export const NO_RETRY_POLICY: RetryPolicy = { maxAttempts: 1, initialDelayMs: 0, maxDelayMs: 0, backoffMultiplier: 1, jitter: false };

export function shouldRetry(policy: RetryPolicy, error: Error, attempt: number): boolean {
  if (attempt >= policy.maxAttempts) return false;
  if (policy.nonRetryableErrors?.includes(error.name)) return false;
  return true;
}

export function getRetryDelay(policy: RetryPolicy, attempt: number): number {
  let delay = policy.initialDelayMs * Math.pow(policy.backoffMultiplier, attempt - 1);
  delay = Math.min(delay, policy.maxDelayMs);
  if (policy.jitter) delay = delay * (0.5 + Math.random() * 0.5);
  return delay;
}

// ─── Middleware ──────────────────────────────────────────────────────────────

export interface MiddlewareContext {
  invocationId: string;
  serviceName: string;
  handlerName: string;
  key: string;
  inputData: any;
  metadata: Record<string, any>;
  startTime: number;
}

export type MiddlewareFn = (ctx: MiddlewareContext, next: () => Promise<any>) => Promise<any>;

export class MiddlewareChain {
  private _global: MiddlewareFn[] = [];
  private _perService = new Map<string, MiddlewareFn[]>();

  use(fn: MiddlewareFn): void { this._global.push(fn); }
  useFor(serviceName: string, fn: MiddlewareFn): void {
    if (!this._perService.has(serviceName)) this._perService.set(serviceName, []);
    this._perService.get(serviceName)!.push(fn);
  }
  getChain(serviceName: string): MiddlewareFn[] {
    return [...this._global, ...(this._perService.get(serviceName) || [])];
  }
  clear(): void { this._global = []; this._perService.clear(); }
}

export function loggingMiddleware(logger: Logger): MiddlewareFn {
  return async (ctx, next) => {
    logger.info(`invocation_start service=${ctx.serviceName} handler=${ctx.handlerName} key=${ctx.key} id=${ctx.invocationId}`);
    try {
      const result = await next();
      logger.info(`invocation_complete service=${ctx.serviceName} handler=${ctx.handlerName} id=${ctx.invocationId} elapsed=${(Date.now() - ctx.startTime).toFixed(1)}ms`);
      return result;
    } catch (err: any) {
      logger.error(`invocation_error service=${ctx.serviceName} handler=${ctx.handlerName} id=${ctx.invocationId} error=${err.message}`);
      throw err;
    }
  };
}

export function metricsMiddleware(metrics: MetricsCollector): MiddlewareFn {
  return async (ctx, next) => {
    metrics.recordStart(ctx.serviceName, ctx.handlerName);
    const start = Date.now();
    try {
      const result = await next();
      metrics.recordComplete(ctx.serviceName, ctx.handlerName, Date.now() - start, true);
      return result;
    } catch (err) {
      metrics.recordComplete(ctx.serviceName, ctx.handlerName, Date.now() - start, false);
      throw err;
    }
  };
}

export function timeoutMiddleware(defaultTimeoutMs: number): MiddlewareFn {
  return async (ctx, next) => {
    const timeoutMs = ctx.metadata.timeoutMs ?? defaultTimeoutMs;
    if (timeoutMs <= 0) return next();
    return Promise.race([
      next(),
      new Promise<never>((_, reject) => setTimeout(() => reject(new InvocationTimeoutError(ctx.invocationId, timeoutMs)), timeoutMs)),
    ]);
  };
}

// ─── Types ───────────────────────────────────────────────────────────────────

export enum HandlerKind {
  WORKFLOW = 'workflow',
  SERVICE = 'service',
  SHARED = 'shared',
}

export interface JournalEntry {
  sequence: number;
  entryType: string;
  inputData?: any;
  outputData?: any;
  completed: boolean;
}

// ─── Awakeable ───────────────────────────────────────────────────────────────

export class Awakeable {
  readonly id: string;
  private _resolved = false;
  private _value: any = null;
  private _error: string | null = null;
  private _resolve: ((value: any) => void) | null = null;
  private _reject: ((error: Error) => void) | null = null;
  private _promise: Promise<any>;

  constructor(id: string) {
    this.id = id;
    this._promise = new Promise((resolve, reject) => { this._resolve = resolve; this._reject = reject; });
  }

  get resolved(): boolean { return this._resolved; }

  resolve(value: any): void {
    if (this._resolved) throw new DoubleResolveError(this.id, 'awakeable');
    this._resolved = true;
    this._value = value;
    this._resolve?.(value);
  }

  reject(error: string): void {
    if (this._resolved) throw new DoubleResolveError(this.id, 'awakeable');
    this._resolved = true;
    this._error = error;
    this._reject?.(new Error(`Awakeable rejected: ${error}`));
  }

  wait(): Promise<any> { return this._promise; }
}

// ─── DurablePromise ──────────────────────────────────────────────────────────

export class DurablePromise {
  readonly id: string;
  private _resolved = false;
  private _value: any = null;
  private _error: string | null = null;
  private _resolve: ((value: any) => void) | null = null;
  private _reject: ((error: Error) => void) | null = null;
  private _promise: Promise<any>;

  constructor(id: string) {
    this.id = id;
    this._promise = new Promise((resolve, reject) => { this._resolve = resolve; this._reject = reject; });
  }

  get resolved(): boolean { return this._resolved; }
  get pending(): boolean { return !this._resolved; }

  resolve(value: any): void {
    if (this._resolved) throw new DoubleResolveError(this.id);
    this._resolved = true;
    this._value = value;
    this._resolve?.(value);
  }

  reject(error: string): void {
    if (this._resolved) throw new DoubleResolveError(this.id);
    this._resolved = true;
    this._error = error;
    this._reject?.(new Error(`Promise rejected: ${error}`));
  }

  awaitValue(): Promise<any> { return this._promise; }
}

// ─── Context ─────────────────────────────────────────────────────────────────

export class Context {
  /** @internal */ _key: string;
  /** @internal */ _invocationId: string;
  /** @internal */ _state: Map<string, any> = new Map();
  /** @internal */ _journal: JournalEntry[] = [];
  /** @internal */ _promises: Map<string, DurablePromise> = new Map();
  /** @internal */ _awakeables: Map<string, Awakeable> = new Map();
  private _awkCounter = 0;

  constructor(key = '', invocationId = '') {
    this._key = key;
    this._invocationId = invocationId || `inv_${Math.random().toString(36).slice(2)}`;
  }

  get key(): string { return this._key; }
  get id(): string { return this._invocationId; }

  async run<T>(fn: () => T | Promise<T>): Promise<T> {
    const seq = this._journal.length;
    const result = await fn();
    this._journal.push({ sequence: seq, entryType: 'durable_step', outputData: result, completed: true });
    return result;
  }

  async get(stateKey: string): Promise<any> { return this._state.get(stateKey) ?? null; }

  async set(stateKey: string, value: any): Promise<void> {
    this._state.set(stateKey, value);
    this._journal.push({ sequence: this._journal.length, entryType: 'state_set', inputData: { key: stateKey, value }, completed: true });
  }

  async clear(stateKey: string): Promise<void> {
    this._state.delete(stateKey);
    this._journal.push({ sequence: this._journal.length, entryType: 'state_clear', inputData: { key: stateKey }, completed: true });
  }

  promise(promiseId: string): DurablePromise {
    if (!this._promises.has(promiseId)) this._promises.set(promiseId, new DurablePromise(promiseId));
    return this._promises.get(promiseId)!;
  }

  awakeable(): Awakeable {
    const id = `awk_${this._invocationId}_${this._awkCounter++}`;
    const awk = new Awakeable(id);
    this._awakeables.set(id, awk);
    return awk;
  }

  async sleep(durationMs: number): Promise<void> {
    this._journal.push({ sequence: this._journal.length, entryType: 'sleep', inputData: { durationMs }, completed: true });
    await new Promise(resolve => setTimeout(resolve, durationMs));
  }

  replayJournal(entries: JournalEntry[], state: Map<string, any>): void {
    this._journal = [...entries];
    this._state = new Map(state);
  }
}

export class ObjectContext extends Context {
  readonly objectType: string;
  constructor(objectType: string, key: string, invocationId = '') {
    super(key, invocationId);
    this.objectType = objectType;
  }
  get fullKey(): string { return `${this.objectType}/${this._key}`; }
}

export class WorkflowContext extends Context {
  readonly workflowId: string;
  constructor(workflowId: string, invocationId = '') {
    super(workflowId, invocationId);
    this.workflowId = workflowId;
  }
}

// ─── Handler Registration ────────────────────────────────────────────────────

export interface HandlerRegistration {
  name: string;
  fn: (ctx: any, input?: any) => Promise<any> | any;
  kind: HandlerKind;
  serviceName: string;
}

// ─── VirtualObject ───────────────────────────────────────────────────────────

export class VirtualObject {
  readonly name: string;
  private _handlers = new Map<string, HandlerRegistration>();

  constructor(name: string) { this.name = name; }

  addHandler(name: string, fn: (ctx: ObjectContext, input?: any) => Promise<any> | any, kind = HandlerKind.WORKFLOW): this {
    this._handlers.set(name, { name, fn, kind, serviceName: this.name });
    return this;
  }

  getHandler(name: string): HandlerRegistration | undefined { return this._handlers.get(name); }
  get handlers(): Map<string, HandlerRegistration> { return this._handlers; }
}

// ─── Service ─────────────────────────────────────────────────────────────────

export class Service {
  readonly name: string;
  private _handlers = new Map<string, HandlerRegistration>();

  constructor(name: string) { this.name = name; }

  addHandler(name: string, fn: (ctx: Context, input?: any) => Promise<any> | any, kind = HandlerKind.SERVICE): this {
    this._handlers.set(name, { name, fn, kind, serviceName: this.name });
    return this;
  }

  getHandler(name: string): HandlerRegistration | undefined { return this._handlers.get(name); }
  get handlers(): Map<string, HandlerRegistration> { return this._handlers; }
}

// ─── Workflow ────────────────────────────────────────────────────────────────

export class Workflow {
  readonly name: string;
  private _handlers = new Map<string, HandlerRegistration>();

  constructor(name: string) { this.name = name; }

  addHandler(name: string, fn: (ctx: WorkflowContext, input?: any) => Promise<any> | any): this {
    this._handlers.set(name, { name, fn, kind: HandlerKind.WORKFLOW, serviceName: this.name });
    return this;
  }

  getHandler(name: string): HandlerRegistration | undefined { return this._handlers.get(name); }
  get handlers(): Map<string, HandlerRegistration> { return this._handlers; }
}

// ─── Storage ─────────────────────────────────────────────────────────────────

export interface StoredJournal {
  invocationId: string;
  serviceName: string;
  handlerName: string;
  key: string;
  entries: JournalEntry[];
  objectState: Record<string, any>;
  output: any;
  error?: string;
  state: string;
  createdAt: number;
  completedAt: number;
}

export interface StoredKeyState {
  fullKey: string;
  state: Record<string, any>;
  updatedAt: number;
}

export interface StorageBackend {
  saveJournal(journal: StoredJournal): void;
  loadJournal(invocationId: string): StoredJournal | null;
  loadJournalsForKey(fullKey: string): StoredJournal[];
  loadAllJournals(): StoredJournal[];
  saveKeyState(keyState: StoredKeyState): void;
  loadKeyState(fullKey: string): StoredKeyState | null;
  deleteJournal(invocationId: string): void;
  clear(): void;
}

export class InMemoryStorage implements StorageBackend {
  private _journals = new Map<string, StoredJournal>();
  private _keyStates = new Map<string, StoredKeyState>();

  saveJournal(journal: StoredJournal): void { this._journals.set(journal.invocationId, journal); }
  loadJournal(invocationId: string): StoredJournal | null { return this._journals.get(invocationId) ?? null; }
  loadJournalsForKey(fullKey: string): StoredJournal[] {
    const parts = fullKey.split('/');
    const serviceName = parts[0];
    const key = parts.slice(1).join('/');
    return Array.from(this._journals.values()).filter(j => j.serviceName === serviceName && j.key === key);
  }
  loadAllJournals(): StoredJournal[] { return Array.from(this._journals.values()); }
  saveKeyState(keyState: StoredKeyState): void { this._keyStates.set(keyState.fullKey, keyState); }
  loadKeyState(fullKey: string): StoredKeyState | null { return this._keyStates.get(fullKey) ?? null; }
  deleteJournal(invocationId: string): void { this._journals.delete(invocationId); }
  clear(): void { this._journals.clear(); this._keyStates.clear(); }
}

// ─── Invocation Record ──────────────────────────────────────────────────────

export interface InvocationRecord {
  invocationId: string;
  serviceName: string;
  handlerName: string;
  key: string;
  inputData?: any;
  outputData?: any;
  error?: string;
  errorCode?: string;
  state: 'queued' | 'running' | 'suspended' | 'completed' | 'failed';
  journal: JournalEntry[];
  createdAt: number;
  startedAt: number;
  completedAt: number;
  attempts: number;
  idempotencyKey?: string;
}

// ─── RuntimeServer ───────────────────────────────────────────────────────────

export class RuntimeServer {
  private _services = new Map<string, VirtualObject | Service | Workflow>();
  private _invocations = new Map<string, InvocationRecord>();
  private _keyState = new Map<string, Map<string, any>>();
  private _keyQueues = new Map<string, string[]>();
  private _keyLocks = new Map<string, string>();
  private _idempotencyMap = new Map<string, string>();
  private _awakeables = new Map<string, Awakeable>();
  private _config: ServerConfig;
  private _middleware = new MiddlewareChain();
  private _metrics = new MetricsCollector();
  private _health = new HealthChecker();
  private _logger: Logger;
  private _storage: StorageBackend;
  private _shuttingDown = false;
  private _startTime = Date.now();
  private _activeTasks = new Set<Promise<void>>();

  constructor(config?: Partial<ServerConfig>, storage?: StorageBackend) {
    this._config = createConfig(config);
    this._logger = createLogger(this._config.logLevel);
    this._storage = storage ?? new InMemoryStorage();
    if (this._config.enableMetrics) this._middleware.use(metricsMiddleware(this._metrics));
    this._middleware.use(loggingMiddleware(this._logger));
    if (this._config.defaultInvocationTimeoutMs > 0) this._middleware.use(timeoutMiddleware(this._config.defaultInvocationTimeoutMs));
    if (this._config.enableHealthChecks) {
      this._health.register('liveness', () => ({ name: 'liveness', status: 'healthy', message: 'alive', latencyMs: 0 }));
      this._health.register('readiness', () => ({
        name: 'readiness', status: this._shuttingDown ? 'degraded' : this._services.size > 0 ? 'healthy' : 'degraded',
        message: this._shuttingDown ? 'shutting down' : this._services.size > 0 ? 'ready' : 'no services registered', latencyMs: 0,
      }));
    }
    // Replay journals from storage to restore state
    this._replayFromStorage();
  }

  get config(): ServerConfig { return this._config; }
  get metrics(): MetricsCollector { return this._metrics; }
  get health(): HealthChecker { return this._health; }
  get middleware(): MiddlewareChain { return this._middleware; }
  get storage(): StorageBackend { return this._storage; }
  get isShuttingDown(): boolean { return this._shuttingDown; }

  register(service: VirtualObject | Service | Workflow): void {
    if (this._services.has(service.name)) throw new Error(`Service already registered: ${service.name}`);
    this._services.set(service.name, service);
    if (this._config.enableMetrics) this._metrics.recordServiceRegistered();
    this._logger.info(`Service registered: ${service.name}`);
    // Re-execute any incomplete invocations for this service
    this._reexecuteIncompleteInvocations(service.name);
  }

  private _reexecuteIncompleteInvocations(serviceName: string): void {
    for (const [invId, record] of this._invocations) {
      if (record.serviceName === serviceName && (record.state === 'running' || record.state === 'queued')) {
        const handlerReg = this._services.get(serviceName)?.getHandler(record.handlerName);
        if (!handlerReg) {
          this._logger.warn(`Cannot re-execute ${invId}: handler ${record.handlerName} not registered`);
          continue;
        }
        // Mark as queued for re-execution
        record.state = 'queued';
        record.error = undefined;
        record.completedAt = 0;
        const fullKey = record.key ? `${serviceName}/${record.key}` : serviceName;
        this._keyLocks.set(fullKey, invId);
        const task = this._execute(invId, fullKey);
        this._activeTasks.add(task);
        task.finally(() => this._activeTasks.delete(task));
        this._logger.info(`Re-executing incomplete invocation: ${invId}`);
      }
    }
  }

  listServices(): string[] { return Array.from(this._services.keys()); }
  getService(name: string): VirtualObject | Service | Workflow | undefined { return this._services.get(name); }

  // ─── Storage / Replay ─────────────────────────────────────────────────────

  private _replayFromStorage(): void {
    const journals = this._storage.loadAllJournals();
    let replayed = 0;
    const incompleteInvocations: StoredJournal[] = [];
    
    for (const j of journals) {
      if (j.state === 'completed' && Object.keys(j.objectState).length > 0) {
        const fullKey = j.key ? `${j.serviceName}/${j.key}` : j.serviceName;
        const stateMap = new Map(Object.entries(j.objectState));
        this._keyState.set(fullKey, stateMap);
        replayed++;
      }
      // Restore invocation record
      this._invocations.set(j.invocationId, {
        invocationId: j.invocationId,
        serviceName: j.serviceName,
        handlerName: j.handlerName,
        key: j.key,
        outputData: j.output,
        error: j.error,
        state: j.state as any,
        journal: j.entries,
        createdAt: j.createdAt,
        startedAt: j.createdAt,
        completedAt: j.completedAt,
        attempts: 1,
      });
      // Track incomplete invocations for re-execution
      if (j.state === 'running' || j.state === 'queued') {
        incompleteInvocations.push(j);
      }
    }
    
    if (replayed > 0) {
      this._logger.info(`Replayed ${replayed} journals from storage, restored ${this._keyState.size} keys`);
    }
    
    // Re-execute incomplete invocations (crash recovery)
    if (incompleteInvocations.length > 0) {
      this._logger.info(`Re-executing ${incompleteInvocations.length} incomplete invocations`);
      for (const j of incompleteInvocations) {
        const service = this._services.get(j.serviceName);
        if (!service) {
          this._logger.warn(`Cannot re-execute ${j.invocationId}: service ${j.serviceName} not registered`);
          continue;
        }
        const handlerReg = service.getHandler(j.handlerName);
        if (!handlerReg) {
          this._logger.warn(`Cannot re-execute ${j.invocationId}: handler ${j.handlerName} not registered`);
          continue;
        }
        // Mark as queued for re-execution
        const record = this._invocations.get(j.invocationId);
        if (record) {
          record.state = 'queued';
          record.error = undefined;
          record.completedAt = 0;
          const fullKey = j.key ? `${j.serviceName}/${j.key}` : j.serviceName;
          this._keyLocks.set(fullKey, j.invocationId);
          const task = this._execute(j.invocationId, fullKey);
          this._activeTasks.add(task);
          task.finally(() => this._activeTasks.delete(task));
        }
      }
    }
  }

  private _persistJournal(record: InvocationRecord): void {
    const objectState: Record<string, any> = {};
    const fullKey = record.key ? `${record.serviceName}/${record.key}` : record.serviceName;
    const stateMap = this._keyState.get(fullKey);
    if (stateMap) { for (const [k, v] of stateMap) objectState[k] = v; }

    this._storage.saveJournal({
      invocationId: record.invocationId,
      serviceName: record.serviceName,
      handlerName: record.handlerName,
      key: record.key,
      entries: record.journal,
      objectState,
      output: record.outputData,
      error: record.error,
      state: record.state,
      createdAt: record.createdAt,
      completedAt: record.completedAt,
    });
  }

  private _persistKeyState(fullKey: string): void {
    const stateMap = this._keyState.get(fullKey);
    if (!stateMap) return;
    const state: Record<string, any> = {};
    for (const [k, v] of stateMap) state[k] = v;
    this._storage.saveKeyState({ fullKey, state, updatedAt: Date.now() });
  }

  registerAwakeable(awakeable: Awakeable): void {
    this._awakeables.set(awakeable.id, awakeable);
  }

  async resolveAwakeable(awakeableId: string, value: any): Promise<void> {
    const awk = this._awakeables.get(awakeableId);
    if (awk) { awk.resolve(value); return; }
    throw new AwakeableNotFoundError(awakeableId);
  }

  async rejectAwakeable(awakeableId: string, error: string): Promise<void> {
    const awk = this._awakeables.get(awakeableId);
    if (awk) { awk.reject(error); return; }
    throw new AwakeableNotFoundError(awakeableId);
  }

  async invoke(serviceName: string, handlerName: string, key = '', inputData?: any, idempotencyKey?: string, timeoutMs?: number): Promise<string> {
    if (this._shuttingDown) throw new ShutdownError();
    if (idempotencyKey && this._idempotencyMap.has(idempotencyKey)) return this._idempotencyMap.get(idempotencyKey)!;

    const service = this._services.get(serviceName);
    if (!service) throw new ServiceNotFoundError(serviceName);
    const handlerReg = service.getHandler(handlerName);
    if (!handlerReg) throw new HandlerNotFoundError(serviceName, handlerName);

    const invocationId = `inv_${Math.random().toString(36).slice(2)}`;
    if (idempotencyKey) this._idempotencyMap.set(idempotencyKey, invocationId);

    const record: InvocationRecord = {
      invocationId, serviceName, handlerName, key, inputData, state: 'queued', journal: [],
      createdAt: Date.now(), startedAt: 0, completedAt: 0, attempts: 0, idempotencyKey,
    };
    this._invocations.set(invocationId, record);

    const fullKey = key ? `${serviceName}/${key}` : serviceName;
    if (service instanceof VirtualObject && this._keyLocks.has(fullKey)) {
      const queue = this._keyQueues.get(fullKey) || [];
      if (queue.length >= this._config.maxQueueDepthPerKey) {
        record.state = 'failed'; record.error = 'Queue depth exceeded'; record.errorCode = 'QUEUE_FULL'; record.completedAt = Date.now();
        return invocationId;
      }
      queue.push(invocationId);
      this._keyQueues.set(fullKey, queue);
    } else {
      this._keyLocks.set(fullKey, invocationId);
      record.state = 'running';
      const task = this._execute(invocationId, fullKey, timeoutMs);
      this._activeTasks.add(task);
      task.finally(() => this._activeTasks.delete(task));
    }

    return invocationId;
  }

  private async _execute(invocationId: string, fullKey: string, timeoutMs?: number): Promise<void> {
    const record = this._invocations.get(invocationId)!;
    const service = this._services.get(record.serviceName)!;
    const handlerReg = service.getHandler(record.handlerName)!;

    const mwCtx: MiddlewareContext = {
      invocationId, serviceName: record.serviceName, handlerName: record.handlerName,
      key: record.key, inputData: record.inputData, metadata: { timeoutMs: timeoutMs ?? this._config.defaultInvocationTimeoutMs }, startTime: Date.now(),
    };
    record.startedAt = Date.now();
    record.attempts = 0;

    // Retry loop
    let lastError: any;
    for (let attempt = 1; attempt <= this._config.maxRetries + 1; attempt++) {
      record.attempts = attempt;
      try {
        const chain = this._middleware.getChain(record.serviceName);
        const result = await this._runChain(chain, mwCtx, async () => {
          let ctx: Context;
          if (service instanceof VirtualObject) {
            ctx = new ObjectContext(service.name, record.key, invocationId);
            const saved = this._keyState.get(fullKey);
            if (saved) for (const [k, v] of saved) ctx._state.set(k, v);
          } else if (service instanceof Workflow) {
            ctx = new WorkflowContext(record.key || invocationId, invocationId);
          } else {
            ctx = new Context(record.key, invocationId);
          }
          const result = await handlerReg.fn(ctx, record.inputData);
          if (service instanceof VirtualObject) this._keyState.set(fullKey, new Map(ctx._state));
          record.journal = [...ctx._journal];
          return result;
        });

        record.outputData = result;
        record.state = 'completed';
        record.completedAt = Date.now();
        // Persist journal and key state to storage
        this._persistJournal(record);
        if (service instanceof VirtualObject) this._persistKeyState(fullKey);
        // Release key lock on success
        if (this._keyLocks.get(fullKey) === invocationId) {
          this._keyLocks.delete(fullKey);
          this._dispatchNext(fullKey);
        }
        return; // Success, exit retry loop
      } catch (err: any) {
        lastError = err;
        this._logger.warn(`Invocation attempt ${attempt} failed: ${record.serviceName}/${record.handlerName} [${invocationId}] — ${err.message}`);
        
        // Check if we should retry
        if (attempt < this._config.maxRetries + 1) {
          // Calculate backoff delay
          const delay = Math.min(
            this._config.retryBaseDelayMs * Math.pow(2, attempt - 1),
            this._config.retryMaxDelayMs
          );
          await new Promise(resolve => setTimeout(resolve, delay));
        }
      }
    }

    // All retries exhausted
    if (lastError) {
      record.error = lastError.message || String(lastError);
      record.errorCode = lastError instanceof VelocityError ? lastError.code : 'UNKNOWN';
      record.state = 'failed';
      record.completedAt = Date.now();
      this._logger.error(`Invocation failed after ${record.attempts} attempts: ${record.serviceName}/${record.handlerName} [${invocationId}] — ${lastError.message}`);
      // Persist failed journal for audit trail
      this._persistJournal(record);
    }
    
    // Release key lock
    if (this._keyLocks.get(fullKey) === invocationId) {
      this._keyLocks.delete(fullKey);
      this._dispatchNext(fullKey);
    }
  }

  private async _runChain(chain: MiddlewareFn[], ctx: MiddlewareContext, finalHandler: () => Promise<any>): Promise<any> {
    let index = 0;
    const next = async (): Promise<any> => {
      if (index < chain.length) {
        const mw = chain[index++];
        return mw(ctx, next);
      }
      return finalHandler();
    };
    return next();
  }

  private _dispatchNext(fullKey: string): void {
    const queue = this._keyQueues.get(fullKey);
    if (queue && queue.length > 0) {
      const nextId = queue.shift()!;
      this._keyLocks.set(fullKey, nextId);
      const record = this._invocations.get(nextId)!;
      record.state = 'running';
      const task = this._execute(nextId, fullKey);
      this._activeTasks.add(task);
      task.finally(() => this._activeTasks.delete(task));
    }
  }

  getInvocation(invocationId: string): InvocationRecord | undefined { return this._invocations.get(invocationId); }

  listInvocations(filter?: { serviceName?: string; state?: string; limit?: number }): InvocationRecord[] {
    let results = Array.from(this._invocations.values());
    if (filter?.serviceName) results = results.filter(r => r.serviceName === filter.serviceName);
    if (filter?.state) results = results.filter(r => r.state === filter.state);
    results.sort((a, b) => b.createdAt - a.createdAt);
    return results.slice(0, filter?.limit ?? 100);
  }

  async healthCheck(): Promise<HealthStatus> { return this._health.check(); }

  getStats(): Record<string, any> {
    const states: Record<string, number> = {};
    for (const inv of this._invocations.values()) states[inv.state] = (states[inv.state] || 0) + 1;
    return {
      registeredServices: this._services.size,
      totalInvocations: this._invocations.size,
      activeInvocations: states['running'] || 0,
      queuedInvocations: states['queued'] || 0,
      completedInvocations: states['completed'] || 0,
      failedInvocations: states['failed'] || 0,
      uptimeSeconds: (Date.now() - this._startTime) / 1000,
      shuttingDown: this._shuttingDown,
      ...(this._config.enableMetrics ? { metrics: this._metrics.snapshot() } : {}),
    };
  }

  async shutdown(gracePeriodMs?: number): Promise<void> {
    if (this._shuttingDown) return;
    this._shuttingDown = true;
    const grace = gracePeriodMs ?? this._config.shutdownGracePeriodMs;
    this._logger.info(`Shutting down (grace=${grace}ms, active=${this._activeTasks.size})...`);
    if (this._activeTasks.size > 0) {
      await Promise.race([
        Promise.allSettled(Array.from(this._activeTasks)),
        new Promise(resolve => setTimeout(resolve, grace)),
      ]);
    }
    this._logger.info('Shutdown complete.');
  }
}

export function createApp(
  services: (VirtualObject | Service | Workflow)[],
  config?: Partial<ServerConfig>,
  storage?: StorageBackend,
): RuntimeServer {
  const server = new RuntimeServer(config, storage);
  for (const svc of services) server.register(svc);
  return server;
}

// ─── HTTP Transport ──────────────────────────────────────────────────────────

export { RuntimeRemoteClient } from './http-transport';
export type { RuntimeRemoteConfig } from './http-transport';
