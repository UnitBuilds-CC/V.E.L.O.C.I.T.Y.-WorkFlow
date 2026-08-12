/**
 * VELOCITY-WorkFlow TypeScript SDK - Retry utilities.
 *
 * Provides exponential backoff with jitter for retrying failed operations.
 *
 * @packageDocumentation
 */

/** Configuration options for retry behavior. */
export interface RetryOptions {
  /** Maximum number of attempts (must be >= 1). */
  maxAttempts: number;
  /** Initial backoff interval in milliseconds. */
  initialIntervalMs: number;
  /** Backoff coefficient (e.g., 2.0 means exponential doubling). Must be >= 1.0. */
  backoffCoefficient: number;
  /** Maximum backoff interval in milliseconds. */
  maxIntervalMs: number;
  /** Whether to add random jitter to backoff. */
  jitter: boolean;
  /** Predicate to determine if an error is retryable. */
  retryable?: (error: unknown) => boolean;
}

/** Default retry options. */
export const DEFAULT_RETRY_OPTIONS: RetryOptions = {
  maxAttempts: 3,
  initialIntervalMs: 100,
  backoffCoefficient: 2.0,
  maxIntervalMs: 60_000,
  jitter: true,
};

/**
 * Calculate backoff duration for a given attempt.
 *
 * @param attempt - Zero-based attempt index.
 * @param options - Retry configuration.
 * @returns Backoff duration in milliseconds.
 */
export function calculateBackoff(
  attempt: number,
  options: RetryOptions
): number {
  let interval = options.initialIntervalMs * Math.pow(options.backoffCoefficient, attempt);
  interval = Math.min(interval, options.maxIntervalMs);

  if (options.jitter) {
    // Full jitter: random value between 0 and calculated interval
    interval = Math.random() * interval;
  }

  return interval;
}

/**
 * Validate retry options, throwing if invalid.
 *
 * @param options - Options to validate.
 * @throws Error if any option is invalid.
 */
export function validateRetryOptions(options: RetryOptions): void {
  if (options.maxAttempts < 1) {
    throw new Error('maxAttempts must be >= 1');
  }
  if (options.initialIntervalMs <= 0) {
    throw new Error('initialIntervalMs must be > 0');
  }
  if (options.backoffCoefficient < 1.0) {
    throw new Error('backoffCoefficient must be >= 1.0');
  }
  if (options.maxIntervalMs < options.initialIntervalMs) {
    throw new Error('maxIntervalMs must be >= initialIntervalMs');
  }
}

/**
 * Sleep for a given number of milliseconds.
 */
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Execute an async function with retry logic and exponential backoff.
 *
 * @param fn - Async function to execute.
 * @param options - Retry configuration (uses defaults for missing fields).
 * @returns Result of the function call.
 * @throws The last error if all retries fail.
 *
 * @example
 * ```typescript
 * const result = await retryWithBackoff(
 *   () => fetchRemoteData(),
 *   { maxAttempts: 5, initialIntervalMs: 200, backoffCoefficient: 2.0, maxIntervalMs: 10_000, jitter: true }
 * );
 * ```
 */
export async function retryWithBackoff<T>(
  fn: () => Promise<T>,
  options: Partial<RetryOptions> = {}
): Promise<T> {
  const opts: RetryOptions = { ...DEFAULT_RETRY_OPTIONS, ...options };
  validateRetryOptions(opts);

  let lastError: unknown;

  for (let attempt = 0; attempt < opts.maxAttempts; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;

      // Check if error is retryable
      if (opts.retryable && !opts.retryable(error)) {
        throw error;
      }

      if (attempt < opts.maxAttempts - 1) {
        const backoff = calculateBackoff(attempt, opts);
        await sleep(backoff);
      }
    }
  }

  throw lastError;
}

/**
 * Create a retry wrapper with pre-configured options.
 *
 * @param options - Retry configuration.
 * @returns A function that wraps another function with retry logic.
 */
export function createRetry(options: Partial<RetryOptions>): <T>(fn: () => Promise<T>) => Promise<T> {
  const opts: RetryOptions = { ...DEFAULT_RETRY_OPTIONS, ...options };
  validateRetryOptions(opts);

  return <T>(fn: () => Promise<T>): Promise<T> => retryWithBackoff(fn, opts);
}
