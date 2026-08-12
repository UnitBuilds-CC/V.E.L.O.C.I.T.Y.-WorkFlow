"""
Velocity Runtime retry policies.

Configurable retry logic with exponential backoff and jitter.
"""

import asyncio
import random
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Set, Type


@dataclass
class RetryPolicy:
    """Configures retry behavior for handler invocations.

    Attributes:
        max_attempts: Maximum number of attempts (1 = no retries).
        initial_delay_ms: Initial retry delay in milliseconds.
        max_delay_ms: Maximum delay between retries.
        backoff_multiplier: Multiplier for exponential backoff.
        jitter: Whether to add random jitter to delays.
        retryable_exceptions: Set of exception types that trigger retries.
            If empty, all exceptions are retried.
        non_retryable_exceptions: Set of exception types that should NEVER be retried.
    """
    max_attempts: int = 3
    initial_delay_ms: int = 100
    max_delay_ms: int = 10_000
    backoff_multiplier: float = 2.0
    jitter: bool = True
    retryable_exceptions: Set[Type[Exception]] = field(default_factory=set)
    non_retryable_exceptions: Set[Type[Exception]] = field(default_factory=set)

    def should_retry(self, exception: Exception, attempt: int) -> bool:
        """Determine if a failed attempt should be retried."""
        if attempt >= self.max_attempts:
            return False
        if any(isinstance(exception, t) for t in self.non_retryable_exceptions):
            return False
        if self.retryable_exceptions:
            return any(isinstance(exception, t) for t in self.retryable_exceptions)
        return True

    def get_delay_ms(self, attempt: int) -> float:
        """Calculate delay for a given attempt number (1-based)."""
        delay = self.initial_delay_ms * (self.backoff_multiplier ** (attempt - 1))
        delay = min(delay, self.max_delay_ms)
        if self.jitter:
            delay = delay * (0.5 + random.random() * 0.5)
        return delay


# ─── Default policies ────────────────────────────────────────────────────────

DEFAULT_RETRY_POLICY = RetryPolicy(
    max_attempts=3,
    initial_delay_ms=100,
    max_delay_ms=10_000,
    backoff_multiplier=2.0,
)

NO_RETRY_POLICY = RetryPolicy(max_attempts=1)

AGGRESSIVE_RETRY_POLICY = RetryPolicy(
    max_attempts=10,
    initial_delay_ms=50,
    max_delay_ms=30_000,
    backoff_multiplier=2.0,
)

CONSERVATIVE_RETRY_POLICY = RetryPolicy(
    max_attempts=5,
    initial_delay_ms=500,
    max_delay_ms=60_000,
    backoff_multiplier=3.0,
)


# ─── Retry executor ─────────────────────────────────────────────────────────

async def execute_with_retry(
    fn: Callable,
    policy: RetryPolicy = DEFAULT_RETRY_POLICY,
    on_retry: Optional[Callable[[int, Exception, float], None]] = None,
) -> Any:
    """Execute an async function with retry logic.

    Args:
        fn: Async callable to execute.
        policy: Retry policy to use.
        on_retry: Optional callback(attempt, exception, delay_ms) called before each retry.

    Returns:
        The result of fn().

    Raises:
        The last exception if all retries are exhausted.
    """
    last_exception = None
    for attempt in range(1, policy.max_attempts + 1):
        try:
            result = fn()
            if asyncio.iscoroutine(result):
                result = await result
            return result
        except Exception as e:
            last_exception = e
            if not policy.should_retry(e, attempt):
                raise
            delay_ms = policy.get_delay_ms(attempt)
            if on_retry:
                on_retry(attempt, e, delay_ms)
            await asyncio.sleep(delay_ms / 1000.0)

    # Should not reach here, but just in case
    if last_exception:
        raise last_exception
    raise RuntimeError("Retry loop exited unexpectedly")
