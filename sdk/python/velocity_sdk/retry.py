"""
VELOCITY-WorkFlow Python SDK - Retry utilities.

Provides exponential backoff with jitter for retrying failed operations.
"""

import random
import time
from typing import Callable, Type, Set, Any
from dataclasses import dataclass, field


@dataclass
class RetryPolicy:
    """Configuration for retry behavior."""

    max_attempts: int = 3
    initial_interval: float = 0.1
    backoff_coefficient: float = 2.0
    max_interval: float = 60.0
    jitter: bool = True
    retryable_exceptions: Set[Type[Exception]] = field(default_factory=lambda: {Exception})

    def validate(self) -> None:
        """Validate retry policy configuration."""
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.initial_interval <= 0:
            raise ValueError("initial_interval must be > 0")
        if self.backoff_coefficient < 1.0:
            raise ValueError("backoff_coefficient must be >= 1.0")
        if self.max_interval < self.initial_interval:
            raise ValueError("max_interval must be >= initial_interval")


def calculate_backoff(
    attempt: int,
    initial_interval: float,
    backoff_coefficient: float,
    max_interval: float,
    jitter: bool,
) -> float:
    """Calculate backoff duration for a given attempt."""
    interval = initial_interval * (backoff_coefficient ** attempt)
    interval = min(interval, max_interval)

    if jitter:
        # Full jitter: random value between 0 and calculated interval
        interval = random.uniform(0, interval)

    return interval


def retry_with_policy(
    policy: RetryPolicy,
    func: Callable[..., Any],
    *args: Any,
    **kwargs: Any,
) -> Any:
    """
    Execute a function with retry logic.

    Args:
        policy: Retry configuration
        func: Function to execute
        *args: Positional arguments for the function
        **kwargs: Keyword arguments for the function

    Returns:
        Result of the function call

    Raises:
        The last exception if all retries fail
    """
    policy.validate()

    last_exception: Exception | None = None

    for attempt in range(policy.max_attempts):
        try:
            return func(*args, **kwargs)
        except tuple(policy.retryable_exceptions) as e:
            last_exception = e

            if attempt < policy.max_attempts - 1:
                backoff = calculate_backoff(
                    attempt,
                    policy.initial_interval,
                    policy.backoff_coefficient,
                    policy.max_interval,
                    policy.jitter,
                )
                time.sleep(backoff)

    raise last_exception  # type: ignore[misc]


class RetryableOperation:
    """Context manager for retryable operations with custom error handling."""

    def __init__(self, policy: RetryPolicy):
        self.policy = policy
        self.attempts: list[Exception] = []

    def __call__(self, func: Callable[..., Any]) -> Callable[..., Any]:
        """Decorator for retryable functions."""
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            self.attempts.clear()
            return retry_with_policy(self.policy, func, *args, **kwargs)
        return wrapper

    def get_attempts(self) -> list[Exception]:
        """Get list of exceptions from previous attempts."""
        return self.attempts.copy()
