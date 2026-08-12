"""
Velocity Runtime configuration management.

Supports environment variables, explicit config, and sensible defaults.
"""

import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class ServerConfig:
    """Configuration for the RuntimeServer."""

    # Network
    host: str = "0.0.0.0"
    port: int = 9080
    engine_url: str = "http://localhost:8080"

    # Concurrency
    max_concurrent_invocations: int = 256
    max_queue_depth_per_key: int = 1000

    # Timeouts (milliseconds)
    default_invocation_timeout_ms: int = 30_000
    default_sleep_timeout_ms: int = 60_000
    shutdown_grace_period_ms: int = 10_000

    # Retry
    max_retries: int = 3
    retry_base_delay_ms: int = 100
    retry_max_delay_ms: int = 10_000

    # Logging
    log_level: str = "INFO"

    # Feature flags
    enable_metrics: bool = True
    enable_health_endpoint: bool = True
    enable_journaling: bool = True

    # Custom metadata
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_env(cls, prefix: str = "VELOCITY_") -> "ServerConfig":
        """Create config from environment variables.

        Reads VELOCITY_HOST, VELOCITY_PORT, VELOCITY_ENGINE_URL, etc.
        """
        config = cls()
        env_map = {
            "HOST": ("host", str),
            "PORT": ("port", int),
            "ENGINE_URL": ("engine_url", str),
            "MAX_CONCURRENT_INVOCATIONS": ("max_concurrent_invocations", int),
            "MAX_QUEUE_DEPTH_PER_KEY": ("max_queue_depth_per_key", int),
            "DEFAULT_INVOCATION_TIMEOUT_MS": ("default_invocation_timeout_ms", int),
            "SHUTDOWN_GRACE_PERIOD_MS": ("shutdown_grace_period_ms", int),
            "MAX_RETRIES": ("max_retries", int),
            "RETRY_BASE_DELAY_MS": ("retry_base_delay_ms", int),
            "RETRY_MAX_DELAY_MS": ("retry_max_delay_ms", int),
            "LOG_LEVEL": ("log_level", str),
            "ENABLE_METRICS": ("enable_metrics", _parse_bool),
            "ENABLE_HEALTH_ENDPOINT": ("enable_health_endpoint", _parse_bool),
            "ENABLE_JOURNALING": ("enable_journaling", _parse_bool),
        }
        for env_suffix, (attr, cast) in env_map.items():
            val = os.environ.get(f"{prefix}{env_suffix}")
            if val is not None:
                try:
                    setattr(config, attr, cast(val))
                except (ValueError, TypeError):
                    pass  # keep default on bad env value
        return config

    def validate(self) -> None:
        """Validate configuration values."""
        if self.port < 1 or self.port > 65535:
            raise ValueError(f"Invalid port: {self.port}")
        if self.max_concurrent_invocations < 1:
            raise ValueError("max_concurrent_invocations must be >= 1")
        if self.default_invocation_timeout_ms < 0:
            raise ValueError("default_invocation_timeout_ms must be >= 0")
        if self.max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if self.retry_base_delay_ms < 0:
            raise ValueError("retry_base_delay_ms must be >= 0")
        if self.shutdown_grace_period_ms < 0:
            raise ValueError("shutdown_grace_period_ms must be >= 0")


def _parse_bool(val: str) -> bool:
    """Parse a boolean from an environment variable string."""
    return val.lower() in ("1", "true", "yes", "on")
