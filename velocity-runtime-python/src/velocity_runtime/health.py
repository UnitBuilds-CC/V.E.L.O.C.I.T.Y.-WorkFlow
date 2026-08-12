"""
Velocity Runtime health checks.

Provides health status reporting for liveness and readiness probes.
"""

import time
import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class HealthCheckResult:
    """Result of a single health check."""
    name: str
    status: str  # "healthy", "degraded", "unhealthy"
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)
    latency_ms: float = 0.0


@dataclass
class HealthStatus:
    """Overall health status."""
    status: str  # "healthy", "degraded", "unhealthy"
    checks: List[HealthCheckResult] = field(default_factory=list)
    timestamp: float = 0.0
    uptime_seconds: float = 0.0

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "timestamp": self.timestamp,
            "uptime_seconds": round(self.uptime_seconds, 2),
            "checks": [
                {
                    "name": c.name,
                    "status": c.status,
                    "message": c.message,
                    "details": c.details,
                    "latency_ms": round(c.latency_ms, 2),
                }
                for c in self.checks
            ],
        }


# Health check function type
HealthCheckFn = Callable[[], Any]  # returns HealthCheckResult or raises


class HealthChecker:
    """Manages health checks for the runtime."""

    def __init__(self, start_time: Optional[float] = None):
        self._checks: Dict[str, HealthCheckFn] = {}
        self._start_time = start_time or time.monotonic()

    def register(self, name: str, fn: HealthCheckFn) -> None:
        """Register a health check function."""
        self._checks[name] = fn

    def unregister(self, name: str) -> None:
        """Remove a health check."""
        self._checks.pop(name, None)

    async def check(self) -> HealthStatus:
        """Run all health checks and return overall status."""
        results = []
        overall = "healthy"

        for name, fn in self._checks.items():
            start = time.monotonic()
            try:
                result = fn()
                if asyncio.iscoroutine(result):
                    result = await result
                if isinstance(result, HealthCheckResult):
                    result.latency_ms = (time.monotonic() - start) * 1000
                    results.append(result)
                    if result.status == "unhealthy":
                        overall = "unhealthy"
                    elif result.status == "degraded" and overall != "unhealthy":
                        overall = "degraded"
                else:
                    # Treat non-HealthCheckResult as healthy
                    results.append(HealthCheckResult(
                        name=name, status="healthy",
                        latency_ms=(time.monotonic() - start) * 1000,
                    ))
            except Exception as e:
                results.append(HealthCheckResult(
                    name=name, status="unhealthy",
                    message=str(e),
                    latency_ms=(time.monotonic() - start) * 1000,
                ))
                overall = "unhealthy"

        return HealthStatus(
            status=overall,
            checks=results,
            timestamp=time.time(),
            uptime_seconds=time.monotonic() - self._start_time,
        )


# ─── Built-in health checks ─────────────────────────────────────────────────

def make_liveness_check() -> HealthCheckFn:
    """Always-healthy liveness probe."""
    def check() -> HealthCheckResult:
        return HealthCheckResult(name="liveness", status="healthy", message="alive")
    return check


def make_readiness_check(server_ref: Any) -> HealthCheckFn:
    """Readiness probe: checks if server has services and is not shutting down."""
    def check() -> HealthCheckResult:
        server = server_ref()
        if server is None:
            return HealthCheckResult(name="readiness", status="unhealthy", message="server not available")
        if getattr(server, '_shutting_down', False):
            return HealthCheckResult(name="readiness", status="degraded", message="shutting down")
        services = server.list_services()
        if not services:
            return HealthCheckResult(
                name="readiness", status="degraded",
                message="no services registered",
                details={"registered_services": 0},
            )
        return HealthCheckResult(
            name="readiness", status="healthy",
            details={"registered_services": len(services)},
        )
    return check


def make_memory_check(max_memory_mb: float = 1024.0) -> HealthCheckFn:
    """Memory usage health check."""
    def check() -> HealthCheckResult:
        try:
            import resource
            usage_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            usage_mb = usage_kb / 1024.0
            if usage_mb > max_memory_mb:
                return HealthCheckResult(
                    name="memory", status="degraded",
                    message=f"Memory usage {usage_mb:.1f}MB exceeds threshold {max_memory_mb}MB",
                    details={"usage_mb": round(usage_mb, 1), "threshold_mb": max_memory_mb},
                )
            return HealthCheckResult(
                name="memory", status="healthy",
                details={"usage_mb": round(usage_mb, 1), "threshold_mb": max_memory_mb},
            )
        except ImportError:
            return HealthCheckResult(name="memory", status="healthy", message="resource module not available")
    return check
