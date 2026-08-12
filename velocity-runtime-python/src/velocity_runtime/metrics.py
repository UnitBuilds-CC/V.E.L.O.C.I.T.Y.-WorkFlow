"""
Velocity Runtime metrics collection.

Lightweight in-process metrics with Prometheus-compatible output.
"""

import time
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class Counter:
    """Monotonically increasing counter."""
    name: str
    description: str
    labels: Dict[str, str] = field(default_factory=dict)
    _value: float = 0.0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def inc(self, amount: float = 1.0) -> None:
        with self._lock:
            self._value += amount

    @property
    def value(self) -> float:
        return self._value


@dataclass
class Histogram:
    """Tracks distribution of observed values."""
    name: str
    description: str
    labels: Dict[str, str] = field(default_factory=dict)
    _sum: float = 0.0
    _count: int = 0
    _min: float = float("inf")
    _max: float = float("-inf")
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def observe(self, value: float) -> None:
        with self._lock:
            self._sum += value
            self._count += 1
            if value < self._min:
                self._min = value
            if value > self._max:
                self._max = value

    @property
    def sum(self) -> float:
        return self._sum

    @property
    def count(self) -> int:
        return self._count

    @property
    def avg(self) -> float:
        return self._sum / self._count if self._count > 0 else 0.0

    @property
    def min(self) -> float:
        return self._min if self._count > 0 else 0.0

    @property
    def max(self) -> float:
        return self._max if self._count > 0 else 0.0


class MetricsCollector:
    """Central metrics registry for the Velocity Runtime."""

    def __init__(self):
        self._counters: Dict[str, Counter] = {}
        self._histograms: Dict[str, Histogram] = {}
        self._lock = threading.Lock()
        self._start_time = time.monotonic()
        self._init_default_metrics()

    def _init_default_metrics(self) -> None:
        """Register default metrics."""
        self._counters["invocations_total"] = Counter(
            "velocity_invocations_total",
            "Total number of handler invocations",
        )
        self._counters["invocations_success"] = Counter(
            "velocity_invocations_success_total",
            "Total successful handler invocations",
        )
        self._counters["invocations_failed"] = Counter(
            "velocity_invocations_failed_total",
            "Total failed handler invocations",
        )
        self._counters["invocations_timeout"] = Counter(
            "velocity_invocations_timeout_total",
            "Total timed-out handler invocations",
        )
        self._histograms["invocation_duration_ms"] = Histogram(
            "velocity_invocation_duration_ms",
            "Handler invocation duration in milliseconds",
        )
        self._counters["services_registered"] = Counter(
            "velocity_services_registered_total",
            "Total services registered",
        )
        self._counters["awakeables_created"] = Counter(
            "velocity_awakeables_created_total",
            "Total awakeables created",
        )
        self._counters["promises_resolved"] = Counter(
            "velocity_promises_resolved_total",
            "Total durable promises resolved",
        )
        self._counters["promises_rejected"] = Counter(
            "velocity_promises_rejected_total",
            "Total durable promises rejected",
        )

    def counter(self, name: str) -> Counter:
        with self._lock:
            if name not in self._counters:
                self._counters[name] = Counter(name, name)
            return self._counters[name]

    def histogram(self, name: str) -> Histogram:
        with self._lock:
            if name not in self._histograms:
                self._histograms[name] = Histogram(name, name)
            return self._histograms[name]

    # ─── High-level recording methods ────────────────────────────────────

    def record_invocation_start(self, service_name: str, handler_name: str) -> None:
        self._counters["invocations_total"].inc()

    def record_invocation_complete(
        self, service_name: str, handler_name: str, duration_ms: float, success: bool
    ) -> None:
        self._histograms["invocation_duration_ms"].observe(duration_ms)
        if success:
            self._counters["invocations_success"].inc()
        else:
            self._counters["invocations_failed"].inc()

    def record_timeout(self) -> None:
        self._counters["invocations_timeout"].inc()

    def record_service_registered(self) -> None:
        self._counters["services_registered"].inc()

    def record_awakeable_created(self) -> None:
        self._counters["awakeables_created"].inc()

    def record_promise_resolved(self) -> None:
        self._counters["promises_resolved"].inc()

    def record_promise_rejected(self) -> None:
        self._counters["promises_rejected"].inc()

    # ─── Output ──────────────────────────────────────────────────────────

    def snapshot(self) -> dict:
        """Return a JSON-serializable snapshot of all metrics."""
        return {
            "uptime_seconds": round(time.monotonic() - self._start_time, 2),
            "counters": {name: c.value for name, c in self._counters.items()},
            "histograms": {
                name: {
                    "sum": h.sum,
                    "count": h.count,
                    "avg": round(h.avg, 3),
                    "min": round(h.min, 3),
                    "max": round(h.max, 3),
                }
                for name, h in self._histograms.items()
            },
        }

    def prometheus_text(self) -> str:
        """Render metrics in Prometheus text exposition format."""
        lines = []
        for name, c in self._counters.items():
            lines.append(f"# HELP {c.name} {c.description}")
            lines.append(f"# TYPE {c.name} counter")
            lines.append(f"{c.name} {c.value}")
        for name, h in self._histograms.items():
            lines.append(f"# HELP {h.name} {h.description}")
            lines.append(f"# TYPE {h.name} summary")
            lines.append(f"{h.name}_sum {h.sum}")
            lines.append(f"{h.name}_count {h.count}")
        return "\n".join(lines) + "\n"

    def reset(self) -> None:
        """Reset all metrics (useful for testing)."""
        for c in self._counters.values():
            with c._lock:
                c._value = 0.0
        for h in self._histograms.values():
            with h._lock:
                h._sum = 0.0
                h._count = 0
                h._min = float("inf")
                h._max = float("-inf")
