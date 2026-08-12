"""Tests for Velocity Runtime production features."""

import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from velocity_runtime import (
    VirtualObject, Service, Workflow,
    ObjectContext, Context, WorkflowContext,
    RuntimeServer, ServerConfig, app,
    # Errors
    VelocityError, ServiceNotFoundError, HandlerNotFoundError,
    ShutdownError, SerializationError,
    # Middleware
    MiddlewareChain, MiddlewareContext,
    # Metrics
    MetricsCollector, Counter, Histogram,
    # Health
    HealthChecker, HealthCheckResult, HealthStatus,
    make_liveness_check, make_readiness_check,
    # Retry
    RetryPolicy, execute_with_retry, NO_RETRY_POLICY,
    # Transport
    InMemoryTransport, TransportRequest, TransportResponse,
    # Serialization
    serialize, deserialize, to_json, from_json, deep_merge,
)


def run_test(name, coro):
    try:
        asyncio.get_event_loop().run_until_complete(coro)
        print(f"  {name}: PASS")
        return True
    except Exception as e:
        print(f"  {name}: FAIL — {e}")
        import traceback; traceback.print_exc()
        return False


# ─── Error Tests ────────────────────────────────────────────────────────────

async def test_error_hierarchy():
    err = ServiceNotFoundError("FooService")
    assert err.code == "SERVICE_NOT_FOUND"
    assert err.service_name == "FooService"
    assert isinstance(err, VelocityError)
    assert "FooService" in err.message


async def test_error_details():
    err = HandlerNotFoundError("Svc", "handler")
    assert err.details["service_name"] == "Svc"
    assert err.details["handler_name"] == "handler"
    assert "Svc/handler" in repr(err)


# ─── Config Tests ───────────────────────────────────────────────────────────

async def test_config_defaults():
    cfg = ServerConfig()
    assert cfg.port == 9080
    assert cfg.max_concurrent_invocations == 256
    assert cfg.default_invocation_timeout_ms == 30_000
    assert cfg.max_retries == 3


async def test_config_validation():
    cfg = ServerConfig(port=0)
    try:
        cfg.validate()
        assert False, "Should have raised"
    except ValueError:
        pass


async def test_config_from_env():
    os.environ["VELOCITY_PORT"] = "8888"
    os.environ["VELOCITY_LOG_LEVEL"] = "DEBUG"
    cfg = ServerConfig.from_env()
    assert cfg.port == 8888
    assert cfg.log_level == "DEBUG"
    del os.environ["VELOCITY_PORT"]
    del os.environ["VELOCITY_LOG_LEVEL"]


# ─── Middleware Tests ────────────────────────────────────────────────────────

async def test_middleware_chain():
    chain = MiddlewareChain()
    calls = []

    async def mw1(ctx, next_fn):
        calls.append("mw1_before")
        result = await next_fn()
        calls.append("mw1_after")
        return result

    async def mw2(ctx, next_fn):
        calls.append("mw2_before")
        result = await next_fn()
        calls.append("mw2_after")
        return result

    chain.use(mw1)
    chain.use(mw2)
    assert len(chain.get_chain("any")) == 2


async def test_per_service_middleware():
    chain = MiddlewareChain()

    async def global_mw(ctx, next_fn):
        return await next_fn()

    async def svc_mw(ctx, next_fn):
        return await next_fn()

    chain.use(global_mw)
    chain.use_for("PaymentService", svc_mw)

    assert len(chain.get_chain("PaymentService")) == 2
    assert len(chain.get_chain("OtherService")) == 1


# ─── Metrics Tests ──────────────────────────────────────────────────────────

async def test_counter():
    c = Counter("test_counter", "A test counter")
    assert c.value == 0
    c.inc()
    assert c.value == 1
    c.inc(5)
    assert c.value == 6


async def test_histogram():
    h = Histogram("test_hist", "A test histogram")
    h.observe(10)
    h.observe(20)
    h.observe(30)
    assert h.count == 3
    assert h.sum == 60
    assert h.avg == 20.0
    assert h.min == 10.0
    assert h.max == 30.0


async def test_metrics_collector():
    mc = MetricsCollector()
    mc.record_invocation_start("Svc", "handler")
    mc.record_invocation_complete("Svc", "handler", 42.5, success=True)
    mc.record_invocation_start("Svc", "handler")
    mc.record_invocation_complete("Svc", "handler", 100.0, success=False)

    snap = mc.snapshot()
    assert snap["counters"]["invocations_total"] == 2
    assert snap["counters"]["invocations_success"] == 1
    assert snap["counters"]["invocations_failed"] == 1
    assert snap["histograms"]["invocation_duration_ms"]["count"] == 2


async def test_metrics_prometheus_output():
    mc = MetricsCollector()
    mc.record_invocation_start("Svc", "h")
    text = mc.prometheus_text()
    assert "velocity_invocations_total" in text
    assert "counter" in text


async def test_metrics_reset():
    mc = MetricsCollector()
    mc.record_invocation_start("Svc", "h")
    mc.reset()
    snap = mc.snapshot()
    assert snap["counters"]["invocations_total"] == 0


# ─── Health Tests ───────────────────────────────────────────────────────────

async def test_health_checker():
    hc = HealthChecker()
    hc.register("liveness", make_liveness_check())
    status = await hc.check()
    assert status.status == "healthy"
    assert len(status.checks) == 1
    assert status.checks[0].name == "liveness"


async def test_health_unhealthy():
    hc = HealthChecker()
    def bad_check():
        raise RuntimeError("disk full")
    hc.register("disk", bad_check)
    status = await hc.check()
    assert status.status == "unhealthy"
    assert "disk full" in status.checks[0].message


async def test_health_degraded():
    hc = HealthChecker()
    def degraded_check():
        return HealthCheckResult(name="memory", status="degraded", message="high usage")
    hc.register("memory", degraded_check)
    status = await hc.check()
    assert status.status == "degraded"


async def test_health_to_dict():
    hc = HealthChecker()
    hc.register("liveness", make_liveness_check())
    status = await hc.check()
    d = status.to_dict()
    assert d["status"] == "healthy"
    assert "checks" in d
    assert "uptime_seconds" in d


# ─── Retry Tests ────────────────────────────────────────────────────────────

async def test_retry_policy():
    policy = RetryPolicy(max_attempts=3, initial_delay_ms=10, jitter=False)
    assert policy.should_retry(Exception("err"), 1)
    assert policy.should_retry(Exception("err"), 2)
    assert not policy.should_retry(Exception("err"), 3)


async def test_retry_non_retryable():
    policy = RetryPolicy(
        max_attempts=3,
        non_retryable_exceptions={ValueError},
    )
    assert not policy.should_retry(ValueError("bad"), 1)
    assert policy.should_retry(TypeError("bad"), 1)


async def test_retry_delay():
    policy = RetryPolicy(initial_delay_ms=100, backoff_multiplier=2.0, jitter=False)
    assert policy.get_delay_ms(1) == 100
    assert policy.get_delay_ms(2) == 200
    assert policy.get_delay_ms(3) == 400


async def test_execute_with_retry_success():
    attempts = 0
    async def flaky():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError("transient")
        return "ok"

    result = await execute_with_retry(flaky, RetryPolicy(max_attempts=5, initial_delay_ms=1, jitter=False))
    assert result == "ok"
    assert attempts == 3


async def test_execute_with_retry_exhausted():
    async def always_fails():
        raise RuntimeError("permanent")

    try:
        await execute_with_retry(always_fails, RetryPolicy(max_attempts=2, initial_delay_ms=1, jitter=False))
        assert False, "Should have raised"
    except RuntimeError:
        pass


async def test_no_retry_policy():
    assert NO_RETRY_POLICY.max_attempts == 1


# ─── Transport Tests ────────────────────────────────────────────────────────

async def test_in_memory_transport():
    transport = InMemoryTransport()
    await transport.connect()
    assert transport.is_connected()

    req = TransportRequest(method="POST", path="/invoke", body={"test": True})
    resp = await transport.send(req)
    assert resp.ok
    assert resp.status_code == 200
    assert len(transport.sent_requests) == 1

    await transport.disconnect()
    assert not transport.is_connected()


async def test_in_memory_transport_custom_handler():
    transport = InMemoryTransport()
    await transport.connect()

    def handler(req):
        return TransportResponse(status_code=201, body={"id": "123"})

    transport.set_handler(handler)
    req = TransportRequest(method="POST", path="/create")
    resp = await transport.send(req)
    assert resp.status_code == 201
    assert resp.body["id"] == "123"


async def test_transport_not_connected():
    transport = InMemoryTransport()
    req = TransportRequest(method="GET", path="/health")
    try:
        await transport.send(req)
        assert False, "Should have raised"
    except Exception:
        pass


# ─── Serialization Tests ────────────────────────────────────────────────────

async def test_serialize_primitives():
    assert serialize("hello") == "hello"
    assert serialize(42) == 42
    assert serialize(None) is None
    assert serialize(True) is True


async def test_serialize_complex():
    import datetime
    dt = datetime.datetime(2024, 1, 15, 10, 30)
    assert serialize(dt) == "2024-01-15T10:30:00"
    assert serialize(b"binary") == "YmluYXJ5"  # base64


async def test_serialize_nested():
    result = serialize({"a": [1, "two", None], "b": {"c": True}})
    assert result == {"a": [1, "two", None], "b": {"c": True}}


async def test_to_json_from_json():
    data = {"key": "value", "num": 42}
    text = to_json(data)
    parsed = from_json(text)
    assert parsed == data


async def test_deep_merge():
    base = {"a": 1, "b": {"c": 2, "d": 3}}
    override = {"b": {"c": 99, "e": 5}, "f": 6}
    result = deep_merge(base, override)
    assert result == {"a": 1, "b": {"c": 99, "d": 3, "e": 5}, "f": 6}


# ─── Server Production Feature Tests ────────────────────────────────────────

async def test_server_service_not_found_error():
    server = RuntimeServer()
    try:
        await server.invoke("NonExistent", "handler")
        assert False, "Should have raised"
    except ServiceNotFoundError as e:
        assert e.service_name == "NonExistent"


async def test_server_handler_not_found_error():
    svc = Service("Svc")
    server = RuntimeServer()
    server.register(svc)
    try:
        await server.invoke("Svc", "nonexistent")
        assert False, "Should have raised"
    except HandlerNotFoundError as e:
        assert e.handler_name == "nonexistent"


async def test_server_duplicate_registration():
    svc = Service("Svc")
    server = RuntimeServer()
    server.register(svc)
    try:
        server.register(svc)
        assert False, "Should have raised"
    except ValueError:
        pass


async def test_server_list_invocations():
    svc = Service("Svc")

    @svc.handler()
    async def handler(ctx: Context, data: str):
        return data

    server = RuntimeServer()
    server.register(svc)

    await server.invoke("Svc", "handler", input_data="a")
    await server.invoke("Svc", "handler", input_data="b")
    await asyncio.sleep(0.1)

    invs = server.list_invocations()
    assert len(invs) == 2

    completed = server.list_invocations(state="completed")
    assert len(completed) == 2


async def test_server_graceful_shutdown():
    svc = Service("Svc")

    @svc.handler()
    async def handler(ctx: Context, data: str):
        await asyncio.sleep(0.05)
        return data

    server = RuntimeServer()
    server.register(svc)

    await server.invoke("Svc", "handler", input_data="x")
    await server.shutdown(grace_period_ms=1000)

    assert server.is_shutting_down

    try:
        await server.invoke("Svc", "handler", input_data="y")
        assert False, "Should have raised"
    except ShutdownError:
        pass


async def test_server_health_check():
    svc = Service("Svc")

    @svc.handler()
    async def handler(ctx: Context, data: str):
        return data

    server = RuntimeServer()
    server.register(svc)

    status = await server.health_check()
    assert status.status == "healthy"
    names = [c.name for c in status.checks]
    assert "liveness" in names
    assert "readiness" in names


async def test_server_stats_with_metrics():
    svc = Service("Svc")

    @svc.handler()
    async def handler(ctx: Context, data: str):
        return data

    server = RuntimeServer()
    server.register(svc)

    await server.invoke("Svc", "handler", input_data="x")
    await asyncio.sleep(0.1)

    stats = server.get_stats()
    assert "metrics" in stats
    assert stats["metrics"]["counters"]["invocations_total"] >= 1
    assert "uptime_seconds" in stats


async def test_server_config_integration():
    cfg = ServerConfig(
        max_concurrent_invocations=10,
        default_invocation_timeout_ms=5000,
        max_retries=1,
    )
    server = RuntimeServer(config=cfg)
    assert server.config.max_retries == 1


async def test_invocation_record_to_dict():
    svc = Service("Svc")

    @svc.handler()
    async def handler(ctx: Context, data: str):
        return data

    server = RuntimeServer()
    server.register(svc)

    inv_id = await server.invoke("Svc", "handler", input_data="test")
    await asyncio.sleep(0.1)

    inv = server.get_invocation(inv_id)
    d = inv.to_dict()
    assert d["invocation_id"] == inv_id
    assert d["state"] == "completed"
    assert d["service_name"] == "Svc"


# ─── Run All Tests ─────────────────────────────────────────────────────────

def main():
    tests = [
        # Errors
        ("error_hierarchy", test_error_hierarchy()),
        ("error_details", test_error_details()),
        # Config
        ("config_defaults", test_config_defaults()),
        ("config_validation", test_config_validation()),
        ("config_from_env", test_config_from_env()),
        # Middleware
        ("middleware_chain", test_middleware_chain()),
        ("per_service_middleware", test_per_service_middleware()),
        # Metrics
        ("counter", test_counter()),
        ("histogram", test_histogram()),
        ("metrics_collector", test_metrics_collector()),
        ("metrics_prometheus_output", test_metrics_prometheus_output()),
        ("metrics_reset", test_metrics_reset()),
        # Health
        ("health_checker", test_health_checker()),
        ("health_unhealthy", test_health_unhealthy()),
        ("health_degraded", test_health_degraded()),
        ("health_to_dict", test_health_to_dict()),
        # Retry
        ("retry_policy", test_retry_policy()),
        ("retry_non_retryable", test_retry_non_retryable()),
        ("retry_delay", test_retry_delay()),
        ("execute_with_retry_success", test_execute_with_retry_success()),
        ("execute_with_retry_exhausted", test_execute_with_retry_exhausted()),
        ("no_retry_policy", test_no_retry_policy()),
        # Transport
        ("in_memory_transport", test_in_memory_transport()),
        ("in_memory_transport_custom_handler", test_in_memory_transport_custom_handler()),
        ("transport_not_connected", test_transport_not_connected()),
        # Serialization
        ("serialize_primitives", test_serialize_primitives()),
        ("serialize_complex", test_serialize_complex()),
        ("serialize_nested", test_serialize_nested()),
        ("to_json_from_json", test_to_json_from_json()),
        ("deep_merge", test_deep_merge()),
        # Server production features
        ("server_service_not_found_error", test_server_service_not_found_error()),
        ("server_handler_not_found_error", test_server_handler_not_found_error()),
        ("server_duplicate_registration", test_server_duplicate_registration()),
        ("server_list_invocations", test_server_list_invocations()),
        ("server_graceful_shutdown", test_server_graceful_shutdown()),
        ("server_health_check", test_server_health_check()),
        ("server_stats_with_metrics", test_server_stats_with_metrics()),
        ("server_config_integration", test_server_config_integration()),
        ("invocation_record_to_dict", test_invocation_record_to_dict()),
    ]

    passed = 0
    failed = 0
    for name, coro in tests:
        if run_test(name, coro):
            passed += 1
        else:
            failed += 1

    print(f"\nResults: {passed} passed, {failed} failed, {passed + failed} total")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
