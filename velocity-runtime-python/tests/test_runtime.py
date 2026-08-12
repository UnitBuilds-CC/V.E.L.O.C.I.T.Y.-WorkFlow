"""Tests for the Velocity Runtime SDK (Restate-compatible)."""

import asyncio
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from velocity_runtime import (
    VirtualObject, Service, Workflow,
    ObjectContext, Context, WorkflowContext,
    Awakeable, DurablePromise, HandlerKind,
    RuntimeServer, app,
)


def run_test(name, coro):
    """Run an async test and report result."""
    try:
        asyncio.get_event_loop().run_until_complete(coro)
        print(f"  {name}: PASS")
        return True
    except Exception as e:
        print(f"  {name}: FAIL — {e}")
        return False


# ─── Virtual Object Tests ──────────────────────────────────────────────────

async def test_virtual_object_creation():
    chat = VirtualObject("ChatAgent")
    assert chat.name == "ChatAgent"

    @chat.handler()
    async def message(ctx: ObjectContext, query: str):
        return f"reply: {query}"

    assert "message" in chat.handlers


async def test_virtual_object_invocation():
    chat = VirtualObject("ChatAgent")

    @chat.handler()
    async def message(ctx: ObjectContext, query: str):
        return f"reply: {query}"

    server = RuntimeServer()
    server.register(chat)

    inv_id = await server.invoke("ChatAgent", "message", key="session-1", input_data="hello")
    await asyncio.sleep(0.1)  # Let it execute

    inv = server.get_invocation(inv_id)
    assert inv.state == "completed"
    assert inv.output_data == "reply: hello"


async def test_virtual_object_state():
    chat = VirtualObject("ChatAgent")

    @chat.handler()
    async def message(ctx: ObjectContext, query: str):
        history = await ctx.get("history") or []
        history.append(query)
        await ctx.set("history", history)
        return len(history)

    server = RuntimeServer()
    server.register(chat)

    # First message
    inv1 = await server.invoke("ChatAgent", "message", key="session-1", input_data="hello")
    await asyncio.sleep(0.1)
    assert server.get_invocation(inv1).output_data == 1

    # Second message — state persists
    inv2 = await server.invoke("ChatAgent", "message", key="session-1", input_data="world")
    await asyncio.sleep(0.1)
    assert server.get_invocation(inv2).output_data == 2


async def test_virtual_object_state_isolation():
    chat = VirtualObject("ChatAgent")

    @chat.handler()
    async def message(ctx: ObjectContext, query: str):
        count = await ctx.get("count") or 0
        count += 1
        await ctx.set("count", count)
        return count

    server = RuntimeServer()
    server.register(chat)

    inv1 = await server.invoke("ChatAgent", "message", key="session-1", input_data="a")
    inv2 = await server.invoke("ChatAgent", "message", key="session-2", input_data="b")
    await asyncio.sleep(0.1)

    assert server.get_invocation(inv1).output_data == 1
    assert server.get_invocation(inv2).output_data == 1  # Isolated state


async def test_single_writer_serialization():
    chat = VirtualObject("ChatAgent")
    execution_order = []

    @chat.handler()
    async def message(ctx: ObjectContext, query: str):
        execution_order.append(query)
        await asyncio.sleep(0.05)  # Simulate work
        return query

    server = RuntimeServer()
    server.register(chat)

    inv1 = await server.invoke("ChatAgent", "message", key="session-1", input_data="first")
    inv2 = await server.invoke("ChatAgent", "message", key="session-1", input_data="second")
    await asyncio.sleep(0.3)

    # Both should complete
    assert server.get_invocation(inv1).state == "completed"
    assert server.get_invocation(inv2).state == "completed"


# ─── Service Tests ─────────────────────────────────────────────────────────

async def test_service_handler():
    payment = Service("PaymentService")

    @payment.handler()
    async def charge(ctx: Context, amount: float):
        return {"status": "charged", "amount": amount}

    server = RuntimeServer()
    server.register(payment)

    inv_id = await server.invoke("PaymentService", "charge", input_data=99.99)
    await asyncio.sleep(0.1)

    inv = server.get_invocation(inv_id)
    assert inv.state == "completed"
    assert inv.output_data["amount"] == 99.99


# ─── Workflow Tests ────────────────────────────────────────────────────────

async def test_workflow_handler():
    order_wf = Workflow("OrderWorkflow")

    @order_wf.handler()
    async def run(ctx: WorkflowContext, order_id: str):
        result1 = await ctx.run(lambda: f"charged-{order_id}")
        result2 = await ctx.run(lambda: f"shipped-{order_id}")
        return [result1, result2]

    server = RuntimeServer()
    server.register(order_wf)

    inv_id = await server.invoke("OrderWorkflow", "run", key="order-123", input_data="order-123")
    await asyncio.sleep(0.1)

    inv = server.get_invocation(inv_id)
    assert inv.state == "completed"
    assert inv.output_data == ["charged-order-123", "shipped-order-123"]


# ─── Durable Step Tests ────────────────────────────────────────────────────

async def test_durable_step():
    svc = Service("TestService")

    @svc.handler()
    async def handler(ctx: Context, data: str):
        step1 = await ctx.run(lambda: data.upper())
        step2 = await ctx.run(lambda: step1 + "!")
        return step2

    server = RuntimeServer()
    server.register(svc)

    inv_id = await server.invoke("TestService", "handler", input_data="hello")
    await asyncio.sleep(0.1)

    inv = server.get_invocation(inv_id)
    assert inv.output_data == "HELLO!"
    assert len(inv.journal) == 2  # Two durable steps


# ─── Awakeable Tests ───────────────────────────────────────────────────────

async def test_awakeable():
    awk = Awakeable(id="test-awk-1")
    assert not awk.resolved

    awk.resolve("approved")
    assert awk.resolved
    result = await awk.wait()
    assert result == "approved"


async def test_awakeable_rejection():
    awk = Awakeable(id="test-awk-2")
    awk.reject("timeout")
    assert awk.resolved

    try:
        await awk.wait()
        assert False, "Should have raised"
    except RuntimeError as e:
        assert "timeout" in str(e)


# ─── Durable Promise Tests ─────────────────────────────────────────────────

async def test_durable_promise():
    promise = DurablePromise(id="approval-1")
    assert promise.pending

    promise.resolve("approved")
    assert promise.resolved
    result = await promise.await_value()
    assert result == "approved"


async def test_durable_promise_rejection():
    promise = DurablePromise(id="approval-2")
    promise.reject("denied")

    try:
        await promise.await_value()
        assert False, "Should have raised"
    except RuntimeError as e:
        assert "denied" in str(e)


async def test_durable_promise_double_resolve():
    promise = DurablePromise(id="approval-3")
    promise.resolve("ok")
    try:
        promise.resolve("again")
        assert False, "Should have raised"
    except RuntimeError:
        pass


# ─── App Factory Tests ─────────────────────────────────────────────────────

async def test_app_factory():
    chat = VirtualObject("Chat")
    payment = Service("Payment")

    @chat.handler()
    async def msg(ctx: ObjectContext, q: str):
        return q

    @payment.handler()
    async def charge(ctx: Context, amount: float):
        return amount

    server = app(services=[chat, payment])
    assert "Chat" in server.list_services()
    assert "Payment" in server.list_services()


# ─── Stats Tests ───────────────────────────────────────────────────────────

async def test_runtime_stats():
    chat = VirtualObject("Chat")

    @chat.handler()
    async def msg(ctx: ObjectContext, q: str):
        return q

    server = RuntimeServer()
    server.register(chat)

    await server.invoke("Chat", "msg", key="k1", input_data="a")
    await server.invoke("Chat", "msg", key="k2", input_data="b")
    await asyncio.sleep(0.1)

    stats = server.get_stats()
    assert stats["registered_services"] == 1
    assert stats["total_invocations"] == 2
    assert stats["completed_invocations"] == 2


# ─── Idempotency Tests ─────────────────────────────────────────────────────

async def test_idempotent_invocation():
    svc = Service("Svc")

    @svc.handler()
    async def handler(ctx: Context, data: str):
        return data

    server = RuntimeServer()
    server.register(svc)

    id1 = await server.invoke("Svc", "handler", input_data="x", idempotency_key="idem-1")
    id2 = await server.invoke("Svc", "handler", input_data="x", idempotency_key="idem-1")
    assert id1 == id2


# ─── Run All Tests ─────────────────────────────────────────────────────────

def main():
    tests = [
        ("virtual_object_creation", test_virtual_object_creation()),
        ("virtual_object_invocation", test_virtual_object_invocation()),
        ("virtual_object_state", test_virtual_object_state()),
        ("virtual_object_state_isolation", test_virtual_object_state_isolation()),
        ("single_writer_serialization", test_single_writer_serialization()),
        ("service_handler", test_service_handler()),
        ("workflow_handler", test_workflow_handler()),
        ("durable_step", test_durable_step()),
        ("awakeable", test_awakeable()),
        ("awakeable_rejection", test_awakeable_rejection()),
        ("durable_promise", test_durable_promise()),
        ("durable_promise_rejection", test_durable_promise_rejection()),
        ("durable_promise_double_resolve", test_durable_promise_double_resolve()),
        ("app_factory", test_app_factory()),
        ("runtime_stats", test_runtime_stats()),
        ("idempotent_invocation", test_idempotent_invocation()),
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
