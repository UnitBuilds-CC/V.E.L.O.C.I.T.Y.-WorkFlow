"""
Example: Order processing workflow using the VELOCITY Python SDK.

Demonstrates the auto-apply decorator system for zero-config workflow discovery.
The Worker automatically discovers @workflow and @activity decorated classes/functions.

Usage:
    # Start the worker (it auto-discovers workflows in this module)
    python -m examples.order_workflow

    # In another terminal, start a workflow:
    from velocity_sdk import VelocityClient
    client = VelocityClient("localhost:7234")
    handle = client.start_workflow("OrderWorkflow", task_queue="orders", input_data=b'{"order_id": "ORD-123"}')
"""

import asyncio
import logging

from velocity_sdk import (
    Worker,
    workflow,
    activity,
    signal,
    query,
    WorkflowContext,
)

logging.basicConfig(level=logging.INFO)


# ─── Activities ───────────────────────────────────────────────────────────────

@activity
def validate_order(order_id: str) -> dict:
    """Validate an order before processing."""
    print(f"  [activity] Validating order {order_id}...")
    return {"valid": True, "order_id": order_id}


@activity
def process_payment(order_id: str, amount: float = 99.99) -> dict:
    """Charge the customer's payment method."""
    print(f"  [activity] Processing payment for {order_id} (${amount:.2f})...")
    return {"status": "charged", "order_id": order_id, "amount": amount}


@activity
def ship_order(order_id: str) -> dict:
    """Schedule shipment for the order."""
    print(f"  [activity] Shipping order {order_id}...")
    return {"status": "shipped", "order_id": order_id, "tracking": f"TRK-{order_id}"}


# ─── Workflow ─────────────────────────────────────────────────────────────────

@workflow(name="OrderWorkflow", task_queue="orders")
class OrderWorkflow:
    """
    Order processing workflow.

    Steps:
    1. Validate the order
    2. Process payment
    3. Ship the order
    4. Return the result
    """

    def __init__(self):
        self._status = "pending"
        self._cancelled = False

    async def run(self, ctx: WorkflowContext, order_id: str, amount: float = 99.99):
        self._status = "validating"
        validation = await ctx.execute_activity("validate_order", order_id)
        if not validation.get("valid"):
            self._status = "rejected"
            return {"status": "rejected", "reason": "invalid order"}

        if self._cancelled:
            return {"status": "cancelled"}

        self._status = "processing_payment"
        payment = await ctx.execute_activity("process_payment", order_id, amount)

        self._status = "shipping"
        shipment = await ctx.execute_activity("ship_order", order_id)

        self._status = "completed"
        return {
            "status": "completed",
            "order_id": order_id,
            "payment": payment,
            "shipment": shipment,
        }

    @signal("cancel")
    def handle_cancel(self, payload):
        """Handle cancellation signal."""
        self._cancelled = True
        print(f"  [signal] Cancel received for workflow")

    @query("status")
    def handle_status_query(self) -> str:
        """Return the current workflow status."""
        return self._status


# ─── Entry Point ──────────────────────────────────────────────────────────────

def main():
    """Start the worker with auto-discovery."""
    worker = Worker(
        task_queue="orders",
        server_address="localhost:7234",
        max_concurrent_workflow_tasks=10,
        max_concurrent_activity_tasks=100,
    )

    print("Starting VELOCITY Order Worker...")
    print("  Task queue: orders")
    print("  Workflows: OrderWorkflow")
    print("  Activities: validate_order, process_payment, ship_order")
    print("  Press Ctrl+C to stop")
    print()

    worker.run()


if __name__ == "__main__":
    main()
