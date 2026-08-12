#!/usr/bin/env python3
"""
Example: Multi-step saga with compensation using the VELOCITY-WorkFlow Python SDK.

Demonstrates:
  - Defining a saga with compensable steps
  - Executing steps in order
  - Triggering compensation on failure
  - Rolling back completed steps in reverse order

Prerequisites:
    1. Start the VELOCITY-WorkFlow server:
       cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server && dotnet run
    2. pip install -r requirements.txt
    3. python examples/saga_pattern.py
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from velocity_sdk import VelocityClient, WorkflowStatus


# ── Saga step definitions ─────────────────────────────────────────────────────

STEPS = [
    {"name": "reserve_inventory", "compensate": "release_inventory"},
    {"name": "charge_payment",    "compensate": "refund_payment"},
    {"name": "book_shipping",     "compensate": "cancel_shipping"},
    {"name": "send_confirmation", "compensate": "send_cancellation_notice"},
]


def execute_step(client, workflow_key: int, step: dict) -> bool:
    """Execute a saga step; returns True on success."""
    print(f"   Executing: {step['name']}")
    ok = client.signal_workflow(workflow_key, step["name"], b"{}")
    return ok


def compensate_step(client, workflow_key: int, step: dict) -> None:
    """Run the compensation action for a failed step."""
    print(f"   Compensating: {step['compensate']}")
    client.signal_workflow(workflow_key, step["compensate"], b"{}")


def run_saga(client, simulate_failure_at: int | None = None) -> bool:
    """
    Execute the saga. If simulate_failure_at is set, the step at that index
    will fail, triggering compensation for all previously completed steps.
    """
    handle = client.start_workflow(
        workflow_type="order-saga",
        namespace="default",
        task_queue="orders",
        total_steps=len(STEPS),
    )
    print(f"  Saga started: key={handle.workflow_key}")

    completed_steps: list[dict] = []

    for i, step in enumerate(STEPS):
        # Simulate a failure at the specified step index
        if simulate_failure_at is not None and i == simulate_failure_at:
            print(f"\n   ✗ Step '{step['name']}' FAILED — triggering compensation")
            # Compensate in reverse order
            for prev in reversed(completed_steps):
                compensate_step(client, handle.workflow_key, prev)
            client.fail_workflow(handle.workflow_key, f"Step {step['name']} failed")
            return False

        success = execute_step(client, handle.workflow_key, step)
        if success:
            completed_steps.append(step)

    # All steps succeeded
    client.complete_workflow(handle.workflow_key, b'{"status": "saga_complete"}')
    print("   ✓ All saga steps completed successfully")
    return True


def main():
    print("=== VELOCITY-WorkFlow Python SDK — Saga Pattern ===\n")

    with VelocityClient("localhost:50051") as client:
        # Scenario 1: Happy path — all steps succeed
        print("Scenario 1: Happy path")
        run_saga(client, simulate_failure_at=None)

        # Scenario 2: Step 2 fails — triggers compensation
        print("\nScenario 2: Payment step fails (index=1)")
        run_saga(client, simulate_failure_at=1)

    print("\n=== Saga examples finished! ===")


if __name__ == "__main__":
    main()
