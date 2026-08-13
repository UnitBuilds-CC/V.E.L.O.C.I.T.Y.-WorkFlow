#!/usr/bin/env python3
"""
Example: Parent-child workflow orchestration using the VELOCITY-WorkFlow Python SDK.

Demonstrates:
  - Starting a parent workflow
  - Spawning child workflows from the parent
  - Waiting for children to complete
  - Aggregating child results in the parent

Prerequisites:
    1. Start the VELOCITY-WorkFlow server:
       cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server && dotnet run
    2. pip install -r requirements.txt
    3. python examples/child_workflow.py
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from velocity_sdk import VelocityClient, WorkflowStatus


def run_child_workflow(client, child_type: str, order_id: int) -> int:
    """Start and complete a child workflow, returning its workflow key."""
    handle = client.start_workflow(
        workflow_type=child_type,
        namespace="default",
        task_queue="children",
        total_steps=2,
        input_data=f'{{"order_id": {order_id}}}'.encode(),
    )
    print(f"   Child '{child_type}' started: key={handle.workflow_key}")

    # Simulate child processing steps
    client.signal_workflow(handle.workflow_key, "process", b"{}")
    client.complete_workflow(handle.workflow_key, b'{"child_result": "ok"}')

    desc = client.describe_workflow(handle.workflow_key)
    print(f"   Child '{child_type}' completed: status={desc.status.name}")
    return handle.workflow_key


def main():
    print("=== VELOCITY-WorkFlow Python SDK — Child Workflows ===\n")

    with VelocityClient("localhost:7234") as client:
        # 1. Start the parent workflow
        parent = client.start_workflow(
            workflow_type="order-orchestrator",
            namespace="default",
            task_queue="orchestration",
            total_steps=4,
        )
        print(f"1. Parent workflow started: key={parent.workflow_key}")

        # 2. Spawn child workflows
        print("\n2. Spawning child workflows...")
        child_keys = []
        child_types = ["validate-order", "process-payment", "arrange-shipping"]
        for i, child_type in enumerate(child_types):
            key = run_child_workflow(client, child_type, order_id=parent.workflow_key + i)
            child_keys.append(key)

        # 3. Signal parent that all children are done
        print("\n3. All children completed — signaling parent...")
        client.signal_workflow(
            parent.workflow_key,
            "children-complete",
            f'{{"children": {child_keys}}}'.encode(),
        )

        # 4. Complete the parent workflow
        client.complete_workflow(
            parent.workflow_key,
            b'{"result": "all_children_done"}',
        )

        # 5. Verify parent is completed
        desc = client.describe_workflow(parent.workflow_key)
        print(f"4. Parent final status: {desc.status.name}")
        assert desc.status == WorkflowStatus.COMPLETED

    print("\n=== Child workflow example finished! ===")


if __name__ == "__main__":
    main()
