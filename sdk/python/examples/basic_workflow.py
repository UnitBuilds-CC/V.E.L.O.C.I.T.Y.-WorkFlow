#!/usr/bin/env python3
"""
Example: Basic workflow with signal and query using the VELOCITY-WorkFlow Python SDK.

Demonstrates:
  - Starting a workflow
  - Sending signals
  - Querying workflow state
  - Completing the workflow

Prerequisites:
    1. Start the VELOCITY-WorkFlow server:
       cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server && dotnet run

    2. Install dependencies:
       cd VELOCITY-WorkFlow/sdk/python && pip install -r requirements.txt

    3. Run this example:
       python examples/basic_workflow.py
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from velocity_sdk import VelocityClient, WorkflowStatus, WorkflowStub, WorkflowStubOptions


def main():
    print("=== VELOCITY-WorkFlow Python SDK — Basic Workflow ===\n")

    with VelocityClient("localhost:7234") as client:
        # 1. Start a workflow using the typed stub
        stub = WorkflowStub(client, WorkflowStubOptions(
            workflow_type="order-processing",
            namespace="default",
            task_queue="orders",
        ))
        handle = stub.start({"order_id": 12345}, total_steps=3)
        print(f"1. Workflow started: key={handle.workflow_key}")

        # 2. Describe the workflow
        desc = client.describe_workflow(handle.workflow_key)
        print(f"2. Status: {desc.status.name}, Step: {desc.current_step}/{desc.total_steps}")

        # 3. Send a signal (e.g. payment confirmed)
        ok = client.signal_workflow(
            handle.workflow_key,
            "payment-confirmed",
            b'{"amount": 99.99}',
        )
        print(f"3. Signal sent: {ok}")

        # 4. Query the workflow state
        state = client.describe_workflow(handle.workflow_key)
        print(f"4. Queried state: status={state.status.name}")

        # 5. Complete the workflow
        ok = client.complete_workflow(handle.workflow_key, b'{"result": "order shipped"}')
        print(f"5. Completed: {ok}")

        # 6. Verify final state
        final = client.describe_workflow(handle.workflow_key)
        assert final.status == WorkflowStatus.COMPLETED
        print(f"6. Final status: {final.status.name}")

    print("\n=== All operations succeeded! ===")


if __name__ == "__main__":
    main()
