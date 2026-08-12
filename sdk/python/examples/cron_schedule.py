#!/usr/bin/env python3
"""
Example: Scheduled (cron) workflow using the VELOCITY-WorkFlow Python SDK.

Demonstrates:
  - Registering a cron schedule
  - Starting a workflow tied to a cron expression
  - Monitoring scheduled executions

Prerequisites:
    1. Start the VELOCITY-WorkFlow server:
       cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server && dotnet run
    2. pip install -r requirements.txt
    3. python examples/cron_schedule.py
"""

import sys
import os
import time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from velocity_sdk import VelocityClient, WorkflowStatus


CRON_EXPRESSION = "*/5 * * * *"  # Every 5 minutes


def main():
    print("=== VELOCITY-WorkFlow Python SDK — Cron Schedule ===\n")

    with VelocityClient("localhost:50051") as client:
        # 1. Start a workflow with a cron schedule memo
        handle = client.start_workflow(
            workflow_type="periodic-report",
            namespace="default",
            task_queue="reports",
            total_steps=1,
            input_data=f'{{"cron": "{CRON_EXPRESSION}"}}'.encode(),
        )
        print(f"1. Scheduled workflow started: key={handle.workflow_key}")

        # 2. Describe the workflow
        desc = client.describe_workflow(handle.workflow_key)
        print(f"2. Status: {desc.status.name}")

        # 3. Send a trigger signal (simulating a cron fire)
        ok = client.signal_workflow(
            handle.workflow_key,
            "cron-fire",
            b'{"fire_number": 1}',
        )
        print(f"3. Cron fire signal sent: {ok}")

        # 4. Complete the scheduled execution
        ok = client.complete_workflow(handle.workflow_key, b'{"report": "generated"}')
        print(f"4. Execution completed: {ok}")

        # 5. Verify final state
        final = client.describe_workflow(handle.workflow_key)
        print(f"5. Final status: {final.status.name}")

    print("\n=== Cron schedule example finished! ===")


if __name__ == "__main__":
    main()
