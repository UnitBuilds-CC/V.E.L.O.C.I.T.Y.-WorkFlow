#!/usr/bin/env python3
"""
Example: Simple workflow worker using the VELOCITY-WorkFlow Python SDK.

This demonstrates that the VELOCITY-WorkFlow gRPC API is language-agnostic.
The same workflow engine serves C#, Python, Go, Java, or any gRPC client.

Prerequisites:
    1. Start the VELOCITY-WorkFlow server:
       cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server
       dotnet run

    2. Generate gRPC stubs:
       cd VELOCITY-WorkFlow/sdk/python
       pip install -r requirements.txt
       python -m grpc_tools.protoc \
           -I../../src/Velocity.Workflow.Server/Protos \
           --python_out=velocity_sdk --grpc_python_out=velocity_sdk \
           ../../src/Velocity.Workflow.Server/Protos/workflow_service.proto

    3. Run this example:
       python examples/simple_worker.py
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from velocity_sdk import VelocityClient, WorkflowStatus


def main():
    print("=== VELOCITY-WorkFlow Python SDK Example ===\n")

    # Connect to the server (no JWT = anonymous access)
    with VelocityClient("localhost:50051") as client:
        # 1. Start a workflow
        print("1. Starting workflow...")
        handle = client.start_workflow(
            workflow_type="order-processing",
            namespace="default",
            task_queue="orders",
            total_steps=5,
            input_data=b'{"order_id": 12345}',
        )
        print(f"   Workflow started: key={handle.workflow_key}")

        # 2. Describe the workflow
        print("\n2. Describing workflow...")
        desc = client.describe_workflow(handle.workflow_key)
        print(f"   Status: {desc.status.name}")
        print(f"   Step: {desc.current_step}/{desc.total_steps}")

        # 3. Send a signal
        print("\n3. Sending signal...")
        ok = client.signal_workflow(
            handle.workflow_key,
            "payment-confirmed",
            b'{"amount": 99.99}',
        )
        print(f"   Signal sent: {ok}")

        # 4. Complete the workflow
        print("\n4. Completing workflow...")
        ok = client.complete_workflow(handle.workflow_key, b'{"result": "order shipped"}')
        print(f"   Completed: {ok}")

        # 5. Verify final state
        print("\n5. Verifying final state...")
        desc = client.describe_workflow(handle.workflow_key)
        print(f"   Status: {desc.status.name}")
        assert desc.status == WorkflowStatus.COMPLETED, f"Expected COMPLETED, got {desc.status}"

    print("\n=== All operations succeeded! ===")
    print("The Python SDK successfully communicated with the Rust/C# workflow engine via gRPC.")


if __name__ == "__main__":
    main()
