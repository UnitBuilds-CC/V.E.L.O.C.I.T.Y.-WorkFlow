"""Hello World example for V.E.L.O.C.I.T.Y.-WorkFlow Python SDK"""

import time
from velocity import (
    Client,
    ClientOptions,
    Worker,
    WorkerOptions,
    WorkflowOptions,
    WorkflowContext,
    ActivityContext,
)


def greeting_workflow(context: WorkflowContext, input: dict) -> str:
    """Simple workflow that greets a user"""
    name = input["name"]
    print(f"Workflow started: {context.workflow_id}")
    
    # In a real implementation, this would execute an activity
    greeting = f"Hello, {name}! Welcome to V.E.L.O.C.I.T.Y.-WorkFlow."
    
    print(f"Workflow completed: {context.workflow_id}")
    return greeting


def greet_activity(context: ActivityContext, name: str) -> str:
    """Activity that generates a greeting"""
    print(f"Activity executing: greeting {name}")
    return f"Hello, {name}! Welcome to V.E.L.O.C.I.T.Y.-WorkFlow."


def main():
    # Create worker
    worker = Worker(WorkerOptions(
        namespace="default",
        task_queue="greeting-queue",
        workflows={
            "greeting-workflow": greeting_workflow,
        },
        activities={
            "greet-activity": greet_activity,
        },
    ))
    
    # Start worker in a separate thread
    import threading
    worker_thread = threading.Thread(target=worker.run)
    worker_thread.daemon = True
    worker_thread.start()
    
    # Give worker time to start
    time.sleep(1)
    
    # Create client
    client = Client(ClientOptions(
        host_port="localhost:7233",
        namespace="default",
    ))
    
    # Start workflow
    execution = client.start_workflow(WorkflowOptions(
        workflow_id=f"greeting-{int(time.time() * 1000)}",
        workflow_type="greeting-workflow",
        task_queue="greeting-queue",
        input={"name": "World"},
    ))
    
    print(f"Started workflow: {execution.workflow_id}")
    
    # Wait for result
    handle = client.get_workflow(execution.workflow_id)
    result = handle.result()
    
    print(f"Workflow result: {result}")
    
    # Cleanup
    worker.stop()
    client.close()


if __name__ == "__main__":
    main()
