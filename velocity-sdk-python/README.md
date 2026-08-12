# V.E.L.O.C.I.T.Y.-WorkFlow Python SDK

Python SDK for V.E.L.O.C.I.T.Y.-WorkFlow - a hardware-native zero-allocation durable execution engine and Temporal alternative.

## Installation

```bash
pip install velocity-workflow
```

## Quick Start

### Define a Workflow

```python
from velocity import WorkflowContext

def greeting_workflow(context: WorkflowContext, input: dict) -> str:
    name = input["name"]
    print(f"Workflow started: {context.workflow_id}")
    
    # Execute an activity
    greeting = f"Hello, {name}! Welcome to V.E.L.O.C.I.T.Y.-WorkFlow."
    
    print(f"Workflow completed: {context.workflow_id}")
    return greeting
```

### Define an Activity

```python
from velocity import ActivityContext

def greet_activity(context: ActivityContext, name: str) -> str:
    print(f"Activity executing: greeting {name}")
    return f"Hello, {name}! Welcome to V.E.L.O.C.I.T.Y.-WorkFlow."
```

### Start a Worker

```python
from velocity import Worker, WorkerOptions

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

worker.run()
```

### Start a Workflow

```python
from velocity import Client, ClientOptions, WorkflowOptions

client = Client(ClientOptions(
    host_port="localhost:7233",
    namespace="default",
))

# Start workflow
execution = client.start_workflow(WorkflowOptions(
    workflow_id="greeting-1",
    workflow_type="greeting-workflow",
    task_queue="greeting-queue",
    input={"name": "World"},
))

print(f"Started workflow: {execution.workflow_id}")

# Wait for result
handle = client.get_workflow(execution.workflow_id)
result = handle.result()
print(f"Workflow result: {result}")

client.close()
```

## Features

- **Durable Execution**: Workflows survive process crashes and server restarts
- **Activity Support**: Execute unreliable code in activities with automatic retries
- **Timers**: Sleep and schedule future work
- **Signals**: Send external events to running workflows
- **Queries**: Query workflow state without affecting execution
- **Child Workflows**: Compose workflows hierarchically
- **Search Attributes**: Index workflows for visibility
- **Memo**: Store arbitrary data with workflows

## API Reference

### Client

- `Client(options)` - Create a new client
- `start_workflow(options)` - Start a new workflow execution
- `execute_workflow(options, timeout)` - Start workflow and wait for result
- `signal_workflow(workflow_id, signal_name, input)` - Signal a running workflow
- `query_workflow(workflow_id, query_type, input)` - Query a workflow
- `terminate_workflow(workflow_id, reason)` - Terminate a workflow
- `cancel_workflow(workflow_id)` - Cancel a workflow
- `describe_workflow(workflow_id)` - Get workflow details
- `get_workflow_history(workflow_id)` - Get workflow history
- `get_workflow(workflow_id)` - Get a workflow handle

### Worker

- `Worker(options)` - Create a new worker
- `run()` - Start the worker (blocks)
- `stop()` - Stop the worker
- `is_running()` - Check if worker is running

### Workflow Registration

- `register_workflow(name, func)` - Register a workflow function
- `get_workflow(name)` - Get a registered workflow
- `has_workflow(name)` - Check if workflow is registered

### Activity Registration

- `register_activity(name, func)` - Register an activity function
- `get_activity(name)` - Get a registered activity
- `has_activity(name)` - Check if activity is registered

## Examples

See the `examples/` directory for complete examples:

- `hello_world.py` - Simple greeting workflow
- `timer.py` - Timer and sleep example
- `signal.py` - Signal handling example
- `child_workflow.py` - Child workflow composition

## Development

```bash
# Install dependencies
pip install -e ".[dev]"

# Format
black src/

# Type check
mypy src/

# Test
pytest
```

## License

MIT
