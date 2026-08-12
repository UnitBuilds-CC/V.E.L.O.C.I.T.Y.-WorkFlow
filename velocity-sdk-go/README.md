# V.E.L.O.C.I.T.Y.-WorkFlow Go SDK

Go SDK for V.E.L.O.C.I.T.Y.-WorkFlow - a hardware-native zero-allocation durable execution engine and Temporal alternative.

## Installation

```bash
go get github.com/velocity-workflow/sdk-go
```

## Quick Start

### Define a Workflow

```go
package main

import (
    "context"
    velocity "github.com/velocity-workflow/sdk-go"
)

func GreetingWorkflow(ctx velocity.WorkflowContext, input interface{}) (interface{}, error) {
    name := input.(map[string]interface{})["name"].(string)
    
    // Execute an activity
    greeting := fmt.Sprintf("Hello, %s! Welcome to V.E.L.O.C.I.T.Y.-WorkFlow.", name)
    
    return greeting, nil
}

func init() {
    velocity.RegisterWorkflow("greeting-workflow", GreetingWorkflow)
}
```

### Define an Activity

```go
package main

import (
    velocity "github.com/velocity-workflow/sdk-go"
)

func GreetActivity(ctx *velocity.ActivityContext, input interface{}) (interface{}, error) {
    name := input.(string)
    return fmt.Sprintf("Hello, %s!", name), nil
}

func init() {
    velocity.RegisterActivity("greet-activity", GreetActivity)
}
```

### Start a Worker

```go
package main

import (
    "log"
    velocity "github.com/velocity-workflow/sdk-go"
)

func main() {
    worker, err := velocity.NewWorker(velocity.WorkerOptions{
        Namespace: "default",
        TaskQueue: "greeting-queue",
        Workflows: map[string]velocity.WorkflowFunction{
            "greeting-workflow": GreetingWorkflow,
        },
        Activities: map[string]velocity.ActivityFunction{
            "greet-activity": GreetActivity,
        },
    })
    if err != nil {
        log.Fatal(err)
    }

    if err := worker.Run(); err != nil {
        log.Fatal(err)
    }
}
```

### Start a Workflow

```go
package main

import (
    "context"
    "log"
    velocity "github.com/velocity-workflow/sdk-go"
)

func main() {
    client, err := velocity.NewClient(velocity.ClientOptions{
        HostPort:  "localhost:7233",
        Namespace: "default",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Start workflow
    exec, err := client.Start(context.Background(), velocity.WorkflowOptions{
        WorkflowID:   "greeting-1",
        WorkflowType: "greeting-workflow",
        TaskQueue:    "greeting-queue",
        Input:        map[string]interface{}{"name": "World"},
    })
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("Started workflow: %s", exec.WorkflowID)

    // Wait for result
    handle := client.GetWorkflow(exec.WorkflowID)
    result, err := handle.Result(context.Background())
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("Workflow result: %v", result)
}
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

- `NewClient(options)` - Create a new client
- `Start(ctx, options)` - Start a new workflow
- `Execute(ctx, options)` - Start workflow and wait for result
- `Signal(ctx, workflowID, options)` - Signal a running workflow
- `Query(ctx, workflowID, options)` - Query a workflow
- `Terminate(ctx, workflowID, reason)` - Terminate a workflow
- `Cancel(ctx, workflowID)` - Cancel a workflow
- `Describe(ctx, workflowID)` - Get workflow details
- `GetHistory(ctx, workflowID)` - Get workflow history
- `GetWorkflow(workflowID)` - Get a workflow handle

### Worker

- `NewWorker(options)` - Create a new worker
- `Run()` - Start the worker (blocks)
- `Stop()` - Stop the worker
- `IsRunning()` - Check if worker is running

### Workflow Registration

- `RegisterWorkflow(name, fn)` - Register a workflow function
- `GetWorkflow(name)` - Get a registered workflow
- `HasWorkflow(name)` - Check if workflow is registered

### Activity Registration

- `RegisterActivity(name, fn)` - Register an activity function
- `GetActivity(name)` - Get a registered activity
- `HasActivity(name)` - Check if activity is registered

## Examples

See the `examples/` directory for complete examples:

- `hello_world/main.go` - Simple greeting workflow
- `timer/main.go` - Timer and sleep example
- `signal/main.go` - Signal handling example
- `child_workflow/main.go` - Child workflow composition

## Development

```bash
# Install dependencies
go mod download

# Build
go build ./...

# Test
go test ./...

# Lint
go vet ./...
```

## License

MIT
