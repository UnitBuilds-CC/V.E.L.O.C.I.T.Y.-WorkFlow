# V.E.L.O.C.I.T.Y.-WorkFlow Java SDK

Java SDK for V.E.L.O.C.I.T.Y.-WorkFlow - a hardware-native zero-allocation durable execution engine and Temporal alternative.

## Installation

Add to your `pom.xml`:

```xml
<dependency>
    <groupId>io.velocity</groupId>
    <artifactId>velocity-sdk-java</artifactId>
    <version>0.1.0</version>
</dependency>
```

## Quick Start

### Define a Workflow

```java
import io.velocity.*;

public class GreetingWorkflow {
    public static Object execute(WorkflowContext context, Object input) {
        String name = (String) ((Map) input).get("name");
        System.out.println("Workflow started: " + context.getWorkflowId());
        
        String greeting = "Hello, " + name + "! Welcome to V.E.L.O.C.I.T.Y.-WorkFlow.";
        
        System.out.println("Workflow completed: " + context.getWorkflowId());
        return greeting;
    }
}
```

### Define an Activity

```java
import io.velocity.*;

public class GreetActivity {
    public static Object execute(ActivityContext context, Object input) {
        String name = (String) input;
        System.out.println("Activity executing: greeting " + name);
        return "Hello, " + name + "! Welcome to V.E.L.O.C.I.T.Y.-WorkFlow.";
    }
}
```

### Start a Worker

```java
import io.velocity.*;
import java.util.Map;
import java.util.HashMap;

public class WorkerMain {
    public static void main(String[] args) throws Exception {
        Map<String, BiFunction<WorkflowContext, Object, Object>> workflows = new HashMap<>();
        workflows.put("greeting-workflow", GreetingWorkflow::execute);

        Map<String, BiFunction<ActivityContext, Object, Object>> activities = new HashMap<>();
        activities.put("greet-activity", GreetActivity::execute);

        Worker worker = new Worker(new WorkerOptions()
            .setNamespace("default")
            .setTaskQueue("greeting-queue")
            .setWorkflows(workflows)
            .setActivities(activities));

        worker.run();
    }
}
```

### Start a Workflow

```java
import io.velocity.*;

public class ClientMain {
    public static void main(String[] args) throws Exception {
        Client client = new Client(new ClientOptions()
            .setHostPort("localhost:7233")
            .setNamespace("default"));

        // Start workflow
        WorkflowExecution execution = client.startWorkflow(new WorkflowOptions()
            .setWorkflowId("greeting-1")
            .setWorkflowType("greeting-workflow")
            .setTaskQueue("greeting-queue")
            .setInput(Map.of("name", "World")));

        System.out.println("Started workflow: " + execution.getWorkflowId());

        // Wait for result
        WorkflowHandle handle = client.getWorkflow(execution.getWorkflowId());
        Object result = handle.getResult();
        System.out.println("Workflow result: " + result);

        client.close();
    }
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

- `Client(options)` - Create a new client
- `startWorkflow(options)` - Start a new workflow execution
- `executeWorkflow(options)` - Start workflow and wait for result
- `signalWorkflow(workflowId, signalName, input)` - Signal a running workflow
- `queryWorkflow(workflowId, queryType, input)` - Query a workflow
- `terminateWorkflow(workflowId, reason)` - Terminate a workflow
- `cancelWorkflow(workflowId)` - Cancel a workflow
- `describeWorkflow(workflowId)` - Get workflow details
- `getWorkflowHistory(workflowId)` - Get workflow history
- `getWorkflow(workflowId)` - Get a workflow handle

### Worker

- `Worker(options)` - Create a new worker
- `run()` - Start the worker (blocks)
- `stop()` - Stop the worker
- `isRunning()` - Check if worker is running

### Workflow Registration

- `WorkflowRegistry.registerWorkflow(name, func)` - Register a workflow function
- `WorkflowRegistry.getWorkflow(name)` - Get a registered workflow
- `WorkflowRegistry.hasWorkflow(name)` - Check if workflow is registered

### Activity Registration

- `ActivityRegistry.registerActivity(name, func)` - Register an activity function
- `ActivityRegistry.getActivity(name)` - Get a registered activity
- `ActivityRegistry.hasActivity(name)` - Check if activity is registered

## Examples

See the `examples/` directory for complete examples:

- `HelloWorld.java` - Simple greeting workflow
- `TimerExample.java` - Timer and sleep example
- `SignalExample.java` - Signal handling example
- `ChildWorkflowExample.java` - Child workflow composition

## Development

```bash
# Build
mvn clean compile

# Test
mvn test

# Package
mvn package
```

## License

MIT
