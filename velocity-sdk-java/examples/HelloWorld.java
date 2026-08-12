package io.velocity;

import java.util.Map;
import java.util.HashMap;

/**
 * Hello World example for V.E.L.O.C.I.T.Y.-WorkFlow Java SDK.
 */
public class HelloWorld {
    /**
     * Simple workflow that greets a user.
     */
    public static Object greetingWorkflow(WorkflowContext context, Object input) {
        @SuppressWarnings("unchecked")
        Map<String, Object> inputMap = (Map<String, Object>) input;
        String name = (String) inputMap.get("name");
        
        System.out.println("Workflow started: " + context.getWorkflowId());
        
        String greeting = "Hello, " + name + "! Welcome to V.E.L.O.C.I.T.Y.-WorkFlow.";
        
        System.out.println("Workflow completed: " + context.getWorkflowId());
        return greeting;
    }

    /**
     * Activity that generates a greeting.
     */
    public static Object greetActivity(ActivityContext context, Object input) {
        String name = (String) input;
        System.out.println("Activity executing: greeting " + name);
        return "Hello, " + name + "! Welcome to V.E.L.O.C.I.T.Y.-WorkFlow.";
    }

    public static void main(String[] args) throws Exception {
        // Register workflows and activities
        Map<String, java.util.function.BiFunction<WorkflowContext, Object, Object>> workflows = new HashMap<>();
        workflows.put("greeting-workflow", HelloWorld::greetingWorkflow);

        Map<String, java.util.function.BiFunction<ActivityContext, Object, Object>> activities = new HashMap<>();
        activities.put("greet-activity", HelloWorld::greetActivity);

        // Create worker
        Worker worker = new Worker(new WorkerOptions()
            .setNamespace("default")
            .setTaskQueue("greeting-queue")
            .setWorkflows(workflows)
            .setActivities(activities));

        // Start worker in a separate thread
        Thread workerThread = new Thread(() -> {
            try {
                worker.run();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        workerThread.setDaemon(true);
        workerThread.start();

        // Give worker time to start
        Thread.sleep(1000);

        // Create client
        Client client = new Client(new ClientOptions()
            .setHostPort("localhost:7233")
            .setNamespace("default"));

        // Start workflow
        WorkflowExecution execution = client.startWorkflow(new WorkflowOptions()
            .setWorkflowId("greeting-" + System.currentTimeMillis())
            .setWorkflowType("greeting-workflow")
            .setTaskQueue("greeting-queue")
            .setInput(Map.of("name", "World")));

        System.out.println("Started workflow: " + execution.getWorkflowId());

        // Wait for result
        WorkflowHandle handle = client.getWorkflow(execution.getWorkflowId());
        Object result = handle.getResult();

        System.out.println("Workflow result: " + result);

        // Cleanup
        worker.stop();
        client.close();
    }
}
