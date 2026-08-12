package main

import (
	"context"
	"fmt"
	"log"
	"time"

	velocity "github.com/velocity-workflow/sdk-go"
)

// GreetingWorkflow is a simple workflow that greets a user.
func GreetingWorkflow(ctx velocity.WorkflowContext, input interface{}) (interface{}, error) {
	name := input.(map[string]interface{})["name"].(string)
	log.Printf("Workflow started: %s", ctx.WorkflowID)

	// In a real implementation, this would execute an activity
	// For now, we'll just return a greeting directly
	greeting := fmt.Sprintf("Hello, %s! Welcome to V.E.L.O.C.I.T.Y.-WorkFlow.", name)

	log.Printf("Workflow completed: %s", ctx.WorkflowID)
	return greeting, nil
}

// GreetActivity is an activity that generates a greeting.
func GreetActivity(ctx *velocity.ActivityContext, input interface{}) (interface{}, error) {
	name := input.(string)
	log.Printf("Activity executing: greeting %s", name)
	return fmt.Sprintf("Hello, %s! Welcome to V.E.L.O.C.I.T.Y.-WorkFlow.", name), nil
}

func main() {
	// Register workflows and activities
	velocity.RegisterWorkflow("greeting-workflow", GreetingWorkflow)
	velocity.RegisterActivity("greet-activity", GreetActivity)

	// Start worker in a goroutine
	worker, err := velocity.NewWorker(velocity.WorkerOptions{
		Namespace: "default",
		TaskQueue: "greeting-queue",
	})
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		if err := worker.Run(); err != nil {
			log.Printf("Worker error: %v", err)
		}
	}()

	// Give worker time to start
	time.Sleep(1 * time.Second)

	// Create client
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
		WorkflowID:   fmt.Sprintf("greeting-%d", time.Now().UnixNano()),
		WorkflowType: "greeting-workflow",
		TaskQueue:    "greeting-queue",
		Input:        map[string]interface{}{"name": "World"},
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Started workflow: %s (RunID: %s)", exec.WorkflowID, exec.RunID)

	// Wait for result
	handle := client.GetWorkflow(exec.WorkflowID)
	result, err := handle.Result(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Workflow result: %v", result)

	// Stop worker
	worker.Stop()
}
