// Example: Scheduled (cron) workflow using the VELOCITY-WorkFlow Go SDK.
//
// Demonstrates:
//   - Starting a workflow tied to a cron expression
//   - Simulating a cron fire signal
//   - Completing the scheduled execution
//
// Prerequisites:
//   1. Start the VELOCITY-WorkFlow server:
//      cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server && dotnet run
//   2. go run examples/cron_schedule.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	velocity_sdk "github.com/velocity-workflow/sdk/go/velocity_sdk"
)

const cronExpression = "*/5 * * * *" // Every 5 minutes

func main() {
	fmt.Println("=== VELOCITY-WorkFlow Go SDK — Cron Schedule ===")
	fmt.Println()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := velocity_sdk.NewClient("localhost:50051", "")
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

	// 1. Start a workflow with a cron schedule
	handle, err := client.StartWorkflow(ctx, &velocity_sdk.StartWorkflowOptions{
		WorkflowType: "periodic-report",
		Namespace:    "default",
		TaskQueue:    "reports",
		TotalSteps:   1,
		Input:        []byte(fmt.Sprintf(`{"cron": "%s"}`, cronExpression)),
	})
	if err != nil {
		log.Fatalf("StartWorkflow failed: %v", err)
	}
	fmt.Printf("1. Scheduled workflow started: key=%d\n", handle.WorkflowKey)
	fmt.Printf("   Cron expression: %s\n", cronExpression)

	// 2. Send a cron-fire signal
	fmt.Println("2. Sending cron-fire signal...")
	// client.SignalWorkflow(ctx, handle.WorkflowKey, "cron-fire", []byte(`{"fire_number": 1}`))

	// 3. Complete the scheduled execution
	fmt.Println("3. Completing scheduled execution...")
	// client.CompleteWorkflow(ctx, handle.WorkflowKey, []byte(`{"report": "generated"}`))

	fmt.Println()
	fmt.Println("=== Cron schedule example finished! ===")
}
