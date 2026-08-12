// Example: Multi-step saga with compensation using the VELOCITY-WorkFlow Go SDK.
//
// Demonstrates:
//   - Defining a saga with compensable steps
//   - Executing steps in order
//   - Triggering compensation on failure
//   - Rolling back completed steps in reverse order
//
// Prerequisites:
//   1. Start the VELOCITY-WorkFlow server:
//      cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server && dotnet run
//   2. go run examples/saga_pattern.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	velocity_sdk "github.com/velocity-workflow/sdk/go/velocity_sdk"
)

// SagaStep defines a forward action and its compensation.
type SagaStep struct {
	Name       string
	Compensate string
}

var steps = []SagaStep{
	{Name: "reserve_inventory", Compensate: "release_inventory"},
	{Name: "charge_payment", Compensate: "refund_payment"},
	{Name: "book_shipping", Compensate: "cancel_shipping"},
	{Name: "send_confirmation", Compensate: "send_cancellation_notice"},
}

// runSaga executes the saga. If simulateFailureAt >= 0, the step at that
// index will fail, triggering compensation for all previously completed steps.
func runSaga(ctx context.Context, client *velocity_sdk.Client, simulateFailureAt int) bool {
	handle, err := client.StartWorkflow(ctx, &velocity_sdk.StartWorkflowOptions{
		WorkflowType: "order-saga",
		Namespace:    "default",
		TaskQueue:    "orders",
		TotalSteps:   int32(len(steps)),
	})
	if err != nil {
		log.Printf("StartWorkflow failed: %v", err)
		return false
	}
	fmt.Printf("  Saga started: key=%d\n", handle.WorkflowKey)

	var completed []SagaStep

	for i, step := range steps {
		if simulateFailureAt >= 0 && i == simulateFailureAt {
			fmt.Printf("\n   ✗ Step '%s' FAILED — triggering compensation\n", step.Name)
			// Compensate in reverse order
			for j := len(completed) - 1; j >= 0; j-- {
				prev := completed[j]
				fmt.Printf("   Compensating: %s\n", prev.Compensate)
				// client.SignalWorkflow(ctx, handle.WorkflowKey, prev.Compensate, nil)
			}
			// client.FailWorkflow(ctx, handle.WorkflowKey, "Step "+step.Name+" failed")
			return false
		}

		fmt.Printf("   Executing: %s\n", step.Name)
		// client.SignalWorkflow(ctx, handle.WorkflowKey, step.Name, nil)
		completed = append(completed, step)
	}

	// client.CompleteWorkflow(ctx, handle.WorkflowKey, []byte(`{"status": "saga_complete"}`))
	fmt.Println("   ✓ All saga steps completed successfully")
	return true
}

func main() {
	fmt.Println("=== VELOCITY-WorkFlow Go SDK — Saga Pattern ===")
	fmt.Println()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := velocity_sdk.NewClient("localhost:50051", "")
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

	// Scenario 1: Happy path
	fmt.Println("Scenario 1: Happy path")
	runSaga(ctx, client, -1)

	// Scenario 2: Payment step fails (index=1)
	fmt.Println("\nScenario 2: Payment step fails (index=1)")
	runSaga(ctx, client, 1)

	fmt.Println("\n=== Saga examples finished! ===")
}
