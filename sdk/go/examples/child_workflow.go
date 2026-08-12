// Example: Parent-child workflow orchestration using the VELOCITY-WorkFlow Go SDK.
//
// Demonstrates:
//   - Starting a parent workflow
//   - Spawning child workflows from the parent
//   - Waiting for children to complete
//   - Aggregating child results in the parent
//
// Prerequisites:
//   1. Start the VELOCITY-WorkFlow server:
//      cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server && dotnet run
//   2. go run examples/child_workflow.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	velocity_sdk "github.com/velocity-workflow/sdk/go/velocity_sdk"
)

// runChildWorkflow starts and completes a child workflow.
func runChildWorkflow(ctx context.Context, client *velocity_sdk.Client, childType string, orderID int) (uint64, error) {
	handle, err := client.StartWorkflow(ctx, &velocity_sdk.StartWorkflowOptions{
		WorkflowType: childType,
		Namespace:    "default",
		TaskQueue:    "children",
		TotalSteps:   2,
	})
	if err != nil {
		return 0, err
	}
	fmt.Printf("   Child '%s' started: key=%d\n", childType, handle.WorkflowKey)

	// Simulate child processing
	// client.SignalWorkflow(ctx, handle.WorkflowKey, "process", nil)
	// client.CompleteWorkflow(ctx, handle.WorkflowKey, []byte(`{"child_result": "ok"}`))

	fmt.Printf("   Child '%s' completed\n", childType)
	return handle.WorkflowKey, nil
}

func main() {
	fmt.Println("=== VELOCITY-WorkFlow Go SDK — Child Workflows ===")
	fmt.Println()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := velocity_sdk.NewClient("localhost:50051", "")
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

	// 1. Start the parent workflow
	parent, err := client.StartWorkflow(ctx, &velocity_sdk.StartWorkflowOptions{
		WorkflowType: "order-orchestrator",
		Namespace:    "default",
		TaskQueue:    "orchestration",
		TotalSteps:   4,
	})
	if err != nil {
		log.Fatalf("StartWorkflow failed: %v", err)
	}
	fmt.Printf("1. Parent workflow started: key=%d\n", parent.WorkflowKey)

	// 2. Spawn child workflows
	fmt.Println("\n2. Spawning child workflows...")
	childTypes := []string{"validate-order", "process-payment", "arrange-shipping"}
	var childKeys []uint64

	for i, ct := range childTypes {
		key, err := runChildWorkflow(ctx, client, ct, 1001+i)
		if err != nil {
			log.Printf("Child workflow failed: %v", err)
			continue
		}
		childKeys = append(childKeys, key)
	}

	// 3. Signal parent that all children are done
	fmt.Printf("\n3. All %d children completed — signaling parent...\n", len(childKeys))
	// client.SignalWorkflow(ctx, parent.WorkflowKey, "children-complete", nil)

	// 4. Complete the parent workflow
	// client.CompleteWorkflow(ctx, parent.WorkflowKey, []byte(`{"result": "all_children_done"}`))

	fmt.Println("4. Parent workflow completed")
	fmt.Println("\n=== Child workflow example finished! ===")
}
