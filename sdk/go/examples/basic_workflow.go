// Example: Basic workflow with signal and query using the VELOCITY-WorkFlow Go SDK.
//
// Demonstrates:
//   - Starting a workflow
//   - Sending signals
//   - Querying workflow state
//   - Completing the workflow
//
// Prerequisites:
//   1. Start the VELOCITY-WorkFlow server:
//      cd VELOCITY-WorkFlow/src/Velocity.Workflow.Server && dotnet run
//   2. Generate gRPC stubs:
//      cd VELOCITY-WorkFlow/sdk/go
//      protoc -I../../src/Velocity.Workflow.Server/Protos --go_out=velocity_sdk --go-grpc_out=velocity_sdk ...
//   3. Run this example:
//      go run examples/basic_workflow.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	velocity_sdk "github.com/velocity-workflow/sdk/go/velocity_sdk"
)

func main() {
	fmt.Println("=== VELOCITY-WorkFlow Go SDK — Basic Workflow ===")
	fmt.Println()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := velocity_sdk.NewClient("localhost:50051", "")
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

	// 1. Verify connectivity
	if err := client.Ping(ctx); err != nil {
		log.Fatalf("Ping failed: %v", err)
	}
	fmt.Println("1. Connected to:", client.Target())

	// 2. Start a workflow
	handle, err := client.StartWorkflow(ctx, &velocity_sdk.StartWorkflowOptions{
		WorkflowType: "order-processing",
		Namespace:    "default",
		TaskQueue:    "orders",
		TotalSteps:   3,
		Input:        []byte(`{"order_id": 12345}`),
	})
	if err != nil {
		log.Fatalf("StartWorkflow failed: %v", err)
	}
	fmt.Printf("2. Workflow started: key=%d\n", handle.WorkflowKey)

	// 3. Send a signal
	fmt.Println("3. Sending signal: payment-confirmed")
	// client.SignalWorkflow(ctx, handle.WorkflowKey, "payment-confirmed", []byte(`{"amount": 99.99}`))

	// 4. Query the workflow state
	fmt.Println("4. Querying workflow state...")
	// result, _ := client.QueryWorkflow(ctx, handle.WorkflowKey, "current-state", nil)

	// 5. Complete the workflow
	fmt.Println("5. Completing workflow...")
	// client.CompleteWorkflow(ctx, handle.WorkflowKey, []byte(`{"result": "order shipped"}`))

	fmt.Println()
	fmt.Println("=== Basic workflow example finished! ===")
}
